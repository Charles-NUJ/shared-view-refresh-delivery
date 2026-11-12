"""Run frozen D01 experiment workers without access to the original workspace."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import platform
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parent
WORKERS = {
    "jcr": "run_joint_calendar.py",
    "tiny": "run_joint_validation.py",
    "rollout": "run_rollout_study.py",
    "controlled": "run_controlled_comparison.py",
    "heldout": "run_heldout_comparison.py",
    "feedback": "run_feedback_study.py",
    "transport": "run_joint_tcp.py",
}


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify():
    records = read(ROOT / "SOURCE_MANIFEST.json")["files"]
    errors = []
    for item in records:
        path = (ROOT / item["path"]).resolve()
        if not path.is_relative_to(ROOT) or not path.is_file():
            errors.append(item["path"])
            continue
        if path.stat().st_size != item["bytes"] or digest(path) != item["sha256"]:
            errors.append(item["path"])
    print(json.dumps({"files_checked": len(records), "errors": errors, "state": "PASS" if not errors else "FAIL"}))
    return not errors


def equivalent(left, right):
    if isinstance(left, bool) or isinstance(right, bool):
        return left == right
    if isinstance(left, (float, int)) and isinstance(right, (float, int)):
        return math.isclose(left, right, rel_tol=1e-7, abs_tol=1e-6)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(equivalent(a, b) for a, b in zip(left, right))
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(equivalent(left[k], right[k]) for k in left)
    return left == right


def execute(suite, out, limit):
    source = ROOT / "experiments" / suite
    if out.exists():
        raise FileExistsError(f"Output exists; choose a new --output: {out}")
    if out.is_relative_to(ROOT) and not out.is_relative_to(ROOT / "outputs"):
        raise ValueError("Outputs inside the repository must be placed under outputs/.")
    out.mkdir(parents=True)
    shutil.copytree(source / "prepared", out / "prepared")
    for folder in ("models", "cases", "runs"):
        (out / folder).mkdir()
    config_path = out / "prepared" / "CONTRACT.json"
    config = read(config_path)
    key = "sessions" if suite == "transport" else "cases"
    if limit is not None:
        config[key] = config[key][:limit]
        write(config_path, config)
    requested = [case["key"] for case in config[key]]
    if suite == "transport":
        dest = out / "raw_results" / "m1_main_20261014" / "inputs" / "cases"
        shutil.copytree(ROOT / "inputs" / "cases", dest)
    else:
        dest = None
    # Record what the worker will read before it starts. These files are local
    # run records and are excluded from the public source tree by .gitignore.
    frozen = [{"path": str(p.relative_to(out)).replace("\\", "/"), "sha256": digest(p)}
              for p in sorted((out / "prepared").iterdir()) if p.is_file()]
    inputs = [] if dest is None else [{"path": str(p), "sha256": digest(p)}
              for p in sorted(dest.rglob("*")) if p.is_file()]
    write(out / "ENVIRONMENT_BEFORE_RUN.json", {"python": platform.python_version(),
                                                "frozen": frozen, "inputs": inputs})
    sys.path.insert(0, str(out / "prepared"))
    spec = importlib.util.spec_from_file_location("d01_frozen_worker", out / "prepared" / WORKERS[suite])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.ROOT = out
    start = time.perf_counter()
    module.worker(out)
    elapsed = time.perf_counter() - start
    if suite == "feedback":
        actual = {}
        for key in requested:
            row = read(out / "cases" / key / "RESULT.json")
            actual[key] = {"methods": {k: v["controlled_execution_cost"] for k, v in row["methods"].items()}}
    else:
        result = read(out / "RESULTS.json")
        actual = {row["key"]: row for row in result["sessions" if suite == "transport" else "cases"]}
    expected = {row["key"]: row for row in read(source / "expected.json")["cases"] if row["key"] in set(requested)}
    errors = []
    if set(requested) != set(actual) or set(requested) != set(expected):
        errors.append({"kind": "case_keys", "requested": len(requested), "actual": len(actual), "expected": len(expected)})
    for case_key, row in expected.items():
        for field, value in row.items():
            if field == "key":
                continue
            if case_key not in actual or field not in actual[case_key] or not equivalent(actual[case_key][field], value):
                errors.append({"kind": "value", "case": case_key, "field": field, "expected": value,
                               "actual": actual.get(case_key, {}).get(field)})
    receipt = {"state": "PASS" if not errors else "FAIL", "suite": suite, "cases_checked": len(requested),
               "full_contract": limit is None, "elapsed_seconds": elapsed, "python": platform.python_version(),
               "errors": errors,
               "scope": "Comparison with stored objective/status summaries; excludes timing and tied-plan identity."}
    write(out / "REPRODUCTION_RECEIPT.json", receipt)
    inventory = [{"path": str(p.relative_to(out)).replace("\\", "/"), "sha256": digest(p)}
                 for p in sorted(out.rglob("*")) if p.is_file() and p.name != "LOCAL_FILE_MANIFEST.json"]
    write(out / "LOCAL_FILE_MANIFEST.json", inventory)
    print(json.dumps(receipt))
    return not errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=["verify", *WORKERS])
    parser.add_argument("--output", type=Path, help="A new directory for outputs; default: outputs/SUITE")
    parser.add_argument("--limit", type=int, help="Run the first N cases as a smoke check")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    if args.suite == "feedback" and args.limit is not None:
        parser.error("feedback requires the full contract for its boundary checks")
    if args.suite == "verify":
        if args.output or args.limit:
            parser.error("verify does not accept --output or --limit")
        ok = verify()
    else:
        out = (args.output or ROOT / "outputs" / args.suite).resolve()
        ok = execute(args.suite, out, args.limit)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
