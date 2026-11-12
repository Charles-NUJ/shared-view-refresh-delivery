from __future__ import annotations

from pathlib import Path
import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]
RUN = "rollout_crp_20261022"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def worker(folder):
    sys.path.insert(0, str(folder / "prepared"))
    from rollout_calendar import rollout_crp
    contract = json.loads((folder / "prepared" / "CONTRACT.json").read_text())
    rows = []
    for case in contract["cases"]:
        result = rollout_crp(case["configuration"], case["initial"])
        output = {
            "key": case["key"],
            "role": case["role"],
            "reference_type": case["reference_type"],
            "reference_optimum": case["reference_optimum"],
            "configuration": case["configuration"],
            "initial": case["initial"],
            "result": result,
        }
        dump(folder / "cases" / case["key"] / "RESULT.json", output)
        row = {
            "key": case["key"], "role": case["role"],
            "CRP": result["base_cost"], "RCRP": result["final_cost"],
            "reference": case["reference_optimum"],
            "dominates_CRP": result["final_cost"] <= result["base_cost"] + 1e-9,
            "matches_reference": abs(result["final_cost"] - case["reference_optimum"]) <= 1e-8,
            "selected_first_edge": result["selected_first_edge"],
            "branches": len(result["branches"]), "wall_ms": result["wall_ms"],
        }
        rows.append(row)
        print(json.dumps(row), flush=True)
    dump(folder / "RESULTS.json", {"state": "FINISHED_AWAITING_INDEPENDENT_AUDIT", "cases": rows})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()
    folder = ROOT / "raw_results" / RUN
    if args.worker:
        worker(folder)
        return
    if folder.exists():
        raise FileExistsError(folder)
    (folder / "prepared").mkdir(parents=True)
    (folder / "cases").mkdir()

    cases = []
    design_root = ROOT / "raw_results" / "joint_calendar_20261019" / "models"
    for path in sorted(design_root.glob("u*/RESULT.json")):
        item = json.loads(path.read_text())
        cases.append({
            "key": "design_" + item["key"], "role": "DESIGN_SEEN_MAIN_CALENDAR",
            "reference_type": "prior_exact_JCR", "reference_optimum": item["JCR"]["objective"],
            "configuration": item["configuration"], "initial": item["initial"],
            "source": str(path.relative_to(ROOT)).replace("\\", "/"), "source_sha256": sha(path),
        })

    validation_contract_path = ROOT / "raw_results" / "joint_validation_20261019" / "prepared" / "CONTRACT.json"
    validation_results_path = ROOT / "raw_results" / "joint_validation_20261019" / "RESULTS.json"
    validation_contract = json.loads(validation_contract_path.read_text())
    references = {item["key"]: item["enumerated_optimum"] for item in json.loads(validation_results_path.read_text())["cases"]}
    for item in validation_contract["cases"]:
        cases.append({
            "key": "holdout_" + item["key"], "role": "HELD_OUT_FROM_ROLLOUT_DESIGN",
            "reference_type": "prior_full_subset_enumeration", "reference_optimum": references[item["key"]],
            "configuration": item["configuration"], "initial": item["initial"],
            "source": str(validation_contract_path.relative_to(ROOT)).replace("\\", "/"),
            "source_sha256": sha(validation_contract_path),
        })

    contract = {
        "id": RUN,
        "frozen_before_formal_run": True,
        "algorithm": "Base CRP plus every feasible first recipient deletion followed by unchanged CRP; return cheapest checked branch.",
        "questions": [
            "Does RCRP ever cost more than its CRP base?",
            "How often does it reach an independently stored exact reference?",
            "Can one-step rollout cross the three ordering barriers seen during design?",
        ],
        "claim_boundary": "Policy improvement only. Exactness is empirical per reference case; no global approximation ratio, solver speed, independent workload, or production claim.",
        "case_counts": {"design_seen": 12, "held_out": 18},
        "cases": cases,
    }
    dump(folder / "prepared" / "CONTRACT.json", contract)
    for name in ["rollout_calendar.py", "contract_prune.py", "run_rollout_study.py"]:
        shutil.copyfile(ROOT / "src" / name, folder / "prepared" / name)
    command = [sys.executable, "-I", "-B", "-X", "utf8", str(folder / "prepared" / "run_rollout_study.py"), "--worker"]
    dump(folder / "COMMAND_ENVIRONMENT_BEFORE_RUN.json", {
        "command": command, "python": sys.version, "platform": platform.platform(),
        "frozen": [{"path": str(path.relative_to(folder)).replace("\\", "/"), "sha256": sha(path)} for path in sorted((folder / "prepared").iterdir())],
    })
    with (folder / "stdout.log").open("wb") as stdout, (folder / "stderr.log").open("wb") as stderr:
        completed = subprocess.run(command, cwd=ROOT, stdout=stdout, stderr=stderr)
    dump(folder / "RECEIPT.json", {"exit_code": completed.returncode, "state": "FINISHED_AWAITING_AUDIT" if completed.returncode == 0 else "FAILED_PRESERVED"})
    dump(folder / "LOCAL_FILE_MANIFEST.json", [
        {"path": str(path.relative_to(folder)).replace("\\", "/"), "bytes": path.stat().st_size, "sha256": sha(path)}
        for path in sorted(folder.rglob("*")) if path.is_file()
    ])
    print(json.dumps({"run": RUN, "exit_code": completed.returncode, "cases": len(cases)}))
    if completed.returncode:
        print((folder / "stderr.log").read_text(errors="replace")[-4000:])
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
