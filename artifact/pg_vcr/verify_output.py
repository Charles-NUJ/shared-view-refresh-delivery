"""Independent reopen, hash, objective, and pressure-gate audit for R60."""
from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path


HERE = Path(__file__).resolve().parent
OUTPUT = HERE / "output"
SUMMARY = OUTPUT / "SUMMARY.csv"
PROTOCOL = OUTPUT / "PROTOCOL.json"
PARENT = OUTPUT / "PARENT_CONFIG.json"
MANIFEST = OUTPUT / "FILE_MANIFEST.json"
RESULT = OUTPUT / "INDEPENDENT_VERIFICATION.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def objective(row: dict) -> tuple:
    return (
        int(row["correctness_failures"]),
        int(row["final_version_failures"]),
        int(row["rho_0_9_violations"]),
        float(row["freshness_shortfall"]),
        float(row["maximum_edge_work"]),
        int(row["total_edge_bytes"]),
        int(row["makespan"]),
    )


def outcome(left: dict, right: dict) -> str:
    return "better" if objective(left) < objective(right) else "equal" if objective(left) == objective(right) else "worse"


def edge_key(left: int, right: int) -> tuple[int, int]:
    return (left, right) if left < right else (right, left)


def main() -> None:
    protocol = load(PROTOCOL)
    parent = load(PARENT)
    manifest = load(MANIFEST)
    errors = []
    for record in manifest["files"]:
        path = OUTPUT / record["path"]
        if not path.is_file():
            errors.append({"kind": "missing", "path": record["path"]})
            continue
        actual = sha256(path)
        if actual != record["sha256"]:
            errors.append(
                {"kind": "hash", "path": record["path"], "expected": record["sha256"], "actual": actual}
            )

    with SUMMARY.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    expected_rows = (
        len(protocol["key_layouts"])
        * len(protocol["demand_profiles"])
        * len(protocol["topologies"])
        * len(protocol["capacities"])
        * len(protocol["methods"])
    )
    if len(rows) != expected_rows:
        errors.append({"kind": "row_count", "expected": expected_rows, "actual": len(rows)})
    case_ids = [row["case_id"] for row in rows]
    if len(set(case_ids)) != len(case_ids):
        errors.append({"kind": "duplicate_case_id"})
    failures = [
        row["case_id"]
        for row in rows
        if int(row["correctness_failures"]) or int(row["final_version_failures"])
    ]
    if failures:
        errors.append({"kind": "correctness_or_final", "cases": failures[:10]})

    index = {
        (row["demand_profile"], row["key_layout"], row["topology"], int(row["capacity"]), row["method"]): row
        for row in rows
    }
    contexts = sorted({key[:4] for key in index})
    primary = protocol["primary_method"]
    comparisons = {}
    for profile in protocol["demand_profiles"]:
        comparisons[profile] = {}
        profile_contexts = [key for key in contexts if key[0] == profile]
        for reference in ("fifo_sp", "oldest_sp", "qoda_greedy", "latest_sp", "replacement_qoda"):
            comparisons[profile][reference] = dict(
                Counter(
                    outcome(index[key + (primary,)], index[key + (reference,)])
                    for key in profile_contexts
                )
            )

    factor_map = protocol["factorial_methods"]
    factor_counts = defaultdict(Counter)
    for key in contexts:
        group = {label: index[key + (method,)] for label, method in factor_map.items()}
        factor_counts["replacement_under_fifo"][
            outcome(group["10_replacement_fifo"], group["00_no_replacement_fifo"])
        ] += 1
        factor_counts["value_density_without_replacement"][
            outcome(group["01_no_replacement_value_density"], group["00_no_replacement_fifo"])
        ] += 1
        factor_counts["joint_vs_fifo"][
            outcome(group["11_replacement_value_density"], group["00_no_replacement_fifo"])
        ] += 1
        factor_counts["joint_vs_replacement_only"][
            outcome(group["11_replacement_value_density"], group["10_replacement_fifo"])
        ] += 1
        factor_counts["joint_vs_value_density_only"][
            outcome(group["11_replacement_value_density"], group["01_no_replacement_value_density"])
        ] += 1

    period = int(parent["version_period_ticks"])
    pressure_checks = 0
    activated = 0
    inactive = 0
    for path in sorted((OUTPUT / "cases_original_profile").glob("*pressure_gated_qoda.json")):
        detail = load(path)
        summary = detail["summary"]
        capacity = int(summary["capacity_bytes_per_tick"])
        by_version = defaultdict(list)
        for task in detail["tasks"]:
            by_version[int(task["version"])].append(task)
        recomputed = {}
        release_ticks = {}
        for version, tasks in sorted(by_version.items()):
            edge_work = defaultdict(float)
            for task in tasks:
                for left, right in zip(task["path"][:-1], task["path"][1:]):
                    edge_work[edge_key(int(left), int(right))] += float(task["declared_bytes"]) / capacity
            recomputed[version] = max(edge_work.values(), default=0.0) / period
            release_ticks[version] = max(int(task["release_tick"]) for task in tasks)
        stored = {int(version): float(value) for version, value in summary["pressure_by_version"].items()}
        if recomputed != stored:
            errors.append({"kind": "pressure_formula", "case": path.stem})
        crossing = [version for version, value in sorted(recomputed.items()) if value >= 1.0]
        if crossing:
            activated += 1
            first = crossing[0]
            if not summary["gate_activated"]:
                errors.append({"kind": "missed_gate", "case": path.stem})
            if int(summary["gate_activation_version"]) != first:
                errors.append({"kind": "gate_version", "case": path.stem})
            if int(summary["gate_activation_tick"]) != release_ticks[first]:
                errors.append({"kind": "gate_tick", "case": path.stem})
        else:
            inactive += 1
            if summary["gate_activated"]:
                errors.append({"kind": "spurious_gate", "case": path.stem})
        pressure_checks += 1

    receipt = load(OUTPUT / "EXECUTION_RECEIPT.json")
    if receipt["state"] != "PASS" or receipt["archive_replay_errors"]:
        errors.append({"kind": "execution_receipt", "value": receipt})
    result = {
        "state": "PASS" if not errors else "FAIL",
        "errors": errors,
        "manifest_files_checked": len(manifest["files"]),
        "summary_sha256": sha256(SUMMARY),
        "rows": len(rows),
        "unique_cases": len(set(case_ids)),
        "correctness_or_final_failures": len(failures),
        "comparisons": comparisons,
        "factor_counts": {key: dict(value) for key, value in factor_counts.items()},
        "pressure_cases_checked": pressure_checks,
        "pressure_gate_activated": activated,
        "pressure_gate_inactive": inactive,
        "archive_replay_checks": receipt["archive_replay_checks"],
        "archive_replay_errors": len(receipt["archive_replay_errors"]),
    }
    RESULT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
