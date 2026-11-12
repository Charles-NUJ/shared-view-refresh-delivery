"""Independent schedule and subset audit for the two-version R57 extension."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "inputs"
OUTPUT = ROOT / "output"
CAPACITY = {(0, 1): 240, (1, 2): 640}
INSTALL_DELAY = 1
EVALUATION_END = 15
LAG = 6


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def edge_key(u: int, v: int) -> tuple[int, int]:
    return (u, v) if u < v else (v, u)


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_problem():
    tasks = {}
    ready = {}
    with (INPUT / "demand_manifest.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            source = int(row["source"])
            destination = int(row["destination"])
            version = int(row["version"])
            step = 1 if source < destination else -1
            tasks[row["id"]] = {
                "source": source,
                "destination": destination,
                "version": version,
                "release": int(row["release_tick"]),
                "bytes": int(row["actual_bytes"]),
                "path": list(range(source, destination + step, step)),
            }
            ready[(source, version)] = int(row["release_tick"])
    central = load(INPUT / "central_results.json")
    frontier = {int(version): int(value["frontier_tick"]) for version, value in central.items()}
    return tasks, ready, frontier


def validate_actual(result: dict, tasks: dict, ready: dict, frontier: dict) -> list[str]:
    errors = []
    selected_v1 = set(map(int, result["selected_intermediate_destinations"]))
    should_select = {
        (destination, version)
        for destination in range(3)
        for version in (1, 2)
        if version == 2 or destination in selected_v1
    }
    seen = {}
    occupancy = {}
    total_ticks = 0
    total_bytes = 0
    for row in result["schedule"]:
        task = tasks[row["task"]]
        hop = int(row["hop"])
        edge = edge_key(task["path"][hop], task["path"][hop + 1])
        duration = int(math.ceil(task["bytes"] / CAPACITY[edge]))
        if int(row["duration"]) != duration:
            errors.append(f"duration {row['task']}/{hop}")
        if int(row["start"]) < task["release"]:
            errors.append(f"release {row['task']}/{hop}")
        if int(row["finish"]) != int(row["start"]) + duration:
            errors.append(f"finish {row['task']}/{hop}")
        if hop > 0:
            previous = seen.get((row["task"], hop - 1))
            if previous is None or int(row["start"]) < int(previous["finish"]):
                errors.append(f"precedence {row['task']}/{hop}")
        seen[(row["task"], hop)] = row
        for tick in range(int(row["start"]), int(row["finish"])):
            key = (edge, tick)
            if key in occupancy:
                errors.append(f"edge conflict {edge}@{tick}")
            occupancy[key] = row["task"]
        total_ticks += duration
        total_bytes += task["bytes"]
    for task_id, task in tasks.items():
        selected = (task["destination"], task["version"]) in should_select
        for hop in range(len(task["path"]) - 1):
            if ((task_id, hop) in seen) != selected:
                errors.append(f"group completeness {task_id}/{hop}")
    visible = {}
    for destination in range(3):
        for version in (1, 2):
            key = f"d{destination}_v{version}"
            if (destination, version) not in should_select:
                visible[key] = []
                continue
            incoming = [
                (task_id, task)
                for task_id, task in tasks.items()
                if task["destination"] == destination and task["version"] == version
            ]
            remote_ready = max(
                int(seen[(task_id, len(task["path"]) - 2)]["finish"])
                for task_id, task in incoming
            ) + INSTALL_DELAY
            installed = max(remote_ready, ready[(destination, version)] + INSTALL_DELAY)
            visible[key] = list(range(installed, EVALUATION_END))
    if visible != result["visible_ticks"]:
        errors.append("visible ticks")
    fresh = {}
    for destination in range(3):
        ticks = []
        for tick in range(EVALUATION_END):
            is_fresh = any(
                tick in visible[f"d{destination}_v{version}"]
                and tick - frontier[version] <= LAG
                for version in (1, 2)
            )
            if is_fresh:
                ticks.append(tick)
        fresh[str(destination)] = ticks
    if fresh != result["fresh_ticks"]:
        errors.append("fresh ticks")
    if sum(map(len, fresh.values())) != int(result["weighted_fresh_tick_utility"]):
        errors.append("utility")
    if total_ticks != int(result["physical_edge_ticks"]):
        errors.append("edge ticks")
    if total_bytes != int(result["physical_edge_bytes"]):
        errors.append("edge bytes")
    return errors


def unit_schedule_actual_conflicts(result: dict, tasks: dict) -> dict:
    occupancy = {}
    conflicts = []
    precedence = []
    seen = {}
    for row in result["schedule"]:
        task = tasks[row["task"]]
        hop = int(row["hop"])
        edge = edge_key(task["path"][hop], task["path"][hop + 1])
        duration = int(math.ceil(task["bytes"] / CAPACITY[edge]))
        seen[(row["task"], hop)] = (int(row["start"]), int(row["start"]) + duration)
        for tick in range(int(row["start"]), int(row["start"]) + duration):
            key = (edge, tick)
            if key in occupancy:
                conflicts.append({"edge": f"{edge[0]}-{edge[1]}", "tick": tick})
            occupancy[key] = row["task"]
    for (task_id, hop), (start, _) in seen.items():
        if hop > 0 and (task_id, hop - 1) in seen and start < seen[(task_id, hop - 1)][1]:
            precedence.append({"task": task_id, "hop": hop})
    return {"edge_conflicts": conflicts, "precedence_errors": precedence}


def main() -> None:
    tasks, ready, frontier = load_problem()
    results = load(OUTPUT / "ROUND2_TWO_VERSION_RESULTS.json")
    enumeration = load(OUTPUT / "ROUND2_INTERMEDIATE_SUBSET_ENUMERATION.json")
    receipt = load(OUTPUT / "ROUND2_TWO_VERSION_RECEIPT.json")
    runner = ROOT / "run_two_version_exact.py"
    errors = []
    if receipt["input_hashes"]["runner"] != sha256(runner):
        errors.append("runner hash mismatch")
    actual_cases = (
        "optimized_actual",
        "full_refresh_exact_scheduler",
        "final_only_exact_scheduler",
        "unit_hop_choice_actual_replay",
    )
    case_checks = {}
    for name in actual_cases:
        case_errors = validate_actual(results[name], tasks, ready, frontier)
        if case_errors:
            errors.extend(f"{name}: {message}" for message in case_errors)
        case_checks[name] = {
            "errors": case_errors,
            "utility": results[name]["weighted_fresh_tick_utility"],
            "edge_ticks": results[name]["physical_edge_ticks"],
            "intermediate_destinations": results[name]["selected_intermediate_destinations"],
        }
    for row in enumeration:
        if abs(float(row["primary_mip_gap"])) > 1e-12 or abs(float(row["secondary_mip_gap"])) > 1e-12:
            errors.append(f"nonzero solver gap for {row['intermediate_destinations']}")
    best_utility = max(int(row["weighted_fresh_tick_utility"]) for row in enumeration)
    best_work = min(
        int(row["physical_edge_ticks"])
        for row in enumeration
        if int(row["weighted_fresh_tick_utility"]) == best_utility
    )
    optimized = results["optimized_actual"]
    if int(optimized["weighted_fresh_tick_utility"]) != best_utility:
        errors.append("optimized utility differs from subset enumeration")
    if int(optimized["physical_edge_ticks"]) != best_work:
        errors.append("optimized work differs from subset enumeration")
    unit_errors = unit_schedule_actual_conflicts(results["unit_hop_optimized_claim"], tasks)
    if not unit_errors["edge_conflicts"]:
        errors.append("unit-hop claim unexpectedly has no actual edge conflicts")
    evidence = {
        "state": "PASS" if not errors else "FAIL",
        "independent_view": "Recompute every actual schedule from archived bytes/capacities and compare the optimized subset against all eight zero-gap fixed-subset solves.",
        "errors": errors,
        "case_checks": case_checks,
        "subset_optimum": {
            "utility": best_utility,
            "minimum_edge_ticks": best_work,
            "best_intermediate_subsets": [
                row["intermediate_destinations"]
                for row in enumeration
                if int(row["weighted_fresh_tick_utility"]) == best_utility
                and int(row["physical_edge_ticks"]) == best_work
            ],
        },
        "unit_hop_claim_under_actual_cost": unit_errors,
        "reopened_hashes": {
            "demand_manifest": sha256(INPUT / "demand_manifest.csv"),
            "central_results": sha256(INPUT / "central_results.json"),
            "runner": sha256(runner),
            "results": sha256(OUTPUT / "ROUND2_TWO_VERSION_RESULTS.json"),
            "subset_enumeration": sha256(OUTPUT / "ROUND2_INTERMEDIATE_SUBSET_ENUMERATION.json"),
            "receipt": sha256(OUTPUT / "ROUND2_TWO_VERSION_RECEIPT.json"),
        },
    }
    target = OUTPUT / "ROUND2_TWO_VERSION_INDEPENDENT_VERIFICATION.json"
    target.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"state": evidence["state"], "errors": errors, "path": str(target)}))
    raise SystemExit(0 if not errors else 1)


if __name__ == "__main__":
    main()
