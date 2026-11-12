"""Round-2 extension: two-version heterogeneous refresh on A-B-C.

This remains a tiny fixed-path problem.  It adds the temporal choice that makes
D01 a refresh problem: an intermediate cumulative version may be skipped, while
the final version must reach every destination.  The full-refresh and final-only
comparators use the same exact scheduler, so their difference comes solely from
version admission rather than a weakened service rule.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import platform
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import scipy
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "inputs"
OUTPUT = ROOT / "output"
CAPACITY = {(0, 1): 240, (1, 2): 640}
INSTALL_DELAY = 1
SERVICE_HORIZON = 32
EVALUATION_END = 15
LAG = 6


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def edge_key(u: int, v: int) -> tuple[int, int]:
    return (u, v) if u < v else (v, u)


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


class Builder:
    def __init__(self) -> None:
        self.names = []
        self.lb = []
        self.ub = []
        self.integrality = []
        self.primary = []
        self.secondary = []
        self.rows = []
        self.row_lb = []
        self.row_ub = []

    def var(self, name, *, lower=0.0, upper=1.0, integer=True, primary=0.0, secondary=0.0):
        index = len(self.names)
        self.names.append(name)
        self.lb.append(lower)
        self.ub.append(upper)
        self.integrality.append(1 if integer else 0)
        self.primary.append(primary)
        self.secondary.append(secondary)
        return index

    def constraint(self, coefficients, lower, upper):
        self.rows.append({key: value for key, value in coefficients.items() if value})
        self.row_lb.append(lower)
        self.row_ub.append(upper)

    def matrix(self):
        row_index, column_index, values = [], [], []
        for row, coefficients in enumerate(self.rows):
            for column, value in coefficients.items():
                row_index.append(row)
                column_index.append(column)
                values.append(value)
        return coo_matrix(
            (values, (row_index, column_index)),
            shape=(len(self.rows), len(self.names)),
            dtype=float,
        ).tocsr()


def load_problem():
    tasks = []
    ready = {}
    with (INPUT / "demand_manifest.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            source = int(row["source"])
            destination = int(row["destination"])
            version = int(row["version"])
            step = 1 if source < destination else -1
            tasks.append(
                {
                    "id": row["id"],
                    "source": source,
                    "destination": destination,
                    "version": version,
                    "release": int(row["release_tick"]),
                    "bytes": int(row["actual_bytes"]),
                    "path": list(range(source, destination + step, step)),
                }
            )
            ready[(source, version)] = int(row["release_tick"])
    central = json.loads((INPUT / "central_results.json").read_text(encoding="utf-8"))
    frontier = {int(version): int(value["frontier_tick"]) for version, value in central.items()}
    return sorted(tasks, key=lambda row: row["id"]), ready, frontier


def solve(tasks, ready, frontier, *, mode: str, duration_mode: str, fixed_intermediate=None):
    if mode not in {"optimized", "all_versions", "final_only", "fixed_intermediate"}:
        raise ValueError(mode)
    builder = Builder()
    group = {}
    for destination in range(3):
        for version in (1, 2):
            lower, upper = 0.0, 1.0
            if version == 2 or mode == "all_versions":
                lower = upper = 1.0
            elif mode == "final_only":
                lower = upper = 0.0
            elif mode == "fixed_intermediate":
                lower = upper = 1.0 if destination in fixed_intermediate else 0.0
            group[(destination, version)] = builder.var(
                f"g[{destination},{version}]",
                lower=lower,
                upper=upper,
                secondary=1e-6 * (10 * version + destination),
            )

    starts = {}
    durations = {}
    edge_occupants = defaultdict(list)
    tasks_by_group = defaultdict(list)
    for task in tasks:
        key = (task["destination"], task["version"])
        tasks_by_group[key].append(task)
        for hop in range(len(task["path"]) - 1):
            edge = edge_key(task["path"][hop], task["path"][hop + 1])
            duration = 1 if duration_mode == "unit-hop" else int(math.ceil(task["bytes"] / CAPACITY[edge]))
            durations[(task["id"], hop)] = duration
            variables = []
            for start in range(task["release"], SERVICE_HORIZON - duration + 1):
                index = builder.var(f"x[{task['id']},{hop},{start}]")
                starts[(task["id"], hop, start)] = index
                variables.append(index)
                for tick in range(start, start + duration):
                    edge_occupants[(edge, tick)].append(index)
            coefficients = {index: 1.0 for index in variables}
            coefficients[group[key]] = -1.0
            builder.constraint(coefficients, 0.0, 0.0)
            if hop > 0:
                previous_duration = durations[(task["id"], hop - 1)]
                precedence = {group[key]: -float(previous_duration)}
                for (task_id, candidate_hop, start), index in starts.items():
                    if task_id != task["id"]:
                        continue
                    if candidate_hop == hop:
                        precedence[index] = precedence.get(index, 0.0) + float(start)
                    elif candidate_hop == hop - 1:
                        precedence[index] = precedence.get(index, 0.0) - float(start)
                builder.constraint(precedence, 0.0, np.inf)
    for occupants in edge_occupants.values():
        builder.constraint({index: 1.0 for index in occupants}, -np.inf, 1.0)
    for key, group_tasks in tasks_by_group.items():
        builder.secondary[group[key]] += float(
            sum(
                durations[(task["id"], hop)]
                for task in group_tasks
                for hop in range(len(task["path"]) - 1)
            )
        )

    visible = {}
    for destination in range(3):
        for version in (1, 2):
            local_ready = ready[(destination, version)] + INSTALL_DELAY
            key = (destination, version)
            for tick in range(EVALUATION_END):
                visible[(destination, version, tick)] = builder.var(
                    f"y[{destination},{version},{tick}]",
                    upper=0.0 if tick < local_ready else 1.0,
                    integer=False,
                )
                builder.constraint(
                    {visible[(destination, version, tick)]: 1.0, group[key]: -1.0},
                    -np.inf,
                    0.0,
                )
                for task in tasks_by_group[key]:
                    last_hop = len(task["path"]) - 2
                    last_duration = durations[(task["id"], last_hop)]
                    coefficients = {visible[(destination, version, tick)]: 1.0}
                    for (task_id, candidate_hop, start), index in starts.items():
                        if (
                            task_id == task["id"]
                            and candidate_hop == last_hop
                            and start + last_duration + INSTALL_DELAY <= tick
                        ):
                            coefficients[index] = coefficients.get(index, 0.0) - 1.0
                    builder.constraint(coefficients, -np.inf, 0.0)

    fresh = {}
    for destination in range(3):
        for tick in range(EVALUATION_END):
            fresh[(destination, tick)] = builder.var(
                f"q[{destination},{tick}]", integer=False, primary=-1.0
            )
            coefficients = {fresh[(destination, tick)]: 1.0}
            for version in (1, 2):
                if tick - frontier[version] <= LAG:
                    coefficients[visible[(destination, version, tick)]] = -1.0
            builder.constraint(coefficients, -np.inf, 0.0)

    matrix = builder.matrix()
    base_constraint = LinearConstraint(
        matrix, np.asarray(builder.row_lb), np.asarray(builder.row_ub)
    )
    bounds = Bounds(np.asarray(builder.lb), np.asarray(builder.ub))
    integrality = np.asarray(builder.integrality, dtype=int)
    started = time.perf_counter_ns()
    primary = milp(
        c=np.asarray(builder.primary),
        integrality=integrality,
        bounds=bounds,
        constraints=base_constraint,
        options={"mip_rel_gap": 0.0, "time_limit": 120.0},
    )
    if not primary.success:
        raise RuntimeError(primary.message)
    optimum = int(round(-float(primary.fun)))
    utility_columns = [index for index, value in enumerate(builder.primary) if value]
    utility_matrix = coo_matrix(
        (
            [-builder.primary[index] for index in utility_columns],
            ([0] * len(utility_columns), utility_columns),
        ),
        shape=(1, len(builder.names)),
    ).tocsr()
    secondary = milp(
        c=np.asarray(builder.secondary),
        integrality=integrality,
        bounds=bounds,
        constraints=[
            base_constraint,
            LinearConstraint(utility_matrix, [float(optimum)], [np.inf]),
        ],
        options={"mip_rel_gap": 0.0, "time_limit": 120.0},
    )
    if not secondary.success:
        raise RuntimeError(secondary.message)
    solution = secondary.x
    schedule = []
    for (task_id, hop, start), index in starts.items():
        if solution[index] <= 0.5:
            continue
        task = next(row for row in tasks if row["id"] == task_id)
        duration = durations[(task_id, hop)]
        edge = edge_key(task["path"][hop], task["path"][hop + 1])
        schedule.append(
            {
                "task": task_id,
                "source": task["source"],
                "destination": task["destination"],
                "version": task["version"],
                "bytes": task["bytes"],
                "hop": hop,
                "edge": f"{edge[0]}-{edge[1]}",
                "capacity_bytes_per_tick": CAPACITY[edge],
                "duration": duration,
                "start": start,
                "finish": start + duration,
            }
        )
    selected_intermediate = [
        destination
        for destination in range(3)
        if solution[group[(destination, 1)]] > 0.5
    ]
    selected_groups = {
        (destination, version)
        for destination in range(3)
        for version in (1, 2)
        if solution[group[(destination, version)]] > 0.5
    }
    schedule_index = {(row["task"], row["hop"]): row for row in schedule}
    visible_ticks = {}
    for destination in range(3):
        for version in (1, 2):
            key = f"d{destination}_v{version}"
            if (destination, version) not in selected_groups:
                visible_ticks[key] = []
                continue
            incoming = tasks_by_group[(destination, version)]
            remote_ready = max(
                int(schedule_index[(task["id"], len(task["path"]) - 2)]["finish"])
                for task in incoming
            ) + INSTALL_DELAY
            installed = max(remote_ready, ready[(destination, version)] + INSTALL_DELAY)
            visible_ticks[key] = list(range(installed, EVALUATION_END))
    fresh_ticks = {}
    for destination in range(3):
        ticks = []
        for tick in range(EVALUATION_END):
            if any(
                tick in visible_ticks[f"d{destination}_v{version}"]
                and tick - frontier[version] <= LAG
                for version in (1, 2)
            ):
                ticks.append(tick)
        fresh_ticks[str(destination)] = ticks
    recomputed_utility = sum(len(ticks) for ticks in fresh_ticks.values())
    if recomputed_utility != optimum:
        raise AssertionError(
            f"schedule utility {recomputed_utility} differs from MILP objective {optimum}"
        )
    return {
        "mode": mode,
        "duration_mode": duration_mode,
        "selected_intermediate_destinations": selected_intermediate,
        "weighted_fresh_tick_utility": optimum,
        "physical_edge_ticks": int(sum(row["duration"] for row in schedule)),
        "physical_edge_bytes": int(sum(row["bytes"] for row in schedule)),
        "primary_status": int(primary.status),
        "primary_mip_gap": float(getattr(primary, "mip_gap", 0.0)),
        "secondary_status": int(secondary.status),
        "secondary_mip_gap": float(getattr(secondary, "mip_gap", 0.0)),
        "solve_elapsed_ns": time.perf_counter_ns() - started,
        "visible_ticks": visible_ticks,
        "fresh_ticks": fresh_ticks,
        "schedule": sorted(schedule, key=lambda row: (row["start"], row["edge"], row["task"])),
    }


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    tasks, ready, frontier = load_problem()
    optimized = solve(tasks, ready, frontier, mode="optimized", duration_mode="heterogeneous")
    all_versions = solve(tasks, ready, frontier, mode="all_versions", duration_mode="heterogeneous")
    final_only = solve(tasks, ready, frontier, mode="final_only", duration_mode="heterogeneous")
    unit = solve(tasks, ready, frontier, mode="optimized", duration_mode="unit-hop")
    unit_actual_replay = solve(
        tasks,
        ready,
        frontier,
        mode="fixed_intermediate",
        duration_mode="heterogeneous",
        fixed_intermediate=tuple(unit["selected_intermediate_destinations"]),
    )
    results = {
        "optimized_actual": optimized,
        "full_refresh_exact_scheduler": all_versions,
        "final_only_exact_scheduler": final_only,
        "unit_hop_optimized_claim": unit,
        "unit_hop_choice_actual_replay": unit_actual_replay,
    }
    dump(OUTPUT / "ROUND2_TWO_VERSION_RESULTS.json", results)
    enumeration = []
    for mask in range(1 << 3):
        destinations = tuple(destination for destination in range(3) if mask & (1 << destination))
        fixed = solve(
            tasks,
            ready,
            frontier,
            mode="fixed_intermediate",
            duration_mode="heterogeneous",
            fixed_intermediate=destinations,
        )
        enumeration.append(
            {
                "intermediate_destinations": list(destinations),
                "weighted_fresh_tick_utility": fixed["weighted_fresh_tick_utility"],
                "physical_edge_ticks": fixed["physical_edge_ticks"],
                "physical_edge_bytes": fixed["physical_edge_bytes"],
                "primary_mip_gap": fixed["primary_mip_gap"],
                "secondary_mip_gap": fixed["secondary_mip_gap"],
            }
        )
    dump(OUTPUT / "ROUND2_INTERMEDIATE_SUBSET_ENUMERATION.json", enumeration)
    receipt = {
        "state": "PASS",
        "scope": "A-B-C fixed paths, two cumulative versions, intermediate optional, final mandatory",
        "objective": "maximize fresh query-visible ticks; among optima minimize actual physical edge ticks",
        "input_hashes": {
            "demand_manifest": sha256(INPUT / "demand_manifest.csv"),
            "central_results": sha256(INPUT / "central_results.json"),
            "runner": sha256(Path(__file__)),
        },
        "output_hashes": {
            "results": sha256(OUTPUT / "ROUND2_TWO_VERSION_RESULTS.json"),
            "intermediate_subset_enumeration": sha256(
                OUTPUT / "ROUND2_INTERMEDIATE_SUBSET_ENUMERATION.json"
            ),
        },
        "software": {"python": platform.python_version(), "scipy": scipy.__version__},
    }
    dump(OUTPUT / "ROUND2_TWO_VERSION_RECEIPT.json", receipt)


if __name__ == "__main__":
    main()
