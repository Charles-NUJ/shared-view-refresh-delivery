"""Run R40 R3 with fixed external windows and gather-broadcast controls."""
from __future__ import annotations

import argparse
import csv
import heapq
import json
import math
import os
import platform
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

import networkx as nx

import run_r40_initial_structural as base
import run_r40_joint_closure as joint


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "R40_ARCHITECTURE_STRUCTURE_PROTOCOL_R3.json"
DEFAULT_OUTPUT = ROOT / "raw_results" / "r40_architecture_structure_r3_20261102"


def objective_tuple(summary: dict, rho: str = "0.9") -> tuple:
    return (
        int(summary["correctness_failures"]),
        int(summary.get("final_version_failures", 0)),
        int(summary["contract_violations"][rho]),
        float(summary["freshness_shortfall_rho_0_9"]),
        float(summary["maximum_edge_work_ticks"]),
        int(summary["total_edge_bytes"]),
        int(summary["makespan_ticks"]),
    )


def deterministic_shortest_path(graph: nx.Graph, source: int, destination: int) -> list[int]:
    return list(min(nx.all_shortest_paths(graph, source, destination), key=lambda p: tuple(p)))


def topology_medoid(graph: nx.Graph) -> int:
    return min(
        graph.nodes(),
        key=lambda root: (
            sum(nx.shortest_path_length(graph, root, node) for node in graph.nodes()),
            root,
        ),
    )


def fixed_window_scores(histories: dict, central: dict, config: dict) -> dict:
    n = int(config["sites"])
    warmup = int(config["warmup_ticks"])
    lag = int(config["primary_lag_ticks"])
    period = int(config["version_period_ticks"])
    last_frontier = max(int(item["frontier_tick"]) for item in central.values())
    windows = {}
    for tail_periods in config["evaluation_tail_periods"]:
        end = last_frontier + int(tail_periods) * period
        if end - warmup <= 0:
            raise AssertionError("fixed freshness denominator must be positive")
        fractions = {}
        mean_ages = {}
        unavailable = {}
        for destination in range(n):
            history = sorted((int(t), int(v)) for t, v in histories[destination])
            position = 0
            current_version = history[0][1]
            good = 0
            missing = 0
            installed_age_sum = 0.0
            installed_ticks = 0
            for tick in range(warmup, end):
                while position + 1 < len(history) and history[position + 1][0] <= tick:
                    position += 1
                    current_version = history[position][1]
                if current_version <= 0:
                    missing += 1
                    continue
                age = tick - int(central[current_version]["frontier_tick"])
                installed_age_sum += age
                installed_ticks += 1
                good += int(age <= lag)
            denominator = end - warmup
            fractions[str(destination)] = good / denominator
            unavailable[str(destination)] = missing / denominator
            mean_ages[str(destination)] = (
                installed_age_sum / installed_ticks if installed_ticks > 0 else None
            )
        violations = {
            str(rho): sum(
                fractions[str(destination)] + 1e-12 < float(rho)
                for destination in range(n)
            )
            for rho in config["rho_values"]
        }
        windows[str(tail_periods)] = {
            "start_tick": warmup,
            "end_tick": end,
            "denominator_ticks": end - warmup,
            "freshness_fraction_by_destination": fractions,
            "unavailable_fraction_by_destination": unavailable,
            "mean_installed_age_by_destination": mean_ages,
            "contract_violations": violations,
            "freshness_shortfall_rho_0_9": sum(
                max(0.0, 0.9 - value) for value in fractions.values()
            ),
        }
    return windows


def apply_fixed_scoring(summary: dict, detail: dict, central: dict, config: dict) -> None:
    legacy = {
        key: summary.get(key)
        for key in (
            "horizon_ticks",
            "minimum_freshness_fraction",
            "mean_installed_age_ticks",
            "contract_violations",
        )
    }
    histories = {
        int(destination): [(int(t), int(v)) for t, v in history]
        for destination, history in detail["install_histories"].items()
    }
    windows = fixed_window_scores(histories, central, config)
    primary_key = str(config["primary_evaluation_tail_periods"])
    primary = windows[primary_key]
    fractions = primary["freshness_fraction_by_destination"]
    ages = [value for value in primary["mean_installed_age_by_destination"].values() if value is not None]
    versions = int(config["versions"])
    final_failures = sum(
        max(version for _, version in histories[destination]) != versions
        for destination in range(int(config["sites"]))
    )
    summary["legacy_method_dependent_window"] = legacy
    summary["evaluation_window_start_tick"] = primary["start_tick"]
    summary["evaluation_window_end_tick"] = primary["end_tick"]
    summary["horizon_ticks"] = primary["end_tick"]
    summary["minimum_freshness_fraction"] = min(fractions.values())
    summary["mean_installed_age_ticks"] = sum(ages) / len(ages) if ages else None
    summary["maximum_unavailable_fraction"] = max(
        primary["unavailable_fraction_by_destination"].values()
    )
    summary["contract_violations"] = primary["contract_violations"]
    summary["freshness_shortfall_rho_0_9"] = primary["freshness_shortfall_rho_0_9"]
    summary["final_version_failures"] = final_failures
    detail["fixed_evaluation_windows"] = windows
    detail["freshness_fraction_by_destination"] = fractions
    detail["unavailable_fraction_by_destination"] = primary[
        "unavailable_fraction_by_destination"
    ]
    detail["mean_age_by_destination"] = primary["mean_installed_age_by_destination"]
    detail["freshness_reference"] = config["freshness_reference"]


def simulate_gather_broadcast(
    graph: nx.Graph,
    root: int,
    capacity: int,
    method: str,
    partials: dict,
    central: dict,
    config: dict,
) -> tuple[dict, dict]:
    n = int(config["sites"])
    versions = int(config["versions"])
    install_delay = int(config["install_delay_ticks"])
    rounding = int(config["declared_byte_rounding"])
    transfers = []
    for version in range(1, versions + 1):
        for source in range(n):
            if source == root:
                continue
            part = partials[(source, version)]
            transfers.append(
                {
                    "id": f"g_k{version:02d}_s{source}_r{root}",
                    "stage": "gather",
                    "source": source,
                    "destination": root,
                    "logical_destination": root,
                    "version": version,
                    "release_tick": int(part["ready_tick"]),
                    "actual_bytes": int(part["actual_bytes"]),
                    "declared_bytes": int(part["declared_bytes"]),
                    "payload_sha256": part["payload_sha256"],
                    "path": deterministic_shortest_path(graph, source, root),
                    "hop": 0,
                    "ready": int(part["ready_tick"]),
                    "active": False,
                    "done": False,
                    "dropped": False,
                    "arrival_tick": None,
                }
            )

    edges = sorted(base.edge_key(*edge) for edge in graph.edges())
    active = {edge: None for edge in edges}
    edge_bytes = {edge: 0 for edge in edges}
    edge_busy = {edge: 0 for edge in edges}
    stage_edge_bytes = {"gather": 0, "broadcast": 0}
    gather_arrivals = defaultdict(set)
    root_local_added = set()
    root_closed = set()
    pending_installs = []
    installed = {destination: 0 for destination in range(n)}
    histories = {destination: [(0, 0)] for destination in range(n)}
    install_records = []
    closure_records = []
    broadcast_receipts = []
    skipped_installs = 0
    dropped_gather = 0
    dropped_broadcast = 0
    dropped_bytes = 0
    schedule_steps = 0
    t = 0

    def latest_common_ready(now: int) -> int:
        return max(
            (
                version
                for version in range(1, versions + 1)
                if all(partials[(source, version)]["ready_tick"] <= now for source in range(n))
            ),
            default=0,
        )

    def add_root_local(now: int) -> None:
        for version in range(1, versions + 1):
            if version not in root_local_added and partials[(root, version)]["ready_tick"] <= now:
                gather_arrivals[version].add(root)
                root_local_added.add(version)

    def discard_waiting(now: int) -> None:
        nonlocal dropped_gather, dropped_broadcast, dropped_bytes
        if method != "gather_broadcast_latest":
            return
        latest = {}
        for state in transfers:
            if state["release_tick"] <= now:
                pair = (state["stage"], state["source"], state["logical_destination"])
                latest[pair] = max(latest.get(pair, 0), state["version"])
        for state in transfers:
            pair = (state["stage"], state["source"], state["logical_destination"])
            if (
                not state["done"]
                and not state["active"]
                and state["hop"] == 0
                and state["release_tick"] <= now
                and state["version"] < latest.get(pair, state["version"])
            ):
                state["done"] = True
                state["dropped"] = True
                dropped_bytes += int(state["actual_bytes"])
                if state["stage"] == "gather":
                    dropped_gather += 1
                else:
                    dropped_broadcast += 1

    def create_closures(now: int) -> None:
        for version in range(1, versions + 1):
            if version in root_closed or len(gather_arrivals[version]) != n:
                continue
            rows = base.merge_rows([partials[(source, version)]["rows"] for source in range(n)])
            digest = base.sha_bytes(base.payload_bytes(rows))
            correct = digest == central[version]["payload_sha256"]
            closure_records.append(
                {
                    "root": root,
                    "version": version,
                    "closure_tick": now,
                    "source_count": len(gather_arrivals[version]),
                    "payload_sha256": digest,
                    "expected_sha256": central[version]["payload_sha256"],
                    "correct": correct,
                }
            )
            root_closed.add(version)
            heapq.heappush(pending_installs, (now + install_delay, root, version, "root"))
            actual = int(central[version]["actual_bytes"])
            declared = int(math.ceil(actual / rounding) * rounding)
            for destination in range(n):
                if destination == root:
                    continue
                path = deterministic_shortest_path(graph, root, destination)
                transfers.append(
                    {
                        "id": f"b_k{version:02d}_r{root}_d{destination}",
                        "stage": "broadcast",
                        "source": root,
                        "destination": destination,
                        "logical_destination": destination,
                        "version": version,
                        "release_tick": now,
                        "actual_bytes": actual,
                        "declared_bytes": declared,
                        "payload_sha256": central[version]["payload_sha256"],
                        "path": path,
                        "hop": 0,
                        "ready": now,
                        "active": False,
                        "done": False,
                        "dropped": False,
                        "arrival_tick": None,
                    }
                )

    def priority(state: dict, now: int) -> tuple:
        if method == "gather_broadcast_fifo":
            return (
                state["release_tick"],
                0 if state["stage"] == "broadcast" else 1,
                state["version"],
                state["id"],
            )
        target = latest_common_ready(now)
        missing = n - len(gather_arrivals[state["version"]]) if state["stage"] == "gather" else 0
        return (
            0 if state["version"] == target else 1,
            0 if state["stage"] == "broadcast" else 1,
            missing,
            -state["version"],
            state["release_tick"],
            state["id"],
        )

    while True:
        for edge in edges:
            item = active[edge]
            if item is None or item[0] != t:
                continue
            _, index = item
            state = transfers[index]
            state["active"] = False
            state["hop"] += 1
            state["ready"] = t
            active[edge] = None
            if state["hop"] == len(state["path"]) - 1:
                state["done"] = True
                state["arrival_tick"] = t
                if state["stage"] == "gather":
                    gather_arrivals[state["version"]].add(state["source"])
                else:
                    broadcast_receipts.append(
                        {
                            "destination": state["destination"],
                            "version": state["version"],
                            "arrival_tick": t,
                            "payload_sha256": state["payload_sha256"],
                        }
                    )
                    heapq.heappush(
                        pending_installs,
                        (t + install_delay, state["destination"], state["version"], "broadcast"),
                    )

        add_root_local(t)
        discard_waiting(t)
        create_closures(t)
        discard_waiting(t)

        while pending_installs and pending_installs[0][0] <= t:
            install_tick, destination, version, stage = heapq.heappop(pending_installs)
            if version > installed[destination]:
                installed[destination] = version
                histories[destination].append((install_tick, version))
                install_records.append(
                    {
                        "destination": destination,
                        "version": version,
                        "install_tick": install_tick,
                        "stage": stage,
                        "payload_sha256": central[version]["payload_sha256"],
                    }
                )
            else:
                skipped_installs += 1

        for edge in edges:
            if active[edge] is not None:
                continue
            candidates = []
            for index, state in enumerate(transfers):
                if state["done"] or state["active"] or state["ready"] > t:
                    continue
                path = state["path"]
                next_edge = base.edge_key(path[state["hop"]], path[state["hop"] + 1])
                if next_edge == edge:
                    candidates.append((priority(state, t), index))
            if not candidates:
                continue
            _, index = min(candidates, key=lambda item: item[0])
            state = transfers[index]
            duration = max(1, int(math.ceil(state["actual_bytes"] / capacity)))
            state["active"] = True
            active[edge] = (t + duration, index)
            edge_bytes[edge] += int(state["actual_bytes"])
            edge_busy[edge] += duration
            stage_edge_bytes[state["stage"]] += int(state["actual_bytes"])
            schedule_steps += 1

        final_installed = all(installed[destination] == versions for destination in range(n))
        transfers_complete = all(state["done"] for state in transfers)
        if (
            final_installed
            and transfers_complete
            and not pending_installs
            and all(item is None for item in active.values())
        ):
            break
        next_times = [item[0] for item in active.values() if item is not None and item[0] > t]
        next_times.extend(
            state["ready"]
            for state in transfers
            if not state["done"] and not state["active"] and state["ready"] > t
        )
        next_times.extend(
            partials[(root, version)]["ready_tick"]
            for version in range(1, versions + 1)
            if version not in root_local_added and partials[(root, version)]["ready_tick"] > t
        )
        if pending_installs and pending_installs[0][0] > t:
            next_times.append(pending_installs[0][0])
        if not next_times:
            raise RuntimeError(
                f"gather-broadcast deadlock t={t} root={root} method={method} "
                f"closed={sorted(root_closed)} installed={installed}"
            )
        t = min(next_times)
        if t > 100000:
            raise RuntimeError("gather-broadcast time guard exceeded")

    makespan = max((tick for history in histories.values() for tick, _ in history), default=0)
    maximum_edge_bytes = max(edge_bytes.values(), default=0)
    summary = {
        "method": method,
        "architecture": "gather_once_broadcast",
        "root": root,
        "topology": graph.graph["name"],
        "capacity_bytes_per_tick": capacity,
        "tasks": len(transfers),
        "gather_tasks": sum(state["stage"] == "gather" for state in transfers),
        "broadcast_tasks": sum(state["stage"] == "broadcast" for state in transfers),
        "dropped_source_tasks": dropped_gather,
        "dropped_broadcast_tasks": dropped_broadcast,
        "dropped_source_bytes": dropped_bytes,
        "schedule_steps": schedule_steps,
        "closures": len(closure_records),
        "visible_installs": len(install_records),
        "skipped_older_installs": skipped_installs,
        "correctness_failures": sum(not record["correct"] for record in closure_records),
        "final_version_failures": sum(installed[destination] != versions for destination in range(n)),
        "makespan_ticks": makespan,
        "total_edge_bytes": sum(edge_bytes.values()),
        "gather_edge_bytes": stage_edge_bytes["gather"],
        "broadcast_edge_bytes": stage_edge_bytes["broadcast"],
        "maximum_edge_bytes": maximum_edge_bytes,
        "maximum_edge_work_ticks": maximum_edge_bytes / capacity,
        "maximum_edge_utilization": max(
            (value / max(1, makespan) for value in edge_busy.values()), default=0.0
        ),
    }
    detail = {
        "summary": summary,
        "architecture": "gather_once_broadcast",
        "root": root,
        "root_policy": config["root_policy"],
        "replacement_rule": config["replacement_rule"],
        "edge_bytes": {f"{u}-{v}": value for (u, v), value in edge_bytes.items()},
        "edge_busy_ticks": {f"{u}-{v}": value for (u, v), value in edge_busy.items()},
        "stage_edge_bytes": stage_edge_bytes,
        "install_histories": {str(destination): history for destination, history in histories.items()},
        "install_records": install_records,
        "closure_records": closure_records,
        "broadcast_receipts": broadcast_receipts,
        "tasks": [
            {
                "id": state["id"],
                "stage": state["stage"],
                "source": state["source"],
                "destination": state["destination"],
                "version": state["version"],
                "actual_bytes": state["actual_bytes"],
                "declared_bytes": state["declared_bytes"],
                "release_tick": state["release_tick"],
                "path": state["path"],
                "arrival_tick": state["arrival_tick"],
                "dropped": state["dropped"],
                "hops_completed": state["hop"],
                "payload_sha256": state["payload_sha256"],
            }
            for state in transfers
        ],
    }
    apply_fixed_scoring(summary, detail, central, config)
    return summary, detail


def run_direct_case(
    tasks: list[dict],
    graph: nx.Graph,
    capacity: int,
    method: str,
    partials: dict,
    central: dict,
    config: dict,
    random_seeds: list[int],
) -> tuple[dict, dict]:
    replacement_methods = {
        "latest_sp",
        "closure_minload",
        "random_latest16",
        "replacement_qoda",
        "replacement_closure_density",
        "replacement_fifo",
        "overlap_gated_closure",
        "overlap_gated_qoda",
        "event_gated_closure",
        "event_gated_qoda",
        "pressure_gated_qoda",
    }
    random_method = method in {"random16", "random_latest16"}
    if method == "latest_sp":
        routing_method = "fifo_sp"
    elif method in {
        "replacement_qoda",
        "replacement_closure_density",
        "replacement_fifo",
        "overlap_gated_closure",
        "overlap_gated_qoda",
        "event_gated_closure",
        "event_gated_qoda",
        "pressure_gated_qoda",
    }:
        routing_method = "fifo_sp"
    elif method == "closure_minload":
        routing_method = "greedy_minload"
    elif method == "random_latest16":
        routing_method = "random16"
    else:
        routing_method = method

    def execute(paths, ranks):
        if method in replacement_methods:
            summary, detail = joint.simulate_replacement(
                tasks, paths, graph, capacity, method, ranks, partials, central, config
            )
        else:
            summary, detail = base.simulate(
                tasks, paths, graph, capacity, method, ranks, partials, central, config
            )
            summary.setdefault("dropped_source_tasks", 0)
            summary.setdefault("dropped_source_bytes", 0)
        summary["architecture"] = "direct_all_to_all"
        summary["root"] = ""
        apply_fixed_scoring(summary, detail, central, config)
        return summary, detail

    if not random_method:
        paths, ranks = base.assign_paths(tasks, graph, capacity, routing_method)
        summary, detail = execute(paths, ranks)
        summary["selected_random_seed"] = ""
        return summary, detail

    candidates = []
    for seed in random_seeds:
        paths, ranks = base.assign_paths(tasks, graph, capacity, "random16", int(seed))
        summary, detail = execute(paths, ranks)
        candidates.append((int(seed), summary, detail))
    seed, summary, detail = min(candidates, key=lambda item: objective_tuple(item[1]) + (item[0],))
    summary["selected_random_seed"] = seed
    detail["random_candidate_summaries"] = [
        {"seed": candidate_seed, **candidate_summary}
        for candidate_seed, candidate_summary, _ in candidates
    ]
    return summary, detail


def run(config_path: Path, output: Path, smoke: bool) -> dict:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    shutil.copyfile(config_path, output / "PROTOCOL.json")
    base.dump_json(
        output / "ENVIRONMENT.json",
        {
            "python": sys.version,
            "executable": sys.executable,
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
            "networkx": nx.__version__,
            "pid": os.getpid(),
            "smoke": smoke,
            "runner": str(Path(__file__).resolve()),
            "runner_sha256": base.sha_file(Path(__file__).resolve()),
            "base_runner": str(Path(base.__file__).resolve()),
            "base_runner_sha256": base.sha_file(Path(base.__file__).resolve()),
            "joint_runner": str(Path(joint.__file__).resolve()),
            "joint_runner_sha256": base.sha_file(Path(joint.__file__).resolve()),
        },
    )
    partials, tasks, central = base.generate_sql_data(output, config)
    base.closure_counterexample(output)
    topologies = config["topologies"][:1] if smoke else config["topologies"]
    capacities = config["capacity_bytes_per_tick"][1:2] if smoke else config["capacity_bytes_per_tick"]
    roots = [0, 2] if smoke else list(map(int, config["roots"]))
    random_seeds = list(map(int, config["random_seeds"][:2] if smoke else config["random_seeds"]))
    direct_methods = ["latest_sp", "closure_minload"] if smoke else [
        item["id"] for item in config["direct_methods"]
    ]
    gather_methods = [item["id"] for item in config["gather_broadcast_methods"]]
    labels = {
        item["id"]: item["legend"]
        for item in config["direct_methods"] + config["gather_broadcast_methods"]
    }
    graphs = {name: base.make_graph(name, int(config["sites"])) for name in topologies}
    base.dump_json(output / "TOPOLOGIES.json", {name: base.graph_record(g) for name, g in graphs.items()})
    last_frontier = max(int(item["frontier_tick"]) for item in central.values())
    base.dump_json(
        output / "PARAMETER_REALIZATION.json",
        {
            "last_commit_frontier_tick": last_frontier,
            "warmup_tick": int(config["warmup_ticks"]),
            "evaluation_windows": {
                str(tail): {
                    "start": int(config["warmup_ticks"]),
                    "end": last_frontier + int(tail) * int(config["version_period_ticks"]),
                }
                for tail in config["evaluation_tail_periods"]
            },
            "primary_tail_periods": int(config["primary_evaluation_tail_periods"]),
            "no_installed_version_is_unavailable": True,
        },
    )
    cases_dir = output / "cases"
    cases_dir.mkdir()
    rows = []
    for topology, graph in graphs.items():
        for capacity in capacities:
            for method in direct_methods:
                started = time.perf_counter_ns()
                summary, detail = run_direct_case(
                    tasks, graph, int(capacity), method, partials, central, config, random_seeds
                )
                summary["planner_elapsed_ns"] = time.perf_counter_ns() - started
                summary["legend"] = labels[method]
                case_id = f"{topology}_c{capacity}_{method}"
                summary["case_id"] = case_id
                detail["summary"] = summary
                base.dump_json(cases_dir / f"{case_id}.json", detail)
                rows.append(summary)
            for method in gather_methods:
                for root in roots:
                    started = time.perf_counter_ns()
                    summary, detail = simulate_gather_broadcast(
                        graph, int(root), int(capacity), method, partials, central, config
                    )
                    summary["planner_elapsed_ns"] = time.perf_counter_ns() - started
                    summary["legend"] = labels[method]
                    case_id = f"{topology}_c{capacity}_{method}_r{root}"
                    summary["case_id"] = case_id
                    detail["summary"] = summary
                    base.dump_json(cases_dir / f"{case_id}.json", detail)
                    rows.append(summary)

    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with (output / "SUMMARY.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    selections = []
    comparisons = []
    if not smoke:
        for topology, graph in graphs.items():
            medoid = topology_medoid(graph)
            for capacity in capacities:
                direct = {
                    row["method"]: row
                    for row in rows
                    if row["topology"] == topology
                    and int(row["capacity_bytes_per_tick"]) == int(capacity)
                    and row["architecture"] == "direct_all_to_all"
                }
                gather_group = [
                    row
                    for row in rows
                    if row["topology"] == topology
                    and int(row["capacity_bytes_per_tick"]) == int(capacity)
                    and row["architecture"] == "gather_once_broadcast"
                ]
                selected = {}
                for method in gather_methods:
                    candidates = [row for row in gather_group if row["method"] == method]
                    medoid_row = next(row for row in candidates if int(row["root"]) == medoid)
                    best_row = min(candidates, key=lambda row: objective_tuple(row) + (int(row["root"]),))
                    selected[method] = {"medoid": medoid_row, "best": best_row}
                    selections.append(
                        {
                            "topology": topology,
                            "capacity": int(capacity),
                            "method": method,
                            "medoid_root": medoid,
                            "best_root": int(best_row["root"]),
                            "medoid_objective": list(objective_tuple(medoid_row)),
                            "best_objective": list(objective_tuple(best_row)),
                            "root_max_edge_work_min": min(float(row["maximum_edge_work_ticks"]) for row in candidates),
                            "root_max_edge_work_max": max(float(row["maximum_edge_work_ticks"]) for row in candidates),
                        }
                    )
                candidates_for_winner = {
                    "Latest-SP [4]": direct["latest_sp"],
                    "ClosureMinLoad (ours)": direct["closure_minload"],
                    "GatherBroadcast-Latest medoid [5,6]": selected["gather_broadcast_latest"]["medoid"],
                    "GatherBroadcast-Latest best-root oracle [5,6]": selected["gather_broadcast_latest"]["best"],
                }
                winner = min(
                    candidates_for_winner,
                    key=lambda name: objective_tuple(candidates_for_winner[name]) + (name,),
                )
                comparisons.append(
                    {
                        "topology": topology,
                        "capacity": int(capacity),
                        "winner": winner,
                        **{
                            f"{name}::{metric}": row[metric]
                            for name, row in candidates_for_winner.items()
                            for metric in (
                                "contract_violations",
                                "freshness_shortfall_rho_0_9",
                                "maximum_edge_work_ticks",
                                "total_edge_bytes",
                                "makespan_ticks",
                                "minimum_freshness_fraction",
                            )
                        },
                    }
                )
        base.dump_json(output / "ROOT_SELECTIONS.json", selections)
        base.dump_json(output / "ARCHITECTURE_COMPARISONS.json", comparisons)

    base.dump_json(
        output / "RESULTS.json",
        {
            "state": "SMOKE_COMPLETE" if smoke else "FORMAL_R3_COMPLETE",
            "protocol_id": config["id"],
            "case_count": len(rows),
            "expected_case_count": (
                len(topologies)
                * len(capacities)
                * (len(direct_methods) + len(gather_methods) * len(roots))
            ),
            "direct_methods": direct_methods,
            "gather_broadcast_methods": gather_methods,
            "topologies": topologies,
            "capacities": capacities,
            "roots": roots,
            "fixed_evaluation_windows": True,
            "no_installed_version_is_unavailable": True,
            "architecture_comparisons": comparisons,
        },
    )
    base.write_manifest(
        output,
        [sys.executable, str(Path(__file__).resolve()), "--config", str(config_path), "--output", str(output)]
        + (["--smoke"] if smoke else []),
    )
    return {
        "output": str(output),
        "cases": len(rows),
        "state": "SMOKE_COMPLETE" if smoke else "FORMAL_R3_COMPLETE",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.config.resolve(), args.output.resolve(), args.smoke), ensure_ascii=False))


if __name__ == "__main__":
    main()
