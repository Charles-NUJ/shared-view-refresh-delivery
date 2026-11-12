"""Run the frozen R40 joint version-closure and topology experiment."""
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


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "R40_JOINT_CLOSURE_PROTOCOL.json"
DEFAULT_OUTPUT = ROOT / "raw_results" / "r40_joint_closure_20261101"


def simulate_replacement(
    tasks: list[dict],
    paths: dict[str, list[int]],
    graph: nx.Graph,
    capacity: int,
    method: str,
    ranks: dict[str, int],
    partials: dict,
    central: dict,
    config: dict,
) -> tuple[dict, dict]:
    n = int(config["sites"])
    versions = int(config["versions"])
    install_delay = int(config["install_delay_ticks"])
    query_weights = list(map(int, config["query_weights"]))
    states = []
    for task in tasks:
        states.append(
            {
                **task,
                "path": paths[task["id"]],
                "hop": 0,
                "ready": int(task["release_tick"]),
                "active": False,
                "done": False,
                "dropped": False,
                "arrival_tick": None,
            }
        )
    edges = sorted(base.edge_key(*edge) for edge in graph.edges())
    if capacity <= 0 or int(config["version_period_ticks"]) <= 0:
        raise ValueError("capacity and version period must be positive")
    pressure_by_version = {}
    for version in range(1, versions + 1):
        offered_work = {edge: 0.0 for edge in edges}
        for state in states:
            if int(state["version"]) != version:
                continue
            for left, right in zip(state["path"][:-1], state["path"][1:]):
                offered_work[base.edge_key(left, right)] += float(state["declared_bytes"]) / capacity
        pressure_by_version[version] = max(offered_work.values(), default=0.0) / int(
            config["version_period_ticks"]
        )
    active = {edge: None for edge in edges}
    edge_bytes = {edge: 0 for edge in edges}
    edge_busy = {edge: 0 for edge in edges}
    arrivals = defaultdict(set)
    local_added = set()
    closure_scheduled = set()
    pending_installs = []
    installed = {destination: 0 for destination in range(n)}
    histories = {destination: [(0, 0)] for destination in range(n)}
    install_records = []
    closure_records = []
    skipped_installs = 0
    dropped_tasks = 0
    dropped_source_bytes = 0
    persistent_gated_methods = {"overlap_gated_closure", "overlap_gated_qoda"}
    event_gated_methods = {"event_gated_closure", "event_gated_qoda"}
    pressure_gated_methods = {"pressure_gated_qoda"}
    gated_methods = persistent_gated_methods | event_gated_methods | pressure_gated_methods
    gate_active = method not in gated_methods
    gate_activation_tick = None
    gate_activation_events = 0
    gate_activation_version = None
    last_observed_pressure = None
    pressure_checked_versions = set()
    t = 0
    schedule_steps = 0

    def add_local(now: int) -> None:
        for destination in range(n):
            for version in range(1, versions + 1):
                key = (destination, version)
                if key not in local_added and partials[(destination, version)]["ready_tick"] <= now:
                    arrivals[key].add(destination)
                    local_added.add(key)

    def update_pressure_gate(now: int) -> None:
        nonlocal gate_active, gate_activation_tick, gate_activation_events
        nonlocal gate_activation_version, last_observed_pressure
        if method not in pressure_gated_methods:
            return
        for version in range(1, versions + 1):
            if version in pressure_checked_versions:
                continue
            version_states = [state for state in states if int(state["version"]) == version]
            if not all(int(state["release_tick"]) <= now for state in version_states):
                continue
            pressure_checked_versions.add(version)
            last_observed_pressure = float(pressure_by_version[version])
            if not gate_active and last_observed_pressure >= 1.0:
                gate_active = True
                gate_activation_tick = now
                gate_activation_version = version
                gate_activation_events = 1

    def discard_superseded(now: int) -> None:
        nonlocal dropped_tasks, dropped_source_bytes, gate_active, gate_activation_tick, gate_activation_events
        update_pressure_gate(now)
        latest_ready = {}
        for state in states:
            if state["release_tick"] <= now:
                pair = (state["source"], state["destination"])
                latest_ready[pair] = max(latest_ready.get(pair, 0), state["version"])
        replaceable = [
            state
            for state in states
            if (
                not state["done"]
                and not state["active"]
                and state["hop"] == 0
                and state["release_tick"] <= now
                and state["version"] < latest_ready[(state["source"], state["destination"])]
            )
        ]
        if method in event_gated_methods:
            gate_active = bool(replaceable)
            if gate_active:
                gate_activation_events += 1
                if gate_activation_tick is None:
                    gate_activation_tick = now
        elif method in persistent_gated_methods and not gate_active:
            if not replaceable:
                return
            gate_active = True
            gate_activation_tick = now
            gate_activation_events = 1
        if not gate_active:
            return
        for state in replaceable:
            state["done"] = True
            state["dropped"] = True
            dropped_tasks += 1
            dropped_source_bytes += int(state["actual_bytes"])

    def add_closures(now: int) -> None:
        for destination in range(n):
            for version in range(1, versions + 1):
                key = (destination, version)
                if key in closure_scheduled or len(arrivals[key]) != n:
                    continue
                rows = base.merge_rows([partials[(source, version)]["rows"] for source in range(n)])
                digest = base.sha_bytes(base.payload_bytes(rows))
                correct = digest == central[version]["payload_sha256"]
                closure_records.append(
                    {
                        "destination": destination,
                        "version": version,
                        "closure_tick": now,
                        "source_count": len(arrivals[key]),
                        "payload_sha256": digest,
                        "expected_sha256": central[version]["payload_sha256"],
                        "correct": correct,
                    }
                )
                heapq.heappush(pending_installs, (now + install_delay, destination, version))
                closure_scheduled.add(key)

    def latest_common_ready(now: int) -> int:
        available = 0
        for version in range(1, versions + 1):
            if all(partials[(source, version)]["ready_tick"] <= now for source in range(n)):
                available = version
        return available

    def priority(state: dict, now: int) -> tuple:
        if method in gated_methods and not gate_active:
            return (
                state["release_tick"],
                state["version"],
                state["source"],
                state["destination"],
                state["id"],
            )
        if method == "random_latest16":
            return (ranks[state["id"]],)
        missing = n - len(arrivals[(state["destination"], state["version"])])
        if method in {
            "replacement_qoda",
            "overlap_gated_qoda",
            "event_gated_qoda",
            "pressure_gated_qoda",
        }:
            remaining_hops = len(state["path"]) - 1 - state["hop"]
            positive_cost = max(
                1.0,
                state["declared_bytes"] * remaining_hops / capacity,
            )
            impact = query_weights[state["destination"]] / positive_cost
            return (
                -impact,
                missing,
                -state["version"],
                state["release_tick"],
                state["id"],
            )
        if method == "replacement_fifo":
            return (
                state["release_tick"],
                state["version"],
                state["source"],
                state["destination"],
                state["id"],
            )
        if method == "replacement_closure_density":
            remaining_hops = len(state["path"]) - 1 - state["hop"]
            positive_cost = max(
                1.0,
                state["declared_bytes"] * remaining_hops / capacity,
            )
            version_gain = max(1, state["version"] - installed[state["destination"]])
            closure_cost = positive_cost * max(1, missing)
            density = query_weights[state["destination"]] * version_gain / closure_cost
            return (
                -density,
                missing,
                -state["version"],
                state["release_tick"],
                state["id"],
            )
        target = latest_common_ready(now)
        target_penalty = 0 if state["version"] == target else 1
        return (
            target_penalty,
            missing,
            -state["version"],
            installed[state["destination"]],
            state["release_tick"],
            state["id"],
        )

    while True:
        for edge in edges:
            item = active[edge]
            if item is None or item[0] != t:
                continue
            _, index = item
            state = states[index]
            state["active"] = False
            state["hop"] += 1
            state["ready"] = t
            active[edge] = None
            if state["hop"] == len(state["path"]) - 1:
                state["done"] = True
                state["arrival_tick"] = t
                arrivals[(state["destination"], state["version"])].add(state["source"])

        add_local(t)
        discard_superseded(t)
        while pending_installs and pending_installs[0][0] <= t:
            install_tick, destination, version = heapq.heappop(pending_installs)
            if version > installed[destination]:
                installed[destination] = version
                histories[destination].append((install_tick, version))
                install_records.append(
                    {"destination": destination, "version": version, "install_tick": install_tick}
                )
            else:
                skipped_installs += 1
        add_closures(t)

        for edge in edges:
            if active[edge] is not None:
                continue
            candidates = []
            for index, state in enumerate(states):
                if state["done"] or state["active"] or state["ready"] > t:
                    continue
                path = state["path"]
                next_edge = base.edge_key(path[state["hop"]], path[state["hop"] + 1])
                if next_edge == edge:
                    candidates.append((priority(state, t), index))
            if not candidates:
                continue
            _, index = min(candidates, key=lambda item: item[0])
            state = states[index]
            duration = max(1, int(math.ceil(state["actual_bytes"] / capacity)))
            state["active"] = True
            active[edge] = (t + duration, index)
            edge_bytes[edge] += int(state["actual_bytes"])
            edge_busy[edge] += duration
            schedule_steps += 1

        complete = all(state["done"] for state in states)
        if complete and not pending_installs and all(item is None for item in active.values()):
            break
        next_times = [item[0] for item in active.values() if item is not None and item[0] > t]
        next_times.extend(
            state["ready"]
            for state in states
            if not state["done"] and not state["active"] and state["ready"] > t
        )
        next_times.extend(
            partials[(destination, version)]["ready_tick"]
            for destination in range(n)
            for version in range(1, versions + 1)
            if (destination, version) not in local_added
            and partials[(destination, version)]["ready_tick"] > t
        )
        if pending_installs and pending_installs[0][0] > t:
            next_times.append(pending_installs[0][0])
        if not next_times:
            raise RuntimeError(f"replacement schedule deadlock at t={t}")
        t = min(next_times)

    makespan = max((tick for history in histories.values() for tick, _ in history), default=0)
    horizon = max(makespan, max(item["frontier_tick"] for item in central.values())) + int(
        config["version_period_ticks"]
    )
    warmup = int(config["warmup_ticks"])
    if horizon - warmup <= 0:
        raise AssertionError("freshness denominator must be positive")
    lag = int(config["primary_lag_ticks"])
    fractions = {}
    mean_ages = {}
    for destination in range(n):
        history = sorted(histories[destination])
        position = 0
        current_version = 0
        good = 0
        age_sum = 0.0
        for tick in range(warmup, horizon):
            while position + 1 < len(history) and history[position + 1][0] <= tick:
                position += 1
                current_version = history[position][1]
            frontier = 0 if current_version == 0 else central[current_version]["frontier_tick"]
            age = tick - frontier
            age_sum += age
            good += int(age <= lag)
        fractions[str(destination)] = good / (horizon - warmup)
        mean_ages[str(destination)] = age_sum / (horizon - warmup)
    violations = {
        str(rho): sum(fractions[str(destination)] + 1e-12 < float(rho) for destination in range(n))
        for rho in config["rho_values"]
    }
    maximum_edge_bytes = max(edge_bytes.values(), default=0)
    maximum_edge_work = max((value / capacity for value in edge_bytes.values()), default=0.0)
    correctness_failures = sum(not record["correct"] for record in closure_records)
    final_version_failures = sum(installed[destination] != versions for destination in range(n))
    summary = {
        "method": method,
        "topology": graph.graph["name"],
        "capacity_bytes_per_tick": capacity,
        "tasks": len(states),
        "dropped_source_tasks": dropped_tasks,
        "dropped_source_bytes": dropped_source_bytes,
        "gate_rule": "activate_on_first_replaceable_waiting_object" if method in gated_methods else "always_enabled",
        "gate_activated": gate_activation_tick is not None,
        "gate_activation_tick": gate_activation_tick,
        "gate_activation_events": gate_activation_events,
        "gate_activation_version": gate_activation_version,
        "last_observed_pressure": last_observed_pressure,
        "pressure_by_version": pressure_by_version if method in pressure_gated_methods else {},
        "schedule_steps": schedule_steps,
        "closures": len(closure_records),
        "visible_installs": len(install_records),
        "skipped_older_installs": skipped_installs,
        "correctness_failures": correctness_failures,
        "final_version_failures": final_version_failures,
        "makespan_ticks": makespan,
        "horizon_ticks": horizon,
        "total_edge_bytes": sum(edge_bytes.values()),
        "maximum_edge_bytes": maximum_edge_bytes,
        "maximum_edge_work_ticks": maximum_edge_work,
        "maximum_edge_utilization": max(
            (value / max(1, makespan) for value in edge_busy.values()), default=0.0
        ),
        "minimum_freshness_fraction": min(fractions.values()),
        "mean_installed_age_ticks": sum(mean_ages.values()) / n,
        "contract_violations": violations,
    }
    detail = {
        "summary": summary,
        "replacement_rule": config["replacement_rule"],
        "edge_bytes": {f"{u}-{v}": value for (u, v), value in edge_bytes.items()},
        "edge_busy_ticks": {f"{u}-{v}": value for (u, v), value in edge_busy.items()},
        "freshness_fraction_by_destination": fractions,
        "mean_age_by_destination": mean_ages,
        "install_histories": {str(key): value for key, value in histories.items()},
        "install_records": install_records,
        "closure_records": closure_records,
        "tasks": [
            {
                "id": state["id"],
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
            }
            for state in states
        ],
    }
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
            "runner_dependency": str(Path(base.__file__).resolve()),
            "runner_dependency_sha256": base.sha_file(Path(base.__file__).resolve()),
        },
    )
    partials, tasks, central = base.generate_sql_data(output, config)
    counterexample = base.closure_counterexample(output)
    topologies = config["topologies"][:1] if smoke else config["topologies"]
    capacities = config["capacity_bytes_per_tick"][1:2] if smoke else config["capacity_bytes_per_tick"]
    random_seeds = config["random_seeds"][:2] if smoke else config["random_seeds"]
    methods = [item["id"] for item in config["methods"]]
    graphs = {name: base.make_graph(name, int(config["sites"])) for name in topologies}
    base.dump_json(output / "TOPOLOGIES.json", {name: base.graph_record(graph) for name, graph in graphs.items()})
    cases_dir = output / "cases"
    cases_dir.mkdir()
    rows = []
    replacement_methods = {"latest_sp", "closure_minload", "random_latest16"}
    for topology, graph in graphs.items():
        for capacity in capacities:
            for method in methods:
                started = time.perf_counter_ns()
                random_method = method in {"random16", "random_latest16"}
                if method in {"latest_sp"}:
                    routing_method = "fifo_sp"
                elif method == "closure_minload":
                    routing_method = "greedy_minload"
                elif method == "random_latest16":
                    routing_method = "random16"
                else:
                    routing_method = method
                if not random_method:
                    paths, ranks = base.assign_paths(tasks, graph, capacity, routing_method)
                    if method in replacement_methods:
                        summary, detail = simulate_replacement(
                            tasks, paths, graph, capacity, method, ranks, partials, central, config
                        )
                    else:
                        summary, detail = base.simulate(
                            tasks, paths, graph, capacity, method, ranks, partials, central, config
                        )
                        summary.setdefault("dropped_source_tasks", 0)
                        summary.setdefault("dropped_source_bytes", 0)
                        summary.setdefault("final_version_failures", 0)
                    summary["freshness_shortfall_rho_0_9"] = sum(
                        max(0.0, 0.9 - float(value))
                        for value in detail["freshness_fraction_by_destination"].values()
                    )
                    summary["selected_random_seed"] = ""
                else:
                    candidates = []
                    for seed in random_seeds:
                        paths, ranks = base.assign_paths(tasks, graph, capacity, "random16", int(seed))
                        if method == "random_latest16":
                            candidate_summary, candidate_detail = simulate_replacement(
                                tasks, paths, graph, capacity, method, ranks, partials, central, config
                            )
                        else:
                            candidate_summary, candidate_detail = base.simulate(
                                tasks, paths, graph, capacity, method, ranks, partials, central, config
                            )
                            candidate_summary.setdefault("dropped_source_tasks", 0)
                            candidate_summary.setdefault("dropped_source_bytes", 0)
                            candidate_summary.setdefault("final_version_failures", 0)
                        candidate_summary["freshness_shortfall_rho_0_9"] = sum(
                            max(0.0, 0.9 - float(value))
                            for value in candidate_detail["freshness_fraction_by_destination"].values()
                        )
                        candidates.append((int(seed), candidate_summary, candidate_detail))
                    selected = min(
                        candidates,
                        key=lambda item: (
                            item[1]["correctness_failures"],
                            item[1].get("final_version_failures", 0),
                            item[1]["contract_violations"]["0.9"],
                            item[1]["freshness_shortfall_rho_0_9"],
                            item[1]["maximum_edge_work_ticks"],
                            item[1]["total_edge_bytes"],
                            item[1]["makespan_ticks"],
                            item[0],
                        ),
                    )
                    seed, summary, detail = selected
                    summary["selected_random_seed"] = seed
                    detail["random_candidate_summaries"] = [
                        {"seed": candidate_seed, **candidate_summary}
                        for candidate_seed, candidate_summary, _ in candidates
                    ]
                summary["planner_elapsed_ns"] = time.perf_counter_ns() - started
                summary["legend"] = next(item["legend"] for item in config["methods"] if item["id"] == method)
                case_id = f"{topology}_c{capacity}_{method}"
                summary["case_id"] = case_id
                detail["summary"] = summary
                base.dump_json(cases_dir / f"{case_id}.json", detail)
                rows.append(summary)

    all_fields = []
    for row in rows:
        for key in row:
            if key not in all_fields:
                all_fields.append(key)
    with (output / "SUMMARY.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=all_fields)
        writer.writeheader()
        writer.writerows(rows)

    exact = base.small_exact_oracle(output, partials, int(config["capacity_bytes_per_tick"][1]))
    primary_capacity = int(config["capacity_bytes_per_tick"][1])
    bulk_values = [
        row["maximum_edge_work_ticks"]
        for row in rows
        if row["method"] == "bulk_sp" and row["capacity_bytes_per_tick"] == primary_capacity
    ]
    comparisons = []
    for topology in topologies:
        for capacity in capacities:
            by_method = {
                row["method"]: row
                for row in rows
                if row["topology"] == topology and row["capacity_bytes_per_tick"] == capacity
            }
            proposed = by_method["closure_minload"]
            latest = by_method["latest_sp"]
            random_latest = by_method["random_latest16"]
            comparisons.append(
                {
                    "topology": topology,
                    "capacity": capacity,
                    "closure_violations_rho_0_9": proposed["contract_violations"]["0.9"],
                    "latest_violations_rho_0_9": latest["contract_violations"]["0.9"],
                    "random_latest_violations_rho_0_9": random_latest["contract_violations"]["0.9"],
                    "closure_max_edge_work": proposed["maximum_edge_work_ticks"],
                    "latest_max_edge_work": latest["maximum_edge_work_ticks"],
                    "random_latest_max_edge_work": random_latest["maximum_edge_work_ticks"],
                    "closure_min_freshness": proposed["minimum_freshness_fraction"],
                    "latest_min_freshness": latest["minimum_freshness_fraction"],
                    "random_latest_min_freshness": random_latest["minimum_freshness_fraction"],
                    "closure_shortfall_rho_0_9": proposed["freshness_shortfall_rho_0_9"],
                    "latest_shortfall_rho_0_9": latest["freshness_shortfall_rho_0_9"],
                    "random_latest_shortfall_rho_0_9": random_latest["freshness_shortfall_rho_0_9"],
                    "closure_total_edge_bytes": proposed["total_edge_bytes"],
                    "latest_total_edge_bytes": latest["total_edge_bytes"],
                    "random_latest_total_edge_bytes": random_latest["total_edge_bytes"],
                }
            )
    gates = {
        "G0": {
            "controlled_sql_occurrence": len(tasks)
            == int(config["versions"]) * int(config["sites"]) * (int(config["sites"]) - 1),
            "real_deployment_prevalence": "UNKNOWN",
        },
        "G1": {
            "evaluated": not smoke,
            "bulk_topology_range_max_edge_work_ticks": (
                max(bulk_values) - min(bulk_values) if len(bulk_values) > 1 else None
            ),
            "topology_changes_result": (
                None
                if smoke
                else max(bulk_values) - min(bulk_values) > 1e-12
            ),
        },
        "G2": {
            "early_exposure_wrong": not counterexample["early_matches_coherent"],
            "cross_version_mix_wrong": not counterexample["mixed_matches_coherent"],
        },
        "G3": {
            "matched_comparisons": comparisons,
            "closure_beats_latest_on_external_then_edge_lexicographic": sum(
                (
                    item["closure_violations_rho_0_9"],
                    item["closure_shortfall_rho_0_9"],
                    item["closure_max_edge_work"],
                    item["closure_total_edge_bytes"],
                )
                < (
                    item["latest_violations_rho_0_9"],
                    item["latest_shortfall_rho_0_9"],
                    item["latest_max_edge_work"],
                    item["latest_total_edge_bytes"],
                )
                for item in comparisons
            ),
            "closure_beats_random_latest_on_external_then_edge_lexicographic": sum(
                (
                    item["closure_violations_rho_0_9"],
                    item["closure_shortfall_rho_0_9"],
                    item["closure_max_edge_work"],
                    item["closure_total_edge_bytes"],
                )
                < (
                    item["random_latest_violations_rho_0_9"],
                    item["random_latest_shortfall_rho_0_9"],
                    item["random_latest_max_edge_work"],
                    item["random_latest_total_edge_bytes"],
                )
                for item in comparisons
            ),
            "complex_algorithm_needed": "UNDETERMINED_UNTIL_RESULT_REVIEW",
        },
        "small_exact": exact,
    }
    base.dump_json(output / "GATE_RESULTS.json", gates)
    base.dump_json(
        output / "RESULTS.json",
        {
            "state": "SMOKE_COMPLETE" if smoke else "FORMAL_JOINT_RUN_COMPLETE",
            "protocol_id": config["id"],
            "case_count": len(rows),
            "methods": methods,
            "topologies": topologies,
            "capacities": capacities,
            "gates": gates,
        },
    )
    base.write_manifest(
        output,
        [sys.executable, str(Path(__file__).resolve()), "--config", str(config_path), "--output", str(output)]
        + (["--smoke"] if smoke else []),
    )
    return {"output": str(output), "cases": len(rows), "gates": gates}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            run(args.config.resolve(), args.output.resolve(), args.smoke),
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
