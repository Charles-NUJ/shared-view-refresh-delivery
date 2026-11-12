"""Run the frozen R40 G0--G3 initial structural experiment.

The data plane uses actual SQLite GROUP BY queries.  The network layer is a
deterministic capacity-limited event model over non-star application-level
transfer graphs.  Literature-named policies are explicit R40 specializations,
not native-system reproductions.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
import itertools
import json
import math
import os
import platform
import random
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

import networkx as nx


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "R40_INITIAL_EXPERIMENT_PROTOCOL.json"
DEFAULT_OUTPUT = ROOT / "raw_results" / "r40_initial_structural_20261101"


def canonical_bytes(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest().upper()


def sha_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest().upper()


def dump_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def edge_key(u: int, v: int) -> tuple[int, int]:
    return (u, v) if u < v else (v, u)


def payload_bytes(rows: list[list[int]]) -> bytes:
    return canonical_bytes(rows)


def merge_rows(parts: list[list[list[int]]]) -> list[list[int]]:
    merged: dict[int, list[int]] = {}
    for part in parts:
        for key, total, count in part:
            acc = merged.setdefault(int(key), [0, 0])
            acc[0] += int(total)
            acc[1] += int(count)
    return [[key, values[0], values[1]] for key, values in sorted(merged.items())]


def make_graph(name: str, n: int) -> nx.Graph:
    if n != 6:
        raise ValueError("Frozen primary graphs require six sites")
    graph = nx.Graph(name=name)
    graph.add_nodes_from(range(n))
    if name == "line6":
        graph.add_edges_from((i, i + 1) for i in range(n - 1))
    elif name == "ring6":
        graph.add_edges_from([(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 0)])
    elif name == "tree6":
        graph.add_edges_from([(0, 1), (0, 2), (1, 3), (1, 4), (2, 5)])
    elif name == "mesh6":
        graph.add_edges_from(
            [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 0), (0, 3), (1, 4), (2, 5)]
        )
    else:
        raise KeyError(name)
    if not nx.is_connected(graph):
        raise AssertionError(f"{name} is disconnected")
    if nx.is_isomorphic(graph, nx.star_graph(n - 1)):
        raise AssertionError(f"{name} is a star")
    return graph


def graph_record(graph: nx.Graph) -> dict:
    return {
        "name": graph.graph["name"],
        "nodes": sorted(graph.nodes()),
        "edges": [list(edge_key(*edge)) for edge in sorted(graph.edges())],
        "degrees": {str(node): int(graph.degree(node)) for node in sorted(graph.nodes())},
        "connected": nx.is_connected(graph),
        "is_star": nx.is_isomorphic(graph, nx.star_graph(graph.number_of_nodes() - 1)),
    }


def generate_sql_data(output: Path, config: dict) -> tuple[dict, list[dict], dict]:
    sources_dir = output / "source_databases"
    sources_dir.mkdir(parents=True)
    n = int(config["sites"])
    versions = int(config["versions"])
    rows_per = int(config["rows_per_source_version"])
    key_count = int(config["keys"])
    period = int(config["version_period_ticks"])
    jitter = int(config["commit_jitter_ticks"])
    build_rate = int(config["build_rows_per_tick"])
    rounding = int(config["declared_byte_rounding"])
    seed = int(config["seed"])

    partials: dict[tuple[int, int], dict] = {}
    commits: dict[tuple[int, int], int] = {}
    manifest_rows: list[dict] = []
    for source in range(n):
        db_path = sources_dir / f"source_{source}.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute(
            "CREATE TABLE facts(epoch INTEGER NOT NULL, key INTEGER NOT NULL, value INTEGER NOT NULL, commit_tick INTEGER NOT NULL)"
        )
        rng = random.Random(seed + 1009 * source)
        cumulative = 0
        for version in range(1, versions + 1):
            commit_tick = (version - 1) * period + ((source * 5 + version * 7) % (jitter + 1))
            rows = []
            for _ in range(rows_per):
                # A fixed skew creates overlapping groups and variable partial payloads.
                key = min(key_count - 1, int(key_count * (rng.random() ** 2.2)))
                value = rng.randint(1, 100)
                rows.append((version, key, value, commit_tick))
            conn.executemany("INSERT INTO facts VALUES (?,?,?,?)", rows)
            conn.commit()
            cumulative += len(rows)
            started = time.perf_counter_ns()
            result = conn.execute(
                "SELECT key, SUM(value), COUNT(*) FROM facts WHERE epoch <= ? GROUP BY key ORDER BY key",
                (version,),
            ).fetchall()
            elapsed_ns = time.perf_counter_ns() - started
            result_rows = [[int(a), int(b), int(c)] for a, b, c in result]
            encoded = payload_bytes(result_rows)
            declared = int(math.ceil(len(encoded) / rounding) * rounding)
            ready_tick = commit_tick + int(math.ceil(cumulative / build_rate))
            record = {
                "source": source,
                "version": version,
                "commit_tick": commit_tick,
                "ready_tick": ready_tick,
                "input_rows": cumulative,
                "groups": len(result_rows),
                "actual_bytes": len(encoded),
                "declared_bytes": declared,
                "payload_sha256": sha_bytes(encoded),
                "sql_elapsed_ns": elapsed_ns,
                "rows": result_rows,
            }
            partials[(source, version)] = record
            commits[(source, version)] = commit_tick
            manifest_rows.append({k: v for k, v in record.items() if k != "rows"})
        conn.close()

    central: dict[int, dict] = {}
    for version in range(1, versions + 1):
        rows = merge_rows([partials[(source, version)]["rows"] for source in range(n)])
        encoded = payload_bytes(rows)
        central[version] = {
            "version": version,
            "rows": rows,
            "groups": len(rows),
            "payload_sha256": sha_bytes(encoded),
            "actual_bytes": len(encoded),
            "frontier_tick": max(commits[(source, version)] for source in range(n)),
        }

    with (output / "sql_partial_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(manifest_rows[0]))
        writer.writeheader()
        writer.writerows(manifest_rows)
    dump_json(output / "central_results.json", {str(k): v for k, v in central.items()})

    tasks = []
    for version in range(1, versions + 1):
        for source in range(n):
            part = partials[(source, version)]
            for destination in range(n):
                if source == destination:
                    continue
                tasks.append(
                    {
                        "id": f"k{version:02d}_s{source}_d{destination}",
                        "version": version,
                        "source": source,
                        "destination": destination,
                        "release_tick": part["ready_tick"],
                        "actual_bytes": part["actual_bytes"],
                        "declared_bytes": part["declared_bytes"],
                        "payload_sha256": part["payload_sha256"],
                    }
                )
    with (output / "demand_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(tasks[0]))
        writer.writeheader()
        writer.writerows(tasks)
    sql_summary = {
        "source_databases": n,
        "versions": versions,
        "source_version_partials": len(partials),
        "remote_transfer_obligations": len(tasks),
        "expected_remote_transfer_obligations": versions * n * (n - 1),
        "demand_density_excluding_self": 1.0,
        "query_placement": "Every site serves the complete global aggregate; sparse placement remains a required negative case.",
        "occurrence_scope": "Controlled executable SQL occurrence; real deployment prevalence is not established.",
    }
    dump_json(output / "G0_SQL_OCCURRENCE.json", sql_summary)
    return partials, tasks, central


def path_candidates(graph: nx.Graph, source: int, destination: int) -> list[list[int]]:
    shortest = nx.shortest_path_length(graph, source, destination)
    cutoff = min(graph.number_of_nodes() - 1, shortest + 2)
    paths = [list(path) for path in nx.all_simple_paths(graph, source, destination, cutoff=cutoff)]
    return sorted(paths, key=lambda path: (len(path), tuple(path)))


def assign_paths(
    tasks: list[dict], graph: nx.Graph, capacity: int, method: str, seed: int | None = None
) -> tuple[dict[str, list[int]], dict[str, int]]:
    cache = {
        (source, destination): path_candidates(graph, source, destination)
        for source in graph.nodes()
        for destination in graph.nodes()
        if source != destination
    }
    paths: dict[str, list[int]] = {}
    ranks: dict[str, int] = {}
    if method in {"bulk_sp", "fifo_sp", "oldest_sp", "qoda_greedy"}:
        for task in tasks:
            paths[task["id"]] = cache[(task["source"], task["destination"])][0]
        return paths, ranks

    if method == "greedy_minload":
        projected = defaultdict(float)
        ordered = sorted(tasks, key=lambda t: (t["release_tick"], t["version"], t["destination"], t["source"]))
        for task in ordered:
            choices = []
            for path in cache[(task["source"], task["destination"])]:
                edges = [edge_key(path[i], path[i + 1]) for i in range(len(path) - 1)]
                new_values = dict(projected)
                for edge in edges:
                    new_values[edge] = new_values.get(edge, 0.0) + task["declared_bytes"] / capacity
                maximum = max(new_values.values(), default=0.0)
                added = sum(task["declared_bytes"] / capacity for _ in edges)
                choices.append(((maximum, added, len(path), tuple(path)), path))
            chosen = min(choices, key=lambda item: item[0])[1]
            paths[task["id"]] = chosen
            for i in range(len(chosen) - 1):
                projected[edge_key(chosen[i], chosen[i + 1])] += task["declared_bytes"] / capacity
        return paths, ranks

    if method == "random16":
        if seed is None:
            raise ValueError("random16 requires a seed")
        rng = random.Random(seed)
        ordered = list(tasks)
        rng.shuffle(ordered)
        ranks = {task["id"]: index for index, task in enumerate(ordered)}
        for task in ordered:
            candidates = cache[(task["source"], task["destination"])]
            paths[task["id"]] = candidates[rng.randrange(len(candidates))]
        return paths, ranks
    raise KeyError(method)


def simulate(
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
        path = paths[task["id"]]
        states.append(
            {
                **task,
                "path": path,
                "hop": 0,
                "ready": int(task["release_tick"]),
                "active": False,
                "done": False,
                "arrival_tick": None,
            }
        )
    edges = sorted(edge_key(*edge) for edge in graph.edges())
    active: dict[tuple[int, int], tuple[int, int] | None] = {edge: None for edge in edges}
    edge_bytes = {edge: 0 for edge in edges}
    edge_busy = {edge: 0 for edge in edges}
    arrivals = defaultdict(set)
    local_added = set()
    closure_scheduled = set()
    pending_installs: list[tuple[int, int, int]] = []
    installed = {destination: 0 for destination in range(n)}
    histories = {destination: [(0, 0)] for destination in range(n)}
    install_records = []
    closure_records = []
    skipped_installs = 0
    t = 0
    schedule_steps = 0

    def process_local(now: int) -> None:
        for destination in range(n):
            for version in range(1, versions + 1):
                key = (destination, version)
                if key not in local_added and partials[(destination, version)]["ready_tick"] <= now:
                    arrivals[(destination, version)].add(destination)
                    local_added.add(key)

    def process_closures(now: int) -> None:
        for destination in range(n):
            for version in range(1, versions + 1):
                key = (destination, version)
                if key in closure_scheduled or len(arrivals[key]) != n:
                    continue
                rows = merge_rows([partials[(source, version)]["rows"] for source in range(n)])
                digest = sha_bytes(payload_bytes(rows))
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

    def priority(state: dict) -> tuple:
        remaining = len(state["path"]) - 1 - state["hop"]
        if method == "bulk_sp":
            return (state["version"], state["source"], state["destination"], state["id"])
        if method == "fifo_sp":
            return (state["release_tick"], state["version"], state["source"], state["destination"])
        if method in {"oldest_sp", "greedy_minload"}:
            return (installed[state["destination"]], state["version"], state["release_tick"], state["id"])
        if method == "qoda_greedy":
            positive_cost = max(1.0, state["declared_bytes"] * remaining / capacity)
            impact = query_weights[state["destination"]] / positive_cost
            return (-impact, state["version"], state["release_tick"], state["id"])
        if method == "random16":
            return (ranks[state["id"]],)
        raise KeyError(method)

    while True:
        # Complete transfers.
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

        process_local(t)

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

        process_closures(t)

        # Start one transfer per free undirected edge.
        for edge in edges:
            if active[edge] is not None:
                continue
            candidates = []
            for index, state in enumerate(states):
                if state["done"] or state["active"] or state["ready"] > t:
                    continue
                path = state["path"]
                next_edge = edge_key(path[state["hop"]], path[state["hop"] + 1])
                if next_edge == edge:
                    candidates.append((priority(state), index))
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
        all_closures = len(closure_scheduled) == n * versions
        if complete and all_closures and not pending_installs and all(value is None for value in active.values()):
            break

        next_times = []
        next_times.extend(item[0] for item in active.values() if item is not None and item[0] > t)
        next_times.extend(state["ready"] for state in states if not state["done"] and not state["active"] and state["ready"] > t)
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
            raise RuntimeError(f"deadlock at t={t}, complete={complete}, closures={len(closure_scheduled)}")
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

    violations = {}
    for rho in config["rho_values"]:
        violations[str(rho)] = sum(fractions[str(destination)] + 1e-12 < float(rho) for destination in range(n))
    maximum_edge_bytes = max(edge_bytes.values(), default=0)
    maximum_edge_work = max((value / capacity for value in edge_bytes.values()), default=0.0)
    maximum_utilization = max((value / max(1, makespan) for value in edge_busy.values()), default=0.0)
    correctness_failures = sum(not record["correct"] for record in closure_records)
    summary = {
        "method": method,
        "topology": graph.graph["name"],
        "capacity_bytes_per_tick": capacity,
        "tasks": len(states),
        "schedule_steps": schedule_steps,
        "closures": len(closure_records),
        "visible_installs": len(install_records),
        "skipped_older_installs": skipped_installs,
        "correctness_failures": correctness_failures,
        "makespan_ticks": makespan,
        "horizon_ticks": horizon,
        "total_edge_bytes": sum(edge_bytes.values()),
        "maximum_edge_bytes": maximum_edge_bytes,
        "maximum_edge_work_ticks": maximum_edge_work,
        "maximum_edge_utilization": maximum_utilization,
        "minimum_freshness_fraction": min(fractions.values()),
        "mean_installed_age_ticks": sum(mean_ages.values()) / n,
        "contract_violations": violations,
    }
    detail = {
        "summary": summary,
        "edge_bytes": {f"{u}-{v}": value for (u, v), value in edge_bytes.items()},
        "edge_busy_ticks": {f"{u}-{v}": value for (u, v), value in edge_busy.items()},
        "freshness_fraction_by_destination": fractions,
        "mean_age_by_destination": mean_ages,
        "install_histories": {str(k): v for k, v in histories.items()},
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
            }
            for state in states
        ],
    }
    return summary, detail


def closure_counterexample(output: Path) -> dict:
    # The values are cumulative source-local GROUP BY results.
    coherent_v1 = [[0, 11, 2]]
    coherent_v2 = [[0, 22, 4]]
    early_v2_source0 = [[0, 20, 2]]
    mixed_v2_v1 = [[0, 21, 3]]
    result = {
        "source_0": {"v1": [[0, 10, 1]], "v2": [[0, 20, 2]]},
        "source_1": {"v1": [[0, 1, 1]], "v2": [[0, 2, 2]]},
        "coherent_v1": coherent_v1,
        "coherent_v2": coherent_v2,
        "early_exposure": early_v2_source0,
        "cross_version_mix": mixed_v2_v1,
        "early_matches_coherent": early_v2_source0 in [coherent_v1, coherent_v2],
        "mixed_matches_coherent": mixed_v2_v1 in [coherent_v1, coherent_v2],
        "version_closure_required": True,
    }
    dump_json(output / "G2_VERSION_CLOSURE_COUNTEREXAMPLE.json", result)
    return result


def small_exact_oracle(output: Path, partials: dict, capacity: int) -> dict:
    graph = nx.cycle_graph(4)
    demands = [(0, 2), (1, 3), (2, 0), (3, 1)]
    sizes = [partials[(source, 1)]["declared_bytes"] for source, _ in demands]
    candidates = [sorted(nx.all_shortest_paths(graph, source, destination)) for source, destination in demands]

    def objective(selection):
        loads = defaultdict(float)
        for size, path in zip(sizes, selection):
            for index in range(len(path) - 1):
                loads[edge_key(path[index], path[index + 1])] += size / capacity
        return (max(loads.values()), sum(loads.values()), tuple(tuple(path) for path in selection)), loads

    best = None
    for selection in itertools.product(*candidates):
        item = objective(selection)
        if best is None or item[0] < best[0]:
            best = item
    greedy_paths = []
    greedy_loads = defaultdict(float)
    for size, options in zip(sizes, candidates):
        scored = []
        for path in options:
            trial = dict(greedy_loads)
            for index in range(len(path) - 1):
                edge = edge_key(path[index], path[index + 1])
                trial[edge] = trial.get(edge, 0.0) + size / capacity
            scored.append(((max(trial.values()), sum(trial.values()), tuple(path)), path))
        chosen = min(scored, key=lambda item: item[0])[1]
        greedy_paths.append(chosen)
        for index in range(len(chosen) - 1):
            greedy_loads[edge_key(chosen[index], chosen[index + 1])] += size / capacity
    greedy_objective, _ = objective(greedy_paths)
    result = {
        "graph": {"nodes": list(graph.nodes()), "edges": [list(edge_key(*e)) for e in graph.edges()]},
        "is_star": nx.is_isomorphic(graph, nx.star_graph(3)),
        "demands": [list(item) for item in demands],
        "sizes": sizes,
        "assignments_examined": math.prod(len(item) for item in candidates),
        "optimal_max_edge_work_ticks": best[0][0],
        "optimal_total_edge_work_ticks": best[0][1],
        "optimal_paths": [list(path) for path in best[0][2]],
        "greedy_max_edge_work_ticks": greedy_objective[0],
        "greedy_total_edge_work_ticks": greedy_objective[1],
        "greedy_paths": greedy_paths,
        "greedy_to_optimal_ratio": greedy_objective[0] / best[0][0],
    }
    dump_json(output / "SMALL_EXACT_PATH_ORACLE.json", result)
    return result


def write_manifest(output: Path, command: list[str]) -> None:
    records = []
    for path in sorted(output.rglob("*")):
        if path.is_file() and path.name != "FILE_MANIFEST.json":
            records.append(
                {
                    "path": path.relative_to(output).as_posix(),
                    "bytes": path.stat().st_size,
                    "sha256": sha_file(path),
                }
            )
    dump_json(
        output / "FILE_MANIFEST.json",
        {
            "files": records,
            "file_count": len(records),
            "byte_count": sum(record["bytes"] for record in records),
            "command": command,
        },
    )


def run(config_path: Path, output: Path, smoke: bool) -> dict:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    shutil.copyfile(config_path, output / "PROTOCOL.json")
    dump_json(
        output / "ENVIRONMENT.json",
        {
            "python": sys.version,
            "executable": sys.executable,
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
            "networkx": nx.__version__,
            "pid": os.getpid(),
            "smoke": smoke,
        },
    )
    partials, tasks, central = generate_sql_data(output, config)
    counterexample = closure_counterexample(output)

    topologies = config["topologies"][:1] if smoke else config["topologies"]
    capacities = config["capacity_bytes_per_tick"][1:2] if smoke else config["capacity_bytes_per_tick"]
    random_seeds = config["random_seeds"][:2] if smoke else config["random_seeds"]
    methods = [item["id"] for item in config["methods"]]
    cases_dir = output / "cases"
    cases_dir.mkdir()
    graphs = {name: make_graph(name, int(config["sites"])) for name in topologies}
    dump_json(output / "TOPOLOGIES.json", {name: graph_record(graph) for name, graph in graphs.items()})
    rows = []
    for topology, graph in graphs.items():
        for capacity in capacities:
            for method in methods:
                plan_started = time.perf_counter_ns()
                if method != "random16":
                    paths, ranks = assign_paths(tasks, graph, capacity, method)
                    summary, detail = simulate(
                        tasks, paths, graph, capacity, method, ranks, partials, central, config
                    )
                    summary["selected_random_seed"] = ""
                    summary["planner_elapsed_ns"] = time.perf_counter_ns() - plan_started
                else:
                    candidates = []
                    for seed in random_seeds:
                        paths, ranks = assign_paths(tasks, graph, capacity, method, int(seed))
                        candidate_summary, candidate_detail = simulate(
                            tasks, paths, graph, capacity, method, ranks, partials, central, config
                        )
                        candidates.append((int(seed), candidate_summary, candidate_detail))
                    rho_key = str(0.9)
                    selected = min(
                        candidates,
                        key=lambda item: (
                            item[1]["correctness_failures"],
                            item[1]["contract_violations"][rho_key],
                            item[1]["maximum_edge_work_ticks"],
                            item[1]["makespan_ticks"],
                            item[1]["total_edge_bytes"],
                            item[0],
                        ),
                    )
                    seed, summary, detail = selected
                    summary["selected_random_seed"] = seed
                    summary["planner_elapsed_ns"] = time.perf_counter_ns() - plan_started
                    detail["random_candidate_summaries"] = [
                        {"seed": candidate_seed, **candidate_summary}
                        for candidate_seed, candidate_summary, _ in candidates
                    ]
                legend = next(item["legend"] for item in config["methods"] if item["id"] == method)
                summary["legend"] = legend
                case_id = f"{topology}_c{capacity}_{method}"
                summary["case_id"] = case_id
                detail["summary"] = summary
                dump_json(cases_dir / f"{case_id}.json", detail)
                rows.append(summary)

    fieldnames = list(rows[0])
    with (output / "SUMMARY.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    exact = small_exact_oracle(output, partials, int(config["capacity_bytes_per_tick"][1]))
    primary_capacity = int(config["capacity_bytes_per_tick"][1])
    topology_values = [
        row["maximum_edge_work_ticks"]
        for row in rows
        if row["method"] == "bulk_sp" and row["capacity_bytes_per_tick"] == primary_capacity
    ]
    g1_effect = max(topology_values) - min(topology_values) if len(topology_values) > 1 else None
    literature = {"bulk_sp", "fifo_sp", "oldest_sp", "qoda_greedy"}
    gap_cases = []
    random_matches = []
    for topology in topologies:
        for capacity in capacities:
            group = [row for row in rows if row["topology"] == topology and row["capacity_bytes_per_tick"] == capacity]
            by_method = {row["method"]: row for row in group}
            greedy = by_method["greedy_minload"]
            best_literature = min(by_method[name]["maximum_edge_work_ticks"] for name in literature)
            if greedy["maximum_edge_work_ticks"] + 1e-12 < best_literature:
                gap_cases.append(
                    {
                        "topology": topology,
                        "capacity": capacity,
                        "greedy": greedy["maximum_edge_work_ticks"],
                        "best_literature": best_literature,
                    }
                )
            random_row = by_method["random16"]
            if abs(random_row["maximum_edge_work_ticks"] - greedy["maximum_edge_work_ticks"]) <= 1e-12:
                random_matches.append({"topology": topology, "capacity": capacity})

    gates = {
        "G0": {
            "controlled_sql_occurrence": len(tasks) == int(config["versions"]) * int(config["sites"]) * (int(config["sites"]) - 1),
            "real_deployment_prevalence": "UNKNOWN",
        },
        "G1": {
            "evaluated": not smoke,
            "bulk_topology_range_max_edge_work_ticks": g1_effect,
            "topology_changes_result": None if smoke else bool(g1_effect and g1_effect > 1e-12),
        },
        "G2": {
            "early_exposure_wrong": not counterexample["early_matches_coherent"],
            "cross_version_mix_wrong": not counterexample["mixed_matches_coherent"],
        },
        "G3": {
            "literature_gap_case_count": len(gap_cases),
            "literature_gap_cases": gap_cases,
            "random_matches_greedy_case_count": len(random_matches),
            "random_matches_greedy_cases": random_matches,
            "complex_algorithm_needed": "UNDETERMINED_UNTIL_RESULT_REVIEW",
        },
        "small_exact": exact,
    }
    dump_json(output / "GATE_RESULTS.json", gates)
    dump_json(
        output / "RESULTS.json",
        {
            "state": "SMOKE_COMPLETE" if smoke else "FORMAL_INITIAL_RUN_COMPLETE",
            "protocol_id": config["id"],
            "case_count": len(rows),
            "methods": methods,
            "topologies": topologies,
            "capacities": capacities,
            "gates": gates,
        },
    )
    write_manifest(output, [sys.executable, str(Path(__file__).resolve()), "--config", str(config_path), "--output", str(output)] + (["--smoke"] if smoke else []))
    return {"output": str(output), "cases": len(rows), "gates": gates}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    result = run(args.config.resolve(), args.output.resolve(), args.smoke)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
