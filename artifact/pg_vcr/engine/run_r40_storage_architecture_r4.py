"""Run R40 R4 across storage-key overlap and transfer architectures."""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import random
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import networkx as nx

import run_r40_architecture_structure_r3 as r3
import run_r40_initial_structural as base
import run_r40_joint_closure as joint


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "R40_STORAGE_ARCHITECTURE_PROTOCOL_R4.json"
DEFAULT_OUTPUT = ROOT / "raw_results" / "r40_storage_architecture_r4_20261102"


def generate_layout_sql(output: Path, config: dict, layout: dict):
    sources_dir = output / "source_databases"
    sources_dir.mkdir(parents=True)
    n = int(config["sites"])
    versions = int(config["versions"])
    rows_per = int(config["rows_per_source_version"])
    keys_per_source = int(config["keys_per_source"])
    shared_fraction = float(layout["shared_fraction"])
    shared_count = int(round(keys_per_source * shared_fraction))
    private_count = keys_per_source - shared_count
    period = int(config["version_period_ticks"])
    jitter = int(config["commit_jitter_ticks"])
    build_rate = int(config["build_rows_per_tick"])
    rounding = int(config["declared_byte_rounding"])
    seed = int(config["seed"])

    partials, commits, manifest_rows = {}, {}, []
    local_key_domains = {}
    for source in range(n):
        shared_keys = list(range(shared_count))
        private_start = shared_count + source * private_count
        private_keys = list(range(private_start, private_start + private_count))
        local_keys = shared_keys + private_keys
        if len(local_keys) != keys_per_source or len(set(local_keys)) != keys_per_source:
            raise AssertionError("invalid local key domain")
        local_key_domains[str(source)] = local_keys
        db_path = sources_dir / f"source_{source}.sqlite"
        connection = sqlite3.connect(db_path)
        connection.execute(
            "CREATE TABLE facts(epoch INTEGER NOT NULL, key INTEGER NOT NULL, "
            "value INTEGER NOT NULL, commit_tick INTEGER NOT NULL)"
        )
        rng = random.Random(seed + 1009 * source)
        cumulative = 0
        for version in range(1, versions + 1):
            commit_tick = (version - 1) * period + ((source * 5 + version * 7) % (jitter + 1))
            rows = []
            for _ in range(rows_per):
                local_index = min(
                    keys_per_source - 1,
                    int(keys_per_source * (rng.random() ** 2.2)),
                )
                key = local_keys[local_index]
                rows.append((version, key, rng.randint(1, 100), commit_tick))
            connection.executemany("INSERT INTO facts VALUES (?,?,?,?)", rows)
            connection.commit()
            cumulative += len(rows)
            started = time.perf_counter_ns()
            result = connection.execute(
                "SELECT key, SUM(value), COUNT(*) FROM facts "
                "WHERE epoch <= ? GROUP BY key ORDER BY key",
                (version,),
            ).fetchall()
            elapsed_ns = time.perf_counter_ns() - started
            result_rows = [[int(a), int(b), int(c)] for a, b, c in result]
            encoded = base.payload_bytes(result_rows)
            record = {
                "layout": layout["id"],
                "source": source,
                "version": version,
                "commit_tick": commit_tick,
                "ready_tick": commit_tick + int(math.ceil(cumulative / build_rate)),
                "input_rows": cumulative,
                "groups": len(result_rows),
                "actual_bytes": len(encoded),
                "declared_bytes": int(math.ceil(len(encoded) / rounding) * rounding),
                "payload_sha256": base.sha_bytes(encoded),
                "sql_elapsed_ns": elapsed_ns,
                "rows": result_rows,
            }
            partials[(source, version)] = record
            commits[(source, version)] = commit_tick
            manifest_rows.append({key: value for key, value in record.items() if key != "rows"})
        connection.close()

    central = {}
    for version in range(1, versions + 1):
        rows = base.merge_rows([partials[(source, version)]["rows"] for source in range(n)])
        encoded = base.payload_bytes(rows)
        central[version] = {
            "layout": layout["id"],
            "version": version,
            "rows": rows,
            "groups": len(rows),
            "payload_sha256": base.sha_bytes(encoded),
            "actual_bytes": len(encoded),
            "frontier_tick": max(commits[(source, version)] for source in range(n)),
        }

    with (output / "sql_partial_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(manifest_rows[0]))
        writer.writeheader()
        writer.writerows(manifest_rows)
    base.dump_json(output / "central_results.json", {str(key): value for key, value in central.items()})

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
    with (output / "demand_manifest.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(tasks[0]))
        writer.writeheader()
        writer.writerows(tasks)

    realization_by_version = []
    for version in range(1, versions + 1):
        partial_sizes = [int(partials[(source, version)]["actual_bytes"]) for source in range(n)]
        result_size = int(central[version]["actual_bytes"])
        direct_bytes = (n - 1) * sum(partial_sizes)
        gather_bytes_by_root = {
            str(root): sum(partial_sizes) - partial_sizes[root] + (n - 1) * result_size
            for root in range(n)
        }
        realization_by_version.append(
            {
                "version": version,
                "partial_group_counts": [
                    int(partials[(source, version)]["groups"]) for source in range(n)
                ],
                "central_group_count": int(central[version]["groups"]),
                "partial_actual_bytes": partial_sizes,
                "mean_partial_actual_bytes": sum(partial_sizes) / n,
                "central_actual_bytes": result_size,
                "result_to_mean_partial_ratio": result_size / (sum(partial_sizes) / n),
                "direct_logical_edge_bytes": direct_bytes,
                "gather_broadcast_logical_edge_bytes_by_root": gather_bytes_by_root,
            }
        )
    base.dump_json(
        output / "KEY_LAYOUT_REALIZATION.json",
        {
            "layout": layout,
            "shared_key_count": shared_count,
            "private_key_count_per_source": private_count,
            "local_key_domains": local_key_domains,
            "union_key_count": len(set().union(*(set(values) for values in local_key_domains.values()))),
            "equal_partial_byte_boundary_ratio": n - 1,
            "by_version": realization_by_version,
        },
    )
    base.dump_json(
        output / "SQL_OCCURRENCE.json",
        {
            "layout": layout["id"],
            "source_databases": n,
            "versions": versions,
            "source_version_partials": len(partials),
            "remote_transfer_obligations": len(tasks),
            "expected_remote_transfer_obligations": versions * n * (n - 1),
            "demand_density_excluding_self": 1.0,
            "occurrence_scope": "Controlled executable SQL occurrence; deployment prevalence remains unknown.",
        },
    )
    return partials, tasks, central, realization_by_version


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
            "r3_runner": str(Path(r3.__file__).resolve()),
            "r3_runner_sha256": base.sha_file(Path(r3.__file__).resolve()),
            "base_runner": str(Path(base.__file__).resolve()),
            "base_runner_sha256": base.sha_file(Path(base.__file__).resolve()),
            "joint_runner": str(Path(joint.__file__).resolve()),
            "joint_runner_sha256": base.sha_file(Path(joint.__file__).resolve()),
        },
    )
    base.closure_counterexample(output)
    topologies = config["topologies"][:1] if smoke else config["topologies"]
    capacities = config["capacity_bytes_per_tick"][1:2] if smoke else config["capacity_bytes_per_tick"]
    layouts = config["key_layouts"][:2] if smoke else config["key_layouts"]
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
    base.dump_json(output / "TOPOLOGIES.json", {name: base.graph_record(graph) for name, graph in graphs.items()})
    cases_dir = output / "cases"
    cases_dir.mkdir()
    layouts_dir = output / "layouts"
    layouts_dir.mkdir()

    rows, selections, comparisons, structure_realizations = [], [], [], []
    for layout in layouts:
        layout_output = layouts_dir / layout["id"]
        layout_output.mkdir()
        partials, tasks, central, layout_realization = generate_layout_sql(layout_output, config, layout)
        last_frontier = max(int(item["frontier_tick"]) for item in central.values())
        base.dump_json(
            layout_output / "PARAMETER_REALIZATION.json",
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
                "primary_window_reason": config["primary_window_reason"],
                "no_installed_version_is_unavailable": True,
            },
        )
        final_realization = layout_realization[-1]
        structure_realizations.append(
            {
                "layout": layout,
                "final_version": final_realization,
                "all_versions": layout_realization,
            }
        )
        for topology, graph in graphs.items():
            medoid = r3.topology_medoid(graph)
            for capacity in capacities:
                group_rows = []
                for method in direct_methods:
                    started = time.perf_counter_ns()
                    summary, detail = r3.run_direct_case(
                        tasks,
                        graph,
                        int(capacity),
                        method,
                        partials,
                        central,
                        config,
                        random_seeds,
                    )
                    summary.update(
                        {
                            "key_layout": layout["id"],
                            "shared_fraction": float(layout["shared_fraction"]),
                            "result_to_mean_partial_ratio_final": final_realization[
                                "result_to_mean_partial_ratio"
                            ],
                            "planner_elapsed_ns": time.perf_counter_ns() - started,
                            "legend": labels[method],
                        }
                    )
                    case_id = f"{layout['id']}_{topology}_c{capacity}_{method}"
                    summary["case_id"] = case_id
                    detail["summary"] = summary
                    base.dump_json(cases_dir / f"{case_id}.json", detail)
                    rows.append(summary)
                    group_rows.append(summary)
                for method in gather_methods:
                    for root in roots:
                        started = time.perf_counter_ns()
                        summary, detail = r3.simulate_gather_broadcast(
                            graph,
                            int(root),
                            int(capacity),
                            method,
                            partials,
                            central,
                            config,
                        )
                        summary.update(
                            {
                                "key_layout": layout["id"],
                                "shared_fraction": float(layout["shared_fraction"]),
                                "result_to_mean_partial_ratio_final": final_realization[
                                    "result_to_mean_partial_ratio"
                                ],
                                "planner_elapsed_ns": time.perf_counter_ns() - started,
                                "legend": labels[method],
                            }
                        )
                        case_id = f"{layout['id']}_{topology}_c{capacity}_{method}_r{root}"
                        summary["case_id"] = case_id
                        detail["summary"] = summary
                        base.dump_json(cases_dir / f"{case_id}.json", detail)
                        rows.append(summary)
                        group_rows.append(summary)

                if smoke:
                    continue
                direct = {
                    row["method"]: row
                    for row in group_rows
                    if row["architecture"] == "direct_all_to_all"
                }
                selected = {}
                for method in gather_methods:
                    candidates = [row for row in group_rows if row["method"] == method]
                    medoid_row = next(row for row in candidates if int(row["root"]) == medoid)
                    best_row = min(candidates, key=lambda row: r3.objective_tuple(row) + (int(row["root"]),))
                    selected[method] = {"medoid": medoid_row, "best": best_row}
                    selections.append(
                        {
                            "key_layout": layout["id"],
                            "topology": topology,
                            "capacity": int(capacity),
                            "method": method,
                            "medoid_root": medoid,
                            "best_root": int(best_row["root"]),
                            "medoid_objective": list(r3.objective_tuple(medoid_row)),
                            "best_objective": list(r3.objective_tuple(best_row)),
                            "root_max_edge_work_min": min(
                                float(row["maximum_edge_work_ticks"]) for row in candidates
                            ),
                            "root_max_edge_work_max": max(
                                float(row["maximum_edge_work_ticks"]) for row in candidates
                            ),
                        }
                    )
                deployable = {
                    "Latest-SP [4]": direct["latest_sp"],
                    "ClosureMinLoad (ours)": direct["closure_minload"],
                    "GatherBroadcast-FIFO [5,6] / medoid": selected["gather_broadcast_fifo"]["medoid"],
                    "GatherBroadcast-Latest (ours) / medoid": selected["gather_broadcast_latest"]["medoid"],
                }
                winner = min(deployable, key=lambda name: r3.objective_tuple(deployable[name]) + (name,))
                comparisons.append(
                    {
                        "key_layout": layout["id"],
                        "shared_fraction": float(layout["shared_fraction"]),
                        "result_to_mean_partial_ratio_final": final_realization[
                            "result_to_mean_partial_ratio"
                        ],
                        "topology": topology,
                        "capacity": int(capacity),
                        "deployable_winner": winner,
                        "deployable": deployable,
                        "best_root_oracle": {
                            "GatherBroadcast-FIFO [5,6]": selected["gather_broadcast_fifo"]["best"],
                            "GatherBroadcast-Latest (ours)": selected["gather_broadcast_latest"]["best"],
                        },
                    }
                )

    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with (output / "SUMMARY.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    base.dump_json(output / "STRUCTURE_REALIZATIONS.json", structure_realizations)
    if not smoke:
        base.dump_json(output / "ROOT_SELECTIONS.json", selections)
        base.dump_json(output / "ARCHITECTURE_COMPARISONS.json", comparisons)
    expected = len(layouts) * len(topologies) * len(capacities) * (
        len(direct_methods) + len(gather_methods) * len(roots)
    )
    state = "SMOKE_R4_COMPLETE" if smoke else "FORMAL_R4_COMPLETE"
    base.dump_json(
        output / "RESULTS.json",
        {
            "state": state,
            "protocol_id": config["id"],
            "case_count": len(rows),
            "expected_case_count": expected,
            "key_layouts": [layout["id"] for layout in layouts],
            "direct_methods": direct_methods,
            "gather_broadcast_methods": gather_methods,
            "topologies": topologies,
            "capacities": capacities,
            "roots": roots,
            "fixed_evaluation_windows": True,
            "primary_tail_periods": int(config["primary_evaluation_tail_periods"]),
            "architecture_comparisons": len(comparisons),
        },
    )
    base.write_manifest(
        output,
        [sys.executable, str(Path(__file__).resolve()), "--config", str(config_path), "--output", str(output)]
        + (["--smoke"] if smoke else []),
    )
    return {"output": str(output), "cases": len(rows), "expected": expected, "state": state}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.config.resolve(), args.output.resolve(), args.smoke), ensure_ascii=False))


if __name__ == "__main__":
    main()
