"""Run the frozen R60 causal pressure-gated value-density study."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import shutil
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path

import networkx as nx


HERE = Path(__file__).resolve().parent
ENGINE = HERE / "engine"
sys.path.insert(0, str(ENGINE))

import run_r40_architecture_structure_r3 as r3  # noqa: E402
import run_r40_initial_structural as base  # noqa: E402
import run_r40_storage_architecture_r4 as r4  # noqa: E402


DEFAULT_PROTOCOL = HERE / "protocol.json"
DEFAULT_OUTPUT = HERE / "output"
COMPARE_FIELDS = (
    "correctness_failures",
    "final_version_failures",
    "dropped_source_tasks",
    "dropped_source_bytes",
    "visible_installs",
    "freshness_shortfall_rho_0_9",
    "maximum_edge_work_ticks",
    "total_edge_bytes",
    "makespan_ticks",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def dump_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def resolve_from(base: Path, value: str | None) -> Path | None:
    """Resolve an optional artifact path relative to its protocol file."""
    if value is None:
        return None
    path = Path(value)
    return path if path.is_absolute() else (base / path).resolve()


def compare(left: tuple, right: tuple) -> str:
    return "better" if left < right else "equal" if left == right else "worse"


def flatten(summary: dict) -> dict:
    return {
        "case_id": summary["case_id"],
        "demand_profile": summary["demand_profile"],
        "key_layout": summary["key_layout"],
        "shared_fraction": summary["shared_fraction"],
        "topology": summary["topology"],
        "capacity": summary["capacity_bytes_per_tick"],
        "method": summary["method"],
        "correctness_failures": summary["correctness_failures"],
        "final_version_failures": summary["final_version_failures"],
        "rho_0_9_violations": summary["contract_violations"]["0.9"],
        "freshness_shortfall": summary["freshness_shortfall_rho_0_9"],
        "maximum_edge_work": summary["maximum_edge_work_ticks"],
        "total_edge_bytes": summary["total_edge_bytes"],
        "makespan": summary["makespan_ticks"],
        "visible_installs": summary["visible_installs"],
        "dropped_source_tasks": summary.get("dropped_source_tasks", 0),
        "dropped_source_bytes": summary.get("dropped_source_bytes", 0),
        "gate_activated": summary.get("gate_activated", ""),
        "gate_activation_tick": summary.get("gate_activation_tick", ""),
    }


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (float(ordered[mid - 1]) + float(ordered[mid])) / 2.0


def relative_reduction(candidate: float, reference: float) -> float | None:
    if reference <= 0:
        return None
    return (reference - candidate) / reference


def comparison_record(candidate: dict, reference: dict) -> dict:
    return {
        "outcome": compare(r3.objective_tuple(candidate), r3.objective_tuple(reference)),
        "candidate_violations": int(candidate["contract_violations"]["0.9"]),
        "reference_violations": int(reference["contract_violations"]["0.9"]),
        "shortfall_reduction": relative_reduction(
            float(candidate["freshness_shortfall_rho_0_9"]),
            float(reference["freshness_shortfall_rho_0_9"]),
        ),
        "max_work_reduction": relative_reduction(
            float(candidate["maximum_edge_work_ticks"]),
            float(reference["maximum_edge_work_ticks"]),
        ),
        "bytes_reduction": relative_reduction(
            float(candidate["total_edge_bytes"]), float(reference["total_edge_bytes"])
        ),
        "makespan_reduction": relative_reduction(
            float(candidate["makespan_ticks"]), float(reference["makespan_ticks"])
        ),
    }


def summarize(rows: list[dict], protocol: dict) -> dict:
    by_key = {
        (row["demand_profile"], row["key_layout"], row["topology"], int(row["capacity_bytes_per_tick"]), row["method"]): row
        for row in rows
    }
    primary = protocol["primary_method"]
    references = ["fifo_sp", "oldest_sp", "qoda_greedy", "latest_sp", "replacement_qoda"]
    comparisons: dict[str, dict] = {}
    topology_counts: dict[str, dict] = defaultdict(dict)
    profile_counts: dict[str, dict] = defaultdict(dict)
    for reference in references:
        records = []
        for profile in protocol["demand_profiles"]:
            for layout in protocol["key_layouts"]:
                for topology in protocol["topologies"]:
                    for capacity in protocol["capacities"]:
                        cand = by_key[(profile, layout, topology, capacity, primary)]
                        ref = by_key[(profile, layout, topology, capacity, reference)]
                        rec = comparison_record(cand, ref)
                        rec.update(
                            demand_profile=profile,
                            key_layout=layout,
                            topology=topology,
                            capacity=capacity,
                        )
                        records.append(rec)
        comparisons[reference] = {
            "counts": dict(Counter(item["outcome"] for item in records)),
            "median_shortfall_reduction": median(
                [item["shortfall_reduction"] for item in records if item["shortfall_reduction"] is not None]
            ),
            "median_max_work_reduction": median(
                [item["max_work_reduction"] for item in records if item["max_work_reduction"] is not None]
            ),
            "median_bytes_reduction": median(
                [item["bytes_reduction"] for item in records if item["bytes_reduction"] is not None]
            ),
            "median_makespan_reduction": median(
                [item["makespan_reduction"] for item in records if item["makespan_reduction"] is not None]
            ),
        }
        for topology in protocol["topologies"]:
            subset = [item for item in records if item["topology"] == topology and item["demand_profile"] == protocol["primary_profile"]]
            topology_counts[topology][reference] = dict(Counter(item["outcome"] for item in subset))
        for profile in protocol["demand_profiles"]:
            subset = [item for item in records if item["demand_profile"] == profile]
            profile_counts[profile][reference] = dict(Counter(item["outcome"] for item in subset))

    factor_methods = protocol["factorial_methods"]
    factorial = []
    for profile in protocol["demand_profiles"]:
        for layout in protocol["key_layouts"]:
            for topology in protocol["topologies"]:
                for capacity in protocol["capacities"]:
                    group = {
                        label: by_key[(profile, layout, topology, capacity, method)]
                        for label, method in factor_methods.items()
                    }
                    factorial.append(
                        {
                            "demand_profile": profile,
                            "key_layout": layout,
                            "topology": topology,
                            "capacity": capacity,
                            "replacement_effect_under_fifo": comparison_record(
                                group["10_replacement_fifo"], group["00_no_replacement_fifo"]
                            ),
                            "value_density_effect_without_replacement": comparison_record(
                                group["01_no_replacement_value_density"], group["00_no_replacement_fifo"]
                            ),
                            "joint_effect": comparison_record(
                                group["11_replacement_value_density"], group["00_no_replacement_fifo"]
                            ),
                            "joint_vs_replacement_only": comparison_record(
                                group["11_replacement_value_density"], group["10_replacement_fifo"]
                            ),
                            "joint_vs_value_density_only": comparison_record(
                                group["11_replacement_value_density"], group["01_no_replacement_value_density"]
                            ),
                        }
                    )

    factor_summary = {}
    for effect in (
        "replacement_effect_under_fifo",
        "value_density_effect_without_replacement",
        "joint_effect",
        "joint_vs_replacement_only",
        "joint_vs_value_density_only",
    ):
        factor_summary[effect] = dict(Counter(item[effect]["outcome"] for item in factorial))

    gate_rows = [row for row in rows if row["method"] == primary]
    gate_summary = {
        "contexts": len(gate_rows),
        "activated": sum(bool(row.get("gate_activated")) for row in gate_rows),
        "inactive": sum(not bool(row.get("gate_activated")) for row in gate_rows),
    }
    return {
        "primary_method": primary,
        "comparisons": comparisons,
        "primary_profile_topology_counts": topology_counts,
        "profile_counts": profile_counts,
        "factorial_counts": factor_summary,
        "gate_summary": gate_summary,
        "factorial_records": factorial,
    }


def write_manifest(output: Path, command: list[str]) -> None:
    files = []
    for path in sorted(output.rglob("*")):
        if path.is_file() and path.name != "FILE_MANIFEST.json":
            files.append(
                {
                    "path": str(path.relative_to(output)).replace("\\", "/"),
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
            )
    dump_json(
        output / "FILE_MANIFEST.json",
        {"state": "COMPLETE", "command": command, "files": files, "file_count": len(files)},
    )


def run(protocol_path: Path, output: Path) -> dict:
    if output.exists():
        raise FileExistsError(output)
    protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
    parent_path = resolve_from(protocol_path.parent, protocol["parent_config"])
    archived_cases = resolve_from(protocol_path.parent, protocol.get("archived_cases"))
    if parent_path is None:
        raise ValueError("parent_config is required")
    parent = json.loads(parent_path.read_text(encoding="utf-8"))
    layouts = {item["id"]: item for item in parent["key_layouts"]}
    output.mkdir(parents=True)
    inputs_dir = output / "inputs"
    cases_dir = output / "cases_original_profile"
    sources_dir = output / "source"
    inputs_dir.mkdir()
    cases_dir.mkdir()
    sources_dir.mkdir()
    shutil.copyfile(protocol_path, output / "PROTOCOL.json")
    shutil.copyfile(parent_path, output / "PARENT_CONFIG.json")
    shutil.copyfile(Path(__file__), sources_dir / Path(__file__).name)
    for path in sorted(ENGINE.glob("*.py")):
        shutil.copyfile(path, sources_dir / path.name)
    dump_json(
        output / "ENVIRONMENT.json",
        {
            "python": sys.version,
            "executable": sys.executable,
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
            "networkx": nx.__version__,
            "protocol_sha256": sha256(protocol_path),
            "parent_config_sha256": sha256(parent_path),
        },
    )

    rows: list[dict] = []
    archive_errors: list[dict] = []
    started = time.time()
    for layout_id in protocol["key_layouts"]:
        layout_dir = inputs_dir / layout_id
        layout_dir.mkdir()
        partials, tasks, central, _ = r4.generate_layout_sql(layout_dir, parent, layouts[layout_id])
        for profile, weights in protocol["demand_profiles"].items():
            config = deepcopy(parent)
            config["query_weights"] = list(map(int, weights))
            for topology in protocol["topologies"]:
                graph = base.make_graph(topology, int(config["sites"]))
                for capacity in protocol["capacities"]:
                    outcomes = {}
                    for method in protocol["methods"]:
                        summary, detail = r3.run_direct_case(
                            tasks,
                            graph,
                            int(capacity),
                            method,
                            partials,
                            central,
                            config,
                            list(map(int, config["random_seeds"])),
                        )
                        case_id = f"{profile}_{layout_id}_{topology}_c{capacity}_{method}"
                        summary.update(
                            {
                                "case_id": case_id,
                                "demand_profile": profile,
                                "key_layout": layout_id,
                                "shared_fraction": float(layouts[layout_id]["shared_fraction"]),
                            }
                        )
                        detail["summary"] = summary
                        outcomes[method] = (summary, detail)
                        rows.append(summary)
                        if profile == protocol["primary_profile"]:
                            dump_json(cases_dir / f"{case_id}.json", detail)

                    if profile == protocol["primary_profile"] and archived_cases is not None:
                        for method in ("fifo_sp", "oldest_sp", "qoda_greedy", "latest_sp"):
                            archived_path = archived_cases / f"{layout_id}_{topology}_c{capacity}_{method}.json"
                            archived = json.loads(archived_path.read_text(encoding="utf-8"))
                            replay_summary, replay_detail = outcomes[method]
                            for field in COMPARE_FIELDS:
                                if replay_summary.get(field) != archived["summary"].get(field):
                                    archive_errors.append(
                                        {
                                            "case": archived_path.stem,
                                            "field": field,
                                            "replay": replay_summary.get(field),
                                            "archive": archived["summary"].get(field),
                                        }
                                    )
                            if replay_detail["install_records"] != archived["install_records"]:
                                archive_errors.append({"case": archived_path.stem, "field": "install_records"})
                            if replay_detail["edge_bytes"] != archived["edge_bytes"]:
                                archive_errors.append({"case": archived_path.stem, "field": "edge_bytes"})

    expected = (
        len(protocol["key_layouts"])
        * len(protocol["demand_profiles"])
        * len(protocol["topologies"])
        * len(protocol["capacities"])
        * len(protocol["methods"])
    )
    if len(rows) != expected:
        raise AssertionError((len(rows), expected))
    failures = [
        row
        for row in rows
        if int(row["correctness_failures"]) != 0 or int(row["final_version_failures"]) != 0
    ]
    if archive_errors or failures:
        raise AssertionError({"archive_errors": archive_errors[:5], "failures": failures[:5]})

    flat = [flatten(row) for row in rows]
    with (output / "SUMMARY.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(flat[0]))
        writer.writeheader()
        writer.writerows(flat)
    dump_json(output / "RESULTS.json", rows)
    analysis = summarize(rows, protocol)
    dump_json(output / "ANALYSIS.json", analysis)
    archive_check_count = (
        4 * len(protocol["key_layouts"]) * len(protocol["topologies"]) * len(protocol["capacities"])
        if archived_cases is not None
        else 0
    )
    receipt = {
        "state": "PASS",
        "case_count": len(rows),
        "common_contexts_per_profile": len(protocol["key_layouts"]) * len(protocol["topologies"]) * len(protocol["capacities"]),
        "methods": protocol["methods"],
        "profiles": protocol["demand_profiles"],
        "archive_replay_enabled": archived_cases is not None,
        "archive_replay_checks": archive_check_count,
        "archive_replay_errors": archive_errors,
        "correctness_or_final_failures": len(failures),
        "elapsed_seconds": time.time() - started,
    }
    dump_json(output / "EXECUTION_RECEIPT.json", receipt)
    command = [sys.executable, str(Path(__file__).resolve()), "--protocol", str(protocol_path), "--output", str(output)]
    write_manifest(output, command)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", type=Path, default=DEFAULT_PROTOCOL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.protocol.resolve(), args.output.resolve()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
