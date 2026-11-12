"""Build the two R60 submission result figures from frozen SUMMARY.csv.

The script performs no simulation.  It aggregates the existing matched
contexts, preserves one method order and one grayscale identity across both
figures, and writes the plotted medians for independent verification.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import pandas as pd

from publication_figure_style import SINGLE_COLUMN_IN, apply_publication_style, line_style


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "expected" / "SUMMARY.csv"
OUTPUT = ROOT / "submission_figures"

METHOD_ORDER = ["pressure_gated_qoda", "fifo_sp", "qoda_greedy"]
METHOD_NAME = {
    "pressure_gated_qoda": "PG-VCR",
    "fifo_sp": "FIFO-SP",
    "qoda_greedy": "QoDA-Greedy",
}
LEGEND_LABEL = {
    "pressure_gated_qoda": "PG-VCR (ours)",
    "fifo_sp": "FIFO-SP [10]",
    "qoda_greedy": "QoDA-Greedy [10]",
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest().upper()


def checked_rows() -> pd.DataFrame:
    frame = pd.read_csv(INPUT)
    selected = frame[frame["method"].isin(METHOD_ORDER)].copy()
    assert len(selected) == 432
    assert set(selected["method"]) == set(METHOD_ORDER)
    assert (selected["correctness_failures"] == 0).all()
    assert (selected["final_version_failures"] == 0).all()
    assert selected.groupby(["method", "capacity"]).size().eq(48).all()
    assert selected.groupby(["method", "shared_fraction"]).size().eq(36).all()
    return selected


def aggregate(frame: pd.DataFrame, factor: str) -> pd.DataFrame:
    metrics = [
        "freshness_shortfall",
        "maximum_edge_work",
        "total_edge_bytes",
        "makespan",
    ]
    grouped = (
        frame.groupby(["method", factor], as_index=False)
        .agg(**{name: (name, "median") for name in metrics}, sample_count=("case_id", "size"))
    )
    grouped["factor"] = factor
    grouped = grouped.rename(columns={factor: "factor_value"})
    return grouped


def tight_y(axis, values) -> None:
    low = float(min(values))
    high = float(max(values))
    span = high - low
    margin = max(0.08 * span, 0.04 * max(abs(low), abs(high), 1.0))
    axis.set_ylim(max(0.0, low - margin), high + margin)
    axis.yaxis.set_major_locator(MaxNLocator(nbins=5))


def panel(axis, data: pd.DataFrame, metric: str, ylabel: str, panel_id: str) -> None:
    all_values = []
    for method in METHOD_ORDER:
        rows = data[data["method"] == method].sort_values("factor_value")
        values = rows[metric].to_numpy(dtype=float)
        all_values.extend(values.tolist())
        axis.plot(
            rows["factor_value"],
            values,
            label=LEGEND_LABEL[method],
            zorder=3,
            **line_style(METHOD_NAME[method]),
        )
    tight_y(axis, all_values)
    axis.set_ylabel(ylabel, labelpad=2.0)
    axis.text(
        -0.055,
        1.015,
        panel_id,
        transform=axis.transAxes,
        ha="left",
        va="bottom",
        fontsize=7.2,
        fontweight="bold",
        clip_on=False,
    )
    axis.grid(axis="y", color="#D6D6D6", linewidth=0.45, zorder=0)
    axis.set_axisbelow(True)
    for spine in axis.spines.values():
        spine.set_visible(True)
        spine.set_color("#171717")
        spine.set_linewidth(0.55)
    axis.tick_params(pad=1.8, width=0.55, length=2.8)
    axis.xaxis.labelpad = 1.5


def shared_legend(fig, axes) -> None:
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.982),
        ncol=3,
        frameon=False,
        fontsize=6.15,
        columnspacing=1.15,
        handlelength=2.35,
        handletextpad=0.42,
        borderaxespad=0.0,
    )


def new_figure():
    fig, axes = plt.subplots(2, 1, figsize=(SINGLE_COLUMN_IN, 4.45))
    fig.subplots_adjust(left=0.175, right=0.985, bottom=0.085, top=0.895, hspace=0.38)
    return fig, axes


def capacity_figure(data: pd.DataFrame) -> None:
    fig, axes = new_figure()
    panel(axes[0], data, "freshness_shortfall", r"Freshness shortfall $S_{0.9}$", "(a)")
    panel(axes[1], data, "maximum_edge_work", r"Max. edge work $\max_e W_e$ (ticks)", "(b)")
    for axis in axes:
        axis.set_xticks([256, 512, 1024])
        axis.set_xlabel("Link capacity (bytes/tick)")
    shared_legend(fig, axes)
    fig.savefig(OUTPUT / "fig_r60_capacity_main.pdf", metadata={"Title": "R60 capacity comparison"})
    fig.savefig(OUTPUT / "fig_r60_capacity_main.png", dpi=300)
    plt.close(fig)


def storage_figure(data: pd.DataFrame) -> None:
    scaled = data.copy()
    scaled["total_edge_bytes"] = scaled["total_edge_bytes"] / 1000.0
    fig, axes = new_figure()
    panel(axes[0], scaled, "total_edge_bytes", r"Edge bytes $B_{\rm tot}$ (thousands)", "(a)")
    panel(axes[1], scaled, "makespan", r"Final completion $C_{\rm final}$ (ticks)", "(b)")
    for axis in axes:
        axis.set_xticks([0.0, 0.2, 0.5, 1.0])
        axis.set_xlabel("Shared-key fraction")
    shared_legend(fig, axes)
    fig.savefig(OUTPUT / "fig_r60_storage_sensitivity.pdf", metadata={"Title": "R60 storage sensitivity"})
    fig.savefig(OUTPUT / "fig_r60_storage_sensitivity.png", dpi=300)
    plt.close(fig)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    apply_publication_style()
    # Keep the same physical canvas for both figures.  Tight bounding boxes can
    # crop a figure-level legend differently when y-label widths differ.
    plt.rcParams["savefig.bbox"] = None
    frame = checked_rows()
    by_capacity = aggregate(frame, "capacity")
    by_storage = aggregate(frame, "shared_fraction")
    plotted = pd.concat([by_capacity, by_storage], ignore_index=True)
    plotted["method_order"] = plotted["method"].map({name: i for i, name in enumerate(METHOD_ORDER)})
    plotted = plotted.sort_values(["factor", "factor_value", "method_order"])
    plotted.drop(columns=["method_order"]).to_csv(OUTPUT / "R60_SUBMISSION_FIGURE_DATA.csv", index=False)
    capacity_figure(by_capacity)
    storage_figure(by_storage)
    audit = {
        "input": str(INPUT),
        "input_sha256": sha256(INPUT),
        "input_rows": int(len(pd.read_csv(INPUT))),
        "selected_rows": int(len(frame)),
        "method_order": [LEGEND_LABEL[name] for name in METHOD_ORDER],
        "method_styles": {
            name: line_style(METHOD_NAME[name]) for name in METHOD_ORDER
        },
        "capacity_samples_per_method_point": 48,
        "storage_samples_per_method_point": 36,
        "comparison_contract": {
            "capacity_figure": "capacity varies; method inputs, fixed paths, score, and the balanced grid over topology, storage, and demand stay matched",
            "storage_figure": "shared-key fraction varies; method inputs, fixed paths, score, and the balanced grid over topology, capacity, and demand stay matched",
        },
        "outputs": {},
    }
    for path in sorted(OUTPUT.iterdir()):
        if path.name != "R60_FIGURE_AUDIT.json":
            audit["outputs"][path.name] = {"sha256": sha256(path), "bytes": path.stat().st_size}
    (OUTPUT / "R60_FIGURE_AUDIT.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
