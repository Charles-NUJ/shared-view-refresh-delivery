"""Grayscale, publication-oriented layout helpers for D01 result figures.

The palette deliberately encodes no algorithm identity by hue.  A method
keeps its line style, marker, gray level, and bar hatch across figures.
Figures use their final single- or double-column physical width, so text
does not shrink after placement in a paper.
"""

from __future__ import annotations

from typing import Iterable

import matplotlib.pyplot as plt


SINGLE_COLUMN_IN = 3.45
DOUBLE_COLUMN_IN = 7.16


_STYLES = {
    # Current D01 methods: ours remain dark and keep one identity everywhere.
    "VC-SP": ("#303030", "o", "-", "////"),
    "VC-GB": ("#111111", "s", "--", "\\\\"),
    "VC-MinLoad": ("#424242", "^", "-.", "xx"),
    "GreedyMinLoad": ("#555555", "D", ":", ".."),
    "Random-16": ("#5E5E5E", "h", (0, (5, 2)), "--"),
    "RandomVC-16": ("#4A4A4A", "P", (0, (3, 1, 1, 1)), "++"),
    # Literature-derived comparators use lighter gray levels.
    "Bulk-SP": ("#B8B8B8", "s", "--", "xx"),
    "FIFO-SP": ("#A6A6A6", "o", "-", "//"),
    "Oldest-SP": ("#919191", "^", "-.", ".."),
    "QoDA-Greedy": ("#7A7A7A", "v", ":", "++"),
    "GatherBroadcast-FIFO": ("#969696", "D", "--", "\\\\"),
    # Structural observations and analytical references are not algorithms.
    "ObservedStructure": ("#111111", "o", "-", ""),
    # Legacy D01 method names retained for older archived figures.
    "JCR": ("#111111", "o", "-", ""),
    "RCRP": ("#262626", "s", "--", "//"),
    "CRP": ("#4A4A4A", "^", "-.", "xx"),
    "FreshRatio": ("#383838", "D", ":", ".."),
    "LPOrder": ("#555555", "P", (0, (5, 1, 1, 1)), "++"),
    "ShareFirst": ("#424242", "X", (0, (3, 1, 1, 1, 1, 1)), "\\\\"),
    "DeliveryFirst": ("#5E5E5E", "v", (0, (1, 1)), "oo"),
    "Random16": ("#666666", "h", (0, (5, 2)), "--"),
    # Input and external/literature comparators: lighter grayscale.
    "Base": ("#B7B7B7", "o", "-", "//"),
    "Literature": ("#969696", "v", "--", "//"),
    "Reference": ("#7A7A7A", None, "--", ""),
    # Stable identities for D01 internal controls and ablations.
    "periodic_all": ("#B7B7B7", "o", "-", "//"),
    "oldest_first": ("#929292", "s", "--", "xx"),
    "demand_greedy": ("#6B6B6B", "^", "-.", ".."),
    "completion_greedy": ("#4A4A4A", "D", ":", "++"),
    "contract_bundle": ("#242424", "P", (0, (5, 1, 1, 1)), "\\\\"),
    "deficit_completion": ("#111111", "X", (0, (3, 1, 1, 1, 1, 1)), "oo"),
    "cyclic_shared": ("#777777", "h", (0, (5, 2)), "--"),
    "receipt_guard": ("#111111", "o", "-", ""),
    "receipt_guard_no_learning": ("#4D4D4D", "s", "--", "//"),
    "periodic_robust_backup": ("#A6A6A6", "^", "-.", "xx"),
    "fixed4": ("#555555", "s", "--", "//"),
    "adaptive": ("#111111", "o", "-", ""),
    "periodic": ("#A6A6A6", "^", "-.", "xx"),
    "joint_learning_price": ("#111111", "o", "-", ""),
    "guarded_joint": ("#242424", "s", "--", "//"),
    "no_learning": ("#5E5E5E", "^", "-.", "xx"),
    "no_install_price": ("#777777", "D", ":", ".."),
    "no_plan_prices": ("#8F8F8F", "P", (0, (5, 1, 1, 1)), "++"),
}


def _style(name: str) -> tuple[str, str | None, object, str]:
    return _STYLES.get(name, _STYLES["Literature"])


def line_style(name: str, *, markevery: int | None = None) -> dict:
    """Return a stable grayscale line style for a named method."""
    color, marker, linestyle, _ = _style(name)
    style = {
        "color": color,
        "linestyle": linestyle,
        "marker": marker,
        "linewidth": 1.25,
        "markersize": 3.8,
    }
    if markevery is not None:
        style["markevery"] = markevery
    return style


def bar_style(name: str) -> dict:
    """Return grayscale, edge, and hatch coding for a named method."""
    color, _, _, hatch = _style(name)
    return {
        "color": color,
        "edgecolor": "#171717",
        "linewidth": 0.45,
        "hatch": hatch,
    }


def reference_line_style() -> dict:
    return {
        "color": _STYLES["Reference"][0],
        "linestyle": "--",
        "linewidth": 0.95,
    }


def legend_label(
    name: str,
    *,
    ours: bool,
    citation: str | None = None,
) -> str:
    """Format a legend label with explicit ownership or a direct citation."""
    if ours:
        suffix = " (ours)"
        if citation:
            suffix += f"; inspired by [{citation}]"
        return f"{name}{suffix}"
    if not citation:
        raise ValueError("A literature baseline needs its direct citation.")
    return f"{name} [{citation}]"


def apply_publication_style() -> None:
    """Set compact, legible defaults suitable for final paper placement."""
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 8.0,
            "axes.titlesize": 8.0,
            "axes.labelsize": 8.0,
            "xtick.labelsize": 7.0,
            "ytick.labelsize": 7.0,
            "legend.fontsize": 7.0,
            "axes.linewidth": 0.65,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.dpi": 180,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "lines.linewidth": 1.25,
            "lines.markersize": 3.8,
        }
    )


def _axes_list(axes) -> list:
    if hasattr(axes, "flat"):
        return list(axes.flat)
    if isinstance(axes, Iterable):
        return list(axes)
    return [axes]


def compact_panels(
    fig,
    axes,
    *,
    left: float = 0.09,
    right: float = 0.99,
    bottom: float = 0.16,
    top: float = 0.82,
    wspace: float = 0.22,
    hspace: float = 0.28,
) -> None:
    """Pack panels without allowing adjacent y labels to collide."""
    panels = _axes_list(axes)
    for axis in panels:
        axis.yaxis.labelpad = 3.0
        axis.xaxis.labelpad = 2.5
        axis.tick_params(pad=2.0, width=0.55, length=3.0)
        axis.grid(axis="y", color="#D6D6D6", linewidth=0.45, zorder=0)
        axis.set_axisbelow(True)
    fig.subplots_adjust(
        left=left,
        right=right,
        bottom=bottom,
        top=top,
        wspace=wspace,
        hspace=hspace,
    )
    fig.align_ylabels(panels)


def shared_legend(
    fig,
    axes,
    *,
    ncol: int,
    y: float = 0.985,
    include: Iterable[str] | None = None,
) -> None:
    """Place one deduplicated legend above a set of panels."""
    wanted = set(include) if include is not None else None
    unique = {}
    for axis in _axes_list(axes):
        handles, labels = axis.get_legend_handles_labels()
        for handle, label in zip(handles, labels):
            if not label or label.startswith("_"):
                continue
            if wanted is not None and label not in wanted:
                continue
            unique.setdefault(label, handle)
    if unique:
        fig.legend(
            list(unique.values()),
            list(unique.keys()),
            loc="upper center",
            bbox_to_anchor=(0.5, y),
            ncol=ncol,
            frameon=False,
            columnspacing=1.0,
            handlelength=1.9,
            handletextpad=0.45,
            borderaxespad=0.0,
        )
