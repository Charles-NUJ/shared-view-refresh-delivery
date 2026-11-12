"""One-step rollout over CRP for a fixed, unissued refresh calendar.

The algorithm acts only on physical recipient edges.  It keeps the original
CRP result, tries every feasible first deletion, completes each branch with
CRP, and returns the cheapest checked branch.  Therefore it cannot cost more
than CRP.  It is a policy-improvement heuristic, not an optimality claim.
"""
from __future__ import annotations

import copy
import time

from contract_prune import contract_prune, cost, feasible, fresh_counts, shrink, validate


def _edge_list(plan):
    return sorted(
        (q["start"], q["view"], q["epoch"], c)
        for q in plan if q["kind"] == "pub" for c in q["recipients"]
    )


def _delete_edge(cfg, plan, edge):
    start, view, epoch, consumer = edge
    trial = copy.deepcopy(plan)
    delivery = next(
        q for q in trial
        if q["kind"] == "pub" and q["start"] == start
        and q["view"] == view and q["epoch"] == epoch
        and consumer in q["recipients"]
    )
    delivery["recipients"].remove(consumer)
    return shrink(cfg, trial)


def rollout_crp(cfg, initial):
    begin = time.perf_counter()
    projected = shrink(cfg, initial)
    if validate(cfg, initial) or validate(cfg, projected) or not feasible(cfg, projected):
        raise ValueError("rollout requires a feasible fixed calendar")

    base = contract_prune(cfg, projected)
    branches = [{
        "first_edge": None,
        "status": "BASE_POLICY",
        "cost": base["final_cost"],
        "counts": base["counts"],
        "plan": base["plan"],
        "policy_ms": base["wall_ms"],
    }]

    for edge in _edge_list(projected):
        trial = _delete_edge(cfg, projected, edge)
        if not feasible(cfg, trial):
            branches.append({"first_edge": list(edge), "status": "INFEASIBLE_FIRST_DELETE"})
            continue
        completion = contract_prune(cfg, trial)
        branches.append({
            "first_edge": list(edge),
            "status": "FEASIBLE_COMPLETION",
            "cost": completion["final_cost"],
            "counts": completion["counts"],
            "plan": completion["plan"],
            "policy_ms": completion["wall_ms"],
        })

    candidates = [item for item in branches if "cost" in item]
    best = min(candidates, key=lambda item: (item["cost"], item["first_edge"] is not None, item["first_edge"] or []))
    assert best["cost"] <= base["final_cost"] + 1e-9
    assert not validate(cfg, best["plan"]) and feasible(cfg, best["plan"])
    assert abs(cost(cfg, best["plan"]) - best["cost"]) <= 1e-9
    return {
        "algorithm": "RCRP-one-step-rollout",
        "base_cost": base["final_cost"],
        "final_cost": best["cost"],
        "counts": fresh_counts(cfg, best["plan"]),
        "plan": best["plan"],
        "selected_first_edge": best["first_edge"],
        "branches": branches,
        "wall_ms": (time.perf_counter() - begin) * 1000,
        "scope": "fixed-calendar feasible policy improvement; no global optimality or speed claim",
    }
