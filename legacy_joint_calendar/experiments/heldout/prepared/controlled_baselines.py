"""Controlled same-domain baselines for D01 fixed-calendar reduction.

The literature-inspired methods below are D01 specializations, not faithful
reimplementations of iShare, BIGSHARED, QoDA, Tempura, or Agamotto.
Every method changes only recipient edges and induced source projections.
"""
from __future__ import annotations
import copy, random, time
import numpy as np
from scipy.optimize import Bounds, LinearConstraint, linprog, milp
from contract_prune import cost, feasible, fresh_counts, shrink, validate, contract_prune
from joint_calendar import compile_model, decode
from rollout_calendar import _delete_edge, _edge_list, rollout_crp

TOL=1e-8

def _checked(cfg, plan):
    plan=shrink(cfg,plan)
    assert not validate(cfg,plan) and feasible(cfg,plan)
    return plan

def _fixed_order_prune(cfg, initial, order_key):
    begin=time.perf_counter(); plan=_checked(cfg,initial); attempts=accepted=0
    while True:
        ranked=order_key(plan); changed=False
        for edge in ranked:
            trial=_delete_edge(cfg,plan,edge); attempts+=1
            if feasible(cfg,trial): plan=trial;accepted+=1;changed=True;break
        if not changed:break
    return {"plan":plan,"cost":cost(cfg,plan),"counts":fresh_counts(cfg,plan),"attempts":attempts,"accepted":accepted,"wall_ms":(time.perf_counter()-begin)*1000}

def freshness_ratio_prune(cfg, initial):
    """Greedy saved declared cost per one-plus aggregate fresh-sample loss."""
    def ranking(plan):
        before=cost(cfg,plan); before_counts=fresh_counts(cfg,plan); rows=[]
        for edge in _edge_list(plan):
            trial=_delete_edge(cfg,plan,edge)
            if not feasible(cfg,trial): score=-float("inf")
            else:
                loss=sum(max(0,a-b) for a,b in zip(before_counts,fresh_counts(cfg,trial)))
                score=(before-cost(cfg,trial))/(1.0+loss)
            rows.append((score,edge))
        return [e for _,e in sorted(rows,key=lambda x:(-x[0],x[1]))]
    out=_fixed_order_prune(cfg,initial,ranking);out["algorithm"]="FreshRatio-CRP";return out

def lp_order_prune(cfg, initial):
    """Freeze root-LP recipient fractions and prune low-fraction edges first."""
    projected=_checked(cfg,initial); model=compile_model(cfg,projected)
    lp=linprog(model["c"],A_ub=model["A"],b_ub=model["b"],bounds=(0,1),method="highs")
    if not lp.success: raise RuntimeError(lp.message)
    pos={tuple(k):i for i,k in enumerate(model["keys"])}; pub_index={(p["start"],p["view"],p["epoch"]):j for j,p in enumerate(model["pubs"])}
    rank={edge:(float(lp.x[pos[("y",pub_index[(edge[0],edge[1],edge[2])],edge[3])]]),-cfg["upper"][edge[1]],edge) for edge in _edge_list(projected)}
    def ordering(plan):return sorted(_edge_list(plan),key=lambda e:rank[e])
    out=_fixed_order_prune(cfg,projected,ordering);out.update(algorithm="LPOrder-CRP",lp_objective=float(lp.fun));return out

def random_order_prune(cfg, initial, seed):
    rng=random.Random(seed)
    def ordering(plan):
        x=_edge_list(plan);rng.shuffle(x);return x
    out=_fixed_order_prune(cfg,initial,ordering);out.update(algorithm="RandomOrder-CRP",seed=seed);return out

def _solve_model(cfg, initial, objective, primary=None, primary_upper=None):
    m=compile_model(cfg,_checked(cfg,initial)); cons=[LinearConstraint(m["A"],-np.inf,m["b"])]
    if primary is not None:cons.append(LinearConstraint(primary.reshape(1,-1),-np.inf,primary_upper))
    r=milp(objective,integrality=m["integer"],bounds=Bounds(np.zeros(len(m["c"])),np.ones(len(m["c"]))),constraints=cons,options={"time_limit":10,"mip_rel_gap":0.0})
    if r.x is None or r.status!=0 or getattr(r,"mip_gap",1)>1e-8:raise RuntimeError((r.status,r.message,getattr(r,"mip_gap",None)))
    plan=decode(cfg,m,np.asarray(r.x));return m,r,plan

def lexicographic_baseline(cfg, initial, primary_kind):
    begin=time.perf_counter(); m=compile_model(cfg,_checked(cfg,initial)); source=np.array([m["c"][i] if key[0] in {"g","a"} else 0.0 for i,key in enumerate(m["keys"])]); delivery=np.array([m["c"][i] if key[0]=="y" else 0.0 for i,key in enumerate(m["keys"])]); primary=source if primary_kind=="source" else delivery
    m1,r1,p1=_solve_model(cfg,initial,primary); optimum=float(primary@r1.x)
    m2,r2,plan=_solve_model(cfg,initial,m["c"],primary=primary,primary_upper=optimum+1e-7)
    plan=_checked(cfg,plan)
    return {"algorithm":"ShareFirst-Lex" if primary_kind=="source" else "DeliveryFirst-Lex","plan":plan,"cost":cost(cfg,plan),"counts":fresh_counts(cfg,plan),"primary_optimum":optimum,"wall_ms":(time.perf_counter()-begin)*1000}

def exact_jcr(cfg, initial):
    begin=time.perf_counter();m=compile_model(cfg,_checked(cfg,initial));m2,r,plan=_solve_model(cfg,initial,m["c"]);plan=_checked(cfg,plan)
    return {"algorithm":"JCR-exact","plan":plan,"cost":cost(cfg,plan),"counts":fresh_counts(cfg,plan),"mip_gap":float(r.mip_gap),"dual_bound":float(r.mip_dual_bound),"wall_ms":(time.perf_counter()-begin)*1000}

def all_methods(cfg, initial, random_seeds):
    crp=contract_prune(cfg,initial); rcrp=rollout_crp(cfg,initial); fr=freshness_ratio_prune(cfg,initial); lp=lp_order_prune(cfg,initial)
    randoms=[random_order_prune(cfg,initial,s) for s in random_seeds]; randoms=sorted(randoms,key=lambda x:(x["cost"],x["seed"])); random_costs=sorted(x["cost"] for x in randoms)
    median=(random_costs[len(random_costs)//2-1]+random_costs[len(random_costs)//2])/2 if len(random_costs)%2==0 else random_costs[len(random_costs)//2]
    share=lexicographic_baseline(cfg,initial,"source"); delivery=lexicographic_baseline(cfg,initial,"delivery"); exact=exact_jcr(cfg,initial)
    return {"Base":{"plan":_checked(cfg,initial),"cost":cost(cfg,shrink(cfg,initial)),"counts":fresh_counts(cfg,shrink(cfg,initial))},"CRP":{"plan":crp["plan"],"cost":crp["final_cost"],"counts":crp["counts"],"wall_ms":crp["wall_ms"]},"FreshRatio":fr,"LPOrder":lp,"ShareFirst":share,"DeliveryFirst":delivery,"Random16Best":randoms[0],"Random16MedianCost":median,"RCRP":{"plan":rcrp["plan"],"cost":rcrp["final_cost"],"counts":rcrp["counts"],"wall_ms":rcrp["wall_ms"]},"JCR":exact}
