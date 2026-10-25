"""D01 feedback specialization. Standard RLS, set membership, and PDHG.

Hard bounds are conditional on a stationary additive coefficient vector in
the prior box and a declared absolute observation-error bound. RLS alone
never supplies a certificate. All modified commands must be unissued.
"""
import copy, math, time
import numpy as np
from scipy.optimize import linprog
from contract_prune import shrink, validate, feasible, cost, fresh_counts, contract_prune
from joint_calendar import compile_model, decode


class CalibrationRejected(ValueError):
    pass


def calibrate(prior, features, observations, epsilon=0., ridge=0.01):
    upper=np.asarray(prior,dtype=float); F=np.asarray(features,dtype=float)
    d=np.asarray(observations,dtype=float)
    if not (upper.ndim==1 and np.all(upper>=0) and F.ndim==2 and
            F.shape==(len(d),len(upper)) and np.all(F>=0) and
            epsilon>=0 and ridge>0 and np.all(np.isfinite(F)) and
            np.all(np.isfinite(d)) and np.all(np.isfinite(upper))):
        raise ValueError('invalid calibration inputs')
    theta=upper.copy(); P=np.eye(len(upper))/ridge; trace=[]
    for phi,measurement in zip(F,d):
        denominator=1.+float(phi@P@phi) # positive: P starts positive definite
        if not denominator>0: raise CalibrationRejected('nonpositive RLS denominator')
        gain=P@phi/denominator
        innovation=float(measurement-phi@theta)
        theta=theta+gain*innovation
        P=P-np.outer(gain,phi@P); P=(P+P.T)/2
        trace.append(dict(innovation=innovation,denominator=denominator,
                          theta=theta.tolist(),P_diag=np.diag(P).tolist()))
    A=np.vstack([F,-F]); b=np.r_[d+epsilon,-d+epsilon]
    if not len(d): A=None; b=None
    bounds=list(zip(np.zeros(len(upper)),upper)); maxima=[]; witnesses=[]
    for k in range(len(upper)):
        objective=np.zeros(len(upper)); objective[k]=-1
        lp=linprog(objective,A_ub=A,b_ub=b,bounds=bounds,method='highs')
        if not lp.success:
            raise CalibrationRejected('empty or unresolved membership set; do not issue')
        # Small outward numerical padding. The theorem concerns the exact LP;
        # this floating point check is not a validated-arithmetic certificate.
        maxima.append(min(upper[k],max(0.,float(lp.x[k]))+1e-7))
        witnesses.append(lp.x.tolist())
    maxima=np.array(maxima)
    # integer ceilings preserve all nonnegative source-feature upper bounds.
    ticks=np.ceil(np.maximum(0,maxima-1e-9)).astype(int)
    ticks=np.minimum(ticks,np.ceil(upper).astype(int))
    # Original positive source durations are retained even for zero estimates.
    if ticks[0]==0:
        ticks[1:]=np.maximum(ticks[1:],1)
    return dict(prior=upper.tolist(),features=F.tolist(),observations=d.tolist(),
                epsilon=float(epsilon),rank=int(np.linalg.matrix_rank(F)) if len(F) else 0,
                point=np.clip(theta,0,upper).tolist(),P=P.tolist(),
                coefficient_upper=maxima.tolist(),ticks=ticks.tolist(),
                extremizers=witnesses,RLS_trace=trace,
                guarantee='CONDITIONAL_STATIONARY_ADDITIVE_MODEL_AND_NOISE_BOUND')


def apply_bounds(cfg,initial,calibration):
    prior=np.array([cfg['prefix']]+cfg['suffix'],dtype=int)
    tick=np.asarray(calibration['ticks'],dtype=int)
    if len(tick)!=len(prior) or np.any(tick<0) or np.any(tick>prior):
        raise CalibrationRejected('bounds cannot expand or change source dimensions')
    new=copy.deepcopy(cfg); new['prefix']=int(tick[0]); new['suffix']=tick[1:].tolist()
    plan=shrink(new,initial)
    if validate(new,plan) or not feasible(new,plan):
        raise CalibrationRejected('new initial calendar failed independent gate')
    return new,plan


def pdhg(model,iterations=2048,checkpoints=(32,128,512,2048),use_prices=True):
    A,b,c=model['A'],model['b'],model['c']
    one=float(abs(A).sum(axis=0).max()); inf=float(abs(A).sum(axis=1).max())
    norm_bound=math.sqrt(one*inf)
    if not norm_bound>0: raise ValueError('positive matrix norm bound required')
    # tau*sigma*||A||_2^2 <= .99^2 < 1. Objective scaling is positive.
    step=.99/norm_bound; scale=max(1.,float(np.max(np.abs(c))))
    cn=c/scale; x=np.zeros(len(c)); extrapolated=x.copy(); q=np.zeros(len(b))
    xsum=np.zeros(len(c)); snapshots=[]; trace=[]
    for k in range(1,iterations+1):
        q=np.maximum(0,q+step*(A@extrapolated-b)) if use_prices else np.zeros_like(q)
        old=x.copy(); x=np.clip(x-step*(cn+A.T@q),0,1)
        extrapolated=2*x-old; xsum+=x
        if k%32==0 or k in checkpoints:
            avg=xsum/k; physical=[i for i,key in enumerate(model['keys']) if key[0] in ['y','a','g']]
            budgets=[i for i,label in enumerate(model['labels']) if label[0]=='budget']
            qq=q*scale; lower=float(-b@qq+np.minimum(0,c+A.T@qq).sum())
            trace.append(dict(iteration=k,average_objective=float(c@avg),
                              average_max_violation=float(max(0,np.max(A@avg-b))),
                              lower=lower,budget_prices=qq[budgets].tolist(),
                              physical_head=avg[physical[:12]].tolist()))
        if k in checkpoints:
            snapshots.append(dict(iteration=k,x=(xsum/k).copy(),q=(q*scale).copy()))
    return dict(x=xsum/iterations,q=q*scale,trace=trace,snapshots=snapshots,
                step=step,norm_bound=norm_bound,objective_scale=scale)


def repair_from_scores(cfg,model,x,q,threshold):
    # Fractions are proposals. All installed-service checks use integer plans.
    scores={tuple(key):float(x[i]) for i,key in enumerate(model['keys']) if key[0]=='y'}
    selected={key for key,value in scores.items() if value>=threshold}
    prices={label[1]:float(q[i]) for i,label in enumerate(model['labels']) if label[0]=='budget'}
    def make(chosen):
        plan=copy.deepcopy(model['builds'])
        for j,p in enumerate(model['pubs']):
            cs=[c for c in p['recipients'] if ('y',j,c) in chosen]
            if cs: plan.append(dict(p,recipients=cs))
        return shrink(cfg,plan)
    plan=make(selected); repairs=0
    while not feasible(cfg,plan):
        oldcounts=np.array(fresh_counts(cfg,plan)); oldcost=cost(cfg,plan)
        deficit=np.maximum(0,model['target']-oldcounts); best=None
        for key in scores.keys()-selected:
            trial=make(selected|{key}); counts=np.array(fresh_counts(cfg,trial))
            gain=float(np.minimum(deficit,np.maximum(0,counts-oldcounts)) @
                       np.array([1.+prices.get(c,0.) for c in range(len(counts))]))
            incremental=max(0.,cost(cfg,trial)-oldcost)
            # denominator >= 1; fixed setup costs are recomputed jointly.
            priority=(gain/(1.+incremental),scores[key],-key[1],-key[2])
            if best is None or priority>best[0]: best=(priority,key,trial)
        if best is None: raise ValueError('full initial calendar should be feasible')
        selected.add(best[1]); plan=best[2]; repairs+=1
    if validate(cfg,plan): raise ValueError('integer proposal failed resource gate')
    return plan,repairs


def price_reduce(cfg,initial,iterations=2048,use_prices=True):
    start=time.perf_counter(); model=compile_model(cfg,initial)
    baseline=contract_prune(cfg,initial); incumbent=baseline['plan']; best=cost(cfg,incumbent)
    numerical=pdhg(model,iterations=iterations,
                   checkpoints=tuple(k for k in [32,128,512,2048] if k<=iterations),use_prices=use_prices)
    candidates=[]; chosen='CRP_FALLBACK'
    for snapshot in numerical['snapshots']:
        for threshold in [.25,.5,.75]:
            proposal,repairs=repair_from_scores(cfg,model,snapshot['x'],snapshot['q'],threshold)
            # This is a standard local cleanup, not a new optimality theorem.
            cleaned=contract_prune(cfg,proposal)['plan']; value=cost(cfg,cleaned)
            accepted=value<best-1e-8
            candidates.append(dict(iteration=snapshot['iteration'],threshold=threshold,
                                   repairs=repairs,cost=value,accepted=accepted,plan=cleaned))
            if accepted: incumbent=cleaned;best=value;chosen='PRICE_GUIDED_INTEGER_CANDIDATE'
    q=numerical['q'];lower=float(-model['b']@q+
                                np.minimum(0,model['c']+model['A'].T@q).sum())
    if validate(cfg,incumbent) or not feasible(cfg,incumbent) or best>baseline['final_cost']+1e-8:
        raise ValueError('failed feasible-incumbent gate')
    return dict(plan=incumbent,objective=best,CRP_objective=baseline['final_cost'],
                lower=lower,reason=chosen,iterations=iterations,candidates=candidates,
                trace=numerical['trace'],x=numerical['x'],q=q,
                step=numerical['step'],norm_bound=numerical['norm_bound'],
                objective_scale=numerical['objective_scale'],use_prices=use_prices,
                total_ms=(time.perf_counter()-start)*1000)
