"""Frozen, local D01 feedback experiment; controlled time is not wall time."""
from pathlib import Path
import argparse,copy,hashlib,json,platform,shutil,subprocess,sys,time
import numpy as np
ROOT=Path(__file__).resolve().parents[3]; RUN='feedback_jcr_20261019'

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2)+'\n',encoding='utf-8')
def inventory(O):
    dump(O/'LOCAL_FILE_MANIFEST.json',[dict(path=str(f.relative_to(O)),bytes=f.stat().st_size,sha256=sha(f)) for f in sorted(O.rglob('*')) if f.is_file() and f.name!='LOCAL_FILE_MANIFEST.json'])

def worker(O,pilot=False):
    sys.path.insert(0,str(O/'prepared'))
    from feedback_jcr import calibrate,apply_bounds,price_reduce,CalibrationRejected
    from joint_calendar import solve
    from contract_prune import cost,shrink,contract_prune,feasible,validate
    contract=json.loads((O/'prepared/CONTRACT.json').read_text()); rows=[]
    specs=contract['cases'][:4] if pilot else contract['cases']
    for spec in specs:
        cfg=spec['configuration']; base=spec['initial']; truth=spec['true_coefficients']
        folder=O/'cases'/spec['key'];folder.mkdir(parents=True)
        F=np.array(contract['probe_features']); observations=F@np.array(truth)
        cal=calibrate([cfg['prefix']]+cfg['suffix'],F,observations,epsilon=0.)
        p,initial=apply_bounds(cfg,base,cal)
        dump(folder/'CALIBRATION.json',cal)
        tcfg=copy.deepcopy(cfg);tcfg['prefix']=truth[0];tcfg['suffix']=truth[1:]
        iterations=128 if pilot else contract['iterations']
        priced=price_reduce(p,initial,iterations=iterations)
        unpriced=price_reduce(p,initial,iterations=iterations,use_prices=False)
        for name,ans in [('priced',priced),('unpriced',unpriced)]:
            np.savez_compressed(folder/(name+'_ITERATES.npz'),x=ans.pop('x'),q=ans.pop('q'))
            dump(folder/(name+'_DECISIONS.json'),ans)
        reference=solve(p,initial,folder/'CALIBRATED_JCR',seconds=10)
        oracle=solve(tcfg,shrink(tcfg,base),folder/'ORACLE_JCR',seconds=10)
        fallback=contract_prune(p,initial)
        plans=dict(Static_JCR=spec['static_plan'],Calibrated_CRP=fallback['plan'],
                   Cost_only_repair=unpriced['plan'],Feedback_Price_JCR=priced['plan'],
                   Calibrated_JCR=reference['plan'],Oracle_JCR=oracle['plan'])
        methods={}
        for name,plan in plans.items():
            if plan is None:raise RuntimeError('reference did not return a plan')
            physical=shrink(tcfg,plan)
            assert not validate(tcfg,physical) and feasible(tcfg,physical)
            declared=cost(cfg,plan) if name=='Static_JCR' else cost(p,shrink(p,plan))
            if name=='Oracle_JCR': declared=cost(tcfg,physical)
            methods[name]=dict(plan=plan,declared_cost=declared,controlled_execution_cost=cost(tcfg,physical),
                              source_jobs=sum(q['kind']=='build' for q in plan),
                              recipient_edges=sum(len(q['recipients']) for q in plan if q['kind']=='pub'))
        probe_cost=float(observations.sum())*cfg['cpu_weight']
        row=dict(key=spec['key'],origin=spec['origin'],role=spec['role'],aliases=spec['aliases'],
                 prior_configuration=cfg,configuration=p,initial=initial,original_initial=base,
                 true_coefficients=truth,calibration=cal,probe_source_cost=probe_cost,
                 methods=methods,price=dict(objective=priced['objective'],lower=priced['lower'],
                 reason=priced['reason'],iterations=iterations,total_ms=priced['total_ms']),
                 calibrated_MIP=dict(status=reference['status'],objective=reference['objective'],
                 gap=reference['mip_gap'],total_ms=reference['compile_ms']+reference['LP_ms']+reference['MIP_ms']),
                 oracle_objective=oracle['objective'])
        dump(folder/'RESULT.json',row);rows.append(row)
        print(json.dumps(dict(key=spec['key'],ticks=cal['ticks'],
              costs={n:round(m['controlled_execution_cost'],6) for n,m in methods.items()},
              price_ms=round(priced['total_ms'],2))),flush=True)
    # Boundary adversaries use only the declared calibration interface.
    bcfg=contract['cases'][0]['configuration'];prior=[bcfg['prefix']]+bcfg['suffix']
    sparse=calibrate(prior,[np.ones(len(prior))],[sum(prior)],epsilon=0.)
    rejected=False
    try:calibrate(prior,[np.ones(len(prior))],[sum(prior)+10],epsilon=0.)
    except CalibrationRejected:rejected=True
    sensitivity=[]
    for eps in [0.,.05,.25]:
        spec=next(x for x in contract['cases'] if max(x['configuration']['suffix'])>1)
        F=np.array(contract['probe_features']);truth=np.array(spec['true_coefficients'])
        observations=F@truth+np.resize(np.array([1.,-1.]),len(F))*eps
        ans=calibrate([spec['configuration']['prefix']]+spec['configuration']['suffix'],F,observations,epsilon=eps)
        sensitivity.append(dict(epsilon=eps,observations=observations.tolist(),calibration=ans,truth=truth.tolist()))
    dump(O/'BOUNDARY_CHECKS.json',dict(rank_deficient=sparse,inconsistent_observation_rejected=rejected,
                                     sensitivity=sensitivity,drift_policy='No future guarantee after stationarity/noise assumptions fail; stop or externally recertify.'))
    dump(O/'RESULTS.json',dict(state='EXECUTED_PENDING_INDEPENDENT_AUDIT',cases=len(rows),pilot=pilot,
                              experiment='Constructed stationary additive execution work; not measured SQL time or production SLA.'))

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--worker',action='store_true');ap.add_argument('--pilot',action='store_true');a=ap.parse_args()
    O=ROOT/'raw_results'/RUN
    if a.worker:worker(O,a.pilot);return
    if a.pilot:raise ValueError('prepare fixed full contract before a pilot worker')
    O.mkdir();(O/'prepared').mkdir();(O/'cases').mkdir();cases=[];seen=set()
    originals=[json.loads(f.read_text()) for f in sorted((ROOT/'raw_results/joint_calendar_20261019/models').glob('u*/RESULT.json'))]
    for old in originals:
        cfg=old['configuration']
        for ratio in [.4,.7,1.]:
            truth=[cfg['prefix']]+[max(1,int(np.ceil(a*ratio))) for a in cfg['suffix']]
            signature=json.dumps([cfg,old['initial'],truth],sort_keys=True)
            if signature in seen:continue
            seen.add(signature);cases.append(dict(key='f'+str(len(cases)+1).zfill(2),origin=old['key'],role='primary',
                    aliases=old['aliases'],configuration=cfg,initial=old['initial'],
                    static_plan=old['JCR']['plan'],true_coefficients=truth))
    # Prespecified weak-scale checks preserve capacity-per-replica, as before.
    base=next(x for x in cases if 'skewed_split_FullOnly' in x['aliases'])
    for factor in [2,4]:
        spec=copy.deepcopy(base);cfg=spec['configuration'];n=len(cfg['views'])
        for field in ['views','lags','initial']:cfg[field]*=factor
        cfg['rate']*=factor
        for field in ['initial','static_plan']:
            for q in spec[field]:
                if q['kind']=='pub':q['recipients']=[c+n*k for k in range(factor) for c in q['recipients']]
        spec.update(key='scale'+str(factor),role='weak_scale');cases.append(spec)
    V=len(cases[0]['configuration']['suffix']);features=[np.ones(V+1).tolist()]
    for v in range(V):
        phi=[1]+[int(i==v) for i in range(V)];features.append(phi)
    contract=dict(id=RUN,cases=cases,probe_features=features,iterations=2048,
        setup='Five predeployment source probes shared/charged to all six methods; fresh-service window starts after setup. Probe work is reported and amortized explicitly.',
        comparisons=['Static_JCR','Calibrated_CRP','Cost_only_repair','Feedback_Price_JCR','Calibrated_JCR','Oracle_JCR'],
        scope='24 distinct stationary source-bound cases from all 12 prior base calendars plus 2 weak-scale cases; true source coefficients are constructed integers inside prior bounds. Exact noiseless observation primary; bounded-noise sensitivity separate. All epochs/starts fixed. Network bounds unchanged. No online adaptation of issued commands or physical wall-clock SLA. Same-information full JCR is an optimal reference, not a method we claim to beat.')
    dump(O/'prepared/CONTRACT.json',contract)
    for name in ['feedback_jcr.py','run_feedback_study.py','joint_calendar.py','contract_prune.py']:
        shutil.copyfile(ROOT/'src'/name,O/'prepared'/name)
    import scipy
    command=[sys.executable,'-I','-B','-X','utf8',str(O/'prepared/run_feedback_study.py'),'--worker']
    dump(O/'ENVIRONMENT_BEFORE_RUN.json',dict(command=command,python=sys.version,numpy=np.__version__,scipy=scipy.__version__,platform=platform.platform(),
         frozen=[dict(path=str(f.relative_to(O)),sha256=sha(f)) for f in sorted((O/'prepared').iterdir())],truth_access='Only observation generator and post-decision evaluator use truth; optimizers receive observations and prior bounds.'))
    with (O/'stdout.log').open('wb') as so,(O/'stderr.log').open('wb') as se:
        r=subprocess.run(command,cwd=ROOT,stdout=so,stderr=se,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    dump(O/'RECEIPT.json',dict(exit_code=r.returncode,state='EXECUTED_PENDING_AUDIT' if not r.returncode else 'FAILED_OUTPUT_PRESERVED'))
    inventory(O);print(json.dumps(dict(exit_code=r.returncode,run=str(O))))
    if r.returncode:print((O/'stderr.log').read_text()[-2500:]);raise SystemExit(r.returncode)

if __name__=='__main__':main()
