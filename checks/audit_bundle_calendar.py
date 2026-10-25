"""Independent event-state, matrix, dual-bound and pruning replay audit."""
from pathlib import Path
import copy,hashlib,itertools,json,math
import numpy as np
from scipy.sparse import load_npz
ROOT=Path('D:/ICDE-VLDB2027/ICDE-VLDB-1');O=ROOT/'raw_results/bundle_calendar_final_20261018';R=ROOT/'reviews/contract_prune_20261018'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def dump(p,x):p.write_text(json.dumps(x,indent=2)+'\n',encoding='utf-8')
def counts(cfg,plan):
 state=list(cfg['initial']);total=[0]*len(state)
 for t in range(cfg['T']):
  for q in plan:
   if q['kind']=='pub' and q['end']==t:
    for c in q['recipients']:state[c]=max(state[c],q['epoch'])
  if t>=cfg['warm']:
   for c,e in enumerate(state):total[c]+=int(t-e<=cfg['lags'][c])
 return total
def objective(cfg,plan):return sum(cfg['cpu_weight']*(q['end']-q['start']) if q['kind']=='build' else cfg['byte_weight']*cfg['upper'][q['view']]*len(q['recipients']) for q in plan)
def required(cfg):return cfg['T']-cfg['warm']-math.floor((cfg['T']-cfg['warm'])*(1-cfg['rho'])+1e-9)
def project(cfg,plan):
 pubs=[copy.deepcopy(q) for q in plan if q['kind']=='pub' and q['recipients']];out=[]
 for q in plan:
  if q['kind']=='build':
   vs=sorted({p['view'] for p in pubs if p['epoch']==q['epoch'] and p['view'] in q['views']})
   if vs:out.append(dict(q,views=vs,end=q['start']+cfg['prefix']+sum(cfg['suffix'][v] for v in vs)))
 for p in pubs:p['end']=p['start']+max(1,math.ceil(cfg['upper'][p['view']]*len(p['recipients'])/cfg['rate']));out.append(p)
 return sorted(out,key=lambda q:(q['start'],q['kind'],q.get('view',-1)))
def legality(cfg,plan):
 ok=True
 for kind in ['build','pub']:
  seq=sorted([q for q in plan if q['kind']==kind],key=lambda q:q['start']);ok &= all(q['start']>=cfg['startup'] and q['end']<cfg['T'] and q['start']<q['end'] for q in seq);ok &= all(a['end']<=b['start'] for a,b in zip(seq,seq[1:]))
 for q in plan:
  if q['kind']=='build':ok &= q['epoch']==q['start'] and q['epoch'] in cfg['captures'] and q['end']-q['start']==cfg['prefix']+sum(cfg['suffix'][v] for v in q['views'])
  else:ok &= bool(q['recipients']) and len(set(q['recipients']))==len(q['recipients']) and all(cfg['views'][c]==q['view'] for c in q['recipients']) and q['end']-q['start']==max(1,math.ceil(cfg['upper'][q['view']]*len(q['recipients'])/cfg['rate'])) and any(b['kind']=='build' and b['epoch']==q['epoch'] and q['view'] in b['views'] and b['end']<=q['start'] for b in plan)
 return bool(ok)
def main():
 R.mkdir(parents=True,exist_ok=True);checks=[];rows=[];iterations=0;matrix_rows=0
 def ck(n,ok,e):checks.append(dict(name=n,state='PASS' if ok else 'FAIL',evidence=e));assert ok,(n,e)
 mf=load(O/'LOCAL_FILE_MANIFEST.json');ck('all_frozen_run_files',all(sha(O/q['path'])==q['sha256'] for q in mf),len(mf))
 contract=load(O/'prepared/CONTRACT.json')
 for row in load(O/'RESULTS.json')['cases']:
  key=row['key'];folder=O/'models'/key;r=load(folder/'CALENDARS.json');cfg=r['configuration'];model=load(folder/'MODEL.json');desc=model['descriptors'];A=load_npz(folder/'A.npz').tocsr();vectors=np.load(folder/'VECTORS.npz');b=vectors['b'];c=vectors['c'];integer=vectors['integer'];build=[i for i,d in enumerate(desc) if d['kind']=='build'];pub=[i for i,d in enumerate(desc) if d['kind']=='pub'];cover=[i for i,d in enumerate(desc) if d['kind']=='coverage'];indexed={}
  for i in build:
   for v in desc[i]['views']:indexed.setdefault((v,desc[i]['epoch']),[]).append(i)
  correct=True
  for i,label in enumerate(model['row_labels']):
   actual={int(j):float(x) for j,x in zip(A.getrow(i).indices,A.getrow(i).data)};typ=label[0];expected_b=0
   if typ=='availability':
    positives=[j for j,x in actual.items() if x==1];correct &= len(positives)==1
    p=desc[positives[0]];expected={positives[0]:1,**{j:-1 for j in indexed.get((p['view'],p['epoch']),[]) if desc[j]['end']<=p['start']}}
   elif typ in ['CPU','network']:
    pool=build if typ=='CPU' else pub;expected={j:1 for j in pool if desc[j]['start']<=label[1]<desc[j]['end']};expected_b=1
   elif typ=='coverage':
    _,consumer,t=label;z=next(j for j in cover if desc[j]['consumer']==consumer and desc[j]['tick']==t);expected={z:1,**{j:-1 for j in pub if consumer in desc[j]['recipients'] and desc[j]['end']<=t<=desc[j]['epoch']+cfg['lags'][consumer]}}
   else:
    consumer=label[1];expected={j:-1 for j in cover if desc[j]['consumer']==consumer};expected_b=sum(t-cfg['initial'][consumer]<=cfg['lags'][consumer] for t in range(cfg['warm'],cfg['T']))-required(cfg)
   correct &= actual==expected and b[i]==expected_b
  matrix_rows+=A.shape[0];ck('all_model_rows_'+key,correct,dict(rows=A.shape[0],columns=A.shape[1]))
  ck('objective_and_integer_domain_'+key,all(c[i]==(cfg['cpu_weight']*(d['end']-d['start']) if d['kind']=='build' else cfg['byte_weight']*cfg['upper'][d['view']]*len(d['recipients']) if d['kind']=='pub' else 0) and integer[i]==int(d['kind']!='coverage') for i,d in enumerate(desc)),key)
  dual=None
  if (folder/'LP_WITNESS.npz').exists():
   wit=np.load(folder/'LP_WITNESS.npz');q=np.maximum(0,-wit['inequality_dual']);dual=float(-b@q+np.minimum(0,c+A.T@q).sum());ck('independent_Lagrangian_lower_'+key,dual<=r['solver']['lp_lower']+1e-5 and abs(dual-r['solver']['lp_lower'])<1e-4,dual)
  if (folder/'SOLUTION.npy').exists():
   x=np.load(folder/'SOLUTION.npy');ck('numerical_incumbent_'+key,np.max(A@x-b)<1e-5 and np.max(abs(x[integer==1]-np.rint(x[integer==1])))<1e-5 and abs(c@x-r['solver']['objective'])<1e-5,r['solver']['status'])
  if r['best'] is None:
   rows.append(dict(key=key,available=False,solver_status=r['solver']['status']));continue
  base=r['best']['plan'];plan=project(cfg,base);pr=r['pruned'];ck('initial_calendar_'+key,legality(cfg,base) and min(counts(cfg,base))>=required(cfg) and abs(objective(cfg,base)-r['best']['objective'])<1e-6,dict(origin=r['best']['origin'],counts=counts(cfg,base)))
  if dual is not None:ck('lower_bounds_base_'+key,dual<=objective(cfg,base)+1e-5,dual)
  accepted=0
  for step in pr['decisions']:
   v,e,s,consumer=step['removed_edge'];trial=copy.deepcopy(plan);target=next(q for q in trial if q['kind']=='pub' and (q['view'],q['epoch'],q['start'])==(v,e,s));target['recipients'].remove(consumer);trial=project(cfg,trial);fresh=counts(cfg,trial);ok=min(fresh)>=required(cfg);iterations+=1
   ck(f'{key}_iteration_{step["iteration"]}',fresh==step['counts'] and ok==step['accepted'] and abs(objective(cfg,plan)-step['cost_before'])<1e-6 and abs(objective(cfg,trial)-step['cost_trial'])<1e-6 and legality(cfg,trial),dict(accepted=ok,counts=fresh))
   if ok:ck(f'{key}_cost_descent_{accepted}',objective(cfg,trial)<=objective(cfg,plan)+1e-8,step['removed_edge']);accepted+=1;plan=trial
  possible=[]
  for i,p in enumerate(plan):
   if p['kind']=='pub':
    for consumer in p['recipients']:
     trial=copy.deepcopy(plan);trial[i]['recipients'].remove(consumer);possible.append(min(counts(cfg,project(cfg,trial)))>=required(cfg))
  R0=sum(len(q['recipients']) for q in base if q['kind']=='pub')
  ck('final_preservation_and_termination_'+key,plan==pr['plan'] and counts(cfg,plan)==pr['counts'] and legality(cfg,plan) and min(counts(cfg,plan))>=required(cfg) and objective(cfg,plan)<=objective(cfg,base)+1e-8 and len(pr['decisions'])<=R0*(R0+1)//2 and accepted==pr['accepted_removals'] and not any(possible),dict(R0=R0,iterations=len(pr['decisions']),accepted=accepted))
  rows.append(dict(key=key,available=True,family=row['family'],cpu=row['cpu'],lag=row['lag'],origin=r['best']['origin'],solver_status=r['solver']['status'],before=objective(cfg,base),after=objective(cfg,plan),CPU_before=sum(q['end']-q['start'] for q in base if q['kind']=='build'),CPU_after=sum(q['end']-q['start'] for q in plan if q['kind']=='build'),charged_before=sum(cfg['upper'][q['view']]*len(q['recipients']) for q in base if q['kind']=='pub'),charged_after=sum(cfg['upper'][q['view']]*len(q['recipients']) for q in plan if q['kind']=='pub'),min_before=min(counts(cfg,base))/(cfg['T']-cfg['warm']),min_after=min(counts(cfg,plan))/(cfg['T']-cfg['warm']),prune_ms=pr['wall_ms'],accepted=accepted,independent_LP_lower=dual))
 dump(R/'CALENDAR_AUDIT.json',dict(state='PASS',marker='AI_INDEPENDENT_IMPLEMENTATION_REPLAY_NOT_EXTERNAL_REVIEW',checks_count=len(checks),checks=checks,matrix_rows_reconstructed=matrix_rows,pruning_iterations_replayed=iterations,cases=rows,model_solver_timeouts=sum(r['solver_status']==1 for r in rows),scope=contract['scope'],input_manifest_sha256=sha(O/'LOCAL_FILE_MANIFEST.json')));print(json.dumps(dict(state='PASS',checks=len(checks),cases=len(rows),iterations=iterations,matrix_rows=matrix_rows,improved=sum(r.get('after',0)<r.get('before',0)-1e-8 for r in rows))))
if __name__=='__main__':main()
