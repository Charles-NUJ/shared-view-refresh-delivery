"""D01-JCR: exact MILP of deletion/projection within one fixed feasible calendar."""
import copy,itertools,math,time
import numpy as np
from scipy.sparse import coo_matrix,save_npz
from scipy.optimize import Bounds,LinearConstraint,milp,linprog
from contract_prune import validate,feasible,shrink,cost
def compile_model(cfg,plan):
 if validate(cfg,plan) or not feasible(cfg,plan):raise ValueError('valid feasible initial calendar required')
 assert 0<=cfg['warm']<cfg['T'] and 0<cfg['rho']<=1 and cfg['rate']>0
 assert min(cfg['upper'])>0 and cfg['prefix']>=0 and min(cfg['suffix'])>=0 and cfg['cpu_weight']>=0 and cfg['byte_weight']>=0
 builds=[copy.deepcopy(q) for q in plan if q['kind']=='build'];pubs=[copy.deepcopy(q) for q in plan if q['kind']=='pub'];owner={}
 for b,q in enumerate(builds):
  for v in q['views']:
   key=(v,q['epoch'])
   if key in owner:raise ValueError('unique source owner for each (view,epoch) required')
   owner[key]=b
 for q in pubs:assert (q['view'],q['epoch']) in owner
 keys=[];obj=[];integer=[];index={}
 def var(key,c=0.,binary=False):
  index[key]=len(keys);keys.append(key);obj.append(c);integer.append(int(binary))
 for j,p in enumerate(pubs):
  for c in p['recipients']:var(('y',j,c),cfg['byte_weight']*cfg['upper'][p['view']],True)
  for k in range(1,len(p['recipients'])+1):
   var(('h',j,k),binary=True)
   for c in p['recipients']:var(('w',j,c,k))
 for b,q in enumerate(builds):
  var(('g',b),cfg['cpu_weight']*cfg['prefix'],True)
  for v in q['views']:var(('a',b,v),cfg['cpu_weight']*cfg['suffix'][v],True)
 for c in range(len(cfg['views'])):
  for t in range(cfg['warm'],cfg['T']):var(('z',c,t))
 rows=[];rhs=[];labels=[]
 def row(label,terms,b):
  rows.append({index[k]:float(v) for k,v in terms.items() if v});rhs.append(float(b));labels.append(label)
 def equal(label,terms,b=0):
  row(label+['+'],terms,b);row(label+['-'],{k:-v for k,v in terms.items()},-b)
 for j,p in enumerate(pubs):
  r=len(p['recipients']);b=owner[(p['view'],p['epoch'])]
  row(['one_mode',j],{('h',j,k):1 for k in range(1,r+1)},1)
  for c in p['recipients']:
   equal(['edge_mode',j,c],{('y',j,c):-1,**{('w',j,c,k):1 for k in range(1,r+1)}})
   row(['payload',j,c],{('y',j,c):1,('a',b,p['view']):-1},0)
  for k in range(1,r+1):
   equal(['cardinality',j,k],{('h',j,k):-k,**{('w',j,c,k):1 for c in p['recipients']}})
   for c in p['recipients']:row(['mode_link',j,c,k],{('w',j,c,k):1,('h',j,k):-1},0)
 for b,q in enumerate(builds):
  row(['prefix_exact',b],{('g',b):1,**{('a',b,v):-1 for v in q['views']}},0)
  for v in q['views']:
   row(['prefix',b,v],{('a',b,v):1,('g',b):-1},0)
   uses={('y',j,c):-1 for j,p in enumerate(pubs) if (p['view'],p['epoch'])==(v,q['epoch']) for c in p['recipients']}
   row(['view_exact',b,v],{('a',b,v):1,**uses},0)
 N=cfg['T']-cfg['warm'];B=math.floor(N*(1-cfg['rho'])+1e-9);target=N-B
 for c in range(len(cfg['views'])):
  row(['budget',c],{('z',c,t):-1 for t in range(cfg['warm'],cfg['T'])},-target)
  for t in range(cfg['warm'],cfg['T']):
   terms={('z',c,t):1}
   for j,p in enumerate(pubs):
    if c not in p['recipients']:continue
    for k in range(1,len(p['recipients'])+1):
     d=max(1,math.ceil(cfg['upper'][p['view']]*k/cfg['rate']))
     if p['start']+d<=t<=p['epoch']+cfg['lags'][c]:terms[('w',j,c,k)]=-1
   row(['coverage',c,t],terms,int(t-cfg['initial'][c]<=cfg['lags'][c]))
 rr=[];cc=[];vv=[]
 for i,terms in enumerate(rows):
  for j,v in terms.items():rr.append(i);cc.append(j);vv.append(v)
 A=coo_matrix((vv,(rr,cc)),shape=(len(rows),len(keys))).tocsr()
 return dict(A=A,b=np.array(rhs),c=np.array(obj),integer=np.array(integer),keys=keys,labels=labels,builds=builds,pubs=pubs,target=target)
def decode(cfg,model,x):
 plan=[]
 for j,q in enumerate(model['pubs']):
  cs=[c for c in q['recipients'] if x[model['keys'].index(('y',j,c))]>.5]
  if cs:plan.append(dict(q,recipients=cs))
 plan.extend(copy.deepcopy(model['builds']))
 return shrink(cfg,plan)
def solve(cfg,plan,folder,seconds=10):
 import json
 folder.mkdir();begin=time.perf_counter();m=compile_model(cfg,plan);compile_ms=(time.perf_counter()-begin)*1000
 save_npz(folder/'A.npz',m['A']);np.savez_compressed(folder/'VECTORS.npz',b=m['b'],c=m['c'],integer=m['integer'])
 meta={k:v for k,v in m.items() if k not in ['A','b','c','integer']};(folder/'MODEL.json').write_text(json.dumps(meta,indent=2)+'\n')
 st=time.perf_counter();lp=linprog(m['c'],A_ub=m['A'],b_ub=m['b'],bounds=(0,1),method='highs');lp_ms=(time.perf_counter()-st)*1000;lower=None
 if lp.success:
  q=np.maximum(0,-lp.ineqlin.marginals);lower=float(-m['b']@q+np.minimum(0,m['c']+m['A'].T@q).sum());np.savez_compressed(folder/'LP_WITNESS.npz',x=lp.x,q=q)
 st=time.perf_counter();r=milp(m['c'],integrality=m['integer'],bounds=Bounds(np.zeros(len(m['c'])),np.ones(len(m['c']))),constraints=LinearConstraint(m['A'],-np.inf,m['b']),options=dict(time_limit=seconds,mip_rel_gap=0.0));mip_ms=(time.perf_counter()-st)*1000
 out=dict(status=int(r.status),message=str(r.message),objective=None,dual_bound=None if getattr(r,'mip_dual_bound',None) is None else float(r.mip_dual_bound),mip_gap=None if getattr(r,'mip_gap',None) is None else float(r.mip_gap),LP_lower=lower,compile_ms=compile_ms,LP_ms=lp_ms,MIP_ms=mip_ms,rows=m['A'].shape[0],variables=m['A'].shape[1],integer_variables=int(sum(m['integer'])),plan=None)
 if r.x is not None:
  x=np.array(r.x);np.save(folder/'SOLUTION.npy',x);decoded=decode(cfg,m,x)
  assert max(m['A']@x-m['b'])<1e-5 and max(abs(x[m['integer']==1]-np.rint(x[m['integer']==1])))<1e-5
  assert not validate(cfg,decoded) and feasible(cfg,decoded) and abs(cost(cfg,decoded)-m['c']@x)<1e-5
  out.update(objective=float(m['c']@x),plan=decoded)
 return out
