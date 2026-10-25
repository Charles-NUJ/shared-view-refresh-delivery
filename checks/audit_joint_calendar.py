from pathlib import Path
import hashlib,json,math,sys
import numpy as np
from scipy.sparse import load_npz
ROOT=Path(__file__).resolve().parents[1];O=ROOT/'raw_results/joint_calendar_20261019';R=ROOT/'reviews/joint_calendar_20261019'
from audit_bundle_calendar import project,counts,legality,objective,required
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 checks=[];rows=[];totalrows=0
 def ck(n,v,e):checks.append(dict(name=n,state='PASS' if v else 'FAIL',evidence=e));assert v,(n,e)
 ck('raw_hashes',all(sha(O/x['path'])==x['sha256'] for x in load(O/'LOCAL_FILE_MANIFEST.json')),sha(O/'LOCAL_FILE_MANIFEST.json'))
 for f in sorted((O/'models').iterdir()):
  r=load(f/'RESULT.json');p=r['configuration'];m=load(f/'MODEL.json');keys=[tuple(k) for k in m['keys']];ix={k:i for i,k in enumerate(keys)};A=load_npz(f/'A.npz').tocsr();vec=np.load(f/'VECTORS.npz');b=vec['b'];c=vec['c'];integer=vec['integer'];pub=m['pubs'];build=m['builds'];owner={(v,x['epoch']):j for j,x in enumerate(build) for v in x['views']}
  ok=True
  for i,label in enumerate(m['labels']):
   tag,*v=label;bound=0.;terms={}
   if tag=='one_mode':
    j,=v;terms={('h',j,k):1 for k in range(1,len(pub[j]['recipients'])+1)};bound=1
   elif tag=='edge_mode':
    j,cs,sgn=v;terms={('y',j,cs):-1,**{('w',j,cs,k):1 for k in range(1,len(pub[j]['recipients'])+1)}}
    if sgn=='-':terms={k:-v for k,v in terms.items()}
   elif tag=='cardinality':
    j,k,sgn=v;terms={('h',j,k):-k,**{('w',j,cs,k):1 for cs in pub[j]['recipients']}}
    if sgn=='-':terms={k:-v for k,v in terms.items()}
   elif tag=='mode_link':j,cs,k=v;terms={('w',j,cs,k):1,('h',j,k):-1}
   elif tag=='payload':j,cs=v;u=pub[j];terms={('y',j,cs):1,('a',owner[(u['view'],u['epoch'])],u['view']):-1}
   elif tag=='prefix_exact':j,=v;terms={('g',j):1,**{('a',j,k):-1 for k in build[j]['views']}}
   elif tag=='prefix':j,k=v;terms={('a',j,k):1,('g',j):-1}
   elif tag=='view_exact':
    j,k=v;terms={('a',j,k):1,**{('y',h,cs):-1 for h,u in enumerate(pub) if (u['view'],u['epoch'])==(k,build[j]['epoch']) for cs in u['recipients']}}
   elif tag=='budget':cs,=v;terms={('z',cs,t):-1 for t in range(p['warm'],p['T'])};bound=-required(p)
   elif tag=='coverage':
    cs,t=v;terms={('z',cs,t):1};bound=int(t-p['initial'][cs]<=p['lags'][cs])
    for j,u in enumerate(pub):
     if cs in u['recipients']:
      for k in range(1,len(u['recipients'])+1):
       end=u['start']+max(1,math.ceil(p['upper'][u['view']]*k/p['rate']))
       if end<=t<=u['epoch']+p['lags'][cs]:terms[('w',j,cs,k)]=-1
   else:raise AssertionError(tag)
   actual=dict(zip(A.getrow(i).indices,A.getrow(i).data));expected={ix[k]:v for k,v in terms.items()};ok &= actual==expected and b[i]==bound
  totalrows+=len(m['labels']);ck('row_rebuild_'+r['key'],ok,A.shape)
  expect=[]
  for k in keys:
   expect.append(p['byte_weight']*p['upper'][pub[k[1]]['view']] if k[0]=='y' else p['cpu_weight']*p['prefix'] if k[0]=='g' else p['cpu_weight']*p['suffix'][k[2]] if k[0]=='a' else 0.)
  ck('objective_domain_'+r['key'],np.allclose(expect,c) and np.array_equal(integer,[int(k[0] in ['y','h','g','a']) for k in keys]),len(keys))
  expected_n=sum(2*len(u['recipients'])+len(u['recipients'])**2 for u in pub)+len(build)+sum(len(x['views']) for x in build)+len(p['views'])*(p['T']-p['warm'])
  ck('polynomial_variable_count_'+r['key'],len(keys)==expected_n,expected_n)
  x=np.load(f/'SOLUTION.npy');ck('incumbent_vector_'+r['key'],np.max(A@x-b)<=1e-6 and np.min(x)>=-1e-7 and np.max(x)<=1+1e-7 and max(abs(x[integer==1]-np.rint(x[integer==1])))<1e-6,float(c@x))
  selected=[]
  for j,u in enumerate(pub):selected.append(dict(u,recipients=[cs for cs in u['recipients'] if x[ix[('y',j,cs)]]>.5]))
  decoded=project(p,[*build,*selected]);J=objective(p,decoded);fresh=counts(p,decoded);q=r['JCR']
  ck('independent_decode_'+r['key'],decoded==q['plan'] and legality(p,decoded) and min(fresh)>=required(p) and abs(J-q['objective'])<1e-6 and abs(J-c@x)<1e-6,fresh)
  wit=np.load(f/'LP_WITNESS.npz');dual=wit['q'];L=float(-b@dual+np.minimum(0,c+A.T@dual).sum())
  ck('weak_dual_'+r['key'],min(dual)>=0 and L<=J+1e-6 and abs(L-q['LP_lower'])<1e-6,L)
  ck('solver_certificate_'+r['key'],q['status']==0 and q['mip_gap']<=1e-7 and abs(q['dual_bound']-J)<1e-6,dict(status=q['status'],U=J,L=q['dual_bound']))
  if r['CRP'] is not None:
   ck('strong_same_domain_CR P_'+r['key'],J<=r['CRP']['final_cost']+1e-6 and legality(p,r['CRP']['plan']) and min(counts(p,r['CRP']['plan']))>=required(p),r['CRP']['final_cost'])
  rows.append(dict(key=r['key'],role=r['role'],aliases=r['aliases'],base=r['base_cost'],CRP=None if r['CRP'] is None else r['CRP']['final_cost'],JCR=J,min_fresh=min(fresh)/(p['T']-p['warm']),LP_lower=L,solver_lower=q['dual_bound'],status=q['status'],variables=q['variables'],matrix_rows=q['rows'],compile_ms=q['compile_ms'],MIP_ms=q['MIP_ms'],LP_ms=q['LP_ms'],consumers=len(p['views'])))
 result=dict(state='PASS',marker='AI_INDEPENDENT_ROW_AND_EXECUTION_CHECK_NOT_HUMAN_PROOF',checks=checks,checks_count=len(checks),matrix_rows_reconstructed=totalrows,cases=rows,unique_bases=sum(x['role']=='unique_base' for x in rows),improved_over_CRP=sum(x['role']=='unique_base' and x['JCR']<x['CRP']-1e-7 for x in rows),scope='Exact only in fixed-calendar deletion/projection domain;reported MILP certificates within numerical tolerance;not a global scheduler or external peer review')
 (R/'MODEL_AUDIT.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k not in ['checks','cases']}))
if __name__=='__main__':main()
