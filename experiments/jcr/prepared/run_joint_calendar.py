from pathlib import Path
import argparse,copy,hashlib,json,platform,shutil,subprocess,sys
ROOT=Path('D:/ICDE-VLDB2027/ICDE-VLDB-1');RUN='joint_calendar_20261019'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2)+'\n',encoding='utf-8')
def worker(O):
 sys.path.insert(0,str(O/'prepared'));from joint_calendar import solve
 from contract_prune import contract_prune,cost,fresh_counts
 cfg=json.loads((O/'prepared/CONTRACT.json').read_text());rows=[]
 for case in cfg['cases']:
  p=case['configuration'];base=case['initial'];greedy=contract_prune(p,base) if case['role']!='weak_scale' else None
  ans=solve(p,base,O/'models'/case['key'],cfg['time_limit']);row=dict(key=case['key'],role=case['role'],aliases=case.get('aliases',[]),configuration=p,initial=base,CRP=greedy,JCR=ans,base_cost=cost(p,base),base_counts=fresh_counts(p,base))
  if ans['plan'] is not None:row['JCR_counts']=fresh_counts(p,ans['plan'])
  dump(O/'models'/case['key']/'RESULT.json',row);rows.append(dict(key=case['key'],role=case['role'],base=row['base_cost'],CRP=None if greedy is None else greedy['final_cost'],JCR=ans['objective'],status=ans['status'],gap=ans['mip_gap'],variables=ans['variables'],rows=ans['rows'],MIP_ms=ans['MIP_ms']))
  print(json.dumps(rows[-1]),flush=True)
 dump(O/'RESULTS.json',dict(state='FINISHED_AWAITING_INDEPENDENT_AUDIT',cases=rows))
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--worker',action='store_true');a=ap.parse_args();O=ROOT/'raw_results'/RUN
 if a.worker:worker(O);return
 O.mkdir();(O/'prepared').mkdir();(O/'models').mkdir();cases=[];by={}
 for f in sorted((ROOT/'raw_results/bundle_calendar_final_20261018/models').glob('*/CALENDARS.json')):
  old=json.loads(f.read_text());p=old['configuration'];base=old['best']['plan'];h=hashlib.sha256(json.dumps([p,base],sort_keys=True).encode()).hexdigest()
  if h not in by:
   case=dict(key='u'+str(len(cases)+1).zfill(2),role='unique_base',configuration=p,initial=base,aliases=[],source_sha256=sha(f));cases.append(case);by[h]=case
  by[h]['aliases'].append(old['key'])
 tiny=json.loads((ROOT/'raw_results/contract_prune_tiny_20261018/CONTRACT_BEFORE_RUN.json').read_text())
 for i,x in enumerate(tiny['cases']):cases.append(dict(key='tiny'+str(i+1),role='exhaustive',**x))
 base=next(x for x in cases if 'skewed_split_FullOnly' in x.get('aliases',[]))
 for factor in [1,2,4,8]:
  p=copy.deepcopy(base['configuration']);n=len(p['views'])
  for field in ['views','lags','initial']:p[field]=p[field]*factor
  p['rate']*=factor;plan=copy.deepcopy(base['initial'])
  for q in plan:
   if q['kind']=='pub':q['recipients']=[c+i*n for i in range(factor) for c in q['recipients']]
  cases.append(dict(key='scale'+str(factor),role='weak_scale',configuration=p,initial=plan,replication=factor))
 contract=dict(id=RUN,time_limit=10,cases=cases,scope='Exact deletion/projection within a feasible unissued calendar;unique (view,epoch) owner,additive source reservations,cardinality-dependent network duration. Twelve unique prior bases,3exhaustive cases,4weak-scale contracts. Weak scale multiplies network rate with consumer replicas;no fixed-capacity or physical scale claim. All variants retain epochs and command starts. No new data seed,global scheduling optimum,or native system claim.')
 dump(O/'prepared/CONTRACT.json',contract)
 for n in ['joint_calendar.py','run_joint_calendar.py','contract_prune.py']:shutil.copyfile(ROOT/'src'/n,O/'prepared'/n)
 import scipy
 command=[sys.executable,'-I','-B','-X','utf8',str(O/'prepared/run_joint_calendar.py'),'--worker']
 dump(O/'COMMAND_ENVIRONMENT_BEFORE_RUN.json',dict(command=command,python=sys.version,scipy=scipy.__version__,platform=platform.platform(),frozen=[dict(path=str(p.relative_to(O)),sha256=sha(p)) for p in (O/'prepared').iterdir()]))
 with (O/'stdout.log').open('wb') as so,(O/'stderr.log').open('wb') as se:r=subprocess.run(command,cwd=ROOT,stdout=so,stderr=se)
 dump(O/'RECEIPT.json',dict(exit_code=r.returncode,state='FINISHED_AWAITING_AUDIT' if not r.returncode else 'FAILED_PRESERVED'))
 dump(O/'LOCAL_FILE_MANIFEST.json',[dict(path=str(p.relative_to(O)),bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(O.rglob('*')) if p.is_file()])
 print(json.dumps(dict(exit_code=r.returncode,run_id=RUN)))
 if r.returncode:print((O/'stderr.log').read_text()[-2000:]);raise SystemExit(r.returncode)
if __name__=='__main__':main()
