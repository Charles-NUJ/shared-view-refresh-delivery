from pathlib import Path
import copy,hashlib,itertools,json,math,random,shutil,subprocess,sys,time,platform
ROOT=Path(__file__).resolve().parents[3];RUN='joint_validation_20261019'
def dump(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def count(p,plan):
 state=list(p['initial']);fresh=[0]*len(state)
 for t in range(p['T']):
  for q in plan:
   if q['kind']=='pub' and q['end']==t:
    for c in q['recipients']:state[c]=max(state[c],q['epoch'])
  if t>=p['warm']:
   for c,e in enumerate(state):fresh[c]+=t-e<=p['lags'][c]
 return fresh
def project(p,base,bits):
 pubs=[dict(q,recipients=[c for c in q['recipients'] if bits.get((i,c),False)]) for i,q in enumerate(base) if q['kind']=='pub'];out=[]
 for q in pubs:
  if q['recipients']:q['end']=q['start']+max(1,math.ceil(p['upper'][q['view']]*len(q['recipients'])/p['rate']));out.append(q)
 for q in base:
  if q['kind']=='build':
   needed=sorted({u['view'] for u in out if u['epoch']==q['epoch'] and u['view'] in q['views']})
   if needed:out.append(dict(q,views=needed,end=q['start']+p['prefix']+sum(p['suffix'][v] for v in needed)))
 return out
def value(p,plan):return sum(p['cpu_weight']*(q['end']-q['start']) if q['kind']=='build' else p['byte_weight']*p['upper'][q['view']]*len(q['recipients']) for q in plan)
def worker(O):
 sys.path.insert(0,str(O/'prepared'));from joint_calendar import solve
 contract=json.loads((O/'prepared/CONTRACT.json').read_text());rows=[]
 for case in contract['cases']:
  p=case['configuration'];base=case['initial'];edges=[(i,c) for i,q in enumerate(base) if q['kind']=='pub' for c in q['recipients']];B=math.floor((p['T']-p['warm'])*(1-p['rho'])+1e-9);target=p['T']-p['warm']-B;best=float('inf');legal=0
  for values in itertools.product([0,1],repeat=len(edges)):
   cal=project(p,base,dict(zip(edges,values)))
   if min(count(p,cal))>=target:best=min(best,value(p,cal));legal+=1
  ans=solve(p,base,O/'models'/case['key'],10)
  assert ans['status']==0 and abs(ans['objective']-best)<1e-6
  rows.append(dict(key=case['key'],subsets=2**len(edges),feasible=legal,enumerated_optimum=best,MIP_objective=ans['objective'],MIP_status=ans['status']))
 dump(O/'RESULTS.json',dict(state='PASS',cases=rows,subsets_examined=sum(x['subsets'] for x in rows),scope='Independent subset enumerator;3old known cases+15new random constructed contracts;not independent data or mathematical proof'));print(json.dumps(dict(cases=len(rows),subsets=sum(x['subsets'] for x in rows))),flush=True)
def main():
 import argparse
 ap=argparse.ArgumentParser();ap.add_argument('--worker',action='store_true');a=ap.parse_args();O=ROOT/'raw_results'/RUN
 if a.worker:worker(O);return
 O.mkdir();(O/'prepared').mkdir();(O/'models').mkdir();old=json.loads((ROOT/'raw_results/contract_prune_tiny_20261018/CONTRACT_BEFORE_RUN.json').read_text());cases=[dict(key='known'+str(i),**x) for i,x in enumerate(old['cases'])];rng=random.Random(19019)
 for i in range(15):
  p=copy.deepcopy(old['cases'][0]['configuration']);p['upper']=[rng.randint(80,150),rng.randint(250,450)];p['lags']=[rng.randint(8,16) for _ in range(4)];base=copy.deepcopy(old['cases'][0]['initial'])
  for q in base:
   if q['kind']=='pub':q['end']=q['start']+max(1,math.ceil(p['upper'][q['view']]*2/p['rate']))
  minimum=min(count(p,base))/(p['T']-p['warm']);p['rho']=max(.1,minimum-rng.choice([0,.05,.10]));cases.append(dict(key='random'+str(i).zfill(2),configuration=p,initial=base))
 dump(O/'prepared/CONTRACT.json',dict(seed=19019,cases=cases,scope='Small independent formulation validation,not performance claims'))
 for n in ['run_joint_validation.py','joint_calendar.py','contract_prune.py']:shutil.copyfile(ROOT/'src'/n,O/'prepared'/n)
 command=[sys.executable,'-I','-B','-X','utf8',str(O/'prepared/run_joint_validation.py'),'--worker'];dump(O/'ENVIRONMENT_BEFORE_RUN.json',dict(command=command,python=sys.version,platform=platform.platform(),frozen=[dict(path=str(p.relative_to(O)),sha256=sha(p)) for p in (O/'prepared').iterdir()]))
 with (O/'stdout.log').open('wb') as so,(O/'stderr.log').open('wb') as se:r=subprocess.run(command,cwd=ROOT,stdout=so,stderr=se)
 dump(O/'RECEIPT.json',dict(exit_code=r.returncode));dump(O/'LOCAL_FILE_MANIFEST.json',[dict(path=str(p.relative_to(O)),bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(O.rglob('*')) if p.is_file()])
 print(json.dumps(dict(exit_code=r.returncode)))
 if r.returncode:print((O/'stderr.log').read_text()[-1800:]);raise SystemExit(r.returncode)
if __name__=='__main__':main()
