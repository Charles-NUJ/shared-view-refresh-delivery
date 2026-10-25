"""Separate direct-SQL, installation, charge, and counterexample checker."""
from pathlib import Path
import csv,hashlib,json,math,sqlite3,statistics
ROOT=Path('D:/ICDE-VLDB2027/ICDE-VLDB-1');O=ROOT/'raw_results/origin_sharing_20261018';R=ROOT/'reviews/origin_mainline_20261018'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def pack(x):return json.dumps(x,ensure_ascii=False,separators=(',',':')).encode()
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def dump(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
def open_source(case):
 con=sqlite3.connect(':memory:');con.execute('CREATE TABLE versions(id,zone_id,amount,minute,valid_from,valid_to)');con.execute('CREATE TABLE zones(zone_id,borough)')
 with (case/'versions.csv').open(encoding='utf-8-sig',newline='') as f:con.executemany('INSERT INTO versions VALUES(?,?,?,?,?,?)',(tuple(int(r[k]) for k in ['id','zone_id','amount','minute','valid_from','valid_to']) for r in csv.DictReader(f)))
 with (case/'zones.csv').open(encoding='utf-8-sig',newline='') as f:con.executemany('INSERT INTO zones VALUES(?,?)',((int(r['zone_id']),r['borough'] or None) for r in csv.DictReader(f)))
 return con
# Deliberately bypass the temporary snapshot and the worker's build/QUERIES.
SELECTS=[('a.zone_id',''),('z.borough',''),('z.borough','AND a.amount>=2000'),('CAST(a.minute/15 AS INTEGER),z.borough','')]
def direct(con,epoch):
 out={}
 for v,(cols,condition) in enumerate(SELECTS):
  order='1,2 NULLS LAST' if v==3 else '1 NULLS LAST'
  q=f'SELECT {cols},COUNT(*),SUM(a.amount) FROM versions a JOIN zones z ON a.zone_id=z.zone_id WHERE a.valid_from<=? AND a.valid_to>? {condition} GROUP BY {cols} ORDER BY {order}'
  out[str(v)]=[list(r) for r in con.execute(q,(epoch,epoch))]
 return out
def witnesses():
 con=sqlite3.connect(':memory:');con.execute('CREATE TABLE versions(id,zone_id,amount,minute,valid_from,valid_to)');con.execute('CREATE TABLE zones(zone_id,borough)');con.executemany('INSERT INTO zones VALUES(?,?)',[(1,'A'),(2,'B')]);con.executemany('INSERT INTO versions VALUES(?,?,?,?,?,?)',[(1,1,100,0,0,1),(1,1,3000,30,1,999),(2,2,500,0,0,1),(3,1,1500,30,1,999)])
 old,new=direct(con,0),direct(con,1);different=[v for v in old if old[v]!=new[v]];assert len(different)==4
 # Capture epochs equal build starts; half-open resource reservations.
 policies={'partial':[dict(kind='build',start=0,end=2,epoch=0,views=[0]),dict(kind='pub',start=2,end=3,epoch=0,consumer=0),dict(kind='build',start=2,end=11,epoch=2,views=[1]),dict(kind='pub',start=11,end=12,epoch=2,consumer=1)],'all_shared':[dict(kind='build',start=0,end=10,epoch=0,views=[0,1]),dict(kind='pub',start=10,end=11,epoch=0,consumer=0),dict(kind='pub',start=11,end=12,epoch=0,consumer=1)]}
 values={}
 for mode,actions in policies.items():
  for resource in ['build','pub']:
   ends=-1
   for a in actions:
    if a['kind']==resource:assert a['start']>=ends and a['end']>a['start'];ends=a['end']
  state=[-10,-10]
  for a in actions:
   if a['kind']=='pub' and a['end']<=3:state[a['consumer']]=max(state[a['consumer']],a['epoch'])
  values[mode]=dict(build_slots=sum(a['end']-a['start'] for a in actions if a['kind']=='build'),installed_at_sample3=state,consumer0_fresh=int(3-state[0]<=3))
 assert values['partial']['build_slots']==11 and values['all_shared']['build_slots']==10 and values['partial']['consumer0_fresh']==1 and values['all_shared']['consumer0_fresh']==0
 return dict(mixed_epoch=dict(old=old,new=new,differing_views=different,conclusion='An old common prefix cannot be labelled as the requested new epoch without recomputation or a proved correction.'),sharing_barrier=dict(policies=policies,observations=values,scope='Constructed deterministic two-view schedule witness,not an executed SQL timing bound. Capture choices differ by policy. Positive durations and one build/one delivery worker. Less total build work alone does not imply installed freshness.'))
def main():
 R.mkdir(exist_ok=True,parents=True);checks=[]
 def ck(n,ok,e):checks.append(dict(name=n,state='PASS' if ok else 'FAIL',evidence=e));assert ok,(n,e)
 cfg=load(O/'prepared/CONTRACT.json');rows=[json.loads(s) for s in (O/'TRIALS.jsonl').read_text().splitlines()];env=load(O/'COMMAND_ENVIRONMENT_BEFORE_RUN.json')
 mf=load(O/'LOCAL_FILE_MANIFEST.json');ck('raw_manifest',all(sha(O/x['path'])==x['sha256'] for x in mf),len(mf));ck('pre_run_freeze',all(sha(Path(x['path']))==x['sha256'] for x in env['inputs']) and all(sha(O/x['path'])==x['sha256'] for x in env['frozen']),env['seeds']);ck('1350_complete_trials',len(rows)==3*10*5*3*3==load(O/'RESULTS.json')['trials'],len(rows))
 expected={};payload_bytes={}
 for family in cfg['families']:
  con=open_source(ROOT/cfg['cases'][family])
  for epoch in cfg['epochs']:
   oracle=direct(con,epoch);p=load(O/'payloads'/f'{family}_{epoch}.json');ck(f'direct_SQL_{family}_{epoch}',p['rows']==oracle,dict(views=4,source='direct join,without temporary snapshot'))
   expected[family,epoch]={v:hashlib.sha256(pack(x)).hexdigest() for v,x in oracle.items()};payload_bytes[family,epoch]=sum(len(pack(x)) for x in oracle.values())
  con.close()
 for i,r in enumerate(rows):
  family,e,k,mode=r['workload'],r['epoch'],r['k'],r['mode'];n_prefix={'shared_all':1,'shared_per_view':4,'unshared_consumer':4*k}[mode];n_agg=4*k if mode=='unshared_consumer' else 4
  fp=hashlib.sha256(pack([[c,c%4,e,expected[family,e][str(c%4)]] for c in range(4*k)])).hexdigest()
  ck(f'trial_{i}',r['payload_sha256']==expected[family,e] and r['prefix_evaluations']==len(r['calls'])==n_prefix and sum(len(x['views']) for x in r['calls'])==r['aggregate_evaluations']==n_agg and r['charged_application_bytes']==k*payload_bytes[family,e] and r['consumers_installed']==4*k and r['installed_fingerprint']==fp and r['source_cpu_ns']>=0 and all(r[x]>0 for x in ['source_wall_ns','install_wall_ns']),dict(workload=family,epoch=e,k=k,rep=r['rep'],mode=mode))
 summary=[]
 for family in cfg['families']:
  for k in cfg['consumers_per_view']:
   cells={mode:[r for r in rows if r['workload']==family and r['k']==k and r['mode']==mode] for mode in cfg['modes']}
   for mode,rs in cells.items():ck(f'30_timing_observations_{family}_{k}_{mode}',len(rs)==30,dict(dependent_epochs=10,repeats=3))
   matched={}
   for mode in ['shared_per_view','unshared_consumer']:
    ratios=[]
    for r in cells['shared_all']:
     p=next(x for x in cells[mode] if (x['epoch'],x['rep'])==(r['epoch'],r['rep']));ratios.append(p['source_wall_ns']/r['source_wall_ns'])
    matched[mode]=dict(median_paired_source_wall_speedup=statistics.median(ratios),min_speedup=min(ratios),max_speedup=max(ratios))
   summary.append(dict(workload=family,k=k,consumers=4*k,observations_per_mode=30,modes={mode:dict(prefix_evaluations=rs[0]['prefix_evaluations'],aggregate_evaluations=rs[0]['aggregate_evaluations'],median_source_wall_ms=statistics.median(x['source_wall_ns'] for x in rs)/1e6,median_source_cpu_ms=statistics.median(x['source_cpu_ns'] for x in rs)/1e6,median_install_wall_ms=statistics.median(x['install_wall_ns'] for x in rs)/1e6) for mode,rs in cells.items()},matched=matched))
 witness=witnesses();dump(R/'STRUCTURAL_WITNESSES.json',witness)
 ck('mixed_epoch_all4views_differ',len(witness['mixed_epoch']['differing_views'])==4, 'actual SQL insert/update/delete fixture')
 ck('less_build_work_not_service_monotonicity',witness['sharing_barrier']['observations']['partial']['consumer0_fresh']>witness['sharing_barrier']['observations']['all_shared']['consumer0_fresh'],'finite constructed schedule')
 result=dict(state='PASS',marker='AI_SOURCE_SHARING_AUDIT_NOT_EXTERNAL_PEER_REVIEW',checks_count=len(checks),checks=checks,trials=len(rows),source_SQL_payloads_checked=120,actual_consumer_installations=sum(r['consumers_installed'] for r in rows),zero_process_CPU_observations=sum(r['source_cpu_ns']==0 for r in rows),scope=cfg['scope'],summary=summary,input_sha256={str(p):sha(p) for p in [O/'TRIALS.jsonl',O/'LOCAL_FILE_MANIFEST.json',O/'prepared/CONTRACT.json',Path(__file__)]})
 dump(R/'SHARING_AUDIT.json',result);print(json.dumps(dict(state='PASS',checks=len(checks),trials=len(rows),installations=result['actual_consumer_installations'],k2=[x for x in summary if x['k']==2])))
if __name__=='__main__':main()
