from pathlib import Path
import base64,hashlib,json,sqlite3,statistics
ROOT=Path(__file__).resolve().parents[1];O=ROOT/'raw_results/joint_tcp_20261019';R=ROOT/'reviews/joint_calendar_20261019'
from audit_origin_sharing import open_source,direct,pack
from audit_bundle_calendar import legality,counts,required
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def lines(p):return [json.loads(x) for x in p.read_text().splitlines() if x.strip()]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 checks=[];sources={};oracles={};sessions=[];history_n=payload_n=states_n=0;times=[];receiver=lines(O/'RECEIVER_EVENTS.jsonl')
 def ck(n,v,e):checks.append(dict(name=n,state='PASS' if v else 'FAIL',evidence=e));assert v,(n,e)
 def oracle(seed,e):
  if seed not in sources:sources[seed]=open_source(ROOT/f'raw_results/m1_main_20261014/inputs/cases/synthetic_{seed}')
  if (seed,e) not in oracles:oracles[seed,e]={int(v):pack(x) for v,x in direct(sources[seed],e).items()}
  return oracles[seed,e]
 ck('raw_hashes',all(sha(O/x['path'])==x['sha256'] for x in load(O/'LOCAL_FILE_MANIFEST.json')),sha(O/'LOCAL_FILE_MANIFEST.json'))
 env=load(O/'ENVIRONMENT_BEFORE_RUN.json');ck('frozen_inputs',all(sha(Path(x['path']))==x['sha256'] for x in env['inputs']) and all(sha(O/x['path'])==x['sha256'] for x in env['frozen']),len(env['inputs']))
 for spec in load(O/'prepared/CONTRACT.json')['sessions']:
  key=spec['key'];p=spec['configuration'];f=O/'runs'/key;result=load(f/'RESULT.json');events=lines(f/'EVENTS.jsonl');frames=lines(f/'STATES.jsonl');source=lines(f/'SOURCE.jsonl');seed=spec['seed'];expected_history=[];epoch=[0]*8;blobs=[oracle(seed,0)[v] for v in p['views']];charged=0;bounds=[0]*8
  ck('initial_contract_'+key,legality(p,spec['plan']) and min(counts(p,spec['plan']))>=required(p),required(p))
  for s in source:
   for v,b64 in s['outputs'].items():
    b=base64.b64decode(b64);payload_n+=1;ck('source_'+key+'_'+str(s['epoch'])+'_'+v,b==oracle(seed,s['epoch'])[int(v)] and len(b)<=p['upper'][int(v)],len(b))
  transport=[x for x in events if x['kind']=='transport'];server=[x for x in receiver if x['session']==key]
  ck('separate_process_log_'+key,len(server)==len(transport) and all((x['consumer'],x['epoch'],x['nominal_tick'],x['bytes'],x['reply'])==(y['consumer'],y['epoch'],y['tick'],y['bytes'],y['reply']) for x,y in zip(server,transport)),len(server))
  for frame in frames:
   t=frame['tick']
   for x in transport:
    if x['tick']!=t:continue
    c=x['consumer'];e=x['epoch'];b=oracle(seed,e)[p['views'][c]];charged+=len(b);times.append(x['round_trip_ms'])
    if x['reply']['status']=='ACK' and e>=epoch[c]:
     epoch[c]=e;blobs[c]=b;expected_history.append((c,t,e,b))
    ck('postcommit_ACK_'+key+'_'+str(t)+'_'+str(c),x['bytes']==len(b) and x['reply']['installed_epoch']==epoch[c] and x['reply']['digest']==hashlib.sha256(blobs[c]).hexdigest(),x['reply']['status'])
   ck('state_'+key+'_'+str(t),frame['epochs']==epoch,epoch);states_n+=1
   if p['warm']<=t<p['T']:
    for c,e in enumerate(epoch):bounds[c]+=int(t-e<=p['lags'][c])
  db=sqlite3.connect('file:'+str(f/'receiver.sqlite').replace('\\','/')+'?mode=ro',uri=True);ck('db_quick_check_'+key,db.execute('PRAGMA quick_check').fetchone()[0]=='ok','persistent DB reopened');history=db.execute('SELECT consumer,tick,epoch,payload FROM history ORDER BY seq').fetchall();installed=db.execute('SELECT consumer,epoch,payload FROM installed ORDER BY consumer').fetchall();db.close()
  ck('persistent_history_'+key,history==expected_history and installed==[(c,epoch[c],blobs[c]) for c in range(8)],len(history));history_n+=len(history)
  revoke=[x['tick'] for x in events if x['kind']=='revoke']
  ck('observed_counts_and_charge_'+key,bounds==result['counts'] and charged==result['payload_bytes'],dict(counts=bounds,bytes=charged))
  if spec['fault']=='none':ck('bounded_'+key,not revoke and result['status']=='CERTIFIED' and min(bounds)>=44,44)
  else:ck('fault_withdrawal_'+key,len(revoke)==1 and result['status']=='REVOKED' and not any(x['kind']=='start' and x['tick']>=revoke[0] for x in events),revoke)
  sessions.append(result)
 pairs=[]
 for case in sorted({x['case'] for x in sessions if x['fault']=='none'}):
  for seed in [7101,7102,7103]:
   rows={x['method']:x for x in sessions if x['case']==case and x['seed']==seed and x['fault']=='none'}
   ck('all_reductions_charge_no_more_than_base_'+case+'_'+str(seed),all(rows[m]['payload_bytes']<=rows['Base']['payload_bytes'] for m in ['CRP','JCR']),[rows[m]['payload_bytes'] for m in ['Base','CRP','JCR']]);pairs.append(dict(case=case,seed=seed,bytes={m:rows[m]['payload_bytes'] for m in ['Base','CRP','JCR']}))
 result=dict(state='PASS',marker='AI_INDEPENDENT_TCP_SQL_DB_REOPEN_NOT_PRODUCTION_SLA',checks=checks,checks_count=len(checks),sessions=len(sessions),bounded_sessions=sum(x['fault']=='none' for x in sessions),fault_sessions=sum(x['fault']!='none' for x in sessions),source_payloads=payload_n,persistent_history_rows=history_n,frame_states=states_n,unique_SQL_snapshot_oracles=len(oracles),transport_calls=len(times),round_trip_ms=dict(median=statistics.median(times),max=max(times)),pairs=pairs,scope='Real loopback TCP/SQL/SQLite,nominal logical service ticks;no timing-to-wallclock mapping or interhost guarantee')
 (R/'TCP_AUDIT.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k not in ['checks','pairs']}))
if __name__=='__main__':main()
