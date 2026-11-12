from pathlib import Path
import argparse,base64,hashlib,json,platform,shutil,socket,struct,subprocess,sys,time
ROOT=Path(__file__).resolve().parents[3];RUN='joint_tcp_20261019'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.write_text(json.dumps(x,indent=2)+'\n')
def recv(s,n):
 b=b''
 while len(b)<n:
  v=s.recv(n-len(b))
  if not v:raise EOFError()
  b+=v
 return b
def rpc(s,x):
 b=json.dumps(x,separators=(',',':')).encode();start=time.perf_counter();s.sendall(struct.pack('!I',len(b))+b);n=struct.unpack('!I',recv(s,4))[0];r=json.loads(recv(s,n));return r,(time.perf_counter()-start)*1000,len(b)+4
def worker(O):
 sys.path.insert(0,str(O/'prepared'));from physical_whole_closure_worker import SourceSQL
 config=json.loads((O/'prepared/CONTRACT.json').read_text());command=[sys.executable,'-I','-B','-X','utf8',str(O/'prepared/joint_tcp_receiver.py'),'--out',str(O)]
 rows=[];so=(O/'receiver_stdout.log').open('wb');se=(O/'receiver_stderr.log').open('wb');child=subprocess.Popen(command,stdout=so,stderr=se,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
 try:
  until=time.perf_counter()+10
  while not (O/'SERVER_READY.json').exists():
   if child.poll() is not None or time.perf_counter()>until:raise RuntimeError('receiver startup failed')
   time.sleep(.01)
  ready=json.loads((O/'SERVER_READY.json').read_text());sock=socket.create_connection(('127.0.0.1',ready['port']),timeout=10);sock.settimeout(10)
  with sock:
   for spec in config['sessions']:
    folder=O/'runs'/spec['key'];folder.mkdir();p=spec['configuration'];plan=spec['plan'];source=SourceSQL(ROOT/f'raw_results/m1_main_20261014/inputs/cases/synthetic_{spec["seed"]}');initial,_=source.build(0,list(range(4)))
    bootstrap=[base64.b64encode(initial[v]).decode() for v in p['views']];reply,_,_=rpc(sock,dict(op='init',session=spec['key'],bootstrap=bootstrap));assert reply['status']=='READY'
    source_log=(folder/'SOURCE.jsonl').open('w');events=[];states=[];cache={};build_pending=[];pub_pending=[];receipts=[];promises={};processed=set();epochs=list(p['initial']);charged=wire=0;status='CERTIFIED';fault_done=False;counts=[0]*8
    for t in range(p['T']+7):
     for rr in list(receipts):
      if rr['process_tick']==t:
       receipts.remove(rr);processed.add(rr['id']);events.append(dict(kind='receipt_processed',tick=t,receipt=rr))
       if rr['reply']['status']=='NACK' and status!='REVOKED':status='REVOKED';events.append(dict(kind='revoke',tick=t,reason='NACK'))
     if status!='REVOKED' and any(t>deadline and id not in processed for id,deadline in promises.items()):
      status='REVOKED';events.append(dict(kind='revoke',tick=t,reason='receipt_bound'))
     for b in list(build_pending):
      if b['end']==t:
       cache.update(b['outputs']);build_pending.remove(b);events.append(dict(kind='build_ready',tick=t,epoch=b['epoch'],views=b['views']))
     for pending in list(pub_pending):
      q=pending['command']
      if q['end']!=t:continue
      pub_pending.remove(pending)
      for c in q['recipients']:
       blob=pending['blob'];id=str(q['start'])+'_'+str(c);fault=spec['fault']!='none' and not fault_done;fault_done |= fault
       msg=dict(op='install',session=spec['key'],consumer=c,epoch=q['epoch'],nominal_tick=t,payload=base64.b64encode(blob).decode(),fail_before_commit=fault and spec['fault']=='NACK')
       response,elapsed,bytes_wire=rpc(sock,msg);charged+=len(blob);wire+=bytes_wire
       if response['status']=='ACK':epochs[c]=response['installed_epoch']
       process=t+(5 if fault and spec['fault']=='ACK_HOLD' else 1);record=dict(id=id,consumer=c,epoch=q['epoch'],reply=response,process_tick=process,install_tick=t);receipts.append(record);promises[id]=t+2
       events.append(dict(kind='transport',tick=t,consumer=c,epoch=q['epoch'],bytes=len(blob),wire_request_bytes=bytes_wire,round_trip_ms=elapsed,reply=response,receipt_process_tick=process,fail_before_commit=msg['fail_before_commit']))
     if t<p['T'] and status!='REVOKED':
      for q in plan:
       if q['start']!=t:continue
       events.append(dict(kind='start',tick=t,command=q))
       if q['kind']=='build':
        outputs,timing=source.build(q['epoch'],q['views']);assert all(len(b)<=p['upper'][v] for v,b in outputs.items())
        source_log.write(json.dumps(dict(epoch=q['epoch'],views=q['views'],timing=timing,outputs={str(v):base64.b64encode(b).decode() for v,b in outputs.items()}))+'\n')
        build_pending.append(dict(q,outputs={(v,q['epoch']):b for v,b in outputs.items()}))
       else:pub_pending.append(dict(command=q,blob=cache[(q['view'],q['epoch'])]))
     if p['warm']<=t<p['T']:
      for c,e in enumerate(epochs):counts[c]+=int(t-e<=p['lags'][c])
     states.append(dict(tick=t,epochs=list(epochs),status=status))
    source_log.close();source.con.close()
    assert not build_pending and not pub_pending
    for filename,data in [('EVENTS.jsonl',events),('STATES.jsonl',states)]:
     (folder/filename).write_text(''.join(json.dumps(x)+'\n' for x in data))
    result=dict(key=spec['key'],case=spec['case'],method=spec['method'],seed=spec['seed'],fault=spec['fault'],status=status,min_nominal_fresh=min(counts)/(p['T']-p['warm']),counts=counts,payload_bytes=charged,wire_request_bytes=wire,transport_calls=sum(x['kind']=='transport' for x in events))
    assert (status=='CERTIFIED' and min(counts)>=44) if spec['fault']=='none' else status=='REVOKED'
    dump(folder/'RESULT.json',result);rows.append(result)
   rpc(sock,dict(op='stop'))
  child.wait(timeout=10);assert child.returncode==0
 finally:
  if child.poll() is None:child.terminate();child.wait(timeout=10)
  so.close();se.close()
 dump(O/'RESULTS.json',dict(state='FINISHED_AWAITING_INDEPENDENT_AUDIT',sessions=rows,scope=config['scope']))
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--worker',action='store_true');a=ap.parse_args();O=ROOT/'raw_results'/RUN
 if a.worker:worker(O);return
 O.mkdir();(O/'prepared').mkdir();(O/'runs').mkdir();sessions=[];records=[json.loads(f.read_text()) for f in sorted((ROOT/'raw_results/joint_calendar_20261019/models').glob('u*/RESULT.json'))]
 aliases=['balanced_uniform_FullOnly','balanced_split_FullOnly','skewed_uniform_ViewOnly','skewed_split_FullOnly']
 for alias in aliases:
  q=next(x for x in records if alias in x['aliases'])
  for method in ['Base','CRP','JCR']:
   plan=q['initial'] if method=='Base' else q[method]['plan']
   for seed in [7101,7102,7103]:sessions.append(dict(key=alias+'_'+method+'_'+str(seed),case=alias,method=method,seed=seed,fault='none',configuration=q['configuration'],plan=plan))
 q=next(x for x in records if 'skewed_split_FullOnly' in x['aliases'])
 for fault in ['NACK','ACK_HOLD']:
  for seed in [7101,7102,7103]:sessions.append(dict(key='fault_'+fault+'_'+str(seed),case='skewed_split_FullOnly',method='JCR',seed=seed,fault=fault,configuration=q['configuration'],plan=q['JCR']['plan']))
 scope='Two actual local processes,source SQL,loopback TCP,post-commit ACK and persistent SQLite. Command start/end and service samples use nominal logical labels without a wall-time bound. 36bounded sessions (4selected calendars*3methods*3existing related mutation traces) plus6fault sessions. Not interhost scale,native comparison,new dataset seeds,or physical freshness SLA.'
 dump(O/'prepared/CONTRACT.json',dict(sessions=sessions,selection='Four profiles declared in design before transport execution;not selected as the largest JCR gains',scope=scope))
 for n in ['run_joint_tcp.py','joint_tcp_receiver.py','physical_whole_closure_worker.py']:shutil.copyfile(ROOT/'src'/n,O/'prepared'/n)
 inputs=[ROOT/f'raw_results/m1_main_20261014/inputs/cases/synthetic_{seed}'/f for seed in [7101,7102,7103] for f in ['versions.csv','zones.csv','IDENTITY.json']]
 command=[sys.executable,'-I','-B','-X','utf8',str(O/'prepared/run_joint_tcp.py'),'--worker'];dump(O/'ENVIRONMENT_BEFORE_RUN.json',dict(command=command,python=sys.version,platform=platform.platform(),inputs=[dict(path=str(p),sha256=sha(p)) for p in inputs],frozen=[dict(path=str(p.relative_to(O)),sha256=sha(p)) for p in (O/'prepared').iterdir()]))
 with (O/'stdout.log').open('wb') as so,(O/'stderr.log').open('wb') as se:r=subprocess.run(command,cwd=ROOT,stdout=so,stderr=se,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
 dump(O/'RECEIPT.json',dict(exit_code=r.returncode));dump(O/'LOCAL_FILE_MANIFEST.json',[dict(path=str(p.relative_to(O)),bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(O.rglob('*')) if p.is_file()])
 print(json.dumps(dict(exit_code=r.returncode,run_id=RUN)))
 if r.returncode:print((O/'stderr.log').read_text()[-2000:]);raise SystemExit(r.returncode)
if __name__=='__main__':main()
