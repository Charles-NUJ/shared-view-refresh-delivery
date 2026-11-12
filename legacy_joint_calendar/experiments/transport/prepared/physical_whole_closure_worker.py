"""D01 instrumented matched physical supplement. Actual SQL/TCP/SQLite; bounded pilot only.

The source runs the unchanged ReceiptController classes extracted before launch.
Its clock is local monotonic time. A received ACK is a conservative upper bound
on installation time; no subtraction of clocks on different hosts is used.
"""
from pathlib import Path
import argparse, concurrent.futures, csv, hashlib, json, math, os, platform
import socket, sqlite3, struct, sys, threading, time, traceback

QUERIES = [
    'SELECT zone_id,COUNT(*),SUM(amount) FROM snapshot GROUP BY zone_id ORDER BY zone_id',
    'SELECT borough,COUNT(*),SUM(amount) FROM snapshot GROUP BY borough ORDER BY borough NULLS LAST',
    'SELECT borough,COUNT(*),SUM(amount) FROM snapshot WHERE amount>=2000 GROUP BY borough ORDER BY borough NULLS LAST',
    'SELECT CAST(minute/15 AS INTEGER) AS bucket,borough,COUNT(*),SUM(amount) FROM snapshot GROUP BY bucket,borough ORDER BY bucket,borough NULLS LAST',
]

def pack(x): return json.dumps(x, ensure_ascii=False, separators=(',', ':')).encode()
def digest(b): return hashlib.sha256(b).hexdigest()
def save(p, x):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(x, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
def environment():
    return dict(python=sys.version, sqlite=sqlite3.sqlite_version,
                platform=platform.platform(), hostname=socket.gethostname(),
                pid=os.getpid(), clock='time.perf_counter; source clock only')
def exact(s, n):
    b=bytearray()
    while len(b)<n:
        part=s.recv(n-len(b))
        if not part: raise ConnectionError('peer closed')
        b.extend(part)
    return bytes(b)
def receive(s):
    n=struct.unpack('!I', exact(s, 4))[0]
    if n>16*1024*1024: raise ValueError('frame exceeds pilot bound')
    return json.loads(exact(s, n))
def send(s, x):
    b=pack(x);s.sendall(struct.pack('!I', len(b))+b);return len(b)+4

class SourceSQL:
    def __init__(self, path):
        self.con=sqlite3.connect(':memory:', check_same_thread=False)
        self.con.execute('CREATE TABLE versions(id INTEGER,zone_id INTEGER,amount INTEGER,minute INTEGER,valid_from INTEGER,valid_to INTEGER)')
        self.con.execute('CREATE TABLE zones(zone_id INTEGER,borough TEXT)')
        with (path/'versions.csv').open(encoding='utf-8-sig', newline='') as f:
            self.con.executemany('INSERT INTO versions VALUES(?,?,?,?,?,?)',
                (tuple(int(r[k]) for k in ['id','zone_id','amount','minute','valid_from','valid_to']) for r in csv.DictReader(f)))
        with (path/'zones.csv').open(encoding='utf-8-sig', newline='') as f:
            self.con.executemany('INSERT INTO zones VALUES(?,?)',
                ((int(r['zone_id']), r['borough'] or None) for r in csv.DictReader(f)))
        self.con.commit()
    def build(self, epoch, views, injected_delay_seconds=0.):
        task_start=time.perf_counter()
        if injected_delay_seconds:time.sleep(injected_delay_seconds)
        st=time.perf_counter()
        self.con.execute('DROP TABLE IF EXISTS snapshot')
        self.con.execute('CREATE TEMP TABLE snapshot AS SELECT a.id,a.zone_id,z.borough,a.amount,a.minute FROM versions a JOIN zones z ON a.zone_id=z.zone_id WHERE a.valid_from<=? AND a.valid_to>?', (epoch,epoch))
        outputs={v:pack([list(r) for r in self.con.execute(QUERIES[v])]) for v in views}
        finish=time.perf_counter()
        return outputs, dict(task_start_monotonic=task_start,sql_start_monotonic=st,
            task_finish_monotonic=finish,sql_ms=(finish-st)*1000,
            worker_ms=(finish-task_start)*1000,injected_delay_seconds=injected_delay_seconds)

def receiver(root, cfg):
    out=root/'output';out.mkdir(exist_ok=True)
    save(out/'ENVIRONMENT.json', environment())
    server=socket.socket();server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    server.bind(('0.0.0.0',cfg['port']));server.listen(16);server.settimeout(2)
    save(out/'READY.json', dict(state='LISTENING',port=cfg['port']))
    log=(out/'INSTALLS.jsonl').open('w',encoding='utf-8');dbs={};events=0
    try:
        while True:
            try:conn,_=server.accept()
            except socket.timeout:
                if (out/'STOP').exists():break
                continue
            with conn:
                conn.settimeout(30);m=receive(conn)
                if m.get('operation')=='stop':send(conn,dict(stopped=True));break
                if m.get('operation')=='ping':send(conn,dict(ready=True));continue
                key=m['session'];assert key in cfg['sessions']
                c=int(m['consumer']);v=int(m['view']);e=int(m['epoch']);assert c%4==v
                b=pack(m['rows']);assert digest(b)==m['sha256']
                if key not in dbs:
                    db=sqlite3.connect(out/(key+'.sqlite'))
                    db.execute('CREATE TABLE installed(consumer INTEGER PRIMARY KEY,view_id INTEGER,epoch INTEGER,payload BLOB)')
                    db.execute('CREATE TABLE result_rows(consumer INTEGER,row_no INTEGER,row_json TEXT,PRIMARY KEY(consumer,row_no))')
                    dbs[key]=db
                db=dbs[key];begin=time.perf_counter();fault=m.get('fault','none')
                if fault=='late_install':time.sleep(m['delay_seconds'])
                previous=db.execute('SELECT epoch FROM installed WHERE consumer=?',(c,)).fetchone()
                if fault=='terminal_failure':
                    reply=dict(kind='nack',consumer=c,version=e)
                else:
                    with db:
                        if previous is None or e>=previous[0]:
                            db.execute('INSERT OR REPLACE INTO installed VALUES(?,?,?,?)',(c,v,e,b))
                            db.execute('DELETE FROM result_rows WHERE consumer=?',(c,))
                            db.executemany('INSERT INTO result_rows VALUES(?,?,?)',
                                ((c,i,json.dumps(r,ensure_ascii=False,separators=(',',':'))) for i,r in enumerate(m['rows'])))
                    installed,payload=db.execute('SELECT epoch,payload FROM installed WHERE consumer=?',(c,)).fetchone()
                    assert previous is None or installed>=previous[0]
                    reply=dict(kind='ack',consumer=c,version=installed,installed_sha256=digest(payload))
                reply['receiver_processing_ms']=(time.perf_counter()-begin)*1000
                # The delay is physical, not a simulated receipt queue.
                if fault=='late_ack':time.sleep(m['delay_seconds'])
                else:time.sleep(cfg['ack_delay_seconds'])
                record=dict(message=m,reply=reply,receiver_wall_utc_ns=time.time_ns())
                log.write(json.dumps(record,ensure_ascii=False,separators=(',',':'))+'\n');log.flush()
                send(conn,reply);events+=1
    finally:
        log.close();server.close()
        for db in dbs.values():db.close()
        save(out/'RESULT.json',dict(state='RECEIVER_FINISHED',events=events,sessions=list(dbs),environment=environment()))

def exchange(cfg, message):
    begin=time.perf_counter()
    with socket.create_connection((cfg['host'],cfg['port']),10) as s:
        s.settimeout(30);wire=send(s,message);reply=receive(s)
    return dict(reply=reply,source_roundtrip_ms=(time.perf_counter()-begin)*1000,
                source_receive_monotonic=time.perf_counter(),wire_sent_bytes=wire,
                payload_bytes=len(pack(message['rows'])))

def run_session(root,cfg,key):
    import numpy as np
    from review3_controllers import make_controller
    out=root/'output'/key;out.mkdir(exist_ok=False);(out/'plans').mkdir()
    spec=cfg['sessions'][key];case=root/'input'/spec['workload']
    processing_delay=int(spec.get('ACK_processing_delay_ticks',0))
    if processing_delay<0:raise ValueError('nonnegative application ACK delay required')
    identity=json.loads((case/'IDENTITY.json').read_text());upper=identity['a_priori_bounds']['json_payload_bytes_upper']
    p=cfg['controller_protocol'];tick=cfg['tick_seconds'];ticks=p['ticks']
    factor=dict(mode='bounded',network_scale=1)
    dv=[p['publish_max_attempts']*(max(1,math.ceil(b*2/4096))+p['publish_jitter_ticks']) for b in upper]
    db=3+p['build_jitter_ticks'];period=max(db,sum(dv));offsets=[db+sum(dv[:v+1]) for v in range(4)]
    lags=[period+offsets[c%4]-1+(5 if c>=4 else 0) for c in range(8)]
    assert tick>0 and p['ack_max_delay']>=1
    if 'lags_override' in spec:
        lags=list(spec['lags_override'])
        if len(lags)!=8 or any(type(x) is not int or x<0 for x in lags):raise ValueError('eight nonnegative integer lags required')
    ctrl=make_controller(spec['method'],upper,lags,factor,p,out/'plans')
    from sparse_certificate import sparse_bound
    from types import MethodType
    original_bound=ctrl.bound;bound_backend_checks=[]
    def checked_bound(self,*args):
        sparse=sparse_bound(self,*args);reference=original_bound(*args)
        if sparse!=reference:raise AssertionError('sparse/reference guard mismatch')
        bound_backend_checks.append(dict(tick=args[0],prefix=args[-1],total=sparse['total_miss_upper'],feasible=sparse['feasible']))
        return sparse
    ctrl.bound=MethodType(checked_bound,ctrl)
    plane=SourceSQL(case);initial,initial_timing=plane.build(0,range(4));payloads={(v,0):b for v,b in initial.items()}
    source_events=[dict(kind='build',epoch=0,views=list(range(4)),sql_ms=initial_timing['sql_ms'],timing=initial_timing,initial=True,
                        payload_sha256={str(v):digest(b) for v,b in initial.items()})]
    for c in range(8):
        b=initial[c%4]
        msg=dict(session=key,command_id=-1,consumer=c,view=c%4,epoch=0,rows=json.loads(b),sha256=digest(b),fault='none',attempt=1)
        response=exchange(cfg,msg);assert response['reply']['kind']=='ack'
        source_events.append(dict(kind='initial_install',message=msg,response=response))
    cached=[0]*4;acked=[0]*8;sizes=[len(initial[v]) for v in range(4)]
    frame_log=(out/'FRAMES.jsonl').open('w');command_log=(out/'COMMANDS.jsonl').open('w')
    net_log=[];learning=[];build_log=[];fault_used=False;sql_fault_used=False;timing_violations=[]
    pool=concurrent.futures.ThreadPoolExecutor(max_workers=2)
    compute=None;network=None;origin=time.perf_counter();initial_times={}
    pending_receipts=[];all_responses=[];all_commands=[]

    def publish(job):
        results=[]
        for c in job['recipients']:
            b=payloads[job['view'],job['epoch']]
            for attempt in range(1,p['publish_max_attempts']+1):
                fault=job['fault'] if c==job['fault_consumer'] else 'none'
                # Repeated NACKs violate the declared F-attempt success contract.
                msg=dict(session=key,command_id=job['id'],consumer=c,view=job['view'],epoch=job['epoch'],
                         rows=json.loads(b),sha256=digest(b),fault=fault,attempt=attempt,
                         delay_seconds=(job['reserved_finish']-job['start']+p['ack_max_delay']+2)*tick)
                response=exchange(cfg,msg);results.append(dict(message=msg,response=response))
                if response['reply']['kind']=='ack':break
        return results

    try:
        for t in range(ticks):
            delay=origin+t*tick-time.perf_counter()
            if delay>0:time.sleep(delay)
            actual_tick=(time.perf_counter()-origin)/tick
            if actual_tick>=t+1:ctrl.book.revoke(t,'physical_event_loop_missed_tick')
            labels=[];records=[]
            if compute and spec['factor']=='late_completion_before_poll' and t==compute['reserved_finish'] and compute.get('injected'):
                # Deliberately make a late task complete before its deadline poll.
                # The exact completion timestamp, rather than done(), must decide.
                time.sleep(cfg['boundary_poll_delay_seconds'])
            if compute and compute['future'].done() and t>=compute['reserved_finish']:
                outputs,timing=compute['future'].result()
                deadline=origin+compute['reserved_finish']*tick
                if timing['task_finish_monotonic']>deadline:
                    ctrl.book.revoke(t,'physical_SQL_exact_completion_after_deadline')
                    timing_violations.append(dict(kind='SQL_finish_late',tick=t,command_id=compute['id'],
                        deadline_monotonic=deadline,completion_monotonic=timing['task_finish_monotonic'],
                        future_done_at_poll=True))
                for v,b in outputs.items():
                    old=sizes[v];payloads[v,compute['epoch']]=b;cached[v]=compute['epoch'];sizes[v]=len(b)
                    labels.append(dict(view=v,epoch=compute['epoch'],known_size=old,bytes=len(b)))
                build_log.append(dict(id=compute['id'],epoch=compute['epoch'],views=compute['views'],sql_ms=timing['sql_ms'],
                                      timing=timing,dispatch_monotonic=compute['dispatch_monotonic'],
                                      reserved_deadline_monotonic=deadline,poll_tick=t,
                                      cache_available_monotonic=time.perf_counter(),
                                      injected=compute.get('injected',False),
                                      payload_sha256={str(v):digest(b) for v,b in outputs.items()}))
                compute=None
            if compute and t>=compute['reserved_finish'] and not compute['future'].done():ctrl.book.revoke(t,'physical_SQL_reservation_overrun')
            if network and network['future'].done() and not network.get('read'):
                responses=network['future'].result();network['read']=True;all_responses.extend(responses)
                for rr in responses:
                    message=rr['message'];reply=rr['response']['reply']
                    # Source receive time is an upper bound on the actual remote install.
                    upper_tick=math.ceil((rr['response']['source_receive_monotonic']-origin)/tick)
                    if reply['kind']=='ack':
                        rec=dict(kind='ack',publication_id=network['id'],consumer=message['consumer'],version=reply['version'],
                                 install_tick=upper_tick,due=max(t,upper_tick+processing_delay),source_time_is_install_upper_bound=True,
                                 transport_ACK_upper_tick=upper_tick,application_delay_ticks=processing_delay)
                    elif message['attempt']==p['publish_max_attempts']:
                        rec=dict(kind='nack',publication_id=network['id'])
                    else:continue
                    pending_receipts.append((max(t,rec.get('due',t)),rec))
            records=[r for due,r in pending_receipts if due<=t]
            pending_receipts=[(due,r) for due,r in pending_receipts if due>t]
            for r in records:
                if r['kind']=='ack':acked[r['consumer']]=max(acked[r['consumer']],r['version'])
            if network and network.get('read') and t>=network['reserved_finish']:network=None
            active_compute=None if compute is None else dict(reserved_finish=compute['reserved_finish'],epoch=compute['epoch'],views=compute['views'])
            active_network=None if network is None else dict(reserved_finish=network['reserved_finish'],epoch=network['epoch'],recipients=network['recipients'])
            control_start=time.perf_counter()
            decision=ctrl.observe_and_decide(t,cached,acked,sizes,records,labels,[1]*8,active_compute,active_network)
            control_ms=(time.perf_counter()-control_start)*1000
            if control_ms>tick*1000 or time.perf_counter()-origin>=(t+1)*tick:
                ctrl.book.revoke(t,'physical_controller_startup_overrun')
                # Promises for commands not sent must be rolled back, as in the wrapper repair.
                ids={x['command_id'] for x in decision['commands']}
                for ident in ids:ctrl.book.obligations.pop(ident,None)
                for command in decision['commands']:
                    d=command['action']
                    if d[0]=='pub':
                        promise=(d[2],list(d[3]),t+d[5]-d[4])
                        if promise in ctrl.promises:ctrl.promises.remove(promise)
                decision['canceled_commands']=decision['commands'];decision['commands']=[]
            for command in decision['commands']:
                d=command['action'];ident=command['command_id']
                if d[0]=='pub' and (d[1],d[2]) not in payloads:
                    # A violated SQL contract may leave a periodic recovery send
                    # without its source result. Never issue it or retain its promise.
                    ctrl.book.revoke(t,'physical_payload_unavailable_before_send')
                    ctrl.book.obligations.pop(ident,None)
                    promise=(d[2],list(d[3]),t+d[5]-d[4])
                    if promise in ctrl.promises:ctrl.promises.remove(promise)
                    decision.setdefault('canceled_commands',[]).append(dict(command_id=ident,action=d,
                        reason='source_version_not_available'))
                    ctrl.schedule={};ctrl.fallback=True;ctrl.backup_start=None;ctrl.next_plan=ticks+1
                    continue
                record=dict(tick=t,source_dispatch_monotonic=time.perf_counter(),source_start_offset_ms=(time.perf_counter()-origin-t*tick)*1000,
                            command_id=ident,action=d)
                all_commands.append(record);command_log.write(json.dumps(record)+'\n');command_log.flush()
                if d[0]=='build':
                    assert compute is None and d[1]==t
                    extra=0.
                    if not sql_fault_used and t>=p['warmup_ticks'] and spec['factor'] in ['sql_overrun','late_completion_before_poll']:
                        extra=(d[4]-d[3])*tick+(tick if spec['factor']=='sql_overrun' else cfg['boundary_SQL_lateness_seconds'])
                        sql_fault_used=True
                    dispatch=time.perf_counter()
                    compute=dict(id=ident,epoch=d[1],views=d[2],reserved_finish=t+d[4]-d[3],
                                  dispatch_monotonic=dispatch,injected=bool(extra),
                                  future=pool.submit(plane.build,d[1],d[2],extra))
                else:
                    assert network is None
                    _,v,e,cs,start,finish=d
                    fault='none';fault_consumer=-1
                    if not fault_used and t>=p['warmup_ticks'] and spec['factor'] in ['terminal_failure','late_install','late_ack']:
                        fault=spec['factor'];fault_consumer=cs[0];fault_used=True
                    network=dict(id=ident,view=v,epoch=e,recipients=cs,start=t,reserved_finish=t+finish-start,
                                 fault=fault,fault_consumer=fault_consumer)
                    network['future']=pool.submit(publish,network.copy())
            frame=dict(tick=t,source_elapsed_ms=(time.perf_counter()-origin)*1000,cached=list(cached),acknowledged=list(acked),
                       receipts=records,labels=labels,decision=decision,current_status=ctrl.book.status,
                       conservative_fresh=[int(t-acked[c]<=lags[c]) for c in range(8)],control_ms=control_ms,
                       application_ACK_delay_ticks=processing_delay,pending_receipts_count=len(pending_receipts))
            frame_log.write(json.dumps(frame,separators=(',',':'))+'\n');frame_log.flush()
        pool.shutdown(wait=True)
        if compute:
            outputs,timing=compute['future'].result()
            build_log.append(dict(id=compute['id'],epoch=compute['epoch'],views=compute['views'],
                sql_ms=timing['sql_ms'],timing=timing,dispatch_monotonic=compute['dispatch_monotonic'],
                reserved_deadline_monotonic=origin+compute['reserved_finish']*tick,
                outside_observed_window=True,injected=compute.get('injected',False),
                payload_sha256={str(v):digest(b) for v,b in outputs.items()}))
        # Preserve even operations completed after the observation window.
        if network and not network.get('read'):
            for rr in network['future'].result():rr['outside_observed_window']=True;all_responses.append(rr)
        save(out/'RESULT.json',dict(state='SOURCE_SESSION_FINISHED',session=key,workload=spec['workload'],factor=spec['factor'],
            method=spec['method'],repetition=spec.get('repetition'),lag_profile=spec['lag_profile'],certificate_backend='D01-SEC_checked_against_reference',bound_backend_checks=bound_backend_checks,ACK_processing_delay_ticks=processing_delay,execution_scope=cfg['scope'],tick_seconds=tick,ticks=ticks,lags=lags,upper_bytes=upper,source_clock_origin=origin,
            initialization=source_events,builds=build_log,transfers=all_responses,
            final_original_window_status=ctrl.book.status,transitions=ctrl.book.transitions,reasons=ctrl.book.reasons,
            fault_injected=fault_used or sql_fault_used,SQL_timing_violations=timing_violations,models=ctrl.plans,certificates=ctrl.certificates,learning=ctrl.learning,
            final_acknowledged=acked,command_count=len(all_commands),observed_window=p['warmup_ticks'],
            elapsed_seconds=time.perf_counter()-origin,
            freshness_scope='ACK-proven lower bound on installed freshness at source tick boundaries; source ACK receive time bounds installation; no synchronized-clock assumption',
            note='Actual closed-loop run with declared application ACK processing delay. Existing controller classes unchanged. One input seed; no native-engine or production timing claim.'))
    finally:
        pool.shutdown(wait=True);frame_log.close();command_log.close();plane.con.close()

def source(root,cfg):
    save(root/'output/ENVIRONMENT.json',environment())
    for key in cfg['sessions']:
        print(json.dumps(dict(stage='SESSION_START',session=key)),flush=True)
        run_session(root,cfg,key)
        print(json.dumps(dict(stage='SESSION_FINISHED',session=key)),flush=True)
    with socket.create_connection((cfg['host'],cfg['port']),10) as s:
        send(s,dict(operation='stop'));assert receive(s)['stopped']
    save(root/'output/RESULT.json',dict(state='SOURCE_FINISHED',sessions=list(cfg['sessions'])))

def main():
    ap=argparse.ArgumentParser();ap.add_argument('role',choices=['source','receiver']);ap.add_argument('--run-dir',required=True);a=ap.parse_args()
    root=Path(a.run_dir);cfg=json.loads((root/'input/config.json').read_text());(root/'output').mkdir(exist_ok=True)
    try:
        (source if a.role=='source' else receiver)(root,cfg)
    except Exception as e:
        save(root/'output/FAILURE.json',dict(error_class=type(e).__name__,traceback=traceback.format_exc()))
        raise
if __name__=='__main__':main()
