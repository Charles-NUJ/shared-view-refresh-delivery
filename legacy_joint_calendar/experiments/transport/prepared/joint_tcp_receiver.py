from pathlib import Path
import argparse,base64,hashlib,json,socket,sqlite3,struct,time
def read(s,n):
 b=b''
 while len(b)<n:
  v=s.recv(n-len(b))
  if not v:raise EOFError()
  b+=v
 return b
def receive(s):return json.loads(read(s,struct.unpack('!I',read(s,4))[0]))
def send(s,x):
 b=json.dumps(x,separators=(',',':')).encode();s.sendall(struct.pack('!I',len(b))+b)
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--out',required=True);a=ap.parse_args();O=Path(a.out);server=socket.socket();server.bind(('127.0.0.1',0));server.listen();(O/'SERVER_READY.json').write_text(json.dumps(dict(port=server.getsockname()[1],host='127.0.0.1',clock='perf_counter same host')))
 log=(O/'RECEIVER_EVENTS.jsonl').open('w');db=None
 with server:
  conn,_=server.accept()
  with conn:
   while True:
    msg=receive(conn);op=msg['op'];begin=time.perf_counter()
    if op=='stop':
     if db is not None:db.close()
     send(conn,dict(status='STOPPED'));break
    if op=='init':
     if db is not None:db.close()
     db=sqlite3.connect(O/'runs'/msg['session']/'receiver.sqlite');db.execute('CREATE TABLE installed(consumer INTEGER PRIMARY KEY,epoch INTEGER,payload BLOB)');db.execute('CREATE TABLE history(seq INTEGER PRIMARY KEY,consumer INTEGER,tick INTEGER,epoch INTEGER,payload BLOB)')
     with db:db.executemany('INSERT INTO installed VALUES(?,?,?)',[(i,0,base64.b64decode(b)) for i,b in enumerate(msg['bootstrap'])])
     send(conn,dict(status='READY'));continue
    c=msg['consumer'];e=msg['epoch'];blob=base64.b64decode(msg['payload']);status='NACK' if msg.get('fail_before_commit') else 'ACK'
    if status=='ACK':
     old=db.execute('SELECT epoch FROM installed WHERE consumer=?',(c,)).fetchone()[0]
     if e>=old:
      with db:
       db.execute('UPDATE installed SET epoch=?,payload=? WHERE consumer=?',(e,blob,c));db.execute('INSERT INTO history(consumer,tick,epoch,payload) VALUES(?,?,?,?)',(c,msg['nominal_tick'],e,blob))
    stored,actual=db.execute('SELECT epoch,payload FROM installed WHERE consumer=?',(c,)).fetchone();end=time.perf_counter()
    reply=dict(status=status,installed_epoch=stored,digest=hashlib.sha256(actual).hexdigest(),receiver_commit_monotonic=end,receiver_ms=(end-begin)*1000)
    log.write(json.dumps(dict(session=msg['session'],consumer=c,epoch=e,nominal_tick=msg['nominal_tick'],bytes=len(blob),reply=reply))+'\n');log.flush();send(conn,reply)
 log.close()
if __name__=='__main__':main()
