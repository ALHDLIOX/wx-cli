import os,json,subprocess,time,hashlib,sys,gzip
from pathlib import Path
os.umask(0o077)
r=Path(__file__).parent;config=json.loads((r/'state.json').read_text());src=Path(config['account_root'])/'msg';dest='/pool-safe/wechat-vault/media/msg';started=time.time()
scan="""import os,json,stat,gzip,sys
writer=gzip.GzipFile(fileobj=sys.stdout.buffer,mode='wb')
from pathlib import Path
root=Path('/pool-safe/wechat-vault/media/msg')
for parent,dirs,files in os.walk(root):
 for name in files:
  p=Path(parent)/name
  try:
   s=p.lstat()
   if stat.S_ISREG(s.st_mode):writer.write((json.dumps([str(p.relative_to(root)),s.st_size,int(s.st_mtime)],ensure_ascii=False)+'\\n').encode())
  except FileNotFoundError:pass
writer.close()
"""
(r/'fast-status.json').write_text(json.dumps({'stage':'comparing_files','at':time.time()}))
remote=subprocess.run(['ssh','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','ConnectTimeout=15','NAS','python3 -'],input=scan.encode(),capture_output=True,check=True)
existing={a[0]:(a[1],a[2]) for a in map(json.loads,gzip.decompress(remote.stdout).decode().splitlines())};pending=[];size=0;allsize=0;allcount=0
for parent,dirs,files in os.walk(src):
 for name in files:
  p=Path(parent)/name
  if p.is_symlink():continue
  try:s=p.stat()
  except FileNotFoundError:continue
  rel=str(p.relative_to(src));allsize+=s.st_size;allcount+=1
  if existing.get(rel)!=(s.st_size,int(s.st_mtime)):pending.append(rel);size+=s.st_size
(r/'fast-plan.json').write_text(json.dumps({'pending_files':len(pending),'pending_bytes':size,'source_files':allcount,'source_bytes':allsize,'at':time.time()}))
print(json.dumps({'pending_files':len(pending),'pending_bytes':size}),flush=True)
(r/'fast-pending.json').write_text(json.dumps(pending))
from concurrent.futures import ThreadPoolExecutor
workers=4
(r/'fast-status.json').write_text(json.dumps({'stage':'streaming','workers':workers,'at':time.time()}))
def transfer(worker):
 with (r/('fast-transfer-'+str(worker)+'.log')).open('a') as log:
  proc=subprocess.Popen(['ssh','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','ConnectTimeout=15','NAS','python3 /pool-safe/wechat-vault/tools/stream_receiver.py /pool-safe/wechat-vault/media/msg /pool-safe/wechat-vault/stream-status-'+str(worker)+'.json'],stdin=subprocess.PIPE,stdout=log,stderr=log)
  try:
   out=proc.stdin
   for rel in pending[worker::workers]:
    with (src/rel).open('rb') as f:
     st=os.fstat(f.fileno());out.write((json.dumps({'path':rel,'bytes':st.st_size,'mtime_ns':st.st_mtime_ns})+'\n').encode());sha=hashlib.sha256();left=st.st_size
     while left:
      b=f.read(min(left,1024*1024))
      if not b:raise EOFError('source truncated')
      out.write(b);sha.update(b);left-=len(b)
     after=os.fstat(f.fileno());stable=(st.st_size,st.st_mtime_ns)==(after.st_size,after.st_mtime_ns)
     out.write((json.dumps({'sha256':sha.hexdigest(),'stable':stable})+'\n').encode())
   out.write(b'{"done":true}\n');out.close();assert proc.wait()==0
  except BaseException:
   try:proc.stdin.close()
   except Exception:pass
   proc.terminate();proc.wait();raise
with ThreadPoolExecutor(max_workers=workers) as pool:list(pool.map(transfer,range(workers)))
subprocess.run(['ssh','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','ConnectTimeout=15','NAS','umask 077; touch /pool-safe/wechat-vault/media-transfer-complete'],check=True)
(r/'sync-complete.json').write_text(json.dumps({'ok':True,'at':time.time(),'method':'sha256_atomic_stream','files':len(pending),'bytes':size}))
(r/'fast-status.json').write_text(json.dumps({'stage':'complete','at':time.time()}));print('COMPLETE',flush=True)
