import sys,os,json,hashlib,time,tempfile
from pathlib import Path
os.umask(0o077)
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=True,mode=0o700)
status=Path(sys.argv[2]);inp=sys.stdin.buffer;count=total=0;started=time.time()
def exact(n):
 b=inp.read(n)
 if len(b)!=n:raise EOFError('incomplete stream')
 return b
def progress(done=False):
 v={'stage':'complete' if done else 'streaming','files':count,'bytes':total,'started':started,'at':time.time()}
 t=status.with_suffix('.tmp');t.write_text(json.dumps(v));t.replace(status)
progress.last=time.time()
progress()
while True:
 line=inp.readline()
 if not line:raise EOFError('missing completion marker')
 h=json.loads(line)
 if h.get('done'):progress(True);break
 rel=Path(h['path'])
 if rel.is_absolute() or '..' in rel.parts:raise ValueError('invalid path')
 dst=root/rel;dst.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
 if not dst.parent.resolve().is_relative_to(root) or dst.is_symlink():raise ValueError('unsafe destination')
 fd,tmp=tempfile.mkstemp(prefix='.wx-transfer-',dir=dst.parent)
 try:
  sha=hashlib.sha256();left=h['bytes']
  with os.fdopen(fd,'wb') as f:
   while left:
    b=exact(min(left,1024*1024));f.write(b);sha.update(b);left-=len(b)
  footer=json.loads(inp.readline())
  if not footer['stable'] or footer['sha256']!=sha.hexdigest():raise ValueError('source changed or hash mismatch')
  os.utime(tmp,ns=(h['mtime_ns'],h['mtime_ns']));os.replace(tmp,dst)
  count+=1;total+=h['bytes']
  if count%100==0 or time.time()-progress.last>2:progress();progress.last=time.time()
 finally:
  if os.path.exists(tmp):os.unlink(tmp)
progress.last=time.time()
