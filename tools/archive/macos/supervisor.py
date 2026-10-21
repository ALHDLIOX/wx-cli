#!/usr/bin/env python3
"""One persistent process for Mac-side archive access; no per-minute Python launches."""
import fcntl,json,os,runpy,sys,threading,time
from pathlib import Path
root=Path.home()/'.wx-cli/nas-archive'
os.umask(0o077)
lock=(root/'supervisor.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
config=json.loads((root/'supervisor-config.json').read_text())
sys.path.insert(0,str(Path(config['repository'])/'tools/archive'))
import sync_incremental,recent_media
# Run the existing media orchestrator and its local scanner in this interpreter.
# SSH workers remain separate, but they do not read the Mac WeChat container.
import subprocess
original_run=subprocess.run
def run(*args,**kwargs):
 command=args[0] if args else kwargs.get('args')
 if isinstance(command,list) and len(command)==2 and command[1]==str(root/'fast_sync.py'):
  runpy.run_path(command[1],run_name='__wxcli_media_scan__')
  return subprocess.CompletedProcess(command,0)
 return original_run(*args,**kwargs)
subprocess.run=run

def repeat(name,interval,action):
 while True:
  start=time.time()
  try:
   result=action() or {};result.update(stage='ok',checked_at=time.time())
  except BaseException as e:
   result={'stage':'error','error_type':type(e).__name__,'checked_at':time.time()}
  result['duration_seconds']=time.time()-start
  (root/(name+'-supervisor-status.json')).write_text(json.dumps(result))
  if name=='incremental':(root/'incremental-status.json').write_text(json.dumps(result))
  time.sleep(max(5,interval-(time.time()-start)))
for name,interval,action in [
 ('incremental',60,lambda:sync_incremental.cycle(root)),
 ('recent-media',60,lambda:recent_media.main(root)),
 ('media',900,lambda:runpy.run_path(str(root/'media_cycle.py'),run_name='__wxcli_media_cycle__') and {})]:
 threading.Thread(target=repeat,args=(name,interval,action),daemon=True).start()
(root/'supervisor-process.json').write_text(json.dumps({'pid':os.getpid(),'started':time.time()}))
while True:time.sleep(60)
