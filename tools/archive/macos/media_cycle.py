import fcntl,subprocess,sys,json,time,os
from pathlib import Path
os.umask(0o077);r=Path(__file__).parent;lock=(r/'media-cycle.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
ssh=['ssh','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','NAS']
def remote(command,material=None):
 return subprocess.run(ssh+[command],input=material,capture_output=True,timeout=14400,check=True)
try:
 subprocess.run([sys.executable,str(r/'fast_sync.py')],check=True,timeout=14400)
 remote('python3 /pool-safe/wechat-vault/tools/index_media.py --media /pool-safe/wechat-vault/media --db /pool-safe/wechat-vault/structured/chat-archive.sqlite3')
 remote('python3 /pool-safe/wechat-vault/tools/link_media.py --archive /pool-safe/wechat-vault/structured/chat-archive.sqlite3 --output /pool-safe/wechat-vault/media-links.sqlite3')
 # Existing one-time bulk decoders take priority; do not race their manifests.
 running=remote("pgrep -fc '^python3 /pool-safe/wechat-vault/tools/(decode_media|convert_wxgf).py' || true")
 if int(running.stdout.strip() or b'0')==0:
  material=(r/'image-material.json').read_bytes()
  for script in ['decode_media','convert_wxgf']:
   remote('python3 /pool-safe/wechat-vault/tools/'+script+'.py --root /pool-safe/wechat-vault/media/msg --output /pool-safe/wechat-vault/decoded --manifest /pool-safe/wechat-vault/decoded-media.sqlite3',material)
 status={'stage':'ok','at':time.time()}
except Exception as e:status={'stage':'error','error_type':type(e).__name__,'at':time.time()}
(r/'media-cycle-status.json').write_text(json.dumps(status))
