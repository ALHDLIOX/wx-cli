#!/usr/bin/env python3
"""Prioritize recently referenced local attachments independently of a bulk scan."""
import fcntl,hashlib,json,os,sqlite3,subprocess,sys,time
from pathlib import Path
from sync_incremental import source_class

def main(root):
 os.umask(0o077);lock=(root/'recent-media.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 state=json.loads((root/'state.json').read_text());cfg=json.loads((root.parent/'config.json').read_text());keys=json.loads((root.parent/'all_keys.json').read_text());rel='message/message_resource.db';k=keys[rel];src=source_class()(root.parent/'setup/libsqlcipher.dylib',Path(cfg['db_dir'])/rel,k if isinstance(k,str) else k['enc_key'])
 import re
 try:
  names=dict(src.rows('SELECT rowid,user_name FROM ChatName2Id'));refs=src.rows('SELECT chat_id,hex(packed_info) FROM MessageResourceInfo WHERE message_create_time>'+str(int(time.time()-86400)))
 finally:src.close()
 account=Path(state['account_root'])/'msg';paths=set()
 for chat,raw in refs:
  if chat not in names:continue
  folder=account/'attach'/hashlib.md5(names[chat].encode()).hexdigest()
  for token in set(re.findall(b'[0-9a-fA-F]{32}',bytes.fromhex(raw or ''))):
   for p in folder.glob('*/Img/'+token.decode().lower()+'*.dat'):
    if p.is_file() and not p.is_symlink():paths.add(p)
 db=sqlite3.connect(root/'recent-media.sqlite3');db.execute('CREATE TABLE IF NOT EXISTS sent(path TEXT PRIMARY KEY,size INTEGER,mtime INTEGER)');pending=[]
 for p in paths:
  st=p.stat();rel=str(p.relative_to(account));old=db.execute('SELECT size,mtime FROM sent WHERE path=?',(rel,)).fetchone()
  if old!=(st.st_size,st.st_mtime_ns):pending.append((p,rel,st))
 pending=sorted(pending,key=lambda item:item[2].st_mtime_ns,reverse=True)[:20]
 ssh=['ssh','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','NAS']
 if pending:
  proc=subprocess.Popen(ssh+['python3 /pool-safe/wechat-vault/tools/stream_receiver.py /pool-safe/wechat-vault/media/msg /pool-safe/wechat-vault/recent-media-transfer.json'],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  try:
   for p,rel,st in pending:
    proc.stdin.write((json.dumps({'path':rel,'bytes':st.st_size,'mtime_ns':st.st_mtime_ns})+'\n').encode());h=hashlib.sha256()
    with p.open('rb') as f:
     while chunk:=f.read(1024*1024):proc.stdin.write(chunk);h.update(chunk)
    after=p.stat();proc.stdin.write((json.dumps({'sha256':h.hexdigest(),'stable':(st.st_size,st.st_mtime_ns)==(after.st_size,after.st_mtime_ns)})+'\n').encode())
   proc.stdin.write(b'{"done":true}\n');proc.stdin.close()
   if proc.wait(timeout=300):raise RuntimeError('transfer_failed')
  finally:
   if proc.poll() is None:proc.kill();proc.wait()
  material=json.loads((root/'image-material.json').read_text());material['paths']=[rel for p,rel,st in pending]
  result=subprocess.run(ssh+['python3 /pool-safe/wechat-vault/tools/decode_recent.py'],input=json.dumps(material).encode(),capture_output=True,check=False,timeout=600)
  if result.returncode:
   (root/'recent-media-decode-error.log').write_bytes(result.stderr)
   raise RuntimeError('remote_decode_failed')
  report=json.loads(result.stdout)
  with db:db.executemany('INSERT OR REPLACE INTO sent VALUES (?,?,?)',[(rel,st.st_size,st.st_mtime_ns) for p,rel,st in pending])
 else:report={'files':0}
 report.update(stage='ok',checked_at=time.time());(root/'recent-media-status.json').write_text(json.dumps(report));print(json.dumps(report))
if __name__=='__main__':
 root=Path(sys.argv[1])
 try:main(root)
 except Exception as e:
  (root/'recent-media-status.json').write_text(json.dumps({'stage':'error','error_type':type(e).__name__,'checked_at':time.time()}))
  raise
