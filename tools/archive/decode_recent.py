#!/usr/bin/env python3
"""Decode a bounded set of already transferred attachments; key arrives on stdin."""
import sys,json,sqlite3,hashlib,os
from pathlib import Path
from decode_media import decode,validated
from convert_wxgf import convert
os.umask(0o077);packet=json.load(sys.stdin);base=Path('/pool-safe/wechat-vault');root=base/'media/msg';out=base/'decoded';db=sqlite3.connect(base/'decoded-media.sqlite3',timeout=180);archive=sqlite3.connect(base/'structured/chat-archive.sqlite3',timeout=180);ok=0;failed=0
for rel in packet['paths']:
 p=Path(rel)
 if p.is_absolute() or '..' in p.parts:raise ValueError('invalid path')
 src=root/p
 if src.is_symlink() or not src.resolve().is_relative_to(root.resolve()):raise ValueError('invalid path')
 st=src.stat();dest=None;sha=None
 try:
  data=decode(src.read_bytes(),bytes.fromhex(packet['aes_hex']),packet['xor'])
  if data.startswith(b'wxgf'):data=convert(data)
  ext=validated(data);dest=rel+'.'+ext;target=out/dest;target.parent.mkdir(parents=True,exist_ok=True);tmp=target.with_name(target.name+'.recent.tmp');tmp.write_bytes(data);tmp.replace(target)
  sha=hashlib.sha256(data).hexdigest();status='ok';ok+=1
 except Exception as e:status=type(e).__name__;failed+=1
 with db:db.execute('INSERT OR REPLACE INTO decoded VALUES (?,?,?,?,?,?)',(rel,st.st_size,st.st_mtime_ns,status,dest,sha))
 with archive:archive.execute('INSERT OR REPLACE INTO media_files VALUES (?,?,?,?,?)',('msg/'+rel,st.st_size,st.st_mtime_ns,src.suffix,'decoded_verified' if status=='ok' else 'decode_unresolved'))
print(json.dumps({'files':len(packet['paths']),'decoded':ok,'unresolved':failed}));db.close();archive.close()
