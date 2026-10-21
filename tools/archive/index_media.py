#!/usr/bin/env python3
"""Index transferred files without claiming media decryption or message association."""
import argparse,sqlite3,os,json
from pathlib import Path
from archive_state import file_lock
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--media',type=Path,required=True);p.add_argument('--db',type=Path,required=True);a=p.parse_args();os.umask(0o077)
with file_lock(str(a.db)+'.ingest.lock'):
 c=sqlite3.connect(a.db,timeout=180);count=size=0
 with c:
  for parent,dirs,files in os.walk(a.media):
   for name in files:
    f=Path(parent)/name
    if f.is_symlink():continue
    st=f.stat();suffix=f.suffix.lower();status='wechat_encoded_unverified' if suffix=='.dat' else 'original_file_unverified'
    c.execute('INSERT OR REPLACE INTO media_files VALUES (?,?,?,?,?)',(str(f.relative_to(a.media)),st.st_size,st.st_mtime_ns,suffix,status));count+=1;size+=st.st_size
    if count%1000==0:c.commit()
  for k,v in [('media_files',count),('media_bytes',size),('media_status','copied original files; decoding not assumed')]:c.execute('INSERT OR REPLACE INTO build_metadata VALUES (?,?)',(k,json.dumps(v)))
 print(json.dumps({'media_files':count,'media_bytes':size}));c.close()
