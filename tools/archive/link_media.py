#!/usr/bin/env python3
"""Link exact MD5 references to files in the same conversation directory.
Absence of an extractable reference is unknown, not proof an attachment is missing.
"""
import argparse,collections,hashlib,json,re,sqlite3,time,os
from contextlib import closing, ExitStack
from pathlib import Path
from archive_state import atomic_json, file_lock

def run(archive,output):
 os.umask(0o077)
 with file_lock(str(output)+'.lock'):
  return _run(archive,output)

def _run(archive,output):
 with ExitStack() as stack:
  src=stack.enter_context(closing(sqlite3.connect(Path(archive).resolve().as_uri()+'?mode=ro',uri=True,timeout=120)))
  out=stack.enter_context(closing(sqlite3.connect(output)))
  return _link(src,out,output)

def _link(src,out,output):
 out.executescript('CREATE TABLE IF NOT EXISTS file_tokens(chat_hash TEXT,token TEXT,path TEXT,PRIMARY KEY(chat_hash,token,path)); CREATE INDEX IF NOT EXISTS token_idx ON file_tokens(chat_hash,token); CREATE TABLE IF NOT EXISTS message_media(message_id INTEGER,path TEXT,evidence TEXT,PRIMARY KEY(message_id,path)); CREATE TABLE IF NOT EXISTS message_media_status(message_id INTEGER PRIMARY KEY,status TEXT);')
 for (path,) in src.execute('SELECT path FROM media_files'):
  parts=Path(path).parts
  if len(parts)>3 and parts[1]=='attach' and re.fullmatch('[0-9a-f]{32}',parts[2]):
   token=Path(path).stem.split('_')[0].lower()
   if re.fullmatch('[0-9a-f]{32}',token):out.execute('INSERT OR IGNORE INTO file_tokens VALUES (?,?,?)',(parts[2],token,path))
 out.commit();refs={}
 if src.execute("SELECT 1 FROM sqlite_master WHERE name='incremental_media_refs'").fetchone():
  for chat,stamp,server,tokens in src.execute("SELECT conversation,timestamp,server_id,tokens FROM incremental_media_refs WHERE server_id != '0'"):
   refs.setdefault((chat,stamp,server),set()).update(json.loads(tokens))
 counts=collections.Counter();start=time.time()
 for mid,chat,body,stamp,server in src.execute('SELECT id,conversation,body_text,timestamp,server_id FROM messages WHERE type_code IN (3,43,49,62)'):
  tokens=set(re.findall(r'(?i)(?:md5\s*=\s*[\"\x27]([0-9a-f]{32})[\"\x27]|<md5>([0-9a-f]{32})</md5>)',body or ''));tokens={x.lower() for t in tokens for x in t if x};tokens.update(refs.get((chat,stamp,server),set()));found=set();ch=hashlib.md5(chat.encode()).hexdigest()
  for token in tokens:
   found.update(r[0] for r in out.execute('SELECT path FROM file_tokens WHERE chat_hash=? AND token=?',(ch,token)))
  status='matched' if found else 'reference_not_found_locally' if tokens else 'no_extractable_reference'
  for path in found:out.execute('INSERT OR IGNORE INTO message_media VALUES (?,?,?)',(mid,path,'exact_xml_or_resource_md5_and_conversation'))
  out.execute('INSERT OR REPLACE INTO message_media_status VALUES (?,?)',(mid,status));counts[status]+=1
  if sum(counts.values())%5000==0:out.commit()
 out.commit();report={'counts':counts,'completed_at':time.time(),'seconds':time.time()-start};atomic_json(Path(str(output)+'.report.json'),report);print(json.dumps(report))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--archive',required=True);p.add_argument('--output',required=True);a=p.parse_args();run(a.archive,a.output)
