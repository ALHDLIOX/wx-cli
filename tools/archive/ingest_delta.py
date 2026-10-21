#!/usr/bin/env python3
"""Idempotently append primary-message batches to an existing archive via stdin gzip JSON.
Existing source edits are retained as evidence rather than silently replacing history.
"""
import collections,gzip,json,sqlite3,sys,time,hashlib
from build_archive import Builder,Decoder
from pathlib import Path
from contextlib import closing
from archive_state import atomic_json, file_lock

def ingest(path,packet):
 path=Path(path)
 with file_lock(str(path)+'.ingest.lock'):
  atomic_json((path.parent.parent if path.parent.name=='structured' else path.parent)/'mac-heartbeat.json',
              {'received_at':time.time(),'mac_status':packet.get('mac_status',{})})
  return _ingest(path,packet)

def _ingest(path,packet):
 with closing(sqlite3.connect(path,timeout=60)) as db:
  return _ingest_db(db,packet)

def _ingest_db(db,packet):
 b=Builder.__new__(Builder);b.db=db;b.db.execute('PRAGMA foreign_keys=ON');b.db.execute('PRAGMA journal_mode=WAL');b.db.execute('PRAGMA cache_size=-131072');b.names={};b.users=set();b.conv_seen=set();b.self_user=packet['self_user'];b.count=0;b.decode_counts=collections.Counter();dec=Decoder();added=0;edits=0;chats=set();initial_max=b.db.execute('SELECT max(id) FROM messages').fetchone()[0] or 0
 b.db.execute('CREATE TABLE IF NOT EXISTS incremental_edits(source_db TEXT,source_table TEXT,source_rowid INTEGER,content_sha256 TEXT,payload_json TEXT,observed_at REAL,PRIMARY KEY(source_db,source_table,source_rowid,content_sha256))')
 b.db.execute('CREATE TABLE IF NOT EXISTS incremental_media_refs(conversation TEXT,local_id INTEGER,timestamp INTEGER,server_id TEXT,tokens TEXT,PRIMARY KEY(conversation,local_id,timestamp,server_id))')
 with b.db:
  b.db.executemany('INSERT INTO incremental_media_refs VALUES (?,?,?,?,?) ON CONFLICT(conversation,local_id,timestamp,server_id) DO UPDATE SET tokens=excluded.tokens WHERE tokens != excluded.tokens',packet.get('resource_refs',[]))
  for r in packet['rows']:
   rel,table,rid=r['source_db'],r['table'],r['rowid'];raw=bytes.fromhex(r['content_hex']);sha=hashlib.sha256(raw).hexdigest()
   old=b.db.execute('SELECT content_sha256 FROM message_sources WHERE source_db=? AND source_table=? AND source_rowid=?',(rel,table,rid)).fetchone()
   if old:
    if old[0]!=sha:
     b.db.execute('INSERT OR IGNORE INTO incremental_edits VALUES (?,?,?,?,?,?)',(rel,table,rid,sha,json.dumps(r),time.time()));edits+=1
    continue
   text,status=dec.decode(raw,r['compression']);kind=r['kind'];before=b.db.execute('SELECT max(id) FROM messages').fetchone()[0] or 0
   b.add(r['conversation'],r['sender'],'name2id' if r['sender'] else 'unknown',r['server_id'],r['timestamp'],kind&0xffffffff,(kind>>32)&0xffffffff,text,status,rel,table,rid,r['local_id'],r['sort_seq'],r['compression'],raw)
   b.db.execute('INSERT INTO message_search(rowid,body_text) SELECT id,body_text FROM messages WHERE id>?',(before,));added+=1;chats.add(r['conversation'])
  for chat in chats:
   b.db.execute('UPDATE conversations SET message_count=(SELECT count(*) FROM messages WHERE conversation=?),first_timestamp=(SELECT min(timestamp) FROM messages WHERE conversation=?),last_timestamp=(SELECT max(timestamp) FROM messages WHERE conversation=?) WHERE username=?',(chat,chat,chat,chat))
  report={'stage':'ok','source_rows_added':added,'edits_observed':edits,'checked_at':time.time()}
  b.db.execute('INSERT OR REPLACE INTO build_metadata VALUES (?,?)',('last_incremental_sync',json.dumps(report)))
  if added:
   new_messages=b.db.execute('SELECT count(*) FROM messages WHERE id>?',(initial_max,)).fetchone()[0]
   for field,increment in [('messages',new_messages),('source_rows',added)]:
    value=json.loads(b.db.execute('SELECT value FROM build_metadata WHERE key=?',(field,)).fetchone()[0])
    b.db.execute('UPDATE build_metadata SET value=? WHERE key=?',(json.dumps(value+increment),field))
   b.db.execute('UPDATE build_metadata SET value=? WHERE key=?',(json.dumps(b.db.execute('SELECT count(*) FROM conversations WHERE message_count>0').fetchone()[0]),'conversations'))

 return report
if __name__=='__main__':print(json.dumps(ingest(sys.argv[1],json.loads(gzip.decompress(sys.stdin.buffer.read())))))
