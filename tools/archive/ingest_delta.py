#!/usr/bin/env python3
"""Idempotently append primary-message batches to an existing archive via stdin gzip JSON.
Existing source edits are retained as evidence rather than silently replacing history.
"""
import collections,gzip,json,sqlite3,sys,time,hashlib
from build_archive import Builder,Decoder,body_fields,digest,iso,TYPES
from pathlib import Path
from contextlib import closing
from archive_state import atomic_json, file_lock

def ingest(path,packet):
 path=Path(path)
 with file_lock(str(path)+'.ingest.lock'):
  atomic_json((path.parent.parent if path.parent.name=='structured' else path.parent)/'mac-heartbeat.json',
              {'received_at':time.time(),'mac_status':packet.get('mac_status',{})})
  if not packet.get('rows') and not packet.get('resource_refs'):
   return {'stage':'ok','source_rows_added':0,'edits_observed':0,'checked_at':time.time()}
  return _ingest(path,packet)

def _ingest(path,packet):
 with closing(sqlite3.connect(path,timeout=60)) as db:
  return _ingest_db(db,packet)


def add_reused(b,r,raw,text,status,previous_message):
 """A reused source slot must also bypass Builder's zero/server-ID dedup keys."""
 conv=r['conversation'];sender=r['sender'];kind=r['kind'];server=str(r['server_id'] or 0)
 b.conversation(conv)
 body,prefix,fields=body_fields(text,conv.endswith('@chatroom'),b.users)
 resolution='name2id' if sender else 'unknown'
 if not sender and prefix:sender=prefix;resolution='content_prefix'
 direction='outgoing' if sender==b.self_user else 'incoming' if sender else 'unknown'
 key=digest(['rowid_reuse',r['source_db'],r['table'],r['rowid'],previous_message,
             r['local_id'],r['timestamp'],server])
 args=(key,conv,sender,resolution,direction,server,r['timestamp'],iso(r['timestamp']),
       kind&0xffffffff,(kind>>32)&0xffffffff,TYPES.get(kind&0xffffffff,'unknown'),body,
       json.dumps(fields,ensure_ascii=False),status)
 mid=b.db.execute('INSERT INTO messages(message_key,conversation,sender,sender_resolution,direction,server_id,timestamp,time_utc,type_code,sub_type,message_type,body_text,fields_json,decode_status) VALUES ('+','.join('?'*14)+')',args).lastrowid
 b.db.execute('INSERT INTO message_sources(message_id,source_db,source_table,source_rowid,local_id,sort_seq,compression,content_sha256,variant_text) VALUES (?,?,?,?,?,?,?,?,NULL)',
              (mid,r['source_db'],r['table'],r['rowid'],r['local_id'],r['sort_seq'],r['compression'],hashlib.sha256(raw).hexdigest()))


def _ingest_db(db,packet):
 b=Builder.__new__(Builder);b.db=db;b.db.execute('PRAGMA foreign_keys=ON');b.db.execute('PRAGMA journal_mode=WAL');b.db.execute('PRAGMA cache_size=-131072');b.names={};b.users=set();b.conv_seen=set();b.self_user=packet['self_user'];b.count=0;b.decode_counts=collections.Counter();dec=Decoder();added=0;edits=0;chats=set();initial_max=b.db.execute('SELECT max(id) FROM messages').fetchone()[0] or 0
 b.db.execute('CREATE TABLE IF NOT EXISTS incremental_edits(source_db TEXT,source_table TEXT,source_rowid INTEGER,content_sha256 TEXT,payload_json TEXT,observed_at REAL,PRIMARY KEY(source_db,source_table,source_rowid,content_sha256))')
 b.db.execute('CREATE TABLE IF NOT EXISTS incremental_media_refs(conversation TEXT,local_id INTEGER,timestamp INTEGER,server_id TEXT,tokens TEXT,PRIMARY KEY(conversation,local_id,timestamp,server_id))')
 # Historical source slots have their own IDs and deliberately no slot UNIQUE.
 b.db.execute('CREATE TABLE IF NOT EXISTS superseded_sources(id INTEGER PRIMARY KEY,message_id INTEGER NOT NULL REFERENCES messages(id),source_db TEXT,source_table TEXT,source_rowid INTEGER,local_id INTEGER,sort_seq INTEGER,compression INTEGER,content_sha256 TEXT,variant_text TEXT,superseded_at REAL)')
 with b.db:
  b.db.executemany('INSERT INTO incremental_media_refs VALUES (?,?,?,?,?) ON CONFLICT(conversation,local_id,timestamp,server_id) DO UPDATE SET tokens=excluded.tokens WHERE tokens != excluded.tokens',packet.get('resource_refs',[]))
  for r in packet['rows']:
   rel,table,rid=r['source_db'],r['table'],r['rowid'];raw=bytes.fromhex(r['content_hex']);sha=hashlib.sha256(raw).hexdigest()
   old=b.db.execute('SELECT s.content_sha256,s.local_id,m.timestamp,m.server_id,s.message_id FROM message_sources s JOIN messages m ON m.id=s.message_id WHERE s.source_db=? AND s.source_table=? AND s.source_rowid=?',(rel,table,rid)).fetchone()
   if old and (old[1],old[2],old[3])==(r['local_id'],r['timestamp'],str(r['server_id'] or 0)):
    if old[0]!=sha:
     b.db.execute('INSERT OR IGNORE INTO incremental_edits VALUES (?,?,?,?,?,?)',(rel,table,rid,sha,json.dumps(r),time.time()));edits+=1
    continue
   text,status=dec.decode(raw,r['compression']);kind=r['kind'];before=b.db.execute('SELECT max(id) FROM messages').fetchone()[0] or 0
   if old:
    b.db.execute('INSERT INTO superseded_sources(message_id,source_db,source_table,source_rowid,local_id,sort_seq,compression,content_sha256,variant_text,superseded_at) SELECT message_id,source_db,source_table,source_rowid,local_id,sort_seq,compression,content_sha256,variant_text,? FROM message_sources WHERE source_db=? AND source_table=? AND source_rowid=?',(time.time(),rel,table,rid))
    b.db.execute('DELETE FROM message_sources WHERE source_db=? AND source_table=? AND source_rowid=?',(rel,table,rid))
    add_reused(b,r,raw,text,status,old[4])
   else:
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
