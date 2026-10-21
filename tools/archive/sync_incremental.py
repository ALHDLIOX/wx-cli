#!/usr/bin/env python3
"""Read live encrypted primary shards and send durable, idempotent batches to NAS.
Keys stay local. Cursors advance only after NAS acknowledges a committed batch.
Rowid replay covers recent edits; this is not a full historical edit reconciliation.
"""
import argparse,fcntl,gzip,hashlib,importlib.util,json,os,re,sqlite3,subprocess,time
from pathlib import Path

def source_class():
 spec=importlib.util.spec_from_file_location('collector',Path(__file__).parent.parent/'fts-collector/collector.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m.Source

def cycle(root):
 state=json.loads((root/'state.json').read_text());home=root.parent;cfg=json.loads((home/'config.json').read_text());keys=json.loads((home/'all_keys.json').read_text());Source=source_class();dbdir=Path(cfg['db_dir']);snapshot=Path(state['source']);c=sqlite3.connect(root/'incremental-cursors.sqlite3');c.execute('CREATE TABLE IF NOT EXISTS cursors(db TEXT,tbl TEXT,cursor INTEGER,salt TEXT,PRIMARY KEY(db,tbl))')
 c.execute('CREATE TABLE IF NOT EXISTS seen(db TEXT,tbl TEXT,rid INTEGER,hash TEXT,PRIMARY KEY(db,tbl,rid))')
 sources={};maps={};rows=[];nexts=[]
 try:
  files=sorted(p for p in (dbdir/'message').glob('*.db') if re.fullmatch(r'(?:biz_)?message_\d+\.db',p.name))
  for p in files:
   rel=str(p.relative_to(dbdir));key=keys[rel];sources[rel]=Source(home/'setup/libsqlcipher.dylib',p,key if isinstance(key,str) else key['enc_key']);sources[rel].rows('BEGIN');maps[rel]=dict(sources[rel].rows('SELECT rowid,user_name FROM Name2Id'))
  users={u for m in maps.values() for u in m.values() if u};names={'Msg_'+hashlib.md5(u.encode()).hexdigest():u for u in users}
  for rel,src in sources.items():
   with (dbdir/rel).open('rb') as f:salt=f.read(16).hex()
   olddb=sqlite3.connect((snapshot/rel).resolve().as_uri()+'?mode=ro',uri=True) if (snapshot/rel).exists() else None
   try:
    for (table,) in src.rows("SELECT name FROM sqlite_master WHERE type='table'"):
     if not re.fullmatch('Msg_[0-9a-f]{32}',table):continue
     old=c.execute('SELECT cursor,salt FROM cursors WHERE db=? AND tbl=?',(rel,table)).fetchone()
     if old and old[1]!=salt:raise RuntimeError('source_generation_changed')
     if old:cursor=old[0]
     elif olddb and olddb.execute('SELECT 1 FROM sqlite_master WHERE name=?',(table,)).fetchone():cursor=olddb.execute(f'SELECT max(rowid) FROM "{table}"').fetchone()[0] or 0
     else:cursor=0
     high=src.rows(f'SELECT max(rowid) FROM "{table}"')[0][0] or 0
     if high<cursor:raise RuntimeError('source_rowids_reset')
     # Replay a small tail to retain recent source edits and same-second messages.
     fetched=src.rows(f'SELECT rowid,local_id,server_id,local_type,sort_seq,real_sender_id,create_time,hex(message_content),WCDB_CT_message_content FROM "{table}" WHERE rowid>{max(0,cursor-8)} ORDER BY rowid LIMIT 1000')
     for rid,local,server,kind,seq,sender,stamp,raw,compression in fetched:
      if table not in names:raise RuntimeError('unresolved_conversation')
      rows.append(dict(source_db=rel,table=table,rowid=rid,local_id=local,server_id=server,kind=kind or 0,sort_seq=seq,sender=maps[rel].get(sender),conversation=names[table],timestamp=stamp,content_hex=raw,compression=compression))
     nexts.append((rel,table,max([cursor]+[r[0] for r in fetched]),salt))
   finally:
    if olddb:olddb.close()
 finally:
  for src in sources.values():src.close()
 changed=[];seen=[]
 for row in rows:
  h=hashlib.sha256(json.dumps(row,sort_keys=True).encode()).hexdigest();identity=(row['source_db'],row['table'],row['rowid']);old=c.execute('SELECT hash FROM seen WHERE db=? AND tbl=? AND rid=?',identity).fetchone()
  if not old or old[0]!=h:changed.append(row);seen.append((*identity,h))
 refs=[];resource_next=None
 c.execute('CREATE TABLE IF NOT EXISTS resource_cursor(id INTEGER PRIMARY KEY,position INTEGER)')
 resource_old=c.execute('SELECT position FROM resource_cursor WHERE id=1').fetchone();resource_pos=resource_old[0] if resource_old else 0
 rel='message/message_resource.db'
 if rel in keys:
  k=keys[rel];res=Source(home/'setup/libsqlcipher.dylib',dbdir/rel,k if isinstance(k,str) else k['enc_key'])
  try:
   chatnames=dict(res.rows('SELECT rowid,user_name FROM ChatName2Id'))
   for resource_rid,chat,local,stamp,server,packed in res.rows('SELECT rowid,chat_id,message_local_id,message_create_time,message_svr_id,hex(packed_info) FROM MessageResourceInfo WHERE rowid>'+str(max(0,resource_pos-8))+' ORDER BY rowid LIMIT 10000'):
    resource_next=max(resource_pos,resource_rid)
    tokens=[x.decode().lower() for x in re.findall(b'[0-9a-fA-F]{32}',bytes.fromhex(packed or ''))]
    if tokens and chat in chatnames:refs.append([chatnames[chat],local,stamp,str(server or 0),json.dumps(sorted(set(tokens)))])
  finally:res.close()
 packet={'self_user':state['self_user'],'rows':changed,'resource_refs':refs};payload=gzip.compress(json.dumps(packet,ensure_ascii=False).encode());pending=root/'incremental-pending.json.gz';pending.write_bytes(payload)
 result=subprocess.run(['ssh','-o','ConnectTimeout=10','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','NAS','python3 /pool-safe/wechat-vault/tools/ingest_delta.py /pool-safe/wechat-vault/structured/chat-archive.sqlite3'],input=payload,capture_output=True,timeout=900,check=True)
 report=json.loads(result.stdout)
 with c:
  c.executemany('INSERT OR REPLACE INTO cursors VALUES (?,?,?,?)',nexts)
  c.executemany('INSERT OR REPLACE INTO seen VALUES (?,?,?,?)',seen)
  if resource_next is not None:c.execute('INSERT OR REPLACE INTO resource_cursor VALUES (1,?)',(resource_next,))
 c.close();pending.unlink();report.update(rows_checked=len(rows),rows_sent=len(changed),shards=len(sources));return report

def main():
 p=argparse.ArgumentParser();p.add_argument('--state-dir',type=Path,required=True);p.add_argument('--once',action='store_true');a=p.parse_args();os.umask(0o077)
 lock=(a.state_dir/'incremental.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 while True:
  start=time.time()
  try:status=cycle(a.state_dir)
  except Exception as e:status={'stage':'error','error_type':type(e).__name__,'checked_at':time.time()}
  status['duration_seconds']=time.time()-start;tmp=a.state_dir/'incremental-status.tmp';tmp.write_text(json.dumps(status));tmp.replace(a.state_dir/'incremental-status.json')
  if a.once:print(json.dumps(status));return
  time.sleep(max(5,60-(time.time()-start)))
if __name__=='__main__':main()
