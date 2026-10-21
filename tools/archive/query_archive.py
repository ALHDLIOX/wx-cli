#!/usr/bin/env python3
"""Read a private structured archive. Search arguments and results are private."""
import argparse,json,sqlite3
from pathlib import Path

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--db',type=Path,required=True)
 sub=p.add_subparsers(dest='command',required=True);sub.add_parser('stats')
 c=sub.add_parser('conversations');c.add_argument('--name');c.add_argument('--limit',type=int,default=20)
 h=sub.add_parser('history');h.add_argument('--chat',required=True);h.add_argument('--limit',type=int,default=50);h.add_argument('--before',type=int)
 s=sub.add_parser('search');s.add_argument('text');s.add_argument('--chat');s.add_argument('--limit',type=int,default=50)
 e=sub.add_parser('search-index');e.add_argument('text');e.add_argument('--chat');e.add_argument('--limit',type=int,default=50)
 a=p.parse_args();db=sqlite3.connect(a.db.resolve().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON')
 if a.command=='stats':
  print(json.dumps({r['key']:json.loads(r['value']) for r in db.execute('SELECT * FROM build_metadata')},ensure_ascii=False,indent=2));db.close();return
 limit=max(1,min(a.limit,1000))
 if a.command=='conversations':
  where=' WHERE display_name LIKE ? ESCAPE \'\\\'' if a.name else '';params=[like(a.name)] if a.name else []
  rows=db.execute('SELECT * FROM conversations'+where+' ORDER BY last_timestamp DESC LIMIT ?',params+[limit])
 elif a.command=='history':
  where='conversation=?';params=[a.chat]
  if a.before is not None:where+=' AND timestamp<?';params.append(a.before)
  rows=db.execute('SELECT * FROM chat_history WHERE '+where+' ORDER BY timestamp DESC,id DESC LIMIT ?',params+[limit])
 elif a.command=='search-index':
  where="content_text LIKE ? ESCAPE '\\'";params=[like(a.text)]
  if a.chat:where+=' AND conversation=?';params.append(a.chat)
  rows=db.execute("SELECT 'index_evidence' AS source_kind,* FROM index_evidence WHERE "+where+' ORDER BY timestamp DESC LIMIT ?',params+[limit])
 else:
  # LIKE supports Chinese queries shorter than the trigram index's three characters.
  if len(a.text)>=3:
   where='id IN (SELECT rowid FROM message_search WHERE message_search MATCH ?)';params=['"'+a.text.replace('"','""')+'"']
  else:where="body_text LIKE ? ESCAPE '\\'";params=[like(a.text)]
  if a.chat:where+=' AND conversation=?';params.append(a.chat)
  rows=db.execute('SELECT * FROM chat_history WHERE '+where+' ORDER BY timestamp DESC,id DESC LIMIT ?',params+[limit])
 for row in rows:print(json.dumps(dict(row),ensure_ascii=False))
 db.close()

def like(s):return '%'+s.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%'
if __name__=='__main__':main()
