import unittest,tempfile,sqlite3
from pathlib import Path
from build_archive import Builder
from ingest_delta import ingest
class IncrementalTests(unittest.TestCase):
 def test_retry_same_second_and_edit(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'db';b=Builder(tmp,p,'self');b.finish()
   r=dict(source_db='message/message_0.db',table='Msg_abc',rowid=1,content_hex=b'first message'.hex(),compression=0,kind=1,conversation='chat',sender='self',server_id=123,timestamp=1700000000,local_id=1,sort_seq=1)
   packet={'self_user':'self','rows':[r]};self.assertEqual(ingest(p,packet)['source_rows_added'],1);self.assertEqual(ingest(p,packet)['source_rows_added'],0)
   r2=dict(r,rowid=2,server_id=124,content_hex=b'second message'.hex());ingest(p,{'self_user':'self','rows':[r2]})
   r['content_hex']=b'edited'.hex();self.assertEqual(ingest(p,packet)['edits_observed'],1)
   c=sqlite3.connect(p);self.assertEqual(c.execute('select count(*) from messages').fetchone()[0],2);self.assertEqual(c.execute("select count(*) from message_search where message_search match 'message'").fetchone()[0],2);self.assertEqual(c.execute('pragma foreign_key_check').fetchall(),[]);c.close()
