import unittest,tempfile,sqlite3,ctypes as C
from pathlib import Path
from build_archive import Builder,Decoder,body_fields

class ArchiveTests(unittest.TestCase):
 def test_zstd_and_invalid_payload(self):
  d=Decoder();l=d.lib;l.ZSTD_compress.argtypes=[C.c_void_p,C.c_size_t,C.c_void_p,C.c_size_t,C.c_int];l.ZSTD_compress.restype=C.c_size_t
  raw='测试消息🧪'.encode();buf=C.create_string_buffer(1024);n=l.ZSTD_compress(buf,1024,raw,len(raw),1)
  self.assertEqual(d.decode(buf.raw[:n],4),(raw.decode(),'ok'))
  self.assertIsNone(d.decode(b'not a frame',4)[0])
 def test_group_prefix_and_xml(self):
  text='wxid_example:\n<msg><appmsg><title>file</title><url>https://example.test</url></appmsg></msg>'
  body,sender,fields=body_fields(text,True)
  self.assertEqual(sender,'wxid_example');self.assertEqual(fields['title'],'file')
  self.assertEqual(body_fields('notes:\nhello',True)[0],'notes:\nhello')
  self.assertEqual(body_fields('<!DOCTYPE x [<!ENTITY e "x">]><x>&e;</x>')[2],{})
 def test_dedup_preserves_zero_ids_variants_and_references(self):
  with tempfile.TemporaryDirectory() as temp:
   b=Builder(Path(temp),Path(temp)/'out.db','me')
   def add(table,rowid,server,text):
    b.add('peer','peer','name2id',server,1700000000,1,0,text,'ok','message/message_0.db',table,rowid,rowid,1,0,text)
   add('Msg_a',1,99,'first');add('Msg_b',1,99,'edited')
   add('Msg_a',2,0,'same second');add('Msg_b',2,0,'same second')
   report=b.finish()
   self.assertEqual(report['source_rows'],4);self.assertEqual(report['messages'],3)
   c=sqlite3.connect(Path(temp)/'out.db')
   self.assertEqual(c.execute('SELECT variant_text FROM message_sources WHERE variant_text IS NOT NULL').fetchall(),[('edited',)])
   self.assertEqual(c.execute("SELECT count(*) FROM message_search WHERE message_search MATCH 'first'").fetchone()[0],1)
   c.close()
 def test_end_to_end_primary_migration_and_index_evidence(self):
  import hashlib
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);(root/'message').mkdir();(root/'migrate').mkdir()
   table='Msg_'+hashlib.md5(b'peer').hexdigest()
   c=sqlite3.connect(root/'message/message_0.db')
   c.executescript('CREATE TABLE Name2Id(user_name TEXT); INSERT INTO Name2Id VALUES ("peer");')
   c.execute('CREATE TABLE "'+table+'"(local_id INTEGER PRIMARY KEY, server_id INTEGER, local_type INTEGER, sort_seq INTEGER,real_sender_id INTEGER,create_time INTEGER,message_content TEXT,WCDB_CT_message_content INTEGER)')
   c.execute('INSERT INTO "'+table+'" VALUES (1,101,1,1,1,1700000000,?,0)',('中文搜索测试',));c.commit();c.close()
   c=sqlite3.connect(root/'migrate/unspportmsg.db')
   c.execute('CREATE TABLE UnsupportMessage(svr_id INTEGER PRIMARY KEY,type INTEGER,sub_type INTEGER,from_user TEXT,to_user TEXT,content TEXT,create_time INTEGER,sequent_id INTEGER)')
   c.execute('INSERT INTO UnsupportMessage VALUES (101,1,0,"peer","me",?,1700000000000,1)',('中文搜索测试',));c.commit();c.close()
   c=sqlite3.connect(root/'message/message_fts.db')
   c.executescript('CREATE TABLE name2id(username TEXT); INSERT INTO name2id VALUES ("peer"); CREATE TABLE message_fts_v4_0_content(c0 TEXT,c1 INTEGER,c2 INTEGER,c3 INTEGER,c4 INTEGER,c5 INTEGER,c6 INTEGER);')
   c.execute('INSERT INTO message_fts_v4_0_content VALUES (?,1,1,1,1,1,1700000000)',('中文搜索测试',));c.commit();c.close()
   report=Builder(root,root/'out.db','me').run()
   self.assertEqual((report['source_rows'],report['messages'],report['index_evidence_rows']),(2,1,1))
   c=sqlite3.connect(root/'out.db');self.assertEqual(c.execute("SELECT count(*) FROM message_search WHERE message_search MATCH '搜索测'").fetchone()[0],1);c.close()
   import subprocess,sys,json
   script=Path(__file__).parent/'query_archive.py'
   for command in [['search','搜索'],['search','搜索测'],['search-index','搜索'],['history','--chat','peer']]:
    result=subprocess.run([sys.executable,str(script),'--db',str(root/'out.db')]+command,capture_output=True,text=True,check=True)
    self.assertEqual(len(result.stdout.splitlines()),1)
   (root/'media').mkdir();(root/'media/example.dat').write_bytes(b'opaque')
   subprocess.run([sys.executable,str(Path(__file__).parent/'index_media.py'),'--media',str(root/'media'),'--db',str(root/'out.db')],capture_output=True,text=True,check=True)
   c=sqlite3.connect(root/'out.db');self.assertEqual(c.execute('SELECT bytes FROM media_files').fetchone()[0],6);c.close()
 def test_refuses_overwrite(self):
  with tempfile.TemporaryDirectory() as temp:
   p=Path(temp)/'existing.db';p.write_bytes(b'preserve')
   with self.assertRaises(ValueError):Builder(temp,p,'me')
   self.assertEqual(p.read_bytes(),b'preserve')
if __name__=='__main__':unittest.main()
