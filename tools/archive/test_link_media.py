import unittest,sqlite3,tempfile,hashlib
from pathlib import Path
from link_media import run
class LinkTests(unittest.TestCase):
 def test_scoped_reference_and_unknown(self):
  with tempfile.TemporaryDirectory() as tmp:
   p=Path(tmp)/'in.db';out=Path(tmp)/'out.db';c=sqlite3.connect(p);c.executescript('CREATE TABLE media_files(path TEXT);CREATE TABLE messages(id INTEGER,conversation TEXT,body_text TEXT,timestamp INTEGER,server_id TEXT,type_code INTEGER);')
   token='a'*32;ch=hashlib.md5(b'chat').hexdigest();c.execute('insert into media_files values (?)',(f'msg/attach/{ch}/2026-10/Img/{token}_t.dat',));c.executemany('insert into messages values (?,?,?,?,?,?)',[(1,'chat',f'<img md5="{token}"/>',1,'1',3),(2,'other',f'<img md5="{token}"/>',1,'2',3),(3,'chat','',1,'3',3)]);c.commit();c.close();run(p,out);c=sqlite3.connect(out);self.assertEqual(c.execute('select message_id,status from message_media_status order by message_id').fetchall(),[(1,'matched'),(2,'reference_not_found_locally'),(3,'no_extractable_reference')]);c.close()
