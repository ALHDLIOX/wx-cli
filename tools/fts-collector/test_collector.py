import sqlite3
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from collector import archive, capture

TABLE = 'message_fts_v4_0_content'
class FakeSource:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.executescript(f'CREATE TABLE {TABLE}(c0 TEXT,c1 INTEGER,c2 INTEGER,c3 INTEGER,c4 INTEGER,c5 INTEGER,c6 INTEGER); CREATE TABLE name2id(username TEXT); INSERT INTO name2id VALUES ("filehelper");')
    def rows(self, sql):
        return self.db.execute(sql).fetchall()
    def add(self, n):
        self.db.executemany(f'INSERT INTO {TABLE} VALUES (?,?,?,?,?,?,?)',
            [(f'test-{i}',i,i,1,1,1,100) for i in range(n)])

class CaptureTests(unittest.TestCase):
    def test_backlog_same_timestamp_restart_and_late_update(self):
        src = FakeSource(); src.add(1201)
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'archive.db'; dst = archive(path)
            self.assertEqual(capture(src,dst,'account',True)['changed'],1201)
            self.assertEqual(capture(src,dst,'account')['changed'],0)
            dst.close(); dst=archive(path)
            self.assertEqual(capture(src,dst,'account',True)['changed'],0)
            src.db.execute(f'UPDATE {TABLE} SET c0="changed" WHERE rowid=1')
            self.assertEqual(capture(src,dst,'account',True)['changed'],1)
            self.assertEqual(dst.execute('SELECT count(*) FROM records').fetchone()[0],1201)
            dst.close()

    def test_failed_batch_does_not_advance_cursor(self):
        src=FakeSource();src.add(4)
        with tempfile.TemporaryDirectory() as d:
            dst=archive(Path(d)/'archive.db')
            dst.execute("CREATE TRIGGER fail BEFORE INSERT ON records WHEN NEW.source_rowid=3 BEGIN SELECT RAISE(ABORT,'disk failure simulation'); END")
            with self.assertRaises(sqlite3.IntegrityError):capture(src,dst,'account')
            self.assertEqual(dst.execute('SELECT count(*) FROM records').fetchone()[0],0)
            self.assertEqual(dst.execute('SELECT count(*) FROM cursors').fetchone()[0],0)
            dst.execute('DROP TRIGGER fail')
            self.assertEqual(capture(src,dst,'account')['changed'],4)
            dst.close()

class PrimaryTests(unittest.TestCase):
    def test_primary_only_message_and_binary_preservation(self):
        src=FakeSource()
        src.db.execute('DROP TABLE name2id')
        src.db.execute('CREATE TABLE Name2Id(user_name TEXT)')
        src.db.execute("INSERT INTO Name2Id VALUES ('filehelper')")
        table='Msg_'+hashlib.md5(b'filehelper').hexdigest()
        src.db.execute(f'CREATE TABLE {table}(local_id INTEGER PRIMARY KEY,sort_seq INTEGER,local_type INTEGER,server_id INTEGER,real_sender_id INTEGER,create_time INTEGER,message_content BLOB,WCDB_CT_message_content INTEGER)')
        src.db.execute(f'INSERT INTO {table} VALUES (1,1,1,101,1,100,?,0)',('primary-only test',))
        src.db.execute(f'INSERT INTO {table} VALUES (2,2,1,102,1,100,?,4)',(b'\x00\xff\x10',))
        with tempfile.TemporaryDirectory() as d:
            dst=archive(Path(d)/'archive.db')
            self.assertEqual(capture(src,dst,'account',True,primary=True)['changed'],2)
            payloads=[json.loads(x[0]) for x in dst.execute('SELECT payload FROM records ORDER BY source_rowid')]
            self.assertEqual(payloads[0]['content'],'primary-only test')
            self.assertEqual(payloads[0]['username'],'filehelper')
            self.assertEqual(payloads[0]['source'],'message_primary')
            self.assertEqual(payloads[1]['content_hex'],'00FF10')
            self.assertIsNone(payloads[1]['content'])
            self.assertFalse(payloads[1]['content_decoded'])
            self.assertEqual(capture(src,dst,'account',primary=True)['changed'],0)
            # A new table must be detected even with a previous shard cursor.
            new='Msg_'+hashlib.md5(b'newchat').hexdigest()
            src.db.execute(f'CREATE TABLE {new} AS SELECT * FROM {table} WHERE local_id=1')
            self.assertEqual(capture(src,dst,'account',primary=True)['changed'],1)
            dst.close()

if __name__=='__main__':unittest.main()
