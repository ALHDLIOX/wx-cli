import sqlite3
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

if __name__=='__main__':unittest.main()
