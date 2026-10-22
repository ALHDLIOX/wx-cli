"""Regression fixtures for deletions, source-slot reuse and failure heartbeats."""
from contextlib import closing, redirect_stdout
import gzip
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from archive_state import read_status
from build_archive import Builder
from exclusion import chat_hash
from ingest_delta import ingest
from macos import supervisor
from nas import heartbeat
import sync_incremental
from test_sync_hardening import KEPT, SqlSource, live_fixture


class RowidSyncTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state, account = live_fixture(self.root)
        self.path = account / 'db_storage/message/message_0.db'
        self.table = 'Msg_' + chat_hash(KEPT)
        self.other = 'Msg_' + chat_hash('fixture_other')
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('INSERT INTO Name2Id VALUES (?)', ('fixture_other',))
            db.execute(f'CREATE TABLE "{self.other}" AS SELECT * FROM "{self.table}"')
            db.execute(f'INSERT INTO "{self.table}" VALUES (2,2,1,2,1,2,?,0)', ('tail',))
        self.packets = []
        self.addCleanup(patch.stopall)
        patch.object(sync_incremental, 'source_class', return_value=SqlSource).start()
        patch.object(sync_incremental, 'wechat_is_running', return_value=True).start()
        self.transfer = patch.object(sync_incremental, 'transfer', side_effect=self.accept).start()
        sync_incremental.cycle(self.state)

    def accept(self, payload):
        self.packets.append(json.loads(gzip.decompress(payload)))
        return subprocess.CompletedProcess([], 0, b'{}')

    def cursor(self, table):
        with closing(sqlite3.connect(self.state / 'incremental-cursors.sqlite3')) as db:
            return db.execute('SELECT cursor FROM cursors WHERE tbl=?', (table,)).fetchone()[0]

    def grow_other(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(f'INSERT INTO "{self.other}" VALUES (2,2,1,2,1,2,?,0)', ('healthy',))

    def test_tail_deletion_then_reuse_and_other_table_continue(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(f'DELETE FROM "{self.table}" WHERE rowid=2')
        self.grow_other()
        result = sync_incremental.cycle(self.state)
        self.assertEqual((result['rowid_rollbacks'], result['blocked_tables']), (1, 0))
        self.assertEqual((self.cursor(self.table), self.cursor(self.other)), (1, 2))
        self.assertEqual([r['table'] for r in self.packets[-1]['rows']], [self.other])
        self.assertEqual(self.packets[-1]['mac_status']['rowid_rollbacks'], 1)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(f'INSERT INTO "{self.table}" VALUES (2,3,1,3,1,3,?,0)', ('replacement',))
        result = sync_incremental.cycle(self.state)
        self.assertEqual(result['rowid_rollbacks'], 0)
        self.assertEqual(self.cursor(self.table), 2)
        self.assertEqual([(r['rowid'], r['server_id']) for r in self.packets[-1]['rows']], [(2, 3)])

    def test_large_or_empty_rollback_blocks_only_one_table(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                with closing(sqlite3.connect(self.state / 'incremental-cursors.sqlite3')) as db, db:
                    db.execute('UPDATE cursors SET cursor=? WHERE tbl=?',
                               (1 if empty else sync_incremental.ROWID_ROLLBACK_LIMIT + 3, self.table))
                if empty:
                    with closing(sqlite3.connect(self.path)) as db, db:
                        db.execute(f'DELETE FROM "{self.table}"')
                before = self.cursor(self.table)
                if not empty:
                    self.grow_other()
                with self.assertLogs(level='WARNING') as logs:
                    result = sync_incremental.cycle(self.state)
                self.assertIn('source_rowids_reset', logs.output[0])
                self.assertEqual((result['blocked_tables'], result['rowid_rollbacks']), (1, 0))
                self.assertEqual(self.cursor(self.table), before)
                self.assertFalse(any(r['table'] == self.table for r in self.packets[-1]['rows']))
                self.assertEqual(self.cursor(self.other), 2)
                self.assertEqual(self.packets[-1]['mac_status']['blocked_tables'], 1)
                if not empty:
                    self.assertEqual([r['table'] for r in self.packets[-1]['rows']], [self.other])

    def test_threshold_and_failed_transfer_do_not_commit_rollback(self):
        before = 2 + sync_incremental.ROWID_ROLLBACK_LIMIT
        with closing(sqlite3.connect(self.state / 'incremental-cursors.sqlite3')) as db, db:
            db.execute('UPDATE cursors SET cursor=? WHERE tbl=?', (before, self.table))
        self.transfer.side_effect = subprocess.TimeoutExpired('fixture', 1)
        with self.assertRaises(subprocess.TimeoutExpired):
            sync_incremental.cycle(self.state)
        self.assertEqual(self.cursor(self.table), before)
        status = read_status(self.state / 'incremental-status.json')
        self.assertEqual((status['rowid_rollbacks'], status['consecutive_errors']), (1, 1))
        self.transfer.side_effect = self.accept
        self.assertEqual(sync_incremental.cycle(self.state)['rowid_rollbacks'], 1)
        self.assertEqual(self.cursor(self.table), 2)

    def test_salt_change_still_aborts_entire_cycle(self):
        with closing(sqlite3.connect(self.state / 'incremental-cursors.sqlite3')) as db, db:
            db.execute('UPDATE cursors SET salt=? WHERE tbl=?', ('changed', self.table))
        self.transfer.reset_mock()
        with self.assertRaisesRegex(RuntimeError, 'source_generation_changed'):
            sync_incremental.cycle(self.state)
        self.transfer.assert_not_called()


class ReusedIngestTests(unittest.TestCase):
    def test_each_identity_field_forces_new_message_even_with_same_payload(self):
        for server in (0, 123):
            for field in ('local_id', 'timestamp', 'server_id'):
                with self.subTest(server=server, field=field), tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / 'archive.db'
                    with redirect_stdout(io.StringIO()):
                        Builder(tmp, path, 'self').finish()
                    row = dict(source_db='message/message_0.db', table='Msg_fixture', rowid=1,
                               content_hex=b'original message'.hex(), compression=0, kind=1,
                               conversation='chat', sender='self', server_id=server,
                               timestamp=1700000000, local_id=1, sort_seq=1)
                    ingest(path, {'self_user': 'self', 'rows': [row]})
                    replacement = dict(row, **{field: row[field] + 1})
                    result = ingest(path, {'self_user': 'self', 'rows': [replacement]})
                    self.assertEqual((result['source_rows_added'], result['edits_observed']), (1, 0))
                    self.assertEqual(ingest(path, {'self_user': 'self', 'rows': [replacement]})['source_rows_added'], 0)
                    # Editing the replacement still preserves its canonical body.
                    edited = dict(replacement, content_hex=b'edited message'.hex())
                    self.assertEqual(ingest(path, {'self_user': 'self', 'rows': [edited]})['edits_observed'], 1)
                    # A third occupant proves historical source IDs/slot coordinates can repeat.
                    third = dict(replacement, timestamp=replacement['timestamp'] + 1,
                                 content_hex=b'third message'.hex())
                    ingest(path, {'self_user': 'self', 'rows': [third]})
                    with closing(sqlite3.connect(path)) as db:
                        self.assertEqual(db.execute('SELECT body_text FROM messages ORDER BY id').fetchall(),
                                         [('original message',), ('original message',), ('third message',)])
                        self.assertEqual(db.execute('SELECT count(*) FROM incremental_edits').fetchone()[0], 1)
                        self.assertEqual(db.execute('SELECT count(*) FROM superseded_sources').fetchone()[0], 2)
                        self.assertEqual(db.execute('SELECT count(*) FROM message_sources').fetchone()[0], 1)
                        self.assertEqual(db.execute('SELECT count(DISTINCT message_id) FROM superseded_sources').fetchone()[0], 2)
                        self.assertTrue(all(r[0] > 0 for r in db.execute('SELECT superseded_at FROM superseded_sources')))
                        self.assertEqual(json.loads(db.execute("SELECT value FROM build_metadata WHERE key='source_rows'").fetchone()[0]), 3)
                        self.assertEqual(db.execute('SELECT message_count FROM conversations').fetchone()[0], 3)
                        self.assertEqual(db.execute("SELECT count(*) FROM message_search WHERE message_search MATCH 'message'").fetchone()[0], 3)
                        self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
                        db.execute("INSERT INTO message_search(message_search,rank) VALUES('integrity-check',1)")
                    script = Path(__file__).parent / 'query_archive.py'
                    for command in (['history', '--chat', 'chat'], ['search', 'message'], ['stats']):
                        result = subprocess.run([sys.executable, str(script), '--db', str(path), *command],
                                                capture_output=True, text=True, check=True)
                        if command == ['stats']:
                            self.assertEqual(json.loads(result.stdout)['messages'], 3)
                        else:
                            self.assertEqual(len(result.stdout.splitlines()), 3)

    def test_failed_reuse_rolls_back_source_move_and_new_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.db'
            with redirect_stdout(io.StringIO()):
                Builder(tmp, path, 'self').finish()
            row = dict(source_db='message/message_0.db', table='Msg_fixture', rowid=1,
                       content_hex=b'original'.hex(), compression=0, kind=1,
                       conversation='chat', sender='self', server_id=1,
                       timestamp=1, local_id=1, sort_seq=1)
            ingest(path, {'self_user': 'self', 'rows': [row]})
            broken = dict(row, timestamp=2)
            del broken['sort_seq']
            with self.assertRaises(KeyError):
                ingest(path, {'self_user': 'self', 'rows': [broken]})
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute('SELECT timestamp FROM messages').fetchall(), [(1,)])
                self.assertEqual(db.execute('SELECT count(*) FROM message_sources').fetchone()[0], 1)
                self.assertEqual(db.execute('SELECT count(*) FROM superseded_sources').fetchone()[0], 0)
                self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])


class FailureHeartbeatTests(unittest.TestCase):
    def test_supervisor_sends_latest_status_and_recovers_after_status_send_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, _ = live_fixture(Path(tmp))
            with patch.object(sync_incremental, '_cycle', side_effect=RuntimeError('source_generation_changed')), patch.object(sync_incremental, 'wechat_is_running', return_value=False), patch.object(subprocess, 'run') as remote:
                remote.return_value = subprocess.CompletedProcess([], 0, b'{}')
                supervisor.text_cycle(state)
                packet = json.loads(gzip.decompress(remote.call_args.kwargs['input']))
                self.assertEqual(remote.call_args.args[0], sync_incremental.INGEST_COMMAND)
                self.assertEqual((packet['rows'], packet['resource_refs'], packet['self_user']), ([], [], 'fixture_self'))
                self.assertFalse(packet['mac_status']['wechat_running'])
                status = packet['mac_status']['workers']['incremental']
                self.assertEqual((status['stage'], status['consecutive_errors']), ('error', 1))
                remote.side_effect = subprocess.TimeoutExpired('PRIVATE', 1)
                with self.assertLogs(level='WARNING') as logs:
                    supervisor.text_cycle(state)
                self.assertNotIn('PRIVATE', ''.join(logs.output))
                self.assertEqual(read_status(state / 'incremental-status.json')['consecutive_errors'], 2)
            with patch.object(sync_incremental, '_cycle', return_value={}), patch.object(sync_incremental, 'wechat_is_running', return_value=True), patch.object(sync_incremental, 'send_status') as send:
                supervisor.text_cycle(state)
                send.assert_not_called()
            self.assertEqual(read_status(state / 'incremental-status.json')['consecutive_errors'], 0)

    def test_empty_packet_changes_only_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'archive.db'
            with redirect_stdout(io.StringIO()):
                Builder(tmp, path, 'self').finish()
            before = path.read_bytes()
            ingest(path, {'self_user': 'self', 'rows': [], 'resource_refs': [],
                          'mac_status': {'blocked_tables': 2, 'rowid_rollbacks': 1}})
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(read_status(path.parent / 'mac-heartbeat.json')['mac_status']['blocked_tables'], 2)
            # Even a missing archive is not created by a status-only packet.
            missing = path.parent / 'missing.db'
            ingest(missing, {'self_user': 'self', 'rows': [], 'resource_refs': []})
            self.assertFalse(missing.exists())

    def test_blocked_tables_alert_once_unknown_and_recovery(self):
        config = {'stale_minutes': 60}
        def beat(blocked):
            return {'received_at': 100, 'mac_status': blocked}
        state, messages = heartbeat.evaluate(beat({'blocked_tables': 2}), {}, config, 100)
        self.assertEqual(messages, ['有 2 张消息表因行号异常暂停同步'])
        state, messages = heartbeat.evaluate(beat({'blocked_tables': 3}), state, config, 200)
        self.assertEqual(messages, [])
        state, messages = heartbeat.evaluate(beat({}), state, config, 300)
        self.assertEqual(messages, [])
        state, messages = heartbeat.evaluate(beat({'blocked_tables': 0}), state, config, 400)
        self.assertEqual(len(messages), 1)
        self.assertIn('已恢复', messages[0])
        self.assertEqual(heartbeat.evaluate(beat({'blocked_tables': 0}), state, config, 500)[1], [])
