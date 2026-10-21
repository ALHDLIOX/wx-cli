from contextlib import closing
import gzip
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import sys
import unittest
from unittest.mock import patch

from archive_state import atomic_json, file_lock, read_status, worker_cycle
from exclusion import Ownership, chat_hash, load_exclusions
from macos import fast_sync, media_cycle
import recent_media
import sync_incremental

EXCLUDED, KEPT = 'fixture_excluded', 'fixture_kept'


def write_exclude(path, users=(EXCLUDED,)):
    atomic_json(path, {'version': 1, 'conversations': {user: 'fixture' for user in users}})


class SqlSource:
    queries = []

    def __init__(self, library, path, key):
        self.db = sqlite3.connect(path)

    def rows(self, sql):
        self.queries.append(sql)
        return self.db.execute(sql).fetchall()

    def close(self):
        self.db.close()


def live_fixture(root):
    account = root / 'account'
    dbdir = account / 'db_storage'
    (dbdir / 'message').mkdir(parents=True)
    (account / 'msg').mkdir()
    snapshot = root / 'snapshot'
    snapshot.mkdir()
    state = root / 'state'
    state.mkdir()
    atomic_json(state / 'state.json', {'source': str(snapshot), 'account_root': str(account), 'self_user': 'fixture_self'})
    atomic_json(root / 'config.json', {'db_dir': str(dbdir)})
    atomic_json(root / 'all_keys.json', {'message/message_0.db': 'fixture_key', 'message/message_resource.db': 'fixture_key'})
    write_exclude(state / 'exclude.json')
    db = sqlite3.connect(dbdir / 'message/message_0.db')
    db.execute('CREATE TABLE Name2Id(user_name TEXT)')
    # Excluded table intentionally has no Name2Id entry and must not be read.
    db.execute('INSERT INTO Name2Id VALUES (?)', (KEPT,))
    for user in (EXCLUDED, KEPT):
        table = 'Msg_' + chat_hash(user)
        db.execute(f'CREATE TABLE "{table}"(local_id INTEGER PRIMARY KEY,server_id INTEGER,local_type INTEGER,sort_seq INTEGER,real_sender_id INTEGER,create_time INTEGER,message_content TEXT,WCDB_CT_message_content INTEGER)')
        db.execute(f'INSERT INTO "{table}" VALUES (1,1,1,1,1,1,?,0)', ('fixture payload',))
    db.commit()
    db.close()
    db = sqlite3.connect(dbdir / 'message/message_resource.db')
    db.executescript('CREATE TABLE ChatName2Id(user_name TEXT);CREATE TABLE MessageResourceInfo(message_id INTEGER PRIMARY KEY,chat_id INTEGER,message_local_id INTEGER,message_create_time INTEGER,message_svr_id INTEGER,packed_info BLOB);')
    for number, user in enumerate((EXCLUDED, KEPT), 1):
        db.execute('INSERT INTO ChatName2Id VALUES (?)', (user,))
        db.execute('INSERT INTO MessageResourceInfo VALUES (?,?,?,?,?,?)', (number, number, 1, 1, number, b'a' * 32))
    db.commit()
    db.close()
    return state, account


class ExclusionTests(unittest.TestCase):
    def test_missing_invalid_and_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'exclude.json'
            self.assertEqual(load_exclusions(p), set())
            for value in ({}, {'version': True, 'conversations': {}}, {'version': 1, 'conversations': []}):
                atomic_json(p, value)
                with self.assertRaisesRegex(ValueError, 'invalid_exclusion'):
                    load_exclusions(p)
            write_exclude(p)
            self.assertEqual(load_exclusions(p), {EXCLUDED})
            p.chmod(0o644)
            with self.assertRaises(ValueError):
                load_exclusions(p)

    def test_shared_tokens_and_original_file_identity(self):
        own = Ownership({EXCLUDED})
        own.add_tokens(EXCLUDED, b'a' * 32 + b' ' + b'b' * 32)
        own.add_tokens(KEPT, b'b' * 32)
        def app(name, size):
            return f'<msg><appmsg><type>6</type><title>{name}</title><appattach><totallen>{size}</totallen></appattach></appmsg></msg>'
        own.add_message(EXCLUDED, app('exclusive.bin', 5))
        own.add_message(EXCLUDED, app('shared.bin', 5))
        own.add_message(KEPT, app('shared.bin', 5))
        for name in ('a' * 32 + '.mp4', 'a' * 32 + '_thumb.jpg'):
            self.assertEqual(own.decision('video/2026-01/' + name, 1), 'exclude')
        self.assertEqual(own.decision('video/2026-01/' + 'b' * 32 + '.mp4', 1), 'keep')
        self.assertEqual(own.decision('file/2026-01/exclusive.bin', 5), 'exclude')
        self.assertEqual(own.decision('file/2026-01/exclusive.bin', 6), 'unresolved')
        self.assertEqual(own.decision('file/2026-01/shared.bin', 5), 'keep')
        own.add_message(KEPT, None, False)
        self.assertEqual(own.decision('file/2026-01/exclusive.bin', 5), 'unresolved')


class WorkerTests(unittest.TestCase):
    def test_atomic_state_failure_preserves_success_and_redacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            success = worker_cycle(root, 'media', lambda: {'files': 1})
            for number in (1, 2):
                def fail():
                    raise RuntimeError('PRIVATE payload key\nsecond line')
                with self.assertRaises(RuntimeError):
                    worker_cycle(root, 'media', fail)
                status = read_status(root / 'media-cycle-status.json')
                self.assertEqual(status['last_success_at'], success['last_success_at'])
                self.assertEqual(status['consecutive_errors'], number)
                self.assertNotIn('PRIVATE', json.dumps(status))
                self.assertLessEqual(len(status['error_summary']), 200)
                self.assertEqual((root / 'media-cycle-status.json').stat().st_mode & 0o777, 0o600)
            result = worker_cycle(root, 'media', lambda: {})
            self.assertEqual(result['consecutive_errors'], 0)
            self.assertNotIn('error_type', result)

    def test_cycles_release_locks_without_garbage_collection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            held_results = []
            for module, method, implementation in [(sync_incremental, 'cycle', '_cycle'),
                                                    (recent_media, 'main', '_run'),
                                                    (media_cycle, 'run', '_run'),
                                                    (fast_sync, 'run', '_run')]:
                with patch.object(module, implementation, return_value={}), patch.object(sync_incremental, 'wechat_is_running', return_value=True):
                    for _ in range(2):
                        held_results.append(getattr(module, method)(root))
                    with patch.object(module, implementation, side_effect=ValueError('fixture')):
                        with self.assertRaises(ValueError):
                            getattr(module, method)(root)
                    getattr(module, method)(root)

    def test_incremental_filters_before_resolution_reloads_and_retries(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, _ = live_fixture(Path(tmp))
            packets = []
            def run(command, **kwargs):
                if command[0] == '/usr/bin/pgrep':
                    self.assertEqual(command, ['/usr/bin/pgrep', '-x', 'WeChat'])
                    return subprocess.CompletedProcess(command, 1)
                packets.append(json.loads(gzip.decompress(kwargs['input'])))
                return subprocess.CompletedProcess(command, 0, b'{}')
            SqlSource.queries = []
            with patch.object(sync_incremental, 'source_class', return_value=SqlSource), patch.object(subprocess, 'run', side_effect=run):
                for _ in range(2):
                    sync_incremental.cycle(state)
                self.assertEqual([row['conversation'] for row in packets[0]['rows']], [KEPT])
                self.assertEqual([row[0] for row in packets[0]['resource_refs']], [KEPT])
                self.assertEqual(packets[1]['rows'], [])
                self.assertFalse(packets[0]['mac_status']['wechat_running'])
                self.assertEqual(len(packets[0]['mac_status']['workers']), 3)
                self.assertFalse(any('Msg_' + chat_hash(EXCLUDED) in sql for sql in SqlSource.queries))
                with closing(sqlite3.connect(state / 'incremental-cursors.sqlite3')) as db:
                    self.assertEqual(db.execute('SELECT tbl FROM cursors').fetchall(), [('Msg_' + chat_hash(KEPT),)])
                atomic_json(state / 'exclude.json', {'invalid': True})
                with self.assertRaises(ValueError):
                    sync_incremental.cycle(state)
                self.assertEqual(len(packets), 2)
                write_exclude(state / 'exclude.json', ())
                # The now included table has no Name2Id: this really was skipped before.
                with self.assertRaisesRegex(RuntimeError, 'unresolved_conversation'):
                    sync_incremental.cycle(state)

    def test_failed_ssh_does_not_advance_cursors(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, _ = live_fixture(Path(tmp))
            def failure(command, **kwargs):
                raise subprocess.CalledProcessError(1, command, stderr=b'PRIVATE')
            with patch.object(sync_incremental, 'source_class', return_value=SqlSource), patch.object(sync_incremental, 'wechat_is_running', return_value=True):
                with patch.object(subprocess, 'run', side_effect=failure):
                    with self.assertRaises(subprocess.CalledProcessError):
                        sync_incremental.cycle(state)
                with closing(sqlite3.connect(state / 'incremental-cursors.sqlite3')) as db:
                    self.assertEqual(db.execute('SELECT * FROM cursors').fetchall(), [])
                status = read_status(state / 'incremental-status.json')
                self.assertEqual(status['consecutive_errors'], 1)
                self.assertNotIn('PRIVATE', json.dumps(status))
                def success(command, **kwargs):
                    packet = json.loads(gzip.decompress(kwargs['input']))
                    self.assertEqual(len(packet['rows']), 1)
                    self.assertEqual(packet['mac_status']['workers']['incremental']['consecutive_errors'], 1)
                    return subprocess.CompletedProcess(command, 0, b'{}')
                with patch.object(subprocess, 'run', side_effect=success):
                    sync_incremental.cycle(state)
                self.assertFalse((state / 'incremental-pending.json.gz').exists())

    def test_media_cycle_direct_calls_keep_subprocess_unmodified(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            atomic_json(root / 'image-material.json', {'fixture': True})
            original = subprocess.run
            with patch.object(fast_sync, 'run', return_value={'exclusion_unresolved': 2}) as scanner, patch.object(media_cycle, 'remote', return_value=subprocess.CompletedProcess([], 0, b'0')) as remote:
                for _ in range(2):
                    self.assertEqual(media_cycle.run(root)['exclusion_unresolved'], 2)
                self.assertEqual(scanner.call_count, 2)
                self.assertEqual(remote.call_count, 10)
                self.assertIs(subprocess.run, original)

    def test_recent_media_skips_excluded_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, account = live_fixture(Path(tmp))
            for user in (EXCLUDED, KEPT):
                path = account / 'msg/attach' / chat_hash(user) / '2026-01/Img' / ('a' * 32 + '.dat')
                path.parent.mkdir(parents=True)
                path.write_bytes(b'fixture')
            with patch.object(recent_media, 'source_class', return_value=SqlSource), patch.object(recent_media.time, 'time', return_value=2), patch.object(recent_media, 'transfer_pending', return_value={}) as transfer:
                recent_media.main(state)
                paths = transfer.call_args.args[2]
                self.assertEqual(len(paths), 1)
                self.assertIn(chat_hash(KEPT), str(next(iter(paths))))

    def test_fail_closed_all_workers_before_any_ssh(self):
        with tempfile.TemporaryDirectory() as tmp:
            state, _ = live_fixture(Path(tmp))
            atomic_json(state / 'exclude.json', {})
            with patch.object(subprocess, 'run') as remote, patch.object(subprocess, 'Popen') as stream, patch.object(sync_incremental, 'wechat_is_running', return_value=True):
                for action in (sync_incremental.cycle, recent_media.main, fast_sync.run, media_cycle.run):
                    with self.assertRaises(ValueError):
                        action(state)
                remote.assert_not_called()
                stream.assert_not_called()

    def test_stream_protocol_and_shared_transfer_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            root = vault / 'media/msg'
            root.mkdir(parents=True)
            status = vault / 'stream-status.json'
            content = b'fixture binary'
            packet = (json.dumps({'path': 'file/fixture.bin', 'bytes': len(content), 'mtime_ns': 1000}) + '\n').encode()
            import hashlib
            packet += content + (json.dumps({'stable': True, 'sha256': hashlib.sha256(content).hexdigest()}) + '\n').encode() + b'{"done":true}\n'
            command = [sys.executable, str(Path(__file__).parent / 'stream_receiver.py'), str(root), str(status)]
            with file_lock(vault / 'media-transfer.lock', shared=True):
                subprocess.run(command, input=packet, capture_output=True, check=True)
            self.assertEqual((root / 'file/fixture.bin').read_bytes(), content)
            self.assertEqual(read_status(status)['stage'], 'complete')
            before = status.read_bytes()
            with file_lock(vault / 'media-transfer.lock'):
                result = subprocess.run(command, input=packet, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
            self.assertEqual(status.read_bytes(), before)

    def test_fast_scan_excludes_whole_directory_and_counts_unresolved(self):
        class Sink(io.BytesIO):
            def close(self):
                self.saved = self.getvalue()
                super().close()
        class Process:
            def __init__(self, *args, **kwargs):
                self.stdin = Sink()
            def wait(self):
                return 0
        with tempfile.TemporaryDirectory() as tmp:
            state, account = live_fixture(Path(tmp))
            own = Ownership({EXCLUDED})
            own.add_tokens(EXCLUDED, b'a' * 32)
            own.add_tokens(KEPT, b'b' * 32)
            for rel in ('attach/' + chat_hash(EXCLUDED) + '/2026-01/Img/x.dat',
                        'attach/' + chat_hash(KEPT) + '/2026-01/Img/y.dat',
                        'video/2026-01/' + 'a' * 32 + '.mp4',
                        'video/2026-01/' + 'b' * 32 + '.mp4', 'file/2026-01/unknown.bin'):
                path = account / 'msg' / rel
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'fixture')
            def run(command, **kwargs):
                return subprocess.CompletedProcess(command, 0, gzip.compress(b''))
            with patch.object(fast_sync, 'live_ownership', return_value=own), patch.object(subprocess, 'run', side_effect=run), patch.object(subprocess, 'Popen', Process):
                for _ in range(2):
                    result = fast_sync.run(state)
            pending = json.loads((state / 'fast-pending.json').read_text())
            self.assertEqual(len(pending), 3)
            self.assertFalse(any(chat_hash(EXCLUDED) in path for path in pending))
            self.assertEqual(result['exclusion_unresolved'], 1)


if __name__ == '__main__':
    unittest.main()
