import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from archive_state import atomic_json, file_lock
from build_archive import Builder
from ingest_delta import ingest
from nas import heartbeat


class HeartbeatTests(unittest.TestCase):
    def test_rules_dedup_recovery_and_privacy(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            atomic_json(vault / 'heartbeat-config.json', {'target': 'weixin:fixture', 'stale_minutes': 60, 'error_threshold': 5})
            workers = {name: {'stage': 'error', 'consecutive_errors': 5, 'error_type': 'RuntimeError',
                              'error_summary': 'PRIVATE chat content'}
                       for name in ('incremental', 'recent-media', 'media')}
            atomic_json(vault / 'mac-heartbeat.json', {'received_at': 1000, 'mac_status': {'wechat_running': False, 'workers': workers}})
            sent = []
            sender = lambda target, text: sent.append((target, text))
            self.assertEqual(heartbeat.run(vault, 5000, sender)['sent'], 5)
            self.assertTrue(any('已中断 66 分钟' in text for _, text in sent))
            self.assertTrue(any('Mac 上微信未运行' == text for _, text in sent))
            for name in workers:
                self.assertTrue(any('worker ' + name in text for _, text in sent))
            self.assertNotIn('PRIVATE', json.dumps(sent))
            self.assertEqual(heartbeat.run(vault, 5300, sender)['sent'], 0)
            for worker in workers.values():
                worker['consecutive_errors'] = 0
            atomic_json(vault / 'mac-heartbeat.json', {'received_at': 5400, 'mac_status': {'wechat_running': True, 'workers': workers}})
            self.assertEqual(heartbeat.run(vault, 5600, sender)['sent'], 5)
            self.assertEqual(sum('已恢复' in text for _, text in sent), 5)
            self.assertTrue(any('中断了 77 分钟' in text for _, text in sent))
            self.assertEqual(heartbeat.run(vault, 5700, sender)['sent'], 0)
            workers['media']['consecutive_errors'] = 6
            workers['media']['error_summary'] = 'RuntimeError: remote_decode_failed'
            atomic_json(vault / 'mac-heartbeat.json', {'received_at': 5800, 'mac_status': {'wechat_running': True, 'workers': workers}})
            self.assertEqual(heartbeat.run(vault, 5800, sender)['sent'], 1)
            self.assertIn('remote_decode_failed', sent[-1][1])

    def test_failed_sender_preserves_entire_state_and_retries(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            atomic_json(vault / 'heartbeat-config.json', {'target': 'weixin:fixture'})
            atomic_json(vault / 'mac-heartbeat.json', {'received_at': 1000, 'mac_status': {'wechat_running': True}})
            heartbeat.run(vault, 1000, lambda *args: None)
            before = (vault / 'heartbeat-state.json').read_bytes()
            def fail(*args):
                raise subprocess.CalledProcessError(1, ['fixture'])
            with self.assertRaises(subprocess.CalledProcessError):
                heartbeat.run(vault, 5000, fail)
            self.assertEqual((vault / 'heartbeat-state.json').read_bytes(), before)
            self.assertEqual(heartbeat.run(vault, 5000, lambda *args: None)['sent'], 1)

    def test_legacy_packet_missing_heartbeat_and_threshold(self):
        config = {'stale_minutes': 60, 'error_threshold': 5}
        state, messages = heartbeat.evaluate({}, {}, config, 10000)
        self.assertEqual(len(messages), 1)
        self.assertEqual(heartbeat.evaluate({}, state, config, 10300)[1], [])
        state, messages = heartbeat.evaluate({'received_at': 10400}, state, config, 10400)
        self.assertEqual(len(messages), 1)
        self.assertIn('已恢复', messages[0])
        _, messages = heartbeat.evaluate({'received_at': 1, 'mac_status': {'workers': {'media': {'consecutive_errors': 4}}}}, {}, config, 2)
        self.assertEqual(messages, [])

    def test_command_uses_argument_list(self):
        with patch.object(subprocess, 'run') as runner:
            heartbeat.send('weixin:fixture', '固定文本')
        self.assertEqual(runner.call_args.args[0], ['hermes', 'send', '-t', 'weixin:fixture', '-q', '固定文本'])
        self.assertNotIn('shell', runner.call_args.kwargs)

    def test_ingest_zero_rows_legacy_atomic_heartbeat_and_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault = Path(tmp)
            (vault / 'structured').mkdir()
            archive = vault / 'structured/chat-archive.sqlite3'
            Builder(vault, archive, 'fixture_self').finish()
            packet = {'self_user': 'fixture_self', 'rows': [], 'mac_status': {'wechat_running': False}}
            with patch('ingest_delta.time.time', return_value=1234):
                ingest(archive, packet)
            value = json.loads((vault / 'mac-heartbeat.json').read_text())
            self.assertEqual(value, {'received_at': 1234, 'mac_status': packet['mac_status']})
            self.assertEqual((vault / 'mac-heartbeat.json').stat().st_mode & 0o777, 0o600)
            with file_lock(str(archive) + '.ingest.lock'):
                with self.assertRaises(BlockingIOError):
                    ingest(archive, packet)
            ingest(archive, {'self_user': 'fixture_self', 'rows': []})
            self.assertEqual(json.loads((vault / 'mac-heartbeat.json').read_text())['mac_status'], {})


if __name__ == '__main__':
    unittest.main()
