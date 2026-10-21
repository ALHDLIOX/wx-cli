from contextlib import redirect_stdout, closing
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from archive_state import atomic_json, file_lock
from build_archive import Builder
from exclusion import Ownership, chat_hash
from ingest_delta import ingest
from purge_conversations import execute, prepare_database, PurgeError, replica, sha256, snapshot_plan
from test_sync_hardening import EXCLUDED, KEPT, write_exclude


def database(path, sql):
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript(sql)
    return db


def app(filename):
    return f'<msg><appmsg><type>6</type><title>{filename}</title><appattach><totallen>5</totallen></appattach></appmsg></msg>'


def message_row(user, kind, body, rowid):
    return dict(source_db='message/message_0.db', table='Msg_' + chat_hash(user),
                rowid=rowid, conversation=user, sender=user, kind=kind,
                content_hex=body.encode().hex(), compression=0, server_id=rowid,
                timestamp=1, local_id=rowid, sort_seq=1)


def fixture(root):
    vault = root / 'vault'
    vault.mkdir()
    exclude = root / 'exclude.json'
    write_exclude(exclude)
    snap = vault / 'snapshots/fixture'
    source = snap / 'databases'
    for name in ('message_0.db', 'biz_message_0.db'):
        db = database(source / 'message' / name, 'CREATE TABLE Name2Id(user_name TEXT);')
        for number, user in enumerate((EXCLUDED, KEPT), 1):
            db.execute('INSERT INTO Name2Id VALUES (?)', (user,))
            table = 'Msg_' + chat_hash(user)
            db.execute(f'CREATE TABLE "{table}"(local_id INTEGER,server_id INTEGER,local_type INTEGER,sort_seq INTEGER,real_sender_id INTEGER,create_time INTEGER,message_content TEXT,WCDB_CT_message_content INTEGER)')
            db.execute(f'INSERT INTO "{table}" VALUES (1,1,49,1,1,1,?,0)', (app('exclusive.bin' if user == EXCLUDED else 'shared.bin'),))
        for table, column in [('DeleteInfo', 'chat_name_id'), ('DeleteResInfo', 'session_name_id'), ('SendInfo', 'chat_name_id'), ('HistorySysMsgInfo', 'session_name_id'), ('HistoryAddMsgInfo', 'session_name_id'), ('MessageGroupTimeInfo', 'chatname_id')]:
            db.execute(f'CREATE TABLE {table}({column} INTEGER,payload BLOB)')
            db.executemany(f'INSERT INTO {table} VALUES (?,?)', [(1, b'PRIVATE'), (2, b'kept')])
        db.commit()
        db.close()
    db = database(source / 'message/media_0.db', 'CREATE TABLE Name2Id(user_name TEXT);CREATE TABLE VoiceInfo(chat_name_id INTEGER,voice_data BLOB);')
    db.executemany('INSERT INTO Name2Id VALUES (?)', [(EXCLUDED,), (KEPT,)])
    db.executemany('INSERT INTO VoiceInfo VALUES (?,?)', [(1, b'PRIVATE'), (2, b'kept')])
    db.commit()
    db.close()
    db = database(source / 'message/message_resource.db', '''
CREATE TABLE ChatName2Id(user_name TEXT);
CREATE TABLE MessageResourceInfo(message_id INTEGER PRIMARY KEY,chat_id INTEGER,packed_info BLOB);
CREATE TABLE MessageResourceDetail(resource_id INTEGER PRIMARY KEY,message_id INTEGER,packed_info BLOB);
CREATE TABLE FtsRange(session_id INTEGER);
CREATE TABLE FtsDeleteInfo(session_id INTEGER);
''')
    db.executemany('INSERT INTO ChatName2Id VALUES (?)', [(EXCLUDED,), (KEPT,)])
    db.executemany('INSERT INTO MessageResourceInfo VALUES (?,?,?)', [(1, 1, b'a' * 32 + b' ' + b'b' * 32), (2, 2, b'b' * 32)])
    db.executemany('INSERT INTO MessageResourceDetail VALUES (?,?,?)', [(100, 1, b'a' * 32), (200, 2, b'b' * 32)])
    for table in ('FtsRange', 'FtsDeleteInfo'):
        db.executemany('INSERT INTO ' + table + ' VALUES (?)', [(1,), (2,)])
    db.commit()
    db.close()
    (source / 'message/message_fts.db').write_bytes(b'fixture opaque private tokenizer database')
    db = database(source / 'session/session.db', 'CREATE TABLE Name2Id(user_name TEXT);')
    db.executemany('INSERT INTO Name2Id VALUES (?)', [(EXCLUDED,), (KEPT,)])
    for table in ('SessionTable', 'SessionDeleteTable', 'SessionNoContactInfoTable', 'SessionDraft'):
        db.execute('CREATE TABLE ' + table + '(username TEXT,payload TEXT)')
        db.executemany('INSERT INTO ' + table + ' VALUES (?,?)', [(EXCLUDED, 'PRIVATE'), (KEPT, 'kept')])
    for table in ('SessionUnreadListTable_1', 'SessionUnreadStatTable_1'):
        db.execute('CREATE TABLE ' + table + '(username_id INTEGER)')
        db.executemany('INSERT INTO ' + table + ' VALUES (?)', [(1,), (2,)])
    db.commit()
    db.close()
    db = database(source / 'contact/contact.db', 'CREATE TABLE name2id(username TEXT);CREATE TABLE contact(username TEXT);CREATE TABLE chat_room(id INTEGER,username TEXT);CREATE TABLE chat_room_info_detail(room_id_ INTEGER,username_ TEXT);CREATE TABLE chatroom_member(room_id INTEGER,member_id INTEGER);')
    for number, user in enumerate((EXCLUDED, KEPT, 'fixture_member'), 1):
        db.execute('INSERT INTO name2id VALUES (?)', (user,))
        db.execute('INSERT INTO contact VALUES (?)', (user,))
        if number <= 2:
            db.execute('INSERT INTO chat_room VALUES (?,?)', (number, user))
            db.execute('INSERT INTO chat_room_info_detail VALUES (?,?)', (number, user))
            db.execute('INSERT INTO chatroom_member VALUES (?,3)', (number,))
    db.commit()
    db.close()
    db = database(source / 'hardlink/hardlink.db', 'CREATE TABLE dir2id(username TEXT);')
    db.executemany('INSERT INTO dir2id VALUES (?)', [(chat_hash(EXCLUDED),), (chat_hash(KEPT),), ('unconfirmed',)])
    for table in ('image_hardlink_info_v4', 'file_hardlink_info_v4', 'video_hardlink_info_v4'):
        db.execute('CREATE TABLE ' + table + '(dir1 INTEGER,dir2 INTEGER)')
        db.executemany('INSERT INTO ' + table + ' VALUES (?,?)', [(0, 1), (0, 2), (0, 3)])
    db.commit()
    db.close()
    archive = vault / 'structured/chat-archive.sqlite3'
    with redirect_stdout(io.StringIO()):
        builder = Builder(source, archive, 'fixture_self')
        entries = [(EXCLUDED, 1, 'PRIVATE archived body'), (EXCLUDED, 49, app('exclusive.bin')),
                   (EXCLUDED, 49, app('shared.bin')), (KEPT, 49, app('shared.bin')),
                   (KEPT, 1, 'retained body')]
        for number, (user, kind, body) in enumerate(entries, 1):
            builder.add(user, user, 'name2id', number, 1, kind, 0, body, 'ok', 'message/message_0.db', 'Msg_' + chat_hash(user), number, number, 1, 0, body)
        for user in (EXCLUDED, KEPT):
            builder.db.execute('INSERT INTO contacts VALUES (?,NULL,NULL,NULL,NULL)', (user,))
            builder.db.execute('INSERT INTO source_tables VALUES (?,?,1,1)', ('message/message_0.db', 'Msg_' + chat_hash(user)))
            builder.db.execute('INSERT INTO index_evidence VALUES (?,1,?,NULL,1,1,1,?)', ('index_' + user, user, 'PRIVATE' if user == EXCLUDED else 'kept'))
        builder.db.executemany('INSERT INTO resource_messages VALUES (?,?,NULL,1,1,1,43)', [(1, EXCLUDED), (2, KEPT)])
        builder.db.executemany('INSERT INTO resource_details VALUES (?,?,43,5,?)', [(100, 1, 'a' * 32), (200, 2, 'b' * 32)])
        builder.finish()
    db = sqlite3.connect(archive)
    db.executescript('CREATE TABLE incremental_edits(source_db TEXT,source_table TEXT,source_rowid INTEGER,content_sha256 TEXT,payload_json TEXT,observed_at REAL,PRIMARY KEY(source_db,source_table,source_rowid,content_sha256));CREATE TABLE incremental_media_refs(conversation TEXT,local_id INTEGER,timestamp INTEGER,server_id TEXT,tokens TEXT,PRIMARY KEY(conversation,local_id,timestamp,server_id));')
    for user, token in ((EXCLUDED, 'a' * 32), (KEPT, 'b' * 32)):
        row = message_row(user, 1, 'PRIVATE' if user == EXCLUDED else 'kept', 1)
        db.execute('INSERT INTO incremental_edits VALUES (?,?,?,?,?,1)',
                   (row['source_db'], row['table'], row['rowid'], 'fixture', json.dumps(row)))
        db.execute('INSERT INTO incremental_media_refs VALUES (?,1,1,?,?)', (user, '1', json.dumps([token])))
    paths = ['attach/' + chat_hash(EXCLUDED) + '/2026-01/Img/original.dat',
             'attach/' + chat_hash(KEPT) + '/2026-01/Img/original.dat',
             'video/2026-01/' + 'a' * 32 + '.mp4', 'video/2026-01/' + 'a' * 32 + '_thumb.jpg',
             'video/2026-01/' + 'b' * 32 + '.mp4',
             'file/2026-01/exclusive.bin', 'file/2026-01/shared.bin', 'file/2026-01/unknown.bin']
    for rel in paths:
        path = vault / 'media/msg' / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'bytes')
        db.execute('INSERT INTO media_files VALUES (?,5,1,?,?)', ('msg/' + rel, path.suffix, 'fixture'))
    db.commit()
    excluded_ids = {row[0] for row in db.execute('SELECT id FROM messages WHERE conversation=?', (EXCLUDED,))}
    kept_id = db.execute('SELECT id FROM messages WHERE conversation=? LIMIT 1', (KEPT,)).fetchone()[0]
    db.close()
    db = database(vault / 'decoded-media.sqlite3', 'CREATE TABLE decoded(path TEXT PRIMARY KEY,size,mtime,status,output,sha256);')
    for rel in paths[:2]:
        output = rel + '.jpg'
        target = vault / 'decoded' / output
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b'decoded')
        db.execute('INSERT INTO decoded VALUES (?,5,1,?,?,?)', (rel, 'ok', output, 'fixture'))
    db.commit()
    db.close()
    db = database(vault / 'media-links.sqlite3', 'CREATE TABLE file_tokens(chat_hash TEXT,token TEXT,path TEXT);CREATE TABLE message_media(message_id INTEGER,path TEXT);CREATE TABLE message_media_status(message_id INTEGER,status TEXT);')
    for user, rel in zip((EXCLUDED, KEPT), paths[:2]):
        db.execute('INSERT INTO file_tokens VALUES (?,?,?)', (chat_hash(user), 'a' * 32, 'msg/' + rel))
    for mid in excluded_ids | {kept_id}:
        db.execute('INSERT INTO message_media VALUES (?,?)', (mid, 'fixture'))
        db.execute('INSERT INTO message_media_status VALUES (?,?)', (mid, 'matched'))
    db.commit()
    db.close()
    hashes = {str(path.relative_to(snap)): sha256(path) for path in source.rglob('*.db')}
    atomic_json(snap / 'manifest.json', {'files': hashes})
    atomic_json(snap / 'verified.json', {'files': [{'path': rel, 'sha256': value} for rel, value in hashes.items()]})
    return vault, exclude, snap, excluded_ids


def no_process(command, **kwargs):
    return subprocess.CompletedProcess(command, 1)


def tree_state(root):
    return {str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns, sha256(path))
            for path in root.rglob('*') if path.is_file()}


class PurgeTests(unittest.TestCase):
    def test_contact_members_use_chat_room_ids(self):
        for has_chat_room in (True, False):
            with self.subTest(has_chat_room=has_chat_room), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'contact.db'
                excluded, kept = 'excluded@chatroom', 'kept@chatroom'
                with closing(database(path, '''
CREATE TABLE name2id(username TEXT);
CREATE TABLE chatroom_member(room_id INTEGER,member_id INTEGER);
''')) as db:
                    db.executemany('INSERT INTO name2id(rowid,username) VALUES (?,?)',
                                   [(1, excluded), (2, kept)])
                    members = [(20, 3), (20, 4), (1, 5), (1, 6)]
                    db.executemany('INSERT INTO chatroom_member VALUES (?,?)', members)
                    if has_chat_room:
                        db.execute('CREATE TABLE chat_room(id INTEGER,username TEXT)')
                        db.executemany('INSERT INTO chat_room VALUES (?,?)',
                                       [(20, excluded), (1, kept)])
                    db.commit()
                    plan = snapshot_plan(path, db, Ownership({excluded}), [])
                target = Path(tmp) / 'purged.db'
                prepare_database(plan, target, 'fixture')
                with closing(sqlite3.connect(target)) as db:
                    self.assertEqual(db.execute('SELECT * FROM chatroom_member ORDER BY room_id,member_id').fetchall(),
                                     [(1, 5), (1, 6)] if has_chat_room else sorted(members))

    def test_dry_run_changes_nothing_and_reports_no_body(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, snap, _ = fixture(Path(tmp))
            before = tree_state(vault)
            result = execute(vault, exclude)
            self.assertEqual(tree_state(vault), before)
            self.assertEqual(result['plan_digest'], execute(vault, exclude)['plan_digest'])
            self.assertTrue(result['dry_run'])
            self.assertEqual(result['exclusion_unresolved'], 1)
            self.assertNotIn('PRIVATE', json.dumps(result))
            self.assertEqual(len(result['files']), 6)

    def test_cli_dry_run_returns_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            before = tree_state(vault)
            result = subprocess.run([sys.executable, str(Path(__file__).parent / 'purge_conversations.py'),
                                     '--vault', str(vault), '--exclude', str(exclude)],
                                    capture_output=True, text=True, check=True)
            self.assertTrue(json.loads(result.stdout)['dry_run'])
            self.assertNotIn('PRIVATE', result.stdout)
            self.assertEqual(tree_state(vault), before)

    def test_apply_archive_snapshots_media_and_manifests(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, snap, removed_ids = fixture(Path(tmp))
            digest = execute(vault, exclude)['plan_digest']
            with patch('purge_conversations.os.geteuid', return_value=0):
                result = execute(vault, exclude, True, digest, no_process)
            self.assertFalse(result['dry_run'])
            self.assertNotIn('PRIVATE', json.dumps(result))
            self.assertGreater(result['released_bytes'], 0)
            report = Path(result['report_path'])
            self.assertEqual(report.stat().st_mode & 0o777, 0o600)
            archive = vault / 'structured/chat-archive.sqlite3'
            with closing(sqlite3.connect(archive)) as db:
                self.assertEqual(db.execute('SELECT DISTINCT conversation FROM messages').fetchall(), [(KEPT,)])
                self.assertEqual(db.execute('SELECT count(*) FROM message_sources').fetchone()[0], 2)
                self.assertEqual(db.execute("SELECT count(*) FROM message_search WHERE message_search MATCH 'PRIVATE'").fetchone()[0], 0)
                self.assertEqual(db.execute('SELECT message_resource_id FROM resource_details').fetchall(), [(2,)])
                self.assertEqual(db.execute('SELECT conversation FROM incremental_media_refs').fetchall(), [(KEPT,)])
                self.assertEqual(db.execute('SELECT username FROM contacts').fetchall(), [(KEPT,)])
                self.assertEqual(db.execute('SELECT conversation FROM index_evidence').fetchall(), [(KEPT,)])
                self.assertEqual(db.execute('SELECT source_table FROM source_tables').fetchall(), [('Msg_' + chat_hash(KEPT),)])
                self.assertEqual(db.execute('SELECT source_table FROM incremental_edits').fetchall(), [('Msg_' + chat_hash(KEPT),)])
                self.assertEqual(json.loads(db.execute("SELECT value FROM build_metadata WHERE key='messages'").fetchone()[0]), 2)
                self.assertEqual(db.execute("SELECT count(*) FROM build_metadata WHERE key LIKE 'purge:%'").fetchone()[0], 1)
                self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
                self.assertEqual(db.execute('PRAGMA freelist_count').fetchone()[0], 0)
                db.execute("INSERT INTO message_search(message_search,rank) VALUES('integrity-check',1)")
                db.commit()
            self.assertFalse((vault / 'media/msg/attach' / chat_hash(EXCLUDED)).exists())
            self.assertFalse((vault / 'decoded/attach' / chat_hash(EXCLUDED)).exists())
            self.assertTrue((vault / 'media/msg/video/2026-01' / ('b' * 32 + '.mp4')).exists())
            self.assertFalse((vault / 'media/msg/video/2026-01' / ('a' * 32 + '.mp4')).exists())
            self.assertTrue((vault / 'media/msg/file/2026-01/shared.bin').exists())
            self.assertTrue((vault / 'media/msg/file/2026-01/unknown.bin').exists())
            self.assertFalse((vault / 'media/msg/file/2026-01/exclusive.bin').exists())
            with closing(sqlite3.connect(vault / 'media-links.sqlite3')) as db:
                self.assertEqual(db.execute('SELECT DISTINCT chat_hash FROM file_tokens').fetchall(), [(chat_hash(KEPT),)])
                for table in ('message_media', 'message_media_status'):
                    self.assertFalse(removed_ids & {row[0] for row in db.execute('SELECT message_id FROM ' + table)})
                self.assertEqual(db.execute('PRAGMA freelist_count').fetchone()[0], 0)
            with closing(sqlite3.connect(vault / 'decoded-media.sqlite3')) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM decoded').fetchone()[0], 1)
                self.assertEqual(db.execute('PRAGMA freelist_count').fetchone()[0], 0)
            source = snap / 'databases'
            for name in ('message_0.db', 'biz_message_0.db'):
                with closing(sqlite3.connect(source / 'message' / name)) as db:
                    self.assertIsNone(db.execute('SELECT name FROM sqlite_master WHERE name=?', ('Msg_' + chat_hash(EXCLUDED),)).fetchone())
                    self.assertIsNotNone(db.execute('SELECT name FROM sqlite_master WHERE name=?', ('Msg_' + chat_hash(KEPT),)).fetchone())
                    for table in ('DeleteInfo', 'DeleteResInfo', 'SendInfo', 'HistorySysMsgInfo', 'HistoryAddMsgInfo', 'MessageGroupTimeInfo'):
                        self.assertEqual(db.execute('SELECT * FROM ' + table).fetchall(), [(2, b'kept')])
            with closing(sqlite3.connect(source / 'message/message_resource.db')) as db:
                self.assertEqual(db.execute('SELECT message_id FROM MessageResourceInfo').fetchall(), [(2,)])
                self.assertEqual(db.execute('SELECT resource_id FROM MessageResourceDetail').fetchall(), [(200,)])
                for table in ('FtsRange', 'FtsDeleteInfo'):
                    self.assertEqual(db.execute('SELECT session_id FROM ' + table).fetchall(), [(2,)])
            with closing(sqlite3.connect(source / 'message/media_0.db')) as db:
                self.assertEqual(db.execute('SELECT chat_name_id FROM VoiceInfo').fetchall(), [(2,)])
            with closing(sqlite3.connect(source / 'session/session.db')) as db:
                for table in ('SessionTable', 'SessionDeleteTable', 'SessionNoContactInfoTable', 'SessionDraft'):
                    self.assertEqual(db.execute('SELECT username FROM ' + table).fetchall(), [(KEPT,)])
                for table in ('SessionUnreadListTable_1', 'SessionUnreadStatTable_1'):
                    self.assertEqual(db.execute('SELECT username_id FROM ' + table).fetchall(), [(2,)])
            with closing(sqlite3.connect(source / 'contact/contact.db')) as db:
                self.assertEqual({row[0] for row in db.execute('SELECT username FROM contact')}, {KEPT, 'fixture_member'})
                self.assertEqual(db.execute('SELECT room_id FROM chatroom_member').fetchall(), [(2,)])
                self.assertEqual(db.execute('SELECT username FROM chat_room').fetchall(), [(KEPT,)])
            with closing(sqlite3.connect(source / 'hardlink/hardlink.db')) as db:
                for table in ('image_hardlink_info_v4', 'file_hardlink_info_v4', 'video_hardlink_info_v4'):
                    self.assertEqual(db.execute('SELECT dir2 FROM ' + table).fetchall(), [(2,), (3,)])
            self.assertFalse((source / 'message/message_fts.db').exists())
            for name in ('manifest.json', 'verified.json'):
                value = json.loads((snap / name).read_text())
                self.assertIn('purged', value)
                self.assertNotIn('message_fts.db', json.dumps(value['files']))
                self.assertNotIn('message_fts.db', json.dumps(value['database_hashes']))
                self.assertEqual(value['purged']['counts']['databases/message/message_fts.db']['files'], 1)
                for rel, item in value['database_hashes'].items():
                    self.assertEqual(sha256(snap / rel), item['sha256'])
            for path in source.rglob('*.db'):
                with closing(sqlite3.connect(path)) as db:
                    self.assertEqual(db.execute('PRAGMA quick_check').fetchall(), [('ok',)])
                    self.assertEqual(db.execute('PRAGMA freelist_count').fetchone()[0], 0)
            self.assertFalse(list(vault.glob('.purge-*')))

    def test_changed_digest_rejected_without_deletions(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            digest = execute(vault, exclude)['plan_digest']
            (vault / 'media/msg/file/2026-01/new.bin').write_bytes(b'fixture')
            before = sha256(vault / 'structured/chat-archive.sqlite3')
            with patch('purge_conversations.os.geteuid', return_value=0), self.assertRaisesRegex(PurgeError, 'plan_digest_mismatch'):
                execute(vault, exclude, True, digest, no_process)
            self.assertEqual(before, sha256(vault / 'structured/chat-archive.sqlite3'))
            self.assertFalse(list(vault.glob('purge-*.json')))

    def test_incremental_edits_preserve_shared_file_and_video(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            archive = vault / 'structured/chat-archive.sqlite3'
            # The retained conversation's snapshot and canonical row still name
            # shared.bin; only ingest's edit evidence names exclusive.bin.
            edits = [message_row(KEPT, 49, app('exclusive.bin'), 4),
                     message_row(KEPT, 43, '<msg><videomsg md5="' + 'a' * 32 + '"/></msg>', 5)]
            result = ingest(archive, {'self_user': 'fixture_self', 'rows': edits})
            self.assertEqual(result['edits_observed'], 2)
            with closing(sqlite3.connect(archive)) as db:
                self.assertEqual(db.execute('SELECT body_text FROM messages WHERE conversation=? AND type_code=49', (KEPT,)).fetchall(),
                                 [(app('shared.bin'),)])
            plan = execute(vault, exclude)
            removed = {item['path'] for item in plan['files']}
            self.assertNotIn('media/msg/file/2026-01/exclusive.bin', removed)
            self.assertNotIn('media/msg/video/2026-01/' + 'a' * 32 + '.mp4', removed)
            with patch('purge_conversations.os.geteuid', return_value=0):
                execute(vault, exclude, True, plan['plan_digest'], no_process)
            for rel in ('file/2026-01/exclusive.bin', 'video/2026-01/' + 'a' * 32 + '.mp4',
                        'video/2026-01/' + 'a' * 32 + '_thumb.jpg'):
                self.assertTrue((vault / 'media/msg' / rel).is_file())
                with closing(sqlite3.connect(archive)) as db:
                    self.assertIsNotNone(db.execute('SELECT path FROM media_files WHERE path=?', ('msg/' + rel,)).fetchone())

    def test_source_variants_preserve_shared_file_and_video(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            archive = vault / 'structured/chat-archive.sqlite3'
            canonical_video = '<msg><videomsg/></msg>'
            ingest(archive, {'self_user': 'fixture_self',
                             'rows': [message_row(KEPT, 43, canonical_video, 6)]})
            # New source rows share canonical message keys, so their different
            # bodies live only in variant_text, never in incremental_edits.
            variants = [dict(message_row(KEPT, 49, app('exclusive.bin'), 40), server_id=4),
                        dict(message_row(KEPT, 43, '<msg><videomsg md5="' + 'a' * 32 + '"/></msg>', 41), server_id=6)]
            result = ingest(archive, {'self_user': 'fixture_self', 'rows': variants})
            self.assertEqual(result['source_rows_added'], 2)
            self.assertEqual(result['edits_observed'], 0)
            with closing(sqlite3.connect(archive)) as db:
                self.assertEqual(db.execute('SELECT body_text FROM messages WHERE conversation=? AND type_code IN (43,49) ORDER BY type_code', (KEPT,)).fetchall(),
                                 [(canonical_video,), (app('shared.bin'),)])
                self.assertEqual(db.execute('SELECT variant_text FROM message_sources WHERE source_rowid IN (40,41) ORDER BY source_rowid').fetchall(),
                                 [(bytes.fromhex(row['content_hex']).decode(),) for row in variants])
                self.assertEqual(db.execute('SELECT count(*) FROM incremental_edits').fetchone()[0], 2)
            before = tree_state(vault)
            plan = execute(vault, exclude)
            self.assertEqual(tree_state(vault), before)
            self.assertEqual(plan['exclusion_unresolved'], 1)
            paths = ('file/2026-01/exclusive.bin', 'video/2026-01/' + 'a' * 32 + '.mp4',
                     'video/2026-01/' + 'a' * 32 + '_thumb.jpg')
            removed = {item['path'] for item in plan['files']}
            for rel in paths:
                self.assertNotIn('media/msg/' + rel, removed)
            with patch('purge_conversations.os.geteuid', return_value=0):
                execute(vault, exclude, True, plan['plan_digest'], no_process)
            with closing(sqlite3.connect(archive)) as db:
                for rel in paths:
                    self.assertEqual((vault / 'media/msg' / rel).read_bytes(), b'bytes')
                    self.assertIsNotNone(db.execute('SELECT path FROM media_files WHERE path=?', ('msg/' + rel,)).fetchone())

    def test_unparseable_source_variant_checks_each_original_file(self):
        bodies = ['PRIVATE malformed variant', '<msg><appmsg>',
                  app('exclusive.bin').replace('<totallen>5</totallen>', '<totallen>unknown</totallen>')]
        for body in bodies:
            with self.subTest(body=body), tempfile.TemporaryDirectory() as tmp:
                vault, exclude, _, _ = fixture(Path(tmp))
                archive = vault / 'structured/chat-archive.sqlite3'
                variant = dict(message_row(KEPT, 49, body, 40), server_id=4)
                result = ingest(archive, {'self_user': 'fixture_self', 'rows': [variant]})
                self.assertEqual(result['source_rows_added'], 1)
                self.assertEqual(result['edits_observed'], 0)
                with closing(sqlite3.connect(archive)) as db:
                    self.assertEqual(db.execute('SELECT variant_text FROM message_sources WHERE source_rowid=40').fetchall(), [(body,)])
                before = tree_state(vault)
                plan = execute(vault, exclude)
                self.assertEqual(tree_state(vault), before)
                retained = 'exclusive.bin' in body
                self.assertEqual('media/msg/file/2026-01/exclusive.bin' in
                                 {item['path'] for item in plan['files']}, not retained)
                self.assertEqual(plan['exclusion_unresolved'], 1)
                self.assertNotIn('PRIVATE', json.dumps(plan))
                with patch('purge_conversations.os.geteuid', return_value=0):
                    result = execute(vault, exclude, True, plan['plan_digest'], no_process)
                self.assertNotIn('PRIVATE', json.dumps(result))
                self.assertEqual((vault / 'media/msg/file/2026-01/exclusive.bin').is_file(), retained)
                with closing(sqlite3.connect(archive)) as db:
                    indexed = db.execute('SELECT path FROM media_files WHERE path=?',
                                         ('msg/file/2026-01/exclusive.bin',)).fetchone()
                    self.assertEqual(indexed is not None, retained)

    def test_unreadable_edit_checks_each_original_file(self):
        row = message_row(KEPT, 49, app('exclusive.bin'), 4)
        payloads = ['PRIVATE invalid JSON', json.dumps([]), json.dumps({'kind': 49}),
                    json.dumps(dict(row, content_hex='invalid hex')),
                    json.dumps(dict(row, compression=None)),
                    json.dumps(dict(row, kind='49')),
                    json.dumps(dict(row, content_hex='ff')),
                    json.dumps(dict(row, compression=4)),
                    json.dumps(dict(row, content_hex=b'<msg><appmsg>'.hex()))]
        for payload in payloads:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmp:
                vault, exclude, _, _ = fixture(Path(tmp))
                archive = vault / 'structured/chat-archive.sqlite3'
                with closing(sqlite3.connect(archive)) as db:
                    db.execute('UPDATE incremental_edits SET payload_json=? WHERE source_table=?',
                               (payload, 'Msg_' + chat_hash(KEPT)))
                    db.commit()
                before = tree_state(vault)
                plan = execute(vault, exclude)
                self.assertEqual(before, tree_state(vault))
                # A decoded malformed XML without this filename is unrelated.
                retained = payload != payloads[-1]
                self.assertEqual('media/msg/file/2026-01/exclusive.bin' in
                                 {item['path'] for item in plan['files']}, not retained)
                self.assertEqual(plan['exclusion_unresolved'], 2 if retained else 1)
                self.assertNotIn('PRIVATE', json.dumps(plan))
                with patch('purge_conversations.os.geteuid', return_value=0):
                    execute(vault, exclude, True, plan['plan_digest'], no_process)
                self.assertEqual((vault / 'media/msg/file/2026-01/exclusive.bin').is_file(), retained)

    def test_excluded_unreadable_edits_do_not_block_file_exclusion(self):
        row = message_row(EXCLUDED, 49, app('exclusive.bin'), 4)
        payloads = [dict(row, content_hex='invalid hex'), dict(row, content_hex='ff'),
                    dict(row, compression=None), dict(row, kind='49')]
        for payload in payloads:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmp:
                vault, exclude, _, _ = fixture(Path(tmp))
                archive = vault / 'structured/chat-archive.sqlite3'
                with closing(sqlite3.connect(archive)) as db:
                    db.execute('UPDATE incremental_edits SET payload_json=? WHERE source_table=?',
                               (json.dumps(payload), 'Msg_' + chat_hash(EXCLUDED)))
                    db.commit()
                plan = execute(vault, exclude)
                self.assertIn('media/msg/file/2026-01/exclusive.bin',
                              {item['path'] for item in plan['files']})
                self.assertEqual(plan['exclusion_unresolved'], 1)

    def test_primary_unresolved_conversation_is_completely_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, snap, _ = fixture(Path(tmp))
            source = snap / 'databases'
            table = 'Msg_' + chat_hash(EXCLUDED)
            alias = 'unresolved:' + table
            for name in ('message_0.db', 'biz_message_0.db'):
                with closing(sqlite3.connect(source / 'message' / name)) as db:
                    db.execute('DELETE FROM Name2Id WHERE user_name=?', (EXCLUDED,))
                    db.execute('UPDATE "' + table + '" SET message_content=?', ('PRIVATE unresolved body',))
                    db.commit()
            archive = vault / 'structured/chat-archive.sqlite3'
            archive.unlink()
            with redirect_stdout(io.StringIO()):
                builder = Builder(source, archive, 'fixture_self')
                builder.primary()  # Reproduce Builder's sparse Name2Id fallback.
                builder.db.execute('INSERT INTO index_evidence VALUES (?,1,?,NULL,1,1,1,?)',
                                   (table, alias, 'PRIVATE unresolved evidence'))
                builder.db.execute('INSERT INTO resource_messages VALUES (1,?,NULL,1,1,1,43)', (alias,))
                builder.db.execute('INSERT INTO resource_details VALUES (1,1,43,5,?)', ('a' * 32,))
                removed_ids = {row[0] for row in builder.db.execute('SELECT id FROM messages WHERE conversation=?', (alias,))}
                self.assertTrue(removed_ids)
                builder.finish()
            ingest(archive, {'self_user': 'fixture_self', 'rows': [], 'resource_refs': [[alias, 1, 1, '1', 'a' * 32]]})
            with closing(sqlite3.connect(vault / 'media-links.sqlite3')) as db:
                for table_name in ('message_media', 'message_media_status'):
                    db.execute('DELETE FROM ' + table_name)
                for mid in removed_ids:
                    db.execute('INSERT INTO message_media VALUES (?,?)', (mid, 'fixture'))
                    db.execute('INSERT INTO message_media_status VALUES (?,?)', (mid, 'matched'))
                db.commit()
            before = tree_state(vault)
            plan = execute(vault, exclude)
            self.assertEqual(before, tree_state(vault))
            with patch('purge_conversations.os.geteuid', return_value=0):
                execute(vault, exclude, True, plan['plan_digest'], no_process)
            with closing(sqlite3.connect(archive)) as db:
                self.assertEqual(db.execute('SELECT DISTINCT conversation FROM messages').fetchall(), [(KEPT,)])
                self.assertEqual(db.execute('SELECT username FROM conversations').fetchall(), [(KEPT,)])
                for table_name in ('index_evidence', 'resource_messages', 'resource_details', 'incremental_media_refs'):
                    self.assertEqual(db.execute('SELECT count(*) FROM ' + table_name).fetchone()[0], 0)
                for table_name in ('message_sources', 'source_tables'):
                    self.assertEqual(db.execute('SELECT count(*) FROM ' + table_name + ' WHERE source_table=?', (table,)).fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT count(*) FROM message_search WHERE message_search MATCH 'PRIVATE'").fetchone()[0], 0)
                self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
                self.assertEqual(db.execute('PRAGMA freelist_count').fetchone()[0], 0)
                db.execute("INSERT INTO message_search(message_search,rank) VALUES('integrity-check',1)")
                db.commit()
            with closing(sqlite3.connect(vault / 'media-links.sqlite3')) as db:
                for table_name in ('message_media', 'message_media_status'):
                    self.assertEqual(db.execute('SELECT count(*) FROM ' + table_name).fetchone()[0], 0)

    def test_migration_snapshot_direct_and_group_ownership(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, snap, _ = fixture(Path(tmp))
            excluded_group, kept_group = 'fixture_excluded@chatroom', 'fixture_kept@chatroom'
            write_exclude(exclude, (EXCLUDED, excluded_group))
            path = snap / 'databases/migrate/unspportmsg.db'
            db = database(path, 'CREATE TABLE UnsupportMessage(svr_id INTEGER PRIMARY KEY,type INTEGER,sub_type INTEGER,from_user TEXT,to_user TEXT,content TEXT,create_time INTEGER,sequent_id INTEGER);')
            entries = [(EXCLUDED, 'fixture_self', 'PRIVATE incoming'),
                       ('fixture_self', EXCLUDED, 'PRIVATE outgoing'),
                       (excluded_group, KEPT, 'PRIVATE group'),
                       (KEPT, excluded_group, 'PRIVATE group reverse'),
                       (EXCLUDED, kept_group, 'retained group'),
                       (kept_group, EXCLUDED, 'retained group reverse'),
                       (KEPT, 'fixture_self', app('exclusive.bin'))]
            db.executemany('INSERT INTO UnsupportMessage VALUES (?,49,0,?,?,?,2,1)',
                           ((number, *entry) for number, entry in enumerate(entries, 100)))
            db.commit()
            db.close()
            # Include actual Builder.unsupported archive copies as well.
            archive = vault / 'structured/chat-archive.sqlite3'
            archive.unlink()
            with redirect_stdout(io.StringIO()):
                builder = Builder(snap / 'databases', archive, 'fixture_self')
                builder.primary()
                builder.unsupported()
                builder.finish()
            before = tree_state(vault)
            plan = execute(vault, exclude)
            self.assertEqual(before, tree_state(vault))
            migration = next(item for item in plan['databases'] if item['path'].endswith('migrate/unspportmsg.db'))
            self.assertEqual(migration['delete']['UnsupportMessage'], 4)
            with patch('purge_conversations.os.geteuid', return_value=0):
                result = execute(vault, exclude, True, plan['plan_digest'], no_process)
            self.assertNotIn('PRIVATE', json.dumps(result))
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute('SELECT svr_id FROM UnsupportMessage ORDER BY svr_id').fetchall(), [(104,), (105,), (106,)])
                self.assertEqual(db.execute('PRAGMA quick_check').fetchall(), [('ok',)])
                self.assertEqual(db.execute('PRAGMA freelist_count').fetchone()[0], 0)
            with closing(sqlite3.connect(archive)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM message_search WHERE message_search MATCH 'PRIVATE'").fetchone()[0], 0)
            self.assertTrue((vault / 'media/msg/file/2026-01/exclusive.bin').is_file())
            for name in ('manifest.json', 'verified.json'):
                manifest = json.loads((snap / name).read_text())
                self.assertEqual(manifest['database_hashes']['databases/migrate/unspportmsg.db']['sha256'], sha256(path))
                self.assertEqual(manifest['purged']['counts']['databases/migrate/unspportmsg.db']['UnsupportMessage'], 4)

    def test_unknown_migration_schema_or_identity_refuses_without_deletions(self):
        schemas = [('CREATE TABLE Unknown(content TEXT);', 'unsupported_migration_schema'),
                   ('CREATE TABLE UnsupportMessage(from_user TEXT,to_user TEXT);', 'unsupported_migration_schema'),
                   ('CREATE TABLE UnsupportMessage(from_user TEXT,to_user TEXT,type INTEGER,content TEXT);'
                    "INSERT INTO UnsupportMessage VALUES (NULL,NULL,1,'PRIVATE');", 'unresolved_migration_ownership')]
        for schema, error in schemas:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as tmp:
                vault, exclude, snap, _ = fixture(Path(tmp))
                path = snap / 'databases/migrate/unspportmsg.db'
                database(path, schema).close()
                before = tree_state(vault)
                with self.assertRaisesRegex(PurgeError, error):
                    execute(vault, exclude)
                self.assertEqual(before, tree_state(vault))
                with patch('purge_conversations.os.geteuid', return_value=0), self.assertRaisesRegex(PurgeError, error):
                    execute(vault, exclude, True, 'unused', no_process)
                self.assertEqual(before, {key: value for key, value in tree_state(vault).items() if key in before})
                self.assertFalse(list(vault.glob('purge-*.json')))
                self.assertFalse(list(vault.glob('.purge-*')))

    def test_running_workers_and_locks_refuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            digest = execute(vault, exclude)['plan_digest']
            for name in ('decode_media', 'convert_wxgf', 'index_media', 'link_media'):
                def busy(command, **kwargs):
                    self.assertEqual(command[:2], ['pgrep', '-f'])
                    return subprocess.CompletedProcess(command, 0 if name in command[2] else 1)
                with patch('purge_conversations.os.geteuid', return_value=0), self.assertRaisesRegex(PurgeError, 'media_worker_running'):
                    execute(vault, exclude, True, digest, busy)
            for lock in ('structured/chat-archive.sqlite3.ingest.lock', 'media-links.sqlite3.lock', 'media-transfer.lock'):
                with file_lock(vault / lock), patch('purge_conversations.os.geteuid', return_value=0), self.assertRaises(BlockingIOError):
                    execute(vault, exclude, True, digest, no_process)
            self.assertFalse(list(vault.glob('purge-*.json')))

    def test_large_message_selection_and_validation_failure_are_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            archive = vault / 'structured/chat-archive.sqlite3'
            # More than one SQL parameter batch; no bind limit per deleted message.
            with closing(sqlite3.connect(archive)) as db:
                for number in range(600):
                    mid = 1000 + number
                    db.execute('INSERT INTO messages(id,message_key,conversation,body_text,decode_status) VALUES (?,?,?,?,?)',
                               (mid, str(mid), EXCLUDED, 'PRIVATE fixture', 'ok'))
                    db.execute('INSERT INTO message_sources(message_id,source_db,source_table,source_rowid) VALUES (?,?,?,?)',
                               (mid, 'message/message_0.db', 'Msg_' + chat_hash(EXCLUDED), mid))
                db.execute("INSERT INTO message_search(message_search) VALUES('rebuild')")
                db.commit()
            digest = execute(vault, exclude)['plan_digest']
            before = sha256(archive)
            with patch('purge_conversations.os.geteuid', return_value=0):
                with patch('purge_conversations.validate', side_effect=PurgeError('quick_check_failed')):
                    with self.assertRaisesRegex(PurgeError, 'quick_check_failed'):
                        execute(vault, exclude, True, digest, no_process)
                self.assertEqual(sha256(archive), before)
                execute(vault, exclude, True, digest, no_process)
            with closing(sqlite3.connect(archive)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM messages').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT count(*) FROM message_sources').fetchone()[0], 2)

    def test_wal_read_replica_does_not_touch_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = sqlite3.connect(root / 'wal.db')
            try:
                db.execute('PRAGMA journal_mode=WAL')
                db.execute('CREATE TABLE fixture(value)')
                db.execute('INSERT INTO fixture VALUES (1)')
                db.commit()
                before = tree_state(root)
                with replica(root / 'wal.db') as copy:
                    self.assertEqual(copy.execute('SELECT value FROM fixture').fetchall(), [(1,)])
                self.assertEqual(before, tree_state(root))
            finally:
                db.close()

    def test_unsafe_decoded_path_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            vault, exclude, _, _ = fixture(Path(tmp))
            with closing(sqlite3.connect(vault / 'decoded-media.sqlite3')) as db:
                db.execute("UPDATE decoded SET output='../outside' WHERE path LIKE ?", ('attach/' + chat_hash(EXCLUDED) + '/%',))
                db.commit()
            with self.assertRaisesRegex(PurgeError, 'unsafe_decoded_output'):
                execute(vault, exclude)


if __name__ == '__main__':
    unittest.main()
