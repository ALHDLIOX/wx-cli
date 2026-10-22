#!/usr/bin/env python3
"""Read-only purge planning, then digest-confirmed removal under NAS locks.

Only usernames, paths, counts, sizes, timestamps and hashes leave this module.
SQLite planning uses temporary read replicas, so even WAL databases cannot create
-shm files in the vault during a dry run.
"""
import argparse
from contextlib import contextmanager, ExitStack
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile

from archive_state import atomic_json, file_lock
from build_archive import Decoder
from exclusion import Ownership, chat_hash, ident, load_exclusions


WORK_SPACE_MULTIPLIER = 2.5
SPACE_RESERVE = 2 * 1024 ** 3


class PurgeError(Exception):
    pass


def progress(stage, path=None):
    event = {'stage': stage, 'at': dt.datetime.now(dt.timezone.utc).isoformat()}
    if path is not None:
        event['path'] = str(path)
    print(json.dumps(event), file=sys.stderr, flush=True)


def validate_work_dir(path):
    path = Path(path)
    if not path.is_absolute():
        raise PurgeError('work_dir_not_absolute')
    try:
        info = path.lstat()
    except OSError:
        raise PurgeError('work_dir_not_directory') from None
    if stat.S_ISLNK(info.st_mode):
        raise PurgeError('work_dir_symlink')
    if not stat.S_ISDIR(info.st_mode):
        raise PurgeError('work_dir_not_directory')
    if info.st_uid != os.getuid():
        raise PurgeError('work_dir_owner_mismatch')
    if stat.S_IMODE(info.st_mode) & ~0o700:
        raise PurgeError('work_dir_permissions')
    return path


@contextmanager
def workspace(vault, work_dir, apply):
    """Set SQLite's temporary location before even the planning connections."""
    parent = validate_work_dir(work_dir) if work_dir is not None else vault
    if work_dir is None and not apply:
        yield None
        return
    if work_dir is None:
        safe_path(vault, vault)
    variables = ('SQLITE_TMPDIR', 'TMPDIR') if work_dir is not None else ('SQLITE_TMPDIR',)
    previous = {name: os.environ.get(name) for name in variables}
    previous_tempdir = tempfile.tempdir
    work = Path(tempfile.mkdtemp(prefix='.purge-', dir=parent))
    try:
        work.chmod(0o700)
        for name in variables:
            os.environ[name] = str(work)
        if work_dir is not None:
            tempfile.tempdir = str(work)
        yield work
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        tempfile.tempdir = previous_tempdir
        shutil.rmtree(work)


def check_space(plan, work):
    size = sum(changes.path.stat().st_size for changes in plan.databases)
    if shutil.disk_usage(work).free <= size * WORK_SPACE_MULTIPLIER + SPACE_RESERVE:
        raise PurgeError('insufficient_work_dir_space')
    if shutil.disk_usage(plan.vault).free <= size + SPACE_RESERVE:
        raise PurgeError('insufficient_vault_space')


@contextmanager
def vault_copy(source, destination):
    """Keep every replacement on its destination filesystem, even metadata."""
    expected = sha256(source)
    fd, name = tempfile.mkstemp(prefix='.purge-', suffix='.tmp', dir=destination.parent)
    target = Path(name)
    try:
        with os.fdopen(fd, 'wb') as output, source.open('rb') as stream:
            shutil.copyfileobj(stream, output)
            output.flush()
            os.fsync(output.fileno())
        directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if sha256(target) != expected:
            raise PurgeError('copy_back_hash_mismatch')
        yield target
    finally:
        target.unlink(missing_ok=True)


def signature(path):
    s = path.stat()
    return (s.st_size, s.st_mtime_ns, s.st_ino)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def safe_path(vault, path):
    path = Path(path)
    if not path.is_relative_to(vault):
        raise PurgeError('unsafe_path')
    for part in (path, *path.parents):
        if part == vault.parent:
            break
        if part.is_symlink():
            raise PurgeError('unsafe_path')
    return path


def files_under(vault, root):
    safe_path(vault, root)
    if not root.exists():
        return []
    result = []
    for parent, dirs, files in os.walk(root):
        for name in dirs + files:
            safe_path(vault, Path(parent) / name)
        result.extend(Path(parent) / name for name in files)
    return sorted(result)


@contextmanager
def replica(path):
    """Include committed WAL frames without opening the source via SQLite."""
    wal = Path(str(path) + '-wal')
    if not wal.exists() or wal.stat().st_size == 0:
        before = signature(path)
        db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
        try:
            yield db
            if signature(path) != before or (wal.exists() and wal.stat().st_size):
                raise PurgeError('database_changed_during_plan')
        finally:
            db.close()
        return
    with tempfile.TemporaryDirectory(prefix='wx-purge-read-') as temporary:
        dest = Path(temporary) / 'copy.sqlite3'
        sources = [p for p in (path, Path(str(path) + '-wal')) if p.exists()]
        before = {p: signature(p) for p in sources}
        for source in sources:
            if source.is_symlink():
                raise PurgeError('unsafe_path')
            shutil.copyfile(source, Path(str(dest) + ('-wal' if source != path else '')))
        if before != {p: signature(p) for p in sources} or (Path(str(path) + '-wal').exists() != (len(sources) == 2)):
            raise PurgeError('database_changed_during_plan')
        db = sqlite3.connect(dest)
        try:
            yield db
        finally:
            db.close()


def tables(db):
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def columns(db, table):
    return [row[1] for row in db.execute('PRAGMA table_info(' + ident(table) + ')')]


def placeholders(values):
    return ','.join('?' for _ in values) or 'NULL'


def in_values(column, values):
    values = tuple(sorted(values))
    return ident(column) + ' IN (' + placeholders(values) + ')', values


class DatabasePlan:
    def __init__(self, path, db):
        for sidecar in (path, Path(str(path) + '-wal'), Path(str(path) + '-shm')):
            if sidecar.is_symlink():
                raise PurgeError('unsafe_path')
        self.path, self.actions, self.counts, self.before, self.logical_bytes = path, [], {}, {}, {}
        self.available = tables(db)
        self.selections = []
        self.fingerprint = {str(p): signature(p) for p in (path, Path(str(path) + '-wal')) if p.exists()}

    def delete(self, db, table, where, args=(), drop=False):
        if table not in self.available:
            return
        count = db.execute('SELECT count(*) FROM ' + ident(table) + ' WHERE ' + where, args).fetchone()[0]
        if not count and not drop:
            return
        cols = columns(db, table)
        sizes = '+'.join('coalesce(length(CAST(' + ident(col) + ' AS BLOB)),0)' for col in cols) or '0'
        size = db.execute('SELECT coalesce(sum(' + sizes + '),0) FROM ' + ident(table) + ' WHERE ' + where, args).fetchone()[0]
        self.actions.append((table, where, tuple(args), drop))
        self.counts[table] = count
        self.logical_bytes[table] = size
        self.before[table] = db.execute('SELECT count(*) FROM ' + ident(table)).fetchone()[0]

    def by_users(self, db, table, column, users):
        if table in self.available and (column == 'rowid' or column in columns(db, table)):
            values = tuple(sorted(users))
            if len(values) <= 500:
                self.delete(db, table, *in_values(column, values))
            else:
                temporary = '_purge_keys_' + str(len(self.selections))
                self.selections.append((temporary, values))
                db.execute('CREATE TEMP TABLE ' + temporary + '(value PRIMARY KEY)')
                db.executemany('INSERT INTO ' + temporary + ' VALUES (?)', ((value,) for value in values))
                self.delete(db, table, ident(column) + ' IN (SELECT value FROM ' + temporary + ')')

    def summary(self, vault):
        return {'path': str(self.path.relative_to(vault)), 'before': self.before,
                'delete': self.counts, 'after': {key: value - self.counts[key] for key, value in self.before.items()},
                'row_bytes': self.logical_bytes,
                'bytes_before': sum(value[0] for value in self.fingerprint.values())}


def name_ids(db, table, users):
    if table not in tables(db):
        return set()
    column = next((col for col in ('user_name', 'username') if col in columns(db, table)), None)
    if column is None:
        return set()
    where, args = in_values(column, users)
    return {row[0] for row in db.execute('SELECT rowid FROM ' + ident(table) + ' WHERE ' + where, args)}


def migration_rows(db, ownership):
    """Match Builder.unsupported's group priority, then direct participants.

    Direct migration records name the local account and the peer. A group record
    belongs to the group, even when its individual sender is excluded. Refuse an
    unknown schema or incomplete direct identity rather than leave plaintext.
    """
    if ('UnsupportMessage' not in tables(db)
            or not {'from_user', 'to_user', 'type', 'content'} <= set(columns(db, 'UnsupportMessage'))):
        raise PurgeError('unsupported_migration_schema')
    for rowid, frm, to, kind, raw in db.execute(
            'SELECT rowid,from_user,to_user,type,CAST(content AS BLOB) FROM UnsupportMessage'):
        if any(value is not None and not isinstance(value, str) for value in (frm, to)):
            raise PurgeError('unresolved_migration_ownership')
        group = next((value for value in (frm, to) if value and value.endswith('@chatroom')), None)
        if group:
            user = group
        elif frm and to and frm != to:
            user = next((value for value in (frm, to) if value in ownership.excluded), None)
        else:
            raise PurgeError('unresolved_migration_ownership')
        yield rowid, user, kind, raw


def snapshot_plan(path, db, ownership, notes):
    plan = DatabasePlan(path, db)
    users = ownership.excluded
    name = path.name
    if re.fullmatch(r'(?:biz_)?message_\d+\.db', name):
        for table in sorted(plan.available & ownership.tables):
            plan.delete(db, table, '1', drop=True)
        ids = name_ids(db, 'Name2Id', users)
        for table, column in [('DeleteInfo', 'chat_name_id'), ('DeleteResInfo', 'session_name_id'),
                              ('SendInfo', 'chat_name_id'), ('HistorySysMsgInfo', 'session_name_id'),
                              ('HistoryAddMsgInfo', 'session_name_id'), ('MessageGroupTimeInfo', 'chatname_id')]:
            plan.by_users(db, table, column, ids)
        plan.by_users(db, 'wcdb_builtin_compression_record', 'tableName', ownership.tables)
    elif re.fullmatch(r'media_\d+\.db', name):
        plan.by_users(db, 'VoiceInfo', 'chat_name_id', name_ids(db, 'Name2Id', users))
    elif name == 'message_resource.db':
        ids = name_ids(db, 'ChatName2Id', users)
        where, args = in_values('chat_id', ids)
        if 'MessageResourceDetail' in plan.available:
            plan.delete(db, 'MessageResourceDetail', 'message_id IN (SELECT message_id FROM MessageResourceInfo WHERE ' + where + ')', args)
        plan.by_users(db, 'MessageResourceInfo', 'chat_id', ids)
        for table in ('FtsRange', 'FtsDeleteInfo'):
            plan.by_users(db, table, 'session_id', ids)
    elif name == 'session.db':
        for table in ('SessionTable', 'SessionDeleteTable', 'SessionNoContactInfoTable', 'SessionDraft'):
            plan.by_users(db, table, 'username', users)
        ids = name_ids(db, 'Name2Id', users)
        for table in sorted(plan.available):
            if table.startswith('SessionUnread'):
                plan.by_users(db, table, 'username_id', ids)
    elif name == 'contact.db':
        if 'chat_room' in plan.available:
            where, args = in_values('username', users)
            ids = {row[0] for row in db.execute('SELECT id FROM chat_room WHERE ' + where, args)}
            plan.by_users(db, 'chatroom_member', 'room_id', ids)
        for table in ('contact', 'chat_room'):
            plan.by_users(db, table, 'username', users)
        plan.by_users(db, 'chat_room_info_detail', 'username_', users)
    elif path.parent.name == 'migrate' and name == 'unspportmsg.db':
        ids = {rowid for rowid, user, _, _ in migration_rows(db, ownership) if user in users}
        plan.by_users(db, 'UnsupportMessage', 'rowid', ids)
    elif name == 'hardlink.db':
        ids = set()
        if 'dir2id' in plan.available:
            for rowid, directory in db.execute('SELECT rowid,username FROM dir2id'):
                parts = Path(directory or '').parts
                if (directory in users or directory in ownership.hashes
                        or (len(parts) >= 2 and '..' not in parts and parts[-2] == 'attach'
                            and parts[-1] in ownership.hashes)):
                    ids.add(rowid)
        for table in sorted(plan.available):
            if {'dir1', 'dir2'} <= set(columns(db, table)):
                where, args = in_values('dir1', ids)
                other, other_args = in_values('dir2', ids)
                plan.delete(db, table, where + ' OR ' + other, args + other_args)
                retained = db.execute('SELECT count(*) FROM ' + ident(table)).fetchone()[0] - plan.counts.get(table, 0)
                notes.append({'path': str(path), 'retained_unconfirmed_hardlinks': retained})
    return plan


def archive_plan(path, db, ownership, media_paths):
    plan = DatabasePlan(path, db)
    # Builder.primary keeps an unresolved identity when Name2Id is sparse. The
    # table hash and source provenance still prove which conversation owns it.
    users = ownership.excluded | {'unresolved:' + table for table in ownership.tables}
    where, args = in_values('conversation', users)
    ids = {row[0] for row in db.execute('SELECT id FROM messages WHERE ' + where, args)}
    source_where, source_args = in_values('source_table', ownership.tables)
    for table in ('message_sources', 'superseded_sources'):
        if table in plan.available:
            ids.update(row[0] for row in db.execute('SELECT message_id FROM ' + ident(table) + ' WHERE ' + source_where, source_args))
    for table in ('message_sources', 'superseded_sources'):
        plan.by_users(db, table, 'message_id', ids)
    plan.delete(db, 'index_evidence', where + ' OR ' + source_where, args + source_args)
    if 'resource_messages' in plan.available:
        plan.delete(db, 'resource_details', 'message_resource_id IN (SELECT resource_id FROM resource_messages WHERE ' + where + ')', args)
    for table in ('resource_messages', 'incremental_media_refs'):
        plan.by_users(db, table, 'conversation', users)
    for table in ('incremental_edits', 'source_tables'):
        plan.by_users(db, table, 'source_table', ownership.tables)
    plan.by_users(db, 'messages', 'id', ids)
    for table in ('conversations', 'contacts'):
        plan.by_users(db, table, 'username', users)
    plan.by_users(db, 'media_files', 'path', {'msg/' + path for path in media_paths})
    return plan, ids


def collect_archive(db, ownership):
    available = tables(db)
    if 'incremental_media_refs' in available:
        for user, packed in db.execute('SELECT conversation,tokens FROM incremental_media_refs'):
            ownership.add_tokens(user, packed)
    if 'resource_details' in available:
        for user, data in db.execute('SELECT m.conversation,d.data_index FROM resource_details d LEFT JOIN resource_messages m ON m.resource_id=d.message_resource_id'):
            ownership.add_tokens(user, data)
    if 'messages' in available:
        for user, body, status in db.execute('SELECT conversation,body_text,decode_status FROM messages WHERE type_code IN (3,43,49,62)'):
            ownership.add_message(user, body, status in ('ok', 'empty'))
    for table in ('message_sources', 'superseded_sources'):
        if not {'messages', table} <= available:
            continue
        # A new source can merge into an existing message without being an edit.
        # Its decoded variant has independent references and parsing validity.
        for user, body in db.execute('SELECT m.conversation,s.variant_text FROM ' + ident(table) + ' s '
                                     'JOIN messages m ON m.id=s.message_id '
                                     'WHERE m.type_code IN (3,43,49,62) AND s.variant_text IS NOT NULL'):
            ownership.add_message(user, body)
    if 'incremental_edits' in available:
        decoder = Decoder()
        for (payload,) in db.execute('SELECT payload_json FROM incremental_edits'):
            user = None
            try:
                row = json.loads(payload)
                if not isinstance(row, dict):
                    raise ValueError()
                candidate = row.get('conversation')
                if isinstance(candidate, str) and candidate:
                    user = candidate
                if type(row['kind']) is not int:
                    raise ValueError()
                if (row['kind'] & 0xffffffff) not in (3, 43, 49, 62):
                    continue
                if user is None or type(row['compression']) is not int:
                    raise ValueError()
                raw = bytes.fromhex(row['content_hex'])
                body, status = decoder.decode(raw, row['compression'])
            except (KeyError, ValueError, TypeError):
                # An unreadable edit may be a retained conversation's new file
                # reference. It cannot authorize deleting any original name.
                ownership.add_message(user, None, decoded=False)
                continue
            ownership.add_message(user, body, status in ('ok', 'empty'))


class PurgePlan:
    def __init__(self, vault, users):
        self.vault, self.users = vault, users
        self.databases, self.files, self.notes, self.snapshots = [], set(), [], set()
        self.unresolved = 0
        self.observed = {}

    def observe(self, path):
        self.observed[str(path)] = signature(path)

    def remove(self, path):
        safe_path(self.vault, path)
        if path.exists():
            self.files.add(path)
            self.observe(path)

    def summary(self):
        return {'usernames': sorted(self.users),
                'databases': [plan.summary(self.vault) for plan in self.databases],
                'files': [{'path': str(path.relative_to(self.vault)), 'delete': 1,
                           'bytes': self.observed[str(path)][0], 'before': 1, 'after': 0,
                           'bytes_before': self.observed[str(path)][0], 'bytes_after': 0} for path in sorted(self.files)],
                'exclusion_unresolved': self.unresolved, 'notes': self.notes}

    def digest(self):
        body = {'plan': self.summary(), 'observed': self.observed,
                'databases': [plan.fingerprint for plan in self.databases]}
        return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def make_plan(vault, exclude):
    vault = Path(vault).absolute()
    safe_path(vault, vault)
    users = load_exclusions(exclude)
    if not users:
        raise PurgeError('empty_exclusion')
    ownership = Ownership(users)
    plan = PurgePlan(vault, users)
    archive = safe_path(vault, vault / 'structured/chat-archive.sqlite3')
    if not archive.is_file():
        raise PurgeError('missing_archive')
    snapshots = []
    for path in files_under(vault, vault / 'snapshots'):
        plan.observe(path)
        if path.suffix == '.db' and path.parent.parent.name == 'databases':
            snapshots.append(path)
    with replica(archive) as db:
        collect_archive(db, ownership)
    # Combine every snapshot before classifying shared files, including retained owners.
    table_users = {'Msg_' + chat_hash(user): user for user in users}
    for path in snapshots:
        if re.fullmatch(r'(?:biz_)?message_\d+\.db', path.name):
            with replica(path) as db:
                for user in db.execute('SELECT user_name FROM Name2Id'):
                    if user[0]:
                        table_users['Msg_' + chat_hash(user[0])] = user[0]
    decoder = Decoder()
    for path in snapshots:
        if path.name == 'message_fts.db':
            continue  # MMFtsTokenizer cannot be opened on NAS.
        with replica(path) as db:
            rows = lambda sql: db.execute(sql).fetchall()
            if path.name == 'message_resource.db':
                ownership.collect_resources(rows)
            elif re.fullmatch(r'(?:biz_)?message_\d+\.db', path.name):
                ownership.collect_messages(rows, table_users, decoder)
            elif path.parent.name == 'migrate' and path.name == 'unspportmsg.db':
                for _, user, kind, raw in migration_rows(db, ownership):
                    if kind in (3, 43, 49, 62):
                        body, status = decoder.decode(raw)
                        ownership.add_message(user, body, status in ('ok', 'empty'))
    media_paths = set()
    media = vault / 'media/msg'
    for path in files_under(vault, media):
        plan.observe(path)
        rel = str(path.relative_to(media))
        decision = ownership.decision(rel, path.stat().st_size)
        if decision == 'exclude':
            media_paths.add(rel)
            plan.remove(path)
        elif decision == 'unresolved':
            plan.unresolved += 1
    # Include indexed paths and decoded outputs even when an original is already missing.
    with replica(archive) as db:
        if 'media_files' in tables(db):
            for rel, size in db.execute('SELECT path,bytes FROM media_files'):
                if rel.startswith('msg/') and ownership.decision(rel[4:], size) == 'exclude':
                    media_paths.add(rel[4:])
        archive_changes, message_ids = archive_plan(archive, db, ownership, media_paths)
        plan.databases.append(archive_changes)
    for path in snapshots:
        if path.name == 'message_fts.db':
            plan.remove(path)
            for suffix in ('-wal', '-shm'):
                plan.remove(Path(str(path) + suffix))
            plan.notes.append({'path': str(path), 'removed_private_fts_files': 1,
                               'index_evidence_preserved_in_archive': 1})
            plan.snapshots.add(path.parents[2])
            continue
        with replica(path) as db:
            changes = snapshot_plan(path, db, ownership, plan.notes)
        if changes.actions:
            plan.databases.append(changes)
            plan.snapshots.add(path.parents[2])
    decoded = safe_path(vault, vault / 'decoded-media.sqlite3')
    if decoded.exists():
        with replica(decoded) as db:
            changes = DatabasePlan(decoded, db)
            remove = set()
            for rel, size, output in db.execute('SELECT path,size,output FROM decoded'):
                if rel in media_paths or ownership.decision(rel, size) == 'exclude':
                    remove.add(rel)
                    media_paths.add(rel)
                    if output:
                        # decode_media, decode_recent and convert_wxgf store relative paths.
                        output = Path(output)
                        if output.is_absolute() or '..' in output.parts:
                            raise PurgeError('unsafe_decoded_output')
                        plan.remove(vault / 'decoded' / output)
            changes.by_users(db, 'decoded', 'path', remove)
        if changes.actions:
            plan.databases.append(changes)
    for path in files_under(vault, vault / 'decoded'):
        plan.observe(path)
        rel = str(path.relative_to(vault / 'decoded'))
        if ownership.decision(rel, path.stat().st_size) == 'exclude' or str(Path(rel).with_suffix('')) in media_paths:
            plan.remove(path)
    links = safe_path(vault, vault / 'media-links.sqlite3')
    if links.exists():
        with replica(links) as db:
            changes = DatabasePlan(links, db)
            changes.by_users(db, 'file_tokens', 'chat_hash', ownership.hashes)
            changes.by_users(db, 'message_media', 'message_id', message_ids)
            changes.by_users(db, 'message_media_status', 'message_id', message_ids)
        if changes.actions:
            plan.databases.append(changes)
    # Track all database versions, not only rows scheduled for deletion.
    for path in [archive, *snapshots, decoded, links]:
        for source in (path, Path(str(path) + '-wal')):
            if source.exists():
                plan.observe(source)
    return plan


def refresh_metadata(db, counts, now):
    available = tables(db)
    values = {}
    for field, table in [('messages', 'messages'), ('source_rows', 'message_sources'),
                         ('contacts', 'contacts'), ('index_evidence_rows', 'index_evidence'),
                         ('auxiliary_rows', 'auxiliary_messages'), ('media_files', 'media_files')]:
        if table in available:
            values[field] = db.execute('SELECT count(*) FROM ' + ident(table)).fetchone()[0]
    values['conversations'] = db.execute('SELECT count(*) FROM conversations WHERE message_count>0').fetchone()[0]
    if 'superseded_sources' in available:
        values['source_rows'] += db.execute('SELECT count(*) FROM superseded_sources').fetchone()[0]
    values['source_duplicates'] = values['source_rows'] - values['messages']
    values['sender_unknown'] = db.execute('SELECT count(*) FROM messages WHERE sender IS NULL').fetchone()[0]
    values['unresolved_conversations'] = db.execute("SELECT count(*) FROM conversations WHERE username LIKE 'unresolved:%' AND message_count>0").fetchone()[0]
    values['timestamp_range'] = list(db.execute('SELECT min(timestamp),max(timestamp) FROM messages').fetchone())
    sources = 'SELECT message_id FROM message_sources'
    if 'superseded_sources' in available:
        sources += ' UNION ALL SELECT message_id FROM superseded_sources'
    values['decode_status'] = dict(db.execute('SELECT m.decode_status,count(*) FROM (' + sources + ') s JOIN messages m ON m.id=s.message_id GROUP BY m.decode_status'))
    if 'media_files' in available:
        values['media_bytes'] = db.execute('SELECT coalesce(sum(bytes),0) FROM media_files').fetchone()[0]
    for key, value in values.items():
        db.execute('INSERT OR REPLACE INTO build_metadata VALUES (?,?)', (key, json.dumps(value)))
    db.execute('INSERT INTO build_metadata VALUES (?,?)', ('purge:' + now, json.dumps({'at': now, 'counts': counts})))


def validate(db, fts=False):
    if db.execute('PRAGMA foreign_key_check').fetchall():
        raise PurgeError('foreign_key_check_failed')
    if db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
        raise PurgeError('quick_check_failed')
    if fts:
        db.execute("INSERT INTO message_search(message_search,rank) VALUES('integrity-check',1)")
        db.commit()


def prepare_database(changes, target, now, log=None):
    log = log or (lambda stage: None)
    log('copy_database')
    with replica(changes.path) as src:
        dest = sqlite3.connect(target)
        try:
            src.backup(dest)
        finally:
            dest.close()
    db = sqlite3.connect(target)
    try:
        db.execute('PRAGMA journal_mode=DELETE')
        db.execute('PRAGMA foreign_keys=ON')
        log('delete_rows')
        for temporary, values in changes.selections:
            db.execute('CREATE TEMP TABLE ' + temporary + '(value PRIMARY KEY)')
            db.executemany('INSERT INTO ' + temporary + ' VALUES (?)', ((value,) for value in values))
        for table, where, args, drop in changes.actions:
            sql = 'DROP TABLE ' + ident(table) if drop else 'DELETE FROM ' + ident(table) + ' WHERE ' + where
            db.execute(sql, () if drop else args)
        is_archive = changes.path.name == 'chat-archive.sqlite3'
        if is_archive:
            log('rebuild_fts')
            db.execute("INSERT INTO message_search(message_search) VALUES('rebuild')")
            refresh_metadata(db, changes.counts, now)
        db.commit()
        log('validate_database')
        validate(db, is_archive)
        log('vacuum')
        db.execute('VACUUM')
        log('validate_database')
        validate(db, is_archive)
    finally:
        db.close()
    target.chmod(0o600)


def running_workers(vault, runner=None):
    runner = runner or subprocess.run
    for name in ('decode_media', 'convert_wxgf', 'index_media', 'link_media'):
        pattern = re.escape(str(vault / 'tools' / (name + '.py'))) + r'(\s|$)'
        result = runner(['pgrep', '-f', pattern], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        if result.returncode == 0:
            raise PurgeError('media_worker_running')
        if result.returncode != 1:
            raise PurgeError('process_check_failed')


def read_snapshot_metadata(vault, snapshot):
    metadata = {}
    for name in ('manifest.json', 'verified.json', 'purge.json'):
        path = safe_path(vault, snapshot / name)
        if name == 'purge.json':
            continue
        try:
            value = json.loads(path.read_text()) if path.exists() else {}
        except (ValueError, UnicodeError):
            raise PurgeError('invalid_snapshot_manifest') from None
        if name == 'manifest.json' and isinstance(value, list):
            seen = set()
            for entry in value:
                if not isinstance(entry, dict):
                    raise PurgeError('invalid_snapshot_manifest')
                rel = entry.get('path')
                size = entry.get('bytes')
                digest = entry.get('sha256')
                if (not isinstance(rel, str) or not rel
                        or Path(rel).is_absolute() or '..' in Path(rel).parts
                        or Path(rel) in seen
                        or type(size) is not int or size < 0
                        or not isinstance(digest, str)
                        or re.fullmatch('[0-9a-fA-F]{64}', digest) is None):
                    raise PurgeError('invalid_snapshot_manifest')
                try:
                    entry_path = safe_path(vault, snapshot / 'databases' / rel)
                    if not stat.S_ISREG(entry_path.lstat().st_mode):
                        raise PurgeError('invalid_snapshot_manifest')
                except (OSError, PurgeError):
                    raise PurgeError('invalid_snapshot_manifest') from None
                seen.add(Path(rel))
        elif not isinstance(value, dict):
            raise PurgeError('invalid_snapshot_manifest')
        metadata[name] = value
    return metadata


def prepare_snapshot(snapshot, counts, now, old_hashes, staged_paths, removed_paths, metadata):
    hashes = {str(path.relative_to(snapshot)): {'sha256': sha256(staged_paths.get(path, path)),
                                              'bytes': staged_paths.get(path, path).stat().st_size}
              for path in sorted((snapshot / 'databases').rglob('*.db')) if path not in removed_paths}
    replacements = {}
    removed_hashes = set()
    for rel, old in old_hashes.items():
        if rel in hashes:
            replacements[old] = hashes[rel]['sha256']
        else:
            removed_hashes.add(old)

    aliases = {}
    for rel, item in hashes.items():
        for alias in (rel, rel.removeprefix('databases/'), str(snapshot / rel)):
            aliases[alias] = item
    path_fields = ('path', 'file', 'relative_path', 'database')

    def removed(value):
        if isinstance(value, str):
            return value.lower() in removed_hashes or value.endswith('message_fts.db')
        if isinstance(value, dict):
            return any(isinstance(value.get(key), str) and value[key].endswith('message_fts.db')
                       for key in path_fields)
        return False

    def rewrite(value, path_hint=None):
        item = aliases.get(path_hint)
        if isinstance(value, str):
            return item['sha256'] if item and re.fullmatch('[0-9a-fA-F]{64}', value) else replacements.get(value.lower(), value)
        if isinstance(value, list):
            return [rewrite(entry) for entry in value if not removed(entry)]
        if isinstance(value, dict):
            hint = next((value[key] for key in path_fields
                         if isinstance(value.get(key), str) and value[key] in aliases), path_hint)
            item = aliases.get(hint)
            result = {key: rewrite(entry, key if key in aliases else None)
                      for key, entry in value.items()
                      if not key.endswith('message_fts.db') and not removed(entry)}
            if item:
                for key in ('sha256', 'hash'):
                    if key in result:
                        result[key] = item['sha256']
                for key in ('size', 'bytes'):
                    if key in result:
                        result[key] = item['bytes']
            return result
        return value

    purged = {'at': now, 'counts': counts}
    audit = {'database_hashes': hashes, 'purged': purged}
    manifest = metadata['manifest.json']
    result = {}
    if isinstance(manifest, list):
        manifest = [dict(entry, bytes=staged_paths.get(path, path).stat().st_size,
                         sha256=sha256(staged_paths.get(path, path)))
                    for entry in manifest
                    for path in [snapshot / 'databases' / entry['path']]
                    if path not in removed_paths]
        result['purge.json'] = audit
        manifest_hashes = {str(Path(entry['path'])): {'bytes': entry['bytes'], 'sha256': entry['sha256']}
                           for entry in manifest}
    else:
        manifest = rewrite(manifest)
        manifest.update(audit)
        manifest_hashes = manifest['database_hashes']
    # List manifests cover all snapshot files, including non-database files.
    # Never carry forward an earlier verification result.
    actual_paths = (sorted(path for path in (snapshot / 'databases').rglob('*')
                           if stat.S_ISREG(path.lstat().st_mode) and path not in removed_paths)
                    if isinstance(manifest, list) else [snapshot / rel for rel in hashes])
    actual = {str(path.relative_to(snapshot)): {'sha256': sha256(staged_paths.get(path, path)),
                                              'bytes': staged_paths.get(path, path).stat().st_size}
              for path in actual_paths}
    if isinstance(manifest, list):
        actual = {rel.removeprefix('databases/'): item for rel, item in actual.items()}
    verified = dict(metadata['verified.json'])
    verified.update(audit)
    verified.update(files=len(manifest_hashes), bytes=sum(item['bytes'] for item in manifest_hashes.values()),
                    sha256_all_match=manifest_hashes == actual)
    result.update({'manifest.json': manifest, 'verified.json': verified})
    return result


def execute(vault, exclude, apply=False, confirm=None, process_runner=None, work_dir=None):
    vault = Path(vault).absolute()
    if apply and os.geteuid() != 0:
        raise PurgeError('root_required')
    with workspace(vault, work_dir, apply) as work:
        return execute_in_workspace(vault, exclude, apply, confirm, process_runner, work,
                                    external_work=work_dir is not None)


def execute_in_workspace(vault, exclude, apply, confirm, process_runner, work, external_work):
    if not apply:
        plan = make_plan(vault, exclude)
        return dict(plan.summary(), dry_run=True, plan_digest=plan.digest())
    with ExitStack() as locks:
        safe_path(vault, vault / 'structured/chat-archive.sqlite3.ingest.lock')
        safe_path(vault, vault / 'media-links.sqlite3.lock')
        locks.enter_context(file_lock(vault / 'structured/chat-archive.sqlite3.ingest.lock'))
        locks.enter_context(file_lock(vault / 'media-links.sqlite3.lock'))
        safe_path(vault, vault / 'media-transfer.lock')
        locks.enter_context(file_lock(vault / 'media-transfer.lock'))
        running_workers(vault, process_runner)
        plan = make_plan(vault, exclude)
        digest = plan.digest()
        if not confirm or confirm != digest:
            raise PurgeError('plan_digest_mismatch')
        if external_work:
            check_space(plan, work)
        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec='microseconds')
        report = dict(plan.summary(), dry_run=False, plan_digest=digest, at=now)
        old_hashes = {snapshot: {str(path.relative_to(snapshot)): sha256(path)
                                for path in (snapshot / 'databases').rglob('*.db')}
                      for snapshot in plan.snapshots}
        # Stage and validate every database and manifest before publishing any change.
        metadata = {snapshot: read_snapshot_metadata(vault, snapshot)
                    for snapshot in plan.snapshots}
        with ExitStack() as copies:
            staged = []
            for number, changes in enumerate(plan.databases):
                target = Path(work) / str(number)
                relative = changes.path.relative_to(vault)
                progress('prepare_database', relative)
                prepare_database(changes, target, now, lambda stage: progress(stage, relative))
                staged.append((changes, target))
            staged_paths = {changes.path: target for changes, target in staged}
            staged_metadata = []
            for number, snapshot in enumerate(sorted(plan.snapshots)):
                counts = {str(changes.path.relative_to(snapshot)): changes.counts for changes in plan.databases
                          if changes.path.is_relative_to(snapshot)}
                counts.update({str(path.relative_to(snapshot)): {'files': 1, 'bytes': plan.observed[str(path)][0]}
                               for path in plan.files if path.is_relative_to(snapshot)})
                values = prepare_snapshot(snapshot, counts, now, old_hashes[snapshot],
                                          staged_paths, plan.files, metadata[snapshot])
                for name, value in values.items():
                    target = Path(work) / (str(number) + '-' + name)
                    atomic_json(target, value)
                    staged_metadata.append((snapshot / name, target))
            if external_work:
                local_databases = []
                for changes, target in staged:
                    progress('copy_back', changes.path.relative_to(vault))
                    local_databases.append((changes, copies.enter_context(vault_copy(target, changes.path))))
                local_metadata = []
                for path, target in staged_metadata:
                    progress('copy_back', path.relative_to(vault))
                    local_metadata.append((path, copies.enter_context(vault_copy(target, path))))
                staged, staged_metadata = local_databases, local_metadata
            running_workers(vault, process_runner)
            # Reject changes during preparation; the two locks exclude ingest/link.
            if any(not Path(path).exists() or signature(Path(path)) != tuple(value)
                   for path, value in plan.observed.items()):
                raise PurgeError('vault_changed_during_purge')
            for changes, target in staged:
                progress('publish_database', changes.path.relative_to(vault))
                os.replace(target, changes.path)
                for suffix in ('-wal', '-shm'):
                    sidecar = Path(str(changes.path) + suffix)
                    safe_path(vault, sidecar)
                    sidecar.unlink(missing_ok=True)
            for path in sorted(plan.files):
                # Media filenames can contain names from the exclusion list.
                progress('delete_media')
                path.unlink()
            for path, target in staged_metadata:
                progress('publish_metadata', path.relative_to(vault))
                os.replace(target, path)
        for root in (vault / 'media/msg', vault / 'decoded'):
            if root.exists():
                for parent, dirs, files in os.walk(root, topdown=False):
                    for directory in dirs:
                        path = Path(parent) / directory
                        if not any(path.iterdir()):
                            path.rmdir()
        released = 0
        for item, changes in zip(report['databases'], plan.databases):
            item['bytes_after'] = changes.path.stat().st_size
            item['released_bytes'] = item['bytes_before'] - item['bytes_after']
            released += item['released_bytes']
        released += sum(item['bytes'] for item in report['files'])
        report['released_bytes'] = released
        stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        report_path = vault / ('purge-' + stamp + '.json')
        progress('write_report')
        atomic_json(report_path, report)
        return dict(report, report_path=str(report_path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vault', type=Path, required=True)
    parser.add_argument('--exclude', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--confirm')
    parser.add_argument('--work-dir', type=Path,
                        help='existing private directory for database preparation and SQLite temporary files')
    args = parser.parse_args()
    # SIGINT already raises KeyboardInterrupt; make SIGTERM unwind the same finally blocks.
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)
    previous_handler = signal.signal(signal.SIGTERM, interrupted)
    try:
        result = execute(args.vault, args.exclude, args.apply, args.confirm, work_dir=args.work_dir)
    except Exception as error:
        # Never print SQLite/JSON/OS exception text, which may contain private data.
        code = str(error) if isinstance(error, PurgeError) else type(error).__name__
        print(json.dumps({'error': code}))
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
