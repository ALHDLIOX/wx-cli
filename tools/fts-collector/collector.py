#!/usr/bin/env python3
"""Read-only primary-message and FTS capture; only locally available records."""
import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

REL = 'message/message_fts.db'
TABLE = re.compile(r'message_fts_v4_\d+_content\Z')


class Source:
    def __init__(self, library, path, key):
        if not re.fullmatch('[0-9a-fA-F]{64}', key):
            raise ValueError('invalid key format')
        self.lib = l = C.CDLL(str(library))
        signatures = {
            'sqlite3_open_v2': ([C.c_char_p, C.POINTER(C.c_void_p), C.c_int, C.c_char_p], C.c_int),
            'sqlite3_key': ([C.c_void_p, C.c_char_p, C.c_int], C.c_int),
            'sqlite3_close': ([C.c_void_p], C.c_int),
            'sqlite3_busy_timeout': ([C.c_void_p, C.c_int], C.c_int),
            'sqlite3_prepare_v2': ([C.c_void_p, C.c_char_p, C.c_int, C.POINTER(C.c_void_p), C.c_void_p], C.c_int),
            'sqlite3_step': ([C.c_void_p], C.c_int),
            'sqlite3_finalize': ([C.c_void_p], C.c_int),
            'sqlite3_column_count': ([C.c_void_p], C.c_int),
            'sqlite3_column_type': ([C.c_void_p, C.c_int], C.c_int),
            'sqlite3_column_int64': ([C.c_void_p, C.c_int], C.c_int64),
            'sqlite3_column_text': ([C.c_void_p, C.c_int], C.c_void_p),
            'sqlite3_column_bytes': ([C.c_void_p, C.c_int], C.c_int),
        }
        for name, (args, result) in signatures.items():
            fn = getattr(l, name); fn.argtypes = args; fn.restype = result
        self.db = C.c_void_p()
        rc = l.sqlite3_open_v2(os.fsencode(path), C.byref(self.db), 1, None)
        try:
            self.check(rc)
            raw = ("x'" + key + "'").encode()
            self.check(l.sqlite3_key(self.db, raw, len(raw)))
            self.check(l.sqlite3_busy_timeout(self.db, 3000))
            self.rows('PRAGMA query_only=ON')
            self.rows('SELECT count(*) FROM sqlite_master')
        except Exception:
            self.close()
            raise

    @staticmethod
    def check(rc):
        if rc != 0:
            raise RuntimeError(f'SQLCipher operation failed ({rc})')

    def rows(self, sql):
        stmt = C.c_void_p(); out = []; l = self.lib
        try:
            self.check(l.sqlite3_prepare_v2(self.db, sql.encode(), -1, C.byref(stmt), None))
            while True:
                rc = l.sqlite3_step(stmt)
                if rc == 101:
                    return out
                if rc != 100:
                    self.check(rc)
                row = []
                for i in range(l.sqlite3_column_count(stmt)):
                    kind = l.sqlite3_column_type(stmt, i)
                    if kind == 5:
                        value = None
                    elif kind == 1:
                        value = l.sqlite3_column_int64(stmt, i)
                    else:
                        ptr = l.sqlite3_column_text(stmt, i)
                        value = C.string_at(ptr, l.sqlite3_column_bytes(stmt, i)).decode('utf-8', 'replace') if ptr else ''
                    row.append(value)
                out.append(row)
        finally:
            if stmt:
                l.sqlite3_finalize(stmt)

    def close(self):
        if self.db:
            self.lib.sqlite3_close(self.db); self.db = None


def archive(path):
    db = sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS records (
        source TEXT NOT NULL, table_name TEXT NOT NULL, identity TEXT NOT NULL,
        source_rowid INTEGER NOT NULL, payload TEXT NOT NULL,
        first_observed REAL NOT NULL, updated_at REAL NOT NULL,
        PRIMARY KEY(source,table_name,identity));
      CREATE TABLE IF NOT EXISTS cursors (
        source TEXT NOT NULL, table_name TEXT NOT NULL, rowid INTEGER NOT NULL,
        PRIMARY KEY(source,table_name));
    ''')
    return db


def capture(src, dst, source_id, full=False, primary=False):
    pattern = re.compile(r'Msg_[0-9a-f]{32}\Z') if primary else TABLE
    tables = [r[0] for r in src.rows("SELECT name FROM sqlite_master WHERE type='table'") if pattern.fullmatch(r[0])]
    if not tables and not primary:
        raise RuntimeError('unsupported FTS schema: no content tables')
    names = dict(src.rows('SELECT rowid,user_name FROM Name2Id' if primary else 'SELECT rowid,username FROM name2id'))
    table_users = {'Msg_'+hashlib.md5(u.encode()).hexdigest():u for u in names.values() if u}
    columns = ('rowid,hex(message_content),local_id,sort_seq,local_type,server_id,real_sender_id,create_time,WCDB_CT_message_content'
               if primary else 'rowid,c0,c1,c2,c3,c4,c5,c6')
    scanned = changed = 0
    for table in tables:
        old = dst.execute('SELECT rowid FROM cursors WHERE source=? AND table_name=?', (source_id, table)).fetchone()
        cursor = -1 if full or not old else old[0]
        while True:
            rows = src.rows(f'SELECT {columns} FROM "{table}" WHERE rowid>{int(cursor)} ORDER BY rowid LIMIT 500')
            if not rows:
                break
            before = dst.total_changes
            # Archive rows and cursor together: a crash cannot advance past unsaved data.
            with dst:
                for row in rows:
                    rid, content, local_id, seq, kind, session, sender, stamp = row[:8]
                    extra = {}
                    username = names.get(session)
                    if primary:
                        raw_hex = content
                        ct = row[8]
                        username = table_users.get(table)
                        extra = dict(server_id=session, content_hex=raw_hex, compression=ct,
                                     content_decoded=ct != 4)
                        content = bytes.fromhex(raw_hex).decode('utf-8','replace') if ct != 4 else None
                        session = table
                    payload = json.dumps(dict(content=content, local_id=local_id, sort_seq=seq,
                        type_code=kind, session_id=session, username=username,
                        sender_id=sender, sender=names.get(sender), timestamp=stamp,
                        source='message_primary' if primary else 'message_fts', **extra),
                        ensure_ascii=False, sort_keys=True)
                    identity = hashlib.sha256(json.dumps([rid,local_id,seq,session,stamp]).encode()).hexdigest()
                    now = time.time()
                    dst.execute('''INSERT INTO records VALUES (?,?,?,?,?,?,?)
                        ON CONFLICT(source,table_name,identity) DO UPDATE SET
                        payload=excluded.payload,updated_at=excluded.updated_at
                        WHERE records.payload!=excluded.payload''',
                        (source_id,table,identity,rid,payload,now,now))
                changed += dst.total_changes - before
                cursor = rows[-1][0]
                dst.execute('''INSERT INTO cursors VALUES (?,?,?) ON CONFLICT(source,table_name)
                    DO UPDATE SET rowid=MAX(cursors.rowid,excluded.rowid)''', (source_id,table,cursor))
            scanned += len(rows)
    return {'scanned':scanned, 'changed':changed, 'tables':len(tables)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, default=Path.home()/'.wx-cli/config.json')
    p.add_argument('--library', type=Path, required=True)
    p.add_argument('--output', type=Path, default=Path.home()/'.wx-cli/collector')
    p.add_argument('--poll', type=float, default=2)
    p.add_argument('--once', action='store_true')
    args = p.parse_args()
    if args.poll <= 0:
        p.error('--poll must be positive')
    os.umask(0o077)
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(args.output, 0o700)
    # Prevent competing collectors from racing cursor/status writes.
    import fcntl
    lock = (args.output/'collector.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    dst = archive(args.output/'archive.sqlite3')
    full_at = 0
    while True:
        start = time.time(); src = None
        try:
            cfg = json.loads(args.config.read_text())
            keys = json.loads(Path(cfg['keys_file']).read_text())
            full = start - full_at >= 60
            rels = sorted(k for k in keys if k == REL or re.fullmatch(r'message/message_\d+\.db', k))
            discovered = {str(p.relative_to(cfg['db_dir'])) for p in Path(cfg['db_dir']).glob('message/message_*.db')
                          if p.name == 'message_fts.db' or re.fullmatch(r'message_\d+\.db',p.name)}
            unknown = sorted(discovered-set(rels))
            result = dict(scanned=0, changed=0, tables=0, sources={}, unknown_shards=unknown)
            for rel in rels:
                try:
                    key = keys[rel]; key = key if isinstance(key,str) else key['enc_key']
                    path = Path(cfg['db_dir'])/rel
                    with path.open('rb') as f:
                        salt = f.read(16)
                    source_id = hashlib.sha256(os.fsencode(path.resolve())+salt).hexdigest()
                    src = Source(args.library, path, key)
                    src.rows('BEGIN')
                    part = capture(src, dst, source_id, full, primary=rel != REL)
                    for field in ('scanned','changed','tables'):
                        result[field] += part[field]
                    part.update(status='ok', source_latest_write=max(q.stat().st_mtime
                        for q in (path,Path(str(path)+'-wal')) if q.exists()))
                    result['sources'][rel] = part
                except Exception as e:
                    result['sources'][rel] = dict(status='error', error_type=type(e).__name__)
                finally:
                    if src:
                        src.close(); src = None
            good = bool(rels) and not unknown and all(x['status']=='ok' for x in result['sources'].values())
            if full and good:
                full_at = start
            result.update(status='ok' if good else 'partial_error', checked_at=time.time(), full_scan=full,
                total_records=dst.execute('SELECT count(*) FROM records').fetchone()[0],
                scope='local primary messages and FTS index; source rows are distinct', poll_seconds=args.poll)
        except Exception as e:
            # Never put source paths, keys or chat content in journal/status errors.
            result = dict(status='error', checked_at=time.time(), error_type=type(e).__name__)
        finally:
            if src:
                src.close()
        tmp = args.output/'status.tmp'
        tmp.write_text(json.dumps(result,ensure_ascii=False,indent=2))
        tmp.replace(args.output/'status.json')
        if args.once:
            print(json.dumps(result)); return 0 if result['status']=='ok' else 1
        time.sleep(max(0.1, args.poll-(time.time()-start)))


if __name__ == '__main__':
    raise SystemExit(main())
