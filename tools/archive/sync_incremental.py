#!/usr/bin/env python3
"""Send durable, idempotent batches; advance cursors only after NAS commits."""
import argparse
from contextlib import closing
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time

from archive_state import atomic_bytes, file_lock, mac_status, read_status, worker_cycle
from exclusion import Ownership, load_exclusions, tokens


def source_class():
    spec = importlib.util.spec_from_file_location('collector', Path(__file__).parent.parent / 'fts-collector/collector.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Source


def wechat_is_running():
    return subprocess.run(['/usr/bin/pgrep', '-x', 'WeChat'], stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL, check=False).returncode == 0


def cycle(root):
    root = Path(root)
    with file_lock(root / 'incremental.lock'):
        return worker_cycle(root, 'incremental', lambda: _cycle(root),
                            {'wechat_running': wechat_is_running()})


def _cycle(root):
    excluded = Ownership(load_exclusions(root / 'exclude.json'))
    state = json.loads((root / 'state.json').read_text())
    home = root.parent
    cfg = json.loads((home / 'config.json').read_text())
    keys = json.loads((home / 'all_keys.json').read_text())
    Source = source_class()
    dbdir, snapshot = Path(cfg['db_dir']), Path(state['source'])
    with closing(sqlite3.connect(root / 'incremental-cursors.sqlite3')) as cursors:
        return collect_and_send(root, state, home, keys, Source, dbdir, snapshot, cursors, excluded)


def collect_and_send(root, state, home, keys, Source, dbdir, snapshot, c, excluded):
    c.execute('CREATE TABLE IF NOT EXISTS cursors(db TEXT,tbl TEXT,cursor INTEGER,salt TEXT,PRIMARY KEY(db,tbl))')
    c.execute('CREATE TABLE IF NOT EXISTS seen(db TEXT,tbl TEXT,rid INTEGER,hash TEXT,PRIMARY KEY(db,tbl,rid))')
    sources, maps, rows, nexts = {}, {}, [], []
    try:
        files = sorted(p for p in (dbdir / 'message').glob('*.db')
                       if re.fullmatch(r'(?:biz_)?message_\d+\.db', p.name))
        for path in files:
            rel = str(path.relative_to(dbdir))
            key = keys[rel]
            sources[rel] = Source(home / 'setup/libsqlcipher.dylib', path,
                                  key if isinstance(key, str) else key['enc_key'])
            sources[rel].rows('BEGIN')
            maps[rel] = dict(sources[rel].rows('SELECT rowid,user_name FROM Name2Id'))
        users = {user for mapping in maps.values() for user in mapping.values() if user}
        names = {'Msg_' + hashlib.md5(user.encode()).hexdigest(): user for user in users}
        for rel, src in sources.items():
            with (dbdir / rel).open('rb') as stream:
                salt = stream.read(16).hex()
            olddb = sqlite3.connect((snapshot / rel).resolve().as_uri() + '?mode=ro', uri=True) if (snapshot / rel).exists() else None
            try:
                for (table,) in src.rows("SELECT name FROM sqlite_master WHERE type='table'"):
                    if not re.fullmatch('Msg_[0-9a-f]{32}', table) or table in excluded.tables:
                        continue
                    old = c.execute('SELECT cursor,salt FROM cursors WHERE db=? AND tbl=?', (rel, table)).fetchone()
                    if old and old[1] != salt:
                        raise RuntimeError('source_generation_changed')
                    if old:
                        cursor = old[0]
                    elif olddb and olddb.execute('SELECT 1 FROM sqlite_master WHERE name=?', (table,)).fetchone():
                        cursor = olddb.execute(f'SELECT max(rowid) FROM "{table}"').fetchone()[0] or 0
                    else:
                        cursor = 0
                    high = src.rows(f'SELECT max(rowid) FROM "{table}"')[0][0] or 0
                    if high < cursor:
                        raise RuntimeError('source_rowids_reset')
                    fetched = src.rows(f'SELECT rowid,local_id,server_id,local_type,sort_seq,real_sender_id,create_time,hex(message_content),WCDB_CT_message_content FROM "{table}" WHERE rowid>{max(0, cursor-8)} ORDER BY rowid LIMIT 1000')
                    for rid, local, server, kind, seq, sender, stamp, raw, compression in fetched:
                        if table not in names:
                            raise RuntimeError('unresolved_conversation')
                        rows.append(dict(source_db=rel, table=table, rowid=rid, local_id=local,
                                         server_id=server, kind=kind or 0, sort_seq=seq,
                                         sender=maps[rel].get(sender), conversation=names[table],
                                         timestamp=stamp, content_hex=raw, compression=compression))
                    nexts.append((rel, table, max([cursor] + [row[0] for row in fetched]), salt))
            finally:
                if olddb:
                    olddb.close()
    finally:
        for source in sources.values():
            source.close()
    changed, seen = [], []
    for row in rows:
        fingerprint = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
        identity = (row['source_db'], row['table'], row['rowid'])
        old = c.execute('SELECT hash FROM seen WHERE db=? AND tbl=? AND rid=?', identity).fetchone()
        if not old or old[0] != fingerprint:
            changed.append(row)
            seen.append((*identity, fingerprint))
    refs, resource_next = [], None
    c.execute('CREATE TABLE IF NOT EXISTS resource_cursor(id INTEGER PRIMARY KEY,position INTEGER)')
    resource_old = c.execute('SELECT position FROM resource_cursor WHERE id=1').fetchone()
    resource_pos = resource_old[0] if resource_old else 0
    rel = 'message/message_resource.db'
    if rel in keys:
        key = keys[rel]
        res = Source(home / 'setup/libsqlcipher.dylib', dbdir / rel,
                     key if isinstance(key, str) else key['enc_key'])
        try:
            chatnames = dict(res.rows('SELECT rowid,user_name FROM ChatName2Id'))
            for rid, chat, local, stamp, server, packed in res.rows('SELECT rowid,chat_id,message_local_id,message_create_time,message_svr_id,hex(packed_info) FROM MessageResourceInfo WHERE rowid>' + str(max(0, resource_pos-8)) + ' ORDER BY rowid LIMIT 10000'):
                resource_next = max(resource_next or resource_pos, rid)
                if chatnames.get(chat) in excluded.excluded:
                    continue
                found = tokens(bytes.fromhex(packed or ''))
                if found and chat in chatnames:
                    refs.append([chatnames[chat], local, stamp, str(server or 0), json.dumps(sorted(found))])
        finally:
            res.close()
    running = read_status(root / 'incremental-status.json').get('wechat_running')
    packet = {'self_user': state['self_user'], 'rows': changed, 'resource_refs': refs,
              'mac_status': mac_status(root, running)}
    payload = gzip.compress(json.dumps(packet, ensure_ascii=False).encode())
    pending = root / 'incremental-pending.json.gz'
    atomic_bytes(pending, payload)
    result = subprocess.run(['ssh', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15',
                             '-o', 'ServerAliveCountMax=3', 'NAS',
                             'python3 /pool-safe/wechat-vault/tools/ingest_delta.py /pool-safe/wechat-vault/structured/chat-archive.sqlite3'],
                            input=payload, capture_output=True, timeout=900, check=True)
    report = json.loads(result.stdout)
    with c:
        c.executemany('INSERT OR REPLACE INTO cursors VALUES (?,?,?,?)', nexts)
        c.executemany('INSERT OR REPLACE INTO seen VALUES (?,?,?,?)', seen)
        if resource_next is not None:
            c.execute('INSERT OR REPLACE INTO resource_cursor VALUES (1,?)', (resource_next,))
    pending.unlink()
    report.update(rows_checked=len(rows), rows_sent=len(changed), shards=len(sources))
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    while True:
        started = time.time()
        try:
            status = cycle(args.state_dir)
        except Exception:
            status = read_status(args.state_dir / 'incremental-status.json')
        if args.once:
            print(json.dumps(status))
            return
        time.sleep(max(5, 60 - (time.time() - started)))


if __name__ == '__main__':
    main()
