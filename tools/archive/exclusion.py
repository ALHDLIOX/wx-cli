"""Shared conservative ownership rules for local filtering and NAS purging.

Unknown ownership never authorizes deletion of a shared video/file namespace.
No remark, message text, packed resource, or key is included in diagnostics.
"""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re
import stat
import xml.etree.ElementTree as ET

TOKEN = re.compile(rb'[0-9a-fA-F]{32}')
MESSAGE_TABLE = re.compile(r'Msg_[0-9a-f]{32}\Z')


def chat_hash(username):
    return hashlib.md5(username.encode()).hexdigest()


def load_exclusions(path):
    path = Path(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return frozenset()
    try:
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError()
        value = json.loads(path.read_text())
        if (not isinstance(value, dict) or type(value.get('version')) is not int
                or value['version'] != 1 or not isinstance(value.get('conversations'), dict)):
            raise ValueError()
        for user, remark in value['conversations'].items():
            if not isinstance(user, str) or not user or '\x00' in user or not isinstance(remark, str):
                raise ValueError()
        return frozenset(value['conversations'])
    except (OSError, ValueError, TypeError):
        raise ValueError('invalid_exclusion') from None


def tokens(raw):
    if isinstance(raw, str):
        raw = raw.encode()
    return {value.decode().lower() for value in TOKEN.findall(raw or b'')}


def ident(name):
    return '"' + name.replace('"', '""') + '"'


class Ownership:
    def __init__(self, excluded):
        self.excluded = frozenset(excluded)
        self.hashes = {chat_hash(user) for user in excluded}
        self.tables = {'Msg_' + value for value in self.hashes}
        self.token_owners = defaultdict(set)
        self.file_owners = defaultdict(set)
        self.files_complete = True

    def add_tokens(self, user, raw):
        for token in tokens(raw):
            self.token_owners[token].add(user)

    def add_message(self, user, text, decoded=True):
        if not decoded or text is None:
            self.files_complete = False
            return
        self.add_tokens(user, text)
        # Only actual file appmsgs supply a reliable original name + byte size.
        if len(text) > 8 * 1024 * 1024 or '<!DOCTYPE' in text.upper() or '<!ENTITY' in text.upper():
            self.files_complete = False
            return
        try:
            element = ET.fromstring(text[text.index('<'):])
            app = element if element.tag == 'appmsg' else element.find('.//appmsg')
            if app is None or app.findtext('type') != '6':
                return
            title = app.findtext('title')
            size = int(app.findtext('.//totallen'))
            if not title or Path(title).name != title or size < 0:
                self.files_complete = False
                return
            self.file_owners[(title, size)].add(user)
        except (ET.ParseError, ValueError, TypeError):
            # A malformed appmsg may hide another reference to the same file.
            self.files_complete = False

    def exclusive(self, owners):
        return bool(owners) and owners <= self.excluded

    def decision(self, relative, size):
        if not self.excluded:
            return 'keep'
        parts = Path(relative).parts
        if len(parts) >= 2 and parts[0] == 'attach':
            return 'exclude' if parts[1] in self.hashes else 'keep'
        if parts and parts[0] == 'video':
            match = re.fullmatch(r'([0-9a-fA-F]{32})(?:\.mp4|_thumb\.jpg)', parts[-1])
            if match and self.exclusive(self.token_owners.get(match[1].lower(), set())):
                return 'exclude'
        if parts and parts[0] == 'file':
            owners = self.file_owners.get((parts[-1], size), set())
            if not owners or not self.files_complete:
                return 'unresolved'
            return 'exclude' if self.exclusive(owners) else 'keep'
        return 'keep'

    def collect_resources(self, rows):
        tables = {r[0] for r in rows("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'MessageResourceInfo' not in tables:
            return
        names = dict(rows('SELECT rowid,user_name FROM ChatName2Id'))
        for chat, packed in rows('SELECT chat_id,hex(packed_info) FROM MessageResourceInfo'):
            self.add_tokens(names.get(chat), bytes.fromhex(packed or ''))
        if 'MessageResourceDetail' in tables:
            for chat, packed in rows('SELECT i.chat_id,hex(d.packed_info) FROM MessageResourceDetail d '
                                     'LEFT JOIN MessageResourceInfo i ON i.message_id=d.message_id'):
                self.add_tokens(names.get(chat), bytes.fromhex(packed or ''))

    def collect_messages(self, rows, table_users, decoder):
        for (table,) in rows("SELECT name FROM sqlite_master WHERE type='table'"):
            if not MESSAGE_TABLE.fullmatch(table):
                continue
            for raw, compression in rows('SELECT hex(message_content),WCDB_CT_message_content FROM '
                                         + ident(table) + ' WHERE (local_type & 4294967295) IN (3,43,49,62)'):
                text, status = decoder.decode(bytes.fromhex(raw or ''), compression)
                self.add_message(table_users.get(table), text, status in ('ok', 'empty'))


def live_ownership(root, state):
    """Read all current owners, including unexcluded conversations, before a scan."""
    ownership = Ownership(load_exclusions(root / 'exclude.json'))
    if not ownership.excluded:
        return ownership
    from build_archive import Decoder
    from sync_incremental import source_class
    config = json.loads((root.parent / 'config.json').read_text())
    keys = json.loads((root.parent / 'all_keys.json').read_text())
    dbdir = Path(config['db_dir'])
    Source = source_class()
    sources = []
    table_users = {'Msg_' + chat_hash(user): user for user in ownership.excluded}
    try:
        for path in sorted((dbdir / 'message').glob('*.db')):
            if not re.fullmatch(r'(?:biz_)?message_\d+\.db|message_resource\.db', path.name):
                continue
            key = keys[str(path.relative_to(dbdir))]
            src = Source(root.parent / 'setup/libsqlcipher.dylib', path,
                         key if isinstance(key, str) else key['enc_key'])
            sources.append((path.name, src))
            src.rows('BEGIN')
            if path.name != 'message_resource.db':
                for _, user in src.rows('SELECT rowid,user_name FROM Name2Id'):
                    if user:
                        table_users['Msg_' + chat_hash(user)] = user
        decoder = Decoder()
        for name, src in sources:
            if name == 'message_resource.db':
                ownership.collect_resources(src.rows)
            else:
                ownership.collect_messages(src.rows, table_users, decoder)
    finally:
        for _, src in sources:
            src.close()
    return ownership
