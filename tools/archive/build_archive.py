#!/usr/bin/env python3
"""Build a local, portable SQLite archive from decrypted WeChat database snapshots.
Original databases remain the lossless source of truth; all reads use mode=ro.
"""
import argparse
import collections
from contextlib import contextmanager
import ctypes as C
import ctypes.util
import datetime as DT
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from archive_state import atomic_json

MSG = re.compile(r'Msg_[0-9a-f]{32}$')
TYPES = {1:'text',3:'image',34:'voice',42:'contact_card',43:'video',47:'sticker',48:'location',49:'app_message',50:'call',62:'short_video',10000:'system',10002:'system_event'}

@contextmanager
def ro(path):
    c=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)
    c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON')
    try:yield c
    finally:c.close()

def ident(name):return '"'+name.replace('"','""')+'"'
def digest(value):return hashlib.sha256(json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
def iso(ts):
    try:return DT.datetime.fromtimestamp(ts,DT.timezone.utc).isoformat()
    except (ValueError,OverflowError,OSError,TypeError):return None

class Decoder:
    def __init__(self):
        self.lib=C.CDLL(ctypes.util.find_library('zstd') or 'libzstd.so.1')
        for n,args,ret in [('ZSTD_getFrameContentSize',[C.c_void_p,C.c_size_t],C.c_ulonglong),('ZSTD_decompress',[C.c_void_p,C.c_size_t,C.c_void_p,C.c_size_t],C.c_size_t),('ZSTD_isError',[C.c_size_t],C.c_uint)]:
            fn=getattr(self.lib,n);fn.argtypes=args;fn.restype=ret
    def decode(self,value,compression=0):
        raw=value.encode() if isinstance(value,str) else bytes(value or b'')
        if not raw:return '', 'empty'
        if compression==4 or raw.startswith(b'\x28\xb5\x2f\xfd'):
            size=self.lib.ZSTD_getFrameContentSize(raw,len(raw))
            if size==2**64-1:size=16*1024*1024
            if size==2**64-2 or size>64*1024*1024:return None,'zstd_invalid_or_oversize'
            buf=C.create_string_buffer(max(size,1));n=self.lib.ZSTD_decompress(buf,size,raw,len(raw))
            if self.lib.ZSTD_isError(n):return None,'zstd_error'
            raw=buf.raw[:n]
        try:return raw.decode('utf-8'),'ok'
        except UnicodeDecodeError:return None,'non_utf8'

def body_fields(text,group=False,known=None):
    if text is None:return None,None,{}
    sender=None;body=text
    if group and ':\n' in text:
        prefix,tail=text.split(':\n',1)
        if len(prefix)<129 and re.fullmatch(r'[A-Za-z0-9_.@-]+',prefix) and (prefix.startswith('wxid_') or prefix in (known or set())):
            sender=prefix;body=tail
    fields={}
    if '<' in body[:100] and len(body)<8*1024*1024 and '<!DOCTYPE' not in body.upper() and '<!ENTITY' not in body.upper():
        try:
            element=ET.fromstring(body[body.index('<'):])
            for tag in ['title','des','url','fileext','totallen','md5','cdnattachurl','thumburl','recorditem','refermsg']:
                node=element.find('.//'+tag)
                if node is not None:
                    fields[tag]=(node.text or '') if len(node)==0 else ET.tostring(node,encoding='unicode')
        except (ET.ParseError,ValueError):pass
    return body,sender,fields

SCHEMA='''
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-65536;
PRAGMA temp_store=FILE;
PRAGMA foreign_keys=ON;
CREATE TABLE contacts(username TEXT PRIMARY KEY,display_name TEXT,nickname TEXT,remark TEXT,alias TEXT);
CREATE TABLE conversations(username TEXT PRIMARY KEY,display_name TEXT,kind TEXT,message_count INTEGER DEFAULT 0,first_timestamp INTEGER,last_timestamp INTEGER);
CREATE TABLE messages(id INTEGER PRIMARY KEY,message_key TEXT NOT NULL UNIQUE,conversation TEXT NOT NULL REFERENCES conversations(username),sender TEXT,sender_resolution TEXT,direction TEXT,server_id TEXT,timestamp INTEGER,time_utc TEXT,type_code INTEGER,sub_type INTEGER,message_type TEXT,body_text TEXT,fields_json TEXT,decode_status TEXT);
CREATE TABLE message_sources(id INTEGER PRIMARY KEY,message_id INTEGER NOT NULL REFERENCES messages(id),source_db TEXT,source_table TEXT,source_rowid INTEGER,local_id INTEGER,sort_seq INTEGER,compression INTEGER,content_sha256 TEXT,variant_text TEXT,UNIQUE(source_db,source_table,source_rowid));
CREATE TABLE source_tables(source_db TEXT,source_table TEXT,row_count INTEGER,imported_count INTEGER,PRIMARY KEY(source_db,source_table));
CREATE TABLE index_evidence(source_table TEXT,source_rowid INTEGER,conversation TEXT,sender TEXT,timestamp INTEGER,local_id INTEGER,type_code INTEGER,content_text TEXT,PRIMARY KEY(source_table,source_rowid));
CREATE TABLE auxiliary_messages(source_db TEXT,source_table TEXT,source_rowid INTEGER,payload_json TEXT,PRIMARY KEY(source_db,source_table,source_rowid));
CREATE TABLE media_files(path TEXT PRIMARY KEY,bytes INTEGER,mtime_ns INTEGER,extension TEXT,format_status TEXT);
CREATE TABLE resource_messages(resource_id INTEGER PRIMARY KEY,conversation TEXT,sender TEXT,local_id INTEGER,server_id TEXT,timestamp INTEGER,type_code INTEGER);
CREATE TABLE resource_details(resource_id INTEGER PRIMARY KEY,message_resource_id INTEGER,type_code INTEGER,bytes INTEGER,data_index TEXT);
CREATE TABLE build_metadata(key TEXT PRIMARY KEY,value TEXT);
CREATE VIEW chat_history AS SELECT m.id,m.conversation,c.display_name AS conversation_name,m.sender,COALESCE(p.display_name,m.sender) AS sender_name,m.direction,m.timestamp,m.time_utc,m.message_type,m.sub_type,m.body_text,m.fields_json,m.decode_status FROM messages m JOIN conversations c ON c.username=m.conversation LEFT JOIN contacts p ON p.username=m.sender;
'''

class Builder:
    def __init__(self,source,output,self_user,media=None):
        self.source=Path(source);self.output=Path(output);self.self_user=self_user;self.media=Path(media) if media else None
        if self.output.exists():raise ValueError('Refusing to overwrite existing archive')
        self.output.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.db=sqlite3.connect(self.output);self.db.executescript(SCHEMA);self.decoder=Decoder();self.names={};self.users=set();self.conv_seen=set();self.decode_counts=collections.Counter();self.count=0
    def contacts(self):
        p=self.source/'contact/contact.db'
        if p.exists():
            with ro(p) as c:
                for r in c.execute('SELECT username,nick_name,remark,alias FROM contact'):
                    user,nick,remark,alias=r
                    if not user:continue
                    display=remark or nick or alias or user;self.names[user]=display;self.users.add(user)
                    self.db.execute('INSERT OR REPLACE INTO contacts VALUES (?,?,?,?,?)',(user,display,nick,remark,alias))
        p=self.source/'session/session.db'
        if p.exists():
            with ro(p) as c:self.users.update(r[0] for r in c.execute('SELECT username FROM SessionTable') if r[0])
    def conversation(self,user,kind=None):
        if user in self.conv_seen:return
        kind=kind or ('group' if user.endswith('@chatroom') else 'service' if user.startswith('gh_') else 'direct')
        self.db.execute('INSERT OR IGNORE INTO conversations(username,display_name,kind) VALUES (?,?,?)',(user,self.names.get(user,user),kind));self.conv_seen.add(user)
    def add(self,conv,sender,resolution,server,stamp,kind,sub,content,status,source_db,table,rowid,local_id,seq,compression,raw):
        self.conversation(conv)
        body,prefix,fields=body_fields(content,conv.endswith('@chatroom'),self.users)
        if not sender and prefix:sender=prefix;resolution='content_prefix'
        direction='outgoing' if sender==self.self_user else 'incoming' if sender else 'unknown'
        server=str(server or 0)
        # Server ID plus conversation/time/type; zero-ID rows retain source identity.
        key=digest([conv,server,stamp,kind,sub] if server!='0' else [source_db,table,rowid])
        args=(key,conv,sender,resolution,direction,server,stamp,iso(stamp),kind,sub,TYPES.get(kind,'unknown'),body,json.dumps(fields,ensure_ascii=False),status)
        cur=self.db.execute('INSERT OR IGNORE INTO messages(message_key,conversation,sender,sender_resolution,direction,server_id,timestamp,time_utc,type_code,sub_type,message_type,body_text,fields_json,decode_status) VALUES ('+','.join('?'*14)+')',args)
        if cur.rowcount:mid=cur.lastrowid;variant=None
        else:
            mid,previous=self.db.execute('SELECT id,body_text FROM messages WHERE message_key=?',(key,)).fetchone();variant=content if previous!=body else None
        blob=raw.encode() if isinstance(raw,str) else bytes(raw or b'')
        self.db.execute('INSERT INTO message_sources(message_id,source_db,source_table,source_rowid,local_id,sort_seq,compression,content_sha256,variant_text) VALUES (?,?,?,?,?,?,?,?,?)',(mid,source_db,table,rowid,local_id,seq,compression,hashlib.sha256(blob).hexdigest(),variant))
        self.count+=1;self.decode_counts[status]+=1
    def primary(self):
        files=sorted(p for p in (self.source/'message').glob('*.db') if re.fullmatch(r'(?:biz_)?message_\d+\.db',p.name))
        # Global name map resolves a shard's table even if its local Name2Id is sparse.
        maps={}
        for p in files:
            with ro(p) as c:maps[p]=dict(c.execute('SELECT rowid,user_name FROM Name2Id'))
            self.users.update(u for u in maps[p].values() if u)
        table_users={'Msg_'+hashlib.md5(u.encode()).hexdigest():u for u in self.users}
        for p in files:
            rel=str(p.relative_to(self.source));names=maps[p]
            with ro(p) as c:
                tables=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'") if MSG.fullmatch(r[0])]
                for table in tables:
                    conv=table_users.get(table,'unresolved:'+table);self.conversation(conv,'unresolved' if conv.startswith('unresolved:') else None)
                    expected=c.execute('SELECT count(*) FROM '+ident(table)).fetchone()[0];got=0
                    for r in c.execute('SELECT rowid AS source_rowid,local_id,server_id,local_type,sort_seq,real_sender_id,create_time,CAST(message_content AS BLOB) AS message_content,WCDB_CT_message_content FROM '+ident(table)):
                        raw=r['message_content'];ct=r['WCDB_CT_message_content'];text,status=self.decoder.decode(raw,ct)
                        sender=names.get(r['real_sender_id']);sender=sender if sender!=conv or not conv.endswith('@chatroom') else None
                        localtype=r['local_type'] or 0
                        self.add(conv,sender,'name2id' if sender else 'unknown',r['server_id'],r['create_time'],localtype&0xffffffff,(localtype>>32)&0xffffffff,text,status,rel,table,r['source_rowid'],r['local_id'],r['sort_seq'],ct,raw);got+=1
                    assert got==expected
                    self.db.execute('INSERT INTO source_tables VALUES (?,?,?,?)',(rel,table,expected,got))
                    self.db.commit()
            print(json.dumps({'stage':'primary','database':rel,'source_rows':self.count}),flush=True)
    def unsupported(self):
        rel='migrate/unspportmsg.db';p=self.source/rel
        if not p.exists():return
        with ro(p) as c:
            count=0
            for r in c.execute('SELECT rowid AS source_rowid,svr_id,type,sub_type,from_user,to_user,CAST(content AS BLOB) AS content,create_time,sequent_id FROM UnsupportMessage'):
                frm,to=r['from_user'],r['to_user'];group=next((x for x in [frm,to] if x and x.endswith('@chatroom')),None)
                conv=group or (to if frm==self.self_user else frm if to==self.self_user else None)
                conv=conv or 'unresolved:migration:'+digest([frm,to])[:24]
                sender=frm if frm and not frm.endswith('@chatroom') else None
                stamp=r['create_time'];stamp=stamp//1000 if stamp and abs(stamp)>=10**12 else stamp
                text,status=self.decoder.decode(r['content']);self.add(conv,sender,'migration_from_user' if sender else 'unknown',r['svr_id'],stamp,r['type'],r['sub_type'] or 0,text,status,rel,'UnsupportMessage',r['source_rowid'],None,r['sequent_id'],0,r['content']);count+=1
            self.db.execute('INSERT INTO source_tables VALUES (?,?,?,?)',(rel,'UnsupportMessage',count,count));self.db.commit()
    def auxiliary(self):
        p=self.source/'message/message_fts.db'
        if p.exists():
            with ro(p) as c:
                names=dict(c.execute('SELECT rowid,username FROM name2id'))
                tables=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'") if re.fullmatch(r'message_fts_v4_\d+_content',r[0])]
                for table in tables:
                    for r in c.execute('SELECT rowid,c0,c1,c2,c3,c4,c5,c6 FROM '+ident(table)):
                        self.db.execute('INSERT INTO index_evidence VALUES (?,?,?,?,?,?,?,?)',(table,r[0],names.get(r[5]),names.get(r[6]),r[7],r[2],r[4],r[1]))
        p=self.source/'chatbot/chatbot_message.db'
        if p.exists():
            with ro(p) as c:
                for table in ['chatbot_message','chatbot_message_v1']:
                    for row in c.execute('SELECT rowid AS source_rowid,* FROM '+ident(table)):
                        data={k:({'blob_hex':v.hex()} if isinstance(v,bytes) else v) for k,v in dict(row).items()}
                        self.db.execute('INSERT INTO auxiliary_messages VALUES (?,?,?,?)',('chatbot/chatbot_message.db',table,row['source_rowid'],json.dumps(data,ensure_ascii=False)))
        p=self.source/'message/message_resource.db'
        if p.exists():
            with ro(p) as c:
                chats=dict(c.execute('SELECT rowid,user_name FROM ChatName2Id'));senders=dict(c.execute('SELECT rowid,user_name FROM SenderName2Id'))
                for r in c.execute('SELECT * FROM MessageResourceInfo'):
                    self.db.execute('INSERT INTO resource_messages VALUES (?,?,?,?,?,?,?)',(r['message_id'],chats.get(r['chat_id']),senders.get(r['sender_id']),r['message_local_id'],str(r['message_svr_id']),r['message_create_time'],r['message_local_type']))
                for r in c.execute('SELECT * FROM MessageResourceDetail'):
                    self.db.execute('INSERT INTO resource_details VALUES (?,?,?,?,?)',(r['resource_id'],r['message_id'],r['type'],r['size'],r['data_index']))
        self.db.commit()
    def finish(self):
        self.db.executescript('''CREATE INDEX messages_conversation_time ON messages(conversation,timestamp,id);
CREATE INDEX messages_sender_time ON messages(sender,timestamp);
CREATE INDEX messages_server ON messages(server_id);
CREATE INDEX sources_message ON message_sources(message_id);
CREATE INDEX resources_message ON resource_messages(conversation,server_id);
CREATE VIRTUAL TABLE message_search USING fts5(body_text,content='messages',content_rowid='id',tokenize='trigram');
INSERT INTO message_search(message_search) VALUES('rebuild');
UPDATE conversations SET message_count=(SELECT count(*) FROM messages m WHERE m.conversation=conversations.username),first_timestamp=(SELECT min(timestamp) FROM messages m WHERE m.conversation=conversations.username),last_timestamp=(SELECT max(timestamp) FROM messages m WHERE m.conversation=conversations.username);
''')
        report={'source_rows':self.count,'messages':self.db.execute('SELECT count(*) FROM messages').fetchone()[0],'conversations':self.db.execute('SELECT count(*) FROM conversations WHERE message_count>0').fetchone()[0],'contacts':self.db.execute('SELECT count(*) FROM contacts').fetchone()[0],'decode_status':dict(self.decode_counts),'unresolved_conversations':self.db.execute("SELECT count(*) FROM conversations WHERE username LIKE 'unresolved:%' AND message_count>0").fetchone()[0],'sender_unknown':self.db.execute("SELECT count(*) FROM messages WHERE sender IS NULL").fetchone()[0],'timestamp_range':list(self.db.execute('SELECT min(timestamp),max(timestamp) FROM messages').fetchone()),'auxiliary_rows':self.db.execute('SELECT count(*) FROM auxiliary_messages').fetchone()[0],'source_root':str(self.source),'schema_version':1,'index_evidence_rows':self.db.execute('SELECT count(*) FROM index_evidence').fetchone()[0]}
        report['source_duplicates']=report['source_rows']-report['messages'];report['built_at']=DT.datetime.now(DT.timezone.utc).isoformat()
        for k,v in report.items():self.db.execute('INSERT INTO build_metadata VALUES (?,?)',(k,json.dumps(v)))
        self.db.commit();assert not self.db.execute('PRAGMA foreign_key_check').fetchall();assert self.db.execute('PRAGMA quick_check').fetchall()==[('ok',)]
        self.db.execute("INSERT INTO message_search(message_search,rank) VALUES('integrity-check',1)");self.db.commit()
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.close()
        self.output.chmod(0o600);atomic_json(self.output.with_suffix('.report.json'),report);print(json.dumps(report,ensure_ascii=False),flush=True);return report
    def run(self):
        self.contacts();self.primary();self.unsupported();self.auxiliary();return self.finish()

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--self-user',required=True);a=p.parse_args();os.umask(0o077)
    Builder(a.source,a.output,a.self_user).run()
if __name__=='__main__':main()
