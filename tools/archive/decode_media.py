#!/usr/bin/env python3
"""Resumeable DAT decoding into a separate tree; originals are never modified.
Key JSON is read from stdin, never logged. Requires cryptography and Pillow.
"""
import argparse,collections,hashlib,io,json,os,sqlite3,struct,sys,time
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
from PIL import Image

def decode(data,key=None,xor=0x88):
    if data[:6] in (b'\x07\x08V1\x08\x07',b'\x07\x08V2\x08\x07'):
        if len(data)<15:raise ValueError('truncated_header')
        aes_len,xor_len=struct.unpack('<II',data[6:14]);end=15+aes_len+16-aes_len%16;tail=len(data)-xor_len
        if end>tail or tail<15:raise ValueError('invalid_lengths')
        k=b'cfcd208495d565ef' if data[3]==ord('1') else key
        if k is None:raise ValueError('missing_key')
        dec=Cipher(algorithms.AES(k),modes.ECB()).decryptor();head=dec.update(data[15:end])+dec.finalize();pad=head[-1]
        if not 1<=pad<=16 or head[-pad:]!=bytes([pad])*pad:raise ValueError('invalid_padding')
        if len(head)-pad!=aes_len:raise ValueError('invalid_plain_length')
        return head[:-pad]+data[end:tail]+bytes(b^xor for b in data[tail:])
    for magic in [b'\xff\xd8\xff',b'\x89PNG\r\n\x1a\n',b'GIF8',b'II*\x00',b'RIFF',b'wxgf']:
        if len(data)>=len(magic):
            k=data[0]^magic[0]
            if all(a^k==b for a,b in zip(data,magic)):return bytes(b^k for b in data) if k else data
    raise ValueError('unknown_format')

def validated(data):
    if data.startswith(b'wxgf'):raise ValueError('wxgf_needs_conversion')
    with Image.open(io.BytesIO(data)) as im:
        fmt=im.format.lower();im.verify()
    with Image.open(io.BytesIO(data)) as im:im.load()
    return {'jpeg':'jpg','tiff':'tif'}.get(fmt,fmt)

def run(root,out,manifest,key,xor):
    os.umask(0o077);out.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(manifest,timeout=180);db.execute("PRAGMA journal_mode=WAL");db.execute('CREATE TABLE IF NOT EXISTS decoded(path TEXT PRIMARY KEY,size INTEGER,mtime INTEGER,status TEXT,output TEXT,sha256 TEXT)')
    existing={r[0]:r[1:] for r in db.execute('SELECT path,size,mtime,status,output FROM decoded')};counts=collections.Counter();start=time.time()
    for parent,dirs,files in os.walk(root):
        dirs[:]=[d for d in dirs if not (Path(parent)/d).is_symlink()]
        for name in files:
            p=Path(parent)/name
            if p.suffix.lower()!='.dat' or p.is_symlink():continue
            st=p.stat();rel=str(p.relative_to(root));old=existing.get(rel)
            if old and old[:2]==(st.st_size,st.st_mtime_ns) and (old[2]!='ok' or (out/old[3]).exists()):counts[old[2]]+=1;continue
            dest=None;sha=None
            try:
                if st.st_size>256*1024*1024:raise ValueError('oversize')
                raw=p.read_bytes();data=decode(raw,key,xor);ext=validated(data)
                if p.stat().st_mtime_ns!=st.st_mtime_ns:raise ValueError('source_changed')
                dest=Path(rel+'.'+ext);target=out/dest;target.parent.mkdir(parents=True,exist_ok=True)
                tmp=target.with_name(target.name+'.tmp');tmp.write_bytes(data);tmp.replace(target)
                sha=hashlib.sha256(data).hexdigest();status='ok'
            except Exception as e:status=str(e) if isinstance(e,ValueError) and str(e) in {'wxgf_needs_conversion','missing_key','unknown_format','invalid_padding','invalid_lengths','invalid_plain_length','truncated_header','oversize','source_changed'} else type(e).__name__
            db.execute('INSERT OR REPLACE INTO decoded VALUES (?,?,?,?,?,?)',(rel,st.st_size,st.st_mtime_ns,status,str(dest) if dest else None,sha));counts[status]+=1
            if sum(counts.values())%20==0:
                db.commit();time.sleep(0.02);Path(str(manifest)+'.status.json').write_text(json.dumps({'stage':'running','counts':counts,'at':time.time(),'started':start}))
    db.commit();db.close();report={'stage':'complete','counts':counts,'at':time.time(),'started':start};Path(str(manifest)+'.status.json').write_text(json.dumps(report));print(json.dumps(report))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--manifest',type=Path,required=True);a=p.parse_args()
    material=json.load(sys.stdin);run(a.root,a.output,a.manifest,bytes.fromhex(material['aes_hex']),material['xor'])
