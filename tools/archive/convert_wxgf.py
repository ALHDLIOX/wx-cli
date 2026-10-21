#!/usr/bin/env python3
"""Convert single-dominant-partition WXGF images, preserving ambiguous animations.
Partition layout reference: https://github.com/sjzar/chatlog/blob/a16b689/pkg/util/dat2img/wxgf.go
"""
import argparse,json,sqlite3,subprocess,sys,hashlib,os,time
from pathlib import Path
from decode_media import decode,validated

def convert(data):
 if len(data)<15 or data[:4]!=b'wxgf':raise ValueError('invalid_wxgf')
 parts=[]
 for marker in (b'\0\0\0\1',b'\0\0\1'):
  pos=max(4,data[4])
  while pos<len(data):
   i=data.find(marker,pos)
   if i<0:break
   n=int.from_bytes(data[i-4:i],'big');pos=i+1
   if 0<n<=len(data)-i:parts.append(data[i:i+n]);pos=i+n
  if parts:break
 if not parts:raise ValueError('no_partition')
 largest=max(parts,key=len)
 if len(parts)>1 and len(largest)<0.6*len(data):raise ValueError('animation_preserved')
 result=subprocess.run(['ffmpeg','-v','error','-threads','1','-f','hevc','-i','pipe:0','-frames:v','1','-f','image2pipe','-c:v','png','pipe:1'],input=largest,capture_output=True,timeout=30,check=True)
 validated(result.stdout);return result.stdout

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--manifest',required=True);a=p.parse_args();os.umask(0o077);key=json.load(sys.stdin);db=sqlite3.connect(a.manifest,timeout=60);counts={}
 for rel,size,mtime in db.execute("SELECT path,size,mtime FROM decoded WHERE status='wxgf_needs_conversion'").fetchall():
  try:
   src=a.root/rel
   if src.stat().st_mtime_ns!=mtime:continue
   data=convert(decode(src.read_bytes(),bytes.fromhex(key['aes_hex']),key['xor']));dest=rel+'.png';target=a.output/dest;target.parent.mkdir(parents=True,exist_ok=True);tmp=target.with_name(target.name+'.tmp');tmp.write_bytes(data);tmp.replace(target)
   with db:db.execute('UPDATE decoded SET status=?,output=?,sha256=? WHERE path=?',('ok',dest,hashlib.sha256(data).hexdigest(),rel))
   status='ok'
  except Exception as e:status=str(e) if isinstance(e,ValueError) else type(e).__name__
  counts[status]=counts.get(status,0)+1
  Path(a.manifest+'.wxgf-status.json').write_text(json.dumps({'counts':counts,'at':time.time()}))
 print(json.dumps(counts))
if __name__=='__main__':main()
