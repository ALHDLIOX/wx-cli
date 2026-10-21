import unittest
try:
 from decode_media import decode,validated
 from cryptography.hazmat.primitives.ciphers import Cipher,algorithms,modes
 from PIL import Image
except ModuleNotFoundError as error:
 if error.name == 'PIL' or error.name == 'cryptography' or error.name.startswith(('PIL.', 'cryptography.')):
  raise unittest.SkipTest('optional Pillow/cryptography dependency is unavailable') from error
 raise
import io,struct
class MediaTest(unittest.TestCase):
 def test_roundtrip(self):
  b=io.BytesIO();Image.new('RGB',(4,4)).save(b,format='PNG');plain=b.getvalue();key=b'0123456789abcdef';n=32;tail=9;p=plain[:n]+bytes([16])*16;e=Cipher(algorithms.AES(key),modes.ECB()).encryptor();cipher=e.update(p)+e.finalize();raw=b'\x07\x08V2\x08\x07'+struct.pack('<II',n,tail)+b'\0'+cipher+plain[n:-tail]+bytes(v^123 for v in plain[-tail:]);self.assertEqual(decode(raw,key,123),plain);self.assertEqual(validated(decode(raw,key,123)),'png')
  with self.assertRaises(ValueError):decode(raw,b'fedcba9876543210',123)
 def test_truncation_and_container(self):
  with self.assertRaises(ValueError):decode(b'\x07\x08V2\x08\x07')
  with self.assertRaisesRegex(ValueError,'wxgf'):validated(b'wxgf12345678')
 def test_unknown(self):
  with self.assertRaises(ValueError):decode(b'not an image')
