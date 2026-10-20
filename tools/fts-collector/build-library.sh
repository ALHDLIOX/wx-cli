#!/bin/sh
# Run on Linux after cargo build --locked --release.
set -eu
python3 - "$@" <<'PY'
import pathlib,subprocess,sys
root=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else '.')
out=pathlib.Path(sys.argv[2] if len(sys.argv)>2 else 'libsqlcipher.so').resolve()
objects=list(root.glob('target/release/build/libsqlite3-sys-*/out/*sqlite3.o'))
crypto=list(root.glob('target/release/build/openssl-sys-*/out/openssl-build/install/lib/libcrypto.a'))
if len(objects)!=1 or len(crypto)!=1:
    raise SystemExit('Expected one SQLCipher object and one libcrypto.a; select a clean Linux release build')
subprocess.run(['cc','-shared','-o',str(out),str(objects[0]),str(crypto[0]),'-ldl','-lpthread','-lm'],check=True)
PY
