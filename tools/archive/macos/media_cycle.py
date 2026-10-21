"""Importable full-media round; locks and status belong to one round only."""
import json
import os
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_state import file_lock, worker_cycle
from exclusion import load_exclusions
from macos import fast_sync

SSH = ['ssh', '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15',
       '-o', 'ServerAliveCountMax=3', 'NAS']


def remote(command, material=None):
    return subprocess.run(SSH + [command], input=material, capture_output=True,
                          timeout=14400, check=True)


def run(root: Path) -> dict:
    root = Path(root)
    with file_lock(root / 'media-cycle.lock'):
        return worker_cycle(root, 'media', lambda: _run(root))


def _run(root):
    load_exclusions(root / 'exclude.json')
    result = fast_sync.run(root)
    remote('python3 /pool-safe/wechat-vault/tools/index_media.py --media /pool-safe/wechat-vault/media --db /pool-safe/wechat-vault/structured/chat-archive.sqlite3')
    remote('python3 /pool-safe/wechat-vault/tools/link_media.py --archive /pool-safe/wechat-vault/structured/chat-archive.sqlite3 --output /pool-safe/wechat-vault/media-links.sqlite3')
    running = remote("pgrep -fc '^python3 /pool-safe/wechat-vault/tools/(decode_media|convert_wxgf).py' || true")
    if int(running.stdout.strip() or b'0') == 0:
        material = (root / 'image-material.json').read_bytes()
        for script in ['decode_media', 'convert_wxgf']:
            remote('python3 /pool-safe/wechat-vault/tools/' + script + '.py --root /pool-safe/wechat-vault/media/msg --output /pool-safe/wechat-vault/decoded --manifest /pool-safe/wechat-vault/decoded-media.sqlite3', material)
    return {'exclusion_unresolved': result.get('exclusion_unresolved', 0)}


if __name__ == '__main__':
    os.umask(0o077)
    print(json.dumps(run(Path.home() / '.wx-cli/nas-archive')))
