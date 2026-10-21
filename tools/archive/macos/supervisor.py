#!/usr/bin/env python3
"""One persistent process for Mac-side archive access."""
import json
import os
from pathlib import Path
import sys
import threading
import time


def repeat(interval, action):
    while True:
        started = time.time()
        try:
            action()
        except Exception:
            # Each worker publishes content-free error state before raising.
            pass
        time.sleep(max(5, interval - (time.time() - started)))


def main(root=None):
    root = Path(root) if root is not None else Path.home() / '.wx-cli/nas-archive'
    os.umask(0o077)
    config = json.loads((root / 'supervisor-config.json').read_text())
    sys.path.insert(0, str(Path(config['repository']) / 'tools/archive'))
    from archive_state import atomic_json, file_lock
    import recent_media
    import sync_incremental
    from macos import media_cycle
    with file_lock(root / 'supervisor.lock'):
        for interval, action in [(60, lambda: sync_incremental.cycle(root)),
                                 (60, lambda: recent_media.main(root)),
                                 (900, lambda: media_cycle.run(root))]:
            threading.Thread(target=repeat, args=(interval, action), daemon=True).start()
        atomic_json(root / 'supervisor-process.json', {'pid': os.getpid(), 'started': time.time()})
        while True:
            time.sleep(60)


if __name__ == '__main__':
    main()
