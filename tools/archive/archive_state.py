"""Private atomic state, per-cycle locks, and content-free worker diagnostics."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time


def atomic_bytes(path, data):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path, value):
    atomic_bytes(path, (json.dumps(value, ensure_ascii=False, indent=2) + '\n').encode())


@contextmanager
def file_lock(path, blocking=False, shared=False):
    # Closing in finally also releases flock when an action raises.
    with Path(path).open('a', encoding='utf-8') as stream:
        os.chmod(path, 0o600)
        fcntl.flock(stream, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | (0 if blocking else fcntl.LOCK_NB))
        yield


def read_status(path):
    try:
        result = json.loads(Path(path).read_text())
        return result if isinstance(result, dict) else {}
    except (OSError, ValueError):
        return {}


def error_fields(error):
    # Exception messages can contain SQL, keys, or a remote process's payload.
    # Only our explicit, content-free codes may augment the exception type.
    kind = type(error).__name__[:100]
    safe_codes = {'invalid_exclusion', 'source_generation_changed', 'source_rowids_reset',
                  'unresolved_conversation', 'transfer_failed', 'remote_decode_failed'}
    first = str(error).split('\n', 1)[0]
    summary = kind + (': ' + first if first in safe_codes else '')
    return {'error_type': kind, 'error_summary': summary[:200]}


STATUS_FILES = {'incremental': 'incremental-status.json',
                'recent-media': 'recent-media-status.json',
                'media': 'media-cycle-status.json'}
SUMMARY_FIELDS = ('stage', 'last_success_at', 'consecutive_errors', 'error_type', 'error_summary')


def worker_cycle(root, name, action, extra=None):
    path = root / STATUS_FILES.get(name, name + '-status.json')
    previous = read_status(path)
    started = time.time()
    base = {'last_success_at': previous.get('last_success_at'),
            'consecutive_errors': previous.get('consecutive_errors', 0)}
    if base['consecutive_errors']:
        base.update({key: previous[key] for key in ('error_type', 'error_summary') if key in previous})
    base.update(extra or {})
    atomic_json(path, dict(base, stage='running', checked_at=started))
    try:
        result = dict(action() or {})
    except Exception as error:
        result = dict(base, stage='error', checked_at=time.time(),
                      consecutive_errors=base['consecutive_errors'] + 1,
                      duration_seconds=time.time() - started)
        result.update(error_fields(error))
        atomic_json(path, result)
        atomic_json(root / (name + '-supervisor-status.json'), result)
        raise
    result.update(base)
    result.update(stage='ok', checked_at=time.time(), last_success_at=time.time(),
                  consecutive_errors=0, duration_seconds=time.time() - started)
    result.pop('error_type', None)
    result.pop('error_summary', None)
    atomic_json(path, result)
    atomic_json(root / (name + '-supervisor-status.json'), result)
    return result


def mac_status(root, wechat_running):
    workers = {}
    for name, filename in STATUS_FILES.items():
        status = read_status(root / filename)
        # Never copy arbitrary worker result fields into the heartbeat.
        workers[name] = {key: status.get(key) for key in SUMMARY_FIELDS}
    return {'workers': workers, 'wechat_running': wechat_running}
