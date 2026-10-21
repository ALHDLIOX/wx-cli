#!/usr/bin/env python3
"""Send content-free Chinese NAS alerts; persist dedup only after all sends succeed."""
import argparse
import copy
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_state import atomic_json, file_lock, read_status


def send(target, text):
    subprocess.run(['hermes', 'send', '-t', target, '-q', text], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def diagnostic(status):
    kind = status.get('error_type') or 'UnknownError'
    if not isinstance(kind, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]{0,99}', kind):
        kind = 'UnknownError'
    # Only summaries emitted by archive_state are allowed in a notification.
    summary = status.get('error_summary')
    codes = {'invalid_exclusion', 'source_generation_changed', 'source_rowids_reset',
             'unresolved_conversation', 'transfer_failed', 'remote_decode_failed'}
    if summary != kind and summary not in {kind + ': ' + code for code in codes}:
        summary = kind
    return kind, summary[:200]


def evaluate(heartbeat, previous, config, now):
    active = copy.deepcopy(previous.get('active', {}))
    desired = {}
    received = heartbeat.get('received_at')
    stale_seconds = config.get('stale_minutes', 60) * 60
    elapsed = max(0, now - received) if isinstance(received, (int, float)) else stale_seconds
    if received is None or elapsed > stale_seconds:
        since = received if isinstance(received, (int, float)) else active.get('stale', {}).get('since', now - stale_seconds)
        desired['stale'] = {'since': since, 'label': 'Mac 同步',
                            'text': f'Mac 同步已中断 {math.floor((now-since)/60)} 分钟（可能睡眠、断网或 app 停止）'}
    mac = heartbeat.get('mac_status') or {}
    if mac.get('wechat_running') is False:
        desired['wechat'] = {'since': now, 'label': 'Mac 上微信', 'text': 'Mac 上微信未运行'}
    elif mac.get('wechat_running') is not True and 'wechat' in active:
        desired['wechat'] = active['wechat']
    workers = mac.get('workers') or {}
    for name in ('incremental', 'recent-media', 'media'):
        key = 'worker:' + name
        status = workers.get(name) or {}
        errors = status.get('consecutive_errors')
        if isinstance(errors, int) and errors >= config.get('error_threshold', 5):
            kind, summary = diagnostic(status)
            desired[key] = {'since': now, 'label': f'worker {name}',
                            'text': f'Mac 同步 worker {name} 连续失败 {errors} 次；{kind}；{summary}'}
        elif errors is None and key in active:
            desired[key] = active[key]
    messages = []
    for key, alert in desired.items():
        if key in active:
            alert['since'] = active[key]['since']
        else:
            messages.append(alert['text'])
    for key, alert in active.items():
        if key not in desired:
            minutes = max(0, math.ceil((now - alert['since']) / 60))
            messages.append(f"{alert['label']}已恢复，中断了 {minutes} 分钟")
    return {'active': desired, 'checked_at': now}, messages


def run(vault, now=None, sender=None):
    vault = Path(vault)
    with file_lock(vault / 'heartbeat.lock'):
        config = json.loads((vault / 'heartbeat-config.json').read_text())
        if (not isinstance(config.get('target'), str) or not config['target']
                or not isinstance(config.get('stale_minutes', 60), (int, float))
                or config.get('stale_minutes', 60) <= 0
                or not isinstance(config.get('error_threshold', 5), int)
                or config.get('error_threshold', 5) <= 0):
            raise ValueError('invalid heartbeat configuration')
        state, messages = evaluate(read_status(vault / 'mac-heartbeat.json'),
                                   read_status(vault / 'heartbeat-state.json'), config,
                                   time.time() if now is None else now)
        for message in messages:
            result = (sender or send)(config['target'], message)
            if result is False:
                raise RuntimeError('notification_failed')
        atomic_json(vault / 'heartbeat-state.json', state)
        return {'sent': len(messages)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--vault', type=Path, required=True)
    args = parser.parse_args()
    run(args.vault)
