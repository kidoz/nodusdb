#!/usr/bin/python3
# Isolated fake worker for the executor's process/protocol failure tests.
import json
import os
from pathlib import Path
import struct
import sys
import time

MODE = 'normal'
size = struct.unpack('<I', sys.stdin.buffer.read(4))[0]
request = json.loads(sys.stdin.buffer.read(size))
def frame(value):
    body = json.dumps(value).encode()
    sys.stdout.buffer.write(struct.pack('<I', len(body)) + body)
    sys.stdout.buffer.flush()
if MODE == 'stall':
    Path(__file__).with_suffix('.started').write_text(str(os.getpid()))
    time.sleep(20)
if MODE == 'memory':
    allocation = bytearray(128 * 1024 * 1024)
    time.sleep(20)
if MODE == 'error':
    frame({'Error': 'private-canary'})
    sys.exit(0)
fields = [dict(id=i + 1, name=name, sql_type=ty, source_type=ty, nullable=True)
          for i, (name, ty) in enumerate([('id', 'BIGINT'), ('name', 'TEXT'), ('active', 'BOOLEAN')])]
if MODE == 'dates':
    fields.append(dict(id=4, name='day', sql_type='DATE', source_type='DATE', nullable=True))
if MODE == 'ambiguous':
    fields[0]['name'] = 'a.b'
selected = request['columns'] or [f['name'] for f in fields]
fields = [next(f for f in fields if f['name'] == name) for name in selected]
frame({'Schema': dict(version=request['version'], table_uuid='changed' if MODE == 'drift' and request['expected'] else 'uuid', fields=fields, snapshot_id=123)})
count = 0
if request['expected']:
    rows = [{'id': '1', 'name': 'alpha', 'active': 'true', 'day': '2026-01-01'}, {'id': '2', 'name': None, 'active': 'false', 'day': '-0001-12-31'}]
    for row in rows[:request['limit']]:
        frame({'Row': [row[name] for name in selected]})
        count += 1
frame({'Done': {'rows': count}})
