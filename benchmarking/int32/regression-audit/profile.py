#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Profile benchmark operators separately from the paired timing gate."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base', type=Path, required=True)
p.add_argument('--target', type=Path, required=True)
p.add_argument('--suite', required=True)
p.add_argument('--queries', nargs='+', required=True)
p.add_argument('--cores', type=int, choices=(1, 8), default=1)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
a.out.parent.mkdir(parents=True, exist_ok=True)
warmups, iterations = 2, 6
fields = ('build_self_ms', 'next_self_ms', 'rows', 'calls', 'pool_tasks', 'pool_work_ms')
report = {'suite': a.suite, 'cores': a.cores, 'warmups': warmups,
          'iterations': iterations, 'results': {}, 'binaries': {}}
for side, binary in (('base', a.base), ('target', a.target)):
    binary = binary.resolve()
    report['binaries'][side] = {'path': str(binary),
                              'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    env = dict(os.environ, IBEX_CORES=str(a.cores), IBEX_COMPACT_INT32='0',
               IBEX_PROFILE_OPERATORS='1')
    for key in ('IBEX_ANSWERS_DIR', 'IBEX_DIGEST_OUT'):
        env.pop(key, None)
    command = ['taskset', '-c', '2' if a.cores == 1 else '0,2,4,6,8,10,12,14',
               str(binary), '--suite', a.suite, '--warmup', str(warmups),
               '--iters', str(iterations), '--include-parse',
               '--timeframe-rows', '1000000']
    for name in ('prices', 'prices_multi', 'trades', 'events', 'lookup', 'users'):
        flag = '--csv' if name == 'prices' else '--csv-' + name.removeprefix('prices_')
        command += [flag, str(Path(f'benchmarking/data/{name}.csv').resolve())]
    run = subprocess.run(command, env=env, capture_output=True, text=True, check=True)
    a.out.with_suffix(f'.{side}.stdout').write_text(run.stdout)
    a.out.with_suffix(f'.{side}.stderr').write_text(run.stderr)
    names = re.findall(r'^bench (\w+):', run.stdout, re.M)
    blocks = run.stderr.split('operator profile:')[1:]
    assert len(blocks) == len(names) * (warmups + iterations), (len(blocks), names)
    assert set(a.queries) <= set(names), set(a.queries) - set(names)
    results = {}
    for index, name in enumerate(names):
        if name not in a.queries:
            continue
        samples = {}
        start = index * (warmups + iterations) + warmups
        for block in blocks[start:start + iterations]:
            ops = {}
            for line in block.splitlines():
                match = re.match(r'profile node=\d+ op="([^"]+)" (.*)', line)
                if not match:
                    continue
                values = dict(re.findall(r'(\w+)=([\d.]+)', match[2]))
                op = ops.setdefault(match[1], dict.fromkeys(fields, 0.0))
                for field in fields:
                    op[field] += float(values[field])
            for op, values in ops.items():
                samples.setdefault(op, []).append(values)
        results[name] = {op: {field: statistics.median(row[field] for row in rows)
                             for field in fields} for op, rows in samples.items()}
    report['results'][side] = results
    print(side, 'profiled', flush=True)
a.out.write_text(json.dumps(report, indent=2) + '\n')
