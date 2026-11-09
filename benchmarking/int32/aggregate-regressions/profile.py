#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Corroborate aggregate/probe timing changes with untimed operator profiles."""
import argparse
import json
import os
from pathlib import Path
import re
import statistics
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base', type=Path, required=True)
p.add_argument('--target', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
work = Path('build-int32-regressions/final-profiles')
work.mkdir(parents=True, exist_ok=True)
report = {'warmups': 2, 'iterations': 6, 'notes': __doc__, 'results': {}}
queries = {'ohlc_by_symbol', 'mean_by_symbol', 'inner_join_symbol'}
fields = ['build_self_ms', 'next_self_ms', 'rows', 'calls', 'pool_tasks', 'pool_work_ms']
for side, binary in [('base', a.base), ('target', a.target)]:
    report['results'][side] = {}
    for cores in (1, 8):
        cpus = '2' if cores == 1 else '0,2,4,6,8,10,12,14'
        env = dict(os.environ, IBEX_CORES=str(cores), IBEX_COMPACT_INT32='0', IBEX_PROFILE_OPERATORS='1')
        for key in ('IBEX_ANSWERS_DIR', 'IBEX_DIGEST_OUT'):
            env.pop(key, None)
        run = subprocess.run(['taskset', '-c', cpus, str(binary.resolve()), '--suite', 'core,join', '--csv', str(Path('benchmarking/data/prices.csv').resolve()), '--csv-lookup', str(Path('benchmarking/data/lookup.csv').resolve()), '--warmup', '2', '--iters', '6', '--no-include-parse'], env=env, capture_output=True, text=True, check=True)
        (work / f'{side}-{cores}.stdout').write_text(run.stdout)
        (work / f'{side}-{cores}.stderr').write_text(run.stderr)
        names = re.findall(r'^bench (\w+):', run.stdout, re.M)
        blocks = run.stderr.split('operator profile:')[1:]
        assert len(blocks) == len(names) * 8, (len(blocks), len(names))
        results = {}
        for index, name in enumerate(names):
            if name not in queries:
                continue
            samples = {}
            for block in blocks[index*8+2:(index+1)*8]:
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
            results[name] = {op: {field: statistics.median(row[field] for row in rows) for field in fields} for op, rows in samples.items()}
        report['results'][side][str(cores)] = results
        print(side, cores, 'profiled', flush=True)
a.out.parent.mkdir(parents=True, exist_ok=True)
a.out.write_text(json.dumps(report, indent=2) + '\n')
