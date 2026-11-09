#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Native resident filter A/B: two sizes/densities/core budgets and a wide control.

Each sample checks every output value, row order, validity and physical width.
Input creation, full checking and result destruction are outside the timer.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ab_queries import wilcoxon_p

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base', type=Path, required=True)
p.add_argument('--target', type=Path, required=True)
p.add_argument('--rows', type=int, nargs='+', default=[1000000, 10000000])
p.add_argument('--keep', type=int, nargs='+', default=[10, 50])
p.add_argument('--cores', type=int, nargs='+', default=[1, 8])
p.add_argument('--iters', type=int, default=20)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
if a.iters < 10 or a.iters % 10 or any(n <= 0 or n % 100 for n in a.rows):
    p.error('iterations must be a positive multiple of 10; rows positive multiples of 100')
if any(c not in (1, 8) for c in a.cores) or any(k not in range(1, 100) for k in a.keep):
    p.error('cores must be 1/8 and keep between 1 and 99')
report = {'iterations': a.iters, 'warmups': 2, 'notes': __doc__, 'cases': [],
          'source_sha256': hashlib.sha256(Path(__file__).with_name('filter_query.cpp').read_bytes()).hexdigest(),
          'binaries': {s: {'path': str(b.resolve()), 'sha256': hashlib.sha256(b.read_bytes()).hexdigest()}
                       for s, b in [('base', a.base), ('target', a.target)]}}
sides = {'base32': (a.base, '1'), 'target32': (a.target, '1'),
         'replica32': (a.base, '1'), 'base64': (a.base, '0'), 'target64': (a.target, '0')}
def comparison(after, before):
    return {'median_paired_ratio': statistics.median(t/b for t, b in zip(after, before)),
            'p': wilcoxon_p([t-b for t, b in zip(after, before)]),
            'slower_pairs': sum(t > b for t, b in zip(after, before))}
for cores in a.cores:
 for rows in a.rows:
  for keep in a.keep:
    workers = {}
    try:
        cpus = ','.join(str(c*2) for c in range(cores))
        for side, (binary, mode) in sides.items():
            proc = subprocess.Popen(['taskset', '-c', cpus, str(binary.resolve()), str(rows), mode, str(keep)],
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                                    env=dict(os.environ, IBEX_CORES=str(cores), IBEX_COMPACT_INT32='0'))
            workers[side] = proc
            if proc.stdout.readline().strip() != 'ready': raise RuntimeError('worker startup failed')
        samples = {s: [] for s in sides}; cpu = {s: [] for s in sides}; orders = []
        output_bytes = {}; compact_columns = {}
        for iteration in range(-2, a.iters):
            order = list(sides); shift = iteration % len(order)
            order = order[shift:] + order[:shift]
            if (iteration // len(order)) % 2: order.reverse()
            if iteration >= 0: orders.append(order)
            checks = []
            for side in order:
                proc = workers[side]; proc.stdin.write('run\n'); proc.stdin.flush()
                response = proc.stdout.readline().split()
                if len(response) != 5: raise RuntimeError(f'{side}: query/check failed')
                ms, cpu_ms, check, nbytes, narrow = response
                expected = 4 if side == 'target32' else 0
                if int(narrow) != expected: raise RuntimeError(f'{side}: unexpected physical width')
                output_bytes[side] = int(nbytes); compact_columns[side] = int(narrow)
                checks.append(check)
                if iteration >= 0:
                    samples[side].append(float(ms)); cpu[side].append(float(cpu_ms))
            if len(set(checks)) != 1: raise RuntimeError('output checksums disagree')
        case = {'rows': rows, 'keep_percent': keep, 'cores': cores, 'affinity': cpus,
                'orders': orders, 'samples_ms': samples, 'cpu_samples_ms': cpu,
                'median_ms': {s: statistics.median(v) for s, v in samples.items()},
                'output_value_bytes': output_bytes, 'compact_columns': compact_columns,
                'compact_change': comparison(samples['target32'], samples['base32']),
                'replica_control': comparison(samples['replica32'], samples['base32']),
                'wide_change': comparison(samples['target64'], samples['base64']),
                'new_compact_over_wide': comparison(samples['target32'], samples['target64'])}
        report['cases'].append(case)
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps({k: v for k, v in case.items() if k not in ('orders', 'samples_ms', 'cpu_samples_ms')}), flush=True)
    finally:
        for proc in workers.values(): proc.stdin.close()
        for proc in workers.values():
            if proc.wait(timeout=30) != 0: raise RuntimeError('worker failed')
for family in ('compact_change', 'replica_control', 'wide_change', 'new_compact_over_wide'):
    ordered = sorted(report['cases'], key=lambda c: c[family]['p']); running = 0.
    for rank, case in enumerate(ordered):
        running = max(running, min(1., case[family]['p'] * (len(ordered)-rank)))
        case[family]['holm_p'] = running
a.out.write_text(json.dumps(report, indent=2)+'\n')
