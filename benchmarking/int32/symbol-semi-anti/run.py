#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Same-executable compact/wide native join probe, with full untimed validation."""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ab_queries import wilcoxon_p

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path)
p.add_argument('--base', type=Path)
p.add_argument('--target', type=Path)
p.add_argument('--rows', type=int, nargs='+', default=[1000000])
p.add_argument('--keys', type=int, nargs='+', default=[128, 4096])
p.add_argument('--cores', type=int, nargs='+', default=[1, 8])
p.add_argument('--nullable', type=int, nargs='+', choices=[0, 1], default=[0, 1])
p.add_argument('--repeats', type=int, default=18)
p.add_argument('--iters', type=int, default=3)
p.add_argument('--right-rows', type=int, default=0)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
if a.repeats < 12 or a.repeats % 6 or any(c not in (1, 8) for c in a.cores):
    p.error('repeats must be a multiple of six >=12, cores must be 1 or 8')
if a.binary is not None:
    assert a.base is None and a.target is None
    assert b'IBEX_DIAG_BIND_SEMI_KEYS' in a.binary.read_bytes()
    binaries = dict.fromkeys(('off','on','replica'), a.binary)
else:
    if a.base is None or a.target is None:
        p.error('provide --binary for a toggle, or both --base and --target')
    binaries = {'off': a.base, 'on': a.target, 'replica': a.base}
a.out.parent.mkdir(parents=True, exist_ok=True)
report = {'binaries': {side: {'path':str(binary.resolve()), 'sha256':hashlib.sha256(binary.read_bytes()).hexdigest()} for side,binary in binaries.items()},
          'comparison': 'same-executable-toggle' if a.binary else 'binaries',
          'source_sha256': hashlib.sha256(Path(__file__).with_name('bench.cpp').read_bytes()).hexdigest(),
          'repeats': a.repeats, 'iterations': a.iters, 'warmups': 2, 'cases': []}
for rows, keys, cores, nullable, compact in itertools.product(a.rows, a.keys, a.cores, a.nullable, (0, 1)):
    workers = {}
    samples = {kind: {side: [] for side in ('off', 'on', 'replica')} for kind in ('semi', 'anti')}
    sums = {}
    try:
        for side in ('off', 'on', 'replica'):
            env = dict(os.environ, IBEX_CORES=str(cores), IBEX_COMPACT_INT32='0',
                       IBEX_DIAG_BIND_SEMI_KEYS='1' if side == 'on' else '0')
            for key in ('IBEX_PROFILE_OPERATORS', 'IBEX_ANSWERS_DIR', 'IBEX_DIGEST_OUT'):
                env.pop(key, None)
            cpus = '2' if cores == 1 else '0,2,4,6,8,10,12,14'
            workers[side] = subprocess.Popen(['taskset', '-c', cpus, str(binaries[side].resolve()),
                                             str(rows), str(keys), str(compact), str(nullable),
                                             str(a.right_rows or keys // 2)], env=env,
                                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            assert workers[side].stdout.readline().strip() == 'ready'
        for repeat in range(a.repeats):
            order = list(workers); shift = repeat % 3
            order = order[shift:] + order[:shift]
            if repeat // 3 % 2:
                order.reverse()
            for kind in ('semi', 'anti') if repeat % 2 == 0 else ('anti', 'semi'):
                for side in order:
                    proc = workers[side]
                    for iters, timed in ((2, False), (a.iters, True)):
                        proc.stdin.write(f'{kind} {iters}\n'); proc.stdin.flush()
                        values = proc.stdout.readline().split()
                        assert len(values) == 2, (side, kind, values)
                        ms, checksum = float(values[0]), int(values[1])
                        assert checksum == sums.setdefault(kind, checksum)
                        if timed:
                            samples[kind][side].append(ms)
        for kind, values in samples.items():
            def compare(after, before):
                return {'ratio': statistics.median(t / b for t, b in zip(after, before)),
                        'p': wilcoxon_p([t - b for t, b in zip(after, before)])}
            report['cases'].append({'rows': rows, 'keys': keys, 'cores': cores, 'nullable': nullable,
                                    'compact': compact, 'right_rows': a.right_rows or keys // 2, 'kind': kind, 'samples_ms': values,
                                    'on_off': compare(values['on'], values['off']),
                                    'replica_off': compare(values['replica'], values['off'])})
        a.out.write_text(json.dumps(report, indent=2) + '\n')
        print(rows, keys, cores, nullable, compact, 'checked', flush=True)
    finally:
        for proc in workers.values():
            proc.stdin.close()
            assert proc.wait(timeout=30) == 0
for family in ('on_off', 'replica_off'):
    order = sorted(report['cases'], key=lambda c: c[family]['p']); adjusted = 0.
    for i, case in enumerate(order):
        adjusted = max(adjusted, min(1., case[family]['p'] * (len(order) - i)))
        case[family]['holm_p'] = adjusted
a.out.write_text(json.dumps(report, indent=2) + '\n')
