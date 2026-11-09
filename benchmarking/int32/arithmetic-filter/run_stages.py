#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired production-stage probes with fresh processes and an unchanged replica."""
import argparse
from contextlib import ExitStack
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
p.add_argument('--csv', type=Path, default=Path('benchmarking/data/trades.csv'))
p.add_argument('--repeats', type=int, default=24)
p.add_argument('--iters', type=int, default=12)
p.add_argument('--modes', nargs='+', default=['query','filter','mask','selection','materialize','presize','gather','gather_symbol','gather_price','gather_qty'])
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
if a.iters <= 0: p.error("iterations must be positive")
if a.repeats < 12 or a.repeats % 6: p.error('repeats must be a multiple of six, at least twelve')
work = Path('build-arithmetic-filter') / a.out.stem
work.mkdir(parents=True, exist_ok=True)
a.out.parent.mkdir(parents=True, exist_ok=True)
samples = {side: {mode: [] for mode in a.modes} for side in ('base','target','replica')}
orders = []
report = {'notes': __doc__, 'repeats': a.repeats, 'inner_iterations': a.iters, 'warmups': 2,
          'inner_statistic': 'median', 'cpu_affinity': '2', 'cores': 1,
          'fresh_processes_each_repeat': True, 'csv_sha256': hashlib.file_digest(a.csv.open('rb'), 'sha256').hexdigest(),
          'binaries': {side: {'path': str(binary.resolve()), 'sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest()} for side,binary in [('base',a.base),('target',a.target)]},
          'samples_ms': samples, 'orders': orders}
for repeat in range(a.repeats):
    side_order = list(samples); shift = repeat % 3
    side_order = side_order[shift:] + side_order[:shift]
    if repeat//3 % 2: side_order.reverse()
    modes = a.modes[repeat%len(a.modes):] + a.modes[:repeat%len(a.modes)]
    orders.append({'sides': side_order, 'modes': modes})
    workers = {}
    with ExitStack() as stack:
        trace = stack.enter_context((work/f'{repeat+1:02d}.log').open('w'))
        try:
            for side in side_order:
                binary = a.target if side == 'target' else a.base
                env = dict(os.environ, IBEX_CORES='1', IBEX_COMPACT_INT32='0')
                for key in ('IBEX_PROFILE_OPERATORS','IBEX_ANSWERS_DIR','IBEX_DIGEST_OUT'): env.pop(key,None)
                err = stack.enter_context((work/f'{repeat+1:02d}-{side}.stderr').open('w'))
                proc = subprocess.Popen(['taskset','-c','2',str(binary.resolve()),str(a.csv.resolve())], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True, env=env)
                workers[side] = proc
                ready = proc.stdout.readline().strip()
                trace.write(f'{side}: {ready}\n')
                if not ready.startswith('ready '): raise RuntimeError(f'{side} startup failed; see {err.name}')
                if 'ready' in report: assert ready == report['ready']
                report['ready'] = ready
            for mode in modes:
                for side in side_order:
                    proc = workers[side]
                    for iters, measured in [(2,False),(a.iters,True)]:
                        proc.stdin.write(f'{mode} {iters}\n'); proc.stdin.flush()
                        line = proc.stdout.readline().strip()
                        trace.write(f'{side} {mode} {iters}: {line}\n'); trace.flush()
                        if not line: raise RuntimeError(f'{side} failed in {mode}; see stderr')
                        if measured: samples[side][mode].append(float(line))
        finally:
            for proc in workers.values():
                proc.stdin.close()
                try: proc.wait(timeout=10)
                except subprocess.TimeoutExpired: proc.terminate(); proc.wait(timeout=10)
                proc.stdout.close()
    a.out.write_text(json.dumps(report,indent=2)+'\n')
    print(f'{repeat+1}/{a.repeats}',flush=True)
def comparison(after,before):
    return {'median_paired_ratio': statistics.median(t/b for t,b in zip(after,before)),
            'p': wilcoxon_p([t-b for t,b in zip(after,before)]), 'slower_pairs': sum(t>b for t,b in zip(after,before))}
report['results'] = {m: {'median_ms': {s:statistics.median(samples[s][m]) for s in samples},
                        'target_base': comparison(samples['target'][m],samples['base'][m]),
                        'target_replica': comparison(samples['target'][m],samples['replica'][m]),
                        'replica_base': comparison(samples['replica'][m],samples['base'][m])} for m in a.modes}
for family in ('target_base','target_replica','replica_base'):
    ordered = sorted(a.modes,key=lambda m:report['results'][m][family]['p']); running=0.
    for rank,mode in enumerate(ordered):
        record=report['results'][mode][family]
        running=max(running,min(1.,record['p']*(len(ordered)-rank)));record['holm_p']=running
a.out.write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report['results'],indent=2))
