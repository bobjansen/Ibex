#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Actual native join operator: Int64 keys, Int64 versus compact Int32 payloads."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ab_queries import wilcoxon_p

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, default=Path('build-release/tools/ibex_bench_compact_int32'))
p.add_argument('--rows', type=int, nargs='+', default=[1000000, 10000000])
p.add_argument('--cores', type=int, nargs='+', default=[1, 8])
p.add_argument('--iters', type=int, default=12)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
if a.iters < 6 or a.iters % 2 or any(c not in (1,8) for c in a.cores):
    p.error('even iters >=6 and cores in {1,8} required')
report = {'iterations': a.iters, 'notes': __doc__,
          'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest(), 'cases': []}
for cores in a.cores:
 for rows in a.rows:
    workers = {}
    try:
        for name, mode in [('wide','0'),('compact','1')]:
            env = dict(os.environ, IBEX_CORES=str(cores))
            cpus = ','.join(str(c*2) for c in range(cores))
            proc = subprocess.Popen(['taskset','-c',cpus,str(a.binary.resolve()),str(rows),mode],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
            workers[name] = proc
            if proc.stdout.readline().strip() != 'ready': raise RuntimeError('worker startup failed')
        samples = {k: [] for k in workers}
        cpu_samples = {k: [] for k in workers}
        for iteration in range(-2,a.iters):
            checks = []
            for name in (list(workers) if iteration%2 == 0 else list(reversed(workers))):
                proc = workers[name]
                proc.stdin.write('run\n'); proc.stdin.flush()
                response = proc.stdout.readline().split()
                if len(response) != 3: raise RuntimeError('native join correctness/reachability check failed')
                ms, check, cpu = response
                checks.append(check)
                if iteration >= 0:
                    samples[name].append(float(ms))
                    cpu_samples[name].append(float(cpu))
            if checks[0] != checks[1]: raise RuntimeError('width outputs disagree')
        base, target = samples['wide'], samples['compact']
        case = {'rows': rows, 'cores': cores, 'samples_ms': samples,
                'median_ms': {k: statistics.median(v) for k,v in samples.items()},
                'cpu_samples_ms': cpu_samples,
                'median_cpu_ms': {k: statistics.median(v) for k,v in cpu_samples.items()},
                'compact_over_wide': statistics.median([t/b for t,b in zip(target,base)]),
                'p': wilcoxon_p([t-b for t,b in zip(target,base)]), 'checksum': checks[0]}
        report['cases'].append(case)
        a.out.parent.mkdir(parents=True,exist_ok=True)
        a.out.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({k:v for k,v in case.items() if k!='samples_ms'}),flush=True)
    finally:
        for proc in workers.values():
            proc.stdin.close()
            if proc.wait(timeout=30) != 0: raise RuntimeError('worker failed')
