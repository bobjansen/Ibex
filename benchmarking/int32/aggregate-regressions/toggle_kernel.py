#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Same-executable kernel counterfactuals with an unchanged replica.

Requires a diagnostic build from one of the adjacent patches. Production
contains no diagnostic switches. Frozen binaries remain in persistent storage
under build-int32-regressions/. The JSON report retains all raw samples.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ab_queries import wilcoxon_p

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--binary', type=Path, default=Path('build-release/tools/ibex_bench'))
p.add_argument('--csv', type=Path, default=Path('benchmarking/data/prices.csv'))
p.add_argument('--repeats', type=int, default=24)
p.add_argument('--iters', type=int, default=12)
p.add_argument('--suite', choices=['groupagg','join','core','filter'], default='groupagg')
p.add_argument('--csv-lookup', type=Path)
p.add_argument('--csv-trades', type=Path)
p.add_argument('--queries', nargs='+')
p.add_argument('--switch', choices=['IBEX_DIAG_BIND_COLLECT_COUNTS', 'IBEX_DIAG_BIND_COLLECT_SCATTER', 'IBEX_DIAG_BIND_CAT_PROBE', 'IBEX_DIAG_BIND_FLOAT_ACCUMULATION', 'IBEX_DIAG_BIND_ARITHMETIC_MASK', 'IBEX_DIAG_UNROLL_GATHER', 'IBEX_DIAG_ALIGN_GATHER'], default='IBEX_DIAG_BIND_COLLECT_COUNTS')
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
if a.repeats < 12 or a.repeats % 6: p.error('repeats must be a multiple of six, at least twelve')
keys = a.queries or (['inner_join_symbol'] if a.suite == 'join' else ['median_by_symbol', 'quantile_by_symbol', 'std_by_symbol'])
if a.suite == 'join' and a.csv_lookup is None: p.error('join needs --csv-lookup')
sides = {'off': '0', 'on': '1', 'replica': '0'}
samples = {s: {q: [] for q in keys} for s in sides}; orders = []; rows = {}
work = Path('build-int32-regressions') / a.out.stem; work.mkdir(parents=True, exist_ok=True)
report = {'suite': a.suite, 'switch': a.switch, 'repeats': a.repeats, 'inner_iterations': a.iters, 'warmups': 2,
          'binary': str(a.binary.resolve()), 'binary_sha256': hashlib.sha256(a.binary.read_bytes()).hexdigest(),
          'csv_sha256': hashlib.file_digest(a.csv.open('rb'), 'sha256').hexdigest(),
          'samples_ms': samples, 'orders': orders, 'rows': rows,
          'lookup_sha256': hashlib.sha256(a.csv_lookup.read_bytes()).hexdigest() if a.csv_lookup else None,
          'trades_sha256': hashlib.file_digest(a.csv_trades.open('rb'), 'sha256').hexdigest() if a.csv_trades else None}
pattern = re.compile(r'bench (\w+):.*?avg_ms=([\d.]+),.*?rows=(\d+),')
for repeat in range(a.repeats):
    order = list(sides); shift = repeat % 3; order = order[shift:] + order[:shift]
    if repeat // 3 % 2: order.reverse()
    orders.append(order)
    for side in order:
        env = dict(os.environ, IBEX_CORES='1', IBEX_COMPACT_INT32='0')
        env[a.switch] = sides[side]
        for key in ('IBEX_PROFILE_OPERATORS', 'IBEX_ANSWERS_DIR', 'IBEX_DIGEST_OUT'): env.pop(key, None)
        run = subprocess.run(['taskset', '-c', '2', str(a.binary.resolve()), '--suite', a.suite,
                              '--csv', str(a.csv.resolve()), '--warmup', '2', '--iters', str(a.iters),
                              '--no-include-parse'] + (['--csv-lookup',str(a.csv_lookup.resolve())] if a.csv_lookup else []) + (['--csv-trades',str(a.csv_trades.resolve())] if a.csv_trades else []), env=env, capture_output=True, text=True, check=True)
        (work/f'{repeat+1:02d}-{side}.log').write_text(run.stdout+run.stderr)
        found = {m[1]: (float(m[2]), int(m[3])) for m in pattern.finditer(run.stdout) if m[1] in keys}
        if set(found) != set(keys): raise RuntimeError('query set differs')
        for query, (ms, count) in found.items():
            if query in rows and rows[query] != count: raise RuntimeError('row count differs')
            rows[query] = count; samples[side][query].append(ms)
    a.out.parent.mkdir(parents=True, exist_ok=True); a.out.write_text(json.dumps(report, indent=2)+'\n')
    print(f'{repeat+1}/{a.repeats}', flush=True)
def comparison(after, before):
    return {'median_paired_ratio': statistics.median(t/b for t, b in zip(after, before)),
            'p': wilcoxon_p([t-b for t, b in zip(after, before)]),
            'slower_pairs': sum(t > b for t, b in zip(after, before))}
report['results'] = {q: {'median_ms': {s: statistics.median(samples[s][q]) for s in sides},
                         'on_off': comparison(samples['on'][q], samples['off'][q]),
                         'replica_off': comparison(samples['replica'][q], samples['off'][q])}
                     for q in keys}
for family in ('on_off', 'replica_off'):
    ordered = sorted(report['results'], key=lambda q: report['results'][q][family]['p']); running = 0.
    for rank, query in enumerate(ordered):
        record = report['results'][query][family]
        running = max(running, min(1., record['p']*(len(ordered)-rank))); record['holm_p'] = running
a.out.write_text(json.dumps(report, indent=2)+'\n'); print(json.dumps(report['results'], indent=2))
