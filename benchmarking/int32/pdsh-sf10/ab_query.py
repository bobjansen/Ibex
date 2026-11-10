#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Interleaved fresh-process A/B of query files and/or binaries for one query (warm REPL :run)."""
import argparse, json, re, statistics, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'benchmarking'))
from ab_queries import wilcoxon_p
TIME = re.compile(r"^time:\s*([\d.]+)\s*(us|ms|s)\s*$")
UNIT = {'us': 1e-3, 'ms': 1.0, 's': 1e3}
p = argparse.ArgumentParser()
p.add_argument('--variant', action='append', required=True, help='name=binary:script')
p.add_argument('--rounds', type=int, default=10); p.add_argument('--warmup', type=int, default=1)
p.add_argument('--iters', type=int, default=5); p.add_argument('--cpus', default='0,2,4,6,8,10,12,14')
p.add_argument('--cores', default='8'); p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
root = Path(__file__).resolve().parents[3]
vs = {}
for v in a.variant:
    name, rest = v.split('=', 1); binary, script = rest.split(':', 1); vs[name] = (Path(binary).resolve(), Path(script).resolve())
samples = {n: [] for n in vs}; answers = {}
names = list(vs)
for r in range(a.rounds):
    order = names[r % len(names):] + names[:r % len(names)]
    if r // len(names) % 2: order.reverse()
    for n in order:
        binary, script = vs[n]
        text = "\n".join([":timing on"] + [f":run {script}"] * (a.warmup + a.iters)) + "\n:quit\n"
        proc = subprocess.run(['taskset', '-c', a.cpus, str(binary), '--plugin-path', str(root/'build-release/tools')],
                              input=text, cwd=root, capture_output=True, text=True, timeout=600,
                              env={**__import__('os').environ, 'IBEX_CORES': a.cores})
        if proc.returncode: sys.exit(f'{n}: exit {proc.returncode}\n{proc.stderr[-2000:]}')
        t = [float(m[1]) * UNIT[m[2]] for m in map(TIME.match, proc.stdout.splitlines()) if m]
        assert len(t) == a.warmup + a.iters, (n, len(t))
        samples[n].append(statistics.median(t[a.warmup:]))
        body = '\n'.join(l for l in proc.stdout.splitlines() if not TIME.match(l))
        answers.setdefault(n, body)
        print(n, 'round', r, '%.1f' % samples[n][-1], flush=True)
report = {'variants': {n: {'binary': str(b), 'script': str(s), 'samples_ms': samples[n], 'median_ms': statistics.median(samples[n])} for n, (b, s) in vs.items()}}
base = names[0]
for n in names[1:]:
    ratios = [x / y for x, y in zip(samples[n], samples[base])]
    report['variants'][n]['vs_' + base] = {'median_ratio': statistics.median(ratios), 'p': wilcoxon_p([x - y for x, y in zip(samples[n], samples[base])])}
report['answers_equal'] = len(set(answers.values())) == 1
a.out.write_text(json.dumps(report, indent=2) + '\n')
for n, v in report['variants'].items(): print(n, '%.1f ms' % v['median_ms'], {k: v[k] for k in v if k.startswith('vs_')})
print('answers equal:', report['answers_equal'])
