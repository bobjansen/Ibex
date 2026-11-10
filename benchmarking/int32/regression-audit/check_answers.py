#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare complete typed answers on bounded deterministic audit workloads."""
import argparse
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base', type=Path, required=True)
p.add_argument('--target', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
work = a.out.parent / 'bounded-answers'
work.mkdir(parents=True, exist_ok=True)
prices = work / 'prices.csv'
with Path('benchmarking/data/prices.csv').open(newline='') as src, prices.open('w', newline='') as dst:
    csv.writer(dst).writerows(itertools.islice(csv.reader(src), 10001))
lookup = Path('benchmarking/data/lookup.csv').resolve()
def digest(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()
report = {'rows': 10000, 'suite': 'scalar,null,timeframe', 'compact': 0,
          'binaries': {}, 'answers': {},
          'fixtures': {str(prices): digest(prices), str(lookup): digest(lookup)}}
for side, binary in (('base', a.base), ('target', a.target)):
    binary = binary.resolve()
    report['binaries'][side] = {'path': str(binary), 'sha256': digest(binary)}
    for cores in (1, 8):
        name = f'{side}-{cores}'
        directory = work / name
        directory.mkdir(exist_ok=True)
        for stale in directory.glob('*.csv'):
            stale.unlink()
        env = dict(os.environ, IBEX_CORES=str(cores), IBEX_COMPACT_INT32='0',
                   IBEX_ANSWERS_DIR=str(directory.resolve()))
        for key in ('IBEX_PROFILE_OPERATORS', 'IBEX_DIGEST_OUT'):
            env.pop(key, None)
        with (work / f'{name}.log').open('w') as log:
            subprocess.run([str(binary), '--suite', report['suite'], '--csv', str(prices.resolve()),
                            '--csv-lookup', str(lookup), '--timeframe-rows', '10000',
                            '--warmup', '0', '--iters', '1', '--no-include-parse'],
                           env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        hashes = {f.name: digest(f) for f in sorted(directory.glob('*.csv'))}
        assert len(hashes) == 29, (name, len(hashes))
        if side == 'target':
            assert hashes == report['answers'][f'base-{cores}'], name
        report['answers'][name] = hashes
        a.out.write_text(json.dumps(report, indent=2) + '\n')
        print(name, '29 typed answers checked', flush=True)
