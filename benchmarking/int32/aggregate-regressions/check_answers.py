#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare full typed answers on the measured CSV datasets, outside timing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base', type=Path, required=True)
p.add_argument('--target', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
a.out.parent.mkdir(parents=True, exist_ok=True)
work = Path('build-int32-regressions/answers').resolve()
work.mkdir(parents=True, exist_ok=True)
report = {'datasets': {}, 'binaries': {}, 'answers': {}}
paths = ['prices', 'prices_multi', 'trades', 'events', 'lookup', 'users']
args = []
for name in paths:
    path = Path(f'benchmarking/data/{name}.csv').resolve()
    report['datasets'][name] = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
    option = '--csv' if name == 'prices' else '--csv-multi' if name == 'prices_multi' else '--csv-' + name
    args += [option, str(path)]
for side, binary in [('base', a.base), ('target', a.target)]:
    binary = binary.resolve()
    report['binaries'][side] = {'path': str(binary), 'sha256': hashlib.file_digest(binary.open('rb'), 'sha256').hexdigest()}
    for cores in (1, 8):
        for compact in ((0,) if side == 'base' else (0, 1)):
            name = f'{side}-{cores}-{compact}'
            directory = work / name
            directory.mkdir(exist_ok=True)
            env = dict(os.environ, IBEX_CORES=str(cores), IBEX_COMPACT_INT32=str(compact), IBEX_ANSWERS_DIR=str(directory))
            env.pop('IBEX_PROFILE_OPERATORS', None)
            env.pop('IBEX_DIGEST_OUT', None)
            with (work / (name + '.log')).open('w') as log:
                subprocess.run([str(binary), '--suite', 'core,filter,join,groupagg', '--warmup', '0', '--iters', '1', '--no-include-parse'] + args, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
            hashes = {f.name: hashlib.file_digest(f.open('rb'), 'sha256').hexdigest() for f in sorted(directory.glob('*.csv'))}
            assert len(hashes) == 23, (name, len(hashes))
            if side == 'target':
                assert hashes == report['answers'][f'base-{cores}-0'], f'Full typed answers differ: {name}'
            report['answers'][name] = hashes
            a.out.write_text(json.dumps(report, indent=2) + '\n')
            print(name, '23 full typed answers match' if side == 'target' else 'reference recorded', flush=True)
