#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compile one benchmark object, then link it against two built git states."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base-build', type=Path, required=True)
p.add_argument('--target-build', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
a.out.mkdir(parents=True, exist_ok=True)
root = Path(__file__).resolve().parents[3]
source = Path(__file__).with_name('filter_query.cpp')
obj = a.out / 'filter_query.o'
subprocess.run(['clang++', '-O3', '-DNDEBUG', '-std=c++23', '-march=native',
                '-Wall', '-Wextra', '-Wpedantic', '-Wconversion', '-Wshadow', '-Werror',
                '-I' + str(root / 'include'), '-isystem',
                str(root / 'build-release/_deps/robin_hood-src/src/include'),
                '-c', str(source), '-o', str(obj)], check=True)
for side, build in [('base', a.base_build), ('target', a.target_build)]:
    subprocess.run(['clang++', str(obj),
                    *(str(build / f'src/{part}/libibex_{part}.a')
                      for part in ('parser', 'runtime', 'ir', 'core')),
                    '-lmvec', '-pthread', '-o', str(a.out / side)], check=True)
(a.out / 'build.json').write_text(json.dumps({
    'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
    'object_sha256': hashlib.sha256(obj.read_bytes()).hexdigest(),
    'base_build': str(a.base_build.resolve()), 'target_build': str(a.target_build.resolve()),
    'binaries': {s: hashlib.sha256((a.out / s).read_bytes()).hexdigest()
                 for s in ('base', 'target')},
}, indent=2) + '\n')
