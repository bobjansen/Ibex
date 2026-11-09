#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Build identical stage-probe source against each state's own headers/libraries.

The original and compact Column layouts differ: sharing a caller object across
these states would be an ABI error. Reuse each state's actual benchmark flags.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--base-build', type=Path, required=True)
p.add_argument('--target-build', type=Path, required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
a.out.mkdir(parents=True, exist_ok=True)
source = Path(__file__).with_name('stages.cpp').resolve()
report = {'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'states': {}}
for side, build in [('base', a.base_build), ('target', a.target_build)]:
    build = build.resolve()
    entry = next(e for e in json.loads((build/'compile_commands.json').read_text()) if e['file'].endswith('/tools/ibex_bench.cpp'))
    state = Path(entry['file']).parents[1]
    obj = (a.out / f'{side}.o').resolve()
    args = shlex.split(entry['command'])
    if Path(args[0]).name == 'ccache': args = args[1:]
    args[args.index('-c')+1] = str(source)
    args[args.index('-o')+1] = str(obj)
    args[1:1] = ['-I'+str(state/'src/runtime'), '-I'+str(state/'libs/csv')]
    subprocess.run(args, cwd=entry['directory'], check=True)
    command = subprocess.check_output(['ninja', '-C', str(build), '-t', 'commands', 'tools/ibex_bench'], text=True).splitlines()[-1]
    link = shlex.split(command)
    assert link[:2] == [':', '&&']
    link = link[2:link.index('&&', 2)]
    for i in range(len(link)-2, -1, -1):
        if link[i] == '-Xlinker' and link[i+1].startswith('--dependency-file='): del link[i:i+2]
    index = next(i for i, x in enumerate(link) if x.endswith('/ibex_bench.cpp.o'))
    link[index] = str(obj)
    binary = (a.out/side).resolve()
    link[link.index('-o')+1] = str(binary)
    subprocess.run(link, cwd=build, check=True)
    report['states'][side] = {'revision_at_build': subprocess.check_output(['git','-C',str(state),'rev-parse','HEAD'],text=True).strip(), 'source_tree': str(state), 'build': str(build), 'compile': args, 'link': link,
                             'object_sha256': hashlib.sha256(obj.read_bytes()).hexdigest(), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
(a.out/'build.json').write_text(json.dumps(report, indent=2)+'\n')
