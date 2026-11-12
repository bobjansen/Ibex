#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Build matching Parquet plugin and physical-width tests using an existing Arrow build.

Useful for the native experiment's lean CMake build (IBEX_BUILD_PARQUET=OFF).
Normal Parquet-enabled builds include these tests in ibex_tests automatically.
No Arrow or Ibex dependency is downloaded or silently rebuilt here.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root', type=Path, default=Path.cwd())
parser.add_argument('--arrow-build', type=Path, required=True)
parser.add_argument('--mutations', action='store_true')
args = parser.parse_args()
root = args.root.resolve()
build = root / 'build-release'
arrow_build = args.arrow_build.resolve()
work = build / 'int32-ingestion'
work.mkdir(exist_ok=True)
commands = json.loads((arrow_build / 'compile_commands.json').read_text())
entry = next(e for e in commands if e['file'].endswith('/libs/parquet/backend.cpp'))
reference = Path(entry['file']).parents[2]
command = shlex.split(entry['command'])
# Keep the actual Arrow includes/defines. Substitute only Ibex's source paths.
command = [a.replace(str(reference / 'include'), str(root / 'include'))
            .replace(str(reference / 'libs'), str(root / 'libs')) for a in command]
command = [a for a in command if a != 'ccache']
source_index = command.index('-c') + 1
output_index = command.index('-o') + 1


def compile_unit(source, output):
    invocation = command.copy()
    invocation[source_index] = str(source)
    invocation[output_index] = str(output)
    subprocess.run(invocation, check=True)


arrow_lib = arrow_build / '_deps/arrow-build/release'
libs = [build / f'src/{name}/libibex_{name}.a' for name in ('runtime', 'ir', 'core')]
link = [str(f) for f in libs] + [str(arrow_lib / 'libparquet.so'),
                               str(arrow_lib / 'libarrow.so'),
                               '-Wl,-rpath,' + str(arrow_lib), '-ldl', '-lmvec', '-pthread']
compiler = command[0]
compile_unit(root / 'libs/parquet/backend.cpp', work / 'backend.o')
compile_unit(root / 'libs/parquet/parquet.cpp', work / 'plugin.o')
subprocess.run([compiler, '-shared', str(work / 'plugin.o'), str(work / 'backend.o'),
                *link, '-o', str(build / 'tools/parquet.so')], check=True)
shutil.copy2(root / 'libs/parquet/parquet.ibex', build / 'tools/parquet.ibex')
catch = build / '_deps/catch2-build/src'
catch_link = [str(catch / 'libCatch2Main.a'), str(catch / 'libCatch2.a')]
test_source = root / 'tests/test_parquet_int32_ingestion.cpp'
test_command = command.copy()
# Backend compile flags do not include Catch2, so add its public headers.
catch_entry = next(e for e in json.loads((build / 'compile_commands.json').read_text())
                   if e['file'].endswith('/tests/test_compact_int32.cpp'))
for flag in shlex.split(catch_entry['command']):
    if 'catch2' in flag and flag.startswith('-I'):
        test_command.insert(1, flag)
# Catch2 includes can also be emitted as `-isystem PATH`.
catch_args = shlex.split(catch_entry['command'])
for i, flag in enumerate(catch_args[:-1]):
    if flag == '-isystem' and 'catch2' in catch_args[i + 1]:
        test_command[1:1] = ['-isystem', catch_args[i + 1]]


def run_test(source, name, should_fail=False):
    invocation = test_command.copy()
    invocation[invocation.index('-c') + 1] = str(source)
    obj = work / (name + '.o')
    invocation[invocation.index('-o') + 1] = str(obj)
    subprocess.run(invocation, check=True)
    binary = work / name
    subprocess.run([compiler, str(obj), *catch_link, *link, '-o', str(binary)], check=True)
    env = dict(os.environ)
    for cores in ('1', '8'):
        result = subprocess.run([str(binary), '[int32-ingestion]', '--reporter', 'compact'],
                                env=dict(env, IBEX_CORES=cores), capture_output=True, text=True)
        (work / f'{name}-{cores}.log').write_text(result.stdout + result.stderr)
        print(result.stdout, end='', flush=True)
        if (result.returncode != 0) != should_fail:
            raise RuntimeError(f'{name} cores={cores}: unexpected result {result.returncode}')


run_test(test_source, 'physical-width-tests')
if args.mutations:
    original = (root / 'libs/parquet/parquet.hpp').read_text()
    # In an isolated header copy, remove just the serial compact decoder.
    start = original.index('inline auto direct_column(')
    condition = 'if (field.type()->id() == arrow::Type::INT32)'
    assert condition in original[start:]
    mutant = original[:start] + original[start:].replace(condition, 'if (false)', 1)
    mutant_header = work / 'serial-wide-mutant.hpp'
    mutant_header.write_text(mutant)
    # Quoted sibling helper headers still come from the real parquet directory.
    test_command[1:1] = ['-I' + str(root / 'libs/parquet')]
    mutant_source = work / 'serial-wide-mutant.cpp'
    mutant_source.write_text(test_source.read_text().replace(
        '../libs/parquet/parquet.hpp', str(mutant_header)))
    run_test(mutant_source, 'serial-wide-mutant', should_fail=True)
    print('The physical-width test rejects eager widening in the serial decoder.')
