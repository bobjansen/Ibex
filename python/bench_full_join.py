# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired full-join A/B across saved Release bridges, with full-result checks.

Pass --baseline-dir containing the old ibex_pyarrow module. Each bridge runs in
its own persistent process; only the requested route executes during a sample.
Use --direct to measure native same-name-key assembly instead of adapter aliases.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time


def worker(args):
    import numpy as np
    import polars as pl
    from polars.testing import assert_frame_equal
    import ibex_pyarrow
    from ibex_polars import IbexEngine
    from ibex_polars._engine import _to_polars

    rng = np.random.default_rng(20261025)
    keys = rng.integers(0, args.keys, args.rows, dtype=np.int64) * args.stride
    right_keys = np.arange(args.keys + args.keys // 5, dtype=np.int64) * args.stride
    left = pl.DataFrame({'k': keys, 'x': rng.random(args.rows)})
    right = pl.DataFrame({'k': right_keys, 'y': rng.random(len(right_keys))})
    query = left.lazy().join(right.lazy(), on='k', how='full', coalesce=True)
    engine = IbexEngine()
    source, bindings, scalars, expected, output = engine._prepare(query, pl.QueryOptFlags())
    if args.direct:
        source = '_ibex_input outer join _ibex_join_0_input on k;'
    def collect():
        return _to_polars(ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars),
                          expected, output)
    actual = collect()
    assert_frame_equal(actual, query.collect(), check_row_order=False)
    for _ in range(2):
        collect()
        query.collect()
    binary = Path(ibex_pyarrow.__file__).resolve()
    print(json.dumps({'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                      'polars_version': pl.__version__, 'polars_threads': pl.thread_pool_size(),
                      'output_rows': actual.height}), flush=True)
    previous = actual
    for route in sys.stdin:
        start = time.perf_counter_ns()
        current = collect() if route.strip() == 'ibex' else query.collect()
        end = time.perf_counter_ns()
        print((end - start) / 1e6, flush=True)
        previous = current  # Release the previous result after stopping the clock.


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-dir', type=Path)
    parser.add_argument('--threads', type=int, choices=(0, 1, 8), default=8, help='0: host defaults')
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--keys', type=int, default=200_000)
    parser.add_argument('--stride', type=int, default=1)
    parser.add_argument('--iters', type=int, default=20)
    parser.add_argument('--direct', action='store_true')
    parser.add_argument('--out', type=Path)
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.rows, args.keys, args.stride) < 1 or args.iters < 6 or args.iters % 2:
        parser.error('positive rows/keys/stride and an even iteration count >=6 required')
    if args.worker:
        return worker(args)
    if args.baseline_dir is None or args.out is None:
        parser.error('--baseline-dir and --out are required')
    root = Path(__file__).resolve().parents[1]
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in (root / 'build-release/CMakeCache.txt').read_text():
        parser.error('build-release must be configured as Release')
    workers, provenance = {}, {}
    samples = {name: [] for name in ('before', 'after', 'polars')}
    try:
        for name, directory in [('before', args.baseline_dir), ('after', root / 'build-release/python')]:
            env = os.environ.copy()
            env.update(PYTHONPATH=f'{directory.resolve()}:{root / "python"}', OPENBLAS_NUM_THREADS='1')
            for key in ('IBEX_CORES', 'POLARS_MAX_THREADS'):
                if args.threads:
                    env[key] = str(args.threads)
                else:
                    env.pop(key, None)
            command = [sys.executable, str(Path(__file__).resolve()), '--worker', '--rows', str(args.rows),
                       '--keys', str(args.keys), '--stride', str(args.stride)]
            if args.direct:
                command.append('--direct')
            proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    text=True, env=env)
            workers[name] = proc
            provenance[name] = json.loads(proc.stdout.readline())
        for iteration in range(args.iters):
            for name in (list(samples) if iteration % 2 == 0 else list(reversed(samples))):
                proc = workers['after' if name == 'polars' else name]
                proc.stdin.write('polars\n' if name == 'polars' else 'ibex\n')
                proc.stdin.flush()
                samples[name].append(float(proc.stdout.readline()))
    finally:
        for proc in workers.values():
            proc.stdin.close()
            if proc.wait(timeout=30) != 0:
                raise RuntimeError('benchmark worker failed')
    report = {'rows': args.rows, 'keys': args.keys, 'stride': args.stride, 'seed': 20261025,
              'threads': args.threads, 'direct_native': args.direct, 'iterations': args.iters,
              'affinity': sorted(os.sched_getaffinity(0)), 'bridges': provenance,
              'correctness': 'complete rows, values and dtypes matched Polars; order unspecified',
              'timed': 'prepared collection and conversion; two warmups; alternating route order; teardown excluded',
              'samples_ms': samples,
              'median_ms': {name: statistics.median(values) for name, values in samples.items()},
              'join_cpp_sha256': hashlib.sha256((root / 'src/runtime/join.cpp').read_bytes()).hexdigest(),
              'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report['median_ms']))


if __name__ == '__main__':
    main()
