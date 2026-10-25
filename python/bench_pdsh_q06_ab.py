# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired unchanged upstream Q6 comparison across saved Python/native builds."""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time


def worker(args):
    import polars as pl
    from polars.testing import assert_frame_equal
    import ibex_pyarrow
    from ibex_polars import IbexEngine
    import ibex._lower

    sys.path.insert(0, str(args.pdsh_root.resolve()))
    module = importlib.import_module('queries.polars.q6')
    query = module.q(lineitem=pl.scan_parquet(args.lineitem))
    engine = IbexEngine().prepare(query)
    calls = {'ibex': lambda: query.collect(engine=engine),
             'polars': lambda: query.collect(engine='streaming')}
    first, results = {}, {}
    for name, call in calls.items():
        start = time.perf_counter_ns()
        results[name] = call()
        first[name] = (time.perf_counter_ns() - start) / 1e6
    assert_frame_equal(results['ibex'], results['polars'], rel_tol=1e-10, abs_tol=1e-8)
    for _ in range(2):
        for call in calls.values():
            call()
    files = {'bridge': ibex_pyarrow.__file__, 'lowerer': ibex._lower.__file__,
             'query': module.__file__}
    print(json.dumps({'sha256': {name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
                                 for name, path in files.items()},
                      'polars_version': pl.__version__, 'polars_threads': pl.thread_pool_size(),
                      'source': engine.explain(query), 'first_collect_ms': first,
                      'result': results['ibex'].to_dict(as_series=False)}), flush=True)
    previous = results['ibex']
    for name in sys.stdin:
        start = time.perf_counter_ns()
        current = calls[name.strip()]()
        elapsed = (time.perf_counter_ns() - start) / 1e6
        previous = current
        print(elapsed, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-python', type=Path)
    parser.add_argument('--baseline-bridge', type=Path)
    parser.add_argument('--pdsh-root', type=Path, required=True)
    parser.add_argument('--lineitem', type=Path, required=True)
    parser.add_argument('--iters', type=int, default=6)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        return worker(args)
    if args.iters < 6 or args.iters % 2 or not all((args.baseline_python, args.baseline_bridge, args.out)):
        parser.error('baseline paths, --out and an even --iters >= 6 are required')
    root = Path(__file__).resolve().parents[1]
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in (root / 'build-release/CMakeCache.txt').read_text():
        parser.error('build-release must be Release')
    processes, metadata = {}, {}
    samples = {name: [] for name in ('before', 'after', 'polars')}
    try:
        for name, bridge, python in [('before', args.baseline_bridge, args.baseline_python),
                                     ('after', root / 'build-release/python', root / 'python')]:
            env = os.environ.copy()
            env.update(PYTHONPATH=f'{bridge.resolve()}:{python.resolve()}', OPENBLAS_NUM_THREADS='1')
            # -c keeps the runner's directory from shadowing the selected Python package.
            command = [sys.executable, '-c',
                       f'import runpy; runpy.run_path({str(Path(__file__).resolve())!r}, run_name="__main__")',
                       '--worker', '--pdsh-root', str(args.pdsh_root), '--lineitem', str(args.lineitem)]
            proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    env=env, text=True)
            processes[name] = proc
            metadata[name] = json.loads(proc.stdout.readline())
        for iteration in range(args.iters):
            for name in (list(samples) if iteration % 2 == 0 else list(reversed(samples))):
                proc = processes['after' if name == 'polars' else name]
                proc.stdin.write('polars\n' if name == 'polars' else 'ibex\n')
                proc.stdin.flush()
                samples[name].append(float(proc.stdout.readline()))
    finally:
        for proc in processes.values():
            proc.stdin.close()
            if proc.wait(timeout=30) != 0:
                raise RuntimeError('benchmark worker failed')
    if metadata['before']['result'] != metadata['after']['result']:
        raise AssertionError('Q6 before/after results differ')
    report = {'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
              'lineitem': str(args.lineitem.resolve()), 'input_bytes': args.lineitem.stat().st_size,
              'ibex_cores': os.environ.get('IBEX_CORES'), 'polars_max_threads': os.environ.get('POLARS_MAX_THREADS'),
              'affinity': sorted(os.sched_getaffinity(0)), 'workers': metadata,
              'iterations': args.iters, 'exact_before_after_result_match': True,
              'native_worktree_patch': subprocess.check_output(
                  ['git', 'diff', 'HEAD', '--', 'src', 'include'], cwd=root, text=True),
              'cpu_frequency_khz': {name: Path('/sys/devices/system/cpu/cpu0/cpufreq', name).read_text().strip()
                                    for name in ('scaling_min_freq', 'scaling_max_freq')},
              'method': 'two warmups after correctness collections; paired alternating route order; prepared Parquet collection including result conversion; previous-result destruction excluded',
              'samples_ms': samples, 'median_ms': {name: statistics.median(v) for name, v in samples.items()}}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report['median_ms']), flush=True)


if __name__ == '__main__':
    main()
