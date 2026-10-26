# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare unchanged upstream PDS-H queries using Polars and IbexEngine.

Parquet input is read during collection. Translation is prepared outside timing.
Unsupported queries are recorded explicitly; no fallback or query rewrites.
Run each thread configuration in a separate process with matched budgets.
"""
import argparse
import hashlib
import importlib
from itertools import permutations
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pdsh-root', type=Path, required=True)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--iters', type=int, default=6)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--check-only', action='store_true', help='compare complete results without timing loops')
    parser.add_argument('--queries', type=int, nargs='+', choices=range(1, 23), default=list(range(1, 23)))
    args = parser.parse_args()
    if not args.check_only and (args.iters < 2 or args.iters % 2):
        parser.error('--iters must be even and at least two')
    root = Path(__file__).resolve().parents[1]
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in (root / 'build-release/CMakeCache.txt').read_text():
        parser.error('build-release must be configured as Release')
    import polars as pl
    from polars.testing import assert_frame_equal
    import ibex_pyarrow
    from ibex_polars import IbexEngine

    sys.path.insert(0, str(args.pdsh_root.resolve()))
    paths = sorted(args.data.resolve().glob('*.parquet'))
    if len(paths) != 8:
        parser.error('--data must contain the eight PDS-H Parquet tables')
    tables = {p.stem: pl.scan_parquet(p) for p in paths}
    bridge = Path(ibex_pyarrow.__file__).resolve()
    report = {
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'upstream_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.pdsh_root, text=True).strip(),
        'upstream_worktree_patch': subprocess.check_output(
            ['git', 'diff', 'HEAD', '--', 'queries/polars'], cwd=args.pdsh_root, text=True),
        'bridge_sha256': hashlib.sha256(bridge.read_bytes()).hexdigest(),
        'bridge': str(bridge), 'polars_version': pl.__version__,
        'python_source_sha256': {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (root / 'python/ibex_polars/_adapter.py', root / 'python/ibex_polars/_engine.py',
                         root / 'python/ibex/_lower.py', Path(__file__).resolve())},
        'native_worktree_patch': subprocess.check_output(
            ['git', 'diff', 'HEAD', '--', 'src', 'include'], cwd=root, text=True),
        'ibex_cores': os.environ.get('IBEX_CORES'), 'polars_threads': pl.thread_pool_size(),
        'affinity': sorted(os.sched_getaffinity(0)),
        'data': [{'path': str(p), 'bytes': p.stat().st_size, 'mtime_ns': p.stat().st_mtime_ns} for p in paths],
        'iterations': 0 if args.check_only else args.iters,
        'check_only': args.check_only, 'query_numbers': args.queries,
        'method': ('Parquet collection; prepared translation; complete correctness comparisons; no warmups or timing loops'
                   if args.check_only else
                   'Parquet collection; prepared translation; two warmups after first collection; alternating route order; previous-result destruction excluded; no result caching'),
        'correctness': 'all rows, order and dtypes; rel_tol=1e-10 abs_tol=1e-8; both Polars executors compared',
        'cpu_frequency_khz': {name: Path('/sys/devices/system/cpu/cpu0/cpufreq', name).read_text().strip()
                              for name in ('scaling_min_freq', 'scaling_max_freq')},
        'ibex_decode_threads': os.environ.get('IBEX_DECODE_THREADS'),
        'ibex_stream_scan': os.environ.get('IBEX_STREAM_SCAN'),
        'ibex_join_probe': os.environ.get('IBEX_JOIN_PROBE'),
        'cpu_timing': 'process_time_ns over collection; sums all threads; excludes correctness checks',
        'queries': {},
    }
    def save():
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + '\n')
    for number in args.queries:
        name = f'q{number:02}'
        entry = report['queries'][name] = {}
        module = importlib.import_module(f'queries.polars.q{number}')
        entry['upstream_source_sha256'] = hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
        query = module.q(**tables)
        engine = IbexEngine()
        started = time.perf_counter_ns()
        try:
            engine.prepare(query)
        except Exception as exc:
            entry.update(status='unsupported' if isinstance(exc, NotImplementedError) else 'failed',
                         error=f'{type(exc).__name__}: {exc}')
            print(name, entry['error'], flush=True)
            save()
            continue
        entry['prepare_ms'] = (time.perf_counter_ns() - started) / 1e6
        entry['native_source'] = engine.explain(query)
        calls = {'polars': lambda: query.collect(engine='in-memory'),
                 'polars_streaming': lambda: query.collect(engine='streaming'),
                 'polars_ibex': lambda: query.collect(engine=engine)}
        print(name, 'checking full results' + ('' if args.check_only else ', then timing'), flush=True)
        try:
            first, outputs = {}, {}
            for route, call in calls.items():
                started = time.perf_counter_ns()
                outputs[route] = call()
                first[route] = (time.perf_counter_ns() - started) / 1e6
            for route in ('polars_streaming', 'polars_ibex'):
                assert_frame_equal(outputs[route], outputs['polars'], check_row_order=True,
                                   rel_tol=1e-10, abs_tol=1e-8)
            entry.update(first_collect_ms=first, output_rows=outputs['polars'].height)
            if args.check_only:
                entry.update(status='passed', native_source=engine.explain(query))
                print(name, 'passed', outputs['polars'].shape, flush=True)
                save()
                continue
            for _ in range(2):
                for call in calls.values():
                    assert_frame_equal(call(), outputs['polars'], check_row_order=True,
                                       rel_tol=1e-10, abs_tol=1e-8)
            samples = {route: [] for route in calls}
            cpu_samples = {route: [] for route in calls}
            previous = None
            for iteration in range(args.iters):
                # Six permutations balance each route's position in each block.
                order = list(permutations(calls))[iteration % 6]
                for route in order:
                    started = time.perf_counter_ns()
                    cpu_started = time.process_time_ns()
                    current = calls[route]()
                    cpu_elapsed = (time.process_time_ns() - cpu_started) / 1e6
                    elapsed = (time.perf_counter_ns() - started) / 1e6
                    previous = current
                    assert_frame_equal(current, outputs['polars'], check_row_order=True,
                                       rel_tol=1e-10, abs_tol=1e-8)
                    samples[route].append(elapsed)
                    cpu_samples[route].append(cpu_elapsed)
                print(name, f'round {iteration + 1}/{args.iters}', flush=True)
            entry.update(status='passed', samples_ms=samples,
                         median_ms={route: statistics.median(values) for route, values in samples.items()},
                         cpu_samples_ms=cpu_samples,
                         median_cpu_ms={route: statistics.median(values) for route, values in cpu_samples.items()})
            print(name, json.dumps(entry['median_ms']), flush=True)
        except Exception as exc:
            entry.update(status='failed', error=f'{type(exc).__name__}: {exc}')
            print(name, entry['error'], flush=True)
        save()
    if any(entry.get('status') == 'failed' for entry in report['queries'].values()):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
