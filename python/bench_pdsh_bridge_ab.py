# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired PDS-H comparison of two saved native bridges and streaming Polars.

Both workers use the same Python adapter, isolating native executor changes.
Run each thread budget separately, with matched IBEX_CORES/POLARS_MAX_THREADS.
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


def worker(args):
    import ibex_pyarrow
    import polars as pl
    from polars.testing import assert_frame_equal
    from ibex_polars import IbexEngine

    sys.path.insert(0, str(args.pdsh_root.resolve()))
    tables = {p.stem: pl.scan_parquet(p) for p in sorted(args.data.resolve().glob('*.parquet'))}
    bridge = Path(ibex_pyarrow.__file__).resolve()
    previous = None
    for line in sys.stdin:
        command = json.loads(line)
        if 'prepare' in command:
            module = importlib.import_module(f'queries.polars.q{command["prepare"]}')
            query = module.q(**tables)
            engine = IbexEngine().prepare(query)
            reference = query.collect(engine='in-memory')
            previous = None
            response = {
                'source': engine.explain(query), 'output_shape': reference.shape,
                'schema': str(reference.schema), 'polars_version': pl.__version__,
                'upstream_source_sha256': hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
                'bridge': str(bridge), 'bridge_sha256': hashlib.sha256(bridge.read_bytes()).hexdigest(),
                'polars_threads': pl.thread_pool_size(),
            }
        else:
            start = time.perf_counter_ns()
            current = query.collect(engine=engine if command['route'] == 'ibex' else 'streaming')
            elapsed = (time.perf_counter_ns() - start) / 1e6
            assert_frame_equal(current, reference, check_row_order=True, rel_tol=1e-10, abs_tol=1e-8)
            previous = current  # destruction of the previous output is outside timing
            response = {'ms': elapsed, 'correctness': 'passed'}
        print(json.dumps(response), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pdsh-root', type=Path, required=True)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--before-dir', type=Path)
    parser.add_argument('--after-dir', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--queries', type=int, nargs='+', default=[1, 2, 3, 4, 5, 6, 12, 18])
    parser.add_argument('--iters', type=int, default=6)
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args)
        return
    if not all((args.before_dir, args.after_dir, args.out)):
        parser.error('--before-dir, --after-dir and --out are required')
    if args.iters < 6 or args.iters % 6:
        parser.error('--iters must be a positive multiple of six to balance route order')
    root = Path(__file__).resolve().parents[1]
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in (root / 'build-release/CMakeCache.txt').read_text():
        parser.error('build-release must be configured as Release')
    paths = sorted(args.data.resolve().glob('*.parquet'))
    if len(paths) != 8:
        parser.error('--data must contain the eight PDS-H Parquet tables')
    report = {
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'native_worktree_patch': subprocess.check_output(['git', 'diff', 'HEAD', '--', 'src', 'include'], cwd=root, text=True),
        'upstream_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.pdsh_root, text=True).strip(),
        'ibex_cores': os.environ.get('IBEX_CORES'), 'affinity': sorted(os.sched_getaffinity(0)),
        'ibex_stream_scan': os.environ.get('IBEX_STREAM_SCAN'),
        'ibex_decode_threads': os.environ.get('IBEX_DECODE_THREADS'),
        'ibex_join_probe': os.environ.get('IBEX_JOIN_PROBE'),
        'cpu_frequency_khz': {n: Path('/sys/devices/system/cpu/cpu0/cpufreq', n).read_text().strip()
                              for n in ('scaling_min_freq', 'scaling_max_freq')},
        'data': [{'path': str(p), 'bytes': p.stat().st_size, 'mtime_ns': p.stat().st_mtime_ns} for p in paths],
        'iterations': args.iters, 'queries': {}, 'driver': Path(__file__).read_text(),
        'method': 'Two persistent bridge workers with identical Python adapter; one in-memory Polars reference per worker/query; first collection and two warmups per route; all six route permutations; collection includes Parquet decode and public result conversion; preparation, correctness checks and prior-output destruction outside timing. First-per-query timings are not fresh-process measurements. Every output is compared in full, including order and dtypes, rel_tol=1e-10 abs_tol=1e-8.',
    }
    workers = {}
    def call(name, command):
        process = workers[name]
        process.stdin.write(json.dumps(command) + '\n')
        process.stdin.flush()
        line = process.stdout.readline()
        if not line:
            raise RuntimeError(f'{name} worker exited: {process.poll()}')
        return json.loads(line)
    def save():
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + '\n')
    try:
        for name, directory in (('before', args.before_dir), ('after', args.after_dir)):
            env = {**os.environ, 'PYTHONPATH': str(directory.resolve()) + os.pathsep + str(root / 'python')}
            workers[name] = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), '--worker', '--pdsh-root', str(args.pdsh_root), '--data', str(args.data)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        for number in args.queries:
            name = f'q{number:02}'
            print(name, 'preparing and checking complete results', flush=True)
            entry = report['queries'][name] = {'status': 'running'}
            metadata = {side: call(side, {'prepare': number}) for side in workers}
            for field in ('source', 'schema', 'output_shape', 'upstream_source_sha256', 'polars_threads'):
                assert metadata['before'][field] == metadata['after'][field], field
            assert metadata['before']['bridge_sha256'] != metadata['after']['bridge_sha256']
            entry['metadata'] = metadata
            routes = {'before': ('before', 'ibex'), 'after': ('after', 'ibex'),
                      'polars_streaming': ('after', 'polars')}
            def collect(route):
                side, engine = routes[route]
                return call(side, {'route': engine})['ms']
            entry['first_per_query_ms'] = {route: collect(route) for route in routes}
            for _ in range(2):
                for route in routes:
                    collect(route)
            samples = {route: [] for route in routes}
            orders = list(permutations(routes))
            for iteration in range(args.iters):
                for route in orders[iteration % 6]:
                    samples[route].append(collect(route))
                print(name, f'round {iteration + 1}/{args.iters}', flush=True)
            ratios = [a / b for a, b in zip(samples['after'], samples['before'])]
            entry.update(status='passed', samples_ms=samples,
                         median_ms={k: statistics.median(v) for k, v in samples.items()},
                         median_paired_change_pct=100 * (statistics.median(ratios) - 1),
                         after_faster_pairs=sum(a < b for a, b in zip(samples['after'], samples['before'])))
            print(name, json.dumps(entry['median_ms']), flush=True)
            save()
    finally:
        save()
        for process in workers.values():
            if process.poll() is None:
                process.stdin.close()
        for process in workers.values():
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == '__main__':
    main()
