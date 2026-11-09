# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired h2o join measurement. Run separately with thread budgets 1 and 8.

CSV loading and full-result correctness checks are outside timers. Preparation
uses a new engine per sample; cached collect uses the same engine/query. Split
execution includes Arrow export. Conversion reuses immutable native output and
is diagnostic, not an additive decomposition of cached collect.
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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'benchmarking'))
from ab_queries import wilcoxon_p
from h2o import run as h2o


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-output', action='store_true', help='Experimental C++ physical materializer')
    parser.add_argument('--right-rows', action='store_true', help='Benchmark prototype against current engine')
    parser.add_argument('--reconstruction', action='store_true', help='Measure reconstruction sub-stages')
    parser.add_argument('--baseline-engine', type=Path, help='Saved _engine.py for Python-only paired A/B')
    parser.add_argument('--data', default='J1_1e7_NA_0_0')
    parser.add_argument('--questions', nargs='+', default=['q1', 'q2', 'q3', 'q4', 'q5'])
    parser.add_argument('--iters', type=int, default=12)
    parser.add_argument('--limit', type=int, help='Smoke check only: truncate each input')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.native_output:
        args.right_rows = True
    if args.iters < 6 or args.iters % 2 or (args.limit is not None and args.limit < 1):
        parser.error('even --iters >= 6 and positive --limit required')
    if set(args.questions) - {f'q{i}' for i in range(1, 6)}:
        parser.error('unknown question')
    import polars as pl
    from polars.testing import assert_frame_equal
    import ibex_pyarrow
    from ibex_polars import IbexEngine, _engine
    engine_type, converter = IbexEngine, _engine._to_polars
    if args.right_rows:
        if args.baseline_engine or args.reconstruction:
            parser.error('--right-rows cannot combine with --baseline-engine or --reconstruction')
        from prototype_right_rows import RightRowsEngine, to_polars
        engine_type, converter = RightRowsEngine, to_polars
        if args.native_output:
            from prototype_native_output import NativeOutputEngine, to_polars
            engine_type, converter = NativeOutputEngine, to_polars
    binary = Path(ibex_pyarrow.__file__).resolve()
    if not binary.is_relative_to(ROOT / 'build-release/python'):
        parser.error('PYTHONPATH must select build-release/python')
    if 'CMAKE_BUILD_TYPE:STRING=Release' not in (ROOT / 'build-release/CMakeCache.txt').read_text():
        parser.error('Release build required')
    if os.getenv('IBEX_CORES') not in ('1', '8') or pl.thread_pool_size() != int(os.environ['IBEX_CORES']):
        parser.error('set IBEX_CORES and POLARS_MAX_THREADS to the same value (1 or 8)')
    baseline = None
    if args.baseline_engine:
        baseline = {'__name__': 'ibex_polars._baseline_engine', '__package__': 'ibex_polars'}
        exec(compile(args.baseline_engine.read_text(), str(args.baseline_engine), 'exec'), baseline)
    if args.right_rows:
        baseline = {'IbexEngine': IbexEngine}
    frames = []
    for label, path in h2o.join_tables(args.data).items():
        keys = {'x': 3, 'small': 1, 'medium': 2, 'big': 3}[label]
        overrides = {f'id{i}': pl.Int32 for i in range(1, keys + 1)}
        overrides['v1' if label == 'x' else 'v2'] = pl.Float32
        frames.append(pl.read_csv(path, schema_overrides=overrides, n_rows=args.limit).with_columns(
            pl.col([f'id{i}' for i in range(4, 4 + keys)]).cast(pl.Categorical)).lazy())
    report = {'data': args.data, 'limit': args.limit, 'smoke_only': args.limit is not None,
              'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'git_status': subprocess.check_output(['git', 'status', '--short'], cwd=ROOT, text=True),
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'source_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in [Path(__file__), *sorted((ROOT / 'python/ibex_polars').glob('*.py'))]},
              'polars_version': pl.__version__, 'threads': pl.thread_pool_size(),
              'affinity': sorted(os.sched_getaffinity(0)), 'iterations': args.iters,
              'correctness': 'full output values and dtypes; join order unspecified',
              'timing': __doc__, 'cases': []}
    if args.right_rows:
        report['prototype'] = 'right rows; unoptimized preparation, single-query strong cache'
        path = ROOT / 'python/prototype_right_rows.py'
        report['source_sha256'][str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if args.native_output:
        report['prototype'] = 'native physical output; supported cat.to attachment; unoptimized preparation'
        for path in (ROOT / 'python/prototype_native_output.py', ROOT / 'python/experiments/materialize.cpp',
                     Path(os.environ.get('IBEX_MATERIALIZER_LIB', str(Path(__file__).resolve().parents[1] / 'build-release/ibex-materialize.so')))):
            report['source_sha256'][str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    if args.baseline_engine:
        report['baseline_engine_sha256'] = hashlib.sha256(args.baseline_engine.read_bytes()).hexdigest()
    for name, description, _, _, make_query, _ in h2o.join_questions(False):
        if name not in args.questions:
            continue
        query = make_query(*frames)
        engine = engine_type()
        source, bindings, scalars, expected, output, categories, left_rows = engine._prepare(query, pl.QueryOptFlags())
        def execute():
            return ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        raw = execute()
        def convert():
            return converter(raw, expected, output, categories, left_rows)
        calls = {'ibex_cached': lambda: query.collect(engine=engine),
                 'polars_streaming': lambda: query.collect(engine='streaming'),
                 'prepare': lambda: engine_type(cache_size=0).prepare(query),
                 'native_export': execute, 'conversion': convert}
        if baseline is not None:
            baseline_engine = baseline['IbexEngine']().prepare(query)
            calls['ibex_baseline'] = lambda: query.collect(engine=baseline_engine)
        if args.native_output and left_rows is not None and len(left_rows) == 4:
            _, right_row, _, payload = left_rows
            physical = payload.physical(raw[right_row])
            calls['physical_materialize'] = lambda: payload.physical(raw[right_row])
            calls['categorical_attach'] = lambda: payload.attach(physical)
        reference = calls['polars_streaming']()
        for route in ('ibex_cached', 'conversion', *(['ibex_baseline'] if baseline is not None else [])):
            assert_frame_equal(calls[route](), reference, check_row_order=False)
        rows = reference.height
        del reference
        for call in calls.values():
            for _ in range(2):
                call()
        samples = {key: [] for key in calls}
        previous = None
        for iteration in range(args.iters):
            order = list(calls) if iteration % 2 == 0 else list(reversed(calls))
            for key in order:
                start = time.perf_counter_ns()
                current = calls[key]()
                elapsed = time.perf_counter_ns() - start
                samples[key].append(elapsed / 1e6)
                previous = current  # teardown outside clock
        ratios = [a / b for a, b in zip(samples['ibex_cached'], samples['polars_streaming'])]
        case = {'question': name, 'description': description, 'rows': rows,
                'left_reconstruction': left_rows is not None, 'samples_ms': samples,
                'median_ms': {k: statistics.median(v) for k, v in samples.items()},
                'median_paired_ibex_over_polars': statistics.median(ratios),
                'wilcoxon_p': wilcoxon_p([a-b for a, b in zip(samples['ibex_cached'], samples['polars_streaming'])])}
        if baseline is not None:
            before, after = samples['ibex_baseline'], samples['ibex_cached']
            case['candidate_over_baseline'] = statistics.median([a / b for a, b in zip(after, before)])
            case['candidate_baseline_p'] = wilcoxon_p([a - b for a, b in zip(after, before)])
        if args.reconstruction:
            from bench_join_reconstruction import measure
            case['reconstruction'] = measure(raw, expected, output, categories, left_rows, args.iters)
            for filename in ('bench_join_reconstruction.py', 'bench_category_isolation.py'):
                path = ROOT / 'python' / filename
                report['source_sha256'][str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
            print(json.dumps({'question': name, 'reconstruction_ms': case['reconstruction'].get('median_ms')}), flush=True)
        report['cases'].append(case)
        print(json.dumps({k: v for k, v in case.items() if k not in ('samples_ms', 'reconstruction')}), flush=True)
        del previous, current, raw, calls, engine
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
