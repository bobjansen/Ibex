# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired original/all-Int32/all-Int64 q5 storage experiment.

Categoricals become shared physical IDs; Float32 payloads become their exact
Int32 bit patterns. Payload arithmetic is absent from these joins. Encoding and
correctness checks are outside timers. Ibex's native storage remains Int64 in
both integer modes: this does not benchmark a native Int32 build.
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


def numeric(frame):
    import polars as pl
    import pyarrow as pa
    columns = []
    for s in frame:
        if s.dtype.base_type() == pl.Categorical:
            s = s.to_physical().cast(pl.Int32, strict=True)
        elif s.dtype == pl.Float32:
            # Arrow view keeps validity and exact IEEE bits, including signed zero.
            s = pl.from_arrow(s.to_arrow().view(pa.int32())).alias(s.name)
        else:
            s = s.cast(pl.Int32, strict=True)
        columns.append(s)
    return pl.DataFrame(columns)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data', default='J1_1e7_NA_0_0')
    p.add_argument('--iters', type=int, default=12)
    p.add_argument('--limit', type=int)
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    if args.iters < 6 or args.iters % 2 or (args.limit is not None and args.limit < 1):
        p.error('even iterations >=6 and positive limit required')
    import polars as pl
    from polars.testing import assert_frame_equal
    import ibex_pyarrow
    from ibex_polars import IbexEngine, _engine
    if os.getenv('IBEX_CORES') not in ('1', '8') or pl.thread_pool_size() != int(os.environ['IBEX_CORES']):
        p.error('set both thread budgets to 1 or 8')
    binary = Path(ibex_pyarrow.__file__).resolve()
    if not binary.is_relative_to(ROOT / 'build-release/python') or 'CMAKE_BUILD_TYPE:STRING=Release' not in (ROOT / 'build-release/CMakeCache.txt').read_text():
        p.error('Release bridge required')
    original = []
    paths = h2o.join_tables(args.data)
    for label in ('x', 'big'):
        original.append(pl.read_csv(paths[label], n_rows=args.limit,
            schema_overrides={**{f'id{i}': pl.Int32 for i in (1, 2, 3)},
                              'v1' if label == 'x' else 'v2': pl.Float32}).with_columns(
                                  pl.col('id4', 'id5', 'id6').cast(pl.Categorical)))
    i32 = [numeric(f) for f in original]
    frames = {'original': original, 'int32': i32, 'int64': [f.cast(pl.Int64) for f in i32]}
    calls, owners, details = {}, [], {}
    for mode, (left, right) in frames.items():
        query = left.lazy().join(right.lazy(), on='id3')
        engine = IbexEngine()
        start = time.perf_counter_ns()
        plan = engine._prepare(query, pl.QueryOptFlags())
        preparation_ms = (time.perf_counter_ns() - start) / 1e6
        source, bindings, scalars, expected, output, categories, rows = plan
        def execute(source=source, bindings=bindings, scalars=scalars):
            return ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        raw = execute()
        def convert(raw=raw, expected=expected, output=output, categories=categories, rows=rows):
            return _engine._to_polars(raw, expected, output, categories, rows)
        calls[mode + ':ibex'] = lambda query=query, engine=engine: query.collect(engine=engine)
        calls[mode + ':polars'] = lambda query=query: query.collect(engine='streaming')
        calls[mode + ':native_export'] = execute
        calls[mode + ':conversion'] = convert
        reference = calls[mode + ':polars']()
        assert_frame_equal(calls[mode + ':ibex'](), reference, check_row_order=False)
        assert_frame_equal(convert(), reference, check_row_order=False)
        details[mode] = {'input_schema': str(left.schema), 'output_rows': reference.height,
                         'input_bytes': left.estimated_size() + right.estimated_size(),
                         'output_bytes': reference.estimated_size(), 'left_reconstruction': rows is not None,
                         'prepare_ms_single_sample': preparation_ms, 'native_schema': str(raw.schema)}
        del reference
        owners.append((query, engine, raw))
        print(json.dumps({'validated': mode, **details[mode]}), flush=True)
    for call in calls.values():
        call(); call()
    samples = {k: [] for k in calls}
    previous = None
    for i in range(args.iters):
        for name in (list(calls) if i % 2 == 0 else list(reversed(calls))):
            start = time.perf_counter_ns()
            current = calls[name]()
            elapsed = time.perf_counter_ns() - start
            samples[name].append(elapsed / 1e6)
            previous = current
    def compare(after, before):
        a, b = samples[after], samples[before]
        return {'median_paired_ratio': statistics.median([x/y for x, y in zip(a, b)]),
                'wilcoxon_p': wilcoxon_p([x-y for x, y in zip(a, b)])}
    report = {'notes': __doc__, 'data': args.data, 'limit': args.limit, 'smoke_only': args.limit is not None,
              'iterations': args.iters, 'polars_version': pl.__version__, 'threads': pl.thread_pool_size(),
              'affinity': sorted(os.sched_getaffinity(0)), 'details': details, 'samples_ms': samples,
              'median_ms': {k: statistics.median(v) for k, v in samples.items()},
              'ibex_over_polars': {m: compare(m+':ibex', m+':polars') for m in frames},
              'int64_over_int32': {e: compare('int64:'+e, 'int32:'+e) for e in ('ibex','polars','native_export','conversion')},
              'git_head': subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
              'git_status': subprocess.check_output(['git','status','--short'],cwd=ROOT,text=True),
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'source_sha256': {str(f.relative_to(ROOT)): hashlib.sha256(f.read_bytes()).hexdigest()
                                for f in [Path(__file__), *sorted((ROOT/'python/ibex_polars').glob('*.py'))]}}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k: report[k] for k in ('median_ms','ibex_over_polars','int64_over_int32')},indent=2))


if __name__ == '__main__':
    main()
