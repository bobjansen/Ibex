# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Isolate category ID remapping and validation; sweep cardinality and nulls.

Standalone stages reuse prepared inputs and are not additive. Named category
mappings isolate cardinality from other columns' category registrations.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import time

import numpy as np
import polars as pl
from polars.testing import assert_series_equal
import pyarrow as pa
from ibex_polars import _engine, _categories


def stages(codes, known, dtype, name):
    frame = codes.to_frame()
    expression = _engine._known_ids(name, known)
    ids = frame.select(expression.alias(name))
    calls = {'lookup': lambda: frame.select(expression.alias(name)),
             'validation': lambda: ids.select(pl.col(name).cat.to(dtype)),
             'fused': lambda: frame.select(expression.cat.to(dtype).alias(name))}
    assert_series_equal(calls['validation']()[name], calls['fused']()[name])
    return calls


def sample(calls, iterations):
    for call in calls.values():
        call(); call()
    samples = {k: [] for k in calls}
    previous = None
    for iteration in range(iterations):
        for key in (list(calls) if iteration % 2 == 0 else list(reversed(calls))):
            start = time.perf_counter_ns()
            current = calls[key]()
            elapsed = time.perf_counter_ns() - start
            samples[key].append(elapsed / 1e6)
            previous = current
    return {'samples_ms': samples, 'median_ms': {k: statistics.median(v) for k, v in samples.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--cardinalities', nargs='+', type=int, default=[10, 10000, 1000000])
    parser.add_argument('--null-fractions', nargs='+', type=float, default=[0, .1, .5])
    parser.add_argument('--iters', type=int, default=12)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1 or min(args.cardinalities) < 2 or args.iters < 6 or args.iters % 2:
        parser.error('positive rows, cardinalities >=2, even iterations >=6 required')
    if any(not 0 <= n <= 1 for n in args.null_fractions):
        parser.error('null fractions must be in [0,1]')
    report = {'rows': args.rows, 'seed': 20261108, 'iterations': args.iters,
              'polars_version': pl.__version__, 'threads': pl.thread_pool_size(),
              'affinity': sorted(os.sched_getaffinity(0)), 'notes': __doc__,
              'source_sha256': {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in (Path(__file__), Path(_engine.__file__))}, 'cases': []}
    for cardinality in args.cardinalities:
        dtype = pl.Categorical(pl.Categories(f'isolation_{cardinality}', namespace='ibex_bench'))
        labels = pl.int_range(cardinality, eager=True).cast(pl.String).cast(dtype)
        physical = labels.to_physical().to_numpy()
        # Reverse dictionary ordering forces the same lookup path as real joins.
        ids = physical[::-1].copy()
        dictionary = labels.reverse().cast(pl.String).to_arrow().cast(pa.string())
        known = _categories.Known(dictionary, ids, dtype)
        rng = np.random.default_rng(20261108)
        values = rng.integers(0, cardinality, args.rows, dtype=np.uint32)
        null_draw = rng.random(args.rows)
        for fraction in args.null_fractions:
            nulls = null_draw < fraction
            codes = pl.from_arrow(pa.array(values, mask=nulls if fraction else None)).rename('c')
            calls = stages(codes, known, dtype, 'c')
            expected = labels.reverse().gather(codes).rename('c')
            assert_series_equal(calls['fused']()['c'], expected)
            result = sample(calls, args.iters)
            case = {'cardinality': cardinality, 'null_fraction': fraction,
                    'nulls': codes.null_count(), 'mapping': 'lookup', **result}
            report['cases'].append(case)
            print(json.dumps({k: v for k, v in case.items() if k != 'samples_ms'}), flush=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
