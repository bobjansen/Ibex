# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Interleaved categorical export A/B using one Release binary and native plan.

Run with PYTHONPATH=build-release/python:python; preparation and assertions are
outside timing. --base selects the previous Python converter, not native code.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
import ibex_pyarrow
from ibex_polars import IbexEngine
from ibex_polars._engine import _to_polars


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='3ee85925')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    assert Path(ibex_pyarrow.__file__).resolve().is_relative_to(root / 'build-release/python')
    namespace = {'__name__': 'ibex_polars._baseline_engine', '__package__': 'ibex_polars'}
    exec(subprocess.check_output(['git', 'show', f'{args.base}:python/ibex_polars/_engine.py'],
                                 text=True), namespace)
    rng = np.random.default_rng(20261025)
    categories = pl.Series('k', [f'key_{i:05d}' for i in range(10000)], dtype=pl.Categorical)
    left = pl.DataFrame({'k': categories.gather(pl.Series(rng.integers(0, 10000, size=1000000))),
                         'x': rng.random(1000000)})
    right = pl.DataFrame({'k': categories, 'y': rng.random(10000)})
    query = left.lazy().join(right.lazy(), on='k')
    engine = IbexEngine()
    source, bindings, scalars, expected, output = engine._prepare(query, pl.QueryOptFlags())
    def execute():
        return ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
    raw = execute()
    reference = query.collect()
    converters = {'before': namespace['_to_polars'], 'after': _to_polars}
    for convert in converters.values():
        assert_frame_equal(convert(raw, expected, output), reference, check_row_order=False)
    report = {'base': args.base, 'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              'binary_sha256': hashlib.sha256(Path(ibex_pyarrow.__file__).read_bytes()).hexdigest(),
              'converter_sha256': hashlib.sha256((root / 'python/ibex_polars/_engine.py').read_bytes()).hexdigest(),
              'polars_version': pl.__version__, 'cpu_affinity': sorted(os.sched_getaffinity(0)), 'threads': {'ibex': os.getenv('IBEX_CORES'),
              'polars': pl.thread_pool_size()}, 'rows': raw.num_rows, 'samples_ms': {}}
    for stage in ('conversion', 'collect'):
        calls = {name: (lambda convert=convert: convert(raw if stage == 'conversion' else execute(), expected, output))
                 for name, convert in converters.items()}
        calls['polars'] = query.collect if stage == 'collect' else lambda: pl.from_arrow(raw)
        for call in calls.values():
            call(); call()
        samples = {name: [] for name in calls}
        # Keep the previous result alive until the clock stops, so its teardown
        # is not charged to the next route's allocation.
        previous = None
        for iteration in range(20):
            order = list(calls) if iteration % 2 == 0 else list(reversed(calls))
            for name in order:
                start = time.perf_counter_ns()
                current = calls[name]()
                samples[name].append((time.perf_counter_ns() - start) / 1e6)
                previous = current
        report['samples_ms'][stage] = samples
    report['median_ms'] = {stage: {name: statistics.median(values) for name, values in samples.items()}
                           for stage, samples in report['samples_ms'].items()}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report['median_ms'], indent=2))


if __name__ == '__main__':
    main()
