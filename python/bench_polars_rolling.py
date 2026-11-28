# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare warmed rolling queries on the same resident frame, including result conversion."""

import argparse
import statistics
import time

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal

from ibex_polars import IbexEngine


def measure(query, ibex, repeats):
    assert_frame_equal(query.collect(engine=ibex), query.collect(engine="streaming"),
                       rel_tol=1e-12, abs_tol=1e-9)
    timings = [[], []]
    engines = (ibex, "streaming")
    for iteration in range(repeats):
        for side in ((1, 0) if iteration % 2 else (0, 1)):
            start = time.perf_counter_ns()
            query.collect(engine=engines[side])
            timings[side].append((time.perf_counter_ns() - start) / 1e6)
    return [statistics.median(values) for values in timings]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--groups", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=10)
    args = parser.parse_args()
    if min(args.rows, args.groups, args.repeats) < 1:
        parser.error("rows, groups and repeats must be positive")
    rng = np.random.default_rng(875)
    frame = pl.LazyFrame({
        "ts": pl.Series(np.arange(args.rows) * 1000, dtype=pl.Datetime("ms")),
        "int64": rng.integers(-10000, 10001, args.rows),
        "group": rng.integers(0, args.groups, args.rows),
        "float64": rng.normal(size=args.rows),
    })
    engine = IbexEngine()
    print("dtype,partition,operation,ibex_ms,polars_streaming_ms", flush=True)
    for dtype in ("int64", "float64"):
        value = pl.col(dtype)
        expressions = {
            "sum_128": value.rolling_sum(128),
            "mean_128": value.rolling_mean(128),
            "sum_60s": value.rolling_sum_by("ts", "60s"),
            "mean_5m": value.rolling_mean_by("ts", "5m"),
        }
        for name, expr in expressions.items():
            for partition, window in (("global", expr), ("group", expr.over("group"))):
                ibex_ms, polars_ms = measure(frame.select(window), engine, args.repeats)
                print(f"{dtype},{partition},{name},{ibex_ms:.2f},{polars_ms:.2f}", flush=True)


if __name__ == "__main__":
    main()
