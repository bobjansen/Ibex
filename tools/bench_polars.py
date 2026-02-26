#!/usr/bin/env python3
"""Polars TimeFrame benchmark — equivalent to ibex_bench --timeframe-rows N.

Requires: pip install polars

Usage:
    python3 tools/bench_polars.py --rows 1000000 --warmup 1 --iters 3
"""

import argparse
import time

import polars as pl


def generate_data(n: int) -> pl.DataFrame:
    # Timestamps: 0 ns, 1_000_000_000 ns, 2_000_000_000 ns ... (1-second spacing)
    # Price: sawtooth 100.0 .. 199.0 repeating — matches ibex synthetic data.
    return pl.DataFrame(
        {
            "ts": (pl.Series(range(n), dtype=pl.Int64) * 1_000_000_000).cast(
                pl.Datetime("ns")
            ),
            "price": pl.Series(
                [100.0 + float(i % 100) for i in range(n)], dtype=pl.Float64
            ),
        }
    )


def bench(name: str, fn, warmup: int, iters: int) -> None:
    for _ in range(warmup):
        fn()
    start = time.perf_counter()
    rows = 0
    for _ in range(iters):
        result = fn()
        rows = len(result)
    elapsed_ms = (time.perf_counter() - start) * 1000
    avg_ms = elapsed_ms / iters
    print(
        f"bench {name}: iters={iters}, total_ms={elapsed_ms:.3f}, "
        f"avg_ms={avg_ms:.3f}, rows={rows}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Polars TimeFrame benchmark")
    ap.add_argument("--rows", type=int, default=100_000, help="Row count")
    ap.add_argument("--warmup", type=int, default=1, help="Warmup iterations")
    ap.add_argument("--iters", type=int, default=5, help="Measured iterations")
    args = ap.parse_args()

    n = args.rows
    print(f"\n-- Polars {pl.__version__} TimeFrame benchmarks ({n} rows, 1s spacing) --")
    df = generate_data(n)

    # as_timeframe: sort ascending by timestamp
    bench("as_timeframe", lambda: df.sort("ts"), args.warmup, args.iters)

    # tf_lag1: vectorized shift — O(n)
    bench(
        "tf_lag1",
        lambda: df.sort("ts").with_columns(pl.col("price").shift(1).alias("prev")),
        args.warmup,
        args.iters,
    )

    # tf_rolling_count_1m: time-based 1-minute rolling count (no rolling_count_by in polars,
    # use rolling_sum_by on a constant-1 column as the canonical workaround)
    bench(
        "tf_rolling_count_1m",
        lambda: df.sort("ts").with_columns(
            pl.col("price")
            .is_not_null()
            .cast(pl.Int64)
            .rolling_sum_by("ts", window_size="1m")
            .alias("c")
        ),
        args.warmup,
        args.iters,
    )

    # tf_rolling_sum_1m: time-based 1-minute rolling sum
    bench(
        "tf_rolling_sum_1m",
        lambda: df.sort("ts").with_columns(
            pl.col("price").rolling_sum_by("ts", window_size="1m").alias("s")
        ),
        args.warmup,
        args.iters,
    )

    # tf_rolling_mean_5m: time-based 5-minute rolling mean
    bench(
        "tf_rolling_mean_5m",
        lambda: df.sort("ts").with_columns(
            pl.col("price").rolling_mean_by("ts", window_size="5m").alias("m")
        ),
        args.warmup,
        args.iters,
    )

    # tf_resample_1m_ohlc: bucket ticks into 1-minute OHLC bars
    bench(
        "tf_resample_1m_ohlc",
        lambda: df.sort("ts")
        .group_by_dynamic("ts", every="1m")
        .agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
        ),
        args.warmup,
        args.iters,
    )


if __name__ == "__main__":
    main()
