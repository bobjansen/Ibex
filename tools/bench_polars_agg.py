#!/usr/bin/env python3
"""Polars grouped central-moment aggregate benchmark.

Companion to the `agg_moments_*` cases in `tools/ibex_fusion_bench`: same data
shape (N rows, ~1000 contiguous groups, integer values) and the same
std/skew/kurtosis grouped aggregation, so the timings are directly comparable.

Semantics are matched to Ibex (and pandas/scipy defaults): sample std (ddof=1),
unbiased Fisher–Pearson skewness, and unbiased Fisher excess kurtosis. Polars
defaults to the *biased* estimators, so `bias=False` is passed explicitly — it
does not change the timing, only what is computed.

Run (no install needed if you have uv):
    uv run --with polars --with numpy python tools/bench_polars_agg.py --rows 2000000

Output format mirrors the C++ bench:
    bench <name>: iters=I, avg_ms=A, min_ms=M, rows=R
"""

import argparse
import time

import numpy as np
import polars as pl


def generate_data(n: int, n_groups: int) -> pl.DataFrame:
    # `g`: ascending, contiguous runs of equal keys (matches ibex grouped_sorted:
    # g = row // per_group, clamped to the last group). `v`: random ints [0,1000).
    per_group = max(n // n_groups, 1)
    g = np.minimum(np.arange(n, dtype=np.int64) // per_group, n_groups - 1)
    rng = np.random.default_rng(45)
    v = rng.integers(0, 1000, size=n, dtype=np.int64)
    return pl.DataFrame({"g": g, "v": v})


def bench(name: str, fn, warmup: int, iters: int) -> None:
    for _ in range(warmup):
        fn()
    times_ms = []
    rows = 0
    for _ in range(iters):
        start = time.perf_counter()
        result = fn()
        times_ms.append((time.perf_counter() - start) * 1000.0)
        rows = result.height
    avg_ms = sum(times_ms) / len(times_ms)
    print(
        f"bench {name}: iters={iters}, avg_ms={avg_ms:.3f}, "
        f"min_ms={min(times_ms):.3f}, rows={rows}"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Polars grouped moment-aggregate benchmark")
    ap.add_argument("--rows", type=int, default=2_000_000, help="Row count")
    ap.add_argument("--groups", type=int, default=1000, help="Number of groups")
    ap.add_argument("--warmup", type=int, default=2, help="Warmup iterations")
    ap.add_argument("--iters", type=int, default=5, help="Measured iterations")
    args = ap.parse_args()

    df = generate_data(args.rows, args.groups)
    print(
        f"\n-- Polars {pl.__version__} grouped moments "
        f"({args.rows} rows, {args.groups} groups) --"
    )

    moments = [
        pl.col("v").std().alias("sd"),
        pl.col("v").skew(bias=False).alias("sk"),
        pl.col("v").kurtosis(fisher=True, bias=False).alias("ku"),
    ]

    # Polars group_by is hash-based and does not require sorted input; this is
    # the apples-to-apples comparison against ibex's hash path, and the baseline
    # ibex's streaming (sorted) path aims to beat.
    bench(
        "agg_moments",
        lambda: df.group_by("g").agg(moments),
        args.warmup,
        args.iters,
    )
    # Pre-sorted input, in case polars exploits it (it generally does not).
    df_sorted = df.sort("g")
    bench(
        "agg_moments_presorted",
        lambda: df_sorted.group_by("g").agg(moments),
        args.warmup,
        args.iters,
    )


if __name__ == "__main__":
    main()
