#!/usr/bin/env python3
"""Benchmark pandas and polars on the same aggregation queries as ibex_bench.

Two polars modes are available so the comparison covers both shapes a real
caller hits:
  • eager (always on): `pl.read_csv` once, then `df.<op>` per iteration. This
    is the "data is already in memory" path and intentionally bypasses the
    polars query optimizer.
  • lazy (`--polars-lazy`): `pl.scan_csv(path).<chain>.collect()` per iteration.
    This is the "data still needs to be read" path; the optimizer fires
    (predicate pushdown into the CSV reader, projection pruning, slice/topk
    fusion) and CSV I/O is part of the timed work.

Writes tab-separated results to --out (default: results/python.tsv).
Progress goes to stderr; TSV rows go to the output file.

Usage:
  uv run bench_python.py --csv data/prices.csv --csv-multi data/prices_multi.csv
  uv run bench_python.py --csv data/prices.csv --skip-pandas
  uv run bench_python.py --csv data/prices.csv --polars-lazy --skip-pandas
"""
import argparse, csv, pathlib, sys, time
import numpy as np
import pandas as pd
import polars as pl

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from bench_mem import reset_peak_rss, peak_rss_mb, CELL_CUTOFF_MS, should_skip, cut_row


# ── Timing helper ─────────────────────────────────────────────────────────────

# Absolute peak RSS (MiB) measured during the most recent timer() call. timer()
# resets the kernel peak counter after warmup and reads it after the measured
# iterations; run() helpers read this when appending the peak_rss_mb column.
LAST_PEAK_RSS_MB = 0.0


def timer(fn, warmup: int, iters: int):
    """Warmup then time fn(). Returns (avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, last_result).
    Side effect: sets module global LAST_PEAK_RSS_MB to the peak RSS during the measured iterations."""
    global LAST_PEAK_RSS_MB
    for _ in range(warmup):
        _t0 = time.perf_counter()
        result = fn()
        if (time.perf_counter() - _t0) * 1000 > CELL_CUTOFF_MS:
            # Over the per-iteration budget: cut this cell. The sentinel
            # avg_ms < 0 is dropped by the writer (blank on the page).
            LAST_PEAK_RSS_MB = 0.0
            return (-1.0, -1.0, -1.0, 0.0, -1.0, -1.0, result)
    reset_peak_rss()
    times = []
    for _ in range(iters):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    LAST_PEAK_RSS_MB = peak_rss_mb()
    t = np.array(times)
    return (
        float(np.mean(t)),
        float(np.min(t)),
        float(np.max(t)),
        float(np.std(t, ddof=0)),
        float(np.percentile(t, 95)),
        float(np.percentile(t, 99)),
        result,
    )


# ── Benchmark suites ──────────────────────────────────────────────────────────


def bench_pandas(csv_path, csv_multi_path, csv_trades_path, warmup, iters):
    _fw = "pandas"
    print("pandas: loading...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_path)
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "pandas",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run(
        "mean_by_symbol",
        lambda: df.groupby("symbol", sort=False)
        .agg(avg_price=("price", "mean"))
        .reset_index(),
    )

    run(
        "ohlc_by_symbol",
        lambda: df.groupby("symbol", sort=False)
        .agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            last=("price", "last"),
        )
        .reset_index(),
    )

    run("update_price_x2", lambda: df.assign(price_x2=df["price"] * 2))

    run("distinct_symbol", lambda: df[["symbol"]].drop_duplicates(ignore_index=True))

    run(
        "order_head_topk",
        lambda: df.sort_values("price", ascending=False, kind="mergesort").head(100),
    )

    run(
        "order_head_topk_by_symbol",
        lambda: df.sort_values("price", ascending=False, kind="mergesort")
        .groupby("symbol", sort=False)
        .head(3),
    )

    run(
        "order_tail_topk",
        lambda: df.sort_values("price", ascending=False, kind="mergesort").tail(100),
    )

    run(
        "order_tail_topk_by_symbol",
        lambda: df.sort_values("price", ascending=False, kind="mergesort")
        .groupby("symbol", sort=False)
        .tail(3),
    )

    # Full-table sorts (every row reordered), in contrast to the top-k queries
    # above which only keep N rows.
    run("sort_price", lambda: df.sort_values("price", kind="mergesort"))

    run(
        "sort_price_desc",
        lambda: df.sort_values("price", ascending=False, kind="mergesort"),
    )

    run("sort_symbol", lambda: df.sort_values("symbol", kind="mergesort"))

    run(
        "sort_symbol_price",
        lambda: df.sort_values(["symbol", "price"], kind="mergesort"),
    )

    run(
        "sort_symbol_price_desc",
        lambda: df.sort_values(
            ["symbol", "price"], ascending=[True, False], kind="mergesort"
        ),
    )

    # Grouped window functions (per symbol).
    run(
        "rank_by_symbol",
        lambda: df.assign(
            rk=df.groupby("symbol")["price"].rank(method="dense", ascending=False)
        ),
    )

    run(
        "lag_by_symbol",
        lambda: df.assign(prev=df.groupby("symbol")["price"].shift(1)),
    )

    run(
        "cumsum_by_symbol",
        lambda: df.assign(cs=df.groupby("symbol")["price"].cumsum()),
    )

    # Expensive group aggregates (per symbol): median, p90, sample stddev.
    run(
        "median_by_symbol",
        lambda: df.groupby("symbol", sort=False)
        .agg(med=("price", "median"))
        .reset_index(),
    )

    run(
        "quantile_by_symbol",
        lambda: df.groupby("symbol", sort=False)["price"]
        .quantile(0.9)
        .reset_index(name="p90"),
    )

    run(
        "std_by_symbol",
        lambda: df.groupby("symbol", sort=False).agg(sd=("price", "std")).reset_index(),
    )

    # Multi-stage pipeline: filter → group-by → order → head.
    run(
        "filter_group_sort",
        lambda: df[df["price"] > 500.0]
        .groupby("symbol", sort=False)
        .agg(avg=("price", "mean"))
        .reset_index()
        .sort_values("avg", ascending=False)
        .head(10),
    )

    # update by → filter on derived column → re-aggregate.
    def _update_group_filter():
        g = df.assign(lr=np.log(df["price"] / df.groupby("symbol")["price"].shift(1)))
        g = g[g["lr"] > 0.0]
        return (
            g.groupby("symbol", sort=False).agg(pos_days=("lr", "count")).reset_index()
        )

    run("update_group_filter", _update_group_filter)

    # rank within group → top-N per group → aggregate survivors.
    def _group_rank_filter():
        g = df.assign(
            rk=df.groupby("symbol")["price"].rank(method="dense", ascending=False)
        )
        g = g[g["rk"] <= 10]
        return (
            g.groupby("symbol", sort=False)
            .agg(avg_top10=("price", "mean"))
            .reset_index()
        )

    run("group_rank_filter", _group_rank_filter)

    # grouped z-score → clip to ±3 → re-aggregate.
    def _normalize_by_group():
        grp = df.groupby("symbol")["price"]
        z = (df["price"] - grp.transform("mean")) / grp.transform("std")
        g = df.assign(clipped=z.clip(-3.0, 3.0))
        return (
            g.groupby("symbol", sort=False)
            .agg(mean_z=("clipped", "mean"), sd_z=("clipped", "std"))
            .reset_index()
        )

    run("normalize_by_group", _normalize_by_group)

    # Transforms / single-pass language features.
    run("pmin_clip", lambda: df.assign(clipped=df["price"].clip(upper=500.0)))
    run(
        "where_update_clip",
        lambda: df.assign(price=df["price"].where(df["price"] <= 900.0, 900.0)),
    )
    run("rbind_two", lambda: pd.concat([df, df], ignore_index=True))

    run("cumsum_price", lambda: df.assign(cs=df["price"].cumsum()))

    run("cumprod_price", lambda: df.assign(cp=df["price"].cumprod()))

    rng_gen = np.random.default_rng(42)
    run("rand_uniform", lambda: df.assign(r=rng_gen.uniform(0.0, 1.0, len(df))))
    run("rand_normal", lambda: df.assign(n=rng_gen.standard_normal(len(df))))
    run("rand_int", lambda: df.assign(r=rng_gen.integers(1, 101, len(df))))
    run("rand_bernoulli", lambda: df.assign(r=rng_gen.binomial(1, 0.3, len(df))))

    # Scalar row-wise math builtins.
    run("abs_price", lambda: df.assign(v=df["price"].abs()))
    run("sqrt_price", lambda: df.assign(v=np.sqrt(df["price"])))
    run("log_price", lambda: df.assign(v=np.log(df["price"])))
    run("exp_price", lambda: df.assign(v=np.exp(df["price"] / 1000.0)))
    run("round_price", lambda: df.assign(v=np.rint(df["price"])))
    run("floor_price", lambda: df.assign(v=np.floor(df["price"])))
    run("ceil_price", lambda: df.assign(v=np.ceil(df["price"])))
    run("sin_price", lambda: df.assign(v=np.sin(df["price"])))
    run("cos_price", lambda: df.assign(v=np.cos(df["price"])))
    run("tanh_price", lambda: df.assign(v=np.tanh(df["price"] / 1000.0)))

    if csv_multi_path:
        print("pandas: loading multi...", file=sys.stderr, flush=True)
        dfm = pd.read_csv(csv_multi_path)

        run(
            "count_by_symbol_day",
            lambda: dfm.groupby(["symbol", "day"], sort=False)
            .size()
            .reset_index(name="n"),
        )

        run(
            "mean_by_symbol_day",
            lambda: dfm.groupby(["symbol", "day"], sort=False)
            .agg(avg_price=("price", "mean"))
            .reset_index(),
        )

        run(
            "ohlc_by_symbol_day",
            lambda: dfm.groupby(["symbol", "day"], sort=False)
            .agg(
                open=("price", "first"),
                high=("price", "max"),
                low=("price", "min"),
                last=("price", "last"),
            )
            .reset_index(),
        )

        # Two-level rollup: by {symbol, day} then re-aggregate by symbol.
        def _symbol_day_to_symbol():
            daily = (
                dfm.groupby(["symbol", "day"], sort=False)
                .agg(daily_mean=("price", "mean"), daily_vol=("price", "std"))
                .reset_index()
            )
            return (
                daily.groupby("symbol", sort=False)
                .agg(mean_of_means=("daily_mean", "mean"), mean_vol=("daily_vol", "mean"))
                .reset_index()
            )

        run("symbol_day_to_symbol", _symbol_day_to_symbol)

    if csv_trades_path:
        print("pandas: loading trades...", file=sys.stderr, flush=True)
        dft = pd.read_csv(csv_trades_path)

        run("filter_simple", lambda: dft[dft["price"] > 500.0])

        run("filter_and", lambda: dft[(dft["price"] > 500.0) & (dft["qty"] < 100)])

        run("filter_arith", lambda: dft[dft["price"] * dft["qty"] > 50000.0])

        run("filter_or", lambda: dft[(dft["price"] > 900.0) | (dft["qty"] < 10)])

        # Correlation matrix over the numeric columns (price, qty).
        run("corr_price_vol", lambda: dft[["price", "qty"]].corr())

    return rows


def bench_polars(csv_path, csv_multi_path, csv_trades_path, warmup, iters):
    _fw = "polars"
    print("polars: loading...", file=sys.stderr, flush=True)
    df = pl.read_csv(csv_path)
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "polars",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run(
        "mean_by_symbol",
        lambda: df.group_by("symbol").agg(pl.col("price").mean().alias("avg_price")),
    )

    run(
        "ohlc_by_symbol",
        lambda: df.group_by("symbol").agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("last"),
        ),
    )

    run(
        "update_price_x2",
        lambda: df.with_columns((pl.col("price") * 2).alias("price_x2")),
    )

    run("distinct_symbol", lambda: df.select("symbol").unique(maintain_order=True))

    run("order_head_topk", lambda: df.sort("price", descending=True).head(100))

    run(
        "order_head_topk_by_symbol",
        lambda: df.sort("price", descending=True)
        .group_by("symbol", maintain_order=True)
        .head(3),
    )

    run("order_tail_topk", lambda: df.sort("price", descending=True).tail(100))

    run(
        "order_tail_topk_by_symbol",
        lambda: df.sort("price", descending=True)
        .group_by("symbol", maintain_order=True)
        .tail(3),
    )

    # Full-table sorts (every row reordered).
    run("sort_price", lambda: df.sort("price"))

    run("sort_price_desc", lambda: df.sort("price", descending=True))

    run("sort_symbol", lambda: df.sort("symbol"))

    run("sort_symbol_price", lambda: df.sort(["symbol", "price"]))

    run(
        "sort_symbol_price_desc",
        lambda: df.sort(["symbol", "price"], descending=[False, True]),
    )

    # Grouped window functions (per symbol).
    run(
        "rank_by_symbol",
        lambda: df.with_columns(
            pl.col("price").rank(method="dense", descending=True).over("symbol").alias("rk")
        ),
    )

    run(
        "lag_by_symbol",
        lambda: df.with_columns(pl.col("price").shift(1).over("symbol").alias("prev")),
    )

    run(
        "cumsum_by_symbol",
        lambda: df.with_columns(pl.col("price").cum_sum().over("symbol").alias("cs")),
    )

    # Expensive group aggregates (per symbol): median, p90, sample stddev.
    run(
        "median_by_symbol",
        lambda: df.group_by("symbol").agg(pl.col("price").median().alias("med")),
    )

    run(
        "quantile_by_symbol",
        lambda: df.group_by("symbol").agg(
            pl.col("price").quantile(0.9, interpolation="linear").alias("p90")
        ),
    )

    run(
        "std_by_symbol",
        lambda: df.group_by("symbol").agg(pl.col("price").std().alias("sd")),
    )

    # Multi-stage pipeline: filter → group-by → order → head.
    run(
        "filter_group_sort",
        lambda: df.filter(pl.col("price") > 500.0)
        .group_by("symbol")
        .agg(pl.col("price").mean().alias("avg"))
        .sort("avg", descending=True)
        .head(10),
    )

    # update by → filter on derived column → re-aggregate.
    run(
        "update_group_filter",
        lambda: df.with_columns(
            (pl.col("price") / pl.col("price").shift(1).over("symbol"))
            .log()
            .alias("lr")
        )
        .filter(pl.col("lr") > 0.0)
        .group_by("symbol")
        .agg(pl.col("lr").count().alias("pos_days")),
    )

    # rank within group → top-N per group → aggregate survivors.
    run(
        "group_rank_filter",
        lambda: df.with_columns(
            pl.col("price").rank(method="dense", descending=True).over("symbol").alias("rk")
        )
        .filter(pl.col("rk") <= 10)
        .group_by("symbol")
        .agg(pl.col("price").mean().alias("avg_top10")),
    )

    # grouped z-score → clip to ±3 → re-aggregate.
    run(
        "normalize_by_group",
        lambda: df.with_columns(
            (
                (pl.col("price") - pl.col("price").mean().over("symbol"))
                / pl.col("price").std().over("symbol")
            ).alias("z")
        )
        .with_columns(pl.col("z").clip(-3.0, 3.0).alias("clipped"))
        .group_by("symbol")
        .agg(
            pl.col("clipped").mean().alias("mean_z"),
            pl.col("clipped").std().alias("sd_z"),
        ),
    )

    # Transforms / single-pass language features.
    run(
        "pmin_clip",
        lambda: df.with_columns(pl.col("price").clip(upper_bound=500.0).alias("clipped")),
    )
    run(
        "where_update_clip",
        lambda: df.with_columns(
            pl.when(pl.col("price") > 900.0)
            .then(900.0)
            .otherwise(pl.col("price"))
            .alias("price")
        ),
    )
    run("rbind_two", lambda: pl.concat([df, df]))

    run("cumsum_price", lambda: df.with_columns(pl.col("price").cum_sum().alias("cs")))

    run(
        "cumprod_price", lambda: df.with_columns(pl.col("price").cum_prod().alias("cp"))
    )

    rng_gen = np.random.default_rng(42)
    run(
        "rand_uniform",
        lambda: df.with_columns(pl.Series("r", rng_gen.uniform(0.0, 1.0, len(df)))),
    )
    run(
        "rand_normal",
        lambda: df.with_columns(pl.Series("n", rng_gen.standard_normal(len(df)))),
    )
    run(
        "rand_int",
        lambda: df.with_columns(pl.Series("r", rng_gen.integers(1, 101, len(df)))),
    )
    run(
        "rand_bernoulli",
        lambda: df.with_columns(pl.Series("r", rng_gen.binomial(1, 0.3, len(df)))),
    )

    # Scalar row-wise math builtins.
    run("abs_price", lambda: df.with_columns(pl.col("price").abs().alias("v")))
    run("sqrt_price", lambda: df.with_columns(pl.col("price").sqrt().alias("v")))
    run("log_price", lambda: df.with_columns(pl.col("price").log().alias("v")))
    run("exp_price", lambda: df.with_columns((pl.col("price") / 1000.0).exp().alias("v")))
    run("round_price", lambda: df.with_columns(pl.col("price").round(0).alias("v")))
    run("floor_price", lambda: df.with_columns(pl.col("price").floor().alias("v")))
    run("ceil_price", lambda: df.with_columns(pl.col("price").ceil().alias("v")))
    run("sin_price", lambda: df.with_columns(pl.col("price").sin().alias("v")))
    run("cos_price", lambda: df.with_columns(pl.col("price").cos().alias("v")))
    run("tanh_price", lambda: df.with_columns((pl.col("price") / 1000.0).tanh().alias("v")))

    if csv_multi_path:
        print("polars: loading multi...", file=sys.stderr, flush=True)
        dfm = pl.read_csv(csv_multi_path)

        run(
            "count_by_symbol_day",
            lambda: dfm.group_by(["symbol", "day"]).agg(pl.len().alias("n")),
        )

        run(
            "mean_by_symbol_day",
            lambda: dfm.group_by(["symbol", "day"]).agg(
                pl.col("price").mean().alias("avg_price")
            ),
        )

        run(
            "ohlc_by_symbol_day",
            lambda: dfm.group_by(["symbol", "day"]).agg(
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("last"),
            ),
        )

        # Two-level rollup: by {symbol, day} then re-aggregate by symbol.
        run(
            "symbol_day_to_symbol",
            lambda: dfm.group_by(["symbol", "day"])
            .agg(
                pl.col("price").mean().alias("daily_mean"),
                pl.col("price").std().alias("daily_vol"),
            )
            .group_by("symbol")
            .agg(
                pl.col("daily_mean").mean().alias("mean_of_means"),
                pl.col("daily_vol").mean().alias("mean_vol"),
            ),
        )

    if csv_trades_path:
        print("polars: loading trades...", file=sys.stderr, flush=True)
        dft = pl.read_csv(csv_trades_path)

        run("filter_simple", lambda: dft.filter(pl.col("price") > 500.0))

        run(
            "filter_and",
            lambda: dft.filter((pl.col("price") > 500.0) & (pl.col("qty") < 100)),
        )

        run(
            "filter_arith",
            lambda: dft.filter(pl.col("price") * pl.col("qty") > 50000.0),
        )

        run(
            "filter_or",
            lambda: dft.filter((pl.col("price") > 900.0) | (pl.col("qty") < 10)),
        )

        # Correlation matrix over the numeric columns (price, qty).
        run(
            "corr_price_vol",
            lambda: dft.select(pl.corr("price", "qty").alias("corr")),
        )

    return rows


# ── Polars lazy: data still needs to be read ─────────────────────────────────
# Mirrors `bench_polars` query-for-query but each iteration starts from
# `pl.scan_csv(path)` and ends in `.collect()`, so polars' query optimizer
# (predicate pushdown, projection pushdown, slice/topk fusion, …) is engaged
# and CSV I/O is part of the timed work — the realistic shape for the
# "I have a CSV on disk and I want this answer" workflow. Pair with the
# eager `bench_polars` for the "data is already in memory" baseline.
def bench_polars_lazy(csv_path, csv_multi_path, csv_trades_path, warmup, iters):
    _fw = "polars"
    print("polars-lazy: warming OS page cache...", file=sys.stderr, flush=True)
    pl.scan_csv(csv_path).collect()  # prime the page cache once
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars_lazy/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "polars_lazy",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    def scan():
        return pl.scan_csv(csv_path)

    run(
        "mean_by_symbol",
        lambda: scan().group_by("symbol").agg(pl.col("price").mean().alias("avg_price")).collect(),
    )
    run(
        "ohlc_by_symbol",
        lambda: scan().group_by("symbol").agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("last"),
        ).collect(),
    )
    run(
        "update_price_x2",
        lambda: scan().with_columns((pl.col("price") * 2).alias("price_x2")).collect(),
    )
    run(
        "distinct_symbol",
        lambda: scan().select("symbol").unique(maintain_order=True).collect(),
    )
    run(
        "order_head_topk",
        lambda: scan().sort("price", descending=True).head(100).collect(),
    )
    run(
        "order_head_topk_by_symbol",
        lambda: scan()
        .sort("price", descending=True)
        .group_by("symbol", maintain_order=True)
        .head(3)
        .collect(),
    )
    run(
        "order_tail_topk",
        lambda: scan().sort("price", descending=True).tail(100).collect(),
    )
    run(
        "order_tail_topk_by_symbol",
        lambda: scan()
        .sort("price", descending=True)
        .group_by("symbol", maintain_order=True)
        .tail(3)
        .collect(),
    )
    # Full-table sorts (every row reordered).
    run("sort_price", lambda: scan().sort("price").collect())
    run("sort_price_desc", lambda: scan().sort("price", descending=True).collect())
    run("sort_symbol", lambda: scan().sort("symbol").collect())
    run("sort_symbol_price", lambda: scan().sort(["symbol", "price"]).collect())
    run(
        "sort_symbol_price_desc",
        lambda: scan().sort(["symbol", "price"], descending=[False, True]).collect(),
    )
    # Grouped window functions (per symbol).
    run(
        "rank_by_symbol",
        lambda: scan()
        .with_columns(
            pl.col("price").rank(method="dense", descending=True).over("symbol").alias("rk")
        )
        .collect(),
    )
    run(
        "lag_by_symbol",
        lambda: scan()
        .with_columns(pl.col("price").shift(1).over("symbol").alias("prev"))
        .collect(),
    )
    run(
        "cumsum_by_symbol",
        lambda: scan()
        .with_columns(pl.col("price").cum_sum().over("symbol").alias("cs"))
        .collect(),
    )
    # Expensive group aggregates (per symbol).
    run(
        "median_by_symbol",
        lambda: scan().group_by("symbol").agg(pl.col("price").median().alias("med")).collect(),
    )
    run(
        "quantile_by_symbol",
        lambda: scan()
        .group_by("symbol")
        .agg(pl.col("price").quantile(0.9, interpolation="linear").alias("p90"))
        .collect(),
    )
    run(
        "std_by_symbol",
        lambda: scan().group_by("symbol").agg(pl.col("price").std().alias("sd")).collect(),
    )
    # Multi-stage pipeline: filter → group-by → order → head.
    run(
        "filter_group_sort",
        lambda: scan()
        .filter(pl.col("price") > 500.0)
        .group_by("symbol")
        .agg(pl.col("price").mean().alias("avg"))
        .sort("avg", descending=True)
        .head(10)
        .collect(),
    )
    # update by → filter on derived column → re-aggregate.
    run(
        "update_group_filter",
        lambda: scan()
        .with_columns(
            (pl.col("price") / pl.col("price").shift(1).over("symbol")).log().alias("lr")
        )
        .filter(pl.col("lr") > 0.0)
        .group_by("symbol")
        .agg(pl.col("lr").count().alias("pos_days"))
        .collect(),
    )
    # rank within group → top-N per group → aggregate survivors.
    run(
        "group_rank_filter",
        lambda: scan()
        .with_columns(
            pl.col("price").rank(method="dense", descending=True).over("symbol").alias("rk")
        )
        .filter(pl.col("rk") <= 10)
        .group_by("symbol")
        .agg(pl.col("price").mean().alias("avg_top10"))
        .collect(),
    )
    # grouped z-score → clip to ±3 → re-aggregate.
    run(
        "normalize_by_group",
        lambda: scan()
        .with_columns(
            (
                (pl.col("price") - pl.col("price").mean().over("symbol"))
                / pl.col("price").std().over("symbol")
            ).alias("z")
        )
        .with_columns(pl.col("z").clip(-3.0, 3.0).alias("clipped"))
        .group_by("symbol")
        .agg(
            pl.col("clipped").mean().alias("mean_z"),
            pl.col("clipped").std().alias("sd_z"),
        )
        .collect(),
    )
    # Transforms / single-pass language features.
    run(
        "pmin_clip",
        lambda: scan()
        .with_columns(pl.col("price").clip(upper_bound=500.0).alias("clipped"))
        .collect(),
    )
    run(
        "where_update_clip",
        lambda: scan()
        .with_columns(
            pl.when(pl.col("price") > 900.0)
            .then(900.0)
            .otherwise(pl.col("price"))
            .alias("price")
        )
        .collect(),
    )
    run("rbind_two", lambda: pl.concat([scan(), scan()]).collect())
    run(
        "cumsum_price",
        lambda: scan().with_columns(pl.col("price").cum_sum().alias("cs")).collect(),
    )
    run(
        "cumprod_price",
        lambda: scan().with_columns(pl.col("price").cum_prod().alias("cp")).collect(),
    )
    # rand_* benchmarks inject a numpy-generated Series; skipping in lazy mode
    # since the inputs aren't expressible as a scan-only chain.

    # Scalar row-wise math builtins.
    run("abs_price",
        lambda: scan().with_columns(pl.col("price").abs().alias("v")).collect())
    run("sqrt_price",
        lambda: scan().with_columns(pl.col("price").sqrt().alias("v")).collect())
    run("log_price",
        lambda: scan().with_columns(pl.col("price").log().alias("v")).collect())
    run("exp_price",
        lambda: scan().with_columns((pl.col("price") / 1000.0).exp().alias("v")).collect())
    run("round_price",
        lambda: scan().with_columns(pl.col("price").round(0).alias("v")).collect())
    run("floor_price",
        lambda: scan().with_columns(pl.col("price").floor().alias("v")).collect())
    run("ceil_price",
        lambda: scan().with_columns(pl.col("price").ceil().alias("v")).collect())
    run("sin_price",
        lambda: scan().with_columns(pl.col("price").sin().alias("v")).collect())
    run("cos_price",
        lambda: scan().with_columns(pl.col("price").cos().alias("v")).collect())
    run("tanh_price",
        lambda: scan().with_columns((pl.col("price") / 1000.0).tanh().alias("v")).collect())

    if csv_multi_path:
        def scan_multi():
            return pl.scan_csv(csv_multi_path)

        run(
            "count_by_symbol_day",
            lambda: scan_multi().group_by(["symbol", "day"]).agg(pl.len().alias("n")).collect(),
        )
        run(
            "mean_by_symbol_day",
            lambda: scan_multi().group_by(["symbol", "day"]).agg(
                pl.col("price").mean().alias("avg_price")
            ).collect(),
        )
        run(
            "ohlc_by_symbol_day",
            lambda: scan_multi().group_by(["symbol", "day"]).agg(
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("last"),
            ).collect(),
        )
        # Two-level rollup: by {symbol, day} then re-aggregate by symbol.
        run(
            "symbol_day_to_symbol",
            lambda: scan_multi()
            .group_by(["symbol", "day"])
            .agg(
                pl.col("price").mean().alias("daily_mean"),
                pl.col("price").std().alias("daily_vol"),
            )
            .group_by("symbol")
            .agg(
                pl.col("daily_mean").mean().alias("mean_of_means"),
                pl.col("daily_vol").mean().alias("mean_vol"),
            )
            .collect(),
        )

    if csv_trades_path:
        def scan_trades():
            return pl.scan_csv(csv_trades_path)

        run(
            "filter_simple",
            lambda: scan_trades().filter(pl.col("price") > 500.0).collect(),
        )
        run(
            "filter_and",
            lambda: scan_trades()
            .filter((pl.col("price") > 500.0) & (pl.col("qty") < 100))
            .collect(),
        )
        run(
            "filter_arith",
            lambda: scan_trades()
            .filter(pl.col("price") * pl.col("qty") > 50000.0)
            .collect(),
        )
        run(
            "filter_or",
            lambda: scan_trades()
            .filter((pl.col("price") > 900.0) | (pl.col("qty") < 10))
            .collect(),
        )
        # Correlation matrix over the numeric columns (price, qty).
        run(
            "corr_price_vol",
            lambda: scan_trades().select(pl.corr("price", "qty").alias("corr")).collect(),
        )

    return rows


# ── Null benchmarks ───────────────────────────────────────────────────────────


def bench_pandas_null(csv_path, csv_lookup_path, warmup, iters):
    _fw = "pandas"
    """Left join producing ~50% null right-column values."""
    print("pandas: loading for null bench...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_path)
    lookup = pd.read_csv(csv_lookup_path)
    lookup_symbols = lookup[["symbol"]].drop_duplicates()
    prices_small = df.head(2000).copy()
    lookup_small = lookup.head(64).copy()
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "pandas",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run("null_left_join", lambda: pd.merge(df, lookup, on="symbol", how="left"))
    run(
        "null_semi_join", lambda: pd.merge(df, lookup_symbols, on="symbol", how="inner")
    )
    run("null_anti_join", lambda: df[~df["symbol"].isin(lookup_symbols["symbol"])])
    # Low-cardinality inner join: prices ⋈ lookup on symbol.
    run("inner_join_symbol", lambda: pd.merge(df, lookup, on="symbol", how="inner"))
    run(
        "null_cross_join_small",
        lambda: pd.merge(prices_small, lookup_small, how="cross"),
    )

    return rows


def bench_polars_null(csv_path, csv_lookup_path, warmup, iters):
    _fw = "polars"
    """Left join producing ~50% null right-column values."""
    print("polars: loading for null bench...", file=sys.stderr, flush=True)
    df = pl.read_csv(csv_path)
    lookup = pl.read_csv(csv_lookup_path)
    lookup_symbols = lookup.select("symbol").unique()
    prices_small = df.head(2000)
    lookup_small = lookup.head(64)
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "polars",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run("null_left_join", lambda: df.join(lookup, on="symbol", how="left"))
    run("null_semi_join", lambda: df.join(lookup_symbols, on="symbol", how="semi"))
    run("null_anti_join", lambda: df.join(lookup_symbols, on="symbol", how="anti"))
    # Low-cardinality inner join: prices ⋈ lookup on symbol.
    run("inner_join_symbol", lambda: df.join(lookup, on="symbol", how="inner"))
    run("null_cross_join_small", lambda: prices_small.join(lookup_small, how="cross"))

    return rows


# ── Entry point ───────────────────────────────────────────────────────────────


def bench_pandas_reshape(csv_multi_path, warmup, iters, reshape_rows):
    _fw = "pandas"
    """Melt (wide→long) and dcast (long→wide) on a synthetic OHLC table."""
    if reshape_rows <= 0:
        print("pandas: reshape skipped (disabled for this size)", file=sys.stderr, flush=True)
        return []
    n_day = 400
    print(f"pandas: building synthetic wide table ({reshape_rows} rows)...", file=sys.stderr, flush=True)
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "pandas",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    # Build a synthetic wide OHLC table (reshape_rows rows, 4 measure cols).
    sym = [f"S{i // n_day:04d}" for i in range(reshape_rows)]
    day = (np.arange(reshape_rows, dtype=np.int64) % n_day) + 1
    base = 100.0 + np.arange(reshape_rows, dtype=float) % 1000
    wide = pd.DataFrame({
        "symbol": sym,
        "day":    day,
        "open":  base,
        "high":  base + 1.0,
        "low":   base - 1.0,
        "close": base + 0.5,
    })
    print(f"  pandas: wide table has {len(wide)} rows", file=sys.stderr, flush=True)

    # melt: wide → long
    run(
        "melt_wide_to_long",
        lambda: wide.melt(
            id_vars=["symbol", "day"], value_vars=["open", "high", "low", "close"]
        ),
    )

    # Build long table for dcast
    long = wide.melt(
        id_vars=["symbol", "day"], value_vars=["open", "high", "low", "close"]
    )

    # dcast (pivot): long → wide
    run(
        "dcast_long_to_wide",
        lambda: long.pivot_table(
            index=["symbol", "day"], columns="variable", values="value", aggfunc="first"
        ).reset_index(),
    )

    # Typed-pivot variants: same value matrix as dcast_long_to_wide but with an
    # integer / categorical pivot key (measures how pivot cost varies with key
    # dtype). The key is derived from `variable` so the result is identical
    # cross-engine regardless of melt row order.
    _codes = {"open": 0, "high": 1, "low": 2, "close": 3}
    long_int = long.assign(pivot_id=long["variable"].map(_codes).astype("int64"))
    run(
        "dcast_long_to_wide_int_pivot",
        lambda: long_int.pivot_table(
            index=["symbol", "day"], columns="pivot_id", values="value", aggfunc="first"
        ).reset_index(),
    )
    long_cat = long.assign(pivot_cat=long["variable"].astype("category"))
    run(
        "dcast_long_to_wide_cat_pivot",
        lambda: long_cat.pivot_table(
            index=["symbol", "day"], columns="pivot_cat", values="value",
            aggfunc="first", observed=True,
        ).reset_index(),
    )

    return rows


def bench_polars_reshape(csv_multi_path, warmup, iters, reshape_rows):
    _fw = "polars"
    """Melt (wide→long) and dcast (long→wide) on a synthetic OHLC table."""
    if reshape_rows <= 0:
        print("polars: reshape skipped (disabled for this size)", file=sys.stderr, flush=True)
        return []
    n_day = 400
    print(f"polars: building synthetic wide table ({reshape_rows} rows)...", file=sys.stderr, flush=True)
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "polars",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    # Build a synthetic wide OHLC table (reshape_rows rows, 4 measure cols).
    sym = [f"S{i // n_day:04d}" for i in range(reshape_rows)]
    day = (np.arange(reshape_rows, dtype=np.int64) % n_day) + 1
    base = (100.0 + np.arange(reshape_rows, dtype=float) % 1000).tolist()
    wide = pl.DataFrame({
        "symbol": sym,
        "day":    day.tolist(),
        "open":  base,
        "high":  [v + 1.0 for v in base],
        "low":   [v - 1.0 for v in base],
        "close": [v + 0.5 for v in base],
    })
    print(f"  polars: wide table has {len(wide)} rows", file=sys.stderr, flush=True)

    # melt: wide → long
    run(
        "melt_wide_to_long",
        lambda: wide.unpivot(
            on=["open", "high", "low", "close"], index=["symbol", "day"]
        ),
    )

    # Build long table for dcast
    long = wide.unpivot(on=["open", "high", "low", "close"], index=["symbol", "day"])

    # dcast (pivot): long → wide
    run(
        "dcast_long_to_wide",
        lambda: long.pivot(on="variable", index=["symbol", "day"], values="value"),
    )

    # Typed-pivot variants: integer / categorical pivot key (same value matrix).
    long_int = long.with_columns(
        pl.col("variable")
        .replace_strict({"open": 0, "high": 1, "low": 2, "close": 3}, return_dtype=pl.Int64)
        .alias("pivot_id")
    )
    run(
        "dcast_long_to_wide_int_pivot",
        lambda: long_int.pivot(on="pivot_id", index=["symbol", "day"], values="value"),
    )
    long_cat = long.with_columns(pl.col("variable").cast(pl.Categorical).alias("pivot_cat"))
    run(
        "dcast_long_to_wide_cat_pivot",
        lambda: long_cat.pivot(on="pivot_cat", index=["symbol", "day"], values="value"),
    )

    return rows


def bench_pandas_fill(n_rows, warmup, iters):
    _fw = "pandas"
    """fill_null, fill_forward (LOCF), fill_backward (NOCB) on 50% null numeric data."""
    print("pandas: building fill data...", file=sys.stderr, flush=True)
    vals = np.where(
        np.arange(n_rows) % 2 == 0,
        100.0 + (np.arange(n_rows) % 100).astype(float),
        np.nan,
    )
    df_fill = pd.DataFrame({"val": vals})
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "pandas",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run("fill_null", lambda: df_fill.assign(v2=df_fill["val"].fillna(0.0)))
    run("fill_forward", lambda: df_fill.assign(v2=df_fill["val"].ffill()))
    run("fill_backward", lambda: df_fill.assign(v2=df_fill["val"].bfill()))
    return rows


def bench_polars_fill(n_rows, warmup, iters):
    _fw = "polars"
    """fill_null, fill_forward (LOCF), fill_backward (NOCB) on 50% null numeric data."""
    print("polars: building fill data...", file=sys.stderr, flush=True)
    vals = np.where(
        np.arange(n_rows) % 2 == 0,
        100.0 + (np.arange(n_rows) % 100).astype(float),
        np.nan,
    )
    # Polars treats NaN and null distinctly; convert NaN -> null once so fill_*
    # benchmarks measure null-handling work (same semantics as Ibex/R).
    df_fill = pl.DataFrame({"val": vals}).with_columns(pl.col("val").fill_nan(None))
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "polars",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run(
        "fill_null",
        lambda: df_fill.with_columns(pl.col("val").fill_null(0.0).alias("v2")),
    )
    run(
        "fill_forward",
        lambda: df_fill.with_columns(pl.col("val").forward_fill().alias("v2")),
    )
    run(
        "fill_backward",
        lambda: df_fill.with_columns(pl.col("val").backward_fill().alias("v2")),
    )
    return rows


def bench_pandas_events(csv_events_path, warmup, iters, csv_users_path=None):
    _fw = "pandas"
    print("pandas: loading events...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_events_path)
    users = pd.read_csv(csv_users_path) if csv_users_path else None
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "pandas",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run(
        "sum_by_user",
        lambda: df.groupby("user_id", sort=False)
        .agg(total=("amount", "sum"))
        .reset_index(),
    )

    run("filter_events", lambda: df[df["amount"] > 500.0])

    # High-cardinality inner join: events (N rows, 100K users) ⋈ users (100K).
    if users is not None:
        run("inner_join_user", lambda: df.merge(users, on="user_id", how="inner"))

        # Join-anchored pipelines (Tier 2): enrich events with the user
        # dimension, then derive + roll up (the classic DW pattern).
        def _join_update_group():
            j = df.merge(users, on="user_id", how="inner")
            j = j.assign(revenue=j["amount"] * j["user_tier_multiplier"])
            return (
                j.groupby(["symbol", "user_segment"], sort=False)
                .agg(total_rev=("revenue", "sum"))
                .reset_index()
            )

        run("join_update_group", _join_update_group)

        def _join_filter_rank():
            j = df.merge(users, on="user_id", how="inner")
            j = j[j["user_segment"] == "premium"]
            j = j.assign(rk=j.groupby("symbol")["amount"].rank(method="dense", ascending=False))
            return j[j["rk"] <= 5]

        run("join_filter_rank", _join_filter_rank)

    return rows


def bench_polars_events(csv_events_path, warmup, iters, csv_users_path=None):
    _fw = "polars"
    print("polars: loading events...", file=sys.stderr, flush=True)
    df = pl.read_csv(csv_events_path)
    users = pl.read_csv(csv_users_path) if csv_users_path else None
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "polars",
                name,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                n,
                f"{LAST_PEAK_RSS_MB:.1f}",
            )
        )

    run(
        "sum_by_user",
        lambda: df.group_by("user_id").agg(pl.col("amount").sum().alias("total")),
    )

    run("filter_events", lambda: df.filter(pl.col("amount") > 500.0))

    # High-cardinality inner join: events (N rows, 100K users) ⋈ users (100K).
    if users is not None:
        run("inner_join_user", lambda: df.join(users, on="user_id", how="inner"))

        # Join-anchored pipelines (Tier 2): join → derive → roll up.
        run(
            "join_update_group",
            lambda: df.join(users, on="user_id", how="inner")
            .with_columns(
                (pl.col("amount") * pl.col("user_tier_multiplier")).alias("revenue")
            )
            .group_by(["symbol", "user_segment"])
            .agg(pl.col("revenue").sum().alias("total_rev")),
        )
        run(
            "join_filter_rank",
            lambda: df.join(users, on="user_id", how="inner")
            .filter(pl.col("user_segment") == "premium")
            .with_columns(
                pl.col("amount").rank(method="dense", descending=True).over("symbol").alias("rk")
            )
            .filter(pl.col("rk") <= 5),
        )

    return rows


def bench_pandas_tf(n_rows, warmup, iters):
    _fw = "pandas"
    """TimeFrame rolling + resample. Data mirrors ibex_bench's TF generator:
       1s-spaced UTC Timestamps, sawtooth price = 100.0 + (i % 100)."""
    print("pandas: building tf data...", file=sys.stderr, flush=True)
    ts = pd.to_datetime(np.arange(n_rows), unit="s")
    price = 100.0 + (np.arange(n_rows) % 100).astype(float)
    df = pd.DataFrame({"ts": ts, "price": price}).set_index("ts")
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            ("pandas", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
             f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}")
        )

    run("tf_lag1",             lambda: df["price"].shift(1))
    run("tf_rolling_count_1m", lambda: df["price"].rolling("60s").count())
    run("tf_rolling_sum_1m",   lambda: df["price"].rolling("60s").sum())
    run("tf_rolling_mean_5m",  lambda: df["price"].rolling("300s").mean())
    run("tf_rolling_median_1m",lambda: df["price"].rolling("60s").median())
    run("tf_rolling_std_1m",   lambda: df["price"].rolling("60s").std())
    # Pandas has no time-aware EWM with a window cap — use the standard
    # exponentially-weighted moving average (no window-length truncation).
    # Same alpha (0.1) as the ibex_bench query.
    run("tf_rolling_ewma_1m",  lambda: df["price"].ewm(alpha=0.1, adjust=False).mean())
    run("tf_resample_1m_ohlc",
        lambda: df["price"].resample("60s").agg(["first", "max", "min", "last"]))
    return rows


def bench_polars_tf(n_rows, warmup, iters):
    _fw = "polars"
    """TimeFrame rolling + resample (Polars eager)."""
    print("polars: building tf data...", file=sys.stderr, flush=True)
    ts = pd.to_datetime(np.arange(n_rows), unit="s")
    price = 100.0 + (np.arange(n_rows) % 100).astype(float)
    df = pl.DataFrame({"ts": ts, "price": price})
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            ("polars", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
             f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}")
        )

    # .rechunk() forces the shifted column to materialize: polars' shift is a
    # zero-copy offset view, so without it this measures ~nothing (flat 0.2ms at
    # any size) instead of the real lag-column cost the other engines pay.
    run("tf_lag1",
        lambda: df.select(pl.col("price").shift(1).alias("prev")).rechunk())
    run("tf_rolling_count_1m",
        lambda: df.rolling(index_column="ts", period="60s").agg(c=pl.len()))
    run("tf_rolling_sum_1m",
        lambda: df.rolling(index_column="ts", period="60s").agg(s=pl.col("price").sum()))
    run("tf_rolling_mean_5m",
        lambda: df.rolling(index_column="ts", period="5m").agg(m=pl.col("price").mean()))
    run("tf_rolling_median_1m",
        lambda: df.rolling(index_column="ts", period="60s").agg(med=pl.col("price").median()))
    run("tf_rolling_std_1m",
        lambda: df.rolling(index_column="ts", period="60s").agg(s=pl.col("price").std()))
    run("tf_rolling_ewma_1m",
        lambda: df.with_columns(pl.col("price").ewm_mean(alpha=0.1, adjust=False).alias("e")))
    run("tf_resample_1m_ohlc",
        lambda: df.group_by_dynamic("ts", every="60s").agg(
            open=pl.col("price").first(), high=pl.col("price").max(),
            low=pl.col("price").min(),  close=pl.col("price").last()))
    return rows


def bench_pandas_asof(n_rows, warmup, iters):
    _fw = "pandas"
    """Tf as-of join: trades (left) joined to quotes (right) on time."""
    print("pandas: building asof data...", file=sys.stderr, flush=True)
    # 100 symbols partition both sides (quote i and any trade derived from it
    # share symbol i % 100), so the by-symbol asof has same-symbol candidates.
    n_sym = 100
    sym_cats = [f"SYM{i}" for i in range(n_sym)]
    # Quotes: 1s-spaced, full resolution
    quotes = pd.DataFrame({
        "ts": pd.to_datetime(np.arange(n_rows), unit="s").astype("datetime64[ns]"),
        "symbol": pd.Categorical.from_codes(np.arange(n_rows) % n_sym, categories=sym_cats),
        "bid": 99.0 + (np.arange(n_rows) % 100) * 0.01,
    }).sort_values("ts")
    # Trades: 10% sampled, jittered timestamps
    rng = np.random.default_rng(42)
    sample = np.sort(rng.choice(n_rows, size=n_rows // 10, replace=False))
    trades = pd.DataFrame({
        "ts": (pd.to_datetime(sample, unit="s")
                + pd.to_timedelta(rng.integers(0, 999, len(sample)), unit="ms")
              ).astype("datetime64[ns]"),
        "symbol": pd.Categorical.from_codes(sample % n_sym, categories=sym_cats),
        "qty": rng.integers(1, 100, len(sample)),
    }).sort_values("ts")
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  pandas/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            ("pandas", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
             f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}")
        )

    run("tf_asof_join", lambda: pd.merge_asof(trades, quotes, on="ts", direction="backward"))
    run("tf_asof_join_by_symbol",
        lambda: pd.merge_asof(trades, quotes, on="ts", by="symbol", direction="backward"))
    return rows


def bench_polars_asof(n_rows, warmup, iters):
    _fw = "polars"
    """Tf as-of join (Polars eager)."""
    print("polars: building asof data...", file=sys.stderr, flush=True)
    # 100 symbols partition both sides (see pandas asof note).
    n_sym = 100
    quotes = pl.DataFrame({
        "ts": pd.to_datetime(np.arange(n_rows), unit="s"),
        "symbol": np.arange(n_rows) % n_sym,
        "bid": 99.0 + (np.arange(n_rows) % 100) * 0.01,
    }).with_columns(pl.col("symbol").cast(pl.Utf8)).sort("ts")
    rng = np.random.default_rng(42)
    sample = np.sort(rng.choice(n_rows, size=n_rows // 10, replace=False))
    trades = pl.DataFrame({
        "ts": pd.to_datetime(sample, unit="s") + pd.to_timedelta(rng.integers(0, 999, len(sample)), unit="ms"),
        "symbol": sample % n_sym,
        "qty": rng.integers(1, 100, len(sample)),
    }).with_columns(pl.col("symbol").cast(pl.Utf8)).sort("ts")
    rows = []

    def run(name, fn):
        if should_skip(_fw, name):
            print(f"  {_fw}/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row(_fw, name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  polars/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            ("polars", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
             f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}")
        )

    run("tf_asof_join",
        lambda: trades.join_asof(quotes, on="ts", strategy="backward"))
    run("tf_asof_join_by_symbol",
        lambda: trades.join_asof(quotes, on="ts", by="symbol", strategy="backward"))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to prices.csv")
    ap.add_argument("--csv-multi", help="Path to prices_multi.csv")
    ap.add_argument("--csv-trades", help="Path to trades.csv")
    ap.add_argument("--csv-events", help="Path to events.csv")
    ap.add_argument("--csv-lookup", help="Path to lookup.csv (null benchmark)")
    ap.add_argument("--csv-users", help="Path to users.csv (inner-join dimension)")
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--iters", type=int, default=5)
    ap.add_argument("--out", default="results/python.tsv")
    ap.add_argument("--skip-pandas", action="store_true", help="Run polars only")
    ap.add_argument("--skip-polars", action="store_true", help="Run pandas only")
    ap.add_argument(
        "--polars-lazy",
        action="store_true",
        help="Also run polars in lazy mode via pl.scan_csv (data still needs to be read; "
        "engages the polars query optimizer + I/O pushdown).",
    )
    ap.add_argument(
        "--fill-rows",
        type=int,
        default=4_000_000,
        help="Row count for in-memory fill benchmarks (default: 4000000)",
    )
    ap.add_argument(
        "--reshape-rows",
        type=int,
        default=100_000,
        help="Row count for synthetic reshape benchmarks (default: 100000)",
    )
    ap.add_argument(
        "--tf-rows",
        type=int,
        default=1_000_000,
        help="Row count for TimeFrame rolling/resample/asof benchmarks (default: 1000000)",
    )
    args = ap.parse_args()

    if args.skip_pandas and args.skip_polars:
        raise SystemExit("error: both --skip-pandas and --skip-polars are set")

    all_rows = []
    if not args.skip_pandas:
        all_rows += bench_pandas(
            args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters
        )
    if not args.skip_polars:
        all_rows += bench_polars(
            args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters
        )
    if not args.skip_polars and args.polars_lazy:
        all_rows += bench_polars_lazy(
            args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters
        )
    if args.csv_multi:
        if not args.skip_pandas:
            all_rows += bench_pandas_reshape(
                args.csv_multi, args.warmup, args.iters, args.reshape_rows
            )
        if not args.skip_polars:
            all_rows += bench_polars_reshape(
                args.csv_multi, args.warmup, args.iters, args.reshape_rows
            )
    if args.csv_events:
        if not args.skip_pandas:
            all_rows += bench_pandas_events(
                args.csv_events, args.warmup, args.iters, args.csv_users
            )
        if not args.skip_polars:
            all_rows += bench_polars_events(
                args.csv_events, args.warmup, args.iters, args.csv_users
            )
    if args.csv_lookup:
        if not args.skip_pandas:
            all_rows += bench_pandas_null(
                args.csv, args.csv_lookup, args.warmup, args.iters
            )
        if not args.skip_polars:
            all_rows += bench_polars_null(
                args.csv, args.csv_lookup, args.warmup, args.iters
            )
    if not args.skip_pandas:
        all_rows += bench_pandas_fill(args.fill_rows, args.warmup, args.iters)
    if not args.skip_polars:
        all_rows += bench_polars_fill(args.fill_rows, args.warmup, args.iters)
    if args.tf_rows > 0:
        if not args.skip_pandas:
            all_rows += bench_pandas_tf(args.tf_rows, args.warmup, args.iters)
            all_rows += bench_pandas_asof(args.tf_rows, args.warmup, args.iters)
        if not args.skip_polars:
            all_rows += bench_polars_tf(args.tf_rows, args.warmup, args.iters)
            all_rows += bench_polars_asof(args.tf_rows, args.warmup, args.iters)

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(
            [
                "framework",
                "query",
                "avg_ms",
                "min_ms",
                "max_ms",
                "stddev_ms",
                "p95_ms",
                "p99_ms",
                "rows",
                "peak_rss_mb",
            ]
        )
        w.writerows(all_rows)
    print(f"results written to {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
