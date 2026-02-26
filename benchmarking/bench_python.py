#!/usr/bin/env python3
"""Benchmark pandas and polars on the same aggregation queries as ibex_bench.

Writes tab-separated results to --out (default: results/python.tsv).
Progress goes to stderr; TSV rows go to the output file.

Usage:
  uv run bench_python.py --csv data/prices.csv --csv-multi data/prices_multi.csv
"""
import argparse, csv, pathlib, sys, time
import pandas as pd
import polars as pl


# ── Timing helper ─────────────────────────────────────────────────────────────

def timer(fn, warmup: int, iters: int):
    """Warmup then time fn(). Returns (avg_ms, last_result)."""
    for _ in range(warmup):
        result = fn()
    t0 = time.perf_counter()
    for _ in range(iters):
        result = fn()
    avg_ms = (time.perf_counter() - t0) * 1000 / iters
    return avg_ms, result


# ── Benchmark suites ──────────────────────────────────────────────────────────

def bench_pandas(csv_path, csv_multi_path, csv_trades_path, warmup, iters):
    print("pandas: loading...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_path)
    rows = []

    def run(name, fn):
        avg_ms, result = timer(fn, warmup, iters)
        n = len(result)
        print(f"  pandas/{name}: avg_ms={avg_ms:.3f}, rows={n}", file=sys.stderr, flush=True)
        rows.append(("pandas", name, f"{avg_ms:.3f}", n))

    run("mean_by_symbol",
        lambda: df.groupby("symbol", sort=False).agg(
            avg_price=("price", "mean")).reset_index())

    run("ohlc_by_symbol",
        lambda: df.groupby("symbol", sort=False).agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            last=("price", "last"),
        ).reset_index())

    run("update_price_x2",
        lambda: df.assign(price_x2=df["price"] * 2))

    if csv_multi_path:
        print("pandas: loading multi...", file=sys.stderr, flush=True)
        dfm = pd.read_csv(csv_multi_path)

        run("count_by_symbol_day",
            lambda: dfm.groupby(["symbol", "day"], sort=False).size()
                       .reset_index(name="n"))

        run("mean_by_symbol_day",
            lambda: dfm.groupby(["symbol", "day"], sort=False).agg(
                avg_price=("price", "mean")).reset_index())

        run("ohlc_by_symbol_day",
            lambda: dfm.groupby(["symbol", "day"], sort=False).agg(
                open=("price", "first"),
                high=("price", "max"),
                low=("price", "min"),
                last=("price", "last"),
            ).reset_index())

    if csv_trades_path:
        print("pandas: loading trades...", file=sys.stderr, flush=True)
        dft = pd.read_csv(csv_trades_path)

        run("filter_simple",
            lambda: dft[dft["price"] > 500.0])

        run("filter_and",
            lambda: dft[(dft["price"] > 500.0) & (dft["qty"] < 100)])

        run("filter_arith",
            lambda: dft[dft["price"] * dft["qty"] > 50000.0])

        run("filter_or",
            lambda: dft[(dft["price"] > 900.0) | (dft["qty"] < 10)])

    return rows


def bench_polars(csv_path, csv_multi_path, csv_trades_path, warmup, iters):
    print("polars: loading...", file=sys.stderr, flush=True)
    df = pl.read_csv(csv_path)
    rows = []

    def run(name, fn):
        avg_ms, result = timer(fn, warmup, iters)
        n = len(result)
        print(f"  polars/{name}: avg_ms={avg_ms:.3f}, rows={n}", file=sys.stderr, flush=True)
        rows.append(("polars", name, f"{avg_ms:.3f}", n))

    run("mean_by_symbol",
        lambda: df.group_by("symbol").agg(
            pl.col("price").mean().alias("avg_price")))

    run("ohlc_by_symbol",
        lambda: df.group_by("symbol").agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("last"),
        ))

    run("update_price_x2",
        lambda: df.with_columns((pl.col("price") * 2).alias("price_x2")))

    if csv_multi_path:
        print("polars: loading multi...", file=sys.stderr, flush=True)
        dfm = pl.read_csv(csv_multi_path)

        run("count_by_symbol_day",
            lambda: dfm.group_by(["symbol", "day"]).agg(pl.len().alias("n")))

        run("mean_by_symbol_day",
            lambda: dfm.group_by(["symbol", "day"]).agg(
                pl.col("price").mean().alias("avg_price")))

        run("ohlc_by_symbol_day",
            lambda: dfm.group_by(["symbol", "day"]).agg(
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("last"),
            ))

    if csv_trades_path:
        print("polars: loading trades...", file=sys.stderr, flush=True)
        dft = pl.read_csv(csv_trades_path)

        run("filter_simple",
            lambda: dft.filter(pl.col("price") > 500.0))

        run("filter_and",
            lambda: dft.filter((pl.col("price") > 500.0) & (pl.col("qty") < 100)))

        run("filter_arith",
            lambda: dft.filter(pl.col("price") * pl.col("qty") > 50000.0))

        run("filter_or",
            lambda: dft.filter((pl.col("price") > 900.0) | (pl.col("qty") < 10)))

    return rows


# ── Null benchmarks ───────────────────────────────────────────────────────────

def bench_pandas_null(csv_path, csv_lookup_path, warmup, iters):
    """Left join producing ~50% null right-column values."""
    print("pandas: loading for null bench...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_path)
    lookup = pd.read_csv(csv_lookup_path)
    rows = []

    def run(name, fn):
        avg_ms, result = timer(fn, warmup, iters)
        n = len(result)
        print(f"  pandas/{name}: avg_ms={avg_ms:.3f}, rows={n}", file=sys.stderr, flush=True)
        rows.append(("pandas", name, f"{avg_ms:.3f}", n))

    run("null_left_join",
        lambda: pd.merge(df, lookup, on="symbol", how="left"))

    return rows


def bench_polars_null(csv_path, csv_lookup_path, warmup, iters):
    """Left join producing ~50% null right-column values."""
    print("polars: loading for null bench...", file=sys.stderr, flush=True)
    df = pl.read_csv(csv_path)
    lookup = pl.read_csv(csv_lookup_path)
    rows = []

    def run(name, fn):
        avg_ms, result = timer(fn, warmup, iters)
        n = len(result)
        print(f"  polars/{name}: avg_ms={avg_ms:.3f}, rows={n}", file=sys.stderr, flush=True)
        rows.append(("polars", name, f"{avg_ms:.3f}", n))

    run("null_left_join",
        lambda: df.join(lookup, on="symbol", how="left"))

    return rows


# ── Entry point ───────────────────────────────────────────────────────────────

def bench_pandas_events(csv_events_path, warmup, iters):
    print("pandas: loading events...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_events_path)
    rows = []

    def run(name, fn):
        avg_ms, result = timer(fn, warmup, iters)
        n = len(result)
        print(f"  pandas/{name}: avg_ms={avg_ms:.3f}, rows={n}", file=sys.stderr, flush=True)
        rows.append(("pandas", name, f"{avg_ms:.3f}", n))

    run("sum_by_user",
        lambda: df.groupby("user_id", sort=False).agg(
            total=("amount", "sum")).reset_index())

    run("filter_events",
        lambda: df[df["amount"] > 500.0])

    return rows


def bench_polars_events(csv_events_path, warmup, iters):
    print("polars: loading events...", file=sys.stderr, flush=True)
    df = pl.read_csv(csv_events_path)
    rows = []

    def run(name, fn):
        avg_ms, result = timer(fn, warmup, iters)
        n = len(result)
        print(f"  polars/{name}: avg_ms={avg_ms:.3f}, rows={n}", file=sys.stderr, flush=True)
        rows.append(("polars", name, f"{avg_ms:.3f}", n))

    run("sum_by_user",
        lambda: df.group_by("user_id").agg(pl.col("amount").sum().alias("total")))

    run("filter_events",
        lambda: df.filter(pl.col("amount") > 500.0))

    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv",         required=True, help="Path to prices.csv")
    ap.add_argument("--csv-multi",   help="Path to prices_multi.csv")
    ap.add_argument("--csv-trades",  help="Path to trades.csv")
    ap.add_argument("--csv-events",  help="Path to events.csv")
    ap.add_argument("--csv-lookup",  help="Path to lookup.csv (null benchmark)")
    ap.add_argument("--warmup",      type=int, default=1)
    ap.add_argument("--iters",       type=int, default=5)
    ap.add_argument("--out",         default="results/python.tsv")
    args = ap.parse_args()

    all_rows = []
    all_rows += bench_pandas(args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters)
    all_rows += bench_polars(args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters)
    if args.csv_events:
        all_rows += bench_pandas_events(args.csv_events, args.warmup, args.iters)
        all_rows += bench_polars_events(args.csv_events, args.warmup, args.iters)
    if args.csv_lookup:
        all_rows += bench_pandas_null(args.csv, args.csv_lookup, args.warmup, args.iters)
        all_rows += bench_polars_null(args.csv, args.csv_lookup, args.warmup, args.iters)

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["framework", "query", "avg_ms", "rows"])
        w.writerows(all_rows)
    print(f"results written to {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
