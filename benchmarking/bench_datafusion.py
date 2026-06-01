#!/usr/bin/env python3
"""Benchmark Apache DataFusion on the same aggregation queries as ibex_bench.

Writes tab-separated results to --out (default: results/datafusion.tsv).
Progress goes to stderr; TSV rows go to the output file.

Usage:
  uv run bench_datafusion.py --csv data/prices.csv --csv-multi data/prices_multi.csv
  uv run bench_datafusion.py --csv data/prices.csv --threads 1
"""
import argparse, csv, pathlib, sys, time
import numpy as np
import pyarrow.csv as pcsv
import pyarrow as pa
from datafusion import SessionContext, SessionConfig


# ── Timing helper ─────────────────────────────────────────────────────────────


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from bench_mem import reset_peak_rss, peak_rss_mb, CELL_CUTOFF_MS

# Absolute peak RSS (MiB) measured during the most recent timer() call.
LAST_PEAK_RSS_MB = 0.0


def timer(fn, warmup: int, iters: int):
    """Warmup then time fn(). Returns (avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, last_result).
    Side effect: sets module global LAST_PEAK_RSS_MB to peak RSS during the measured iterations."""
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


def batch_row_count(batches):
    """Count total rows across a list of RecordBatch objects."""
    return sum(rb.num_rows for rb in batches)


# ── Benchmark suites ──────────────────────────────────────────────────────────


def _register_arrow(ctx, name, csv_path):
    """Read a CSV into an Arrow table and register it in-memory (not lazy)."""
    table = pcsv.read_csv(csv_path)
    try:
        ctx.deregister_table(name)
    except Exception:
        pass
    ctx.register_record_batches(name, [table.to_batches()])


def bench_datafusion_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, ctx):
    print("datafusion: loading...", file=sys.stderr, flush=True)
    _register_arrow(ctx, "prices", csv_path)
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = batch_row_count(result)
        print(
            f"  datafusion/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "datafusion",
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
        lambda: ctx.sql(
            "SELECT symbol, AVG(price) AS avg_price FROM prices GROUP BY symbol"
        ).collect(),
    )

    run(
        "ohlc_by_symbol",
        lambda: ctx.sql(
            "SELECT symbol, "
            "FIRST_VALUE(price) AS open, MAX(price) AS high, "
            "MIN(price) AS low, LAST_VALUE(price) AS last "
            "FROM prices GROUP BY symbol"
        ).collect(),
    )

    run(
        "update_price_x2",
        lambda: ctx.sql(
            "SELECT *, price * 2 AS price_x2 FROM prices"
        ).collect(),
    )

    run(
        "cumsum_price",
        lambda: ctx.sql(
            "SELECT *, SUM(price) OVER ("
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ") AS cs FROM prices"
        ).collect(),
    )

    run(
        "rand_uniform",
        lambda: ctx.sql(
            "SELECT *, random() AS r FROM prices"
        ).collect(),
    )

    if csv_multi_path:
        print("datafusion: loading multi...", file=sys.stderr, flush=True)
        _register_arrow(ctx, "prices_multi", csv_multi_path)

        run(
            "count_by_symbol_day",
            lambda: ctx.sql(
                "SELECT symbol, day, COUNT(*) AS n "
                "FROM prices_multi GROUP BY symbol, day"
            ).collect(),
        )

        run(
            "mean_by_symbol_day",
            lambda: ctx.sql(
                "SELECT symbol, day, AVG(price) AS avg_price "
                "FROM prices_multi GROUP BY symbol, day"
            ).collect(),
        )

        run(
            "ohlc_by_symbol_day",
            lambda: ctx.sql(
                "SELECT symbol, day, "
                "FIRST_VALUE(price) AS open, MAX(price) AS high, "
                "MIN(price) AS low, LAST_VALUE(price) AS last "
                "FROM prices_multi GROUP BY symbol, day"
            ).collect(),
        )

    if csv_trades_path:
        print("datafusion: loading trades...", file=sys.stderr, flush=True)
        _register_arrow(ctx, "trades", csv_trades_path)

        run(
            "filter_simple",
            lambda: ctx.sql(
                "SELECT * FROM trades WHERE price > 500.0"
            ).collect(),
        )

        run(
            "filter_and",
            lambda: ctx.sql(
                "SELECT * FROM trades WHERE price > 500.0 AND qty < 100"
            ).collect(),
        )

        run(
            "filter_arith",
            lambda: ctx.sql(
                "SELECT * FROM trades WHERE price * qty > 50000.0"
            ).collect(),
        )

        run(
            "filter_or",
            lambda: ctx.sql(
                "SELECT * FROM trades WHERE price > 900.0 OR qty < 10"
            ).collect(),
        )

    return rows


def bench_datafusion_null(csv_path, csv_lookup_path, warmup, iters, ctx):
    """Left join producing ~50% null right-column values."""
    print("datafusion: loading for null bench...", file=sys.stderr, flush=True)
    _register_arrow(ctx, "prices", csv_path)
    _register_arrow(ctx, "lookup", csv_lookup_path)
    # Materialize lookup_symbols
    batches = ctx.sql("SELECT DISTINCT symbol FROM lookup").collect()
    for tbl in ("lookup_symbols", "prices_small", "lookup_small"):
        try:
            ctx.deregister_table(tbl)
        except Exception:
            pass
    ctx.register_record_batches("lookup_symbols", [batches])
    # Small tables for cross join
    batches = ctx.sql("SELECT * FROM prices LIMIT 2000").collect()
    ctx.register_record_batches("prices_small", [batches])
    batches = ctx.sql("SELECT * FROM lookup LIMIT 64").collect()
    ctx.register_record_batches("lookup_small", [batches])
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = batch_row_count(result)
        print(
            f"  datafusion/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "datafusion",
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
        "null_left_join",
        lambda: ctx.sql(
            "SELECT * FROM prices LEFT JOIN lookup USING (symbol)"
        ).collect(),
    )
    run(
        "null_semi_join",
        lambda: ctx.sql(
            "SELECT p.* FROM prices p "
            "WHERE EXISTS (SELECT 1 FROM lookup_symbols l WHERE l.symbol = p.symbol)"
        ).collect(),
    )
    run(
        "null_anti_join",
        lambda: ctx.sql(
            "SELECT p.* FROM prices p "
            "WHERE NOT EXISTS (SELECT 1 FROM lookup_symbols l WHERE l.symbol = p.symbol)"
        ).collect(),
    )
    run(
        "null_cross_join_small",
        lambda: ctx.sql(
            "SELECT * FROM prices_small CROSS JOIN lookup_small"
        ).collect(),
    )

    return rows


def bench_datafusion_reshape(warmup, iters, reshape_rows, ctx):
    """Melt (wide->long) and dcast (long->wide) on a synthetic OHLC table."""
    if reshape_rows <= 0:
        print("datafusion: reshape skipped (disabled for this size)", file=sys.stderr, flush=True)
        return []
    n_day = 400
    print(
        f"datafusion: building synthetic wide table ({reshape_rows} rows)...",
        file=sys.stderr,
        flush=True,
    )
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = batch_row_count(result)
        print(
            f"  datafusion/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "datafusion",
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

    # Build synthetic wide table via Arrow
    idx = np.arange(reshape_rows)
    sym = [f"S{i // n_day:04d}" for i in range(reshape_rows)]
    day = (idx % n_day + 1).astype(np.int64)
    base = 100.0 + (idx % 1000).astype(np.float64)
    table = pa.table({
        "symbol": sym,
        "day": day,
        "open": base,
        "high": base + 1.0,
        "low": base - 1.0,
        "close": base + 0.5,
    })
    ctx.register_record_batches("wide", [table.to_batches()])
    print(
        f"  datafusion: wide table has {reshape_rows} rows",
        file=sys.stderr,
        flush=True,
    )

    # melt via UNION ALL (DataFusion has no UNPIVOT)
    run(
        "melt_wide_to_long",
        lambda: ctx.sql(
            "SELECT symbol, day, 'open' AS variable, open AS value FROM wide "
            "UNION ALL "
            "SELECT symbol, day, 'high' AS variable, high AS value FROM wide "
            "UNION ALL "
            "SELECT symbol, day, 'low' AS variable, low AS value FROM wide "
            "UNION ALL "
            "SELECT symbol, day, 'close' AS variable, close AS value FROM wide"
        ).collect(),
    )

    # Build long table for dcast
    long_batches = ctx.sql(
        "SELECT symbol, day, 'open' AS variable, open AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'high' AS variable, high AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'low' AS variable, low AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'close' AS variable, close AS value FROM wide"
    ).collect()
    ctx.register_record_batches("long_tbl", [long_batches])

    # dcast via conditional aggregation (DataFusion has no PIVOT)
    run(
        "dcast_long_to_wide",
        lambda: ctx.sql(
            "SELECT symbol, day, "
            "MAX(CASE WHEN variable = 'open' THEN value END) AS open, "
            "MAX(CASE WHEN variable = 'high' THEN value END) AS high, "
            "MAX(CASE WHEN variable = 'low' THEN value END) AS low, "
            "MAX(CASE WHEN variable = 'close' THEN value END) AS close "
            "FROM long_tbl GROUP BY symbol, day"
        ).collect(),
    )

    return rows


def bench_datafusion_events(csv_events_path, warmup, iters, ctx):
    print("datafusion: loading events...", file=sys.stderr, flush=True)
    _register_arrow(ctx, "events", csv_events_path)
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = batch_row_count(result)
        print(
            f"  datafusion/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "datafusion",
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
        lambda: ctx.sql(
            "SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id"
        ).collect(),
    )

    run(
        "filter_events",
        lambda: ctx.sql(
            "SELECT * FROM events WHERE amount > 500.0"
        ).collect(),
    )

    return rows


def bench_datafusion_fill(n_rows, warmup, iters, ctx):
    """fill_null only. fill_forward/backward skipped (IGNORE NULLS is O(n²) in DataFusion)."""
    print("datafusion: building fill data...", file=sys.stderr, flush=True)
    vals = np.where(
        np.arange(n_rows) % 2 == 0,
        100.0 + (np.arange(n_rows) % 100).astype(float),
        np.nan,
    )
    mask = np.isnan(vals)
    arr = pa.array(np.where(mask, 0.0, vals), type=pa.float64(), mask=mask)
    table = pa.table({"val": arr})
    ctx.register_record_batches("fill_data", [table.to_batches()])
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = batch_row_count(result)
        print(
            f"  datafusion/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "datafusion",
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
        lambda: ctx.sql(
            "SELECT COALESCE(val, 0.0) AS v2 FROM fill_data"
        ).collect(),
    )
    # fill_forward / fill_backward skipped: DataFusion *supports* LAST_VALUE(... )
    # IGNORE NULLS, but over an unbounded preceding frame it is O(n²) — measured
    # not to finish even at 1M rows in 150s — so it is genuinely impractical here,
    # unlike ClickHouse which evaluates the same pattern in a single O(n) pass.
    return rows


def bench_datafusion_tf(n_rows, warmup, iters, ctx):
    """TimeFrame lag + rolling + resample. DataFusion supports time RANGE window
    frames (RANGE BETWEEN INTERVAL ... PRECEDING), so the rolling windows are true
    time windows — apples-to-apples with ibex/duckdb/polars. resample uses
    date_bin(). Left unimplemented: as-of join (no native DataFusion asof) and
    time-windowed EWMA (no native function — would need a UDF)."""
    print("datafusion: building tf data...", file=sys.stderr, flush=True)
    idx = np.arange(n_rows)
    ts = pa.array(idx.astype("datetime64[s]"), type=pa.timestamp("s"))
    price = pa.array(100.0 + (idx % 100).astype(float), type=pa.float64())
    table = pa.table({"ts": ts, "price": price})
    ctx.register_record_batches("tf_data", [table.to_batches()])
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = batch_row_count(result)
        print(
            f"  datafusion/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "datafusion",
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

    # Time-range frame: 1m / 5m windows over the timestamp, inclusive of the
    # current row (matches the other engines' window semantics).
    def w(period):
        return (
            f"OVER (ORDER BY ts RANGE BETWEEN INTERVAL '{period}' PRECEDING "
            f"AND CURRENT ROW)"
        )

    run("tf_lag1",
        lambda: ctx.sql(
            "SELECT lag(price) OVER (ORDER BY ts) AS prev FROM tf_data").collect())
    run("tf_rolling_count_1m",
        lambda: ctx.sql(f"SELECT count(*) {w('60 seconds')} AS c FROM tf_data").collect())
    run("tf_rolling_sum_1m",
        lambda: ctx.sql(f"SELECT sum(price) {w('60 seconds')} AS s FROM tf_data").collect())
    run("tf_rolling_mean_5m",
        lambda: ctx.sql(f"SELECT avg(price) {w('300 seconds')} AS m FROM tf_data").collect())
    run("tf_rolling_median_1m",
        lambda: ctx.sql(f"SELECT median(price) {w('60 seconds')} AS med FROM tf_data").collect())
    run("tf_rolling_std_1m",
        lambda: ctx.sql(f"SELECT stddev_samp(price) {w('60 seconds')} AS s FROM tf_data").collect())
    run("tf_resample_1m_ohlc",
        lambda: ctx.sql(
            "SELECT date_bin(INTERVAL '60 seconds', ts) AS bucket, "
            "       first_value(price ORDER BY ts) AS open, max(price) AS high, "
            "       min(price) AS low, last_value(price ORDER BY ts) AS close "
            "FROM tf_data GROUP BY bucket ORDER BY bucket").collect())
    return rows


# ── Entry point ───────────────────────────────────────────────────────────────


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to prices.csv")
    ap.add_argument("--csv-multi", help="Path to prices_multi.csv")
    ap.add_argument("--csv-trades", help="Path to trades.csv")
    ap.add_argument("--csv-events", help="Path to events.csv")
    ap.add_argument("--csv-lookup", help="Path to lookup.csv (null benchmark)")
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--iters", type=int, default=5)
    ap.add_argument("--out", default="results/datafusion.tsv")
    ap.add_argument(
        "--threads",
        type=int,
        default=0,
        help="DataFusion target partitions (0 = auto)",
    )
    ap.add_argument(
        "--fill-rows",
        type=int,
        default=4_000_000,
        help="Row count for in-memory fill benchmarks (default: 4000000)",
    )
    ap.add_argument(
        "--tf-rows", type=int, default=0, help=argparse.SUPPRESS,
    )
    ap.add_argument(
        "--reshape-rows",
        type=int,
        default=100_000,
        help="Row count for synthetic reshape benchmarks (default: 100000)",
    )
    args = ap.parse_args()

    if args.threads > 0:
        config = SessionConfig().with_target_partitions(args.threads)
        ctx = SessionContext(config)
    else:
        ctx = SessionContext()

    all_rows = []
    all_rows += bench_datafusion_core(
        args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters, ctx
    )
    if args.csv_multi:
        all_rows += bench_datafusion_reshape(
            args.warmup, args.iters, args.reshape_rows, ctx
        )
    if args.csv_events:
        all_rows += bench_datafusion_events(args.csv_events, args.warmup, args.iters, ctx)
    if args.csv_lookup:
        all_rows += bench_datafusion_null(
            args.csv, args.csv_lookup, args.warmup, args.iters, ctx
        )
    all_rows += bench_datafusion_fill(args.fill_rows, args.warmup, args.iters, ctx)
    if args.tf_rows > 0:
        all_rows += bench_datafusion_tf(args.tf_rows, args.warmup, args.iters, ctx)

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
        # Drop cells cut by the per-iteration cutoff (sentinel avg_ms < 0).
        w.writerows([r for r in all_rows if float(r[2]) >= 0])
    print(f"results written to {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
