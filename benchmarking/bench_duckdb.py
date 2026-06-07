#!/usr/bin/env python3
"""Benchmark DuckDB on the same aggregation queries as ibex_bench.

Writes tab-separated results to --out (default: results/duckdb.tsv).
Progress goes to stderr; TSV rows go to the output file.

Usage:
  uv run bench_duckdb.py --csv data/prices.csv --csv-multi data/prices_multi.csv
  uv run bench_duckdb.py --csv data/prices.csv --threads 1
"""
import argparse, csv, pathlib, sys, time
import numpy as np
import duckdb


# ── Timing helper ─────────────────────────────────────────────────────────────


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from bench_mem import reset_peak_rss, peak_rss_mb, CELL_CUTOFF_MS, should_skip, cut_row

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


# ── Benchmark suites ──────────────────────────────────────────────────────────


def bench_duckdb_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, con):
    print("duckdb: loading...", file=sys.stderr, flush=True)
    con.execute(f"CREATE OR REPLACE TABLE prices AS SELECT * FROM read_csv_auto('{csv_path}')")
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "duckdb",
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
        lambda: con.sql(
            "SELECT symbol, AVG(price) AS avg_price FROM prices GROUP BY symbol"
        ).fetchnumpy(),
    )

    run(
        "ohlc_by_symbol",
        lambda: con.sql(
            "SELECT symbol, "
            "FIRST(price) AS open, MAX(price) AS high, "
            "MIN(price) AS low, LAST(price) AS last "
            "FROM prices GROUP BY symbol"
        ).fetchnumpy(),
    )

    run(
        "update_price_x2",
        lambda: con.sql(
            "SELECT *, price * 2 AS price_x2 FROM prices"
        ).fetchnumpy(),
    )

    run(
        "distinct_symbol",
        lambda: con.sql("SELECT DISTINCT symbol FROM prices").fetchnumpy(),
    )

    run(
        "order_head_topk",
        lambda: con.sql(
            "SELECT * FROM prices ORDER BY price DESC LIMIT 100"
        ).fetchnumpy(),
    )

    run(
        "order_head_topk_by_symbol",
        lambda: con.sql(
            "SELECT * FROM (SELECT *, ROW_NUMBER() OVER ("
            "PARTITION BY symbol ORDER BY price DESC) AS rn FROM prices) "
            "WHERE rn <= 3"
        ).fetchnumpy(),
    )

    run(
        "order_tail_topk",
        lambda: con.sql(
            "SELECT * FROM prices ORDER BY price ASC LIMIT 100"
        ).fetchnumpy(),
    )

    run(
        "order_tail_topk_by_symbol",
        lambda: con.sql(
            "SELECT * FROM (SELECT *, ROW_NUMBER() OVER ("
            "PARTITION BY symbol ORDER BY price ASC) AS rn FROM prices) "
            "WHERE rn <= 3"
        ).fetchnumpy(),
    )

    # Full-table sorts (no LIMIT — every row materialised in order).
    run(
        "sort_price",
        lambda: con.sql("SELECT * FROM prices ORDER BY price ASC").fetchnumpy(),
    )

    run(
        "sort_symbol_price",
        lambda: con.sql(
            "SELECT * FROM prices ORDER BY symbol ASC, price ASC"
        ).fetchnumpy(),
    )

    run(
        "cumsum_price",
        lambda: con.sql(
            "SELECT *, SUM(price) OVER ("
            "ORDER BY rowid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ") AS cs FROM prices"
        ).fetchnumpy(),
    )

    run(
        "rand_uniform",
        lambda: con.sql(
            "SELECT *, random() AS r FROM prices"
        ).fetchnumpy(),
    )

    if csv_multi_path:
        print("duckdb: loading multi...", file=sys.stderr, flush=True)
        con.execute(
            f"CREATE OR REPLACE TABLE prices_multi AS SELECT * FROM read_csv_auto('{csv_multi_path}')"
        )

        run(
            "count_by_symbol_day",
            lambda: con.sql(
                "SELECT symbol, day, COUNT(*) AS n "
                "FROM prices_multi GROUP BY symbol, day"
            ).fetchnumpy(),
        )

        run(
            "mean_by_symbol_day",
            lambda: con.sql(
                "SELECT symbol, day, AVG(price) AS avg_price "
                "FROM prices_multi GROUP BY symbol, day"
            ).fetchnumpy(),
        )

        run(
            "ohlc_by_symbol_day",
            lambda: con.sql(
                "SELECT symbol, day, "
                "FIRST(price) AS open, MAX(price) AS high, "
                "MIN(price) AS low, LAST(price) AS last "
                "FROM prices_multi GROUP BY symbol, day"
            ).fetchnumpy(),
        )

    if csv_trades_path:
        print("duckdb: loading trades...", file=sys.stderr, flush=True)
        con.execute(
            f"CREATE OR REPLACE TABLE trades AS SELECT * FROM read_csv_auto('{csv_trades_path}')"
        )

        run(
            "filter_simple",
            lambda: con.sql(
                "SELECT * FROM trades WHERE price > 500.0"
            ).fetchnumpy(),
        )

        run(
            "filter_and",
            lambda: con.sql(
                "SELECT * FROM trades WHERE price > 500.0 AND qty < 100"
            ).fetchnumpy(),
        )

        run(
            "filter_arith",
            lambda: con.sql(
                "SELECT * FROM trades WHERE price * qty > 50000.0"
            ).fetchnumpy(),
        )

        run(
            "filter_or",
            lambda: con.sql(
                "SELECT * FROM trades WHERE price > 900.0 OR qty < 10"
            ).fetchnumpy(),
        )

    return rows


def bench_duckdb_null(csv_path, csv_lookup_path, warmup, iters, con):
    """Left join producing ~50% null right-column values."""
    print("duckdb: loading for null bench...", file=sys.stderr, flush=True)
    con.execute(f"CREATE OR REPLACE TABLE prices AS SELECT * FROM read_csv_auto('{csv_path}')")
    con.execute(f"CREATE OR REPLACE TABLE lookup AS SELECT * FROM read_csv_auto('{csv_lookup_path}')")
    con.execute("CREATE OR REPLACE TABLE lookup_symbols AS SELECT DISTINCT symbol FROM lookup")
    con.execute("CREATE OR REPLACE TABLE prices_small AS SELECT * FROM prices LIMIT 2000")
    con.execute("CREATE OR REPLACE TABLE lookup_small AS SELECT * FROM lookup LIMIT 64")
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "duckdb",
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
        lambda: con.sql(
            "SELECT * FROM prices LEFT JOIN lookup USING (symbol)"
        ).fetchnumpy(),
    )
    run(
        "null_semi_join",
        lambda: con.sql(
            "SELECT * FROM prices SEMI JOIN lookup_symbols USING (symbol)"
        ).fetchnumpy(),
    )
    run(
        "null_anti_join",
        lambda: con.sql(
            "SELECT * FROM prices ANTI JOIN lookup_symbols USING (symbol)"
        ).fetchnumpy(),
    )
    run(
        "null_cross_join_small",
        lambda: con.sql(
            "SELECT * FROM prices_small CROSS JOIN lookup_small"
        ).fetchnumpy(),
    )

    return rows


def bench_duckdb_reshape(warmup, iters, reshape_rows, con):
    """Melt (wide->long) and dcast (long->wide) on a synthetic OHLC table."""
    if reshape_rows <= 0:
        print("duckdb: reshape skipped (disabled for this size)", file=sys.stderr, flush=True)
        return []
    n_day = 400
    print(
        f"duckdb: building synthetic wide table ({reshape_rows} rows)...",
        file=sys.stderr,
        flush=True,
    )
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "duckdb",
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

    con.execute(f"""
        CREATE OR REPLACE TABLE wide AS
        SELECT
            printf('S%04d', (i / {n_day})::INTEGER) AS symbol,
            ((i % {n_day}) + 1)::INTEGER AS day,
            100.0 + (i % 1000) AS open,
            101.0 + (i % 1000) AS high,
            99.0  + (i % 1000) AS low,
            100.5 + (i % 1000) AS close
        FROM generate_series(0, {reshape_rows - 1}) t(i)
    """)
    print(
        f"  duckdb: wide table has {reshape_rows} rows",
        file=sys.stderr,
        flush=True,
    )

    run(
        "melt_wide_to_long",
        lambda: con.sql(
            "UNPIVOT wide ON open, high, low, close "
            "INTO NAME variable VALUE value"
        ).fetchnumpy(),
    )

    # Build long table for dcast
    con.execute(
        "CREATE OR REPLACE TABLE long_tbl AS "
        "UNPIVOT wide ON open, high, low, close "
        "INTO NAME variable VALUE value"
    )

    run(
        "dcast_long_to_wide",
        lambda: con.sql(
            "PIVOT long_tbl ON variable USING FIRST(value) "
            "GROUP BY symbol, day"
        ).fetchnumpy(),
    )

    return rows


def bench_duckdb_events(csv_events_path, warmup, iters, con):
    print("duckdb: loading events...", file=sys.stderr, flush=True)
    con.execute(
        f"CREATE OR REPLACE TABLE events AS SELECT * FROM read_csv_auto('{csv_events_path}')"
    )
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "duckdb",
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
        lambda: con.sql(
            "SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id"
        ).fetchnumpy(),
    )

    run(
        "filter_events",
        lambda: con.sql(
            "SELECT * FROM events WHERE amount > 500.0"
        ).fetchnumpy(),
    )

    return rows


def bench_duckdb_fill(n_rows, warmup, iters, con):
    """fill_null, fill_forward (LOCF), fill_backward (NOCB) on 50% null numeric data."""
    print("duckdb: building fill data...", file=sys.stderr, flush=True)
    con.execute(f"""
        CREATE OR REPLACE TABLE fill_data AS
        SELECT CASE WHEN i % 2 = 0 THEN 100.0 + (i % 100) ELSE NULL END AS val
        FROM generate_series(0, {n_rows - 1}) t(i)
    """)
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "duckdb",
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
        lambda: con.sql(
            "SELECT COALESCE(val, 0.0) AS v2 FROM fill_data"
        ).fetchnumpy(),
    )
    run(
        "fill_forward",
        lambda: con.sql(
            "SELECT LAST_VALUE(val IGNORE NULLS) OVER ("
            "ORDER BY rowid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ") AS v2 FROM fill_data"
        ).fetchnumpy(),
    )
    run(
        "fill_backward",
        lambda: con.sql(
            "SELECT FIRST_VALUE(val IGNORE NULLS) OVER ("
            "ORDER BY rowid ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"
            ") AS v2 FROM fill_data"
        ).fetchnumpy(),
    )
    return rows


def bench_duckdb_tf(n_rows, warmup, iters, con):
    """TimeFrame rolling + resample. Same shape as bench_python tf_data."""
    print("duckdb: building tf data...", file=sys.stderr, flush=True)
    con.execute(f"""
        CREATE OR REPLACE TABLE tf_data AS
        SELECT TIMESTAMP '1970-01-01' + INTERVAL (i) SECOND AS ts,
               100.0 + (i % 100) AS price
        FROM generate_series(0, {n_rows - 1}) t(i)
    """)
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr, flush=True,
        )
        rows.append(("duckdb", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
                     f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}"))

    # All rolling queries use a range-based time window aligned with the ibex
    # `window 1m` / `window 5m` semantics (inclusive on both ends).
    def w(period):
        return (
            f"OVER (ORDER BY ts RANGE BETWEEN INTERVAL {period} PRECEDING "
            f"AND CURRENT ROW)"
        )

    run("tf_lag1",
        lambda: con.sql("SELECT lag(price) OVER (ORDER BY ts) AS prev FROM tf_data").fetchnumpy())
    run("tf_rolling_count_1m",
        lambda: con.sql(f"SELECT count(*) {w('60 SECONDS')} AS c FROM tf_data").fetchnumpy())
    run("tf_rolling_sum_1m",
        lambda: con.sql(f"SELECT sum(price) {w('60 SECONDS')} AS s FROM tf_data").fetchnumpy())
    run("tf_rolling_mean_5m",
        lambda: con.sql(f"SELECT avg(price) {w('300 SECONDS')} AS m FROM tf_data").fetchnumpy())
    run("tf_rolling_median_1m",
        lambda: con.sql(f"SELECT median(price) {w('60 SECONDS')} AS med FROM tf_data").fetchnumpy())
    run("tf_rolling_std_1m",
        lambda: con.sql(f"SELECT stddev_samp(price) {w('60 SECONDS')} AS s FROM tf_data").fetchnumpy())
    # DuckDB has no built-in time-aware EWMA. Skip — leave the cell empty so
    # the page surfaces that rather than printing a misleading number.
    run("tf_resample_1m_ohlc",
        lambda: con.sql(
            "SELECT time_bucket(INTERVAL 60 SECONDS, ts) AS bucket, "
            "       first(price ORDER BY ts) AS open, "
            "       max(price) AS high, min(price) AS low, "
            "       last(price ORDER BY ts) AS close "
            "FROM tf_data GROUP BY bucket ORDER BY bucket"
        ).fetchnumpy())
    return rows


def bench_duckdb_asof(n_rows, warmup, iters, con):
    """Tf as-of join: trades (~10%) joined to quotes (1s) on ts (backward)."""
    print("duckdb: building asof data...", file=sys.stderr, flush=True)
    con.execute(f"""
        CREATE OR REPLACE TABLE quotes AS
        SELECT TIMESTAMP '1970-01-01' + INTERVAL (i) SECOND AS ts,
               'SYM' || (i % 100) AS symbol,
               99.0 + (i % 100) * 0.01 AS bid
        FROM generate_series(0, {n_rows - 1}) t(i)
        ORDER BY ts;
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE trades AS
        WITH sampled AS (
            SELECT i FROM generate_series(0, {n_rows - 1}) t(i)
            USING SAMPLE 10 PERCENT (reservoir, 42)
        )
        SELECT TIMESTAMP '1970-01-01' + INTERVAL (i) SECOND
                 + INTERVAL ((hash(i) % 999)) MILLISECOND AS ts,
               'SYM' || (i % 100) AS symbol,
               1 + (hash(i) % 99) AS qty
        FROM sampled ORDER BY ts;
    """)
    rows = []

    def run(name, fn):
        if should_skip("duckdb", name):
            print(f"  duckdb/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("duckdb", name))
            return
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(next(iter(result.values())))
        print(
            f"  duckdb/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr, flush=True,
        )
        rows.append(("duckdb", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
                     f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}"))

    run("tf_asof_join",
        lambda: con.sql(
            "SELECT t.ts, t.qty, q.bid "
            "FROM trades t ASOF LEFT JOIN quotes q ON t.ts >= q.ts"
        ).fetchnumpy())
    run("tf_asof_join_by_symbol",
        lambda: con.sql(
            "SELECT t.ts, t.qty, q.bid "
            "FROM trades t ASOF LEFT JOIN quotes q "
            "ON t.symbol = q.symbol AND t.ts >= q.ts"
        ).fetchnumpy())
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
    ap.add_argument("--out", default="results/duckdb.tsv")
    ap.add_argument(
        "--threads",
        type=int,
        default=0,
        help="DuckDB thread count (0 = auto, typically all cores)",
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

    con = duckdb.connect()
    if args.threads > 0:
        con.execute(f"SET threads TO {args.threads}")

    all_rows = []
    all_rows += bench_duckdb_core(
        args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters, con
    )
    if args.csv_multi:
        all_rows += bench_duckdb_reshape(
            args.warmup, args.iters, args.reshape_rows, con
        )
    if args.csv_events:
        all_rows += bench_duckdb_events(args.csv_events, args.warmup, args.iters, con)
    if args.csv_lookup:
        all_rows += bench_duckdb_null(
            args.csv, args.csv_lookup, args.warmup, args.iters, con
        )
    all_rows += bench_duckdb_fill(args.fill_rows, args.warmup, args.iters, con)
    if args.tf_rows > 0:
        all_rows += bench_duckdb_tf(args.tf_rows, args.warmup, args.iters, con)
        all_rows += bench_duckdb_asof(args.tf_rows, args.warmup, args.iters, con)

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
