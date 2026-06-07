#!/usr/bin/env python3
"""Benchmark ClickHouse (embedded via chdb) on the same queries as ibex_bench.

Writes tab-separated results to --out (default: results/clickhouse.tsv).
Progress goes to stderr; TSV rows go to the output file.

Usage:
  uv run bench_clickhouse.py --csv data/prices.csv --csv-multi data/prices_multi.csv
  uv run bench_clickhouse.py --csv data/prices.csv --threads 1
"""
import argparse, csv, pathlib, sys, time
import numpy as np
from chdb.session import Session


# ── Timing helper ─────────────────────────────────────────────────────────────


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from bench_mem import reset_peak_rss, peak_rss_mb, CELL_CUTOFF_MS, should_skip, cut_row, run_phase

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


def _count_rows(sess, sql):
    """Run a count() wrapper query to get result row count (not timed)."""
    res = sess.query(f"SELECT count() FROM ({sql})", "TabSeparated")
    return int(res.data().strip())


_SINK = "_bench_sink"


def _materialize(sess, sql):
    """Execute the query and materialise its full result into an in-memory table.

    Unlike `FORMAT Null` (which streams rows through a sink and discards them in
    bounded blocks), CREATE ... AS builds the whole result in ClickHouse's native
    columnar memory — so output-heavy queries (melt/dcast/wide filters) pay the
    same allocate+write cost ibex/polars/duckdb/datafusion do when they return a
    result. Aggregations stay cheap because their result is small. CREATE OR
    REPLACE drops the previous result first, so peak memory is one result."""
    sess.query(f"CREATE OR REPLACE TABLE {_SINK} ENGINE = Memory AS {sql}")


# ── Benchmark suites ──────────────────────────────────────────────────────────


def bench_clickhouse_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, sess):
    print("clickhouse: loading...", file=sys.stderr, flush=True)
    sess.query(f"CREATE OR REPLACE TABLE prices ENGINE = Memory AS SELECT * FROM file('{csv_path}', CSVWithNames)")
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "clickhouse",
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
        "SELECT symbol, avg(price) AS avg_price FROM prices GROUP BY symbol",
    )

    run(
        "ohlc_by_symbol",
        "SELECT symbol, "
        "any(price) AS open, max(price) AS high, "
        "min(price) AS low, anyLast(price) AS last "
        "FROM prices GROUP BY symbol",
    )

    run(
        "update_price_x2",
        "SELECT *, price * 2 AS price_x2 FROM prices",
    )

    run(
        "distinct_symbol",
        "SELECT DISTINCT symbol FROM prices",
    )

    run(
        "order_head_topk",
        "SELECT * FROM prices ORDER BY price DESC LIMIT 100",
    )

    run(
        "order_head_topk_by_symbol",
        "SELECT * FROM (SELECT *, row_number() OVER ("
        "PARTITION BY symbol ORDER BY price DESC) AS rn FROM prices) "
        "WHERE rn <= 3",
    )

    run(
        "order_tail_topk",
        "SELECT * FROM prices ORDER BY price ASC LIMIT 100",
    )

    run(
        "order_tail_topk_by_symbol",
        "SELECT * FROM (SELECT *, row_number() OVER ("
        "PARTITION BY symbol ORDER BY price ASC) AS rn FROM prices) "
        "WHERE rn <= 3",
    )

    # Full-table sorts (no LIMIT — every row materialised in order).
    run(
        "sort_price",
        "SELECT * FROM prices ORDER BY price ASC",
    )

    run(
        "sort_symbol_price",
        "SELECT * FROM prices ORDER BY symbol ASC, price ASC",
    )

    run(
        "cumsum_price",
        "SELECT *, sum(price) OVER ("
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        ") AS cs FROM prices",
    )

    run(
        "rand_uniform",
        "SELECT *, rand() / 4294967295.0 AS r FROM prices",
    )

    if csv_multi_path:
        print("clickhouse: loading multi...", file=sys.stderr, flush=True)
        sess.query(
            f"CREATE OR REPLACE TABLE prices_multi ENGINE = Memory "
            f"AS SELECT * FROM file('{csv_multi_path}', CSVWithNames)"
        )

        run(
            "count_by_symbol_day",
            "SELECT symbol, day, count() AS n "
            "FROM prices_multi GROUP BY symbol, day",
        )

        run(
            "mean_by_symbol_day",
            "SELECT symbol, day, avg(price) AS avg_price "
            "FROM prices_multi GROUP BY symbol, day",
        )

        run(
            "ohlc_by_symbol_day",
            "SELECT symbol, day, "
            "any(price) AS open, max(price) AS high, "
            "min(price) AS low, anyLast(price) AS last "
            "FROM prices_multi GROUP BY symbol, day",
        )

    if csv_trades_path:
        print("clickhouse: loading trades...", file=sys.stderr, flush=True)
        sess.query(
            f"CREATE OR REPLACE TABLE trades ENGINE = Memory "
            f"AS SELECT * FROM file('{csv_trades_path}', CSVWithNames)"
        )

        run(
            "filter_simple",
            "SELECT * FROM trades WHERE price > 500.0",
        )

        run(
            "filter_and",
            "SELECT * FROM trades WHERE price > 500.0 AND qty < 100",
        )

        run(
            "filter_arith",
            "SELECT * FROM trades WHERE price * qty > 50000.0",
        )

        run(
            "filter_or",
            "SELECT * FROM trades WHERE price > 900.0 OR qty < 10",
        )

    return rows


def bench_clickhouse_null(csv_path, csv_lookup_path, warmup, iters, sess):
    """Left join producing ~50% null right-column values."""
    print("clickhouse: loading for null bench...", file=sys.stderr, flush=True)
    sess.query(f"CREATE OR REPLACE TABLE prices ENGINE = Memory AS SELECT * FROM file('{csv_path}', CSVWithNames)")
    sess.query(f"CREATE OR REPLACE TABLE lookup ENGINE = Memory AS SELECT * FROM file('{csv_lookup_path}', CSVWithNames)")
    sess.query("CREATE OR REPLACE TABLE lookup_symbols ENGINE = Memory AS SELECT DISTINCT symbol FROM lookup")
    sess.query("CREATE OR REPLACE TABLE prices_small ENGINE = Memory AS SELECT * FROM prices LIMIT 2000")
    sess.query("CREATE OR REPLACE TABLE lookup_small ENGINE = Memory AS SELECT * FROM lookup LIMIT 64")
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "clickhouse",
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
        "SELECT * FROM prices LEFT JOIN lookup USING (symbol)",
    )
    run(
        "null_semi_join",
        "SELECT * FROM prices LEFT SEMI JOIN lookup_symbols USING (symbol)",
    )
    run(
        "null_anti_join",
        "SELECT * FROM prices LEFT ANTI JOIN lookup_symbols USING (symbol)",
    )
    run(
        "null_cross_join_small",
        "SELECT * FROM prices_small CROSS JOIN lookup_small",
    )

    return rows


def bench_clickhouse_reshape(warmup, iters, reshape_rows, sess):
    """Melt (wide->long) and dcast (long->wide) on a synthetic OHLC table."""
    if reshape_rows <= 0:
        print("clickhouse: reshape skipped (disabled for this size)", file=sys.stderr, flush=True)
        return []
    n_day = 400
    print(
        f"clickhouse: building synthetic wide table ({reshape_rows} rows)...",
        file=sys.stderr,
        flush=True,
    )
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "clickhouse",
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

    sess.query(f"""
        CREATE OR REPLACE TABLE wide ENGINE = Memory AS
        SELECT
            concat('S', lpad(toString(toUInt32(number / {n_day})), 4, '0')) AS symbol,
            toInt32((number % {n_day}) + 1) AS day,
            100.0 + (number % 1000) AS open,
            101.0 + (number % 1000) AS high,
            99.0  + (number % 1000) AS low,
            100.5 + (number % 1000) AS close
        FROM numbers({reshape_rows})
    """)
    print(
        f"  clickhouse: wide table has {reshape_rows} rows",
        file=sys.stderr,
        flush=True,
    )

    # melt via UNION ALL
    melt_sql = (
        "SELECT symbol, day, 'open' AS variable, open AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'high' AS variable, high AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'low' AS variable, low AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'close' AS variable, close AS value FROM wide"
    )
    run("melt_wide_to_long", melt_sql)

    # Build long table for dcast
    sess.query(f"CREATE OR REPLACE TABLE long_tbl ENGINE = Memory AS {melt_sql}")

    # dcast via conditional aggregation
    run(
        "dcast_long_to_wide",
        "SELECT symbol, day, "
        "anyIf(value, variable = 'open') AS open, "
        "anyIf(value, variable = 'high') AS high, "
        "anyIf(value, variable = 'low') AS low, "
        "anyIf(value, variable = 'close') AS close "
        "FROM long_tbl GROUP BY symbol, day",
    )

    return rows


def bench_clickhouse_events(csv_events_path, warmup, iters, sess):
    print("clickhouse: loading events...", file=sys.stderr, flush=True)
    sess.query(
        f"CREATE OR REPLACE TABLE events ENGINE = Memory "
        f"AS SELECT * FROM file('{csv_events_path}', CSVWithNames)"
    )
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "clickhouse",
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
        "SELECT user_id, sum(amount) AS total FROM events GROUP BY user_id",
    )

    run(
        "filter_events",
        "SELECT * FROM events WHERE amount > 500.0",
    )

    return rows


def bench_clickhouse_fill(n_rows, warmup, iters, sess):
    """fill_null, fill_forward (LOCF) and fill_backward on 50% null numeric data.
    Forward/backward use last_value/first_value with IGNORE NULLS over an
    unbounded frame — ClickHouse evaluates these in a single pass (O(n) and fast
    at scale, unlike DataFusion's quadratic IGNORE NULLS)."""
    print("clickhouse: building fill data...", file=sys.stderr, flush=True)
    sess.query(f"""
        CREATE OR REPLACE TABLE fill_data ENGINE = Memory AS
        SELECT
            rowNumberInAllBlocks() AS _rowid,
            CASE WHEN number % 2 = 0
                 THEN toNullable(100.0 + (number % 100))
                 ELSE CAST(NULL AS Nullable(Float64))
            END AS val
        FROM numbers({n_rows})
    """)
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "clickhouse",
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
        "SELECT coalesce(val, 0.0) AS v2 FROM fill_data",
    )
    run(
        "fill_forward",
        "SELECT last_value(val) IGNORE NULLS OVER "
        "(ORDER BY _rowid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM fill_data",
    )
    run(
        "fill_backward",
        # Equivalent to first_value IGNORE NULLS over an unbounded *following* frame,
        # but ClickHouse evaluates that O(n^2) (72s at 32M). Reversing the order
        # (DESC) turns it into the same single-pass O(n) form fill_forward uses
        # (~0.65s at 16M, 24x faster) — verified identical results.
        "SELECT last_value(val) IGNORE NULLS OVER "
        "(ORDER BY _rowid DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM fill_data",
    )
    return rows


def bench_clickhouse_tf(n_rows, warmup, iters, sess):
    """TimeFrame rolling + lag + resample via ClickHouse window functions.

    ClickHouse window frames take only numeric (row) offsets, not RANGE INTERVAL.
    The tf data is 1s-spaced, so the time window [t-Ns, t] (inclusive, as duckdb's
    RANGE BETWEEN INTERVAL N SECONDS and DataFusion use) is exactly N+1 rows:
    `ROWS BETWEEN N PRECEDING AND CURRENT ROW`. For this regular series that is
    identical to those engines' time-range windows (verified: same rolling sums).
    EWMA is omitted: ClickHouse's exponentialMovingAverage uses a different
    (time-decay) parameterisation than the alpha=0.1 form, so it is not comparable.
    """
    print("clickhouse: building tf data...", file=sys.stderr, flush=True)
    sess.query(f"""
        CREATE OR REPLACE TABLE tf_data ENGINE = Memory AS
        SELECT toDateTime(number) AS ts, 100.0 + (number % 100) AS price
        FROM numbers({n_rows})
    """)
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "clickhouse",
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

    # Window [t-Ns, t] inclusive == N+1 rows on the 1s series: 60s -> 60 PRECEDING,
    # 5m -> 300 PRECEDING.
    def w(rows_back):
        return f"OVER (ORDER BY ts ROWS BETWEEN {rows_back} PRECEDING AND CURRENT ROW)"

    run("tf_lag1",
        f"SELECT lagInFrame(price, 1) {w(1)} AS prev FROM tf_data")
    run("tf_rolling_count_1m",
        f"SELECT count(*) {w(60)} AS c FROM tf_data")
    run("tf_rolling_sum_1m",
        f"SELECT sum(price) {w(60)} AS s FROM tf_data")
    run("tf_rolling_mean_5m",
        f"SELECT avg(price) {w(300)} AS m FROM tf_data")
    run("tf_rolling_median_1m",
        f"SELECT median(price) {w(60)} AS med FROM tf_data")
    run("tf_rolling_std_1m",
        f"SELECT stddevSamp(price) {w(60)} AS s FROM tf_data")
    run("tf_resample_1m_ohlc",
        "SELECT toStartOfInterval(ts, INTERVAL 60 SECOND) AS bucket, "
        "       argMin(price, ts) AS open, max(price) AS high, "
        "       min(price) AS low, argMax(price, ts) AS close "
        "FROM tf_data GROUP BY bucket ORDER BY bucket")
    return rows


def bench_clickhouse_asof(n_rows, warmup, iters, sess):
    """tf_asof_join_by_symbol via ClickHouse's native ASOF JOIN. The keyless
    tf_asof_join is left out: chdb's ASOF JOIN requires at least one equi-join
    column, so a symbol-less variant would need a synthetic constant key."""
    print("clickhouse: building asof data...", file=sys.stderr, flush=True)
    # Quotes: 1s-spaced, 100 symbols. Trades: ~10% sample, sub-second offset so
    # each trade falls between quote ticks (mirrors the duckdb/polars asof setup).
    sess.query(f"""
        CREATE OR REPLACE TABLE quotes ENGINE = Memory AS
        SELECT toDateTime64(number, 3) AS ts,
               'SYM' || toString(number % 100) AS symbol,
               99.0 + (number % 100) * 0.01 AS bid
        FROM numbers({n_rows})
    """)
    sess.query(f"""
        CREATE OR REPLACE TABLE trades ENGINE = Memory AS
        SELECT toDateTime64(number + (cityHash64(number) % 1000) / 1000.0, 3) AS ts,
               'SYM' || toString(number % 100) AS symbol,
               1 + (cityHash64(number) % 99) AS qty
        FROM numbers({n_rows}) WHERE number % 10 = 0
    """)
    rows = []

    def run(name, sql):
        if should_skip("clickhouse", name):
            print(f"  clickhouse/{name}: SKIPPED (cut at a smaller scale)", file=sys.stderr, flush=True)
            rows.append(cut_row("clickhouse", name))
            return
        n = _count_rows(sess, sql)

        def fn():
            _materialize(sess, sql)

        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, _ = timer(
            fn, warmup, iters
        )
        print(
            f"  clickhouse/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            ("clickhouse", name, f"{avg_ms:.3f}", f"{min_ms:.3f}", f"{max_ms:.3f}",
             f"{stddev_ms:.3f}", f"{p95_ms:.3f}", f"{p99_ms:.3f}", n, f"{LAST_PEAK_RSS_MB:.1f}")
        )

    run("tf_asof_join_by_symbol",
        "SELECT t.ts AS ts, t.qty AS qty, q.bid AS bid "
        "FROM trades AS t ASOF LEFT JOIN quotes AS q "
        "ON t.symbol = q.symbol AND t.ts >= q.ts")
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
    ap.add_argument("--out", default="results/clickhouse.tsv")
    ap.add_argument(
        "--threads",
        type=int,
        default=0,
        help="ClickHouse max_threads (0 = auto)",
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

    sess = Session()
    if args.threads > 0:
        sess.query(f"SET max_threads = {args.threads}")
    # chdb holds every benchmark table in :memory:, so at the largest scales a
    # heavy query (the asof join, a window over 50M rows) can exhaust RAM. Cap the
    # per-query budget at ~85% of system memory so it raises a catchable
    # "Memory limit exceeded" instead of getting OS-OOM-killed (uncatchable) —
    # run_phase() then drops just that phase and keeps the rest.
    try:
        with open("/proc/meminfo") as mf:
            mem_kb = int(next(l for l in mf if l.startswith("MemTotal:")).split()[1])
        sess.query(f"SET max_memory_usage = {int(mem_kb * 1024 * 0.85)}")
    except Exception as e:  # noqa: BLE001
        print(f"clickhouse: could not set max_memory_usage: {e}", file=sys.stderr)

    all_rows = []
    all_rows += run_phase("core", lambda: bench_clickhouse_core(
        args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters, sess))
    if args.csv_multi:
        all_rows += run_phase("reshape", lambda: bench_clickhouse_reshape(
            args.warmup, args.iters, args.reshape_rows, sess))
    if args.csv_events:
        all_rows += run_phase("events", lambda: bench_clickhouse_events(
            args.csv_events, args.warmup, args.iters, sess))
    if args.csv_lookup:
        all_rows += run_phase("null", lambda: bench_clickhouse_null(
            args.csv, args.csv_lookup, args.warmup, args.iters, sess))
    all_rows += run_phase("fill", lambda: bench_clickhouse_fill(
        args.fill_rows, args.warmup, args.iters, sess))
    if args.tf_rows > 0:
        all_rows += run_phase("tf", lambda: bench_clickhouse_tf(
            args.tf_rows, args.warmup, args.iters, sess))
        all_rows += run_phase("asof", lambda: bench_clickhouse_asof(
            args.tf_rows, args.warmup, args.iters, sess))

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
