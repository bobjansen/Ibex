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


def timer(fn, warmup: int, iters: int):
    """Warmup then time fn(). Returns (avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, last_result)."""
    for _ in range(warmup):
        result = fn()
    times = []
    for _ in range(iters):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
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


# ── Benchmark suites ──────────────────────────────────────────────────────────


def bench_clickhouse_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, sess):
    print("clickhouse: loading...", file=sys.stderr, flush=True)
    sess.query(f"CREATE OR REPLACE TABLE prices ENGINE = Memory AS SELECT * FROM file('{csv_path}', CSVWithNames)")
    rows = []

    def run(name, sql):
        n = _count_rows(sess, sql)

        def fn():
            sess.query(sql, "Null")

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
        n = _count_rows(sess, sql)

        def fn():
            sess.query(sql, "Null")

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
    n_day = 400
    print(
        f"clickhouse: building synthetic wide table ({reshape_rows} rows)...",
        file=sys.stderr,
        flush=True,
    )
    rows = []

    def run(name, sql):
        n = _count_rows(sess, sql)

        def fn():
            sess.query(sql, "Null")

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
        n = _count_rows(sess, sql)

        def fn():
            sess.query(sql, "Null")

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
    """fill_null on 50% null numeric data. fill_forward/backward skipped (no IGNORE NULLS)."""
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
        n = _count_rows(sess, sql)

        def fn():
            sess.query(sql, "Null")

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
            )
        )

    run(
        "fill_null",
        "SELECT coalesce(val, 0.0) AS v2 FROM fill_data",
    )
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
        "--reshape-rows",
        type=int,
        default=100_000,
        help="Row count for synthetic reshape benchmarks (default: 100000)",
    )
    args = ap.parse_args()

    sess = Session()
    if args.threads > 0:
        sess.query(f"SET max_threads = {args.threads}")

    all_rows = []
    all_rows += bench_clickhouse_core(
        args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters, sess
    )
    if args.csv_multi:
        all_rows += bench_clickhouse_reshape(
            args.warmup, args.iters, args.reshape_rows, sess
        )
    if args.csv_events:
        all_rows += bench_clickhouse_events(args.csv_events, args.warmup, args.iters, sess)
    if args.csv_lookup:
        all_rows += bench_clickhouse_null(
            args.csv, args.csv_lookup, args.warmup, args.iters, sess
        )
    all_rows += bench_clickhouse_fill(args.fill_rows, args.warmup, args.iters, sess)

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
            ]
        )
        w.writerows(all_rows)
    print(f"results written to {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
