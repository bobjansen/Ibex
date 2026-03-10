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


# ── Benchmark suites ──────────────────────────────────────────────────────────


def bench_duckdb_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, con):
    print("duckdb: loading...", file=sys.stderr, flush=True)
    con.execute(f"CREATE OR REPLACE TABLE prices AS SELECT * FROM read_csv_auto('{csv_path}')")
    rows = []

    def run(name, fn):
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
    n_day = 400
    print(
        f"duckdb: building synthetic wide table ({reshape_rows} rows)...",
        file=sys.stderr,
        flush=True,
    )
    rows = []

    def run(name, fn):
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
