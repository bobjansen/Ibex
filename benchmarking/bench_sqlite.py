#!/usr/bin/env python3
"""Benchmark SQLite on the same aggregation queries as ibex_bench.

SQLite is a row-oriented database included as a baseline to show where
columnar engines (ibex, DuckDB, DataFusion) excel on analytical workloads.

Writes tab-separated results to --out (default: results/sqlite.tsv).
Progress goes to stderr; TSV rows go to the output file.

Skipped queries (no clean SQLite equivalent):
  - ohlc_by_symbol, ohlc_by_symbol_day  (no FIRST/LAST aggregate)
  - rand_*                                (no per-row random in SQL)
  - cumprod_price                         (no native support)
  - fill_forward, fill_backward           (no IGNORE NULLS in window)

Usage:
  uv run bench_sqlite.py --csv data/prices.csv --csv-multi data/prices_multi.csv
"""
import argparse, csv, pathlib, sqlite3, sys, time
import numpy as np
import pandas as pd


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


def _load_csv(con, table_name, csv_path):
    """Load a CSV file into an in-memory SQLite table using pandas."""
    print(f"sqlite: loading {csv_path} → {table_name}...", file=sys.stderr, flush=True)
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, con, if_exists="replace", index=False)
    return len(df)


# ── Benchmark suites ──────────────────────────────────────────────────────────


def bench_sqlite_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, con):
    _load_csv(con, "prices", csv_path)
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  sqlite/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "sqlite",
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
        lambda: con.execute(
            "SELECT symbol, AVG(price) AS avg_price FROM prices GROUP BY symbol"
        ).fetchall(),
    )

    run(
        "update_price_x2",
        lambda: con.execute(
            "SELECT *, price * 2 AS price_x2 FROM prices"
        ).fetchall(),
    )

    run(
        "cumsum_price",
        lambda: con.execute(
            "SELECT *, SUM(price) OVER ("
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ") AS cs FROM prices"
        ).fetchall(),
    )

    if csv_multi_path:
        _load_csv(con, "prices_multi", csv_multi_path)

        run(
            "count_by_symbol_day",
            lambda: con.execute(
                "SELECT symbol, day, COUNT(*) AS n "
                "FROM prices_multi GROUP BY symbol, day"
            ).fetchall(),
        )

        run(
            "mean_by_symbol_day",
            lambda: con.execute(
                "SELECT symbol, day, AVG(price) AS avg_price "
                "FROM prices_multi GROUP BY symbol, day"
            ).fetchall(),
        )

    if csv_trades_path:
        _load_csv(con, "trades", csv_trades_path)

        run(
            "filter_simple",
            lambda: con.execute(
                "SELECT * FROM trades WHERE price > 500.0"
            ).fetchall(),
        )

        run(
            "filter_and",
            lambda: con.execute(
                "SELECT * FROM trades WHERE price > 500.0 AND qty < 100"
            ).fetchall(),
        )

        run(
            "filter_arith",
            lambda: con.execute(
                "SELECT * FROM trades WHERE price * qty > 50000.0"
            ).fetchall(),
        )

        run(
            "filter_or",
            lambda: con.execute(
                "SELECT * FROM trades WHERE price > 900.0 OR qty < 10"
            ).fetchall(),
        )

    return rows


def bench_sqlite_null(csv_path, csv_lookup_path, warmup, iters, con):
    """Left join producing ~50% null right-column values."""
    _load_csv(con, "prices", csv_path)
    _load_csv(con, "lookup", csv_lookup_path)
    con.execute("CREATE TABLE IF NOT EXISTS lookup_symbols AS SELECT DISTINCT symbol FROM lookup")
    con.execute("CREATE TABLE IF NOT EXISTS prices_small AS SELECT * FROM prices LIMIT 2000")
    con.execute("CREATE TABLE IF NOT EXISTS lookup_small AS SELECT * FROM lookup LIMIT 64")
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  sqlite/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "sqlite",
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
        lambda: con.execute(
            "SELECT * FROM prices LEFT JOIN lookup USING (symbol)"
        ).fetchall(),
    )
    run(
        "null_semi_join",
        lambda: con.execute(
            "SELECT p.* FROM prices p "
            "WHERE EXISTS (SELECT 1 FROM lookup_symbols l WHERE l.symbol = p.symbol)"
        ).fetchall(),
    )
    run(
        "null_anti_join",
        lambda: con.execute(
            "SELECT p.* FROM prices p "
            "WHERE NOT EXISTS (SELECT 1 FROM lookup_symbols l WHERE l.symbol = p.symbol)"
        ).fetchall(),
    )
    run(
        "null_cross_join_small",
        lambda: con.execute(
            "SELECT * FROM prices_small CROSS JOIN lookup_small"
        ).fetchall(),
    )

    return rows


def bench_sqlite_reshape(warmup, iters, reshape_rows, con):
    """Melt (wide->long) and dcast (long->wide) on a synthetic OHLC table."""
    n_day = 400
    print(
        f"sqlite: building synthetic wide table ({reshape_rows} rows)...",
        file=sys.stderr,
        flush=True,
    )
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  sqlite/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "sqlite",
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

    # Build synthetic wide table
    idx = np.arange(reshape_rows)
    sym = [f"S{i // n_day:04d}" for i in range(reshape_rows)]
    day = (idx % n_day + 1).astype(int)
    base = 100.0 + (idx % 1000).astype(float)
    wide_df = pd.DataFrame({
        "symbol": sym,
        "day": day,
        "open": base,
        "high": base + 1.0,
        "low": base - 1.0,
        "close": base + 0.5,
    })
    wide_df.to_sql("wide", con, if_exists="replace", index=False)
    print(
        f"  sqlite: wide table has {reshape_rows} rows",
        file=sys.stderr,
        flush=True,
    )

    # melt via UNION ALL
    run(
        "melt_wide_to_long",
        lambda: con.execute(
            "SELECT symbol, day, 'open' AS variable, open AS value FROM wide "
            "UNION ALL "
            "SELECT symbol, day, 'high' AS variable, high AS value FROM wide "
            "UNION ALL "
            "SELECT symbol, day, 'low' AS variable, low AS value FROM wide "
            "UNION ALL "
            "SELECT symbol, day, 'close' AS variable, close AS value FROM wide"
        ).fetchall(),
    )

    # Build long table for dcast
    con.execute("DROP TABLE IF EXISTS long_tbl")
    con.execute(
        "CREATE TABLE long_tbl AS "
        "SELECT symbol, day, 'open' AS variable, open AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'high' AS variable, high AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'low' AS variable, low AS value FROM wide "
        "UNION ALL "
        "SELECT symbol, day, 'close' AS variable, close AS value FROM wide"
    )

    # dcast via conditional aggregation
    run(
        "dcast_long_to_wide",
        lambda: con.execute(
            "SELECT symbol, day, "
            "MAX(CASE WHEN variable = 'open' THEN value END) AS open, "
            "MAX(CASE WHEN variable = 'high' THEN value END) AS high, "
            "MAX(CASE WHEN variable = 'low' THEN value END) AS low, "
            "MAX(CASE WHEN variable = 'close' THEN value END) AS close "
            "FROM long_tbl GROUP BY symbol, day"
        ).fetchall(),
    )

    return rows


def bench_sqlite_events(csv_events_path, warmup, iters, con):
    _load_csv(con, "events", csv_events_path)
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  sqlite/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "sqlite",
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
        lambda: con.execute(
            "SELECT user_id, SUM(amount) AS total FROM events GROUP BY user_id"
        ).fetchall(),
    )

    run(
        "filter_events",
        lambda: con.execute(
            "SELECT * FROM events WHERE amount > 500.0"
        ).fetchall(),
    )

    return rows


def bench_sqlite_fill(n_rows, warmup, iters, con):
    """fill_null only (fill_forward/backward need IGNORE NULLS which SQLite lacks)."""
    print("sqlite: building fill data...", file=sys.stderr, flush=True)
    vals = np.where(
        np.arange(n_rows) % 2 == 0,
        100.0 + (np.arange(n_rows) % 100).astype(float),
        np.nan,
    )
    pd.DataFrame({"val": vals}).to_sql("fill_data", con, if_exists="replace", index=False)
    rows = []

    def run(name, fn):
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, warmup, iters
        )
        n = len(result)
        print(
            f"  sqlite/{name}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, p99_ms={p99_ms:.3f}, rows={n}",
            file=sys.stderr,
            flush=True,
        )
        rows.append(
            (
                "sqlite",
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
        lambda: con.execute(
            "SELECT COALESCE(val, 0.0) AS v2 FROM fill_data"
        ).fetchall(),
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
    ap.add_argument("--out", default="results/sqlite.tsv")
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

    con = sqlite3.connect(":memory:")

    all_rows = []
    all_rows += bench_sqlite_core(
        args.csv, args.csv_multi, args.csv_trades, args.warmup, args.iters, con
    )
    if args.csv_multi:
        all_rows += bench_sqlite_reshape(
            args.warmup, args.iters, args.reshape_rows, con
        )
    if args.csv_events:
        all_rows += bench_sqlite_events(args.csv_events, args.warmup, args.iters, con)
    if args.csv_lookup:
        all_rows += bench_sqlite_null(
            args.csv, args.csv_lookup, args.warmup, args.iters, con
        )
    all_rows += bench_sqlite_fill(args.fill_rows, args.warmup, args.iters, con)

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
