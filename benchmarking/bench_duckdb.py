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


def _prices_ts_path(csv_path):
    """prices_ts.csv beside the prices CSV, or None when absent."""
    p = pathlib.Path(csv_path)
    if p.name == "prices.csv":
        cand = p.with_name("prices_ts.csv")
        if cand.exists():
            return str(cand)
    return None


def bench_duckdb_core(csv_path, csv_multi_path, csv_trades_path, warmup, iters, con):
    print("duckdb: loading...", file=sys.stderr, flush=True)
    con.execute(f"CREATE OR REPLACE TABLE prices AS SELECT * FROM read_csv_auto('{csv_path}')")
    pts_path = _prices_ts_path(csv_path)
    if pts_path is not None:
        con.execute(
            f"CREATE OR REPLACE TABLE prices_ts AS SELECT * FROM read_csv_auto('{pts_path}')"
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
        "sort_price_desc",
        lambda: con.sql("SELECT * FROM prices ORDER BY price DESC").fetchnumpy(),
    )

    run(
        "sort_symbol",
        lambda: con.sql("SELECT * FROM prices ORDER BY symbol ASC").fetchnumpy(),
    )

    run(
        "sort_symbol_price",
        lambda: con.sql(
            "SELECT * FROM prices ORDER BY symbol ASC, price ASC"
        ).fetchnumpy(),
    )

    run(
        "sort_symbol_price_desc",
        lambda: con.sql(
            "SELECT * FROM prices ORDER BY symbol ASC, price DESC"
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

    # Cumulative product via exp(cumsum(ln)) — DuckDB has no PRODUCT window.
    run(
        "cumprod_price",
        lambda: con.sql(
            "SELECT *, EXP(SUM(LN(price)) OVER ("
            "ORDER BY rowid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            ")) AS cp FROM prices"
        ).fetchnumpy(),
    )

    # Grouped window functions (partition by symbol).
    run(
        "rank_by_symbol",
        lambda: con.sql(
            "SELECT *, DENSE_RANK() OVER ("
            "PARTITION BY symbol ORDER BY price DESC) AS rk FROM prices"
        ).fetchnumpy(),
    )

    run(
        "lag_by_symbol",
        lambda: con.sql(
            "SELECT *, LAG(price, 1) OVER ("
            "PARTITION BY symbol ORDER BY rowid) AS prev FROM prices"
        ).fetchnumpy(),
    )

    run(
        "cumsum_by_symbol",
        lambda: con.sql(
            "SELECT *, SUM(price) OVER ("
            "PARTITION BY symbol ORDER BY rowid "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cs FROM prices"
        ).fetchnumpy(),
    )

    # Expensive group aggregates (by symbol): median, p90, sample stddev.
    run(
        "median_by_symbol",
        lambda: con.sql(
            "SELECT symbol, MEDIAN(price) AS med FROM prices GROUP BY symbol"
        ).fetchnumpy(),
    )

    run(
        "quantile_by_symbol",
        lambda: con.sql(
            "SELECT symbol, QUANTILE_CONT(price, 0.9) AS p90 FROM prices GROUP BY symbol"
        ).fetchnumpy(),
    )

    run(
        "std_by_symbol",
        lambda: con.sql(
            "SELECT symbol, STDDEV_SAMP(price) AS sd FROM prices GROUP BY symbol"
        ).fetchnumpy(),
    )

    # Multi-stage pipeline: filter → group-by → order → head.
    run(
        "filter_group_sort",
        lambda: con.sql(
            "SELECT symbol, AVG(price) AS avg FROM prices WHERE price > 500.0 "
            "GROUP BY symbol ORDER BY avg DESC LIMIT 10"
        ).fetchnumpy(),
    )

    # update by → filter on derived column → re-aggregate.
    run(
        "update_group_filter",
        lambda: con.sql(
            "WITH lr AS (SELECT symbol, "
            "LN(price / LAG(price, 1) OVER (PARTITION BY symbol ORDER BY rowid)) AS lr "
            "FROM prices) "
            "SELECT symbol, COUNT(lr) AS pos_days FROM lr WHERE lr > 0.0 GROUP BY symbol"
        ).fetchnumpy(),
    )

    # rank within group → top-N per group → aggregate survivors.
    run(
        "group_rank_filter",
        lambda: con.sql(
            "WITH ranked AS (SELECT *, DENSE_RANK() OVER ("
            "PARTITION BY symbol ORDER BY price DESC) AS rk FROM prices) "
            "SELECT symbol, AVG(price) AS avg_top10 FROM ranked WHERE rk <= 10 GROUP BY symbol"
        ).fetchnumpy(),
    )

    # grouped z-score → clip to ±3 → re-aggregate.
    run(
        "normalize_by_group",
        lambda: con.sql(
            "WITH z AS (SELECT symbol, "
            "(price - AVG(price) OVER (PARTITION BY symbol)) "
            "/ STDDEV_SAMP(price) OVER (PARTITION BY symbol) AS z FROM prices), "
            "clipped AS (SELECT symbol, LEAST(GREATEST(z, -3.0), 3.0) AS clipped FROM z) "
            "SELECT symbol, AVG(clipped) AS mean_z, STDDEV_SAMP(clipped) AS sd_z "
            "FROM clipped GROUP BY symbol"
        ).fetchnumpy(),
    )

    # Tier 3 funnel on the timestamped table: log returns → 5-minute time-windowed
    # momentum → Sharpe-like ratio per symbol. ts is int64 ns, so the 5-minute
    # window is RANGE 300000000000 PRECEDING (inclusive both ends — matches ibex's
    # closed-both window); first return coalesced to 0; STDDEV_SAMP = ddof=1.
    if pts_path is not None:
        run(
            "log_return_momentum",
            lambda: con.sql(
                "WITH base AS (SELECT symbol, ts, "
                "COALESCE(LN(price / LAG(price, 1) OVER ("
                "PARTITION BY symbol ORDER BY ts)), 0.0) AS lr FROM prices_ts), "
                "mom AS (SELECT symbol, AVG(lr) OVER (PARTITION BY symbol ORDER BY ts "
                "RANGE BETWEEN 300000000000 PRECEDING AND CURRENT ROW) AS mom FROM base) "
                "SELECT symbol, AVG(mom) AS mean_mom, STDDEV_SAMP(mom) AS std_mom, "
                "AVG(mom) / STDDEV_SAMP(mom) AS sharpe FROM mom GROUP BY symbol"
            ).fetchnumpy(),
        )

    # Transforms / single-pass language features.
    run(
        "pmin_clip",
        lambda: con.sql("SELECT *, LEAST(price, 500.0) AS clipped FROM prices").fetchnumpy(),
    )
    run(
        "where_update_clip",
        lambda: con.sql(
            "SELECT symbol, CASE WHEN price > 900.0 THEN 900.0 ELSE price END AS price "
            "FROM prices"
        ).fetchnumpy(),
    )
    run(
        "rbind_two",
        lambda: con.sql(
            "SELECT * FROM prices UNION ALL SELECT * FROM prices"
        ).fetchnumpy(),
    )

    run(
        "rand_uniform",
        lambda: con.sql(
            "SELECT *, random() AS r FROM prices"
        ).fetchnumpy(),
    )

    # Normal deviates via Box-Muller (DuckDB has no native normal RNG).
    run(
        "rand_normal",
        lambda: con.sql(
            "SELECT *, sqrt(-2.0 * LN(random())) * cos(2.0 * pi() * random()) AS n "
            "FROM prices"
        ).fetchnumpy(),
    )

    run(
        "rand_int",
        lambda: con.sql(
            "SELECT *, CAST(floor(random() * 100) AS BIGINT) + 1 AS r FROM prices"
        ).fetchnumpy(),
    )

    run(
        "rand_bernoulli",
        lambda: con.sql(
            "SELECT *, CAST(random() < 0.3 AS INTEGER) AS r FROM prices"
        ).fetchnumpy(),
    )

    # Scalar row-wise math builtins.
    for _name, _sql in (
        ("abs_price", "SELECT *, abs(price) AS v FROM prices"),
        ("sqrt_price", "SELECT *, sqrt(price) AS v FROM prices"),
        ("log_price", "SELECT *, ln(price) AS v FROM prices"),
        ("exp_price", "SELECT *, exp(price / 1000.0) AS v FROM prices"),
        ("round_price", "SELECT *, CAST(round(price) AS BIGINT) AS v FROM prices"),
        ("floor_price", "SELECT *, floor(price) AS v FROM prices"),
        ("ceil_price", "SELECT *, ceil(price) AS v FROM prices"),
        ("sin_price", "SELECT *, sin(price) AS v FROM prices"),
        ("cos_price", "SELECT *, cos(price) AS v FROM prices"),
        ("tanh_price", "SELECT *, tanh(price / 1000.0) AS v FROM prices"),
    ):
        run(_name, lambda sql=_sql: con.sql(sql).fetchnumpy())

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

        # Two-level rollup (funnel): by {symbol, day} then re-aggregate by symbol.
        run(
            "symbol_day_to_symbol",
            lambda: con.sql(
                "WITH daily AS (SELECT symbol, day, AVG(price) AS daily_mean, "
                "STDDEV_SAMP(price) AS daily_vol FROM prices_multi GROUP BY symbol, day) "
                "SELECT symbol, AVG(daily_mean) AS mean_of_means, AVG(daily_vol) AS mean_vol "
                "FROM daily GROUP BY symbol"
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

        # Correlation over the numeric columns (price, qty).
        run(
            "corr_price_vol",
            lambda: con.sql("SELECT CORR(price, qty) AS corr FROM trades").fetchnumpy(),
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
    # Low-cardinality inner join: prices ⋈ lookup on symbol.
    run(
        "inner_join_symbol",
        lambda: con.sql(
            "SELECT * FROM prices JOIN lookup USING (symbol)"
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

    # Typed-pivot variants: integer pivot key, and a categorical (ENUM) key.
    # Same value matrix as dcast_long_to_wide; measures pivot cost by key dtype.
    con.execute(
        "CREATE OR REPLACE TABLE long_int AS SELECT symbol, day, value, "
        "CASE variable WHEN 'open' THEN 0 WHEN 'high' THEN 1 WHEN 'low' THEN 2 "
        "ELSE 3 END AS pivot_id FROM long_tbl"
    )
    run(
        "dcast_long_to_wide_int_pivot",
        lambda: con.sql(
            "PIVOT long_int ON pivot_id USING FIRST(value) GROUP BY symbol, day"
        ).fetchnumpy(),
    )

    con.execute("DROP TABLE IF EXISTS long_cat")
    con.execute("DROP TYPE IF EXISTS measure")
    con.execute("CREATE TYPE measure AS ENUM ('open', 'high', 'low', 'close')")
    con.execute(
        "CREATE TABLE long_cat AS SELECT symbol, day, value, "
        "variable::measure AS pivot_cat FROM long_tbl"
    )
    run(
        "dcast_long_to_wide_cat_pivot",
        lambda: con.sql(
            "PIVOT long_cat ON pivot_cat USING FIRST(value) GROUP BY symbol, day"
        ).fetchnumpy(),
    )

    return rows


def bench_duckdb_events(csv_events_path, warmup, iters, con, csv_users_path=None):
    print("duckdb: loading events...", file=sys.stderr, flush=True)
    con.execute(
        f"CREATE OR REPLACE TABLE events AS SELECT * FROM read_csv_auto('{csv_events_path}')"
    )
    if csv_users_path:
        con.execute(
            f"CREATE OR REPLACE TABLE users AS SELECT * FROM read_csv_auto('{csv_users_path}')"
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

    # High-cardinality inner join: events ⋈ users on user_id (~100K keys).
    if csv_users_path:
        run(
            "inner_join_user",
            lambda: con.sql(
                "SELECT * FROM events JOIN users USING (user_id)"
            ).fetchnumpy(),
        )

        # Join-anchored pipelines (Tier 2): join → derive → roll up.
        run(
            "join_update_group",
            lambda: con.sql(
                "WITH j AS (SELECT e.symbol, u.user_segment, "
                "e.amount * u.user_tier_multiplier AS revenue "
                "FROM events e JOIN users u USING (user_id)) "
                "SELECT symbol, user_segment, SUM(revenue) AS total_rev "
                "FROM j GROUP BY symbol, user_segment"
            ).fetchnumpy(),
        )
        run(
            "join_filter_rank",
            lambda: con.sql(
                "WITH j AS (SELECT * FROM events JOIN users USING (user_id) "
                "WHERE user_segment = 'premium'), "
                "r AS (SELECT *, DENSE_RANK() OVER ("
                "PARTITION BY symbol ORDER BY amount DESC) AS rk FROM j) "
                "SELECT * FROM r WHERE rk <= 5"
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
    ap.add_argument("--csv-users", help="Path to users.csv (inner-join dimension)")
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
        all_rows += bench_duckdb_events(
            args.csv_events, args.warmup, args.iters, con, args.csv_users
        )
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
