#!/usr/bin/env python3
"""Window-OHLC benchmark suite: Ibex vs Polars vs DuckDB.

Computes a rolling open/close per time window, grouped by symbol, ordered by
symbol -- the query shape:

    t[ select { price, open = first(price), close = last(price),
                window_start = window_start() },
       by symbol, window <W> [aligned], order symbol ]

Two window flavours:
  * aligned  -- tumbling buckets snapped to the epoch grid (window ... aligned)
  * sliding  -- trailing window ending at each row               (window ...)

All engines read the SAME Parquet file (generated once by Ibex's data_gen
plugin so the data is identical and reproducible via seed_rng). Input is
pre-loaded into memory; only compute + full materialisation is timed. We drop
one warmup iteration and report the MIN (least noisy) and median.

Usage:
  uv run --project <ibex-root> benchmarking/window_ohlc/run.py \
      --rows 100000 1000000 --symbols 3 --window aligned sliding \
      --out benchmarking/window_ohlc/results/results.tsv

Environment:
  IBEX_ROOT  repo root       (default: two levels up from this file)
  BUILD_DIR  cmake build dir (default: $IBEX_ROOT/build-release)
"""
from __future__ import annotations
import argparse, os, pty, re, select, subprocess, sys, time, statistics as st
from pathlib import Path

HERE = Path(__file__).resolve().parent
IBEX_ROOT = Path(os.environ.get("IBEX_ROOT", HERE.parent.parent))
BUILD_DIR = Path(os.environ.get("BUILD_DIR", IBEX_ROOT / "build-release"))
IBEX_BIN = BUILD_DIR / "tools" / "ibex"
WINDOW = "10s"          # window width, shared by every engine
WINDOW_NS = 10_000_000_000
DATA_DIR = HERE / "data"

# ── data generation (Ibex data_gen -> shared Parquet) ────────────────────────

# Mean gap between consecutive ticks, in milliseconds, across ALL symbols.
#
# Pinned rather than left to `gen_ticks`'s own default, which is 1000.0 -- one
# tick per SECOND for the whole feed. At that rate a 5M-row/100-symbol file
# spans 57.8 days, a 10-second bar holds 1.1 ticks, and `resample` produces
# 4.76M groups from 5M rows: an identity operation wearing the name of an
# aggregation. Real feeds are three to four orders of magnitude denser.
#
# At 1ms, 5M ticks span ~83 minutes, and a 10-second bar holds ~100 ticks at
# 100 symbols or ~3300 at 3 -- bars with contents, which is what the suite
# claims to measure. The value is in the filename because a cached file
# generated at another density is a DIFFERENT benchmark, not a reusable one.
TICK_INTERVAL_MS = 1.0


def gen_data(rows: int, nsym: int, interval_ms: float = TICK_INTERVAL_MS) -> Path:
    """Generate (or reuse) a Parquet tick file with `rows` rows and `nsym`
    symbols. Deterministic via seed_rng(42)."""
    DATA_DIR.mkdir(exist_ok=True)
    tag = f"r{rows}_s{nsym}_i{interval_ms:g}"
    path = DATA_DIR / f"ticks_{tag}.parquet"
    if path.exists():
        return path
    symbols = ",".join(f"S{i}" for i in range(nsym))
    script = HERE / "data" / f"_gen_{tag}.ibex"
    script.write_text(
        'import data_gen;\n'
        'extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";\n'
        'seed_rng(42);\n'
        f'let t = gen_ticks({rows}, "{symbols}", 100.0, 0.5, {interval_ms}, 0);\n'
        f'write_parquet(t, "{path}");\n'
    )
    subprocess.run([str(IBEX_BIN), str(script)], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    script.unlink(missing_ok=True)
    return path

# ── Ibex (driven through a pty so the REPL prints `:timing`) ──────────────────

def _ibex_query(window: str) -> str:
    win = f"window {WINDOW} aligned" if window == "aligned" else f"window {WINDOW}"
    ws = ", window_start = window_start()" if window == "aligned" else ""
    return ('t[ select { price = price, open = first(price), high = max(price), '
            'low = min(price), close = last(price), volume_sum = sum(volume)'
            f'{ws} }}, by symbol, {win}, order symbol ];')

def bench_ibex(parquet: Path, window: str, iters: int,
               budget_s: float) -> tuple[float, float, bool]:
    q = _ibex_query(window)
    setup = [
        'extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";',
        f'let t = as_timeframe(read_parquet("{parquet}"), "timestamp");',
        ':timing on',
    ]
    pid, fd = pty.fork()
    if pid == 0:
        os.execv(str(IBEX_BIN), ["ibex", "--no-history"])
    buf = b""
    time_re = re.compile(r'time:\s*[\d.]+\s*(?:ms|us|s)\b')

    def drain(min_times, timeout_s: float | None):
        """Read until at least `min_times` `time:` lines are present.

        `None` means no deadline. This is distinct from the normal benchmark
        budget: `--budget-s 0` promises to disable that budget rather than
        silently replacing it with the old five-minute REPL timeout.
        """
        deadline = None if timeout_s is None else time.time() + timeout_s
        while deadline is None or time.time() < deadline:
            r, _, _ = select.select([fd], [], [], 1.0)
            if r:
                nonlocal buf
                buf += os.read(fd, 65536)
            if len(time_re.findall(buf.decode(errors='replace'))) >= min_times:
                return

    for line in setup:
        os.write(fd, (line + "\n").encode())
    drain(0, 5)                            # setup emits no `time:` lines
    over = False
    for i in range(1, iters + 2):          # iters + 1 warmup
        t0 = time.perf_counter()
        os.write(fd, (q + "\n").encode())
        # The drain timeout IS the budget: waiting longer than we are willing to
        # spend on the whole execution only delays the abort.
        drain(i, budget_s if budget_s else None)
        if budget_s and time.perf_counter() - t0 > budget_s:
            over = True
            break
    os.write(fd, b"\x04")
    try:
        while True:
            r, _, _ = select.select([fd], [], [], 0.5)
            if not r:
                break
            buf += os.read(fd, 65536)
    except OSError:
        pass
    os.close(fd)
    mult = {"s": 1000.0, "ms": 1.0, "us": 0.001}
    times = [float(v) * mult[u] for v, u in
             re.findall(r'time:\s*([\d.]+)\s*(ms|us|s)\b', buf.decode(errors='replace'))]
    # Normally drop the warmup; an aborted run may have produced only the
    # warmup, and reporting that is better than reporting nothing.
    vals = sorted(times[1:]) if len(times) > 1 else sorted(times)
    if not vals:
        return float("inf"), float("inf"), True
    return vals[0], vals[len(vals) // 2], over

# ── Polars (eager, in-memory, materialised) ──────────────────────────────────

def bench_polars(parquet: Path, window: str, iters: int,
                 budget_s: float) -> tuple[float, float, bool]:
    import polars as pl
    df = pl.read_parquet(parquet)          # preloaded, not timed
    part = ["symbol", "window_start"]
    if window == "aligned":
        # The frame is expanding WITHIN each bucket (equivalently, DuckDB's
        # ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), so high/low/volume
        # are cumulative, not whole-bucket. `.max().over()` here would compute a
        # different -- and cheaper -- answer than the other two engines.
        def q():
            return (df.with_columns(pl.col("timestamp").dt.truncate(WINDOW).alias("window_start"))
                      .with_columns(pl.col("price").first().over(part).alias("open"),
                                    pl.col("price").cum_max().over(part).alias("high"),
                                    pl.col("price").cum_min().over(part).alias("low"),
                                    pl.col("price").alias("close"),
                                    pl.col("volume").cum_sum().over(part).alias("volume_sum"))
                      .sort("symbol"))
    else:
        def q():
            agg = (df.rolling(index_column="timestamp", period=WINDOW, group_by="symbol")
                     .agg(open=pl.col("price").first(), high=pl.col("price").max(),
                          low=pl.col("price").min(), close=pl.col("price").last(),
                          volume_sum=pl.col("volume").sum()))
            return df.join(agg, on=["symbol", "timestamp"], how="left").sort("symbol")
    return _timeit(q, iters, budget_s)

# ── DuckDB (in-memory table, materialised to Arrow) ──────────────────────────

def _duckdb_sql(window: str) -> str:
    if window == "aligned":
        return f"""
        SELECT timestamp, symbol, price,
               first_value(price) OVER w AS open, max(price) OVER w AS high,
               min(price) OVER w AS low, price AS close,
               sum(volume) OVER w AS volume_sum,
               time_bucket(INTERVAL '{WINDOW}', timestamp) AS window_start
        FROM t
        WINDOW w AS (PARTITION BY symbol, time_bucket(INTERVAL '{WINDOW}', timestamp)
                     ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ORDER BY symbol"""
    return f"""
        SELECT timestamp, symbol, price,
               first_value(price) OVER w AS open, max(price) OVER w AS high,
               min(price) OVER w AS low, price AS close,
               sum(volume) OVER w AS volume_sum
        FROM t
        WINDOW w AS (PARTITION BY symbol ORDER BY timestamp
                     RANGE BETWEEN INTERVAL '{WINDOW}' PRECEDING AND CURRENT ROW)
        ORDER BY symbol"""


def bench_duckdb(parquet: Path, window: str, iters: int, threads: int,
                 budget_s: float) -> tuple[float, float, bool]:
    import duckdb
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet}')")
    sql = _duckdb_sql(window)
    fetch = getattr(duckdb.DuckDBPyConnection, "to_arrow_table", None)
    q = (lambda: con.execute(sql).to_arrow_table()) if fetch else \
        (lambda: con.execute(sql).fetch_arrow_table())
    return _timeit(q, iters, budget_s)

# ── cross-engine verification ────────────────────────────────────────────────
# A benchmark that reports three timings for three different answers is worth
# nothing. This recomputes the OHLCV columns in Polars and DuckDB and compares
# them against Ibex row by row, so "identical output" is a checked claim rather
# than an asserted one.

OHLCV = ["open", "high", "low", "close", "volume_sum"]

def verify_engines(parquet: Path, window: str, tol: float = 1e-9) -> list[str]:
    """Return a list of human-readable mismatches; empty means all three agree."""
    import polars as pl
    import duckdb

    problems: list[str] = []
    # Ibex writes its result out so it can be read back without a pty.
    out = HERE / "data" / f"_verify_{window}.parquet"
    script = HERE / "data" / f"_verify_{window}.ibex"
    script.write_text(
        'extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";\n'
        'extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";\n'
        f'let t = as_timeframe(read_parquet("{parquet}"), "timestamp");\n'
        f'let r = {_ibex_query(window)[:-1]};\n'
        f'write_parquet(r, "{out}");\n'
    )
    subprocess.run([str(IBEX_BIN), str(script)], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    script.unlink(missing_ok=True)

    # Sort every frame the same way; `order symbol` alone is not a total order.
    keys = ["symbol", "timestamp"]
    ibex = pl.read_parquet(out).sort(keys)
    out.unlink(missing_ok=True)

    df = pl.read_parquet(parquet)
    part = ["symbol", "window_start"]
    if window == "aligned":
        pol = (df.with_columns(pl.col("timestamp").dt.truncate(WINDOW).alias("window_start"))
                 .with_columns(pl.col("price").first().over(part).alias("open"),
                               pl.col("price").cum_max().over(part).alias("high"),
                               pl.col("price").cum_min().over(part).alias("low"),
                               pl.col("price").alias("close"),
                               pl.col("volume").cum_sum().over(part).alias("volume_sum")))
    else:
        agg = (df.rolling(index_column="timestamp", period=WINDOW, group_by="symbol")
                 .agg(open=pl.col("price").first(), high=pl.col("price").max(),
                      low=pl.col("price").min(), close=pl.col("price").last(),
                      volume_sum=pl.col("volume").sum()))
        pol = df.join(agg, on=["symbol", "timestamp"], how="left")
    pol = pol.sort(keys)

    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet}')")
    fetch = getattr(duckdb.DuckDBPyConnection, "to_arrow_table", None)
    res = con.execute(_duckdb_sql(window))
    duck = pl.from_arrow(res.to_arrow_table() if fetch else res.fetch_arrow_table()).sort(keys)

    if not (len(ibex) == len(pol) == len(duck)):
        return [f"{window}: row counts differ - "
                f"ibex={len(ibex)} polars={len(pol)} duckdb={len(duck)}"]

    for name, other in (("polars", pol), ("duckdb", duck)):
        for col in OHLCV:
            missing = [w for w, f in (("ibex", ibex), (name, other)) if col not in f.columns]
            if missing:
                problems.append(f"{window}: {', '.join(missing)} missing column {col}")
                continue
            a = ibex[col].cast(pl.Float64)
            b = other[col].cast(pl.Float64)
            bad = int(((a - b).abs() > tol).sum())
            if bad:
                problems.append(f"{window}: {name} differs from ibex in {col} "
                                f"on {bad}/{len(a)} rows")
    return problems

# ── shared timing helper ─────────────────────────────────────────────────────

def _timeit(fn, iters: int, budget_s: float) -> tuple[float, float, bool]:
    """Time `fn` over `iters` iterations plus a warmup; abort if one execution
    exceeds `budget_s`.

    The budget checks the WARMUP too, and that is the point: a cell this slow
    costs `iters + 1` executions to learn something one execution already told
    us. DuckDB's 50M sliding case ran ~35 min per execution, so the full six
    held a finished 5-hour matrix hostage for an uncompetitive number. The
    aborted time is reported as measured and flagged, not dropped -- "slower
    than the budget, here is roughly how much" beats a blank cell.
    """
    ts = []
    for _ in range(iters + 1):             # +1 warmup
        t0 = time.perf_counter(); fn(); ts.append((time.perf_counter() - t0) * 1000)
        if budget_s and ts[-1] > budget_s * 1000:
            return ts[-1], ts[-1], True
    ts = sorted(ts[1:])
    return ts[0], ts[len(ts) // 2], False

# ── orchestration ────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, nargs="+", default=[100_000])
    ap.add_argument("--symbols", type=int, nargs="+", default=[3])
    ap.add_argument("--window", nargs="+", default=["aligned"], choices=["aligned", "sliding"])
    ap.add_argument("--iters", type=int, default=9)
    ap.add_argument("--duckdb-threads", type=int, default=8)
    ap.add_argument("--budget-s", type=float, default=300.0,
                    help="abort a cell once one execution exceeds this many "
                         "seconds, recording it as over_budget (0 = no budget)")
    ap.add_argument("--threads", choices=["auto", "1"], default="auto",
                    help="'1' = single-threaded (per-core / apples-to-apples): "
                         "POLARS_MAX_THREADS=1 and DuckDB threads=1. 'auto' = each "
                         "engine's default parallelism (DuckDB uses --duckdb-threads).")
    ap.add_argument("--engines", nargs="+", default=["ibex", "polars", "duckdb"])
    ap.add_argument("--out", type=Path, default=HERE / "results" / "results.tsv")
    ap.add_argument("--verify", action="store_true",
                    help="check all three engines produce identical OHLCV values, then exit")
    args = ap.parse_args()

    # Polars fixes its thread pool at import time, so the env var must be set
    # before bench_polars() triggers the first `import polars`. main() runs
    # before any such import, so setting it here is honoured.
    if args.threads == "1":
        os.environ["POLARS_MAX_THREADS"] = "1"
        # Ibex runs parallel islands by DEFAULT now, so pinning the other two
        # without pinning it would quietly hand Ibex threads its competitors
        # were denied -- and the headline "single-threaded, Ibex wins at every
        # scale" result was measured back when Ibex had no threads to give.
        os.environ["IBEX_PARALLEL"] = "0"
        duck_threads = 1
        label = "1t"        # single-threaded / per-core
    else:
        duck_threads = args.duckdb_threads
        label = "mt"        # multi-threaded (each engine's default)

    if args.verify:
        failures = []
        for rows in args.rows:
            for nsym in args.symbols:
                pq = gen_data(rows, nsym)
                for window in args.window:
                    problems = verify_engines(pq, window)
                    tag = f"{rows} rows x {nsym} symbols, {window}"
                    if problems:
                        failures.extend(problems)
                        for p in problems:
                            print(f"MISMATCH  {tag}: {p}", file=sys.stderr)
                    else:
                        print(f"ok        {tag}: ibex == polars == duckdb on "
                              + ", ".join(OHLCV), file=sys.stderr)
        sys.exit(1 if failures else 0)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    rows_out = ["engine\tthreads\trows\tsymbols\twindow\tmin_ms\tmedian_ms\tstatus"]
    print(f"{'engine':7} {'thr':>4} {'rows':>9} {'sym':>4} {'window':8} {'min_ms':>9} {'med_ms':>9} status",
          file=sys.stderr)
    for rows in args.rows:
        for nsym in args.symbols:
            pq = gen_data(rows, nsym)
            for window in args.window:
                for engine in args.engines:
                    if engine == "ibex":
                        mn, md, over = bench_ibex(pq, window, args.iters, args.budget_s)
                        tag = label
                    elif engine == "polars":
                        mn, md, over = bench_polars(pq, window, args.iters, args.budget_s)
                        tag = label
                    elif engine == "duckdb":
                        mn, md, over = bench_duckdb(pq, window, args.iters, duck_threads,
                                                    args.budget_s)
                        tag = label
                    else:
                        continue
                    status = "over_budget" if over else "ok"
                    rows_out.append(f"{engine}\t{tag}\t{rows}\t{nsym}\t{window}\t"
                                    f"{mn:.3f}\t{md:.3f}\t{status}")
                    print(f"{engine:7} {tag:>4} {rows:>9} {nsym:>4} {window:8} "
                          f"{mn:>9.3f} {md:>9.3f} {status}", file=sys.stderr)
                    # Rewritten after every cell, not once at the end: a sweep
                    # that stalls or is interrupted still leaves every cell it
                    # finished on disk for the uploader to ship.
                    args.out.write_text("\n".join(rows_out) + "\n")
    print(f"\nwrote {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
