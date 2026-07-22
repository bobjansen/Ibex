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

def gen_data(rows: int, nsym: int) -> Path:
    """Generate (or reuse) a Parquet tick file with `rows` rows and `nsym`
    symbols. Deterministic via seed_rng(42)."""
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f"ticks_r{rows}_s{nsym}.parquet"
    if path.exists():
        return path
    symbols = ",".join(f"S{i}" for i in range(nsym))
    script = HERE / "data" / f"_gen_r{rows}_s{nsym}.ibex"
    script.write_text(
        'import data_gen;\n'
        'extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";\n'
        'seed_rng(42);\n'
        f'let t = gen_ticks({rows}, "{symbols}");\n'
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
    return ('t[ select { price = price, open = first(price), close = last(price)'
            f'{ws} }}, by symbol, {win}, order symbol ];')

def bench_ibex(parquet: Path, window: str, iters: int) -> tuple[float, float]:
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

    def drain(min_times, timeout):
        """Read until at least `min_times` `time:` lines are present, or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            r, _, _ = select.select([fd], [], [], 1.0)
            if r:
                nonlocal buf
                buf += os.read(fd, 65536)
            if len(time_re.findall(buf.decode(errors='replace'))) >= min_times:
                return

    for line in setup:
        os.write(fd, (line + "\n").encode())
    drain(0, 5)                            # setup emits no `time:` lines
    for i in range(1, iters + 2):          # iters + 1 warmup
        os.write(fd, (q + "\n").encode())
        drain(i, 300)
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
    times = sorted(times[1:])              # drop warmup
    return times[0], times[len(times) // 2]

# ── Polars (eager, in-memory, materialised) ──────────────────────────────────

def bench_polars(parquet: Path, window: str, iters: int) -> tuple[float, float]:
    import polars as pl
    df = pl.read_parquet(parquet)          # preloaded, not timed
    if window == "aligned":
        def q():
            return (df.with_columns(pl.col("timestamp").dt.truncate(WINDOW).alias("window_start"))
                      .with_columns(pl.col("price").first().over(["symbol", "window_start"]).alias("open"),
                                    pl.col("price").alias("close"))
                      .sort("symbol"))
    else:
        def q():
            agg = (df.rolling(index_column="timestamp", period=WINDOW, group_by="symbol")
                     .agg(open=pl.col("price").first(), close=pl.col("price").last()))
            return df.join(agg, on=["symbol", "timestamp"], how="left").sort("symbol")
    return _timeit(q, iters)

# ── DuckDB (in-memory table, materialised to Arrow) ──────────────────────────

def bench_duckdb(parquet: Path, window: str, iters: int, threads: int) -> tuple[float, float]:
    import duckdb
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet}')")
    if window == "aligned":
        sql = f"""
        SELECT timestamp, symbol, price,
               first_value(price) OVER w AS open, price AS close,
               time_bucket(INTERVAL '{WINDOW}', timestamp) AS window_start
        FROM t
        WINDOW w AS (PARTITION BY symbol, time_bucket(INTERVAL '{WINDOW}', timestamp)
                     ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ORDER BY symbol"""
    else:
        sql = f"""
        SELECT timestamp, symbol, price,
               first_value(price) OVER w AS open, price AS close
        FROM t
        WINDOW w AS (PARTITION BY symbol ORDER BY timestamp
                     RANGE BETWEEN INTERVAL '{WINDOW}' PRECEDING AND CURRENT ROW)
        ORDER BY symbol"""
    fetch = getattr(duckdb.DuckDBPyConnection, "to_arrow_table", None)
    q = (lambda: con.execute(sql).to_arrow_table()) if fetch else \
        (lambda: con.execute(sql).fetch_arrow_table())
    return _timeit(q, iters)

# ── shared timing helper ─────────────────────────────────────────────────────

def _timeit(fn, iters: int) -> tuple[float, float]:
    ts = []
    for _ in range(iters + 1):             # +1 warmup
        t0 = time.perf_counter(); fn(); ts.append((time.perf_counter() - t0) * 1000)
    ts = sorted(ts[1:])
    return ts[0], ts[len(ts) // 2]

# ── orchestration ────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, nargs="+", default=[100_000])
    ap.add_argument("--symbols", type=int, nargs="+", default=[3])
    ap.add_argument("--window", nargs="+", default=["aligned"], choices=["aligned", "sliding"])
    ap.add_argument("--iters", type=int, default=9)
    ap.add_argument("--duckdb-threads", type=int, default=8)
    ap.add_argument("--threads", choices=["auto", "1"], default="auto",
                    help="'1' = single-threaded (per-core / apples-to-apples): "
                         "POLARS_MAX_THREADS=1 and DuckDB threads=1. 'auto' = each "
                         "engine's default parallelism (DuckDB uses --duckdb-threads).")
    ap.add_argument("--engines", nargs="+", default=["ibex", "polars", "duckdb"])
    ap.add_argument("--out", type=Path, default=HERE / "results" / "results.tsv")
    args = ap.parse_args()

    # Polars fixes its thread pool at import time, so the env var must be set
    # before bench_polars() triggers the first `import polars`. main() runs
    # before any such import, so setting it here is honoured.
    if args.threads == "1":
        os.environ["POLARS_MAX_THREADS"] = "1"
        duck_threads = 1
        label = "1t"        # single-threaded / per-core
    else:
        duck_threads = args.duckdb_threads
        label = "mt"        # multi-threaded (each engine's default)
    # Ibex is single-threaded regardless, so its `threads` tag is always "1t".

    args.out.parent.mkdir(parents=True, exist_ok=True)
    rows_out = ["engine\tthreads\trows\tsymbols\twindow\tmin_ms\tmedian_ms"]
    print(f"{'engine':7} {'thr':>4} {'rows':>9} {'sym':>4} {'window':8} {'min_ms':>9} {'med_ms':>9}",
          file=sys.stderr)
    for rows in args.rows:
        for nsym in args.symbols:
            pq = gen_data(rows, nsym)
            for window in args.window:
                for engine in args.engines:
                    if engine == "ibex":
                        mn, md = bench_ibex(pq, window, args.iters)
                        tag = "1t"
                    elif engine == "polars":
                        mn, md = bench_polars(pq, window, args.iters)
                        tag = label
                    elif engine == "duckdb":
                        mn, md = bench_duckdb(pq, window, args.iters, duck_threads)
                        tag = label
                    else:
                        continue
                    rows_out.append(f"{engine}\t{tag}\t{rows}\t{nsym}\t{window}\t{mn:.3f}\t{md:.3f}")
                    print(f"{engine:7} {tag:>4} {rows:>9} {nsym:>4} {window:8} {mn:>9.3f} {md:>9.3f}",
                          file=sys.stderr)
    args.out.write_text("\n".join(rows_out) + "\n")
    print(f"\nwrote {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
