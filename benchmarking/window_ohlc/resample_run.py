#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Resample benchmark: one OHLCV bar per 10-second bucket per symbol.

The companion to `run.py`, which computes a RUNNING bar state at every tick
(one output row per input row). This computes finished bars -- one row per
bucket per symbol -- and that difference is the point. It is a much friendlier
shape for the competition: Polars gets `group_by_dynamic().agg()`, DuckDB and
ClickHouse get a plain GROUP BY, instead of the per-row window formulations the
running variant forces on them. Expect the gap to narrow. Publishing the narrow
number next to the wide one is what makes the wide one believable.

No ordering clause here. Output cardinality is buckets x symbols, i.e. tiny, so
a sort would measure nothing; `run.py`'s `order symbol` exists because a
downstream per-symbol stage wants a tall frame grouped, which does not apply.

Usage:
  uv run --project /home/brj/ibex benchmarking/window_ohlc/resample_run.py --verify
  uv run --project /home/brj/ibex benchmarking/window_ohlc/resample_run.py \
      --rows 5000000 --symbols 3 100 --out results/resample.tsv
"""
from __future__ import annotations
import argparse
import importlib.util
import io
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("ohlc_run", HERE / "run.py")
RUN = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(RUN)

WINDOW = RUN.WINDOW          # "10s"
WINDOW_SECS = 10
# Thread budget, applied IDENTICALLY to all four engines. Not a tuning knob --
# a fairness invariant. The first version of this suite inherited the window
# suite's `--duckdb-threads 8` for DuckDB and ClickHouse while Polars and Ibex
# took the whole box, i.e. it handicapped two engines by a factor of three and
# reported the result as a comparison.
#
# NOTE on this dev box: it is an i7-13700 (8 P-cores + 8 E-cores), but WSL2
# hides the distinction -- every vCPU advertises a 2-way sibling pair, so
# `taskset` pins to vCPUs the hypervisor may place on either core type. Pinning
# constrains the BUDGET reliably; it does not guarantee P-core residency.
THREADS = 8
OHLCV = ["open", "high", "low", "close", "volume_sum"]
KEYS = ["symbol", "window_start"]


# ── queries ──────────────────────────────────────────────────────────────────

IBEX_Q = (f't[ resample {WINDOW}, select {{ open = first(price), high = max(price), '
          'low = min(price), close = last(price), volume_sum = sum(volume) }, '
          'by symbol ];')

# arg_min/arg_max rather than `first(price ORDER BY timestamp)`: same answer,
# without asking the engine for an ordered aggregate it would have to sort for.
DUCK_SQL = f"""
    SELECT symbol,
           time_bucket(INTERVAL '{WINDOW}', timestamp) AS window_start,
           arg_min(price, timestamp) AS open,
           max(price)                AS high,
           min(price)                AS low,
           arg_max(price, timestamp) AS close,
           sum(volume)               AS volume_sum
    FROM t
    GROUP BY symbol, window_start"""

CH_SQL = f"""
    SELECT symbol,
           toStartOfInterval(timestamp, INTERVAL {WINDOW_SECS} second) AS window_start,
           argMin(price, timestamp) AS open,
           max(price)               AS high,
           min(price)               AS low,
           argMax(price, timestamp) AS close,
           sum(volume)              AS volume_sum
    FROM t
    GROUP BY symbol, window_start"""


# ── engines ──────────────────────────────────────────────────────────────────

def bench_ibex(parquet: Path, iters: int, budget_s: float):
    RUN._ibex_query = lambda _w: IBEX_Q           # bench_ibex looks it up by name
    return RUN.bench_ibex(parquet, "aligned", iters, budget_s)


def _polars_query(df):
    import polars as pl
    return (df.group_by_dynamic("timestamp", every=WINDOW, group_by="symbol")
              .agg(open=pl.col("price").first(), high=pl.col("price").max(),
                   low=pl.col("price").min(), close=pl.col("price").last(),
                   volume_sum=pl.col("volume").sum()))


def bench_polars(parquet: Path, iters: int, budget_s: float):
    import polars as pl  # noqa: PLC0415 -- must follow POLARS_MAX_THREADS
    df = pl.read_parquet(parquet)                 # preloaded, not timed
    return RUN._timeit(lambda: _polars_query(df), iters, budget_s)


def bench_duckdb(parquet: Path, iters: int, budget_s: float):
    import duckdb
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={THREADS}")
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet}')")
    fetch = getattr(duckdb.DuckDBPyConnection, "to_arrow_table", None)
    q = (lambda: con.execute(DUCK_SQL).to_arrow_table()) if fetch else \
        (lambda: con.execute(DUCK_SQL).fetch_arrow_table())
    return RUN._timeit(q, iters, budget_s)


def _ch_session(parquet: Path, threads: int = 0):
    from chdb.session import Session
    sess = Session()
    sess.query(f"SET max_threads = {threads or THREADS}")
    sess.query("CREATE OR REPLACE TABLE t ENGINE = Memory AS "
               f"SELECT * FROM file('{parquet}', Parquet)")
    return sess


def _ch_arrow(sess):
    """Run CH_SQL and materialise it as Arrow.

    ArrowStream, not TabSeparated: the methodology times full materialisation,
    and counting rows would let ClickHouse skip the part every other engine
    pays for. Note "Arrow" is the FILE format (footer; needs `open_file`) --
    the streaming one is what the others' `to_arrow_table()` compares to.
    """
    import pyarrow as pa
    return pa.ipc.open_stream(io.BytesIO(sess.query(CH_SQL, "ArrowStream").bytes())).read_all()


def bench_clickhouse(parquet: Path, iters: int, budget_s: float):
    sess = _ch_session(parquet)
    try:
        return RUN._timeit(lambda: _ch_arrow(sess), iters, budget_s)
    finally:
        sess.close()


ENGINES = {"ibex": bench_ibex, "polars": bench_polars,
           "duckdb": bench_duckdb, "clickhouse": bench_clickhouse}


# ── cross-engine verification ────────────────────────────────────────────────
# A benchmark that reports four timings for four different answers is worth
# nothing. This recomputes the bars in every engine and compares them to Ibex.

def verify(parquet: Path, tol: float = 1e-9) -> list[str]:
    """Return human-readable mismatches; empty means every engine agrees."""
    import duckdb
    import polars as pl

    problems: list[str] = []

    out = RUN.DATA_DIR / "_verify_resample.parquet"
    script = RUN.DATA_DIR / "_verify_resample.ibex"
    script.write_text(
        'extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";\n'
        'extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";\n'
        f'let t = as_timeframe(read_parquet("{parquet}"), "timestamp");\n'
        f'let r = {IBEX_Q[:-1]};\n'
        f'write_parquet(r, "{out}");\n')
    proc = subprocess.run([str(RUN.IBEX_BIN), str(script)], capture_output=True, text=True)
    script.unlink(missing_ok=True)
    if proc.returncode != 0:
        return [f"ibex failed: {proc.stderr.strip()[:400]}"]

    ib = pl.read_parquet(out)
    # Ibex labels each bar with the time index; name it like the others.
    tcol = "window_start" if "window_start" in ib.columns else ib.columns[0]
    ib = ib.rename({tcol: "window_start"}).sort(KEYS)

    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet}')")
    dk = pl.from_arrow(con.execute(DUCK_SQL).arrow()).sort(KEYS)

    sess = _ch_session(parquet)
    ch = pl.from_arrow(_ch_arrow(sess)).sort(KEYS)
    sess.close()

    pol = (_polars_query(pl.read_parquet(parquet))
           .rename({"timestamp": "window_start"}).sort(KEYS))

    for name, other in (("polars", pol), ("duckdb", dk), ("clickhouse", ch)):
        if other.height != ib.height:
            problems.append(f"{name}: {other.height} rows vs ibex {ib.height}")
            continue
        for col in OHLCV:
            if col not in other.columns:
                problems.append(f"{name}: missing column {col}")
                continue
            a, b = ib[col].cast(pl.Float64), other[col].cast(pl.Float64)
            bad = int(((a - b).abs() > tol).sum())
            if bad:
                problems.append(f"{name}.{col}: {bad}/{ib.height} rows differ")
    return problems


# ── orchestration ────────────────────────────────────────────────────────────

def main() -> None:
    global THREADS          # declared first: `--threads` defaults to it below
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, nargs="+", default=[5_000_000])
    ap.add_argument("--symbols", type=int, nargs="+", default=[3, 100])
    ap.add_argument("--engines", nargs="+", default=list(ENGINES))
    ap.add_argument("--iters", type=int, default=5)
    ap.add_argument("--budget-s", type=float, default=120.0)
    ap.add_argument("--threads", type=int, default=THREADS,
                    help="thread budget given to EVERY engine (fairness invariant)")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--out", type=Path, default=HERE / "results" / "resample.tsv")
    args = ap.parse_args()

    # Polars fixes its pool at import time and Ibex reads its own env, so both
    # must be set before any engine module loads or the query runs.
    THREADS = args.threads
    os.environ["POLARS_MAX_THREADS"] = str(THREADS)
    os.environ["IBEX_THREADS"] = str(THREADS)
    print(f"# thread budget: {THREADS} for every engine", file=sys.stderr)

    if args.verify:
        failures = []
        for rows in args.rows:
            for nsym in args.symbols:
                pq = RUN.gen_data(rows, nsym)
                problems = verify(pq)
                tag = f"{rows} rows x {nsym} symbols"
                if problems:
                    failures += problems
                    for p in problems:
                        print(f"MISMATCH  {tag}: {p}", file=sys.stderr)
                else:
                    print(f"ok        {tag}: all engines agree on " + ", ".join(OHLCV),
                          file=sys.stderr)
        sys.exit(1 if failures else 0)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    out_rows = ["engine\tthreads\trows\tsymbols\tmin_ms\tmedian_ms\tstatus"]
    print(f"{'engine':11} {'rows':>9} {'sym':>4} {'min_ms':>9} {'med_ms':>9} status",
          file=sys.stderr)
    for rows in args.rows:
        for nsym in args.symbols:
            pq = RUN.gen_data(rows, nsym)
            for eng in args.engines:
                mn, md, over = ENGINES[eng](pq, args.iters, args.budget_s)
                status = "over_budget" if over else "ok"
                print(f"{eng:11} {rows:>9} {nsym:>4} {mn:9.1f} {md:9.1f} {status}",
                      file=sys.stderr)
                out_rows.append(f"{eng}\t{THREADS}t\t{rows}\t{nsym}\t{mn:.3f}\t{md:.3f}\t{status}")
    args.out.write_text("\n".join(out_rows) + "\n")
    print(f"\nwrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
