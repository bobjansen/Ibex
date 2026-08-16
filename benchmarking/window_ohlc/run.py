#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

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
WINDOW_SECS = 10
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
        # `rolling(group_by=)` emits one row per input row in (group, time)
        # order, so sorting the frame that way first lets the results be
        # hstacked positionally. The previous formulation joined them back on
        # (symbol, timestamp), which FANS OUT on tied timestamps -- 200,018 rows
        # out of a 200,000-row input, i.e. Polars was not doing the same work as
        # the others. Ties are rare but real at tick density.
        # closed="both" -- polars defaults to "right", i.e. (t-10s, t], while
        # Ibex and DuckDB use [t-10s, t]. On sparse data no tick ever lands
        # exactly on a boundary and the difference is invisible; at tick density
        # it is not.
        base = df.sort(["symbol", "timestamp"])
        def q():
            agg = (base.rolling(index_column="timestamp", period=WINDOW, group_by="symbol",
                               closed="both")
                       .agg(open=pl.col("price").first(), high=pl.col("price").max(),
                            low=pl.col("price").min(), close=pl.col("price").last(),
                            volume_sum=pl.col("volume").sum()))
            # `rolling(group_by=)` does NOT emit in input order, so re-sort it
            # the same way before stacking. A stable sort is required: ties on
            # (symbol, timestamp) are real at tick density, and their relative
            # order is what carries the correspondence to `base`.
            return base.hstack(agg.sort(["symbol", "timestamp"],
                                        maintain_order=True).select(OHLCV))
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


# ── ClickHouse (embedded via chdb, in-memory table, materialised to Arrow) ───

def _clickhouse_sql(window: str) -> str:
    if window == "aligned":
        return f"""
        SELECT timestamp, symbol, price,
               first_value(price) OVER w AS open, max(price) OVER w AS high,
               min(price) OVER w AS low, price AS close,
               sum(volume) OVER w AS volume_sum,
               toStartOfInterval(timestamp, INTERVAL {WINDOW_SECS} second) AS window_start
        FROM t
        WINDOW w AS (PARTITION BY symbol, toStartOfInterval(timestamp, INTERVAL {WINDOW_SECS} second)
                     ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ORDER BY symbol"""
    # ClickHouse rejects a RANGE offset that does not fit in 32 bits, so the
    # 10^10-nanosecond window cannot be written directly, and it rejects an
    # INTERVAL there too ("OFFSET expression must be constant with numeric
    # type"). Ordering by MICROSECONDS puts the offset inside int32 while
    # keeping the frame far finer than the ~1ms tick spacing. It is the closest
    # faithful expression available; `--verify` is what decides whether the
    # rounding changes any answer.
    return f"""
        SELECT timestamp, symbol, price,
               first_value(price) OVER w AS open, max(price) OVER w AS high,
               min(price) OVER w AS low, price AS close,
               sum(volume) OVER w AS volume_sum
        FROM t
        WINDOW w AS (PARTITION BY symbol ORDER BY toUnixTimestamp64Micro(timestamp)
                     RANGE BETWEEN {WINDOW_SECS * 1_000_000} PRECEDING AND CURRENT ROW)
        ORDER BY symbol"""


def _ch_session(parquet: Path, threads: int):
    from chdb.session import Session
    sess = Session()
    sess.query(f"SET max_threads = {threads}")
    sess.query("CREATE OR REPLACE TABLE t ENGINE = Memory AS "
               f"SELECT * FROM file('{parquet}', Parquet)")
    return sess


def _ch_arrow(sess, sql: str):
    """Materialise to Arrow -- counting rows would let ClickHouse skip the work
    every other engine pays for. ArrowStream, not "Arrow": the latter is the
    file format (footer, needs open_file)."""
    import io, pyarrow as pa
    return pa.ipc.open_stream(io.BytesIO(sess.query(sql, "ArrowStream").bytes())).read_all()


def bench_clickhouse(parquet: Path, window: str, iters: int, threads: int,
                     budget_s: float) -> tuple[float, float, bool]:
    sess = _ch_session(parquet, threads)
    sql = _clickhouse_sql(window)
    try:
        return _timeit(lambda: _ch_arrow(sess, sql), iters, budget_s)
    finally:
        sess.close()


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


# Engines whose trailing frame provably differs from Ibex's on some rows.
# Ibex's window is [t - dur, t] and POSITION-bounded: it ends at the current
# row. A SQL `RANGE` frame is peer-inclusive at BOTH ends -- every row sharing
# the current ORDER BY value is in it, including later ones. On top of that,
# DuckDB's INTERVAL arithmetic is microsecond-resolution, and ClickHouse rejects
# a RANGE offset wider than 32 bits so the query orders by microseconds, which
# makes any two ticks inside one microsecond peers.
_FRAME_ENGINES = ("polars", "duckdb", "clickhouse")


def _tied_timestamps(frame):
    """Mask of rows sharing a timestamp with another row of the same symbol.

    `ORDER BY timestamp` is not a total order when timestamps tie, so every
    engine is free to sequence the peers differently -- and `last(price)` or a
    cumulative sum over a prefix then legitimately differs. This affects BOTH
    window flavours, unlike the frame differences below. Ibex's order is the
    table's row order; SQL's is unspecified.

    The clean fix is a tie-breaking row id carried in the data and added to
    every ORDER BY, which would make all four engines deterministic. Until then
    these rows are excluded from the equality claim rather than hidden.
    """
    import numpy as np, polars as pl
    ts = frame["timestamp"].cast(pl.Int64).to_numpy()
    sym = frame["symbol"].to_numpy()
    if ts.size == 0:
        return np.zeros(0, dtype=bool)
    same = (ts[1:] == ts[:-1]) & (sym[1:] == sym[:-1])
    mask = np.zeros(ts.size, dtype=bool)
    mask[:-1] |= same
    mask[1:] |= same
    return mask


def _frame_differs(frame, engine: str):
    """Mask of rows where `engine`'s frame provably covers different rows.

    Not a tolerance and not a proximity guess: it recomputes each frame's
    [left, right] row range under that engine's own arithmetic and compares it
    to Ibex's. A divergence inside this set is explained by semantics; one
    outside it is a hard failure and stays reported as such.

    Whether Ibex SHOULD admit later peers is a language question -- SPEC defines
    the window on TIME, which argues yes; a streaming engine cannot see the
    future, which argues no. Flagged here, not decided.

    `frame` must be sorted by (symbol, timestamp) so each symbol is contiguous.
    """
    import numpy as np, polars as pl   # module-level import would cost every run
    ts = frame["timestamp"].cast(pl.Int64).to_numpy()
    sym = frame["symbol"].to_numpy()
    mask = np.zeros(ts.size, dtype=bool)
    if ts.size == 0:
        return mask
    bounds = np.flatnonzero(sym[1:] != sym[:-1]) + 1
    for lo, hi in zip(np.r_[0, bounds], np.r_[bounds, ts.size]):
        t = ts[lo:hi]
        pos = np.arange(t.size)
        left_ibex = np.searchsorted(t, t - WINDOW_NS, side="left")
        key = t
        if engine == "clickhouse":
            key = tu = t // 1000        # ClickHouse's ORDER BY key IS microseconds
            left = np.searchsorted(tu, tu - (WINDOW_NS // 1000), side="left")
            right = np.searchsorted(tu, tu, side="right") - 1
        elif engine == "duckdb":
            left = np.searchsorted(t, (t - WINDOW_NS) // 1000 * 1000, side="left")
            right = np.searchsorted(t, t, side="right") - 1
        else:                                   # polars, closed="both", ns-exact
            left = left_ibex
            right = np.searchsorted(t, t, side="right") - 1
        # `first(price)` is ambiguous when the frame's FIRST row is one of a
        # tied pair: which peer counts as first is exactly the ordering SQL
        # leaves unspecified. That is a property of the frame's edge, not of
        # the current row, so it is not caught by the tied-row mask.
        edge_tied = np.zeros(t.size, dtype=bool)
        if t.size > 1:
            # Peers are equal under the ENGINE's ordering key: nanoseconds for
            # Ibex/Polars/DuckDB, microseconds for ClickHouse.
            tie_next = np.r_[key[1:] == key[:-1], False]
            for lft in (left, left_ibex):
                edge_tied |= tie_next[np.clip(lft, 0, t.size - 1)]
        mask[lo:hi] = (left != left_ibex) | (right != pos) | edge_tied
    return mask


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
        base = df.sort(["symbol", "timestamp"])
        agg = (base.rolling(index_column="timestamp", period=WINDOW, group_by="symbol",
                               closed="both")
                   .agg(open=pl.col("price").first(), high=pl.col("price").max(),
                        low=pl.col("price").min(), close=pl.col("price").last(),
                        volume_sum=pl.col("volume").sum()))
        pol = base.hstack(agg.sort(["symbol", "timestamp"],
                                   maintain_order=True).select(OHLCV))
    pol = pol.sort(keys)

    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{parquet}')")
    fetch = getattr(duckdb.DuckDBPyConnection, "to_arrow_table", None)
    res = con.execute(_duckdb_sql(window))
    duck = pl.from_arrow(res.to_arrow_table() if fetch else res.fetch_arrow_table()).sort(keys)

    sess = _ch_session(parquet, 0)
    ch = pl.from_arrow(_ch_arrow(sess, _clickhouse_sql(window))).sort(keys)
    sess.close()

    if not (len(ibex) == len(pol) == len(duck) == len(ch)):
        return [f"{window}: row counts differ - "
                f"ibex={len(ibex)} polars={len(pol)} duckdb={len(duck)} "
                f"clickhouse={len(ch)}"]

    # Only the trailing window does interval arithmetic, so only it can hit the
    # microsecond quantization; `aligned` uses ROWS frames and must match exactly.
    # Only the trailing flavour does interval arithmetic; `aligned` uses ROWS
    # frames and must match exactly.
    # Only the trailing flavour has a frame that can differ; `aligned` uses ROWS
    # frames throughout and must match exactly.
    ties = _tied_timestamps(ibex)
    explained = {e: (ties if window == "aligned" else ties | _frame_differs(ibex, e))
                 for e in _FRAME_ENGINES}

    for name, other in (("polars", pol), ("duckdb", duck), ("clickhouse", ch)):
        for col in OHLCV:
            missing = [w for w, f in (("ibex", ibex), (name, other)) if col not in f.columns]
            if missing:
                problems.append(f"{window}: {', '.join(missing)} missing column {col}")
                continue
            a = ibex[col].cast(pl.Float64)
            b = other[col].cast(pl.Float64)
            differs = ((a - b).abs() > tol).to_numpy()
            bad = int(differs.sum())
            if not bad:
                continue
            if name in explained:
                unexplained = int((differs & ~explained[name]).sum())
                if unexplained == 0:
                    print(f"note      {window}: {name} differs from ibex in {col} on "
                          f"{bad}/{len(a)} rows, ALL at tied timestamps or "
                          f"where its frame provably differs -- expected",
                          file=sys.stderr)
                    continue
                problems.append(f"{window}: {name} differs from ibex in {col} on "
                                f"{bad}/{len(a)} rows, {unexplained} of them NOT explained "
                                f"by tied timestamps or frame semantics")
                continue
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
    ap.add_argument("--threads", default="auto",
                    help="thread budget applied to EVERY engine -- a fairness "
                         "invariant, not a tuning knob. An integer pins all four "
                         "(POLARS_MAX_THREADS, IBEX_CORES, DuckDB PRAGMA, "
                         "ClickHouse max_threads). '1' additionally sets "
                         "IBEX_PARALLEL=0. 'auto' = each engine's own default, "
                         "which is NOT comparable across engines.")
    ap.add_argument("--engines", nargs="+",
                    default=["ibex", "polars", "duckdb", "clickhouse"])
    ap.add_argument("--out", type=Path, default=HERE / "results" / "results.tsv")
    ap.add_argument("--verify", action="store_true",
                    help="check all four engines produce identical OHLCV values, then exit")
    args = ap.parse_args()

    # Polars fixes its thread pool at import time, so the env var must be set
    # before bench_polars() triggers the first `import polars`. main() runs
    # before any such import, so setting it here is honoured.
    if args.threads not in ("auto", "1"):
        # An explicit budget: every engine gets the same one. Left to
        # `--duckdb-threads` alone this pinned DuckDB and ClickHouse while
        # Polars and Ibex took the whole box -- a threefold handicap reported
        # as a comparison.
        n = int(args.threads)
        os.environ["POLARS_MAX_THREADS"] = str(n)
        os.environ["IBEX_CORES"] = str(n)
        duck_threads = n
        label = f"{n}t"
        print(f"# thread budget: {n} for every engine", file=sys.stderr)
    elif args.threads == "1":
        os.environ["POLARS_MAX_THREADS"] = "1"
        os.environ["IBEX_CORES"] = "1"
        # Ibex runs parallel islands by DEFAULT now, so pinning the other two
        # without pinning it would quietly hand Ibex threads its competitors
        # were denied -- and the headline "single-threaded, Ibex wins at every
        # scale" result was measured back when Ibex had no threads to give.
        os.environ["IBEX_PARALLEL"] = "0"
        duck_threads = 1
        label = "1t"        # single-threaded / per-core
    elif args.threads == "auto":
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
                        print(f"ok        {tag}: all four engines agree on "
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
                    elif engine == "clickhouse":
                        mn, md, over = bench_clickhouse(pq, window, args.iters, duck_threads,
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
