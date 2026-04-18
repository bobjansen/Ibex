#!/usr/bin/env python3
"""Benchmark SQLite-backed 1BRC-style aggregation across Ibex, Polars, pandas, and SQLite.

This compares the same logical workload:
  select station, temp from measurements
  -> group by station
  -> min/mean/max(temp)
  -> order by station

The Ibex path reads SQLite through the bundled ADBC plugin. Polars and pandas
read from SQLite via Python and aggregate in-memory. The SQLite baseline pushes
the full aggregation down into SQL.

Usage:
  ./.venv/bin/python benchmarking/bench_sqlite_source.py \
      --sqlite-db /tmp/measurements_1m.sqlite \
      --driver /home/brj/miniforge3/envs/ibex/lib/libadbc_driver_sqlite.so
"""

from __future__ import annotations

import argparse
import csv
import os
import pathlib
import sqlite3
import subprocess
import sys
import time
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl


def timer(fn: Callable[[], object], warmup: int, iters: int):
    """Warm up then time fn(). Returns avg/min/max/stddev/p95/p99/result."""
    for _ in range(warmup):
        result = fn()
    times = []
    for _ in range(iters):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000.0)
    arr = np.array(times, dtype=float)
    return (
        float(np.mean(arr)),
        float(np.min(arr)),
        float(np.max(arr)),
        float(np.std(arr, ddof=0)),
        float(np.percentile(arr, 95)),
        float(np.percentile(arr, 99)),
        result,
    )


def ensure_measurements_sqlite(input_path: pathlib.Path, output_path: pathlib.Path, limit: int,
                               batch_size: int = 50_000) -> None:
    """Load the first N rows of measurements.txt into SQLite when missing."""
    if output_path.exists():
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(output_path)
    cur = conn.cursor()
    cur.execute("pragma journal_mode = WAL")
    cur.execute("pragma synchronous = OFF")
    cur.execute("drop table if exists measurements")
    cur.execute("create table measurements(station text not null, temp real not null)")

    pending: list[tuple[str, float]] = []
    imported = 0
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if imported >= limit:
                break
            station, temp = line.rstrip("\n").split(";", 1)
            pending.append((station, float(temp)))
            imported += 1
            if len(pending) >= batch_size:
                cur.executemany("insert into measurements(station, temp) values (?, ?)", pending)
                pending.clear()

    if pending:
        cur.executemany("insert into measurements(station, temp) values (?, ?)", pending)
    cur.execute("create index idx_measurements_station on measurements(station)")
    conn.commit()
    conn.close()


def bench_ibex(driver: str, sqlite_db: str, ibex_bin: str, plugin_path: str, ld_library_path: str):
    """Run the SQLite-backed aggregation through the release REPL."""
    script = (
        'import "adbc";\n'
        f'read_adbc("{driver}", "{sqlite_db}", "select station, temp from measurements")'
        '[select { min_temp = min(temp), avg_temp = mean(temp), max_temp = max(temp) }, '
        'by station, order station];\n'
    )
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = (
        ld_library_path
        if not env.get("LD_LIBRARY_PATH")
        else f"{ld_library_path}:{env['LD_LIBRARY_PATH']}"
    )
    completed = subprocess.run(
        [ibex_bin, "--plugin-path", plugin_path],
        input=script,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Ibex REPL failed with code {completed.returncode}\nstdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    rows = None
    for line in completed.stdout.splitlines():
        if line.startswith("rows: "):
            rows = int(line.split(": ", 1)[1])
            break
    if rows is None:
        raise RuntimeError(f"Could not parse Ibex row count from output:\n{completed.stdout}")
    return rows


def bench_polars(sqlite_db: str):
    conn = sqlite3.connect(sqlite_db)
    try:
        df = pl.read_database("select station, temp from measurements", connection=conn)
    finally:
        conn.close()
    return (
        df.group_by("station")
        .agg(
            pl.col("temp").min().alias("min_temp"),
            pl.col("temp").mean().alias("avg_temp"),
            pl.col("temp").max().alias("max_temp"),
        )
        .sort("station")
    )


def bench_pandas(sqlite_db: str):
    conn = sqlite3.connect(sqlite_db)
    try:
        df = pd.read_sql_query("select station, temp from measurements", conn)
    finally:
        conn.close()
    return (
        df.groupby("station", as_index=False)
        .agg(min_temp=("temp", "min"), avg_temp=("temp", "mean"), max_temp=("temp", "max"))
        .sort_values("station", kind="mergesort")
    )


def bench_sqlite_pushdown(sqlite_db: str):
    conn = sqlite3.connect(sqlite_db)
    try:
        return conn.execute(
            "select station, min(temp) as min_temp, avg(temp) as avg_temp, max(temp) as max_temp "
            "from measurements group by station order by station"
        ).fetchall()
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite-db", default="/tmp/measurements_1m.sqlite")
    parser.add_argument("--measurements-txt", default="examples/measurements.txt")
    parser.add_argument("--limit", type=int, default=1_000_000)
    parser.add_argument("--ensure-db", action="store_true",
                        help="Create the SQLite DB from measurements.txt when missing.")
    parser.add_argument("--driver", required=True,
                        help="Path to libadbc_driver_sqlite.so")
    parser.add_argument("--ibex-bin", default="build-release/tools/ibex")
    parser.add_argument("--plugin-path", default="build-release/tools")
    parser.add_argument("--ld-library-path", default="/home/brj/miniforge3/envs/ibex/lib")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iters", type=int, default=5)
    parser.add_argument("--out", default="benchmarking/results/sqlite_source.tsv")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    sqlite_db = pathlib.Path(args.sqlite_db)
    if args.ensure_db:
        ensure_measurements_sqlite(pathlib.Path(args.measurements_txt), sqlite_db, args.limit)
    if not sqlite_db.exists():
        raise SystemExit(
            f"SQLite database not found: {sqlite_db}\n"
            "Use --ensure-db to create it from measurements.txt."
        )

    rows_out = []

    def run(framework: str, query: str, fn: Callable[[], object]) -> None:
        avg_ms, min_ms, max_ms, stddev_ms, p95_ms, p99_ms, result = timer(
            fn, args.warmup, args.iters
        )
        rows = len(result)
        print(
            f"{framework}/{query}: avg_ms={avg_ms:.3f}, stddev_ms={stddev_ms:.3f}, "
            f"p99_ms={p99_ms:.3f}, rows={rows}",
            file=sys.stderr,
            flush=True,
        )
        rows_out.append(
            (
                framework,
                query,
                f"{avg_ms:.3f}",
                f"{min_ms:.3f}",
                f"{max_ms:.3f}",
                f"{stddev_ms:.3f}",
                f"{p95_ms:.3f}",
                f"{p99_ms:.3f}",
                rows,
            )
        )

    run(
        "ibex",
        "sqlite_1brc",
        lambda: list(
            range(
                bench_ibex(
                    args.driver,
                    str(sqlite_db),
                    args.ibex_bin,
                    args.plugin_path,
                    args.ld_library_path,
                )
            )
        ),
    )
    run("polars", "sqlite_1brc", lambda: bench_polars(str(sqlite_db)))
    run("pandas", "sqlite_1brc", lambda: bench_pandas(str(sqlite_db)))
    run("sqlite", "sqlite_1brc_pushdown", lambda: bench_sqlite_pushdown(str(sqlite_db)))

    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(
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
        writer.writerows(rows_out)

    print(f"wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
