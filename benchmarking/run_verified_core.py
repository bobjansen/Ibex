#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Run only the seven value-checked queries through the existing harnesses.

Run one engine per process. No other benchmarks or builds should run alongside.
"""
import argparse
import csv
import importlib
import os
import pathlib
import re
import subprocess

QUERIES = {
    "mean_by_symbol", "update_price_x2", "distinct_symbol", "sort_price",
    "order_head_topk", "order_tail_topk", "filter_simple",
}
HEADER = ["framework", "query", "avg_ms", "min_ms", "max_ms", "stddev_ms",
          "p95_ms", "p99_ms", "rows", "peak_rss_mb"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True,
                        choices=["ibex", "polars", "duckdb", "datafusion", "clickhouse"])
    parser.add_argument("--csv", required=True)
    parser.add_argument("--csv-trades", required=True)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iters", type=int, default=10)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    if args.threads < 1 or args.warmup < 0 or args.iters < 1:
        parser.error("threads/iters must be positive and warmup nonnegative")
    for key in ("POLARS_MAX_THREADS", "IBEX_CORES", "OMP_NUM_THREADS"):
        os.environ[key] = str(args.threads)
    if args.engine == "ibex":
        root = pathlib.Path(__file__).resolve().parents[1]
        cache = (root / "build-release/CMakeCache.txt").read_text()
        if not re.search(r"^CMAKE_BUILD_TYPE:[^=]+=Release$", cache, re.M):
            parser.error("build-release must be configured as Release")
        output = subprocess.check_output([
            str(root / "build-release/tools/ibex_bench"),
            "--csv", args.csv, "--csv-trades", args.csv_trades,
            "--suite", "core,sort,filter", "--include-parse",
            "--warmup", str(args.warmup), "--iters", str(args.iters),
        ], text=True)
        rows = []
        for line in output.splitlines():
            match = re.match(r"bench ([^:]+):", line)
            if match and match[1] in QUERIES:
                fields = dict(re.findall(r"(\w+)=([\d.]+)", line))
                rows.append(["ibex", match[1]] + [fields[key] for key in HEADER[2:]])
    else:
        module_name = {"polars": "bench_python"}.get(args.engine, "bench_" + args.engine)
        module = importlib.import_module(module_name)
        module.should_skip = lambda framework, name: name not in QUERIES
        extra = []
        session = None
        if args.engine == "duckdb":
            import duckdb
            session = duckdb.connect()
            session.execute(f"SET threads={args.threads}")
        elif args.engine == "datafusion":
            from datafusion import SessionConfig, SessionContext
            session = SessionContext(SessionConfig().with_target_partitions(args.threads))
        elif args.engine == "clickhouse":
            from chdb.session import Session
            session = Session()
            session.query(f"SET max_threads={args.threads}")
        if session is not None:
            extra = [session]
        function = getattr(module, "bench_polars" if args.engine == "polars"
                           else f"bench_{args.engine}_core")
        rows = function(args.csv, None, args.csv_trades, args.warmup, args.iters, *extra)
        rows = [row for row in rows if row[1] in QUERIES]
        if args.engine in ("duckdb", "clickhouse"):
            session.close()
    if {row[1] for row in rows} != QUERIES or any(float(row[2]) < 0 for row in rows):
        raise RuntimeError("A required query is absent or was cut")
    path = pathlib.Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(HEADER)
        writer.writerows(rows)


if __name__ == "__main__":
    main()
