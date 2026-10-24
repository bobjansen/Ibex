# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Matched resident grouped-mean comparison: Engine, independent API, Polars.

Start with the same resident categorical Polars table for all three routes.
Report preparation separately; every timed collection executes the query.
"""

import argparse
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import numpy as np
import pyarrow as pa
import polars as pl
from polars.testing import assert_frame_equal

import ibex
from ibex_polars import IbexEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--iters", type=int, default=12)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1 or args.iters < 6 or args.iters % 6:
        parser.error("positive rows and iterations divisible by six required")
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("benchmark_data", root / "benchmarking/data/gen_data.py")
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    rng = np.random.default_rng(generator.SEED + 1)
    table = pa.table({
        "symbol": pa.array(generator.make_tickers(generator.N_SYMBOLS)).take(
            pa.array(rng.integers(0, generator.N_SYMBOLS, size=args.rows))),
        "day": pa.array([f"2024-01-0{d}" for d in range(2, 2 + generator.N_DAYS)]).take(
            pa.array(rng.integers(0, generator.N_DAYS, size=args.rows))),
        "price": np.round(rng.uniform(1.0, 1000.0, size=args.rows), 4),
    })
    resident = pl.from_arrow(table).with_columns(pl.col("symbol", "day").cast(pl.Categorical))
    del table
    query = resident.lazy().group_by("symbol", "day").agg(avg_price=pl.mean("price"))
    engine = IbexEngine()
    start = time.perf_counter_ns()
    engine.prepare(query)
    prep = {"engine": (time.perf_counter_ns() - start) / 1e6}
    start = time.perf_counter_ns()
    independent_input = ibex.DataFrame(resident.to_arrow()).lazy()
    prep["independent"] = (time.perf_counter_ns() - start) / 1e6
    independent = independent_input.group_by("symbol", "day").agg(avg_price=ibex.col("price").mean())
    calls = {"engine": lambda: query.collect(engine=engine), "independent": independent.collect,
             "polars": lambda: query.collect(engine="streaming")}
    first, results = {}, {}
    for name, call in calls.items():
        start = time.perf_counter_ns()
        results[name] = call()
        first[name] = (time.perf_counter_ns() - start) / 1e6
    assert_frame_equal(results["engine"], results["polars"], check_row_order=False, rel_tol=1e-12, abs_tol=1e-9)
    reference = results["polars"].with_columns(pl.col("symbol", "day").cast(pl.String))
    assert_frame_equal(pl.from_arrow(results["independent"].to_arrow()), reference,
                       check_row_order=False, rel_tol=1e-12, abs_tol=1e-9)
    for _ in range(2):
        for call in calls.values():
            call()
    names = list(calls)
    orders = list(itertools.permutations(names))
    samples = {name: [] for name in names}
    for iteration in range(args.iters):
        for name in orders[iteration % 6]:
            start = time.perf_counter_ns()
            calls[name]()
            samples[name].append((time.perf_counter_ns() - start) / 1e6)
    canonical = results["engine"].with_columns(pl.col("symbol", "day").cast(pl.String)).sort("symbol", "day")
    files = list((root / "python/ibex").glob("*.py")) + list((root / "python/ibex_polars").glob("*.py")) + [
        Path(__file__), root / "src/runtime/aggregate_chunked.cpp", root / "python/ibex_pyarrow_module.cpp"]
    report = {
        "query": "mean_by_symbol_day", "rows": args.rows, "groups": canonical.height,
        "input": "resident Polars categorical keys; prices_multi seed 43, 252 symbols, four days",
        "resident_chunks": {name: resident[name].n_chunks() for name in resident.columns},
        "input_prepare_ms": prep, "first_collect_ms": first, "samples_ms": samples,
        "median_ms": {name: statistics.median(values) for name, values in samples.items()},
        "result_sha256": hashlib.sha256(json.dumps(canonical.rows(), separators=(",", ":")).encode()).hexdigest(),
        "ibex_cores": os.environ.get("IBEX_CORES"), "polars_threads": pl.thread_pool_size(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)), "polars_version": pl.__version__,
        "platform": platform.platform(), "build": "Release",
        "ibex_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "worktree_dirty": True,
        "source_sha256": {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
        "native_source": engine.explain(query),
        "notes": ["Same resident categorical input for all routes; initial data generation/encoding excluded.",
                  "Preparation translates/binds inputs without executing aggregation; no results cached.",
                  "First collection order is Engine, independent, Polars. Native runtime initialization is charged to Engine; independent first collection shares the initialized runtime.",
                  "Warm timings include output conversion; independent output is Arrow-backed, Engine output is Polars.",
                  "All six execution orders balanced; each index forms a matched pair."]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("rows", "median_ms", "input_prepare_ms", "first_collect_ms", "result_sha256")}))


if __name__ == "__main__":
    main()
