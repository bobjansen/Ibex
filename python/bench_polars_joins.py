# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Matched resident joins: ordinary Polars collect vs prepared IbexEngine collect.

Use a Release bridge. Input creation and correctness checks are outside timing;
plan/binding preparation and cold collections are reported separately.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal

import ibex_pyarrow
from ibex_polars import IbexEngine


def elapsed(call):
    start = time.perf_counter_ns()
    result = call()
    return result, (time.perf_counter_ns() - start) / 1e6


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--keys", type=int, default=200_000)
    parser.add_argument("--iters", type=int, default=12)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1000 or args.keys < 100 or args.iters < 6 or args.iters % 2:
        parser.error("rows>=1000, keys>=100 and an even number of iterations>=6 required")
    root = Path(__file__).resolve().parents[1]
    binary = Path(ibex_pyarrow.__file__).resolve()
    if not binary.is_relative_to(root / "build-release/python"):
        parser.error("set PYTHONPATH to build-release/python:python; a Release bridge is required")
    cache = (root / "build-release/CMakeCache.txt").read_text()
    if "CMAKE_BUILD_TYPE:STRING=Release" not in cache:
        parser.error("build-release is not configured as Release")
    rng = np.random.default_rng(20261025)
    lk = rng.integers(0, args.keys, size=args.rows, dtype=np.int64)
    rk = np.arange(args.keys, dtype=np.int64)
    left = pl.DataFrame({"k": lk, "x": rng.random(args.rows)})
    right = pl.DataFrame({"k": rk, "y": rng.random(args.keys)})
    partial = right.head(args.keys // 2)
    full_right = pl.DataFrame({"k": np.arange(args.keys + args.keys // 5, dtype=np.int64),
                               "y": rng.random(args.keys + args.keys // 5)})
    many_keys = max(1, min(args.keys // 2, 25000))
    many_left = pl.DataFrame({"k": rng.integers(0, many_keys, size=args.rows // 4, dtype=np.int64),
                              "x": rng.random(args.rows // 4)})
    many_right = pl.DataFrame({"k": np.tile(np.arange(many_keys, dtype=np.int64), 2),
                               "y": rng.random(2 * many_keys)})
    composite_left = left.with_columns(pl.Series("tenant", lk % 17))
    composite_right = right.with_columns(pl.Series("tenant", rk % 17))
    categories = pl.Series("k", [f"key_{i:05d}" for i in range(min(args.keys, 10000))], dtype=pl.Categorical)
    cat_left = pl.DataFrame({"k": categories.gather(pl.Series(rng.integers(0, len(categories), size=args.rows))),
                             "x": rng.random(args.rows)})
    cat_right = pl.DataFrame({"k": categories, "y": rng.random(len(categories))})
    cross_left = pl.DataFrame({"l": np.arange(min(1000, args.rows // 10), dtype=np.int64)})
    cross_right = pl.DataFrame({"r": np.arange(100, dtype=np.int64)})
    cases = [
        ("inner_unique", left, right, {"on": "k", "how": "inner"}),
        ("left_partial", left, partial, {"on": "k", "how": "left"}),
        ("full_partial", left, full_right, {"on": "k", "how": "full", "coalesce": True}),
        ("semi_half", left, partial, {"on": "k", "how": "semi"}),
        ("anti_half", left, partial, {"on": "k", "how": "anti"}),
        ("inner_many_to_many", many_left, many_right, {"on": "k", "how": "inner"}),
        ("inner_composite", composite_left, composite_right, {"on": ["tenant", "k"], "how": "inner"}),
        ("inner_categorical", cat_left, cat_right, {"on": "k", "how": "inner"}),
        ("cross", cross_left, cross_right, {"how": "cross"}),
    ]
    report = {"seed": 20261025, "rows": args.rows, "keys": args.keys, "iterations": args.iters,
              "comparison": "prepared IbexEngine vs ordinary Polars auto collect; complete materialized results",
              "timed": "collect including native execution and result export; input creation, preparation and assertions excluded",
              "order": "alternating execution order per repetition; two warmups per route",
              "polars_version": pl.__version__, "polars_threads": pl.thread_pool_size(),
              "ibex_cores_env": os.environ.get("IBEX_CORES"), "cpu_affinity": sorted(os.sched_getaffinity(0)),
              "platform": platform.platform(),
              "cpu_model": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                                 if line.startswith("model name")), "unknown"),
              "build": "Release", "binary": str(binary),
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
              "cases": {}}
    files = [Path(__file__), *sorted((root / "python/ibex").glob("*.py")), *sorted((root / "python/ibex_polars").glob("*.py"))]
    report["source_sha256"] = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
    for name, lhs, rhs, options in cases:
        query = lhs.lazy().join(rhs.lazy(), **options)
        engine = IbexEngine()
        _, prepare_ms = elapsed(lambda: engine.prepare(query))
        calls = {"polars": query.collect, "ibex": lambda: query.collect(engine=engine)}
        expected, polars_cold = elapsed(calls["polars"])
        actual, ibex_cold = elapsed(calls["ibex"])
        assert_frame_equal(actual, expected, check_row_order=False)
        for _ in range(2):
            for call in calls.values():
                call()
        samples = {route: [] for route in calls}
        for iteration in range(args.iters):
            for route in (("polars", "ibex") if iteration % 2 == 0 else ("ibex", "polars")):
                _, duration = elapsed(calls[route])
                samples[route].append(duration)
        medians = {route: statistics.median(times) for route, times in samples.items()}
        report["cases"][name] = {
            "left_rows": lhs.height, "right_rows": rhs.height, "output_rows": actual.height,
            "schema": {key: str(value) for key, value in actual.schema.items()},
            "engine_prepare_ms": prepare_ms, "cold_collect_ms": {"polars": polars_cold, "ibex_prepared": ibex_cold},
            "ibex_prepare_plus_first_ms": prepare_ms + ibex_cold,
            "samples_ms": samples, "median_ms": medians,
            "iqr_ms": {route: float(np.percentile(times, 75) - np.percentile(times, 25)) for route, times in samples.items()},
            "speedup_polars_over_ibex": medians["polars"] / medians["ibex"],
            "correctness": "all output rows, values and dtypes matched; order unspecified"}
        print(f"{name}: Polars {medians['polars']:.2f} ms, Ibex {medians['ibex']:.2f} ms; prep {prepare_ms:.2f} ms", flush=True)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
