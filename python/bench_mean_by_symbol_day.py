# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Zoom in on the website's grouped-mean win through the independent API.

Match prices_multi's seed, symbols, days and rounded prices. Prepare inputs
outside timers. Compare the historical string-key reference and the current
categorical-key reference, always executing complete resident lazy queries.
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

import numpy as np
import pyarrow as pa
import polars
import ibex
import ibex_pyarrow
from ibex._lower import lower


def grouped_mean(pl, frame):
    return frame.group_by("symbol", "day").agg(avg_price=pl.col("price").mean())


def check(actual, reference):
    expected = reference.to_dict(as_series=False)
    observed = actual.to_dict(as_series=False)
    def keyed(columns):
        return {(s, d): p for s, d, p in zip(columns["symbol"], columns["day"], columns["avg_price"])}
    left, right = keyed(observed), keyed(expected)
    assert actual.shape == reference.shape
    assert actual.columns == reference.columns
    assert actual.schema["avg_price"] == reference.to_arrow().schema.field("avg_price").type
    assert left.keys() == right.keys()
    assert np.allclose([left[k] for k in sorted(left)], [right[k] for k in sorted(left)],
                       rtol=1e-12, atol=1e-9)
    return left


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--iters", type=int, default=12)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1 or args.iters < 6 or args.iters % 6:
        parser.error("positive rows and iterations divisible by six required for balanced three-way order")
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("benchmark_data", root / "benchmarking/data/gen_data.py")
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    rng = np.random.default_rng(generator.SEED + 1)
    # Arrow take avoids retaining row-sized NumPy Unicode arrays. RNG calls
    # are in exactly the same order as the repository's CSV generator.
    symbols = pa.array(generator.make_tickers(generator.N_SYMBOLS)).take(
        pa.array(rng.integers(0, generator.N_SYMBOLS, size=args.rows)))
    days = pa.array([f"2024-01-0{d}" for d in range(2, 2 + generator.N_DAYS)]).take(
        pa.array(rng.integers(0, generator.N_DAYS, size=args.rows)))
    prices = np.round(rng.uniform(1.0, 1000.0, size=args.rows), 4)
    table = pa.table({"symbol": symbols, "price": prices, "day": days})
    prep = {}
    start = time.perf_counter_ns()
    native_input = ibex.DataFrame(table).lazy()
    prep["ibex"] = (time.perf_counter_ns() - start) / 1e6
    start = time.perf_counter_ns()
    plain = polars.from_arrow(table)
    prep["polars_string"] = (time.perf_counter_ns() - start) / 1e6
    start = time.perf_counter_ns()
    encoded = plain.with_columns(polars.col("symbol", "day").cast(polars.Categorical))
    prep["polars_categorical"] = prep["polars_string"] + (time.perf_counter_ns() - start) / 1e6
    query = grouped_mean(ibex, native_input)
    queries = {"ibex": query,
               "polars_string": grouped_mean(polars, plain.lazy()),
               "polars_categorical": grouped_mean(polars, encoded.lazy())}
    calls = {name: (value.collect if name == "ibex" else
                   lambda value=value: value.collect(engine="streaming")) for name, value in queries.items()}
    cold = {}
    results = {}
    for name, call in calls.items():
        start = time.perf_counter_ns()
        results[name] = call()
        cold[name] = (time.perf_counter_ns() - start) / 1e6
    native_values = check(results["ibex"], results["polars_string"])
    check(results["ibex"], results["polars_categorical"])
    for _ in range(2):
        for call in calls.values():
            call()
    samples = {name: [] for name in calls}
    names = list(calls)
    # Each engine occupies each position twice over six iterations.
    orders = [(0, 1, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0), (1, 0, 2), (0, 2, 1)]
    for iteration in range(args.iters):
        for position in orders[iteration % 6]:
            name = names[position]
            start = time.perf_counter_ns()
            calls[name]()
            samples[name].append((time.perf_counter_ns() - start) / 1e6)
    source, bindings, scalars = lower(query, validate=True)
    native_endpoint = []
    for _ in range(args.iters):
        start = time.perf_counter_ns()
        ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        native_endpoint.append((time.perf_counter_ns() - start) / 1e6)
    marker = "const PAYLOAD = "
    website, _ = json.JSONDecoder().raw_decode((root / "docs/benchmarks.html").read_text().split(marker, 1)[1])
    published = website["data"]["16000000"]["mean_by_symbol_day"]
    files = list((root / "python/ibex").glob("*.py")) + [root / "python/ibex_pyarrow_module.cpp", Path(__file__)]
    # Compare results exactly across native core counts, after canonical row order.
    digest = hashlib.sha256(json.dumps([(k, native_values[k]) for k in sorted(native_values)],
                                      separators=(",", ":")).encode()).hexdigest()
    report = {
        "query": "mean_by_symbol_day", "rows": args.rows, "groups": len(native_values),
        "input": "prices_multi generator: 252 symbols, four string days, seed 43; in-memory Arrow",
        "native_source": source, "input_prepare_ms": prep, "first_collect_ms": cold,
        "samples_ms": samples, "median_ms": {name: statistics.median(values) for name, values in samples.items()},
        "native_endpoint_samples_ms": native_endpoint, "result_sha256": digest,
        "ibex_cores": os.environ.get("IBEX_CORES"), "polars_threads": polars.thread_pool_size(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)), "polars_version": polars.__version__,
        "platform": platform.platform(), "build": "Release",
        "ibex_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "worktree_dirty": True,
        "source_sha256": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
        "website_reference": {"meta": website["meta"], "rows": 16_000_000,
                              "ibex_ms": published["ibex"], "polars_ms": published["polars"],
                              "ratio": published["polars"] / published["ibex"]},
        "notes": ["Input preparation outside collection timings; no result caching.",
                  "Both reference routes use the current installed engine; string mode matches historical key representation, not its old engine version/hardware.",
                  "Categorical reference keys are prepared outside timers, matching the current repo benchmark contract.",
                  "Result verification decodes key representations and aligns unordered groups outside timers.",
                  "Native endpoint diagnostics include parsing, planning, execution and Arrow export; they are a separate timing block."]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"rows": args.rows, "groups": len(native_values), "median_ms": report["median_ms"],
                      "prepare_ms": prep, "first_collect_ms": cold}))


if __name__ == "__main__":
    main()
