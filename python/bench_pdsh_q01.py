# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Interleaved in-memory timings of the unchanged upstream q01 function.

Preparation is outside query timings, all samples are retained, and no query
results are cached. Run in separate processes with matched thread budgets.
"""

import argparse
import importlib
import hashlib
import json
from pathlib import Path
import os
import statistics
import sys
import time
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import polars
import ibex
import ibex_pyarrow
from ibex._lower import lower

from check_pdsh_q01 import assert_equal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdsh-root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--groups", type=int, default=6)
    parser.add_argument("--lineitem", type=Path,
                        help="load actual PDS-H q01 columns into memory before timing instead of generating data")
    parser.add_argument("--iters", type=int, default=12)
    parser.add_argument("--input-mode", choices=("bound", "raw"), default="bound",
                        help="raw repeats the prototype's previous Arrow import on every collection")
    parser.add_argument("--polars-engine", choices=("streaming", "in-memory"), default="streaming")
    parser.add_argument("--compare-inputs", action="store_true",
                        help="add the previous raw-import route to the interleaved comparison")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1 or args.groups < 1 or args.iters < 2 or args.iters % 2:
        parser.error("positive rows/groups and an even iteration count >= 2 required")
    sys.path.insert(0, str(args.pdsh_root.resolve()))
    module = importlib.import_module("queries.polars.q1")
    rng = np.random.default_rng(20261012)
    keys = rng.integers(0, args.groups, args.rows)
    # Two independent key columns, with exactly the requested number of pairs.
    labels = np.array([f"g{i:05}" for i in range((args.groups + 1) // 2)])
    table = pa.table({
        "l_returnflag": pa.array(labels[keys // 2]),
        "l_linestatus": pa.array(np.where(keys % 2, "O", "F")),
        "l_shipdate": pa.array(rng.integers(9000, 10500, args.rows, dtype=np.int32), type=pa.date32()),
        "l_quantity": rng.integers(1, 50, args.rows, dtype=np.int64),
        "l_extendedprice": rng.uniform(1.0, 10000.0, args.rows),
        "l_discount": rng.uniform(0.0, 0.1, args.rows),
        "l_tax": rng.uniform(0.0, 0.08, args.rows),
    })
    if args.lineitem:
        # Both frontends receive identical columns and rows; file I/O is
        # outside input preparation and query timings in this memory test.
        table = pq.read_table(args.lineitem, columns=table.column_names).combine_chunks()
    started = time.perf_counter_ns()
    native_input = ibex.DataFrame(table).lazy() if args.input_mode == "bound" else ibex.LazyFrame(("table", table))
    native_prep_ms = (time.perf_counter_ns() - started) / 1e6
    started = time.perf_counter_ns()
    reference_input = polars.from_arrow(table).lazy()
    reference_prep_ms = (time.perf_counter_ns() - started) / 1e6
    with patch.object(module, "pl", ibex):
        native = module.q(lineitem=native_input)
    reference = module.q(lineitem=reference_input)
    # Capture the actual first collection in this process, then validate those
    # outputs before warmups/timing. Input preparation is separately reported.
    samples = {"ibex": [], "polars": []}
    calls = {"ibex": native.collect, "polars": lambda: reference.collect(engine=args.polars_engine)}
    if args.compare_inputs:
        with patch.object(module, "pl", ibex):
            raw = module.q(lineitem=ibex.LazyFrame(("table", table)))
        calls["ibex_raw"] = raw.collect
        samples["ibex_raw"] = []
    fresh = {}
    first_results = {}
    for name in calls:
        started = time.perf_counter_ns()
        first_results[name] = calls[name]()
        fresh[name] = (time.perf_counter_ns() - started) / 1e6
    assert_equal(first_results["ibex"], first_results["polars"])
    if args.compare_inputs:
        assert_equal(first_results["ibex_raw"], first_results["polars"])
    for _ in range(2):
        for call in calls.values():
            call()
    for iteration in range(args.iters):
        names = list(calls)
        if len(names) == 3:
            # Balance first/middle/last positions in a three-way comparison.
            shift = iteration % 3
            names = names[shift:] + names[:shift]
            if iteration % 2:
                names.reverse()
        elif iteration % 2:
            names.reverse()
        for name in names:
            started = time.perf_counter_ns()
            calls[name]()
            samples[name].append((time.perf_counter_ns() - started) / 1e6)
    source, tables, scalars = lower(native, validate=True)
    native_only = []
    lowering = []
    for _ in range(args.iters):
        started = time.perf_counter_ns()
        lower(native, validate=True)
        lowering.append((time.perf_counter_ns() - started) / 1e6)
        started = time.perf_counter_ns()
        ibex_pyarrow.execute_pipeline(source, tables=tables, scalars=scalars)
        native_only.append((time.perf_counter_ns() - started) / 1e6)
    report = {
        "rows": table.num_rows, "groups": first_results["ibex"].shape[0], "polars_version": polars.__version__,
        "input": str(args.lineitem.parent.name) + "/" + args.lineitem.name if args.lineitem else "synthetic",
        "ibex_cores": os.environ.get("IBEX_CORES"), "polars_threads": polars.thread_pool_size(),
        "input_mode": args.input_mode,
        "polars_engine": args.polars_engine,
        "input_prepare_ms": {"ibex": native_prep_ms, "polars": reference_prep_ms},
        "first_collect_ms": fresh, "samples_ms": samples,
        "median_ms": {name: statistics.median(values) for name, values in samples.items()},
        "lowering_samples_ms": lowering, "native_endpoint_samples_ms": native_only,
        "note": "Native endpoint includes planning, execution and Arrow export; raw mode also repeats Arrow import. No results cached.",
    }
    # Normalize small results after timing for an exact serial/parallel check.
    normalized = pa.table(first_results["ibex"].to_dict(as_series=False),
                          schema=first_results["ibex"].to_arrow().schema.remove_metadata())
    output = pa.BufferOutputStream()
    with pa.ipc.new_stream(output, normalized.schema) as writer:
        writer.write_table(normalized)
    report["result_sha256"] = hashlib.sha256(output.getvalue().to_pybytes()).hexdigest()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"rows": report["rows"], "groups": report["groups"], "median_ms": report["median_ms"],
                      "lower_ms": statistics.median(lowering), "native_endpoint_ms": statistics.median(native_only)}))


if __name__ == "__main__":
    main()
