# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Matched before/after timings of categorical Arrow result conversion.

Reuse real native outputs; query execution and Arrow export are outside timers.
The baseline reproduces d6c79ef3's output boundary, including dtype correction.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal

import ibex_pyarrow
from ibex_polars import IbexEngine
from ibex_polars._engine import _to_polars


def previous_boundary(result, expected, output):
    result = result.select(list(expected))
    metadata = dict(result.schema.metadata or {})
    metadata.pop(b"ibex.ordering", None)
    result = result.replace_schema_metadata(metadata or None)
    for index, name in enumerate(result.column_names):
        if result.column(index).type != output[name]:
            result = result.set_column(index, name, result.column(index).cast(output[name]))
    frame = pl.from_arrow(result)
    return frame if frame.schema == expected else frame.cast(expected, strict=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--iters", type=int, default=30)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.rows < 1008 or args.iters < 12 or args.iters % 2:
        parser.error("rows >= 1008 and even iterations >= 12 required")
    rng = np.random.default_rng(43)
    resident = pl.DataFrame({
        "symbol": np.array([f"S{i:03}" for i in range(252)])[rng.integers(0, 252, args.rows)],
        "day": np.array(["Mon", "Tue", "Wed", "Thu"])[rng.integers(0, 4, args.rows)],
        "price": rng.uniform(1, 1000, args.rows),
    }).with_columns(pl.col("symbol", "day").cast(pl.Categorical))
    engine = IbexEngine()
    queries = {
        "grouped_mean": resident.lazy().group_by("symbol", "day").agg(avg_price=pl.mean("price")),
        "categorical_projection": resident.lazy(),
        "numeric_projection": resident.lazy().select("price"),
        "string_projection": resident.with_columns(pl.col("symbol", "day").cast(pl.String)).lazy(),
    }
    reports = []
    for label, query in queries.items():
        source, bindings, scalars, expected, output = engine._prepare(query, pl.QueryOptFlags())
        raw = ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        calls = {"before": lambda: previous_boundary(raw, expected, output),
                 "after": lambda: _to_polars(raw, expected, output)}
        reference = query.collect(engine="streaming")
        for call in calls.values():
            assert_frame_equal(call(), reference, check_row_order=False, rel_tol=1e-12, abs_tol=1e-9)
        for _ in range(5):
            for call in calls.values():
                call()
        samples = {name: [] for name in calls}
        for iteration in range(args.iters):
            names = list(calls) if iteration % 2 == 0 else list(reversed(calls))
            for name in names:
                start = time.perf_counter_ns()
                frame = calls[name]()
                elapsed = time.perf_counter_ns() - start
                samples[name].append(elapsed / 1e6)
                del frame
        converted = calls["after"]().to_arrow()
        numeric_shared = {name: raw[name].chunk(0).buffers()[1].address == converted[name].chunk(0).buffers()[1].address
                          for name, dtype in expected.items() if dtype == pl.Float64}
        reports.append({"case": label, "rows": raw.num_rows, "columns": raw.num_columns,
                        "raw_arrow_schema": str(raw.schema), "polars_schema": str(expected),
                        "numeric_payload_shared": numeric_shared, "samples_ms": samples,
                        "median_ms": {k: statistics.median(v) for k, v in samples.items()}})
        print(json.dumps({k: reports[-1][k] for k in ("case", "rows", "median_ms")}), flush=True)
    root = Path(__file__).resolve().parents[1]
    paths = [Path(__file__), root / "python/ibex_polars/_engine.py"]
    report = {"cases": reports, "baseline_commit": "d6c79ef3", "build": "Release",
              "ibex_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "polars_version": pl.__version__, "polars_threads": pl.thread_pool_size(),
              "ibex_cores": os.environ.get("IBEX_CORES"), "cpu_affinity": sorted(os.sched_getaffinity(0)),
              "source_sha256": {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
              "notes": ["Warmed full Engine output boundary; numerical execution, Arrow export and input preparation excluded.",
                        "Thirty matched pairs with alternating order by default; immutable native output reused.",
                        "Default category mappings; named categories may require additional remapping."]}
    args.out.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
