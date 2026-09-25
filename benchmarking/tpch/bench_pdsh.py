#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Time the upstream Polars PDS-H implementations on polars-benchmark's own data.

This deliberately invokes the upstream query modules unchanged, so the
reference is the PDS project's own Polars lazy expressions and DuckDB SQL.
This repository used to carry a second, hand-written Polars implementation
alongside them; it was removed because a reference engine we maintain
ourselves is one we can accidentally tune, and its query shapes had drifted
from upstream's.
"""
import argparse
import csv
import os
import pathlib
import statistics
import subprocess
import sys
import tempfile


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
import bench_env  # noqa: E402


def percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    k = (len(data) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(data) - 1)
    return data[lo] + (data[hi] - data[lo]) * (k - lo)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("polars", "duckdb"), required=True)
    parser.add_argument("--polars-engine", choices=("in-memory", "streaming"), default="in-memory",
                        help="which Polars executor to select (Polars only). Upstream pins "
                             "'in-memory' explicitly unless told otherwise, so this is a real "
                             "choice rather than a default; see the note in main().")
    parser.add_argument("--threads", type=int, default=None,
                        help="connection thread count (DuckDB only)")
    parser.add_argument("--pdsh-root", type=pathlib.Path, required=True,
                        help="checkout of pola-rs/polars-benchmark")
    parser.add_argument("--sf", default="1")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iters", type=int, default=5)
    parser.add_argument("--framework", default=None)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if args.threads is not None and (args.engine != "duckdb" or args.threads < 1):
        parser.error("--threads requires --engine duckdb and a positive value")
    if args.polars_engine != "in-memory" and args.engine != "polars":
        parser.error("--polars-engine applies to --engine polars")

    if not (args.pdsh_root / "queries" / args.engine).is_dir():
        parser.error(f"{args.pdsh_root} is not a Polars PDS-H checkout")
    # The tables polars-benchmark generated itself, read where it put them:
    # PATH_TABLES/scale-<float SF>/<table>.parquet.
    tables = bench_env.pdsh_scale_dir(args.pdsh_root, args.sf)
    if not tables.is_dir():
        parser.error(f"missing {tables}; run `make data-tables SCALE_FACTOR={float(args.sf)}` "
                     f"in {args.pdsh_root}")

    # Upstream's `obtain_engine_config()` returns "in-memory" EXPLICITLY unless
    # `RUN_POLARS_STREAMING` is set -- it is a selected configuration, not a
    # default that tracks whatever Polars considers current. Reporting a ratio
    # against one executor while the other exists is a choice, so name the
    # executor in the framework rather than letting "pdsh-polars" stand for
    # whichever one happened to run.
    streaming = args.polars_engine == "streaming"
    default_framework = f"pdsh-{args.engine}" + ("-stream" if streaming else "")
    framework = args.framework or default_framework
    args.out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    with tempfile.TemporaryDirectory(prefix="ibex_pdsh_") as tmp:
        timing_dir = pathlib.Path(tmp)
        for query_number in range(1, 23):
            timing_file = timing_dir / f"{args.engine}_q{query_number}.csv"
            env = os.environ | {
                "SCALE_FACTOR": args.sf,
                "PATH_TABLES": str(bench_env.pdsh_tables_root(args.pdsh_root)),
                "PATH_TIMINGS": str(timing_dir),
                "PATH_TIMINGS_FILENAME": timing_file.name,
                "RUN_IO_TYPE": "parquet",
                "RUN_PRE_RUN": "false",
                "RUN_LOG_TIMINGS": "true",
                "RUN_ITERATIONS": str(args.warmup + args.iters),
                "RUN_POLARS_STREAMING": "true" if streaming else "false",
            }
            print(f"=== {framework} q{query_number:02d} ===", file=sys.stderr)
            command = [sys.executable, "-m", f"queries.{args.engine}.q{query_number}"]
            if args.engine == "duckdb" and args.threads is not None:
                # Apply the setting to the exact connection owned by upstream,
                # then invoke its otherwise unchanged query implementation.
                launcher = (
                    "from queries.duckdb import utils; "
                    f"utils.get_connection().execute('SET threads={args.threads}'); "
                    f"from queries.duckdb.q{query_number} import q; q()"
                )
                command = [sys.executable, "-c", launcher]
            subprocess.run(
                command,
                cwd=args.pdsh_root,
                env=env,
                check=True,
            )
            with timing_file.open(newline="") as f:
                samples = [float(row["duration[s]"]) * 1000.0 for row in csv.DictReader(f)]
            if len(samples) != args.warmup + args.iters:
                raise RuntimeError(f"q{query_number}: expected {args.warmup + args.iters} samples, got {len(samples)}")
            samples = samples[args.warmup:]
            rows.append((f"q{query_number:02d}", samples))

    with args.out.open("w") as f:
        # samples_ms: every timed iteration in run order, as in bench_ibex.py.
        f.write("framework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\tsamples_ms\n")
        for query, samples in rows:
            f.write(
                f"{framework}\t{query}\t{statistics.mean(samples):.3f}\t{min(samples):.3f}\t"
                f"{max(samples):.3f}\t{statistics.pstdev(samples) if len(samples) > 1 else 0.0:.3f}\t"
                f"{percentile(samples, .95):.3f}\t{percentile(samples, .99):.3f}\t"
                f"{','.join(f'{d:.3f}' for d in samples)}\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
