#!/usr/bin/env python3
"""Time the upstream Polars PDS-H implementations against Ibex's Parquet data.

This deliberately invokes the upstream query modules unchanged.  It therefore
measures the PDS project's Polars lazy expressions and DuckDB SQL separately
from this repository's hand-written Polars implementation (``bench_polars.py``).
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
DATA_ROOT = IBEX_ROOT / "benchmarking/data/tpch"


def percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    k = (len(data) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(data) - 1)
    return data[lo] + (data[hi] - data[lo]) * (k - lo)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=("polars", "duckdb"), required=True)
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

    parquet = DATA_ROOT / f"parquet_sf{args.sf}"
    if not parquet.is_dir():
        parser.error(f"missing {parquet}; run gen_data.sh {args.sf} then gen_parquet.sh {args.sf}")
    if not (args.pdsh_root / "queries" / args.engine).is_dir():
        parser.error(f"{args.pdsh_root} is not a Polars PDS-H checkout")

    # PDS derives table locations as PATH_TABLES/scale-<factor>/<table>.parquet.
    # Keep the data in Ibex's scale-specific location and expose that layout by
    # a symlink, avoiding an extra multi-GB copy on the EC2 benchmark box.
    # Pydantic parses SCALE_FACTOR as float, so PDS formats SF-1 as
    # ``scale-1.0`` (not ``scale-1``).
    pdsh_data = DATA_ROOT / f"scale-{float(args.sf)}"
    if pdsh_data.exists() or pdsh_data.is_symlink():
        if not pdsh_data.is_symlink() or pdsh_data.resolve() != parquet.resolve():
            parser.error(f"refusing to replace existing PDS data path: {pdsh_data}")
    else:
        pdsh_data.symlink_to(parquet.name)

    framework = args.framework or f"pdsh-{args.engine}"
    args.out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    with tempfile.TemporaryDirectory(prefix="ibex_pdsh_") as tmp:
        timing_dir = pathlib.Path(tmp)
        for query_number in range(1, 23):
            timing_file = timing_dir / f"{args.engine}_q{query_number}.csv"
            env = os.environ | {
                "SCALE_FACTOR": args.sf,
                "PATH_TABLES": str(DATA_ROOT),
                "PATH_TIMINGS": str(timing_dir),
                "PATH_TIMINGS_FILENAME": timing_file.name,
                "RUN_IO_TYPE": "parquet",
                "RUN_PRE_RUN": "false",
                "RUN_LOG_TIMINGS": "true",
                "RUN_ITERATIONS": str(args.warmup + args.iters),
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
        f.write("framework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\n")
        for query, samples in rows:
            f.write(
                f"{framework}\t{query}\t{statistics.mean(samples):.3f}\t{min(samples):.3f}\t"
                f"{max(samples):.3f}\t{statistics.pstdev(samples) if len(samples) > 1 else 0.0:.3f}\t"
                f"{percentile(samples, .95):.3f}\t{percentile(samples, .99):.3f}\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
