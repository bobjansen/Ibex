#!/usr/bin/env python3
"""Bound Discovery concurrency from attributed worker wall time and query wall."""
import argparse
import csv
import math
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", type=Path)
    args = parser.parse_args()
    cases = {}
    rows = csv.DictReader(args.samples.open())
    if not {"discovery_pool_ms", "pool_threads"}.issubset(rows.fieldnames or []):
        parser.error("Rerun the current tool with --profile-workers for attribution and pool size")
    for row in rows:
        key = tuple(row[k] for k in ("rows", "cardinality", "chunk_rows", "cores", "shape", "aggregate", "side"))
        cases.setdefault(key, []).append(row)
    if not cases:
        parser.error("No samples")
    print("rows cardinality chunk cores shape aggregate side pool phase_cap median_avg_jobs min_peak_bound breaches/samples")
    for key, samples in cases.items():
        pools = {int(r["pool_threads"]) for r in samples}
        if len(pools) != 1:
            raise ValueError("Pool size changed within a case")
        pool = pools.pop()
        # This harness uses the standard Discovery descriptor (ceiling 64),
        # no physical worker override, and an unknown source row estimate.
        cap = max(1, min(int(key[3]), pool, 64))
        ratios = [float(r["discovery_pool_ms"]) / float(r["ms"]) for r in samples]
        # CSV times have nanosecond precision. Round conservatively before
        # taking a ceiling so a boundary cannot become a false overrun.
        lower_bounds = [math.ceil(max(0.0, float(r["discovery_pool_ms"]) - 1e-6) /
                                  (float(r["ms"]) + 1e-6)) for r in samples]
        breaches = sum(r > cap for r in lower_bounds)
        print(*key, pool, cap, f"{statistics.median(ratios):.2f}", min(lower_bounds), f"{breaches}/{len(samples)}")
    print("The worker-time/query-wall ratio is a lower bound on peak Discovery concurrency, not an exact peak or CPU utilization.")
    print("A bound exceeding the cap proves an overrun. A smaller bound does not prove enforcement; zero may mean profiling was disabled.")


if __name__ == "__main__":
    main()
