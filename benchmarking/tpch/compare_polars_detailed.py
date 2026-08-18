#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""
Detailed benchmark comparison: Ibex vs Polars (and optionally upstream PDS).

Reads TSV benchmark results and produces a formatted comparison report showing:
- Query-by-query performance metrics
- Geometric mean and win/loss statistics
- Improvement trends (e.g., vs previous runs)
- Highlighted strong/weak areas

Usage:
    python3 compare_polars_detailed.py [--ibex FILE] [--polars FILE] [--pdsh FILE] [--title TEXT]

Examples:
    # Compare current Ibex and Polars SF-2 results
    python3 compare_polars_detailed.py --ibex results/ibex_sf2.tsv --polars results/polars_sf2.tsv

    # Include upstream PDS results
    python3 compare_polars_detailed.py \\
        --ibex results/ibex_sf2.tsv \\
        --polars results/polars_sf2.tsv \\
        --pdsh results/pdsh_polars_sf2.tsv \\
        --title "SF-2 Query Alignment Test"

    # Compare archived runs
    python3 compare_polars_detailed.py \\
        --ibex results/runs/20260818T185136Z_4616c063_sf2/ibex_sf2.tsv \\
        --polars results/runs/20260818T185136Z_4616c063_sf2/polars_sf2.tsv \\
        --title "Aug 18: Post-alignment"
"""

import argparse
import csv
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


def read_benchmark_results(filepath: str) -> Dict[str, float]:
    """Read a TSV benchmark file and return {query -> avg_ms}."""
    results = {}
    try:
        with open(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                query = row['query']
                avg_ms = float(row['avg_ms'])
                results[query] = avg_ms
    except FileNotFoundError:
        print(f"error: {filepath} not found", file=sys.stderr)
        sys.exit(1)
    return results


def calculate_geomean(ratios: List[float]) -> float:
    """Calculate geometric mean of a list of ratios."""
    if not ratios:
        return 1.0
    return math.exp(sum(math.log(r) for r in ratios) / len(ratios))


def format_ratio(ratio: float) -> str:
    """Format a performance ratio as a human-readable string."""
    if ratio < 0.95:
        return f"✓ {1/ratio:.2f}x faster"
    elif ratio > 1.05:
        return f"✗ {ratio:.2f}x slower"
    else:
        return "≈ 1.0x"


def format_trend(ratio: float) -> str:
    """Format a trend ratio (shorter version for vs-PDS column)."""
    if ratio < 0.95:
        return f"{1/ratio:.2f}x↑"
    elif ratio > 1.05:
        return f"{ratio:.2f}x↓"
    else:
        return "≈"


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--ibex", default="results/ibex_sf2.tsv",
                        help="Ibex benchmark results (default: results/ibex_sf2.tsv)")
    parser.add_argument("--polars", default="results/polars_sf2.tsv",
                        help="Polars benchmark results (default: results/polars_sf2.tsv)")
    parser.add_argument("--pdsh", help="Upstream PDS Polars results (optional)")
    parser.add_argument("--title", default="PDS-H Benchmark Comparison: Ibex vs Polars",
                        help="Report title")
    args = parser.parse_args()

    # Read results
    ibex_results = read_benchmark_results(args.ibex)
    polars_results = read_benchmark_results(args.polars)
    pdsh_results = read_benchmark_results(args.pdsh) if args.pdsh else {}

    if not ibex_results or not polars_results:
        print("error: could not read benchmark results", file=sys.stderr)
        sys.exit(1)

    # Print header
    print()
    print("=" * 110)
    print(f"{args.title}")
    print("=" * 110)

    # Prepare column widths
    col_ibex = 14
    col_polars = 14
    col_ratio = 16
    col_pdsh = 16 if pdsh_results else 0

    # Print table header
    header = f"{'Query':<8} {'Ibex (ms)':<{col_ibex}} {'Polars (ms)':<{col_polars}} {'Ibex/Polars':<{col_ratio}}"
    if pdsh_results:
        header += f" {'vs PDS':<{col_pdsh}}"
    print(header)
    print("=" * (110 if pdsh_results else 94))

    # Collect metrics
    speedups = []
    pdsh_speedups = []
    win_count = [0, 0, 0]  # [ibex_wins, ties, polars_wins]

    # Process each query
    for query in sorted(ibex_results.keys(), key=lambda q: int(q[1:])):
        ibex_time = ibex_results.get(query, 0)
        polars_time = polars_results.get(query, 0)
        pdsh_time = pdsh_results.get(query, 0) if pdsh_results else 0

        if ibex_time and polars_time:
            ratio = ibex_time / polars_time
            speedups.append(ratio)

            ratio_str = format_ratio(ratio)
            if ratio < 0.95:
                win_count[0] += 1
            elif ratio > 1.05:
                win_count[2] += 1
            else:
                win_count[1] += 1

            trend_str = ""
            if pdsh_time:
                pdsh_ratio = ibex_time / pdsh_time
                pdsh_speedups.append(pdsh_ratio)
                trend_str = format_trend(pdsh_ratio)

            # Print row
            row = f"{query:<8} {ibex_time:>10.1f}      {polars_time:>10.1f}      {ratio_str:<{col_ratio}}"
            if pdsh_results:
                row += f" {trend_str:<{col_pdsh}}"
            print(row)

    print("=" * (110 if pdsh_results else 94))

    # Print summary statistics
    if speedups:
        geomean = calculate_geomean(speedups)
        arith_mean = sum(speedups) / len(speedups)

        print()
        print(f"Results:        {win_count[0]} wins, {win_count[1]} ties, {win_count[2]} losses")

        if geomean >= 1:
            print(f"Geometric mean: Ibex is {geomean:.2f}x SLOWER than Polars")
        else:
            print(f"Geometric mean: Ibex is {1/geomean:.2f}x FASTER than Polars")

        print(f"Arithmetic mean ratio: {arith_mean:.2f}x")

        if pdsh_speedups:
            pdsh_geomean = calculate_geomean(pdsh_speedups)
            if pdsh_geomean >= 1:
                print(f"vs PDS Polars:  Ibex is {pdsh_geomean:.2f}x slower")
            else:
                print(f"vs PDS Polars:  Ibex is {1/pdsh_geomean:.2f}x faster")

    print()


if __name__ == "__main__":
    main()
