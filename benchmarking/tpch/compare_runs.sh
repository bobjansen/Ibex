#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# compare_runs.sh — compare two benchmark runs side-by-side
# Shows before/after performance metrics and improvement summary
#
# Usage:
#   ./compare_runs.sh <before_run_dir> <after_run_dir> [--scale SF]
#
# Examples:
#   # Compare two archived SF-2 runs
#   ./compare_runs.sh \
#     results/runs/20260814T145823Z_aaad3eae_sf2 \
#     results/runs/20260818T185136Z_4616c063_sf2
#
#   # Compare SF-4 runs
#   ./compare_runs.sh \
#     results/runs/20260814T145823Z_aaad3eae_sf4 \
#     results/runs/20260818T185136Z_4616c063_sf4 \
#     --scale 4

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: $0 <before_run> <after_run> [--scale N]" >&2
    echo "" >&2
    echo "Examples:" >&2
    echo "  # Compare SF-2 runs" >&2
    echo "  $0 results/runs/before_sf2 results/runs/after_sf2" >&2
    echo "" >&2
    echo "  # Compare SF-4 runs" >&2
    echo "  $0 results/runs/before_sf4 results/runs/after_sf4 --scale 4" >&2
    exit 1
fi

BEFORE_DIR="$1"
AFTER_DIR="$2"
SCALE="${3:-2}"

if [[ ! -d "$BEFORE_DIR" ]] || [[ ! -d "$AFTER_DIR" ]]; then
    echo "error: run directories not found" >&2
    exit 1
fi

# Infer scale from directory name if not specified
if [[ "$SCALE" == "--scale" ]]; then
    SCALE="$4"
fi

echo ""
echo "================================================================================"
echo "BENCHMARK RUN COMPARISON"
echo "================================================================================"
echo ""
echo "BEFORE: $(basename "$BEFORE_DIR")"
if [[ -f "$BEFORE_DIR/manifest.json" ]]; then
    echo "        Commit: $(grep -o '"commit": "[^"]*' "$BEFORE_DIR/manifest.json" | cut -d'"' -f4 | cut -c1-8)"
    echo "        Cores:  $(grep -o '"cores": "[^"]*' "$BEFORE_DIR/manifest.json" | cut -d'"' -f4)"
fi
echo ""
echo "AFTER:  $(basename "$AFTER_DIR")"
if [[ -f "$AFTER_DIR/manifest.json" ]]; then
    echo "        Commit: $(grep -o '"commit": "[^"]*' "$AFTER_DIR/manifest.json" | cut -d'"' -f4 | cut -c1-8)"
    echo "        Cores:  $(grep -o '"cores": "[^"]*' "$AFTER_DIR/manifest.json" | cut -d'"' -f4)"
fi
echo ""
echo "================================================================================"
echo ""

# Find the TSV files
BEFORE_IBEX=$(find "$BEFORE_DIR" -name "ibex_sf${SCALE}.tsv" | head -1)
BEFORE_POLARS=$(find "$BEFORE_DIR" -name "polars_sf${SCALE}.tsv" | head -1)

AFTER_IBEX=$(find "$AFTER_DIR" -name "ibex_sf${SCALE}.tsv" | head -1)
AFTER_POLARS=$(find "$AFTER_DIR" -name "polars_sf${SCALE}.tsv" | head -1)

if [[ -z "$BEFORE_IBEX" ]] || [[ -z "$AFTER_IBEX" ]]; then
    echo "error: could not find SF-$SCALE ibex results in both runs" >&2
    exit 1
fi

if [[ -z "$BEFORE_POLARS" ]] || [[ -z "$AFTER_POLARS" ]]; then
    echo "error: could not find SF-$SCALE polars results in both runs" >&2
    exit 1
fi

echo "BEFORE RESULTS:"
echo "==============="
python3 compare_polars_detailed.py \
    --ibex "$BEFORE_IBEX" \
    --polars "$BEFORE_POLARS" \
    --title "Before: $(basename "$BEFORE_DIR")"

echo ""
echo "AFTER RESULTS:"
echo "=============="
python3 compare_polars_detailed.py \
    --ibex "$AFTER_IBEX" \
    --polars "$AFTER_POLARS" \
    --title "After: $(basename "$AFTER_DIR")"

echo ""
echo "================================================================================"
echo "IMPROVEMENT ANALYSIS"
echo "================================================================================"
echo ""

# Run Python to calculate improvements
python3 - "$BEFORE_IBEX" "$BEFORE_POLARS" "$AFTER_IBEX" "$AFTER_POLARS" << 'PYEOF'
import csv
import math
import sys

def read_results(filepath):
    results = {}
    with open(filepath) as f:
        for row in csv.DictReader(f, delimiter='\t'):
            results[row['query']] = float(row['avg_ms'])
    return results

def calc_geomean(ratios):
    if not ratios:
        return 1.0
    return math.exp(sum(math.log(r) for r in ratios) / len(ratios))

before_ibex = read_results(sys.argv[1])
before_polars = read_results(sys.argv[2])
after_ibex = read_results(sys.argv[3])
after_polars = read_results(sys.argv[4])

before_ratios = [before_ibex[q] / before_polars[q]
                 for q in sorted(before_ibex.keys())
                 if q in before_polars]
after_ratios = [after_ibex[q] / after_polars[q]
                for q in sorted(after_ibex.keys())
                if q in after_polars]

before_geomean = calc_geomean(before_ratios)
after_geomean = calc_geomean(after_ratios)
improvement_pct = (1 - after_geomean/before_geomean) * 100 if before_geomean > 0 else 0

before_wins = sum(1 for r in before_ratios if r < 0.95)
after_wins = sum(1 for r in after_ratios if r < 0.95)

print(f"Geometric Mean Performance (Ibex vs Polars):")
print(f"  Before: {before_geomean:.2f}x slower")
print(f"  After:  {after_geomean:.2f}x slower")
print(f"  Change: {improvement_pct:+.1f}% {'✓ IMPROVEMENT' if improvement_pct > 0 else '✗ REGRESSION'}")
print()
print(f"Query Wins:")
print(f"  Before: {before_wins}/22 wins")
print(f"  After:  {after_wins}/22 wins")
print(f"  Change: {after_wins - before_wins:+d}")
print()

# Find biggest improvements
improvements = []
for q in sorted(before_ibex.keys()):
    if q in before_polars and q in after_ibex and q in after_polars:
        before_r = before_ibex[q] / before_polars[q]
        after_r = after_ibex[q] / after_polars[q]
        if before_r > 1.0 and after_r > 0:
            # Improvement: ratio decreased (closer to 1.0)
            improvement = before_r / after_r if after_r < before_r else 0
        elif after_r > 1.0 and before_r > 0:
            # Regression: ratio increased
            improvement = -(after_r / before_r) if before_r < after_r else 0
        else:
            improvement = 0
        improvements.append((q, before_r, after_r, improvement))

improvements.sort(key=lambda x: abs(x[3]), reverse=True)

print("Top Changes:")
for q, before_r, after_r, imp in improvements[:5]:
    if imp > 1:
        print(f"  {q}: {before_r:.2f}x → {after_r:.2f}x ({imp:.2f}x improvement ✓)")
    elif imp < -1:
        print(f"  {q}: {before_r:.2f}x → {after_r:.2f}x ({-imp:.2f}x regression ✗)")
    else:
        print(f"  {q}: {before_r:.2f}x → {after_r:.2f}x (≈ unchanged)")
PYEOF

echo ""
