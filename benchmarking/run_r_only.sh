#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Run the R-only benchmark slice: data.table, dplyr, and the native ibex dplyr
# backend.  Its datasets and query IDs come from the website scale suite.
#
# Usage:
#   benchmarking/run_r_only.sh [--sizes 1M,2M,4M] [--warmup 1] [--iters 5]
#                               [--keep-data] [--skip-install]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build-release}"
WARMUP=1
ITERS=5
KEEP_DATA=0
INSTALL_IBEX=1
SIZES=(1000000 2000000 4000000 8000000 16000000 32000000 50000000)

parse_size() {
    local token="$1"
    if [[ "$token" =~ ^[0-9]+$ ]]; then printf '%s' "$token"; return; fi
    if [[ "$token" =~ ^([0-9]+)[mM]$ ]]; then printf '%s000000' "${BASH_REMATCH[1]}"; return; fi
    echo "invalid size: $token" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sizes)
            IFS=',' read -r -a raw_sizes <<< "$2"
            SIZES=()
            for value in "${raw_sizes[@]}"; do SIZES+=("$(parse_size "$value")"); done
            shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --keep-data) KEEP_DATA=1; shift ;;
        --skip-install) INSTALL_IBEX=0; shift ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$BUILD_DIR/src/runtime/libibex_runtime.a" ]]; then
    echo "error: $BUILD_DIR is not an Ibex build directory with libibex_runtime.a" >&2
    exit 1
fi

RESULT_ROOT="$SCRIPT_DIR/results/r_only"
DATA_ROOT="$SCRIPT_DIR/data/r_only"
COMBINED="$SCRIPT_DIR/results/r_only.tsv"
R_LIB="$(mktemp -d "${TMPDIR:-/tmp}/ibex-r-bench.XXXXXX")"
cleanup() { rm -rf "$R_LIB"; }
trap cleanup EXIT

if [[ $INSTALL_IBEX -eq 1 ]]; then
    echo "━━━ Installing local R ibex package ━━━"
    # Build a tarball first, then install *that*. Installing the source
    # directory reuses whatever src/*.o are lying in it, and those objects do
    # not depend on the static libraries the package links against — so a
    # rebuilt libibex_runtime.a does not relink ibex.so, and the benchmark
    # silently measures the previous engine. `R CMD build` copies to a clean
    # tree, which is what makes the install honest.
    build_dir_tmp="$(mktemp -d "${TMPDIR:-/tmp}/ibex-r-pkg.XXXXXX")"
    (
        cd "$build_dir_tmp"
        R_ENVIRON_USER=/dev/null R_PROFILE_USER=/dev/null \
            IBEX_ROOT="$IBEX_ROOT" IBEX_BUILD_DIR="$BUILD_DIR" \
            R CMD build "$IBEX_ROOT/r/ibex"
        R_ENVIRON_USER=/dev/null R_PROFILE_USER=/dev/null \
            IBEX_ROOT="$IBEX_ROOT" IBEX_BUILD_DIR="$BUILD_DIR" \
            R CMD INSTALL --library="$R_LIB" ibex_*.tar.gz
    )
    rm -rf "$build_dir_tmp"
fi

mkdir -p "$RESULT_ROOT" "$DATA_ROOT" "$(dirname "$COMBINED")"
printf 'dataset_rows\tframework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\trows\tpeak_rss_mb\n' > "$COMBINED"

for rows in "${SIZES[@]}"; do
    data_dir="$DATA_ROOT/$rows"
    result_dir="$RESULT_ROOT/$rows"
    mkdir -p "$data_dir" "$result_dir"
    echo "━━━ R-only suite: $rows rows ━━━"
    uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/data/gen_data.py" "$data_dir" --rows "$rows"
    R_ENVIRON_USER=/dev/null R_PROFILE_USER=/dev/null \
        R_LIBS_USER="$R_LIB${R_LIBS_USER:+:$R_LIBS_USER}" Rscript "$SCRIPT_DIR/bench_r_only.R" \
        --csv "$data_dir/prices.csv" --csv-multi "$data_dir/prices_multi.csv" \
        --csv-trades "$data_dir/trades.csv" --csv-events "$data_dir/events.csv" \
        --csv-lookup "$data_dir/lookup.csv" --csv-users "$data_dir/users.csv" \
        --warmup "$WARMUP" --iters "$ITERS" --out "$result_dir/r_only.tsv"
    tail -n +2 "$result_dir/r_only.tsv" | awk -v n="$rows" 'BEGIN { FS=OFS="\t" } { print n, $0 }' >> "$COMBINED"
    if [[ $KEEP_DATA -eq 0 ]]; then rm -rf "$data_dir"; fi
done

echo "Wrote $COMBINED"
