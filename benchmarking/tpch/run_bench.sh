#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run_bench.sh — time TPC-H/PDS-H queries: Ibex, this tree's Polars implementation,
# and the upstream Polars PDS-H Polars + DuckDB implementations.
# Every engine is run in both its default multi-threaded configuration and a
# one-thread configuration. Ibex's parallel islands are enabled explicitly for
# the MT row and disabled for the ST row, making the comparison reproducible
# even when the caller has ambient IBEX_* settings.
#
# Prerequisite (once per scale factor):
#   ./gen_data.sh <scale> && ./gen_parquet.sh <scale>
#
# The queries read benchmarking/data/tpch/parquet/, a symlink this script points
# at parquet_sf<scale>/ for the scale it is timing. Correctness (check_answers.py)
# is only defined at SF-1; higher scales are timing-only.
#
# Usage:
#   ./run_bench.sh [--sf N] [--warmup N] [--iters N] [--pdsh-root DIR]
#                  [--skip-pdsh] [--cores N]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_ROOT="$IBEX_ROOT/benchmarking/data/tpch"
RESULTS="$SCRIPT_DIR/results"

SCALE=1
WARMUP=1
ITERS=5
SKIP_PDSH=0
# Cores the whole comparison is pinned to. Unset means "every core on the box",
# which is the wrong default for a CROSS-ENGINE run on a big local machine:
# Polars sizes its pool from nproc and thrashes above ~8, which inflates Ibex's
# apparent lead. Pin both engines to the same set instead.
CORES="${IBEX_BENCH_CORES:-}"
PDSH_ROOT="${PDSH_ROOT:-}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf)     SCALE="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters)  ITERS="$2";  shift 2 ;;
        --pdsh-root) PDSH_ROOT="$2"; shift 2 ;;
        --skip-pdsh) SKIP_PDSH=1; shift ;;
        --cores)  CORES="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

PARQUET_DIR="$DATA_ROOT/parquet_sf${SCALE}"
if [[ ! -d "$PARQUET_DIR" ]]; then
    echo "error: $PARQUET_DIR not found — run ./gen_data.sh $SCALE && ./gen_parquet.sh $SCALE first" >&2
    exit 1
fi

if [[ -z "$PDSH_ROOT" && "$SKIP_PDSH" -eq 0 ]]; then
    echo "error: --pdsh-root is required (a checkout of pola-rs/polars-benchmark)" >&2
    echo "       or pass --skip-pdsh to time only ibex and this tree's Polars." >&2
    exit 1
fi

# Pin every engine to the same cores. `taskset` bounds the process; the two
# variables stop Ibex's worker pool and Polars' pool from each sizing themselves
# from nproc and oversubscribing the pinned set. The per-engine ST rows below
# override these, so the MT/ST contrast survives the pin.
PIN=()
if [[ -n "$CORES" ]]; then
    PIN=(taskset -c "0-$((CORES - 1))")
    export IBEX_THREADS="$CORES"
    export POLARS_MAX_THREADS="$CORES"
    echo "=== pinned to ${CORES} cores (of $(nproc)) ==="
fi

# Point the path the queries read at this scale's data.
ln -sfn "parquet_sf${SCALE}" "$DATA_ROOT/parquet"
echo "=== scale factor: SF-${SCALE} (parquet -> parquet_sf${SCALE}) ==="

# Results are suffixed by scale so runs at different scales do not clobber.
SUFFIX="_sf${SCALE}"

echo "=== ibex (multi-threaded, ${CORES:-$(nproc)} cores) ==="
IBEX_THREADS="${CORES:-auto}" IBEX_PARALLEL=1 "${PIN[@]}" python3 "$SCRIPT_DIR/bench_ibex.py" \
    --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/ibex${SUFFIX}.tsv"

echo "=== ibex-st (single-threaded) ==="
IBEX_THREADS=1 IBEX_PARALLEL=0 "${PIN[@]}" python3 "$SCRIPT_DIR/bench_ibex.py" \
    --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/ibex_st${SUFFIX}.tsv.tmp"
sed 's/^ibex\t/ibex-st\t/' "$RESULTS/ibex_st${SUFFIX}.tsv.tmp" > "$RESULTS/ibex_st${SUFFIX}.tsv"
rm -f "$RESULTS/ibex_st${SUFFIX}.tsv.tmp"

echo "=== polars (multi-threaded, ${CORES:-$(nproc)} cores) ==="
"${PIN[@]}" uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/polars${SUFFIX}.tsv"

echo "=== polars-st (single-threaded, apples-to-apples vs. ibex) ==="
POLARS_MAX_THREADS=1 "${PIN[@]}" uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" \
    --warmup "$WARMUP" --iters "$ITERS" --out "$RESULTS/polars_st${SUFFIX}.tsv.tmp"
sed 's/^polars\t/polars-st\t/' "$RESULTS/polars_st${SUFFIX}.tsv.tmp" > "$RESULTS/polars_st${SUFFIX}.tsv"
rm -f "$RESULTS/polars_st${SUFFIX}.tsv.tmp"

if [[ "$SKIP_PDSH" -eq 1 ]]; then
    # The upstream engines are pinned baselines here: their numbers move only
    # when polars-benchmark or DuckDB itself changes, so a run without that
    # checkout reuses whatever is already in results/ rather than dropping the
    # columns. Say so, because a carried-forward row is not a measured one.
    echo
    echo "=== upstream PDS-H engines SKIPPED (--skip-pdsh) ==="
    for f in "$RESULTS"/pdsh_*"${SUFFIX}.tsv"; do
        [[ -e "$f" ]] || continue
        echo "    carrying forward $(basename "$f") ($(date -r "$f" +%Y-%m-%d))"
    done
    echo
    python3 "$SCRIPT_DIR/print_table.py" "$RESULTS"/*"${SUFFIX}.tsv"
    exit 0
fi

echo "=== upstream PDS-H Polars (multi-threaded, $(nproc) cores) ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" --engine polars --pdsh-root "$PDSH_ROOT" \
    --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" --framework pdsh-polars \
    --out "$RESULTS/pdsh_polars${SUFFIX}.tsv"

echo "=== upstream PDS-H Polars (single-threaded) ==="
POLARS_MAX_THREADS=1 uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" \
    --engine polars --pdsh-root "$PDSH_ROOT" --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" \
    --framework pdsh-polars-st --out "$RESULTS/pdsh_polars_st${SUFFIX}.tsv"

echo "=== upstream PDS-H DuckDB SQL ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" --engine duckdb --pdsh-root "$PDSH_ROOT" \
    --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" --framework pdsh-duckdb \
    --out "$RESULTS/pdsh_duckdb${SUFFIX}.tsv"

echo "=== upstream PDS-H DuckDB SQL (single-threaded) ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" --engine duckdb --threads 1 \
    --pdsh-root "$PDSH_ROOT" --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" \
    --framework pdsh-duckdb-st --out "$RESULTS/pdsh_duckdb_st${SUFFIX}.tsv"

echo
python3 "$SCRIPT_DIR/print_table.py" "$RESULTS"/*"${SUFFIX}.tsv"
