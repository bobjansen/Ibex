#!/usr/bin/env bash
# run_bench.sh — time TPC-H/PDS-H queries: Ibex, this tree's Polars implementation,
# and the upstream Polars PDS-H Polars + DuckDB implementations.
# (multi-threaded, matching Polars' default/published numbers) vs.
# polars-st (single-threaded, the fair apples-to-apples comparison since
# Ibex has no multithreading yet).
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_ROOT="$IBEX_ROOT/benchmarking/data/tpch"
RESULTS="$SCRIPT_DIR/results"

SCALE=1
WARMUP=1
ITERS=5
PDSH_ROOT="${PDSH_ROOT:-}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf)     SCALE="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters)  ITERS="$2";  shift 2 ;;
        --pdsh-root) PDSH_ROOT="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

PARQUET_DIR="$DATA_ROOT/parquet_sf${SCALE}"
if [[ ! -d "$PARQUET_DIR" ]]; then
    echo "error: $PARQUET_DIR not found — run ./gen_data.sh $SCALE && ./gen_parquet.sh $SCALE first" >&2
    exit 1
fi

if [[ -z "$PDSH_ROOT" ]]; then
    echo "error: --pdsh-root is required (a checkout of pola-rs/polars-benchmark)" >&2
    exit 1
fi

# Point the path the queries read at this scale's data.
ln -sfn "parquet_sf${SCALE}" "$DATA_ROOT/parquet"
echo "=== scale factor: SF-${SCALE} (parquet -> parquet_sf${SCALE}) ==="

# Results are suffixed by scale so runs at different scales do not clobber.
SUFFIX="_sf${SCALE}"

echo "=== ibex ==="
python3 "$SCRIPT_DIR/bench_ibex.py" --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/ibex${SUFFIX}.tsv"

echo "=== polars (multi-threaded, $(nproc) cores) ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/polars${SUFFIX}.tsv"

echo "=== polars-st (single-threaded, apples-to-apples vs. ibex) ==="
POLARS_MAX_THREADS=1 uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" \
    --warmup "$WARMUP" --iters "$ITERS" --out "$RESULTS/polars_st${SUFFIX}.tsv.tmp"
sed 's/^polars\t/polars-st\t/' "$RESULTS/polars_st${SUFFIX}.tsv.tmp" > "$RESULTS/polars_st${SUFFIX}.tsv"
rm -f "$RESULTS/polars_st${SUFFIX}.tsv.tmp"

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
