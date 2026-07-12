#!/usr/bin/env bash
# run_bench.sh — time the 6 implemented PDS-H queries: ibex vs. polars
# (multi-threaded, matching Polars' default/published numbers) vs.
# polars-st (single-threaded, the fair apples-to-apples comparison since
# Ibex has no multithreading yet).
#
# Prerequisite: ./gen_data.sh && ./gen_parquet.sh (once).
#
# Usage:
#   ./run_bench.sh [--warmup N] [--iters N]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS="$SCRIPT_DIR/results"

WARMUP=1
ITERS=5
while [[ $# -gt 0 ]]; do
    case "$1" in
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters)  ITERS="$2";  shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -d "$IBEX_ROOT/benchmarking/data/tpch/parquet" ]]; then
    echo "error: benchmarking/data/tpch/parquet not found — run ./gen_data.sh && ./gen_parquet.sh first" >&2
    exit 1
fi

echo "=== ibex ==="
python3 "$SCRIPT_DIR/bench_ibex.py" --warmup "$WARMUP" --iters "$ITERS" --out "$RESULTS/ibex.tsv"

echo "=== polars (multi-threaded, $(nproc) cores) ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" --warmup "$WARMUP" --iters "$ITERS" --out "$RESULTS/polars.tsv"

echo "=== polars-st (single-threaded, apples-to-apples vs. ibex) ==="
POLARS_MAX_THREADS=1 uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" --warmup "$WARMUP" --iters "$ITERS" --out "$RESULTS/polars_st.tsv.tmp"
sed 's/^polars\t/polars-st\t/' "$RESULTS/polars_st.tsv.tmp" > "$RESULTS/polars_st.tsv"
rm -f "$RESULTS/polars_st.tsv.tmp"

echo
python3 "$SCRIPT_DIR/print_table.py" "$RESULTS"/*.tsv
