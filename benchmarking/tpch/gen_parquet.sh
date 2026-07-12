#!/usr/bin/env bash
# gen_parquet.sh — convert the generated SF-1 dbgen .tbl files to Parquet.
#
# Run gen_data.sh first. Polars' own PDS-H benchmark scans Parquet, not CSV,
# so benchmarking/tpch/queries/*.ibex read Parquet — this is what produces
# benchmarking/data/tpch/parquet/*.parquet from the raw dbgen tables.
#
# Usage:
#   ./gen_parquet.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IBEX_EVAL="${IBEX_EVAL:-$IBEX_ROOT/build-release/tools/ibex_eval}"
PLUGIN_DIR="${PLUGIN_DIR:-$IBEX_ROOT/build-release/tools}"

if [[ ! -x "$IBEX_EVAL" ]]; then
    echo "error: ibex_eval not found at $IBEX_EVAL — run 'cmake --build build-release' first" >&2
    exit 1
fi
if [[ ! -d "$IBEX_ROOT/benchmarking/data/tpch/sf1" ]]; then
    echo "error: benchmarking/data/tpch/sf1 not found — run ./gen_data.sh first" >&2
    exit 1
fi

mkdir -p "$IBEX_ROOT/benchmarking/data/tpch/parquet"
cd "$IBEX_ROOT"
"$IBEX_EVAL" --plugin-path "$PLUGIN_DIR" "$SCRIPT_DIR/convert_to_parquet.ibex"
echo "parquet tables written to benchmarking/data/tpch/parquet/"
