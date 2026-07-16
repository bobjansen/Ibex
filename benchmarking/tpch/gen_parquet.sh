#!/usr/bin/env bash
# gen_parquet.sh — convert generated dbgen .tbl files to Parquet, per scale factor.
#
# Run gen_data.sh <scale> first. Polars' own PDS-H benchmark scans Parquet, not
# CSV, so benchmarking/tpch/queries/*.ibex read Parquet — this produces
# benchmarking/data/tpch/parquet_sf<scale>/*.parquet from the raw dbgen tables.
#
# The queries hard-code the path benchmarking/data/tpch/parquet/, which is a
# symlink; run_bench.sh points it at the parquet_sf<scale>/ dir for the scale it
# is timing, so both scale factors can coexist on disk.
#
# Usage:
#   ./gen_parquet.sh [scale_factor]   # default 1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IBEX_EVAL="${IBEX_EVAL:-$IBEX_ROOT/build-release/tools/ibex_eval}"
PLUGIN_DIR="${PLUGIN_DIR:-$IBEX_ROOT/build-release/tools}"
DATA_ROOT="$IBEX_ROOT/benchmarking/data/tpch"

SCALE="${1:-1}"
SF_DIR="sf${SCALE}"
OUT_DIR="parquet_sf${SCALE}"

if [[ ! -x "$IBEX_EVAL" ]]; then
    echo "error: ibex_eval not found at $IBEX_EVAL — run 'cmake --build build-release' first" >&2
    exit 1
fi
if [[ ! -d "$DATA_ROOT/$SF_DIR" ]]; then
    echo "error: $DATA_ROOT/$SF_DIR not found — run ./gen_data.sh $SCALE first" >&2
    exit 1
fi

mkdir -p "$DATA_ROOT/$OUT_DIR"

# convert_to_parquet.ibex is written for SF-1 (sf1/ -> parquet/). Retarget its
# directory paths to this scale's input/output dirs; only the dir components
# change.
CONVERT="$(mktemp --suffix=.ibex)"
trap 'rm -f "$CONVERT"' EXIT
sed -e "s#/tpch/sf1/#/tpch/${SF_DIR}/#g" \
    -e "s#/tpch/parquet/#/tpch/${OUT_DIR}/#g" \
    "$SCRIPT_DIR/convert_to_parquet.ibex" > "$CONVERT"

cd "$IBEX_ROOT"
"$IBEX_EVAL" --plugin-path "$PLUGIN_DIR" "$CONVERT"
echo "parquet tables written to benchmarking/data/tpch/$OUT_DIR/"
