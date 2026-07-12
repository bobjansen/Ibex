#!/usr/bin/env bash
# gen_data.sh — fetch/build tpch-dbgen and generate SF-1 PDS-H data.
#
# Usage:
#   ./gen_data.sh [scale_factor]   # default scale_factor=1
#
# Produces:
#   benchmarking/data/tpch/dbgen/        — cloned+built tpch-dbgen (gitignored)
#   benchmarking/data/tpch/sf<N>/*.tbl   — generated tables ('|'-delimited, trailing '|')
#   benchmarking/data/tpch/dbgen/answers/q*.out — official qualification answers (SF=1 only)
#
# tpch-dbgen source is the same C tool Polars vendors in pola-rs/polars-benchmark
# (itself a copy of the standard TPC-H dbgen 2.13.0 distribution). We sparse-clone
# just that subdirectory rather than committing EULA'd C source into this repo.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_ROOT="$IBEX_ROOT/benchmarking/data/tpch"
DBGEN_DIR="$DATA_ROOT/dbgen"

SCALE="${1:-1}"
OUT_DIR="$DATA_ROOT/sf${SCALE}"

if [[ ! -d "$DBGEN_DIR" ]]; then
    echo "── Fetching tpch-dbgen ─────────────────────────────────────────────"
    mkdir -p "$DATA_ROOT"
    TMP_CLONE="$(mktemp -d)"
    trap 'rm -rf "$TMP_CLONE"' EXIT
    if git clone --depth 1 --filter=blob:none --sparse \
            https://github.com/pola-rs/polars-benchmark.git "$TMP_CLONE" \
            >/dev/null 2>&1 \
        && (cd "$TMP_CLONE" && git sparse-checkout set tpch-dbgen) >/dev/null 2>&1; then
        mv "$TMP_CLONE/tpch-dbgen" "$DBGEN_DIR"
    else
        echo "  sparse checkout failed, falling back to a full shallow clone" >&2
        rm -rf "$TMP_CLONE"
        TMP_CLONE="$(mktemp -d)"
        trap 'rm -rf "$TMP_CLONE"' EXIT
        git clone --depth 1 https://github.com/pola-rs/polars-benchmark.git "$TMP_CLONE"
        mv "$TMP_CLONE/tpch-dbgen" "$DBGEN_DIR"
    fi
    rm -rf "$TMP_CLONE"
    trap - EXIT
fi

if [[ ! -x "$DBGEN_DIR/dbgen" ]]; then
    echo "── Building dbgen ──────────────────────────────────────────────────"
    make -C "$DBGEN_DIR" CC="${CC:-gcc}" dbgen
fi

echo "── Generating SF-${SCALE} data ─────────────────────────────────────────"
mkdir -p "$OUT_DIR"
(
    cd "$DBGEN_DIR"
    ./dbgen -f -s "$SCALE"
    mv ./*.tbl "$OUT_DIR/"
)

echo "── Done ─────────────────────────────────────────────────────────────"
echo "  tables:  $OUT_DIR/*.tbl"
echo "  answers: $DBGEN_DIR/answers/q*.out  (valid at SF=1 only)"
