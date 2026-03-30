#!/usr/bin/env bash
# ibex-parquet-build.sh — build the bundled parquet plugin in an existing Ibex build tree.
#
# Usage:
#   ibex-parquet-build.sh
#
# Environment overrides:
#   IBEX_ROOT   — repo root       (default: directory above this script)
#   BUILD_DIR   — cmake build dir (default: $IBEX_ROOT/build)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"

if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    echo "error: build directory not configured: $BUILD_DIR" >&2
    echo "       Run cmake -B $BUILD_DIR ... first." >&2
    exit 1
fi

echo "▸ building parquet plugin in $BUILD_DIR"
cmake --build "$BUILD_DIR" --parallel --target ibex_parquet_plugin
