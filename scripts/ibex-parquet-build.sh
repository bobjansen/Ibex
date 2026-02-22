#!/usr/bin/env bash
# ibex-parquet-build.sh — build the standalone parquet plugin.
#
# Usage:
#   ibex-parquet-build.sh
#
# Environment overrides:
#   IBEX_ROOT   — repo root       (default: directory above this script)
#   BUILD_DIR   — ibex build dir  (default: $IBEX_ROOT/build)
#   PLUGIN_DIR  — plugin build dir (default: $IBEX_ROOT/build/plugins/parquet)
#   CXX         — C++ compiler    (default: clang++)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
PLUGIN_DIR="${PLUGIN_DIR:-$IBEX_ROOT/build/plugins/parquet}"
CXX="${CXX:-clang++}"
C="${C:-clang}"
BUILD_TYPE="${BUILD_TYPE:-Release}"

if [[ ! -f "$BUILD_DIR/src/runtime/libibex_runtime.a" ]]; then
    echo "error: missing ibex build artifacts in $BUILD_DIR" >&2
    echo "       Run: cmake --build $BUILD_DIR" >&2
    exit 1
fi

mkdir -p "$PLUGIN_DIR"

echo "▸ configuring parquet plugin in $PLUGIN_DIR"
cmake -S "$IBEX_ROOT/plugins/parquet" -B "$PLUGIN_DIR" -G Ninja \
    -DCMAKE_C_COMPILER="$C" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_INSTALL_PREFIX="$PLUGIN_DIR/_install" \
    -DIBEX_ROOT="$IBEX_ROOT" \
    -DIBEX_BUILD_DIR="$BUILD_DIR"

echo "▸ building parquet plugin"
cmake --build "$PLUGIN_DIR" --parallel

echo "✓ built $IBEX_ROOT/libraries/parquet.so"
