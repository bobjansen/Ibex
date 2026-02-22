#!/usr/bin/env bash
# ibex-e2e.sh — end-to-end checks for REPL, plugins, and transpilation.
#
# Usage:
#   ibex-e2e.sh [--skip-build] [--skip-tests] [--skip-release] [--skip-parquet] [--skip-repl] [--skip-compile]
#
# Environment:
#   IBEX_ROOT   — repo root (default: directory above this script)
#   BUILD_DIR   — debug build dir (default: $IBEX_ROOT/build)
#   RELEASE_DIR — release build dir (default: $IBEX_ROOT/build-release)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
RELEASE_DIR="${RELEASE_DIR:-$IBEX_ROOT/build-release}"

SKIP_BUILD=false
SKIP_TESTS=false
SKIP_RELEASE=false
SKIP_PARQUET=false
SKIP_REPL=false
SKIP_COMPILE=false

for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --skip-tests) SKIP_TESTS=true ;;
        --skip-release) SKIP_RELEASE=true ;;
        --skip-parquet) SKIP_PARQUET=true ;;
        --skip-repl) SKIP_REPL=true ;;
        --skip-compile) SKIP_COMPILE=true ;;
        *) echo "error: unknown option: $arg" >&2; exit 1 ;;
    esac
done

if [[ "$SKIP_BUILD" == false ]]; then
    echo "▸ configuring debug build"
    cmake -B "$BUILD_DIR" -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
    echo "▸ building debug"
    cmake --build "$BUILD_DIR" --parallel
fi

if [[ "$SKIP_TESTS" == false ]]; then
    echo "▸ running tests"
    ctest --test-dir "$BUILD_DIR" --output-on-failure
fi

if [[ "$SKIP_RELEASE" == false ]]; then
    echo "▸ configuring release build"
    cmake -B "$RELEASE_DIR" -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release
    echo "▸ building release"
    cmake --build "$RELEASE_DIR" --parallel
fi

if [[ "$SKIP_PARQUET" == false ]]; then
    echo "▸ building parquet plugin"
    BUILD_TYPE=Release IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" "$IBEX_ROOT/scripts/ibex-parquet-build.sh"
fi

if [[ "$SKIP_REPL" == false ]]; then
    echo "▸ REPL smoke (csv plugin)"
    repl_out="$(mktemp)"
    printf ":load tests/data/iris.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/libraries" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    if rg -n "error:" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    echo "▸ REPL smoke (parquet plugin)"
    repl_out="$(mktemp)"
    printf ":load tests/data/parquet_smoke.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$IBEX_ROOT/libraries" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    if rg -n "error:" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"
fi

if [[ "$SKIP_COMPILE" == false ]]; then
    echo "▸ transpile (csv)"
    out_cpp="$(mktemp --suffix=.cpp)"
    "$BUILD_DIR/tools/ibex_compile" "$IBEX_ROOT/tests/data/compile_csv.ibex" -o "$out_cpp"
    rg -n "read_csv\\(\\\"data/iris.csv\\\"\\)" "$out_cpp" >/dev/null
    rm -f "$out_cpp"

    echo "▸ transpile (parquet)"
    out_cpp="$(mktemp --suffix=.cpp)"
    "$BUILD_DIR/tools/ibex_compile" "$IBEX_ROOT/tests/data/compile_parquet.ibex" -o "$out_cpp"
    rg -n "read_parquet\\(\\\"data/flights-1m.parquet\\\"\\)" "$out_cpp" >/dev/null
    rm -f "$out_cpp"
fi

echo "✓ e2e checks passed"
