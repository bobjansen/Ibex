#!/usr/bin/env bash
# ibex-plugin-build.sh — compile a C++ plugin source file into a loadable .so
# for use with the Ibex REPL.
#
# Usage:
#   ibex-plugin-build.sh <plugin.cpp> [-o output.so]
#   ibex-plugin-build.sh libraries/csv.cpp
#   ibex-plugin-build.sh my_parquet.cpp -o /path/to/parquet.so
#
# Without -o, the .so is written next to the source file (stem + ".so").
#
# The plugin source must export:
#   extern "C" void ibex_register(ibex::runtime::ExternRegistry*);
#
# Environment overrides:
#   IBEX_ROOT   — repo root          (default: directory above this script)
#   BUILD_DIR   — cmake build dir    (default: $IBEX_ROOT/build)
#   CXX         — C++ compiler       (default: clang++)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
CXX="${CXX:-clang++}"

# ── Arg parsing ──────────────────────────────────────────────────────────────
if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Usage: $(basename "$0") <plugin.cpp> [-o output.so]"
    echo ""
    echo "Compiles a C++ plugin into a shared library loadable by the Ibex REPL."
    echo "The plugin must export:  extern \"C\" void ibex_register(ibex::runtime::ExternRegistry*);"
    exit 1
fi

CPP_FILE="$1"; shift
OUTPUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) OUTPUT="$2"; shift 2 ;;
        *)  echo "error: unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$CPP_FILE" ]]; then
    echo "error: file not found: $CPP_FILE" >&2; exit 1
fi

if [[ -z "$OUTPUT" ]]; then
    # Default: stem.so next to the source file
    DIR="$(cd "$(dirname "$CPP_FILE")" && pwd)"
    BASE="$(basename "${CPP_FILE%.cpp}")"
    OUTPUT="$DIR/$BASE.so"
fi

# ── Include paths ─────────────────────────────────────────────────────────────
IBEX_INCS=(
    "-I$IBEX_ROOT/include"
    "-I$IBEX_ROOT/libraries"
    "-I$BUILD_DIR/_deps/fmt-src/include"
    "-I$BUILD_DIR/_deps/spdlog-src/include"
    "-I$BUILD_DIR/_deps/robin_hood-src/src/include"
)

# ── Runtime libraries ─────────────────────────────────────────────────────────
_fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmt.a"
[[ -f "$_fmt_lib" ]] || _fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmtd.a"

_spdlog_lib="$BUILD_DIR/_deps/spdlog-build/libspdlog.a"
[[ -f "$_spdlog_lib" ]] || _spdlog_lib="$BUILD_DIR/_deps/spdlog-build/libspdlogd.a"

IBEX_LIBS=(
    "$BUILD_DIR/src/runtime/libibex_runtime.a"
    "$BUILD_DIR/src/ir/libibex_ir.a"
    "$BUILD_DIR/src/core/libibex_core.a"
    "$_fmt_lib"
    "$_spdlog_lib"
)

# ── Validate build tree ───────────────────────────────────────────────────────
for lib in "${IBEX_LIBS[@]}"; do
    if [[ ! -f "$lib" ]]; then
        echo "error: required library not found: $lib" >&2
        echo "       Run: cmake --build $BUILD_DIR" >&2
        exit 1
    fi
done

# ── Compile ───────────────────────────────────────────────────────────────────
echo "▸ compiling plugin $CPP_FILE → $OUTPUT"
"$CXX" -std=c++23 -fPIC -shared \
    "${IBEX_INCS[@]}" \
    "$CPP_FILE" \
    "${IBEX_LIBS[@]}" \
    -o "$OUTPUT"

echo "✓ built $OUTPUT"
