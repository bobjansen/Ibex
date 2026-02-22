#!/usr/bin/env bash
# ibex-build.sh — transpile an .ibex file and produce a compiled binary.
#
# Usage:
#   ibex-build.sh <file.ibex> [-o output]
#   ibex-build.sh tests/data/iris.ibex -o ./iris
#
# Without -o, the binary is written next to the source file with no extension.
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

IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"

# ── Arg parsing ──────────────────────────────────────────────────────────────
if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Usage: $(basename "$0") <file.ibex> [-o output]"
    exit 1
fi

IBEX_FILE="$1"; shift
OUTPUT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) OUTPUT="$2"; shift 2 ;;
        *)  echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$OUTPUT" ]]; then
    OUTPUT="${IBEX_FILE%.ibex}"
fi

if [[ ! -f "$IBEX_FILE" ]]; then
    echo "error: file not found: $IBEX_FILE" >&2; exit 1
fi
if [[ ! -x "$IBEX_COMPILE" ]]; then
    echo "error: ibex_compile not found at $IBEX_COMPILE" >&2
    echo "       Run: cmake --build $BUILD_DIR --target ibex_compile_bin" >&2
    exit 1
fi

# ── Include / library paths ───────────────────────────────────────────────────
IBEX_INCS=(
    "-I$IBEX_ROOT/include"
    "-I$BUILD_DIR/_deps/fmt-src/include"
    "-I$BUILD_DIR/_deps/spdlog-src/include"
    "-I$BUILD_DIR/_deps/robin_hood-src/src/include"
)

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

# ── Transpile then compile ────────────────────────────────────────────────────
TMPDIR_WORK="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_WORK"' EXIT

BASE="$(basename "${IBEX_FILE%.ibex}")"
CPP_FILE="$TMPDIR_WORK/$BASE.cpp"

echo "▸ transpiling $IBEX_FILE → $CPP_FILE"
"$IBEX_COMPILE" "$IBEX_FILE" -o "$CPP_FILE"

echo "▸ compiling   → $OUTPUT"
"$CXX" -std=c++23 "${IBEX_INCS[@]}" "$CPP_FILE" "${IBEX_LIBS[@]}" -o "$OUTPUT"

echo "✓ built $OUTPUT"
