#!/usr/bin/env bash
# ibex-run.sh — transpile an .ibex file to C++, compile it, and run it.
#
# Usage:
#   ibex-run.sh <file.ibex> [-- arg1 arg2 ...]
#   ibex-run.sh tests/data/iris.ibex
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

detect_cxx_std_flag() {
    local candidate
    for candidate in c++23 gnu++23 c++2b gnu++2b; do
        if printf 'int main() { return 0; }\n' | "$CXX" -x c++ -std="$candidate" - -o /dev/null >/dev/null 2>&1; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    echo "error: unable to find a supported C++23-or-newer standard flag for $CXX" >&2
    return 1
}

CXX_STD_FLAG="$(detect_cxx_std_flag)"

IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"

# ── Arg parsing ──────────────────────────────────────────────────────────────
if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
    echo "Usage: $(basename "$0") <file.ibex> [-- program-args...]"
    exit 1
fi

IBEX_FILE="$1"; shift
# Anything after -- is passed to the compiled binary
PROG_ARGS=()
if [[ $# -gt 0 && "$1" == "--" ]]; then
    shift; PROG_ARGS=("$@")
fi

if [[ ! -f "$IBEX_FILE" ]]; then
    echo "error: file not found: $IBEX_FILE" >&2; exit 1
fi
if [[ ! -x "$IBEX_COMPILE" ]]; then
    echo "error: ibex_compile not found at $IBEX_COMPILE" >&2
    echo "       Run: cmake --build $BUILD_DIR --target ibex_compile_bin" >&2
    exit 1
fi

# ── Include / library paths from the build tree ──────────────────────────────
IBEX_INCS=(
    "-I$IBEX_ROOT/include"
    "-I$IBEX_ROOT/libraries"
    "-I$BUILD_DIR/_deps/fmt-src/include"
    "-I$BUILD_DIR/_deps/spdlog-src/include"
    "-I$BUILD_DIR/_deps/robin_hood-src/src/include"
)

# Bundled extern headers live under libs/<name> (e.g. libs/csv/csv.hpp).
if [[ -d "$IBEX_ROOT/libs" ]]; then
    while IFS= read -r -d '' lib_dir; do
        IBEX_INCS+=("-I$lib_dir")
    done < <(find "$IBEX_ROOT/libs" -mindepth 1 -maxdepth 1 -type d -print0)
fi

# Support both debug (libfmtd.a) and release (libfmt.a) build trees.
_fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmt.a"
[[ -f "$_fmt_lib" ]] || _fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmtd.a"

_spdlog_lib="$BUILD_DIR/_deps/spdlog-build/libspdlog.a"
[[ -f "$_spdlog_lib" ]] || _spdlog_lib="$BUILD_DIR/_deps/spdlog-build/libspdlogd.a"

# jemalloc: pools large freed allocations instead of returning them to the OS,
# eliminating page-fault overhead on repeated large-table operations.
# The versioned .so.2 name is used because most distros don't ship an unversioned symlink.
_jemalloc=""
for _candidate in \
        "$(ldconfig -p 2>/dev/null | awk '/libjemalloc\.so\.2/{print $NF; exit}')" \
        /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
        /usr/lib/aarch64-linux-gnu/libjemalloc.so.2 \
        /usr/lib/libjemalloc.so.2 \
        /usr/local/lib/libjemalloc.so.2; do
    if [[ -n "$_candidate" && -f "$_candidate" ]]; then
        _jemalloc="$_candidate"
        break
    fi
done

IBEX_LIBS=(
    "$BUILD_DIR/src/runtime/libibex_runtime.a"
    "$BUILD_DIR/src/ir/libibex_ir.a"
    "$BUILD_DIR/src/core/libibex_core.a"
    "$_fmt_lib"
    "$_spdlog_lib"
)
[[ -n "$_jemalloc" ]] && IBEX_LIBS+=("$_jemalloc")

# ── Transpile → compile → run ─────────────────────────────────────────────────
TMPDIR_WORK="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_WORK"' EXIT

BASE="$(basename "${IBEX_FILE%.ibex}")"
CPP_FILE="$TMPDIR_WORK/$BASE.cpp"
BIN_FILE="$TMPDIR_WORK/$BASE"

echo "▸ transpiling $IBEX_FILE"
"$IBEX_COMPILE" "$IBEX_FILE" -o "$CPP_FILE"

echo "▸ compiling   $CPP_FILE"
"$CXX" -std="$CXX_STD_FLAG" "${IBEX_INCS[@]}" "$CPP_FILE" "${IBEX_LIBS[@]}" -o "$BIN_FILE"

echo "▸ running"
echo ""
# When jemalloc is linked, configure a 30-second dirty-page decay window.
# This keeps recently freed large allocations physically resident so that
# repeated operations on large tables avoid per-call page-fault overhead.
# The caller can override by setting MALLOC_CONF in the environment beforehand.
if [[ -n "$_jemalloc" ]]; then
    export MALLOC_CONF="${MALLOC_CONF:-dirty_decay_ms:30000,muzzy_decay_ms:30000}"
fi
"$BIN_FILE" "${PROG_ARGS[@]}"
