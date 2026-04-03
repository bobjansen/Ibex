#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
CXX="${CXX:-clang++}"
PARITY_CXXFLAGS="${PARITY_CXXFLAGS:-}"
PARITY_LDFLAGS="${PARITY_LDFLAGS:-}"

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

EXTRA_CXXFLAGS=()
if [[ -n "$PARITY_CXXFLAGS" ]]; then
    # shellcheck disable=SC2206
    EXTRA_CXXFLAGS=($PARITY_CXXFLAGS)
fi

EXTRA_LDFLAGS=()
if [[ -n "$PARITY_LDFLAGS" ]]; then
    # shellcheck disable=SC2206
    EXTRA_LDFLAGS=($PARITY_LDFLAGS)
fi

IBEX_EVAL="$BUILD_DIR/tools/ibex_eval"
IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"
CASES_DIR="$SCRIPT_DIR/cases"

if [[ ! -x "$IBEX_EVAL" ]]; then
    echo "error: ibex_eval not found at $IBEX_EVAL" >&2
    exit 1
fi
if [[ ! -x "$IBEX_COMPILE" ]]; then
    echo "error: ibex_compile not found at $IBEX_COMPILE" >&2
    exit 1
fi

IBEX_INCS=(
    "-I$ROOT_DIR/include"
    "-I$ROOT_DIR/libraries"
    "-I$BUILD_DIR/_deps/fmt-src/include"
    "-I$BUILD_DIR/_deps/spdlog-src/include"
    "-I$BUILD_DIR/_deps/robin_hood-src/src/include"
)

if [[ -d "$ROOT_DIR/libs" ]]; then
    while IFS= read -r -d '' lib_dir; do
        IBEX_INCS+=("-I$lib_dir")
    done < <(find "$ROOT_DIR/libs" -mindepth 1 -maxdepth 1 -type d -print0)
fi

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

TMPDIR_WORK="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_WORK"' EXIT

fail=0
for case_file in "$CASES_DIR"/*.ibex; do
    name="$(basename "${case_file%.ibex}")"
    cpp_file="$TMPDIR_WORK/$name.cpp"
    bin_file="$TMPDIR_WORK/$name.bin"
    interp_out="$TMPDIR_WORK/$name.interp.out"
    transpiled_out="$TMPDIR_WORK/$name.transpiled.out"
    diff_out="$TMPDIR_WORK/$name.diff"

    "$IBEX_EVAL" "$case_file" >"$interp_out"
    "$IBEX_COMPILE" "$case_file" -o "$cpp_file"
    "$CXX" "${EXTRA_CXXFLAGS[@]}" -std="$CXX_STD_FLAG" \
        "${IBEX_INCS[@]}" "$cpp_file" "${IBEX_LIBS[@]}" "${EXTRA_LDFLAGS[@]}" -o "$bin_file"
    "$bin_file" >"$transpiled_out"

    if ! diff -u "$interp_out" "$transpiled_out" >"$diff_out"; then
        echo "parity mismatch: $name" >&2
        cat "$diff_out" >&2
        fail=1
    else
        echo "parity ok: $name"
    fi
done

exit "$fail"
