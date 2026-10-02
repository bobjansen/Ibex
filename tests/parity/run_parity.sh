#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

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

# Some upstream Clang + libstdc++ combinations (e.g. clang-18 in the
# clang-werror CI job) reject <expected> unless __cpp_concepts is forced to
# 202002L. CMake's ibex_compiler_options target carries this workaround (see
# cmake/CompilerOptions.cmake), but this script compiles standalone via a raw
# $CXX invocation, so it has to detect and apply it independently.
std_expected_works() {
    printf '#include <expected>\nint main() { std::expected<int, int> x = 1; return *x; }\n' \
        | "$CXX" -x c++ -std="$CXX_STD_FLAG" - -o /dev/null >/dev/null 2>&1
}

CONCEPTS_WORKAROUND=()
if ! std_expected_works; then
    CONCEPTS_WORKAROUND=(-D__cpp_concepts=202002L -Wno-builtin-macro-redefined)
fi

EXTRA_CXXFLAGS=()
if ((${#CONCEPTS_WORKAROUND[@]})); then
    EXTRA_CXXFLAGS+=("${CONCEPTS_WORKAROUND[@]}")
fi
if [[ -n "$PARITY_CXXFLAGS" ]]; then
    # shellcheck disable=SC2206
    EXTRA_CXXFLAGS+=($PARITY_CXXFLAGS)
fi

EXTRA_LDFLAGS=()
if [[ -n "$PARITY_LDFLAGS" ]]; then
    # shellcheck disable=SC2206
    EXTRA_LDFLAGS=($PARITY_LDFLAGS)
fi

IBEX_EVAL="$BUILD_DIR/tools/ibex_eval"
IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"
STRUCTURED_RUNNER="$SCRIPT_DIR/structured_runner.cpp"
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
    "-isystem"
    "$BUILD_DIR/_deps/robin_hood-src/src/include"
)

if [[ -d "$ROOT_DIR/libs" ]]; then
    while IFS= read -r -d '' lib_dir; do
        IBEX_INCS+=("-I$lib_dir")
    done < <(find "$ROOT_DIR/libs" -mindepth 1 -maxdepth 1 -type d -print0)
fi

IBEX_LIBS=(
    "$BUILD_DIR/src/runtime/libibex_runtime.a"
    "$BUILD_DIR/src/parser/libibex_parser.a"
    "$BUILD_DIR/src/ir/libibex_ir.a"
    "$BUILD_DIR/src/core/libibex_core.a"
)

TMPDIR_WORK="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_WORK"' EXIT

# Conformance gate (see cases/README.md): every `<name>.ibex` the interpreter
# runs must also transpile+match on ibex_compile, OR carry a sibling
# `<name>.unsupported` marker whose first line is a one-line reason. A marked
# case is checked two ways instead — the interpreter must still accept it, and
# ibex_compile must still reject it (a marker that has gone stale fails the
# suite so it gets deleted). An unmarked case that does not transpile fails with
# instructions.
fail=0
for case_file in "$CASES_DIR"/*.ibex; do
    name="$(basename "${case_file%.ibex}")"
    if [[ -n "${PARITY_CASE:-}" && "$name" != "$PARITY_CASE" ]]; then
        continue
    fi
    cpp_file="$TMPDIR_WORK/$name.cpp"
    bin_file="$TMPDIR_WORK/$name.bin"
    marker="$CASES_DIR/$name.unsupported"
    compile_err="$TMPDIR_WORK/$name.compile.err"

    if [[ -f "$marker" ]]; then
        reason="$(head -n 1 "$marker")"
        if ! "$IBEX_EVAL" "$case_file" >/dev/null 2>&1; then
            echo "parity: $name is marked .unsupported but the interpreter rejects it" >&2
            echo "    marker reason: $reason" >&2
            echo "    a parity case must be valid Ibex the interpreter runs" >&2
            fail=1
            continue
        fi
        if "$IBEX_COMPILE" "$case_file" --table-entry-point -o "$cpp_file" >/dev/null 2>&1; then
            echo "parity: $name.unsupported is STALE — ibex_compile now handles this case" >&2
            echo "    delete tests/parity/cases/$name.unsupported and let the case run for real" >&2
            fail=1
            continue
        fi
        echo "parity skip: $name (unsupported: $reason)"
        continue
    fi

    if ! "$IBEX_COMPILE" "$case_file" --table-entry-point -o "$cpp_file" 2>"$compile_err"; then
        echo "parity: $name does not transpile —" >&2
        sed 's/^/    /' "$compile_err" >&2
        echo "    fix ibex_compile, or add tests/parity/cases/$name.unsupported" >&2
        echo "    with a one-line reason (see cases/README.md)" >&2
        fail=1
        continue
    fi
    "$CXX" "${EXTRA_CXXFLAGS[@]}" -std="$CXX_STD_FLAG" \
        "${IBEX_INCS[@]}" "$cpp_file" "$STRUCTURED_RUNNER" "${IBEX_LIBS[@]}" "${EXTRA_LDFLAGS[@]}" -o "$bin_file"
    if ! "$bin_file" "$case_file"; then
        echo "parity mismatch: $name" >&2
        fail=1
    else
        echo "parity ok: $name"
    fi
done

# Effect cases: scripts whose statements have effects (a table sink between two
# reads), where the order the statements run in is the thing under test. The
# table comparison above cannot see it, so run the transpiled program and the
# interpreter and compare what each prints. A case reads back what it writes, so
# a statement that ran out of order changes the printed table.
EFFECT_CASES_DIR="$SCRIPT_DIR/effect_cases"
SQLITE_DRIVER="$(sed -n 's/^ADBC_DRIVER_SQLITE_LIBRARY:FILEPATH=//p' "$BUILD_DIR/CMakeCache.txt" 2>/dev/null)"
FAST_FLOAT_INC="$BUILD_DIR/_deps/fast_float-src/include"
EFFECT_INCS=("${IBEX_INCS[@]}")
if [[ -d "$FAST_FLOAT_INC" ]]; then
    EFFECT_INCS+=("-isystem" "$FAST_FLOAT_INC")
fi
if [[ -d "$EFFECT_CASES_DIR" ]]; then
    for case_file in "$EFFECT_CASES_DIR"/*.ibex; do
        [[ -e "$case_file" ]] || break
        name="$(basename "${case_file%.ibex}")"
        if [[ -n "${PARITY_CASE:-}" && "$name" != "$PARITY_CASE" ]]; then
            continue
        fi
        cpp_file="$TMPDIR_WORK/effect_$name.cpp"
        bin_file="$TMPDIR_WORK/effect_$name.bin"
        # A case that reads a database names the SQLite ADBC driver by placeholder:
        # the driver is wherever this build found it, and a build without it skips
        # the case rather than failing.
        if grep -q '@SQLITE_DRIVER@' "$case_file"; then
            if [[ -z "$SQLITE_DRIVER" || ! -f "$SQLITE_DRIVER" ]]; then
                echo "parity skip: effect case $name (ADBC SQLite driver not found)"
                continue
            fi
            sed "s#@SQLITE_DRIVER@#$SQLITE_DRIVER#g" "$case_file" >"$TMPDIR_WORK/effect_$name.ibex"
            case_file="$TMPDIR_WORK/effect_$name.ibex"
        fi
        if ! "$IBEX_COMPILE" "$case_file" --import-path "$BUILD_DIR/tools" -o "$cpp_file" \
            2>"$TMPDIR_WORK/effect_$name.compile.err"; then
            echo "parity: effect case $name does not transpile —" >&2
            sed 's/^/    /' "$TMPDIR_WORK/effect_$name.compile.err" >&2
            fail=1
            continue
        fi
        # A program that includes adbc.hpp links the ADBC client library, the Arrow
        # bridge and the driver manager (see scripts/ibex-build.sh).
        EFFECT_LIBS=("${IBEX_LIBS[@]}")
        if grep -q '#include "adbc.hpp"' "$cpp_file"; then
            EFFECT_LIBS=("$BUILD_DIR/libs/adbc/libibex_adbc.a" "$BUILD_DIR/src/interop/libibex_interop.a"
                "$BUILD_DIR/libs/adbc/libibex_adbc_driver_manager.a" "${IBEX_LIBS[@]}" -ldl)
        fi
        "$CXX" "${EXTRA_CXXFLAGS[@]}" -std="$CXX_STD_FLAG" "${EFFECT_INCS[@]}" "$cpp_file" \
            "${EFFECT_LIBS[@]}" "${EXTRA_LDFLAGS[@]}" -o "$bin_file"
        rm -f /tmp/ibex_parity_"$name"*
        if ! "$bin_file" >"$TMPDIR_WORK/effect_$name.compiled.out" 2>&1; then
            echo "parity mismatch: effect case $name — the transpiled program failed:" >&2
            sed 's/^/    /' "$TMPDIR_WORK/effect_$name.compiled.out" >&2
            fail=1
            rm -f /tmp/ibex_parity_"$name"*
            continue
        fi
        rm -f /tmp/ibex_parity_"$name"*
        if ! "$IBEX_EVAL" "$case_file" --plugin-path "$BUILD_DIR/tools" \
            >"$TMPDIR_WORK/effect_$name.interp.out" 2>&1; then
            echo "parity: effect case $name — the interpreter failed:" >&2
            sed 's/^/    /' "$TMPDIR_WORK/effect_$name.interp.out" >&2
            fail=1
            rm -f /tmp/ibex_parity_"$name"*
            continue
        fi
        rm -f /tmp/ibex_parity_"$name"*
        # The interpreter also prints each effect statement's own value (a sink's
        # row count); the final table, from its `rows:` line on, is the result.
        for side in interp compiled; do
            sed -n '/^rows: /,$p' "$TMPDIR_WORK/effect_$name.$side.out" \
                >"$TMPDIR_WORK/effect_$name.$side.table"
        done
        if ! diff -u "$TMPDIR_WORK/effect_$name.interp.table" "$TMPDIR_WORK/effect_$name.compiled.table"; then
            echo "parity mismatch: effect case $name (interpreted vs transpiled)" >&2
            fail=1
        else
            echo "parity ok: effect case $name"
        fi
    done
fi

# Orphan markers (no matching case) are almost always a rename left half-done.
for marker in "$CASES_DIR"/*.unsupported; do
    [[ -e "$marker" ]] || break
    if [[ ! -f "${marker%.unsupported}.ibex" ]]; then
        echo "parity: $(basename "$marker") has no matching .ibex case" >&2
        fail=1
    fi
done

exit "$fail"
