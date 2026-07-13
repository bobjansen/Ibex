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
    cmake -B "$BUILD_DIR" -G Ninja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug
    echo "▸ building debug"
    cmake --build "$BUILD_DIR" --parallel
fi

if [[ "$SKIP_TESTS" == false ]]; then
    echo "▸ running tests"
    ctest --test-dir "$BUILD_DIR" --output-on-failure
fi

if [[ "$SKIP_RELEASE" == false ]]; then
    echo "▸ configuring release build"
    cmake -B "$RELEASE_DIR" -G Ninja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release
    echo "▸ building release"
    cmake --build "$RELEASE_DIR" --parallel
fi

if [[ "$SKIP_PARQUET" == false ]]; then
    echo "▸ building parquet plugin"
    IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" "$IBEX_ROOT/scripts/ibex-parquet-build.sh"
fi

if [[ "$SKIP_REPL" == false ]]; then
    echo "▸ building csv plugin"
    cmake --build "$BUILD_DIR" --parallel --target ibex_csv_plugin

    echo "▸ REPL smoke (csv plugin)"
    repl_out="$(mktemp)"
    printf ":load tests/data/iris.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    if rg -n "error:" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    echo "▸ REPL smoke (parquet plugin)"
    repl_out="$(mktemp)"
    printf ":load tests/data/parquet_smoke.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    if rg -n "error:" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    echo "▸ REPL smoke (parquet plugin, chunked read batch-boundary check)"
    repl_out="$(mktemp)"
    printf ":load tests/data/parquet_chunk_check.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_chunk_check_out.parquet"
    if rg -n "error:" "$repl_out" >/dev/null || ! rg -n "20000100000" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    echo "▸ REPL smoke (null keys: group-by / distinct / order / join)"
    repl_out="$(mktemp)"
    printf ":load tests/data/null_keys_check.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    # The data pits nulls against a GENUINE 0 throughout, so each assertion below
    # actually discriminates: the broken code merged the two everywhere.
    #   group-by   : null group of 2 (broken: one 0-group of 4)
    #   update+by  : null rows total 30, zero rows total 3 (broken: both 33)
    #   join       : null-filled label on the left join (broken: matched "zero")
    #   dcast      : null row key keeps its null-ness (broken: printed as 0)
    #   asof join  : null equality key unmatched (broken: matched the k=0 right row)
    if rg -n "error:" "$repl_out" >/dev/null \
        || ! rg -n "^\| null \| 2 " "$repl_out" >/dev/null \
        || ! rg -n "^\| 0    \| 2 " "$repl_out" >/dev/null \
        || ! rg -n "^\| null \| 10  \| 30 " "$repl_out" >/dev/null \
        || ! rg -n "^\| 0    \| 1   \| 3 " "$repl_out" >/dev/null \
        || ! rg -n "^\| null \| 10  \| null " "$repl_out" >/dev/null \
        || ! rg -n "^\| null \| 10  \| 20  " "$repl_out" >/dev/null \
        || ! rg -n "\| null \| 20 \| null " "$repl_out" >/dev/null \
        || ! rg -n "^\| null \| null \| 2 " "$repl_out" >/dev/null \
        || ! rg -n "^\| 0    \| null \| 1 " "$repl_out" >/dev/null \
        || ! rg -n "^\| null \| 0    \| 1 " "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    echo "▸ REPL smoke (parquet plugin, nulls survive a CSV -> parquet -> read round-trip)"
    repl_out="$(mktemp)"
    printf ":load tests/data/parquet_nulls_check.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_nulls_check_out.parquet"
    # 26.66667 is mean(i) with the two nulls skipped; reading them back as 0
    # (which read_parquet used to do, silently) gives 16 instead.
    if rg -n "error:" "$repl_out" >/dev/null \
        || ! rg -n "26.66667" "$repl_out" >/dev/null \
        || ! rg -n "\| \"gamma\" \| 1    \| 3.5" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    if command -v uv >/dev/null 2>&1; then
        echo "▸ REPL smoke (parquet plugin, sparse plain-string read across a compressed page boundary)"
        # The fixture needs pyarrow to control encoding, compression, and page
        # size (see gen_parquet_plain_page_boundary.py); ibex's write_parquet
        # cannot. Without the emit-per-batch fix, row 1023 silently reads as
        # text-002047 — the reused page decompression buffer's content.
        uv run --project "$IBEX_ROOT" python "$IBEX_ROOT/tests/data/gen_parquet_plain_page_boundary.py" \
            "$IBEX_ROOT/tests/data/parquet_plain_page_boundary_out.parquet"
        repl_out="$(mktemp)"
        printf ":load tests/data/parquet_plain_page_boundary_check.ibex\n:quit\n" \
            | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
        rm -f "$IBEX_ROOT/tests/data/parquet_plain_page_boundary_out.parquet"
        if rg -n "error:" "$repl_out" >/dev/null \
            || ! rg -n '"text-001023"' "$repl_out" >/dev/null \
            || ! rg -n '"text-001024"' "$repl_out" >/dev/null; then
            cat "$repl_out" >&2
            rm -f "$repl_out"
            exit 1
        fi
        rm -f "$repl_out"
    else
        echo "▸ SKIP: sparse plain-string page-boundary check (uv not found; fixture needs pyarrow)"
    fi

    echo "▸ REPL smoke (parquet plugin, categorical cross-row-group dictionary remap)"
    repl_out="$(mktemp)"
    printf ":load tests/data/parquet_categorical_check.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_categorical_check_out.parquet"
    # 524288 / 624288 / 100000 are the per-category counts; they only come out
    # right if each row group's local dictionary codes were remapped into one
    # unified dictionary (see the .ibex file). A local-code read reports the same
    # row count with the wrong values, so counting rows alone would not catch it.
    if rg -n "error:" "$repl_out" >/dev/null \
        || ! rg -n "524288" "$repl_out" >/dev/null \
        || ! rg -n "624288" "$repl_out" >/dev/null \
        || ! rg -n "100000" "$repl_out" >/dev/null \
        || ! rg -n "3000000" "$repl_out" >/dev/null \
        || ! rg -n "1995-01-01 00:00:00.000000000" "$repl_out" >/dev/null; then
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

    echo "▸ transpile (parquet https)"
    out_cpp="$(mktemp --suffix=.cpp)"
    "$BUILD_DIR/tools/ibex_compile" "$IBEX_ROOT/tests/data/compile_parquet_https.ibex" -o "$out_cpp"
    rg -n "read_parquet\\(\\\"https://data.example.com/flights-1m.parquet\\\"\\)" "$out_cpp" >/dev/null
    rm -f "$out_cpp"

    echo "▸ transpile (parquet s3)"
    out_cpp="$(mktemp --suffix=.cpp)"
    "$BUILD_DIR/tools/ibex_compile" "$IBEX_ROOT/tests/data/compile_parquet_s3.ibex" -o "$out_cpp"
    rg -n "read_parquet\\(\\\"s3://market-data/flights-1m.parquet\\?region=us-east-1\\\"\\)" "$out_cpp" >/dev/null
    rm -f "$out_cpp"
fi

echo "✓ e2e checks passed"
