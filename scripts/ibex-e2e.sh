#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

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

    echo "▸ REPL smoke (first-party parquet backend)"
    repl_out="$(mktemp)"
    printf ":load tests/data/parquet_builtin_smoke.ibex\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    if rg -n "error:" "$repl_out" >/dev/null; then
        cat "$repl_out" >&2
        rm -f "$repl_out"
        exit 1
    fi
    rm -f "$repl_out"

    echo "▸ REPL smoke (parquet plugin, string chunk-boundary check)"
    repl_out="$(mktemp)"
    # A tiny chunk budget forces hundreds of write-side string chunks on a small
    # column, standing in for the >2 GB column that used to fail outright.
    printf ":load tests/data/parquet_string_chunk_check.ibex\n:quit\n" \
        | IBEX_PARQUET_STRING_CHUNK_BYTES=1024 IBEX_LIBRARY_PATH="$BUILD_DIR/tools" \
          "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_string_chunk_check_out.parquet"
    if rg -n "error:" "$repl_out" >/dev/null || ! rg -n "1250025000" "$repl_out" >/dev/null; then
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

    echo "▸ REPL smoke (:peek reports a table's order-sensitive claims)"
    repl_out="$(mktemp)"
    printf ":load tests/data/table_claims_check.ibex\n:peek quotes\n:peek live\n:peek windowed\n:quit\n" \
        | IBEX_LIBRARY_PATH="$BUILD_DIR/tools" "$BUILD_DIR/tools/ibex" >"$repl_out" 2>&1
    # `quotes` carries nothing; `live` still carries the ordering its binding
    # was given three statements earlier; `windowed` carries all three. The
    # first assertion is the one that would pass vacuously if :peek printed
    # nothing at all, so the other two have to find their text.
    if rg -n "error:" "$repl_out" >/dev/null \
        || ! rg -n "ordered by: ts asc" "$repl_out" >/dev/null \
        || ! rg -n "time_index: ts.*grouped by: sym" "$repl_out" >/dev/null \
        || [ "$(rg -c "ordered by" "$repl_out")" != "2" ]; then
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

    echo "▸ whole-script (parquet plugin, fixed-width decode split across row groups)"
    dec_st="$(mktemp)"
    dec_mt="$(mktemp)"
    IBEX_CORES=1 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_row_group_decode_check.ibex" >"$dec_st" 2>&1
    IBEX_CORES=8 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_row_group_decode_check.ibex" >"$dec_mt" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_row_group_decode_out.parquet"
    # Every value moves if a row-group range writes at the wrong offset — see
    # the .ibex file. The two runs must also be byte-identical.
    if rg -n "error:" "$dec_st" >/dev/null \
        || ! rg -n "\| 1300000 +\| 845000650000 +\| 1 +\| 1300000 +\| 2762500 +\| 433334 +\| 433333 +\| 650000 +\|" "$dec_st" >/dev/null \
        || ! diff -q "$dec_st" "$dec_mt" >/dev/null; then
        echo "--- single-threaded ---" >&2
        cat "$dec_st" >&2
        echo "--- multi-threaded ---" >&2
        cat "$dec_mt" >&2
        rm -f "$dec_st" "$dec_mt"
        exit 1
    fi
    rm -f "$dec_st" "$dec_mt"

    echo "▸ whole-script (parquet plugin, streamed scan across row-group dictionaries)"
    # Streaming makes a lazy source arrive one ROW GROUP at a time instead of as
    # one table (pipelined-execution plan, Phases 1-2). It is the DEFAULT, so
    # the comparison run is the one that opts out with IBEX_STREAM_SCAN=0 —
    # getting that backwards would compare streaming against streaming and pass
    # without testing anything.
    #
    # The Categorical column is the interesting case. Parquet writes one
    # dictionary per row group and the writer above builds these two to
    # DISAGREE — "gamma" sits at the code "alpha" occupies in the other group —
    # so a streamed chunk carries codes that mean nothing outside its own unit.
    # The scan operator remaps them onto a dictionary shared by all its chunks.
    # Without that remap this reports alpha 624288 and no gamma at all, with the
    # row count still right. Nothing in the PDS-H suite is dictionary-encoded,
    # so this is the only coverage that remap has.
    #
    # Written and read by two different scripts on purpose: only a batch-
    # eligible script reaches the streaming path, and one with a write_parquet
    # side effect is not one. The writer runs first for its file.
    "$BUILD_DIR/tools/ibex_eval" --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_categorical_check.ibex" >/dev/null 2>&1
    str_off="$(mktemp)"
    str_on="$(mktemp)"
    IBEX_STREAM_SCAN=0 "$BUILD_DIR/tools/ibex_eval" --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_stream_categorical_check.ibex" >"$str_off" 2>&1
    IBEX_STREAM_SCAN=1 IBEX_PROFILE_OPERATORS=1 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_stream_categorical_check.ibex" >"$str_on" 2>&1
    # A third run, streaming but NOT parallel. A serial scan decodes one unit
    # per window and never submits to the pool, which is a different control
    # path through the window loop and not a cosmetic difference: it is where
    # every unit after the first was once dropped, silently and with the row
    # count simply coming out short.
    str_serial="$(mktemp)"
    IBEX_STREAM_SCAN=1 IBEX_CORES=1 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_stream_categorical_check.ibex" >"$str_serial" 2>&1
    # An all-rejected streamed scan must still preserve its empty output
    # schema. Otherwise the source emits no chunks and MaterializeOperator
    # returns a column-less Table, unlike the eager path.
    empty_off="$(mktemp)"
    empty_on="$(mktemp)"
    IBEX_STREAM_SCAN=0 "$BUILD_DIR/tools/ibex_eval" --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_stream_empty_check.ibex" >"$empty_off" 2>&1
    IBEX_STREAM_SCAN=1 "$BUILD_DIR/tools/ibex_eval" --plugin-path "$BUILD_DIR/tools" \
        "$IBEX_ROOT/tests/data/parquet_stream_empty_check.ibex" >"$empty_on" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_categorical_check_out.parquet"
    # chunks=2 is the part that keeps this honest: without it a silently
    # declined stream would pass the comparison by being the same code path.
    if rg -n "error:" "$str_on" >/dev/null \
        || ! rg -n 'op="scan [^"]*" .*chunks=2' "$str_on" >/dev/null \
        || ! rg -n '"alpha" \| 524288' "$str_on" >/dev/null \
        || ! rg -n '"beta"\s+\| 624288' "$str_on" >/dev/null \
        || ! rg -n '"gamma" \| 100000' "$str_on" >/dev/null \
        || ! diff -q <(rg -v "^(profile |operator profile:)" "$str_off") \
                     <(rg -v "^(profile |operator profile:)" "$str_on") >/dev/null \
        || ! diff -q "$str_off" "$str_serial" >/dev/null \
        || rg -n "error:" "$empty_on" >/dev/null \
        || ! diff -q "$empty_off" "$empty_on" >/dev/null; then
        echo "--- materialized ---" >&2
        cat "$str_off" >&2
        echo "--- streamed ---" >&2
        cat "$str_on" >&2
        echo "--- streamed, serial ---" >&2
        cat "$str_serial" >&2
        echo "--- empty materialized ---" >&2
        cat "$empty_off" >&2
        echo "--- empty streamed ---" >&2
        cat "$empty_on" >&2
        rm -f "$str_off" "$str_on" "$str_serial" "$empty_off" "$empty_on"
        exit 1
    fi
    rm -f "$str_off" "$str_on" "$str_serial" "$empty_off" "$empty_on"

    echo "▸ whole-script (parquet plugin, fused key scan across row groups)"
    # Run through ibex_eval, not the REPL: the fused key scan only exists on the
    # whole-script planner's path (it comes from dynamic filter pushdown, which
    # needs the whole plan), and a `:load` in the REPL never reaches it.
    #
    # Run it twice, single- and multi-threaded, and require the SAME BYTES. The
    # scan hands one row group to each worker, so a merge in completion order
    # rather than file order would permute the selection — which changes the
    # key/payload totals but not the row count.
    scan_st="$(mktemp)"
    scan_mt="$(mktemp)"
    IBEX_CORES=1 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" "$IBEX_ROOT/tests/data/parquet_key_scan_check.ibex" \
        >"$scan_st" 2>&1
    IBEX_CORES=8 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" "$IBEX_ROOT/tests/data/parquet_key_scan_check.ibex" \
        >"$scan_mt" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_key_scan_facts.parquet" \
        "$IBEX_ROOT/tests/data/parquet_key_scan_dims.parquet"
    # 13 rows / key total 10047253 / payload total 48 — see the .ibex file for
    # where each number comes from.
    if rg -n "error:" "$scan_st" >/dev/null \
        || ! rg -n "\| 13 +\| 10047253 +\| 1 +\| 1300000 +\| 48 +\|" "$scan_st" >/dev/null \
        || ! diff -q "$scan_st" "$scan_mt" >/dev/null; then
        echo "--- single-threaded ---" >&2
        cat "$scan_st" >&2
        echo "--- multi-threaded ---" >&2
        cat "$scan_mt" >&2
        rm -f "$scan_st" "$scan_mt"
        exit 1
    fi
    rm -f "$scan_st" "$scan_mt"

    echo "▸ whole-script (parquet plugin, fused string filter across row groups)"
    # Same shape for the `like` a query never reads its column outside the
    # filter: matched inside the decoder, one row group per worker. Run
    # single- and multi-threaded and require the SAME BYTES, because a merge in
    # completion order would permute the selection and move the id total.
    #
    # The profile is checked too, not only the answer: without the fused scan
    # this query still returns exactly these numbers, just by materializing the
    # text column first. "source string filter scan" is what says the
    # optimization ran at all.
    str_st="$(mktemp)"
    str_mt="$(mktemp)"
    IBEX_CORES=1 IBEX_PROFILE_OPERATORS=1 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" "$IBEX_ROOT/tests/data/parquet_string_filter_check.ibex" \
        >"$str_st" 2>&1
    IBEX_CORES=8 "$BUILD_DIR/tools/ibex_eval" \
        --plugin-path "$BUILD_DIR/tools" "$IBEX_ROOT/tests/data/parquet_string_filter_check.ibex" \
        >"$str_mt" 2>&1
    rm -f "$IBEX_ROOT/tests/data/parquet_string_filter.parquet"
    # 1114286 rows / id total 724286828571 — see the .ibex file for the
    # arithmetic behind both.
    if rg -n "error:" "$str_st" >/dev/null \
        || ! rg -n "source string filter scan" "$str_st" >/dev/null \
        || ! rg -n "\| 1114286 +\| 724286828571 +\| 1 +\| 1300000 +\|" "$str_st" >/dev/null \
        || ! diff -q <(rg -v "^(operator )?profile" "$str_st") "$str_mt" >/dev/null; then
        echo "--- single-threaded ---" >&2
        cat "$str_st" >&2
        echo "--- multi-threaded ---" >&2
        cat "$str_mt" >&2
        rm -f "$str_st" "$str_mt"
        exit 1
    fi
    rm -f "$str_st" "$str_mt"

    if command -v uv >/dev/null 2>&1; then
        echo "▸ whole-script (parquet plugin, fused string filter over a nullable column)"
        # The fixture needs pyarrow to write a PLAIN *nullable* string column
        # over several row groups; write_parquet cannot. Nulls send the fused
        # scan down its level-aware branch, where values arrive compacted and
        # definition levels map them back to rows.
        uv run --project "$IBEX_ROOT" python \
            "$IBEX_ROOT/tests/data/gen_parquet_string_filter_nulls.py" \
            "$IBEX_ROOT/tests/data/parquet_string_filter_nulls_out.parquet" >/dev/null
        null_out="$(mktemp)"
        IBEX_PROFILE_OPERATORS=1 "$BUILD_DIR/tools/ibex_eval" \
            --plugin-path "$BUILD_DIR/tools" \
            "$IBEX_ROOT/tests/data/parquet_string_filter_nulls_check.ibex" >"$null_out" 2>&1
        rm -f "$IBEX_ROOT/tests/data/parquet_string_filter_nulls_out.parquet"
        # 400 + 800 = 1200 of 1500 rows; the 300 null rows fail both polarities.
        if rg -n "error:" "$null_out" >/dev/null \
            || ! rg -n "source string filter scan" "$null_out" >/dev/null \
            || ! rg -n "\| 400 +\| 300000 +\| 800 +\| 600000 +\|" "$null_out" >/dev/null; then
            cat "$null_out" >&2
            rm -f "$null_out"
            exit 1
        fi
        rm -f "$null_out"
    fi
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
