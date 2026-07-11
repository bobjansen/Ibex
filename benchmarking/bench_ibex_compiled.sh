#!/usr/bin/env bash
# bench_ibex_compiled.sh — compile ibex queries to C++ and benchmark executables.
#
# Usage:
#   ./bench_ibex_compiled.sh [--csv path] [--warmup N] [--iters N]
#                            [--out results/ibex_compiled.tsv]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
if [[ -n "${BUILD_DIR:-}" ]]; then
    : # user-specified, keep it
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex_compile" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
fi
IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"
CXX="${CXX:-clang++}"
CXXFLAGS="${CXXFLAGS:-}"

# Each compiled query runs as its own process, so peak RSS is captured with GNU
# time (the `time` apt package). When it is unavailable the peak cell is "-".
GNU_TIME=""
if [[ -x /usr/bin/time ]]; then
    GNU_TIME="/usr/bin/time"
fi

lld_ok=0
if "$CXX" -fuse-ld=lld -Wl,--version -x c++ - -o /tmp/ibex_lld_test </dev/null \
    >/dev/null 2>&1; then
    lld_ok=1
    rm -f /tmp/ibex_lld_test
fi

if [[ -z "$CXXFLAGS" ]]; then
    if [[ $lld_ok -eq 1 ]]; then
        CXXFLAGS="-O3 -DNDEBUG -std=gnu++23 -flto=thin -fuse-ld=lld"
    else
        CXXFLAGS="-O3 -DNDEBUG -std=gnu++23"
    fi
fi

# Prefer non-LTO build if lld is unavailable.
if [[ $lld_ok -ne 1 && "$BUILD_DIR" == "$IBEX_ROOT/build-release" ]]; then
    if [[ -x "$IBEX_ROOT/build/tools/ibex_compile" ]]; then
        BUILD_DIR="$IBEX_ROOT/build"
        IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"
    fi
fi

CSV="$SCRIPT_DIR/data/prices.csv"
CSV_MULTI="$SCRIPT_DIR/data/prices_multi.csv"
CSV_TRADES="$SCRIPT_DIR/data/trades.csv"
CSV_EVENTS="$SCRIPT_DIR/data/events.csv"
WARMUP=1
ITERS=5
OUT="$SCRIPT_DIR/results/ibex_compiled.tsv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --csv)         CSV="$2";         shift 2 ;;
        --csv-multi)   CSV_MULTI="$2";   shift 2 ;;
        --csv-trades)  CSV_TRADES="$2";  shift 2 ;;
        --csv-events)  CSV_EVENTS="$2";  shift 2 ;;
        --warmup)      WARMUP="$2";      shift 2 ;;
        --iters)       ITERS="$2";       shift 2 ;;
        --out)         OUT="$2";         shift 2 ;;
        # Accepted-but-ignored: scale_suite passes these to every backend so a
        # uniform driver works; the compiled path doesn't run those shapes.
        --reshape-rows|--tf-rows) shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -x "$IBEX_COMPILE" ]]; then
    echo "error: ibex_compile not found at $IBEX_COMPILE" >&2
    echo "       Run: cmake --build $BUILD_DIR" >&2
    exit 1
fi
if [[ ! -f "$CSV" ]]; then
    echo "error: $CSV not found — run data/gen_data.py first" >&2
    exit 1
fi

TMP_DIR="$SCRIPT_DIR/results/compiled"
mkdir -p "$TMP_DIR" "$(dirname "$OUT")"

compile_query() {
    local name="$1"
    local query="$2"
    local table_name="${3:-prices}"
    local csv_file="${4:-$CSV}"
    local ibex_path="$TMP_DIR/$name.ibex"
    local cpp_path="$TMP_DIR/$name.cpp"
    local bin_path="$TMP_DIR/$name"

    cat > "$ibex_path" <<EOF
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
let ${table_name} = read_csv("${csv_file}");
${query};
EOF

    "$IBEX_COMPILE" "$ibex_path" -o "$cpp_path" \
        --bench --bench-warmup "$WARMUP" --bench-iters "$ITERS"
    if [[ ! -f "$cpp_path" ]]; then
        echo "error: failed to generate $cpp_path" >&2
        exit 1
    fi

    # If the runtime archive contains LLVM bitcode, we need LTO-capable linker.
    if file "$BUILD_DIR/src/runtime/libibex_runtime.a" | grep -q "LLVM IR bitcode"; then
        if [[ $lld_ok -ne 1 ]]; then
            echo "error: libibex_runtime.a is LTO (LLVM bitcode) but lld is unavailable." >&2
            echo "       Build a non-LTO runtime (e.g., build/), or install lld." >&2
            exit 1
        fi
    fi

    fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmt.a"
    if [[ ! -f "$fmt_lib" ]]; then
        fmt_lib="$BUILD_DIR/_deps/fmt-build/libfmtd.a"
    fi
    if [[ ! -f "$fmt_lib" ]]; then
        echo "error: fmt library not found in $BUILD_DIR/_deps/fmt-build" >&2
        exit 1
    fi

    # The runtime archive references symbols defined in the ir and core
    # archives (e.g. ibex::ir::is_row_local_update_expr), so link all three
    # in a group to resolve cross-archive references regardless of order.
    "$CXX" $CXXFLAGS \
        -I"$IBEX_ROOT/include" \
        -I"$IBEX_ROOT/libs/csv" \
        -I"$BUILD_DIR/_deps/fmt-src/include" \
        -I"$BUILD_DIR/_deps/fast_float-src/include" \
        -I"$BUILD_DIR/_deps/robin_hood-src/src/include" \
        "$cpp_path" \
        -Wl,--start-group \
        "$BUILD_DIR/src/runtime/libibex_runtime.a" \
        "$BUILD_DIR/src/ir/libibex_ir.a" \
        "$BUILD_DIR/src/core/libibex_core.a" \
        -Wl,--end-group \
        "$fmt_lib" \
        -o "$bin_path"

    echo "$bin_path"
}

time_bin() {
    # Run the compiled binary once and echo "<avg_ms>\t<peak_rss_mb>".
    # The binary prints "avg_ms=X.XXX" to stderr (timing is internal); when GNU
    # time is available its -v report (also on stderr) carries the peak RSS.
    local bin="$1"
    local out avg peak peak_kb
    if [[ -n "$GNU_TIME" ]]; then
        out="$("$GNU_TIME" -v "$bin" 2>&1 >/dev/null)"
    else
        out="$("$bin" 2>&1 >/dev/null)"
    fi
    avg="$(echo "$out" | grep -oP 'avg_ms=\K[0-9.]+' | head -1)"
    peak="-"
    if [[ -n "$GNU_TIME" ]]; then
        peak_kb="$(echo "$out" | grep -oP 'Maximum resident set size \(kbytes\): \K[0-9]+' | head -1)"
        [[ -n "$peak_kb" ]] && peak="$(awk -v k="$peak_kb" 'BEGIN{printf "%.1f", k/1024.0}')"
    fi
    printf '%s\t%s\n' "$avg" "$peak"
}

declare -a NAMES
declare -a QUERIES

NAMES+=("mean_by_symbol")
QUERIES+=("prices[select {avg_price = mean(price)}, by symbol]")

NAMES+=("ohlc_by_symbol")
QUERIES+=("prices[select {open = first(price), high = max(price), low = min(price), last = last(price)}, by symbol]")

NAMES+=("update_price_x2")
QUERIES+=("prices[update {price_x2 = price * 2}]")

echo "=== ibex (compiled) ===" >&2
{
    printf "framework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\trows\tpeak_rss_mb\n"
    for i in "${!NAMES[@]}"; do
        name="${NAMES[$i]}"
        query="${QUERIES[$i]}"
        bin_path="$(compile_query "$name" "$query")"
        IFS=$'\t' read -r avg_ms peak_mb < <(time_bin "$bin_path")
        printf "ibex-compiled\t%s\t%s\t-\t-\t-\t-\t-\t-\t%s\n" "$name" "$avg_ms" "$peak_mb"
        printf "  %s: avg_ms=%s peak_rss_mb=%s\n" "$name" "$avg_ms" "$peak_mb" >&2
    done

    if [[ -f "$CSV_MULTI" ]]; then
        declare -a MULTI_NAMES=()
        declare -a MULTI_QUERIES=()

        MULTI_NAMES+=("count_by_symbol_day")
        MULTI_QUERIES+=("prices_multi[select {n = count()}, by {symbol, day}]")

        MULTI_NAMES+=("mean_by_symbol_day")
        MULTI_QUERIES+=("prices_multi[select {avg_price = mean(price)}, by {symbol, day}]")

        MULTI_NAMES+=("ohlc_by_symbol_day")
        MULTI_QUERIES+=("prices_multi[select {open = first(price), high = max(price), low = min(price), last = last(price)}, by {symbol, day}]")

        for i in "${!MULTI_NAMES[@]}"; do
            name="${MULTI_NAMES[$i]}"
            query="${MULTI_QUERIES[$i]}"
            bin_path="$(compile_query "$name" "$query" "prices_multi" "$CSV_MULTI")"
            IFS=$'\t' read -r avg_ms peak_mb < <(time_bin "$bin_path")
            printf "ibex-compiled\t%s\t%s\t-\t-\t-\t-\t-\t-\t%s\n" "$name" "$avg_ms" "$peak_mb"
            printf "  %s: avg_ms=%s peak_rss_mb=%s\n" "$name" "$avg_ms" "$peak_mb" >&2
        done
    fi

    if [[ -f "$CSV_TRADES" ]]; then
        declare -a FILTER_NAMES=()
        declare -a FILTER_QUERIES=()

        FILTER_NAMES+=("filter_simple")
        FILTER_QUERIES+=("trades[filter price > 500.0]")

        FILTER_NAMES+=("filter_and")
        FILTER_QUERIES+=("trades[filter price > 500.0 && qty < 100]")

        FILTER_NAMES+=("filter_arith")
        FILTER_QUERIES+=("trades[filter price * qty > 50000.0]")

        FILTER_NAMES+=("filter_or")
        FILTER_QUERIES+=("trades[filter price > 900.0 || qty < 10]")

        for i in "${!FILTER_NAMES[@]}"; do
            name="${FILTER_NAMES[$i]}"
            query="${FILTER_QUERIES[$i]}"
            bin_path="$(compile_query "$name" "$query" "trades" "$CSV_TRADES")"
            IFS=$'\t' read -r avg_ms peak_mb < <(time_bin "$bin_path")
            printf "ibex-compiled\t%s\t%s\t-\t-\t-\t-\t-\t-\t%s\n" "$name" "$avg_ms" "$peak_mb"
            printf "  %s: avg_ms=%s peak_rss_mb=%s\n" "$name" "$avg_ms" "$peak_mb" >&2
        done
    fi

    if [[ -f "$CSV_EVENTS" ]]; then
        declare -a EVENTS_NAMES=()
        declare -a EVENTS_QUERIES=()

        EVENTS_NAMES+=("sum_by_user")
        EVENTS_QUERIES+=("events[select {total = sum(amount)}, by user_id]")

        EVENTS_NAMES+=("filter_events")
        EVENTS_QUERIES+=("events[filter amount > 500.0]")

        for i in "${!EVENTS_NAMES[@]}"; do
            name="${EVENTS_NAMES[$i]}"
            query="${EVENTS_QUERIES[$i]}"
            bin_path="$(compile_query "$name" "$query" "events" "$CSV_EVENTS")"
            IFS=$'\t' read -r avg_ms peak_mb < <(time_bin "$bin_path")
            printf "ibex-compiled\t%s\t%s\t-\t-\t-\t-\t-\t-\t%s\n" "$name" "$avg_ms" "$peak_mb"
            printf "  %s: avg_ms=%s peak_rss_mb=%s\n" "$name" "$avg_ms" "$peak_mb" >&2
        done
    fi
} > "$OUT"

echo "results written to $OUT" >&2
