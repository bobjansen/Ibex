#!/usr/bin/env bash
# bench_ibex.sh — run ibex_bench and write results/ibex.tsv.
#
# ibex_bench output format:
#   bench <name>: iters=N, total_ms=X.XXX, avg_ms=X.XXX, rows=N
#
# The first two queries (mean_by_symbol, ohlc_by_symbol) are timed with
# --no-include-parse (pure interpreter execution, no parse/lower overhead).
# The parse_* variants are always timed with parse+lower included by ibex_bench
# internally; we expose them as the "ibex+parse" framework row.
#
# Usage:
#   ./bench_ibex.sh [--csv path] [--csv-multi path] [--warmup N] [--iters N]
#                   [--suite name[,name...]] [--merge-validity-rows N]
#                   [--rng-micro-rows N] [--reshape-rows N]
#                   [--out results/ibex.tsv]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
if [[ -n "${BUILD_DIR:-}" ]]; then
    : # user-specified, keep it
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex_bench" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="$IBEX_ROOT/build"
fi
IBEX_BENCH="$BUILD_DIR/tools/ibex_bench"

# ── Arg parsing ───────────────────────────────────────────────────────────────
CSV="$SCRIPT_DIR/data/prices.csv"
CSV_MULTI="$SCRIPT_DIR/data/prices_multi.csv"
CSV_TRADES="$SCRIPT_DIR/data/trades.csv"
CSV_EVENTS="$SCRIPT_DIR/data/events.csv"
CSV_LOOKUP="$SCRIPT_DIR/data/lookup.csv"
WARMUP=1
ITERS=5
SUITE=""
MERGE_VALIDITY_ROWS=""
RNG_MICRO_ROWS=""
RESHAPE_ROWS=""
OUT="$SCRIPT_DIR/results/ibex.tsv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --csv)         CSV="$2";         shift 2 ;;
        --csv-multi)   CSV_MULTI="$2";   shift 2 ;;
        --csv-trades)  CSV_TRADES="$2";  shift 2 ;;
        --csv-events)  CSV_EVENTS="$2";  shift 2 ;;
        --csv-lookup)  CSV_LOOKUP="$2";  shift 2 ;;
        --warmup)      WARMUP="$2";      shift 2 ;;
        --iters)       ITERS="$2";       shift 2 ;;
        --suite)       SUITE="$2";       shift 2 ;;
        --merge-validity-rows) MERGE_VALIDITY_ROWS="$2"; shift 2 ;;
        --rng-micro-rows) RNG_MICRO_ROWS="$2"; shift 2 ;;
        --reshape-rows) RESHAPE_ROWS="$2"; shift 2 ;;
        --out)         OUT="$2";         shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -x "$IBEX_BENCH" ]]; then
    echo "error: ibex_bench not found at $IBEX_BENCH" >&2
    echo "       Run: cmake --build $BUILD_DIR" >&2
    exit 1
fi
NEEDS_CSV=1
if [[ -n "$SUITE" ]]; then
    NEEDS_CSV=0
    IFS=',' read -r -a SUITE_TOKENS <<< "$SUITE"
    for tok in "${SUITE_TOKENS[@]}"; do
        tok="${tok//[[:space:]]/}"
        tok="${tok,,}"
        tok="${tok//-/_}"
        if [[ "$tok" != "merge_validity" && "$tok" != "rng_micro" ]]; then
            NEEDS_CSV=1
            break
        fi
    done
fi
if [[ "$NEEDS_CSV" -eq 1 && ! -f "$CSV" ]]; then
    echo "error: $CSV not found — run data/gen_data.py first" >&2; exit 1
fi

mkdir -p "$(dirname "$OUT")"

# ── Run ibex_bench ────────────────────────────────────────────────────────────
# --no-include-parse: queries[0,1] (mean_by_symbol, ohlc_by_symbol) measure
#   pure execution only. queries[2,3] (parse_*) always include parse regardless.
BENCH_ARGS=(
    --warmup  "$WARMUP"
    --iters   "$ITERS"
    --no-include-parse
)
[[ "$NEEDS_CSV" -eq 1 ]] && BENCH_ARGS+=(--csv "$CSV")
[[ -n "$SUITE" ]] && BENCH_ARGS+=(--suite "$SUITE")
[[ -n "$MERGE_VALIDITY_ROWS" ]] && BENCH_ARGS+=(--merge-validity-rows "$MERGE_VALIDITY_ROWS")
[[ -n "$RNG_MICRO_ROWS" ]] && BENCH_ARGS+=(--rng-micro-rows "$RNG_MICRO_ROWS")
[[ -n "$RESHAPE_ROWS" ]] && BENCH_ARGS+=(--reshape-rows "$RESHAPE_ROWS")
[[ -f "$CSV_MULTI" ]]   && BENCH_ARGS+=(--csv-multi   "$CSV_MULTI")
[[ -f "$CSV_TRADES" ]]  && BENCH_ARGS+=(--csv-trades  "$CSV_TRADES")
[[ -f "$CSV_EVENTS" ]]  && BENCH_ARGS+=(--csv-events  "$CSV_EVENTS")
[[ -f "$CSV_LOOKUP" ]]  && BENCH_ARGS+=(--csv-lookup  "$CSV_LOOKUP")

echo "=== ibex ===" >&2
raw="$("$IBEX_BENCH" "${BENCH_ARGS[@]}")"
echo "$raw" >&2

# ── Parse to TSV ──────────────────────────────────────────────────────────────
# Input:  bench mean_by_symbol: iters=5, total_ms=12.345, avg_ms=2.469,
#           min_ms=2.1, max_ms=3.0, stddev_ms=0.2, p95_ms=2.9, p99_ms=3.0, rows=252
# Output: ibex<TAB>mean_by_symbol<TAB>2.469<TAB>2.1<TAB>3.0<TAB>0.2<TAB>2.9<TAB>3.0<TAB>252
{
    printf "framework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\trows\n"
    echo "$raw" | awk '
    /^bench / {
        name = $2; sub(/:$/, "", name)
        avg_ms = ""; min_ms = ""; max_ms = ""; stddev_ms = ""; p95_ms = ""; p99_ms = ""; rows = ""
        for (i = 3; i <= NF; i++) {
            v = $i; sub(/,$/, "", v)
            if (v ~ /^avg_ms=/)    { split(v, a, "="); avg_ms    = a[2] }
            if (v ~ /^min_ms=/)    { split(v, a, "="); min_ms    = a[2] }
            if (v ~ /^max_ms=/)    { split(v, a, "="); max_ms    = a[2] }
            if (v ~ /^stddev_ms=/) { split(v, a, "="); stddev_ms = a[2] }
            if (v ~ /^p95_ms=/)    { split(v, a, "="); p95_ms    = a[2] }
            if (v ~ /^p99_ms=/)    { split(v, a, "="); p99_ms    = a[2] }
            if (v ~ /^rows=/)      { split(v, a, "="); rows      = a[2] }
        }
        fw = (name ~ /^parse_/) ? "ibex+parse" : "ibex"
        q  = name; sub(/^parse_/, "", q)
        print fw "\t" q "\t" avg_ms "\t" min_ms "\t" max_ms "\t" stddev_ms "\t" p95_ms "\t" p99_ms "\t" rows
    }'
} > "$OUT"

echo "results written to $OUT" >&2
