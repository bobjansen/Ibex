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
#                   [--out results/ibex.tsv]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
IBEX_BENCH="$BUILD_DIR/tools/ibex_bench"

# ── Arg parsing ───────────────────────────────────────────────────────────────
CSV="$SCRIPT_DIR/data/prices.csv"
CSV_MULTI="$SCRIPT_DIR/data/prices_multi.csv"
CSV_TRADES="$SCRIPT_DIR/data/trades.csv"
CSV_EVENTS="$SCRIPT_DIR/data/events.csv"
CSV_LOOKUP="$SCRIPT_DIR/data/lookup.csv"
WARMUP=1
ITERS=5
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
        --out)         OUT="$2";         shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -x "$IBEX_BENCH" ]]; then
    echo "error: ibex_bench not found at $IBEX_BENCH" >&2
    echo "       Run: cmake --build $BUILD_DIR" >&2
    exit 1
fi
if [[ ! -f "$CSV" ]]; then
    echo "error: $CSV not found — run data/gen_data.py first" >&2; exit 1
fi

mkdir -p "$(dirname "$OUT")"

# ── Run ibex_bench ────────────────────────────────────────────────────────────
# --no-include-parse: queries[0,1] (mean_by_symbol, ohlc_by_symbol) measure
#   pure execution only. queries[2,3] (parse_*) always include parse regardless.
BENCH_ARGS=(
    --csv     "$CSV"
    --warmup  "$WARMUP"
    --iters   "$ITERS"
    --no-include-parse
)
[[ -f "$CSV_MULTI" ]]   && BENCH_ARGS+=(--csv-multi   "$CSV_MULTI")
[[ -f "$CSV_TRADES" ]]  && BENCH_ARGS+=(--csv-trades  "$CSV_TRADES")
[[ -f "$CSV_EVENTS" ]]  && BENCH_ARGS+=(--csv-events  "$CSV_EVENTS")
[[ -f "$CSV_LOOKUP" ]]  && BENCH_ARGS+=(--csv-lookup  "$CSV_LOOKUP")

echo "=== ibex ===" >&2
raw="$("$IBEX_BENCH" "${BENCH_ARGS[@]}")"
echo "$raw" >&2

# ── Parse to TSV ──────────────────────────────────────────────────────────────
# Input:  bench mean_by_symbol: iters=5, total_ms=12.345, avg_ms=2.469, rows=252
# Output: ibex<TAB>mean_by_symbol<TAB>2.469<TAB>252
{
    printf "framework\tquery\tavg_ms\trows\n"
    echo "$raw" | awk '
    /^bench / {
        name = $2; sub(/:$/, "", name)
        avg_ms = ""; rows = ""
        for (i = 3; i <= NF; i++) {
            if ($i ~ /^avg_ms=/) {
                split($i, a, "="); avg_ms = a[2]; sub(/,$/, "", avg_ms)
            }
            if ($i ~ /^rows=/) {
                split($i, a, "="); rows = a[2]; sub(/,$/, "", rows)
            }
        }
        fw = (name ~ /^parse_/) ? "ibex+parse" : "ibex"
        q  = name; sub(/^parse_/, "", q)
        print fw "\t" q "\t" avg_ms "\t" rows
    }'
} > "$OUT"

echo "results written to $OUT" >&2
