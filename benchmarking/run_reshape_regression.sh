#!/usr/bin/env bash
# run_reshape_regression.sh — guard melt/dcast performance for Ibex.
#
# Usage:
#   ./benchmarking/run_reshape_regression.sh
#   ./benchmarking/run_reshape_regression.sh --melt-max-ms 7 --dcast-max-ms 35

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"

if [[ -n "${BUILD_DIR:-}" ]]; then
    : # user-specified
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex_bench" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="$IBEX_ROOT/build"
fi

DATA_DIR="$SCRIPT_DIR/data"
CSV="$DATA_DIR/prices.csv"
CSV_MULTI="$DATA_DIR/prices_multi.csv"
CSV_TRADES="$DATA_DIR/trades.csv"
CSV_EVENTS="$DATA_DIR/events.csv"
CSV_LOOKUP="$DATA_DIR/lookup.csv"

WARMUP=2
ITERS=15
RESHAPE_ROWS=""
MELT_MAX_MS=8.0
DCAST_MAX_MS=40.0
OUT="$SCRIPT_DIR/results/ibex_reshape_regression.tsv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir)    BUILD_DIR="$2"; shift 2 ;;
        --csv)          CSV="$2"; shift 2 ;;
        --csv-multi)    CSV_MULTI="$2"; shift 2 ;;
        --csv-trades)   CSV_TRADES="$2"; shift 2 ;;
        --csv-events)   CSV_EVENTS="$2"; shift 2 ;;
        --csv-lookup)   CSV_LOOKUP="$2"; shift 2 ;;
        --warmup)       WARMUP="$2"; shift 2 ;;
        --iters)        ITERS="$2"; shift 2 ;;
        --reshape-rows) RESHAPE_ROWS="$2"; shift 2 ;;
        --melt-max-ms)  MELT_MAX_MS="$2"; shift 2 ;;
        --dcast-max-ms) DCAST_MAX_MS="$2"; shift 2 ;;
        --out)          OUT="$2"; shift 2 ;;
        -h|--help)
            cat <<'EOF'
Usage: run_reshape_regression.sh [options]

Options:
  --build-dir <path>      Build dir containing tools/ibex_bench
  --csv <path>            prices.csv (required by bench_ibex.sh)
  --csv-multi <path>      prices_multi.csv used by reshape suite
  --csv-trades <path>     trades.csv
  --csv-events <path>     events.csv
  --csv-lookup <path>     lookup.csv
  --warmup <N>            Warmup iterations (default: 2)
  --iters <N>             Timed iterations (default: 15)
  --reshape-rows <N>      Wide rows for reshape synthetic data (default: ibex_bench default)
  --melt-max-ms <X>       Max allowed avg_ms for melt_wide_to_long (default: 8.0)
  --dcast-max-ms <X>      Max allowed avg_ms for dcast_long_to_wide (default: 40.0)
  --out <path>            Output TSV path (default: benchmarking/results/ibex_reshape_regression.tsv)
EOF
            exit 0
            ;;
        *)
            echo "error: unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ ! -f "$CSV" ]]; then
    echo "error: missing $CSV" >&2
    echo "hint: run 'uv run --project . benchmarking/data/gen_data.py benchmarking/data'" >&2
    exit 1
fi
if [[ ! -f "$CSV_MULTI" ]]; then
    echo "error: missing $CSV_MULTI" >&2
    echo "hint: run 'uv run --project . benchmarking/data/gen_data.py benchmarking/data'" >&2
    exit 1
fi

echo "Running reshape regression benchmark (warmup=$WARMUP, iters=$ITERS)..." >&2
BENCH_ARGS=(
    --csv "$CSV"
    --csv-multi "$CSV_MULTI"
    --csv-trades "$CSV_TRADES"
    --csv-events "$CSV_EVENTS"
    --csv-lookup "$CSV_LOOKUP"
    --suite reshape
    --warmup "$WARMUP"
    --iters "$ITERS"
    --out "$OUT"
)
if [[ -n "$RESHAPE_ROWS" ]]; then
    BENCH_ARGS+=(--reshape-rows "$RESHAPE_ROWS")
fi
IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" \
    bash "$SCRIPT_DIR/bench_ibex.sh" "${BENCH_ARGS[@]}"

extract_avg_ms() {
    local query="$1"
    awk -F'\t' -v q="$query" '$1 == "ibex" && $2 == q { print $3; exit }' "$OUT"
}

MELT_AVG_MS="$(extract_avg_ms "melt_wide_to_long")"
DCAST_AVG_MS="$(extract_avg_ms "dcast_long_to_wide")"

if [[ -z "$MELT_AVG_MS" || -z "$DCAST_AVG_MS" ]]; then
    echo "error: reshape queries missing from $OUT" >&2
    exit 1
fi

within_threshold() {
    local value="$1"
    local max_allowed="$2"
    awk -v v="$value" -v m="$max_allowed" 'BEGIN { exit !(v <= m) }'
}

status=0
if within_threshold "$MELT_AVG_MS" "$MELT_MAX_MS"; then
    echo "PASS melt_wide_to_long: avg_ms=$MELT_AVG_MS <= $MELT_MAX_MS" >&2
else
    echo "FAIL melt_wide_to_long: avg_ms=$MELT_AVG_MS > $MELT_MAX_MS" >&2
    status=1
fi

if within_threshold "$DCAST_AVG_MS" "$DCAST_MAX_MS"; then
    echo "PASS dcast_long_to_wide: avg_ms=$DCAST_AVG_MS <= $DCAST_MAX_MS" >&2
else
    echo "FAIL dcast_long_to_wide: avg_ms=$DCAST_AVG_MS > $DCAST_MAX_MS" >&2
    status=1
fi

if [[ "$status" -ne 0 ]]; then
    exit "$status"
fi

echo "reshape regression benchmark passed." >&2
