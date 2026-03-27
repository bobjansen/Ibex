#!/usr/bin/env bash
# bench_join.sh — run ibex_join_bench and archive raw output in bench_results/.
#
# Usage:
#   ./benchmarking/bench_join.sh [--rows N] [--warmup N] [--iters N]
#                                [--suite name[,name...]] [--verify]
#                                [--max-output-rows N]
#                                [--out-dir bench_results]
#
# The raw benchmark output is always written to a timestamped text file:
#   bench_results/join_YYYYMMDD_HHMMSS.txt
#
# A summary TSV is also maintained at:
#   bench_results/join_index.tsv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
if [[ -n "${BUILD_DIR:-}" ]]; then
    : # keep user override
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex_join_bench" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="$IBEX_ROOT/build"
fi

IBEX_JOIN_BENCH="$BUILD_DIR/tools/ibex_join_bench"
OUT_DIR="$IBEX_ROOT/bench_results"
INDEX_FILE=""
ROWS_ARG=""
WARMUP_ARG=""
ITERS_ARG=""
SUITE_ARG=""
MAX_OUTPUT_ROWS_ARG=""
VERIFY_ARG=0

JOIN_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rows)
            ROWS_ARG="$2"
            JOIN_ARGS+=("$1" "$2")
            shift 2
            ;;
        --warmup)
            WARMUP_ARG="$2"
            JOIN_ARGS+=("$1" "$2")
            shift 2
            ;;
        --iters)
            ITERS_ARG="$2"
            JOIN_ARGS+=("$1" "$2")
            shift 2
            ;;
        --suite)
            SUITE_ARG="$2"
            JOIN_ARGS+=("$1" "$2")
            shift 2
            ;;
        --max-output-rows)
            MAX_OUTPUT_ROWS_ARG="$2"
            JOIN_ARGS+=("$1" "$2")
            shift 2
            ;;
        --verify)
            VERIFY_ARG=1
            JOIN_ARGS+=("$1")
            shift
            ;;
        --out-dir)
            OUT_DIR="$2"
            shift 2
            ;;
        --index-file)
            INDEX_FILE="$2"
            shift 2
            ;;
        -h|--help)
            sed -n '1,16p' "$0"
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ ! -x "$IBEX_JOIN_BENCH" ]]; then
    echo "error: ibex_join_bench not found at $IBEX_JOIN_BENCH" >&2
    echo "       Run: cmake --build $BUILD_DIR --target ibex_join_bench" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"
if [[ -z "$INDEX_FILE" ]]; then
    INDEX_FILE="$OUT_DIR/join_index.tsv"
fi
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="$OUT_DIR/join_${TIMESTAMP}.txt"
TMP_RAW="$(mktemp)"
trap 'rm -f "$TMP_RAW"' EXIT

{
    echo "# ibex_join_bench"
    echo "# timestamp: $(date --iso-8601=seconds)"
    echo "# cwd: $IBEX_ROOT"
    echo "# build_dir: $BUILD_DIR"
    printf "# command:"
    printf " %q" "$IBEX_JOIN_BENCH" "${JOIN_ARGS[@]}"
    printf "\n\n"
    "$IBEX_JOIN_BENCH" "${JOIN_ARGS[@]}"
} | tee "$OUT_FILE" "$TMP_RAW"

if [[ ! -f "$INDEX_FILE" ]]; then
    printf "timestamp\tlog_file\tsuite\trows_arg\twarmup\titers\tmax_output_rows\tverify\tbenchmark\tbench_iters\ttotal_ms\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\trows\n" > "$INDEX_FILE"
fi

awk -v ts="$(date --iso-8601=seconds)" \
    -v log_file="$OUT_FILE" \
    -v suite="$SUITE_ARG" \
    -v rows_arg="$ROWS_ARG" \
    -v warmup="$WARMUP_ARG" \
    -v iters_arg="$ITERS_ARG" \
    -v max_output_rows="$MAX_OUTPUT_ROWS_ARG" \
    -v verify="$VERIFY_ARG" '
/^bench / {
    bench = $2
    sub(/:$/, "", bench)
    bench_iters = total_ms = avg_ms = min_ms = max_ms = stddev_ms = p95_ms = p99_ms = rows = ""
    for (i = 3; i <= NF; i++) {
        v = $i
        sub(/,$/, "", v)
        if (v ~ /^iters=/)      { split(v, a, "="); bench_iters = a[2] }
        if (v ~ /^total_ms=/)   { split(v, a, "="); total_ms = a[2] }
        if (v ~ /^avg_ms=/)     { split(v, a, "="); avg_ms = a[2] }
        if (v ~ /^min_ms=/)     { split(v, a, "="); min_ms = a[2] }
        if (v ~ /^max_ms=/)     { split(v, a, "="); max_ms = a[2] }
        if (v ~ /^stddev_ms=/)  { split(v, a, "="); stddev_ms = a[2] }
        if (v ~ /^p95_ms=/)     { split(v, a, "="); p95_ms = a[2] }
        if (v ~ /^p99_ms=/)     { split(v, a, "="); p99_ms = a[2] }
        if (v ~ /^rows=/)       { split(v, a, "="); rows = a[2] }
    }
    print ts "\t" log_file "\t" suite "\t" rows_arg "\t" warmup "\t" iters_arg "\t" \
          max_output_rows "\t" verify "\t" bench "\t" bench_iters "\t" total_ms "\t" avg_ms "\t" \
          min_ms "\t" max_ms "\t" stddev_ms "\t" p95_ms "\t" p99_ms "\t" rows
}
' "$TMP_RAW" >> "$INDEX_FILE"

echo
echo "results written to $OUT_FILE"
echo "summary updated at $INDEX_FILE"
