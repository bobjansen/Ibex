#!/usr/bin/env bash
# run_scale_ibex_vs_polars.sh — compare Ibex vs Polars (default and single-thread) across sizes.
#
# Default sizes:
#   1M, 2M, 4M, 8M, 16M, 32M, 64M rows
#
# Usage:
#   ./run_scale_ibex_vs_polars.sh [--sizes 1M,2M,4M,...,64M] [--warmup N] [--iters N]
#                                 [--skip-ibex] [--skip-polars] [--skip-polars-st]
#                                 [--keep-data]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
if [[ -n "${BUILD_DIR:-}" ]]; then
    : # user-specified, keep it
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex_bench" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
fi

WARMUP=1
ITERS=5
SKIP_IBEX=0
SKIP_POLARS=0
SKIP_POLARS_ST=0
KEEP_DATA=0

# 1M .. 64M (powers of two)
SIZES=(1000000 2000000 4000000 8000000 16000000 32000000 64000000)

parse_size_token() {
    local tok="$1"
    if [[ "$tok" =~ ^[0-9]+$ ]]; then
        printf "%s" "$tok"
        return 0
    fi
    if [[ "$tok" =~ ^([0-9]+)[mM]$ ]]; then
        printf "%s000000" "${BASH_REMATCH[1]}"
        return 0
    fi
    if [[ "$tok" =~ ^([0-9]+)[kK]$ ]]; then
        printf "%s000" "${BASH_REMATCH[1]}"
        return 0
    fi
    return 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sizes)
            IFS=',' read -r -a raw_sizes <<< "$2"
            SIZES=()
            for token in "${raw_sizes[@]}"; do
                token="${token//[[:space:]]/}"
                if [[ -z "$token" ]]; then
                    continue
                fi
                parsed="$(parse_size_token "$token")" || {
                    echo "error: invalid size token '$token' in --sizes" >&2
                    exit 1
                }
                SIZES+=("$parsed")
            done
            shift 2
            ;;
        --warmup)      WARMUP="$2"; shift 2 ;;
        --iters)       ITERS="$2"; shift 2 ;;
        --skip-ibex)   SKIP_IBEX=1; shift ;;
        --skip-polars) SKIP_POLARS=1; shift ;;
        --skip-polars-st) SKIP_POLARS_ST=1; shift ;;
        --keep-data)   KEEP_DATA=1; shift ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ "${#SIZES[@]}" -eq 0 ]]; then
    echo "error: no sizes configured" >&2
    exit 1
fi
if [[ $SKIP_IBEX -eq 1 && $SKIP_POLARS -eq 1 && $SKIP_POLARS_ST -eq 1 ]]; then
    echo "error: all frameworks are skipped" >&2
    exit 1
fi

DATA_ROOT="$SCRIPT_DIR/data/scales"
RESULT_ROOT="$SCRIPT_DIR/results/scales_ibex_polars"
COMBINED_TSV="$SCRIPT_DIR/results/scales_ibex_polars.tsv"
COMBINED_CSV="$SCRIPT_DIR/results/scales_ibex_polars.csv"
GEN_DATA="$SCRIPT_DIR/data/gen_data.py"

mkdir -p "$DATA_ROOT" "$RESULT_ROOT" "$(dirname "$COMBINED_TSV")"

printf "dataset_rows\tframework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\trows\n" > "$COMBINED_TSV"
printf "dataset_rows,framework,query,avg_ms,min_ms,max_ms,stddev_ms,p95_ms,p99_ms,rows\n" > "$COMBINED_CSV"

append_tagged_results() {
    local dataset_rows="$1"
    local file_path="$2"
    if [[ ! -f "$file_path" ]]; then
        return 0
    fi
    tail -n +2 "$file_path" | awk -v n="$dataset_rows" '
        BEGIN { OFS="\t" }
        {
            sub(/\r$/, "", $0)
            print n, $0
        }
    ' >> "$COMBINED_TSV"
    tail -n +2 "$file_path" | awk -v n="$dataset_rows" '
        BEGIN { FS="\t"; OFS="," }
        {
            sub(/\r$/, "", $0)
            print n, $1, $2, $3, $4, $5, $6, $7, $8, $9
        }
    ' >> "$COMBINED_CSV"
}

for rows in "${SIZES[@]}"; do
    size_data_dir="$DATA_ROOT/$rows"
    size_result_dir="$RESULT_ROOT/$rows"
    mkdir -p "$size_result_dir"

    echo "━━━ Dataset: ${rows} rows ━━━"
    uv run --project "$SCRIPT_DIR" "$GEN_DATA" "$size_data_dir" --rows "$rows"

    csv="$size_data_dir/prices.csv"
    csv_multi="$size_data_dir/prices_multi.csv"
    csv_trades="$size_data_dir/trades.csv"
    csv_events="$size_data_dir/events.csv"
    csv_lookup="$size_data_dir/lookup.csv"

    if [[ $SKIP_IBEX -eq 0 ]]; then
        echo "  → ibex"
        IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" \
            bash "$SCRIPT_DIR/bench_ibex.sh" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
                --reshape-rows "$rows" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --out "$size_result_dir/ibex.tsv"
        append_tagged_results "$rows" "$size_result_dir/ibex.tsv"
    fi

    if [[ $SKIP_POLARS -eq 0 ]]; then
        echo "  → polars (default threads)"
        uv run --project "$SCRIPT_DIR" python3 "$SCRIPT_DIR/bench_python.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$rows" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --skip-pandas \
            --out "$size_result_dir/polars.tsv"
        append_tagged_results "$rows" "$size_result_dir/polars.tsv"
    fi

    if [[ $SKIP_POLARS_ST -eq 0 ]]; then
        echo "  → polars (single thread)"
        local_polars_st_raw="$size_result_dir/polars_st_raw.tsv"
        local_polars_st="$size_result_dir/polars_st.tsv"
        POLARS_MAX_THREADS=1 uv run --project "$SCRIPT_DIR" python3 "$SCRIPT_DIR/bench_python.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$rows" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --skip-pandas \
            --out "$local_polars_st_raw"
        awk 'BEGIN { FS=OFS="\t" } NR==1 { print; next } { $1="polars-st"; print }' \
            "$local_polars_st_raw" > "$local_polars_st"
        append_tagged_results "$rows" "$local_polars_st"
    fi

    if [[ $KEEP_DATA -eq 0 ]]; then
        rm -rf "$size_data_dir"
    fi

    echo
done

echo "combined results written to:"
echo "  $COMBINED_TSV"
echo "  $COMBINED_CSV"
