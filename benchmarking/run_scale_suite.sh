#!/usr/bin/env bash
# run_scale_suite.sh — run benchmark suite across multiple dataset sizes.
#
# Default sizes:
#   1M, 2M, 4M, 8M, 16M, 32M, 64M rows
#
# Usage:
#   ./run_scale_suite.sh [--sizes 1M,2M,4M,...,64M] [--warmup N] [--iters N]
#                        [--skip-ibex] [--skip-ibex-compiled]
#                        [--skip-python] [--skip-r]
#                        [--skip-pandas] [--skip-dplyr] [--keep-data]
#                        [--to-readme] [--to-readme-rows N] [--to-readme-out path]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
if [[ -f "${BUILD_DIR:-}" ]]; then
    : # user-specified, keep it
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex_bench" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"
fi

WARMUP=1
ITERS=5
SKIP_IBEX=0
SKIP_IBEX_COMPILED=0
SKIP_PYTHON=0
SKIP_R=0
SKIP_PANDAS=0
SKIP_DPLYR=0
KEEP_DATA=0
TO_README=0
TO_README_ROWS=4000000
TO_README_OUT=""

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
        --skip-ibex-compiled) SKIP_IBEX_COMPILED=1; shift ;;
        --skip-python) SKIP_PYTHON=1; shift ;;
        --skip-r)      SKIP_R=1; shift ;;
        --skip-pandas) SKIP_PANDAS=1; shift ;;
        --skip-dplyr)  SKIP_DPLYR=1; shift ;;
        --keep-data)   KEEP_DATA=1; shift ;;
        --to-readme|--to_readme) TO_README=1; shift ;;
        --to-readme-rows|--to_readme_rows)
            parsed="$(parse_size_token "$2")" || {
                echo "error: invalid value '$2' for --to-readme-rows" >&2
                exit 1
            }
            TO_README_ROWS="$parsed"
            shift 2
            ;;
        --to-readme-out|--to_readme_out) TO_README_OUT="$2"; shift 2 ;;
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

DATA_ROOT="$SCRIPT_DIR/data/scales"
RESULT_ROOT="$SCRIPT_DIR/results/scales"
COMBINED_TSV="$SCRIPT_DIR/results/scales.tsv"
COMBINED_CSV="$SCRIPT_DIR/results/scales.csv"
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

emit_readme_markdown() {
    local combined_tsv="$1"
    local out_path="$2"
    local requested_rows="$3"
    local warmup="$4"
    local iters="$5"

    python3 - "$combined_tsv" "$out_path" "$requested_rows" "$warmup" "$iters" <<'PY'
import csv
import sys
from collections import OrderedDict
from pathlib import Path

tsv_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
requested_rows = int(sys.argv[3])
warmup = int(sys.argv[4])
iters = int(sys.argv[5])

rows = []
with tsv_path.open("r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for r in reader:
        r["dataset_rows"] = int(r["dataset_rows"])
        r["avg_ms"] = float(r["avg_ms"])
        rows.append(r)

if not rows:
    raise SystemExit("error: no benchmark rows available to format")

matching = [r for r in rows if r["dataset_rows"] == requested_rows]
selected_rows = requested_rows
if not matching:
    selected_rows = max(r["dataset_rows"] for r in rows)
    matching = [r for r in rows if r["dataset_rows"] == selected_rows]

preferred_frameworks = ["ibex", "ibex-compiled", "polars", "pandas", "data.table", "dplyr"]
present_frameworks = {r["framework"] for r in matching}
frameworks = [fw for fw in preferred_frameworks if fw in present_frameworks]
for fw in sorted(present_frameworks):
    # Keep README output focused on user-facing frameworks.
    if fw == "ibex+parse":
        continue
    if fw not in frameworks:
        frameworks.append(fw)

if not frameworks:
    raise SystemExit("error: no frameworks available after filtering")

query_order = OrderedDict()
for r in matching:
    if r["framework"] == "ibex+parse":
        continue
    query_order.setdefault(r["query"], None)
queries = list(query_order.keys())

vals = {}
for r in matching:
    if r["framework"] == "ibex+parse":
        continue
    vals[(r["query"], r["framework"])] = r["avg_ms"]

def fmt_ms(v: float) -> str:
    if v < 1.0:
        return f"{v:.3f} ms"
    if v < 10.0:
        return f"{v:.2f} ms"
    return f"{v:.1f} ms"

columns = ["query", *frameworks]
table_rows: list[list[str]] = []
for q in queries:
    row = [q]
    for fw in frameworks:
        v = vals.get((q, fw))
        row.append("-" if v is None else fmt_ms(v))
    table_rows.append(row)

widths = [len(c) for c in columns]
for row in table_rows:
    for i, cell in enumerate(row):
        if len(cell) > widths[i]:
            widths[i] = len(cell)

def align_cell(value: str, width: int, right: bool) -> str:
    return value.rjust(width) if right else value.ljust(width)

def md_sep(width: int, right: bool) -> str:
    width = max(3, width)
    if right:
        return "-" * (width - 1) + ":"
    return "-" * width

lines = []
lines.append("## Benchmark")
lines.append("")
if selected_rows == requested_rows:
    lines.append(
        f"Scale benchmark snapshot on **{selected_rows:,} rows** "
        f"(warmup={warmup}, iters={iters})."
    )
else:
    lines.append(
        f"Scale benchmark snapshot on **{selected_rows:,} rows** "
        f"(requested {requested_rows:,}; warmup={warmup}, iters={iters})."
    )
lines.append("")
header_cells = [align_cell(c, widths[i], right=(i > 0)) for i, c in enumerate(columns)]
lines.append("| " + " | ".join(header_cells) + " |")
sep_cells = [md_sep(widths[i], right=(i > 0)) for i in range(len(columns))]
lines.append("| " + " | ".join(sep_cells) + " |")
for row in table_rows:
    cells = [align_cell(c, widths[i], right=(i > 0)) for i, c in enumerate(row)]
    lines.append("| " + " | ".join(cells) + " |")
lines.append("")
lines.append(
    "_Generated by `benchmarking/run_scale_suite.sh --to-readme` from "
    "`benchmarking/results/scales.tsv`._"
)
lines.append("")

out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text("\n".join(lines), encoding="utf-8")
PY
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
                --warmup "$WARMUP" --iters "$ITERS" \
                --out "$size_result_dir/ibex.tsv"
        append_tagged_results "$rows" "$size_result_dir/ibex.tsv"
    fi

    if [[ $SKIP_IBEX_COMPILED -eq 0 ]]; then
        echo "  → ibex (compiled)"
        IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" \
            bash "$SCRIPT_DIR/bench_ibex_compiled.sh" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --out "$size_result_dir/ibex_compiled.tsv"
        append_tagged_results "$rows" "$size_result_dir/ibex_compiled.tsv"
    fi

    if [[ $SKIP_PYTHON -eq 0 ]]; then
        echo "  → python (pandas + polars)"
        py_args=()
        if [[ $SKIP_PANDAS -eq 1 ]]; then
            py_args+=(--skip-pandas)
        fi
        uv run --project "$SCRIPT_DIR" "$SCRIPT_DIR/bench_python.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/python.tsv" \
            "${py_args[@]}"
        append_tagged_results "$rows" "$size_result_dir/python.tsv"
    fi

    if [[ $SKIP_R -eq 0 ]]; then
        echo "  → R (data.table + dplyr)"
        r_args=()
        if [[ $SKIP_DPLYR -eq 1 ]]; then
            r_args+=(--skip-dplyr)
        fi
        Rscript "$SCRIPT_DIR/bench_r.R" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/r.tsv" \
            "${r_args[@]}"
        append_tagged_results "$rows" "$size_result_dir/r.tsv"
    fi

    if [[ $KEEP_DATA -eq 0 ]]; then
        rm -rf "$size_data_dir"
    fi

    echo
done

echo "combined results written to:"
echo "  $COMBINED_TSV"
echo "  $COMBINED_CSV"

if [[ $TO_README -eq 1 ]]; then
    if [[ -z "$TO_README_OUT" ]]; then
        TO_README_OUT="$SCRIPT_DIR/results/scales_readme.md"
    fi
    emit_readme_markdown "$COMBINED_TSV" "$TO_README_OUT" "$TO_README_ROWS" "$WARMUP" "$ITERS"
    echo
    echo "README markdown written to:"
    echo "  $TO_README_OUT"
fi
