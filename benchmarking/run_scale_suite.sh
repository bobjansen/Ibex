#!/usr/bin/env bash
# run_scale_suite.sh — run benchmark suite across multiple dataset sizes.
#
# Default sizes:
#   1M, 2M, 4M, 8M, 16M, 32M, 50M rows
# Per-cell cutoff: any query whose single warm iteration exceeds
# IBEX_CELL_CUTOFF_MS (default 60000 = 1 min) is dropped by its harness, so a
# pathologically slow cell at the largest sizes can't dominate wall-clock.
# (ibex's slowest cell is well under a second even at 50M, so 1 min never touches
# it — it only trims the slow competitor cells sooner.)
#
# Usage:
#   ./run_scale_suite.sh [--sizes 1M,2M,4M,...,64M] [--warmup N] [--iters N]
#                        [--skip-ibex] [--skip-ibex-compiled]
#                        [--skip-python] [--skip-r]
#                        [--skip-duckdb] [--skip-duckdb-st] [--duckdb-all-sizes]
#                        [--skip-datafusion] [--skip-datafusion-st]
#                        [--skip-clickhouse] [--skip-clickhouse-st]
#                        [--skip-sqlite] [--with-sqlite]
#                        [--with-frollapply]
#                        [--skip-pandas] [--skip-dplyr] [--skip-polars-st]
#                        [--keep-data]
#                        [--to-readme] [--to-readme-rows N] [--to-readme-out path]
#
# Trimmed-by-default frameworks (see plans/benchmark-perf-priorities.md): the
# suite skips work that never changes the competitive picture but dominates
# wall-clock, so reruns stay cheap. The web page reuses pinned numbers for these.
#   - sqlite: never within 2x of the best engine and the slowest by far — skipped
#     by default. Pass --with-sqlite to regenerate its (stable) baseline numbers.
#   - duckdb: 0 wins and tracks the other columnar engines, so it runs only at a
#     representative scale subset (DUCKDB_SCALES). Pass --duckdb-all-sizes for all.
#   - data.table/dplyr frollapply (tf_rolling_median_1m, tf_rolling_std_1m):
#     O(n*window) and the two biggest single cells in the run — skipped by
#     default. Pass --with-frollapply to include them.

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
TF_ROWS_OVERRIDE=""
# The reshape benchmarks build an in-memory wide table sized to the dataset —
# the bench's biggest RAM consumer (sqlite's :memory: variant needs ~1.8GB per
# 1M rows). Above this many rows the suite passes --reshape-rows 0 so each engine
# skips reshape (blank cell) instead of OOM-killing the run. Default fits the
# 64GB r7i.2xlarge with margin; raise it on a bigger box or override via env.
RESHAPE_MAX_ROWS="${RESHAPE_MAX_ROWS:-20000000}"
# SQLite is 10-1000x slower than the columnar engines (seconds/query at 8M),
# so it dominates wall-clock at scale while adding little beyond a slow-baseline
# data point. When run (--with-sqlite), cap it at this size (gives a 1M/2M/4M
# scaling trend) and skip it above; override via env to include or exclude more.
SQLITE_MAX_ROWS="${SQLITE_MAX_ROWS:-4000000}"
# duckdb is never the fastest engine in this suite (0 wins) and closely tracks
# the other columnar engines, so running it at every scale costs wall-clock
# without adding signal. Run it only at this representative subset; set
# DUCKDB_SCALES="" (or pass --duckdb-all-sizes) to run every size.
DUCKDB_SCALES="${DUCKDB_SCALES:-1000000,4000000,16000000}"
SKIP_IBEX=0
SKIP_IBEX_COMPILED=0
SKIP_PYTHON=0
SKIP_R=0
SKIP_PANDAS=0
SKIP_DPLYR=0
SKIP_POLARS_ST=0
SKIP_DUCKDB=0
SKIP_DUCKDB_ST=0
SKIP_DATAFUSION=0
SKIP_DATAFUSION_ST=0
SKIP_CLICKHOUSE=0
SKIP_CLICKHOUSE_ST=0
# sqlite and the data.table/dplyr frollapply cells are pinned (reused on the web
# page), so they are off by default — see the header. Opt back in with the flags.
SKIP_SQLITE=1
SKIP_FROLLAPPLY=1
DUCKDB_ALL_SIZES=0
KEEP_DATA=0
TO_README=0
TO_README_ROWS=4000000
TO_README_OUT=""

# Default scales: 1M .. 50M. Cells whose single warm iteration exceeds the
# per-cell cutoff (IBEX_CELL_CUTOFF_MS, default 1 min) are dropped by each
# harness, so the large sizes stay bounded.
SIZES=(1000000 2000000 4000000 8000000 16000000 32000000 50000000)

in_csv_list() {
    # in_csv_list <needle> <comma,separated,list> — exact-match membership test.
    local needle="$1" list="$2" item
    local -a arr
    IFS=',' read -r -a arr <<< "$list"
    for item in "${arr[@]}"; do
        [[ "$item" == "$needle" ]] && return 0
    done
    return 1
}

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
        # Override the per-scale TF row count; unset (default) uses each scale's
        # own row count for TF benches.
        --tf-rows)     TF_ROWS_OVERRIDE="$2"; shift 2 ;;
        --skip-ibex)   SKIP_IBEX=1; shift ;;
        --skip-ibex-compiled) SKIP_IBEX_COMPILED=1; shift ;;
        --skip-python) SKIP_PYTHON=1; shift ;;
        --skip-r)      SKIP_R=1; shift ;;
        --skip-pandas) SKIP_PANDAS=1; shift ;;
        --skip-dplyr)  SKIP_DPLYR=1; shift ;;
        --skip-polars-st) SKIP_POLARS_ST=1; shift ;;
        --skip-duckdb) SKIP_DUCKDB=1; shift ;;
        --skip-duckdb-st) SKIP_DUCKDB_ST=1; shift ;;
        --skip-datafusion) SKIP_DATAFUSION=1; shift ;;
        --skip-datafusion-st) SKIP_DATAFUSION_ST=1; shift ;;
        --skip-clickhouse) SKIP_CLICKHOUSE=1; shift ;;
        --skip-clickhouse-st) SKIP_CLICKHOUSE_ST=1; shift ;;
        --skip-sqlite) SKIP_SQLITE=1; shift ;;
        --with-sqlite) SKIP_SQLITE=0; shift ;;
        --duckdb-all-sizes) DUCKDB_ALL_SIZES=1; shift ;;
        --with-frollapply) SKIP_FROLLAPPLY=0; shift ;;
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
# Carry-forward skip set. A harness cuts a cell whose warm iteration exceeds the
# per-iteration budget and emits a sentinel row (avg_ms < 0); we record that
# "framework|query" here and feed the accumulated set back via IBEX_SKIP_CELLS so
# every LARGER size skips it outright — no ever-slower warm iteration is re-paid.
SKIP_FILE="$SCRIPT_DIR/results/skip_cells.txt"

mkdir -p "$DATA_ROOT" "$RESULT_ROOT" "$(dirname "$COMBINED_TSV")"
: > "$SKIP_FILE"

printf "dataset_rows\tframework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\trows\tpeak_rss_mb\n" > "$COMBINED_TSV"
printf "dataset_rows,framework,query,avg_ms,min_ms,max_ms,stddev_ms,p95_ms,p99_ms,rows,peak_rss_mb\n" > "$COMBINED_CSV"

append_tagged_results() {
    local dataset_rows="$1"
    local file_path="$2"
    if [[ ! -f "$file_path" ]]; then
        return 0
    fi
    # Rows with avg_ms < 0 ($3) are cut/skipped cells: record the cell into the
    # carry-forward skip set and drop it from the combined output (blank on page).
    tail -n +2 "$file_path" | awk -v n="$dataset_rows" -v sf="$SKIP_FILE" '
        BEGIN { FS="\t"; OFS="\t" }
        {
            sub(/\r$/, "", $0)
            if (($3 + 0) < 0) { print $1 "|" $2 >> sf; next }
            print n, $0
        }
    ' >> "$COMBINED_TSV"
    tail -n +2 "$file_path" | awk -v n="$dataset_rows" '
        BEGIN { FS="\t"; OFS="," }
        {
            sub(/\r$/, "", $0)
            if (($3 + 0) < 0) next
            print n, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        }
    ' >> "$COMBINED_CSV"
}

# A benchmark engine crashing (e.g. an OS-OOM at the largest scale) must not
# abort the whole sweep under `set -e`: log it and continue so the remaining
# engines and sizes still run. The engine's cells are simply blank for this size.
engine_failed() {
    echo "  \u26a0 ${1} failed at ${rows} rows — continuing (cells blank for this size)" >&2
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

preferred_frameworks = ["ibex", "ibex-compiled", "polars", "polars-st", "duckdb", "duckdb-st", "datafusion", "datafusion-st", "clickhouse", "clickhouse-st", "sqlite", "pandas", "data.table", "dplyr"]
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

    # Carry forward cells cut at any smaller size: harnesses read IBEX_SKIP_CELLS
    # and skip these outright (no warm iteration). Empty on the first size.
    export IBEX_SKIP_CELLS="$(sort -u "$SKIP_FILE" | paste -sd, -)"
    if [[ -n "$IBEX_SKIP_CELLS" ]]; then
        echo "  (carrying forward $(sort -u "$SKIP_FILE" | grep -c .) cut cell(s) from smaller sizes)"
    fi

    # Cap the memory-heavy reshape benchmark: pass 0 (= skip) above the budget.
    RESHAPE_ROWS="$rows"
    if (( rows > RESHAPE_MAX_ROWS )); then
        RESHAPE_ROWS=0
        echo "  (reshape skipped: ${rows} > RESHAPE_MAX_ROWS=${RESHAPE_MAX_ROWS})"
    fi

    uv run --project "$IBEX_ROOT" "$GEN_DATA" "$size_data_dir" --rows "$rows"

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
                --reshape-rows "$rows" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --out "$size_result_dir/ibex.tsv" || engine_failed "ibex"
        append_tagged_results "$rows" "$size_result_dir/ibex.tsv"
    fi

    if [[ $SKIP_IBEX_COMPILED -eq 0 ]]; then
        echo "  → ibex (compiled)"
        IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" \
            bash "$SCRIPT_DIR/bench_ibex_compiled.sh" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --out "$size_result_dir/ibex_compiled.tsv" || engine_failed "ibex-compiled"
        append_tagged_results "$rows" "$size_result_dir/ibex_compiled.tsv"
    fi

    if [[ $SKIP_PYTHON -eq 0 ]]; then
        echo "  → python (pandas + polars)"
        py_args=()
        if [[ $SKIP_PANDAS -eq 1 ]]; then
            py_args+=(--skip-pandas)
        fi
        uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_python.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/python.tsv" \
            "${py_args[@]}" || engine_failed "python"
        append_tagged_results "$rows" "$size_result_dir/python.tsv"

        if [[ $SKIP_POLARS_ST -eq 0 ]]; then
            echo "  → polars (single thread)"
            polars_st_raw="$size_result_dir/polars_st_raw.tsv"
            polars_st_tsv="$size_result_dir/polars_st.tsv"
            POLARS_MAX_THREADS=1 IBEX_FW_SUFFIX=-st uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_python.py" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
                --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
                --fill-rows "$rows" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --skip-pandas \
                --out "$polars_st_raw" || { engine_failed "polars-st"; : > "$polars_st_raw"; }
            awk 'BEGIN { FS=OFS="\t" } NR==1 { print; next } { if ($1 == "polars") $1="polars-st"; print }' \
                "$polars_st_raw" > "$polars_st_tsv"
            append_tagged_results "$rows" "$polars_st_tsv"
        fi
    fi

    if [[ $SKIP_R -eq 0 ]]; then
        echo "  → R (data.table + dplyr)"
        r_args=()
        if [[ $SKIP_DPLYR -eq 1 ]]; then
            r_args+=(--skip-dplyr)
        fi
        if [[ $SKIP_FROLLAPPLY -eq 1 ]]; then
            r_args+=(--skip-frollapply)
        fi
        Rscript "$SCRIPT_DIR/bench_r.R" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/r.tsv" \
            "${r_args[@]}" || engine_failed "R"
        append_tagged_results "$rows" "$size_result_dir/r.tsv"
    fi

    run_duckdb=1
    if [[ $SKIP_DUCKDB -eq 1 ]]; then
        run_duckdb=0
    elif [[ $DUCKDB_ALL_SIZES -eq 0 && -n "$DUCKDB_SCALES" ]] \
            && ! in_csv_list "$rows" "$DUCKDB_SCALES"; then
        run_duckdb=0
        echo "  (duckdb skipped: ${rows} not in DUCKDB_SCALES=${DUCKDB_SCALES})"
    fi
    if [[ $run_duckdb -eq 1 ]]; then
        echo "  → duckdb"
        uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_duckdb.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/duckdb.tsv" || engine_failed "duckdb"
        append_tagged_results "$rows" "$size_result_dir/duckdb.tsv"

        if [[ $SKIP_DUCKDB_ST -eq 0 ]]; then
            echo "  → duckdb (single thread)"
            duckdb_st_raw="$size_result_dir/duckdb_st_raw.tsv"
            duckdb_st_tsv="$size_result_dir/duckdb_st.tsv"
            IBEX_FW_SUFFIX=-st uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_duckdb.py" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
                --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
                --fill-rows "$rows" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --threads 1 \
                --out "$duckdb_st_raw" || { engine_failed "duckdb-st"; : > "$duckdb_st_raw"; }
            awk 'BEGIN { FS=OFS="\t" } NR==1 { print; next } { if ($1 == "duckdb") $1="duckdb-st"; print }' \
                "$duckdb_st_raw" > "$duckdb_st_tsv"
            append_tagged_results "$rows" "$duckdb_st_tsv"
        fi
    fi

    if [[ $SKIP_DATAFUSION -eq 0 ]]; then
        echo "  → datafusion"
        uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_datafusion.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/datafusion.tsv" || engine_failed "datafusion"
        append_tagged_results "$rows" "$size_result_dir/datafusion.tsv"

        if [[ $SKIP_DATAFUSION_ST -eq 0 ]]; then
            echo "  → datafusion (single thread)"
            datafusion_st_raw="$size_result_dir/datafusion_st_raw.tsv"
            datafusion_st_tsv="$size_result_dir/datafusion_st.tsv"
            IBEX_FW_SUFFIX=-st uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_datafusion.py" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
                --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
                --fill-rows "$rows" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --threads 1 \
                --out "$datafusion_st_raw" || { engine_failed "datafusion-st"; : > "$datafusion_st_raw"; }
            awk 'BEGIN { FS=OFS="\t" } NR==1 { print; next } { if ($1 == "datafusion") $1="datafusion-st"; print }' \
                "$datafusion_st_raw" > "$datafusion_st_tsv"
            append_tagged_results "$rows" "$datafusion_st_tsv"
        fi
    fi

    if [[ $SKIP_CLICKHOUSE -eq 0 ]]; then
        echo "  → clickhouse (chdb)"
        uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_clickhouse.py" \
            --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
            --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
            --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
            --fill-rows "$rows" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$size_result_dir/clickhouse.tsv" || engine_failed "clickhouse"
        append_tagged_results "$rows" "$size_result_dir/clickhouse.tsv"

        if [[ $SKIP_CLICKHOUSE_ST -eq 0 ]]; then
            echo "  → clickhouse (single thread)"
            clickhouse_st_raw="$size_result_dir/clickhouse_st_raw.tsv"
            clickhouse_st_tsv="$size_result_dir/clickhouse_st.tsv"
            IBEX_FW_SUFFIX=-st uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_clickhouse.py" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
                --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
                --fill-rows "$rows" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --threads 1 \
                --out "$clickhouse_st_raw" || { engine_failed "clickhouse-st"; : > "$clickhouse_st_raw"; }
            awk 'BEGIN { FS=OFS="\t" } NR==1 { print; next } { if ($1 == "clickhouse") $1="clickhouse-st"; print }' \
                "$clickhouse_st_raw" > "$clickhouse_st_tsv"
            append_tagged_results "$rows" "$clickhouse_st_tsv"
        fi
    fi

    if [[ $SKIP_SQLITE -eq 0 ]]; then
        if (( rows > SQLITE_MAX_ROWS )); then
            echo "  (sqlite skipped: ${rows} > SQLITE_MAX_ROWS=${SQLITE_MAX_ROWS} — too slow at this size)"
        else
            echo "  → sqlite"
            uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_sqlite.py" \
                --csv "$csv" --csv-multi "$csv_multi" --csv-trades "$csv_trades" \
                --csv-events "$csv_events" --csv-lookup "$csv_lookup" \
                --reshape-rows "$RESHAPE_ROWS" --tf-rows "${TF_ROWS_OVERRIDE:-$rows}" \
                --fill-rows "$rows" \
                --warmup "$WARMUP" --iters "$ITERS" \
                --out "$size_result_dir/sqlite.tsv" || engine_failed "sqlite"
            append_tagged_results "$rows" "$size_result_dir/sqlite.tsv"
        fi
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
