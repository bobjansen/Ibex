#!/usr/bin/env bash
# run_all.sh — generate data, run all benchmarks, print comparison table.
#
# Usage:
#   ./run_all.sh [--warmup N] [--iters N] [--skip-ibex] [--skip-python]
#                [--skip-r]
#
# Environment overrides:
#   IBEX_ROOT   — repo root        (default: parent of this script)
#   BUILD_DIR   — cmake build dir  (default: $IBEX_ROOT/build)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
# Default to release build; fall back to whatever build/ exists.
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
SKIP_PYTHON=0
SKIP_R=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --warmup)      WARMUP="$2";   shift 2 ;;
        --iters)       ITERS="$2";    shift 2 ;;
        --skip-ibex)   SKIP_IBEX=1;   shift   ;;
        --skip-python) SKIP_PYTHON=1; shift   ;;
        --skip-r)      SKIP_R=1;      shift   ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

DATA="$SCRIPT_DIR/data"
CSV="$DATA/prices.csv"
CSV_MULTI="$DATA/prices_multi.csv"
RESULTS="$SCRIPT_DIR/results"

# ── 1. Generate data ──────────────────────────────────────────────────────────
echo "━━━ Generating data ━━━"
uv run --project "$SCRIPT_DIR" "$DATA/gen_data.py" "$DATA"
echo ""

# ── 2. ibex ───────────────────────────────────────────────────────────────────
if [[ $SKIP_IBEX -eq 0 ]]; then
    echo "━━━ ibex ━━━"
    IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" \
        bash "$SCRIPT_DIR/bench_ibex.sh" \
            --csv "$CSV" --csv-multi "$CSV_MULTI" \
            --warmup "$WARMUP" --iters "$ITERS" \
            --out "$RESULTS/ibex.tsv"
    echo ""
fi

# ── 3. Python (pandas + polars) ───────────────────────────────────────────────
if [[ $SKIP_PYTHON -eq 0 ]]; then
    echo "━━━ Python (pandas + polars) ━━━"
    uv run --project "$SCRIPT_DIR" "$SCRIPT_DIR/bench_python.py" \
        --csv "$CSV" --csv-multi "$CSV_MULTI" \
        --warmup "$WARMUP" --iters "$ITERS" \
        --out "$RESULTS/python.tsv"
    echo ""
fi

# ── 4. R (data.table + dplyr) ────────────────────────────────────────────────
if [[ $SKIP_R -eq 0 ]]; then
    echo "━━━ R (data.table + dplyr) ━━━"
    Rscript "$SCRIPT_DIR/bench_r.R" \
        --csv "$CSV" --csv-multi "$CSV_MULTI" \
        --warmup "$WARMUP" --iters "$ITERS" \
        --out "$RESULTS/r.tsv"
    echo ""
fi

# ── 5. Print table ────────────────────────────────────────────────────────────
echo "━━━ Summary ━━━"
uv run --project "$SCRIPT_DIR" python3 "$SCRIPT_DIR/print_table.py" \
    "$RESULTS"/ibex.tsv "$RESULTS"/python.tsv "$RESULTS"/r.tsv 2>/dev/null \
    || python3 "$SCRIPT_DIR/print_table.py" "$RESULTS"/*.tsv
