#!/usr/bin/env bash
# Run the full quant demo end-to-end and print a side-by-side coefficient
# comparison.  Assumes build-release/ exists.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

PYTHON="${PYTHON:-python3}"
IBEX_EVAL="${IBEX_EVAL:-$REPO_ROOT/build-release/tools/ibex_eval}"
PLUGIN_DIR="${PLUGIN_DIR:-$REPO_ROOT/build-release/tools}"

if [[ ! -x "$IBEX_EVAL" ]]; then
    echo "error: ibex_eval binary not found at $IBEX_EVAL — run 'cmake --build build-release' first" >&2
    exit 1
fi

echo "── Generating synthetic data ──────────────────────────────────────"
"$PYTHON" "$SCRIPT_DIR/gen_data.py"
echo

echo "── Ibex pipeline ──────────────────────────────────────────────────"
cd "$REPO_ROOT"
"$IBEX_EVAL" --plugin-path "$PLUGIN_DIR" "$SCRIPT_DIR/quant_demo.ibex"
echo

echo "── Polars + scikit-learn pipeline ─────────────────────────────────"
"$PYTHON" "$SCRIPT_DIR/quant_demo_polars.py"
echo

echo "── Coefficient diff (term-by-term) ────────────────────────────────"
paste -d'|' "$DATA_DIR/coefficients.csv" "$DATA_DIR/coefficients_polars.csv"
