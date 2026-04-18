#!/usr/bin/env bash
# run_scale_regression.sh — guard Ibex scale benchmark performance against README snapshot.
#
# Usage:
#   ./benchmarking/run_scale_regression.sh
#   ./benchmarking/run_scale_regression.sh --allowed-regression-pct 15
#   ./benchmarking/run_scale_regression.sh --bench-tsv benchmarking/results/scales/4000000/ibex.tsv

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

README_PATH="$IBEX_ROOT/README.md"
ROWS=4000000
WARMUP=2
ITERS=15
ALLOWED_REGRESSION_PCT=10
BENCH_TSV=""
OUT="$SCRIPT_DIR/results/ibex_scale_regression.tsv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir) BUILD_DIR="$2"; shift 2 ;;
        --readme) README_PATH="$2"; shift 2 ;;
        --rows) ROWS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --allowed-regression-pct) ALLOWED_REGRESSION_PCT="$2"; shift 2 ;;
        --bench-tsv) BENCH_TSV="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help)
            cat <<'EOF'
Usage: run_scale_regression.sh [options]

Options:
  --build-dir <path>              Build dir containing tools/ibex_bench
  --readme <path>                 README.md baseline source (default: repo README.md)
  --rows <N>                      Dataset rows to compare (default: 4000000)
  --warmup <N>                    Warmup iterations when generating fresh results (default: 2)
  --iters <N>                     Timed iterations when generating fresh results (default: 15)
  --allowed-regression-pct <X>    Max allowed slowdown vs README per query (default: 10)
  --bench-tsv <path>              Reuse an existing ibex.tsv instead of rerunning benchmarks
  --out <path>                    Where to write the comparison TSV
EOF
            exit 0
            ;;
        *)
            echo "error: unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ ! -f "$README_PATH" ]]; then
    echo "error: missing README baseline: $README_PATH" >&2
    exit 1
fi

mkdir -p "$(dirname "$OUT")"

if [[ -z "$BENCH_TSV" ]]; then
    DATA_DIR="$SCRIPT_DIR/data/scales/$ROWS"
    BENCH_TSV="$SCRIPT_DIR/results/scales/$ROWS/ibex.tsv"
    mkdir -p "$(dirname "$BENCH_TSV")"

    echo "Generating ${ROWS}-row benchmark data..." >&2
    uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/data/gen_data.py" "$DATA_DIR" --rows "$ROWS"

    echo "Running Ibex scale benchmark regression check (warmup=$WARMUP, iters=$ITERS)..." >&2
    IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="$BUILD_DIR" \
        bash "$SCRIPT_DIR/bench_ibex.sh" \
            --csv "$DATA_DIR/prices.csv" \
            --csv-multi "$DATA_DIR/prices_multi.csv" \
            --csv-trades "$DATA_DIR/trades.csv" \
            --csv-events "$DATA_DIR/events.csv" \
            --csv-lookup "$DATA_DIR/lookup.csv" \
            --reshape-rows "$ROWS" \
            --warmup "$WARMUP" \
            --iters "$ITERS" \
            --out "$BENCH_TSV"
fi

if [[ ! -f "$BENCH_TSV" ]]; then
    echo "error: missing benchmark TSV: $BENCH_TSV" >&2
    exit 1
fi

python3 - "$README_PATH" "$BENCH_TSV" "$OUT" "$ROWS" "$ALLOWED_REGRESSION_PCT" <<'PY'
import csv
import math
import sys
from pathlib import Path

readme_path = Path(sys.argv[1])
bench_tsv = Path(sys.argv[2])
out_path = Path(sys.argv[3])
rows = int(sys.argv[4])
allowed_pct = float(sys.argv[5])

lines = readme_path.read_text(encoding="utf-8").splitlines()

table_header_idx = None
for idx, line in enumerate(lines):
    if line.strip().startswith("| query") and "ibex" in line:
        table_header_idx = idx
        break

if table_header_idx is None:
    raise SystemExit(f"error: could not find benchmark table with ibex column in {readme_path}")

header_cells = [c.strip() for c in lines[table_header_idx].strip().strip("|").split("|")]
try:
    ibex_col = header_cells.index("ibex")
except ValueError as exc:
    raise SystemExit("error: README benchmark table has no ibex column") from exc

baseline = {}
for line in lines[table_header_idx + 2 :]:
    stripped = line.strip()
    if not stripped.startswith("|"):
        break
    cells = [c.strip() for c in stripped.strip("|").split("|")]
    if len(cells) <= ibex_col:
        continue
    query = cells[0]
    ibex_value = cells[ibex_col]
    if ibex_value == "-" or not query:
        continue
    try:
        baseline[query] = float(ibex_value.removesuffix(" ms"))
    except ValueError as exc:
        raise SystemExit(f"error: could not parse README ibex value '{ibex_value}' for {query}") from exc

if not baseline:
    raise SystemExit("error: no README baseline rows found")

current = {}
with bench_tsv.open("r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        if row.get("framework") != "ibex":
            continue
        try:
            current[row["query"]] = float(row["avg_ms"])
        except (KeyError, ValueError) as exc:
            raise SystemExit(f"error: malformed ibex benchmark row in {bench_tsv}: {row}") from exc

missing = sorted(set(baseline) - set(current))
if missing:
    missing_str = ", ".join(missing)
    raise SystemExit(f"error: current benchmark is missing README queries: {missing_str}")

fieldnames = [
    "query",
    "dataset_rows",
    "baseline_ms",
    "current_ms",
    "delta_ms",
    "delta_pct",
    "max_allowed_ms",
    "status",
]
rows_out = []
status = 0
slowdown_factors = []
for query in sorted(baseline):
    base = baseline[query]
    curr = current[query]
    max_allowed = base * (1.0 + allowed_pct / 100.0)
    delta = curr - base
    delta_pct = 0.0 if base == 0 else (delta / base) * 100.0
    ok = curr <= max_allowed
    if not ok:
        status = 1
    slowdown_factors.append(curr / base if base > 0 else 1.0)
    rows_out.append(
        {
            "query": query,
            "dataset_rows": rows,
            "baseline_ms": f"{base:.6f}",
            "current_ms": f"{curr:.6f}",
            "delta_ms": f"{delta:.6f}",
            "delta_pct": f"{delta_pct:.3f}",
            "max_allowed_ms": f"{max_allowed:.6f}",
            "status": "PASS" if ok else "FAIL",
        }
    )

out_path.parent.mkdir(parents=True, exist_ok=True)
with out_path.open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
    writer.writeheader()
    writer.writerows(rows_out)

gmean = math.exp(sum(math.log(x) for x in slowdown_factors) / len(slowdown_factors))
print(f"README baseline: {readme_path}")
print(f"Current benchmark: {bench_tsv}")
print(f"Dataset rows: {rows}")
print(f"Allowed regression: {allowed_pct:.1f}%")
print(f"Geometric mean slowdown vs README: {gmean:.3f}x")
print(f"Comparison TSV written to: {out_path}")
for row in rows_out:
    if row["status"] == "FAIL":
        print(
            f"FAIL {row['query']}: current={row['current_ms']} ms, "
            f"baseline={row['baseline_ms']} ms, allowed={row['max_allowed_ms']} ms"
        )

raise SystemExit(status)
PY

echo "scale regression benchmark passed." >&2
