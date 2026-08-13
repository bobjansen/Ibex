#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run_thread_scaling.sh — how each engine converts cores into throughput.
#
# Runs the whole scale suite repeatedly at ONE dataset size, once per thread
# count, and emits a combined CSV with a `threads` column. The per-query curve
# across that column is the deliverable: it separates the queries that scale
# from the ones that are still serial, which a single geomean cannot do.
#
# Running the whole suite per thread count (rather than a hand-picked subset)
# is deliberate. The interesting cells are the FLAT ones — dcast, melt, the
# rolling family — and a subset chosen in advance is exactly the instrument
# that cannot find a flat line it did not already expect.
#
# Engines default to ibex + polars + duckdb: the three that actually thread and
# whose curves are comparable. pandas, dplyr and the -st variants are skipped —
# `-st` IS the threads=1 point of this sweep, so running it again would just be
# the same measurement under a second name.
#
# Every pass gets the same uniform budget via run_scale_suite.sh --threads, and
# is pinned to cores 0..T-1 so a budget of T threads cannot be serviced by more
# than T cores. On a hyperthreaded box Linux numbers one thread per physical
# core first, so the low half of the range is physical cores and the sweep only
# starts doubling up on siblings past the physical core count — which is the
# shape you want, and the reason the SMT knee is visible rather than smeared.
#
# Usage:
#   ./run_thread_scaling.sh --threads 1,2,4,8,16 --rows 16M
#   ./run_thread_scaling.sh --threads 1,2,4,8 --engines ibex,python
#   ./run_thread_scaling.sh --threads 1,8 --no-pin      # budget only, no taskset

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"

THREAD_LIST="1,2,4,8"
ROWS="16M"
WARMUP=1
ITERS=5
ENGINES="ibex,python,duckdb"
PIN=1
OUT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --threads)  THREAD_LIST="$2"; shift 2 ;;
        --rows)     ROWS="$2"; shift 2 ;;
        --warmup)   WARMUP="$2"; shift 2 ;;
        --iters)    ITERS="$2"; shift 2 ;;
        --engines)  ENGINES="$2"; shift 2 ;;
        --no-pin)   PIN=0; shift ;;
        --out)      OUT="$2"; shift 2 ;;
        -h|--help)  sed -n '5,32p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

# ── Engine selection ──────────────────────────────────────────────────────────
# Expressed as run_scale_suite.sh skip flags. Every -st variant is off in every
# combination: threads=1 is the same measurement.
declare -A ENGINE_KNOWN=([ibex]=1 [python]=1 [duckdb]=1 [datafusion]=1 [clickhouse]=1)
SUITE_ARGS=(--skip-ibex-compiled --skip-r --skip-pandas
            --skip-ibex-st --skip-polars-st --skip-duckdb-st
            --skip-datafusion-st --skip-clickhouse-st
            --duckdb-all-sizes --keep-data)

IFS=',' read -r -a ENGINE_LIST <<< "$ENGINES"
declare -A WANT=()
for e in "${ENGINE_LIST[@]}"; do
    if [[ -z "${ENGINE_KNOWN[$e]+x}" ]]; then
        echo "error: unknown engine '$e' (valid: ${!ENGINE_KNOWN[*]})" >&2
        exit 1
    fi
    WANT[$e]=1
done
[[ -z "${WANT[ibex]+x}"       ]] && SUITE_ARGS+=(--skip-ibex)
[[ -z "${WANT[python]+x}"     ]] && SUITE_ARGS+=(--skip-python)
[[ -z "${WANT[duckdb]+x}"     ]] && SUITE_ARGS+=(--skip-duckdb)
[[ -z "${WANT[datafusion]+x}" ]] && SUITE_ARGS+=(--skip-datafusion)
[[ -z "${WANT[clickhouse]+x}" ]] && SUITE_ARGS+=(--skip-clickhouse)

IFS=',' read -r -a THREADS <<< "$THREAD_LIST"
for t in "${THREADS[@]}"; do
    [[ "$t" =~ ^[1-9][0-9]*$ ]] || { echo "error: bad thread count '$t'" >&2; exit 1; }
done

AVAILABLE=$(nproc)
TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
[[ -z "$OUT" ]] && OUT="$SCRIPT_DIR/results/thread_scaling_${TIMESTAMP}.csv"
mkdir -p "$(dirname "$OUT")"

echo "threads,dataset_rows,framework,query,avg_ms,min_ms,max_ms,stddev_ms,p95_ms,p99_ms,rows,peak_rss_mb" > "$OUT"

echo "Thread sweep : ${THREADS[*]}"
echo "Dataset      : $ROWS"
echo "Engines      : ${ENGINE_LIST[*]}"
echo "vCPUs on box : $AVAILABLE"
echo "Pinning      : $([[ $PIN -eq 1 ]] && echo 'taskset -c 0-(T-1)' || echo 'none (budget only)')"
echo "Output       : $OUT"
echo ""

FAILED=()
for t in "${THREADS[@]}"; do
    if (( t > AVAILABLE )); then
        # Oversubscribing turns a scaling measurement into a scheduling
        # measurement. Refusing the cell is more useful than publishing it.
        echo "━━━ threads=${t}: SKIPPED (box has only ${AVAILABLE} vCPUs) ━━━"
        FAILED+=("threads=${t} (oversubscribed)")
        continue
    fi

    echo "━━━ threads=${t} ━━━"
    PIN_CMD=()
    (( PIN == 1 )) && PIN_CMD=(taskset -c "0-$((t - 1))")

    rc=0
    IBEX_ROOT="$IBEX_ROOT" BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build-release}" \
        "${PIN_CMD[@]}" bash "$SCRIPT_DIR/run_scale_suite.sh" \
            --sizes "$ROWS" --warmup "$WARMUP" --iters "$ITERS" \
            --threads "$t" "${SUITE_ARGS[@]}" || rc=$?

    # A non-zero suite exit means SOME engine failed, not that nothing ran —
    # the suite writes its CSV before exiting. Take whatever landed and record
    # the failure, rather than losing a completed pass to one bad engine.
    if [[ -f "$SCRIPT_DIR/results/scales.csv" ]]; then
        tail -n +2 "$SCRIPT_DIR/results/scales.csv" | sed "s/^/${t},/" >> "$OUT"
    fi
    if (( rc != 0 )); then
        echo "  (threads=${t} exited ${rc} — at least one engine failed)"
        FAILED+=("threads=${t} (suite exit ${rc})")
    fi

    # The carry-forward skip set cuts a cell for every LARGER size. Across a
    # thread sweep the size never changes, so a cell cut because it was slow at
    # threads=1 must not stay cut at threads=16 — that is precisely the cell
    # whose scaling we are trying to measure.
    : > "$SCRIPT_DIR/results/skip_cells.txt"
    echo ""
done

# --keep-data held the generated CSVs across passes; drop them now.
rm -rf "$SCRIPT_DIR/data/scales"

echo "thread-scaling results written to:"
echo "  $OUT"
if (( ${#FAILED[@]} > 0 )); then
    echo >&2
    echo "warning: ${#FAILED[@]} pass(es) incomplete — the curve has holes:" >&2
    printf '  - %s\n' "${FAILED[@]}" >&2
    exit 1
fi
