#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# bench_polars_engines.sh — Polars' two engines on the same Polars code:
# its streaming engine against IbexEngine (python/ibex_polars), over
#
#   * h2o db-benchmark: benchmarking/h2o/run.py --engines polars polars-ibex
#     (the harness's Polars queries on resident frames, bounded full-answer
#     validation, min of --iters after --warmup). Groupby runs the questions
#     IbexEngine supports, --h2o-questions; q6 (median), q8 (top_k + explode)
#     and q9 (corr) are not translated yet. Joins run all five. And
#   * PDS-H: python/bench_pdsh_engine.py --skip-in-memory (the unchanged
#     upstream Polars queries on Parquet, every result compared in full,
#     medians of --pdsh-iters interleaved rounds).
#
# Each thread count runs every suite in its own processes. Engines get the
# first N CPUs of this process's affinity, so pin the whole run to one CPU per
# physical core, e.g. on an i7-13700:
#
#   taskset -c 0,2,4,6,8,10,12,14 python/bench_polars_engines.sh \
#       --pdsh-root ../polars-benchmark
#
# Then summarise: python/summarize_polars_engines.py <out-dir>
#
# Options:
#   --pdsh-root DIR      upstream pola-rs/polars-benchmark checkout (required
#                        unless --skip-pdsh)
#   --pdsh-data DIR      PDS-H Parquet tables (default <pdsh-root>/data/tables/scale-10.0)
#   --threads LIST       comma-separated thread counts (default 8,1)
#   --h2o-data LIST      comma-separated h2o datasets (default: the six 1e7 sets)
#   --h2o-questions LIST groupby questions (default q1,q2,q3,q4,q5,q7,q10)
#   --warmup N / --iters N   h2o warmups and timed iterations (default 1 / 5)
#   --pdsh-iters N       PDS-H interleaved rounds, even (default 6)
#   --out-dir DIR        results (default python/polars-engines-results/<stamp>)
#   --skip-h2o / --skip-pdsh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-$ROOT/.venv/bin/python}"
PDSH_ROOT="" PDSH_DATA="" THREADS="8,1" WARMUP=1 ITERS=5 PDSH_ITERS=6 OUT="" SKIP_H2O=0 SKIP_PDSH=0
H2O_DATA="J1_1e7_NA_0_0,G1_1e7_1e2_0_0,G1_1e7_1e2_5_0,G1_1e7_1e2_0_1,G1_1e7_1e1_0_0,G1_1e7_2e0_0_0"
H2O_QUESTIONS="q1,q2,q3,q4,q5,q7,q10"
while (($#)); do
    case "$1" in
        --pdsh-root) PDSH_ROOT="$2"; shift 2 ;;
        --pdsh-data) PDSH_DATA="$2"; shift 2 ;;
        --threads) THREADS="$2"; shift 2 ;;
        --h2o-data) H2O_DATA="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --pdsh-iters) PDSH_ITERS="$2"; shift 2 ;;
        --out-dir) OUT="$2"; shift 2 ;;
        --skip-h2o) SKIP_H2O=1; shift ;;
        --skip-pdsh) SKIP_PDSH=1; shift ;;
        --h2o-questions) H2O_QUESTIONS="$2"; shift 2 ;;
        -h|--help) sed -n '4,36p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done
if ((!SKIP_PDSH)); then
    [[ -n "$PDSH_ROOT" ]] || { echo "error: --pdsh-root is required (or --skip-pdsh)" >&2; exit 2; }
    PDSH_ROOT="$(cd "$PDSH_ROOT" && pwd)"
    PDSH_DATA="${PDSH_DATA:-$PDSH_ROOT/data/tables/scale-10.0}"
fi
OUT="${OUT:-$ROOT/python/polars-engines-results/$(date +%Y%m%dT%H%M%S)}"
mkdir -p "$OUT"
IFS=, read -r -a THREAD_LIST <<< "$THREADS"
IFS=, read -r -a H2O_LIST <<< "$H2O_DATA"
IFS=, read -r -a H2O_QS <<< "$H2O_QUESTIONS"
GROUPBY=() JOIN=()
for d in "${H2O_LIST[@]}"; do
    case "$d" in G1_*) GROUPBY+=("$d") ;; J1_*) JOIN+=("$d") ;; *) echo "error: $d is not an h2o data name" >&2; exit 2 ;; esac
done
mapfile -t CPUS < <("$PY" -c 'import os; print(*sorted(os.sched_getaffinity(0)), sep="\n")')
for n in "${THREAD_LIST[@]}"; do
    if ((n < 1 || n > ${#CPUS[@]})); then
        echo "error: $n threads but only ${#CPUS[@]} CPUs in this process's affinity" >&2; exit 2
    fi
done

# The h2o harness records the build it measured; this rebuilds only if stale.
"$PY" "$ROOT/benchmarking/h2o/provenance.py" --build >/dev/null

status=0
for n in "${THREAD_LIST[@]}"; do
    cpus="$(IFS=,; echo "${CPUS[*]:0:n}")"
    if ((!SKIP_H2O)); then
        h2o=(--engines polars polars-ibex --threads "$n" --warmup "$WARMUP" --iters "$ITERS")
        if ((${#GROUPBY[@]})); then
            echo "=== h2o groupby, $n threads (CPUs $cpus) ===" >&2
            taskset -c "$cpus" "$PY" "$ROOT/benchmarking/h2o/run.py" "${GROUPBY[@]}" "${h2o[@]}" \
                --questions "${H2O_QS[@]}" --out "$OUT/h2o_groupby_${n}t.tsv" || status=1
        fi
        if ((${#JOIN[@]})); then
            echo "=== h2o join, $n threads (CPUs $cpus) ===" >&2
            # Naming the five join questions also limits the groupby fixture
            # checks to q1-q5, which IbexEngine translates.
            taskset -c "$cpus" "$PY" "$ROOT/benchmarking/h2o/run.py" "${JOIN[@]}" "${h2o[@]}" \
                --questions q1 q2 q3 q4 q5 --out "$OUT/h2o_join_${n}t.tsv" || status=1
        fi
    fi
    if ((!SKIP_PDSH)); then
        echo "=== PDS-H, $n threads (CPUs $cpus) ===" >&2
        # PYTHONSAFEPATH keeps the runner's own directory from shadowing the
        # packages on PYTHONPATH.
        PYTHONSAFEPATH=1 PYTHONPATH="$ROOT/build-release/python:$ROOT/python" \
            IBEX_CORES="$n" POLARS_MAX_THREADS="$n" OPENBLAS_NUM_THREADS=1 \
            taskset -c "$cpus" "$PY" "$ROOT/python/bench_pdsh_engine.py" --skip-in-memory \
            --pdsh-root "$PDSH_ROOT" --data "$PDSH_DATA" --iters "$PDSH_ITERS" \
            --out "$OUT/pdsh_${n}t.json" || status=1
    fi
done
echo "results in $OUT; summarise with: $PY $ROOT/python/summarize_polars_engines.py $OUT" >&2
exit "$status"
