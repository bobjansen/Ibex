#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
out="${1:-$PWD/benchmarking/results/local/int32/2026-11-10-integer-abs}"
mkdir -p "$out"
out="$(realpath "$out")"

run_gate() {
    local name="$1" base="$2" cores="$3" suite="$4" repeats="$5" warmup="$6" iters="$7"
    local cpus=2
    [[ "$cores" == 8 ]] && cpus=0,2,4,6,8,10,12,14
    mkdir -p "$out/$name"
    IBEX_PERFCMP_TMPDIR="$out/$name" IBEX_PERFCMP_JOBS=6 \
    IBEX_CORES="$cores" IBEX_COMPACT_INT32=0 \
    benchmarking/compare_ibex_git.sh --base "$base" --target WORKTREE \
        --ibex-suite "$suite" --interleave --replica-control \
        --repeats "$repeats" --warmup "$warmup" --iters "$iters" --taskset "$cpus" \
        --csv "$PWD/benchmarking/data/prices.csv" \
        --csv-multi "$PWD/benchmarking/data/prices_multi.csv" \
        --csv-trades "$PWD/benchmarking/data/trades.csv" \
        --csv-events "$PWD/benchmarking/data/events.csv" \
        --csv-lookup "$PWD/benchmarking/data/lookup.csv" \
        --csv-users "$PWD/benchmarking/data/users.csv" --keep-temp > "$out/$name/run.log" 2>&1
    local directory
    directory="$(sed -n 's/^Kept temporary directory: //p' "$out/$name/run.log")"
    [[ -d "$directory" ]]
    .venv/bin/python benchmarking/int32/paired_report.py "$directory" \
        --out "$out/$name-paired.json" > "$out/$name-paired.log"
    echo "$name complete"
}

run_gate before-after-one 16de4eb3 1 all 12 1 3
run_gate before-after-eight 16de4eb3 8 all 12 1 3
run_gate original-one 0caffc17 1 scalar 18 2 9
run_gate original-eight 0caffc17 8 scalar 18 2 15
