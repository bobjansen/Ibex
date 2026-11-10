#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
out="${1:-$PWD/benchmarking/results/local/int32/2026-11-10-symbol-semi-anti}"
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

run_gate before-after-one f1eb185c 1 all 12 1 3
run_gate before-after-eight f1eb185c 8 all 12 1 3
.venv/bin/python benchmarking/int32/symbol-semi-anti/build.py \
    --base-build "$HOME/.cache/ibex/perfcmp/base/build" \
    --target-build "$HOME/.cache/ibex/perfcmp/target-worktree/build" \
    --out build-symbol-semi-anti/probe-production > "$out/production-probe-build.log" 2>&1
.venv/bin/python benchmarking/int32/symbol-semi-anti/run.py \
    --base build-symbol-semi-anti/probe-production/base \
    --target build-symbol-semi-anti/probe-production/target \
    --rows 1000000 4000000 --cores 1 8 --repeats 18 --iters 3 \
    --out "$out/production-integer.json" > "$out/production-integer.log" 2>&1
.venv/bin/python benchmarking/int32/symbol-semi-anti/run.py \
    --base build-symbol-semi-anti/probe-production/base \
    --target build-symbol-semi-anti/probe-production/target \
    --rows 1000000 --cores 1 8 --right-rows 100000 --repeats 18 --iters 3 \
    --out "$out/production-dense-control.json" > "$out/production-dense-control.log" 2>&1
