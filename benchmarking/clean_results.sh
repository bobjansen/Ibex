#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: benchmarking/clean_results.sh [--apply]

Preview cleanup of Git-ignored outputs in Ibex benchmark result locations.
Pass --apply to delete the listed results, archives, and profiling artifacts.
Tracked files, input datasets, build caches, and AWS configuration are preserved.
EOF
}

if [[ $# -eq 0 ]]; then
    mode=-n
elif [[ $# -eq 1 && $1 == --apply ]]; then
    mode=-f
elif [[ $# -eq 1 && ( $1 == -h || $1 == --help ) ]]; then
    usage
    exit 0
else
    usage >&2
    exit 2
fi

repo_root="$(dirname -- "${BASH_SOURCE[0]}")/.."
git -C "$repo_root" clean "$mode" -dX -- \
    bench_results \
    benchmarking/results \
    benchmarking/h2o/results \
    benchmarking/tpch/results \
    benchmarking/tpch/report/out \
    benchmarking/discovery-policy/results \
    benchmarking/width-policy/results \
    benchmarking/worker-caps/results \
    benchmarking/window_ohlc/results \
    ':(glob)benchmarking/int32/**/*.json' \
    benchmarking/composite-hash/results \
    ':(glob)benchmarking/composite-hash/ibex-perfcmp.*' \
    ':(glob)benchmarking/composite-hash/**/*.'{log,csv,json,out,perf,asm,symbol} \
    ':(glob)benchmarking/composite-hash/**/q18-perf-*.txt' \
    python/h2o-adapter-results \
    python/join-performance-results \
    python/pdsh-performance-results \
    python/polars-engines-results
