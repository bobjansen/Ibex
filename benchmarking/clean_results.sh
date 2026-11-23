#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: benchmarking/clean_results.sh [--include-data] [--apply]

Preview cleanup of Git-ignored results, archives, and profiling artifacts.
Pass --include-data to also select ignored benchmark datasets and related files.
Pass --apply to delete the selected files. Future runs may need to regenerate
or fetch the datasets. Tracked files and AWS configuration are preserved.
EOF
}

mode=-n
data_paths=()
for arg in "$@"; do
    case "$arg" in
        --apply) mode=-f ;;
        --include-data) data_paths=(benchmarking/data benchmarking/window_ohlc/data) ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done

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
    python/polars-engines-results \
    "${data_paths[@]}"
