#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run-all.sh — launch every benchmark suite behind the website from one command.
#
# The suites are independent AWS runs on independent boxes, so they go in
# PARALLEL and this script waits for all of them. What it adds over invoking the
# three runners by hand is a single run id and one index manifest tying their
# artifacts to one commit — the thing that makes a set of pages citable as one
# result rather than three measurements that happen to sit next to each other.
#
# Two tiers, deliberately:
#
#   main          every engine, the headline cross-engine comparison
#                 (run-per-engine.sh, one box per engine)
#   window-ohlc   4 engines, the bar/resample deep dive
#   thread-scaling 3 engines, how cores turn into throughput
#
# Deep dives drop pandas, dplyr and the -st variants on purpose. They exist to
# answer one question well, and an engine that loses by 50x on every cell adds
# a column nobody reads. `ibex-st` stays where it is the control (main), and is
# skipped in thread-scaling because threads=1 IS that measurement.
#
# Usage:
#   ./benchmarking/aws/run-all.sh                        # all three, spot
#   ./benchmarking/aws/run-all.sh --suites main          # just the headline
#   ./benchmarking/aws/run-all.sh --suites thread-scaling --scaling-threads 1,2,4,8,16
#   ./benchmarking/aws/run-all.sh --on-demand
#
# Each sub-runner keeps its own options; the flags here cover what differs
# between runs. Anything more specific: call that runner directly.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
SUITES="main,window-ohlc,thread-scaling"
ON_DEMAND=0
KEY_NAME=""

# Per-suite knobs. The defaults are the published configuration for `main` and
# the intended first run for the other two.
MAIN_TYPE="r7i.2xlarge"
MAIN_SIZES="1M,2M,4M,8M,16M,32M,50M"
MAIN_ENGINES="ibex,python,r,duckdb,datafusion,clickhouse"
OHLC_TYPE="m7i.8xlarge"
SCALING_TYPE="r7i.4xlarge"
SCALING_THREADS="1,2,4,8,16"
SCALING_ROWS="16M"
SCALING_ENGINES="ibex,python,duckdb"
SCALING_TPC=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --suites)            SUITES="$2"; shift 2 ;;
        --main-type)         MAIN_TYPE="$2"; shift 2 ;;
        --main-sizes)        MAIN_SIZES="$2"; shift 2 ;;
        --main-engines)      MAIN_ENGINES="$2"; shift 2 ;;
        --ohlc-type)         OHLC_TYPE="$2"; shift 2 ;;
        --scaling-type)      SCALING_TYPE="$2"; shift 2 ;;
        --scaling-threads)   SCALING_THREADS="$2"; shift 2 ;;
        --scaling-rows)      SCALING_ROWS="$2"; shift 2 ;;
        --scaling-engines)   SCALING_ENGINES="$2"; shift 2 ;;
        --scaling-threads-per-core) SCALING_TPC="$2"; shift 2 ;;
        --key)               KEY_NAME="$2"; shift 2 ;;
        --region)            REGION="$2"; shift 2 ;;
        --on-demand)         ON_DEMAND=1; shift ;;
        -h|--help)           sed -n '5,32p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

declare -A SUITE_KNOWN=([main]=1 [window-ohlc]=1 [thread-scaling]=1)
IFS=',' read -r -a SUITE_LIST <<< "$SUITES"
for s in "${SUITE_LIST[@]}"; do
    [[ -n "${SUITE_KNOWN[$s]+x}" ]] || {
        echo "error: unknown suite '$s' (valid: ${!SUITE_KNOWN[*]})" >&2; exit 1; }
done

# Fail the whole orchestration before spending anything if the commit is not
# fetchable — otherwise three boxes boot, clone, and terminate one by one.
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

RUN_ID=$(date -u +%Y%m%dT%H%M%S)_${COMMIT:0:8}
RUN_DIR="$IBEX_ROOT/benchmarking/results/runs/$RUN_ID"
mkdir -p "$RUN_DIR"

COMMON_ARGS=(--region "$REGION")
[[ "$ON_DEMAND" -eq 1 ]] && COMMON_ARGS+=(--on-demand)
[[ -n "$KEY_NAME" ]] && COMMON_ARGS+=(--key "$KEY_NAME")

echo "Run id  : $RUN_ID"
echo "Commit  : ${COMMIT:0:8} ($BRANCH)"
echo "Suites  : ${SUITE_LIST[*]}"
echo "Market  : $([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot)"
echo "Logs    : $RUN_DIR"
echo ""
for s in "${SUITE_LIST[@]}"; do
    case "$s" in
        main)           echo "  main          $(bench_topology_line "$REGION" "$MAIN_TYPE")" ;;
        window-ohlc)    echo "  window-ohlc   $(bench_topology_line "$REGION" "$OHLC_TYPE")" ;;
        thread-scaling) echo "  thread-scaling $(bench_topology_line "$REGION" "$SCALING_TYPE") — sweep ${SCALING_THREADS}" ;;
    esac
done
echo ""

# ── Launch each suite in the background ───────────────────────────────────────
declare -A PID_OF
for s in "${SUITE_LIST[@]}"; do
    log="$RUN_DIR/${s}.log"
    case "$s" in
        main)
            "$SCRIPT_DIR/run-per-engine.sh" "${COMMON_ARGS[@]}" \
                --type "$MAIN_TYPE" --sizes "$MAIN_SIZES" --engines "$MAIN_ENGINES" \
                > "$log" 2>&1 &
            ;;
        window-ohlc)
            "$SCRIPT_DIR/run-window-ohlc.sh" "${COMMON_ARGS[@]}" \
                --type "$OHLC_TYPE" > "$log" 2>&1 &
            ;;
        thread-scaling)
            tpc_args=()
            [[ -n "$SCALING_TPC" ]] && tpc_args=(--threads-per-core "$SCALING_TPC")
            "$SCRIPT_DIR/run-thread-scaling.sh" "${COMMON_ARGS[@]}" \
                --type "$SCALING_TYPE" --threads "$SCALING_THREADS" \
                --rows "$SCALING_ROWS" --engines "$SCALING_ENGINES" \
                "${tpc_args[@]}" > "$log" 2>&1 &
            ;;
    esac
    PID_OF[$s]=$!
    echo "  launched ${s} (pid ${PID_OF[$s]}) → ${log}"
done

echo ""
echo "Waiting. Follow along with:  tail -f $RUN_DIR/*.log"
echo ""

declare -A EXIT_OF
FAILED=0
for s in "${SUITE_LIST[@]}"; do
    rc=0
    wait "${PID_OF[$s]}" || rc=$?
    EXIT_OF[$s]=$rc
    if (( rc == 0 )); then
        echo "✓ ${s} finished"
    else
        # One suite failing must not abandon the others — they are separate
        # boxes and separate pages, and the ones that worked are still results.
        echo "✗ ${s} FAILED (exit ${rc}) — see $RUN_DIR/${s}.log" >&2
        FAILED=$(( FAILED + 1 ))
    fi
done

# ── Index manifest ────────────────────────────────────────────────────────────
# Points at each suite's own manifest rather than restating it, so there is
# exactly one place per suite where the box and settings are recorded.
INDEX="$RUN_DIR/manifest.json"
{
    printf '{\n'
    printf '  "run_id": "%s",\n' "$RUN_ID"
    printf '  "commit": "%s",\n' "$COMMIT"
    printf '  "branch": "%s",\n' "$BRANCH"
    printf '  "generated_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '  "region": "%s",\n' "$REGION"
    printf '  "market": "%s",\n' "$([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot)"
    printf '  "suites": {\n'
    first=1
    for s in "${SUITE_LIST[@]}"; do
        (( first == 0 )) && printf ',\n'
        first=0
        printf '    "%s": { "exit_code": %s, "log": "%s" }' \
            "$s" "${EXIT_OF[$s]}" "${s}.log"
    done
    printf '\n  }\n}\n'
} > "$INDEX"

echo ""
echo "Run $RUN_ID complete: $(( ${#SUITE_LIST[@]} - FAILED ))/${#SUITE_LIST[@]} suite(s) succeeded."
echo "  $INDEX"
echo ""
echo "Per-suite artifacts and manifests are under benchmarking/results/."
(( FAILED > 0 )) && exit 1
exit 0
