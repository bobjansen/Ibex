#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run-thread-scaling.sh — how each engine converts cores into throughput.
#
# One box, one dataset size, the whole suite run once per thread count. Every
# engine gets the SAME budget at each point and is pinned to cores 0..T-1, so
# the resulting per-query curve is a property of the engines rather than of who
# read nproc first.
#
# WHY ONE BOX AND NOT SEVERAL SIZES OF BOX: varying the instance type confounds
# software scaling with hardware (different memory bandwidth, different sustained
# clocks, a different NUMA story). Holding the box fixed and varying only the
# budget is the only form of this experiment whose answer is about Ibex.
#
# vCPUs ARE HYPERTHREADS on every current x86 family — r7i.4xlarge is 16 vCPU
# but 8 physical cores. The launcher prints the real topology before it commits
# to anything, and --threads-per-core 1 disables SMT outright when you want a
# clean physical-core curve with no sibling quietly doing half the work.
#
# Default box is r7i.4xlarge (16 vCPU / 8 cores): double the physical cores of
# the published r7i.2xlarge, which is the first genuinely new data point.
#
# Usage:
#   ./benchmarking/aws/run-thread-scaling.sh --on-demand
#   ./benchmarking/aws/run-thread-scaling.sh --threads 1,2,4,8,16 --rows 16M
#   ./benchmarking/aws/run-thread-scaling.sh --type r7i.8xlarge --threads 1,2,4,8,16,32
#   ./benchmarking/aws/run-thread-scaling.sh --threads-per-core 1 --threads 1,2,4,8
#
# Prereq: ./benchmarking/aws/setup.sh (and ideally ./build-ami.sh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="r7i.4xlarge"
THREAD_LIST="1,2,4,8,16"
ROWS="16M"
ENGINES="ibex,python,duckdb"
WARMUP=1
ITERS=5
THREADS_PER_CORE=""
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --threads)          THREAD_LIST="$2"; shift 2 ;;
        --rows)             ROWS="$2"; shift 2 ;;
        --engines)          ENGINES="$2"; shift 2 ;;
        --warmup)           WARMUP="$2"; shift 2 ;;
        --iters)            ITERS="$2"; shift 2 ;;
        --threads-per-core) THREADS_PER_CORE="$2"; shift 2 ;;
        --type)             INSTANCE_TYPE="$2"; shift 2 ;;
        --key)              KEY_NAME="$2"; shift 2 ;;
        --region)           REGION="$2"; shift 2 ;;
        --on-demand)        ON_DEMAND=1; shift ;;
        -h|--help)          sed -n '5,32p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run benchmarking/aws/setup.sh first}"
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/thread_scaling.csv"
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

# ── Topology preflight ────────────────────────────────────────────────────────
read -r VCPUS CORES TPC MEM <<< "$(bench_instance_topology "$REGION" "$INSTANCE_TYPE")"
EFFECTIVE_VCPUS="$VCPUS"
if [[ -n "$THREADS_PER_CORE" && "$CORES" != "?" ]]; then
    EFFECTIVE_VCPUS=$((CORES * THREADS_PER_CORE))
fi

echo "Commit  : ${COMMIT:0:8} ($BRANCH)"
echo "Box     : $(bench_topology_line "$REGION" "$INSTANCE_TYPE")"
if [[ -n "$THREADS_PER_CORE" ]]; then
    echo "          SMT override: ThreadsPerCore=${THREADS_PER_CORE} → ${EFFECTIVE_VCPUS} usable vCPU"
fi
echo "Sweep   : ${THREAD_LIST} threads"
echo "Dataset : ${ROWS}"
echo "Engines : ${ENGINES}"
echo "Type    : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Result  : s3://$S3_BUCKET/$RESULT_KEY"

# Refuse a sweep point the box cannot honour. The box would skip it anyway, but
# finding that out an hour in — after paying for the boot, the build and every
# smaller point — is a poor way to learn you asked for 32 threads on 16 vCPUs.
if [[ "$EFFECTIVE_VCPUS" != "?" ]]; then
    IFS=',' read -r -a SWEEP <<< "$THREAD_LIST"
    for t in "${SWEEP[@]}"; do
        if (( t > EFFECTIVE_VCPUS )); then
            echo "" >&2
            echo "ERROR: sweep asks for ${t} threads but ${INSTANCE_TYPE} offers ${EFFECTIVE_VCPUS} vCPU." >&2
            echo "       Use a larger --type, or drop that point from --threads." >&2
            exit 1
        fi
    done
    # A sweep that never reaches the physical core count leaves the interesting
    # half of the curve unmeasured; worth saying, not worth refusing.
    HIGHEST="${SWEEP[-1]}"
    if [[ "$CORES" != "?" ]] && (( HIGHEST < CORES )); then
        echo "" >&2
        echo "NOTE: the sweep tops out at ${HIGHEST} threads on a ${CORES}-core box —" >&2
        echo "      the curve will not show what full occupancy does." >&2
    fi
fi
echo ""

KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi
bench_cpu_options_args CPU_ARGS "$REGION" "$INSTANCE_TYPE" "$THREADS_PER_CORE"

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_SCALING_MODE=1" \
    "IBEX_SCALING_THREADS=${THREAD_LIST}" \
    "IBEX_SCALING_ROWS=${ROWS}" \
    "IBEX_SCALING_ENGINES=${ENGINES}" \
    "IBEX_WARMUP=${WARMUP}" \
    "IBEX_ITERS=${ITERS}" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_RESULT_KEY=${RESULT_KEY}" \
    "IBEX_REGION=${REGION}")

INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" --instance-type "$INSTANCE_TYPE" \
    --image-id "$AMI" "${MARKET_ARGS[@]}" "${CPU_ARGS[@]}" \
    --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" --security-group-ids "$SG_ID" \
    --user-data "$USER_DATA" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":120,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ibex-thread-scaling},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"

RESULT_DIR="$IBEX_ROOT/benchmarking/results"
OUTPUT="$RESULT_DIR/thread_scaling_aws_${TIMESTAMP}.csv"
MANIFEST="$RESULT_DIR/thread_scaling_aws_${TIMESTAMP}.manifest.json"
mkdir -p "$RESULT_DIR"

# Write the manifest at LAUNCH, not on success: a run that dies still leaves a
# record of what was asked for, and the partial CSV beside it is then readable.
bench_write_manifest "$MANIFEST" "thread-scaling" "$COMMIT" "$BRANCH" \
    "$REGION" "$INSTANCE_TYPE" \
    "thread_sweep=${THREAD_LIST}" \
    "dataset_rows=${ROWS}" \
    "engines=${ENGINES}" \
    "warmup=${WARMUP}" \
    "iters=${ITERS}" \
    "threads_per_core_override=${THREADS_PER_CORE:-none}" \
    "instance_id=${INSTANCE_ID}" \
    "result_key=${RESULT_KEY}"
echo "Manifest: $MANIFEST"
echo "Waiting for results..."

while ! aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" >/dev/null 2>&1; do
    state=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$state" == "terminated" || "$state" == "shutting-down" ]]; then
        # Every completed thread point is already in the partial key. A sweep
        # that died at 16 still answers the question up to 8.
        if aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY%.csv}.partial.csv" \
                "${OUTPUT%.csv}.partial.csv" --region "$REGION" 2>/dev/null; then
            echo "instance ended without a final result; recovered partial sweep" >&2
            echo "Partial: ${OUTPUT%.csv}.partial.csv" >&2
        else
            echo "instance ended with no result; inspect the console log above" >&2
        fi
        exit 1
    fi
    sleep 30
done

aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY%.csv}.box.txt" \
    "${OUTPUT%.csv}.box.txt" --region "$REGION" 2>/dev/null || true
echo "Done: $OUTPUT"
echo "Box facts: ${OUTPUT%.csv}.box.txt"
