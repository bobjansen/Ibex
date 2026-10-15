#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run-h2o.sh — run the h2o db-benchmark (benchmarking/h2o) on one clean EC2 box.
#
# Ibex, Polars 2 streaming and DuckDB on the upstream h2o data, generated on the
# box by the vendored upstream R generators. run.py pins every engine to cores
# 0..N-1; with --threads-per-core 1 (the default here) those are N physical
# cores. One full run per --cores count, all on the same box.
#
# Options:
#   --data LIST          comma-separated h2o data names (default: the standard
#                        1e7 set: J1_1e7_NA_0_0 and five G1_1e7_* groupby sets)
#   --cores LIST         core counts, one full run each (default: 1,8)
#   --type TYPE          instance type (default: r7i.4xlarge: 8 physical cores)
#   --threads-per-core N 1 disables SMT (default 1; pass 2 to keep it)
#   --volume-size GB     root volume (default 100)
#   --warmup N / --iters N / --key KEY / --region R / --on-demand
#
# The commit must be pushed: the box clones origin and checks out HEAD.
#
# Usage:
#   ./benchmarking/aws/run-h2o.sh --on-demand
#   ./benchmarking/aws/run-h2o.sh --on-demand --cores 1,2,4,8 --data G1_1e7_1e2_0_0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="r7i.4xlarge"
DATA="J1_1e7_NA_0_0,G1_1e7_1e2_0_0,G1_1e7_1e2_5_0,G1_1e7_1e2_0_1,G1_1e7_1e1_0_0,G1_1e7_2e0_0_0"
CORES="1,8"
THREADS_PER_CORE=1
VOLUME_SIZE=100
WARMUP=1
ITERS=5
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --data) DATA="$2"; shift 2 ;;
        --cores) CORES="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --threads-per-core) THREADS_PER_CORE="$2"; shift 2 ;;
        --volume-size) VOLUME_SIZE="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

for _d in ${DATA//,/ }; do
    if [[ ! "$_d" =~ ^(G1|J1)_[0-9e]+_[0-9eNA]+_[0-9]+_[01]$ ]]; then
        echo "error: '$_d' is not an h2o data name (e.g. G1_1e7_1e2_0_0, J1_1e7_NA_0_0)" >&2
        exit 1
    fi
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run benchmarking/aws/setup.sh first}"
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/h2o.tar.gz"
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

# A count above the box's physical cores would put two engines' threads on SMT
# siblings, or fail taskset: refuse it here.
read -r _vcpus _phys _tpc _mem <<< "$(bench_instance_topology "$REGION" "$INSTANCE_TYPE")"
if [[ "$_phys" != "?" ]]; then
    for _n in ${CORES//,/ }; do
        if (( _n > _phys )); then
            echo "error: --cores ${_n} exceeds ${INSTANCE_TYPE}'s ${_phys} physical cores" >&2
            exit 1
        fi
    done
fi

CPU_ARGS=()
bench_cpu_options_args CPU_ARGS "$REGION" "$INSTANCE_TYPE" "$THREADS_PER_CORE"
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "h2o    : ${DATA}; ${WARMUP} warmup + ${ITERS} timed iterations"
echo "Cores  : ${CORES} (ThreadsPerCore=${THREADS_PER_CORE})"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Box    : $(bench_topology_line "$REGION" "$INSTANCE_TYPE")"
echo "Result : s3://$S3_BUCKET/$RESULT_KEY"

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_H2O_MODE=1" "IBEX_H2O_DATA=${DATA}" "IBEX_H2O_CORES=${CORES}" \
    "IBEX_WARMUP=${WARMUP}" "IBEX_ITERS=${ITERS}" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" "IBEX_RESULT_KEY=${RESULT_KEY}" "IBEX_REGION=${REGION}")
INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" --instance-type "$INSTANCE_TYPE" --image-id "$AMI" \
    "${MARKET_ARGS[@]}" "${CPU_ARGS[@]}" --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" --security-group-ids "$SG_ID" --user-data "$USER_DATA" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${VOLUME_SIZE},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ibex-h2o},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo "Waiting for artifact..."
PARTIAL_KEY="${RESULT_KEY%.tar.gz}.partial.tar.gz"
OUTPUT="$IBEX_ROOT/benchmarking/results/h2o_aws_${TIMESTAMP}.tar.gz"
while ! aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" >/dev/null 2>&1; do
    state=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$state" == "terminated" || "$state" == "shutting-down" ]]; then
        echo "instance ended without a final artifact" >&2
        if aws s3 cp "s3://${S3_BUCKET}/${PARTIAL_KEY}" "${OUTPUT%.tar.gz}.PARTIAL.tar.gz" \
               --region "$REGION" >/dev/null 2>&1; then
            echo "PARTIAL result: ${OUTPUT%.tar.gz}.PARTIAL.tar.gz" >&2
        fi
        exit 1
    fi
    sleep 30
done

aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"
echo "Done: $OUTPUT"
# A clean finish still reports a failed run.py; say so rather than read as success.
if tar -xzOf "$OUTPUT" results/failures.txt 2>/dev/null | grep -q .; then
    echo "WARNING: some core counts failed or mismatched; see results/failures.txt" >&2
fi
