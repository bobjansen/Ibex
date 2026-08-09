#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run-r-only.sh — run the R-only suite on one clean EC2 benchmark box.
#
# Three R-facing paths over identical fixtures -- `data.table`, in-memory
# `dplyr`, and Ibex's native lazy dplyr backend -- across a row-count sweep.
# This is the comparison the website's data.table column cannot make: it is R
# against R, in one process, with no engine reading a different file format.
#
# EIGHT cores, deliberately. Ibex, data.table and dplyr all scale differently
# with thread count, and an unpinned box turns a query comparison into a
# thread-count comparison; eight is the local house cap for cross-engine work.
# The instance type is therefore chosen for its MEMORY, not its cores: 32M rows
# has to fit in three frameworks at once, so the default is an 8-vCPU
# memory-optimized box rather than a general-purpose one of the same width.
#
# Crash robustness follows the OHLC runner: the artifact is refreshed under a
# separate partial key after EVERY size, so a run that is interrupted, runs out
# of memory at the top of the sweep, or loses its spot instance still yields
# every size that finished. The box self-terminates either way.
#
# Usage:
#   ./benchmarking/aws/run-r-only.sh
#   ./benchmarking/aws/run-r-only.sh --sizes 1M,4M --on-demand
#   ./benchmarking/aws/run-r-only.sh --cores 8 --iters 7

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
# 8 vCPU / 64 GiB. The cores are the point of the run; the memory is what makes
# the top of the sweep possible at all. At 32M rows the fixtures alone are
# ~3.5GB on disk, and the events+users phase holds the 32M-row join output in
# both R and Ibex at once. A general-purpose 8-vCPU box (32 GiB) runs the sweep
# up to 16M comfortably and is a reasonable --type when 32M is not needed.
INSTANCE_TYPE="r7i.2xlarge"
SIZES="1M,2M,4M,8M,16M,32M"
CORES=8
WARMUP=1
ITERS=5
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sizes) SIZES="$2"; shift 2 ;;
        --cores) CORES="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        -h|--help) sed -n '2,27p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run benchmarking/aws/setup.sh first}"
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/r_only.tar.gz"
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "Sizes  : ${SIZES}"
echo "Cores  : ${CORES} (process pinned; Ibex, data.table and OpenMP all held to this budget)"
echo "Iters  : ${ITERS} timed, ${WARMUP} warmup"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Result : s3://$S3_BUCKET/$RESULT_KEY"
echo "Partial: s3://$S3_BUCKET/${RESULT_KEY%.tar.gz}.partial.tar.gz (refreshed after each size)"

ENV_ARGS=(
    "IBEX_R_ONLY_MODE=1"
    "IBEX_SIZES=${SIZES}"
    "IBEX_R_ONLY_CORES=${CORES}"
    "IBEX_WARMUP=${WARMUP}"
    "IBEX_ITERS=${ITERS}"
    "IBEX_S3_BUCKET=${S3_BUCKET}"
    "IBEX_RESULT_KEY=${RESULT_KEY}"
    "IBEX_REGION=${REGION}"
)

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" "${ENV_ARGS[@]}")
INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" --instance-type "$INSTANCE_TYPE" --image-id "$AMI" \
    "${MARKET_ARGS[@]}" --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" --security-group-ids "$SG_ID" --user-data "$USER_DATA" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":200,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ibex-r-only},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo "Waiting for artifact..."
while ! aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" >/dev/null 2>&1; do
    state=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$state" == "terminated" || "$state" == "shutting-down" ]]; then
        # The box snapshots after every size, so a sweep that died at 32M still
        # has 1M..16M sitting in the partial key.
        PARTIAL="$IBEX_ROOT/benchmarking/results/r_only_aws_${TIMESTAMP}.partial.tar.gz"
        mkdir -p "$(dirname "$PARTIAL")"
        if aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY%.tar.gz}.partial.tar.gz" \
                "$PARTIAL" --region "$REGION" 2>/dev/null; then
            echo "instance ended without a final artifact; recovered partial results" >&2
            echo "Partial: $PARTIAL" >&2
        else
            echo "instance ended without an artifact; inspect the console log above" >&2
        fi
        exit 1
    fi
    sleep 30
done

OUTPUT="$IBEX_ROOT/benchmarking/results/r_only_aws_${TIMESTAMP}.tar.gz"
mkdir -p "$(dirname "$OUTPUT")"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"
echo "Done: $OUTPUT"
echo "Inspect: tar -tzf $OUTPUT && tar -xzOf $OUTPUT r_only/versions.txt"
echo "Combined TSV: tar -xzOf $OUTPUT r_only/combined.tsv | column -t -s\$'\\t' | head"
