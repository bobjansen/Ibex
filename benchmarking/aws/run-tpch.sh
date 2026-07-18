#!/usr/bin/env bash
# run-tpch.sh — run the TPC-H/PDS-H quartet on one clean EC2 benchmark box.
#
# The artifact contains per-query TSVs for Ibex, this tree's Polars and
# polars-st, and upstream Polars PDS-H's Polars, polars-st and DuckDB SQL.
#
# Usage:
#   ./benchmarking/aws/run-tpch.sh --on-demand --sf 1 --warmup 1 --iters 5

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="r7i.2xlarge"
SCALES="1"
WARMUP=1
ITERS=5
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SCALES="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run benchmarking/aws/setup.sh first}"
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/tpch.tar.gz"
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "TPC-H  : SF-${SCALES}; ${WARMUP} warmup + ${ITERS} timed iterations"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Result : s3://$S3_BUCKET/$RESULT_KEY"

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_TPCH_MODE=1" "IBEX_TPCH_SCALES=${SCALES}" "IBEX_WARMUP=${WARMUP}" "IBEX_ITERS=${ITERS}" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" "IBEX_RESULT_KEY=${RESULT_KEY}" "IBEX_REGION=${REGION}")
INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" --instance-type "$INSTANCE_TYPE" --image-id "$AMI" \
    "${MARKET_ARGS[@]}" --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" --security-group-ids "$SG_ID" --user-data "$USER_DATA" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":250,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ibex-tpch},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo "Waiting for artifact..."
while ! aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" >/dev/null 2>&1; do
    state=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$state" == "terminated" || "$state" == "shutting-down" ]]; then
        echo "instance ended without an artifact; inspect the console log above" >&2
        exit 1
    fi
    sleep 30
done

OUTPUT="$IBEX_ROOT/benchmarking/results/tpch_aws_${TIMESTAMP}.tar.gz"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"
echo "Done: $OUTPUT"
