#!/usr/bin/env bash
# run-window-ohlc.sh — run the window-OHLC suite on one clean EC2 benchmark box.
#
# Rolling open/close bars per time window per symbol, across Ibex, Polars and
# DuckDB on identical Ibex-generated Parquet. Two sweeps: symbol count at a
# fixed row count, and row count at a fixed symbol count. Every engine is
# pinned to the same cores with the same thread budget.
#
# The artifact is a tarball of per-sweep TSVs plus a versions.txt. A partial
# tarball is refreshed after every sweep under a separate key, so a run that
# stalls or is interrupted still yields every sweep that finished.
#
# --budget-s (default 300) caps one execution of one cell; slower cells stop
# after a single execution and are recorded as over_budget.
#
# Usage:
#   ./benchmarking/aws/run-window-ohlc.sh --on-demand
#   ./benchmarking/aws/run-window-ohlc.sh --rows "5000000 50000000" --cores "8 32"
#   ./benchmarking/aws/run-window-ohlc.sh --budget-s 60

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
# 32 vCPU / 128 GiB: enough memory to hold a 50M-row frame in three engines at
# once, and enough cores for the thread-scaling question to have an answer.
INSTANCE_TYPE="m7i.8xlarge"
ROWS="5000000 20000000 50000000"
SYMBOLS="3 8 20 100"
SWEEP_ROWS="20000000"
SWEEP_SYMBOLS="3"
CORES=""
ITERS=5
BUDGET_S=300
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rows) ROWS="$2"; shift 2 ;;
        --symbols) SYMBOLS="$2"; shift 2 ;;
        --sweep-rows) SWEEP_ROWS="$2"; shift 2 ;;
        --sweep-symbols) SWEEP_SYMBOLS="$2"; shift 2 ;;
        --cores) CORES="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --budget-s) BUDGET_S="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run benchmarking/aws/setup.sh first}"
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/window_ohlc.tar.gz"
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "Rows   : ${ROWS} (at ${SWEEP_SYMBOLS} symbols)"
echo "Symbols: ${SYMBOLS} (at ${SWEEP_ROWS} rows)"
echo "Cores  : ${CORES:-all vCPUs on the box}"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Budget : ${BUDGET_S}s per execution ($([[ "$BUDGET_S" == "0" ]] && echo disabled || echo 'slower cells recorded as over_budget'))"
echo "Result : s3://$S3_BUCKET/$RESULT_KEY"
echo "Partial: s3://$S3_BUCKET/${RESULT_KEY%.tar.gz}.partial.tar.gz (refreshed after each sweep)"

ENV_ARGS=(
    "IBEX_OHLC_MODE=1"
    "IBEX_OHLC_ROWS=${ROWS}"
    "IBEX_OHLC_SYMBOLS=${SYMBOLS}"
    "IBEX_OHLC_SWEEP_ROWS=${SWEEP_ROWS}"
    "IBEX_OHLC_SWEEP_SYMBOLS=${SWEEP_SYMBOLS}"
    "IBEX_OHLC_ITERS=${ITERS}"
    "IBEX_OHLC_BUDGET_S=${BUDGET_S}"
    "IBEX_S3_BUCKET=${S3_BUCKET}"
    "IBEX_RESULT_KEY=${RESULT_KEY}"
    "IBEX_REGION=${REGION}"
)
# Unset means "use every vCPU", which bootstrap resolves with nproc on the box.
[[ -n "$CORES" ]] && ENV_ARGS+=("IBEX_OHLC_CORES=${CORES}")

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" "${ENV_ARGS[@]}")
INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" --instance-type "$INSTANCE_TYPE" --image-id "$AMI" \
    "${MARKET_ARGS[@]}" --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" --security-group-ids "$SG_ID" --user-data "$USER_DATA" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":250,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ibex-window-ohlc},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo "Waiting for artifact..."
while ! aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" >/dev/null 2>&1; do
    state=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$state" == "terminated" || "$state" == "shutting-down" ]]; then
        # The box snapshots after every sweep, so a run that died partway
        # through still has every completed sweep sitting in the partial key.
        PARTIAL="$IBEX_ROOT/benchmarking/results/window_ohlc_aws_${TIMESTAMP}.partial.tar.gz"
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

OUTPUT="$IBEX_ROOT/benchmarking/results/window_ohlc_aws_${TIMESTAMP}.tar.gz"
mkdir -p "$(dirname "$OUTPUT")"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"
echo "Done: $OUTPUT"
echo "Inspect: tar -tzf $OUTPUT && tar -xzOf $OUTPUT results/versions.txt"
