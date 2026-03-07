#!/usr/bin/env bash
# run.sh — launch a spot EC2 instance, run the scale benchmark suite, download results.
#
# Usage:
#   ./benchmarking/aws/run.sh [options]
#
# Options:
#   --sizes  1M,2M,4M,8M,16M   (default)
#   --warmup N                  warmup iterations  (default: 1)
#   --iters  N                  timed iterations   (default: 5)
#   --type   INSTANCE_TYPE      EC2 instance type  (default: c6i.xlarge)
#   --key    KEY_PAIR_NAME      EC2 key pair for SSH debugging (optional)
#   --region AWS_REGION         override region
#
# Environment:
#   S3_BUCKET   — bucket name (loaded from .config if not set)
#   AWS_REGION  — region      (loaded from .config if not set)
#
# Estimated cost: ~$0.05 for a full 1M–16M run on c6i.xlarge spot

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Load config ───────────────────────────────────────────────────────────────
CONFIG_FILE="$SCRIPT_DIR/.config"
[[ -f "$CONFIG_FILE" ]] && source "$CONFIG_FILE"

# ── Defaults (override via env or args) ───────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="c6i.xlarge"
SIZES="1M,2M,4M,8M,16M"
WARMUP=1
ITERS=5
KEY_NAME=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sizes)   SIZES="$2";         shift 2 ;;
        --warmup)  WARMUP="$2";        shift 2 ;;
        --iters)   ITERS="$2";         shift 2 ;;
        --type)    INSTANCE_TYPE="$2"; shift 2 ;;
        --key)     KEY_NAME="$2";      shift 2 ;;
        --region)  REGION="$2";        shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

# ── Git info ──────────────────────────────────────────────────────────────────
REPO_URL=$(git -C "$IBEX_ROOT" remote get-url origin 2>/dev/null \
    | sed 's|git@github.com:|https://github.com/|')
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/scales.csv"

# ── AMI: latest Ubuntu 24.04 (Noble) amd64 ───────────────────────────────────
AMI=$(aws ec2 describe-images \
    --region "$REGION" \
    --owners 099720109477 \
    --filters \
        "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
        "Name=state,Values=available" \
    --query "sort_by(Images, &CreationDate)[-1].ImageId" \
    --output text)

SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=ibex-bench" \
    --query "SecurityGroups[0].GroupId" \
    --output text)

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "Sizes  : $SIZES"
echo "AMI    : $AMI"
echo "Type   : $INSTANCE_TYPE (spot)"
echo "Bucket : s3://$S3_BUCKET"
echo ""

# ── User-data: clone repo and delegate to bootstrap.sh ───────────────────────
# Variables below expand on the *local* machine; the resulting script has
# hardcoded values so the instance needs no extra configuration.
USER_DATA=$(cat <<EOF
#!/bin/bash
exec > /var/log/ibex-bench.log 2>&1
set -x
apt-get update -qq
apt-get install -y git
git clone "${REPO_URL}" /ibex
cd /ibex && git checkout "${COMMIT}"
IBEX_S3_BUCKET="${S3_BUCKET}" \\
IBEX_RESULT_KEY="${RESULT_KEY}" \\
IBEX_REGION="${REGION}" \\
IBEX_SIZES="${SIZES}" \\
IBEX_WARMUP="${WARMUP}" \\
IBEX_ITERS="${ITERS}" \\
  bash /ibex/benchmarking/aws/bootstrap.sh
EOF
)

# ── Launch spot instance ──────────────────────────────────────────────────────
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")

INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --instance-type "$INSTANCE_TYPE" \
    --image-id "$AMI" \
    --instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}' \
    --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" \
    --security-group-ids "$SG_ID" \
    --user-data "$USER_DATA" \
    --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Instance: $INSTANCE_ID"
echo "Log     : aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --output text"
if [[ -n "$KEY_NAME" ]]; then
    echo "SSH     : ssh ubuntu@<public-ip> tail -f /var/log/ibex-bench.log"
    echo "          (get IP: aws ec2 describe-instances --instance-ids $INSTANCE_ID --region $REGION --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
fi
echo ""
echo "Waiting for s3://${S3_BUCKET}/${RESULT_KEY} ..."
echo "(this typically takes 45–60 min)"
echo ""

# ── Poll S3 ───────────────────────────────────────────────────────────────────
TIMEOUT=7200  # 2 hours
START=$(date +%s)
DOTS=0

while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after 2h. Check instance logs:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --output text"
        exit 1
    fi

    if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" &>/dev/null; then
        echo ""
        break
    fi

    # Print a dot every 30s, newline every 20 dots
    printf "."
    DOTS=$(( DOTS + 1 ))
    (( DOTS % 20 == 0 )) && echo " $(( (NOW - START) / 60 ))m elapsed"
    sleep 30
done

# ── Download results ──────────────────────────────────────────────────────────
OUTPUT="$IBEX_ROOT/benchmarking/results/scales_aws_${TIMESTAMP}.csv"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"

ELAPSED=$(( ($(date +%s) - START) / 60 ))
echo "Done in ${ELAPSED}m. Results: benchmarking/results/scales_aws_${TIMESTAMP}.csv"
