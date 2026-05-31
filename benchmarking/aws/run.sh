#!/usr/bin/env bash
# run.sh — launch an EC2 instance (spot by default), run the scale benchmark suite, download results.
#
# Usage:
#   ./benchmarking/aws/run.sh [options]
#
# Options:
#   --sizes  1M,2M,4M,8M,16M   (default)
#   --warmup N                  warmup iterations  (default: 1)
#   --iters  N                  timed iterations   (default: 5)
#   --type   INSTANCE_TYPE      EC2 instance type  (default: r7i.2xlarge)
#   --key    KEY_PAIR_NAME      EC2 key pair for SSH debugging (optional)
#   --region AWS_REGION         override region
#   --on-demand                 use an on-demand instance instead of spot
#                               (no capacity-reclaim risk; recommended for long
#                                full-suite runs that can't afford to be killed)
#
# Environment:
#   S3_BUCKET   — bucket name (loaded from .config if not set)
#   AWS_REGION  — region      (loaded from .config if not set)
#
# Estimated cost: a full 1M–16M run on r7i.2xlarge (~$0.53/hr on-demand) is well
# under $1; spot is cheaper but risks capacity reclaim on long runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Load config ───────────────────────────────────────────────────────────────
CONFIG_FILE="$SCRIPT_DIR/.config"
[[ -f "$CONFIG_FILE" ]] && source "$CONFIG_FILE"

# ── Defaults (override via env or args) ───────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
# r7i.2xlarge: 8 vCPU (faster parallel build) + 64GB RAM (headroom for the
# in-memory reshape benchmark, which needs ~28GB at 16M). Same Sapphire Rapids
# CPU as c7i, so per-core timings stay comparable; run every size on one box.
INSTANCE_TYPE="r7i.2xlarge"
SIZES="1M,2M,4M,8M,16M"
WARMUP=1
ITERS=5
TF_ROWS=""
BOTH_THREADING=0
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sizes)   SIZES="$2";         shift 2 ;;
        --warmup)  WARMUP="$2";        shift 2 ;;
        --iters)   ITERS="$2";         shift 2 ;;
        --tf-rows) TF_ROWS="$2";       shift 2 ;;
        --both-threading) BOTH_THREADING=1; shift ;;
        --type)    INSTANCE_TYPE="$2"; shift 2 ;;
        --key)     KEY_NAME="$2";      shift 2 ;;
        --region)  REGION="$2";        shift 2 ;;
        --on-demand) ON_DEMAND=1;      shift ;;
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
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Bucket : s3://$S3_BUCKET"
echo ""

# ── User-data: clone repo and delegate to bootstrap.sh ───────────────────────
# Variables below expand on the *local* machine; the resulting script has
# hardcoded values so the instance needs no extra configuration.
USER_DATA=$(cat <<EOF
#!/bin/bash
# Stream to both /var/log/ibex-bench.log (full log for SSH users) AND the
# kernel/serial console (visible via \`aws ec2 get-console-output\` without
# SSH). Buffer overflow on the console is fine — we keep the file as
# the authoritative log.
exec > >(stdbuf -oL tee -a /var/log/ibex-bench.log > /dev/console) 2>&1
set -x
apt-get update -qq
apt-get install -y git
git clone "${REPO_URL}" /ibex
cd /ibex
# Fail loudly if the commit isn't on origin (e.g. forgot to push) instead of
# silently building stale origin/HEAD — user-data has no top-level set -e.
git checkout "${COMMIT}" || {
    echo "FATAL: commit ${COMMIT} not found on origin — did you 'git push'?"
    shutdown -h now
    exit 1
}
IBEX_S3_BUCKET="${S3_BUCKET}" \\
IBEX_RESULT_KEY="${RESULT_KEY}" \\
IBEX_REGION="${REGION}" \\
IBEX_SIZES="${SIZES}" \\
IBEX_WARMUP="${WARMUP}" \\
IBEX_ITERS="${ITERS}" \\
IBEX_TF_ROWS="${TF_ROWS}" \\
IBEX_BOTH_THREADING="${BOTH_THREADING}" \\
  bash /ibex/benchmarking/aws/bootstrap.sh
EOF
)

# ── Launch instance ───────────────────────────────────────────────────────────
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")

# Spot by default (cheapest); --on-demand trades cost for no capacity-reclaim
# risk, which matters for long full-suite runs that can't afford to be killed
# near the end.
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --instance-type "$INSTANCE_TYPE" \
    --image-id "$AMI" \
    "${MARKET_ARGS[@]}" \
    --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" \
    --security-group-ids "$SG_ID" \
    --user-data "$USER_DATA" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
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

    # Detect a dead instance (spot interruption, failed bootstrap, OOM shutdown)
    # so we fail fast instead of waiting out the full 2h timeout for a result
    # that will never arrive. Checked every ~2.5 min to keep API calls light.
    if (( DOTS % 5 == 0 )); then
        STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
            --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
        if [[ "$STATE" == "terminated" || "$STATE" == "shutting-down" ]]; then
            # Race: the instance may have uploaded results just before self-
            # terminating, so re-check S3 once before giving up.
            if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" &>/dev/null; then
                echo ""
                break
            fi
            echo ""
            echo "Instance $INSTANCE_ID is '$STATE' without producing a result —"
            echo "likely a spot interruption or a failed bootstrap. Check the console:"
            echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
            exit 1
        fi
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
