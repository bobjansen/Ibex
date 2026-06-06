#!/usr/bin/env bash
# run.sh — launch one EC2 instance (spot by default), run the whole scale
# benchmark suite on it, download the results.
#
# For one-box-per-engine fan-out (each engine isolated on its own instance,
# running in parallel) use run-per-engine.sh instead.
#
# Boots from the baked AMI (build-ami.sh) when configured in .config, otherwise
# from the stock Ubuntu image (provisioning then runs on the instance).
#
# Usage:
#   ./benchmarking/aws/run.sh [options]
#
# Options:
#   --sizes  1M,2M,4M,8M,16M,32M,50M   (default)
#   --warmup N                  warmup iterations  (default: 1)
#   --iters  N                  timed iterations   (default: 5)
#   --tf-rows N                 override TF row count
#   --both-threading            also run single-thread duckdb/datafusion/clickhouse
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
#   IBEX_AMI    — baked AMI id (loaded from .config; written by build-ami.sh)
#
# Estimated cost: a full 1M–16M run on r7i.2xlarge (~$0.53/hr on-demand) is well
# under $1; spot is cheaper but risks capacity reclaim on long runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

# ── Load config ───────────────────────────────────────────────────────────────
bench_load_config "$SCRIPT_DIR"

# ── Defaults (override via env or args) ───────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
# r7i.2xlarge: 8 vCPU (faster parallel build) + 64GB RAM (headroom for the
# in-memory reshape benchmark, which needs ~28GB at 16M). Same Sapphire Rapids
# CPU as c7i, so per-core timings stay comparable; run every size on one box.
INSTANCE_TYPE="r7i.2xlarge"
SIZES="1M,2M,4M,8M,16M,32M,50M"
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

# ── Git info + push preflight ─────────────────────────────────────────────────
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/scales.csv"

# The EC2 instance clones from origin, so a local-only commit cannot be
# benchmarked by this runner.
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

# ── AMI + security group ──────────────────────────────────────────────────────
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "Sizes  : $SIZES"
echo "AMI    : $AMI"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Bucket : s3://$S3_BUCKET"
echo ""

# ── User-data: clone/update repo and delegate to bootstrap.sh ─────────────────
USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_RESULT_KEY=${RESULT_KEY}" \
    "IBEX_REGION=${REGION}" \
    "IBEX_SIZES=${SIZES}" \
    "IBEX_WARMUP=${WARMUP}" \
    "IBEX_ITERS=${ITERS}" \
    "IBEX_TF_ROWS=${TF_ROWS}" \
    "IBEX_BOTH_THREADING=${BOTH_THREADING}")

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
PARTIAL_KEY="${RESULT_KEY%scales.csv}scales.partial.csv"
echo "Waiting for s3://${S3_BUCKET}/${RESULT_KEY} ..."
echo "(45 min for a 1M–16M run; a full 1M–50M sweep can take several hours)"
echo "Live partial progress (completed sizes, refreshed ~60s):"
echo "  aws s3 cp s3://${S3_BUCKET}/${PARTIAL_KEY} - --region ${REGION} | column -t -s,"
echo ""

# ── Poll S3 ───────────────────────────────────────────────────────────────────
TIMEOUT=21600  # 6 hours — a full 1M–50M sweep with all engines can run this long
START=$(date +%s)
DOTS=0

while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after 6h. Check instance logs:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --output text"
        exit 1
    fi

    if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" &>/dev/null; then
        echo ""
        break
    fi

    # Detect a dead instance (spot interruption, failed bootstrap, OOM shutdown)
    # so we fail fast instead of waiting out the full timeout for a result that
    # will never arrive. Checked every ~2.5 min to keep API calls light.
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
