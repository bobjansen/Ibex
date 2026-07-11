#!/usr/bin/env bash
# bisect-git.sh — run a performance git bisect on one EC2 instance.
#
# The instance runs `git bisect` locally and, for each candidate, compares the
# fixed good commit against that candidate with benchmarking/compare_ibex_git.sh.
# This avoids booting/provisioning one machine per bisect step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
GOOD_REF=""
BAD_REF="HEAD"
SUITE="fill"
QUERY="fill_forward"
THRESHOLD_PCT="10"
REPEATS=7
ITERS=9
WARMUP=1
DATA_ROWS=4000000
INTERLEAVE=1
TASKSET_CPUS="2-3"
INSTANCE_TYPE=""
KEY_NAME=""
ON_DEMAND=1

usage() {
    cat <<'EOF'
Usage: bisect-git.sh --good REF [options]

Options:
  --good REF             known-good commit (required; must be pushed)
  --bad REF              known-bad commit (default: HEAD; must be pushed)
  --suite NAME           ibex suite to run per candidate (default: fill)
  --query NAME           query row to classify (default: fill_forward)
  --threshold-pct N      target is bad when delta_pct >= N (default: 10)
  --repeats N            repeats per comparison (default: 7)
  --iters N              timed iterations per repeat (default: 9)
  --warmup N             warmup iterations (default: 1)
  --data-rows N          fact-table rows for gen_data.py (default: 4000000)
  --serial               disable interleaving inside each comparison
  --taskset CPUSET       pin benchmark runs (default: 2-3)
  --type INSTANCE        EC2 instance type (default: auto from --data-rows)
  --key KEY_PAIR         EC2 key pair for SSH debugging (optional)
  --region REGION        override region
  --on-demand            force on-demand (default)
  --spot                 force spot
  -h, --help             show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --good) GOOD_REF="$2"; shift 2 ;;
        --bad) BAD_REF="$2"; shift 2 ;;
        --suite) SUITE="$2"; shift 2 ;;
        --query) QUERY="$2"; shift 2 ;;
        --threshold-pct) THRESHOLD_PCT="$2"; shift 2 ;;
        --repeats) REPEATS="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --data-rows) DATA_ROWS="$2"; shift 2 ;;
        --serial) INTERLEAVE=0; shift ;;
        --taskset) TASKSET_CPUS="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        --spot) ON_DEMAND=0; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

[[ -n "$GOOD_REF" ]] || { echo "ERROR: --good is required" >&2; usage; exit 1; }
[[ "$DATA_ROWS" =~ ^[0-9]+$ ]] || { echo "ERROR: --data-rows must be an integer" >&2; exit 1; }
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --repeats must be positive" >&2; exit 1; }
[[ "$ITERS" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --iters must be positive" >&2; exit 1; }
[[ "$WARMUP" =~ ^[0-9]+$ ]] || { echo "ERROR: --warmup must be non-negative" >&2; exit 1; }
[[ "$THRESHOLD_PCT" =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "ERROR: --threshold-pct must be numeric" >&2; exit 1; }

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

if [[ -z "$INSTANCE_TYPE" ]]; then
    if   (( DATA_ROWS <=  4000000 )); then INSTANCE_TYPE="c7i.2xlarge"
    elif (( DATA_ROWS <= 16000000 )); then INSTANCE_TYPE="r7i.2xlarge"
    elif (( DATA_ROWS <= 32000000 )); then INSTANCE_TYPE="r7i.4xlarge"
    else                                   INSTANCE_TYPE="r7i.8xlarge"
    fi
fi

REPO_URL=$(bench_repo_url "$IBEX_ROOT")
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
GOOD_COMMIT=$(git -C "$IBEX_ROOT" rev-parse "$GOOD_REF^{commit}") \
    || { echo "ERROR: cannot resolve --good '$GOOD_REF'" >&2; exit 1; }
BAD_COMMIT=$(git -C "$IBEX_ROOT" rev-parse "$BAD_REF^{commit}") \
    || { echo "ERROR: cannot resolve --bad '$BAD_REF'" >&2; exit 1; }

[[ "$GOOD_COMMIT" != "$BAD_COMMIT" ]] || { echo "ERROR: good and bad are the same commit" >&2; exit 1; }

bench_require_pushed "$IBEX_ROOT" "$GOOD_COMMIT" "$BRANCH" "$REPO_URL" || exit 1
bench_require_pushed "$IBEX_ROOT" "$BAD_COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/bisect_${TIMESTAMP}_${GOOD_COMMIT:0:8}_${BAD_COMMIT:0:8}/report.txt"

AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Good   : ${GOOD_COMMIT:0:8} ($GOOD_REF)"
echo "Bad    : ${BAD_COMMIT:0:8} ($BAD_REF)"
echo "Suite  : $SUITE"
echo "Query  : $QUERY"
echo "Bad if : delta_pct >= ${THRESHOLD_PCT}%"
echo "Repeats: $REPEATS (interleave $([[ "$INTERLEAVE" -eq 1 ]] && echo on || echo off)), iters $ITERS, warmup $WARMUP"
echo "AMI    : $AMI"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Bucket : s3://$S3_BUCKET"
echo ""

USER_DATA=$(bench_user_data "$REPO_URL" "$BAD_COMMIT" \
    "IBEX_BISECT_MODE=1" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_RESULT_KEY=${RESULT_KEY}" \
    "IBEX_REGION=${REGION}" \
    "IBEX_GOOD=${GOOD_COMMIT}" \
    "IBEX_BAD=${BAD_COMMIT}" \
    "IBEX_SUITE=${SUITE}" \
    "IBEX_BISECT_QUERY=${QUERY}" \
    "IBEX_BISECT_THRESHOLD_PCT=${THRESHOLD_PCT}" \
    "IBEX_REPEATS=${REPEATS}" \
    "IBEX_ITERS=${ITERS}" \
    "IBEX_WARMUP=${WARMUP}" \
    "IBEX_DATA_ROWS=${DATA_ROWS}" \
    "IBEX_INTERLEAVE=${INTERLEAVE}" \
    "IBEX_TASKSET=${TASKSET_CPUS}")

KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")

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
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":80,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench-bisect},{Key=Good,Value=${GOOD_COMMIT:0:8}},{Key=Bad,Value=${BAD_COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Instance: $INSTANCE_ID"
echo "Log     : aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo ""
echo "Waiting for s3://${S3_BUCKET}/${RESULT_KEY} ..."
echo "(single-instance git bisect; runtime depends on commit count and repeats)"
echo ""

TIMEOUT=21600
START=$(date +%s)
DOTS=0
while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after $((TIMEOUT/3600))h. Check instance logs:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
        exit 1
    fi

    if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" &>/dev/null; then
        echo ""
        break
    fi

    if (( DOTS % 5 == 0 )); then
        STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
            --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)
        if [[ "$STATE" == "terminated" || "$STATE" == "shutting-down" ]]; then
            if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" &>/dev/null; then
                echo ""
                break
            fi
            echo ""
            echo "Instance $INSTANCE_ID is '$STATE' without producing a report. Check the console:"
            echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
            exit 1
        fi
    fi

    printf "."
    DOTS=$(( DOTS + 1 ))
    (( DOTS % 20 == 0 )) && echo " $(( (NOW - START) / 60 ))m elapsed"
    sleep 30
done

OUTPUT="$IBEX_ROOT/benchmarking/results/bisect_aws_${TIMESTAMP}.txt"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"

ELAPSED=$(( ($(date +%s) - START) / 60 ))
echo "Done in ${ELAPSED}m. Report: benchmarking/results/bisect_aws_${TIMESTAMP}.txt"
echo ""
cat "$OUTPUT"
