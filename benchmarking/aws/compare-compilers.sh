#!/usr/bin/env bash
# compare-compilers.sh — Clang vs GCC for Ibex generated C++ on one EC2 box.
#
# Launches a clean AWS instance, builds Ibex Release with native optimizations,
# generates fixed benchmark CSVs, then compiles/times the generated query C++
# with latest Clang and latest GCC on the same machine.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE=""
REPEATS=3
ITERS=7
WARMUP=1
DATA_ROWS=4000000
INTERLEAVE=1
TASKSET_CPUS="2-3"
KEY_NAME=""
ON_DEMAND=-1

usage() {
    cat <<'EOF'
Usage: compare-compilers.sh [options]

Options:
  --repeats N           repeats per compiler, median-reduced (default: 3)
  --iters N             timed iterations inside each compiled binary (default: 7)
  --warmup N            warmup iterations inside each compiled binary (default: 1)
  --data-rows N         fact-table rows for gen_data.py (default: 4000000)
  --serial              disable interleaving (all clang repeats, then gcc)
  --taskset CPUSET      pin the benchmark cores (default: 2-3)
  --type INSTANCE       EC2 instance type (default: auto from --data-rows)
  --key KEY_PAIR        EC2 key pair for SSH debugging (optional)
  --region REGION       override region
  --on-demand           force on-demand (default for >4M rows)
  --spot                force spot (default for 4M rows)
  -h, --help            show this help

Environment: S3_BUCKET / AWS_REGION / IBEX_AMI are loaded from .config.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
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

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

[[ "$DATA_ROWS" =~ ^[0-9]+$ ]] || { echo "ERROR: --data-rows must be an integer (e.g. 16000000)" >&2; exit 1; }
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --repeats must be a positive integer" >&2; exit 1; }
[[ "$ITERS" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: --iters must be a positive integer" >&2; exit 1; }
[[ "$WARMUP" =~ ^[0-9]+$ ]] || { echo "ERROR: --warmup must be a non-negative integer" >&2; exit 1; }

if [[ -z "$INSTANCE_TYPE" ]]; then
    if   (( DATA_ROWS <=  4000000 )); then INSTANCE_TYPE="c7i.2xlarge"
    elif (( DATA_ROWS <= 16000000 )); then INSTANCE_TYPE="r7i.2xlarge"
    elif (( DATA_ROWS <= 32000000 )); then INSTANCE_TYPE="r7i.4xlarge"
    else                                   INSTANCE_TYPE="r7i.8xlarge"
    fi
fi

if (( ON_DEMAND < 0 )); then
    if (( DATA_ROWS > 4000000 )); then
        ON_DEMAND=1
        echo "note: scale run (${DATA_ROWS} rows) → on-demand to avoid spot reclaim; pass --spot to override." >&2
    else
        ON_DEMAND=0
    fi
fi

REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/compare_compilers_${TIMESTAMP}_${COMMIT:0:8}/report.txt"

AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "Rows   : $DATA_ROWS"
echo "Repeats: $REPEATS (interleave $([[ "$INTERLEAVE" -eq 1 ]] && echo on || echo off)), iters $ITERS, warmup $WARMUP"
echo "AMI    : $AMI"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Bucket : s3://$S3_BUCKET"
echo ""

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_COMPARE_COMPILERS_MODE=1" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_RESULT_KEY=${RESULT_KEY}" \
    "IBEX_REGION=${REGION}" \
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
        "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench-compilers},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Instance: $INSTANCE_ID"
echo "Log     : aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo ""
echo "Waiting for s3://${S3_BUCKET}/${RESULT_KEY} ..."
echo "(build + generated-query compiler comparison; typically 20-40 min at 4M rows)"
echo ""

TIMEOUT=14400
START=$(date +%s)
DOTS=0

while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after 4h. Check instance logs:"
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

OUTPUT="$IBEX_ROOT/benchmarking/results/compare_compilers_aws_${TIMESTAMP}.txt"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"

ELAPSED=$(( ($(date +%s) - START) / 60 ))
echo "Done in ${ELAPSED}m. Report: benchmarking/results/compare_compilers_aws_${TIMESTAMP}.txt"
echo ""
cat "$OUTPUT"
