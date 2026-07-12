#!/usr/bin/env bash
# compare-plugin-git.sh — A/B two git commits of ibex for a plugin-backed
# extern function (read_parquet, read_adbc, kafka_recv, ...), on a clean EC2
# box.
#
# The local benchmarking/compare_plugin_git.sh is the tool for this, but a
# laptop / WSL2 box is too noisy for sub-5% verdicts (thermal drift,
# background load). This runs the identical comparison on a dedicated, idle,
# fixed-clock EC2 instance — both commits are built and timed on the same
# box, repeats interleaved — and pulls the report back.
#
# Why a separate tool from compare-git.sh: ibex_bench (and therefore
# compare-git.sh) never loads a dynamically-loaded plugin — its "scan mode"
# hardcodes a direct C++ call to read_csv(), and compare_ibex_git.sh's
# configure_and_build builds with -DIBEX_BUILD_PARQUET=OFF. Neither can
# exercise read_parquet, read_adbc, or any other plugin-registered extern
# function. This drives the real REPL binary (tools/ibex --plugin-path ...)
# on the remote box instead, via benchmarking/compare_plugin_git.sh.
#
# Boots from the baked AMI (build-ami.sh) when configured in .config,
# otherwise from stock Ubuntu (provisioning then runs on the instance).
#
# Usage:
#   ./benchmarking/aws/compare-plugin-git.sh \
#       --query benchmarking/queries/parquet_scan_agg.ibex \
#       --plugin-target ibex_parquet_plugin \
#       --configure-arg -DIBEX_BUILD_PARQUET=ON
#
# Options:
#   --base   REF            base commit   (default: HEAD~1; must be pushed)
#   --target REF             target commit (default: HEAD;   must be pushed)
#   --query <path>            query template, repo-relative, __INPUT__ token (required)
#   --plugin-target <t1,..>   cmake target(s) building the plugin .so (required)
#   --configure-arg <arg>     extra `cmake -D...` configure arg (repeatable)
#   --parquet-rows N          rows for benchmarking/gen_parquet_data.py fixture
#                              regenerated on the instance (default: 8000000)
#   --repeats N               repeats per side, (default: 3)
#   --iters   N                timed iterations per repeat       (default: 5)
#   --warmup  N                warmup iterations                 (default: 1)
#   --inner-runs N             REPL invocations per timed iteration (default: 1)
#   --serial                   disable interleaving (all base repeats, then target)
#   --taskset CPUSET           pin the benchmark cores             (default: 2)
#   --type   INSTANCE          EC2 instance type   (default: c7i.2xlarge)
#   --key    KEY_PAIR          EC2 key pair for SSH debugging (optional)
#   --region REGION            override region
#   --on-demand / --spot       force market type (default: spot)
#
# Environment: S3_BUCKET / AWS_REGION / IBEX_AMI are loaded from .config.
#
# Cost: a single comparison on c7i.2xlarge (~$0.36/hr on-demand) builds two
# ibex trees (with Arrow/Parquet ON, unlike compare-git.sh) + a handful of
# repeats — typically 15-30 min, well under $0.50.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="c7i.2xlarge"
BASE_REF="HEAD~1"
TARGET_REF="HEAD"
QUERY=""
PLUGIN_TARGETS=""
declare -a CONFIGURE_ARGS=()
PARQUET_ROWS=8000000
REPEATS=3
ITERS=5
WARMUP=1
INNER_RUNS=1
INTERLEAVE=1
TASKSET_CPUS="2"
KEY_NAME=""
ON_DEMAND=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base) BASE_REF="$2"; shift 2 ;;
        --target) TARGET_REF="$2"; shift 2 ;;
        --query) QUERY="$2"; shift 2 ;;
        --plugin-target) PLUGIN_TARGETS="$2"; shift 2 ;;
        --configure-arg) CONFIGURE_ARGS+=("$2"); shift 2 ;;
        --parquet-rows) PARQUET_ROWS="$2"; shift 2 ;;
        --repeats) REPEATS="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --inner-runs) INNER_RUNS="$2"; shift 2 ;;
        --serial) INTERLEAVE=0; shift ;;
        --taskset) TASKSET_CPUS="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        --spot) ON_DEMAND=0; shift ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$QUERY" || -z "$PLUGIN_TARGETS" ]]; then
    echo "ERROR: --query and --plugin-target are required" >&2
    exit 1
fi

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

# ── Resolve refs + push preflight ─────────────────────────────────────────────
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
BASE_COMMIT=$(git -C "$IBEX_ROOT" rev-parse "$BASE_REF^{commit}") \
    || { echo "ERROR: cannot resolve --base '$BASE_REF'" >&2; exit 1; }
TARGET_COMMIT=$(git -C "$IBEX_ROOT" rev-parse "$TARGET_REF^{commit}") \
    || { echo "ERROR: cannot resolve --target '$TARGET_REF'" >&2; exit 1; }

if [[ "$BASE_COMMIT" == "$TARGET_COMMIT" ]]; then
    echo "ERROR: base and target resolve to the same commit ${BASE_COMMIT:0:8}." >&2
    exit 1
fi

# The instance clones origin and builds both commits there (via
# compare_plugin_git.sh's own worktree machinery), so both must be on upstream.
bench_require_pushed "$IBEX_ROOT" "$BASE_COMMIT" "$BRANCH" "$REPO_URL" || exit 1
bench_require_pushed "$IBEX_ROOT" "$TARGET_COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/compare_plugin_${TIMESTAMP}_${BASE_COMMIT:0:8}_${TARGET_COMMIT:0:8}/report.txt"

AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Base   : ${BASE_COMMIT:0:8} ($BASE_REF)"
echo "Target : ${TARGET_COMMIT:0:8} ($TARGET_REF)"
echo "Query  : $QUERY"
echo "Plugin : $PLUGIN_TARGETS"
echo "Repeats: $REPEATS (interleave $([[ "$INTERLEAVE" -eq 1 ]] && echo on || echo off)), iters $ITERS, warmup $WARMUP, inner-runs $INNER_RUNS"
echo "AMI    : $AMI"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Bucket : s3://$S3_BUCKET"
echo ""

USER_DATA=$(bench_user_data "$REPO_URL" "$TARGET_COMMIT" \
    "IBEX_COMPARE_PLUGIN_MODE=1" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_RESULT_KEY=${RESULT_KEY}" \
    "IBEX_REGION=${REGION}" \
    "IBEX_BASE=${BASE_COMMIT}" \
    "IBEX_TARGET=${TARGET_COMMIT}" \
    "IBEX_QUERY=${QUERY}" \
    "IBEX_PLUGIN_TARGETS=${PLUGIN_TARGETS}" \
    "IBEX_CONFIGURE_ARGS=${CONFIGURE_ARGS[*]}" \
    "IBEX_PARQUET_ROWS=${PARQUET_ROWS}" \
    "IBEX_REPEATS=${REPEATS}" \
    "IBEX_ITERS=${ITERS}" \
    "IBEX_WARMUP=${WARMUP}" \
    "IBEX_INNER_RUNS=${INNER_RUNS}" \
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
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench-compare-plugin},{Key=Base,Value=${BASE_COMMIT:0:8}},{Key=Target,Value=${TARGET_COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Instance: $INSTANCE_ID"
echo "Log     : aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo ""
echo "Waiting for s3://${S3_BUCKET}/${RESULT_KEY} ..."
echo "(build of both commits + repeats; typically 15-30 min)"
echo ""

TIMEOUT=10800
START=$(date +%s)
DOTS=0

while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out. Check instance logs:"
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
            echo "Instance $INSTANCE_ID is '$STATE' without producing a report —"
            echo "likely a spot interruption or a failed build. Check the console:"
            echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
            exit 1
        fi
    fi

    printf "."
    DOTS=$(( DOTS + 1 ))
    (( DOTS % 20 == 0 )) && echo " $(( (NOW - START) / 60 ))m elapsed"
    sleep 30
done

OUTPUT="$IBEX_ROOT/benchmarking/results/compare_plugin_aws_${TIMESTAMP}.txt"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"

ELAPSED=$(( ($(date +%s) - START) / 60 ))
echo "Done in ${ELAPSED}m. Report: benchmarking/results/compare_plugin_aws_${TIMESTAMP}.txt"
echo ""
cat "$OUTPUT"
