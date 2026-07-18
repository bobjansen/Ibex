#!/usr/bin/env bash
# run-per-engine.sh — run the scale benchmark with ONE EC2 instance per engine.
#
# Instead of run.sh's single box that runs every engine sequentially, this
# launches a fleet — one instance per engine — so each engine gets a whole box
# to itself (no cross-engine memory pressure or thread contention) and they all
# run in parallel. Each instance runs run_scale_suite.sh with only its own
# engine enabled (via IBEX_SUITE_ARGS), uploads its slice of scales.csv, then
# self-terminates. This script polls all of them and concatenates the slices
# into one combined CSV identical in shape to run.sh's output.
#
# Boots from the baked AMI (build-ami.sh) when configured — strongly recommended
# here, since otherwise every engine's box pays the full provisioning cost.
#
# Prereq: ./benchmarking/aws/setup.sh   (and ideally ./build-ami.sh)
#
# Usage:
#   ./benchmarking/aws/run-per-engine.sh [options]
#
# Options:
#   --engines LIST   comma-separated subset of:
#                      ibex,python,r,duckdb,datafusion,clickhouse,sqlite
#                    (default: ibex,python,r,duckdb,datafusion,clickhouse)
#                    "python" = pandas + polars + polars-st;  "r" = data.table + dplyr.
#   --sizes  LIST    dataset sizes (default: 1M,2M,4M,8M,16M,32M,50M)
#   --warmup N       warmup iterations (default: 1)
#   --iters  N       timed iterations  (default: 5)
#   --tf-rows N      override TF row count
#   --type   TYPE    instance type for every engine box (default: r7i.2xlarge)
#   --key    KEY     EC2 key pair for SSH debugging (optional)
#   --region REGION  override region
#   --on-demand      on-demand instead of spot (default: spot)
#
# Environment: S3_BUCKET / AWS_REGION / IBEX_AMI loaded from .config if unset.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="r7i.2xlarge"
SIZES="1M,2M,4M,8M,16M,32M,50M"
WARMUP=1
ITERS=5
TF_ROWS=""
KEY_NAME=""
ON_DEMAND=0
ENGINES="ibex,python,r,duckdb,datafusion,clickhouse"

# Per-engine run_scale_suite.sh args: each enables exactly one engine and skips
# the rest. sqlite and ibex-compiled stay off unless their engine is selected.
# duckdb runs every size (--duckdb-all-sizes) since it has its own box; the -st
# variants are included because there's no shared box to keep them off for.
declare -A ENGINE_ARGS=(
    [ibex]="--skip-ibex-compiled --skip-python --skip-r --skip-duckdb --skip-duckdb-st --skip-datafusion --skip-datafusion-st --skip-clickhouse --skip-clickhouse-st"
    [python]="--skip-ibex --skip-ibex-compiled --skip-r --skip-duckdb --skip-duckdb-st --skip-datafusion --skip-datafusion-st --skip-clickhouse --skip-clickhouse-st"
    [r]="--skip-ibex --skip-ibex-compiled --skip-python --skip-duckdb --skip-duckdb-st --skip-datafusion --skip-datafusion-st --skip-clickhouse --skip-clickhouse-st"
    [duckdb]="--skip-ibex --skip-ibex-compiled --skip-python --skip-r --skip-datafusion --skip-datafusion-st --skip-clickhouse --skip-clickhouse-st --duckdb-all-sizes"
    [datafusion]="--skip-ibex --skip-ibex-compiled --skip-python --skip-r --skip-duckdb --skip-duckdb-st --skip-clickhouse --skip-clickhouse-st"
    [clickhouse]="--skip-ibex --skip-ibex-compiled --skip-python --skip-r --skip-duckdb --skip-duckdb-st --skip-datafusion --skip-datafusion-st"
    [sqlite]="--skip-ibex --skip-ibex-compiled --skip-python --skip-r --skip-duckdb --skip-duckdb-st --skip-datafusion --skip-datafusion-st --skip-clickhouse --skip-clickhouse-st --with-sqlite"
)

# Only the native Ibex worker needs the CMake tree. Building Ibex on the R,
# Python, and SQL-engine workers used to compile ~400 targets (including Arrow,
# Parquet, tools, and tests) before running an engine that never uses them.
declare -A ENGINE_BUILD_REQUIRED=(
    [ibex]=1
    [python]=0
    [r]=0
    [duckdb]=0
    [datafusion]=0
    [clickhouse]=0
    [sqlite]=0
)

# The Ibex scale worker executes tools/ibex_bench only. Limit Ninja to that
# target instead of building tests, examples, bridges, and unrelated tools.
declare -A ENGINE_BUILD_TARGETS=(
    [ibex]="ibex_bench"
    [python]=""
    [r]=""
    [duckdb]=""
    [datafusion]=""
    [clickhouse]=""
    [sqlite]=""
)

while [[ $# -gt 0 ]]; do
    case "$1" in
        --engines)   ENGINES="$2"; shift 2 ;;
        --sizes)     SIZES="$2"; shift 2 ;;
        --warmup)    WARMUP="$2"; shift 2 ;;
        --iters)     ITERS="$2"; shift 2 ;;
        --tf-rows)   TF_ROWS="$2"; shift 2 ;;
        --type)      INSTANCE_TYPE="$2"; shift 2 ;;
        --key)       KEY_NAME="$2"; shift 2 ;;
        --region)    REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        -h|--help)   sed -n '2,40p' "$0"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

IFS=',' read -r -a ENGINE_LIST <<< "$ENGINES"
for e in "${ENGINE_LIST[@]}"; do
    if [[ -z "${ENGINE_ARGS[$e]+x}" ]]; then
        echo "error: unknown engine '$e' (valid: ${!ENGINE_ARGS[*]})" >&2
        exit 1
    fi
done

# ── Git info + push preflight ─────────────────────────────────────────────────
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RUN_PREFIX="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/per-engine"
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Commit  : ${COMMIT:0:8} ($BRANCH)"
echo "Engines : ${ENGINE_LIST[*]}"
echo "Sizes   : $SIZES"
echo "AMI     : $AMI"
echo "Type    : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot)), one per engine"
echo "Bucket  : s3://$S3_BUCKET/$RUN_PREFIX"
echo ""

KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

# ── Launch one instance per engine ────────────────────────────────────────────
declare -A INSTANCE_OF RESULT_KEY_OF DONE_OF
for engine in "${ENGINE_LIST[@]}"; do
    result_key="${RUN_PREFIX}/${engine}/scales.csv"
    RESULT_KEY_OF[$engine]="$result_key"
    DONE_OF[$engine]=0

    user_data=$(bench_user_data "$REPO_URL" "$COMMIT" \
        "IBEX_S3_BUCKET=${S3_BUCKET}" \
        "IBEX_RESULT_KEY=${result_key}" \
        "IBEX_REGION=${REGION}" \
        "IBEX_SIZES=${SIZES}" \
        "IBEX_WARMUP=${WARMUP}" \
        "IBEX_ITERS=${ITERS}" \
        "IBEX_TF_ROWS=${TF_ROWS}" \
        "IBEX_BUILD_REQUIRED=${ENGINE_BUILD_REQUIRED[$engine]}" \
        "IBEX_BUILD_TARGETS=${ENGINE_BUILD_TARGETS[$engine]}" \
        "IBEX_SUITE_ARGS=${ENGINE_ARGS[$engine]}")

    instance_id=$(aws ec2 run-instances \
        --region "$REGION" \
        --instance-type "$INSTANCE_TYPE" \
        --image-id "$AMI" \
        "${MARKET_ARGS[@]}" \
        --instance-initiated-shutdown-behavior terminate \
        --iam-instance-profile "Name=ibex-bench" \
        --security-group-ids "$SG_ID" \
        --user-data "$user_data" \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
        --tag-specifications \
            "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench-${engine}},{Key=Commit,Value=${COMMIT:0:8}},{Key=Engine,Value=${engine}}]" \
        "${KEY_ARGS[@]}" \
        --query "Instances[0].InstanceId" \
        --output text)

    INSTANCE_OF[$engine]="$instance_id"
    echo "  ${engine}: $instance_id → s3://${S3_BUCKET}/${result_key}"
done

echo ""
echo "All instances launched. Polling for results..."
echo "Per-engine console logs:"
for engine in "${ENGINE_LIST[@]}"; do
    echo "  ${engine}: aws ec2 get-console-output --instance-id ${INSTANCE_OF[$engine]} --region $REGION --latest --output text"
done
echo ""

# ── Poll until every engine's result lands (or its instance dies) ─────────────
# Use a run-specific directory. Reusing benchmarking/results/scales directly
# allowed a dead/reclaimed instance to be reported as successful when a stale
# per_engine_<name>.csv from an earlier run happened to exist there.
RESULT_DIR="$IBEX_ROOT/benchmarking/results/scales/${TIMESTAMP}_${COMMIT:0:8}"
mkdir -p "$RESULT_DIR"
TIMEOUT=21600  # 6h — a full 1M-50M sweep for the slowest engine can run long
START=$(date +%s)
REMAINING=${#ENGINE_LIST[@]}

while (( REMAINING > 0 )); do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after $((TIMEOUT/3600))h with $REMAINING engine(s) unfinished."
        break
    fi

    for engine in "${ENGINE_LIST[@]}"; do
        [[ "${DONE_OF[$engine]}" -eq 1 ]] && continue
        if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY_OF[$engine]}" --region "$REGION" &>/dev/null; then
            aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY_OF[$engine]}" \
                "$RESULT_DIR/per_engine_${engine}.csv" --region "$REGION" --only-show-errors
            DONE_OF[$engine]=1
            REMAINING=$(( REMAINING - 1 ))
            echo ""
            echo "✓ ${engine} done ($((  (NOW - START) / 60 ))m) — $REMAINING remaining"
            continue
        fi
        # Dead-instance detection: a terminated box that never produced a result
        # (spot reclaim / failed bootstrap / OOM) won't ever finish.
        STATE=$(aws ec2 describe-instances --instance-ids "${INSTANCE_OF[$engine]}" --region "$REGION" \
            --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
        if [[ "$STATE" == "terminated" || "$STATE" == "shutting-down" ]]; then
            if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY_OF[$engine]}" --region "$REGION" &>/dev/null; then
                continue  # uploaded just before terminating; pick it up next loop
            fi
            DONE_OF[$engine]=1
            REMAINING=$(( REMAINING - 1 ))
            echo ""
            echo "⚠ ${engine} instance ${INSTANCE_OF[$engine]} is '$STATE' with no result — $REMAINING remaining"
            echo "  console: aws ec2 get-console-output --instance-id ${INSTANCE_OF[$engine]} --region $REGION --latest --output text"
        fi
    done

    (( REMAINING > 0 )) && { printf "."; sleep 30; }
done

# ── Combine per-engine slices into one CSV ────────────────────────────────────
COMBINED="$IBEX_ROOT/benchmarking/results/scales_aws_${TIMESTAMP}.csv"
HEADER="dataset_rows,framework,query,avg_ms,min_ms,max_ms,stddev_ms,p95_ms,p99_ms,rows,peak_rss_mb"
echo "$HEADER" > "$COMBINED"
got=0
for engine in "${ENGINE_LIST[@]}"; do
    f="$RESULT_DIR/per_engine_${engine}.csv"
    if [[ -f "$f" ]]; then
        tail -n +2 "$f" >> "$COMBINED"
        got=$(( got + 1 ))
    else
        echo "  (no results for ${engine})"
    fi
done

echo ""
ELAPSED=$(( ($(date +%s) - START) / 60 ))
echo "Done in ${ELAPSED}m. Combined ${got}/${#ENGINE_LIST[@]} engine(s):"
echo "  $COMBINED"
