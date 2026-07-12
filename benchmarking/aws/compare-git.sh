#!/usr/bin/env bash
# compare-git.sh — A/B two git commits of the SAME ibex code on a clean EC2 box.
#
# The local compare_ibex_git.sh is the tool for this, but a laptop / WSL2 box is
# too noisy for sub-5% verdicts (thermal drift, background load). This runs the
# identical comparison on a dedicated, idle, fixed-clock EC2 instance — both
# commits are built and timed on the same box, repeats interleaved — and pulls
# the report back.
#
# Boots from the baked AMI (build-ami.sh) when configured in .config, otherwise
# from stock Ubuntu (provisioning then runs on the instance).
#
# Usage:
#   ./benchmarking/aws/compare-git.sh [options]
#
# Options:
#   --base   REF          base commit   (default: HEAD~1; must be pushed)
#   --target REF          target commit (default: HEAD;   must be pushed)
#   --suite  a,b,c        ibex suite selection (default: all suites)
#   --repeats N           repeats per side, median-reduced (default: 3)
#   --iters   N           timed iterations per repeat       (default: 5)
#   --warmup  N           warmup iterations                 (default: 1)
#   --data-rows N         fact-table rows for gen_data.py   (default: 4000000)
#   --serial              disable interleaving (all base repeats, then target)
#   --replica-control     build BASE twice and balance all three run positions
#   --artifacts           download exact benchmark binaries + build metadata
#   --taskset CPUSET      pin the benchmark cores             (default: 2)
#   --type   INSTANCE     EC2 instance type   (default: c7i.2xlarge)
#   --key    KEY_PAIR     EC2 key pair for SSH debugging (optional)
#   --region REGION       override region
#   --on-demand           force on-demand   (default for scale runs, >4M rows)
#   --spot                force spot         (default for 4M runs)
#
# Environment: S3_BUCKET / AWS_REGION / IBEX_AMI are loaded from .config.
#
# Cost: a single suite comparison on c7i.2xlarge (~$0.36/hr on-demand) builds
# two ibex trees (three with replica control) + a handful of repeats — typically
# 15-30 min without the replica, well under $0.20.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

bench_load_config "$SCRIPT_DIR"

# ── Defaults ──────────────────────────────────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
# c7i.2xlarge: 8 vCPU Sapphire Rapids (fixed clock → stable timings, same core
# as run.sh's r7i so numbers stay comparable) + 16GB (ample for the 4M-row
# benchmark and two -j8 ibex builds with Arrow/Parquet off). Empty means
# "auto-size from --data-rows" (see below); an explicit --type always wins.
INSTANCE_TYPE=""
BASE_REF="HEAD~1"
TARGET_REF="HEAD"
SUITE=""
# Leaner than compare_ibex_git.sh's local defaults on purpose: a dedicated EC2
# box barely drifts (IQRs are typically <1%), so 3 interleaved repeats × 5 iters
# = 15 samples/side already pins the verdict. The heavier local defaults exist
# to fight WSL2 noise, which isn't present here — and each extra pass is costly
# at scale (the suite runs (1+iters)×repeats×2 times, or ×3 with a replica).
REPEATS=3
ITERS=5
WARMUP=1
DATA_ROWS=4000000
INTERLEAVE=1
REPLICA_CONTROL=0
ARTIFACTS=0
TASKSET_CPUS="2"
KEY_NAME=""
ON_DEMAND=-1   # -1 = auto (spot for small, on-demand for scale); 0/1 = forced

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base)      BASE_REF="$2";      shift 2 ;;
        --target)    TARGET_REF="$2";    shift 2 ;;
        --suite)     SUITE="$2";         shift 2 ;;
        --repeats)   REPEATS="$2";       shift 2 ;;
        --iters)     ITERS="$2";         shift 2 ;;
        --warmup)    WARMUP="$2";        shift 2 ;;
        --data-rows) DATA_ROWS="$2";     shift 2 ;;
        --serial)    INTERLEAVE=0;       shift ;;
        --replica-control) REPLICA_CONTROL=1; shift ;;
        --artifacts) ARTIFACTS=1; shift ;;
        --taskset)   TASKSET_CPUS="$2";  shift 2 ;;
        --type)      INSTANCE_TYPE="$2"; shift 2 ;;
        --key)       KEY_NAME="$2";      shift 2 ;;
        --region)    REGION="$2";        shift 2 ;;
        --on-demand) ON_DEMAND=1;        shift ;;
        --spot)      ON_DEMAND=0;        shift ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ "$REPLICA_CONTROL" -eq 1 ]]; then
    INTERLEAVE=1
fi

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

[[ "$DATA_ROWS" =~ ^[0-9]+$ ]] || { echo "ERROR: --data-rows must be an integer (e.g. 16000000)" >&2; exit 1; }

# Auto-size the box from the fact-table row count unless --type overrides. The
# in-memory reshape/group benchmarks are the memory ceiling: ~28GB at 16M rows,
# scaling ~linearly (see run.sh), so a 16GB box OOMs above ~4M. Pick RAM with
# headroom; all are the same Sapphire Rapids core so per-core timings stay
# comparable across sizes.
if [[ -z "$INSTANCE_TYPE" ]]; then
    if   (( DATA_ROWS <=  4000000 )); then INSTANCE_TYPE="c7i.2xlarge"   # 16 GB
    elif (( DATA_ROWS <= 16000000 )); then INSTANCE_TYPE="r7i.2xlarge"   # 64 GB
    elif (( DATA_ROWS <= 32000000 )); then INSTANCE_TYPE="r7i.4xlarge"   # 128 GB
    else                                   INSTANCE_TYPE="r7i.8xlarge"   # 256 GB
    fi
fi

# Market: spot is cheapest and fine for the short 4M run (a reclaim is cheap to
# retry), but a scale run is long and reclaim-costly — losing an hour-long 32M
# build+bench near the end hurts. So default scale runs to on-demand; --on-demand
# / --spot force either way.
if (( ON_DEMAND < 0 )); then
    if (( DATA_ROWS > 4000000 )); then
        ON_DEMAND=1
        echo "note: scale run (${DATA_ROWS} rows) → on-demand to avoid spot reclaim; pass --spot to override." >&2
    else
        ON_DEMAND=0
    fi
fi

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

# The instance clones origin and builds both commits there, so both must be on
# the upstream — a local-only commit would make compare_ibex_git.sh fail to
# create its worktree.
bench_require_pushed "$IBEX_ROOT" "$BASE_COMMIT" "$BRANCH" "$REPO_URL" || exit 1
bench_require_pushed "$IBEX_ROOT" "$TARGET_COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/compare_${TIMESTAMP}_${BASE_COMMIT:0:8}_${TARGET_COMMIT:0:8}/report.txt"
ARTIFACT_KEY=""
if [[ "$ARTIFACTS" -eq 1 ]]; then
    ARTIFACT_KEY="${RESULT_KEY%/report.txt}/artifacts.tar.gz"
fi

# ── AMI + security group ──────────────────────────────────────────────────────
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Base   : ${BASE_COMMIT:0:8} ($BASE_REF)"
echo "Target : ${TARGET_COMMIT:0:8} ($TARGET_REF)"
echo "Suite  : ${SUITE:-all}"
echo "Repeats: $REPEATS (interleave $([[ "$INTERLEAVE" -eq 1 ]] && echo on || echo off), replica-control $([[ "$REPLICA_CONTROL" -eq 1 ]] && echo on || echo off)), iters $ITERS, warmup $WARMUP"
echo "AMI    : $AMI"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Bucket : s3://$S3_BUCKET"
[[ -n "$ARTIFACT_KEY" ]] && echo "Archive: s3://$S3_BUCKET/$ARTIFACT_KEY"
echo ""

# ── User-data: clone/update repo (checked out at target) → bootstrap.sh ───────
USER_DATA=$(bench_user_data "$REPO_URL" "$TARGET_COMMIT" \
    "IBEX_COMPARE_MODE=1" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_RESULT_KEY=${RESULT_KEY}" \
    "IBEX_ARTIFACT_KEY=${ARTIFACT_KEY}" \
    "IBEX_REGION=${REGION}" \
    "IBEX_BASE=${BASE_COMMIT}" \
    "IBEX_TARGET=${TARGET_COMMIT}" \
    "IBEX_SUITE=${SUITE}" \
    "IBEX_REPEATS=${REPEATS}" \
    "IBEX_ITERS=${ITERS}" \
    "IBEX_WARMUP=${WARMUP}" \
    "IBEX_DATA_ROWS=${DATA_ROWS}" \
    "IBEX_INTERLEAVE=${INTERLEAVE}" \
    "IBEX_REPLICA_CONTROL=${REPLICA_CONTROL}" \
    "IBEX_TASKSET=${TASKSET_CPUS}")

# ── Launch instance ───────────────────────────────────────────────────────────
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
        "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench-compare},{Key=Base,Value=${BASE_COMMIT:0:8}},{Key=Target,Value=${TARGET_COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Instance: $INSTANCE_ID"
echo "Log     : aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo ""
echo "Waiting for s3://${S3_BUCKET}/${RESULT_KEY} ..."
echo "(build of both commits + repeats; typically 15-30 min)"
echo ""

# ── Poll S3 ───────────────────────────────────────────────────────────────────
TIMEOUT=10800  # 3h — headroom for large --data-rows (full suite x repeats x2 at
               # 32M is much slower than 4M). The instance self-terminates on its
               # own; this only bounds how long we poll for the download.
START=$(date +%s)
DOTS=0

while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after 90m. Check instance logs:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
        exit 1
    fi

    if aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" &>/dev/null; then
        echo ""
        break
    fi

    # Fail fast if the instance died (spot reclaim, failed build, OOM) instead of
    # waiting out the whole timeout. Checked every ~2.5 min.
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

# ── Download + print the report ───────────────────────────────────────────────
OUTPUT="$IBEX_ROOT/benchmarking/results/compare_aws_${TIMESTAMP}.txt"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"

if [[ -n "$ARTIFACT_KEY" ]]; then
    ARTIFACT_OUTPUT="${OUTPUT%.txt}_artifacts.tar.gz"
    aws s3 cp "s3://${S3_BUCKET}/${ARTIFACT_KEY}" "$ARTIFACT_OUTPUT" --region "$REGION"
    echo "Artifacts: ${ARTIFACT_OUTPUT#$IBEX_ROOT/}"
fi

ELAPSED=$(( ($(date +%s) - START) / 60 ))
echo "Done in ${ELAPSED}m. Report: benchmarking/results/compare_aws_${TIMESTAMP}.txt"
echo ""
cat "$OUTPUT"
