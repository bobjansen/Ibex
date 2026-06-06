#!/usr/bin/env bash
# build-ami.sh — bake a reusable benchmark AMI, repeatably.
#
# Launches one builder instance from the stock Ubuntu 24.04 image, runs
# bootstrap.sh in provision-only mode (installs the full toolchain + R + uv
# packages, warms the uv cache, and — unless --no-prebuild — builds ibex once so
# the Arrow/FetchContent tree is baked in), then snapshots the stopped instance
# into an AMI and records its id in benchmarking/aws/.config (IBEX_AMI=).
#
# After this, run.sh and run-per-engine.sh boot from the baked AMI and skip the
# multi-minute provisioning step on every instance — the big win when fanning a
# run out across one box per engine.
#
# Prereq: ./benchmarking/aws/setup.sh (S3 bucket + IAM role + security group).
#
# Usage:
#   ./benchmarking/aws/build-ami.sh [options]
#
# Options:
#   --commit REF     git ref to bake as the prebuild baseline (default: HEAD).
#                    Must be pushed to origin. Pick a recent main commit so the
#                    baked Arrow tree matches what runs will build against.
#   --no-prebuild    thin AMI: deps + warmed caches only, no baked build-release.
#   --type TYPE      builder instance type (default: c7i.2xlarge — fast build).
#   --name NAME      AMI name (default: ibex-bench-<commit>-<timestamp>).
#   --key KEY_PAIR   EC2 key pair for SSH debugging (optional).
#   --region REGION  override region.
#   --keep-instance  don't terminate the builder after imaging (for debugging).
#
# Environment: S3_BUCKET / AWS_REGION loaded from .config if unset.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
COMMIT_REF="HEAD"
PREBUILD=1
INSTANCE_TYPE="c7i.2xlarge"
AMI_NAME=""
KEY_NAME=""
KEEP_INSTANCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --commit)       COMMIT_REF="$2"; shift 2 ;;
        --no-prebuild)  PREBUILD=0; shift ;;
        --type)         INSTANCE_TYPE="$2"; shift 2 ;;
        --name)         AMI_NAME="$2"; shift 2 ;;
        --key)          KEY_NAME="$2"; shift 2 ;;
        --region)       REGION="$2"; shift 2 ;;
        --keep-instance) KEEP_INSTANCE=1; shift ;;
        -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run aws/setup.sh first}"

REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse "$COMMIT_REF")
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
[[ -z "$AMI_NAME" ]] && AMI_NAME="ibex-bench-${COMMIT:0:8}-${TIMESTAMP}"
# Under benchmarks/ so the instance's IAM policy (PutObject on benchmarks/*,
# see setup.sh) permits the status upload; the 30-day lifecycle rule cleans it.
STATUS_KEY="benchmarks/ami-build/${TIMESTAMP}_${COMMIT:0:8}/status"

# The builder clones origin and checks out $COMMIT — must be pushed.
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

# Always build the AMI FROM the stock Ubuntu image, never from a prior baked AMI.
AMI_BASE=$(bench_base_ubuntu_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

echo "Baking AMI from stock Ubuntu"
echo "  Base image : $AMI_BASE"
echo "  Commit     : ${COMMIT:0:8} ($BRANCH)"
echo "  Prebuild   : $([[ $PREBUILD -eq 1 ]] && echo "yes (warm Arrow tree)" || echo "no (thin)")"
echo "  Builder    : $INSTANCE_TYPE"
echo "  AMI name   : $AMI_NAME"
echo "  Region     : $REGION"
echo ""

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_PROVISION_ONLY=1" \
    "IBEX_PREBUILD=${PREBUILD}" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" \
    "IBEX_AMI_STATUS_KEY=${STATUS_KEY}" \
    "IBEX_REGION=${REGION}")

KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")

# `stop` (not terminate) on the provision-only shutdown so we can image it.
INSTANCE_ID=$(aws ec2 run-instances \
    --region "$REGION" \
    --instance-type "$INSTANCE_TYPE" \
    --image-id "$AMI_BASE" \
    --instance-initiated-shutdown-behavior stop \
    --iam-instance-profile "Name=ibex-bench" \
    --security-group-ids "$SG_ID" \
    --user-data "$USER_DATA" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications \
        "ResourceType=instance,Tags=[{Key=Name,Value=ibex-bench-amibuilder},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" \
    --query "Instances[0].InstanceId" \
    --output text)

echo "Builder instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo ""

# Best-effort cleanup of the builder unless --keep-instance.
cleanup_instance() {
    [[ "$KEEP_INSTANCE" -eq 1 ]] && return 0
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'TerminatingInstances[0].CurrentState.Name' --output text 2>/dev/null \
        | sed 's/^/Builder instance terminating: /' || true
}

# ── Wait for provisioning to report status ────────────────────────────────────
echo "Waiting for provisioning to finish (toolchain + R + uv$([[ $PREBUILD -eq 1 ]] && echo " + ibex build"))..."
echo "(~10-20 min with prebuild; ~5-10 min thin)"
TIMEOUT=3600
START=$(date +%s)
STATUS=""
while true; do
    NOW=$(date +%s)
    if (( NOW - START > TIMEOUT )); then
        echo ""
        echo "Timed out after $((TIMEOUT/60))m waiting for provisioning. Check the console:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
        cleanup_instance
        exit 1
    fi

    STATUS=$(aws s3 cp "s3://${S3_BUCKET}/${STATUS_KEY}" - --region "$REGION" 2>/dev/null || true)
    if [[ "$STATUS" == "ok" ]]; then
        echo ""
        echo "✓ Provisioning succeeded."
        break
    elif [[ "$STATUS" == "fail" ]]; then
        echo ""
        echo "Provisioning reported FAILURE. Check the console:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
        cleanup_instance
        exit 1
    fi

    # Detect a dead builder (terminated by a failure trap) so we don't wait out
    # the full timeout.
    STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$STATE" == "terminated" || "$STATE" == "shutting-down" ]]; then
        echo ""
        echo "Builder $INSTANCE_ID is '$STATE' without reporting success. Check the console:"
        echo "  aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
        exit 1
    fi

    printf "."
    sleep 30
done

# ── Wait for the instance to fully stop, then image it ────────────────────────
echo "Waiting for instance to stop (clean snapshot)..."
aws ec2 wait instance-stopped --instance-ids "$INSTANCE_ID" --region "$REGION"

echo "Creating AMI '$AMI_NAME'..."
AMI_ID=$(aws ec2 create-image \
    --region "$REGION" \
    --instance-id "$INSTANCE_ID" \
    --name "$AMI_NAME" \
    --description "ibex benchmark AMI — commit ${COMMIT:0:8}, prebuild=${PREBUILD}" \
    --tag-specifications \
        "ResourceType=image,Tags=[{Key=Name,Value=${AMI_NAME}},{Key=Commit,Value=${COMMIT:0:8}}]" \
        "ResourceType=snapshot,Tags=[{Key=Name,Value=${AMI_NAME}}]" \
    --query "ImageId" --output text)

echo "AMI: $AMI_ID — waiting for it to become available..."
aws ec2 wait image-available --image-ids "$AMI_ID" --region "$REGION"
echo "✓ AMI $AMI_ID available."

cleanup_instance

# ── Record the AMI so runners pick it up automatically ────────────────────────
bench_save_config "$SCRIPT_DIR" "IBEX_AMI" "$AMI_ID"
echo ""
echo "✓ Saved IBEX_AMI=$AMI_ID to $SCRIPT_DIR/.config"
echo ""
echo "Benchmark runs now boot from this AMI automatically:"
echo "  ./benchmarking/aws/run.sh                 # whole suite on one box"
echo "  ./benchmarking/aws/run-per-engine.sh      # one box per engine"
echo ""
echo "Rebuild the AMI any time (e.g. after a toolchain bump) by re-running this script."
