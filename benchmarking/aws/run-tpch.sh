#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run-tpch.sh — run the TPC-H/PDS-H quartet on one clean EC2 benchmark box.
#
# The artifact contains per-query TSVs for Ibex, this tree's Polars and
# polars-st, and upstream Polars PDS-H's Polars, polars-st and DuckDB SQL.
#
# Every engine is pinned to the same core set. --cores defaults to the PHYSICAL
# core count of --type (resolved from the instance type), because an unpinned
# cross-engine run is not a measurement -- Polars sizes its pool from nproc and
# thrashes above ~8 threads, which inflates Ibex's lead. --cores 0 opts out.
#
# Options:
#   --sf LIST            comma-separated scale factors (default: 1)
#   --type TYPE          instance type (default: r7i.2xlarge)
#   --cores N            pin every engine to N cores (default: the box's
#                        physical core count; 0 = unpinned, not a valid
#                        cross-engine comparison)
#   --threads-per-core N 1 disables SMT, for a clean physical-core number
#   --volume-size GB     root volume (default 250; the launch refuses to start
#                        if the SF list cannot fit -- ~1.6 GB per SF-unit,
#                        cumulative, since .tbl and Parquet both persist)
#   --no-polars-in-memory  drop the whole-table Polars passes (they OOM at high
#                        SF) and run the streaming reference instead
#   --warmup N / --iters N / --key KEY / --region R / --on-demand
#
# Usage:
#   ./benchmarking/aws/run-tpch.sh --on-demand --sf 1 --warmup 1 --iters 5
#   ./benchmarking/aws/run-tpch.sh --on-demand --type r7i.8xlarge \
#       --sf 1,8,30,100 --threads-per-core 1 --volume-size 400

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
bench_load_config "$SCRIPT_DIR"

REGION="${AWS_REGION:-us-east-1}"
INSTANCE_TYPE="r7i.2xlarge"
SCALES="1"
WARMUP=1
ITERS=5
KEY_NAME=""
ON_DEMAND=0
# Empty means "resolve the box's PHYSICAL core count and pin to that". An
# unpinned cross-engine run is not a measurement: run_bench.sh notes that Polars
# sizes its pool from nproc and thrashes above ~8 threads, which inflates Ibex's
# apparent lead. Pass --cores 0 to deliberately run unpinned.
CORES=""
THREADS_PER_CORE=""
VOLUME_SIZE=250
POLARS_IN_MEMORY=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf) SCALES="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --type) INSTANCE_TYPE="$2"; shift 2 ;;
        --cores) CORES="$2"; shift 2 ;;
        --threads-per-core) THREADS_PER_CORE="$2"; shift 2 ;;
        --volume-size) VOLUME_SIZE="$2"; shift 2 ;;
        --no-polars-in-memory) POLARS_IN_MEMORY=0; shift ;;
        --key) KEY_NAME="$2"; shift 2 ;;
        --region) REGION="$2"; shift 2 ;;
        --on-demand) ON_DEMAND=1; shift ;;
        -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

S3_BUCKET="${S3_BUCKET:?S3_BUCKET not set — run benchmarking/aws/setup.sh first}"
REPO_URL=$(bench_repo_url "$IBEX_ROOT")
COMMIT=$(git -C "$IBEX_ROOT" rev-parse HEAD)
BRANCH=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD)
bench_require_pushed "$IBEX_ROOT" "$COMMIT" "$BRANCH" "$REPO_URL" || exit 1

TIMESTAMP=$(date -u +%Y%m%dT%H%M%S)
RESULT_KEY="benchmarks/${TIMESTAMP}_${COMMIT:0:8}/tpch.tar.gz"
AMI=$(bench_resolve_ami "$REGION")
SG_ID=$(bench_security_group "$REGION")

# Resolve the pin from the instance itself so --type alone is enough. Syncing
# --type and --cores by hand is the failure that has no error message: the run
# simply produces an unpinned, ibex-flattering number.
if [[ -z "$CORES" ]]; then
    read -r _vcpus _cores _tpc _mem <<< "$(bench_instance_topology "$REGION" "$INSTANCE_TYPE")"
    if [[ "$_cores" == "?" ]]; then
        echo "error: cannot resolve the core count for ${INSTANCE_TYPE}; pass --cores N explicitly" >&2
        echo "       (--cores 0 runs unpinned, which is not a valid cross-engine comparison)" >&2
        exit 1
    fi
    CORES="$_cores"
fi

CPU_ARGS=()
bench_cpu_options_args CPU_ARGS "$REGION" "$INSTANCE_TYPE" "$THREADS_PER_CORE"

# The .tbl files and the Parquet copy BOTH persist, for every scale in the list:
# ~1.0 GB/SF raw + ~0.61 GB/SF Parquet, cumulative. Refuse to launch a run whose
# data cannot fit rather than discover it when the largest scale dies on ENOSPC.
sf_total=0
for _sf in ${SCALES//,/ }; do sf_total=$((sf_total + _sf)); done
need_gb=$(( (sf_total * 16 + 9) / 10 + 30 ))   # 1.6 GB/SF-unit + 30 GB for the OS/build
if (( need_gb > VOLUME_SIZE )); then
    echo "error: SF list '${SCALES}' needs about ${need_gb} GB but --volume-size is ${VOLUME_SIZE} GB" >&2
    echo "       pass --volume-size ${need_gb} (or larger)" >&2
    exit 1
fi
KEY_ARGS=()
[[ -n "$KEY_NAME" ]] && KEY_ARGS=(--key-name "$KEY_NAME")
MARKET_ARGS=()
if [[ "$ON_DEMAND" -eq 0 ]]; then
    MARKET_ARGS=(--instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time","InstanceInterruptionBehavior":"terminate"}}')
fi

echo "Commit : ${COMMIT:0:8} ($BRANCH)"
echo "TPC-H  : SF-${SCALES}; ${WARMUP} warmup + ${ITERS} timed iterations"
echo "Type   : $INSTANCE_TYPE ($([[ "$ON_DEMAND" -eq 1 ]] && echo on-demand || echo spot))"
echo "Box    : $(bench_topology_line "$REGION" "$INSTANCE_TYPE")"
if [[ "$CORES" == "0" ]]; then
    echo "Cores  : UNPINNED - engines size their own pools; not a valid cross-engine comparison"
else
    echo "Cores  : pinned to ${CORES}${THREADS_PER_CORE:+ (ThreadsPerCore=${THREADS_PER_CORE})}"
fi
echo "Disk   : ${VOLUME_SIZE} GB (need ~${need_gb} GB for SF ${SCALES})"
echo "Result : s3://$S3_BUCKET/$RESULT_KEY"

USER_DATA=$(bench_user_data "$REPO_URL" "$COMMIT" \
    "IBEX_TPCH_MODE=1" "IBEX_TPCH_SCALES=${SCALES}" "IBEX_WARMUP=${WARMUP}" "IBEX_ITERS=${ITERS}" \
    "IBEX_TPCH_CORES=${CORES}" "IBEX_TPCH_POLARS_IN_MEMORY=${POLARS_IN_MEMORY}" \
    "IBEX_S3_BUCKET=${S3_BUCKET}" "IBEX_RESULT_KEY=${RESULT_KEY}" "IBEX_REGION=${REGION}")
INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" --instance-type "$INSTANCE_TYPE" --image-id "$AMI" \
    "${MARKET_ARGS[@]}" "${CPU_ARGS[@]}" --instance-initiated-shutdown-behavior terminate \
    --iam-instance-profile "Name=ibex-bench" --security-group-ids "$SG_ID" --user-data "$USER_DATA" \
    --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${VOLUME_SIZE},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ibex-tpch},{Key=Commit,Value=${COMMIT:0:8}}]" \
    "${KEY_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)

echo "Instance: $INSTANCE_ID"
echo "Log: aws ec2 get-console-output --instance-id $INSTANCE_ID --region $REGION --latest --output text"
echo "Waiting for artifact..."
while ! aws s3 ls "s3://${S3_BUCKET}/${RESULT_KEY}" --region "$REGION" >/dev/null 2>&1; do
    state=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$REGION" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)
    if [[ "$state" == "terminated" || "$state" == "shutting-down" ]]; then
        echo "instance ended without an artifact; inspect the console log above" >&2
        exit 1
    fi
    sleep 30
done

OUTPUT="$IBEX_ROOT/benchmarking/results/tpch_aws_${TIMESTAMP}.tar.gz"
aws s3 cp "s3://${S3_BUCKET}/${RESULT_KEY}" "$OUTPUT" --region "$REGION"
echo "Done: $OUTPUT"
