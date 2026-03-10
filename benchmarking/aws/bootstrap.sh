#!/usr/bin/env bash
# bootstrap.sh — runs on the EC2 instance after the repo is cloned.
# Called from user-data; all output goes to /var/log/ibex-bench.log.
#
# Expected env vars (set by run.sh via user-data):
#   IBEX_S3_BUCKET    — results destination bucket
#   IBEX_RESULT_KEY   — S3 key for results/scales.csv
#   IBEX_REGION       — AWS region
#   IBEX_SIZES        — comma-separated sizes e.g. "1M,2M,4M,8M,16M"
#   IBEX_WARMUP       — warmup iterations
#   IBEX_ITERS        — timed iterations

set -euo pipefail

# ── System deps ───────────────────────────────────────────────────────────────
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y \
    clang-18 cmake ninja-build \
    r-base r-cran-data.table r-cran-optparse \
    python3 curl unzip

# AWS CLI v2 (for s3 cp to upload results)
curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp/awscli-install
/tmp/awscli-install/aws/install
rm -rf /tmp/awscliv2.zip /tmp/awscli-install

# uv (Python package manager used by the benchmark suite)
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="/root/.local/bin:$PATH"

# ── Build ibex (release) ──────────────────────────────────────────────────────
cmake -B /ibex/build-release -G Ninja \
    -DCMAKE_CXX_COMPILER=clang-18 \
    -DCMAKE_BUILD_TYPE=Release \
    -S /ibex
ninja -C /ibex/build-release

# ── Run benchmark suite ───────────────────────────────────────────────────────
cd /ibex/benchmarking
IBEX_ROOT=/ibex BUILD_DIR=/ibex/build-release \
    bash run_scale_suite.sh \
        --sizes "${IBEX_SIZES}" \
        --warmup "${IBEX_WARMUP}" \
        --iters "${IBEX_ITERS}" \
        --skip-ibex-compiled \
        --skip-pandas \
        --skip-dplyr \
        --skip-duckdb-st \
        --skip-datafusion-st \
        --skip-clickhouse-st

# ── Upload results ────────────────────────────────────────────────────────────
aws s3 cp results/scales.csv \
    "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" \
    --region "${IBEX_REGION}"

echo "Results uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}"

# Self-terminate (--instance-initiated-shutdown-behavior terminate was set at launch)
shutdown -h now
