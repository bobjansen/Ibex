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

# Print disk usage at the top so out-of-space failures are obvious from the
# console without having to SSH in.
df -h / || true

# ── System deps ───────────────────────────────────────────────────────────────
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y \
    git ninja-build \
    libcurl4-openssl-dev libssl-dev zlib1g-dev \
    r-base r-cran-data.table r-cran-optparse \
    python3 python3-dev curl unzip \
    wget gnupg lsb-release software-properties-common ca-certificates

# ── Modern CMake ──────────────────────────────────────────────────────────────
# Noble's apt only ships CMake 3.28, on which Arrow 22's ThirdpartyToolchain
# mis-resolves thirdparty/versions.txt → empty Boost SHA256 → configure fails.
# Install the official Kitware binary, pinned to match the known-good local
# version. Bump CMAKE_VERSION to re-baseline (must match install-deps.sh).
CMAKE_VERSION=3.31.6
curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" -o /tmp/cmake.tar.gz
tar -xzf /tmp/cmake.tar.gz -C /opt
export PATH="/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin:$PATH"
cmake --version | head -1    # record exact CMake in the bench log

# ── Modern Clang ──────────────────────────────────────────────────────────────
# Ubuntu Noble's apt only ships clang-18, which reports __cpp_concepts=201907L
# and forces the libstdc++ <expected> workaround in CompilerOptions.cmake. For
# credible performance numbers we build with a current stable Clang from
# apt.llvm.org instead of relying on that macro override. Bump CLANG_VERSION to
# re-baseline the bench compiler.
CLANG_VERSION=21
curl -fsSL https://apt.llvm.org/llvm.sh -o /tmp/llvm.sh
chmod +x /tmp/llvm.sh
/tmp/llvm.sh "${CLANG_VERSION}"
"clang++-${CLANG_VERSION}" --version    # record exact compiler in the bench log

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
    -DCMAKE_C_COMPILER="clang-${CLANG_VERSION}" \
    -DCMAKE_CXX_COMPILER="clang++-${CLANG_VERSION}" \
    -DIBEX_PARQUET_S3=OFF \
    -DCMAKE_BUILD_TYPE=Release \
    -S /ibex
ninja -C /ibex/build-release

# ── Run benchmark suite ───────────────────────────────────────────────────────
# IBEX_BOTH_THREADING=1: keep the single-threaded (-st) variants for engines
# that have them (polars-st, duckdb-st, datafusion-st, clickhouse-st), and
# include pandas + dplyr. This produces a CSV with cells for every engine in
# both parallelism modes (where the engine supports both) — the reviewer's
# "apples-to-apples plus best-effort" pair in one launch.
cd /ibex/benchmarking
EXTRA_SKIPS=(--skip-ibex-compiled)
if [[ "${IBEX_BOTH_THREADING:-0}" != "1" ]]; then
    EXTRA_SKIPS+=(--skip-pandas --skip-dplyr
                  --skip-duckdb-st --skip-datafusion-st --skip-clickhouse-st)
fi

TF_ROWS_ARG=()
if [[ -n "${IBEX_TF_ROWS:-}" ]]; then
    TF_ROWS_ARG=(--tf-rows "${IBEX_TF_ROWS}")
fi

IBEX_ROOT=/ibex BUILD_DIR=/ibex/build-release \
    bash run_scale_suite.sh \
        --sizes "${IBEX_SIZES}" \
        --warmup "${IBEX_WARMUP}" \
        --iters "${IBEX_ITERS}" \
        "${TF_ROWS_ARG[@]}" \
        "${EXTRA_SKIPS[@]}"

# ── Upload results ────────────────────────────────────────────────────────────
aws s3 cp results/scales.csv \
    "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" \
    --region "${IBEX_REGION}"

echo "Results uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}"

# Self-terminate (--instance-initiated-shutdown-behavior terminate was set at launch)
shutdown -h now
