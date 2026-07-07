#!/usr/bin/env bash
# bootstrap.sh — runs on the EC2 instance after the repo is cloned/checked out.
# Called from user-data (see lib.sh:bench_user_data); all output goes to
# /var/log/ibex-bench.log and the serial console.
#
# Two modes:
#   • normal (default): provision (if needed) → build ibex → run the suite →
#     upload results → self-terminate.
#   • provision-only (IBEX_PROVISION_ONLY=1): provision + warm caches +
#     (optionally) pre-build, write a status file to S3, then stop the instance
#     so build-ami.sh can snapshot it into a reusable AMI. No benchmark runs.
#
# On a baked AMI the provisioning marker is present, so the expensive apt /
# toolchain / R / uv install is skipped and the run goes straight to an
# incremental build + suite.
#
# Expected env vars (set by the runner via user-data):
#   IBEX_S3_BUCKET    — results destination bucket
#   IBEX_RESULT_KEY   — S3 key for results/scales.csv      (normal mode)
#   IBEX_REGION       — AWS region
#   IBEX_SIZES        — comma-separated sizes e.g. "1M,2M,4M,8M,16M"  (normal)
#   IBEX_WARMUP       — warmup iterations                   (normal mode)
#   IBEX_ITERS        — timed iterations                    (normal mode)
#   IBEX_TF_ROWS      — optional --tf-rows override         (normal mode)
#   IBEX_BOTH_THREADING — include single-thread variants    (normal mode)
#   IBEX_SUITE_ARGS   — verbatim extra args for run_scale_suite.sh (overrides
#                       the default arg set; used by run-per-engine.sh)
#   IBEX_PROVISION_ONLY — "1" to provision + image-prep and stop (build-ami.sh)
#   IBEX_PREBUILD     — "1" (default) to build ibex during provisioning so the
#                       baked AMI carries a warm Arrow/FetchContent tree
#   IBEX_AMI_STATUS_KEY — S3 key to write "ok"/"fail" to (provision-only mode)

set -euo pipefail

# Print disk usage at the top so out-of-space failures are obvious from the
# console without having to SSH in.
df -h / || true

# Toolchain versions. Kept here (not inside provision) because the build step
# below references them on a baked AMI too. Bump to re-baseline the bench
# compiler/cmake — must match install-deps.sh.
#   - Noble's apt clang-18 reports __cpp_concepts=201907L (forces the libstdc++
#     <expected> workaround); we want a current stable Clang for credible numbers.
#   - Noble's apt CMake 3.28 mis-resolves Arrow 22's thirdparty versions.txt.
CLANG_VERSION=21
CMAKE_VERSION=3.31.6

MARKER=/opt/ibex/.ami-provisioned

# ── Provisioning ──────────────────────────────────────────────────────────────
# Everything an instance needs before it can build + benchmark. Idempotent and
# baked into the AMI by build-ami.sh; guarded by $MARKER so a baked AMI skips it.
provision() {
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y \
        git ninja-build \
        libjemalloc-dev \
        libcurl4-openssl-dev libssl-dev zlib1g-dev \
        r-base r-cran-data.table r-cran-optparse \
        python3 python3-dev curl unzip \
        time \
        wget gnupg lsb-release software-properties-common ca-certificates

    # jemalloc is ibex's intended allocator: with malloc_conf="dirty_decay_ms:-1"
    # (set in tools/ibex_bench.cpp) it retains freed column buffers so warm
    # iterations reuse already-faulted pages. Without it ibex_bench links glibc
    # malloc, whose 32MB mmap threshold munmaps every column >4M float64 rows on
    # free and re-faults it next iteration — a ~5x cliff at 4M->8M that is a
    # measurement artifact of the box's allocator, not ibex.

    # Modern CMake from Kitware (Noble's 3.28 breaks Arrow 22's from-source build).
    curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" -o /tmp/cmake.tar.gz
    tar -xzf /tmp/cmake.tar.gz -C /opt
    rm -f /tmp/cmake.tar.gz
    ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/cmake" /usr/local/bin/cmake
    ln -sf "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/ctest" /usr/local/bin/ctest

    # Current stable Clang from apt.llvm.org. llvm.sh installs only versioned
    # binaries (clang-21 / clang++-21); the build below passes those explicitly.
    curl -fsSL https://apt.llvm.org/llvm.sh -o /tmp/llvm.sh
    chmod +x /tmp/llvm.sh
    /tmp/llvm.sh "${CLANG_VERSION}"

    # dplyr + tidyr aren't in apt; install from CRAN so the R bench's dplyr cells
    # (and reshape) run. Matches install-deps.sh.
    Rscript -e '
        needed <- c("dplyr", "tidyr")
        missing <- needed[!sapply(needed, requireNamespace, quietly = TRUE)]
        if (length(missing) > 0) {
            install.packages(missing, repos = "https://cloud.r-project.org")
        }
    '

    # AWS CLI v2 (for s3 cp to upload results / status). --update is a no-op on a
    # fresh box and lets provisioning re-run cleanly on a baked image.
    curl -fsSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscliv2.zip
    unzip -q /tmp/awscliv2.zip -d /tmp/awscli-install
    /tmp/awscli-install/aws/install --update
    rm -rf /tmp/awscliv2.zip /tmp/awscli-install

    # uv (Python package manager used by the benchmark suite). Symlink onto the
    # default PATH so baked-AMI runs find it without sourcing a profile.
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ln -sf /root/.local/bin/uv /usr/local/bin/uv
    ln -sf /root/.local/bin/uvx /usr/local/bin/uvx 2>/dev/null || true

    mkdir -p /opt/ibex
    date -u +"provisioned %Y-%m-%dT%H:%M:%SZ clang-${CLANG_VERSION} cmake-${CMAKE_VERSION}" > "$MARKER"
}

if [[ -f "$MARKER" ]]; then
    echo "✓ AMI pre-provisioned ($(cat "$MARKER")) — skipping system dep install"
elif [[ "${IBEX_PROVISION_ONLY:-0}" == "1" ]]; then
    : # provision-only handles its own provisioning below
else
    provision
fi

# clang/cmake/uv live on the default PATH after provision (versioned clang +
# /usr/local/bin symlinks), but be defensive for the baked case.
export PATH="/usr/local/bin:/root/.local/bin:$PATH"

# ── Build ibex (release) ──────────────────────────────────────────────────────
# Reuses an existing build-release if the AMI baked one (incremental: ninja
# rebuilds only the ibex objects that changed between the baked commit and this
# run's commit; Arrow and the other FetchContent deps are version-pinned and
# stay built). IBEX_PARQUET_S3=OFF drops the bundled AWS SDK (~64% of targets).
build_ibex() {
    cmake -B /ibex/build-release -G Ninja \
        -DCMAKE_C_COMPILER="clang-${CLANG_VERSION}" \
        -DCMAKE_CXX_COMPILER="clang++-${CLANG_VERSION}" \
        -DIBEX_PARQUET_S3=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -S /ibex
    ninja -C /ibex/build-release
}

# ── Provision-only mode (build-ami.sh) ─────────────────────────────────────────
# Provision, warm the caches a real run will reuse, write a status file to S3,
# then stop the instance (shutdown behaviour is `stop`, set by build-ami.sh) so
# it can be snapshotted into an AMI. The marker is removed first so provisioning
# always runs fresh during an image build.
if [[ "${IBEX_PROVISION_ONLY:-0}" == "1" ]]; then
    STATUS=fail
    report_status() {
        if [[ -n "${IBEX_AMI_STATUS_KEY:-}" && -n "${IBEX_S3_BUCKET:-}" ]]; then
            echo "$STATUS" | aws s3 cp - \
                "s3://${IBEX_S3_BUCKET}/${IBEX_AMI_STATUS_KEY}" \
                --region "${IBEX_REGION:-us-east-1}" || true
        fi
        echo "provision-only finished: ${STATUS}"
        shutdown -h now
    }
    trap report_status EXIT

    rm -f "$MARKER"
    provision
    export PATH="/usr/local/bin:/root/.local/bin:$PATH"

    # Warm the uv environment (numpy/pandas/polars/duckdb/datafusion/chdb) so the
    # heavy first-run downloads are baked into the image.
    ( cd /ibex/benchmarking && uv sync --project /ibex 2>/dev/null \
        || uv run --project /ibex python3 -c "import sys; print('uv warm', sys.version)" ) || true

    # Pre-build ibex so the AMI carries a warm Arrow/FetchContent tree (the
    # biggest single build cost). Default on; set IBEX_PREBUILD=0 for a thin AMI.
    if [[ "${IBEX_PREBUILD:-1}" == "1" ]]; then
        build_ibex
    fi

    STATUS=ok   # trap uploads this and stops the instance
    exit 0
fi

# ── Compare mode (compare-git.sh) ─────────────────────────────────────────────
# A/B two git commits of the SAME ibex code on this clean box, to get low-noise
# perf verdicts that a laptop/WSL2 can't. Delegates to compare_ibex_git.sh,
# which builds both commits in throwaway trees and times ibex_bench on each;
# here we just regenerate the (untracked) benchmark CSVs, run it, upload the
# report and self-terminate. compare_ibex_git.sh does its own build, so this
# path deliberately skips build_ibex — but it reuses the baked lightgbm source
# so the two throwaway builds don't each re-clone it.
#
#   IBEX_BASE / IBEX_TARGET — commits to compare (both must be on origin)
#   IBEX_REPEATS, IBEX_ITERS, IBEX_WARMUP — passed through
#   IBEX_SUITE       — optional --ibex-suite selection
#   IBEX_INTERLEAVE  — "1" (default) alternates base/target repeats
#   IBEX_TASKSET     — optional core pinning (e.g. "2-3")
#   IBEX_DATA_ROWS   — fact-table row count for gen_data.py (default 4000000)
#   IBEX_RESULT_KEY  — S3 key the report text is uploaded to
if [[ "${IBEX_COMPARE_MODE:-0}" == "1" ]]; then
    REPORT=/ibex/benchmarking/results/compare_report.txt
    mkdir -p /ibex/benchmarking/results

    finish_compare() {
        local code=$?
        if [[ -f "$REPORT" ]] && aws s3 cp "$REPORT" \
            "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" --region "${IBEX_REGION}"; then
            echo "Report uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY} (exit ${code})"
        else
            echo "WARNING: no report uploaded (exit ${code})"
        fi
        shutdown -h now
    }
    trap finish_compare EXIT

    # The fixed benchmark CSVs are not tracked in git — regenerate them at the
    # default 4M so both commits read identical inputs.
    uv run --project /ibex /ibex/benchmarking/data/gen_data.py \
        /ibex/benchmarking/data --rows "${IBEX_DATA_ROWS:-4000000}"

    # compare_ibex_git.sh defaults to the bare clang/clang++ names, which don't
    # exist on this box (llvm.sh installs only versioned binaries) — point it at
    # the same clang-${CLANG_VERSION} the normal build_ibex uses.
    COMPARE_ENV=(
        IBEX_CC="clang-${CLANG_VERSION}"
        IBEX_CXX="clang++-${CLANG_VERSION}"
    )
    [[ -d /ibex/build-release/_deps/lightgbm-src ]] \
        && COMPARE_ENV+=(IBEX_LIGHTGBM_SRC=/ibex/build-release/_deps/lightgbm-src)

    COMPARE_ARGS=(--base "${IBEX_BASE}" --target "${IBEX_TARGET}"
        --repeats "${IBEX_REPEATS:-5}" --iters "${IBEX_ITERS:-7}" --warmup "${IBEX_WARMUP:-1}")
    [[ -n "${IBEX_SUITE:-}" ]] && COMPARE_ARGS+=(--ibex-suite "${IBEX_SUITE}")
    [[ "${IBEX_INTERLEAVE:-1}" == "1" ]] && COMPARE_ARGS+=(--interleave)
    [[ -n "${IBEX_TASKSET:-}" ]] && COMPARE_ARGS+=(--taskset "${IBEX_TASKSET}")

    # tee: live progress on the serial console AND a captured report to upload.
    env "${COMPARE_ENV[@]}" \
        bash /ibex/benchmarking/compare_ibex_git.sh "${COMPARE_ARGS[@]}" 2>&1 \
        | tee "$REPORT"

    exit 0  # finish_compare (EXIT trap) uploads the report and terminates
fi

# ── Normal run: build + suite ──────────────────────────────────────────────────
build_ibex

# Each launch produces a complete, self-contained CSV. The default arg set runs
# the full multi-threaded engine set + pandas + dplyr and duckdb at every size;
# single-thread variants only when IBEX_BOTH_THREADING=1. IBEX_SUITE_ARGS, when
# set, REPLACES this default set verbatim — run-per-engine.sh uses it to enable
# exactly one engine per instance.
cd /ibex/benchmarking
if [[ -n "${IBEX_SUITE_ARGS:-}" ]]; then
    read -r -a SUITE_ARGS <<< "${IBEX_SUITE_ARGS}"
else
    SUITE_ARGS=(--skip-ibex-compiled --duckdb-all-sizes)
    if [[ "${IBEX_BOTH_THREADING:-0}" != "1" ]]; then
        SUITE_ARGS+=(--skip-duckdb-st --skip-datafusion-st --skip-clickhouse-st)
    fi
fi

TF_ROWS_ARG=()
if [[ -n "${IBEX_TF_ROWS:-}" ]]; then
    TF_ROWS_ARG=(--tf-rows "${IBEX_TF_ROWS}")
fi

# ── Upload results + self-terminate on ANY exit ───────────────────────────────
# scales.csv is written incrementally (header at start, then per-size/per-engine
# appends), so uploading on EXIT preserves completed sizes even when a later size
# fails (e.g. an OOM at 16M). The trap also guarantees the instance shuts down
# rather than idling after a failed suite.
CSV=/ibex/benchmarking/results/scales.csv
PARTIAL_KEY="${IBEX_RESULT_KEY%scales.csv}scales.partial.csv"

finish() {
    local code=$?
    [[ -n "${UPLOADER_PID:-}" ]] && kill "${UPLOADER_PID}" 2>/dev/null || true
    if [[ -f "$CSV" ]]; then
        if aws s3 cp "$CSV" \
            "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" \
            --region "${IBEX_REGION}"; then
            echo "Results uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY} (suite exit ${code})"
        else
            echo "WARNING: result upload failed (suite exit ${code})"
        fi
    else
        echo "No results to upload (suite exit ${code})"
    fi
    shutdown -h now
}
trap finish EXIT

# Periodic partial-result uploader: sync the in-progress scales.csv every 60s so
# completed sizes are visible long before the whole sweep finishes.
( while sleep 60; do
      [[ -f "$CSV" ]] && aws s3 cp "$CSV" \
          "s3://${IBEX_S3_BUCKET}/${PARTIAL_KEY}" \
          --region "${IBEX_REGION}" --only-show-errors || true
  done ) &
UPLOADER_PID=$!

IBEX_ROOT=/ibex BUILD_DIR=/ibex/build-release \
    bash run_scale_suite.sh \
        --sizes "${IBEX_SIZES}" \
        --warmup "${IBEX_WARMUP}" \
        --iters "${IBEX_ITERS}" \
        "${TF_ROWS_ARG[@]}" \
        "${SUITE_ARGS[@]}"

# Result upload and self-termination are handled by the EXIT trap above.
