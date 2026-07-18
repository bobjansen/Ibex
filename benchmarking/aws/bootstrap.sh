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

    # Latest GCC available to this Ubuntu image. The PPA is intentionally used
    # only to widen apt's candidate set; the exact version is resolved at image
    # build/run time so the compiler benchmark tracks the current toolchain.
    add-apt-repository -y ppa:ubuntu-toolchain-r/test || true
    apt-get update -qq
    local gcc_version
    gcc_version="$(apt-cache search --names-only '^g\+\+-[0-9]+$' \
        | awk '{ sub(/^g\+\+-/, "", $1); print $1 }' \
        | sort -n | tail -1)"
    if [[ -n "$gcc_version" ]]; then
        apt-get install -y "gcc-${gcc_version}" "g++-${gcc_version}"
    else
        apt-get install -y gcc g++
    fi

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
    date -u +"provisioned %Y-%m-%dT%H:%M:%SZ clang-${CLANG_VERSION} gcc-${gcc_version:-default} cmake-${CMAKE_VERSION}" > "$MARKER"
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

find_latest_gxx() {
    local best="" best_ver="" path base ver
    for path in /usr/bin/g++-[0-9]*; do
        [[ -x "$path" ]] || continue
        base="$(basename "$path")"
        ver="${base#g++-}"
        [[ "$ver" =~ ^[0-9]+$ ]] || continue
        if [[ -z "$best_ver" || "$ver" -gt "$best_ver" ]]; then
            best_ver="$ver"
            best="$path"
        fi
    done
    if [[ -n "$best" ]]; then
        echo "$best"
    elif command -v g++ >/dev/null 2>&1; then
        command -v g++
    else
        return 1
    fi
}

find_latest_gcc() {
    local gxx gcc_candidate
    gxx="$(find_latest_gxx)"
    gcc_candidate="${gxx/g++/gcc}"
    if [[ -x "$gcc_candidate" ]]; then
        echo "$gcc_candidate"
    elif command -v gcc >/dev/null 2>&1; then
        command -v gcc
    else
        return 1
    fi
}

find_latest_gxx_version() {
    local gxx
    gxx="$(find_latest_gxx)"
    "$gxx" -dumpfullversion 2>/dev/null || "$gxx" -dumpversion
}

install_latest_gcc() {
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y --no-install-recommends \
        ca-certificates gnupg software-properties-common
    add-apt-repository -y ppa:ubuntu-toolchain-r/test || true
    apt-get update -qq

    local gcc_version
    gcc_version="$(apt-cache search --names-only '^g\+\+-[0-9]+$' \
        | awk '{ sub(/^g\+\+-/, "", $1); print $1 }' \
        | sort -n | tail -1)"
    if [[ -n "$gcc_version" ]]; then
        apt-get install -y --no-install-recommends "gcc-${gcc_version}" "g++-${gcc_version}"
    else
        apt-get install -y --no-install-recommends gcc g++
    fi
}

ensure_compiler_compare_toolchain() {
    if ! command -v "clang++-${CLANG_VERSION}" >/dev/null 2>&1; then
        echo "FATAL: clang++-${CLANG_VERSION} is missing; rebuild the AMI or rerun provisioning." >&2
        exit 1
    fi

    # Older baked AMIs predate the GCC install added for compare-compilers.sh.
    # Repair just this missing toolchain slice instead of forcing an AMI rebuild.
    if ! find_latest_gxx >/dev/null 2>&1; then
        echo "No g++ found on this AMI; installing latest available GCC toolchain."
        install_latest_gcc
    fi

    local gxx
    if ! gxx="$(find_latest_gxx)"; then
        echo "FATAL: no g++ found after attempting GCC installation." >&2
        exit 1
    fi
    echo "Compiler comparison toolchain:"
    "clang++-${CLANG_VERSION}" --version | head -1
    "$gxx" --version | head -1
}

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
        -DIBEX_ENABLE_MARCH_NATIVE=ON \
        -DIBEX_ENABLE_LTO=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -S /ibex
    ninja -C /ibex/build-release
}

build_ibex_with_compiler() {
    local build_dir="$1" cc="$2" cxx="$3"
    cmake -B "$build_dir" -G Ninja \
        -DCMAKE_C_COMPILER="$cc" \
        -DCMAKE_CXX_COMPILER="$cxx" \
        -DIBEX_PARQUET_S3=OFF \
        -DIBEX_BUILD_TESTS=OFF \
        -DIBEX_BUILD_EXAMPLES=OFF \
        -DIBEX_BUILD_PYTHON_BRIDGE=OFF \
        -DIBEX_BUILD_PARQUET=OFF \
        -DIBEX_BUILD_ADBC=OFF \
        -DIBEX_BUILD_KAFKA=OFF \
        -DIBEX_ENABLE_MARCH_NATIVE=ON \
        -DIBEX_ENABLE_LTO=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -S /ibex
    ninja -C "$build_dir" ibex_compile_bin ibex_runtime
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
#   IBEX_REPLICA_CONTROL — "1" adds the balanced same-source replica
#   IBEX_TASKSET     — optional core pinning (e.g. "2-3")
#   IBEX_DATA_ROWS   — fact-table row count for gen_data.py (default 4000000)
#   IBEX_RESULT_KEY  — S3 key the report text is uploaded to
#   IBEX_ARTIFACT_KEY — optional S3 key for exact binaries + build metadata
if [[ "${IBEX_COMPARE_MODE:-0}" == "1" ]]; then
    REPORT=/ibex/benchmarking/results/compare_report.txt
    ARTIFACT=/ibex/benchmarking/results/compare_artifacts.tar.gz
    mkdir -p /ibex/benchmarking/results

    finish_compare() {
        local code=$?
        if [[ -n "${IBEX_ARTIFACT_KEY:-}" ]]; then
            if [[ -f "$ARTIFACT" ]] && aws s3 cp "$ARTIFACT" \
                "s3://${IBEX_S3_BUCKET}/${IBEX_ARTIFACT_KEY}" --region "${IBEX_REGION}"; then
                echo "Artifacts uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_ARTIFACT_KEY}"
            else
                echo "WARNING: no artifact archive uploaded (exit ${code})"
            fi
        fi
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
    export IBEX_CC="clang-${CLANG_VERSION}"
    export IBEX_CXX="clang++-${CLANG_VERSION}"
    if [[ -d /ibex/build-release/_deps/lightgbm-src ]]; then
        export IBEX_LIGHTGBM_SRC=/ibex/build-release/_deps/lightgbm-src
    fi

    COMPARE_ARGS=(--base "${IBEX_BASE}" --target "${IBEX_TARGET}"
        --repeats "${IBEX_REPEATS:-5}" --iters "${IBEX_ITERS:-7}" --warmup "${IBEX_WARMUP:-1}")
    [[ -n "${IBEX_SUITE:-}" ]] && COMPARE_ARGS+=(--ibex-suite "${IBEX_SUITE}")
    [[ "${IBEX_INTERLEAVE:-1}" == "1" ]] && COMPARE_ARGS+=(--interleave)
    [[ "${IBEX_REPLICA_CONTROL:-0}" == "1" ]] && COMPARE_ARGS+=(--replica-control)
    [[ -n "${IBEX_TASKSET:-}" ]] && COMPARE_ARGS+=(--taskset "${IBEX_TASKSET}")
    if [[ -n "${IBEX_ARTIFACT_KEY:-}" ]]; then
        mkdir -p /ibex/perfcmp
        export IBEX_PERFCMP_TMPDIR=/ibex/perfcmp
        COMPARE_ARGS+=(--keep-temp)
    fi

    # tee: live progress on the serial console AND a captured report to upload.
    env "${COMPARE_ENV[@]}" \
        bash /ibex/benchmarking/compare_ibex_git.sh "${COMPARE_ARGS[@]}" 2>&1 \
        | tee "$REPORT"

    if [[ -n "${IBEX_ARTIFACT_KEY:-}" ]]; then
        shopt -s nullglob
        artifact_roots=(/ibex/perfcmp/ibex-perfcmp.*)
        if [[ "${#artifact_roots[@]}" -ne 1 ]]; then
            echo "ERROR: expected one retained comparison directory, found ${#artifact_roots[@]}" >&2
            exit 1
        fi
        artifact_root="${artifact_roots[0]}"
        artifact_files=(
            build-base/tools/ibex_bench
            build-target/tools/ibex_bench
        )
        if [[ -f "$artifact_root/build-ctrl/tools/ibex_bench" ]]; then
            artifact_files+=(build-ctrl/tools/ibex_bench)
        fi

        {
            echo "base=${IBEX_BASE}"
            echo "target=${IBEX_TARGET}"
            echo "created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            uname -a
            lscpu
            "${IBEX_CXX}" --version
            cmake --version
            sha256sum "${artifact_files[@]/#/$artifact_root/}"
            file "${artifact_files[@]/#/$artifact_root/}"
            for binary in "${artifact_files[@]/#/$artifact_root/}"; do
                echo
                echo "ldd $binary"
                ldd "$binary"
            done
        } > "$artifact_root/artifact-manifest.txt" 2>&1

        tar -C "$artifact_root" -czf "$ARTIFACT" \
            artifact-manifest.txt "${artifact_files[@]}"
    fi

    exit 0  # finish_compare (EXIT trap) uploads the report and terminates
fi

# ── Bisect mode (bisect-git.sh) ────────────────────────────────────────────────
# Run a performance git bisect on one clean box. Each candidate is compared
# against the fixed known-good commit via compare_ibex_git.sh; the candidate is
# marked bad when the selected query regresses beyond IBEX_BISECT_THRESHOLD_PCT.
if [[ "${IBEX_BISECT_MODE:-0}" == "1" ]]; then
    REPORT=/ibex/benchmarking/results/bisect_report.txt
    mkdir -p /ibex/benchmarking/results

    finish_bisect() {
        local code=$?
        if [[ ! -f "$REPORT" ]]; then
            {
                echo "bisect-git failed before producing a report"
                echo "exit_status=${code}"
                echo
                echo "Last 200 lines of /var/log/ibex-bench.log:"
                tail -n 200 /var/log/ibex-bench.log 2>/dev/null || true
            } > "$REPORT"
        fi
        if aws s3 cp "$REPORT" \
            "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" --region "${IBEX_REGION}"; then
            echo "Bisect report uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY} (exit ${code})"
        else
            echo "WARNING: bisect report upload failed (exit ${code})"
        fi
        shutdown -h now
    }
    trap finish_bisect EXIT

    uv run --project /ibex /ibex/benchmarking/data/gen_data.py \
        /ibex/benchmarking/data --rows "${IBEX_DATA_ROWS:-4000000}"

    export IBEX_CC="clang-${CLANG_VERSION}"
    export IBEX_CXX="clang++-${CLANG_VERSION}"
    if [[ -d /ibex/build-release/_deps/lightgbm-src ]]; then
        export IBEX_LIGHTGBM_SRC=/ibex/build-release/_deps/lightgbm-src
    fi

    CHECK=/tmp/ibex-bisect-check.sh
    cat > "$CHECK" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

candidate="$(git -C /ibex rev-parse HEAD)"
short="${candidate:0:8}"
step_dir="/ibex/benchmarking/results/bisect/${short}"
mkdir -p "$step_dir"
report="$step_dir/report.txt"

echo
echo "=== bisect candidate ${candidate} ==="

args=(--base "${IBEX_GOOD}" --target "${candidate}"
    --repeats "${IBEX_REPEATS:-7}"
    --iters "${IBEX_ITERS:-9}"
    --warmup "${IBEX_WARMUP:-1}"
    --csv /ibex/benchmarking/data/prices.csv
    --keep-temp)
[[ -n "${IBEX_SUITE:-}" ]] && args+=(--ibex-suite "${IBEX_SUITE}")
[[ "${IBEX_INTERLEAVE:-1}" == "1" ]] && args+=(--interleave)
[[ -n "${IBEX_TASKSET:-}" ]] && args+=(--taskset "${IBEX_TASKSET}")

bash /ibex/benchmarking/compare_ibex_git.sh "${args[@]}" > "$report" 2>&1
cat "$report"

query="${IBEX_BISECT_QUERY:-fill_forward}"
threshold="${IBEX_BISECT_THRESHOLD_PCT:-10}"
row="$(awk -v q="$query" '$2 == q { line=$0 } END { print line }' "$report")"
if [[ -z "$row" ]]; then
    echo "bisect: query '${query}' not found; skipping ${candidate}" >&2
    exit 125
fi

delta_pct="$(awk -v q="$query" '$2 == q { gsub(/[+%]/, "", $6); print $6 }' "$report" | tail -1)"
if awk -v d="$delta_pct" -v t="$threshold" 'BEGIN { exit (d >= t) ? 0 : 1 }'; then
    echo "bisect decision: BAD (${query} delta_pct=${delta_pct}% >= ${threshold}%)"
    exit 1
fi

echo "bisect decision: GOOD (${query} delta_pct=${delta_pct}% < ${threshold}%)"
exit 0
EOF
    chmod +x "$CHECK"

    {
        echo "Ibex AWS performance bisect"
        echo "good=${IBEX_GOOD}"
        echo "bad=${IBEX_BAD}"
        echo "suite=${IBEX_SUITE:-all}"
        echo "query=${IBEX_BISECT_QUERY:-fill_forward}"
        echo "threshold_pct=${IBEX_BISECT_THRESHOLD_PCT:-10}"
        echo "repeats=${IBEX_REPEATS:-7}"
        echo "iters=${IBEX_ITERS:-9}"
        echo "warmup=${IBEX_WARMUP:-1}"
        echo "interleave=${IBEX_INTERLEAVE:-1}"
        echo "pinning=${IBEX_TASKSET:-<none>}"
        echo

        git -C /ibex bisect reset || true
        git -C /ibex bisect start "${IBEX_BAD}" "${IBEX_GOOD}"
        set +e
        git -C /ibex bisect run "$CHECK"
        bisect_code=$?
        set -e
        echo
        echo "git bisect run exit_status=${bisect_code}"
        echo
        git -C /ibex bisect log || true
        git -C /ibex bisect reset || true
        exit "$bisect_code"
    } 2>&1 | tee "$REPORT"

    exit 0
fi

# ── Compare-plugin mode (compare-plugin-git.sh) ────────────────────────────────
# A/B two git commits for a plugin-backed extern function (read_parquet,
# read_adbc, ...) on this clean box. Delegates to compare_plugin_git.sh, which
# builds both commits (with the given plugin target, e.g. Arrow/Parquet ON —
# unlike IBEX_COMPARE_MODE above, which builds with it OFF) and drives the
# real REPL + --plugin-path, since ibex_bench never loads a plugin. Skips
# build_ibex (compare_plugin_git.sh does its own builds).
#
#   IBEX_BASE / IBEX_TARGET     — commits to compare (both must be on origin)
#   IBEX_QUERY                  — repo-relative query template (__INPUT__ token)
#   IBEX_PLUGIN_TARGETS         — comma-separated cmake target(s) for the plugin .so
#   IBEX_CONFIGURE_ARGS         — space-separated extra `cmake -D...` args
#   IBEX_PARQUET_ROWS           — rows for gen_parquet_data.py (default 8000000)
#   IBEX_REPEATS, IBEX_ITERS, IBEX_WARMUP, IBEX_INNER_RUNS — passed through
#   IBEX_INTERLEAVE              — "1" (default) alternates base/target repeats
#   IBEX_TASKSET                 — optional core pinning (e.g. "2-3")
#   IBEX_RESULT_KEY               — S3 key the report text is uploaded to
if [[ "${IBEX_COMPARE_PLUGIN_MODE:-0}" == "1" ]]; then
    REPORT=/ibex/benchmarking/results/compare_plugin_report.txt
    mkdir -p /ibex/benchmarking/results

    finish_compare_plugin() {
        local code=$?
        if [[ -f "$REPORT" ]] && aws s3 cp "$REPORT" \
            "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" --region "${IBEX_REGION}"; then
            echo "Report uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY} (exit ${code})"
        else
            echo "WARNING: no report uploaded (exit ${code})"
        fi
        shutdown -h now
    }
    trap finish_compare_plugin EXIT

    # The Parquet fixture is not tracked in git (benchmarking/data/** is
    # gitignored) — regenerate it once so both commits read the identical file.
    uv run --project /ibex /ibex/benchmarking/gen_parquet_data.py \
        /ibex/benchmarking/data --rows "${IBEX_PARQUET_ROWS:-8000000}"

    # compare_plugin_git.sh defaults to the bare clang/clang++ names, which
    # don't exist on this box (llvm.sh installs only versioned binaries).
    COMPARE_ENV=(
        IBEX_CC="clang-${CLANG_VERSION}"
        IBEX_CXX="clang++-${CLANG_VERSION}"
    )

    COMPARE_ARGS=(
        --base "${IBEX_BASE}" --target "${IBEX_TARGET}"
        --query "/ibex/${IBEX_QUERY}"
        --input "/ibex/benchmarking/data/prices.parquet"
        --plugin-target "${IBEX_PLUGIN_TARGETS}"
        --repeats "${IBEX_REPEATS:-3}" --iters "${IBEX_ITERS:-5}"
        --warmup "${IBEX_WARMUP:-1}" --inner-runs "${IBEX_INNER_RUNS:-1}"
    )
    for arg in ${IBEX_CONFIGURE_ARGS:-}; do
        COMPARE_ARGS+=(--configure-arg "$arg")
    done
    [[ "${IBEX_INTERLEAVE:-1}" == "0" ]] && COMPARE_ARGS+=(--serial)
    [[ -n "${IBEX_TASKSET:-}" ]] && COMPARE_ARGS+=(--taskset "${IBEX_TASKSET}")

    # tee: live progress on the serial console AND a captured report to upload.
    env "${COMPARE_ENV[@]}" \
        bash /ibex/benchmarking/compare_plugin_git.sh "${COMPARE_ARGS[@]}" 2>&1 \
        | tee "$REPORT"

    exit 0  # finish_compare_plugin (EXIT trap) uploads the report and terminates
fi

# ── Compare-compiler mode (compare-compilers.sh) ──────────────────────────────
# Compare full Ibex builds compiled with latest Clang vs latest GCC on one
# clean box. Each side gets its own runtime/core/ir archives and generated
# query executable built with the matching compiler.
#
#   IBEX_REPEATS, IBEX_ITERS, IBEX_WARMUP — passed through
#   IBEX_INTERLEAVE  — "1" (default) alternates clang/gcc repeats
#   IBEX_TASKSET     — optional core pinning (e.g. "2-3")
#   IBEX_DATA_ROWS   — fact-table row count for gen_data.py (default 4000000)
#   IBEX_RESULT_KEY  — S3 key the report text is uploaded to
if [[ "${IBEX_COMPARE_COMPILERS_MODE:-0}" == "1" ]]; then
    REPORT=/ibex/benchmarking/results/compare_compilers_report.txt
    mkdir -p /ibex/benchmarking/results

    finish_compare_compilers() {
        local code=$?
        if [[ ! -f "$REPORT" ]]; then
            {
                echo "compare-compilers failed before producing a benchmark report"
                echo "exit_status=${code}"
                echo
                echo "Last 200 lines of /var/log/ibex-bench.log:"
                tail -n 200 /var/log/ibex-bench.log 2>/dev/null || true
            } > "$REPORT"
        fi
        if [[ -f "$REPORT" ]] && aws s3 cp "$REPORT" \
            "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" --region "${IBEX_REGION}"; then
            echo "Report uploaded to s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY} (exit ${code})"
        else
            echo "WARNING: no report uploaded (exit ${code})"
        fi
        shutdown -h now
    }
    trap finish_compare_compilers EXIT

    ensure_compiler_compare_toolchain
    GCC_CC="$(find_latest_gcc)"
    GCC_CXX="$(find_latest_gxx)"
    build_ibex_with_compiler /ibex/build-release-clang \
        "clang-${CLANG_VERSION}" "clang++-${CLANG_VERSION}"
    build_ibex_with_compiler /ibex/build-release-gcc "$GCC_CC" "$GCC_CXX"

    uv run --project /ibex /ibex/benchmarking/data/gen_data.py \
        /ibex/benchmarking/data --rows "${IBEX_DATA_ROWS:-4000000}"

    COMPARE_ARGS=(
        --clang-build-dir /ibex/build-release-clang
        --gcc-build-dir /ibex/build-release-gcc
        --clang-cxx "clang++-${CLANG_VERSION}"
        --gcc-cxx "$GCC_CXX"
        --repeats "${IBEX_REPEATS:-3}"
        --iters "${IBEX_ITERS:-7}"
        --warmup "${IBEX_WARMUP:-1}"
        --out "$REPORT"
    )
    [[ "${IBEX_INTERLEAVE:-1}" == "1" ]] && COMPARE_ARGS+=(--interleave)
    [[ -n "${IBEX_TASKSET:-}" ]] && COMPARE_ARGS+=(--taskset "${IBEX_TASKSET}")

    bash /ibex/benchmarking/compare_ibex_compilers.sh "${COMPARE_ARGS[@]}"

    exit 0  # finish_compare_compilers (EXIT trap) uploads the report and terminates
fi

# ── Normal run: build + suite ──────────────────────────────────────────────────
if [[ "${IBEX_TPCH_MODE:-0}" == "1" ]]; then
    # PDS-H is intentionally kept separate from the scale suite: it uses TPC-H
    # data and reports a per-query TSV rather than synthetic scale-query rows.
    # The upstream checkout is pinned so an artifact can always be reproduced.
    PDSH_REPO=https://github.com/pola-rs/polars-benchmark.git
    PDSH_COMMIT=e0b0746a70355516d061c89584a517299565e0ef
    PDSH_ROOT=/opt/polars-benchmark
    ARTIFACT=/ibex/benchmarking/results/tpch.tar.gz

    finish_tpch() {
        local code=$?
        mkdir -p /ibex/benchmarking/results
        {
            echo "ibex_commit=$(git -C /ibex rev-parse HEAD)"
            echo "pdsh_commit=$PDSH_COMMIT"
            uv run --project /ibex python3 -c 'import duckdb, polars; print(f"duckdb={duckdb.__version__}"); print(f"polars={polars.__version__}")'
        } > /ibex/benchmarking/tpch/results/versions.txt 2>&1 || true
        tar -C /ibex/benchmarking/tpch -czf "$ARTIFACT" results
        aws s3 cp "$ARTIFACT" "s3://${IBEX_S3_BUCKET}/${IBEX_RESULT_KEY}" --region "${IBEX_REGION}" || true
        shutdown -h now
        return "$code"
    }
    trap finish_tpch EXIT

    build_ibex
    if [[ ! -d "$PDSH_ROOT/.git" ]]; then
        git clone "$PDSH_REPO" "$PDSH_ROOT"
    fi
    git -C "$PDSH_ROOT" fetch --quiet origin "$PDSH_COMMIT"
    git -C "$PDSH_ROOT" checkout --force "$PDSH_COMMIT"
    uv sync --project /ibex

    IFS=',' read -r -a TPCH_SCALES <<< "${IBEX_TPCH_SCALES:-1}"
    for scale in "${TPCH_SCALES[@]}"; do
        bash /ibex/benchmarking/tpch/gen_data.sh "$scale"
        bash /ibex/benchmarking/tpch/gen_parquet.sh "$scale"
        bash /ibex/benchmarking/tpch/run_bench.sh --sf "$scale" \
            --warmup "${IBEX_WARMUP:-1}" --iters "${IBEX_ITERS:-5}" --pdsh-root "$PDSH_ROOT"
    done
    exit 0
fi

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
