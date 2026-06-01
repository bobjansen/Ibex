#!/usr/bin/env bash
# install-deps.sh — install all benchmark dependencies on Ubuntu 22.04/24.04.
#
# This script installs everything needed to run the full benchmark suite:
#   - C++ toolchain (current stable Clang from apt.llvm.org, cmake, ninja)
#   - Python 3.11+ with uv (manages numpy, pandas, polars, duckdb)
#   - R with data.table and dplyr/tidyr
#   - jemalloc for ibex page-retention optimization
#
# Usage:
#   sudo ./benchmarking/aws/install-deps.sh          # install everything
#   sudo ./benchmarking/aws/install-deps.sh --no-r   # skip R packages
#
# Designed for: Ubuntu 22.04 (Jammy) and 24.04 (Noble) on x86_64.
# Also works on Debian 12+ and aarch64 with minor adjustments.

set -euo pipefail

INSTALL_R=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-r)     INSTALL_R=0; shift ;;
        -h|--help)
            echo "Usage: sudo $(basename "$0") [--no-r]"
            echo ""
            echo "Options:"
            echo "  --no-r   Skip R and R package installation"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ $EUID -ne 0 ]]; then
    echo "error: this script must be run as root (use sudo)" >&2
    exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "━━━ System packages ━━━"
apt-get update -qq

# ── C++ toolchain ────────────────────────────────────────────────────────────
# Noble's apt only ships clang-18 (reports __cpp_concepts=201907L, forcing the
# libstdc++ <expected> workaround). Pull a current stable Clang from
# apt.llvm.org so benchmark builds use a modern compiler. Bump CLANG_VERSION to
# re-baseline; must match bootstrap.sh.
CLANG_VERSION=21
apt-get install -y --no-install-recommends \
    ninja-build \
    libjemalloc-dev libcurl4-openssl-dev libssl-dev zlib1g-dev \
    git curl unzip ca-certificates time \
    wget gnupg lsb-release software-properties-common

# Noble's apt CMake is 3.28, on which Arrow 22's from-source build fails
# (mis-resolved versions.txt → empty Boost SHA256). Install the official
# Kitware binary, pinned to match bootstrap.sh.
CMAKE_VERSION=3.31.6
curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" -o /tmp/cmake.tar.gz
tar -xzf /tmp/cmake.tar.gz -C /opt
update-alternatives --install /usr/bin/cmake cmake "/opt/cmake-${CMAKE_VERSION}-linux-x86_64/bin/cmake" 100

curl -fsSL https://apt.llvm.org/llvm.sh -o /tmp/llvm.sh
chmod +x /tmp/llvm.sh
/tmp/llvm.sh "${CLANG_VERSION}"

# Make the freshly installed Clang the unversioned default.
update-alternatives --install /usr/bin/clang++ clang++ "/usr/bin/clang++-${CLANG_VERSION}" 100
update-alternatives --install /usr/bin/clang   clang   "/usr/bin/clang-${CLANG_VERSION}"   100

echo "✓ C++ toolchain: $(clang++ --version | head -1)"
echo "✓ CMake: $(cmake --version | head -1)"
echo "✓ Ninja: $(ninja --version)"
echo ""

# ── Python + uv ──────────────────────────────────────────────────────────────
echo "━━━ Python ━━━"
apt-get install -y --no-install-recommends python3 python3-venv python3-dev

if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Make uv available for current script and future shells
    export PATH="/root/.local/bin:$HOME/.local/bin:$PATH"
fi

echo "✓ Python: $(python3 --version)"
echo "✓ uv: $(uv --version)"
echo ""

# Python packages are managed by uv via the repo-root pyproject.toml.
# The first `uv run` invocation will automatically install:
#   numpy, pandas, polars, duckdb
echo "Python packages (numpy, pandas, polars, duckdb) will be installed"
echo "automatically by uv on first benchmark run."
echo ""

# ── R ────────────────────────────────────────────────────────────────────────
if [[ $INSTALL_R -eq 1 ]]; then
    echo "━━━ R ━━━"
    apt-get install -y --no-install-recommends \
        r-base r-cran-data.table r-cran-optparse

    # dplyr and tidyr may not be in apt; install from CRAN if missing
    Rscript -e '
        needed <- c("dplyr", "tidyr")
        missing <- needed[!sapply(needed, requireNamespace, quietly = TRUE)]
        if (length(missing) > 0) {
            install.packages(missing, repos = "https://cloud.r-project.org")
        }
    '

    echo "✓ R: $(Rscript --version 2>&1 | head -1)"
    echo "✓ data.table: $(Rscript -e 'cat(as.character(packageVersion("data.table")))')"
    echo "✓ dplyr: $(Rscript -e 'cat(as.character(packageVersion("dplyr")))')"
    echo ""
else
    echo "━━━ R: skipped (--no-r) ━━━"
    echo ""
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo "━━━ Done ━━━"
echo ""
echo "Next steps:"
echo "  1. Build ibex (release):"
echo "     cmake -B build-release -G Ninja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release"
echo "     ninja -C build-release"
echo ""
echo "  2. Run benchmarks:"
echo "     cd benchmarking"
echo "     ./run_all.sh                          # single size (4M rows)"
echo "     ./run_scale_suite.sh --sizes 1M,4M    # multi-size scaling"
echo ""
echo "  3. Skip specific frameworks:"
echo "     ./run_scale_suite.sh --skip-r --skip-dplyr --skip-pandas --skip-duckdb-st"
