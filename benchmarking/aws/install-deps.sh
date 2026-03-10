#!/usr/bin/env bash
# install-deps.sh — install all benchmark dependencies on Ubuntu 22.04/24.04.
#
# This script installs everything needed to run the full benchmark suite:
#   - C++ toolchain (clang-18, cmake, ninja) for building ibex
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
apt-get install -y --no-install-recommends \
    clang-18 cmake ninja-build \
    libjemalloc-dev \
    git curl unzip ca-certificates

# Ensure clang-18 is the default if no unversioned clang exists
if ! command -v clang++ &>/dev/null; then
    update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-18 100
    update-alternatives --install /usr/bin/clang   clang   /usr/bin/clang-18   100
fi

echo "✓ C++ toolchain: $(clang++-18 --version | head -1)"
echo "✓ CMake: $(cmake --version | head -1)"
echo "✓ Ninja: $(ninja --version)"
echo ""

# ── Python + uv ──────────────────────────────────────────────────────────────
echo "━━━ Python ━━━"
apt-get install -y --no-install-recommends python3 python3-venv

if ! command -v uv &>/dev/null; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Make uv available for current script and future shells
    export PATH="/root/.local/bin:$HOME/.local/bin:$PATH"
fi

echo "✓ Python: $(python3 --version)"
echo "✓ uv: $(uv --version)"
echo ""

# Python packages are managed by uv via benchmarking/pyproject.toml.
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
echo "     cmake -B build-release -G Ninja -DCMAKE_CXX_COMPILER=clang++-18 -DCMAKE_BUILD_TYPE=Release"
echo "     ninja -C build-release"
echo ""
echo "  2. Run benchmarks:"
echo "     cd benchmarking"
echo "     ./run_all.sh                          # single size (4M rows)"
echo "     ./run_scale_suite.sh --sizes 1M,4M    # multi-size scaling"
echo ""
echo "  3. Skip specific frameworks:"
echo "     ./run_scale_suite.sh --skip-r --skip-dplyr --skip-pandas --skip-duckdb-st"
