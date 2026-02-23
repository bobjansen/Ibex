#!/usr/bin/env bash
# install.sh — install the CSV plugin's dependencies into the Ibex include tree.
#
# After running this, ibex-plugin-build.sh libs/csv/csv.cpp will work without
# any additional include flags.
#
# Usage:
#   libs/csv/install.sh
#
# Environment overrides:
#   IBEX_ROOT  — repo root      (default: two directories above this script)
#   BUILD_DIR  — cmake build dir (default: $IBEX_ROOT/build)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$(dirname "$SCRIPT_DIR")")}"
BUILD_DIR="${BUILD_DIR:-$IBEX_ROOT/build}"

DEST="$IBEX_ROOT/include"
RAPIDCSV_SRC="$BUILD_DIR/_deps/rapidcsv-src/src/rapidcsv.h"
RAPIDCSV_VERSION="v8.83"

# Prefer the copy already fetched by the main CMake build.
if [[ -f "$RAPIDCSV_SRC" ]]; then
    echo "▸ installing rapidcsv.h from build tree"
    cp "$RAPIDCSV_SRC" "$DEST/rapidcsv.h"
else
    # Fall back to a direct download of the pinned version.
    echo "▸ downloading rapidcsv $RAPIDCSV_VERSION"
    curl -fsSL \
        "https://raw.githubusercontent.com/d99kris/rapidcsv/$RAPIDCSV_VERSION/src/rapidcsv.h" \
        -o "$DEST/rapidcsv.h"
fi

echo "✓ installed $DEST/rapidcsv.h"
