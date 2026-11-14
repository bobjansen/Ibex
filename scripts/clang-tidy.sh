#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Run the clang-tidy version CI runs: scripts/clang-tidy-version holds it,
# and the CI clang-tidy job installs that same version. Findings differ between
# versions, so a local run on any other version proves nothing about CI.
# CLANG_TIDY_BIN overrides the executable (a path, or another version on
# purpose).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLANG_TIDY_VERSION="$(tr -d '[:space:]' < "$REPO_ROOT/scripts/clang-tidy-version")"

CLANG_TIDY_BIN="${CLANG_TIDY_BIN:-}"
if [[ -z "$CLANG_TIDY_BIN" ]]; then
    if command -v "clang-tidy-$CLANG_TIDY_VERSION" >/dev/null 2>&1; then
        CLANG_TIDY_BIN="clang-tidy-$CLANG_TIDY_VERSION"
    elif command -v clang-tidy >/dev/null 2>&1 &&
        clang-tidy --version | grep -q "version $CLANG_TIDY_VERSION\."; then
        CLANG_TIDY_BIN="clang-tidy"
    fi
fi

if [[ -z "$CLANG_TIDY_BIN" ]]; then
    echo "error: clang-tidy $CLANG_TIDY_VERSION not found (the version CI runs, from" >&2
    echo "       scripts/clang-tidy-version). Install it from apt.llvm.org:" >&2
    echo "         wget -qO /tmp/llvm.sh https://apt.llvm.org/llvm.sh" >&2
    echo "         sudo bash /tmp/llvm.sh $CLANG_TIDY_VERSION && sudo apt-get install -y clang-tidy-$CLANG_TIDY_VERSION" >&2
    echo "       or set CLANG_TIDY_BIN to run another version deliberately." >&2
    exit 1
fi

# Default the compile database to the repo's Debug build dir, so
# `scripts/clang-tidy.sh <files>` works without `-p`. An explicit -p wins, and
# arguments after `--` mean the caller supplied the compile flags themselves.
needs_build_dir=1
for arg in "$@"; do
    case "$arg" in
        -p | --p | -p=* | --p=* | -- | --version | -version | --help | -help | --list-checks | -list-checks)
            needs_build_dir=0
            break
            ;;
    esac
done

if [[ "$needs_build_dir" -eq 1 ]]; then
    if [[ ! -f "$REPO_ROOT/build/compile_commands.json" ]]; then
        echo "error: $REPO_ROOT/build/compile_commands.json not found; configure build/ or pass -p <build-dir>" >&2
        exit 1
    fi
    set -- -p "$REPO_ROOT/build" "$@"
fi

exec "$CLANG_TIDY_BIN" "$@"
