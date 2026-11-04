#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Prefer the newest supported clang-tidy; CLANG_TIDY_BIN pins a specific tool.
set -euo pipefail

CLANG_TIDY_BIN="${CLANG_TIDY_BIN:-}"
if [[ -z "$CLANG_TIDY_BIN" ]]; then
    for candidate in clang-tidy-22 clang-tidy-21 clang-tidy-20 clang-tidy-19 clang-tidy-18 clang-tidy; do
        if command -v "$candidate" >/dev/null 2>&1; then
            CLANG_TIDY_BIN="$candidate"
            break
        fi
    done
fi

if [[ -z "$CLANG_TIDY_BIN" ]]; then
    echo "error: clang-tidy not found; install clang-tidy or set CLANG_TIDY_BIN" >&2
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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$needs_build_dir" -eq 1 ]]; then
    if [[ ! -f "$REPO_ROOT/build/compile_commands.json" ]]; then
        echo "error: $REPO_ROOT/build/compile_commands.json not found; configure build/ or pass -p <build-dir>" >&2
        exit 1
    fi
    set -- -p "$REPO_ROOT/build" "$@"
fi

exec "$CLANG_TIDY_BIN" "$@"
