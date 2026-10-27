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

exec "$CLANG_TIDY_BIN" "$@"
