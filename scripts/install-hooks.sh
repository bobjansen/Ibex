#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Install git hooks for this repo (local only).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(dirname "$SCRIPT_DIR")"

git -C "$IBEX_ROOT" config core.hooksPath .githooks
echo "✓ Git hooks installed (core.hooksPath=.githooks)"
