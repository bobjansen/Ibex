#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Build the Ibex engine document and resolve its bibliography and references.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${script_dir}"

for command in pdflatex bibtex; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "error: ${command} is required to build engine.tex" >&2
        exit 1
    fi
done

pdflatex -interaction=nonstopmode -halt-on-error engine.tex
bibtex engine
pdflatex -interaction=nonstopmode -halt-on-error engine.tex
pdflatex -interaction=nonstopmode -halt-on-error engine.tex

echo "built ${script_dir}/engine.pdf"
