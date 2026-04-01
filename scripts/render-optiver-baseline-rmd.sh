#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

output_format="${1:-html_document}"

Rscript -e "renv::load('${repo_root}'); rmarkdown::render('notebooks/baseline-lgb-xgb-and-catboost.Rmd', output_format='${output_format}')"
