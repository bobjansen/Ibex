#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
cd "${repo_root}"

output_format="${1:-html_document}"
build_dir="${IBEX_BUILD_DIR:-${repo_root}/build-release}"

cmake --build "${build_dir}" --parallel --target ibex_runtime ibex_parquet_plugin ibex_lightbm_plugin

Rscript -e "renv::load('${repo_root}'); install.packages('${repo_root}/r/ribex', repos = NULL, type = 'source', INSTALL_opts = '--preclean'); rmarkdown::render('notebooks/baseline-lgb-xgb-and-catboost.Rmd', output_format='${output_format}')"
