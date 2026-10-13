#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# Generate h2o db-benchmark data with the upstream R generators (vendored in
# datagen/, unmodified) into benchmarking/data/h2o/. Needs R + data.table.
#
#   gen_data.sh groupby 1e7 1e2 0 0    # G1_1e7_1e2_0_0.csv  (N, K, NA%, sorted)
#   gen_data.sh join    1e7 0 0        # J1_1e7_NA_0_0.csv + small/medium/big
#
# Existing files are kept: the generators are seeded (set.seed(108)), so a
# rerun would produce the same bytes.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${H2O_DATA_DIR:-$HERE/../data/h2o}"
mkdir -p "$OUT"
task="${1:?usage: gen_data.sh groupby|join N [K] NA sorted}"; shift
cd "$OUT"
case "$task" in
  groupby)
    Rscript "$HERE/datagen/groupby-datagen.R" "$@" ;;
  join)
    # join-datagen.R takes (N, K-ignored, NA, sorted).
    Rscript "$HERE/datagen/join-datagen.R" "$1" 0 "$2" "$3" ;;
  *) echo "unknown task: $task" >&2; exit 2 ;;
esac
ls -la "$OUT"
