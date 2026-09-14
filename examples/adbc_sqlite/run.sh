#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# One command: build the trades database, run trades.ibex against it through
# the ADBC SQLite driver, and check the exported summary.
#
#   examples/adbc_sqlite/run.sh [BUILD_DIR]
#
# BUILD_DIR defaults to ./build and must have been configured with
# -DIBEX_BUILD_ADBC=ON. The SQLite driver is, in order:
#   $ADBC_DRIVER_SQLITE (a path to libadbc_driver_sqlite.so),
#   $CONDA_PREFIX/lib/libadbc_driver_sqlite.so if a conda env provides it,
#   the name "sqlite", resolved through a driver manifest such as the one
#   scripts/install_adbc_driver.sh sqlite writes.
# Override the tool or plugin locations with $IBEX_BIN / $IBEX_PLUGIN_DIR.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
build="${1:-build}"
ibex="${IBEX_BIN:-$build/tools/ibex}"
plugins="${IBEX_PLUGIN_DIR:-$(dirname "$ibex")}"

driver="${ADBC_DRIVER_SQLITE:-}"
if [[ -n "$driver" && ! -f "$driver" ]]; then
    echo "ADBC_DRIVER_SQLITE=$driver does not exist" >&2
    exit 1
fi
if [[ -z "$driver" && -f "${CONDA_PREFIX:-/nonexistent}/lib/libadbc_driver_sqlite.so" ]]; then
    driver="$CONDA_PREFIX/lib/libadbc_driver_sqlite.so"
fi
# Otherwise by name; if no manifest exists the read fails and names it.
driver="${driver:-sqlite}"
for f in "$ibex" "$plugins/adbc.so" "$plugins/args.so" "$plugins/csv.so"; do
    if [[ ! -e "$f" ]]; then
        echo "missing $f -- build with -DIBEX_BUILD_ADBC=ON first" >&2
        exit 1
    fi
done

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

python3 "$here/make_trades_db.py" "$work/trades.sqlite"
"$ibex" --plugin-path "$plugins" "$here/trades.ibex" -- \
    --driver "$driver" --db "$work/trades.sqlite" --out "$work/summary.csv"

if diff -u "$here/expected_summary.csv" "$work/summary.csv"; then
    echo "summary.csv matches expected_summary.csv"
else
    echo "summary.csv differs from expected_summary.csv" >&2
    exit 1
fi
