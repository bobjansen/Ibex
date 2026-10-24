#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# download-demo-data.sh — fetch real-world demo datasets for Ibex examples.
#
# Usage:
#   download-demo-data.sh
#   download-demo-data.sh --force
#   download-demo-data.sh --skip-taxi
#
# Downloads into:
#   data/demo/
#   data/flights-1m.parquet (path used by the parquet examples and smoke tests)
#
# Datasets:
#   - palmerpenguins (small, clean teaching dataset)
#   - Titanic (single-table tutorial dataset)
#   - nycflights13 (relational join demo set)
#   - NYC TLC green taxi parquet + zone lookup (optional; larger)
#   - flights-1m.parquet from vega/falcon: the first 1M rows of the BTS
#     on-time data (2006-01-01..2006-02-28), built by Falcon's
#     data/convert_flights.ipynb; checksum-verified

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
DATA_DIR="${DATA_DIR:-$IBEX_ROOT/data/demo}"

FORCE=false
SKIP_TAXI=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [--force] [--skip-taxi]

Options:
  --force      re-download files even if they already exist
  --skip-taxi  skip the larger NYC TLC taxi parquet download
  -h, --help   show this help text

Destination:
  $DATA_DIR
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=true ;;
        --skip-taxi) SKIP_TAXI=true ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

download() {
    local url="$1"
    local dest="$2"
    local tmp="${dest}.tmp"

    mkdir -p "$(dirname "$dest")"

    if [[ -f "$dest" && "$FORCE" == false ]]; then
        echo "▸ keeping   ${dest#$IBEX_ROOT/}"
        return
    fi

    echo "▸ fetching  ${dest#$IBEX_ROOT/}"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --retry 3 --retry-delay 2 "$url" -o "$tmp"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$tmp" "$url"
    else
        echo "error: need curl or wget to download demo data" >&2
        exit 1
    fi

    mv "$tmp" "$dest"
}

download_csv_without_first_column() {
    local url="$1"
    local dest="$2"
    local tmp="${dest}.tmp"

    mkdir -p "$(dirname "$dest")"

    if [[ -f "$dest" && "$FORCE" == false ]]; then
        echo "▸ keeping   ${dest#$IBEX_ROOT/}"
        return
    fi

    echo "▸ fetching  ${dest#$IBEX_ROOT/}"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --retry 3 --retry-delay 2 "$url" -o "$tmp"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$tmp" "$url"
    else
        echo "error: need curl or wget to download demo data" >&2
        exit 1
    fi

    # Rdatasets prepends a row-number column; strip the first CSV field so the
    # result matches the original nycflights13 table layout.
    sed 's/^[^,]*,//' "$tmp" > "$dest"
    rm -f "$tmp"
}

verify_sha256() {
    local file="$1"
    local expected="$2"
    local actual

    if command -v sha256sum >/dev/null 2>&1; then
        actual="$(sha256sum "$file" | cut -d' ' -f1)"
    else
        actual="$(shasum -a 256 "$file" | cut -d' ' -f1)"
    fi

    if [[ "$actual" != "$expected" ]]; then
        echo "error: checksum mismatch for ${file#$IBEX_ROOT/}" >&2
        echo "  expected $expected" >&2
        echo "  actual   $actual" >&2
        exit 1
    fi
}

echo "▸ destination: $DATA_DIR"

# palmerpenguins
download \
    "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/main/inst/extdata/penguins.csv" \
    "$DATA_DIR/palmerpenguins/penguins.csv"
download \
    "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/main/inst/extdata/penguins_raw.csv" \
    "$DATA_DIR/palmerpenguins/penguins_raw.csv"

# pandas tutorial titanic dataset
download \
    "https://raw.githubusercontent.com/pandas-dev/pandas/main/doc/data/titanic.csv" \
    "$DATA_DIR/titanic/titanic.csv"

# nycflights13 relational demo set
download \
    "https://raw.githubusercontent.com/tidyverse/nycflights13/master/data-raw/airlines.csv" \
    "$DATA_DIR/nycflights13/airlines.csv"
download \
    "https://raw.githubusercontent.com/tidyverse/nycflights13/master/data-raw/airports.csv" \
    "$DATA_DIR/nycflights13/airports.csv"
download \
    "https://raw.githubusercontent.com/tidyverse/nycflights13/master/data-raw/planes.csv" \
    "$DATA_DIR/nycflights13/planes.csv"
download \
    "https://raw.githubusercontent.com/tidyverse/nycflights13/master/data-raw/weather.csv" \
    "$DATA_DIR/nycflights13/weather.csv"
download_csv_without_first_column \
    "https://vincentarelbundock.github.io/Rdatasets/csv/nycflights13/flights.csv" \
    "$DATA_DIR/nycflights13/flights.csv"

# NYC TLC data: one monthly parquet plus the zone lookup table.
if [[ "$SKIP_TAXI" == false ]]; then
    download \
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet" \
        "$DATA_DIR/nyc_taxi/green_tripdata_2024-01.parquet"
    download \
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" \
        "$DATA_DIR/nyc_taxi/taxi_zone_lookup.csv"
fi

# Falcon flights sample, pinned to the commit that added it.
download \
    "https://raw.githubusercontent.com/vega/falcon/a7351378997d5a3c5764b0f84d279e329f1132df/data/flights-1m.parquet" \
    "$IBEX_ROOT/data/flights-1m.parquet"
verify_sha256 \
    "$IBEX_ROOT/data/flights-1m.parquet" \
    "474d651871f360e798fd07cb591ffe7a8ef859e8cf7109610960871fb3bf44c5"

echo "✓ demo datasets ready under ${DATA_DIR#$IBEX_ROOT/}"
