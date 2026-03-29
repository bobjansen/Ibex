#!/usr/bin/env bash
# download-demo-data.sh — fetch real-world demo datasets for Ibex examples.
#
# Usage:
#   download-demo-data.sh
#   download-demo-data.sh --force
#   download-demo-data.sh --skip-taxi
#
# Downloads into:
#   data/demo/
#
# Datasets:
#   - palmerpenguins (small, clean teaching dataset)
#   - Titanic (single-table tutorial dataset)
#   - nycflights13 (relational join demo set)
#   - NYC TLC green taxi parquet + zone lookup (optional; larger)

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

echo "✓ demo datasets ready under ${DATA_DIR#$IBEX_ROOT/}"
