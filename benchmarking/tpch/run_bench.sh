#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# run_bench.sh — time TPC-H/PDS-H queries: Ibex, this tree's Polars implementation,
# and the upstream Polars PDS-H Polars + DuckDB implementations.
# Every engine is run in both its default multi-threaded configuration and a
# one-thread configuration. Ibex's parallel islands are enabled explicitly for
# the MT row and disabled for the ST row, making the comparison reproducible
# even when the caller has ambient IBEX_* settings.
#
# Prerequisite (once per scale factor):
#   ./gen_data.sh <scale> && ./gen_parquet.sh <scale>
#
# The queries read benchmarking/data/tpch/parquet/, a symlink this script points
# at parquet_sf<scale>/ for the scale it is timing. Correctness (check_answers.py)
# is only defined at SF-1; higher scales are timing-only.
#
# Every run is also ARCHIVED under results/runs/<utc>_<commit>/ with a manifest
# recording the commit, the settings and the box. results/ itself keeps holding
# the latest run under fixed names, because print_table.py and the website
# generator read those — the archive is what makes two runs comparable, which
# fixed names alone can never be: a rerun silently overwrites the numbers you
# wanted to compare against. Use compare_runs.py to diff two archived runs.
#
# Usage:
#   ./run_bench.sh [--sf N] [--warmup N] [--iters N] [--pdsh-root DIR]
#                  [--skip-pdsh] [--cores N] [--label TEXT] [--no-archive]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DATA_ROOT="$IBEX_ROOT/benchmarking/data/tpch"
RESULTS="$SCRIPT_DIR/results"

SCALE=1
WARMUP=1
ITERS=5
SKIP_PDSH=0
ARCHIVE=1
LABEL=""
# Cores the whole comparison is pinned to. Unset means "every core on the box",
# which is the wrong default for a CROSS-ENGINE run on a big local machine:
# Polars sizes its pool from nproc and thrashes above ~8, which inflates Ibex's
# apparent lead. Pin both engines to the same set instead.
CORES="${IBEX_BENCH_CORES:-}"
PDSH_ROOT="${PDSH_ROOT:-}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sf)     SCALE="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters)  ITERS="$2";  shift 2 ;;
        --pdsh-root) PDSH_ROOT="$2"; shift 2 ;;
        --skip-pdsh) SKIP_PDSH=1; shift ;;
        --cores)  CORES="$2"; shift 2 ;;
        --label)  LABEL="$2"; shift 2 ;;
        --no-archive) ARCHIVE=0; shift ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

PARQUET_DIR="$DATA_ROOT/parquet_sf${SCALE}"
if [[ ! -d "$PARQUET_DIR" ]]; then
    echo "error: $PARQUET_DIR not found — run ./gen_data.sh $SCALE && ./gen_parquet.sh $SCALE first" >&2
    exit 1
fi

if [[ -z "$PDSH_ROOT" && "$SKIP_PDSH" -eq 0 ]]; then
    echo "error: --pdsh-root is required (a checkout of pola-rs/polars-benchmark)" >&2
    echo "       or pass --skip-pdsh to time only ibex and this tree's Polars." >&2
    exit 1
fi

# Pin every engine to the same cores. `taskset` bounds the process; the two
# variables stop Ibex's worker pool and Polars' pool from each sizing themselves
# from nproc and oversubscribing the pinned set. The per-engine ST rows below
# override these, so the MT/ST contrast survives the pin.
PIN=()
if [[ -n "$CORES" ]]; then
    PIN=(taskset -c "0-$((CORES - 1))")
    export IBEX_CORES="$CORES"
    export POLARS_MAX_THREADS="$CORES"
    echo "=== pinned to ${CORES} cores (of $(nproc)) ==="
fi

# ── Archiving ────────────────────────────────────────────────────────────────
# Copy this run's TSVs somewhere a later run cannot overwrite, next to a
# manifest saying what produced them.
#
# The manifest records the things that silently invalidate a comparison. The
# commit and the dirty flag, because a number from an uncommitted tree cannot be
# reproduced. The core count and scale, because the same query at 8 and 24 cores
# is two different measurements. Which engine rows were MEASURED versus carried
# forward from an earlier run, because `--skip-pdsh` leaves real-looking columns
# in the table that this run did not produce. And the CPU, since these land in
# the repo and get read on other machines.
archive_run() {
    [[ "$ARCHIVE" -eq 1 ]] || return 0
    local commit branch dirty stamp dir
    commit=$(git -C "$IBEX_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)
    branch=$(git -C "$IBEX_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
    dirty=false
    [[ -n "$(git -C "$IBEX_ROOT" status --porcelain 2>/dev/null)" ]] && dirty=true
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    # A dirty run's commit label names its PARENT commit, not the code that
    # actually ran — two runs can show the identical "<stamp>_<commit>" prefix
    # while one is clean and the other is a since-uncommitted change, which is
    # exactly the mix-up that made a real fix look like it had "disappeared"
    # when eyeballed via `ls`. Marking it in the directory name, not only
    # buried in manifest.json, is what makes that visible without opening a
    # file.
    local dirty_suffix=""
    [[ "$dirty" == true ]] && dirty_suffix="-dirty"
    dir="$RESULTS/runs/${stamp}_${commit:0:8}${dirty_suffix}_sf${SCALE}"
    mkdir -p "$dir"

    local measured=("ibex" "ibex-st" "polars" "polars-st") carried=()
    if [[ "$SKIP_PDSH" -eq 1 ]]; then
        carried=("pdsh-polars" "pdsh-polars-st" "pdsh-duckdb" "pdsh-duckdb-st")
    else
        measured+=("pdsh-polars" "pdsh-polars-st" "pdsh-duckdb" "pdsh-duckdb-st")
    fi

    local f
    for f in "$RESULTS"/*"${SUFFIX}.tsv"; do
        [[ -e "$f" ]] && cp "$f" "$dir/"
    done

    local join_list
    join_list() { local IFS=,; echo "$*"; }
    {
        printf '{\n'
        printf '  "suite": "pdsh",\n'
        printf '  "commit": "%s",\n' "$commit"
        printf '  "branch": "%s",\n' "$branch"
        printf '  "dirty": %s,\n' "$dirty"
        printf '  "generated_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '  "scale_factor": %s,\n' "$SCALE"
        printf '  "cores": "%s",\n' "${CORES:-all:$(nproc)}"
        printf '  "warmup": %s,\n' "$WARMUP"
        printf '  "iters": %s,\n' "$ITERS"
        printf '  "measured": "%s",\n' "$(join_list "${measured[@]}")"
        printf '  "carried_forward": "%s",\n' "$(join_list "${carried[@]}")"
        printf '  "host": "%s",\n' "$(uname -n)"
        printf '  "kernel": "%s",\n' "$(uname -r)"
        printf '  "cpu": "%s",\n' "$(LC_ALL=C lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -1)"
        printf '  "nproc": %s,\n' "$(nproc)"
        printf '  "label": "%s"\n' "$LABEL"
        printf '}\n'
    } > "$dir/manifest.json"

    echo
    echo "archived to ${dir#"$IBEX_ROOT"/}"
    [[ "$dirty" == true ]] && echo "  WARNING: working tree was dirty — this run is not reproducible"

    # results/*.tsv (the "latest" files) get overwritten every run, and this
    # archive directory only supports comparing two runs you pick by hand
    # (compare_runs.py). Append this run's numbers to the one file that is
    # never overwritten, so a query's trend across commits is one `show_history.py`
    # call instead of opening N manifests by hand.
    python3 "$SCRIPT_DIR/append_history.py" "$dir" || true
    return 0
}

# Point the path the queries read at this scale's data.
ln -sfn "parquet_sf${SCALE}" "$DATA_ROOT/parquet"
echo "=== scale factor: SF-${SCALE} (parquet -> parquet_sf${SCALE}) ==="

# Results are suffixed by scale so runs at different scales do not clobber.
SUFFIX="_sf${SCALE}"

echo "=== ibex (multi-threaded, ${CORES:-$(nproc)} cores) ==="
IBEX_CORES="${CORES:-auto}" "${PIN[@]}" python3 "$SCRIPT_DIR/bench_ibex.py" \
    --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/ibex${SUFFIX}.tsv"

echo "=== ibex-st (single-threaded) ==="
IBEX_CORES=1 "${PIN[@]}" python3 "$SCRIPT_DIR/bench_ibex.py" \
    --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/ibex_st${SUFFIX}.tsv.tmp"
sed 's/^ibex\t/ibex-st\t/' "$RESULTS/ibex_st${SUFFIX}.tsv.tmp" > "$RESULTS/ibex_st${SUFFIX}.tsv"
rm -f "$RESULTS/ibex_st${SUFFIX}.tsv.tmp"

echo "=== polars (multi-threaded, ${CORES:-$(nproc)} cores) ==="
"${PIN[@]}" uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" --warmup "$WARMUP" --iters "$ITERS" \
    --out "$RESULTS/polars${SUFFIX}.tsv"

echo "=== polars-st (single-threaded, apples-to-apples vs. ibex) ==="
POLARS_MAX_THREADS=1 "${PIN[@]}" uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_polars.py" \
    --warmup "$WARMUP" --iters "$ITERS" --out "$RESULTS/polars_st${SUFFIX}.tsv.tmp"
sed 's/^polars\t/polars-st\t/' "$RESULTS/polars_st${SUFFIX}.tsv.tmp" > "$RESULTS/polars_st${SUFFIX}.tsv"
rm -f "$RESULTS/polars_st${SUFFIX}.tsv.tmp"

if [[ "$SKIP_PDSH" -eq 1 ]]; then
    # The upstream engines are pinned baselines here: their numbers move only
    # when polars-benchmark or DuckDB itself changes, so a run without that
    # checkout reuses whatever is already in results/ rather than dropping the
    # columns. Say so, because a carried-forward row is not a measured one.
    echo
    echo "=== upstream PDS-H engines SKIPPED (--skip-pdsh) ==="
    for f in "$RESULTS"/pdsh_*"${SUFFIX}.tsv"; do
        [[ -e "$f" ]] || continue
        echo "    carrying forward $(basename "$f") ($(date -r "$f" +%Y-%m-%d))"
    done
    echo
    python3 "$SCRIPT_DIR/print_table.py" "$RESULTS"/*"${SUFFIX}.tsv"
    archive_run
    exit 0
fi

echo "=== upstream PDS-H Polars (multi-threaded, $(nproc) cores) ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" --engine polars --pdsh-root "$PDSH_ROOT" \
    --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" --framework pdsh-polars \
    --out "$RESULTS/pdsh_polars${SUFFIX}.tsv"

echo "=== upstream PDS-H Polars (single-threaded) ==="
POLARS_MAX_THREADS=1 uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" \
    --engine polars --pdsh-root "$PDSH_ROOT" --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" \
    --framework pdsh-polars-st --out "$RESULTS/pdsh_polars_st${SUFFIX}.tsv"

echo "=== upstream PDS-H DuckDB SQL ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" --engine duckdb --pdsh-root "$PDSH_ROOT" \
    --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" --framework pdsh-duckdb \
    --out "$RESULTS/pdsh_duckdb${SUFFIX}.tsv"

echo "=== upstream PDS-H DuckDB SQL (single-threaded) ==="
uv run --project "$IBEX_ROOT" "$SCRIPT_DIR/bench_pdsh.py" --engine duckdb --threads 1 \
    --pdsh-root "$PDSH_ROOT" --sf "$SCALE" --warmup "$WARMUP" --iters "$ITERS" \
    --framework pdsh-duckdb-st --out "$RESULTS/pdsh_duckdb_st${SUFFIX}.tsv"

echo
python3 "$SCRIPT_DIR/print_table.py" "$RESULTS"/*"${SUFFIX}.tsv"
archive_run
