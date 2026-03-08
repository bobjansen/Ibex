#!/usr/bin/env bash
# compare_ibex_git.sh — compare ibex benchmark timings across git states.
#
# Compares two states:
#   - git refs (commit/tag/branch), built in temporary detached worktrees
#   - WORKTREE (the current checkout, including uncommitted changes)
#
# Defaults:
#   base   = HEAD
#   target = WORKTREE
#
# Usage examples:
#   ./benchmarking/compare_ibex_git.sh
#   ./benchmarking/compare_ibex_git.sh --target HEAD~1
#   ./benchmarking/compare_ibex_git.sh --base v0.3.0 --target HEAD
#
# Notes:
#   - Uses Release builds in temporary per-state build directories
#   - Uses the same CSV inputs for both sides (from the current repo by default)

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: compare_ibex_git.sh [options]

Options:
  --base <ref|WORKTREE>     Base state (default: HEAD)
  --target <ref|WORKTREE>   Target state (default: WORKTREE)
  --warmup <N>              Warmup iterations for ibex_bench (default: 1)
  --iters <N>               Timed iterations for ibex_bench (default: 7)
  --repeats <N>             Repeats per side; report median (default: 3)
  --taskset <cpuset>        Pin benchmark runs with taskset -c
  --numa-node <N>           Pin benchmark runs with numactl node/memory bind
  --csv <path>              prices.csv path
  --csv-multi <path>        prices_multi.csv path
  --csv-trades <path>       trades.csv path
  --csv-events <path>       events.csv path
  --csv-lookup <path>       lookup.csv path
  --keep-temp               Keep temporary worktrees/results directory
  -h, --help                Show this help

States:
  WORKTREE means the current checkout including local uncommitted changes.
  Any other value is resolved as a git revision.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
BASE_STATE="HEAD"
TARGET_STATE="WORKTREE"
WARMUP=1
ITERS=7
REPEATS=3
KEEP_TEMP=0
TASKSET_CPUSET=""
NUMA_NODE=""

CSV="$REPO_ROOT/benchmarking/data/prices.csv"
CSV_MULTI="$REPO_ROOT/benchmarking/data/prices_multi.csv"
CSV_TRADES="$REPO_ROOT/benchmarking/data/trades.csv"
CSV_EVENTS="$REPO_ROOT/benchmarking/data/events.csv"
CSV_LOOKUP="$REPO_ROOT/benchmarking/data/lookup.csv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base) BASE_STATE="$2"; shift 2 ;;
        --target) TARGET_STATE="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --repeats) REPEATS="$2"; shift 2 ;;
        --taskset) TASKSET_CPUSET="$2"; shift 2 ;;
        --numa-node) NUMA_NODE="$2"; shift 2 ;;
        --csv) CSV="$2"; shift 2 ;;
        --csv-multi) CSV_MULTI="$2"; shift 2 ;;
        --csv-trades) CSV_TRADES="$2"; shift 2 ;;
        --csv-events) CSV_EVENTS="$2"; shift 2 ;;
        --csv-lookup) CSV_LOOKUP="$2"; shift 2 ;;
        --keep-temp) KEEP_TEMP=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "error: unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ ! -f "$CSV" ]]; then
    echo "error: CSV not found: $CSV" >&2
    exit 1
fi
if [[ ! "$WARMUP" =~ ^[0-9]+$ ]]; then
    echo "error: --warmup must be a non-negative integer" >&2
    exit 1
fi
if [[ ! "$ITERS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --iters must be a positive integer" >&2
    exit 1
fi
if [[ ! "$REPEATS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --repeats must be a positive integer" >&2
    exit 1
fi
if [[ -n "$TASKSET_CPUSET" ]] && ! command -v taskset >/dev/null 2>&1; then
    echo "error: taskset not found but --taskset was provided" >&2
    exit 1
fi
if [[ -n "$NUMA_NODE" ]] && ! command -v numactl >/dev/null 2>&1; then
    echo "error: numactl not found but --numa-node was provided" >&2
    exit 1
fi

TMP_ROOT="$(mktemp -d /tmp/ibex-perfcmp.XXXXXX)"
BASE_WT=""
TARGET_WT=""
PIN_DESC="<none>"
declare -a PIN_PREFIX=()
if [[ -n "$NUMA_NODE" ]]; then
    PIN_PREFIX+=(numactl "--cpunodebind=$NUMA_NODE" "--membind=$NUMA_NODE")
    PIN_DESC="numactl --cpunodebind=$NUMA_NODE --membind=$NUMA_NODE"
fi
if [[ -n "$TASKSET_CPUSET" ]]; then
    PIN_PREFIX+=(taskset -c "$TASKSET_CPUSET")
    if [[ "$PIN_DESC" == "<none>" ]]; then
        PIN_DESC="taskset -c $TASKSET_CPUSET"
    else
        PIN_DESC="$PIN_DESC | taskset -c $TASKSET_CPUSET"
    fi
fi

cleanup() {
    if [[ -n "$BASE_WT" ]]; then
        git -C "$REPO_ROOT" worktree remove --force "$BASE_WT" >/dev/null 2>&1 || true
    fi
    if [[ -n "$TARGET_WT" ]]; then
        git -C "$REPO_ROOT" worktree remove --force "$TARGET_WT" >/dev/null 2>&1 || true
    fi
    if [[ "$KEEP_TEMP" -eq 0 ]]; then
        rm -rf "$TMP_ROOT"
    fi
}
trap cleanup EXIT

resolve_state_dir() {
    local state="$1"
    local slot="$2"
    if [[ "$state" == "WORKTREE" ]]; then
        printf "%s" "$REPO_ROOT"
        return 0
    fi
    git -C "$REPO_ROOT" rev-parse --verify "$state^{commit}" >/dev/null
    local wt="$TMP_ROOT/wt-$slot"
    git -C "$REPO_ROOT" worktree add --detach "$wt" "$state" >/dev/null
    if [[ "$slot" == "base" ]]; then
        BASE_WT="$wt"
    else
        TARGET_WT="$wt"
    fi
    printf "%s" "$wt"
}

state_label() {
    local state="$1"
    local dir="$2"
    if [[ "$state" == "WORKTREE" ]]; then
        local head dirty=""
        head="$(git -C "$dir" rev-parse --short HEAD)"
        if ! git -C "$dir" diff --quiet || ! git -C "$dir" diff --cached --quiet; then
            dirty=" (dirty)"
        fi
        printf "WORKTREE@%s%s" "$head" "$dirty"
        return 0
    fi
    local full short
    full="$(git -C "$REPO_ROOT" rev-parse --verify "$state^{commit}")"
    short="$(git -C "$REPO_ROOT" rev-parse --short "$full")"
    printf "%s@%s" "$state" "$short"
}

configure_and_build() {
    local src_dir="$1"
    local build_dir="$2"
    local log_file="$3"

    if ! cmake -S "$src_dir" -B "$build_dir" \
        -G Ninja \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_BUILD_TYPE=Release \
        -DIBEX_ENABLE_MARCH_NATIVE=ON >"$log_file" 2>&1; then
        cat "$log_file" >&2
        return 1
    fi
    if ! cmake --build "$build_dir" --parallel --target ibex_bench >>"$log_file" 2>&1; then
        cat "$log_file" >&2
        return 1
    fi
}

run_bench_once() {
    local src_dir="$1"
    local build_dir="$2"
    local out_tsv="$3"
    local log_file="$4"

    local args=(
        --csv "$CSV"
        --warmup "$WARMUP"
        --iters "$ITERS"
        --out "$out_tsv"
    )
    [[ -f "$CSV_MULTI" ]] && args+=(--csv-multi "$CSV_MULTI")
    [[ -f "$CSV_TRADES" ]] && args+=(--csv-trades "$CSV_TRADES")
    [[ -f "$CSV_EVENTS" ]] && args+=(--csv-events "$CSV_EVENTS")
    [[ -f "$CSV_LOOKUP" ]] && args+=(--csv-lookup "$CSV_LOOKUP")

    local -a cmd=(bash "$src_dir/benchmarking/bench_ibex.sh" "${args[@]}")
    if [[ "${#PIN_PREFIX[@]}" -gt 0 ]]; then
        cmd=("${PIN_PREFIX[@]}" "${cmd[@]}")
    fi

    if ! env IBEX_ROOT="$src_dir" BUILD_DIR="$build_dir" \
        "${cmd[@]}" >>"$log_file" 2>&1; then
        cat "$log_file" >&2
        return 1
    fi
}

aggregate_median() {
    local out_tsv="$1"
    shift
    {
        for f in "$@"; do
            tail -n +2 "$f" | awk -F'\t' '
                BEGIN { OFS="\t" }
                { print $1, $2, $3, $9 }
            '
        done
    } | sort -t$'\t' -k1,1 -k2,2 -k3,3g | awk -F'\t' '
        {
            key = $1 "\t" $2
            vals[key, ++n[key]] = $3 + 0.0
            rows[key] = $4
        }
        END {
            OFS = "\t"
            print "framework", "query", "median_ms", "min_ms", "max_ms", "rows", "samples"
            for (k in n) {
                m = n[k]
                if (m % 2 == 1) {
                    med = vals[k, (m + 1) / 2]
                } else {
                    med = (vals[k, m / 2] + vals[k, (m / 2) + 1]) / 2.0
                }
                minv = vals[k, 1]
                maxv = vals[k, m]
                split(k, p, "\t")
                printf "%s\t%s\t%.4f\t%.4f\t%.4f\t%s\t%d\n", p[1], p[2], med, minv, maxv, rows[k], m
            }
        }
    ' | sort -t$'\t' -k1,1 -k2,2 > "$out_tsv"
}

run_repeated_bench_and_aggregate() {
    local side="$1"
    local src_dir="$2"
    local build_dir="$3"
    local out_tsv="$4"
    local log_file="$5"
    local -a repeats=()

    configure_and_build "$src_dir" "$build_dir" "$log_file"
    local r repeat_out
    for ((r = 1; r <= REPEATS; ++r)); do
        repeat_out="$TMP_ROOT/${side}.repeat${r}.tsv"
        echo "  -> $side repeat $r/$REPEATS" >&2
        run_bench_once "$src_dir" "$build_dir" "$repeat_out" "$log_file"
        repeats+=("$repeat_out")
    done
    aggregate_median "$out_tsv" "${repeats[@]}"
}

BASE_DIR="$(resolve_state_dir "$BASE_STATE" "base")"
TARGET_DIR="$(resolve_state_dir "$TARGET_STATE" "target")"
BASE_LABEL="$(state_label "$BASE_STATE" "$BASE_DIR")"
TARGET_LABEL="$(state_label "$TARGET_STATE" "$TARGET_DIR")"

BASE_TSV="$TMP_ROOT/base.tsv"
TARGET_TSV="$TMP_ROOT/target.tsv"
REPORT_TSV="$TMP_ROOT/report.tsv"
SUMMARY_TSV="$TMP_ROOT/summary.tsv"
BASE_BUILD_DIR="$TMP_ROOT/build-base"
TARGET_BUILD_DIR="$TMP_ROOT/build-target"
BASE_LOG="$TMP_ROOT/log-base.txt"
TARGET_LOG="$TMP_ROOT/log-target.txt"

echo "Benchmarking base:   $BASE_LABEL" >&2
echo "Benchmarking target: $TARGET_LABEL" >&2
echo "Using warmup=$WARMUP, iters=$ITERS, repeats=$REPEATS" >&2
echo "Pinning: $PIN_DESC" >&2

run_repeated_bench_and_aggregate "base" "$BASE_DIR" "$BASE_BUILD_DIR" "$BASE_TSV" "$BASE_LOG"
run_repeated_bench_and_aggregate "target" "$TARGET_DIR" "$TARGET_BUILD_DIR" "$TARGET_TSV" "$TARGET_LOG"

awk -F'\t' '
    NR == FNR {
        if (FNR == 1) next
        key = $1 "\t" $2
        base[key] = $3 + 0.0
        base_rng[key] = ($5 + 0.0) - ($4 + 0.0)
        rows[key] = $6
        base_n[key] = $7 + 0
        next
    }
    FNR == 1 { next }
    {
        key = $1 "\t" $2
        tgt[key] = $3 + 0.0
        tgt_rng[key] = ($5 + 0.0) - ($4 + 0.0)
        tgt_n[key] = $7 + 0
    }
    END {
        OFS = "\t"
        print "framework", "query", "base_ms", "target_ms", "delta_ms", "delta_pct", "speedup_x", "base_range_ms", "target_range_ms", "base_samples", "target_samples", "rows"
        for (k in base) {
            if (!(k in tgt)) continue
            b = base[k]
            t = tgt[k]
            d = t - b
            pct = (b != 0.0) ? (100.0 * d / b) : 0.0
            sp = (t != 0.0) ? (b / t) : 0.0
            split(k, p, "\t")
            r = (k in rows) ? rows[k] : ""
            br = (k in base_rng) ? base_rng[k] : 0.0
            tr = (k in tgt_rng) ? tgt_rng[k] : 0.0
            bn = (k in base_n) ? base_n[k] : 0
            tn = (k in tgt_n) ? tgt_n[k] : 0
            print p[1], p[2], sprintf("%.4f", b), sprintf("%.4f", t), sprintf("%+.4f", d), sprintf("%+.2f%%", pct), sprintf("%.3f", sp), sprintf("%.4f", br), sprintf("%.4f", tr), bn, tn, r
        }
    }
' "$BASE_TSV" "$TARGET_TSV" | sort -t$'\t' -k1,1 -k2,2 > "$REPORT_TSV"

awk -F'\t' '
    FNR == 1 { next }
    {
        fw = $1
        b = $3 + 0.0
        t = $4 + 0.0

        base_sum[fw] += b
        tgt_sum[fw] += t
        n[fw] += 1
        base_sum["ALL"] += b
        tgt_sum["ALL"] += t
        n["ALL"] += 1

        if (b > 0.0 && t > 0.0) {
            log_sum[fw] += log(b / t)
            log_n[fw] += 1
            log_sum["ALL"] += log(b / t)
            log_n["ALL"] += 1
        }
    }
    END {
        OFS = "\t"
        print "scope", "queries", "total_base_ms", "total_target_ms", "total_delta_ms", "total_delta_pct", "geom_speedup_x", "geom_samples"
        order[1] = "ALL"
        idx = 2
        for (k in n) {
            if (k != "ALL")
                order[idx++] = k
        }
        for (i = 1; i < idx; ++i) {
            k = order[i]
            b = base_sum[k]
            t = tgt_sum[k]
            d = t - b
            pct = (b != 0.0) ? (100.0 * d / b) : 0.0
            if (log_n[k] > 0) {
                g = exp(log_sum[k] / log_n[k])
            } else {
                g = 0.0
            }
            print k, n[k], sprintf("%.4f", b), sprintf("%.4f", t), sprintf("%+.4f", d), sprintf("%+.2f%%", pct), sprintf("%.3f", g), log_n[k]
        }
    }
' "$REPORT_TSV" > "$SUMMARY_TSV"

printf "\nbase=%s\ntarget=%s\n\n" "$BASE_LABEL" "$TARGET_LABEL"
if command -v column >/dev/null 2>&1; then
    column -t -s $'\t' "$REPORT_TSV"
else
    cat "$REPORT_TSV"
fi

echo
echo "Summary (totals + geometric mean speedup):"
if command -v column >/dev/null 2>&1; then
    column -t -s $'\t' "$SUMMARY_TSV"
else
    cat "$SUMMARY_TSV"
fi

if [[ "$KEEP_TEMP" -eq 1 ]]; then
    echo
    echo "Kept temporary directory: $TMP_ROOT"
fi
