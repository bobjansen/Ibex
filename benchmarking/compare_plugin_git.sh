#!/usr/bin/env bash
# compare_plugin_git.sh — A/B two git commits of ibex for a plugin-backed
# extern function (read_parquet, read_adbc, kafka_recv, ...).
#
# Why this exists: ibex_bench (and therefore compare_ibex_git.sh /
# benchmarking/aws/compare-git.sh) never loads a dynamically-loaded plugin —
# its "scan mode" hardcodes a direct C++ call to read_csv(), and
# compare_ibex_git.sh's configure_and_build builds with
# -DIBEX_BUILD_PARQUET=OFF. Neither can exercise read_parquet, read_adbc, or
# any other plugin-registered extern function, chunked or not. This script
# drives the real REPL binary (tools/ibex --plugin-path ...) instead, so a
# plugin's extern functions actually run.
#
# Methodology: bench-1brc.sh's measure_runner (REPL + --plugin-path under
# /usr/bin/time for wall-clock + RSS, INNER_RUNS amortizing process startup)
# crossed with compare_ibex_git.sh's two-worktree base/target build with
# interleaved repeats (WSL2/laptop drift is real — see
# project_bench_interleaved_methodology memory).
#
# Usage:
#   ./benchmarking/compare_plugin_git.sh \
#       --query benchmarking/queries/parquet_scan_agg.ibex \
#       --input "$(pwd)/benchmarking/data/prices.parquet" \
#       --plugin-target ibex_parquet_plugin \
#       --configure-arg -DIBEX_BUILD_PARQUET=ON
#
# --input must be an absolute path: it is baked into the query once and the
# same query text runs unmodified against both worktrees' REPL binaries.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: compare_plugin_git.sh --query <path.ibex> --input <path> --plugin-target <t1,t2,...> [options]

Required:
  --query <path.ibex>        Query template; __INPUT__ is replaced with --input
  --input <path>              Absolute path substituted into the query
  --plugin-target <t1,t2,...> CMake target(s) that build the plugin .so
                               (built alongside ibex_repl_bin), comma-separated

Options:
  --base <ref|WORKTREE>       Base state (default: HEAD)
  --target <ref|WORKTREE>     Target state (default: WORKTREE)
  --configure-arg <arg>       Extra `cmake -D...` configure arg (repeatable)
  --warmup <N>                Warmup runs before timing (default: 1)
  --iters <N>                 Timed iterations per repeat (default: 5)
  --inner-runs <N>            REPL invocations per timed iteration, to
                               amortize process startup (default: 3)
  --repeats <N>                Repeats per side (default: 3)
  --serial                    Disable interleaving (all base repeats, then target)
  --taskset <cpuset>          Pin runs with taskset -c
  --numa-node <N>              Pin runs with numactl node/memory bind
  --out <path.tsv>            Report path (default: benchmarking/results/compare_plugin_<ts>.tsv)
  --keep-temp                 Keep temporary worktrees/build dirs
  -h, --help                  Show this help

States:
  WORKTREE means the current checkout including local uncommitted changes.
  Any other value is resolved as a git revision.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

BASE_STATE="HEAD"
TARGET_STATE="WORKTREE"
QUERY=""
INPUT_PATH=""
PLUGIN_TARGETS=""
declare -a CONFIGURE_ARGS=()
WARMUP=1
ITERS=5
INNER_RUNS=3
REPEATS=3
INTERLEAVE=1
TASKSET_CPUSET=""
NUMA_NODE=""
OUT=""
KEEP_TEMP=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base) BASE_STATE="$2"; shift 2 ;;
        --target) TARGET_STATE="$2"; shift 2 ;;
        --query) QUERY="$2"; shift 2 ;;
        --input) INPUT_PATH="$2"; shift 2 ;;
        --plugin-target) PLUGIN_TARGETS="$2"; shift 2 ;;
        --configure-arg) CONFIGURE_ARGS+=("$2"); shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --inner-runs) INNER_RUNS="$2"; shift 2 ;;
        --repeats) REPEATS="$2"; shift 2 ;;
        --serial) INTERLEAVE=0; shift ;;
        --taskset) TASKSET_CPUSET="$2"; shift 2 ;;
        --numa-node) NUMA_NODE="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --keep-temp) KEEP_TEMP=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "error: unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$QUERY" || -z "$INPUT_PATH" || -z "$PLUGIN_TARGETS" ]]; then
    echo "error: --query, --input, and --plugin-target are required" >&2
    usage
    exit 1
fi
if [[ ! -f "$QUERY" ]]; then
    echo "error: query file not found: $QUERY" >&2
    exit 1
fi
if [[ "$INPUT_PATH" != /* ]]; then
    echo "error: --input must be an absolute path (it is baked into the query once, then reused" >&2
    echo "       unmodified against both worktrees): $INPUT_PATH" >&2
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
if [[ ! "$INNER_RUNS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: --inner-runs must be a positive integer" >&2
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
if [[ -z "$OUT" ]]; then
    OUT="$REPO_ROOT/benchmarking/results/compare_plugin_$(date +%Y%m%d_%H%M%S).tsv"
fi

IFS=',' read -r -a PLUGIN_TARGET_ARR <<< "$PLUGIN_TARGETS"

TMP_ROOT="$(mktemp -d "${IBEX_PERFCMP_TMPDIR:-/tmp}/ibex-plugincmp.XXXXXX")"
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

    declare -a launcher_args=()
    if command -v ccache >/dev/null 2>&1; then
        launcher_args=(-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache)
    fi

    if ! cmake -S "$src_dir" -B "$build_dir" \
        -G Ninja \
        -DCMAKE_C_COMPILER="${IBEX_CC:-clang}" \
        -DCMAKE_CXX_COMPILER="${IBEX_CXX:-clang++}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DIBEX_ENABLE_MARCH_NATIVE=ON \
        -DIBEX_BUILD_PYTHON_BRIDGE=OFF \
        "${launcher_args[@]}" \
        "${CONFIGURE_ARGS[@]}" >"$log_file" 2>&1; then
        cat "$log_file" >&2
        return 1
    fi
    if ! cmake --build "$build_dir" --parallel --target ibex_repl_bin "${PLUGIN_TARGET_ARR[@]}" \
        >>"$log_file" 2>&1; then
        cat "$log_file" >&2
        return 1
    fi
}

# ── Query template + REPL input (shared across both sides — same query text,
# absolute --input path, so it's worktree-independent) ─────────────────────
BENCH_QUERY="$TMP_ROOT/query.ibex"
sed "s#__INPUT__#$INPUT_PATH#g" "$QUERY" > "$BENCH_QUERY"
REPL_INPUT="$TMP_ROOT/query.repl"
printf ':load %s\n:q\n' "$BENCH_QUERY" > "$REPL_INPUT"

# ── bench-1brc.sh's measure_runner/summarize_file, adapted for two sides ───
measure_runner() {
    local timed_runner="$1"
    local memory_runner="$2"
    local out_file="$3"

    for ((i = 0; i < WARMUP; ++i)); do
        "$timed_runner" >/dev/null 2>/dev/null
    done
    for ((i = 0; i < ITERS; ++i)); do
        local time_file="$TMP_ROOT/time.txt"
        local rss_file="$TMP_ROOT/rss.txt"
        /usr/bin/time -f '%e' -o "$time_file" "$timed_runner" >/dev/null 2>/dev/null
        /usr/bin/time -f '%M' -o "$rss_file" "$memory_runner" >/dev/null 2>/dev/null
        printf '%s\t%s\n' "$(cat "$time_file")" "$(cat "$rss_file")" >> "$out_file"
    done
}

summarize_file() {
    local label="$1"
    local path="$2"
    awk -v label="$label" -v inner_runs="$INNER_RUNS" '
        BEGIN { sum_ms = 0.0; n = 0; min_ms = -1.0; max_ms = 0.0; sum_rss = 0.0; max_rss = 0.0; }
        NF {
            ms = ($1 + 0.0) * 1000.0 / inner_runs;
            rss = $2 + 0.0;
            sum_ms += ms; sum_rss += rss; ++n;
            if (min_ms < 0.0 || ms < min_ms) min_ms = ms;
            if (ms > max_ms) max_ms = ms;
            if (rss > max_rss) max_rss = rss;
        }
        END {
            if (n == 0) exit 1;
            printf "%s\t%.3f\t%.3f\t%.3f\t%.0f\t%.0f\t%d\n",
                   label, sum_ms / n, min_ms, max_ms, sum_rss / n, max_rss, n;
        }
    ' "$path"
}

BASE_DIR="$(resolve_state_dir "$BASE_STATE" "base")"
TARGET_DIR="$(resolve_state_dir "$TARGET_STATE" "target")"
BASE_LABEL="$(state_label "$BASE_STATE" "$BASE_DIR")"
TARGET_LABEL="$(state_label "$TARGET_STATE" "$TARGET_DIR")"

BASE_BUILD_DIR="$TMP_ROOT/build-base"
TARGET_BUILD_DIR="$TMP_ROOT/build-target"
BASE_LOG="$TMP_ROOT/log-base.txt"
TARGET_LOG="$TMP_ROOT/log-target.txt"

echo "Benchmarking base:   $BASE_LABEL" >&2
echo "Benchmarking target: $TARGET_LABEL" >&2
echo "Query: $QUERY (input: $INPUT_PATH)" >&2
echo "Plugin targets: ${PLUGIN_TARGET_ARR[*]}" >&2
echo "warmup=$WARMUP iters=$ITERS inner_runs=$INNER_RUNS repeats=$REPEATS interleave=$([[ "$INTERLEAVE" -eq 1 ]] && echo on || echo off)" >&2
echo "Pinning: $PIN_DESC" >&2

echo "Building base ..." >&2
configure_and_build "$BASE_DIR" "$BASE_BUILD_DIR" "$BASE_LOG"
echo "Building target ..." >&2
configure_and_build "$TARGET_DIR" "$TARGET_BUILD_DIR" "$TARGET_LOG"

BASE_TIMES="$TMP_ROOT/base.times.tsv"
TARGET_TIMES="$TMP_ROOT/target.times.tsv"
: > "$BASE_TIMES"
: > "$TARGET_TIMES"

# Build runner scripts for both sides up front (needed regardless of interleave mode).
build_runners() {
    local side="$1" build_dir="$2" log_file="$3"
    local repl="$build_dir/tools/ibex"
    local plugin_dir="$build_dir/tools"
    if [[ ! -x "$repl" ]]; then
        echo "error: REPL not found at $repl (see $log_file)" >&2
        exit 1
    fi

    local timed_runner="$TMP_ROOT/${side}_timed_runner.sh"
    cat > "$timed_runner" <<EOF
#!/usr/bin/env bash
set -euo pipefail
for ((j = 0; j < $INNER_RUNS; ++j)); do
    ${PIN_PREFIX[*]+"${PIN_PREFIX[@]}"} "$repl" --plugin-path "$plugin_dir" < "$REPL_INPUT" >/dev/null
done
EOF
    chmod +x "$timed_runner"
    local memory_runner="$TMP_ROOT/${side}_memory_runner.sh"
    cat > "$memory_runner" <<EOF
#!/usr/bin/env bash
set -euo pipefail
${PIN_PREFIX[*]+"${PIN_PREFIX[@]}"} "$repl" --plugin-path "$plugin_dir" < "$REPL_INPUT" >/dev/null
EOF
    chmod +x "$memory_runner"
}

build_runners "base" "$BASE_BUILD_DIR" "$BASE_LOG"
build_runners "target" "$TARGET_BUILD_DIR" "$TARGET_LOG"

for ((i = 0; i < WARMUP; ++i)); do
    "$TMP_ROOT/base_timed_runner.sh" >/dev/null 2>/dev/null
    "$TMP_ROOT/target_timed_runner.sh" >/dev/null 2>/dev/null
done

if [[ "$INTERLEAVE" -eq 1 ]]; then
    echo "Interleaving base/target repeats" >&2
    for ((r = 1; r <= REPEATS; ++r)); do
        echo "  -> base repeat $r/$REPEATS" >&2
        measure_runner "$TMP_ROOT/base_timed_runner.sh" "$TMP_ROOT/base_memory_runner.sh" "$BASE_TIMES"
        echo "  -> target repeat $r/$REPEATS" >&2
        measure_runner "$TMP_ROOT/target_timed_runner.sh" "$TMP_ROOT/target_memory_runner.sh" "$TARGET_TIMES"
    done
else
    for ((r = 1; r <= REPEATS; ++r)); do
        echo "  -> base repeat $r/$REPEATS" >&2
        measure_runner "$TMP_ROOT/base_timed_runner.sh" "$TMP_ROOT/base_memory_runner.sh" "$BASE_TIMES"
    done
    for ((r = 1; r <= REPEATS; ++r)); do
        echo "  -> target repeat $r/$REPEATS" >&2
        measure_runner "$TMP_ROOT/target_timed_runner.sh" "$TMP_ROOT/target_memory_runner.sh" "$TARGET_TIMES"
    done
fi

mkdir -p "$(dirname "$OUT")"
{
    printf "state\tavg_ms\tmin_ms\tmax_ms\tavg_maxrss_kb\tmax_maxrss_kb\tsamples\n"
    summarize_file "base:$BASE_LABEL" "$BASE_TIMES"
    summarize_file "target:$TARGET_LABEL" "$TARGET_TIMES"
} | tee "$OUT" >&2

echo "Report written to $OUT" >&2
