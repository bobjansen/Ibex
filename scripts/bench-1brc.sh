#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IBEX_ROOT="${IBEX_ROOT:-$(dirname "$SCRIPT_DIR")}"
if [[ -n "${BUILD_DIR:-}" ]]; then
    : # user-specified
elif [[ -x "$IBEX_ROOT/build-release/tools/ibex" ]]; then
    BUILD_DIR="$IBEX_ROOT/build-release"
else
    BUILD_DIR="$IBEX_ROOT/build"
fi

INPUT="$IBEX_ROOT/examples/measurements.txt"
QUERY="$IBEX_ROOT/examples/onebrc.ibex"
WARMUP=1
ITERS=5
OUT="$IBEX_ROOT/benchmarking/results/onebrc.tsv"
SKIP_INTERPRETED=0
SKIP_COMPILED=1
SKIP_POLARS=0
INNER_RUNS=20

while [[ $# -gt 0 ]]; do
    case "$1" in
        --input) INPUT="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --inner-runs) INNER_RUNS="$2"; shift 2 ;;
        --with-compiled) SKIP_COMPILED=0; shift ;;
        --skip-interpreted) SKIP_INTERPRETED=1; shift ;;
        --skip-compiled) SKIP_COMPILED=1; shift ;;
        --skip-polars) SKIP_POLARS=1; shift ;;
        *)
            echo "unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ ! -f "$INPUT" ]]; then
    echo "error: input file not found: $INPUT" >&2
    exit 1
fi
if [[ ! -f "$QUERY" ]]; then
    echo "error: query file not found: $QUERY" >&2
    exit 1
fi

IBEX_REPL="$BUILD_DIR/tools/ibex"
IBEX_PLUGIN_DIR="$BUILD_DIR/tools"
IBEX_COMPILE="$BUILD_DIR/tools/ibex_compile"
CXX="${CXX:-clang++}"
CXXFLAGS="${CXXFLAGS:-}"

lld_ok=0
if "$CXX" -fuse-ld=lld -Wl,--version -x c++ - -o /tmp/ibex_onebrc_lld_test </dev/null \
    >/dev/null 2>&1; then
    lld_ok=1
    rm -f /tmp/ibex_onebrc_lld_test
fi

if [[ -z "$CXXFLAGS" ]]; then
    if [[ $lld_ok -eq 1 ]]; then
        CXXFLAGS="-O3 -DNDEBUG -std=gnu++23 -flto=thin -fuse-ld=lld"
    else
        CXXFLAGS="-O3 -DNDEBUG -std=gnu++23"
    fi
fi

mkdir -p "$(dirname "$OUT")"

echo "=== building ibex 1BRC prerequisites ===" >&2
cmake --build "$BUILD_DIR" --parallel --target ibex_repl_bin ibex_compile_bin ibex_csv_plugin >&2

if [[ ! -x "$IBEX_REPL" ]]; then
    echo "error: ibex REPL not found at $IBEX_REPL" >&2
    exit 1
fi
if [[ ! -x "$IBEX_COMPILE" ]]; then
    echo "error: ibex compiler not found at $IBEX_COMPILE" >&2
    exit 1
fi
if [[ ! -f "$IBEX_PLUGIN_DIR/csv.so" && ! -f "$IBEX_PLUGIN_DIR/csv.dylib" && ! -f "$IBEX_PLUGIN_DIR/csv.dll" ]]; then
    echo "error: csv plugin not found in $IBEX_PLUGIN_DIR" >&2
    exit 1
fi

INPUT_ROWS="$(wc -l < "$INPUT" | tr -d '[:space:]')"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

REPL_INPUT="$TMP_DIR/onebrc.repl"
COMPILED_BIN="$TMP_DIR/onebrc"
INTERPRETED_TIMES="$TMP_DIR/interpreted.txt"
COMPILED_TIMES="$TMP_DIR/compiled.txt"
POLARS_TIMES="$TMP_DIR/polars.txt"
BENCH_QUERY="$TMP_DIR/onebrc_bench.ibex"
INTERPRETED_RUNNER="$TMP_DIR/interpreted_runner.sh"
INTERPRETED_MEMORY_RUNNER="$TMP_DIR/interpreted_memory_runner.sh"
COMPILED_RUNNER="$TMP_DIR/compiled_runner.sh"
COMPILED_MEMORY_RUNNER="$TMP_DIR/compiled_memory_runner.sh"
POLARS_SCRIPT="$TMP_DIR/onebrc_polars.py"
POLARS_RUNNER="$TMP_DIR/polars_runner.sh"
POLARS_MEMORY_RUNNER="$TMP_DIR/polars_memory_runner.sh"
UV_CACHE_DIR_BENCH="${UV_CACHE_DIR:-$TMP_DIR/uv-cache}"

cat > "$BENCH_QUERY" <<'EOF'
extern fn read_csv(path: String, nulls: String, delimiter: String, has_header: Bool, schema: String) -> DataFrame from "csv.hpp";

let measurements = read_csv("__INPUT__", "", ";", false, "cat,f64")
    [select { station = col1, temp = col2 }];
let summary = measurements[select {
    min_temp = min(temp),
    avg_temp = mean(temp),
    max_temp = max(temp)
}, by station, order station];
summary;
EOF

python3 - <<'PY' "$BENCH_QUERY" "$INPUT"
from pathlib import Path
import sys
path = Path(sys.argv[1])
input_path = sys.argv[2].replace("\\", "\\\\").replace('"', '\\"')
path.write_text(path.read_text().replace("__INPUT__", input_path))
PY

printf ':load %s\n:q\n' "$BENCH_QUERY" > "$REPL_INPUT"

cat > "$INTERPRETED_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
for ((j = 0; j < $INNER_RUNS; ++j)); do
    "$IBEX_REPL" --plugin-path "$IBEX_PLUGIN_DIR" < "$REPL_INPUT" >/dev/null
done
EOF
chmod +x "$INTERPRETED_RUNNER"

cat > "$INTERPRETED_MEMORY_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
"$IBEX_REPL" --plugin-path "$IBEX_PLUGIN_DIR" < "$REPL_INPUT" >/dev/null
EOF
chmod +x "$INTERPRETED_MEMORY_RUNNER"

detect_cxx_std_flag() {
    local candidate
    for candidate in c++23 gnu++23 c++2b gnu++2b; do
        if printf 'int main() { return 0; }\n' | "$CXX" -x c++ -std="$candidate" - -o /dev/null \
            >/dev/null 2>&1; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    echo "error: unable to find a supported C++23-or-newer standard flag for $CXX" >&2
    exit 1
}

if [[ "$SKIP_COMPILED" -eq 0 ]]; then
    echo "=== compiling transpiled 1BRC binary ===" >&2
    CPP_FILE="$TMP_DIR/onebrc.cpp"

    IBEX_INCS=(
        "-I$IBEX_ROOT/include"
        "-I$BUILD_DIR/_deps/fmt-src/include"
        "-I$BUILD_DIR/_deps/fast_float-src/include"
        "-I$BUILD_DIR/_deps/spdlog-src/include"
        "-I$BUILD_DIR/_deps/robin_hood-src/src/include"
    )
    if [[ -d "$IBEX_ROOT/libs" ]]; then
        while IFS= read -r -d '' lib_dir; do
            IBEX_INCS+=("-I$lib_dir")
        done < <(find "$IBEX_ROOT/libs" -mindepth 1 -maxdepth 1 -type d -print0)
    fi

    FMT_LIB="$BUILD_DIR/_deps/fmt-build/libfmt.a"
    [[ -f "$FMT_LIB" ]] || FMT_LIB="$BUILD_DIR/_deps/fmt-build/libfmtd.a"
    SPDLOG_LIB="$BUILD_DIR/_deps/spdlog-build/libspdlog.a"
    [[ -f "$SPDLOG_LIB" ]] || SPDLOG_LIB="$BUILD_DIR/_deps/spdlog-build/libspdlogd.a"
    JEMALLOC_LIB=""
    for candidate in \
        "$(ldconfig -p 2>/dev/null | awk '/libjemalloc\.so\.2/{print $NF; exit}')" \
        /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 \
        /usr/lib/aarch64-linux-gnu/libjemalloc.so.2 \
        /usr/lib/libjemalloc.so.2 \
        /usr/local/lib/libjemalloc.so.2; do
        if [[ -n "$candidate" && -f "$candidate" ]]; then
            JEMALLOC_LIB="$candidate"
            break
        fi
    done

    "$IBEX_COMPILE" --no-print "$BENCH_QUERY" -o "$CPP_FILE" >&2
    "$CXX" $CXXFLAGS "${IBEX_INCS[@]}" "$CPP_FILE" \
        "$BUILD_DIR/src/runtime/libibex_runtime.a" \
        "$BUILD_DIR/src/ir/libibex_ir.a" \
        "$BUILD_DIR/src/core/libibex_core.a" \
        "$FMT_LIB" \
        "$SPDLOG_LIB" \
        ${JEMALLOC_LIB:+$JEMALLOC_LIB} \
        -o "$COMPILED_BIN"
fi

cat > "$COMPILED_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$IBEX_ROOT"
for ((j = 0; j < $INNER_RUNS; ++j)); do
    "$COMPILED_BIN" >/dev/null 2>/dev/null
done
EOF
chmod +x "$COMPILED_RUNNER"

cat > "$COMPILED_MEMORY_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$IBEX_ROOT"
"$COMPILED_BIN" >/dev/null 2>/dev/null
EOF
chmod +x "$COMPILED_MEMORY_RUNNER"

cat > "$POLARS_SCRIPT" <<'EOF'
from pathlib import Path
import sys

import polars as pl


def run_once(path: Path) -> int:
    df = pl.read_csv(
        path,
        separator=";",
        has_header=False,
        new_columns=["station", "temp"],
        schema_overrides={"station": pl.String, "temp": pl.Float64},
    )
    summary = (
        df.group_by("station")
        .agg(
            pl.col("temp").min().alias("min_temp"),
            pl.col("temp").mean().alias("avg_temp"),
            pl.col("temp").max().alias("max_temp"),
        )
        .sort("station")
    )
    return summary.height


def main() -> int:
    path = Path(sys.argv[1])
    inner_runs = int(sys.argv[2])
    rows = 0
    for _ in range(inner_runs):
        rows = run_once(path)
    print(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
EOF

cat > "$POLARS_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$IBEX_ROOT"
export UV_CACHE_DIR="$UV_CACHE_DIR_BENCH"
uv run --project "$IBEX_ROOT" --frozen python3 "$POLARS_SCRIPT" "$INPUT" "$INNER_RUNS" >/dev/null
EOF
chmod +x "$POLARS_RUNNER"

cat > "$POLARS_MEMORY_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$IBEX_ROOT"
export UV_CACHE_DIR="$UV_CACHE_DIR_BENCH"
uv run --project "$IBEX_ROOT" --frozen python3 "$POLARS_SCRIPT" "$INPUT" 1 >/dev/null
EOF
chmod +x "$POLARS_MEMORY_RUNNER"

measure_runner() {
    local timed_runner="$1"
    local memory_runner="$2"
    local out_file="$3"

    : > "$out_file"
    for ((i = 0; i < WARMUP; ++i)); do
        "$timed_runner" >/dev/null 2>/dev/null
        "$memory_runner" >/dev/null 2>/dev/null
    done
    for ((i = 0; i < ITERS; ++i)); do
        local time_file="$TMP_DIR/time.txt"
        local rss_file="$TMP_DIR/rss.txt"
        /usr/bin/time -f '%e' -o "$time_file" "$timed_runner" >/dev/null 2>/dev/null
        /usr/bin/time -f '%M' -o "$rss_file" "$memory_runner" >/dev/null 2>/dev/null
        printf '%s\t%s\n' "$(cat "$time_file")" "$(cat "$rss_file")" >> "$out_file"
    done
}

run_interpreted() {
    measure_runner "$INTERPRETED_RUNNER" "$INTERPRETED_MEMORY_RUNNER" "$INTERPRETED_TIMES"
}

run_compiled() {
    measure_runner "$COMPILED_RUNNER" "$COMPILED_MEMORY_RUNNER" "$COMPILED_TIMES"
}

run_polars() {
    if ! command -v uv >/dev/null 2>&1; then
        return 1
    fi
    measure_runner "$POLARS_RUNNER" "$POLARS_MEMORY_RUNNER" "$POLARS_TIMES"
}

summarize_file() {
    local framework="$1"
    local path="$2"
    awk -v fw="$framework" -v rows="$INPUT_ROWS" -v inner_runs="$INNER_RUNS" '
        BEGIN {
            sum_ms = 0.0;
            n = 0;
            min_ms = -1.0;
            max_ms = 0.0;
            sum_rss = 0.0;
            max_rss = 0.0;
        }
        NF {
            ms = ($1 + 0.0) * 1000.0 / inner_runs;
            rss = $2 + 0.0;
            sum_ms += ms;
            sum_rss += rss;
            ++n;
            if (min_ms < 0.0 || ms < min_ms) {
                min_ms = ms;
            }
            if (ms > max_ms) {
                max_ms = ms;
            }
            if (rss > max_rss) {
                max_rss = rss;
            }
        }
        END {
            if (n == 0) {
                exit 1;
            }
            printf "%s\t%.3f\t%.3f\t%.3f\t%.0f\t%.0f\t%d\t%d\n",
                   fw,
                   sum_ms / n,
                   min_ms,
                   max_ms,
                   sum_rss / n,
                   max_rss,
                   n,
                   rows;
        }
    ' "$path"
}

RESULTS="$(
{
    printf "framework\tavg_ms\tmin_ms\tmax_ms\tavg_maxrss_kb\tmax_maxrss_kb\titers\trows\n"
    if [[ "$SKIP_INTERPRETED" -eq 0 ]]; then
        echo "=== timing interpreted 1BRC query ===" >&2
        run_interpreted
        summarize_file "ibex-interpreted" "$INTERPRETED_TIMES"
    fi
    if [[ "$SKIP_COMPILED" -eq 0 ]]; then
        echo "=== timing compiled 1BRC query ===" >&2
        if run_compiled; then
            summarize_file "ibex-compiled" "$COMPILED_TIMES"
        else
            echo "warning: skipping compiled 1BRC timing; current transpiled runtime aborts on this string-key aggregation query" >&2
        fi
    fi
    if [[ "$SKIP_POLARS" -eq 0 ]]; then
        echo "=== timing polars 1BRC query ===" >&2
        if run_polars; then
            summarize_file "polars" "$POLARS_TIMES"
        else
            echo "warning: skipping polars 1BRC timing; uv/polars benchmark run failed" >&2
        fi
    fi
}
)"

printf '%s\n' "$RESULTS" > "$OUT"
if command -v column >/dev/null 2>&1; then
    printf '%s\n' "$RESULTS" | column -t -s $'\t'
else
    printf '%s\n' "$RESULTS"
fi

echo "results written to $OUT" >&2
