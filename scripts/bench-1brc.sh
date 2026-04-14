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
COMPILED_RUNNER="$TMP_DIR/compiled_runner.sh"
POLARS_SCRIPT="$TMP_DIR/onebrc_polars.py"
POLARS_RUNNER="$TMP_DIR/polars_runner.sh"
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

run_interpreted() {
    : > "$INTERPRETED_TIMES"
    for ((i = 0; i < WARMUP; ++i)); do
        for ((j = 0; j < INNER_RUNS; ++j)); do
            "$IBEX_REPL" --plugin-path "$IBEX_PLUGIN_DIR" < "$REPL_INPUT" >/dev/null
        done
    done
    for ((i = 0; i < ITERS; ++i)); do
        local tfile="$TMP_DIR/time.txt"
        /usr/bin/time -f '%e' -o "$tfile" bash -lc '
            for ((j = 0; j < '"$INNER_RUNS"'; ++j)); do
                "'"$IBEX_REPL"'" --plugin-path "'"$IBEX_PLUGIN_DIR"'" < "'"$REPL_INPUT"'" >/dev/null
            done
        '
        cat "$tfile" >> "$INTERPRETED_TIMES"
        printf '\n' >> "$INTERPRETED_TIMES"
    done
}

run_compiled() {
    : > "$COMPILED_TIMES"
    for ((i = 0; i < WARMUP; ++i)); do
        for ((j = 0; j < INNER_RUNS; ++j)); do
            if ! (cd "$IBEX_ROOT" && "$COMPILED_BIN" >/dev/null 2>/dev/null); then
                return 1
            fi
        done
    done
    for ((i = 0; i < ITERS; ++i)); do
        local tfile="$TMP_DIR/time.txt"
        if ! /usr/bin/time -f '%e' -o "$tfile" "$COMPILED_RUNNER" >/dev/null 2>/dev/null; then
            return 1
        fi
        cat "$tfile" >> "$COMPILED_TIMES"
        printf '\n' >> "$COMPILED_TIMES"
    done
}

run_polars() {
    : > "$POLARS_TIMES"
    if ! command -v uv >/dev/null 2>&1; then
        return 1
    fi
    for ((i = 0; i < WARMUP; ++i)); do
        if ! "$POLARS_RUNNER" >/dev/null 2>/dev/null; then
            return 1
        fi
    done
    for ((i = 0; i < ITERS; ++i)); do
        local tfile="$TMP_DIR/time.txt"
        if ! /usr/bin/time -f '%e' -o "$tfile" "$POLARS_RUNNER" >/dev/null 2>/dev/null; then
            return 1
        fi
        cat "$tfile" >> "$POLARS_TIMES"
        printf '\n' >> "$POLARS_TIMES"
    done
}

summarize_file() {
    local framework="$1"
    local path="$2"
    awk -v fw="$framework" -v rows="$INPUT_ROWS" -v inner_runs="$INNER_RUNS" '
        BEGIN {
            sum = 0.0;
            n = 0;
            min = -1.0;
            max = 0.0;
        }
        NF {
            v = $1 + 0.0;
            sum += v;
            ++n;
            if (min < 0.0 || v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
        }
        END {
            if (n == 0) {
                exit 1;
            }
            printf "%s\t%.3f\t%.3f\t%.3f\t%d\t%d\n",
                   fw,
                   sum * 1000.0 / (n * inner_runs),
                   min * 1000.0 / inner_runs,
                   max * 1000.0 / inner_runs,
                   n,
                   rows;
        }
    ' "$path"
}

{
    printf "framework\tavg_ms\tmin_ms\tmax_ms\titers\trows\n"
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
} | tee "$OUT"

echo "results written to $OUT" >&2
