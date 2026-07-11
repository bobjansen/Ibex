#!/usr/bin/env bash
# compare_ibex_compilers.sh — compare generated Ibex C++ with two C++ compilers.
#
# Builds/times the same compiled Ibex queries with Clang and GCC, reducing
# repeated samples by median and reporting per-query deltas. The Ibex compiler
# itself is expected to be built already; this script only controls the C++
# compiler used for the generated query translation units.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: compare_ibex_compilers.sh [options]

Options:
  --build-dir <dir>      Ibex build dir with tools/ibex_compile (default: build-release)
  --clang-cxx <path>     Clang++ executable (default: clang++)
  --gcc-cxx <path>       G++ executable (default: g++)
  --clang-flags <flags>  Override generated-query Clang flags
  --gcc-flags <flags>    Override generated-query GCC flags
  --warmup <N>           Warmup iterations inside each compiled binary (default: 1)
  --iters <N>            Timed iterations inside each compiled binary (default: 7)
  --repeats <N>          Repeats per compiler; report median (default: 3)
  --interleave           Alternate clang/gcc repeats (default: off)
  --taskset <cpuset>     Pin benchmark runs with taskset -c
  --csv <path>           prices.csv path
  --csv-multi <path>     prices_multi.csv path
  --csv-trades <path>    trades.csv path
  --csv-events <path>    events.csv path
  --out <path>           Write full report text here as well as stdout
  --keep-temp            Keep temporary repeat TSVs/logs
  -h, --help             Show this help
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

BUILD_DIR="$REPO_ROOT/build-release"
CLANG_CXX="${CLANG_CXX:-clang++}"
GCC_CXX="${GCC_CXX:-g++}"
CLANG_FLAGS="${CLANG_FLAGS:-}"
GCC_FLAGS="${GCC_FLAGS:-}"
WARMUP=1
ITERS=7
REPEATS=3
INTERLEAVE=0
TASKSET_CPUSET=""
OUT=""
KEEP_TEMP=0

CSV="$REPO_ROOT/benchmarking/data/prices.csv"
CSV_MULTI="$REPO_ROOT/benchmarking/data/prices_multi.csv"
CSV_TRADES="$REPO_ROOT/benchmarking/data/trades.csv"
CSV_EVENTS="$REPO_ROOT/benchmarking/data/events.csv"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build-dir) BUILD_DIR="$2"; shift 2 ;;
        --clang-cxx) CLANG_CXX="$2"; shift 2 ;;
        --gcc-cxx) GCC_CXX="$2"; shift 2 ;;
        --clang-flags) CLANG_FLAGS="$2"; shift 2 ;;
        --gcc-flags) GCC_FLAGS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --repeats) REPEATS="$2"; shift 2 ;;
        --interleave) INTERLEAVE=1; shift ;;
        --taskset) TASKSET_CPUSET="$2"; shift 2 ;;
        --csv) CSV="$2"; shift 2 ;;
        --csv-multi) CSV_MULTI="$2"; shift 2 ;;
        --csv-trades) CSV_TRADES="$2"; shift 2 ;;
        --csv-events) CSV_EVENTS="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --keep-temp) KEEP_TEMP=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "error: unknown option: $1" >&2; usage; exit 1 ;;
    esac
done

[[ "$WARMUP" =~ ^[0-9]+$ ]] || { echo "error: --warmup must be a non-negative integer" >&2; exit 1; }
[[ "$ITERS" =~ ^[1-9][0-9]*$ ]] || { echo "error: --iters must be a positive integer" >&2; exit 1; }
[[ "$REPEATS" =~ ^[1-9][0-9]*$ ]] || { echo "error: --repeats must be a positive integer" >&2; exit 1; }
[[ -x "$BUILD_DIR/tools/ibex_compile" ]] || { echo "error: ibex_compile not found at $BUILD_DIR/tools/ibex_compile" >&2; exit 1; }
[[ -f "$CSV" ]] || { echo "error: CSV not found: $CSV" >&2; exit 1; }
command -v "$CLANG_CXX" >/dev/null 2>&1 || { echo "error: clang compiler not found: $CLANG_CXX" >&2; exit 1; }
command -v "$GCC_CXX" >/dev/null 2>&1 || { echo "error: gcc compiler not found: $GCC_CXX" >&2; exit 1; }
if [[ -n "$TASKSET_CPUSET" ]] && ! command -v taskset >/dev/null 2>&1; then
    echo "error: taskset not found but --taskset was provided" >&2
    exit 1
fi

TMP_ROOT="$(mktemp -d "${IBEX_COMPILER_CMP_TMPDIR:-/tmp}/ibex-compiler-cmp.XXXXXX")"
cleanup() {
    if [[ "$KEEP_TEMP" -eq 0 ]]; then
        rm -rf "$TMP_ROOT"
    fi
}
trap cleanup EXIT

default_clang_flags() {
    local cxx="$1"
    if printf 'int main(){return 0;}\n' \
        | "$cxx" -O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native -flto=thin -fuse-ld=lld \
            -x c++ - -o "$TMP_ROOT/clang_lto_test" >/dev/null 2>&1; then
        echo "-O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native -flto=thin -fuse-ld=lld"
    else
        echo "-O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native"
    fi
    rm -f "$TMP_ROOT/clang_lto_test"
}

default_gcc_flags() {
    local cxx="$1"
    if printf 'int main(){return 0;}\n' \
        | "$cxx" -O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native -flto=auto \
            -x c++ - -o "$TMP_ROOT/gcc_lto_test" >/dev/null 2>&1; then
        echo "-O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native -flto=auto"
    else
        echo "-O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native"
    fi
    rm -f "$TMP_ROOT/gcc_lto_test"
}

[[ -n "$CLANG_FLAGS" ]] || CLANG_FLAGS="$(default_clang_flags "$CLANG_CXX")"
[[ -n "$GCC_FLAGS" ]] || GCC_FLAGS="$(default_gcc_flags "$GCC_CXX")"

declare -a PIN_PREFIX=()
PIN_DESC="<none>"
if [[ -n "$TASKSET_CPUSET" ]]; then
    PIN_PREFIX=(taskset -c "$TASKSET_CPUSET")
    PIN_DESC="taskset -c $TASKSET_CPUSET"
fi

run_compiler_repeat() {
    local side="$1" cxx="$2" flags="$3" repeat="$4"
    local out_tsv="$TMP_ROOT/${side}.repeat${repeat}.tsv"
    local log_file="$TMP_ROOT/${side}.repeat${repeat}.log"
    local -a args=(
        --csv "$CSV"
        --warmup "$WARMUP"
        --iters "$ITERS"
        --out "$out_tsv"
    )
    [[ -f "$CSV_MULTI" ]] && args+=(--csv-multi "$CSV_MULTI")
    [[ -f "$CSV_TRADES" ]] && args+=(--csv-trades "$CSV_TRADES")
    [[ -f "$CSV_EVENTS" ]] && args+=(--csv-events "$CSV_EVENTS")

    echo "  -> $side repeat $repeat/$REPEATS" >&2
    if ! env IBEX_ROOT="$REPO_ROOT" BUILD_DIR="$BUILD_DIR" CXX="$cxx" CXXFLAGS="$flags" \
        "${PIN_PREFIX[@]}" bash "$REPO_ROOT/benchmarking/bench_ibex_compiled.sh" "${args[@]}" \
        >"$log_file" 2>&1; then
        cat "$log_file" >&2
        return 1
    fi
}

aggregate_median() {
    local out_tsv="$1"
    shift
    local unsorted_tsv="$TMP_ROOT/aggregate.$(basename "$out_tsv").unsorted"
    {
        for f in "$@"; do
            tail -n +2 "$f" | awk -F'\t' 'BEGIN { OFS="\t" } { print $2, $3, $9 }'
        done
    } | sort -t$'\t' -k1,1 -k2,2g | awk -F'\t' '
        function quantile(key, p,    m, pos, lo, frac) {
            m = n[key]
            if (m <= 1) return vals[key, 1]
            pos = 1 + (m - 1) * p
            lo = int(pos)
            frac = pos - lo
            if (lo >= m) return vals[key, m]
            return vals[key, lo] + frac * (vals[key, lo + 1] - vals[key, lo])
        }
        {
            key = $1
            vals[key, ++n[key]] = $2 + 0.0
            rows[key] = $3
        }
        END {
            OFS = "\t"
            print "query", "median_ms", "iqr_ms", "rows", "samples"
            for (k in n) {
                med = quantile(k, 0.5)
                iqr = quantile(k, 0.75) - quantile(k, 0.25)
                printf "%s\t%.4f\t%.4f\t%s\t%d\n", k, med, iqr, rows[k], n[k]
            }
        }
    ' > "$unsorted_tsv"
    {
        head -n 1 "$unsorted_tsv"
        tail -n +2 "$unsorted_tsv" | sort -t$'\t' -k1,1
    } > "$out_tsv"
}

aggregate_side() {
    local side="$1" out_tsv="$2"
    local -a repeats=()
    local r
    for ((r = 1; r <= REPEATS; ++r)); do
        repeats+=("$TMP_ROOT/${side}.repeat${r}.tsv")
    done
    aggregate_median "$out_tsv" "${repeats[@]}"
}

render_report() {
    local report_tsv="$1" summary_tsv="$2"
    local unsorted_report="$TMP_ROOT/report.unsorted.tsv"

    awk -F'\t' -v noise_k="${NOISE_K:-1.5}" '
        NR == FNR {
            if (FNR == 1) next
            clang[$1] = $2 + 0.0
            clang_iqr[$1] = $3 + 0.0
            rows[$1] = $4
            clang_n[$1] = $5 + 0
            next
        }
        FNR == 1 { next }
        {
            q = $1
            gcc[q] = $2 + 0.0
            gcc_iqr[q] = $3 + 0.0
            gcc_n[q] = $5 + 0
        }
        END {
            OFS = "\t"
            print "query", "clang_ms", "gcc_ms", "gcc_delta_ms", "gcc_delta_pct", "gcc_vs_clang_x", "clang_iqr_ms", "gcc_iqr_ms", "verdict", "clang_samples", "gcc_samples", "rows"
            for (q in clang) {
                if (!(q in gcc)) continue
                c = clang[q]
                g = gcc[q]
                d = g - c
                pct = (c != 0.0) ? (100.0 * d / c) : 0.0
                ratio = (g != 0.0) ? (c / g) : 0.0
                ci = (q in clang_iqr) ? clang_iqr[q] : 0.0
                gi = (q in gcc_iqr) ? gcc_iqr[q] : 0.0
                noise = ci; if (gi > noise) noise = gi
                ad = d; if (ad < 0) ad = -ad
                if (ad > noise_k * noise) {
                    verdict = (d > 0) ? "clang-faster" : "gcc-faster"
                } else {
                    verdict = "noise"
                }
                print q, sprintf("%.4f", c), sprintf("%.4f", g), sprintf("%+.4f", d), sprintf("%+.2f%%", pct), sprintf("%.3f", ratio), sprintf("%.4f", ci), sprintf("%.4f", gi), verdict, clang_n[q], gcc_n[q], rows[q]
            }
        }
    ' "$TMP_ROOT/clang.tsv" "$TMP_ROOT/gcc.tsv" > "$unsorted_report"
    {
        head -n 1 "$unsorted_report"
        tail -n +2 "$unsorted_report" | sort -t$'\t' -k1,1
    } > "$report_tsv"

    awk -F'\t' '
        function insert_sorted(kind, value,    i) {
            i = ++cnt[kind]
            while (i > 1 && vals[kind, i - 1] > value) {
                vals[kind, i] = vals[kind, i - 1]
                --i
            }
            vals[kind, i] = value
        }
        function median_of(kind,    m) {
            m = cnt[kind]
            if (m == 0) return 0.0
            if (m % 2 == 1) return vals[kind, (m + 1) / 2]
            return (vals[kind, m / 2] + vals[kind, (m / 2) + 1]) / 2.0
        }
        FNR == 1 { next }
        {
            c = $2 + 0.0
            g = $3 + 0.0
            d = g - c
            clang_sum += c
            gcc_sum += g
            n += 1
            insert_sorted("clang", c)
            insert_sorted("gcc", g)
            insert_sorted("delta", d)
            if (c > 0.0) insert_sorted("pct", 100.0 * d / c)
            if (c > 0.0 && g > 0.0) {
                ratio = c / g
                log_sum += log(ratio)
                log_n += 1
                insert_sorted("ratio", ratio)
            }
        }
        END {
            OFS = "\t"
            d = gcc_sum - clang_sum
            pct = (clang_sum != 0.0) ? 100.0 * d / clang_sum : 0.0
            geom = (log_n > 0) ? exp(log_sum / log_n) : 0.0
            print "queries", "total_clang_ms", "total_gcc_ms", "total_delta_ms", "total_delta_pct", "median_clang_ms", "median_gcc_ms", "median_delta_ms", "median_delta_pct", "median_gcc_vs_clang_x", "geom_gcc_vs_clang_x", "geom_samples"
            print n, sprintf("%.4f", clang_sum), sprintf("%.4f", gcc_sum), sprintf("%+.4f", d), sprintf("%+.2f%%", pct), sprintf("%.4f", median_of("clang")), sprintf("%.4f", median_of("gcc")), sprintf("%+.4f", median_of("delta")), sprintf("%+.2f%%", median_of("pct")), sprintf("%.3f", median_of("ratio")), sprintf("%.3f", geom), log_n
        }
    ' "$report_tsv" > "$summary_tsv"
}

CLANG_VER="$("$CLANG_CXX" --version | head -1)"
GCC_VER="$("$GCC_CXX" --version | head -1)"

echo "Benchmarking generated Ibex C++ compilers" >&2
echo "Clang: $CLANG_CXX ($CLANG_VER)" >&2
echo "GCC  : $GCC_CXX ($GCC_VER)" >&2
echo "Clang flags: $CLANG_FLAGS" >&2
echo "GCC flags  : $GCC_FLAGS" >&2
echo "Using build dir: $BUILD_DIR" >&2
echo "Using warmup=$WARMUP, iters=$ITERS, repeats=$REPEATS, interleave=$([[ "$INTERLEAVE" -eq 1 ]] && echo on || echo off)" >&2
echo "Pinning: $PIN_DESC" >&2

if [[ "$INTERLEAVE" -eq 1 ]]; then
    for ((r = 1; r <= REPEATS; ++r)); do
        run_compiler_repeat clang "$CLANG_CXX" "$CLANG_FLAGS" "$r"
        run_compiler_repeat gcc "$GCC_CXX" "$GCC_FLAGS" "$r"
    done
else
    for ((r = 1; r <= REPEATS; ++r)); do
        run_compiler_repeat clang "$CLANG_CXX" "$CLANG_FLAGS" "$r"
    done
    for ((r = 1; r <= REPEATS; ++r)); do
        run_compiler_repeat gcc "$GCC_CXX" "$GCC_FLAGS" "$r"
    done
fi

aggregate_side clang "$TMP_ROOT/clang.tsv"
aggregate_side gcc "$TMP_ROOT/gcc.tsv"

REPORT_TSV="$TMP_ROOT/report.tsv"
SUMMARY_TSV="$TMP_ROOT/summary.tsv"
render_report "$REPORT_TSV" "$SUMMARY_TSV"

REPORT_TEXT="$TMP_ROOT/report.txt"
{
    printf 'clang=%s\n' "$CLANG_VER"
    printf 'gcc=%s\n' "$GCC_VER"
    printf 'clang_cxx=%s\n' "$CLANG_CXX"
    printf 'gcc_cxx=%s\n' "$GCC_CXX"
    printf 'clang_flags=%s\n' "$CLANG_FLAGS"
    printf 'gcc_flags=%s\n' "$GCC_FLAGS"
    printf 'build_dir=%s\n' "$BUILD_DIR"
    printf 'warmup=%s\niters=%s\nrepeats=%s\ninterleave=%s\npinning=%s\n\n' \
        "$WARMUP" "$ITERS" "$REPEATS" "$INTERLEAVE" "$PIN_DESC"
    if command -v column >/dev/null 2>&1; then
        column -t -s $'\t' "$REPORT_TSV"
    else
        cat "$REPORT_TSV"
    fi
    echo
    echo "Summary (GCC relative to Clang; ratio >1 means GCC faster):"
    if command -v column >/dev/null 2>&1; then
        column -t -s $'\t' "$SUMMARY_TSV"
    else
        cat "$SUMMARY_TSV"
    fi
    if [[ "$KEEP_TEMP" -eq 1 ]]; then
        echo
        echo "Kept temporary directory: $TMP_ROOT"
    fi
} | tee "$REPORT_TEXT"

if [[ -n "$OUT" ]]; then
    mkdir -p "$(dirname "$OUT")"
    cp "$REPORT_TEXT" "$OUT"
fi
