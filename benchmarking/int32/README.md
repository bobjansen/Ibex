# Native Int32 storage experiment

Developed on `experiment/native-int32` from a329ac4e, then integrated into the
main `python` branch in d81edba3 alongside the earlier Python work.

This experiment compiles the same C++ source twice with `Value=int32_t` and
`Value=int64_t`. It uses Ibex's existing `Column<T>` implementation, including
its actual `Column::filter`, and the robin_hood map dependency used by the
runtime. Scan and random gather are direct typed loops; group is a count-map
control, not the complete runtime aggregate operator. No Polars or Python data
conversion runs inside the measured workload. Python only orchestrates workers.

For this original control, the interpreter stored integer columns as Int64. This is evidence about
native storage/kernel potential, not a working Int32 interpreter build or an
end-to-end query speedup. This control did not change production sources, so it does not
measure dispatch/code-size effects from integrating another runtime column type.

## Results

Pinned to one P-core at 3.2 GHz, Release optimization, two warmups, 12 paired
alternating samples. Input generation, reference checks and output destruction
are outside timers. Each sample checks every filtered/gathered output element
and each group count; width checksums must also agree. A corrupt-result mutant
was rejected by all four operation gates. Raw samples/provenance: `results.json`.

At 10M rows and 100K groups:

| Native control | Int64 ms | Int32 ms | Paired Int32/Int64 |
|---|---:|---:|---:|
| Scan + wide sum | 4.10 | 2.58 | 0.631 |
| Filter ~50% | 103.38 | 55.09 | 0.533 |
| Random gather | 92.46 | 64.21 | 0.691 |
| Hash count | 212.12 | 237.57 | 1.120 |

All p=0.00049 in this slice. The 1M/10M row by 1K/100K group sweep has the same
signs, with smaller scan/filter gains at 1M rows. Sum and counts stay 64-bit;
gather indices stay size_t, so this compares element/key width, not row-index
width. The native integer workload is bounded and does not test overflow,
nulls, mixed arithmetic or promotion semantics.

The hash result is a reason to separate key and payload width. Both map entry
pairs occupy 16 bytes (`pair<const Key,uint64_t>`), and hashes agree for all
one million checked nonnegative values. Thus narrower keys do not shrink the
entry layout in this control. The cause of its slowdown is not yet attributed:
code generation/layout is a possible contributor. This is not evidence that
all native group-bys would regress. Text sizes were 19,905 bytes (Int32) and
19,609 (Int64), for two separate binaries, not both variants in one runtime.

## Reproduce

Build both binaries, using the robin_hood headers from a configured Release tree:

```bash
export ROBIN_HOOD_INCLUDE="$PWD/build-release/_deps/robin_hood-src/src/include"
for bits in 32 64; do
  clang++ -O3 -DNDEBUG -std=c++23 -Wall -Wextra -Wpedantic -Wconversion -Wshadow \
    -Werror -I include -isystem "$ROBIN_HOOD_INCLUDE" -DVALUE_BITS=$bits \
    benchmarking/int32/native_width.cpp -o build-release/ibex-native-width$bits
done
cmake --build build-release -j6
taskset -c 2 .venv/bin/python benchmarking/int32/run.py --out benchmarking/results/local/int32/native-widths-rerun.json
```

The coordinator uses `benchmarking/ab_queries.py`'s Wilcoxon implementation.
The source is templated identically for both builds and needs no runtime
per-row width branches. Cross-type answers are compared after every sample.

## Native runtime prototype

Compact integer backing is implemented with Int64 logical/arithmetic
semantics, and since 2026-11-12 it is the only behaviour: the opt-in
`IBEX_COMPACT_INT32=1` switch, and with it the wide Int32 ingestion, is gone. The
guarantees are in [the runtime contracts](../../src/runtime/CONTRACTS.md)
(§8); the retired implementation plan, with its correctness checks and
historical gates, is `git show ed7fd554:plans/native-int32-storage.md`.
Scripts below that set `IBEX_COMPACT_INT32` predate that; the variable is now
ignored.

`lhs join rhs on id` with Int64 keys and three right-side integer payloads,
inputs built before timing, result materialization timed, two warmups and 12
alternating pairs (all p=0.00049):

| Rows | Threads | Wide ms | Compact ms | Paired compact/wide |
|---:|---:|---:|---:|---:|
| 1,000,000 | 1 | 14.33 | 11.08 | 0.777 |
| 10,000,000 | 1 | 292.61 | 253.67 | 0.867 |
| 1,000,000 | 8 | 6.88 | 5.92 | 0.863 |
| 10,000,000 | 8 | 104.64 | 97.77 | 0.933 |

The actual query control is `tools/bench_compact_int32.cpp`; its driver is:

```bash
cmake --build build-release -j6
.venv/bin/python benchmarking/int32/run_compact_join.py \
  --out benchmarking/results/local/int32/native-compact-query-rerun.json
```

It parses `lhs join rhs on id` and executes it through `runtime::interpret`,
with Int64 keys, shuffled left keys, three right payloads and complete output
association checks. It honors both one- and eight-thread compute budgets.
The earlier direct `join_tables` wrapper control was serial regardless of
`IBEX_CORES`; its timings are not used as parallel-query evidence.

`check_parquet.py` checks native file decoding/results across compact on/off,
one/eight threads and an optional original engine. `check_mutation.py` runs
ten selected compact test cases against a separately compiled corrupt producer.
`paired_report.py` analyzes retained git-comparison repeat files using paired
Wilcoxon tests and an unchanged control, rather than the shell harness's
IQR-based display label. Final raw sample arrays are checked in alongside
these scripts.

The ingestion follow-up preserves physical Int32 in the serial Parquet decoder,
including nullable/selected reads, and adopts owned Arrow Int32 buffers.
`check_ingestion.py --arrow-build <configured-release-build> --mutations`
rebuilds the matching Parquet plugin and runs physical-width/lifetime tests
against an existing Arrow dependency. It also demonstrates that restoring eager
serial widening makes the tests fail. See the plan for validation and remaining
storage migration work.

## Cached fallback removal

The column-owned Int64 cache is removed. Const scalar integer access and
`integer_view()` return logical Int64 values directly from physical Int32 or
Int64 storage. The iterator is random-access and not contiguous. Compact
columns reject Int64 pointer/span requests; `widened_int64()` explicitly
returns an independent owned conversion when an operation needs one. Native
Arrow export emits Int32 directly. See the plan for semantics, checks and the
remaining performance gate.

`check_read_memory.cpp` is an isolated memory mechanism check, not a timing
benchmark. The identical source was built at -O3 -DNDEBUG against the cached
a2cf5b10 column header and the new header, using the same other includes.
At 50M values the old read allocates about 381 MiB for its cached Int64 view;
the new read has no RSS growth. Both verify the same independent Int64
checksum. Raw results/provenance are in `no-cache-read-memory.json`.

To reproduce (paths are persistent; binaries and generated headers are scratch):

```bash
mkdir -p build-memory-baseline/include/ibex/core
git show a2cf5b10:include/ibex/core/column.hpp > build-memory-baseline/include/ibex/core/column.hpp
clang++ -O3 -DNDEBUG -std=c++23 -I build-memory-baseline/include -I include \
  -isystem "$ROBIN_HOOD_INCLUDE" benchmarking/int32/check_read_memory.cpp \
  -o build-memory-baseline/read-memory
clang++ -O3 -DNDEBUG -std=c++23 -I include -isystem "$ROBIN_HOOD_INCLUDE" \
  benchmarking/int32/check_read_memory.cpp -o build-release/read-memory
build-memory-baseline/read-memory 50000000
build-release/read-memory 50000000
```

Historical measurement identifiers map to the rebased source commits in
[history-revisions.md](history-revisions.md).

## End-to-end PDS-H check

The [SF-10 PDS-H run](pdsh-sf10/README.md) at `8b70b057` matches every Polars
answer. Against the previous native run (`5f43038d`, 2026-10-23), every query
is faster once q21's query-text change is accounted for. The geomean
Ibex/Polars 2 streaming is 0.888 at 8 cores and 0.844 at 1 core; against
DuckDB it is 0.734 and 0.774. The two points are about 290 commits apart,
so this shows no end-to-end regression on the branch, not gains attributable
to Int32 storage.

## Integer read paths

`6bdbc65b` made Int64 readers return an `IntegerView`, which tests the
physical width (Int64 or compact Int32) on every element. In tight loops that
test is not free, even with compact storage off. [integer_paths.py](integer_paths.py)
is the probe that found where: 24 queries over seeded 5M-row tables, each
aimed at one family of loops that read Int64 columns (filters, casts, math,
lag/cumsum/fills, grouped shifts, sorts and top-k, semi/anti/left/outer joins,
wide/pair/mixed/packed group keys, corr, melt).

At one core, `be3c9ef4` (before the view) against `68b5957f` showed a
column-vs-column filter `a > b` at 26 -> 258 ms (`cmp_into` stopped
vectorizing), `Float64(a)` +10.6%, the single-Int64-key left join +9.1% and
outer join +3.9%, `fill_forward`/`fill_backward` +6%/+7.5%, top-k on an Int64
sort key +6% and grouped `lag` +3.6%. After `781be0c7` every query is at or
below its `be3c9ef4` time (`a > b` 25.6 ms, left join -8.8%). The h2o joins
and groupby questions behind the same series are in the commit messages of
`f80dd2db` through `781be0c7`.

The fixes follow two patterns, both in `src/runtime/runtime_internal.hpp`:
`with_physical_values(column, body)` tests the width once and compiles a short
loop per width; `contiguous_values(column, scratch)` gives a long kernel a raw
`const T*`, the column's own buffer or, for compact storage, a copy widened
once. Wrapping a large inline switch in a generic lambda instead cost h2o q7
60% (`7cdc4ed3`; fixed out of line in `fd9e6668`).

```bash
.venv/bin/python benchmarking/int32/integer_paths.py gen   # -> benchmarking/data/int32_paths
# compact Int32 against Int64 storage, one build (the default inputs)
.venv/bin/python benchmarking/int32/integer_paths.py run --rounds 2
# two builds, on any of csv, parquet64, parquet
.venv/bin/python benchmarking/int32/integer_paths.py run --input csv \
    --ibex <build A>/tools/ibex --ibex <build B>/tools/ibex --rounds 2
```

`gen` writes the tables as CSV and twice as Parquet, with the integer columns
as Int64 (`t64.parquet`) and as Int32 (`t.parquet`, loaded as compact
storage). The Parquet pair differs only in width; the script binds both in
memory with a projection, since a lazy read would re-decode the file in every
filter. Compare builds made with the same flags (two Release trees, or two
`~/.cache/ibex/perfcmp` slots, which build with `-march=native` and have no
Parquet plugin, so CSV only), each with its plugins next to the binary. Runs
pin to one CPU with `IBEX_CORES=1` and, through `systemd-run`, a 16G memory
cap; the script times queries and does not check answers (the
`[compact-int32]` tests in `tests/test_integer_no_cache.cpp` do).

Compact against Int64 storage, one `build-release`, two rounds of five
(minimum, ms). "At flip" is the commit that made compact the default; the
element-wise losses it showed were fixed after it:

| Query | Int64 | Int32 | Int32 vs Int64 | At flip |
|---|---:|---:|---:|---:|
| `filter a > 500000` / `a > b` | 27.4 / 28.3 | 19.7 / 19.9 | -28% / -30% | -28% / -31% |
| `lag(a, 1)` | 2.3 | 1.4 | -39% | +21% |
| `order { a }` | 280.4 | 215.2 | -23% | -23% |
| semi / anti join on `k` | 37.5 / 37.6 | 29.5 / 29.6 | -21% | -20% |
| `cumsum(a)` | 3.9 | 3.1 | -21% | -23% |
| `Float64(a)` | 5.6 | 4.6 | -19% | -16% |
| fill forward / backward | 4.2 / 4.2 | 3.8 / 3.7 | -10% / -12% | +32% / +35% |
| `corr(a, b)` by `g` | 24.5 | 21.8 | -11% | -4% |
| `abs(a)` / `a * 3 + b` / `pmax(a, b)` | 3.6 / 5.9 / 4.6 | 3.4 / 5.6 / 4.5 | -7% / -6% / -3% | +27% / +34% / +43% |
| left / outer join on `k` | 307.5 / 328.5 | 308.1 / 311.6 | +0.2% / -5% | +0.1% / -5% |
| group-bys (4 shapes) | | | -0.9% to -2.8% | -1.4% to -2.9% |
| `melt` | 58.8 | 59.0 | +0.4% | +5% |
| top-k / top-k by `g` | 11.3 / 57.8 | 11.2 / 58.4 | -1% / +1.1% | +22% / +11% |
| grouped `lag` | 116.5 | 118.2 | +1.4% | +2.3% |

The fixes: the numeric-tree leaves (`IntColumn`, `lag`/`lead` nodes), the
single-shift copy, the fills and top-k's sort key read the physical Int32
values and widen as they store, instead of through `IntegerView` or a whole
column widened first; melt widens its measures a block at a time. Large
widening copies (`widen_int32`, also behind `contiguous_values`) stream
their stores past 2^20 values, as glibc's memmove does for the wide copy.
Streaming bypasses the cache, so a widened scratch that the kernel reads
straight back could lose where it would have fit in L3 (30 MB here). Checked
with `gen --rows 2000000` (16 MB per widened column) against a build that
never streams: `lag` +26% without streaming (+87% at 5M rows), every other
query within +-2%, joins and sorts included.
The fills became branch-free at both widths: Clang compiled the carry's
ternary as a branch on the null bit, which 10% scattered nulls mispredict,
so the Int64 fills also fell from 8.6 / 8.0 to 4.3 / 4.2 ms (perfcmp slots,
CSV input). Every other Int64 query stayed within noise of the flip commit.
