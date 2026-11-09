# Native Int32 storage experiment

Developed on `experiment/native-int32` from d123a11f, then integrated into the
main `python` branch in c68a8288 alongside the earlier Python work.

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

Compact integer backing is now implemented on this branch behind
`IBEX_COMPACT_INT32=1`, with Int64 logical/arithmetic semantics. See
[the implementation plan](../../plans/native-int32-storage.md) for guarantees,
correctness checks, actual interpreter-query measurements and the unresolved
existing-workload regression. It remains opt-in while the broader performance gate is open.

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
715bf960 column header and the new header, using the same other includes.
At 50M values the old read allocates about 381 MiB for its cached Int64 view;
the new read has no RSS growth. Both verify the same independent Int64
checksum. Raw results/provenance are in `no-cache-read-memory.json`.

To reproduce (paths are persistent; binaries and generated headers are scratch):

```bash
mkdir -p build-memory-baseline/include/ibex/core
git show 715bf960:include/ibex/core/column.hpp > build-memory-baseline/include/ibex/core/column.hpp
clang++ -O3 -DNDEBUG -std=c++23 -I build-memory-baseline/include -I include \
  -isystem "$ROBIN_HOOD_INCLUDE" benchmarking/int32/check_read_memory.cpp \
  -o build-memory-baseline/read-memory
clang++ -O3 -DNDEBUG -std=c++23 -I include -isystem "$ROBIN_HOOD_INCLUDE" \
  benchmarking/int32/check_read_memory.cpp -o build-release/read-memory
build-memory-baseline/read-memory 50000000
build-release/read-memory 50000000
```
