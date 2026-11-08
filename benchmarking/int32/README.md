# Native Int32 storage experiment

Branch: `experiment/native-int32`, based on the `python` branch at d123a11f.
The original checkout's uncommitted Python work is preserved separately.

This experiment compiles the same C++ source twice with `Value=int32_t` and
`Value=int64_t`. It uses Ibex's existing `Column<T>` implementation, including
its actual `Column::filter`, and the robin_hood map dependency used by the
runtime. Scan and random gather are direct typed loops; group is a count-map
control, not the complete runtime aggregate operator. No Polars or Python data
conversion runs inside the measured workload. Python only orchestrates workers.

The interpreter still stores integer columns as Int64. This is evidence about
native storage/kernel potential, not a working Int32 interpreter build or an
end-to-end query speedup. Production sources are unchanged, so it does not yet
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
    benchmarking/int32/native_width.cpp -o /tmp/ibex-native-width$bits
done
cmake --build build-release -j6
taskset -c 2 .venv/bin/python benchmarking/int32/run.py --out /tmp/native-widths.json
```

The coordinator uses `benchmarking/ab_queries.py`'s Wilcoxon implementation.
The source is templated identically for both builds and needs no runtime
per-row width branches. Cross-type answers are compared after every sample.

## Next native experiment

Preserve compact integer payload storage through a native filter/join while
retaining existing Int64 logical/arithmetic semantics and key handling. Measure
actual query gains, null handling and promotion costs. Use the required git-state
A/B harness and Int64 regression suite once runtime code changes; the current
experiment changes no runtime hot path. Evaluate compact hash keys separately,
including real entry sizes and operator profiles, before increasing dispatch
coverage. A global Int32 replacement is not justified by these controls.
