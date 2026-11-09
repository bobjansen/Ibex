# Preserve physical Int32 in runtime filter results

The main `python` worktree contains the native Int32 implementation and the
physical-pointer gather fix (merge c68a8288). Compact ingestion remains opt-in;
this follow-up preserves the physical width of columns that are already compact.

Table and native-chunk filters now use shared helpers to allocate an Int32
output for an Int32 source and copy its selected physical values directly.
Projection, limits, empty results and disjoint parallel output windows use the
same rule. Existing Int64 inputs stay wide, even if their values fit Int32.
Metadata and validity retain their established handling. Arithmetic still reads
logical Int64 values and aggregate results remain wide. General-purpose
IntegerView and Column::filter are unchanged; this change covers runtime filter
materialization.

Width decisions occur once per column during allocation and copying, outside
all selected-row loops. Compact copies reuse the existing Int32 gather kernel;
they do not widen and narrow payloads or add a per-row width branch.

## Correctness gates

Before implementation, all three new tests fail specifically because compact
payloads become wide. After implementation they pass 3,727 assertions. The
cases cover owned and borrowed/sliced Int32 buffers, Int32 extrema, null payloads,
nonzero input ranges, empty mask words, projection, limits, empty output, native
chunk transport identity, and concurrent windows sharing a validity word.
They also verify preservation of both small and large Int64 values.

Both compact ingestion modes pass 2,488 fast CTest cases. Four network cases
are skipped, as are two opt-in Parquet cases when compact ingestion is disabled.
All 96 Python tests pass with compact ingestion off/on. Strict GCC runtime and
Arrow interop builds pass with `-Werror -Wpedantic -Wconversion -Wshadow`.

## Results

Paired samples (`paired.json`, local raw output) compare the same compact inputs before and after
output-width preservation. Improvements below are median paired ratios;
standalone stage times are not substituted for whole-query times.

| Rows | Selected | Cores | Before ms | After ms | Improvement | Holm p |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 10% | 1 | 1.517 | 1.437 | 4.68% | 0.00499 |
| 1,000,000 | 50% | 1 | 2.701 | 2.413 | 11.11% | 0.00071 |
| 10,000,000 | 10% | 1 | 15.650 | 14.923 | 5.09% | 0.00071 |
| 10,000,000 | 50% | 1 | 27.521 | 24.100 | 12.20% | 0.00071 |
| 1,000,000 | 10% | 8 | 0.387 | 0.364 | unresolved | 0.82276 |
| 1,000,000 | 50% | 8 | 1.053 | 0.931 | 11.45% | 0.00155 |
| 10,000,000 | 10% | 8 | 3.893 | 3.604 | 7.96% | 0.00071 |
| 10,000,000 | 50% | 8 | 9.519 | 8.300 | 13.31% | 0.00071 |

Seven of eight cases resolve in favor of compact output; the 1M-row, 10%
selection case at eight cores is unresolved (paired ratio 1.0070). All unchanged
replica comparisons remain unresolved after Holm correction (minimum p=0.2007).
Wide-input before/after comparisons in this sweep also remain unresolved.

At 10M rows and 50% selection, output value buffers fall from 200 MB to 120 MB
(40%). Four columns remain physical Int32; the row identifier stays Int64.
Validity storage is excluded from these byte totals and is unchanged. Median
paired CPU ratios are 0.877 at one core and 0.863 at eight cores: CPU time falls
along with wall time. This is a resident mixed-width filter workload, not a
claim of a universal filter or ingestion speedup.

The 23-query wide regression slice (`wide-regression.json`, local raw output) has no resolved
candidate/base effects after Holm correction. Summed median times move by
−0.03%; this is descriptive, not proof of equivalence or a statistically tested
suite speedup. The earlier broader integration gate remains open separately.

Operator profiles (`operator-summary.json`, local raw output) on the 10M/50% compact case show
lower filter next time at both budgets and unchanged scan/filter row, call and
task counts within each budget. Profiles use six standalone runs (first two
excluded), not the paired performance samples. Serial and parallel accounting
have different row-count conventions, so those counters are not used to infer
cross-budget work multiplication. [Assembly](assembly.md) confirms six
instructions per selected value at both physical widths; compact payloads now
use 32-bit loads and stores. Cross-build code layout can contribute to precise
wall-time deltas; the byte reduction and preserved output widths are directly
checked.

Typed CSV answer hashes (`answer-hashes.json`, local raw output) match the pre-filter implementation
at one/eight cores. Six Parquet queries (`parquet-correctness.json`, local raw output), including
selection and wide arithmetic, are byte-identical to the pre-filter native
worktree at both core budgets with compact ingestion off/on. A separately built
benchmark mutant corrupts the first selected row identifier: full checking
rejects it for both input widths (exit 5). No mutation is applied to production
sources. Conditions and binary/object hashes (`conditions.json`, local raw output) identify the
measured source and the identical benchmark object linked into both sides.

## Measurement design and reproduction

`filter_query.cpp` constructs resident tables, parses `t[filter k < threshold]`,
and times the normal interpreter/pipeline query. It checks every output value,
row order and validity bit after timing; result destruction is also untimed.
The table has an Int64 row identifier and four integer columns whose physical
width varies, including the predicate key and a nullable payload. Input values
and query answers agree across widths. No CSV/Parquet loading or export is timed.

`build.py` compiles one benchmark object and links that exact object against
two built revisions' parser/runtime/IR/core libraries. The public column layout
is identical in these revisions. `run.py` interleaves baseline/candidate compact,
an unchanged compact replica, and baseline/candidate wide processes. It rotates
and reverses five-side order, warms each side twice, and retains 20 paired
samples, CPU times, binary hashes and output physical widths. Full output checks
run on every sample. The sweep uses 1M/10M rows, 10%/50% selection, and one/eight
P-cores. Holm correction is applied across each eight-case comparison family.

The wide-workload git regression slice uses the established harness first:

```bash
mkdir -p build-filter-storage/perfcmp
IBEX_CORES=1 IBEX_COMPACT_INT32=0 IBEX_PERFCMP_JOBS=6 \
IBEX_PERFCMP_TMPDIR="$PWD/build-filter-storage/perfcmp" \
benchmarking/compare_ibex_git.sh --base dff7f963 --target WORKTREE \
  --ibex-suite core,filter,join,groupagg \
  --csv "$PWD/benchmarking/data/prices.csv" \
  --csv-multi "$PWD/benchmarking/data/prices_multi.csv" \
  --csv-trades "$PWD/benchmarking/data/trades.csv" \
  --csv-events "$PWD/benchmarking/data/events.csv" \
  --csv-lookup "$PWD/benchmarking/data/lookup.csv" \
  --csv-users "$PWD/benchmarking/data/users.csv" \
  --interleave --replica-control --repeats 20 --warmup 2 --iters 7 \
  --taskset 2 --keep-temp
```

Then, after all builds have finished and before running other work:

```bash
.venv/bin/python benchmarking/int32/filter-storage/build.py \
  --base-build /home/brj/.cache/ibex/perfcmp/base/build \
  --target-build /home/brj/.cache/ibex/perfcmp/target-worktree/build \
  --out build-filter-storage/bin
.venv/bin/python benchmarking/int32/filter-storage/run.py \
  --base build-filter-storage/bin/base --target build-filter-storage/bin/target \
  --out benchmarking/results/local/int32/filter-storage/paired.json
```

Raw git-comparison logs and TSVs, frozen benchmark binaries, full typed CSV
answers and test/build logs remain under the persistent ignored directories
`build-filter-storage/` and `build-release/`. Historical performance gates
against the original noncompact implementation remain separate from this
before/after filter comparison.
