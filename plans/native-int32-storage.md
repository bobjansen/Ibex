# Native compact Int32 storage

Status: integrated into the main `python` branch in c68a8288, including the
validated physical-pointer gather fix. Compact ingestion remains opt-in through
`IBEX_COMPACT_INT32=1`; the broader existing-workload performance gate remains
open. Integration does not enable compact ingestion by default.

## Design constraint update

There are no customers to migrate. Existing C++ pointer/reference APIs, Column
layouts, variant indices and plugin ABI are not compatibility requirements for
this work. Preserve language-level integer correctness and performance; change
internal callers and rebuild plugins when a cleaner representation requires it.
The hidden compact tag is still experimental, but the cached Int64 buffer has
now been removed. Kernels accept physical integer readers or request an
explicit owned widening at the operation that needs it. Keeping a second
representation merely to avoid updating internal callers is not a design goal.

## Scope and semantics

`IBEX_COMPACT_INT32=1` preserves signed Int32 Parquet and Arrow inputs in
compact backing. No data-dependent shrinking of Int64 values is performed;
UInt32 stays on the existing Int64 path. Float storage is unchanged.

Logical columns, scalar values, arithmetic and aggregate results remain Int64.
There is no new ColumnValue alternative and no Int32 arithmetic that can wrap
before promotion. Const scalar integer access returns Int64 by value. Logical
integer iterators / `integer_view()` read either physical width without
allocating and are random-access, not contiguous. `visit_integer_storage()`
allows a kernel to dispatch once over an actual typed span.

A compact column has no Int64 pointer to return: const `data()`, `span()` and
`buffer_data()` reject that request. An operation that requires contiguous
Int64 explicitly calls `widened_int64()` and owns the independent result.
This does not mutate or attach anything to the compact source. Mutable Int64
access promotes the destination's storage under copy-on-write.

Grouping, joins, numeric evaluation blocks, scalar/vector comparisons, order,
shifts, cumulative and rolling operators, null filling and reshape readers
have been changed to read physical integer inputs. The older contiguous
vector arithmetic fallback explicitly owns its widened operands for the call.
Compact payload gathers and concatenation retain physical width; nullable
batched gathers write the Int32 output directly, avoiding promotion of an
uninitialized output buffer. Display reads scalar values. Arrow export emits
physical Int32 buffers directly and rebases sliced validity when necessary.

Integer semantics are preserved for operations and intermediates representable
in Int64; this does not define a policy for results outside Int64 or introduce
128-bit accumulators. The historical measurements below predate cache removal.

`Column::reserve(n)` is representation-agnostic: it reserves logical element
slots in the current backing. `capacity()` reports those slots, not bytes.
Promotion can allocate but retains the reservation. Copy-on-write protects
shared source values. The materializer uses the ordinary
reserve API; there is no special compact-reserve call at that boundary.

The prototype reuses immutable-backing ownership and reserves the otherwise
invalid SIZE_MAX external offset as an internal tag. Column object layouts and
variant indices are unchanged, but compiled inline accessors have changed:
plugins must be rebuilt with matching headers before receiving compact columns.
Parquet decoding uses the schema's signed Int32 guarantee, including the
parallel decoder and selected-row path. No inference from absent statistics is
needed for this first slice.

## Cached fallback removal (2026-11-09)

The CompactIntBacking Int64 vector, once_flag and publication atomic are gone.
IntegerView is a non-owning logical reader / random-access value iterator over
Int32 or Int64. It returns Int64 values before arithmetic. It is deliberately
not a contiguous iterator and does not expose an Int64 pointer for compact
input. Ordinary mutable iterators still address owned wide storage; their
positions must compare correctly with logical iterators, including pointers
already advanced to an insertion/erase position. The full suite caught and
now covers a bug in that translation during chunk appends.

Validation: the final 2,471-case Release fast suite passes with compact
imports both off and on (four network cases skipped). Sixteen compact cases
pass at eight cores with 541 assertions; the one-core run passes 535 assertions
with the ranged-gather case skipped because its pool has one worker. Cases
compare ordinary and compact inputs across arithmetic, null filling, guarded
updates, shifts, cumulative operators, grouping, ordering, distinct, joins and
row binding, including a million-row slice. Source tables remain compact.
Ten selected cases reject a corrupt producer; the isolated serial Parquet
mutation is also rejected at one/eight cores. Strict GCC runtime/interop builds
and matching plugin rebuilds pass. Logs are in `build-release/no-cache-*.log`.

A memory mechanism check compiles the identical standalone
[check_read_memory.cpp](../benchmarking/int32/check_read_memory.cpp) against the
cached checkpoint (715bf960) and current column headers, at -O3 -DNDEBUG.
Fifty million signed Int32 values are constructed outside the read loop and
both binaries verify the complete Int64 checksum against an independent
formula. Peak RSS before/after logical reads is 198,696/589,224 KiB for the
cached checkpoint and 198,480/198,480 KiB after removal. That is about 381 MiB
of avoided persistent widening, not a throughput benchmark or a claim about
all query peak-memory costs. Raw results and hashes (`no-cache-read-memory.json`, local raw output).

The current Parquet query outputs remain byte-identical to the original engine
at one/eight cores, compact imports on/off:
output checks (`no-cache-parquet-correctness.json`, local raw output).
The operation-owned contiguous arithmetic conversion remains a cost to measure;
removing the column cache does not eliminate every operation's need for wide
output or scratch buffers.

The final Release comparison is against the cached checkpoint 715bf960, not
against the original Python branch. It uses 12 paired/interleaved repeats,
five inner iterations, one pinned core and a byte-identical unchanged control.
Across 23 existing queries, summed medians are 585.219/587.086 ms (+0.32%).
Paired filters are slower: AND +2.0%, OR +2.2%, simple +3.3%; median/quantile
also show smaller slowdowns (~0.9%). The paired std result is ~6.1% faster,
but its mechanism is not attributed and it does not independently clear the
older original-baseline regression. The filter effects each have p=0.00049,
Holm p=0.011 over 23 comparisons, with the control comparisons corroborating
the direction. This remains an experiment with an open performance gate.
Final paired samples/statistics (`no-cache-regression-paired.json`, local raw output).
Raw logs/repeats are retained in
`build-ingestion-check/results/ibex-perfcmp.dCti1h/`.

Next performance work should dispatch physical spans once at kernel entry
where the logical reader costs instructions in a row loop. Restoring an
implicit column-owned wide cache is not the intended fix.


## Ingestion follow-up (2026-11-09)

The first ingestion slice now preserves signed Int32 in the serial direct
Parquet decoder as well as the existing sharded decoder. Previously, nullable
and serial direct reads could still produce wide columns even with the flag
enabled; output-only comparisons did not expose that gap. The direct path now
writes the final Int32 buffer, including zeroed null slots, without an Int64
intermediate. Selected rows and files lacking statistics use the same path.

Adopting Arrow C Data imports now retain the producer's signed Int32 buffer,
logical slice and lifetime owner. Borrowed imports still copy, because they
cannot retain the caller's storage. The Arrow-table fallback similarly adopts
one null-free chunk; nullable/multiple chunks are copied into compact storage.
UInt32 remains Int64, including values above INT32_MAX. No Int64 column is
narrowed based on its values.

`visit_integer_storage(visitor)` exposes a typed immutable Int32 or Int64 span,
with one dispatch per call and no wide cache allocation. Visitors must promote
operands before arithmetic. This is the access boundary for subsequent kernel
work; at that checkpoint, pointer/reference consumers still used a cached wide
fallback, which has since been removed.
That ingestion checkpoint retained the internal compact tag and compatibility
cache. The subsequent removal described above eliminates the cache.

Validation: five new core/Arrow ingestion cases (47 assertions), the full
2,465-case fast suite in both import modes (four network cases skipped), and
strict GCC runtime build pass. Two additional Parquet physical-width cases
(119 assertions per run) pass at one/eight cores, covering the Arrow fallback,
serial/whole-table/sharded decoding, slices, nulls, selection and absent
statistics. A separately compiled decoder with compact serial output disabled
fails the physical-width check at both thread counts. Original seven compact
tests also continue to reject a corrupt producer. Native query results remain
byte-identical to the original engine across both modes and one/eight threads:
ingestion output checks (`ingestion-parquet-correctness.json`, local raw output).
Bundled plugins were rebuilt with matching headers.

For a normal Parquet-enabled build the physical-width tests are included in
CTest with compact mode explicitly enabled at one/eight cores. For this lean
native build, reuse the existing Arrow dependency without rebuilding it:

```bash
python3 benchmarking/int32/check_ingestion.py \
  --arrow-build /home/brj/ibex/build-release --mutations
```

Build/test logs live in `build-release/ingestion-*.log` and
`build-release/int32-ingestion/`. The branch worktree is persistent at
`/home/brj/ibex/.worktrees/native-int32`. New timing claims for ingestion itself
have not been established; the historical query measurements below precede
this follow-up. A fresh 12-pair Release comparison against the original baseline
(with a byte-identical unchanged control) still finds std +5.4%, median +1.6%
and quantile +1.8%; each has paired p=0.00049, Holm p=0.011 over 23 cases.
The sum of query medians is +0.29%. This run used the machine's current turbo
policy, rather than the earlier fixed clock, so compare paired effects within
this run rather than absolute times across dates. These are default-path
regression screens, not measurements of compact ingestion throughput. The
performance gate remains open. Current raw pairs and statistics (`ingestion-regression-paired.json`, local raw output).

## Correctness validation (original prototype)

- Release CTest excluding slow: 2,460 cases registered, zero failures, four
  environment-dependent network tests skipped. Passed with compact imports
  both disabled and enabled.
- Seven compact-specific tests / 90 assertions pass, covering signed extrema,
  exact INT64_MAX, a non-double-exact large integer product, sum 4 billion,
  computed sum 8e18, grouped/nullable reductions, mixed-width expressions,
  copy-on-write, reservation, self-append and promotion beyond Int32.
- A separately compiled corrupt compact producer makes all seven cases fail;
  the production source/binary is not mutated.
- Native Parquet results are byte-identical to the original engine and between
  compact modes / one and eight threads. Cases include nulls, negative extrema,
  exact large values, one million rows in 16 row groups and selected-row decoding.
  Recorded output (`parquet-correctness.json`, local raw output).
- Strict GCC runtime build passes with warnings as errors. Clang Release builds
  cleanly. Bundled plugins were rebuilt; Parquet reused the existing Arrow 22
  dependency with the new Ibex headers/runtime.

## Native query performance

The source is [bench_compact_int32.cpp](../tools/bench_compact_int32.cpp).
It parses and executes `lhs join rhs on id` through the normal interpreter
entry point. Keys stay Int64; three right-side integer payloads change backing.
Inputs are constructed before timing. Result allocation/materialization is
timed; complete key uniqueness/value association checks and destruction are
outside timing. There are two warmups and 12 alternating paired measurements,
pinned to one P-core or eight separate P-cores, with Release optimization.

| Rows | Threads | Wide ms | Compact ms | Paired compact/wide |
|---:|---:|---:|---:|---:|
| 1,000,000 | 1 | 14.33 | 11.08 | 0.777 |
| 10,000,000 | 1 | 292.61 | 253.67 | 0.867 |
| 1,000,000 | 8 | 6.88 | 5.92 | 0.863 |
| 10,000,000 | 8 | 104.64 | 97.77 | 0.933 |

All paired p=0.00049. At 10M rows CPU time falls from 292.5 to 253.6 ms
at one thread, and 618.7 to 510.2 ms at eight threads. This establishes a
materialized-join benefit, not a general arithmetic or whole-suite speedup.
Raw samples and binary hash (`compact-query-results.json`, local raw output).

## Existing-workload regression gate

`compare_ibex_git.sh` compared HEAD to WORKTREE with balanced interleaving,
12 repeats, five inner iterations, one pinned core, explicit datasets and an
unchanged replica build. Both baseline binaries were byte-identical. The
23-query core/filter/join/groupagg slice was ~0.94% slower in summed medians.
Paired analysis flags `std_by_symbol` at +7.5% (p=0.00049, Holm p=0.011 over
23 comparisons), with +6.8% against the unchanged control. Median and quantile
also show smaller resolved effects in this final run. These regressions are not
cleared. Their mechanism is not yet attributed; code layout/inlining remains a
hypothesis rather than a finding.

A prior longer, focused 24-pair rerun also showed std +6.1% (p=0.010), while
its unchanged control had no resolved difference. The shell comparison's
IQR-based label called that run "noise"; the paired Wilcoxon result did not.
Use [paired_report.py](../benchmarking/int32/paired_report.py) on retained raw
repeats when evaluating this work. Final sample arrays and comparisons (`regression-paired.json`, local raw output),
24-pair focused rerun (`groupagg-paired-24.json`, local raw output).

## Reproduce

Build matching native runtime/plugins in Release. Then:

```bash
ctest --test-dir build-release -LE slow --output-on-failure
IBEX_COMPACT_INT32=1 ctest --test-dir build-release -LE slow --output-on-failure
.venv/bin/python benchmarking/int32/check_mutation.py
.venv/bin/python benchmarking/int32/check_parquet.py --out benchmarking/results/local/int32/parquet-check-rerun.json
cmake --build build-release -j6
.venv/bin/python benchmarking/int32/run_compact_join.py --out benchmarking/results/local/int32/compact-join-rerun.json
```

For an actual file query:

```bash
IBEX_COMPACT_INT32=1 IBEX_CORES=8 build-release/tools/ibex_eval \
  --plugin-path build-release/tools query.ibex
```

Before promotion: profile the existing-workload regressions and measure real
Parquet arithmetic workloads, including peak memory. The operation-owned
contiguous-arithmetic conversion is explicit but still costs a copy; typed
readers can also affect generated code and must pass the performance gate.

## Gather-loop follow-up

The diagnosed wide-filter overhead has a focused implementation fix: kernel
ColumnView stores physical base pointers, and gather chooses Int32/Int64 once
before selection iteration. Neither width carries iterator-offset bookkeeping
inside the selected-row loop. Native Release assembly has six instructions per
selected value at both widths, with sign extension folded into the Int32 load.

A 20-pair git A/B against cache-free checkpoint 6e1f4938, with unchanged replica
control, improves the four wide CSV filters by 2.3–3.1%; all survive Holm
correction and the control comparisons are unresolved. This addresses the
localized gather regression without restoring a widening cache. It does not
clear the entire Int32 performance gate or claim faster physical Int32 filtering.
See the [implementation, validation and raw evidence](../benchmarking/int32/gather-loop/README.md).
