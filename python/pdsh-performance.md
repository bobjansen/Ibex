# PDS-H through the Polars engine adapter

Measured 2026-10-25 on an Intel i7-13700, Linux, Release build, with CPU frequency
locked at 3.2 GHz. These are the unchanged upstream Polars query functions on
upstream-generated scale-10 Parquet data (59,986,052 lineitem rows and
15,000,000 orders). This compares `query.collect` using
Polars 2.0.0's in-memory executor, its streaming executor, and `IbexEngine`.
It does not substitute the native hand-written Ibex PDS-H queries.

The comparison-policy follow-up below reduces default-thread Q6 from 8.20 s to
0.69 s. The subsequent native planning follow-up below brings eight-thread Q6
from 747 ms to 219 ms. The initial suite table remains the pre-change baseline.

## Results (initial baseline)

Warm collection medians in milliseconds. The table uses the default-thread run
and the repeated pinned runs after a busy browser process had exited. Initial
pinned runs showed substantial interference and were excluded from this table;
their raw samples are retained separately below.

| Threads | Query | Polars in-memory ms | Polars streaming ms | Polars–Ibex ms |
|---|---|---:|---:|---:|
| 1 | Q1 | 5,469 | 4,049 | 4,027 |
| 1 | Q4 | 1,890 | 1,181 | 1,468 |
| 1 | Q6 | 1,756 | 1,478 | 10,326 |
| 8 | Q1 | 792 | 577 | 847 |
| 8 | Q4 | 298 | 168 | 346 |
| 8 | Q6 | 223 | 217 | 8,351 |
| Default (24) | Q1 | 487 | 317 | 673 |
| Default (24) | Q4 | 224 | 99 | 266 |
| Default (24) | Q6 | 110 | 107 | 8,311 |

In the initial baseline, Q6 was the clear performance outlier: at default threading Polars–Ibex is about
75 times slower than standard Polars and 78 times slower than streaming Polars.
Polars–Ibex collection takes approximately 8.3 seconds at both eight and
24 threads. These measurements identify a problem, not its cause; no runtime
or adapter optimization was made as part of this benchmark.

Q1's first default-thread collection took 528 / 318 / 1,211 ms for standard
Polars / streaming / Polars–Ibex, compared with warm medians of
487 / 317 / 673 ms. Preparation is separately recorded. First-use cost remains
relevant alongside warm throughput.

## Coverage

The adapter now supports **16 of 22 queries: Q1–Q10, Q12, Q14 and Q17–Q20**.
The latest additions are Q7, Q8, Q9, Q10, Q14, Q17, Q19 and Q20. All eight
additions pass complete result comparisons against both in-memory Polars and
streaming Polars on unchanged SF10 Parquet inputs, with matched one- and
eight-thread budgets. The earlier eight queries retain their correctness and
performance evidence below. The new-query measurements are reported next.

The remaining six queries fail explicitly before native execution. These are
first blockers, not exhaustive inventories per query:

| Queries | First translation blocker |
|---|---|
| Q11, Q22 | Inequality joins (`IEJoin`) |
| Q13, Q16 | Regular-expression operators in string `contains` |
| Q15 | Reductions broadcast inside a filter |
| Q21 | Expression-level filtered aggregation (`Filter`) |

Floating rounding now supports 0–15 decimal places, half-to-even and
half-away-from-zero, nulls, non-finite values and signed zero. It guards native
Int64 conversion and projects rounded reductions after aggregation, without
materializing an intermediate query result. Larger precisions remain rejected.
Fused arithmetic preserves operand order and separate multiply/add rounding.
A single-expression `unique` projection uses native distinct; grouped list
results and independently sized projection columns remain rejected.

For the pinned visitor's exact `join with a fused predicate` failure,
preparation copies the caller's flags and revisits the same logical query with
Polars predicate pushdown disabled. This exposes ordinary join/filter nodes
for the native optimizer. Other visitor failures still propagate, cardinality
validation remains checked, and there is no execution fallback to Polars.
The original caller flags and prepared-plan cache keys remain unchanged.

[Full SF10 checks and coverage](pdsh-performance-results/expanded-16-checks.json)
retain complete-result check outcomes, generated sources, query hashes and
mutation evidence. The Python suite passes 118 tests and 692 subtests.

## Newly supported queries: measurements

The eight newly supported queries use unchanged upstream functions and SF10
Parquet files. These are Release warm-process, warm-file-cache collection
medians in milliseconds on the same i7-13700 locked at 3.2 GHz. Translation is
prepared outside timing. Each route has an initial correctness collection and
two warmups, followed by six rounds balancing all six executor orderings.
Collection includes source reading, execution and public result conversion;
previous-result destruction and full-result checks are outside timing.
Every warmup and timed output matches in-memory Polars, including dtype,
column names and row order (`rel_tol=1e-10`, `abs_tol=1e-8`). No execution falls
back to Polars. Ordinary desktop apps remain running; no build or other
benchmark overlapped these runs.

Eight matched threads, pinned to CPUs 0,2,4,6,8,10,12,14:

| Query | Polars in-memory | Polars streaming | Polars–Ibex |
|---|---:|---:|---:|
| Q7 | 502 | 435 | 520 |
| Q8 | 621 | 472 | 525 |
| Q9 | 1,363 | 632 | 1,067 |
| Q10 | 568 | 474 | 1,213 |
| Q14 | 305 | 281 | 328 |
| Q17 | 288 | 219 | 468 |
| Q19 | 308 | 311 | 2,164 |
| Q20 | 775 | 413 | 543 |

Polars–Ibex beats in-memory Polars on Q8, Q9 and Q20, but streaming Polars
wins all eight. Q7, Q8 and Q14 are relatively close to streaming (about 11–19%
slower). Q10 is 2.6× slower, Q17 2.1×, and Q19 is the largest gap at 7.0×.
These measurements identify the gaps; they do not establish their causes.

One matched thread, pinned to CPU 2:

| Query | Polars in-memory | Polars streaming | Polars–Ibex |
|---|---:|---:|---:|
| Q7 | 3,469 | 2,933 | 3,009 |
| Q8 | 4,776 | 3,394 | 3,472 |
| Q9 | 9,049 | 4,265 | 6,729 |
| Q10 | 3,158 | 3,148 | 4,090 |
| Q14 | 2,118 | 2,057 | 2,138 |
| Q17 | 1,992 | 1,445 | 3,113 |
| Q19 | 2,212 | 2,114 | 6,941 |
| Q20 | 4,149 | 2,774 | 3,117 |

Process CPU screen for Polars–Ibex (CPU seconds sum all engine threads):

| Query | 1-thread CPU s | 8-thread CPU s | CPU 8/1 | Wall speedup |
|---|---:|---:|---:|---:|
| Q7 | 3.01 | 3.34 | 1.11× | 5.79× |
| Q8 | 3.47 | 3.67 | 1.06× | 6.62× |
| Q9 | 6.73 | 7.35 | 1.09× | 6.31× |
| Q10 | 4.09 | 4.57 | 1.12× | 3.37× |
| Q14 | 2.14 | 2.26 | 1.06× | 6.51× |
| Q17 | 3.11 | 3.28 | 1.05× | 6.65× |
| Q19 | 6.94 | 12.30 | 1.77× | 3.21× |
| Q20 | 3.12 | 3.47 | 1.11× | 5.74× |

CPU growth is a screen for parallel cost, not proof of redundant decoding.
It can also reflect contention or per-task overhead; source/row counters are
needed before attributing an increase to duplicated work. Q19 stands out:
its CPU total rises 77% for a 3.21× wall speedup. Q10 also scales less well
than the other additions, though its CPU growth is only 12%.

[Eight-thread samples and generated plans](pdsh-performance-results/expanded-16-timings-eight.json)
retain bridge and Python-source hashes, query/data provenance, preparation
and first-per-query collection times, wall-time samples and process CPU
samples. Process CPU counts all threads and excludes correctness checks.
[One-thread samples and generated plans](pdsh-performance-results/expanded-16-timings-one.json)
retain the matching serial run. Both budgets use the identical bridge and
Python sources, and ran sequentially.
First-per-query times follow other routes/queries in the same process and
are not fresh-process first-use measurements.

Reproduce this extension's timings with `python/bench_pdsh_engine.py`,
`--queries 7 8 9 10 14 17 19 20 --iters 6`, and the same upstream/data paths
as the commands in Reproduction below. Use matched `IBEX_CORES` and
`POLARS_MAX_THREADS`, pin one thread to CPU 2 and eight threads to
0,2,4,6,8,10,12,14, and run the budgets sequentially. Rebuild the complete
Release tree immediately before each run.

## Q19: projection and alias barriers

The Q19 investigation identifies missing predicate pushdown before the join.
Actual profiles at both one and eight threads show the adapter joining all
**59,986,052 lineitems**, then retaining only **1,134 rows**. The join and wide
payload assembly happen before the selective ship-mode, instruction and
brand/container/quantity predicates. The read-only physical inspection shows
three fused filter/update/project steps over `MaterializedInput(Join)`.

The optimizer already implements Q19's disjunction implications in
`src/ir/join_pushdown.cpp`: it can prefilter each input while retaining the
original mixed predicate above an inner join. But its walker recognizes a
Filter directly over Join. The adapter inserts projections and restores its
internal column aliases with computed selections (Update + Project), so that
shape is hidden. Canonicalization has no filter-through-Project rule, and its
filter-through-Update rule deliberately declines predicates reading outputs
of the update. Native Rename has an inverse map and an existing commute rule.

Controlled equivalent sources isolate these barriers. Each run uses six
alternating rounds, with preparation outside timing and every complete result
checked against streaming Polars. No production implementation was changed.
**The host frequency policy has changed since the published tables above:**
these new runs use dynamic powersave/turbo settings rather than the previous
3.2GHz lock. Compare columns within these diagnostic runs; their absolute
values are not directly comparable to the earlier published baseline.

| Diagnostic representation | One thread ms | Eight threads ms |
|---|---:|---:|
| Current adapter | 5,107 | 1,674 |
| Remove only identity projections | 5,571 | 1,910 |
| Compact equivalent source | 1,631 | 229 |
| Polars streaming | 1,318 | 192 |

The compact source uses the same scalar bindings, predicates, aggregate and
rounding as the adapter, with the join/filter adjacent. It filters `part`
to **4,754 rows**, joins to **3,052 lineitems**, then applies the original
mixed predicate to retain the same **1,134 rows**. Its join output is about
19,655× smaller; the profiles show the large materialization disappearing.
Both one- and eight-thread profiles reproduce these row counts. Removing
identity selections alone leaves the alias-update barriers and does not
reduce the 60-million-row join.

Two additional eight-thread controls isolate the individual wrappers:

| Representation | Median ms |
|---|---:|
| Compact source | 237 |
| Same source with one plain projection before its filter | 1,681 |
| Same source with a native Rename on demanded discount | 230 |

A separate paired control inserts just `update { ship_mode = l_shipmode }`
and makes the predicate read `ship_mode`: it measures **1,674 ms** versus
**230 ms** for the compact source. Thus both a Project and a column-alias
Update can independently block the existing join rewrite.

The appropriate shared-optimizer fix is to commute filters through
projections when the predicate's columns remain in scope, and to remap
predicates through safe column aliases. It must preserve scalar shadowing,
unknown-column errors, computed/nonlocal update semantics, outer-join rules
and join cardinality assertions. The existing disjunction pushdown can then
perform the selective input filtering; no Q19-specific kernel is needed.

[Diagnostic samples, generated sources, drivers, plans and actual profiles](pdsh-performance-results/q19-pushdown-investigation.json)
retain the evidence. All successful variants return revenue **30,104,438.09**.
One failed predicate-only native-rename trial is explicitly excluded from
numerical conclusions; the archive records that separate demand-pruning issue.

### Shared optimizer implementation

The native canonicalizer now moves row-local filters through projections when
every referenced column remains visible. It also inverse-maps predicates through
unguarded bare-column copies. Computed outputs, alias dependencies, duplicate
definitions, lexical copies and nonlocal expressions retain their barriers.
Unqualified names hidden by a projection stay above it, preserving scalar scope
and unknown-column errors. Existing join-kind and cardinality-assertion gates
remain in force.

After join pushdown, the REPL canonicalizes the new input filters again before
scan planning. This lets scan predicates reach source columns through aliases
and projections, avoiding eager payload decoding. The Python adapter and the
upstream query remain unchanged.

The broader SF10 check exposed a bounded-morsel scheduling deadlock in Q7:
all pool workers could park on output backpressure while a downstream stage
waited for its join-probe batch. Long morsel pipelines now reserve one pool
worker, as streaming scan pipelines already do. A focused regression stops
consuming a producer and verifies that downstream work can still finish;
failure cancels the producer so the test itself cannot hang.

All 16 supported SF10 queries match full Polars results at both one and eight
threads. Regression coverage includes retained null payloads and inner, left,
right and full joins, plus a lazy-source test that rejects unfiltered payload
decodes. Full CTest passes all 2,378 cases, including interpreter/transpiled
parity, with four network tests skipped by their existing guards. The
WebAssembly runtime builds, and a CSV plugin rebuilt through
`ibex-plugin-build.sh` loads and returns the expected aggregate.

Production Q19 measurements use twelve paired rounds with all six execution
orders balanced, two warmups, the original SF10 query, the same Python adapter
and complete output checks after every collection. Release bridges were saved
before the native changes and rebuilt in full before each measurement. The
host was locked at 3.2GHz; one thread is pinned to CPU 2 and eight threads to
CPUs 0,2,4,6,8,10,12,14. These affinity restrictions differ from the earlier
unpinned cross-engine tables, so compare columns within this new paired run.

| Threads | Before ms | After ms | Polars streaming ms | Ibex speedup |
|---|---:|---:|---:|---:|
| One | 22,058 | 5,579 | 4,313 | 4.0× |
| Eight | 2,126 | 398 | 306 | 5.3× |

At eight threads the remaining gap is 92 ms (30%), down from 1,820 ms.
Every paired collection is faster after the change at both thread counts.
Actual production profiles reproduce the **3,052-row join**, **1,134 retained
rows** and revenue **30,104,438.09** at both budgets. These profiles validate
cardinality; the paired warm runs above supply the timing comparison.

- [One-thread production samples](pdsh-performance-results/q19-production-one.json)
- [Eight-thread production samples](pdsh-performance-results/q19-production-eight.json)
- [Native filter/join regression comparison](pdsh-performance-results/q19-native-regression.txt)
- [Native paired samples and signed-rank statistics](pdsh-performance-results/q19-native-statistics.json)
- [Correctness, profiles and mutation evidence](pdsh-performance-results/q19-production-evidence.json)

The native regression fixture uses million-row resident inputs and twelve
interleaved pairs at both budgets. Its eight-case sum moves by +0.04% at one
worker and +0.65% at eight. The comparison script's IQR rule labels every case
noise, but a paired Wilcoxon check detects an eight-worker categorical join
regression: **1.6335 → 1.6960 ms** (ratio of medians +3.8%; median of paired
ratios **+7.1%, p=0.0015**). The other native cases do not resolve at p<0.05.
Both Q19 improvements have p=0.00049. The native join, chunked-join and
physical-planner object files are byte-identical between builds, while linked
code addresses move. Code layout is a possible explanation, not a demonstrated
cause or a resolved regression. This small native cost remains recorded
alongside the Q19 improvement.

## Earlier eight-query coverage

The earlier extension covered compatible string representations, literal
membership (including null policies), conditional expressions and typed null
branches, Boolean sums, literal starts/ends/contains matching, Date year
extraction, deterministic cached subplans, floating grouping keys, numeric
min/max and null-inclusive distinct counts over integer/Boolean/string values.
String matching escapes native LIKE metacharacters; regex mode accepts only
patterns without regex operators. Literal membership accepts at most 256
scalar values and adds no list/struct column support. Cached deterministic
subplans may be recomputed by native execution; this is not a physical result
cache. Floating group keys normalize NaNs for equality and return original
labels. Float extrema skip NaNs when ordinary values exist, return NaN for
all-NaN groups, and null for empty/all-null inputs.

Validation includes 113 Python tests and 660 subtests. New regressions exercise
resident and Parquet strings/membership, null predicates and branches, NaN
membership and groups, signed zeros, empty inputs, Date extraction, shared
subplans, extrema and distinct-count null handling. Corrupting the corresponding
translations makes the new regression cases fail; sources were restored before
validation. Upstream query functions and optimization flags are unchanged, and
query execution never falls back to Polars.

- [Eight-worker full-result coverage](pdsh-performance-results/sf10-expanded-coverage-eight.json)
- [One-worker full-result coverage](pdsh-performance-results/sf10-expanded-coverage-single.json)

Reproduce the coverage check without benchmark loops:

```bash
PYTHONPATH=build-release/python:python IBEX_CORES=8 POLARS_MAX_THREADS=8 \
  OPENBLAS_NUM_THREADS=1 .venv/bin/python python/bench_pdsh_engine.py \
  --pdsh-root ../polars-benchmark \
  --data ../polars-benchmark/data/tables/scale-10.0 \
  --check-only --out results/pdsh-coverage-eight.json
```

For that earlier extension, use `--queries 2 3 5 12 18`; the latest additions
use `--queries 7 8 9 10 14 17 19 20`. The JSON keeps
all outcomes, upstream source hashes, generated native source for successful
queries, input metadata and bridge identity. `first_collect_ms` is diagnostic
first-use timing, not a warm benchmark result.

## Expanded-query performance check

A subsequent performance check measures all eight supported queries on the same
SF10 data, Release build and 3.2 GHz CPU lock. One thread is pinned to CPU 2;
eight threads use CPUs 0,2,4,6,8,10,12,14. Each query first passes complete result
comparisons against both Polars executors, then two warmups and six rounds that
balance all six executor orderings. Preparation and previous-result destruction
are outside timing; Parquet reading and public result conversion are inside.

Warm medians in milliseconds, eight threads:

| Query | Polars in-memory | Polars streaming | Polars–Ibex |
|---|---:|---:|---:|
| Q1 | 806 | 567 | 800 |
| Q2 | 47 | 61 | 137 |
| Q3 | 425 | 336 | 527 |
| Q4 | 303 | 165 | 298 |
| Q5 | 526 | 381 | 494 |
| Q6 | 220 | 213 | 224 |
| Q12 | 203 | 173 | 2,528 |
| Q18 | 1,474 | 540 | 1,022 |

Warm medians in milliseconds, one thread:

| Query | Polars in-memory | Polars streaming | Polars–Ibex |
|---|---:|---:|---:|
| Q1 | 5,458 | 4,033 | 3,995 |
| Q2 | 315 | 252 | 659 |
| Q3 | 3,117 | 2,372 | 2,940 |
| Q4 | 1,890 | 1,184 | 1,430 |
| Q5 | 3,949 | 2,768 | 3,265 |
| Q6 | 1,495 | 1,477 | 1,447 |
| Q12 | 1,374 | 1,154 | 3,464 |
| Q18 | 6,386 | 3,306 | 2,705 |

Q18 illustrates the importance of process warmth. At one thread the warm native
median beats streaming Polars (2,705 vs 3,306 ms), but fresh-process first
collections reverse that comparison (3,704 vs 3,374 ms). These separate workers
each execute Q18 as their first query, with preparation outside timing and a
warm file cache. Complete output values and dtypes match exactly across all
three routes in this check. Each first-use value is one observation:

| Threads | Polars in-memory | Polars streaming | Polars–Ibex |
|---|---:|---:|---:|
| One | 6,293 | 3,374 | 3,704 |
| Eight | 1,466 | 530 | 1,364 |

Q12 is the clear outlier, about 14.6× slower than streaming Polars. Its second
profiled collection took 3,063 ms, of which the serial filter reported 2,819 ms;
the inner join reported about 27 ms of build/next self time. Pool work plus
unqueued time closes to pool capacity in this profile (1,306 + 23,201 ≈ 24,507
thread-ms). This identifies filtering as the dominant issue, rather than merely
inferring it from the overall speedup.

A separate paired diagnostic changes only the frontend representation of
nonempty, null-free literal membership with its default null policy. Returning
the existing OR-of-equalities branch directly removes an unnecessary surrounding
null-handling case; equality already propagates the null. Both complete outputs
match Polars. Over six alternating pairs, Q12 falls from **2,544 to 378 ms**.
A separate profile confirms that the diagnostic performs selected-column
decoding, while the current plan retains the expensive standalone filter. This
demonstrates the importance of the expression shape. It is a diagnostic, not a shipped optimization; the main table measures
the committed adapter. Empty lists and alternate null policies require their
existing handling. The diagnostic JSON retains both generated native sources,
all samples and the driver used.

- [One-thread timing samples](pdsh-performance-results/sf10-expanded-performance-single.json)
- [Eight-thread timing samples](pdsh-performance-results/sf10-expanded-performance-eight.json)
- [Q18 fresh-process collections](pdsh-performance-results/q18-fresh-process.json)
- [Q12 operator profiles](pdsh-performance-results/q12-profile-eight.txt)
- [Q12 diagnostic operator profiles](pdsh-performance-results/q12-membership-profile-eight.txt)
- [Q12 membership diagnostic](pdsh-performance-results/q12-membership-diagnostic-eight.json)

## Q12: physical inspection and the execution entry point

A deeper investigation used the REPL's read-only `:explain` machinery on the
filter, join and aggregate portions, then compared actual execution profiles.
Before the membership fix, the adapter's line filter reported
`serial(expression needs rows outside the morsel)`. Removing the unnecessary null-handling case reports
`morsel-parallel`. Both joins report the same `StreamingProbe`, `SingleKeyInner`
strategy and `orientation=runtime`. Handwritten native Q12 uses the same
orders-to-lineitem source order. Reversing the compact native join measured
291 vs 293 ms; explicit input schema annotations and removing projection/rename
wrappers also made no material difference in this experiment.

A second frontend cost is the redundant `false OR ...` seed in membership.
After scalar binding, that leaf prevents the string IN-list extractor from
recognizing an OR-chain consisting only of column/literal equalities. Removing
it permits the existing fused set-filter path. All complete results match.
The six-round alternating membership comparison measured:

| Diagnostic representation | Eight-thread median ms |
|---|---:|
| Null wrapper removed, false OR seed retained | 371 |
| Null wrapper and false OR seed removed | 297 |
| Handwritten native source through Python bridge | 293 |
| Polars streaming | 172 |

The handwritten-source comparison above initially suggested no remaining
frontend gap. Running the actual CLI benchmark exposed a distinction: **the
same source does not enter the same execution path through the Python bridge.**

`python/ibex_pyarrow_module.cpp::py_execute_pipeline` calls
`ReplSession::execute`. That enters `execute_statements`; its table-expression
evaluator applies logical rewrites and demand/filter pushdown, then eagerly
calls `decode_demanded_lazy_sources` before `interpret`. By contrast,
`execute_script` tries `try_execute_whole_script`, which executes the unified
plan through `optimize_and_execute_plan`. That path additionally registers
streaming scans and deferred probe scans in the execution context. The Python
entry point did not use that registration path before the bound-expression
fix below.

Actual profiles distinguish the routes: CLI Q12 has a live streaming stage
(`stage_threads_peak=1`), overlapping unit decoding and downstream execution;
Python-hosted native Q12 has none (`stage_threads_peak=0`) and whole-column
decodes precede its operators. This is stronger evidence than join source
order or the unresolved capability plan alone.

A controlled check runs identical native Q12 in two persistent CLI workers,
changing only `IBEX_STREAM_SCAN`. Twelve alternating pairs after two warmups
measure **197 ms with streaming enabled and 308 ms disabled**. Disabling it
reproduces the approximate 293 ms Python-hosted result. The whole-script
planner remains active on both sides; it is scan streaming that changes.

The allocator hypothesis was tested and rejected as the main explanation.
Disabling the CLI's huge-page startup adjustment left it around 195 ms;
explicitly enabling the CLI's glibc huge-page/top-pad settings for Python left
the handwritten Python-hosted query around 286 ms. These small differences do
not explain the roughly 100 ms execution-path gap.

Component diagnostics further separate the remaining work. Inputs for the
resident join/aggregate are prepared outside timing. The read/filter checks
force the relevant columns through counts and checksums, returning small
outputs; these are separate queries and their times are not additive:

| Component | Ibex ms | Polars streaming ms |
|---|---:|---:|
| Lineitem read/filter | 218 | 134 |
| Orders read | 54 | 35 |
| Join and aggregate with resident inputs | 35 | 33 |
| Complete cleaned query | 296 | 173 |

These findings motivated the membership and bound-expression fixes below.
Calling the existing fresh-script planner blindly is insufficient: the library
path must carry its injected table/scalar bindings and preserve session
semantics. The measurements in this section preceded those production fixes.

The [deep-dive archive](pdsh-performance-results/q12-deep-dive.json) retains
physical inspection inputs and outputs, both execution profiles, all paired
samples, component checks, native query text and diagnostic drivers. One early
multi-variant trial is explicitly excluded from numerical conclusions because
all timings, including the Polars reference, drifted substantially; its cause
was not established.

## Q12: membership and bound-expression fixes

The membership translation now emits a bare OR-chain of equalities when its
null policy permits it. Empty/all-null lists and `nulls_equal=True` retain
explicit null handling. Python pipelines also opt into the existing script
executor for each table expression: source statistics, deferred probe scans
and streaming scan registration are shared with the CLI. Statements retain
their order and bindings; this does not fuse across a materialized prefix
binding. Ordinary REPL execution keeps its default behavior.

Deferred scan registrations now carry the call's scalar registry. The script
path previously needed none, but Python predicates retain typed lexical
bindings; both whole-source deferred probes and per-unit scans must receive
them. The native regression reader refuses whole-file decoding, and the test
fails when streaming is disabled. The actual Q12 profile now has a live
streaming stage (`stage_threads_peak=1`).

Twelve alternating pairs compare the saved pre-executor bridge with the new
bridge, both using the fixed membership translation. These are Release SF10
warm collection medians in milliseconds, with matched thread budgets:

| Threads | Fixed membership, old executor | Fixed membership, new executor | Polars streaming |
|---|---:|---:|---:|
| 1 | 1,259.4 | 1,207.7 | 1,165.4 |
| 8 | 296.0 | 191.4 | 179.1 |

The executor wins all 12 pairs at each budget; median paired reductions are
4.2% and 35.0% (exact two-sided sign-test p=0.000488 each). The eight-thread
result is about 7% behind streaming Polars. The earlier pre-membership
production result was about 2.5 seconds; that historical measurement was a
separate run, not a paired baseline for this table.

Fresh-process first Ibex collections were 1,713 → 1,218 ms at one thread and
534 → 200 ms at eight threads. These use warm file caches, not cold storage;
the Polars reference runs in a worker already warmed by Ibex. Desktop apps
remained running; no compilation or other benchmark overlapped these timings.

Complete SF10 results match both Polars executors for all eight supported
queries (Q1–Q6, Q12, Q18) at eight threads. The focused Q12 comparison checks
every row, dtype and ordering on every sample at both thread budgets. Python
validation passes 114 tests and 660 subtests; native CTest reports zero
failures in 2,372 cases, with four network tests skipped. GCC runtime and
plugin rebuilds pass. The required native A/B has no regressions in its four
filter cases; those small in-memory cases are a regression screen rather
than evidence for this streaming change's benefit.

The [paired results and drivers](pdsh-performance-results/q12-bound-streaming.json)
retain samples, first-use measurements, bridge hashes, generated source,
full-scope checks and native A/B output. The
[actual runtime profile](pdsh-performance-results/q12-bound-streaming-profile-eight.txt)
contains two Q12 executions; use the second for warm profiling.

## Shared executor: all supported queries

A paired SF10 sweep compares the saved old bridge with the new bridge while
using the same current Python adapter in both workers. Membership is already
simplified on both sides, so this isolates the shared executor. Each worker
prepares once per query; all complete outputs are checked against in-memory
Polars, including every timed result. Two warmups follow the initial collection,
then six rounds balance all six orderings of the two bridges and streaming
Polars. Collection includes Parquet decoding and public result conversion.

Warm medians in milliseconds at eight threads:

| Query | Old executor | Shared executor | Paired change | Polars streaming |
|---|---:|---:|---:|---:|
| Q1 | 770 | 808 | +4.9% | 576 |
| Q2 | 133 | 127 | −4.1% | 67 |
| Q3 | 524 | 401 | −23.6% | 346 |
| Q4 | 302 | 258 | −14.2% | 167 |
| Q5 | 487 | 466 | −5.5% | 386 |
| Q6 | 229 | 222 | −2.9% | 221 |
| Q12 | 294 | 191 | −35.4% | 179 |
| Q18 | 1,028 | 438 | −57.5% | 535 |

Seven queries improve in this sweep, although Q6's small difference is less
consistent (five of six pairs). Q2–Q5, Q12 and Q18 win all six pairs. Q1 is a
regression: a separate twelve-pair repeat measures 777 → 800 ms, with a median
paired slowdown of 3.2%; the new path loses all twelve pairs. The result is
therefore a 3–5% repeatable slowdown rather than a uniform improvement.

Q18's warm result beats streaming Polars, but fresh-process first collection
is nearly level. Three independent processes per route, with preparation
outside timing and full correctness checks afterwards, measure median first
collections of **1,359 → 536 ms**, versus **542 ms** for streaming Polars.
File caches remain warm. The general sweep's first-per-query samples follow
other queries in the same processes and must not be called fresh-process times.

Warm medians in milliseconds at one thread, using the same method:

| Query | Old executor | Shared executor | Paired change | Polars streaming |
|---|---:|---:|---:|---:|
| Q1 | 3,856 | 3,936 | +1.7% | 4,065 |
| Q2 | 651 | 577 | -11.3% | 256 |
| Q3 | 2,939 | 2,464 | -15.8% | 2,398 |
| Q4 | 1,412 | 1,404 | -0.5% | 1,189 |
| Q5 | 3,223 | 2,966 | -7.8% | 2,788 |
| Q6 | 1,448 | 1,510 | +4.3% | 1,484 |
| Q12 | 1,263 | 1,204 | -4.5% | 1,161 |
| Q18 | 2,656 | 3,728 | +40.7% | 3,363 |

The single-thread result is mixed: Q1 slows by 1.7%, Q6 by 4.3%, and
Q18 by 40.7% (all six pairs lose for each). Q4 is effectively flat.
Every timed output matches the in-memory Polars reference at both budgets.
The thread configurations ran sequentially with CPUs pinned and no overlapping
compilation or benchmarks. Ordinary desktop applications remained running.

A focused Q18 check changes only `IBEX_STREAM_SCAN=0`. At one thread it
measures **2,622 → 2,327 ms**, with the shared executor winning all six pairs
(median paired improvement 11.1%); streaming Polars stays at 3,359 ms.
Deferred probe registration and the shared logical planner remain enabled.
Disabling scan streaming removes the regression, pointing to single-thread
streaming admission as the next issue rather than a general planner regression.
No engine policy has been changed in response to these measurements.
A fresh-process single-thread check also loses: 3,713 → 4,225 ms, versus
3,433 ms for streaming Polars. These are single observations per route,
with warm files and the first collection timed before any reference query.

The [paired runner](bench_pdsh_bridge_ab.py) accepts saved extension directories
through `--before-dir` and `--after-dir`. Evidence retains all samples, bridge
hashes, identical generated sources, dataset metadata and query hashes:

- [Eight-thread sweep](pdsh-performance-results/pdsh-bound-streaming-eight.json)
- [One-thread sweep](pdsh-performance-results/pdsh-bound-streaming-one.json)
- [Twelve-pair Q1 repeat](pdsh-performance-results/pdsh-bound-streaming-q1-repeat-eight.json)
- [Fresh-process Q18, eight threads](pdsh-performance-results/pdsh-bound-fresh-q18-eight.json)
- [Fresh-process Q18, one thread](pdsh-performance-results/pdsh-bound-fresh-q18-one.json)
- [Q18 with streaming disabled, one thread](pdsh-performance-results/pdsh-bound-streaming-q18-no-stream-one.json)

## Measurement method

Each thread configuration runs in a separate process. One worker is pinned to
CPU 2; eight workers use CPUs 0,2,4,6,8,10,12,14; default threading uses the full
machine. `IBEX_CORES` and `POLARS_MAX_THREADS` receive matched budgets, or are
both unset for the default run. OpenBLAS is limited to one thread.

Translation/preparation is outside timing. Collection includes Parquet reading,
query execution, Arrow export and construction of the Polars result. There is
no result caching. After the initial correctness collections and two warmups,
six interleaved rounds balance each executor's position. Destruction of the
previous result is outside timing. These are warm-process, warm-file-cache
measurements, not cold-storage latency. Initial per-query collection times and
preparation times are retained separately in the JSON reports; only Q1 is the
first query in each fresh process.

Correctness compares every row, column name, dtype and row order against
standard Polars, for both other executors. Floating-point tolerance is
`rel_tol=1e-10, abs_tol=1e-8`. Reports retain every sample, the bridge hash,
upstream query source hashes, revisions, affinity and data file metadata.

A concurrent rebase briefly interrupted initial build attempts. Timing began
only after the tree was reconciled and the full Release rebuild succeeded.
The measured bridge and Python packages were copied aside to insulate the run
from subsequent edits. The snapshot's adapter sources are identical to the
recorded measurement revision.

## Reproduction

Build the complete Release tree first. Run the commands sequentially, with no
concurrent compilation or benchmark work. Replace the upstream checkout path
with its location on your machine:

```bash
cmake --build build-release -j 6
PYTHONPATH=build-release/python:python IBEX_CORES=8 POLARS_MAX_THREADS=8 \
  OPENBLAS_NUM_THREADS=1 taskset -c 0,2,4,6,8,10,12,14 \
  .venv/bin/python python/bench_pdsh_engine.py \
  --pdsh-root ../polars-benchmark \
  --data ../polars-benchmark/data/tables/scale-10.0 \
  --out results/pdsh-eight.json
```

For one worker, set both thread variables to 1 and use `taskset -c 2`. For
default threading, unset both variables and omit `taskset`. Affinity numbers
here describe this i7-13700's P-core topology and need adapting on other hosts.

## Saved measurements

- [Single worker](pdsh-performance-results/sf10-single.json)
- [Eight workers](pdsh-performance-results/sf10-eight.json)
- [Default threading](pdsh-performance-results/sf10-default.json)
- Excluded initial runs with competing load: [single](pdsh-performance-results/sf10-single-initial.json), [eight](pdsh-performance-results/sf10-eight-initial.json)

The [runner](bench_pdsh_engine.py) records all 22 query outcomes in every report.
Those initial timing reports covered three queries. There is no whole-suite
performance score; current coverage and its 14 remaining blockers appear above.

## Comparison-policy follow-up

Implemented in `9b8d7237` (native IR/runtime) and `54a9c347` (Python translation).
The IR now carries a floating-point comparison policy. Ordinary Ibex operators
retain IEEE comparisons; the Python frontends select NaNs-equal-and-greatest
ordering through reserved internal calls that lower immediately to comparison
nodes. No expanded `case`/`is_null`/`is_nan` predicate is needed. Validity masks
still supply null propagation, including in projected Boolean expressions.
The policy survives code generation. IEEE-specific shortcuts that do not
implement this policy decline it instead of silently changing its meaning.

The original Q6 profile spent about 8.55 of 8.67 seconds in a serial filter over
59,986,052 rows. With the new policy, the filter is evaluated in the scan path
and selective decoding is restored. A diagnostic warm collection took 0.88 s;
the paired measurements below are the performance result.

| Threads | Before ms | After ms | Polars streaming ms |
|---|---:|---:|---:|
| One | 9,404 | 1,984 | 1,473 |
| Default (24) | 8,202 | 691 | 105 |

This is about 4.7× faster at one thread and 11.9× at default threading. The
remaining default-thread gap to direct native Q6 (about 146 ms in the diagnostic)
is not solved by this change.

These are paired Release measurements of the unchanged upstream Q6 function
on the same scale-10 file, using saved pre-change Python/native packages and
the corrected packages in separate persistent workers. Six rounds alternate
route order after correctness collections and two warmups. Only one worker
executes at a time. Timing includes reading Parquet and constructing the
result, with translation and previous-result destruction outside timing.
Before/after revenues are exactly equal at both thread counts, and both workers
compare their complete results with Polars. Raw reports include generated
source, binary/lowerer/query hashes and the concurrent native lint patch present
in the measured Release build.

Validation includes all 2,364 CTest cases (including the full transpilation
parity suite), 61 Python tests, strict GCC compilation, and a rebuilt/loaded
CSV plugin. A further compiled parity fixture exercises all six internal
comparison operators. The comparison matrix covers resident and Parquet input,
NaNs, infinities, signed zero, nulls, reversed operands, projected values and
filters; Parquet includes all-NaN/all-null and mixed row groups with statistics.
Deliberately corrupting the kernel, substituting IEEE comparisons in the
adapter, or dropping the generated-code policy makes the corresponding
regression fail. Correct sources and binaries were restored afterward.

The native `compare_ibex_git.sh` check compared `6abe2e53` with `54a9c347` on
100,000-row inputs, one worker pinned to CPU 2, 12 interleaved repeats and five
iterations. No regression was detected across `filter_simple`, `filter_and`,
`filter_arith` and `filter_or`. The two unchanged cases were classified as noise;
two measured faster, which is not attributed to this feature because the
ordinary IEEE kernels are unchanged. See the [native A/B report](pdsh-performance-results/nan-policy-native-filter-ab.txt).

- [Paired runner](bench_pdsh_q06_ab.py)
- [Single-thread samples](pdsh-performance-results/q06-nan-policy-single.json)
- [Default-thread samples](pdsh-performance-results/q06-nan-policy-default.json)

Reproduce after saving the old bridge and Python packages in separate
directories, rebuilding the full Release tree, and choosing thread settings as
above:

```bash
.venv/bin/python python/bench_pdsh_q06_ab.py \
  --baseline-python saved-baseline/python --baseline-bridge saved-baseline/bridge \
  --pdsh-root ../polars-benchmark \
  --lineitem ../polars-benchmark/data/tables/scale-10.0/lineitem.parquet \
  --out results/q06-policy.json
```


## Bound constants, range choice, and NaN-aware bounds fusion

The next investigation isolated three native engine issues. Scalar bindings were
broadcast into columns and missed optimizations that recognized literal IR
operands. Source filtering chose the first eligible column in predicate order.
Finally, Polars NaN comparisons declined the blocked bounds kernels.

Three separate commits address these:

- `75935032`: resolve scalar references to typed literals in a local filter copy
  for each execution, using the current registry and column scope precedence.
  The reusable plan and its scalar bindings remain independent.
- `f9664aea`: rank complete scan intervals by the fraction of the source min/max
  domain they cover. This is a uniform-distribution estimate used only to choose
  work order; it never determines which rows pass. Missing metadata and ties
  retain predicate order.
- `25d1f021`: fuse NaN-high comparisons against numeric bounds. Each absorbed
  conjunct intersects a separate NaN acceptance flag. A NaN-high lower bound
  admits NaNs; an IEEE comparison or a NaN-high upper/equality bound excludes
  them. NaN literals and non-range inequalities retain the generic path.

Paired measurements use the same upstream Q6, SF10 input, CPU settings and
runner as above, with twelve alternating rounds. Local parallel comparisons
are capped at eight threads. Each worker checks its complete result against
Polars; before/after revenue is exactly equal. Both use the same Python adapter.

| Threads | Before ms | After all three ms | Polars streaming ms |
|---|---:|---:|---:|
| One | 1,988 | 1,442 | 1,460 |
| Eight | 747 | 219 | 189 |

At eight threads this is 3.4× faster, reducing the gap to Polars streaming from
558 ms to 30 ms. Fresh-process first collections, including translation, took
1,257 ms before and 313 ms after, versus 192 ms for Polars in the target worker.
Those are single observations on a warm file cache, not warm medians.

The intermediate paired eight-thread runs measured 748 → 393 ms with scalar
resolution alone and 749 → 264 ms after range selection. The last bounds-fusion
step brings the final result to 219 ms. These are separate sessions, each
paired against the original bridge, rather than a simultaneous four-way run.

Validation: 2,371 CTest cases excluding the slow transpilation suite, 61 Python
tests, strict GCC compilation, and the WebAssembly runtime build passed. A CSV
plugin rebuilt through `ibex-plugin-build.sh` loaded and returned all 150 iris rows. Four network tests were skipped by
their existing environment guards. Regression tests cover fresh bindings on a
reused plan, lexical and column scope, typed nulls, source key-scan reachability,
metadata/domain changes, full-width integer estimates, both float policies,
NaNs, infinities, signed zero, reversed comparisons, mixed policies and exact
selection equality at one, two, three and eight workers. Deliberately corrupting
scalar resolution, disabling interval ranking, and rejecting NaNs in the fused
kernel made every newly added test case fail in its corresponding mutation run;
correct sources and binaries were restored.

- [Scalar-resolution samples](pdsh-performance-results/q06-bound-constants-eight.json)
- [Range-choice samples](pdsh-performance-results/q06-range-choice-eight.json)
- [Combined one-thread samples](pdsh-performance-results/q06-native-constants-single.json)
- [Combined eight-thread samples](pdsh-performance-results/q06-native-constants-eight.json)


The native regression check found a separate cost on resident filters. Both
sizes use twelve paired rounds, five iterations, one worker pinned to CPU 2,
and the same generated three-column trades fixture (the million-row input
repeats the 100,000-row fixture ten times outside timing).

| Input rows | Filter | Before ms | After ms | Change |
|---|---|---:|---:|---:|
| 100,000 | Simple | 0.1560 | 0.1740 | +11.5% |
| 100,000 | Arithmetic | 0.1800 | 0.2025 | +12.5% |
| 1,000,000 | Simple | 1.6930 | 1.7865 | +5.5% |
| 1,000,000 | Arithmetic | 2.0595 | 2.2120 | +7.4% |

AND/OR filters were classified as noise at both sizes. These simple/arithmetic
regressions remain recorded: 18–23 microseconds at 100,000 rows and 94–153
microseconds at one million rows. Separate `perf` samples put most CPU in
output gathering; its `kernel_filter.cpp.o` is byte-identical between baseline
and target (SHA256 `b728048115002c73b1db571623d2385962faf7c11849c14b450d549d5ffc97d3`).
The larger CPU shares are in the unchanged integer gather instantiations,
whose linked addresses moved. This suggests code layout sensitivity, rather
than demonstrating additional scalar-resolution work, and is not a resolved
regression. Q6's measured improvement is reported alongside this limitation.

- [Native filter A/B, 100,000 rows](pdsh-performance-results/native-constants-filter-ab.txt)
- [Native filter A/B, one million rows](pdsh-performance-results/native-constants-filter-million-ab.txt)
