# PDS-H through the Polars engine adapter

Measured 2026-10-25 on an Intel i7-13700, Linux, Release build, with CPU frequency
locked at 3.2 GHz. These use the upstream Polars query functions on
upstream-generated scale-10 Parquet data (59,986,052 lineitem rows and
15,000,000 orders). This compares `query.collect` using
Polars 2.0.0's in-memory executor, its streaming executor, and `IbexEngine`.
It does not substitute the native hand-written Ibex PDS-H queries.
The latest Q16 runs use a local correction from `contains("MEDIUM POLISHED*")`
to `starts_with("MEDIUM POLISHED")`; its patch is included in the new evidence.

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

The adapter now supports **all 22 queries**, checked against both Polars
executors on SF10 at one and eight threads. The final additions are Q11 and Q22
(numeric inequality joins with a provably singleton input), Q22's Unicode
string slicing and literal alternatives, and Q21's filtered distinct counts.
General inequality joins and other filtered aggregates remain unsupported.
Q13 and the locally corrected Q16 use the temporary literal-`.*` to LIKE rule
described below. Q15 adds reduction broadcasting inside filters.
The preceding additions were Q7, Q8, Q9, Q10, Q14, Q17, Q19 and Q20. All eight
additions pass complete result comparisons against both in-memory Polars and
streaming Polars on unchanged SF10 Parquet inputs, with matched one- and
eight-thread budgets. The earlier eight queries retain their correctness and
performance evidence below. The new-query measurements are reported next.

Coverage is for these query shapes and accepted boundary policies; it does not
claim complete Polars API compatibility. Q16 retains the explicit local prefix
correction, and the temporary LIKE approximation can match across newlines.

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

[Full SF10 checks and coverage](pdsh-performance-results/complete-22-checks.json)
retain complete-result check outcomes, generated sources, query hashes and
mutation evidence. The configured Python and upstream conformance tests pass.

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

## Completing the suite: Q11, Q21 and Q22

All 22 queries now pass complete result comparisons on SF10 at one and eight
threads. Q16 retains the local prefix correction documented above. The final
translations are deliberately bounded:

- Q11 and Q22's `IEJoin` nodes compare against a global one-row aggregate.
  The adapter proves that at least one input has at most one row, then emits
  a cross join plus numeric predicates. This is linear in the other input's
  size. General many-row inequality joins still fail before native binding.
- Q22's literal `|` alternatives become ORs of LIKE matches, capped at 256.
  String slicing uses native Unicode-codepoint substring with an adjustment
  for Polars' clipping of negative-start intervals.
- Q21's filtered distinct count masks rejected values for native distinct
  counting, then adds a separate flag for a selected null. Rejected rows and
  null predicates do not introduce an extra distinct null. Other filtered
  aggregates remain unsupported. UInt32 count literals use Int64 storage;
  UInt32 input columns remain unsupported.

Q21 exposed a native scheduler deadlock after translation. Multiple bounded
scan producers occupied every pool thread, while a stage waited for queued
workers of its own exhausted scan to retire. A waiting stage now assists only
tasks from that exact batch, skipping unrelated producers that could park on
another ring. A deterministic two-worker regression fails before the fix and
passes afterward; it also covers unbarriered task groups. This fix applies to
Ibex's runtime generally, without adding a language keyword or query special case.

The benchmark runner now infers the query scale from `scale-N`, or accepts
`--scale-factor`. Q11's upstream default was 1 even with SF10 files; its proper
SF10 threshold produces **8,685 rows**, rather than the misleading empty result.
Q11 sorts only by value: two part keys tied at 8,481,255.12 reverse order between
Polars' own executors. Validation checks descending values and complete keyed
rows, allowing only tie reordering for Q11; other queries retain strict order
checks. No query is rewritten to add a tie-breaker.

The newly admitted count literals also exercised an upstream zero-width-frame
test. Export now preserves the native row count through a null-only height
carrier before dropping its column, instead of losing height in Arrow-to-Polars
conversion. All 2,378 fast CTest cases finish without failures, including the
Python/upstream conformance tests; four network cases use their existing skip
guards. Interpreter/transpiled parity also passes. GCC, WebAssembly and
the required plugin rebuild/load check pass. The final lock-style cleanup
produces a byte-identical native object.

Warm collection medians, measured 2026-10-26 on the 3.2GHz host with SF10 query
parameters. Twelve rounds balance all six execution orders after two warmups.
Preparation and full correctness comparisons are outside timing. One thread
uses CPU 2; eight use CPUs 0,2,4,6,8,10,12,14. Release was rebuilt before each
measurement and no tests or builds ran alongside timing.

| Threads | Query | Polars in-memory ms | Polars streaming ms | Polars–Ibex ms |
|---|---|---:|---:|---:|
| One | Q11 | 781 | 420 | 820 |
| One | Q21 | 6,351 | 4,662 | 6,644 |
| One | Q22 | 555 | 451 | 1,207 |
| Eight | Q11 | 146 | 78 | 162 |
| Eight | Q21 | 1,087 | 688 | 3,589 |
| Eight | Q22 | 111 | 85 | 534 |

Functional coverage is complete; performance parity is not. At eight threads,
Q11 is 2.1× slower than streaming Polars, Q21 5.2× and Q22 6.3×. These are
targets for further investigation, not resolved performance issues.

The native filter/join regression fixture uses twelve interleaved pairs against
`ca71a346`, the runtime before stage assistance. Its eight-case sum changes by
-0.18% at one worker and -0.77% at eight. All cases fit the script's IQR noise
band, but unadjusted paired Wilcoxon tests flag two small slowdowns: one-worker
categorical join (median 7.2455 → 7.3465 ms; paired change +0.93%, p=0.021), and
eight-worker OR filter (0.205 → 0.219 ms; paired change +4.43%, p=0.035).
The one-worker integer join and AND filter improve in those tests. These
per-case shifts remain recorded; a flat suite total does not resolve them.

- [All 22 SF10 checks at both budgets](pdsh-performance-results/complete-22-checks.json)
- [Final three queries, one thread](pdsh-performance-results/completion-three-one.json)
- [Final three queries, eight threads](pdsh-performance-results/completion-three-eight.json)
- [Native regression comparison](pdsh-performance-results/completion-native-regression.txt)
- [Native paired samples and statistics](pdsh-performance-results/completion-native-statistics.json)
- [Validation, scheduler diagnosis and profiles](pdsh-performance-results/completion-evidence.json)

## Q13 and Q16: temporary ordered-literal matching

Regex-mode `contains` now recognizes literal fragments separated by `.*` and
translates them to native LIKE `%`. It retains fragment order, escapes literal
LIKE metacharacters, and rejects other regex operators. This is explicitly a
temporary approximation: `%` may cross newlines whereas regex dot normally
cannot. A regression test pins that accepted difference. Literal-mode matching
continues to treat `.*` literally. No regex engine or C++ changes are introduced.

Q13 uses `special.*requests`; Q16 uses `.*Customer.*Complaints.*`. Q16's other
predicate is locally corrected to `starts_with("MEDIUM POLISHED")` in the
benchmark checkout. The benchmark runner now captures local query patches as
well as upstream HEAD and per-query source hashes, so the variation remains
explicit. The full SF10 results of both queries match Polars at one and eight
threads, as do the other 17 supported queries. All nine Python/conformance
CTest groups pass. At this stage the remaining unsupported queries were Q11,
Q21 and Q22; the completion above supersedes that coverage count.

Warm medians on SF10, measured 2026-10-26 with twelve rounds, two warmups and
all six execution orders balanced. The host remains locked at 3.2GHz; one
thread is pinned to CPU 2, eight to CPUs 0,2,4,6,8,10,12,14. Preparation and full
result comparisons are outside timing, and no builds or tests run alongside
the measurements. Q16 uses the corrected prefix predicate in both engines.

| Threads | Query | Polars in-memory ms | Polars streaming ms | Polars–Ibex ms |
|---|---|---:|---:|---:|
| One | Q13 | 4,034 | 3,013 | 3,793 |
| One | Q16 | 456 | 433 | 383 |
| Eight | Q13 | 845 | 447 | 741 |
| Eight | Q16 | 96 | 115 | 119 |

Q13 remains 26% slower than streaming Polars at one thread and 66% slower at
eight, while outperforming its in-memory executor. This extension enables the
query; it does not resolve that performance gap. Q16 is close to streaming
Polars at eight threads and faster at one.

- [Full 22-query checks at both budgets](pdsh-performance-results/expanded-19-checks.json)
- [One-thread Q13/Q16 samples](pdsh-performance-results/q13-q16-literal-patterns-one.json)
- [Eight-thread Q13/Q16 samples](pdsh-performance-results/q13-q16-literal-patterns-eight.json)
- [Validation and local Q16 patch](pdsh-performance-results/q13-q16-literal-patterns-evidence.json)

## Q15: reductions inside filters

The Python lowerer now supports global reductions inside filter predicates.
It shares the current input, computes the requested reductions in one native
aggregate, extracts lexical scalars, and applies the predicate against that same
input. All predicates in one filter see its pre-filter rows; consecutive filters
reduce their own inputs. Generated names avoid input-column collisions. Existing
NaN, null, empty-reduction and narrow-type policies remain in use. Nested
reductions and mixed reduction/row projections remain unsupported.

This enables the unchanged upstream Q15, including all ties for maximum supplier
revenue. Native profiles show the supplier/revenue join executing once and
producing 100,000 rows, followed by the one-row global reduction. The entire
query executes in one native bridge call, without Python result extraction.
No C++ engine changes are needed.

Measured 2026-10-26 on the same SF10 inputs and 3.2GHz host. These are medians
of twelve rounds after two warmups; all six route orders are balanced. Preparation
and complete result checks are outside timing. One thread is pinned to CPU 2;
eight threads use CPUs 0,2,4,6,8,10,12,14. The full Release tree was rebuilt before
each measurement; no builds or tests ran alongside the timed collections.

| Threads | Polars in-memory ms | Polars streaming ms | Polars–Ibex ms |
|---|---:|---:|---:|
| One | 1,904 | 1,869 | 1,784 |
| Eight | 284 | 261 | 284 |

At that stage, all 17 supported queries matched both Polars executors on SF10 at one and eight
threads. All nine Python/conformance CTest groups pass. Regression tests cover
ties, empty/all-null inputs, NaNs, scalar-only predicates, multiple reductions,
consecutive-filter scope, temporary-name collisions and one-call execution.

- [Full 22-query coverage checks at both budgets](pdsh-performance-results/expanded-17-checks.json)
- [One-thread Q15 samples](pdsh-performance-results/q15-filter-reductions-one.json)
- [Eight-thread Q15 samples](pdsh-performance-results/q15-filter-reductions-eight.json)
- [Q15 source, operator profile and validation](pdsh-performance-results/q15-filter-reductions-evidence.json)

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
String matching escapes native LIKE metacharacters; the later compatibility
rule above also accepts literal `.*` separators. Literal membership accepts at most 256
scalar values and adds no list/struct column support. At that stage, cached
deterministic subplans could be recomputed by native execution; the later
cache-identity follow-up below replaces this with shared native bindings.
Floating group keys normalize NaNs for equality and return original
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
performance score; completed coverage and the subsequent measurements appear above.

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

## Q22: initial bottleneck investigation

Release profiling at SF10, pinned to eight P-cores, identifies the two
country-code substring updates as the first target: 196.7 and 195.6 ms of
self time out of 581.4 ms native execution. The anti join accounts for
80.7 ms of next time (including its input wait); the singleton cross join
adds only 1.6 ms of build time. Pool occupancy is 20.5%. These are diagnostic
profile measurements, not replacement benchmark medians. Complete output
matches Polars streaming at both one and eight threads.

The read-only physical inspection of the customer substring update reports
`serial(no leading parallel-map step)`. The native substring builtin has no
column kernel: its scalar implementation allocates a codepoint-offset vector
and walks the entire string for each row, even for a positive two-character
prefix. The two customer branches independently perform this computation.
At one thread their self times remain 200.3 and 200.2 ms, consistent with the
serial bottleneck seen at eight threads. Profile row counts can include
forwarded calls through wrappers and are not evidence of extra source reads.

A controlled experiment keeps both customer variants in memory and leaves
orders in Parquet. Shortening phone strings to their first two characters
before collection leaves the final result unchanged, while still executing
the same substring expression. Eight-sample medians are 505.2 ms for full
phone strings and 378.0 ms for shortened strings. This sequential diagnostic
is not a paired optimization benchmark; it supports targeting the substring
walk but also shows that avoiding the full walk alone will leave substantial
scalar-dispatch and other query costs.

The recommended first change is a native substring column kernel that reads
only the required codepoints for nonnegative bounded slices, preserves Unicode
and null semantics, and can run across morsels. Sharing the filtered customer
branch and improving the seven LIKE alternatives are subsequent opportunities.
No execution change is included in this investigation. Raw profiles, physical
inspection and control samples are in
`pdsh-performance-results/q22-investigation.json`.

## Native substring: bounded scans without an offset array

Commit `96f2a981` removes the per-call codepoint-offset allocation from native
substring. Nonnegative starts scan only to the requested start and end; an
omitted length copies the suffix without walking it first. Negative starts
count codepoints, then locate the slice with the same forward decoder.
Nonpositive lengths return immediately. This preserves the existing Unicode,
clamping, null and malformed-byte behavior. It benefits both frontends; no
Polars-specific execution change is involved. The scalar dispatch path remains
and no new column kernel is included.

The saved before/after Release bridges use identical Python adapters and SF10
inputs. Twelve paired rounds balance all six before/after/Polars route orders;
all complete outputs match the Polars reference. Only the clean eight-thread
rerun is reported below; an earlier exploratory run overlapped a plugin build
and is excluded. Pinning is CPU 2 for one thread and CPUs
0,2,4,6,8,10,12,14 for eight. Warm collection medians, milliseconds:

| Workload | Threads | Before | After | Polars streaming | Median paired change |
|---|---:|---:|---:|---:|---:|
| Full Q22 | 1 | 1,192.8 | 1,030.5 | 463.8 | −13.8% |
| Full Q22 | 8 | 536.1 | 384.7 | 84.1 | −28.3% |
| Customer phone prefix only | 1 | 265.2 | 184.5 | 63.2 | −31.5% |

The focused prefix query reads all 1.5 million customer phones from Parquet,
extracts their first two codepoints, and returns every prefix. It includes
decode and result export, rather than timing only the helper. All three
comparisons improve in every pair; two-sided exact signed-rank p=0.000488
for each. The remaining gap includes scalar evaluation, repeated customer
work and LIKE filtering; this change does not resolve those costs.

The required `compare_ibex_git.sh` regression check compares `6a5a75d8` to
the candidate on the scalar, filter and join suites, using twelve interleaved
repeats, seven inner iterations and two warmups, pinned to eight P-cores.
The sum of the 20 query medians changes by +0.54%; every query is classified
as noise by the harness, and none has an unadjusted exact signed-rank p<0.05.
This is a regression screen, not evidence that unrelated operators improve.

Validation: all 2,379 fast CTest entries pass (four network-dependent skips);
substring tests cover 97 assertions including extreme bounds and truncated
UTF-8. Existing tests also cover null propagation and serial/parallel result
identity. The CSV plugin is rebuilt using `scripts/ibex-plugin-build.sh` and
a fresh load returns the expected count and sum. Formatting and diff checks
pass.

Raw before/after sources, bridge hashes, samples and the focused benchmark
driver are preserved in `pdsh-performance-results/q22-substring-{one,eight,prefix-one}.json`.
Native regression results and exact paired statistics are in
`q22-substring-native-regression.txt` and `q22-substring-statistics.json`
in the same directory.

## Preserve Polars common-subplan caches in native lowering

Commit `6276e6f7` carries the Polars visitor's `Cache.id_` through the immutable
frontend graph. Different visitor node IDs with the same cache identity now
refer to one native `let` binding. Lowering emits dependencies before their
consumers, including nested caches, and preserves independent cache IDs.
This uses Polars' own common-subplan detection; there is no duplicate-plan
detector in the adapter. The change is confined to Python translation.

Q22's generated source now contains one customer scan, substring update and
country filter, bound as `_ibex_cache_0` and reused for the mean and anti join.
The complete query still uses one native pipeline call. Shared results live
only during that call: subsequent collections reread Parquet and recompute
the shared subtree. The engine's prepared-plan cache never retains these
intermediate results.

The paired comparison uses the same Release bridge in both processes,
before-Python packages from `8e901bf4`, and after-Python packages from
`6276e6f7`. Twelve rounds balance all six before/after/streaming route orders.
Workers are pinned to CPU 2 at one thread and CPUs 0,2,4,6,8,10,12,14 at eight;
SF10 inputs, warm file caches and the same Polars reference checks apply.
Warm collection medians, milliseconds:

| Threads | Before | Shared cache | Polars streaming | Median paired change |
|---|---:|---:|---:|---:|
| 1 | 1,034.3 | 659.2 | 460.9 | −36.1% |
| 8 | 379.5 | 214.5 | 81.0 | −43.9% |

Both comparisons improve in every pair, with two-sided exact signed-rank
p=0.000488. Every complete output matches the reference, including order
and dtypes. These gains are additional to the preceding substring change.
An operator-profile diagnostic shows the substring update in the cache
materialization statement, with no second substring update in the consumers.
Profile wrapper row counts can include forwarded calls and must not be read
as exact source-decode counts.

Validation covers shared branches with independent updates, nested caches,
independent cache identities, null strings, empty results and fresh Parquet
contents on repeated collections. All 118 directly selected Python unit
tests and all nine Python/conformance CTest groups pass. Full SF10 correctness
checks pass for all 22 PDS queries at both one and eight threads, against
both Polars executors. These checks are separate from the timing runs.

`pdsh-performance-results/q22-cache-{one,eight}.json` retain before/after
generated source, native bridge hashes, Python file hashes, paired samples
and the comparison driver. `q22-cache-evidence.json` contains the profile,
test summaries and paired statistics; `q22-cache-complete-{one,eight}.json`
contain the complete-suite correctness checks.

## Complete 22-query comparison after shared-cache lowering

The later Q10 follow-up below changes its floating-key representative reducer.
The complete-suite tables and geometric means here describe this measured
sweep; they do not combine timings from later comparisons.

Measured 2026-10-26 at `5ec3c8c6`, with the complete Release tree rebuilt.
SF10, i7-13700 locked to 3.2 GHz; eight threads pinned to physical P-cores
0,2,4,6,8,10,12,14. All 22 query results match both Polars executors,
including order and dtypes, with the previously documented Q11 tie rule.
The agreed Q16 prefix correction and temporary literal-pattern compatibility
remain in use; the upstream patch and source hashes are retained in the raw report.

Each query prepares outside timing, collects once for validation, warms each
route twice, then collects in six timed rounds covering all six executor
orderings. Every measured result is checked in full. Parquet decode, native
execution and public result export are timed; preparation, correctness checks
and prior-result destruction are excluded. These are warm collection medians
in milliseconds. First-per-query timings in the raw report are not
fresh-process measurements.

### Eight threads

| Query | Polars in-memory | Polars streaming | Polars–Ibex | Ibex / streaming |
|---|---:|---:|---:|---:|
| Q01 | 813.4 | 569.3 | 834.5 | 1.47× |
| Q02 | 50.4 | 64.0 | 72.0 | 1.12× |
| Q03 | 441.2 | 348.9 | 407.2 | 1.17× |
| Q04 | 307.2 | 173.4 | 260.9 | 1.50× |
| Q05 | 529.5 | 384.9 | 469.5 | 1.22× |
| Q06 | 222.0 | 221.3 | 218.4 | 0.99× |
| Q07 | 491.2 | 407.2 | 512.3 | 1.26× |
| Q08 | 604.4 | 457.1 | 521.2 | 1.14× |
| Q09 | 1,336.2 | 619.3 | 1,068.8 | 1.73× |
| Q10 | 551.1 | 465.2 | 1,194.0 | 2.57× |
| Q11 | 140.7 | 80.9 | 98.9 | 1.22× |
| Q12 | 203.0 | 177.6 | 192.0 | 1.08× |
| Q13 | 837.1 | 437.5 | 739.2 | 1.69× |
| Q14 | 295.7 | 277.5 | 323.8 | 1.17× |
| Q15 | 275.0 | 259.6 | 284.1 | 1.09× |
| Q16 | 95.1 | 106.8 | 113.7 | 1.06× |
| Q17 | 278.6 | 222.1 | 283.7 | 1.28× |
| Q18 | 1,454.4 | 528.6 | 915.1 | 1.73× |
| Q19 | 297.0 | 303.1 | 405.8 | 1.34× |
| Q20 | 807.7 | 440.3 | 554.5 | 1.26× |
| Q21 | 1,092.8 | 691.5 | 1,067.3 | 1.54× |
| Q22 | 108.9 | 77.9 | 217.4 | 2.79× |

The geometric mean of per-query Ibex/Polars ratios is **1.02× versus in-memory
Polars** and **1.37× versus streaming Polars** (larger means slower). Summing
the 22 individual medians gives 11.233 s for Polars in-memory, 7.314 s for
Polars streaming and 10.754 s for Polars–Ibex; this sum is not a separately
timed sequential suite run. Ibex has a lower median than in-memory Polars
on 12 queries. Only Q6 has a lower median than streaming Polars, and its
small difference should not be treated as an established win.

The largest relative gaps against streaming are Q22 (2.79×) and Q10 (2.57×),
followed by Q9 and Q18 (both 1.73×). The largest absolute excess is Q10
(729 ms), followed by Q9 (449 ms), Q18 (386 ms) and Q21 (376 ms). This
comparison identifies priorities; it does not establish the causes of those costs.

Raw sources, samples, CPU timings and complete run metadata are in
`pdsh-performance-results/all22-eight.json`. Paired ratios and unadjusted
exact signed-rank statistics are in `all22-statistics-eight.json`. Six rounds
provide a broad survey: small differences need more repeats before making
per-query performance claims. No builds or tests ran during collection.

### One thread

Same Release bridge and Python translation, pinned to CPU 2, with six timed
rounds and complete result checks for every query. Warm medians, milliseconds:

| Query | Polars in-memory | Polars streaming | Polars–Ibex | Ibex / streaming |
|---|---:|---:|---:|---:|
| Q01 | 5,627.8 | 4,073.8 | 4,153.9 | 1.02× |
| Q02 | 307.6 | 250.9 | 321.5 | 1.28× |
| Q03 | 3,184.9 | 2,406.8 | 2,558.4 | 1.06× |
| Q04 | 1,972.5 | 1,214.3 | 1,489.8 | 1.23× |
| Q05 | 4,063.9 | 2,793.3 | 3,059.2 | 1.10× |
| Q06 | 1,501.0 | 1,486.7 | 1,513.3 | 1.02× |
| Q07 | 3,497.2 | 2,944.1 | 2,985.3 | 1.01× |
| Q08 | 4,813.4 | 3,433.2 | 3,552.0 | 1.03× |
| Q09 | 8,948.3 | 4,284.8 | 6,812.9 | 1.59× |
| Q10 | 3,360.2 | 3,361.5 | 4,380.4 | 1.30× |
| Q11 | 821.6 | 450.1 | 487.4 | 1.08× |
| Q12 | 1,467.3 | 1,229.1 | 1,288.5 | 1.05× |
| Q13 | 4,415.8 | 3,237.5 | 4,067.0 | 1.26× |
| Q14 | 2,237.3 | 2,175.4 | 2,249.9 | 1.03× |
| Q15 | 2,010.1 | 1,980.3 | 1,906.7 | 0.96× |
| Q16 | 476.8 | 450.6 | 400.4 | 0.89× |
| Q17 | 2,105.4 | 1,544.4 | 1,948.7 | 1.26× |
| Q18 | 6,552.8 | 3,404.7 | 2,200.4 | 0.65× |
| Q19 | 2,244.0 | 2,146.9 | 2,912.2 | 1.36× |
| Q20 | 4,225.8 | 2,857.5 | 3,110.7 | 1.09× |
| Q21 | 6,451.9 | 4,719.3 | 4,494.5 | 0.95× |
| Q22 | 560.0 | 452.1 | 661.9 | 1.46× |

The geometric mean of per-query runtime ratios is **0.84× versus in-memory
Polars** and **1.10× versus streaming Polars**. Ibex has lower medians on
16/22 queries versus in-memory Polars and 4/22 versus streaming Polars; these
counts are descriptive and do not establish small per-query wins. Sums of
individual query medians are 70.846 s, 50.897 s and 56.555 s for in-memory
Polars, streaming Polars and Polars–Ibex respectively.

**Q9 rerun:** the original single-thread Q9 interval coincided with an observed
host load average above 30. Its medians inflated to 21.617 s, 11.837 s and
18.063 s across the three executors, versus the earlier correctness-run
collections of roughly 8.9 s, 4.3 s and 7.5 s. The entire six-round Q9
comparison was repeated in a fresh process after load settled below 2; all
three routes returned to the timings shown above, with all outputs matching.
The table uses that complete rerun, without selecting individual samples.
This observation does not identify which activity caused the slowdown.

`all22-one.json` is the combined report with an explicit Q9 override record.
`all22-one-original.json` preserves the original full run, and
`all22-one-q09-rerun.json` preserves the separate comparison.
`all22-statistics-one.json` records paired ratios, unadjusted exact signed-rank
statistics and the calculation driver. All are in `pdsh-performance-results/`.
No native executable or Python implementation changed between the two budgets
or the Q9 rerun; current file hashes match both reports.

### Scaling and remaining gaps

Against streaming Polars, the geometric-mean ratio grows from 1.10× at one
thread to 1.37× at eight. For example, Q10 scales 3.67× in Ibex versus 7.23×
in streaming Polars; Q18 scales 2.40× versus 6.44×; Q22 scales 3.04× versus
5.80×. These measurements show where the relative gap grows with the thread
budget; they do not establish whether the cause is scheduling, serial work,
contention, or a different algorithm. Q9 remains slower at both budgets
(1.59× at one thread and 1.73× at eight).

For the large single-thread Q18 advantage, a separate first-collection check
uses one fresh Python process per executor, with preparation outside timing
and full result validation afterward. It measures 6.546 s for in-memory
Polars, 3.422 s for streaming Polars and 2.675 s for Polars–Ibex, versus
warm medians of 6.553 s, 3.405 s and 2.200 s. The advantage persists but is
smaller on a fresh process. This is one observation per executor, not a
statistical estimate. The driver and results are in
`pdsh-performance-results/all22-fresh-q18-one.json`.

## Q10: remove a generated null-preserving reducer from floating keys

Commit `6b706267` changes only Python translation. Q10 has seven logical
grouping keys, including `c_acctbal`. The adapter normalizes a floating key
into a NaN flag and a normalized value, then reduces the original value to
return the original group label. Its generated `first_null(c_acctbal)` made
`aggregate_is_streamable` decline the entire aggregate. The first profile
attributes 570.6 ms of 1,292.3 ms native execution to the materialized aggregate,
with no worker tasks of its own. It groups 1,147,084 joined rows into 381,105
groups before the final top 20.

The representative can safely use null-skipping `first`:

- Null keys have a null normalized value and are grouped separately. Every
  original value in that group is null; either reducer returns null.
- NaNs have a true flag and normalized zero. Every original value in that
  group is non-null, so both reducers preserve the first NaN payload.
- Ordinary values, including equal signed zeros, are non-null. Both reducers
  preserve the same first value and zero sign.

The adapter therefore uses its internal `first_non_null` operation for this
generated label only. Ordinary Polars `first()` translation continues to
preserve nulls. Read-only physical inspection changes from
`Aggregate(MaterializeAll)` to the streaming/hash-fallback aggregate phase
chain with parallel-capable discovery, accumulation, ordering and emission.
A controlled source-only replacement preserves Q10's complete output and
measures 1,182 → 731 ms at eight threads before the production change.

Two persistent workers then compare the before/after Python packages with
**identical Release bridges**, SF10 inputs and captured scalar bindings.
Twelve paired rounds cover every three-route ordering twice; preparation and
validation are outside timing. CPU 2 is used for one thread; physical P-cores
0,2,4,6,8,10,12,14 for eight. Warm collection medians, milliseconds:

| Threads | Before | After | Polars streaming | Median paired change |
|---|---:|---:|---:|---:|
| 1 | 4,136.4 | 3,965.8 | 3,151.8 | −3.8% |
| 8 | 1,172.2 | 719.3 | 475.0 | −38.5% |

Q10 improves in every pair at both budgets (two-sided exact signed-rank
p=0.000488 each). The remaining eight-thread runtime is 1.51× streaming
Polars, down from 2.47× in the same comparison. This removes a major serial
barrier; it does not explain or resolve all remaining decode/join costs.

Q10 is the first query in each newly launched comparison worker. Its first
native collections are 4,310 → 4,149 ms at one thread and 1,292 → 812 ms at
eight, so the gain also appears before repeated native collections. These
are single observations after preparation and a Polars reference collection;
they are not cold-storage or process-startup timings.

Q18 is the only other suite query using the generated floating-key helper.
Its one-thread medians are 2,070.7 → 2,047.8 ms (median paired −1.35%,
11/12 faster pairs, unadjusted p=0.0161). At eight threads they are
903.1 → 894.5 ms (median paired −0.13%, 6/12 faster pairs, p=0.519), an
unclear effect. Its final grouping is small, so the large Q10 benefit should
not be generalized to Q18.

The later diagnostic profile confirms aggregate discovery now submits worker
tasks. Its durations are excluded from before/after interpretation: the host
load rose above 26, then 40, and all three executors slowed substantially.
Those conditions occurred after the paired timing runs. The profile is
retained as reachability evidence rather than a second performance result.

The added regression fails against the old translation and passes after the
change. It checks Float32/Float64, all-null groups, distinct NaN payloads and
both orders of signed zero. Existing tests cover mixed keys and empty inputs.
All nine Python/conformance CTest groups pass; a complete SF10 eight-thread
check passes all 22 queries against both Polars executors. Both changed queries
also pass all one-thread paired checks. The other 20 queries have byte-identical
generated source relative to the preceding full-suite run. No runtime code
or public API changes are included.

Samples, generated sources, bridge/Python hashes and the driver are in
`pdsh-performance-results/q10-{one,eight}.json`; exact paired statistics are
in `q10-statistics.json`. `q10-evidence.json` contains the routing inspection,
profiles, source-control driver and regression/test evidence.
`q10-complete-eight.json` records the full-suite check.

## Q22: remaining work after substring and cache fixes

Investigation only; no implementation change. Current Q22 generated source is
byte-identical to the preceding complete-suite sweep. The last clean
shared-cache profile remains applicable: about 124 ms in the serial substring
update, 32 ms in the country-filter stage, and 50 ms in the consumers
(including orders decoding and the anti join). The host is busy during this
follow-up, so no new timing claim is made.

The next opportunity is to do the country test before deriving the prefix.
All seven alternatives are two ASCII characters and the slice starts at zero
with length two. For this shape, searching the sliced prefix for an
alternative is equivalent to testing whether the original phone starts with
that alternative. Full selected-frame checks confirm both formulations agree
on SF10. Filtering phones first would reduce substring input from 1,500,000
to 419,974 rows: **72% fewer slices**, independent of how fast each slice is.
This does not undo shared-cache reuse: the selected country subtree can still
be computed once and shared by the average and anti-join branches.

The country test itself currently lowers to seven separate LIKE calls and an
OR tree. Because the bounded prefix and every alternative have the same
length, it is also an exact membership test on the prefix. A single lookup,
or a fused prefix-membership predicate before slicing, can avoid repeated
matching and intermediate masks. This equivalence requires the proven bounds;
general `contains` expressions must retain substring semantics.

A second opportunity is to compute the average from the unchanged shared
country subtree, then apply `c_acctbal > average` on the anti join's left
branch before joining orders. Complete sorted frame comparisons confirm the
reordering preserves the selected rows:

| Work item | Current | With earlier balance filter |
|---|---:|---:|
| Anti-join input customers | 419,974 | 190,691 |
| Anti-join output customers | 140,489 | 63,914 |

This removes about 55% of left-side probes, but still requires the same
15-million-row orders membership build. The singleton cross join itself
was only around 1.6 ms in the clean profile, so removing that node alone
is not the main target.

The priority is therefore country-prefix filtering and its placement, followed
by the earlier balance predicate. A native substring column kernel remains
a separate general improvement: the earlier fix removed the offset array,
but scalar argument construction and dispatch still occur per row.
Cardinality counts, full-frame equivalence checks and the inspection driver
are in `pdsh-performance-results/q22-remaining-evidence.json`.

## Q22: substring bounds bound as scalars now take the direct plan

A fresh profile on an idle host showed the serial substring update was
the largest single cost (114 ms of a 177 ms customer statement at eight
threads). That update was serial because of where its constants came from,
not because of its kernel: the adapter binds `start` and `length` as
scalars (`^_ibex_scalar_1`), and `make_direct_string_plan` accepted only
`Int` literals. The update therefore declined the direct, prefix-assigned
parallel string plan and ran the per-row scalar evaluator on one thread.
Bound `Int` scalars are now resolved like literals. A column of the same
name still shadows a non-lexical reference, and row-varying bounds still
decline.

| Q22, SF10 | Before | After | Polars streaming |
|---|---:|---:|---:|
| eight threads, median ms | 219.1 | 116.4 | 75.8 |
| one thread, median ms | 651.9 | 642.0 | 443.1 |

The customer statement falls from 177 ms to 65 ms at eight threads. The
substring self time drops from 114 ms to 17 ms, and the update now uses the
pool. The one-thread gain is small, which confirms the eight-thread win is
parallelism and not a cheaper slice. The remaining eight-thread cost is the
seven-LIKE country filter (~32 ms), the orders decode and the anti join
(~48 ms together). The prefix-first and earlier-threshold rewrites above
target those. Samples and profiles are in
`pdsh-performance-results/q22-scalar-substring-bounds.json`.

## Q22: country filter below the prefix slice, and borrowed string ranges

Two changes that only pay off together.

**Adapter: filter before a leading slice.** When a filter reads a column
`c = slice(s, 0, n)` only through LIKE-shaped tests (`contains`,
`starts_with`, `ends_with` or literal alternatives) whose literal text is
exactly `n` codepoints, each test becomes `like(s, 'L%')`. That is
equivalent row by row, including nulls, because `c` has at most `n`
codepoints. The filter is then applied below the slice, so Q22 slices
419,974 phones instead of 1,500,000. A literal of another length, any other
use of `c`, or a nonzero start keeps the original placement.

**Runtime: ranged column kernels borrow String inputs.** For a partial
row range, `evaluate_field` used to copy each referenced column with
per-row `push_back` before calling a column kernel such as `like`. With the
filter now fused into the scan's ranged path, Q22 paid that copy once per
alternative, seven times per range. String columns are now passed as an
external view that shares the source column's ownership.

| Q22, SF10 median ms | neither | pushdown only | borrow only | both | Polars streaming |
|---|---:|---:|---:|---:|---:|
| eight threads | 116.4 | 123.1 | 118.5 | **88.4** | 79.8 |
| one thread | 642.0 | 486.3 | 644.6 | **496.1** | 440.9 |

Pushdown alone wins at one thread but loses at eight, where the ranged
copies dominate. Borrowing alone does nothing for Q22. All 22 queries still
match both Polars executors. An earlier in-process A/B that toggled the
rewrite under one `IbexEngine` was invalid: the prepared-plan cache served
the first translation to both arms. Compare states in separate processes.
Samples are in `pdsh-performance-results/q22-prefix-pushdown.json`.

Remaining at eight threads: the orders decode and the anti join (~48 ms).
Applying the balance threshold before the anti join is next.

## Q22: the anti join probes orders on the scan workers (and Q01 streams fully)

A fresh profile showed the anti join's consumer statement (~42 ms at eight
threads) draining all 15M order keys, then scanning them in one burst after
decode (`scan_kept_right`, 14.7 ms serial plus a 13.6 ms barrier). The
streamed swapped build already exists: the operator buffers the small left
side and has each orders chunk probed by the worker that decoded it. Q22
never reached it, for two independent reasons:

* **The sink needed both sides to be lazy scans.** Q22's left side is the
  materialized `_ibex_cache_0` binding. `chain_source_rows` now also sizes an
  in-memory table, the cheapest left of all to drain first.
* **The right chain could not stream to the probe.** The adapter lowers
  each renaming `select` to an Update and a Project, so the join key reached
  the join as Project, Update, Project, Update over the orders scan. The
  planner only admitted updates directly above a streamed run and stopped
  at the first projection. The steps above ran serially, and the sink was
  withdrawn. Metadata-only projections and renames now come along with the
  updates around them, as long as at least one update does.

The second change is general. Q01's computed columns have the same shape
over lineitem.

| SF10, eight threads | Before | After | Rounds faster |
|---|---:|---:|---:|
| Q01 | 822.3 ms | 649.7 ms (−21.0%) | 5/5 |
| Q22 | 87.8 ms | 82.3 ms (−6.3%) | 5/5 |
| Q06 | 218.6 ms | 218.3 ms | 3/5 |
| Q07 | 503.5 ms | 505.6 ms | 2/5 |

Q22 at one thread is −1.0% (5/5 rounds). These are medians of five
interleaved rounds, alternating the two bridge builds in separate processes.
One sequential all-22 pass per state showed no other query moving beyond
Polars' own drift, and all 22 match. Polars streaming takes ~76–82 ms on Q22
in these sittings. Data is in
`pdsh-performance-results/q22-streamed-anti-probe.json`.

The earlier balance threshold before the anti join was also measured, by
hand-reordering the query (not committed): no change at eight threads, −8% at
one. Doing it properly needs a planner rule that swaps a singleton cross join
below the anti join, seen through the adapter's projections, so it is
deferred.
