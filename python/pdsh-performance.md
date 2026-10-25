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

The expanded adapter supports **8 of 22 queries: Q1–Q6, Q12 and Q18**.
Q2, Q3, Q5, Q12 and Q18 are newly supported. All eight pass complete result
comparisons against both standard Polars and Polars streaming on the unchanged
SF10 Parquet inputs, at one and eight matched workers. The performance table
above remains the earlier three-query baseline; the subsequent eight-query
performance check below publishes the expanded timing results.

The remaining 14 queries fail explicitly before native execution. These are
first blockers, not exhaustive inventories per query:

| Queries | First translation blocker |
|---|---|
| Q7, Q17, Q19, Q20 | Polars visitor cannot expose joins with fused predicates |
| Q8, Q10, Q14 | Floating decimal rounding (`round`, half-to-even) |
| Q9 | Fused arithmetic (`fms`) |
| Q11, Q22 | Inequality joins (`IEJoin`) |
| Q13, Q16 | Regular-expression operators in string `contains` |
| Q15 | Reductions broadcast inside a filter |
| Q21 | Expression-level filtered aggregation (`Filter`) |

The new translations cover compatible string representations, literal
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

Use `--queries 2 3 5 12 18` for just the newly supported queries. The JSON keeps
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
