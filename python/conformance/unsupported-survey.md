# Unsupported-feature survey

This survey analyzes the initial pinned Polars 2.0.0 run described in
[README.md](README.md), plus the adapter's explicit capability checks. It does
not change engine support. There were 1,578 rejected lazy collections across
330 tests, 52 matching collections, and zero mismatches. Another 730 tests
never exercised Ibex.

Counts below describe **the first rejection reached per collection**, not
independent feature counts, workload popularity, or the number of tests that
implementing a feature would make pass. Parametrization and repeated collects
amplify counts. A single query can contain several unsupported features.

| First blocker | Rejected collections | Observed gaps |
|---|---:|---|
| Input dtypes | 1,144 | Other integer widths, Float32, temporal types, Decimal, Struct, Null |
| Plan operations | 271 | MapFunction 259; Distinct 5; Join 4; Slice 3 |
| Aggregates | 49 | Column count, first/last, implode, median, min/max, n_unique, first/last non-null; nonnumeric means |
| Expression operations | 49 | Expression SortBy 36; Sort 9; Window 4 |
| Scalar/vector functions and operators | 33 | Struct construction, fused arithmetic, anonymous functions, ranges, floor division, abs/negation, cumulative functions, logarithms, string extraction, and optimizer-generated predicates |
| Literal forms/dtypes | 21 | Unsigned/128-bit literals and Series literals |
| Grouping/update semantics | 10 | Dynamic/rolling/UDF/sliced groups, computed/renamed group keys, reduction broadcasting |
| Casts | 1 | Duration cast |

## Dtypes dominate the visible failures

At the time of this initial survey, the adapter accepted resident Int64,
Float64, Boolean, String, Date, and Categorical columns. In that run, first
dtype rejections were:

| Type | Collections |
|---|---:|
| Int32 | 331 |
| Int8 | 128 |
| UInt8 | 122 |
| Int16 | 108 |
| UInt32 | 107 |
| UInt16 | 106 |
| UInt64 | 74 |
| UInt128 | 54 |
| Int128 | 50 |
| Datetime/Duration/Time, including timezone-aware datetimes | 50 |
| Struct | 5 |
| Float32 | 4 |
| Decimal | 3 |
| Null | 2 |

Int8/Int16/Int32, UInt8/UInt16/UInt32, and Float32 account for 906 first
rejections. These are a useful first target, but widening input buffers alone
is insufficient: Polars' expression promotion, aggregate result types and
overflow behavior must be preserved. UInt64 and 128-bit integers need separate
range/representation decisions. A wholly Null column is also different from
a nullable Int64 column; the latter already works.

For example, the upstream first/last group tests use an Int32 `idx` column.
Many reject that column before reaching the unsupported first/last aggregate.
Supporting Int32 will reveal those next blockers rather than make all of the
affected tests pass.

## Plan nodes bundle different features

`MapFunction` is a visitor node category, not a single user operation. Small
visitor probes confirm that `.set_sorted()` appears as `hint_sorted`, while
unpivot and explode appear under the same category with different payloads.
The large dynamic-group parametrization calls `.set_sorted()` before dynamic
grouping and variance/square-root expressions; its current rejection hides
those additional gaps.

Sortedness hints should therefore be considered separately from data-shaping
operations. Join, distinct and explicit slice nodes are also rejected, but
their small counts reflect the selected test modules: the dedicated join,
unique and slice modules were not part of this initial survey.

## Reductions and expressions

The adapter currently translates numeric sum/mean and row count (`pl.len()`).
The observed aggregate rejections are:

* Column `count`: 17; this requires its non-null-count semantics, not an alias
  to row count.
* `first`: 13; `last`: 1; first/last non-null: 1 each.
* `implode`: 7; `median`: 4.
* `min`, `max`, `n_unique`: 1 each.
* Mean of Boolean or Date inputs: 2.

Two more quantile rejections appear in the function category because that is
how the visitor exposes them. Grouped scalar reductions do not cover list
aggregation, filtered reductions, arbitrary group expressions or broadcasts
such as subtracting a global mean inside `with_columns`.

Frame-level column-key sort is supported. The 49 expression-node rejections
instead concern expression-level sort/sort_by and window expressions such as
`.over(...)`.

Several optimized forms need their own translation: fused arithmetic, dynamic
predicates, and the visitor's `negate` function can reject otherwise familiar
arithmetic. Supporting the source-level operator is not always sufficient.

## Adapter gaps versus native capability

These are limitations of the current Polars integration. The native IR already
defines Min, Max, First, Last, Median, CountDistinct, Stddev and Quantile
aggregates, as well as join variants. Mapping them into the adapter remains
work: null behavior, output types, aggregate options, ordering and quantile
interpolation must agree with Polars. Native capability alone does not establish
compatibility.

Source inspection also identifies explicit limits that this run did not
necessarily reach: floating-point grouping keys; computed sort keys; most
casts; remote/multiple-file scans and nondefault Parquet policies; and
background/batch/async/sink execution. These have no occurrence count here.

## Coverage gaps and suggested order

The harness currently observes explicit lazy collection during test bodies.
Eager/Series-only tests are unexercised, including all 15 selected vertical
aggregation tests. Dedicated join, window, I/O, string and nested-type modules
were not selected. This report is therefore a starting inventory, not a
complete Polars feature matrix.

Suggested implementation order, based on the evidence rather than predicted
pass counts:

1. Narrow integer and Float32 input/result compatibility, with explicit dtype
   and promotion checks; keep wide integers a separate task.
2. Sortedness-hint handling, distinct from the other MapFunction variants.
3. Common existing-native reductions: column count, min/max, first/last, then
   distinct count and median with their Polars semantics verified.
4. Common expression translations: numeric casts, negation/abs, supported
   fused arithmetic forms, and reduction broadcasting.
5. Expand targeted lazy coverage for joins, windows, temporal operations and
   I/O before prioritizing these larger additions from occurrence counts.

After each change, rerun the survey to reveal next blockers and promote newly
passing query ordinals into the regression baseline. Keep eager-only and
unselected features visible as unmeasured rather than treating them as either
supported or unsupported.

## Int32 follow-up

The adapter now widens Int32 inputs to native Int64 storage and wraps
Int32-valued expression/reduction results modulo `2**32` into the signed range.
It returns the declared Polars output types without introducing native Int32
variants. Checked Int64-to-Int32 narrowing remains outside this support.

The same survey now records 20 passing tests, 326 tests with unsupported
queries and 730 unexercised tests. Query matches increased from 52 to 73;
rejections decreased from 1,578 to 1,557, with zero mismatches. The modest gain
relative to the original 331 Int32 first rejections illustrates the masked
blockers: many queries now get farther but reject another operation. Newly
matching queries are included in `required_pass_queries`.

## Float32 follow-up

Float32 inputs now widen to Float64, with Float64 arithmetic/reductions and
only the declared Float32 outputs narrowed at export. This intentionally
retains intermediate precision rather than reproducing Polars' Float32
rounding. It can affect cancellation, overflow/underflow and predicate results;
see the engine README and its explicit policy tests. No native Float32 variant
or intermediate rounding operations were introduced. Float grouping keys and
explicit Float64-to-Float32 narrowing casts remain unsupported.

The survey now has 23 passing tests, 323 tests with unsupported queries and
730 unexercised tests; 76 matching queries, 1,554 rejections and zero mismatches.
The upstream checks and tolerances remain unchanged. Newly matching queries
are promoted into the required regression baseline.
