# Fusing whole-script optimization with decoding

Status: Stages 1–3 IMPLEMENTED 2026-07-16 (uncommitted); Stage 4 not started.
See "Implementation notes" at the bottom for what landed and what the
whole-script benchmark mode now measures.
Goal: no field is decoded (or materialized) unless it participates in the
result — driven by the whole-script plan, not by how the query author split
statements.

## Where things actually stand (verified 2026-07-16)

The mechanism the goal needs already exists. `try_execute_whole_script`
(`src/repl/repl.cpp:4228`) lowers an eligible script into relational plans,
runs `ir::required_columns` + `ir::scan_predicates` over the *complete* DAG,
and materializes each lazy source through `LazyTable::project` /
`project_where` with only the demanded columns. **All 22** PDS-H queries are now eligible. Three
declined until 2026-07-17 — and did so *silently*; see "The gate declines three
queries, not one" below.

Correctness is now validated end to end:

- All 22 queries produce **byte-identical CSVs** on the statement path vs the
  batch path, pinned to `parquet_sf1` (an earlier apparent q07/q08/q21
  divergence was the `benchmarking/data/tpch/parquet` symlink being flipped
  to a different scale factor mid-comparison — see gotcha below).
- `check_answers.py` (which drives `ibex_eval` → batch path): 22/22 OK on SF-1.

What the batch path buys today, measured: a *naturally written* q03-style
query (filter in its own `let`, no hand-written `select` after the scan) runs
**0.25s / 258MB** through the batch planner vs **1.1s / 908MB**
statement-at-a-time (SF-2 lineitem). The statement path decodes all 16
lineitem columns because each statement's root demand is "all"; the batch
path decodes 3.

But three gaps keep this from being the benchmark story:

### Gap 1 — the benchmark never exercises the batch planner

`bench_ibex.py` splits each query into statements and pipes them one at a
time into the REPL (`:timing on`), summing per-statement times. The batch
planner only engages on whole-file execution (`ibex file.ibex`, `ibex_eval`).
The queries compensate by hand-fusing scan+filter+select into single
statements — the author is doing the optimizer's job, and every future query
must be hand-tuned the same way or silently decode the world.

### Gap 2 — predicate-only columns are still gathered and materialized

Both drivers compute `required_columns` **before**
`remove_applied_scan_filters` (statement path `src/repl/repl.cpp:3283`, batch
path `:4302`). So a column referenced only by a pushed-down filter is in
`names`, and `project_where` (`src/runtime/lazy_table.cpp:132`) gathers it
row-by-row into the scan output — which the very next Project drops.

The full-column decode of predicate columns is unavoidable (the selection
needs them); the *gather + materialization* is pure waste:

- q03: `l_shipdate` gathered for 3.2M survivors (SF-1)
- q13: `o_comment` — a ~49-byte string column gathered for ~1.45M survivors
- q10: `l_returnflag`; q06: `l_quantity` + `l_shipdate`; q19: several

### Gap 3 — multi-scan sources lose pushdown in batch mode

`scan_predicates` erases any source scanned more than once in the plan
(`src/ir/scan_predicates.cpp:201`, `scan_counts != 1`). Statement-at-a-time
rarely hits this (each statement scans once); a whole-script plan hits it
whenever a file is read twice — q07 (nation ×2), q15/q20/q21 (lineitem ×2,
`lower()` clones bound IR per reference). Measured pinned to SF-1
(min-of-3, whole process incl. ~0.1s startup):

| query | stmt path | batch path |
|---|---:|---:|
| q15 | 0.06s | 0.12s |
| q21 | 0.81s | 0.90s |
| q07 | 0.31s | 0.33s |
| q03 (control, single-scan) | 0.14s | 0.15s |

So today the batch path is parity-or-mildly-worse on the hand-tuned suite.
Its wins only appear on queries nobody hand-tuned.

## Plan

### Stage 1 — stop materializing predicate-only columns (small, both paths)

Reorder both drivers: extract `scan_predicates`, decide which lazy sources
get pushed conjuncts, `remove_applied_scan_filters` from the plan, **then**
compute `required_columns` on the reduced plan and pass those names to
`project_where`. `project_where` already handles names that exclude predicate
columns — the predicate table is decoded for the selection either way; the
output loop simply stops gathering non-demanded ones.

Also fix while there: batch path `project_where` call (`repl.cpp:4318`)
doesn't pass `&scalars`; the statement path (`:3299`) does.

Measure: q03/q06/q10/q13/q19 scan statements, isolated (whole-query PDS-H
numbers cannot see decoder-scale changes — repeat spread swamps them).

### Stage 2 — per-scan predicate identity (unblocks de-hand-tuning)

Key `scan_predicates` (and the driver's materialization) by scan *instance*
(node id), not source name, so two scans of one file each get their own
conjuncts and demanded columns. Driver materializes one table per scan
instance (unique registry binding + rewritten scan name), sharing the
`LazyTable` so unfiltered column decodes still hit the cache once.

Adjacent cheap win: `project_where` currently never caches the full-column
predicate decode it does (`lazy_table.cpp:81`); when selection is computed
from a whole-file decode, insert those columns into `cache_` — a second scan
of the same file reuses them.

This removes the q15/q21 batch regressions and is the prerequisite for
writing queries naturally (no hand-fused scan-filter-select statements).

### Stage 3 — make the benchmark measure whole-script execution

Add a REPL command (e.g. `:run <file>`) that executes a file through
`try_execute_whole_script` in the warm process and reports one `:timing`
line for the whole script; give `bench_ibex.py` a `--whole-script` mode that
uses it. Keep the per-statement mode for stage breakdowns. Then queries can
be written naturally and the planner's decode decisions show up in the
numbers Polars is compared against.

(Whole-file `ibex_eval` per iteration is not an alternative: ~0.2s
Arrow/AWS-SDK dlopen noise per run.)

### Stage 4 — READ THIS BEFORE WRITING ANY DECODER CODE

**Stage 4 as originally drafted below is largely already-rejected work.** The
2026-07-16 re-profile plus `plans/parquet-filtering-scan-observations.md`
change the picture:

- Points (2) and (3) of the draft — dense-decode the block, evaluate the
  selection in-block, gather survivors — are **rejected experiment #1**, which
  has now been measured and rejected **four times** (see that document's
  "Paths not worth repeating" and its 2026-07-14 (4) follow-up). The mechanism
  is structural, not a tuning problem: at every selectivity tested the
  survivors still touch every page, so both paths decode every value, and the
  dense path merely adds a scratch write plus a gather on top. It loses most
  where it is exercised most (densest selection, +3–9%). There is no threshold
  that fires without losing.
- Point (1), row-group statistics pruning, is sound but does nothing for these
  files: every lineitem row group's `l_shipdate` range spans the predicate, so
  stats read 6/6 groups. Worth doing for clustered/sorted data — not against
  PDS-H as the acceptance benchmark.
- The genuinely open lever is **selection-awareness inside the encoding
  decoder** (skipping within RLE/bit-packed runs, indexing PLAIN values without
  forcing rejected values through scratch). That capability is *below* Apache's
  public `TypedColumnReader` API — it needs an Arrow patch/fork, and the same
  document measured a hand-written native page decoder at only 1.15× on
  numerics and **0.93× (slower) on strings** once Arrow's SIMD was enabled. The
  maintenance cost is real and the ceiling is a few ms per scan.

Current q03 lineitem scan profile (39.5ms, after 3e8a46d + fb50e37):

| component | self |
|---|---:|
| `decode_physical_column<DOUBLE>` per-value lambda | 32.5% |
| `decode_physical_column<INT64>` per-value lambda | 18.5% |
| memmove | 10.5% |
| `direct_decode_table` glue | 9.4% |
| `filter_selection` self | 7.6% |
| Arrow's actual decode (GetSpaced + unpack32_avx2) | ~9.6% |

Checked and found already correct (do not "fix" these): the selected path
reserves its output column exactly (`direct_column`'s `out.reserve(output_rows)`
on every arm), and `DirectValidity` starts all-true and only writes on a null,
discarding the bitmap entirely when the column proves null-free.

**Recommended next single-thread work is NOT here.** The observations doc's own
conclusion after decomposing q13 stands: decode is no longer the dominant term;
the join and group-by are. See `plans/filtered-scan-and-groupby-plan.md`
finding 2.2 (per-group Key boxing / malloc churn, ~12% of q10's aggregate) —
and note its finding 2.1 (faster string hash) is now **refuted and reverted**.

### Stage 4 (original draft — superseded by the section above)

Today the seam is `ColumnDecodeFn(names, selection)`
(`include/ibex/runtime/lazy_table.hpp:23`): `project_where` fully decodes
predicate columns, computes a selection in the runtime, then decodes the
rest per-value through Arrow `ReadBatch`/`Skip` glue — measured at ~44% of
q03's scan statement (see `plans/filtered-scan-and-groupby-plan.md`,
finding 1; per-row-group experiments in
`plans/parquet-filtering-scan-observations.md`).

Extend the seam to a request struct — demanded columns **plus the pushed
conjuncts** — so the parquet plugin can, per row group:

1. prune the whole group on min/max statistics before any decode,
2. dense-decode predicate columns, evaluate the selection in-block,
3. dense-decode remaining columns and gather survivors inside the decoder
   (never materializing predicate-only columns at all — subsumes Stage 1
   for the parquet source).

This folds the already-diagnosed "direct per-row-group decode +
selection-aware late materialization are ONE work item" conclusion into the
plan-driven seam. Known traps, all previously hit: `ColumnDecodeFn` is
plugin ABI (rebuild the .so in lockstep); Arrow `Skip()` decodes partial-page
gaps (dense decode + gather, never Skip, for scattered selections); the
LazyTable cache must not be poisoned by filtered decodes; conjunct
evaluation needs the same semantics as `filter_selection` (fused same-column
range passes — `src/runtime/filter.cpp`).

### Stage 5 (horizon) — late materialization across joins

The planner knows join selectivities (cardinality estimates landed with the
whole-script arc). Decode join keys + predicate columns first, probe, then
decode payload columns only for join survivors. Requires scan operators that
hold the `LazyTable` and accept a selection at execution time rather than
materializing up front. Don't start until Stage 4's per-row-group machinery
exists — it's the same gather path.

## Implementation notes (2026-07-16)

Stages 1–3 landed together; 1,147 Release tests pass, `check_answers.py`
22/22 OK, and all 22 queries stay byte-identical on both paths pinned to
`parquet_sf1`.

- **Stage 1**: both drivers now run `scan_predicates` →
  `remove_applied_scan_filters` → `required_columns`, in that order, so
  predicate-only columns are decoded for the selection but never gathered
  into the scan output. The batch path's missing `&scalars` turned out to be
  correct-by-construction (batch-eligible scripts cannot bind scalars —
  documented in a comment instead of "fixed").
- **Stage 2**: `ir::split_scan_instances` (in `scan_predicates.{hpp,cpp}`)
  renames each scan of a multi-scanned lazy source to `name#k`; both drivers
  materialize per instance and resolve instances back to the shared
  `LazyTable`. `project_where` now caches its whole-file predicate decodes
  (they are legitimate whole-column cache entries) and reuses cached whole
  columns for predicates. Inline self-joins of a lazy source work through
  the statement path too. q15 batch improved 0.12s → 0.08s.
- **Stage 3**: REPL `:run <file>` executes a file exactly like
  `ibex file.ibex` (batch planner, fresh scope, one `:timing` line);
  `bench_ibex.py --whole-script` drives it (query file minus its `write_csv`
  sink) and writes `results/ibex_whole_script.tsv`.

**What the new mode measures (SF-1, warmup 1 / iters 5, serial — treat
small deltas as drift per the interleaved-methodology note):** whole-script
vs statement mode is at parity on single-reference queries, and slower
exactly where `lower()` clones a shared `let` binding per reference:
q21 785 vs 617ms (li_F consumed 3×), q15 72 vs 46ms, q11 38 vs 22ms,
q22 68 vs 49ms, q14 65 vs 52ms, q02 73 vs 64ms.

**Shared subplan materialization — DONE 2026-07-16.** The lowerer counts
table-position references per binding in an AST pre-pass
(`count_table_refs`, lower.cpp); a binding bound once, referenced ≥2×, whose
lowered plan `contains_expensive_node` (join/aggregate/distinct/order/
update/window/reshape — NOT plain scan/filter/project, which stay inlined so
each consumer keeps its own pushdown) is moved to
`ScriptPlan::shared_bindings` instead of `bindings_`; later references then
miss `bindings_` and lower to `Scan(name)` via the existing fallback. The
batch executor materializes each shared plan once, in declaration order,
into a base registry every sink/result `evaluate()` copies from; shared
tables contribute exact schemas and row counts to the join-rewrite passes.
Sink-arg and bare-final references are deliberately not counted — the
executor's `cached_bindings` already dedups those.

Measured SF-2, whole-script vs statement mode (back-to-back pairs on a
drifty box): q11 +70%→+0.6%, q15 +55%→+2%, q22 +40%→**faster** (92 vs
98ms), q21 +27%→**faster** (1336 vs 1450ms), q14/q02 → parity. Cross-boundary
column pruning (main plan demanding fewer of a shared binding's output
columns) is still v2 — shared plans materialize their full output schema.

**Correction (2026-07-17): the q15/q22 numbers above are not what they look
like.** Those two queries never reached the planner at all — they decline the
gate and silently fall back, so "q22 92 vs 98ms, the shared `substring` update
runs once" was measuring statement mode against itself, and the earlier claim
of "parity-or-better on every query measured" rested partly on a fallback.
This went unnoticed because declining was silent. It is now reported (below).

Re-measured SF-1, min-of-2-rounds interleaved, quiet box, whole-script vs
statements: **geomean 0.983× over the 19 planned queries**, suite total
3247 vs 3299ms (−1.6%). Best q02 −11.2%, q05 −8.8%, q22 −7.8%, q14/q06 −6.2%;
worst q06 +6.2%, q04 +5.0%. Nothing regresses beyond noise, so whole-script is
now the harness default — and it is the path `ibex file.ibex` already takes.

## The gate declines three queries, not one

`try_execute_whole_script` returns `nullopt` to mean "fall back", which is
invisible from outside. It now fills a `decline_reason`, and `execute_script`
prints `planner: whole-script` / `planner: statements (<reason>)` under
`--verbose`; `bench_ibex.py` records it per query into the TSV's `mode` column
and warns when anything fell back. A benchmark that cannot see which engine
path it measured cannot tell a regression from a gate change.

- **q16** — `script uses \`import\``. Known, structural.
- **q15, q22** — FIXED. Diagnosed 2026-07-17 and it was **not** architectural,
  it was a regression this plan's own shared-binding work introduced. Written
  up below because the first diagnosis was wrong in an instructive way.

## Gotchas for whoever picks this up

- `benchmarking/data/tpch/parquet` is a **symlink** flipped by
  `run_bench.sh --sf N`. Any A/B comparison must pin explicit
  `parquet_sf<N>/` paths — a mid-run flip produced convincing-looking fake
  divergences during this analysis. `check_answers.py` is only meaningful
  with the symlink at `parquet_sf1`.
- The REPL's multiline reader chokes on some query files piped raw; reflow
  through `bench_ibex.split_top_level_statements` (one statement per line)
  when emulating the harness.
- Post-commit perf hook: check `pgrep -af 'perf|ibex|ninja'` before trusting
  any measurement.

## q15/q22: a self-inflicted decline, and a wrong diagnosis worth remembering

The symptom: both queries declined with `scalar(): the enclosing query's
columns are not statically known`. The obvious reading — whole-script lowering
runs before any source is read, so `read_parquet`'s schema is open and
`scalar()` cannot classify an identifier as inner-vs-outer — is wrong, and two
experiments killed it:

1. Ascribing the *reads* with their full concrete schemas (all 16 lineitem
   columns) changed nothing. So an open source schema was not the cause.
2. Flipping `share_repeated_bindings_` to false made both queries plan
   immediately. So the cause was sharing.

The real mechanism: an aggregate's or projection's output schema is **known
regardless of its input**, because the field list names it outright —
`[select { total_revenue = ... }, by { s_suppkey = l_suppkey }]` is exactly
`{s_suppkey, total_revenue}` whatever it reads. So with the binding *inlined*,
`infer_schema` walks the plan and resolves the join's schema fine, open sources
and all. But the shared-binding pass rewrites the reference to `Scan("revenue")`,
and `Scan` resolves its schema through `source_schemas()`, which knows nothing
about a binding the executor has not materialized yet. The schema was knowable
and sharing threw it away.

Fix (`lower_script`, the shared-binding bind site): infer the schema off the
plan before moving it into `shared_bindings_` and record it in
`binding_schemas_`, which `source_schemas()` already overlays. Four lines. The
trigger condition is worth internalizing: **a binding is shared exactly when it
is expensive and referenced twice — and the second reference is very often the
one inside `scalar(...)`.** So sharing systematically breaks decorrelation on
the queries it exists to help.

Measured SF-1, pinned to cores 2-3, interleaved, min-of-8 in one warm REPL,
3 rounds — `:run` planned vs `:run` falling back (same path, same framing):
q15 46.7 → 45.7ms (−2.2%), q22 51.1 → 49.9ms (−2.3%). Consistent sign every
round. 1155 tests, 22/22 answers.

**The note this corrects.** The plan previously recorded "q22 needed Update in
the expensive set (per-row substring re-ran twice)" as a *fix*, with "q22 92 vs
98ms → faster" as evidence. Adding `Update` to the expensive set is what made
`in_scope` shared, which is what made q22 stop planning. The 92-vs-98ms was
statement mode timed against itself, and the "fix" was the fallback. A silent
decline plus a plausible story reads exactly like a win. This is why
`--report-planner` exists.

**Two things learned about ascription** (from experiment 1, both real):
- `ScalarType` has no `Categorical`, so a dictionary-encoded Parquet string
  column cannot be ascribed by its decoded type at all. `String` does validate
  against a Categorical column, so ascription works — but the user has to know
  to write the type the column *isn't*.
- Ascription is exact/closed by default, so ascribing a raw reader means
  naming every column in the file. For a benchmark that exists to measure
  projection pushdown that is actively counterproductive.

## Probe: what a naturally-written query costs today (2026-07-17)

The suite's queries hand-fuse scan+filter+select, i.e. the author does the
optimizer's job. Asked whether to rewrite them naturally so the planner has to
do the work, this probe measured three phrasings of q03 (SF-1, pinned to cores
2-3, min-of-8 in one warm REPL, all three whole-script planned, all three
producing the correct answer):

| phrasing | ms |
|---|---:|
| hand-fused (what the suite ships) | ~95 |
| filters moved above both joins, hand-written projections kept | ~102 |
| fully natural (no hand-written `select` after any scan) | **~585** |

**Filter pushdown through joins works** — moving all three conjuncts above two
joins costs 7%. `push_filters_into_joins` earns its keep here.

**Projection pushdown does not reach a scan through a join.** The whole 6x is
lineitem decoding all 16 columns instead of 3. This is the known gotcha from
the projection-pushdown work ("pushdown only reaches a source through the
statement that scans it; `let j = a join b` demands ALL of both sides"), and
`update { o_orderkey = l_orderkey }` — the rename the language *requires* before
an equijoin, since TPC-H keys don't match by name — sits between the scan and
the join. Whether the blocker is Update or Join is the first thing to check;
that determines whether the fix is in `required_columns`' Update arm or its
Join arm.

**So: rewrite the queries only after fixing this.** Doing it now would post a 6x
regression on q03 and similar elsewhere. The order is (1) make demand analysis
reach scans through joins/updates, (2) then de-hand-fuse the suite, which at
that point measures the optimizer instead of the author. The upside is real —
the hand-fusion is currently hiding a 6x gap that every naturally-written user
query walks into, and Polars' lazy API does this pushdown for free, which is
why its published queries can afford to be written the same way ours are.

## Correction: ascription, `*`, and what it actually costs (2026-07-17)

The note above ("ascribing a raw reader means naming every column") was half
right and misleading. Corrected after testing:

- **`*` exists and is the answer to verbosity.** `read_parquet(...) as
  DataFrame<{ l_shipdate: Date, * }>` names only what you care about. Exact is
  the *default*, not the only option.
- **`*` was BROKEN under the whole-script planner** — a real bug, found here.
  `lower.cpp`'s IR clone rebuilt AscribeNode via `builder_.ascribe(asc.schema())`,
  and the builder's `bool open = false` default silently turned every cloned
  wildcard into an exact schema. Whole-script lowering clones bound IR per
  reference; statement mode does not clone, so only one path saw it. Fixed, and
  `open` is now a required parameter so a dropped argument is a compile error
  rather than a silent meaning change. Test: "wildcard ascription survives the
  IR clone".
- **Ascription still forces a full decode, and that is a genuine open gap.**
  Even `{ l_shipdate: Date, * }` — one named column — makes the lineitem scan
  cost 228ms vs 60ms unascribed (~4x); the closed 16-column form costs 235ms.
  Naming a column in an ascription should not imply materializing it: the check
  needs each column's *name and type*, which the Parquet footer already carries,
  not its *data*. So `required_columns` should not turn an Ascribe's field list
  into data demand, and the runtime check over a lazy scan should validate
  against the source schema rather than the materialized projection. Until then,
  ascribing a reader silently disables projection pushdown.

Note this is now academic for q15/q22 — the shared-binding schema fix (2241720)
means they need no ascription at all. It matters for anyone who ascribes a
reader for static checking and unknowingly pays 4x for it.
