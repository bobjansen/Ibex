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

**The 6x is NOT a decode problem. Measured, not reasoned.** Dumping the demand
map (`required_columns`) for each phrasing settles it:

    natural       lineitem scan demands: l_discount l_extendedprice l_orderkey l_shipdate
    hand-fused    lineitem scan demands: l_discount l_extendedprice l_orderkey

The scans are the same four columns (hand-fused drops l_shipdate only because
its filter was absorbed into the scan). Demand is narrow in every phrasing --
`required_columns`' Join arm widens only for Asof, and passes `need` + join keys
to both sides otherwise. So "projection pushdown does not reach through a join"
is simply false, and the 6x buys nothing in the scan.

Bisecting the phrasings (SF-1, pinned, min-of-5) localizes it but does not yet
explain it:

| phrasing | ms |
|---|---:|
| hand-fused | ~95 |
| all three sides projected, lineitem renamed by `update` not `select` | ~123 |
| natural + orders projected (customer bare) | ~610 |
| natural + customer projected (orders renamed by `update`) | ~575 |
| fully natural | ~585 |

`update` on the lineitem side costs 14ms, not 6x. Projecting *either* customer
or orders alone changes nothing. Projecting *all three* drops it to 123ms. That
non-linearity is the whole clue: this is a threshold, not an additive cost --
something is bailing out entirely, and only flips back on when the last leaf is
right.

**Diagnosed to one line, 2026-07-17.** Instrumenting the aggregate's child node
kind across the four phrasings settles what the timings could not:

| phrasing | ms | aggregate's child |
|---|---:|---|
| hand-fused (no filter above the joins at all) | 95 | `Project` |
| filters above joins, every side `select` | 102 | `Project` — filter **was pushed** |
| same, but lineitem renamed by `update` | 123 | `FilterUpdateProject` — **not pushed** |
| fully natural | 585 | `FilterUpdateProject` — **not pushed** |

A `Project` child means the top-level filter is *gone*: `push_filters_into_joins`
moved every conjunct onto a join side. A `FilterUpdateProject` child means the
filter is still sitting above both joins, so they run on unfiltered inputs --
6M lineitem rows instead of 3.2M, and the customer/orders join unfiltered too.
That is the 6x, and it is a *filter* pushdown failure, not a projection one.

**The trigger is a join side being `Update(Scan)` rather than `Project(Scan)`.**
Nothing else separates the 102ms and 123ms rows -- same filter, same scans, same
demand (both decode the same four lineitem columns). Since TPC-H keys do not
match by name, an equijoin *requires* a rename, and `update` is the natural way
to write one -- so the natural phrasing trips this on every join.

**Ruled out, with evidence, do not re-run:**
- *Projection pushdown / wide scans.* Demand is narrow in every phrasing; the
  lineitem scan decodes the same 4 columns natural or hand-fused.
- *`required_columns`' Join arm.* Widens only for Asof; otherwise passes `need`
  plus join keys to both sides.
- *Inner-join reordering.* Never fires for q03 in ANY phrasing --
  `reorder_inner_joins_for_aggregates` needs an Aggregate whose child is
  *directly* a Join, and here it is always a Project or a fused node. (Its
  `schemas_are_unambiguous` gate is never reached, so it is not the cause. That
  the pass is dead on this query shape is a separate finding worth its own look.)
- *`infer_schema`'s Update arm.* Correct: propagates `known` and `open` from its
  input, so an `Update(Scan)` side is Known and closed like a `Project(Scan)`.

**Where to look next.** `join_pushdown.cpp` contains zero references to
`FilterProject` / `FilterUpdateProject`, and `walk` (:231) only matches a plain
`NodeKind::Filter` directly over a `NodeKind::Join`. Two candidates remain, and
they are cheap to separate:
1. The fused node is built *before* pushdown runs in the `update` shape, so the
   filter is invisible to `walk` -- but then the `select` shape should fuse too,
   and it evidently does not. Check when FilterUpdateProject is formed in each.
2. `push_conjunct`'s side attribution (:96-124) rejects a conjunct because an
   `Update` side's schema is the source's FULL column list (16 lineitem columns
   + the alias), where a `Project` side's is 4. A wider side schema can only
   change the outcome via `left.find(name) != nullptr` -- so a conjunct that is
   unambiguous over projected sides may look ambiguous over unprojected ones.

Print the destination `push_conjunct` picks per conjunct for the 102ms and
123ms plans; they differ by exactly one node, so the divergence will be obvious.

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
- **Ascription forced a full decode (~4x) — FIXED, for exact and wildcard
  alike.** `required_columns` handled Ascribe under its `default:` arm, which
  widens demand to every column, so ascribing a reader silently disabled
  projection pushdown: `{ l_shipdate: Date, * }` — one named column — cost
  228ms against 60ms unascribed, and the exact 16-column form 235ms.

  The fix is to check the ascription against the source's **schema**, not its
  data. `LazyTable::schema()` is the Parquet footer, and both drivers already
  hold every source's names and types in `schemas` before any pass runs. A new
  `ir::check_ascriptions` proves each ascription over a statically known input
  and marks the node checked; a checked ascription is a pure identity, so the
  interpreter skips its runtime check and `required_columns` passes the parent's
  demand straight through. Both forms are now at parity with an unascribed scan
  (60-63ms vs 60ms), and a bad ascription is now caught *before* a page is
  decoded rather than after.

  **This supersedes the note that briefly stood here** claiming an exact
  ascription "must" widen because "no unlisted column" can only be checked
  against the whole input. That was wrong: it is a question about the schema,
  and the schema answers it. The wildcard-only fix (55ff818) was a half measure
  built on the same mistake — it fixed the case where the assertion happened not
  to need the extras, rather than noticing the assertion never needed *data* at
  all.

  Two things this touched that are worth knowing:
  - `ir::ColumnType` has **no Categorical** — `column_ir_type` maps both
    `Column<string>` and `Column<Categorical>` to `String`. So the leniency the
    interpreter needs (its check sees runtime variants, where Categorical is
    distinct) does not exist at the schema level, and plain equality is correct
    in the new pass. `s_name: String` over a dictionary-encoded column still
    passes, which is the case that would have broken had this gone the other way.
  - The lowerer's static check (`lower.cpp`) and the interpreter's runtime check
    disagree on exactly that point — the static one uses strict equality. It has
    never bitten because it only fires on a known input schema and readers never
    had one. Left alone, but it is a live inconsistency if reader schemas ever
    reach the lowerer.

  Bad ascriptions stay fatal even when nothing else reads the column — verified
  on a lazy source for both a missing column and a wrong type; `ibex file.ibex`
  and `ibex_eval` exit 1. (Interactive `:run` reports and keeps the session.)

Note this is now academic for q15/q22 — the shared-binding schema fix (2241720)
means they need no ascription at all. It matters for anyone who ascribes a
reader for static checking and unknowingly pays 4x for it.

### Correction: the pushdown finding is real but does NOT explain the 6x

Re-reading the table above: `upd_line` (123ms) and `natural` (585ms) BOTH have a
`FilterUpdateProject` child, i.e. both fail to push the filter. So an unpushed
filter is worth ~21ms here (102 -> 123), not 6x. The claim that "the trigger is
a join side being Update(Scan)" explains which plans lose pushdown; it does not
explain the 460ms that separates the two plans that both lost it.

So there are TWO independent effects, and only the small one is diagnosed:

1. **A join side that is `Update(Scan)` loses filter pushdown.** Real, isolated
   (the only difference between the 102ms and 123ms plans), worth ~21ms on q03.
2. **Something about customer/orders being unprojected costs ~460ms**, on top of
   (1) and independent of it. `upd_line` differs from `natural` only in that
   customer and orders are `select`-projected rather than bare/`update`-renamed.
   Undiagnosed. Note it is NOT the scan: demand is narrow in both and both
   decode the same columns.

The bisect earlier in this document already contained the refutation and it was
missed: projecting *either* customer or orders alone leaves ~590ms, but
projecting *both* (plus lineitem's `update`) gives 123ms. A single all-or-nothing
gate somewhere still fits effect (2) — it just is not join reordering (dead on
this shape) and not the filter pushdown above (which the 123ms plan also fails).

Next: instrument what each JOIN actually receives — input row counts and column
counts per side — for the 123ms and 585ms plans. The row counts will separate
"the joins see unfiltered inputs" from "the joins carry too many columns", which
is the question effect (2) turns on. Do not reason about it from node kinds
again; two diagnoses in this thread died that way.

### SOLVED: filter pushdown is schema-blind over readers (2026-07-17)

`push_filters_into_joins` is called from **`lower.cpp`** (4810, 4841, 4849,
4877), not from the driver — and at lower time `source_schemas` is **empty**,
because a reader like `read_parquet` has no declared return schema. Instrumenting
`classify` shows it plainly (`sources has 0 entries`, and the join's child is an
`ExternCall`, not a `Scan` — this runs before `hoist_extern_sources`).

The ordering is deliberate and documented at `lower.cpp:4805`: pushdown must run
*before* canonicalize, because canonicalize fuses `Filter(Join(...))` into
`FilterUpdateProject` and cannot tell which side produces a column. So pushdown
gets exactly one attempt, at the moment when reader schemas do not yet exist.

Whether a conjunct pushes then depends on whether the join side happens to
describe itself:

| side shape | schema at lower time | result |
|---|---|---|
| `Project(reader)` | **Known** — a Project's schema is its own field list, whatever it reads | pushes |
| `Update(reader)`, bare `reader` | **Unknown** — Update propagates its input's schema, and the reader has none | blocked |

Measured, per conjunct (`IBEX_DEBUG_PUSH` instrumentation of `classify`):

- all sides `select`: 5 classify calls, every conjunct pushed -> 102ms
- lineitem `update`: `right known=false`, so `l_shipdate` -> `Above` -> 123ms
- fully natural: **zero classify calls** — nothing matched `Filter` over `Join`
  at all, because with both sides unknown nothing pushed at the inner join
  either, and canonicalize then fused the filter away -> 585ms

The driver's own `push_filters_into_joins` call (`repl.cpp:4368`), which *does*
have real schemas, is effectively dead here: by the time it runs the filter is
inside a fused node and the pass only matches a plain `Filter` over a `Join`.

**This is why the hand-fused suite is fast.** The `select` after each scan is not
just doing projection pushdown by hand — it is *supplying the schema* that makes
filter pushdown possible at all. That is a far bigger thing to have been doing by
hand than anyone realized.

**Confirmed by construction:** ascribing the three readers with their exact
schemas (nothing else changed, still `update` renames, still filters above both
joins) takes natural q03 from **574ms to 108ms**, against 96ms hand-fused, with
the correct answer. An ascription gives the lowerer the schema, which is exactly
and only what the pass was missing.

**The fix is not to ascribe in user code.** It is to give the lowerer the reader
schemas it already could have: a Parquet footer read is cheap and happens anyway.
`try_execute_whole_script` currently lowers first and resolves lazy sources
second; resolving them first (the extern args are literals in the AST) and
feeding their schemas into `lower_script` would make every naturally-written
join push its filters without the author writing anything. Ascription remains
the escape hatch for readers whose schema genuinely cannot be known up front.

Note this also explains the earlier bisect's non-linearity, which no
node-kind-level story could: it is not a threshold, it is per-conjunct. Each
conjunct pushes only if the side owning it self-describes, so projecting one
table pushes only that table's conjunct — and one unpushed conjunct above a
6M-row join dominates whatever the other two saved.

### `reorder_inner_joins_for_aggregates` is dead on 12 of 13 queries (2026-07-17)

The pass matches an `Aggregate` whose child is **directly** a `Join`
(`join_reorder.cpp:169`). Instrumenting that check across the PDS-H suite, the
aggregate's child is:

| query | child |
|---|---|
| q02 q03 q05 q07 q08 q09 q10 q11 q13 q18 | `Project` |
| q01 | `Update` |
| q20 | `FilterProject` |
| q13 (2nd) | `Aggregate` |
| **q21** | **`Join`** — the only query where the pass fires |

So costed join reordering runs on 1 of 13 queries checked. Everywhere else a
`Project` sits between the aggregate and the join chain, because the aggregate
block's `select` lowers to one.

**Why the tests did not catch it:** `tests/test_ir_join_reorder.cpp` hand-builds
`Aggregate(Join(Join(Scan, Scan), Scan))` with the Builder — the exact shape the
pass wants, and a shape the lowerer does not emit for any of these queries. The
pass is correct on its own terms and unreachable in practice. Any test for an
optimizer pass that constructs its own IR is testing the pass against the
author's mental model of the plan, not against the plan.

**Relaxation looks sound.** Inner joins are associative and commutative, the
aggregate is already checked order-insensitive (`aggregate_order_insensitive`),
and `Project`/`Filter`/`Update`/the fused variants are row-wise — none depends on
the order or grouping of rows below it, and reordering a join chain changes
neither the column set nor the multiset of rows. So the gate can walk down
through row-wise unary nodes to find the chain, and reorder it in place rather
than replacing the aggregate's immediate child.

Unlike filter pushdown, this pass has real schemas: it runs from the driver
(`repl.cpp:4371`) after the lazy sources are resolved, so `schemas_are_unambiguous`
and the cardinality estimates are working with the truth. The only thing wrong is
the shape gate.

**Before doing it:** this changes join order on ~11 queries that have never had
it, so it must be benchmarked per query, not just in aggregate — a costed
reorder that helps the geomean can still regress an individual query badly, and
the estimates have never been exercised on these shapes. Also worth checking
whether q21 (the one query it does fire on) is actually faster for it; if that
was never measured either, the pass has no evidence behind it at all.

**Test it from the lowerer, not the Builder.** Parse a query, lower it, and
assert the pass fires — otherwise the next shape change silently kills it again.

### FIXED: the lowerer now gets reader schemas (2026-07-17)

Natural q03: **585 -> 96ms**, identical to hand-fused. Every phrasing now lands
at 92-96ms — the shape of the query no longer decides whether its filters push.

Three parts:

1. **Per-call-site schema keys** (`ir::extern_call_site_key`). `infer_schema`
   keyed an `ExternCall` by its callee, which assumes one schema per function —
   false for a generic reader, since `read_parquet` returns a different schema
   per path. A call site is now keyed `read_parquet("path")`, and infer_schema
   prefers that over the bare callee (which still serves declared reader return
   schemas).
2. **`lower_script(program, reader_schemas)`.** They reach the Lowerer's
   `binding_schemas_`, which `source_schemas()` overlays.
3. **The driver resolves readers before lowering.** It lowers once to discover
   which readers are called with which literal arguments, asks each for its
   schema (a footer read — the executor pays it anyway), then lowers for real.

**The bug within the bug:** `lower_script` built its schema map with
`build_source_schemas(lowerer.table_extern_decls())` rather than calling
`lowerer.source_schemas()` — the accessor that overlays `binding_schemas_`. So
the schemas were threaded correctly all the way in and then dropped one line
before the pass that needed them. The first end-to-end attempt showed 624ms and
looked like a failed idea; it was a one-word bug.

Cost: one extra lowering and one footer read per distinct reader, per script.
Interleaved A/B (pinned, min-of-6, old vs new binary) on the hand-fused suite:
q03 99.5/97.5 -> 95.7/98.8, q21 699/691 -> 706/678, q05 130/134 -> 131/135 —
no regression. (A serial before/after run read +11% across every query
*including one that got faster*; that was box drift. Interleave.)

Two tests changed, and the change is the point: `source_instances` counted
LazyTable constructions, and each script now builds one more — the schema probe.
Both assertions now say so.

**This removes the argument for de-hand-fusing being blocked.** The suite's
`select` after each scan is no longer load-bearing for filter pushdown. Rewriting
the queries naturally is now a phrasing decision, not a 6x cliff — though it is
still worth doing only if the queries should measure the optimizer rather than
the author.
