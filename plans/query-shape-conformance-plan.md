# PDS-H query-shape conformance regression

## Purpose

Commit `eb5231c` ("Match query shapes") rewrote `benchmarking/tpch/queries/*.ibex`
to match the upstream `polars-benchmark` query shapes — no more hand-written
predicate pushdown, projection pushdown, or join-key pre-renaming. That is the
right thing for the suite to measure (it now shows what the planner does for a
naturally-written query, not what an author who already knew the answer did),
but it cost real performance: the planner does not yet recover everything the
hand-fusion was doing for free. This plan tracks closing that gap.

Commit `322fb14` ("Ascribe required schemas") then added `as DataFrame<{...}>`
schema ascriptions to most of the same query files, which moved the numbers
again — in both directions, depending on the query, for reasons unrelated to
what it looks like it's doing. Untangling those two effects is most of this
plan's investigation so far.

Related: [[project_join_reorder_cost_model]] and [[project_q02_q04_regression_two_bugs]]
(memory) cover the join-order and semi/anti-join fixes already landed against
the same realignment. [decode-fusion-plan.md](decode-fusion-plan.md)'s
"Correction: ascription, `*`, and what it actually costs" and "SOLVED: filter
pushdown is schema-blind over readers" sections (2026-07-17) are the ancestor
of one of the four mechanisms below — re-verified still accurate against
today's code (2026-08-20), see Mechanism 1.

## Measured baseline (SF-2, 8 cores, `bench_ibex.py`, same HEAD binary both sides)

Pre-`eb5231c` (hand-fused) query text vs current HEAD query text, both run
through the *same* current-HEAD `ibex`/`ibex_eval` — isolates the query-shape
question from any engine change since `eb5231c`:

| query | old (hand-fused) | new (aligned) | ratio |
|---|---:|---:|---:|
| q21 | 233ms | 468ms | **2.00x** |
| q06 | 23ms | 44ms | **1.91x** |
| q13 | 132ms | 199ms | **1.51x** |
| q22 | 49ms | 74ms | **1.51x** |
| q15 | 42ms | 53ms | 1.27x |
| q14 | 32ms | 37ms | 1.16x |
| (rest) | — | — | ≤1.08x or *faster* (q11 0.78x, q17 0.68x, q16 0.64x, q18 0.81x, q20 0.83x) |
| **total** | 1898ms | 2148ms | **1.13x** |

q21 alone is 234ms of the 250ms total regression — it dominates. Several
queries are *faster* under the new shape (the planner's own join reordering
and semi-join fixes are real wins on their own merits), so this is not a
uniform "the planner is worse" story — it's concentrated in a handful of
queries, each with its own cause.

**Do not re-derive this table by re-running `ab_queries.py` against two git
commits' binaries** — the query files are the only thing that needs to vary
here; building two engine binaries conflates the shape question with whatever
engine work landed in between (multithreading, join reorder, decode fusion —
see [[project_pdsh_polars_benchmark]] for how much of that there's been).
Reproduce with: swap `benchmarking/tpch/queries/*.ibex` for a `git show
eb5231c~1:...` checkout of the same files, run `bench_ibex.py` with
`IBEX_CORES=8 IBEX_PARALLEL=1`, `taskset -c 0-7`, warmup 2 / iters 6, restore
the tree. Ascription on/off variants: strip `\s*as DataFrame<\{[^}]*\}>` with a
regex — for every query in this suite that's textually identical to checking
out `322fb14~1`, since that commit touches only query files.

## Four independent mechanisms found, not one

The instinct "make ascription a no-op everywhere" (tried, reverted — see
below) is wrong because these four don't share a fix. Fixing one can make
another worse.

### Mechanism 1 — lower-time filter-into-join pushdown is schema-blind (RE-VERIFIED LIVE, not new)

`push_filters_into_joins` runs from **`lower.cpp`** (4 call sites: 5186, 5238,
5249, 5280), before the driver has read any Parquet footer. A join side's
schema is Known there only if it's a `Project` (a `select` sets its own
schema) or a checked `Ascribe` (via `lowerer.source_schemas()` overlaying
`binding_schemas_` — but only from the *second* call site onward, since the
first (5186) uses `build_source_schemas` from extern **declarations** only,
before any per-call-site ascription is recorded). A bare `read_parquet(...)`
or `Update(read_parquet(...))` join side is Unknown, and the pass silently
declines to push anything past it.

This is exactly what the hand-fused queries were doing by writing `select
{p_partkey = ps_partkey, ...}` right after every scan: giving each join side
a Known `Project` schema, which is what actually let `push_filters_into_joins`
fire, not the projection narrowing itself. The realigned queries — matching
Polars' idiom of filtering late, joining bare readers — lose this for free.
Ascribing the reader restores it.

Confirmed still true against current code (2026-08-20): `build_source_schemas`
in `lower.cpp:106` still keys off `table_extern_decls_` only; the ascription
overlay path (`binding_schemas_` → `lowerer.source_schemas()`) is unchanged in
shape from `decode-fusion-plan.md`'s account. Not re-benchmarked from scratch
this session, but the mechanism was independently corroborated by Mechanism 4
below (the barrier-stall regression only appears on ascribed queries whose
filter would actually reach the join this way).

**Fix, not yet scoped**: `push_filters_into_joins`'s first call site (5186)
running before any binding's schema exists is the gap — if the *driver*
populated real Parquet-footer-derived schemas before lowering (rather than
after, as today), every join side would have a Known schema regardless of
`select`/ascribe/bare, and this whole class of accidental dependency on
authorial style goes away. This is a bigger, more structural change than the
other three fixes below (touches the lower/driver boundary) — scope it
separately before starting.

### Mechanism 2 — `scan_predicates`'s `projected_scan()` doesn't see through `Ascribe`

`src/ir/scan_predicates.cpp:39` (`projected_scan`) walks a `Filter`'s child
through `Project` to find the `Scan` underneath, to decide whether the filter
can fuse into the Parquet decode. It has no case for `Ascribe`, so
`Filter(Ascribe(Scan))` returns `nullptr` and the filter never fuses — full
decode, then an ordinary `Filter` operator, instead of `project_where`'s
selective decode.

**Confirmed** on q06 (single-table filter+sum, no join — Mechanism 1 doesn't
apply): stripping just the `as DataFrame<{...}>` restores it to old-baseline
parity (23.0ms old, 22.5ms unascribed-new, 43.9ms ascribed-new — a clean 91%
regression from ascription alone). q06 is the only query in the suite this
isolates cleanly, since every other regressed query also involves a join.

**Fix**: teach `projected_scan()` to walk through a `checked()` `Ascribe`
node, exactly like it already walks through `Project`. Small, narrow,
additive — verified NOT sufficient alone (see "tried and reverted" below),
needs to ship together with a fix or gate for Mechanism 4, or it just
reproduces that regression on more queries.

**IMPLEMENTED 2026-08-20 — and the warning above was right, in a new way.**
`projected_scan()` now walks through a checked `Ascribe` exactly like `Project`
(same pattern as `required_columns`'s existing Ascribe arm: only when
`checked()`, since an unproven ascription's schema isn't trustworthy). Tests:
1,639/1,639 pass; `check_answers.py` 22/22 OK at SF-1 (a stray `parquet` →
`parquet_sf2` symlink from this session's Mechanism-4 profiling produced 15
spurious FAILs first — see the gotcha in `decode-fusion-plan.md`; re-pinning
to `parquet_sf1` was all that was needed, not a code bug).

Full-suite SF-2 A/B (interleaved-adjacent, same box, `IBEX_CORES=8`, warmup 1 /
iters 4, geomean of `min_ms` ratios): **0.938 — a real 6.2% net win.** But it
is not uniform, and the losers are informative:

| query | before (min ms) | after | ratio |
|---|---:|---:|---:|
| q13 | 184.4 | 112.2 | **0.61** |
| q06 | 43.4 | 25.1 | **0.58** |
| q14 | 39.6 | 30.7 | 0.78 |
| q15 | 58.6 | 45.1 | 0.77 |
| q09 | 174.0 | 137.8 | 0.79 |
| q20/q17/q18/q11/q08/q07 | — | — | 0.88–0.92 |
| q10/q02/q16/q19/q22/q12/q05 | — | — | 0.97–1.04 (parity) |
| q21 | 552.0 | 610.0 | 1.11 |
| q03 | 69.3 | 81.0 | **1.17** |
| q01 | 95.4 | 118.2 | **1.24** |
| **q04** | 51.2 | 89.9 | **1.76** |

**q04 traced further, and the first trace below was WRONG about which call
site — recorded here because the correction is itself the finding.**
`IBEX_DEBUG_ASCRIBE` instrumentation (temporary, not committed) showed
`lineitem` going through a deferred-scan path with the row/timing numbers
flipped: **before**, 49.8ms decodes lineitem's full 12M unfiltered rows;
**after**, 106.7ms decodes only the 7.58M survivors of `l_commitdate <
l_receiptdate` — more time for fewer rows. First hypothesis: the probe
deferred-scan path (`repl.cpp`'s `ir::deferrable_probe_scans` loop,
`DeferredScan.conjuncts` populated from `predicates`). **Tested directly by
withholding conjuncts at that call site — q04's timing didn't move at all.**
Wrong call site: `collect_deferrable` (`scan_predicates.cpp:396`) only fires
for `JoinKind::Inner`, and q04 is a `semi` join — that path never touches it.

**Second hypothesis, tested and confirmed real, then reverted because it's not
a fix — it breaks correctness:** the *other* `DeferredScan` construction site
in `repl.cpp` (~line 5071, `if (exec.stream_scans && lazy->scan_units().size()
> 1)` — the pipelined/streaming per-row-group registration, `stream_scans`
defaults **true**) also reads `.conjuncts` from `predicates`. Withholding
conjuncts there and re-running `check_answers.py`: **q1, q4, q13, q15, q20,
q21 all FAIL** (wrong row counts/values) — proof this streaming path is where
the fused conjunct is actually *enforced* as correctness for these queries,
not an optional optimization. Reverted immediately; 22/22 OK restored.

**This is the real finding, and it changes the diagnosis substantially: every
query that moved — winners (q13, q15, q06, q09, q14, …) and losers (q04, and
presumably q01/q03/q21) alike — routes through the chunked/streaming
pipelined-scan engine (`chunked.cpp`, `stream_scans` seam), not the plain
`LazyTable::project_where` this plan (and the Mechanism 4 section above) has
been profiling with `IBEX_PROFILE_OPERATORS`.** That profiler's "source decode
whole/selected" labels come from `LazyTable::decode_columns`, called from the
*non-streaming* path — informative for q13/q15's specific numbers, but not
proof of the mechanism behind q04's regression or the general win, since q04
never takes that path at all. The actual lever is inside chunked.cpp's
per-source-unit decode (`project_where_unit`/`materialize_deferred_scan_unit`,
seen earlier in `lazy_table.cpp:781` and `interpreter.cpp`), which decodes
row-group-by-row-group rather than whole-file and evidently has performance
characteristics — sometimes much better, sometimes much worse — that neither
match nor were predicted by the whole-file `project_where` analysis above.

**UPDATE — the streaming-vs-whole-file framing above was a dead end, and the
real mechanism is now found.** Tested directly: `IBEX_STREAM_SCAN=0` forces
every deferred scan through the whole-file `project_where` instead of the
per-unit `project_where_unit`. q04, warm-process repeats, stream ON vs OFF:
**~87ms either way** — identical. The streaming/chunked engine was never the
cause; it just happened to be the call site my `IBEX_DEBUG_ASCRIBE` probe
landed on. Whole-file and per-unit both cost the same, because they share the
same underlying tradeoff.

**Root cause: `project_where`(`_unit`)'s split-decode is a net win only when
the predicate columns are expensive to skip gathering AND the remaining
(non-predicate, downstream-demanded) columns are cheap/few to gather. When
that ratio inverts, fusing a filter into it is a regression — and Mechanism 2
now lets more filters reach that fork with no gate deciding which side of it
they're on.**

The mechanism, spelled out: fusing a filter here always pays the same two
costs regardless of shape — decode the predicate columns *densely, whole* (to
compute the selection; unavoidable either way) and decode the *remaining*
demanded columns via a **Selection-gathered** decode (`decode_columns(...,
&selected, ...)`) instead of a **dense** one. The saving it buys is narrower:
skipping the *gather* of the predicate columns into final output (they were
going to be decoded either way, fused or not — the only thing avoided is
copying them into the result once selection is known). So:

- **q13 wins** because its predicate column (`o_comment`) is an expensive
  String, and its remaining set is two cheap Int64s (`o_orderkey`,
  `o_custkey`). Skipping the o_comment *gather* is a real saving; paying for
  two small int gathers instead of two small dense decodes is cheap. Net win,
  even though `o_comment`'s own filter is *unselective* (98.9% of orders
  survive at SF-1 — should be the losing case per the finding below, and
  isn't, because the predicate-avoidance term dominates here).
- **q04 loses** because its predicate columns (`l_commitdate`,
  `l_receiptdate`) are cheap Dates — there's nothing expensive being saved —
  while its remaining set is exactly one Int64 (`l_orderkey`, the only thing
  a semi join needs) that now has to be gathered instead of densely decoded,
  at 63% survival (SF-2) — a dense-ish selection, which is precisely where a
  gathered decode is known to lose most (`plans/done/parquet-filtering-scan-observations.md`'s
  rejected-experiment finding, cited in `decode-fusion-plan.md`'s Stage 4:
  "It loses most where it is exercised most (densest selection, +3-9%). There
  is no threshold that fires without losing.") — that finding was about a
  different code shape (in-decoder dense-then-gather) but the same physics
  applies to `project_where`'s remaining-column gather.
- **q01 is the sharpest confirming case, not yet benchmarked in isolation but
  matching the pattern exactly**: predicate is one cheap Date
  (`l_shipdate <= '1998-09-02'`, ~98% survival — dense/unselective, the
  losing regime) and the remaining set is **six columns including two
  Strings** (`l_returnflag`, `l_linestatus`) that must now be
  Selection-gathered instead of densely decoded. Worse ratio than q04 on both
  axes (more remaining columns, some of them strings, at even higher
  survival) — consistent with q01 being the second-worst regression (+24%)
  after q04.

**This generalizes and corrects Mechanism 3's framing.** Mechanism 3 called
for "an actual cost gate... so [deferred-probe scanning] stops being a net
loss on unselective joins" — that diagnosis was too narrow. The gap isn't
specific to deferred-probe scans, ascription, or streaming; it's in
`project_where`/`project_where_unit` themselves, and it was invisible before
because Mechanism 2 blocked most filters from ever reaching this fork. A
correct fix needs a real decision at the point a filter is considered for
scan fusion: estimate what's saved (predicate columns' decode cost, weighted
by type — String/Categorical high, numeric low) against what's paid
(remaining columns' gather-vs-dense delta, which scales with column count,
type, and survival rate). That is exactly the kind of guess-free cost model
`plans/join-order` work already had to build for join reordering — reuse that
instinct rather than a hand-picked threshold (see
`feedback_no_premature_constant_tuning` memory: finish + verify before tuning
a constant, and this isn't even at "tune a constant" yet — the shape of the
model doesn't exist).

**q03/q21 not yet individually confirmed against this model** — presumed the
same family (q03's post-join filters land on wide per-table remaining sets
after Mechanism 1 pushes them to scan level; q21's shape wasn't examined this
session at all) but do not cite as settled without checking each query's
actual predicate-column-cost vs remaining-column-cost split.

**Where this leaves the fix**: implemented, correct (1,639/1,639 tests,
22/22 `check_answers.py`), real net win on the full suite (geomean 0.938) —
but landing it as-is trades several small/medium wins for one large regression
(q04, +76%) and two medium ones (q01 +24%, q03 +17%) plus q21 drifting worse
(+11%), all traced to one mechanism: `projected_scan` now lets filters reach
`project_where`'s fork with no cost gate deciding whether fusing is actually
cheaper for that filter's shape (see the "Root cause" writeup above — it is
not the chunked/streaming path, that was a dead end, ruled out with
`IBEX_STREAM_SCAN=0`). The user who owns this branch judged the
`scan_predicates.cpp` code change stand-alone correct (kept, committed as
`120a567` "Peek through ascribe") — it is a real fix for what it targets,
just not perf-neutral on its own, exactly as this section warned before
landing was attempted. **What remains is the cost gate itself**: a decision at
the fusion fork weighing predicate-column decode-avoidance (String/Categorical
high, numeric low) against remaining-column gather-vs-dense cost (scales with
column count, type, and survival rate) — sketched above, not designed. Until
that gate exists, expect this shape of regression on any naturally-written
filter whose remaining/demanded set is wide, stringy, or whose predicate
columns are cheap and unselective — which, per the "naturally-written queries"
probe earlier in `decode-fusion-plan.md`, is most real queries, not just this
suite's four regressed ones.

### Cost gate: design scope (2026-08-20, not implemented)

Scoping only — no code, no constants. The goal is a decision at the point
`scan_predicates`' output would otherwise unconditionally fuse a filter:
should THIS scan's conjunct set be removed from the plan (letting
`project_where`/`project_where_unit` handle it) or left as an ordinary
`Filter` node?

**There are three cost terms, not one — worth separating because they may
pull in different directions and only one is confidently the dominant driver
from this session's evidence.**

1. **Decode-before-vs-after-filtering.** Unfused, `decode_columns` reads all N
   demanded columns (predicate + remaining) in one dense combined call before
   any row is dropped. Fused, only the M predicate columns are read to compute
   the selection; the N-M remaining columns are read afterward. Total bytes
   touched end up similar either way (fused doesn't skip reading the remaining
   columns, just defers and gathers them) — this term is probably small.
2. **Selection-evaluator speed.** `project_where`'s selection path
   (`select_bounds`/`filter_selection`, vectorized range/set checks) is likely
   faster per row than the ordinary `Filter` operator's generic AST-walking
   evaluator, independent of decode cost. This term favors fusing and is not
   measured yet this session — worth isolating before assuming it's small.
3. **Remaining-column gather vs. dense decode.** Fused, the N-M remaining
   columns are read via a **Selection-gathered** decode; unfused, they're read
   densely (as part of term 1) then row-filtered in memory (cheap index
   compaction, no re-decode). **This is the term this session's evidence
   points to as dominant**: q13 wins because gathering 2 cheap Int64s costs
   little against skipping an expensive String's gather; q01/q04 lose because
   the remaining set is disproportionately expensive (wide/stringy, or just
   nonzero) relative to what fusing saves on the predicate side, at high
   survival rates — which is exactly where a gathered decode is known to lose
   most (the Stage-4 rejected-experiment finding, different code shape, same
   physics).

**MVP scope: model term 3 alone first, verify it explains the measured cases,
add terms 1/2 only if it doesn't.** That's a materially smaller model to build
and calibrate, and per `feedback_no_premature_constant_tuning` there is no
reason to model what hasn't been shown to matter.

**Inputs already available, no new plumbing needed:**

- **Selectivity, real not guessed**: `make_relation_sampler` (`repl.cpp:1569`)
  already exists, is already wired into the driver
  (`source_stats.sample = make_relation_sampler(lazy_sources)`, `repl.cpp:4902`)
  for join-order costing, and already does exactly the sampling this gate
  needs: one `project_where_unit` call over one row group, returning
  `predicate_passed / sampled_rows`. It handles arbitrary predicate shapes
  (including q04's column-vs-column `l_commitdate < l_receiptdate`, which a
  footer-min/max approach can't estimate at all) and already declines cleanly
  (`nullopt`) when a predicate isn't sampleable. Reuse this function directly;
  do not build a second sampling path.
- **Column types**: `LazyTable::schema()`, already available at decision time
  in both driver call sites.
- **Footer min/max fallback**: `LazyTable::column_stats()` (already plumbed,
  used today for the dense-unique-key proof at `repl.cpp:1452`) gives a
  zero-decode selectivity estimate for a literal range predicate on an integer
  column, for when a sample isn't available or is judged too costly to take at
  this call site. Weaker than the sampler (no distinct-count info, and only
  covers integer columns) but free.
- **Remaining-column set**: already computed, just needs retaining — Stage 1's
  reordering computes `required_columns` after removing applied filters
  specifically to narrow it; the gate needs the value from *before* that
  narrowing (full demand) and *after* (narrowed demand) simultaneously, and
  their difference (predicate-referenced ∩ demand) is the remaining set. One
  extra `required_columns` call, not new infrastructure.

**Inputs that need real measurement, not invention:**

- **Per-type decode-cost-per-row weights** (numeric vs. String vs.
  Categorical) for term 3's gather side. This session's ad hoc profiling and
  `decode-fusion-plan.md`'s existing q03 profile (`decode_physical_column<DOUBLE>`
  32.5% vs `decode_physical_column<INT64>` 18.5% of scan time) are calibration
  *data points*, not a calibrated model — a controlled microbenchmark comparing
  dense vs. Selection-gathered decode per type, at a few survival rates, is
  the right way to get these. `tools/ibex_fusion_bench.cpp` is the precedent
  for this kind of isolated fusion-ratio benchmark in this codebase (different
  fusion, same idiom) — extend it or add a sibling rather than inventing a new
  harness pattern.
- **The margin, not just the sign.** Given the "symmetric prize" lesson from
  the join-order cost-model work (a mis-estimate can turn a good plan into a
  bad one, not just fail to improve it), and that declining to fuse simply
  reproduces today's known-safe baseline while wrongly fusing risks a
  q04/q01-shaped regression, the gate should require a clear predicted margin
  to fuse, not decide on any positive expected value. What margin is
  calibration work, not a design decision to make blind.

**Where the code should live**: a new `src/ir/scan_fusion_cost.{hpp,cpp}`,
mirroring `join_order.{hpp,cpp}`'s existing shape — a pure function over
already-computed inputs (predicate columns, remaining columns, schema,
optional `RelationSample`) returning a fuse/don't-fuse decision (plus a
`heuristic` flag, mirroring `CardinalityEstimate`, for anything derived from
the footer fallback rather than a real sample). Call it from both driver
`applied_filters` construction sites (`repl.cpp` ~5000 and ~4999) — same
pattern as `CardinalityOptions`, no new object threaded through the rest of
the driver.

**Validation protocol before landing, not after**: before wiring this into
the driver at all, build a decision table — every PDS-H query's per-scan
predicate/remaining column split, type, and measured selectivity, run through
the model's formula by hand or in a scratch script — and check its predicted
fuse/don't-fuse call against what's *already measured* this session: fuse
q06/q09/q13/q14/q15 (wins), decline q01/q04 (confirmed losses), and get
q03/q21 individually measured (not yet done) before trusting the model on
them. A model that doesn't reproduce known cases isn't ready to decide new
ones. Land behind an env var (the codebase's standing pattern —
`IBEX_STREAM_SCAN`, `IBEX_PARALLEL`) for A/B before defaulting on.

**Explicitly out of scope for the MVP**: mixed predicate sets where some
conjuncts are cheap-to-sample and others aren't; interaction with dynamic
membership filters (`DynamicScanFilter`) on deferred/probe scans, which have
their own economics already (Stage 5's two-phase probe) and may need a
separate decision or none at all since that path already gates on join
selectivity; whether the decision should be per-conjunct rather than
per-scan (today's `applied_filters` is per-scan, all-or-nothing — a
per-conjunct decision is a bigger change and there's no evidence yet it's
needed).

### Cost gate calibration, first measurements (2026-08-20)

Ran the microbenchmark this scope called for, and it substantially **sharpens
and partly overturns** the term-3 framing above — worth reading before
building the model, not just skimming.

**Setup**: synthetic 8M-row, 8-row-group Parquet file (`key: Int64` shuffled
0..N so `filter key < threshold` gives exact, scattered-survivor selectivity
— not a contiguous prefix, matching a real predicate's pattern), plus one
payload column per type under test: `payload_i64: Int64`, `payload_f64:
Float64`, `payload_str: String` (~40 chars, comment-field-representative).
Query shape: `read_parquet(...) as DataFrame<{key, payload_X}>[filter key <
threshold][select {payload_X}]` — isolates exactly term 3 (the remaining
column's gather-vs-dense cost), since the predicate (`key`) is always the
same cheap Int64 regardless of which type is under test.

**Method — a real, not simulated, A/B**: ran every (type, selectivity) query
twice — once against this session's HEAD (`120a567`, filter fuses into
`project_where`) and once against `322fb14` (pre-Mechanism-2,
`scan_predicates.cpp` swapped back temporarily, rebuilt, measured, restored —
not left in the tree). `IBEX_PROFILE_OPERATORS=1`, `IBEX_CORES=8`,
`taskset -c 0-7`, min-of-5 warm repeats per query, 22/22 `check_answers.py`
confirmed after restoring HEAD. This is the comparison the earlier
"gathered vs. 100%-selectivity dense-with-no-filter-at-all" numbers in this
section were NOT — that comparison forgot the unfused path also has to
decode the predicate column and then pay a separate in-memory
filter/compaction pass, and both of those scale with the remaining column's
own size and type too. The corrected numbers below invert several of the
earlier informal conclusions.

| remaining type | sel=10% | 30% | 50% | 70% | 90% | 100% (no filter) |
|---|---:|---:|---:|---:|---:|---:|
| Int64 (fused/unfused ratio) | 1.07 | 1.04 | **1.38** | **1.37** | **1.45** | 0.86 |
| Float64 (ratio) | 1.10 | 1.10 | **1.22** | **1.44** | **1.57** | 1.06 |
| String (ratio) | **0.40** | **0.56** | **0.72** | **0.71** | **0.72** | 0.97 |

(ratio = fused_ms / unfused_ms; **bold** = the losing side's ratio is far
from parity, i.e. a real effect, not noise; sel=100% has no filter, so
fused/unfused converge as expected — kept as a sanity check.)

**The finding: it's dominated by the remaining column's *type*, not
selectivity.** String remaining columns are cheaper fused at every
selectivity tested, by 28–60%. Int64/Float64 remaining columns are cheaper
*unfused* at nearly every selectivity tested (except the sparsest, 10%),
by 4–57%. Selectivity shifts the margin some but never flips the sign within
the range tested here.

**Why, in plain terms**: for a String (variable-width, allocates per value),
gathering only the survivors touches less allocation and copying than
decoding all 8M strings and then compacting the survivors in a second pass —
gather wins by avoiding wasted allocation on rows that get discarded anyway.
For Int64/Float64 (fixed-width, trivially copyable), a dense sequential
decode of all 8M values followed by a cache-friendly compaction pass beats a
*selective* Parquet decode, where reading through a scattered `Selection`
evidently costs the decoder more per value than a plain sequential read does
— consistent with the already-documented "Skip() partial-page gaps" trap in
`decode-fusion-plan.md`'s Stage 4 notes, now with a number attached instead
of just the warning.

**This reconciles every case measured so far, cleanly:**

- **q13 wins** despite its remaining columns (`o_orderkey`, `o_custkey`,
  both Int64 — the *losing* shape per this table) because its predicate
  column (`o_comment`, String) is expensive enough to skip gathering that it
  swamps the ~40% loss on two small Int64 remaining columns. The model needs
  a term-1-vs-term-3 tradeoff for cases like this, not term 3 alone.
- **q04 loses** because its remaining set (`l_orderkey`, Int64 — the losing
  shape) has no expensive predicate column to offset it (`l_commitdate`,
  `l_receiptdate` are cheap Dates) — nothing saved, a real cost paid.
- **q01 loses**, and the shape explains why even more precisely than before:
  its remaining set is FOUR real numeric columns (`l_quantity`,
  `l_extendedprice`, `l_discount`, `l_tax` — all in the losing shape) plus
  two tiny fixed-width categorical-like Strings (`l_returnflag`,
  `l_linestatus`, single characters — nowhere near this benchmark's 40-char
  strings, so their fuse-side win is real but small in absolute terms) against
  one cheap Date predicate. Four numeric losses outweigh two near-trivial
  string wins — net loss, and a bigger one than q04's single-column case,
  matching q01 being the worse regression of the two.

**Revised MVP model, now grounded in numbers instead of a shape argument**:
classify each column (predicate and remaining) into a cheap bucket (fixed-width
numeric: Int32/Int64/Float32/Float64/Bool/Date) and an expensive bucket
(String/Categorical — the only two types this session measured, and the ones
`ir::ColumnType` actually has). Fuse when predicate-side expensive-bucket
columns exist (skip their gather) and that saving isn't swamped by
remaining-side cheap-bucket columns (each costs *more*, not less, to fuse,
so more of them argues harder against fusing); lean toward fusing when
remaining is expensive-bucket-dominated regardless of predicate shape. This
is a **type-classification model, not a selectivity model** — selectivity
still matters (the margins do shift with it) but isn't the primary signal
this session expected it to be. Real per-row weights (not just the
sign/direction) still need to come from this same harness, generalized: more
row-group counts, more columns in the remaining/predicate sets simultaneously
(these numbers are all single-remaining-column; a q01-shaped multi-column
remaining set hasn't been measured directly, only reasoned about), and this
box's numbers only (SF/scale and CPU-specific — re-run before trusting exact
figures elsewhere, per `feedback_bench_interleaved_methodology`).

**Categorical measured 2026-08-20 — code-level split landed, and the physical
result is the OPPOSITE of "should behave like the cheap bucket".**

`ir::ColumnType` gained a `Categorical` variant, distinct from `String`
(`include/ibex/ir/node.hpp`); `column_ir_type` (`repl.cpp:1338`, the only
site converting a physical `Column<Categorical>` to a schema type) now
reports it as `Categorical` instead of collapsing it into `String`. Ascription
comparisons needed a lenient equality, not plain `==`, because no
`parser::ScalarType` spells `Categorical` — a user ascribing a dictionary
column always writes `String`, and that must still pass. Added
`ir::ascription_type_satisfies(have, field)`
(`include/ibex/ir/schema.hpp` / `src/ir/schema.cpp`, non-anonymous-namespace
so it's linkable) and used it in both places that used to disagree on this —
`schema.cpp`'s `check_one` (the proof `check_ascriptions` runs) and
`lower.cpp`'s static ascription check (the "live inconsistency" note two
sections up in `decode-fusion-plan.md` already flagged, now actually
unified rather than left to drift). `key_kind` (schema.cpp) maps
`Categorical` to `KeyKind::String`, matching its existing "several IR types
share a kind" comment. Interpreter and emitter switches got explicit
`Categorical` arms (compiler-enforced via `-Wswitch`, not hand-audited).
1,639/1,639 tests, 22/22 `check_answers.py`, verified `:schema` reports
`Categorical` correctly on a real dictionary-encoded Parquet column.

**Then measured it with the same fused/unfused harness, on a genuine
`dictionary<values=string, indices=int32>` column (20-category, 8M rows,
verified via `:schema`) — and it does NOT get int speeds today:**

| sel% | fused_ms | unfused_ms | ratio |
|---:|---:|---:|---:|
| 10 | 32.91 | 13.38 | **2.46** |
| 30 | 45.33 | 13.41 | **3.38** |
| 50 | 53.61 | 17.79 | **3.01** |
| 70 | 48.23 | 17.17 | **2.81** |
| 90 | 40.57 | 17.66 | **2.30** |
| 100 (no filter) | 7.57 | 7.50 | 1.01 |

Fused is 2.3–3.4× SLOWER than unfused — worse than plain String's own ratio at
the same selectivities. **The reasoning ("essentially an int, expect int
speeds") is right about the codes; the code doesn't deliver it yet.** Root
cause found: `plan_sharded_column` (`libs/parquet/parquet.hpp:1502`), which
decides whether a column's decode can be split across row groups (the
mechanism `sharded_numeric` gives every fixed-width numeric type — Int, Float,
Date, Timestamp), has **no `case arrow::Type::DICTIONARY`** — falls to
`default: return std::nullopt`. So a Categorical column, despite being
fixed-width dictionary codes underneath, is decoded as one indivisible
whole-column task, the same category `String` is stuck in (see Mechanism 4's
q13 finding above) — and evidently costs even more than String's whole-column
path when gathered, not less.

**This is a real, separate, scoped fix, not a modeling question**: give
`plan_sharded_column` a `case arrow::Type::DICTIONARY` that shards the fixed-width
code array the way `sharded_numeric` already shards Int32, keeping the
dictionary itself (small, shared) out of the per-shard work. Once that lands,
Categorical should re-measure into the cheap bucket the reasoning always
expected — re-run this exact harness before trusting that, don't assume.
**Until then, the cost-gate MVP model must bucket Categorical as
expensive-when-fused (same as String, arguably worse) based on measured
behavior, not on the type's theoretical cost** — bucketing it cheap today
would make the gate wrong in the same direction Mechanism 2 already got
burned by.

**IMPLEMENTED and RE-MEASURED 2026-08-20.** `libs/parquet/parquet.hpp` gained
`sharded_dictionary` and a `case arrow::Type::DICTIONARY` in
`plan_sharded_column`: each row-group shard decodes into its own LOCAL
dictionary (there is no way to know a row group's dictionary ahead of time)
and writes LOCAL codes directly into the shared flat codes buffer — as
parallel as any fixed-width numeric shard, since a code is just an int32.
`ShardedColumn` gained a `finalize` step (new, only dictionary uses it — every
other type still needs none, "there is nothing to merge afterwards" as the
struct's comment already said): run once on the calling thread after every
shard decodes, it unifies the per-shard local dictionaries into one global one
and remaps the already-written codes in place — the same per-chunk-to-one
remap `build_categorical_column` already does across Arrow chunks, done here
across shard outputs instead. `direct_decode_table` calls `finalize` for every
planned column that has one, right after the decode phase and before assembly.

Correctness verified two ways beyond `check_answers.py`/the full suite (both
still 22/22 and 1,639/1,639): `select {n=count()}, by {payload_cat}` over a
genuine 8-row-group dictionary column, dense AND filtered, matched Python's
`pyarrow`-computed ground truth exactly on every category — the filtered case
specifically exercises the Selection-aware per-shard decode plus the finalize
remap together, the two things most likely to have an off-by-one.

Re-ran the exact calibration harness on the same categorical column:

| sel% | fused_ms (before → after) | unfused_ms | ratio (before → after) |
|---:|---|---:|---|
| 10 | 32.91 → 14.26 | 13.38 | 2.46 → **1.07** |
| 30 | 45.33 → 22.33 | 13.41 | 3.38 → **1.67** |
| 50 | 53.61 → 23.71 | 17.79 | 3.01 → **1.33** |
| 70 | 48.23 → 25.16 | 17.17 | 2.81 → **1.47** |
| 90 | 40.57 → 26.76 | 17.66 | 2.30 → **1.52** |

Fused decode time roughly halved at every selectivity, and `pool_tasks` on the
gather stage went from 0/1 (one indivisible task) to 8 (matching the 8 row
groups) — confirmed via `IBEX_PROFILE_OPERATORS`. **Categorical's ratio now
sits in the same band Int64/Float64 showed in their own calibration
(1.04–1.57 across the same selectivities)** — the reasoning was right: once
`plan_sharded_column` can split it, a dictionary column behaves like the
fixed-width numeric bucket it structurally is, not like String.

**But this does NOT, by itself, move q01 or q04's whole-query numbers** —
checked directly, same-session interleaved A/B against the pre-fix binary,
`IBEX_PROFILE_OPERATORS` on the exact "source decode selected" stage: q01
(warm, min-of-3) 71–73ms either side of the fix, indistinguishable from
noise; q04/q13 (unaffected controls — no Categorical column in their remaining
sets) likewise unchanged. **Why**: q01's gathered-decode stage bundles SIX
remaining columns in one call — `l_quantity`/`l_extendedprice`/`l_discount`/
`l_tax` (Float64) plus `l_returnflag`/`l_linestatus` (Categorical, confirmed
via `:schema` against the real TPC-H Parquet files — `l_shipinstruct` and
`l_shipmode` are Categorical too). The dictionary fix only touches two of
those six columns, and it fixes them from *worse than String* down to
*in line with the other four*, which were already there — Float64's own
gather-vs-dense loss (measured earlier: 1.10–1.57× across the same
selectivities, never fixed by this session's work, a materially different
problem since `sharded_numeric` was already correctly parallel and the
inefficiency is inside the gathered decode kernel itself, not the task split)
still dominates the stage. **The logic was sound and the win is real and
verified — it just isn't the win that moves q01's number, because q01's
remaining set was never Categorical-dominated. It moves any query whose
remaining set actually IS Categorical-heavy relative to its numeric columns**,
which this suite doesn't currently have an example of in isolation; worth
watching for on real workloads rather than expecting on PDS-H.

**Corrected cost-gate MVP model**: Categorical now belongs in the same bucket
as Int64/Float64/Date (cheap-fixed-width) for the fusion decision, not the
expensive bucket it needed provisionally before this fix landed. The
remaining, still-open item for the model is what Float64's own 1.1–1.6× loss
implies: fixed-width numeric types are not "free" when gathered either, just
cheaper than String/Categorical's *old* (pre-fix) cost — the MVP model's
two-bucket framing (cheap vs. expensive) is directionally right but the
"cheap" bucket still has a real, measured cost that the gate's margin
requirement (see the design-scope section above) has to account for, not
treat as zero.

### Float64/Int64's own gather-vs-dense loss — ATTEMPTED 2026-08-20, reverted, no improvement

Asked to fix the remaining 1.1–1.6× loss the same way Categorical's was
fixed. **This is not the same class of bug.** Categorical's fix was a real
task-parallelism gap — `plan_sharded_column` never split dictionary columns
across row groups at all. Numeric columns were already correctly sharded;
`sharded_numeric`/`decode_numeric_into` already parallelize across row groups
for both dense and selective decode. What's left is per-value cost *inside*
one shard's decode, and — this needed to be found before writing any code —
`decode-fusion-plan.md`'s Stage 4 already documents this exact territory as
**"rejected experiment #1," measured and rejected four times**: "dense-decode
the block, evaluate the selection in-block, gather survivors... the mechanism
is structural, not a tuning problem: at every selectivity tested the
survivors still touch every page, so both paths decode every value, and the
dense path merely adds a scratch write plus a gather on top... There is no
threshold that fires without losing." The doc's own conclusion: the only
genuinely open lever is selection-awareness *inside* the encoding decoder
(below Arrow's public `TypedColumnReader` API, needing an Arrow patch/fork),
previously measured at only 1.15× on numerics and 0.93× (slower) on strings.

Flagged this to the user before writing code; asked to attempt it fresh
anyway rather than trust the old finding blind, so it was attempted with a
real measurement, honestly reported either way.

**What was tried**: a specialized `try_decode_numeric_selected_dense` (stayed
above the `TypedColumnReader` boundary — not the below-Arrow-API lever the
doc flagged as the only real ceiling): for the common case (Raw/Out same
layout, every covered row group proven null-free, selection not sparse
enough to prefer `Skip()`), read each batch via `ReadBatch` into a reusable
scratch buffer, then gather selected offsets directly into `output_data` with
a tight loop — skipping `decode_physical_column`'s generic per-value `emit`
callback and per-value `DirectValidity::append` branch. Falls back to the
existing generic path (unchanged) for anything it declines. Correctness: 22/22
`check_answers.py`, 1,639/1,639 tests, both checked with the fast path live.

**Measured, not assumed — same calibration harness, same-session A/B (fast
path compiled in vs. `&& false`-disabled, same binary swap technique used for
the dictionary fix), two rounds each to see the noise floor**:

| sel% | f64 fast/disabled | i64 fast/disabled |
|---:|---|---|
| 10 | −4.2% | **+22.0%** |
| 30 | −17.8% | −13.9% |
| 50 | −17.8% | −7.4% |
| 70 | **+6.8%** | **+5.7%** |
| 90 | −3.8% | **+5.4%** |

No consistent sign. Worse: re-running the *same* disabled configuration twice
back-to-back showed run-to-run swings as large as the effect itself (i64@10%:
7.40ms one round, 10.13ms the next — a 37% spread with *nothing changed*).
This box's noise floor at these tiny (5–20ms) measurement scales swamps
whatever the fast path does or doesn't buy — consistent with
`feedback_bench_interleaved_methodology`'s standing warning about this WSL2
box, and exactly why the project's own bench discipline calls for the full
interleaved-rounds-plus-Wilcoxon protocol before trusting a number this size,
which this quick check deliberately did not run (not worth the time to build
that rigor for what the prior finding already predicted).

**Reverted in full** — the function and its call site, back to the exact
pre-attempt `decode_numeric_into`. 22/22 `check_answers.py` and 1,639/1,639
tests reconfirmed after reverting. This *is not a refutation* of the idea so
much as a **reproduction of the existing "rejected experiment #1" finding**
via a different specific implementation, which is useful to have on record:
two independent implementations (the block-dense-decode-then-gather shape
this doc already tried, and this session's leaner scratch-then-tight-loop
gather) both land in the same place. **Do not re-attempt this specific
approach a third time without new evidence** — e.g., a different box with a
usable noise floor, or `perf stat`/hardware counters showing where the cycles
actually go rather than inferring from wall-clock deltas this small. The
below-Arrow-API lever remains the only path the project has identified with
real headroom, at a cost (Arrow fork, 1.15× ceiling, string regression) this
session did not attempt and does not recommend attempting without it being
explicitly worth that maintenance burden.

Calibration harness, dataset, and raw JSON results live in this session's
scratchpad only (not committed — ephemeral per-session location); reproduce
by regenerating the dataset and queries described above (categorical: build
via `pa.DictionaryArray.from_arrays`, not a plain string column — a plain
`pa.array()` of strings does not get an Arrow `dictionary<...>` field type
just because Parquet happens to page-encode it with `RLE_DICTIONARY`, and
ibex's reader classifies by the Arrow field type, not the page encoding)
rather than looking for the files.

### Follow-up: `perf` profiling found a real confound, changed the picture, still no win

Asked to profile further rather than accept the noisy wall-clock verdict above.
`perf record -e cpu-clock` (WSL2 has no hardware PMU access —
`perf stat -e cycles` returns `<not supported>` — but software timer-based
sampling works and is enough to see where time actually goes) on a
sample-rich synthetic workload (60 warm repeats in one process, 999 Hz).

**Found a real methodology bug first**: the calibration dataset this whole
session (String/Categorical/Int64/Float64, the fix that landed, and the fast
path just reverted) was written with `pq.write_table`'s default compression —
**Snappy**. The real TPC-H benchmark files
(`benchmarking/data/tpch/parquet_sf*`) are **`UNCOMPRESSED`** (checked
directly against `lineitem.parquet`'s row-group metadata). Every ratio
measured this session was on a differently-compressed dataset than the one
the whole investigation is about.

**On the Snappy-compressed profile**: `snappy::DecompressBranchless` alone was
36.27% of self time, `SerializedPageReader::DecompressIfNeeded`'s children
49.20% — decompression dominates the profile outright, and it's paid
identically by dense and selective decode (a page must be fully decompressed
before ANY value, selected or not, can be read out of it). This explains why
the first fast-path attempt showed no signal: the code it targeted (the
post-decode gather step) was a small slice of a decompression-dominated cost.

**Regenerated everything uncompressed and re-profiled.** With compression out
of the way, `sharded_numeric<double>`'s own lambda is a real 20.56% of self
time — no longer buried under decompression. The dominant `memmove` traced to
`PlainDecoder::Decode`'s internal page-to-buffer copy (Arrow's own machinery,
identical for dense and selective decode, not something the fast path could
have touched) — not an artifact of the gather step being optimized.

**Re-applied the fast path and re-measured on uncompressed data, two clean
rounds** (per-query `whole_ms` baseline now stable at ~6.2ms ± 0.5ms across
runs — a much tighter noise floor than the compressed-data attempt, whose
"identical" reruns swung 37%):

| sel% | f64 fast/disabled (min-of-2) | i64 fast/disabled (min-of-2) |
|---:|---:|---:|
| 10 | 1.02 | 0.99 |
| 30 | 0.96 | 1.02 |
| 50 | 1.02 | 1.06 |
| 70 | 1.13 | 1.00 |
| 90 | 1.10 | 1.04 |

Parity or slightly worse at every point, consistent across both rounds — a
materially more confident negative result than the first (noise-dominated)
attempt, not just a repeat of it. **Reverted again**, same as before; 22/22
`check_answers.py`, 1,639/1,639 tests reconfirmed.

**What this changes**: the profiling was worth doing — it surfaced a real
bug (wrong compression setting confounding every calibration number this
session produced) and gave a properly-controlled re-measurement instead of
leaving the question on a noisy first attempt. It does NOT change the
verdict: even cleanly measured, on the correct (uncompressed) file format,
the fast path doesn't win. This now stands on firmer ground as a genuine
third confirmation of "rejected experiment #1," not a noise-obscured one.

**Open question this raised — now closed, re-measured 2026-08-20 on
uncompressed data (matching real TPC-H files exactly).** Full type ×
selectivity grid, same technique (real A/B: current HEAD vs. `322fb14`'s
`scan_predicates.cpp` swapped in, rebuilt, measured, restored — 22/22
`check_answers.py` reconfirmed after), min-of-5 per point:

| type | 10% | 30% | 50% | 70% | 90% | (compressed, for comparison) |
|---|---:|---:|---:|---:|---:|---|
| Int64 | 0.97 | 0.99 | 1.32 | 1.36 | 1.58 | 1.07 / 1.04 / 1.38 / 1.37 / 1.45 |
| Float64 | 0.93 | 1.05 | 1.22 | 1.31 | 1.46 | 1.10 / 1.10 / 1.22 / 1.44 / 1.57 |
| String | 0.35 | 0.44 | 0.49 | 0.58 | 0.63 | 0.40 / 0.56 / 0.72 / 0.71 / 0.72 |
| Categorical | 0.76 | 1.12 | 1.53 | 1.35 | 1.60 | 2.46 / 3.38 / 3.01 / 2.81 / 2.30 |

**Int64/Float64 track the compressed-data numbers closely** — the "cheap
bucket still has a real, non-zero cost" finding is robust to compression
setting, confirmed rather than an artifact.

**String fusion wins even bigger uncompressed** (0.35–0.63 vs. 0.40–0.72
compressed) — makes sense: with no decompression to share, the avoided dense
decode of 8M 40-char strings is a larger fraction of what's left.

**Categorical is the big correction, and it's good news**: 0.76–1.60 on the
correct file format, essentially indistinguishable from Int64/Float64's own
range (0.93–1.58 / 0.93–1.46) — a much cleaner confirmation of "the dictionary
sharding fix delivers int speeds" than the compressed-data measurement showed.
The earlier 2.3–3.4×→1.07–1.67× improvement was real (the sharding fix is
unambiguously correct and necessary — see above), but the compressed-data
*post-fix* ratio was itself inflated by decompression interacting with the
Skip/dense-batch machinery differently for dictionary pages than for plain
numeric ones; measured on the file format that actually matters, Categorical
lands cleanly in the cheap bucket alongside Int64/Float64, not near it.

**Cost-gate model, now calibrated on the right data**: the two-bucket
framing holds — cheap (Int64/Float64/Date/Categorical, ~0.9–1.6× depending on
selectivity, real but bounded cost) vs. expensive (String, 0.35–0.63×, a
genuine win to fuse). Any margin threshold built from this session's numbers
should use the uncompressed figures above, not the earlier compressed ones.

### Mechanism 3 — `deferrable_probe_scans`'s `match_probe_chain()` doesn't see through `Ascribe` either — and that's currently an ACCIDENTAL win

`collect_deferrable` (`scan_predicates.cpp:367`) marks a scan eligible for
deferred-probe treatment — decoded lazily per-chunk by the join itself using
dynamic key bounds, **deliberately bypassing the whole-table cache** (see
`repl.cpp`'s comment at the `deferred_scans.emplace` site) — when it's the
sole right-side feed of a single-key inner join, found by
`match_probe_chain()` walking through `Project`/`Rename`/`Update` only. No
`Ascribe` case; same `default: return std::nullopt` decline as Mechanism 2.

**Confirmed** on q12 (`orders join lineitem on {o_orderkey=l_orderkey}`, no
scan-level filter at all — Mechanisms 1 and 2 don't apply): `required_columns`
computes the *identical* narrow demand for both ascribed and unascribed runs
(verified with a temporary debug print, reverted — see method note below).
The difference is call count: ascribed lineitem decodes its 5 needed columns
**once**; unascribed decodes the same 5 columns **~9 times** (repeated,
uncached, once per chunk the deferred-probe join touches). q12's join isn't
selective enough for deferred materialization to pay for itself, so the
repeated small decodes cost ~3x a single big parallel one (223ms ascribed vs
613ms unascribed for lineitem's "source decode whole" stage). Same shape very
likely explains q09 (1.71x worse under the reverted elide-fix, see below).

**This means ascription is currently protecting q09/q12 from a real,
independent cost-model gap in deferred-probe scanning**: it fires whenever the
structural shape matches, with no check on whether the join is actually
selective enough to make repeated uncached decoding worth it. **Do not fix
Mechanism 2 without keeping this blind spot in mind** — see "tried and
reverted" below for what happens if both get unblocked at once.

**Fix, two options, not yet chosen**: (a) leave `match_probe_chain` as-is
(don't extend it to see through `Ascribe`) as a stopgap — keeps q09/q12
protected by accident, but permanently forecloses deferred-probe for anyone
who ascribes; or (b) the real fix — give deferred-probe scanning an actual
cost gate (estimated join selectivity, or cache the per-chunk decode across
calls) so it stops being a net loss on unselective joins regardless of
ascription. (b) is bigger scope but is the only one that also helps
*unascribed* queries hitting the same shape, which is presumably most queries
today given none of the suite ascribes by convention going forward.

**FIXED (2026-08-20), option (b) chosen** — no stopgap. Two-part change in
`src/ir/scan_predicates.cpp`:

1. `match_probe_chain()` gained a `case NodeKind::Ascribe:` — declines
   (`return std::nullopt`) unless `checked()`, otherwise descends into the
   single child unchanged (Ascribe never renames, so no key remapping is
   needed, unlike `Project`/`Rename`). This is the same transparency
   Mechanism 2 gave `projected_scan()`.
2. New `contains_row_reducing_node(const Node&) -> bool`, the actual cost
   gate: recursively true if the subtree contains a `Filter`/`FilterProject`/
   `FilterUpdateProject`/`FilterHead`/`FilterTail`/`Head`/`Tail`/`TopK`/
   `Distinct`, or a `Join` that isn't a plain unpredicated `Inner` join.
   `collect_deferrable()` now requires
   `contains_row_reducing_node(*join.children()[0])` (the build/left side) to
   be true before marking the right side deferrable — i.e. deferred-probe
   scanning is only offered when the build side is known to have been cut
   down first, which is the actual economic precondition (a large,
   unfiltered build side means many probe chunks touch the deferred scan,
   each paying its own uncached decode — the q12/q09 shape). This protects
   q09/q12 by the real structural reason instead of by ascription accident,
   and stops foreclosing deferred-probe for ascribed queries whose build side
   *is* cut down.

Verified: 10 dedicated `deferrable_probe_scans` tests (2 new — "an unfiltered
build side blocks eligibility", "a build side reduced by a nested join still
counts") plus the full suite (1641/1641) pass. q09/q12 confirmed unaffected
(still protected, now for the right reason). q04 confirmed unaffected (semi
join — `collect_deferrable` is Inner-only, never reached this path anyway).
Kept.

**REVERTED 2026-08-20, same day, after a full-suite re-check the "kept" verdict
above never ran.** Everything above this paragraph was checked against three
named queries (q04, q09, q12) individually, never against the other 19. A
clean, contention-free full-suite SF-2/8-core A/B (interleaved binary swap,
same technique as elsewhere in this doc, `warmup 2 / iters 5`, box confirmed
idle via `ps --sort=-pcpu` first) surfaced a real regression the targeted
checks missed entirely: **q08 (`part join lineitem` unfiltered, six more joins
before any predicate lands) went from ~77-80ms to ~155-170ms, a clean ~2x**,
isolated by rebuilding with only `scan_predicates.cpp` swapped between the two
commits (chunked.cpp's `materialize_row_local` refactor confirmed innocent —
same fast number either way). Full-suite totals: 2230ms without the gate,
2459ms with it — **+10.3%, this fix is a net loss on the suite it shipped
with, not a win.**

**Why q08 falsifies the heuristic**: `contains_row_reducing_node` uses "does
the build side contain a Filter/Head/Tail/TopK/Distinct/predicated-join
node" as a stand-in for "the build side is smaller than the probe side" — but
in q08, `part` (the build side of `part join lineitem`) has no filter at that
point in the chain (the `p_type` predicate lands six joins later) yet is still
genuinely tiny relative to `lineitem` (200K vs 12M rows at SF-2) purely
because it's a dimension table with no row-reducing operator anywhere near it.
Presence-of-filter and "small relative to the probe side" are different
properties, and this heuristic conflates them: it correctly blocks q12's
`orders join lineitem` (orders is itself large and unfiltered, a real
whole-table-vs-whole-table join) but wrongly blocks q08's `part join lineitem`
(part is small by construction, not by filtering) using the same test.

**The two pieces don't decompose cleanly, either** — this was checked before
reverting, not assumed: keeping only the `Ascribe`-transparency half of the
`match_probe_chain` change (dropping the `contains_row_reducing_node`
condition) reintroduces the *original* q12 regression this whole mechanism
exists to prevent, for the reason given further up this section (ascription
was accidentally blocking `match_probe_chain` from ever reaching q12's
lineitem at all; making it transparent without a gate unblocks the same bad
unconditional deferral). So there is no free subset of this change to keep —
it needs a real selectivity/cardinality signal (row counts from footer stats
or `column_stats()`, not a structural Filter-node proxy) to tell q08's shape
apart from q12's, which is unbuilt. **Fully reverted**
(`src/ir/scan_predicates.cpp` and its two new tests in
`test_ir_required_columns.cpp` back to pre-Mechanism-3-gate state; the
`chunked.cpp` `materialize_row_local` consolidation is kept, since it's
confirmed perf-neutral on its own and a legitimate dedup regardless of this
gate's fate). 1643/1643 ctest, 22/22 `check_answers.py` reconfirmed at the
reverted state.

**Where this leaves Mechanism 3**: back to its original, pre-2026-08-20-fix
status — `match_probe_chain` does not see through `Ascribe`. The idea is not
dead: a real fix needs an actual cardinality estimate at the
`collect_deferrable` decision point (the same kind of input the "Cost gate"
design-scope section above already scoped for a *different* fusion decision —
`make_relation_sampler`/`column_stats()` are the same inputs this gate would
need too) rather than a structural proxy. Real per-source row counts are
already computed and available at both driver call sites
(`row_counts.insert_or_assign(source.source_name, lazy.value()->rows())`,
`repl.cpp` ~4885) for free (footer metadata, no scan) — a build-side-vs-
probe-side row-count ratio is a much better-grounded signal than "does the
build side contain a Filter node" and is the natural next thing to try,
**but needs calibrating against more than 2 data points** (q08's `part`
400K/lineitem 12M ≈ 3.3%, defer-and-win; q12's `orders` 3M/lineitem 12M ≈
25%, defer-and-lose, at SF-2 — a threshold that separates these two is not
yet validated against the rest of the suite's deferred-probe-eligible shapes).
Do not re-attempt the pure-structural-proxy shape a second time; a
row-count-ratio version is worth trying, but treat 2 data points as a
starting hypothesis, not a calibrated threshold (`feedback_no_premature_constant_tuning`).

**Correction, found while chasing the row-count idea (2026-08-20, same
session), then corrected a second time after a right challenge**: the
"q09/q12 stay protected by the ascription accident" framing above is not
accurate for q12 specifically. Traced with temporary debug instrumentation in
`collect_deferrable` (added and reverted within this session, not
committed): on the current tree, `match_probe_chain` on q12's `orders join
lineitem` fails because the join's right side is a **`Filter` node directly
wrapping the lineitem `Scan`** — `l_shipmode`/`l_commitdate`/`l_receiptdate`'s
predicates have already been pushed onto the scan by the time
`deferrable_probe_scans`' second call site runs.

**This first write-up called that "incidentally fixed... as a side effect of
unrelated architectural work" — that claim was wrong, caught on review.**
Ascription unblocking `push_filters_into_joins` is not new and not
incidental: it is the mechanism Mechanism 1's own section above already
documents (`lowerer.source_schemas()` treats a checked `Ascribe` as Known
schema from the second call site onward, which is *why* "Ascribing the
reader restores it" — that sentence is from the original 2026-07-17 finding,
re-verified live earlier the same session as this plan's "RE-VERIFIED LIVE"
note). Mechanism 4's section names this exact interaction already:
"ascription specifically... enables Mechanism 1's pushdown, which lands a
filter on the scan, which `scan_predicates` fuses into `project_where`'s
selective path." q12 is ascribed (`as DataFrame<{...}>` on both `orders` and
`lineitem`), so this Filter-on-Scan shape is simply q12 exhibiting the
already-known, already-documented behavior — not a new consequence of
Ascribe-as-Scan-metadata, and not evidence that Mechanism 1's real gap (the
*first* `push_filters_into_joins` call site, `lower.cpp:5186`, which runs
before any schema — ascribed or not — exists yet) is fixed. That gap is
unchanged and unconfirmed either way; nothing this session touched it.

**What's still real and useful from this trace, correctly scoped**: q12's
`lineitem` doesn't reach `collect_deferrable`'s decision point at all,
because Mechanism 1 (as already understood, via ascription) already routed
its filter onto the scan, and Mechanism 2 (landed) lets that filter fuse
into `project_where`'s selective decode directly — a materially different,
already-existing path than deferred-probe, that happens to also solve q12's
regression without needing Mechanism 3 at all. That interaction (Mechanism 1
+ Mechanism 2 jointly routing some deferred-probe-eligible-looking joins away
from `collect_deferrable` before it ever runs) is real and worth accounting
for before calibrating a row-count-ratio gate: **re-survey which queries
still reach `collect_deferrable`'s decision point on the current tree before
assuming the eligible set matches what the original Mechanism 3 write-up
measured** — it may have shrunk for reasons unrelated to any fix attempted
here.

### Mechanism 4 — a predicate column's whole-file decode stalls on task imbalance (RETARGETED 2026-08-20 — not project_where, and not select_bounds)

Originally framed as "`project_where`'s fused path's barrier stalls", by
analogy to `[[project_scan_predicates_rename_blind_spot]]` (memory,
2026-08-19), which documented q12/q14 costing hard (barrier_wait_ms 43-55ms)
when a `Rename`-fix change routed more filters into `project_where`'s
three-phase fused decode. That fix was reverted; the underlying barrier-stall
problem in `direct_decode_table`'s task scheduling was never fixed, just
avoided.

**Re-profiling q13/q15 live (below) shows this is a different call site than
that framing assumed.** Because Mechanism 2 blocks fusion for both queries,
neither ever reaches `project_where`'s fused path at all — they take the
*ordinary* `project()` → `project_uncached` → `decode_columns` route (demand
includes the predicate column because the interpreter's plain `Filter` node
still needs it). The barrier stall lives in `direct_decode_table`
(`libs/parquet/parquet.hpp:1767`) itself — the same underlying scheduling
issue the old memory found, but hit from the unfused path, not the fused one.
Keep the mechanism identity (`direct_decode_table` task imbalance), drop the
`project_where`-specific framing.

**Re-confirmed live today (2026-08-20) via a different trigger**: q13 and q15
both regress from ascription specifically because ascription **enables**
Mechanism 1's pushdown, which lands a filter on the scan, which `scan_predicates`
fuses into `project_where`'s selective path (Mechanism 2 doesn't block it here
because these filters reach a bare/Rename-wrapped scan, not through an
`Ascribe`) — and that selective path is the one with the barrier problem.
`IBEX_PROFILE_OPERATORS=1` on q13: ascribed `barrier_wait_ms=100.3` (of
237ms wall) vs unascribed `barrier_wait_ms=0.2` (of 161ms wall, and unascribed
is *faster overall* despite doing "more" decode work, because it skips the
fused path entirely). q15 shows the same shape (32.8ms vs 0.5ms barrier wait).

**This is the same root defect as the old memory, just reached by a new
route** (ascription-enabled pushdown, not a `Rename`-skip fix) — worth fixing
once, not per-trigger. That memory's own next step still stands: the
`WorkerPool`'s shared-queue dynamic dispatch (confirmed present — see
`direct_decode_table` in `libs/parquet/parquet.hpp`, which already
oversubscribes via an atomic cursor and is NOT the bottleneck) is not being
used by `select_bounds` (`src/runtime/filter.cpp:3259`), whose morsel count is
sized ≈ to the worker count with no headroom for the shared queue to
rebalance an uneven block. **Not yet verified this is still the actual
mechanism on current HEAD** — the barrier-count/wait numbers above are fresh,
but which specific stage inside the 3-phase path owns the wait was not
re-isolated down to `select_bounds` specifically this session (the old memory's
diagnosis was on a different code path). Confirm before implementing.

**UPDATE 2026-08-20: the `select_bounds` hypothesis is WRONG. Verified live —
the stall is in `direct_decode_table`'s predicate-column decode, not
`filter.cpp`.** Reprofiled q13 and q15 (ascribed, `IBEX_PROFILE_OPERATORS=1`,
SF-2, pinned 0-7) with per-node breakdown, something the earlier whole-process
numbers never gave. In both queries the dominant node is `source decode
whole` — `LazyTable::decode_whole_columns` → `decode_columns` →
`direct_decode_table` (`libs/parquet/parquet.hpp:1767`) decoding the
predicate-only column(s) densely, *before* any `select_bounds`/`filter` node
even runs. `select_bounds` itself is small in both cases (q15's `filter` node:
4.3ms of a 49ms stage). Two distinct sub-causes, neither fixed by
oversubscribing `select_bounds`'s morsel count:

- **q13** (Mechanism 2 blocks fusion — the `!like(o_comment,...)` filter sits
  directly over `Ascribe(Scan)`, so `scan_predicates` never picks it up):
  `decode_whole_columns` decodes all three of `orders`'s referenced/needed
  columns at once — `o_comment` (String, `plan_sharded_column` has no case for
  strings, so it is **one indivisible whole-column task** — the code's own
  comment: "a dictionary or string column has to be decoded end to end by a
  single worker") plus `o_orderkey`/`o_custkey` (Int64, shardable, 3 tasks
  each across orders' 3 row groups). 7 tasks, 8 workers. `source_self_ms≈100`,
  `barrier_wait_ms≈100` — essentially ALL of it is the caller parked. The
  6 int64 shards finish fast and go idle; the one `o_comment` task, decoding
  all 3M rows of a string column alone, sets the floor. `direct_decode_table`'s
  existing shared-cursor oversubscription (confirmed real, see Mechanism-4's
  original note) cannot help here — there is nothing to steal *from*, the
  string decode is irreducibly one task. Un-ascribing the same query does NOT
  hit this at all: it takes an entirely different route,
  `fusable_string_conjuncts`/`scan_string_filters` ("source string filter
  scan"), which evaluates the LIKE pattern inside the decoder without ever
  materializing `o_comment` densely — that route only exists because the
  bare/Rename-wrapped scan lets a *different* pass (the string-conjunct
  fusion, same `Ascribe`-blindness bug as Mechanism 2/3) succeed.
- **q15** (same Mechanism-2 block, `l_shipdate` range filter directly over
  `Ascribe(Scan)`): `decode_whole_columns` decodes `l_shipdate` alone — Date is
  numeric/shardable, so this doesn't hit the string trap, but lineitem's row
  groups (SF-2) are too few relative to 8 workers, so the split is genuinely
  undersubscribed. `barrier_wait_ms=27` of a 49ms stage — real, but milder,
  and *this* half of the finding is closer in shape to the original
  hypothesis, just in the wrong file: the fix would be finer-grained task
  splitting inside `direct_decode_table`'s row-group loop
  (`libs/parquet/parquet.hpp:1840`), not `select_bounds`.

**So "oversubscribe `select_bounds`" is not the fix; it doesn't touch the code
path either query actually stalls in.** `select_bounds` is only reached at all
when `scan_predicates` fuses a filter into `project_where`'s selective path —
Mechanism 2 blocking fusion means neither q13 nor q15 gets there today. The
real levers, in order of how directly they address what was just measured:

1. **Fix Mechanism 2** (`projected_scan` sees through checked `Ascribe`) —
   this was already next in the suggested order, but it now also matters here:
   fixing it routes q13/q15 through `project_where`'s fused path, where
   `l_shipdate` becomes a predicate-only column that's still decoded whole
   (same task-imbalance exposure) but at least the o_comment-shaped case might
   route through `fusable_string_conjuncts` instead of the dense path — needs
   checking once landed, not assumed.
2. **Row-group task granularity in `direct_decode_table`** — split a task
   further when `tasks.size() < workers` for a single-column decode (q15's
   shape), so the existing worker budget is actually used.
3. **String-column decode is fundamentally one task** — that's q13's shape,
   and no morsel-count tuning fixes it. Either it needs sub-column sharding
   (splitting the decode itself, not just the task count — bigger, riskier
   scope, undo-able only with real profiling of the string decoder), or the
   fix is upstream: make `fusable_string_conjuncts` see through `Ascribe` too
   (parallel to Mechanism 2/3's fix) so ascribed LIKE-filtered scans take the
   fused route that already exists and already avoids this for free.

Not yet re-measured: whether fixing Mechanism 2 alone (without touching
`direct_decode_table`) changes these numbers materially — do that first, it's
the cheapest experiment, before scoping either of items 2-3 above.

## What's NOT explained by any of the above

**q21** (the single biggest loser, +234ms of the +250ms total): ascribed vs
unascribed measured within noise of each other (480ms vs 483ms, single-run
profiling — not the full interleaved protocol, re-measure before citing an
exact number). This is a genuine query-shape gap: the realigned q21 computes
`count() by l_orderkey` over the *entire* unfiltered ~12M-row (SF-2) lineitem
table, twice, before joining down to `o_orderstatus == "F"` — the hand-written
original filtered orders to "F" and semi-joined that in *first*, before any
aggregation. Recovering this needs the planner to push a filter back through a
self-join and two aggregates onto the group-by's input — real, currently
unimplemented, planner work, not a quick pass fix. Both ascribed and
unascribed q21 show large barrier counts too (16 vs 63) — plausibly
Mechanism 4 again, layered on top of the shape gap, but not isolated from it
this session.

**q22** (+51%): also within noise between ascribed/unascribed (~86ms vs
~84ms). The query's *logic* genuinely changed under realignment (an
`anti join` became `left join` + `filter is_null(...)`, plus the correlated
scalar average moved from an inline `scalar()` subquery to a materialized
`cross join`) — this needs its own investigation into whether the rewritten
shape is inherently more expensive or whether it's tripping one of the same
four mechanisms in a way not yet isolated. Not started.

## Tried and reverted — do not repeat without new evidence

**"Elide every checked `Ascribe` node from the IR right after
`check_ascriptions` proves it"** (`ir::elide_checked_ascriptions`, touched
`schema.hpp`/`schema.cpp`/`repl.cpp`) — mechanically correct (22/22
`check_answers.py`, full ctest suite green), and it does fix Mechanism 2 for
free (q06 22.2ms, q13 116.4ms — both better than the old hand-fused baseline).
**But it also unblocks Mechanism 3 for q09/q12** (296.9ms and 81.9ms — both
*worse* than even the pre-fix ascribed numbers), because eliding the node is
structurally identical to never having ascribed at all, everywhere, not just
at the one call site that needed it. Net effect on the SF-2 suite: **+25%
total, worse than the +13% starting point.** Reverted in full; the diff never
landed. Any future fix that makes `Ascribe` transparent to *every* pass needs
to check q09/q12 specifically before being trusted, not just the queries it
was aimed at.

## Suggested order of attack

**Superseded 2026-08-20 — Mechanism 2 is landed, and it surfaced the real
next item.** The plan below (numbered 1-5) reflects understanding as of
2026-07-17, before Mechanism 2 shipped. Mechanism 2's own writeup above
(`projected_scan` walking through checked `Ascribe`) covers what actually
happened landing it: real net win (geomean 0.938 at SF-2) but four
regressions (q01, q03, q04, q21) all traced to one root cause —
`project_where`/`project_where_unit`'s split-decode has no cost gate deciding
whether fusing a given filter is actually cheaper than not, so Mechanism 2
routes more filters into a fork that sometimes loses. That gate — weighing
predicate-column decode-avoidance against remaining-column gather-vs-dense
cost — **is now the single highest-priority item**, superseding both the
original Mechanism 4 items below (2) and Mechanism 3's framing (3): it turned
out to be the same underlying gap Mechanism 3 named, just broader (not
specific to deferred-probe scans) and only exposed once Mechanism 2 stopped
blocking filters from reaching it.

1. ~~Mechanism 2 first~~ — DONE. See above.
2. **Mechanism 4's two sub-causes** (q13/q15 `direct_decode_table` task
   imbalance) remain open but are now second priority, not first — the cost
   gate above is more load-bearing. q15's shape (row-group task count
   undersubscribed, `libs/parquet/parquet.hpp:1840`) is a narrowly-scoped fix
   if still needed after the gate lands. q13's shape (a String column's
   whole-file decode is irreducibly one task) needs `fusable_string_conjuncts`
   (`lazy_table.cpp`) to see through `Ascribe` too. Do NOT implement
   `select_bounds` morsel oversubscription — confirmed not on the stall path.
3. **The cost gate** (was: "Mechanism 3's real fix"). Design, don't guess:
   estimate predicate-column decode cost (type-weighted: String/Categorical
   high, numeric low) vs. remaining-column gather-vs-dense delta (scales with
   column count, type, survival rate). Needs real per-query verification
   (q01/q03/q04/q06/q09/q12/q13/q15/q21 all have different shapes on this
   axis) before landing, per `feedback_no_premature_constant_tuning`.
4. **Mechanism 1** (schema-blind lower-time pushdown) — unchanged from before,
   still the most structural of the original four, still needs its own
   scoping pass.
5. **q21/q22** — unchanged; q21 may now partly be explained by the cost-gate
   gap (not yet confirmed, see above) rather than being purely a separate
   filter-through-aggregate pushdown gap. Check before assuming both apply.

## Method note

Findings above that cite `required_columns`/`decode_columns` call counts or
per-source demand were obtained with temporary `getenv("IBEX_DEBUG_DEMAND")`
debug prints in `repl.cpp` and `lazy_table.cpp`, added and reverted within the
same session (never committed). Reproduce by re-adding a print at
`repl.cpp`'s `const auto demand = ir::required_columns(*rewritten);` (inside
`try_execute_whole_script`) and at the top of `LazyTable::decode_columns`, not
by trusting this document's numbers as a standing instrumentation surface.

## Mechanism 5 — the chunked engine treats `Ascribe` as a hard pipeline barrier (found, root-caused, fix attempted and REVERTED — net negative)

Asked (2026-08-20) for a "next round of analysis" given q01/q03/q04 were still
losing badly even after the dictionary-sharding fix. Full-suite SF-2 re-A/B
(dict-fix HEAD vs. pre-Mechanism-2 baseline, same technique as always) showed
q04 still +65%, q01 +22%, q03 +18% — the dictionary fix helped several other
queries but never touched these three, as expected (q04's only remaining
column is Int64, no Categorical involved).

**Root cause, found via `IBEX_PROFILE_OPERATORS` on q04**: an `ascribe`
profile node was costing 36–58ms of self time — bigger than the actual
Parquet decode stages. Traced it: `interpreter.cpp` has zero
`ExecutionProfileScope` references, so any node showing up in the profile
comes from the **chunked/streaming engine's** `build_operator`/
`profile_operator` wrapper (`chunked.cpp:12392`), not the plain interpreter.
`execution_capability()` (`src/runtime/pipeline.cpp`) — which classifies
every node kind for parallel-island/pipeline eligibility — has **no case for
`Ascribe`**, so it falls to `default: Barrier`, same bucket as a genuinely
unsupported node. Since every ascribed reader (the norm since Mechanism 1)
wraps its scan in `Ascribe(Scan(...))`, island formation (which requires a
row-local chain rooted directly at a bare `Scan`) never starts, and the scan
falls all the way through to the generic `interpret_node` fallback
(`chunked.cpp:12385`) — losing the windowed-unit pipelining
(`DeferredScanSourceOperator`/`build_pipelined_scan`) that a bare (unascribed)
scan gets. q04's wall time broke into fully additive, non-overlapping
self-time buckets (ascribe 36.6 + decode_selected 21 + decode_whole 15.5 +
join_semi 14.4 ≈ 88ms = wall time almost exactly) — the signature of a query
that lost all pipelining, not just a slow decode step. Structurally the same
class of bug as Mechanism 2 (a checked `Ascribe` is a proven identity — never
reorders/drops/duplicates rows, exactly as row-local as `Project`/`Rename`),
just in a completely different subsystem.

**Fix attempted**: three call sites needed to agree, not one —
`execution_capability(const ir::Node&)` (gate: `ParallelMap` only when
`checked()`, else `Barrier` — an unchecked ascription still needs a runtime
type validation the chunked engine has no operator for),
`expressions_are_subset_evaluable` (checked Ascribe has no expression to
evaluate, trivially `true`), and `build_row_local_map_operator`
(`chunked.cpp` — a checked Ascribe needs no wrapper operator at all: `return
child;`, the cheapest possible row-local op is not building one). **First
attempt also added `Ascribe` to `is_metadata_only_node`, which was wrong and
self-defeating**: that flag demotes an all-metadata-only island chain back
out of eligibility before it ever reaches the scan-unlock check — for
Project/Rename that demotion is correct (nothing to gain), but for a bare
`[Ascribe]` chain directly over a deferred Scan, the entire point is reaching
that Scan, and the demotion threw the island away first. Removed Ascribe from
that one function specifically; kept it in the other three.

**Verified working for q04**: `IBEX_DEBUG_ISLAND` instrumentation confirmed
`island.eligible()` now fires with `input_kind=Scan` for the ascribed
scan. Correctness: 22/22 `check_answers.py`, 1,639/1,639 tests, both
reconfirmed. **q04 dropped from 90ms to 58ms** — landing almost exactly on
the pre-Mechanism-2 baseline (54.91ms), i.e. the regression is functionally
gone.

**But the full-suite re-measurement was net NEGATIVE: geomean 1.13 (vs. 0.97
with the dictionary fix alone)** — q05 (+134%), q08 (+150%), q09 (+73%),
q17 (+159%), q18 (+35%), q03 (+35%) all got *worse*, several far more than
q04 improved. Profiled q05: a `join inner keys=1` node's **build** phase
alone cost 75ms (previously near-zero). Root cause of the new regression:
`is_streamable_inner_join`'s non-deferred-probe path
(`chunked.cpp:12106-12114`) calls `build_operator` on the join's right side
and then **immediately, fully materializes it** via `materialize_operator`
before the join even starts. Mechanism 3's `match_probe_chain` still can't
see through `Ascribe` (unfixed, unrelated to this session's work), so an
ascribed right side can never take the deferred-probe streaming path that
would actually consume a pipelined scan's output incrementally — it always
lands on the eager-materialize path. With this fix, that right side is now
ELIGIBLE for the windowed/pipelined scan machinery — and pays its overhead
(ring buffers, per-morsel dispatch, sequencing) for **zero benefit**, since
the very next thing that happens is pulling every chunk into one Table
anyway. Pipelining is a win exactly when its output overlaps with something
else (q04's semi join reads it incrementally); it is pure loss when the
consumer immediately blocks on full materialization regardless (q05, q08,
q09, q17, q18's inner joins) — and this fix, as implemented, cannot tell
those two situations apart.

**Reverted in full** (`git checkout src/runtime/pipeline.cpp
src/runtime/chunked.cpp`) — confirmed clean (no diff), 22/22
`check_answers.py`, 1,639/1,639 tests reconfirmed at the reverted state.

**What a real fix needs, not attempted this session**: the eligibility
decision has to be consumer-aware, not just producer-aware. Candidates,
roughest to most surgical:
1. **Fix Mechanism 3 first** (`match_probe_chain` sees through `Ascribe`,
   mirroring Mechanism 2's fix) — if ascribed scans could take the
   deferred-probe streaming path instead of the eager-materialize one, the
   consumer side of q05/q08/q09/q17/q18's joins would actually benefit from
   pipelining rather than paying for it uselessly, potentially making this
   session's Mechanism 5 fix safe *as a side effect* rather than needing its
   own gate. Untested — the two fixes were never tried together.
2. **Gate on the immediate consumer**, not just the producer chain: don't
   route a scan through the pipelined path when `build_operator_impl` already
   knows (as it does at `chunked.cpp:12070`/`12111`) that the very next step
   is `materialize_operator`. This is more surgical than (1) but adds a
   second, consumer-side condition to an already-subtle eligibility check —
   higher risk of a fourth mechanism hiding in the interaction.
3. **Do nothing until (1) is tried** — cheapest next experiment, and it may
   make this fix net-positive without further changes to pipeline.cpp's
   classification logic at all.

**Where this leaves the suite**: only the dictionary-sharding fix (Mechanism
"Categorical") is landed. q01/q03/q04/q21 remain regressed relative to the
pre-Mechanism-2 baseline — q04 has a diagnosed, demonstrated-working fix that
is unsafe to land standalone; q01/q03 were never individually re-profiled
this round (assumed same family as q04, i.e. also blocked on Mechanism 5,
not confirmed). Next session should start from suggestion (1) above rather
than re-deriving Mechanism 5 from scratch.

### Mechanism 5, retry after Mechanism 3's real fix (2026-08-20) — falsified the hypothesis, found a benchmark artifact, found a HANG. Reverted again.

Suggestion (1) above was tried directly: Mechanism 3 was fixed first (see
above), then the *exact same* Mechanism 5 fix (same 3-part change to
`pipeline.cpp`/`chunked.cpp`, `is_metadata_only_node` bug avoided from the
start this time) was re-applied on top. Built clean, 22/22
`check_answers.py`, 1,641/1,641 tests passed.

**The hypothesis was falsified**: full-suite geomean came back at **1.1511 —
worse than the first attempt's 1.13**, not better. q09 specifically got *more*
regressed (1.775x vs. the first attempt's 1.726x), the opposite of what
"ascribed scans can now take the deferred-probe streaming path" predicted.
Mechanism 3's fix did not change which path q05/q08/q09/q17/q18's joins take
under Mechanism 5 — those joins' build sides don't satisfy
`contains_row_reducing_node` in the shapes that matter here, so they were
never rerouted at all; the two fixes are independent, not synergistic, on
this suite.

**Digging into q05 specifically surfaced a confound in the measurement
itself, not the fix**: `bench_ibex.py --warmup 1 --iters 4` (the setting used
for the regression numbers above and in the first attempt) wasn't enough
warmup for a REPL whole-script run under this change. Re-run with
`--warmup 4 --iters 6`:

| query | `--warmup 1` avg | `--warmup 4` avg |
|---|---|---|
| q05 | 124.48ms | 65.59ms |
| q08 | 166.93ms | 79.36ms |
| q09 | 287.84ms | 133.13ms |
| q18 | 241.37ms | 123.24ms |

q09 and q18 actually came out *better* than the pre-Mechanism-5 baseline once
properly warmed; q05/q08 landed much closer to parity (not fully closed,
q17 stayed elevated around 1.42x). So a real fraction of the "net negative"
finding from both attempts was measurement noise from under-warming a
pipelining-heavy code path, not a genuine regression — the true picture is
closer to neutral-to-mixed than "1.15x worse across the board".

**That better picture doesn't matter, because a full 22-query sweep at
`--warmup 4 --iters 6` (launched to get the fair final numbers) hung on
q11**: `bench_ibex.py` raised `subprocess.TimeoutExpired` after 300 seconds
with q11 producing no output at all. Reproduced directly and immediately:
`timeout 30 ... :run q11.ibex` — killed by the timeout, zero output, on the
very first invocation. Not slow-but-completing; a genuine hang.

q11 (`benchmarking/tpch/queries/q11.ibex`) has a distinguishing shape none of
the other 21 queries share: it binds `q1` (a 3-way join result) once, then
consumes it **twice** — once directly into a `by`-grouped aggregate, and
again through an uncorrelated scalar subquery (`q2 = q1[select {tmp =
sum(...)}]`) joined back in via `cross join`. That's two independent
consumers pulling from the same upstream node. `chunked.cpp`'s own comments
around `build_parallel_island`/pipelined-scan state already flag "concurrent
cache_ writes and concurrent decode_ calls" as a hazard class; two consumers
of one now-pipelined Ascribe→Scan chain matches that shape closely. This is
a **hypothesis, not a diagnosis** — no instrumentation was added before
reverting, given the severity (a liveness bug, not a perf regression) made
further live experimentation on the hung path a bad trade.

**Reverted in full again** (`git checkout src/runtime/pipeline.cpp
src/runtime/chunked.cpp`), confirmed clean via `git diff --stat` (both files
absent from the changed list). Rebuilt; q11 confirmed back to ~14ms
(not hanging). Re-verified 22/22 `check_answers.py` and the full ctest suite
(1,641/1,641) at this final state: Mechanism 2 + Mechanism 3 (real fix) +
dictionary sharding + Categorical split all kept; Mechanism 5 fully reverted,
two-for-two failed attempts.

**Conclusion for Mechanism 5, as originally written here**: closed for now,
not because the perf case is dead (the warm-up-corrected numbers suggest it
may be closer to viable than either attempt showed) but because it has now
twice produced a consumer/producer interaction problem in `chunked.cpp` —
first a materialize-after-pipeline perf trap, second a suspected
concurrent-access hang — and both times the actual defect turned out to live
in shared-binding/multi-consumer interaction with the pipelined-scan
machinery, not in the `execution_capability`/`is_metadata_only_node`
classification itself.

**That conclusion was wrong on both counts — corrected same-session, 3rd
attempt (2026-08-20).**

The "warm-up-corrected numbers" claim above compared Mechanism-5-at-warmup-1
against Mechanism-5-at-warmup-4 — never against a real no-Mechanism-5
baseline at the same warmup. That's apples-to-oranges: it shows the fix's
own numbers stabilizing with more warmup, not that the regression shrinks
relative to baseline. Re-running a proper same-session A/B (`git show
322fb14:...` swap technique, `--warmup 4 --iters 6` both sides) gives the
real picture:

| query | baseline | with Mechanism 5 | ratio |
|---|---|---|---|
| q05 | 27.7ms | 64.6ms | 2.33x |
| q08 | 36.8ms | 83.5ms | 2.27x |
| q09 | 63.3ms | 127.7ms | 2.02x |
| q11 | 12.0ms | 25.6ms | 2.14x |
| q17 | 18.0ms | 48.4ms | 2.69x |
| q18 | 81.8ms | 125.1ms | 1.53x |

**geomean 1.296 — a stable 30% suite-wide regression**, reproduced twice
back to back (q11: 25.6ms then 24.5ms; q09: 127.7ms then 144.2ms — noisy in
absolute terms but consistently ~2x baseline, not converging toward parity).
q04 is still the one clear win (0.83x). q11 itself — the query suspected of
hanging — turns out to be a 2.1x regression in its own right once actually
measured against baseline, not merely "the query that used to hang."

The multi-consumer hang investigation (below) is retracted as a
*conclusion*, not as *work done* — the reproduction attempts and the
shared-binding code finding stand (they're accurate), but the punchline
("closed because of a consumer/producer interaction, perf case still open")
was reached without ever running the real baseline comparison. The actual
reason to keep this closed is much simpler: it's a stable ~30% regression on
top of whatever the hang risk is, independent of it.

**Corrected conclusion**: Mechanism 5 is closed. The diagnosis (no
`execution_capability` case for Ascribe) is still correct, and it still
wins q04 specifically, but every attempt at the classification-gate fix
(three now) has cost far more elsewhere than it wins — first net +13%,
second (warmup-confused) reported as improving, third re-measured properly
at +30%. This is not a case of "almost there, needs one more tweak" — the
`materialize_operator`-after-pipelining interaction (the root cause
identified after the first attempt, still the best-supported explanation)
needs the consumer side of the fix, not another pass over the producer-side
classification switch. q04/q01/q03/q21 remain regressed relative to the
pre-Mechanism-2 baseline with no safe fix landed this session.

### 4th attempt — the consumer-aware fix, targeted at exactly the two join call sites (2026-08-20)

The producer-side classification change (`pipeline.cpp`'s `execution_capability`)
was re-applied unchanged, plus a new, targeted consumer-side change in
`chunked.cpp`: at the two call sites that build a join's probe/right side and
immediately, synchronously drain it —

```cpp
auto right_op = build_operator(*join.children()[1], ...);
auto right = materialize_operator(std::move(right_op.value()));
```

(`is_streamable_inner_join`'s non-deferred-probe branch, and the streamable
semi/anti branch) — a new helper, `materialize_join_side`, checks whether the
node is a (possibly `Ascribe`/`Project`/`Rename`-wrapped) chain rooted at a
deferred/lazy scan via `is_deferred_scan_rooted` (built on the same
`analyze_parallel_island` the classification switch feeds). When it is, the
helper calls `interpret_node` directly — the same one-shot
`direct_decode_table` decode this shape got before Ascribe became
`ParallelMap`-eligible — instead of routing through `build_operator`'s
island/pipelined-scan machinery only to drain it immediately with nothing
downstream to overlap with. Everything else (a non-scan-rooted right side,
or one feeding a table already in the registry) is untouched — same
`build_operator`+`materialize_operator` as before.

**Result: a real, confirmed partial fix — q05 (2.33x → 0.99x), q08 (2.27x →
0.98x), q18 (1.53x → 1.08x) all landed at or near parity.** This validates
the diagnosis directly: the tax is real, is exactly the shape described, and
disappears when skipped.

**But the suite is still net regressed: geomean 1.239** (down from 1.296 —
a ~6 point improvement, not a fix), and **q09 (1.93x), q11 (2.05x), q17
(2.79x) — the three queries this session's analysis specifically named — are
barely moved or worse.** Several queries that were near-parity in the 3rd
attempt now show new 1.1-1.5x regressions they didn't have before (q02, q06,
q13, q16, q20).

**Why the fix doesn't reach q09/q11/q17**: those three share a shape the
first three (q05/q08/q18) don't — their ascribed, newly-`ParallelMap`-eligible
scan chain feeds directly into a consumer that *also* drains synchronously
with nothing to overlap, just one level further out than the two call sites
targeted here. q17's `relevant_lineitems` (a join whose right side IS the
exact shape this fix targets — `lineitem_raw[select{...}]`, 6 row groups) is
itself a **shared binding**, referenced twice (`quantity_limits`'s aggregate,
and the final join) — per `repl.cpp`'s shared-binding handling, it is
`evaluate`d exactly once at the top level, and that whole-plan evaluation
materializes the join's *output* synchronously regardless of what happens to
its right side internally. q11 is the same shared-binding shape found in the
hang investigation. q09/q13/q16/q20 plausibly share a variant: an `Aggregate`
node (`ParallelBarrier` in `execution_capability`, but still a `child_->next()`
drain loop with nothing else running concurrently at that point in the plan)
sitting directly above a newly-eligible ascribed chain. **The two call sites
fixed here are one instance of a broader pattern — "parallel/pipelined
machinery built and then immediately, synchronously drained with no
concurrent consumer" — not the only instance.** A full fix needs the same
treatment at every terminal synchronous-drain point (shared-binding
`evaluate`, `Aggregate`'s build path, possibly others not yet enumerated),
not just the two join call sites, and each one adds surface area for a
5th/6th interacting mechanism.

**Reverted** (`git checkout src/runtime/pipeline.cpp src/runtime/chunked.cpp`),
confirmed clean via `git diff --stat`. 22/22 `check_answers.py` and full
ctest (1,641/1,641) both green at every stage of this attempt (with the fix,
and after reverting). q11 confirmed not hanging at any point during this
attempt (27ms, consistent with earlier soak findings — the hang still has no
confirmed reproduction this session).

**Status**: four attempts now, all reverted. The diagnosis has gotten
progressively more precise each time (classification gate → materialize-
after-pipeline → the two join call sites → the broader synchronous-drain
pattern), but landing it requires finding and fixing *every* terminal drain
point that can receive a newly-`ParallelMap`-eligible Ascribe chain, not
just the two identified so far. That is real, scoped, follow-up work for a
future session — the next concrete step is enumerating those drain points
(starting with the shared-binding `evaluate` in `repl.cpp` and `Aggregate`'s
build path in `chunked.cpp`) rather than another blind full-suite A/B.
q04/q01/q03/q21 remain regressed relative to the pre-Mechanism-2 baseline
with no safe fix landed this session.

## Final full-suite re-measurement, 2026-08-20 — the gap has almost entirely closed, and it's now one query

Everything above this section was written incrementally, chasing one
mechanism at a time; two of the "still needs a fix" claims made along the way
(Mechanism 1 for q12, Mechanism 4's `fusable_string_conjuncts` item for q13)
turned out to be stale the moment they were actually checked against the
current tree — see the corrections inline above. That pattern (reasoning
from old diagnoses instead of re-measuring) was worth breaking out of
entirely: this section is one clean, full 22-query SF-2/8-core run against
the tree as it stands at `38b8c4d` (Mechanism 2 + Ascribe-as-Scan-metadata +
dictionary sharding landed; Mechanism 3's gate reverted), `warmup 2 / iters
6`, box idle (`ps --sort=-pcpu` checked first), compared line-by-line against
this plan's very first baseline table.

| query | old (hand-fused) | current (min ms) | ratio |
|---|---:|---:|---:|
| q21 | 233 | 542.0 | **2.33x — still the whole story** |
| q22 | 49 | 64.5 | **1.32x — real, unexplained, unchanged** |
| q14 | 32 | 36.3 | 1.14x |
| q06 | 23 | 20.7 | 0.90x — resolved |
| q15 | 42 | 39.3 | 0.94x — resolved |
| q13 | 132 | 107.4 | 0.81x — resolved |
| **total (22 queries)** | **1898** | **2199.4** | **1.159** |
| **total excluding q21** | **1665** | **1657.4** | **0.995 — parity** |

**Every query the original four mechanisms named (q06, q13, q15, q14) is at
or better than the old hand-fused baseline.** Pulling q21 out of the total
shows the other 21 queries are, in aggregate, at parity with the pre-`eb5231c`
hand-fused baseline (0.995x) — the +13-16% suite-wide regression this whole
plan was opened to chase is now **entirely concentrated in one query**, not
spread across the suite the way the original baseline table suggested.

**q21 is not a new finding** — it was already flagged, at the very top of
this plan's "What's NOT explained by any of the above" section, as a genuine
planner gap unrelated to Ascribe: the realigned query aggregates the entire
unfiltered ~12M-row lineitem table twice before joining down to
`o_orderstatus == "F"`, where the hand-written original filtered `orders` to
`"F"` and semi-joined it in *before* aggregating. That diagnosis stands and
is now the ONLY substantial remaining item — not one of four, one of one.
**q22** (1.32x) is the other item this plan's earlier sections already
flagged as a separate, unstarted rewrite-shape investigation (`anti join` →
`left join`+`is_null`, `scalar()` → `cross join`) — small next to q21 but
real and unmoved by anything landed this session.

**q09 measured 238-270ms here**, not obviously tied to any named mechanism
and not measured cleanly enough this session to trust as a finding — it swung
between ~125ms and ~300ms across this session's various runs depending on
box contention, wider than any other query's variance. Before treating it as
a lead, re-measure with the interleaved-adjacent protocol
(`feedback_bench_interleaved_methodology`), not a single run.

**Re-run at `warmup 5 / iters 10` (2x the reps), twice back to back, on
request (the single `warmup 2 / iters 6` run above was reasonably
questioned)**: totals 2244.8ms and 2300.4ms (round-to-round delta 2.5%,
individual queries mostly within ±10% — consistent with this box's known
noise floor, not a methodology gap). q21 specifically was the most stable
number in the whole suite (559.9 vs 567.7ms, 1.4% delta) — its 2.33-2.4x
regression is a real, robust effect, not noise. The excl-q21 parity finding
holds at the heavier rep count too (1684.9/1665 = 1.012, round 1; consistent
direction round 2). q09 stayed in the same 259-280ms band both heavy-rep
rounds — tighter than the single-run number suggested, though still not
checked with a true interleaved A/B against an old-baseline binary, so its
status as "not tied to a named mechanism" stands but "noisy" should be
downgraded to "just not yet compared against a baseline."

**Where this leaves the plan**: Mechanisms 1, 2, 4, and 5 are done —
landed, moot, or (Mechanism 5) closed with the architectural fix
superseding the leaf-patch attempts. Mechanism 3 is reverted and open but
demonstrably low-priority now (its target queries, q09/q12, aren't visibly
regressed in this measurement — q12 is fully resolved at 83ms against no
old-baseline number to compare, but nowhere near the multi-hundred-ms
regression the original Mechanism 3 diagnosis described). **The suggested
order of attack above is superseded**: there is no longer a ranked list of
four mechanisms to work through. There is q21 (see below — real, scoped, but
NOT the filter-pushdown gap this plan originally guessed), q22 (smaller,
unstarted), and q09 (unconfirmed, needs a clean re-measurement before it's
even a lead).

## q21, investigated 2026-08-20 — the original diagnosis was wrong about the mechanism, right about the query

This plan's very first draft guessed q21's gap was "needs a filter pushed
back through a self-join and two aggregates" (the `o_orderstatus == "F"`
filter, applied late in the realigned query vs. early in the old hand-fused
one). **Tested directly and that specific hypothesis is false.**

**Confirmed the gap is real and large first**: the literal pre-`eb5231c`
hand-fused query text (`git show eb5231c~1:...q21.ibex`), run against
*today's* engine, is ~212-220ms warm at SF-2/8c vs. the current query's
~527-552ms — a robust 2.4-2.5x, reproduced 3 rounds interleaved, still
correct (0/100 row mismatches against the official SF-1 answer file).

**Tested the guessed fix — filter `orders` to `"F"` and semi-join it into
`lineitem` before the aggregates, nothing else changed**: no win. Slightly
*slower* than the current query (verified correct, 0/100 mismatches). The
early semi-join pass costs about what the smaller aggregate saves — `"F"`
status is ~50% of orders, not a small fraction, so restricting to it doesn't
shrink the dominant cost much.

**Tested a deeper rewrite mirroring the old query's full candidate-narrowing
chain** (restrict further to orders with at least one late Saudi-supplier
line, via a `distinct`+semi-join chain, *before* the aggregates) **on top of
the F-status restriction, keeping the current query's `count()`-based
aggregate shape**: also verified correct (0/100 mismatches), but markedly
*worse* — 684-967ms warm, worse than both the current query and the F-only
attempt. Confirmed with a serial (`IBEX_PARALLEL=0`) run too, ruling out a
scheduling artifact: old-hand ~460-556ms serial vs. this rewrite ~1.3-1.8s
serial, same 2.5-3x gap persists without any parallelism in play at all.

**The actual lever, found by profiling both queries' operators
(`IBEX_PROFILE_OPERATORS=1`) and comparing shapes, not by more guessing**:
the old hand-fused query computes `n_sup`/`n_late` (the two distinct-supplier
counts q21's `exists`/`not exists` need) as **per-order scalar aggregates** —
`candidate_li[distinct { l_orderkey, l_suppkey }][select { n = count() }, by
{ l_orderkey }]` — then joins those two small, already-aggregated,
one-row-per-order tables together. The current query (and every rewrite
tried here that kept its shape) instead **self-joins at line granularity
first, aggregates after**: `candidate_li[grouped, filter >1] JOIN
candidate_li[filter late] on { l_orderkey }`. That join's output size is the
*product* of each side's per-order line counts, summed over orders — an
order with 5 lines where 2 are late contributes up to 5×2=10 join rows
before any reduction happens. Confirmed directly: even after adding the deep
candidate-narrowing on top, this rewrite's join still processed ~3.66M rows —
the same order of magnitude as the *unnarrowed* current query's join,
because narrowing which orders qualify does nothing to shrink the
per-order multiplicative blowup once an order does qualify. The old query
never pays this cost at all: `distinct`+`count()` per order is O(lines),
never O(lines²)-shaped, regardless of how many suppliers or how much lateness
an order has.

**This means the plan's original framing was wrong about the mechanism.**
It isn't a missing filter-pushdown rule — filter pushdown was tested directly
and doesn't move the number. It's that the realigned query's algorithm has a
genuine join-shaped cost the old query's `distinct`-based algorithm never
incurs, and no filter-pushdown-style rewrite closes that gap; only
restructuring the *aggregation strategy itself* (self-join-then-count →
distinct-then-small-join) does, and that specific restructuring is exactly
the classic TPC-H q21 "collapse an `exists`/`not exists` pair into two
distinct-supplier counts" trick a human author applies by hand — not a
generic optimizer rule like predicate pushdown. Automatically discovering
"replace this self-join-and-aggregate subplan with an equivalent
distinct-and-count subplan, because the join's only consumer is a `== 1`/`>
1` scalar test" is a real query-rewrite capability (recognizing when a join
computed only to feed a cardinality comparison can be replaced by a cheaper
aggregate that computes the same cardinality directly) — bigger and more
speculative than anything else in this plan, closer to a research question
than a scoped fix.

**What was NOT done**: writing that general rewrite rule. It's out of scope
for the confidence this session has in it — one query's evidence isn't
enough to design a rule safely (per this plan's own repeated lesson: a
heuristic that fixes what it was checked against and regresses everything
else nearby, see Mechanism 3 and Mechanism 5's five combined attempts). If
this is worth pursuing, the next step is finding 2-3 more queries (in this
suite or PDS-H generally) with the same "self-join whose sole purpose is
feeding a cardinality comparison" shape, to have more than one data point
before designing a rewrite rule.

**RULED OUT 2026-08-20**: hand-swapping the old query text back in, even
though it's verified correct and faster. Standing project rule, restated by
the user when this was raised: **join order and rewrite shape are the
engine's responsibility, not the query writer's** — the engine is free to
reorder/rewrite; the author of a `.ibex` query is not expected to hand-encode
a specific join structure to get good performance, which is exactly what the
old hand-fused text does (and exactly what `eb5231c` moved the suite away
from testing). So the only acceptable fix here is a real planner capability,
not a text substitution — this isn't a "which do you prefer" tradeoff, it's
already decided.

**Where this leaves q21**: real, well-understood now (not just diagnosed),
and genuinely NOT a quick fix — it needs "recognize distinct-count-replaceable
self-joins" as its own scoped planner project, informed by more than one
example query first (see above: one query's evidence isn't enough to design
a rewrite rule safely, per this plan's own repeated Mechanism-3/5 lesson).
Next concrete step, not yet done: survey PDS-H (this suite and/or the wider
benchmark) for other queries with the same shape — a self-join whose only
consumer is a cardinality/existence comparison (`== 1`, `> 1`, `exists`) —
before scoping the rule itself. Until that survey exists, this stays a named,
understood gap rather than an active work item.
