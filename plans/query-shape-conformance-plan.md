# PDS-H query-shape conformance regression

**Status 2026-08-27: substantially resolved.** Compacted from a ~1750-line
investigation diary; the blow-by-blow (five Mechanism-5 attempts, the fusion
calibration harness runs, the q21 rewrite experiments) is in git history at the
pre-compaction commit's parent if a number needs chasing.

## What this was

`482eb583` ("Match query shapes"; recorded here as `eb5231c` before the
post-commit hook re-committed it) rewrote `benchmarking/tpch/queries/*.ibex` to
the upstream `polars-benchmark` shapes — no hand-written predicate/projection
pushdown or join-key pre-renaming. Right for the suite to measure (it now shows
what the *planner* does), but it cost **+13-16% at SF-2/8c** because the planner
did not yet recover what the hand-fusion did for free. `322fb14` then added
`as DataFrame<{...}>` ascriptions, which moved numbers again in both directions.

## Where it landed (final full-suite SF-2/8c, `38b8c4d`)

| | old (hand-fused) | current | ratio |
|---|---:|---:|---:|
| total, 22 queries | 1898 | 2199 | 1.159 |
| **total excluding q21** | 1665 | 1657 | **0.995 — parity** |
| q21 | 233 | 542 | **2.33× — the whole remaining gap** |
| q06 / q13 / q15 / q14 | — | — | 0.90 / 0.81 / 0.94 / 1.14 — resolved |

q22 (was 1.32×) **resolved 2026-08-27** — see bottom.

**The +13-16% suite regression is gone. What remains is q21 alone**, and q21 is
a known single-query planner gap, not a conformance issue — see below and
`project_q21_is_occupancy_bound` in memory.

## Four mechanisms found (not one shared fix — fixing one can worsen another)

### Mechanism 1 — lower-time filter-into-join pushdown is schema-blind
`push_filters_into_joins` runs from `lower.cpp` before any Parquet footer is
read. A join side's schema is Known only if it's a `Project` or a checked
`Ascribe` (and only from the *second* call site — the first, `lower.cpp:5186`,
uses extern **declarations** only). A bare `read_parquet(...)` join side is
Unknown and the pass declines to push past it. This is what the hand-fused
queries bought by writing `select {...}` after every scan. **Fix (unbuilt,
structural):** have the driver populate real footer-derived schemas *before*
lowering, so every join side has a Known schema regardless of authorial style.
Touches the lower/driver boundary — scope separately.

### Mechanism 2 — `projected_scan()` didn't see through `Ascribe` — LANDED `120a567`
`scan_predicates.cpp`'s `projected_scan` walked `Filter`→`Project`→`Scan` but
had no `Ascribe` case, so `Filter(Ascribe(Scan))` never fused into the Parquet
decode. Now walks through a `checked()` `Ascribe` like `Project`. Full-suite
SF-2 A/B: **geomean 0.938, a real 6.2% net win** — but not uniform: q13 0.61,
q06 0.58, q09 0.79 win; **q04 1.76, q01 1.24, q03 1.17 regress**. All four
regressions trace to one root cause → the cost gate below. The code change is
stand-alone correct and committed; the gate is the follow-up.

### Mechanism 3 — `match_probe_chain()` doesn't see through `Ascribe` either
Same blindness in `collect_deferrable` (deferred-probe eligibility). Currently
an **accidental win**: it keeps q09/q12 from taking deferred-probe scanning,
which for those queries is a net loss (unfiltered build side → many probe chunks
each pay an uncached per-chunk decode; q12 lineitem 223ms ascribed vs 613ms
unascribed). **A gate was built (`contains_row_reducing_node`) and REVERTED**
(`38b8c4d`) — net −10.3% full-suite: a structural "was this side filtered" proxy
can't tell q08's `part` (small *by construction*, no filter) from q12's `orders`
(large, unfiltered) and wrongly blocked q08 → ~2× regression. A **row-count-ratio**
version (footer `rows()` are already available at both driver call sites) is the
candidate — but calibrate against more than the 2 data points (q08 ~3.3%,
q12 ~25% at SF-2). Do NOT re-attempt the pure structural proxy. Low priority —
Mechanism 1+2 jointly route most deferred-probe-eligible-looking joins away from
`collect_deferrable` before it runs; re-survey what still reaches it first.

### Mechanism 4 — a predicate column's whole-file decode stalls on task imbalance
Not `project_where`, not `select_bounds` — it's `direct_decode_table`
(`libs/parquet/parquet.hpp:~1767`) itself. q13: `o_comment` (String) is one
indivisible whole-column task alongside sharded Int64s → 7 tasks, 8 workers, the
string task sets the floor, `barrier_wait_ms ≈ 100` of 237ms. q15: `l_shipdate`
is shardable but lineitem's SF-2 row groups are too few for 8 workers. **Two
scoped fixes:** (a) split a row-group task further when `tasks.size() < workers`
for a single-column decode (q15); (b) make `fusable_string_conjuncts` see
through `Ascribe` so ascribed LIKE-filtered scans take the fused
string-in-decoder route that already avoids the dense decode (q13). Second
priority behind the cost gate.

### Mechanism 5 — chunked engine treats `Ascribe` as a hard pipeline barrier
`execution_capability()` (`src/runtime/pipeline.cpp`) has no `Ascribe` case →
`default: Barrier`, so every ascribed reader (the norm) blocks island formation
and loses windowed-unit pipelining. **Four fix attempts, all reverted** (net
+13% → +30% → +24% suite; one suspected hang on q11's shared-binding shape). The
producer-side classification is correct in isolation (q04 −8%) but every scan it
newly makes `ParallelMap`-eligible then pays pipelining overhead for zero
benefit when its consumer immediately, synchronously drains it
(`materialize_operator` after `build_operator` at the join call sites, plus
shared-binding `evaluate`, plus `Aggregate`'s build path). **Root cause is
architectural** and was addressed the other way — `fuse_checked_ascriptions`
folds `Ascribe(Scan)` into one `Scan` node (`2d4ac98`,
`ascribe-as-scan-metadata-plan.md`), so `Ascribe` stops being a peer node every
pass special-cases. Do NOT retry the leaf-level classification patch —
see `project_ascribe_pipeline_barrier` in memory.

## The scan-fusion cost gate — RE-MEASURED 2026-09-02, NO LONGER THE PRIORITY

**The regressions this gate was designed to recover are gone.** Measured on the
current tree by building a "never fuse" variant (`absorb_lazy_scan_filters`
marking nothing applied) and A/B-ing all 22 at SF-8, 8 physical cores, min-of-5
twice per side, answers byte-identical on every query:

| | ratio (fused / unfused) | |
|---|---|---|
| **fusing wins big** | q03 0.39, q07 0.55, q15 0.68, q14 0.69, q06 0.73, q19 0.75, **q04 0.77**, q12 0.80, q08 0.81, q09 0.83, q02 0.84 | |
| **fusing wins** | q10 0.90, q20 0.91 | |
| **neutral (±3%)** | q01 0.98, q18 0.98, q16 0.99, q17 0.99, q22 0.99, q21 1.01, q11 1.02, q05 1.03 | |
| **fusing loses** | **q13 1.10** | the only one |

Against the three regressions that motivated the gate at `120a567`:

| | then | now |
|---|---|---|
| q04 | **1.76** | **0.77** — a 23% win |
| q01 | 1.24 | 0.98 — neutral |
| q03 | 1.17 | **0.39** — a 2.6× win |

Something between `120a567` and here absorbed them — `2ffbd59c`'s
dictionary-coded predicate-only DATE32 scan is the obvious candidate, since
q03/q04/q12/q14/q15 all filter on dates and all now fuse profitably — but the
attribution was not chased, because the conclusion does not depend on it.

**The MVP model below is also stale in sign, not just in magnitude.** Its worked
example says q13 *fuses* (predicate column `o_comment` is String and expensive
enough to swamp the Int64 remaining set). q13 is now the single query that
loses. So a two-bucket model calibrated on the old table would get the one
remaining case wrong, and rebuilding it needs a fresh calibration anyway.

**Recommendation: close the cost gate as a work item.** Fusing is the right
default on 21 of 22 queries; a gate can win at most q13's 10% on one query, at
the cost of a model, a calibration table, and a decision every scan pays.

### What replaces it: q13's predicate side, not its remaining side

q13's loss is not the gather-vs-dense trade this section modelled. Its fused
route runs `string_filter_scan` over all of `o_comment` and *that* is the cost
(SF-8, 8 cores, operator profile):

```
fused    wall 398ms  pool_work 2614ms   string filter scan: pool_src 1136ms, pool_work 917ms
                                        + selected decode:  pool_src  145ms
unfused  wall 294ms  pool_work 1577ms   decode whole:       pool_src  706ms
```

Fusing q13 does **66% more CPU work**, partly hidden by better occupancy (0.82
vs 0.67). The model's premise — that fusing *saves* predicate-column work — is
what fails here: a non-anchored `not like '%special%requests%'` over 12M strings
costs more in the decoder than a dense decode plus an in-memory filter. Mechanism
4(b) has landed (q13 does take the fused string route), so the open question is
narrower and different: **make the fused string scan competitive with dense
decode + filter, or decline fusion for non-anchored LIKE over a large String
column.** One query, one mechanism, no cost model.

## Appendix — the original design (kept for the calibration method)

*Superseded by the re-measurement above: the ratios, the two-bucket model, the
named call sites and the validation set below all date from `120a567` and no
longer describe the tree. Kept because the method — build a never-fuse variant,
A/B the suite, read the ratio per query — is what produced the table above and
is worth repeating whenever a decode change lands. Do not implement from it.*

> **This gate now has a sibling with a measured price.** 2026-09-02 built
> `push_computed_columns_into_joins` — hoisting an `Update`'s single-side
> sub-expressions below the join and dropping the columns
> `join_output_demand` shows nothing above still reads. Byte-identical on all
> 22, full suite green, worth **−9 to −15%** where post-join rows × payload
> width is large, and **+40% on q12** where the same push evaluates a predicate
> over 12M `orders` rows to save gathering 1.3M. It was reverted for want of a
> cost model — but **not the same one**. Correcting a claim first made here on
> 2026-09-02: these two gates want *different* inputs. This section's own
> calibration is explicit that the scan-fusion signal is the remaining column's
> **type**, not selectivity, so its MVP needs no cardinality at all; the join
> pushdown needs exactly what this gate does without — join-output rows against
> each side's post-filter rows. Siblings in spirit, not a shared prerequisite.
> Patch and full narrative: `beat-polars-plan.md` §6, and
> `project_q14_bandwidth_and_selected_gather` in session memory.

Mechanism 2 routes more filters into `project_where`/`project_where_unit`'s
split-decode, which has **no cost gate**. Fusing a filter always: decodes the
predicate columns densely+whole, and decodes the *remaining* demanded columns
via a Selection-gathered decode instead of dense. It saves only the *gather* of
the predicate columns into output.

**Calibrated fusion ratios (STALE — see the re-measurement above)** (`fused_ms / unfused_ms`, synthetic 8M-row
8-row-group **uncompressed** file — real TPC-H files are `UNCOMPRESSED`, the
first calibration used Snappy and was wrong; single remaining column):

| remaining type | 10% | 30% | 50% | 70% | 90% |
|---|---:|---:|---:|---:|---:|
| Int64 | 0.97 | 0.99 | 1.32 | 1.36 | 1.58 |
| Float64 | 0.93 | 1.05 | 1.22 | 1.31 | 1.46 |
| String | **0.35** | **0.44** | **0.49** | **0.58** | **0.63** |
| Categorical | 0.76 | 1.12 | 1.53 | 1.35 | 1.60 |

**The signal is the remaining column's *type*, not selectivity.** String
remaining columns are cheaper fused everywhere (fixed-width allocates per value;
gathering survivors avoids wasted allocation). Fixed-width numeric is cheaper
*unfused* almost everywhere (dense decode + cache-friendly compaction beats a
scattered `Selection` decode). Categorical **now** lands in the numeric bucket —
it needed `plan_sharded_column` to gain a `DICTIONARY` case (LANDED: per-shard
local dictionaries + a `finalize` remap; dict column 2.3-3.4× → ~1.1-1.6× fused).

- **q13 wins** despite Int64 remaining columns because its predicate column
  `o_comment` (String) is expensive enough to skip-gather that it swamps the
  ~40% loss on two small Int64s.
- **q04 / q01 lose** — remaining set is numeric (q04: one Int64; q01: four
  Float64 + tiny Categoricals) with cheap Date predicates: nothing saved, real
  cost paid.

**Float64/Int64's own gather-vs-dense loss** was attempted twice
(block-dense-then-gather; scratch-then-tight-loop) — both reverted, no win.
`perf` (WSL2 software sampling) showed the cost is inside Arrow's
`PlainDecoder::Decode` page-to-buffer `memmove`, identical for dense and
selective. The only real lever is selection-awareness *below* Arrow's
`TypedColumnReader` API (Arrow fork, ~1.15× ceiling, string regression) — not
recommended. Don't re-attempt above-API fixes without hardware counters.

**MVP model:** two-bucket (cheap = fixed-width numeric + Date + Categorical;
expensive = String). Fuse when predicate-side has expensive-bucket columns and
that saving isn't swamped by remaining-side cheap-bucket columns (more of them →
argues harder against fusing); lean toward fusing when remaining is
expensive-bucket-dominated. The cheap bucket still has a real ~0.9-1.6× cost the
margin must account for.

**Inputs available, no new plumbing:** `make_relation_sampler` (`repl.cpp:1569`,
already wired for join-order costing) gives real per-scan selectivity;
`LazyTable::schema()` / `column_stats()` give types and a footer fallback; the
remaining-column set is one extra `required_columns` call.

**Where the code goes:** new `src/ir/scan_fusion_cost.{hpp,cpp}` mirroring
`join_order.{hpp,cpp}` — pure function, fuse/don't-fuse + `heuristic` flag,
called from both driver `applied_filters` sites. Land behind an env var.
**Validation before landing:** build a decision table of every PDS-H scan's
predicate/remaining split + type + measured selectivity, check the model
reproduces fuse q06/q09/q13/q14/q15, decline q01/q04.

## Tried and reverted — do not repeat without new evidence

- **`elide_checked_ascriptions`** (delete the node after the proof) — fixes
  Mechanism 2 free but unblocks Mechanism 3 for q09/q12, net **+25%**. Superseded
  by `fuse_checked_ascriptions` (fuse into Scan, don't delete).
- **Mechanism 3 structural gate** (`contains_row_reducing_node`) — net −10.3%,
  can't tell "small by construction" from "small by filtering".
- **Mechanism 5 classification patch** ×4 — see above.
- **Above-Arrow-API numeric selective-decode fast path** ×2 — no win, cost is in
  Arrow's page copy.
- **Hand-swapping the old q21 query text back in** — RULED OUT by standing
  project rule: join order and rewrite shape are the engine's responsibility
  (`feedback_engine_owns_join_order`).

## q21 — real, understood, NOT a conformance gap

The original guess ("push the `o_orderstatus == 'F'` filter back through the
self-join and aggregates") was **tested directly and is false** — filter
pushdown doesn't move the number (`"F"` is ~50% of orders).

The actual mechanism: the realigned query **self-joins at line granularity then
aggregates**; the old hand-fused query computes the two distinct-supplier counts
as **per-order scalar aggregates then joins two small one-row-per-order tables**.
The self-join's output is the *product* of each side's per-order line counts (an
order with 5 lines, 2 late → up to 10 join rows) — O(lines²)-shaped where the
`distinct`+`count()` algorithm is O(lines). Narrowing which orders qualify does
nothing about the per-order blowup.

Closing this needs "recognize a self-join whose sole consumer is a cardinality
comparison (`==1` / `>1` / `exists`) and replace it with an equivalent
distinct-and-count subplan" — a real query-rewrite capability, closer to a
research question than a scoped fix. **Survey done: q21 is the ONLY query in the
canonical 22 with this shape** (every other existence test maps to `semi`/`anti
join` directly). **Recommendation: close as a known, understood, single-query
gap — not a work item** — unless real-world telemetry turns up more instances.

Separately, q21 is occupancy-bound at the runtime level and *that* has yielded
wins (−11.2% at SF-4, 2026-08-27) — see `project_q21_is_occupancy_bound` and
`project_scan_instance_split_no_cost_gate` in memory.

## q22 — RESOLVED 2026-08-27

Engine-side rewrite now recognizes a left equijoin + `is_null` of a provably
non-null right marker as an anti join (when demand analysis proves no right
output is observed), and removes a direct `Distinct` on the right of a semi/anti
join. Exposed two runtime costs, both fixed: literal-bound UTF-8 `substring` now
uses the direct parallel count/prefix/write string protocol; dense-integer
semi/anti intersection scans the large key column through a bounded dense hit
table instead of millions of hash probes. Final q22 A/B: **−65.9%** median
paired ratio (Wilcoxon p=0.000061); 23.9ms vs 74.3ms SF-2/8c. All 22 answers +
parity pass.

## Remaining work, ranked

1. **Scan-fusion cost gate** — designed above, not built. Highest priority: it's
   what makes Mechanism 2 fully safe and unblocks landing more fusion.
2. **Mechanism 4** (a) row-group task granularity in `direct_decode_table`,
   (b) `fusable_string_conjuncts` Ascribe transparency.
3. **Mechanism 1** — footer schemas before lowering. Structural, own scoping.
4. **Mechanism 3** — row-count-ratio gate, low priority, re-survey first.
5. **q21** — closed as a known single-query gap unless more examples appear.
