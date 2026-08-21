# Deferred-probe selectivity cost model — scoping

Status: scoping only — no code, no constants.
Written: 2026-08-21

## Problem

Deferred-probe registration (`ir::deferrable_probe_scans` /
`match_probe_chain`, `scan_predicates.cpp`) currently gates on
`build_side_worth_deferring`: `estimated build rows * 2 < probe's exact row
count`. This is a **row-count** proxy standing in for what the decision
actually needs — an estimate of how much of the probe scan's key domain the
build side's keys will actually match. The two diverge exactly when a build
side is small *by construction* rather than small *by filtering*: a table
that is naturally smaller than the probe side but still covers (or nearly
covers) the join key's whole value domain gains nothing from deferring — the
published Bloom/bounds reject nothing — while still paying every fixed cost
of the apparatus (build-side materialization, Bloom construction, Phase A's
key-only scan).

`deferred-probe-streaming-plan.md`'s Stage 3 hit this concretely: extending
registration to admit a row-local `Filter` above the probe scan made both
q03 and q12 newly eligible. q03 (customer, filtered by market segment,
joined into orders, filtered by order date — a genuinely reduced build side)
won big by finally reaching the existing, already-fast two-phase mechanism.
q12 (`orders` unfiltered, ~1.5M rows, joined into `lineitem`) lost badly at
2 and 8 cores: `orders` is smaller than `lineitem` by row count but *not* by
key-domain coverage — every `l_orderkey` in `lineitem` references a real
`o_orderkey` in `orders` (referential integrity, no filter removed any
orders), so the Bloom is, in the limit, a 100%-pass-rate no-op. The row-count
gate cannot see this distinction; it needs an estimate of real key-domain
overlap, not row counts.

## Why the obvious approaches are already known to fail here

Three independent prior attempts in this codebase hit variants of this exact
problem. Read all three before designing anything — each rules out a design
that looks reasonable in isolation.

1. **A structural "was this side filtered" proxy — tried, reverted, -10.3%
   full suite.** `project_query_shape_conformance_regression`'s Mechanism 3
   gated deferred-probe eligibility on `contains_row_reducing_node` (does the
   build subtree have a filter anywhere in it). It correctly declined q12
   but also wrongly declined q08's `part join lineitem` (`part` is small by
   construction — no filter needed at all, and gating on filter-presence
   alone cannot tell "small because it's a small table" from "small because
   nothing filtered it and it's still huge relative to what matters"). The
   lesson recorded there: **"presence-of-filter isn't the same property as
   smaller-than-probe-side, and the two don't decompose cleanly" — a real
   cardinality signal is required, not a structural stand-in.** Do not
   propose a filter-presence or node-shape gate here.

2. **Sampled distinct-value counts on exactly this key — tried, disabled by
   default after regressing q10 5x.** `make_relation_sampler` (`repl.cpp`)
   is real, wired, and used for join-order costing via
   `cardinality.cpp`'s `distinct_below`/`resolve_key_distincts` — but a
   single-row-group sample gave `o_orderkey`/`l_orderkey` a *worse* distinct
   estimate than the footer span it was meant to improve on, because TPC-H's
   order-key generation is not uniformly distributed across row groups. It
   stays gated behind `IBEX_EXPERIMENTAL_JOIN_SAMPLING`, opt-in only. **Do
   not reuse the sampler's distinct-count path for this decision without a
   validated, representative sampling strategy — none exists yet.** (The
   *predicate-selectivity* half of the same sampler — `predicate_passed /
   sampled_rows` for an explicit filter, used by `query-shape-conformance-plan.md`'s
   separate scan-fusion cost gate — is a different statistical question and
   is not shown unreliable the same way; it answers "what fraction of a
   sampled row group passes this filter", not "how many distinct values does
   this whole column have". Do not conflate the two when deciding what's
   safe to reuse.)

3. **A naive footer-derived distinct-count ratio would reproduce the same
   failure through a third mechanism.** `ir::distinct_estimate`
   (`cardinality.hpp`) falls back to `min(rows, max - min + 1)` on a
   column's footer statistics when no uniqueness proof is available. That
   estimate is *already documented* (in `RelationSample`'s own doc comment,
   from the join-order work) to read "far higher than its true distinct
   count" specifically for `l_orderkey`, because TPC-H's key generator skips
   values. A ratio of `distinct_estimate(build_key) / distinct_estimate(probe_key)`
   would divide by an inflated `l_orderkey` estimate and make q12 look
   *more* selective than it is — reproducing Stage 3's exact failure through
   a smarter-looking but equally broken formula. **Any design here must
   avoid depending on a footer-derived distinct-count estimate for the
   *probe* side's key column specifically**, since that is the one column
   this codebase has already caught lying in this direction.

## What's already available and safe to reuse

- **`SourceRowCounts`** (`row_counts`, footer-exact, zero decode) — already
  threaded into `deferrable_probe_scans`/`build_side_worth_deferring`.
- **`ir::estimate_cardinality`** — already called by `build_side_worth_deferring`
  for the build side's row estimate; sees through filters.
- **`ir::column_origin_of` / `ir::column_origins`** (`column_origins.hpp`) —
  traces any node's output column back to its base `(source, column)`,
  walking through Project/Rename/Filter/Order/Head/Tail/Distinct and (per
  `plan_join_key_origins`'s existing use, `scan_predicates.cpp`) through
  nested joins, one side at a time. Purely structural, no decode. This is
  the piece that lets a formula reach "which base table does this join key
  ultimately come from" for a multi-level build side like q03's
  `customer join orders`.
- **`ir::distinct_estimate`'s row-count cap.** Its result is always capped
  by the node's own row estimate — "no operator invents values, none can
  leave more distinct values than rows" (its own doc comment). This means a
  build side's distinct-key estimate is safe from the footer-inflation
  problem in item 3 above *without* needing a uniqueness proof: a build
  side's own (possibly filtered) row count is always a valid upper bound on
  how many distinct keys it can contribute, regardless of the key column's
  skip pattern. The risk in item 3 lives entirely in using the same kind of
  estimate on the *probe* side, whose row count (millions of fact rows) is
  nowhere near its true key cardinality.
- **`prove_unique_columns`** (`repl.cpp`) exists but is capped at
  `kMaxProofRows = 1<<20` (~1M rows) specifically because proving costs a
  full column decode and the cap is calibrated for genuinely small dimension
  tables. `orders` (1.5M rows at SF-1, more at higher scale) is *already
  over this cap* — this mechanism will not fire for the table this whole
  investigation centers on. Don't design around it firing here; treat it as
  unavailable for build sides at this scale.

## Candidate direction (not a decision — needs prototyping and calibration)

Avoid asking the probe side anything about its own key's distinctness at
all. Instead: estimate the **build side's origin table's own row count** —
not the build side's row count *after* whatever filters ran, but the
*unfiltered* row count of the base table its join key structurally comes
from — and use that as a domain-size proxy.

```
origin = column_origin_of(join.children()[0], keys.front().left, schemas)
domain_size = origin.has_value() ? row_counts[origin->source] : nullopt
build_rows = estimate_cardinality(join.children()[0], row_counts, schemas).rows
selectivity_estimate = domain_size.has_value() && build_rows.has_value()
                          ? build_rows / domain_size
                          : nullopt  // no information: stay permissive, as today
```

For q12: `origin = (orders, o_orderkey)`, `domain_size = row_counts["orders"]
= 1.5M`, `build_rows` (orders, unfiltered) `= 1.5M` → ratio `= 1.0` → not
selective, decline.

For q03: `origin` still resolves to `(orders, o_orderkey)` through the
`customer join orders` build subtree (via `column_origin_of` walking the
join), `domain_size = 1.5M` (orders' *own*, unfiltered row count — a
property of the table, not of this query's filters), `build_rows` = the
`customer`+`orders`-filtered estimate (a few hundred thousand, per
`estimate_cardinality` seeing through both filters) → ratio well under 1 →
selective, accept.

Why this sidesteps all three known failure modes: it never estimates
distinct values on the probe side at all (failure modes 2 and 3), and it is
a real cardinality signal, not a structural filter-presence proxy (failure
mode 1) — `domain_size` is the same regardless of whether a filter is
present anywhere; what changes the ratio is the build side's *estimated row
count*, which `estimate_cardinality` already computes by seeing through
filters, exactly the distinction Mechanism 3 needed and didn't have.

## Open questions to settle before implementing

1. **What replaces `nullopt`?** When `column_origin_of` can't resolve a
   single origin (a computed/derived key, a union of several base tables, an
   Update-produced column) — fall back to the current row-count gate,
   staying permissive by default, matching `build_side_worth_deferring`'s
   existing convention for missing information. Confirm this doesn't just
   silently degrade to today's behavior for most real queries (check what
   fraction of the current 10 eligible queries would hit this fallback).
2. **Add to the existing gate, or replace it?** Recommend *add*: keep the
   current `build_rows * 2 < probe_rows` check as a cheap pre-filter (it's
   already computed, free), and only evaluate the domain-size ratio when
   that passes — avoids adding `column_origin_of` cost to the common case
   where the row-count check alone already declines.
3. **What margin, not just what sign.** A ratio computed from two estimates
   (one of them itself an estimate, `estimate_cardinality`) needs a
   deliberate margin, not a bare `< 1` — this codebase's own join-order cost
   work has an explicit "symmetric prize" lesson: a bad estimate can turn a
   good plan bad, not just fail to improve one. Calibration work, not a
   design decision to make blind — see `query-shape-conformance-plan.md`'s
   Cost gate section for the same lesson applied to a different decision.
4. **Two-key joins.** `collect_deferrable` already handles a two-key join by
   filtering on its first component only (independent-component pruning).
   Does the domain-size formula apply per-component the same way, or does a
   two-key join need its own calibration? Not investigated here.
5. **Does this fully explain q12, or only half of it?** Stage 3's
   benchmarking of the *streaming* consumption loop specifically (not just
   registration) showed q12 stayed roughly flat across 1/2/8 cores even
   after correctly reaching the new per-unit path. The existing two-phase
   `Precomputed` branch already threads its scan/replay step across
   `process_worker_pool()` workers (`probe_parallel_workers`); the Stage 2/3
   streaming consumption loop calls `emit_swapped()` serially, one unit at a
   time, on the calling thread — decode-ahead is parallel (`DeferredScanSourceOperator`'s
   window), but the join-probe step itself is not. If probe cost (not
   decode cost) dominates a large, low-selectivity join like q12, a correct
   selectivity gate would simply decline to defer it at all, sidestepping
   the question — but a *correctly gated, genuinely selective* case that
   still produces a large candidate set per unit could hit the same serial-probe
   ceiling. This is a distinct, unconfirmed suspicion, not investigated
   further here, and belongs in `deferred-probe-streaming-plan.md`'s own
   scope if the streaming mechanism is revisited — flagging it here only so
   it isn't mistaken for solved once a selectivity gate lands.

## Validation protocol

Before wiring anything into the driver: build a decision table (by hand or
script) — every currently-eligible query's build-side origin, footer row
count, and estimated build rows — and check the formula's predicted
accept/decline against what Stage 3 already measured directly: **must
decline q12** (measured: 2-8 core regression), **must still accept q03**
(measured: clean win at every core count), and must not flip any of
q02/q05/q07/q08/q09/q16/q17/q18/q20/q21's current registration decision
without an explicit, separately-measured reason. A model that doesn't
reproduce these known cases by hand isn't ready to wire in.

Once wired: full `check_answers.py` (22/22) and `ctest`, then interleaved
A/B at 1/2/8 cores on q03/q12 plus the other currently-eligible queries as
controls, `build-release`, before considering Stage 3 of
`deferred-probe-streaming-plan.md` again.

## Explicitly out of scope

- Any sampling-based estimate (`make_relation_sampler`'s distinct-count
  path) — unsafe without a validated, representative sampling strategy;
  that is `join_order.cpp`'s open problem, not this plan's to solve.
- Relaxing `prove_unique_columns`'s `kMaxProofRows` cap — a separate,
  cost-sensitive decision (measured +5.5% geomean when applied
  unconditionally); not needed by the candidate direction above, which
  doesn't depend on a uniqueness proof at all.
- Any structural/filter-presence proxy — already tried, already reverted,
  -10.3% full suite.
- The streaming consumption loop's own probe-parallelism (open question 5
  above) — belongs to `deferred-probe-streaming-plan.md`, not here.
- Re-attempting Stage 3's `match_probe_chain`/`base_scan_of` Filter-admission
  change — that code was reverted and is not touched by this plan; it is the
  *consumer* of whatever gate comes out of this work, once one exists.
