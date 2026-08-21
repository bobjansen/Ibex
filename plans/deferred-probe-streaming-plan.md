# Parallel deferred-probe streaming plan

Status: in progress — Stage 2 code landed and synthetically validated, but
unreachable by any current TPC-H query (see Stage 2 below); real-query
measurement blocked on Stage 3 (`Filter` admission) or a different target.
Written: 2026-08-21
Revised: 2026-08-21 (Stage 1 complete — design narrowed, see "Stage 1 findings")
Revised: 2026-08-21 (Stage 2 complete — see the Stage 2 entry under "Staged implementation")

## Problem

Q12's selective build side can publish a dynamic filter that avoids substantial
right-side decode. The current chunked inner join nevertheless materializes
the entire right input before yielding its first left-side result.

Extending deferred-probe eligibility through a `Filter` by using
`project_where` was a decisive negative result: compared with the ordinary
chunked decode baseline, it won 58.30% at one core (8/8), but lost 81.14% at
two cores and 49.96% at eight (both 0/8). The ordinary path scaled roughly
196 ms to 43 ms from one to two cores; the deferred path stayed around
82/78/78 ms at one/two/eight cores.

A trial parallel `project_where_unit` then paid a large serial `rbind`
cost (about 400 ms at two cores). A separate wrapper-reconstruction attempt
was incorrect: the physical source demand omitted a renamed join key
(`join key not found in right table: o_orderkey`).

The existing cost-aware deferred-probe gate is correct and stays. This plan
does not change the Parquet plugin or weaken the eager fallback.

## Goal

For eligible right-side shapes, feed join-ready, bounded chunks directly from
a parallel physical deferred scan into the join. Preserve the regular scan's
source-unit decode parallelism and avoid a whole-table intermediate between
decode and probe.

## Stage 1 findings

Stage 1 mapped `LazyTable`, `ChunkedInnerJoinOperator`'s deferred-probe path,
join lowering, and `required_columns`/`join_output_demand`, per the staged
plan below. Two things came out of that mapping that change the design in
this document from what was originally proposed:

1. **No demand-pass extension is needed, and none of this is even new
   ground.** `required_columns(root)` already computes the exact final,
   whole-plan-aware demand this plan calls its correctness invariant — and
   every `DeferredScan` (probe scans included) already gets its
   `.demand`/`.demand_all` from exactly that call (`repl.cpp`, around the
   `ir::required_columns(*rewritten)` sites that build `DeferrableProbeScan`
   registrations). This was live and load-bearing before this plan was
   written. Stage 2 reads `scan.demand`/`scan.demand_all` off the existing
   `DeferredScan` struct; it computes nothing new.

2. **The streaming and wrapper-transform machinery this plan set out to
   build already exists, unwired to the probe path.** Tracing
   `ChunkedInnerJoinOperator::resolve_deferred_probe` /
   `resolve_deferred_probe_pair` down to their primitives:

   - `plan_deferred_scan` / `materialize_deferred_scan_unit` /
     `deferred_scan_units` (`interpreter.cpp`) are the *same* functions the
     ordinary streamed-scan path uses, parameterized only on
     `scan.filter->ready`. `DeferredScanSourceOperator` (`chunked.cpp`) is
     generic over any `DeferredScan` and has no structural dependency on how
     the scan reached "ready" — it already does source-unit-parallel,
     filter-aware, pushed-down decode.
   - Wrapper correctness (Project/Rename/Update/Filter above a probe scan)
     is already solved, just not chunked: `interpret_wrapped_right`
     (`chunked.cpp`) shadows the scan name in a registry copy holding an
     already-materialized Table and replays the ordinary interpreter over
     the wrapper subtree. This is the "row-local transform" idea this plan
     originally proposed compiling from scratch — except it is already
     written, already correct, and already exercised by every wrapped
     deferred-probe query today.
   - The actual gap is narrow: both `resolve_deferred_probe*` functions call
     `interpret_node` **once**, over the whole right subtree, immediately
     after `slot.ready = true`, and `emit_swapped()` / `emit_swapped_pair()`
     consume the resulting Table in a single pass. There is no per-chunk
     loop analogous to the ordinary (non-deferred) join path's
     `probe_chunk_against_right`, which already pulls a streamed side one
     chunk at a time against a prebuilt hash index.

   `build_operator_impl`'s stream-scan/island gate (`deferred->filter ==
   nullptr`) is a static "is this ever a probe scan" test, not a "has it
   published yet" test — irrelevant here, because the join's own
   `resolve_deferred_probe*` never goes through `build_operator_impl`; it
   calls `interpret_node` directly. That gate needs no change.

**Conclusion: this plan is a composition of three already-built, already
measured/correct primitives, not a new plan/operator/transform-IR type.**
The Design section below reflects that; it replaces the originally proposed
`DeferredProbePlan` / `RowLocalTransform` / `DeferredProbeScanOperator`
types entirely.

## Design

```
build side -> materialize/hash -> publish DynamicScanFilter (slot.ready = true)
                                      |
                                      v
                       DeferredScanSourceOperator (existing, reused)
                       - source-unit scheduling (existing window logic)
                       - constrained physical decode (plan_deferred_scan,
                         same plan/pushdown as the ordinary streamed path)
                                      |
                                      v
              per-unit wrapper replay (existing interpret_wrapped_right,
              called once per unit's Table instead of once for the whole
              scan — no new transform IR)
                                      |
                                      v
      new: chunked swapped-probe consumption loop in ChunkedInnerJoinOperator
      (single-key Mode::Swapped and two-key pair path), replacing
      "materialize right_ whole, emit_swapped once"
```

The only genuinely new code is the consumption loop: a per-unit variant of
`emit_swapped` / `emit_swapped_pair` that probes each decoded-and-wrapped
unit's Table against the already-built left-side index as it arrives, and
returns chunks from `ChunkedInnerJoinOperator::next()` as they're ready,
instead of draining every unit before returning anything. Ordering,
backpressure, and reorder-buffer questions apply to *this* loop and to
`DeferredScanSourceOperator`'s existing unit-window scheduling — not to a new
operator boundary.

This is still a hypothesis to measure, not a guarantee from reuse. The
failed `project_where` path was limited by serial assembly as much as by
decode; the same risk applies to whatever glues per-unit probe outputs back
into join output order. The new loop must separately time source decode,
wrapper replay, queue/reorder waiting, and join-consumption time. A bounded
reorder buffer that serializes the same amount of work is a failed design,
not an acceptable implementation detail.

### Wrapper admission

Admission stays row-local-only and reuses `interpret_wrapped_right`'s
existing behavior rather than a new eligibility classifier: any wrapper
chain the ordinary deferred-probe path already interprets correctly today
(Project, Rename, Update, row-local Filter) is eligible for the chunked
loop, because it is the *same* interpreter call, just invoked once per unit.
A wrapper that the existing single-shot path could not have handled (an
aggregate, window, sort dependency, join, or stateful expression above the
scan) was never reachable here in the first place — `match_probe_chain`
(`scan_predicates.cpp`) already declines those before a scan is ever
registered as a deferrable probe. There is no new decline surface to design;
the existing registration-time admission is the admission gate.

## Scope

In scope:

- source-unit-parallel deferred scans with bounded delivery, reusing
  `DeferredScanSourceOperator` unchanged;
- per-unit replay of existing wrapper interpretation (`interpret_wrapped_right`
  called per unit instead of once);
- a new chunked consumption loop in `ChunkedInnerJoinOperator`'s
  `Mode::Swapped` path (single-key and two-key pair);
- semantic, byte-identity, mutation, and multicore q12 validation, with q03
  as the selective direct-scan deferred-probe control.

Out of scope:

- Parquet API/plugin changes;
- a new transform IR or plan-compiler type (superseded by Stage 1's reuse
  finding);
- a general arbitrary operator-streaming framework;
- changing the deferred-probe cost gate (`build_side_worth_deferring`);
- removing the eager fallback;
- wrapper shapes `match_probe_chain` does not already admit.

The Parquet boundary is deliberate: this reuses the existing source API for
physical demand and dynamic filtering. It does not move projection or filter
evaluation into Parquet.

## Staged implementation

1. **Done.** Map `LazyTable`, `ChunkedInnerJoinOperator`'s deferred-probe
   path, `DeferredScanSourceOperator`, join lowering, and
   `required_columns`/`join_output_demand`. Outcome: no demand-pass
   extension needed; no new transform-compiler needed. See "Stage 1
   findings" above.

2. **Implemented; validated on a synthetic case; unreachable by the current
   TPC-H suite. Not yet measured on a real query.** In `resolve_deferred_probe`
   (single-key), after `try_two_phase_probe` declines (`TwoPhase::NotApplicable`),
   `try_stream_probe_scan` now constructs a `DeferredScanSourceOperator` over
   `deferred_scan_units(*deferred_probe_)` for a bare, Int64-keyed `Scan` and
   switches straight to `Mode::Swapped`; `next()` pulls one unit's Table at a
   time into `right_` and reuses the existing `emit_swapped()` per unit
   instead of once over a whole materialize. `initialize()` needed one
   correctness fix alongside it: it only short-circuited for
   `Mode::Precomputed` after `resolve_deferred_probe()`, so it fell through
   into its own key lookup on a still-empty `right_`; it now also
   short-circuits on `Mode::Swapped`.

   **Instrumentation swept all 22 TPC-H queries and found the path is never
   taken today**, for two distinct reasons worth keeping separate:

   - q03 and q12 (this plan's own named examples) never reach
     `resolve_deferred_probe` at all. Their post-join filters get pushed onto
     the probe scan by `push_filters_into_joins`, and `match_probe_chain` has
     no `Filter` case, so `collect_deferrable` declines to register them —
     this is the plan's own documented, already-reverted `project_where`
     history, not new breakage. These two cannot serve as Stage 2/3 controls
     until Stage 3 (row-local `Filter` admission) lands; naming them as
     Stage 2 controls in the Stage 1 revision above was a mapping gap this
     step caught.
   - Every query that *does* register (q02/q05/q07/q08/q09/q16/q17/q18/q20/q21)
     resolves via `try_two_phase_probe` to `Precomputed` or
     `RightMaterialized` — never `NotApplicable` — because decode-fusion
     stage 5's two-phase mechanism (Bloom-filtered key-only Phase A, already
     internally worker-pool-parallel, late materialization of only
     candidate/matching rows) already dominates the "materialize the whole
     right scan" case this plan was framed against. `try_stream_probe_scan`
     is reached only when Phase A's own escape hatch declines (build-side
     membership passes >75% of sampled probe keys — `kMembershipPassRateCutoff`
     in `lazy_table.cpp` — so a Bloom/candidate-selection pass is judged not
     worth its own gather-decode cost), which does not occur anywhere in the
     current suite.

   **Synthetic validation** (`tests/test_interpreter.cpp`, "Deferred probe
   streams a bare probe scan unit by unit when two-phase declines"): a
   reader-backed `LazyTable` with 4 units and 200K rows, joined against a
   build side whose key domain covers the full probe key range (forcing the
   escape hatch), asserts both a Phase A whole-column scan and ≥4 per-unit
   decodes of a payload column, and checks output correctness (row count and
   a summed payload column) byte-exact against hand-computed values.
   **One nuance surfaced here that generalizes:** the join's own key column
   contributes nothing to proving per-unit streaming — Phase A's escape-hatch
   attempt already decodes it whole into `LazyTable::cache_` before
   declining, and `project_where_unit` correctly (deliberately, per
   `lazy_table.hpp`'s documented policy) reuses that cached column instead of
   re-decoding it per unit. Only a non-key payload column exercises the new
   per-unit path; a query whose probe-side demand is the join key alone gets
   no benefit from this mechanism regardless of Stage 2/3, by design elsewhere
   in the codebase.

   A rough interleaved timing pass on that same synthetic case (4M probe
   rows, `IBEX_CORES` 1/2/8, 6 rounds each, wall-clock only — not yet the
   full `MEASURING.md` protocol) showed streaming winning at 1 core
   (~0.17s vs ~0.20s) and 2 cores (~0.116s vs ~0.140s), and roughly at parity
   at 8 cores (~0.077s vs ~0.078s, both directions appearing across rounds).
   No 2-core collapse of the kind that killed the earlier `project_where`
   attempt. This is a good-faith feasibility signal, not a publishable
   result, and it says nothing about whether Stage 3 (wrapper admission,
   starting with `Filter`) can reach a real query without the same
   regression `project_where` hit.

   **Kill the work here** if a real-query benchmark (once Stage 3 makes one
   reachable) does not retain material two-core scaling, or if assembling
   ordered output merely reproduces the old serial cost. Wrapper support and
   the two-key pair path cannot recover either failure.

   A debug env pair remains for this work: `IBEX_DEBUG_PROBE_STREAM=1` traces
   which branch `resolve_deferred_probe` takes; `IBEX_DEBUG_PROBE_STREAM_DISABLE=1`
   forces the pre-existing whole-materialize behavior even when streaming is
   eligible, for A/B timing against an identical registered/declined scenario.

3. Extend the same loop to wrapped shapes: call `interpret_wrapped_right`
   per unit's Table (registry-shadowed, as it is today) instead of once
   over the whole scan. Needs renamed-key, computed-update, and
   filter-only-column tests before being kept.

4. Apply the same loop shape to the two-key pair path
   (`resolve_deferred_probe_pair` / `emit_swapped_pair`).

5. Harden cancellation, error propagation, ordering, and backpressure across
   `DeferredScanSourceOperator`'s existing unit window and the new probe
   loop. The new loop remains subject to `build_side_worth_deferring` (or
   its explicit successor) exactly as the whole-materialize path is today.

6. Run the full correctness and performance gate. Keep the path only if it
   scales and wins at realistic core counts.

## Validation and acceptance criteria

Correctness coverage must include direct scans and every wrapper shape
`match_probe_chain` already admits, renamed join keys, computed updates,
filter-only columns, nulls, empty inputs, and multiple source units.
Deferred output must be byte-identical to the eager path. Mutation tests
should detect omission of every required physical source column (reusing
the existing `.demand`/`.demand_all` fields — no new demand computation to
mutation-test). Unsupported shapes must demonstrably fall back exactly as
they do today (`match_probe_chain` returning nullopt).

Performance work follows `MEASURING.md`, uses `build-release`, and reports
interleaved repeated A/B q12 runs at one, two, and eight cores. Operator
profiles must show source-unit decode parallelism and no whole-table
concatenation. q03 is the named selective direct-scan control at the same
core counts: it distinguishes a q12-specific wrapper/shape result from a
generally sound mechanism. Also benchmark representative wrapped and
fallback joins.

Success is explicitly not a one-core win: the path must retain meaningful
two-core decode scaling and be a statistically supported net win at two and
eight cores. Otherwise keep the current cost-aware gate only.

## Risks to settle before integration

- Define ordering guarantees and bound the reorder buffer in the new probe
  loop; `DeferredScanSourceOperator` already serves units in order, but the
  loop must not re-serialize that gain while assembling probe output.
- Instrument and budget serial reorder/assembly explicitly; bounded memory
  alone is not evidence of parallel scaling.
- Bound in-flight chunks for backpressure and memory control.
- Confirm `interpret_wrapped_right`'s per-unit replay cost is not itself a
  serial bottleneck at small unit sizes (registry copy + interpreter
  dispatch per unit) before assuming reuse is free.
- Keep declined-plan diagnostics so future eligibility changes are testable.
- Gate every new deferred path through `build_side_worth_deferring` (or an
  explicit successor); add transform-cost estimates only if measurement
  later warrants them.
