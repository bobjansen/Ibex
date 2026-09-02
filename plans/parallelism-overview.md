# Multi-core execution: the inconsistency catalogue

**The design lives in [`src/runtime/PARALLELISM.md`](../src/runtime/PARALLELISM.md)**
(the mental model, the three layers, who owns which decision, the determinism
contract, the config surface — kept current with the code). This file is the
companion *plan*: Part 2 below is the catalogue of places the implementation
diverges from itself, with a suggested order of attack and the measured findings
that closed or refused each track.

Part 1 (a from-scratch restatement of the design in parallel-database
vocabulary) moved into `PARALLELISM.md` on 2026-08-27 rather than being kept in
two places. Line references below are `file:line` at 2026-08-18 and have
drifted — the names are the stable handle; re-`grep` before trusting a number.

---

## Part 2 — Inconsistencies

Undocumented divergence rather than bugs: each new operator copies whichever
neighbour it was written next to, and the spread widens. Severity is the cost of
leaving it, not correctness today. Several are RESOLVED; kept so they are not
"re-fixed".

### Column-type divergence
- **I1 — three answers for "can this column type gather in parallel?"** —
  **RESOLVED.** `gather_column` was a divergent second copy of the `gather_rows`
  kernel; now written in terms of it, 64-row-aligned ranges via `for_row_ranges`
  make bit-packed `bool` safe everywhere. `TwoPhaseFilterOperator` stays separate
  by design (writes at a prefix-summed offset — a different operation).
- **I2 — String/Categorical silently excluded from three fan-out decisions for
  three reasons** (`stageable_conjunct_columns` width; `evaluate_field_maybe_parallel`
  Int/Double only; parallel partial-agg Int/Double only). Each reasonable; no
  shared "is this type parallel-capable in role X" predicate.
- **I3 — multi-key Categorical group discovery has no partitioned path**
  (`multi_cat_find_or_insert` is serial). Unmarked in code.

### Serial-path vs chunked-path divergence
- **I4 — whole-table operators (`filter/aggregate/join/sort/update.cpp`) vs the
  chunked reimplementations; parallel coverage differs sharply.** Corrected twice:
  every entry point routes through the chunked engine (`interpret()` →
  `build_operator` → chunked → `MaterializeOperator`); the whole-table functions
  run *only* for a subtree beneath a declined node (Median/quantile/EWMA) *within
  one statement* — a `let` boundary means they almost never run. So this is a
  drift hazard and a cleanup tax, not a user-facing perf gap.
  - **Distinct COLLAPSED** (`74f6e32`); **inner join COLLAPSED** (worktree after
    `6f0a03e`, kept whole-table signature, delegates to
    `ChunkedInnerJoinOperator`); **sort** already converged (no duplicate radix).
  - **Aggregate is NOT a collapse** — a median aggregate is itself the declining
    node, so `aggregate_table` runs at top level for every median query, and a
    mixed `median(x)+sum(y)` needs both in one pass. The escape hatch is design.
    I4 is complete at 2 of 3; the goal was zero *duplicated* implementations.
  - Coverage test lesson: write it as a **single statement** and mutation-check,
    or it verifies the operator it already had (a bind-then-aggregate test hits
    the chunked path and never the fallback).
- **The `6215c88` hash pathology (LANDED, 12.9×).** `median(v) by {a,b}` over
  1M rows: 517ms at 9800 groups, 20ms at 5000 — same row count. `hash_key_row`'s
  `hash_combine` over identity `std::hash<int64_t>` never finalizes, so for two
  small int keys the result is ~`b + (a<<6)`, linear, one probe cluster. One
  `fmix64` fixes it; the 6000→9800 ramp flattens 29/102/258/411/523 → 31/31/33/43ms.
  Needed *both* multi-key and a specific table size to show — a sweep varying one
  dimension misses it. The finalizer had to be added in three places
  (`hash_key_row`, `hash_key_value`, `mix_one`, `multi_cat_find_or_insert`) —
  I4's thesis: the hazard is a constant written more than once.

### Gate / threshold divergence
- **I5 — `gather_column`'s `exec` was optional, so half the callers gathered
  serially by omission** — **RESOLVED**, `exec` now required; surfaced I15.
- **I6 — nine private row thresholds** (`parallel_min_rows` 65536, `kMinProbeRows`
  16384, `kMinParallelPredicateRows` 262144, `kDefaultPartitionMinRows` 262144,
  …) coexist with the two `ExecutionContext` knobs. Per-operator break-evens
  genuinely differ; the problem is a test/benchmark can't sweep them without
  recompiling. *Convergence:* express as `ExecutionContext`-scaled multiples.
- **I7 — `parallel_min_cells` (width) consulted by 2 of ~30 sites**
  (`island_is_worth_morselizing`, `gather_rows_parallel`). The finding behind it
  ("131k rows won at 6 cols, lost at 2") applies verbatim to filter gather, join
  concat, aggregate emit — none check it.
- **I8 — two authorities on whether a query is parallel** — **RESOLVED.**
  `IBEX_JOIN_PROBE` / `IBEX_STREAM_SCAN` are now `ExecutionContext` fields set by
  `configure_parallel_from_env`, one shared `env_flag` parser.
- **I9 — worker-count caps differ arbitrarily** (`min(...,64)` / `min(...,16)` /
  uncapped). The 64 and 16 are undocumented. (low)
- **I10 — aggregate emit gates group count against a *rows* threshold** — unit
  mismatch, conservative. (low)
- **I11 — some gates build the pool before deciding they don't want it** —
  **RESOLVED** (2026-08-21): test `exec.parallel` before `process_worker_pool()`.

### Structural
- **I12 — two thread species, one on the books** — **PARTLY RESOLVED.** The raw
  `std::thread` in `PipelinedStageOperator` is a distinct kind: long-lived, parks
  on consumer backpressure. Hosting it on the pool would deadlock (N producers
  parked, consumer needs a pool batch to drain them). Now `on_stage_thread()`
  names it, `StageThreadScope` counts it, the profile reports `stage_threads_peak`.
  It had corrupted a measurement (stage work charged to main-thread self time,
  `self_ms > wall_ms`; fixed by splitting `stage_self_ns`). Remaining: make the
  two species explicit in the primitive. `stream_buffered.hpp` (also detached a
  thread) removed 2026-08-22.
- **I13 — cancellation reaches islands and scan pipelines, not intra-operator
  fan-outs.** Consistent with the "per node/chunk/statement, not mid-op"
  contract; the gap grows each time an operator gets a bigger parallel section.
- **I14 — two island-grain philosophies** (island grain from thread count;
  aggregate morsel count from row count for FP determinism). Nothing warns the
  next author that reusing `island_grain` inside a reduction breaks determinism.
- **I15 — join output assembly fanned out once per COLUMN; sort fans out once for
  the table** — **RESOLVED.** `gather_columns_batched` now holds the sort's
  single-batch (column × range) shape; had two instances (`join.cpp` +
  `ChunkedInnerJoinOperator`), both collapsed. `gather_entry` lost its now-always-
  null `exec` param. Measured neutral (the per-column barrier was eating the
  gather it bought).

### Not inconsistencies (recorded so they are not "fixed")
- Hash-join build side is never threaded — 1.5% of q10 vs the probe's ~15%.
- Prefix sums in `TwoPhaseFilterOperator` / `try_discover_partitioned` are serial
  on purpose: O(morsels), not O(rows).
- A refused island runs as one whole-table chunk, not a serial morsel sweep —
  fix for a measured 3× regression.
- `parallel_threads` deliberately does not cap the scan pipeline.

---

## Status of the "order of attack"

Items 1–6 (I8 into context; I1+I5 one gather family; I15 single-batch assembly;
barrier instrumentation; price the scheduler; close the accounting) — **all
done**. Closure is 99.6% on the pool, 99.9% on stage threads.

**Item 7, the task scheduler — DROPPED, on measurement.** `pool_idle_ms` and
`stage_park_ms` are ~0 across all 22 queries while ~67% of the pool sits with
nothing queued: no thread is ever blocked behind another's work, so stealing has
nothing to steal. Re-measured 2026-08-21 at `190235b` (39 commits on): occupancy
30.4→32.8%, `pool_idle_ms` still ~0. Revisit only if occupancy rises far enough
that queues build.

**Item 8, I4 collapse** — done at 2 of 3 (above).

**Item 9, create parallel work in the five idle-dominant queries (q13/q21/q18/
q10/q20)** — the standing structural lever in place of the scheduler. Sub-status:
- Intra-operator fan-out (q04 semi-join build) — **DONE** `7fde36d`
  (`init_int_swapped` parallelized; q04 ~−39% earlier, now decode-bound).
- Join `assemble_output` (q21/q20/q09) — **not a live target**: I15 already made
  it cheap and parallel; what dominates is `ring_wait` on the right-side scan, a
  producer-side cost.
- The aggregate (q13/q18/q10/q20) — the one genuinely open thread. Every
  probe-cost trick tried and rejected (prefetch, hash reuse, packed-key domain
  narrowing, two-level grouping, pass-3 shortcuts — see
  memory: `project_reverted_perf_dead_ends`).
  A real win needs a **partition-owned aggregate API** (owns a partition from key
  discovery through mergeable state). First piece LANDED `4fedf2a4` (PairIntKey,
  q20 −15-18%); Int64 single-key slice + the ordered-run finalize are the live
  work — see `owned-agg-per-chunk-barrier-plan.md` and
  memory: `project_q21_is_occupancy_bound`.

**Items 10–11** (type exclusions; elide the first-occurrence merge when the
consumer provably ignores order) — small, unstarted, worth less than they look.

---

## Multiple producers per staged breaker — investigated, largely reverted

The suite-wide "70% idle, producers are the bottleneck everywhere" motivated
asking whether a staged breaker should have several producers. Findings:

- The engine already runs 2–3 concurrent producers when the plan shape produces
  them (nested streamable joins, deferred-key probes: q09 peak 2, q18 3).
- The real gap is **sibling scans that could overlap but don't**: in q10 the
  `orders` filter runs at 18.5% occupancy in its own 13ms window while the
  independent lineitem filter runs at 65% — `build_operator` recurses depth-first
  through the left-deep join chain, so `orders`' decode doesn't start until the
  lineitem side resolves. Synthetic A/B (`bench_multi_producer.cpp`, since
  deleted) confirmed **~15%** real wall savings from decoding the two
  concurrently.
- **POC against q10** (`is_streamable_inner_join` branch, raw `std::thread` runs
  the right side's materialize concurrently with `build_operator(left)`, guarded
  on single-base-scan + no shared source + no `ModelResult`): landed, ~8% on q10
  in isolation, ~3% in-suite.
- **Generalization — reverted.** Widening eligibility (`subtrees_share_source`):
  kept, flat. Same overlap in `streamable_semi_anti`: q04 +19%. In
  `build_binary_materializing_operator`: q09 +57% — that choke point materializes
  **both** sides, so overlapping two expensive full materializations contends for
  bandwidth instead of filling idle cores.
- **`HelperThreadSlot`** (RAII budget slot vs `compute_thread_count()`) was built
  to bound the raw threads — but a trace showed q09 hits the choke point exactly
  once, so thread count was never the mechanism. The cost is structural
  (both-sides materialization), not oversubscription.
- **All of it removed 2026-08-21** ahead of the kernel-pipeline restructure. The
  join builder is serial left-build-then-right-materialize again; branch
  concurrency has zero footprint. Recover from `38d6b307`/`190235b2`/`a3b39c6d`
  if a **cost-aware** gate (skip when both sides are large) is ever justified.
  `PipelinedStageOperator`'s stage thread is untouched.

## Deferred-probe streaming — stopped, code removed

Stage 1 (mapping onto the demand machinery) landed and is production. Stage 2
(streaming probe for bare probe scans) was instrumented unreachable, removed.
Stage 3 (admitting a row-local Filter above the probe scan): q03 won 2.6–3.2× at
1 core by finally reaching the *existing* two-phase mechanism, but q12 collapsed
— its unfiltered `orders` build side covers the probe key domain, so the Bloom
rejects nothing. **Trap worth keeping:** `try_two_phase_probe`'s `Precomputed`
branch computes `li`/`ri` from a hash probe *before* the wrapper chain runs —
sound while that chain is 1:1, wrong the moment a Filter can drop rows there.
Reopening needs the selectivity cost model — see
memory: `project_deferred_probe_selectivity_scoping`.

---

## Rejected: weakening first-occurrence group ordering

Turned down 2026-08-17. Recorded because the argument is tempting from inside the
engine and someone will make it again.

**Proposal:** declare group-by output order unspecified (at least for the hash
case) → the serial first-occurrence merge in `try_discover_partitioned`
disappears, the partitioned path emits partition-by-partition, grouped
aggregation streams its output.

**Why not:** the prize is ~26ms of ~172ms on q18 at 3M groups (O(groups), the
worst case) and proportionally less everywhere. Against it: (1) it **spends the
determinism contract** — byte-identical serial/parallel output is what makes
every threshold in Part 1 a free parameter; (2) it blunts the main verification
method (`diff <(IBEX_CORES=1) <(IBEX_CORES=8)`); (3) it is a breaking change to
user scripts — Ibex is `data.table`-inspired, where `by=` preserves first
appearance.

**Do instead:** elide the merge at PLAN level when the consumer provably ignores
order (feeds an `order`, a scalar aggregate, or a join) — a *required-ordering*
property propagating DOWN the plan. No language change, nothing breaks, captures
the cost where it is free. This is item 11.

---

## Measured: where the main thread's time goes (PDS-H SF-1, 8 cores)

This table was wrong twice before it was right — both errors inflated "serial"
and were misattributed idle (stage-thread work 79ms; ring parks 374ms).

| | ms | share of self |
|---|---|---|
| wall | 1286.9 | |
| main-thread self | 1243.7 | |
| — genuinely serial work | **442.8** | 35.6% |
| — parked at a barrier | **414.7** | 33.3% |
| — parked on a pipeline ring | **386.2** | 31.1% |
| pool worker work | 3053.2 | |
| pool worker backpressure park | **0.0** | |

**64% of the main thread's non-worker time is idle, not serial. The machine is
~70% empty** — pool occupancy 29.7%, `pool_unqueued_ms` 69.2% of capacity,
closure 99.6%. Total real work `(3053 + 443) / 1286.9` = **2.72× on 8 cores**.

Per-query spread: q02 97.1% empty, q16 85.1%, q22 89.5% (small queries run on one
thread); q06, the most parallel, still 38.8% empty.

Stage producers: `stage_park_ms` is 0.0 on all 22 queries — a producer never
once filled its ring and waited for the consumer. Combined with `pool_idle_ms ==
0.0`: **nothing in this engine is ever blocked by a downstream consumer being too
slow. Consumers wait on producers, never the reverse.**

**The three idle kinds want different treatment:**
- **Barrier park** (~420ms) — a work-participating `join` recovers only ~47ms
  (3.6% of wall); the work still has to be done.
- **Ring park** (~374ms) — mostly the producer side being the bottleneck, often
  correct not wasteful (q06 is 98.7% idle because the query genuinely has nothing
  for the main thread to do).
- **Serial work** (~443ms) — the only part needing algorithmic attention.

**The lever is more and finer parallel work, not better scheduling of what
exists** — see memory: `project_serial_fraction_is_the_ceiling`.

## Before attributing serial time to an operator, check which node it is

Two attribution mistakes made in one session (2026-09-02), both of which read as
architectural limits and were neither:

- **"The join build side is serial."** It is not, structurally: the build side's
  input is a real operator chain (`MaterializeOperator(left_).run()`) and the
  scan under it gets pool tasks. What was actually serial in a q14 experiment was
  the `Update` sitting between them, because one field shape fell off
  `plan_direct_field` — and that route is **all-or-nothing per update node**, so
  a single unrecognised field serialises the whole node. `serial_fraction` moved
  0.133 → 0.288 from one such field over 1.6M rows. Fixed in `4ac93b33`; see
  `parallel-chunkview-output-plan.md`. The general rule: read `pool_tasks` per
  node before naming the operator, because "this operator is serial" and "this
  operator's *expression* fell off a fast path" look identical from the outside.

- **`__memmove` percentages are not DRAM traffic.** A decode change sized off a
  33%-of-profile `__memmove` measured 20% *slower*, because the buffer being
  copied is a 64Ki-row scratch batch that lives in L2. Ask whether the buffer
  fits in cache before treating a copy as bandwidth. `beat-polars-plan.md` §8.6.

And one measurement that bounds the whole discussion: on the dev box the pure
page-cache read ceiling is **44.5 GB/s at 8 threads** (14.3 at one). A query
that reads 903 MB therefore has a ~20 ms floor — which is how we know q14, at
~104 ms, is 4.5× off its own I/O floor rather than anywhere near a hardware
limit. Cheap to re-measure; worth doing before calling anything bandwidth-bound.

## When a fan-out moves work rather than adding it

`push_computed_columns_into_joins` (built and reverted, `beat-polars-plan.md`
§6) is the cautionary case for a whole class: moving an expression to a
different operator changes the **row count it is evaluated over**, and that
factor is invisible in the plan without cardinality estimates. Pushing
`Int64(like(p_type, …))` onto q14's `part` side was 2.7× more evaluations and
paid; pushing `Int64(o_orderpriority == …)` onto q12's `orders` side was 9× more
and cost 40%. Same pass, same shape, opposite verdicts. Any fan-out or
placement change that alters which operator evaluates an expression needs both
row counts before it is safe to enable.
