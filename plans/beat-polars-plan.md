# Beating Polars Multi-Core

Status: **proposed.** Written 2026-08-16 on branch `streaming-scan-units`, after
the pipelined-execution Phase 2 slices (streamed scan units, concurrent decode,
source→map pipeline, join-probe handoff) landed. This is the umbrella plan: it
sets the target, decomposes the remaining gap into workstreams with expected
payoffs, and sequences them. It supersedes nothing — it points into
`pipelined-execution-plan.md`, `runtime-multithreading-plan.md`,
`chunked-execution-plan.md`, and `join-perf-plan.md` for mechanism.

## 1. What "beat Polars" means, in numbers

PDS-H SF-2, pinned cores, same budget both engines
(`benchmarking/tpch/results/runs/`):

| cores | Ibex | Polars | Ibex/Polars |
|---|---:|---:|---:|
| 1 | 3779 ms | 8164 ms | **0.46** (2.16× faster) |
| 8 | 2309 ms | 2000 ms | **1.15** (15% slower) |

Ibex behaves like a program with an implied parallel fraction of **~44%**
(Amdahl fit holds at every core count); Polars fits ~86%.

**The decisive arithmetic: we do not need Polars-level scaling.** Because the
single-core base is 2.16× faster, "beat Polars at 8 cores" only requires an
8-core speedup of 1.89× over our own 1-core time. Solving Amdahl:

| outcome at 8 cores (SF-2) | needed total | needed speedup | needed parallel fraction |
|---|---:|---:|---:|
| parity with Polars | 2000 ms | 1.89× | **54%** |
| beat by 10% | 1800 ms | 2.10× | **60%** |
| beat by 20% | 1667 ms | 2.27× | **64%** |

So the goal is: **raise the implied parallel fraction from ~44% to ~60–65%**,
while not giving back the single-core lead (the 1-core geomean is a hard
no-regression gate — it is the asset the whole strategy leans on).

The same thing in absolute terms, which is how the workstreams are ranked: at
8 cores the total is ≈ serial + parallel/8 ≈ 2116 + 208 ms. **The fight is over
~2.1 s of calling-thread (serial) time.** Beating Polars by 10% means removing
or parallelizing ~450 ms of it. The operator table below says exactly where it
is, so every workstream is stated as "serial ms attacked".

## 2. Where the serial time is

Calling-thread self-ms summed over the suite at 8 cores, with worker help drawn
(`pool_work / self`), from `pipelined-execution-plan.md` ("Where the serial
time actually is", post-semi-join numbers):

| operator | self ms | pool ms | worker help | verdict |
|---|---:|---:|---:|---|
| scan | 631 | 2902 | 4.6× | best-parallelized op in the engine; residual is small-query tax + per-unit glue |
| aggregate | 552 | 945 | 1.7× | gid discovery serial by design; high-cardinality path declines; first chunk discovered serially |
| **join inner** | **435** | 80 | **0.2×** | the largest essentially-unparallelized operator |
| join semi | 183 | 285 | 1.6× | predicate threaded (done); gather serial by measurement |
| join anti | 36 | 14 | 0.4× | small |
| update | 70 | 145 | 2.1× | fine |
| distinct | 48 | 191 | 3.9× | fine |

Plus two costs that don't appear as operator self-time:

- **The sink concat.** Streaming a large intermediate into a `let` binding glues
  chunks into one contiguous `Table` (`MaterializeOperator`). q21's `li_F` is
  ~58 MB of memmove the single-chunk path never paid. This scales with
  *output* size and is why streamed-scan losers are exactly the queries binding
  large intermediates (q18, q21, q22).
- **Pipeline scheduling gaps.** The first scheduler slice moved the implied
  fraction only ~1 pt (48→49 at SF-1) with named regressions: the 2-core cliff
  (+6.9% SF-4), q18 (+13–24%), q22 (+12–19%), q17/q20 (~+10%).

## 3. Workstreams

Ordered by serial-ms attacked per unit of risk. W1 and W2 are intra-operator
and independent of the scheduler; W3 is the scheduler itself; W4 is structural.
W1/W2 can proceed in parallel with W3.

### W1 — Parallel inner join (~435 ms attacked; expect 250–300 ms)

**Status 2026-08-16: DONE** (commits `294230c` + `e06e1d7`), in two slices,
all under one `IBEX_JOIN_PROBE` gate (`probe_parallel_workers`, on by
default) with a `parallel_probes` counter and both-direction tests:

1. *Stream-mode probe* (the unshelved stash): `probe_ranges_parallel` fans
   the probe over contiguous ranges into per-worker (li, ri) parts,
   concatenated in range order — byte-identical. The part concat itself runs
   on workers above 64k output rows; the serial concat was most of the tax on
   high-match probes (SF-2 geomean 0.987 → 0.972 from that alone).
2. *Swapped mode + phase A*: `emit_swapped`'s two phases unified into one
   `probe_swapped` scan/replay (per-worker hit parts, replay into disjoint
   slices at prefix-summed offsets); `try_two_phase_probe`'s candidate-key
   loop uses the same parts with two prefix offsets (first hit, first pair)
   so survivors, the key gather, and pair expansion all write disjointly;
   `apply_membership_filter`/`membership_selection` split the Bloom probing
   into per-range kept lists.

**Cumulative W1 A/B** (interleaved ABBA×2 min-of-4, 8 pinned cores, on/off
same binary): **SF-1 geomean 0.950, SF-2 geomean 0.943** — a ~5.7% suite
win. q11 −41%, q05 −17%, q09 −12%, q07 −12%, q10 −11%, q16 −10%, q03 −6%.
Small-query upticks (q02/q13/q22 +4%) did not reproduce across runs. All 22
answers OK serial and parallel; full suite green.

**Deliberately not done:** multi-key probes (`keys_->size() > 1` never
reaches `ChunkedInnerJoinOperator` — q02/q05/q09's two-key joins run in
`join_table_impl`, a separate serial probe; measure before threading),
partitioned parallel build (step 3 — build_index was 42 ms suite-wide, still
believed not worth it), and the phase-A selectivity estimate (step 4, waits
on the footer-stats cost model).

The one large operator drawing no pool help. Its self-time is *not one lever*
(measured split: `probe_chunk` 91, `assemble_output` 48, `build_index` 42,
`emit_swapped` 7, remainder = phase-A in-memory passes):

1. **Unshelve the parallel probe** (`git stash`: "parallel inner-join probe",
   probe + assemble ≈ 139 ms). Probe threading is already known to work
   (−7.4% on the operator, [[project_join_parallelism]]); what made it
   unmeasurable before was that assemble was the other half — thread both, and
   emit into presized disjoint slices using the two-phase-filter machinery
   (`compute → prefix-sum → presize → gather`), which already solved the
   identical "unknown output cardinality" problem including bit-packed columns
   (`SharedBitWords`). Ordering: morsel-ascending, same contract as everything
   else.
2. **Thread phase A's in-memory passes** — `filter_selection` is already
   parallel elsewhere; `apply_membership_filter` and the two-phase hit loop are
   row-range fan-outs over source-global selections, the same shape as
   `select_bounds_range`.
3. **Partitioned parallel build** only if 1–2 leave `build_index` dominant.
   It's 42 ms suite-wide today — likely stays serial forever (open question 3
   in the pipelined plan). Do not start here.
4. **Membership-vs-conjunct selectivity estimate** for phase A ordering, from
   the footer-stats cost model behind `join_reorder` (q10's abandon is now
   cheap but still wasted work).

Known dead ends, do not redo: column-axis join gather (measured zero — pushdown
already strips the emit to ~1 column); probe-operator Bloom; the two-phase
`left_copy` branch (never runs in PDS-H).

Exit: `join inner` worker help ≥ 1.5×, self ≤ ~200 ms, byte-identical answers,
no 1-core regression.

### W2 — Aggregate serial residue (~552 ms attacked; expect 150–250 ms)

Three known, separate causes:

1. **First-chunk serial discovery under streaming.** The cumulative-rows gate
   fix seeds late-activated partitions, but chunk 0 is still discovered
   serially then seeded (q20's residual). Defer the aggregate's first chunk
   until the partition decision is made — buffer one chunk, decide, replay.
   Named follow-up in the pipelined plan.
2. **High-cardinality group-by** ([[project_high_cardinality_groupby_gap]] —
   8× slower at 5M groups; q13's 150k-customer group-by declines parallelism
   *by design*). The lever is not lowering the gate (measured dead end: q13
   +9.3%, [[project_int_partition_gate_dead_end]]) but making partial state
   cheap enough to qualify: finish the fat-slot diet (`text_value` → side
   string store keyed by slot index; gets slots near 16 B where n_aggs share a
   cache line — the "big remaining win" already identified in
   `runtime-multithreading-plan.md`), and gate on estimated cardinality from
   footer stats/distinct heuristics instead of row count.
3. **The serial gid pass.** It defines first-occurrence group order, so
   parallel assignment needs a reconciliation pass — previously judged more
   expensive than the scan. Re-derive that judgement only *after* 1–2, at
   which point gid assignment may be the last serial piece. A
   morsel-local-gids + ordered-merge-rename scheme preserves first-occurrence
   order deterministically (same argument as the categorical merge); prototype
   standalone before wiring in.

Also here: grouped median/quantile per-row Key grouping
([[project_grouped_median_grouping_gap]], 860 ms residual — off the PDS-H path
but the same machinery).

Exit: aggregate worker help ≥ 2.5×; q13/q20/q10 flat or better at 1 core.

### W3 — The scheduler, second slice (raises the fraction rather than shrinking the serial term)

The pipelined plan's own conclusion stands: the first slice was a source-overlap
win (~4%), not the curve-changer. The named work, in order:

1. **Progress-aware admission/backpressure replacing static reservation.**
   The 2-core cliff (+6.9% SF-4) is a policy bug: one reserved thread halves
   the decode budget while still paying handoff cost. A static one-producer
   gate was measured worse and withdrawn — the admission signal must be
   *progress* (is the ring draining? is the consumer blocked?), not
   configuration.
2. **Run down q18/q22/q17/q20 pipeline regressions** — the loosened
   no-regression bar is explicitly temporary. q22 and q18 look sink-concat
   shaped (large bound intermediates → W4); confirm with the profiler before
   assuming.
3. **Aggregate-output stages.** `PipelinedStageOperator` covers streamable
   join probes; a hash aggregate's *output* is also a stream (emit groups in
   first-occurrence order while the caller sorts/joins them). This is the next
   breaker-output edge and covers the very common `by → order/head` tail.
4. **Shared stage admission + general pipeline decomposition** (source →
   breaker as the unit, blocking ops as barriers), then
5. **Phase 3: retire the island executor** as a special case.

Exit for the slice, unchanged from the pipelined plan: the implied parallel
fraction is the leading metric; the 2-core point must not be a loss; interleaved
1/2/4/8 sweep, not single runs.

### W4 — Kill the sink concat: chunked `let` bindings (~structural; q18/q21/q22)

"That cost is inherent while a `let` binding is one contiguous `Table`" — so
change the invariant. Let a binding hold what `MaterializeOperator` receives: a
chunk list with schema/metadata, materialized to contiguous lazily and *only*
when a consumer actually requires contiguity. Consumers that iterate chunks
(every chunked operator, i.e. the whole executor) read the list directly; the
contiguous view is built at most once on first demand (same pattern as
`LazyTable::cache_`, same single-query lease making it safe).

This is `chunked-execution-plan.md` territory and touches the binding/registry
contract, so scope it as its own design note before coding. It is the only way
the "wins track output size" ceiling lifts; without it every streamed query that
binds a large intermediate pays a copy Polars doesn't.

Sequencing: after W3.1–W3.2 confirm which regressions are actually concat-bound.

### W5 — Small-query overhead floor (defensive)

q02/q13/q16 lose ~3% *downstream* of decode threading
([[project_decode_threading_small_query_tax]] — 18 rounds to pin, footer-bytes
gate a dead end). As W1–W3 add machinery, the tax can only grow. Standing
guard, not a project: every workstream's gate includes the small-query trio at
1 and 8 cores, and any new per-query setup cost (pools, rings, stages) must be
lazily constructed or amortized process-wide.

## 4. Sequencing

```
W1 join (probe+assemble, phase A)   ──┐  independent of scheduler; start now
W2 aggregate (defer-first-chunk,      ├─ interleave; each lands separately
   fat slot, cardinality gate)      ──┘  with its own sweep
W3.1 progress-aware admission       ── next scheduler slice; fixes 2-core cliff
W3.2 q18/q22 rundown                ── decides W4's priority
W4 chunked let bindings             ── design note first; largest structural risk
W3.3+ aggregate-output stages,
      general scheduler, island retirement
W2.3 parallel gid                   ── only if still dominant after the above
```

Rough payoff bookkeeping against the ~450 ms target (SF-2, 8c): W1 ≈ 250–300,
W2 ≈ 150–250, W3.1/W3.2 ≈ 100–200 (removing the named regressions alone), W4
covers the q18/q21/q22 tail. That overshoots the 20%-win threshold with margin,
which is the right posture — history on this codebase says roughly half of any
predicted win survives measurement.

## 5. Validation (unchanged, restated as gates)

- **Answers**: all 22 PDS-H answers byte-identical, every change, every grain,
  serial and parallel. Streaming woke previously-dead cross-chunk paths; the
  window-remainder + serial shapes are the ones the suite doesn't exercise —
  keep the deliberate e2e serial leg.
- **The leading metric is the implied parallel fraction** from an interleaved
  1/2/4/8 pinned sweep (`run_bench.sh` archives + `compare_runs.py` with the
  reference-engine drift check). Wall-time-only wins that don't move the
  fraction belong to the serial-lever workstreams and must say so.
- **1-core geomean is a hard floor.** The single-core lead is the strategy.
- Per-query gates: q02/q13/q16 (small-query tax), q06 (best scaler — the
  canary for scan health), q18/q21/q22 (sink concat), q20 (aggregate gates),
  2-core totals (the cliff).
- Interleaved A/B with a layout control for anything narrower than the suite;
  ±10% box drift means never compare absolute totals across sessions.
- Determinism: partition on row/data properties only, never thread count;
  first-occurrence order tests must use keys whose occurrence order differs
  from sorted order.

## 6. Measured dead ends — do not revisit without new evidence

Collected here so no workstream re-runs them: probe-operator Bloom;
column-axis join gather; the two-phase join `left_copy` branch; lookahead
decode window (+52% RSS, zero wall); static one-producer admission gate;
lowering the int-partition row gate; footer-bytes small-query gate; naive
morsel islands around 1:1 operators; AVX2 filter left-pack; more island
*coverage* (pushdown structurally excludes it — intra-operator and pipeline
parallelism are the only tools that fit PDS-H).

## 7. What winning looks like

SF-2, 8 pinned cores: Ibex total ≤ 1670 ms vs Polars ~2000 ms (≥1.2× faster),
with the 1-core total still ≤ 3800 ms (≥2.1× faster) — i.e. **faster than
Polars at every core count**, which is the headline the single-core work
earned and the multi-core work has so far been giving away. Re-verify at SF-4
and on the AWS 4-physical-core box before publishing any number
([[project_bench_two_tier_framework]]).
