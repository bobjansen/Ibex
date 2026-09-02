# Beating Polars Multi-Core

Status: **proposed.** Written 2026-08-16 on branch `streaming-scan-units`, after
the pipelined-execution Phase 2 slices (streamed scan units, concurrent decode,
source→map pipeline, join-probe handoff) landed. This is the umbrella plan: it
sets the target, decomposes the remaining gap into workstreams with expected
payoffs, and sequences them. It points into
`kernel-pipeline-execution-plan.md`, `runtime-multithreading-plan.md`, and
`join-perf-plan.md` for mechanism. The former pipelined- and chunked-execution
plans were removed as superseded; their remaining work lives in the kernel
pipeline plan and bigger-than-ram plan.
Absorbed `pds.md` (the 2026-08-11 status snapshot this grew out of) on
2026-08-22: §5 and §6 took its still-unique traps and dead ends, §8 keeps
its baseline record.

## 2026-08-27 update — q10 no longer rescans carried group fields

Fresh SF-2 profiling corrected two stale diagnoses. Q10's outer aggregation
does not use the generic seven-key grouper: functional-dependency reduction
has already rewritten it to `keys=1 aggs=7` — `c_custkey`, the revenue sum,
and six `First` fields carrying the descriptive group columns. The apparent
37ms join build at node 31 is also inclusive of recursively building the join
subtree, not a 37ms hash-table build over `nation`.

The real aggregate waste was simpler. Integer-key discovery already knows the
first source row of every new group, but each non-null `First` aggregate then
rescanned all 229,871 input rows and probed the gid-indexed slot array. Q10 did
that six times. The integer path now records first rows while assigning gids,
gathers those fields once per new group, and skips them in the ordinary
accumulation pass. Large first-row gathers split disjoint gid ranges across the
worker pool; nullable `First` stays on the old null-skipping scan permanently,
including when validity disappears in a later chunk.

Same-binary A/B via `IBEX_DISABLE_DISCOVERY_FIRST=1`, pinned to cores 0-7:

- six 25-iteration paired blocks: **-10.9% median**, 6/6 wins;
- eight independent 15-iteration paired blocks: **-10.5% median**, 8/8 wins
  (two-sided sign p=0.0078);
- four 15-iteration one-core blocks: **-2.5% median**, 4/4 wins;
- control queries q02/q03/q18: +1.4% / +1.4% / +0.6% medians, inconsistent in
  direction and below the practical floor.

The required 88-case engine gate (`HEAD` vs worktree, 15 interleaved repeats)
classified every individual result as noise; aggregate Ibex time was 0.77%
lower in the worktree.

A forced fact-first join order saved only about 2-3ms and is not the main
lever. The remaining q10 gap is upstream decode/join materialization, not
mixed-key grouping; do not lower the generic partition gate or rebuild the
discarded mixed-key anchor for this query.

## 2026-08-27 update — q22 is effectively closed

The SF-2 run at `92e47974` made q22 the largest matched Polars-streaming gap:
Ibex averaged 74.312ms (71.037ms minimum) at eight cores versus Polars'
22.837ms (20.917ms minimum), a 3.25x ratio. It was not one bottleneck but three
stacked costs that the canonical query shape exposed:

1. The planner now proves
   `filter is_null(right_marker)` over a left equijoin is an anti join when the
   marker is non-null on every match and no right output is demanded. It also
   removes a direct right-side `distinct` from semi/anti joins, because
   duplicate right rows cannot change existence. Value-lineage follows bare
   column copies through `update`/`project`/`distinct`, which proves q22's
   copied marker is the non-null join key without making a nullable-column
   assumption.
2. Literal-bound `substring` joins interpolation in `DirectStringPlan`:
   UTF-8 byte counts, prefix assignment, and disjoint writes replace per-row
   string allocation and use the existing parallel field-window protocol.
3. Swapped integer semi/anti build uses a dense candidate/hit table when the
   smaller key range is at most four slots per row and no more than 2 Mi slots.
   Workers scan the large side into private hit tables, OR them deterministically,
   and retain the dense representation for the buffered-left probe.

The final HEAD-vs-worktree measurement used 15 position-balanced blocks and
105 internally timed executions per side on cores 0-7. q22's median fell from
73.557ms to 25.058ms; the median paired ratio was **0.3408 (-65.92%)**, with
15/15 wins and two-sided exact Wilcoxon p=0.000061. All 22 answers match. The
final 15-repeat interleaved `compare_ibex_git.sh` no-regression gate measured
-0.37% across the 85 execution benchmarks and a 1.001 geometric speedup; every
individual verdict was noise.

A final standalone core sweep (`warmup=3`, `iters=25`) measured:

| cores | avg ms | min ms | speedup vs 1c |
|---:|---:|---:|---:|
| 1 | 51.76 | 50.49 | 1.00x |
| 2 | 36.14 | 33.26 | 1.43x |
| 4 | 28.61 | 24.91 | 1.81x |
| 8 | 23.93 | 22.40 | 2.16x |

Against the matched Polars artifact this leaves only a 4.8% average q22 gap
at eight cores, inside the run-to-run comparison floor rather than a multi-day
workstream. At one core Ibex is 28.8% faster than Polars streaming (51.76ms
versus 72.722ms). The old q22 scaling was 1.58x; the final path reaches 2.16x.

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

### 1a. Where the fraction actually is (measured 2026-08-16, post-W1/W1b/W2)

Interleaved sweep on one binary at `68af53e`, SF-2, pinned, min-of-3. Absolute
totals are **not** comparable to the table above — different session, and box
drift is ±10% — but the speedup column is within-session and therefore is:

| cores | total | speedup vs own 1c | implied parallel fraction |
|---|---:|---:|---:|
| 1 | 3944 ms | 1.000× | — |
| 2 | 3164 ms | 1.247× | **39.6%** |
| 3 | 2674 ms | 1.475× | **48.3%** |
| 4 | 2278 ms | 1.732× | **56.4%** |
| 8 | 1820 ms | 2.214× | **62.7%** |

Two findings, and the second is the important one.

**At 8 cores the target band is reached.** 62.7% is inside the 60–65% goal, and
the 2.214× speedup clears the 2.10× this plan set for "beat by 10%". That does
not by itself say we beat Polars — that claim needs Polars re-measured in the
same session, which has not been done and must be before anything is published.

**The Amdahl premise no longer holds, and its failure IS the 2-core cliff.**
§1 says "Amdahl fit holds at every core count"; it does not any more. A real
Amdahl program yields the same `p` at every `n`. Ours climbs monotonically —
39.6 → 48.3 → 56.4 → 62.7 — which is the signature of parallel work being
*declined* at low core counts rather than executed slowly. There are nine
`workers < 2 → decline` gates across `chunked.cpp`/`join.cpp`; with anything
reserving a producer thread, a 2-core run leaves a budget of 1 and every one of
them falls back to its serial loop. Less of the program is *eligible* at 2 cores
than at 8. (Parallel still beats serial at every core count — 2c parallel/serial
is 0.847 — so nothing is actively backfiring; the work is simply not being
offered.)

The prize, sized against the 8-core fraction: if 2 cores were as eligible as 8
they would run ≈2707 ms rather than 3164 ms (**−457 ms**), and 4 cores ≈2090 ms
rather than 2278 ms (**−188 ms**). That is the largest single number left on the
board, and it is W3.1's.

**Confirmed by experiment, and the mechanism is now certain.** Pin to the same
two physical cores and vary only the thread BUDGET:

| pinned cores | thread budget | total | speedup vs 1c | implied p |
|---|---|---:|---:|---:|
| 1 | 1 | 3778 ms | 1.000× | — |
| 2 | 2 | 3149 ms | 1.200× | 33.3% |
| 2 | 3 | 2816 ms | 1.342× | 50.9% |
| 2 | **4** | **2715 ms** | **1.392×** | **56.3%** |
| 2 | 8 | 2744 ms | 1.377× | 54.8% |

No extra hardware — 23 points of parallel fraction appear purely from raising
the budget, and **434 ms of the predicted 457 ms is recovered by that alone**.
So the 2-core deficit is not the scheduler running slowly and not a hardware
limit: at a budget of 2 the program simply stops *offering* the work. The
budget-8-on-2-cores row also shows the other edge — oversubscription starts
costing again, so the fix is not "always oversubscribe" but "never let the
effective budget collapse toward 1".

**W3.1 DONE**, though not in the shape this plan predicted — see below.

**1-core no-regression gate, verified for this session's three commits**
(`a0dd4c1`, `315504e`, `68af53e` vs `d1ca618`): geomean **0.9965**, no query
worse than +2%. The single-core lead is intact.

The same thing in absolute terms, which is how the workstreams are ranked: at
8 cores the total is ≈ serial + parallel/8 ≈ 2116 + 208 ms. **The fight is over
~2.1 s of calling-thread (serial) time.** Beating Polars by 10% means removing
or parallelizing ~450 ms of it. The operator table below says exactly where it
is, so every workstream is stated as "serial ms attacked".

## 2. Where the serial time is

Calling-thread self-ms summed over the suite at 8 cores, with worker help drawn
(`pool_work / self`), from the archived pipelined-execution measurement study
(git history; post-semi-join numbers):

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

**Status 2026-08-16: probed and largely re-scoped.** The three causes below were
written from the operator table, which reports *self time* — and self time on an
operator that blocks on `batch.wait()` counts the wait. Phase timers inside the
worst case (q18's `sum(l_quantity) by l_orderkey`: 12M rows, 3M groups, the
single largest aggregate self-time in the suite at 213 ms) say the residue is
not where the table implies:

| phase | ms | serial? |
|---|---:|---|
| pass-3 partition probe (`try_discover_partitioned`) | 80 | no — 8 workers |
| slot-array fill (`size_group_arrays`) | 44 | **yes** |
| first-occurrence ordered merge | 26 | **yes** |
| key-array growth (`resize_keys`) | 12 | **yes** |
| accumulate (`accumulate_gids`) | 10 | yes, but tiny |

So: the accumulate — the thing `try_accumulate_parallel` declines on and the
thing cause 2 is about — is 5% of the operator, not the lever. The gid probe,
the largest single phase, is *already* threaded. What is left serial is
**memory-shaped work: growing and zeroing per-group arrays** (56 of the 172 ms).

Landed from this: `SlotArray` grows uninitialized and the tail fill is one
`memset`, fanned out across workers. Honest result — **cold** (`ibex q.ibex`,
what a script does) q18 −4.3%; **warm** (the PDS-H harness reuses one process,
so the allocator returns already-faulted pages) the suite geomean does not move,
two interleaved runs at 0.9975 and 1.0014. The cost was first-touch page faults,
and page faults only exist the first time.

**Measured dead end — gid-sharded accumulate.** Reusing the discovery scatter
(`scatter_rows_` + partition bounds) to accumulate by *group* instead of by
*row* needs no private slot arrays at all, sidesteps the 32 MB partial-state
budget that makes cause 2 decline, and is byte-identical to serial (rows stay
ascending within a partition, so float sums associate the same way and
First/Last pick the same row). It is still a loss: each worker's rows are spread
through the whole chunk, so all 8 stream the entire gid and value columns —
8× the read traffic. q18's accumulate went 10 ms → 17 ms. Do not redo it
without a shape where accumulate is actually dominant.

**What to do instead**, in order of measured size:
- The ordered merge (26 ms) and the array growth (56 ms) are the serial half.
  Growth is `realloc`/`mremap` already; the remaining cost is the kernel's, so
  the lever is fewer/larger faults (huge pages via an aligned `mmap` +
  `MADV_HUGEPAGE` backing for `SlotArray`) rather than more threads.
- Re-derive cause 3 (parallel gid) against 80 ms of *already-parallel* probe,
  not against a serial pass — the premise it was written under is gone.
- Cause 2's fat-slot diet is **done, and finished past the target**:
  `AggSlotCore` is now **16 B** (static_asserted), the size the multithreading
  plan called the big remaining win. It got there by deleting state that was not
  information: `func`/`kind` were a per-GROUP copy of something that varies per
  AGGREGATE (every reader already passed `plan_[agg_i]` in), and `has_value` is
  `count != 0` — which `agg_combine`'s Stddev arm had already been relying on.
  Wall time is a wash warm (three interleaved runs: 0.987, 1.010, 0.997) and
  slightly positive cold (q20 −4%, q16 −5%); **the measurable win is memory** —
  q18 peak RSS 593 MB → 535 MB (−9.9%), q20 −5%. It also cuts
  `per_morsel_bytes` by a third, which is the gate deciding whether a group-by
  can afford replicated partial state at all, so it widens the path cause 2 is
  about rather than speeding up the one query measured.

The original three, kept for the parts still standing:

1. **First-chunk serial discovery under streaming — measured dead end.** The
   cumulative-rows gate fix seeds late-activated partitions, but chunk 0 is
   still discovered serially then seeded (q20's residual). Buffering chunk zero,
   pulling chunk one to decide admission, then replaying both in order did make
   the first q20 aggregate take the partitioned path. It did not pay: SF-8,
   8 cores, interleaved 16-pair A/B pinned to CPUs 0-7 measured 687.6 ms →
   675.5 ms minimum, a −1.0% paired effect (p=0.098; byte-identical), below
   the 2% practical floor. Removed. Do not retry this exact one-chunk lookahead
   without evidence that it can avoid, rather than merely move, serial work.
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

### W3.1 — Decode gets its own thread budget (DONE)

The plan guessed the 2-core deficit was the scheduler reserving a producer
thread, and §1a guessed it was the nine `workers < 2 → decline` gates. **Both
guesses were wrong.** The gates do not decline at 2 cores — they get
`workers = 2` and pass — and a first attempt at cutting finer probe units with a
work-stealing cursor measured 0.9972 at 8 cores, a wash, and was reverted.

Splitting the 2-core gain per query found it instead: it is concentrated in
scan-dominated queries (q06 −34% — the plan's own scan canary — q19 −30%,
q12 −28%, q14 −24%, q15 −20%) while compute-heavy ones LOSE (q01 +4.6%,
q17 +3.2%). That is memory-stall hiding in Parquet decode.
`scan_pipeline_worker_count` clamped decode to `pool.size()`, so a 2-thread pool
starved it.

The fix is to stop expressing two different budgets with one number. The pool is
now sized for DECODE — `min(2·cores, max(cores, saturation))` — while
`configure_parallel_from_env` pins `parallel_threads` to the core count, so every
compute gate still sizes itself to hardware. A flat multiplier is wrong at both
ends and the policy has to bend:

| cores | 1× | 2× | 3× |
|---|---:|---:|---:|
| 1 | **3787** | 3919 | 3947 |
| 2 | 3247 | **2832** | 2868 |
| 4 | 2320 | **2165** | 2189 |
| 8 | **1809** | 1971 | 1959 |

At one core there is nothing to overlap and a second thread only flips the
`pool.size() < 2` guards on; at eight the memory system is saturated and the
extra threads are contention.

**Result** (interleaved, SF-2, stamped scale factor, sequential with nothing else
on the box):

| cores | new | old | ratio | implied p before → after |
|---|---:|---:|---:|---|
| 1 | 3802 ms | 3786 ms | 1.004 | — |
| 2 | 2818 ms | 3165 ms | **0.890** | 32.8% → **51.7%** |
| 4 | 2170 ms | 2250 ms | **0.964** | 54.1% → **57.2%** |
| 8 | 1894 ms | 1891 ms | 1.002 | 57.2% → **57.3%** |

The core-count ramp — the thing that broke the Amdahl premise — is largely gone.
The 1-core hard gate and the 8-core headline configuration are both untouched by
design. Predicted −12%/−7%/neutral/neutral in advance; got −11.0%/−3.6%/+0.4%/
+0.2%, so 4 cores came in at about half the predicted gain.

`IBEX_THREADS` is renamed to `IBEX_CORES` (compute budget), joined by
`IBEX_DECODE_THREADS` (absolute pool size) and `IBEX_DECODE_SATURATION` (default
8). The old name conflated a core count with a pool size in one number, which is
precisely why "more decode, same compute" was inexpressible. Setting the old
name now warns rather than silently falling back to `hardware_concurrency()` —
that silent fallback destroyed a measurement here.

**The saturation constant is a property of this box's memory system, not of the
code.** Four points on one machine is the first row of the table a heuristic
would come from, not the heuristic. Sweep `IBEX_DECODE_SATURATION` on the AWS
4-physical-core box before publishing any number
([[project_bench_two_tier_framework]]).

### W3 — The scheduler, second slice (raises the fraction rather than shrinking the serial term)

The pipelined plan's own conclusion stands: the first slice was a source-overlap
win (~4%), not the curve-changer. The named work, in order:

1. **Progress-aware admission/backpressure replacing static reservation.**
   The 2-core cliff (+6.9% SF-4) is a policy bug: one reserved thread halves
   the decode budget while still paying handoff cost. A static one-producer
   gate was measured worse and withdrawn — the admission signal must be
   *progress* (is the ring draining? is the consumer blocked?), not
   configuration.

   **Sharpened by the §1a sweep, and now the top item on the board.** The
   deficit is not a dip at one core count, it is a monotonic ramp in how much
   work is *eligible*: 39.6% of the program parallelises at 2 cores against
   62.7% at 8. Before touching the scheduler's admission policy, audit the nine
   `workers < 2 → decline` gates — a budget of 1 makes each of them fall back
   to a serial loop wholesale, so at low core counts they, not the scheduler,
   may be most of the ramp. Cheap experiment first: force the budget to
   `max(2, …)` at 2 cores and re-run the sweep. If the ramp flattens, the fix
   is the gates; if it does not, it is the reservation. Worth ≈457 ms at 2
   cores and ≈188 ms at 4.
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

This touches the binding/registry contract (formerly
`chunked-execution-plan.md` territory; that file is in git history), so
scope it as its own design note before coding. It is the only way
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
W1 join (probe+assemble, phase A)   ── DONE (294230c, e06e1d7): SF-2 geomean 0.943
W2 aggregate                        ── probed; the residue is array growth, not
                                       accumulate. Slot fill landed (cold-only).
                                       Remaining levers are small; see W2 above.
W1b join_table_impl probe scan      ── DONE (below): geomean 0.973 / 0.981
W3.1 progress-aware admission       ── next scheduler slice; fixes 2-core cliff
W3.2 q18/q22 rundown                ── decides W4's priority
W4 chunked let bindings             ── design note first; largest structural risk
W3.3+ aggregate-output stages,
      explicit breaker phases and general scheduler
W2.3 parallel gid                   ── only if still dominant after the above
```

### W1b — `join_table_impl`'s probe scan (DONE)

Suite operator table re-measured 2026-08-16 (SF-2, 8 pinned cores,
`IBEX_PROFILE_OPERATORS`, one process per query): `join inner` reads as 574 ms
self / 0.42× help, the largest unparallelized operator. **Most of that is not
join work.** Timing inside the operator, q09's 169 ms splits as ~76 ms of
deferred-probe phase A (a fused decode charged to the join), ~23 ms of actual
probe and assemble, and ~62 ms in `join_table_impl`. Same caution as W2: the
table over-attributes.

Timing `join_table_impl` itself across the suite gives the real target — and
it is not "multi-key", which is how W1's follow-up note framed it:

| query | keys | total ms | probe scan | materialize |
|---|---|---:|---:|---:|
| q13 | 1, but a **LEFT** join | 139 | **120** | 20 |
| q09 | 2 | 62 | 21 | 20 |
| q05 | 2 | 35 | 33 | 2 |
| q20 | 2 | 18 | 18 | 0.2 |

`ChunkedInnerJoinOperator` handles inner joins with one key; everything else
falls to `join_table_impl`, whose probe scan was wholly serial. The single
biggest item is q13's LEFT join, not a multi-key one. Both
`build_indices_from_left_scan` and `build_indices_from_right_scan` are shared by
every path in that function, so threading those two covers left/right/outer and
multi-key at once.

Landed: contiguous ranges into per-worker `(li, ri)` parts concatenated in range
order — byte-identical to serial, the same contract as W1's `probe_ranges_parallel`
— under the existing `IBEX_JOIN_PROBE` gate, with a `parallel_probes` counter and
a both-direction test covering the two-key and left-join shapes. The one
cross-range write in each direction (`left_matched_flags` / `right_matched_flags`)
goes through `std::atomic_ref` with a relaxed store. Every lookup closure was
checked read-only first (`RowKeyIndex::find` is const; the fast paths probe a
finished `robin_hood` map through const references).

**Isolated A/B** (same binary pair, interleaved ABBA×3, 8 pinned cores, SF-2):
geomean **0.9725** and **0.9810** on two runs. q13 −19%, q05 −20%, q20 −10%,
q09 −6%, q16 −4%, q03 −3%. q22 is +8–11% and reproduces, but it is **not this
change**: q22 never calls the fan-out (its join is a 1-row cross join, 0.71 ms
of `join_table_impl` total), and the regression is still there with
`IBEX_JOIN_PROBE=0` on the same binary — it is code layout in a hot TU, the
per-query noise floor [[project_cow_detach_hot_loop]] documents.

Still serial after this: `materialize` (~42 ms suite-wide, the gather), and the
semi/anti branch of the right scan.

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
  ±10% box drift means never compare absolute totals across sessions. Drift
  quantified once and worth keeping (from the pds.md baseline): a single round
  resolves nothing under ~10%; three rounds resolve ~5%; ~5% effects need 5+
  interleaved rounds; ~3% needed 18. Several apparent findings evaporated at
  higher round counts.
- **The data symlink flips per scale factor.**
  `benchmarking/data/tpch/parquet` is a symlink the bench script retargets;
  pin explicit `parquet_sf<N>` paths in any A/B or a stale symlink silently
  doubles every number.
- **The serial-vs-parallel answer diff has exactly three legitimate
  exceptions** — q01, q09, q15 differ in the last ulp from the parallel
  group-by float reduction (deterministic across thread counts, unlike the
  serial summation order). Every other output must be byte-identical; a diff
  showing a fourth file is a bug, not drift.
- **Allocation-shaped changes must also be measured cold.** The harness runs 22
  queries in one warm process, so the allocator hands back already-faulted pages
  and first-touch cost is invisible to it — W2's slot fill is −4.3% on a cold
  `ibex q18.ibex` and exactly 0 on the suite. Both numbers are real; report
  both, and never quote only the flattering one.
- **`self_ms` is not serial time.** An operator that fans out and joins spends
  its wait inside `next()`, and the profile counts it. Any workstream sized off
  the operator table must confirm with phase timers before committing, which is
  what re-scoped W2.
- Determinism: partition on row/data properties only, never thread count;
  first-occurrence order tests must use keys whose occurrence order differs
  from sorted order.

## 6. Measured dead ends — do not revisit without new evidence

Collected here so no workstream re-runs them: probe-operator Bloom;
column-axis join gather; gid-sharded aggregate accumulate over the discovery
scatter (8× read amplification, q18 10 ms → 17 ms — see W2);
the two-phase join `left_copy` branch; lookahead
decode window (+52% RSS, zero wall); static one-producer admission gate;
lowering the int-partition row gate; footer-bytes small-query gate; naive
morsel islands around 1:1 operators; AVX2 filter left-pack; more island
*coverage* (pushdown structurally excludes it — intra-operator and pipeline
parallelism are the only tools that fit PDS-H). Two more, carried from the
pds.md baseline because they are recorded nowhere else:

- **Categorical code hashing in the generic group-by path.** Hashing and
  comparing Categorical keys by dictionary code bought only 8% and produced
  **wrong answers on q16**: its group keys come from a join, so different
  chunks carry different dictionaries and codes are not comparable. The
  single-Cat fast path survives only on "dictionaries only grow and never
  reorder", which stops holding once a join sits between source and group-by.
  A sound version needs per-column dictionary-pointer tracking plus an index
  rebuild on every new dictionary — real machinery for 8%. Related trap:
  q10's expensive key columns (`c_name`, `c_address`, `c_phone`, `c_comment`)
  are `Column<std::string>`, NOT Categorical — their Parquet pages fall back
  to PLAIN when the dictionary overflows. Any plan assuming TPC-H strings
  are Categorical is wrong.
- **Functional-dependency group-key reduction** — built, passed everything,
  reverted because the discovery point does not cover q10 (customer is never
  the build side, so `c_custkey`'s uniqueness is never observed). Full
  narrative and the two bugs found while building it: §8.4.
- **Selected-row gather straight out of the Parquet page buffer** (2026-09-02).
  For a selection too dense for `Skip` and too sparse for the dense decode to
  be honest — q14's 1.25% — copy only the selected values out of PLAIN pages and
  look up only the selected codes in dictionary pages. Correct, byte-identical,
  and it did cut decode bytes; **~20% slower at both 1 and 8 cores**, and
  prefetching changed nothing. The reason generalises: the dense path's
  intermediate is a 64Ki-row scratch batch — 512 KB, **L2-resident** — so the
  copy it replaces never touched DRAM. **Do not size a decode change off
  `__memmove` percentages without first asking whether the buffer fits in
  cache.** See §8.6.
- **Moving a computed column across a join without a cost model** (2026-09-02).
  `push_computed_columns_into_joins`: hoist an `Update`'s maximal single-side
  sub-expressions to their own side, then use `join_output_demand` to drop the
  source columns nothing above still reads (that second phase is what makes it
  narrow the join rather than widen it). Byte-identical on all 22, full suite
  green, and worth **−9 to −15%** where post-join rows × payload width is large
  — but **q12 +40%**, because the same push puts
  `Int64(o_orderpriority == …)` on 12M `orders` rows to save gathering 1.3M.
  The mechanism is real in both directions and the pass cannot tell them apart:
  it needs join-output cardinality against each side's post-filter row count.
  A cost gate is the prerequisite, not a refinement — same missing estimate as
  the scan-fusion gate in `query-shape-conformance-plan.md`. See §8.6.

## 7. What winning looks like

SF-2, 8 pinned cores: Ibex total ≤ 1670 ms vs Polars ~2000 ms (≥1.2× faster),
with the 1-core total still ≤ 3800 ms (≥2.1× faster) — i.e. **faster than
Polars at every core count**, which is the headline the single-core work
earned and the multi-core work has so far been giving away. Re-verify at SF-4
and on the AWS 4-physical-core box before publishing any number
([[project_bench_two_tier_framework]]).

## 8. Appendix — the PDS-H baseline record (was `pds.md`, 2026-08-11)

`pds.md` was the status snapshot taken at `f916e13`, five days before this
plan; the plan's §1–§5 supersede its analysis. Kept here, compressed, are the
things that exist nowhere else: the full per-query standings, the provenance
of its six open levers, and two findings with design detail worth preserving.
The original file is in git history at the commit that absorbed it.

### 8.1 Standings, 2026-08-11 (min of 5, 8 pinned cores, whole-script)

SF-1:

| query | ibex | ibex-st | polars | polars-st | ibex/polars | st/st |
|---|---:|---:|---:|---:|---:|---:|
| q01 | 70.8 | 202.4 | 68.7 | 296.5 | 1.03x | 0.68x |
| q02 | 27.8 | 27.5 | 39.0 | 56.7 | 0.71x | 0.48x |
| q03 | 49.6 | 71.5 | 31.4 | 86.6 | 1.58x | 0.82x |
| q04 | 69.8 | 93.6 | 31.9 | 87.6 | 2.19x | 1.07x |
| q05 | 73.8 | 122.9 | 53.0 | 144.6 | 1.39x | 0.85x |
| q06 | 15.5 | 43.7 | 12.7 | 28.2 | 1.23x | 1.55x |
| q07 | 47.4 | 80.7 | 110.1 | 265.8 | 0.43x | 0.30x |
| q08 | 32.3 | 73.1 | 43.5 | 96.4 | 0.74x | 0.76x |
| q09 | 98.9 | 168.1 | 83.0 | 225.8 | 1.19x | 0.74x |
| q10 | 92.3 | 113.5 | 43.3 | 92.3 | 2.13x | 1.23x |
| q11 | 24.1 | 24.1 | 22.5 | 22.8 | 1.07x | 1.06x |
| q12 | 42.3 | 70.7 | 37.4 | 124.9 | 1.13x | 0.57x |
| q13 | 193.9 | 185.7 | 108.5 | 209.4 | 1.79x | 0.89x |
| q14 | 18.8 | 38.5 | 10.7 | 29.6 | 1.75x | 1.30x |
| q15 | 31.0 | 47.7 | 23.0 | 56.3 | 1.35x | 0.85x |
| q16 | 61.4 | 54.2 | 26.2 | 33.3 | 2.34x | 1.63x |
| q17 | 17.8 | 41.6 | 81.9 | 350.0 | 0.22x | 0.12x |
| q18 | 184.7 | 192.0 | 176.2 | 641.0 | 1.05x | 0.30x |
| q19 | 35.8 | 59.6 | 16.0 | 50.4 | 2.23x | 1.18x |
| q20 | 114.1 | 131.5 | 50.2 | 179.8 | 2.27x | 0.73x |
| q21 | 194.6 | 235.0 | 285.9 | 1020 | 0.68x | 0.23x |
| q22 | 33.1 | 36.5 | 26.2 | 41.4 | 1.26x | 0.88x |
| **total** | **1.53 s** | 2.11 s | 1.38 s | 4.14 s | **1.11x** | 0.51x |
| **geomean** | 52.4 | 77.6 | 44.2 | 109.8 | **1.19x** | **0.71x** |

SF-2 (same protocol): total 3.08 s / 2.59 s (1.19x), geomean 1.30x, st/st
0.68x; MT gain ibex 1.60×, polars 3.08×. The gap concentrated in q10
(2.13×→2.95×), q16 (2.34×→3.28×), q13 (1.79×→2.40×). Comfortably ahead: q17,
q07, q21, q02, q08 — the dynamic-filter-pushdown and semi-join queries.

The framing that survives from these tables: single-threaded Ibex was ahead
(st/st 0.71×/0.68×) and the whole deficit was scaling — which is why §1's
arithmetic (don't match Polars' scaling, raise the parallel fraction from ~44%
to ~60–65%) is the strategy.

### 8.2 The 2026-08-11 round — decode parallelism

Seven commits, cumulative suite effect ≈ −3.7% then −14%; ibex MT gain
1.01× → 1.48×/1.60×: `f86465c` parallel Parquet decode, `ac5713c` parallel
group-by accumulate, `c838a35` range-split fused-bounds filter, `ec0bf8b`
ExecutionContext-driven decoder, `ec62911` row-range gather, `ac60c6d`
row-group key-filter scan (q17 −28%), `f916e13` row-group fixed-width decode.

Two lessons that generalized: **our own pushdown competes with the
parallelizer** (projection pushdown and late materialization strip the join
emit to one column so a column-axis gather has nothing to split; decode
fusion leaves no operator for the island planner to thread), and **queue
indivisible tasks first** (q01's whole-column dictionary decodes queued
behind ~30 shard tasks set the finish time; reordering turned 6.5% slower
into 13.7% faster).

### 8.3 Where the six 2026-08-11 levers went

| lever | disposition |
|---|---|
| 1. Parallel high-cardinality group discovery | DONE for integer keys (q18 −20%, q20 −12.5%, q13 −10%); the PairIntKey partition-owned path landed 2026-08-21 (`4fedf2a4`, q20 −14.9…−18.1% at 8c) |
| 2. Fuse q13's join into aggregation | landed 2026-08-18 (`6ed55728`, join/count fusion; profiled 149→126 ms, whole-script median ~104 ms) |
| 3. q10 mixed-key fast paths / redundant group keys | the FD-reduction half was built and reverted (§8.4); the mixed-key coverage notes live in `parallelism-overview.md`'s aggregate sections |
| 4. q16 `distinct`, q04 semi-join | q04's serial swapped-build landed (`7fde36d`, q04 53.5→~32 ms); q16's composite-categorical distinct remains the I3 gap in `parallelism-overview.md` |
| 5. Multithreaded scheduling across operators | W3 above |
| 6. Small-query decode-threading tax | W5 above |

Also from that round, now tracked elsewhere: the filter-only string column is
never materialized (`2975559`, q13 −17.8%; design and traps in
`decode-fusion-plan.md`), and the operator-profile mechanics
(`serial_fraction`, `amdahl_ceiling`, occupancy) are documented in
`MEASURING.md`.

### 8.4 Functional-dependency group-key reduction — built, reverted

The full mechanism was implemented — `FunctionalDependency` on
`TableProperties` with a transitive closure, a `derive()` rule retiring a
dependency when either end is dropped/renamed/overwritten, join-side emission
from the existing `build_unique_` flag, and aggregate-side key reduction
rewriting dropped keys as `first()` aggregates. It compiled, passed 22/22
answers and the full test suite.

Reverted because **the discovery point does not cover q10.** Every
`build_index` call shows the three joins building on `nation(c_nationkey)`,
`orders(c_custkey)`, and `cust_orders(o_orderkey)` — `customer` is never the
build side (the planner correctly indexes the smaller filtered `orders`,
57,069 rows, and probes with `customer`, 150,000), so `customer.c_custkey`'s
uniqueness is never observed. The discoverable dependency
(`o_orderkey -> everything`) has a determinant that is not a group key, and
`o_orderkey -> c_name` does not yield `c_custkey -> c_name`.

Two bugs found while building it, worth knowing if it is rebuilt:

- The indexed side's key column is usually **not in the join output** — an
  equi-join emits one copy, from the other side — so the determinant must be
  looked up under the counterpart key's name.
- Nulls break the claim: two rows null in the determinant need not come from
  the same source row, so a dependency may only be claimed for a column with
  no validity bitmap. An inner join's own key is safe (a null key matches
  nothing).

To make it fire, uniqueness must be observed on the **probe** side (a
duplicate-detection set over already-hashed probe keys — ~150k inserts of
speculative work) or at the scan. A design fork needing its own measurement.

### 8.5 Mapped join keys were a silent deoptimization (`465d0d3`)

`a join b on { id_1 = id_2 }` was answer-equivalent to the rename-to-match
idiom and slower: filter pushdown, semi/anti pushdown (q18's essential
rewrite), the reorder cost model (q03 −23%, q02 −29%), join ordering, and
deferrable-probe registration all key on bare column names, so the mapped
spelling lost every one. Rewriting q03 to it cost **+13.4% min / +16.4%
median**. The passes were not wrong; nothing normalized their input.
`ir::normalize_mapped_join_keys` now renames the right side's key to the
left's wherever the fold is unobservable (Inner/Left/Semi/Anti, right key
unread above, no cross-side name collision), from `push_filters_into_joins`
which precedes every other join pass. Residual, documented at each gate:
Right and Outer joins, and any join whose right key column really is read
above it — those need an order-restoring Project or an equivalence-class key
model in `join_reorder`/`join_order`.

### 8.6 q14 and the ceiling that wasn't — measured 2026-09-02 (SF-8, 8 cores)

Recorded because two plausible framings of q14 were both wrong, in opposite
directions, and the corrections are reusable.

q14 is at **parity single-threaded** (234 ms vs polars-stream 222 ms) and loses
2.3× multi-threaded. Its CPU inflates with cores — 279 ms at 1c, 425 at 4c, 537
at 8c on physical P-cores — and eight *independent* single-threaded copies each
take 632 ms instead of 285 ms. So it contends for a shared resource and no
scheduler, ring or fan-out change will move it.

**But that resource is not streaming memory bandwidth.** Measured ceiling on the
dev box (`taskset -c 0,2,…,14`):

```
                 1 thread    4 threads    8 threads
DRAM read         15.8 GB/s   34.4 GB/s   38.6 GB/s
pread page cache  14.3 GB/s   42.5 GB/s   44.5 GB/s
```

Eight copies move 8 × 903 MB in 632 ms = **11.4 GB/s, a quarter of peak**. The
contention is L3 capacity plus a non-streaming access pattern (the 1.6M-entry
join hash probe, the 600k-row gathers), not the DRAM ceiling.

q14 reads **903 MB** (counted with `strace`: 194 large reads, so nothing is read
twice), for a predicate no page statistic can prune — every SF-8 row group's
`l_shipdate` min/max spans 1992-01-02…1998-12-01. Against the 8-thread ceiling
that is a ~20.5 ms pure-I/O floor:

| | wall | effective | vs floor |
|---|---|---|---|
| floor (just read the bytes) | 20.5 ms | 44 GB/s | 1.0× |
| polars-stream | 41.3 ms | 21.9 GB/s | **2.0×** |
| ibex | ~104 ms | 8.7 GB/s | **4.5×** |

**So polars is at about half the achievable bandwidth, not at a limit**, and
ibex has ~4.5× of headroom to the floor and ~2.4× to polars. Neither "q14 is
bandwidth-bound" nor "the fix is to read fewer bytes" survives these numbers;
the lever is the compute and access pattern over the 600k surviving rows.

Two corrections to standing beliefs came out of the same session:

- **A join's build side is not inherently serial.** The scan under it does get
  pool tasks. What ran serially in the q14 experiment was the `Update` above it,
  because one field shape fell off `plan_direct_field` and that route is
  all-or-nothing per node (`4ac93b33`). Check which node is serial before
  attributing it to the build.
- **File-layout facts worth not re-deriving.** SF-8 `lineitem`, 46 row groups of
  1Mi rows, uncompressed. Bytes/row gives the encoding: `l_partkey` 8.25 and
  `l_extendedprice` 8.23 are **PLAIN** (their dictionaries outgrew the page
  limit); `l_discount` 0.50 and `l_shipdate` 1.51 are dictionary. Chunks are
  *mixed* — `l_partkey` has 1 dictionary page, 7 PLAIN data pages and 1
  RLE_DICTIONARY data page — so "is this column dictionary-encoded" is a
  per-page question, not a per-column one.
