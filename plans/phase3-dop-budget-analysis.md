# Phase 3 item 2 — DOP and memory budgets: analysis before any code

Status: **analysis only, nothing built.** Written 2026-08-24 at `18623638`,
after Phase 3 items 1 and 4 closed, because item 2 is the one remaining Phase 3
item whose premise deserves checking before it is implemented rather than after.

The item as written in `kernel-pipeline-execution-plan.md`:

> 2. Represent pipeline DOP and memory budget in `ExecutionContext` child
>    budgets; an inner kernel observes the allocation and cannot seize the full
>    pool.

This document argues that **the DOP half is a precondition for work that is not
justified yet, not a win on its own, and the memory half has no consumer at
all** — and that there is a small, real piece of the item worth doing now for
reasons that have nothing to do with performance. It ends with the cheapest
experiment that would change this conclusion.

## 1. What the tree actually does today

Five facts, read from the tree rather than from this plan's own history.

**A single process pool, sized for decode.** `process_worker_pool()` is
constructed with `decode_thread_count()` (`worker_pool.cpp:615`), which is
`min(2*cores, max(cores, IBEX_DECODE_SATURATION))` — deliberately
oversubscribed, because decode is memory-latency bound and compute is not.

**A single flat compute budget already exists.** `ExecutionContext::
parallel_threads` is that budget, pinned to `compute_thread_count()` by
`configure_parallel_from_env`. It is read at ~30 sites, almost all spelling the
same idiom by hand:

```cpp
const std::size_t budget =
    exec.parallel_threads != 0 ? exec.parallel_threads : pool.size();
```

So "represent pipeline DOP in `ExecutionContext`" is, at the flat level,
already done. The word doing the work in item 2 is **child**.

**Nesting is a crash, by design.** `WorkerPool::submit` calls
`invariant_violation` when called from a pool worker
(`worker_pool.cpp:406-411`); 38 `on_worker_pool_thread()` call sites ask first
and take a serial path. The policy is outermost-wins, and it is enforced rather
than documented. **There is therefore no such thing today as an inner kernel
that could "seize the full pool" while an outer one holds it** — the inner
kernel does not run in parallel at all.

**46 fan-out sites** call `pool.submit` (`chunked.cpp` 28, `update.cpp` 7,
`kernel_update.cpp` 4, `runtime_internal.hpp` 2 shared helpers, one each in
`aggregate/filter/join/lazy_table/sort`). The plan's inventory says 41; that
count is stale, `chunked.cpp` has grown. Every one of them is either the
outermost fan-out or degrades to serial.

**One genuine exception, and it is bounded.** A `PipelinedStageOperator`
producer runs on a raw thread that sets `t_on_stage_thread`, **not**
`t_on_pool_thread` (`worker_pool.cpp:306-312`). It may therefore legally submit
while its consumer also submits. `IBEX_PARALLEL_STATS` shows
`stage_threads_peak` reaching 2–3 concurrently on q09/q10/q18, so up to four
threads can hold independent batches in one pool. This is the only live,
unbudgeted concurrent DOP allocation in the engine — the exact mechanism item 2
names.

## 2. The measurement that decides it

A budget rations a scarce resource. The pool is not scarce; it is mostly empty.

From `benchmarking/profile_suite.py 8`, recorded in
`parallelism-overview.md` and re-measured once already at `190235b` after a
question about staleness:

| | `6f0a03e` | `190235b` |
|---|---|---|
| occupancy (`pool_work_ms` / capacity) | 30.4% | 32.8% |
| **parked with nothing queued** (`pool_unqueued_ms` / capacity) | **69.2%** | **66.9%** |
| worker backpressure park (`pool_idle_ms` / capacity) | 0.0% | ~0.01% |
| closure | 99.6% | 99.7% |

Two readings matter here.

**`pool_idle_ms ≈ 0` on all 22 queries** means no worker ever waits behind
another's work. Nothing queues. This is what killed the task-scheduler track
(item 7, "demoted four times and now refused outright"), and it kills the
contention premise of item 2 by the same argument: a child budget's only power
is to *withhold* threads from a consumer so a sibling can have them. When the
queue never backs up, withholding threads cannot speed anything up, and can
only lower the DOP of whichever consumer asked first.

**Even the one real exception shows no harm.** The 2–3 concurrent stage threads
plus the consumer are exactly the over-allocation a child budget would prevent,
and `pool_idle_ms ≈ 0` says their batches never make each other wait. `submit`
clamps `worker_count` to `threads_`, so the pool never grows either. There is a
mechanism, and there is no measured victim.

The risk table in the kernel-pipeline plan already states the bar:

> A new executor becomes an unmeasured scheduler project → Preserve the current
> pool and outermost-wins policy. **Require queue/occupancy evidence before
> adding work stealing or branch concurrency.**

The occupancy evidence exists, and it points the other way.

## 3. The history says thread-count budgets specifically do not work here

This is not a hypothetical. A helper-thread budget of exactly this design was
built and removed on 2026-08-21 (`parallelism-overview.md`, "Generalization
attempt"):

* `HelperThreadSlot`: RAII slot, one atomic counter, budgeted against
  `compute_thread_count()` — the design a child budget would generalize.
* It was diagnosed as the fix for q09's +57% regression from overlapping join
  materializations: "recursion spawns one raw thread per nesting level".
* **The diagnosis was wrong.** A temporary entry-count trace showed
  `build_binary_materializing_operator` is hit *exactly once* for q09, and
  re-testing at budget = 1, 2, and 8 gave the same ~230–250ms. There was never
  a pile-up for a budget to bound.
* The real cost was structural: that function materializes *both* sides, so
  overlapping them contends for the same cores and memory bandwidth instead of
  filling idle ones.
* The budget was correct, tested, and **never found a case it helped**. It was
  removed with the rest.

The recorded conclusion — the same one Phase 3 item 4 was closed on — is that
branch concurrency needs a **cost-aware** gate, not a thread-count one. A child
DOP budget is a thread-count gate. Building one now re-runs an experiment this
repo has already paid for.

## 4. The memory half has no consumer whatsoever

`grep` for memory accounting in `src/` and `include/` finds nothing: no
reservation, no limit, no spill path, no operator that changes behaviour when
memory is tight. The only "spill" hits are register-spill comments in
`window.cpp`.

So a `memory_budget` field in `ExecutionContext` would be a number nothing
reads, and there is no second implementation of any operator for it to select.
That is precisely the failure mode the plan's own risk table names ("a physical
IR mirrors every logical node and adds ceremony") and the reason `KernelContext`
was deliberately not built in Phase 2: **its stated trigger is not met.**

Worth being clear about what this is *not* saying. There is a real memory
problem in this engine — `[[project_runtime_binding_lifetime]]`, bindings are
never collected and OOM'd at SF-5. A budget field does not address it. That
wants a lifetime/GC fix at the binding layer, and it would be the *first*
plausible consumer of a memory accounting API, not the second.

## 5. What is genuinely worth doing now, and why it is not a perf item

One piece of item 2 stands on its own: **the compute budget has no accessor.**
~30 sites hand-spell `exec.parallel_threads != 0 ? exec.parallel_threads :
pool.size()`, and that fallback contradicts the field's own documentation:

> `configure_parallel_from_env` pins this to `compute_thread_count()`; 0 falls
> back to the pool size, which is now sized for DECODE and therefore larger, so
> **compute paths must not rely on that fallback.**

Thirty compute paths rely on that fallback. Honest scoping of how bad that is:
every in-tree caller (`interpreter.cpp:1546`, `repl.cpp` ×3) runs
`configure_parallel_from_env`, so `parallel_threads` is never actually 0 in
production and **this is a latent trap, not a live bug**. It is reachable only
through the library API with a hand-built context — which is the documented
spelling for "ignore the environment", and which would silently get a
decode-sized compute budget that measurably regresses compute (q01 +4.6%,
q17 +3.2% per `decode_thread_count`'s own table).

The fix is mechanical and has no performance claim attached: give
`ExecutionContext` a `compute_budget()` accessor (and name the decode budget
next to it, since `scan_pipeline_worker_count` deliberately uses the *pool*
size and that distinction currently survives only as a comment), then convert
the sites. Its value is that **the budget becomes a named thing with one
definition** — which is the actual precondition for ever making it hierarchical,
and is worth having whether or not a child budget is ever built.

## 6. Recommendation

Split item 2 into three, and do only the first.

* **2a — name the budget.** One `compute_budget()` / `decode_budget()` pair on
  `ExecutionContext`; convert the ~30 hand-spelled sites; delete the
  documented-but-relied-upon fallback trap. Mechanical, no behaviour change, no
  perf claim, gated by ctest + `check_answers.py` both modes. **Do now.**
* **2b — child DOP budgets.** *Blocked, not rejected.* It is the enabling
  precondition for concurrent independent branches (multiple producers per
  staged breaker), which `parallelism-overview.md` names as the most promising
  remaining direction. It is worthless before that, because the pool is 67%
  empty and nothing ever queues. Build it when a multi-producer change is ready
  to consume it, in the same change that consumes it.
* **2c — memory budget.** *Rejected as scoped.* Reopen when a consumer exists —
  the first plausible one is binding lifetime/GC, not the executor.

This keeps the plan's exit criterion intact. Phase 3's exit is that the old
island abstraction is an execution mode of a pipeline, which items 1, 3 and 4
have delivered; it does not require a budget nothing needs.

## 7. The cheapest experiment that would overturn this

The conclusion rests on "the pool is idle and nothing queues". Two ways to
falsify it, in order of cost:

1. **Re-run `benchmarking/profile_suite.py 8`** on the current tree. The last
   reading is from `190235b`, ~40 commits and one whole migration phase ago,
   and this phase changed how every query is constructed. If occupancy has
   risen materially or `pool_idle_ms` has become non-zero anywhere, queues are
   forming and 2b's premise changes. Cost: one profiled suite run, no build,
   read-only. **This should happen before 2b is discussed again, and its result
   is worth recording here either way.**
2. **De-risk the multi-producer estimate** (`parallelism-overview.md`'s own
   candidate 1): a throwaway harness decoding two independent large Parquet
   scans sequentially vs concurrently, outside any query plan. q10's sibling
   scans measure 65% and 18.5% occupancy in *separate* time windows — 83.5% of
   one core-set, so there is arithmetic slack for overlap. If that synthetic
   overlap delivers real wall-clock savings on this box, multi-producer becomes
   a live target and 2b becomes its prerequisite. If it does not, 2b stays
   parked indefinitely.

Note the ordering: experiment 2 is what makes 2b worth building, and experiment
1 is what tells us whether to bother running experiment 2.

## 8. How this could be wrong

* **`profile_suite.py` measures PDS-H at SF-1 on this box.** A workload with
  more independent branches, or a machine with more cores, could queue where
  this one does not. The finding is "not justified by the measurements we
  have", not "impossible".
* **`pool_unqueued_ms` cannot distinguish** "the plan has no parallel work
  available" from "work exists but nothing queued it". The overview says as
  much. Both readings argue against a *budget* (which only subtracts), but only
  the second argues for multi-producer work.
* **The stage-thread exception is real** even though it is currently harmless.
  If stage count ever grows beyond 2–3 — more staged breakers per query, or
  nested pipelines — it becomes an unbounded allocation with no accounting, and
  2b arrives on its own schedule rather than multi-producer's.

---

## 9. The `IBEX_PARALLEL=0` question, and what measuring it found (2026-08-24)

Asked whether the `=0` mode could be retired, on the reasoning that a
default-off config path rots. Three findings, in increasing order of
importance. The last one is a live default-configuration regression.

### 9.1 The flag is not what it looks like

`exec.parallel` is not the map-pipeline switch. It is read at 22 sites and
gates essentially all compute parallelism: the update kernel's field splitting
(`kernel_update.cpp:2140,2542`), sort (`sort.cpp:294`), grouped update
(`update.cpp:1831,1867`), the aggregate/join/distinct fan-outs
(`chunked.cpp:3011, 3425, 3686, 6509, 7127, 7389, 8958`), the pipelined stage
and scan (`:12414, :12634`), and only then the map seam (`:12530`).

Retiring it would delete no code — the serial chain it selects is also taken at
`=1` by every input below `parallel_min_rows`/`parallel_min_cells`, every
`SerialOnlyReason` shape, every `morsel_count < 2`, and every nested call under
`on_worker_pool_thread()`. What it *would* delete is the byte-identity oracle
(`MEASURING.md`, five uses in `ibex-e2e.sh`, the per-commit gate this whole
migration has used) and the `ibex-st` published benchmark line
(`run_bench.sh:179`, `gen_website.py`, `run_scale_suite.sh`,
`window_ohlc/run.py`).

### 9.2 The experiment surfaced an abort in the default config

Running the 22 PDS-H queries at `IBEX_CORES=1` **crashed on q19**:

```
ibex internal invariant violated (runtime/interpreter):
eval_numeric_update_blocks_into: missing double output
```

It passed at `IBEX_CORES=2/4/8` and at `IBEX_CORES=1 IBEX_PARALLEL=0`, so the
flag was masking it. Minimised to a shape with no core-count dependence at all
— **any non-empty table filtered to zero rows, then given a multi-node numeric
update, aborted the process in all four modes and on both the int and double
arms.**

Root cause: `494f15c1` (2026-08-22, this migration's Phase 2 item 2) split
`eval_numeric_update_blocks_into` out of `eval_numeric_update_blocks` and added
a null-destination check. `Column<T>::resize_for_overwrite(0)` leaves `data()`
null, so the check reported an absent output for a write with nothing to write.
Before the split the empty case fell out of the block loop never running.

Fixed by returning early on `rows == 0`, ahead of the destination checks and
the scratch allocation. Regression test:
`"Interpret compiled numeric update over an emptied range"`, both arms,
**verified to SIGABRT without the fix**.

Two process notes worth more than the fix:

* **The first version of that test passed without the fix.** It built an
  already-empty table, which never reaches the numeric evaluator; the failing
  shape needs a *non-empty* table emptied by a filter, and a multi-node tree
  (a single binary op takes another path). Exactly
  [[project_rewrite_test_reference_trap]] — the test has to be shown failing.
* **A scripted revert silently no-oped** (its assert fired, ninja said "no work
  to do") and made a *fixed* build look like an unfixed one. Read the
  background output before trusting a negative result.

**Gate gap this exposes:** every gate in this migration ran at 8 cores. Nothing
ran the suite at `IBEX_CORES=1`, so single-core-only chunk shapes were
unexercised for two days. `check_answers.py` at `IBEX_CORES=1` costs ~10s and
should join the standard gate.

### 9.3 The measured answer: not redundant, and DOP=1 is a second code path

With the crash fixed, both modes across all 22 queries
(`IBEX_CORES=1` vs `IBEX_CORES=1 IBEX_PARALLEL=0`, stdout plus
`IBEX_PARALLEL_STATS` plus per-node `pool_calls`):

* **stdout byte-identical on all 22.**
* **`pool_calls == 0` in both modes on all 22** — at one core nothing fans out
  to the pool either way, in any operator.
* **The stats differ on 19 of 22.** Always the same way: `parallel=0` in both,
  but `serial: N->0` and `morsels: N->0` (q03/q10: 115 morsels; q19: 97;
  q12/q14: 92), plus `parallel_fields: 6->0` (q01) and
  `chunk_direct_updates: 4->1` (q19).

So at `IBEX_CORES=1` the morsel pipeline is still **constructed and executed,
one morsel at a time, with zero workers and zero pool calls.** The flag is
therefore *not* redundant with `IBEX_CORES=1` — it selects a different path —
and that path costs real time. Five queries, five interleaved rounds each,
seconds of wall time, warm cache:

| query | morsel path (`CORES=1`) | serial path (`CORES=1 PARALLEL=0`) | delta |
|---|---|---|---|
| q03 | 0.33 0.35 0.35 0.33 0.35 | 0.21 0.19 0.22 0.20 0.23 | ~+65% |
| q10 | 0.33 0.37 0.34 0.35 0.36 | 0.24 0.23 0.20 0.20 0.20 | ~+60% |
| q12 | 0.24 0.25 0.25 0.26 0.25 | 0.13 0.12 0.13 0.12 0.12 | ~+100% |
| q14 | 0.23 0.24 0.25 0.24 0.24 | 0.12 0.08 0.11 0.09 0.08 | ~+140% |
| q19 | 0.32 0.35 0.33 (0.32 0.33) | 0.14 0.15 0.13 0.13 (0.52 cold) | ~+140% |

Every round of every query separates cleanly — no overlap between the two sets,
which is the bar `[[project_bench_interleaved_methodology]]` sets, and the
effect is far outside the ±10% build-layout envelope. These include process
start and decode, which both sides pay.

**This is a live regression in the default configuration on a one-core
machine**, and the benchmark harness cannot see it because `run_bench.sh:179`
sets `IBEX_CORES=1 IBEX_PARALLEL=0` together — it measures the fast path, while
a user on one core who sets neither gets the morselizing one.

### 9.4 The simplification, and why it is better than deleting the flag

`morsel_worker_count` (`chunked.cpp:11401`) **already computes that it cannot
parallelize** at one core: `workers < 2 ? 0 : workers`. That verdict is used to
run the morsels serially rather than to decline the pipeline. The proposed
change is to let it decline: when the compute budget cannot supply two workers,
the handoff at `chunked.cpp:12530` should not fire and the plan should compose
serially.

Three reasons this beats retiring the flag:

1. **It removes the overhead**, which retiring the flag does not.
2. **It makes the flag genuinely redundant.** Once DOP=1 resolves to
   `PipelineMode::Serial`, `IBEX_CORES=1` and `IBEX_PARALLEL=0` are the same
   path by construction, and retiring the flag becomes a documented equivalence
   rather than a judgement call — the right order, since the flag is the oracle
   that proves the two agree.
3. **It is the same defect as item 2a**, one level up: a boolean is standing in
   for a budget. DOP is a number; DOP=1 should not be a different code path.
   That is the Umbra endpoint, and it is what Phase 3 should mean by "the
   island is an execution mode".

**The equivalence evidence already exists**: §9.3's run is 22 queries of
byte-identical output between "morsel pipeline at DOP=1" and "serial composer".
The serial composer demonstrably handles every shape in the suite.

Not yet built — it changes executor construction for every single-core query
and wants its own gates (ctest, `check_answers.py` at `IBEX_CORES=1` and 8, and
an A/B at 8 cores to confirm the multi-core path is untouched).

### 9.5 State of the tree after this section

* `src/runtime/update.cpp` — `rows == 0` early return in
  `eval_numeric_update_blocks_into`.
* `tests/test_interpreter.cpp` — the regression test, both arms.
* Gates: debug ctest **1754/1754**; `check_answers.py` **22/22** under
  `IBEX_PARALLEL=1`, `IBEX_PARALLEL=0`, and `IBEX_CORES=1`.
