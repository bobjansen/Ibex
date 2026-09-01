---
name: cooperative_pipeline_waits
description: "Make the pipeline executor's backpressure waits (OrderedChunkRing::acquire / ::take and the stage-ring equivalents) cooperatively drain the worker pool while parked, so nested worker-pool fan-out under a pipeline scan/join worker stops deadlocking. Unblocks the idle-capacity decode gate and every other `on_worker_pool_thread()` serial fallback."
metadata:
  node_type: plan
  type: project
  status: in-progress
---

# Cooperative pipeline waits

**Status: STEPS A + B + C LANDED (2026-09-01, better-plans).** Step D (the other
`on_worker_pool_thread()` serial gates — `scan_shard_target`, `for_row_ranges`,
`DeferredScanSourceOperator::unit_window`) still pending.

- **C** (this commit): `parallel_readers` (parquet.hpp) fans out a *nested* decode
  (one already on a pool worker — a dimension table scanned inside a pipeline)
  by column, capped at `kNestedDecodeFanout = 4` extra readers. An earlier cut
  sized it exactly via a `pool.size() - g_pool_busy` counter, but the per-task
  atomic RMW to maintain `g_pool_busy` cost measurably on task-heavy queries
  (q01/q20/q21); the fixed cap needs no bookkeeping and the cooperative ring
  waits absorb any oversubscription.

  Realises the dimension-table decode win — standalone interleaved min-wall
  SF-8 8c, stable across runs: **q02 −13-18%, q18 −8-10%, q14 −4-9%,
  q10 −4-8%**, q16/q19 −2-3%. Heavy queries (q01/q20/q21/q12) swung ±15%
  between runs on a contended WSL2 box — inconclusive; needs a clean-box
  `run_bench.sh` re-measure. q19 (deadlocked in the pre-cooperative prototype)
  clean in every check.

  Step C testing revealed step B's *ungated* `wait_for_batch` assist could pick
  up an unrelated **older**-generation `PipelinedScanOperator::run_worker` task,
  which parks on its ring and strands the nested decode the waiter is blocked
  on (q19 hung ~1/3). `try_run_one_pending` is now uniformly generation-gated;
  the q18 cost once blamed on gating it was the poll spin, fixed separately by
  the outstanding-nested gate.

- **A** (`9919da0e`): `WorkerPool::try_run_one_pending()` + bounded `wait_for_batch` park.
- **B** (`8e84cf88`): `cooperative_ring_wait()` at `OrderedChunkRing::acquire`/`take`
  and the `PipelinedStageOperator` consumer wait. Two gates, each added after a
  measured failure of the naive cut:

  * **Generation gate** on the *ring-wait* assist. A cooperative waiter must only
    run strictly-nested work: every `submit` stamps a monotonic generation,
    `run_task` tracks the running one thread-locally, and the ring-wait assist
    (`try_run_one_pending`) skips a queued task whose generation is `<=` the
    caller's. Without it a `MorselPipelineOperator` worker parked in `acquire`
    picked up a **sibling** `run_worker`, which parked on the same ring and
    recursed without bound (an 8-deep tower hung two morsel-pipeline E2E tests).
    Applies to **all** cooperative assist, `wait_for_batch` included — step B
    first left `wait_for_batch` ungated (a supposed ~3% q18 cost) and step C's
    nested decode then showed an ungated waiter can pick up an unrelated
    *older*-gen scan-worker task and strand itself. The q18 cost was the poll
    spin, handled by the outstanding-nested gate below.

  * **Outstanding-nested gate** on entering the cooperative loop at all. It runs
    only when `this_thread_has_outstanding_nested_work()` — a thread-local count
    of batches this thread submitted from within a task that have not finished
    (decremented by the last task of each). A pipeline worker that has fanned
    out nothing takes a plain `cv.wait`. Without this, the 250µs poll loop spins
    on the pool mutex and burns ~1 extra core on q18 (~3% wall standalone; more
    under interleaved A/B, where the extra CPU biases the paired comparison —
    the first A/Bs read q18 at +10%, standalone min-wall was +3%, and with the
    gate q18 is at parity).

  1831 tests + check_answers 22/22; guard test asserts `max_depth == 1` for
  nested cooperative bodies.

---

Written after a q19 SF-8 hang
diagnosed the deadlock precisely — see [[project_nested_decode_fanout_deadlock]].

## Problem

During pipeline execution every worker-pool thread is running a long-lived
`PipelinedScanOperator::run_worker` (or `MorselPipelineOperator::run_worker`)
body. Those bodies block on **ring** state — `OrderedChunkRing::acquire` (a
producer waiting for a free slot) and `OrderedChunkRing::take` (the consumer
waiting for the next in-order chunk) — via `cv.wait(lock, pred)` on the ring's
own mutex. They do **not** touch the pool task queue while parked.

Only `WorkerPool::Batch::wait()` (`wait_for_batch` in
`src/runtime/worker_pool.cpp`) cooperatively drains the pool queue while blocked
(`assist_one`). So when a pipeline worker nests a `pool.submit(...)` and calls
`Batch::wait()` on it, the child tasks land in the queue with **no one to run
them**: every other pool thread is parked in a ring wait inside its own worker
body, and the nesting thread is parked on its batch. Deadlock.

Concrete trace (hung q19, 8 threads, `IBEX_NESTED_DECODE_FANOUT` prototype):

| thread | top ibex frame | state |
|---|---|---|
| main | `MaterializeOperator::run` → `PipelinedScanOperator::next` → `OrderedChunkRing::take` | parked — next chunk |
| worker A | `run_worker` → `run_unit` → `materialize_deferred_scan_unit` → `WorkerPool::Batch::wait()` | parked — its nested child tasks |
| workers B–H | `run_worker` → `OrderedChunkRing::acquire` | parked — ring backpressure |

Cycle: main ⇒ A's chunk; A ⇒ its child decode tasks; child tasks ⇒ a free pool
thread; B–H ⇒ main to drain the ring. Nobody moves.

This is why `parallel_readers`, `DeferredScanSourceOperator::unit_window`,
`scan_shard_target`, `for_row_ranges` (`runtime_internal.hpp:139`) and
`evaluate_field_maybe_parallel` (`:518`) all bail to serial under
`on_worker_pool_thread()`. The guard is load-bearing, not conservative.

## Goal

Make the pipeline backpressure waits **work-conserving**: a thread parked in a
ring wait runs queued pool tasks until either its own wait condition is met or
the queue drains. Then:

- nested `pool.submit` under a pipeline worker is safe — the parked siblings run
  the child tasks;
- the `on_worker_pool_thread()` serial fallbacks can be replaced with an
  idle-capacity check (workers parked in a cooperative wait are real spare
  capacity), realising the measured decode win
  ([[project_dimension_table_decode_idle_cores]]: q02 −10%, q10/q14/q16 −4%,
  byte-identical) and the same for the other bail sites.

Non-goal: work stealing, a DAG scheduler, or removing the one-query-at-a-time
lease. The pool stays "small and boring".

## Design

### 1. A cooperative-drain primitive on `WorkerPool`

```
[[nodiscard]] auto try_run_one_pending() noexcept -> bool;   // pop+run one queued task, false if queue empty
void wait_for_pending(std::stop_token-ish, std::chrono::nanoseconds timeout);  // park on impl_->work until work OR timeout
```

`try_run_one_pending` is `assist_one`'s body promoted to a public method (the
per-batch `assist_one` lambda becomes a thin wrapper over it). Runs *any*
queued task — safe because the query lease guarantees every queued task belongs
to the current query.

### 2. Waking a cooperative waiter when new work arrives

A thread parked in `OrderedChunkRing::acquire` is on the ring cv, not the pool
cv, so `submit`'s `impl_->work.notify_all()` does not reach it. Two options:

- **(v1, recommended) bounded-timeout poll.** The ring wait loop re-checks the
  pool queue every ~250µs–1ms. This path is already a backpressure stall; a
  sub-ms poll bound is negligible against it, and it is ~10 lines with no new
  cross-primitive coupling. The timeout only bounds the "work was enqueued the
  instant after I last checked" window.
- **(v2, later if v1's poll shows up in a profile) shared notify.** `submit` /
  `submit_unbarriered` also fire a process-global `g_cooperative_work` cv;
  `OrderedChunkRing::wake_all` and `publish` fire it too; cooperative waiters
  `wait` on that cv with a compound predicate. Removes the poll, adds coupling.

Start with v1.

### 3. Rewrite the three ring waits

`OrderedChunkRing::acquire`, `OrderedChunkRing::take`, and the
`PipelinedStageOperator` consumer/producer waits (`pipeline_executor.cpp` ~2157
and ~2213 — the stage operator uses a raw `std::thread`, so its producer wait
does NOT need this; only re-check whether its *consumer* wait, which runs on a
pool worker when the stage is nested, does).

Pattern, replacing `cv.wait(lock, pred)`:

```
while (!pred()) {
    lock.unlock();
    const bool helped = pool.try_run_one_pending();
    lock.lock();
    if (pred()) break;
    if (!helped) {
        const AssistIdleScope idle;            // profiler: parked, not assisting
        cv.wait_for(lock, kCoopPollInterval, pred);
    }
}
```

`RingWaitScope` still wraps the whole thing (the time is still "the consumer is
behind"), but a nested `AssistScope` / `AssistIdleScope` split lets the profiler
distinguish *ran someone else's task* from *truly parked* — load-bearing for the
occupancy / serial-fraction accounting the ceiling analysis depends on
([[project_serial_fraction_is_the_ceiling]], [[project_task_clock_finds_multiplied_work]]).

### 4. Fix `wait_for_batch`'s own latent version of this

`wait_for_batch` (worker_pool.cpp:501) already does the cooperative loop but
falls to an **indefinite** `state.done.wait` when `assist_one` returns false. If
a sibling enqueues work after that park, this thread is not rewoken (it is on
the batch cv). Same bounded-timeout treatment. Without this the deadlock just
moves from the ring wait to the batch wait.

### 5. Lift the serial-fallback gates

Once 1–4 are in and proven deadlock-free, replace `!on_worker_pool_thread()` at
each site with an idle-capacity test. Do them **one at a time, each its own A/B**:

1. `parallel_readers` (parquet.hpp) — the measured win, do first.
2. `DeferredScanSourceOperator::unit_window`.
3. `scan_shard_target` (parquet.hpp).
4. `for_row_ranges` / `evaluate_field_maybe_parallel` (runtime_internal.hpp).

Idle-capacity signal: not "threads in a task body" (the prototype's
`pool_busy_estimate`, which reads 8/8 during a pipeline). Count **threads
currently parked in a cooperative wait** — those are the ones that will run a
nested task. Add a `g_cooperative_waiters` atomic incremented across the
`try_run_one_pending` loop.

## Sequencing & proof

- **A.** Primitive (`try_run_one_pending`) + `wait_for_batch` timeout fix.
  Byte-identical, no perf change (nothing nests yet). Full ctest + check_answers.
- **B.** Convert the ring waits + profiler `AssistScope` split. Add a guard
  test: a minimized q19 shape — a fan-out source nested inside a pipeline whose
  worker count == pool size — that hangs on `main` today and must complete.
  Run it under a shrunk pool (`IBEX_CORES=2`, 4+ blocking workers). Interleaved
  suite A/B: must be byte-identical and within noise (the waits are equivalent
  when nothing nests).
- **C.** Flip gate 1 (`parallel_readers`). Interleaved SF-8 A/B, expect
  q02/q10/q14/q16 in the −4 to −10% range, no regressions, byte-identical.
  Re-verify q19 specifically, pinned to 8 cores, several times.
- **D.** Flip gates 2–4, one A/B each.

## Risks

- **Re-entrancy depth.** A cooperatively-run task may itself hit a ring wait and
  recurse. Bounded by pool size × pipeline nesting depth (breaker count), but
  stack growth is real — cap cooperative nesting (a thread-local depth counter;
  above N, park instead of assist).
- **Profiler model.** `occupancy`, `pool_work_ms`, `serial_fraction`,
  `pool_unqueued_ms` all shift meaning. The `AssistScope` split (§3) is the
  minimum; the closure checks in `profile_suite.py` must still close. Do not
  land B without re-validating the accounting against a known query.
- **Determinism.** Cooperative execution reorders which thread runs which task.
  Results must not depend on it (tasks are independent; sequence ordering is
  preserved by the ring). Verify byte-identical at every step
  ([[project_parallel_reduction_determinism]]).
- **Fairness / priority inversion.** A consumer parked in `take` now spends time
  running producer tasks — which is the point — but a pathological plan could
  have it run *unrelated* deep tasks while the chunk it needs waits behind them.
  The bounded poll re-checks `pred()` every iteration, so it cannot get stuck,
  only slowed. Watch for it in the suite A/B.
- **`PipelinedStageOperator` interaction.** Its producer is a raw stage thread
  by design. Confirm the cooperative ring waits do not let a pool worker start
  assisting in a way that reintroduces the saturated-pool hazard that operator's
  raw-thread choice exists to avoid.

## Payoff

Direct: the decode gate (~4% on 4 queries, byte-identical, more on q02).
Indirect and larger: every `on_worker_pool_thread()` serial fallback becomes a
real parallel path, and nested fan-out — which the parallelism roadmap
([[project_parallelism_overview_doc]]) wants generally — stops being unsafe.
