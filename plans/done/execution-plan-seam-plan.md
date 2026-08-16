# Execution-Plan Seam Plan

**Prerequisite for [runtime-multithreading-plan.md](runtime-multithreading-plan.md)**
— completed before that plan's Phase 0. The master plan repeatedly names
"the existing pipeline planner" as the single owner of the parallel-eligibility
decision (see its *Operator categories* section). That owner does not exist in
the execution path today. This plan establishes it.

## Problem

The master plan asserts:

> This is an extension of the chunked pipeline planner … There must be one owner
> of this decision, one eligibility vocabulary, and one fallback path.

But there is no planner in the execution path to extend:

- `plan_pipelines()` / `PipelinePlan` (formerly in `include/ibex/runtime/pipeline.hpp`,
  `src/runtime/pipeline.cpp`) is **analysis-only**: it segments the IR tree into
  `PipelineSegment`s and classifies nodes via `classify_node` into
  `Source` / `Passthrough` / `Breaker`. It builds no operators and drives no
  execution.
- Its **only caller is `tests/test_operator.cpp`**. Nothing in `src/` consumes it.
- Actual execution is `interpret()` → `build_operator()` →
  `MaterializeOperator::run()` (`src/runtime/interpreter.cpp:1433`).
  `build_operator()` recurses the IR directly and constructs the pull-operator
  tree; it never consults `PipelinePlan`.

So the two representations are disconnected: a test-only segmenter and the real
recursive builder. Until one authoritative seam decides island vs. barrier vs.
serial fallback, the master plan's "one owner" cannot be honoured, and the
parallel-eligibility check would inevitably grow a second copy.

## Decision

**Option B is selected.** `build_operator()` remains the authoritative
physical-execution seam. `analyze_parallel_island()` owns both the
execution-capability vocabulary and the expression-level eligibility checks.
Phase 1 will invoke it from that seam once `RuntimeOptions` can request a
parallel executor; the serial prerequisite deliberately does not run analysis
only to discard it on a hot query-construction path.

### Option A — authoritative physical-plan builder (not selected)

Make `plan_pipelines()` (or a successor `build_physical_plan()`) the artifact
`build_operator()` consumes: segment first, then build operators per segment,
so segment boundaries *are* the barrier/island boundaries the executor sees.

- Pro: a real single owner; the master plan's language is literally true; the
  segment structure already models "breaker ends a segment."
- Con: larger refactor. `build_operator()` currently interleaves construction
  with fusion recognition (`FilterProject`, `FilterUpdateProject`, join-shape
  selection). That fusion logic must move into, or be consulted by, the planner
  so segments reflect the *fused* physical operators, not raw IR kinds.

### Option B — `build_operator`-hosted island selection (selected)

Keep `build_operator()` authoritative (it matches reality and already owns
fusion). Factor the **eligibility decision** into one analysis pass that
`build_operator()` calls at each `Passthrough` chain to decide whether to emit a
parallel-island operator or the existing serial chain.

- Pro: smaller, matches the current architecture, no second executor. Fusion
  stays where it is.
- Con: the master plan's prose must be rewritten — the owner is
  `build_operator` + one eligibility pass, **not** `plan_pipelines`. `PipelinePlan`
  either becomes the backing structure for that pass or is retired.

**Recommendation: Option B**, because `build_operator()` already owns the two
things the decision depends on (operator fusion and node construction), and
Option A's win — "a real planner object" — is cosmetic if that object just
re-derives fusion. The non-negotiable requirement from the master plan is *one
eligibility vocabulary in one place*; Option B satisfies it without a second
execution engine. Record the decision in the master plan before Phase 0 starts.

## Delivered work

1. **Execution-capability vocabulary.** The master plan adds
   four categories (Parallel map / Ordered stream / Barrier / Parallel barrier)
   as a single function in `src/runtime/pipeline.cpp`. No behavior change yet —
   the builder retains its serial fallback, so `IBEX_THREADS=1` output is
   byte-identical.
2. **One eligibility pass.** A single function that, given a candidate
   `Passthrough` chain, returns whether it is a parallel map. It owns the
   expression-tree scan the master plan's Phase 0 item 7 describes (extern /
   plugin / generator / neighbour-reading transforms → ineligible). This is the
   single place the master plan's "one eligibility vocabulary" lives. Every other
   site queries it; none re-implements it.
3. **Lowerer-based ownership test.** Following the
   standing lesson in `project_join_reorder_cost_model` ("test optimizer passes
   FROM THE LOWERER"), assert the capability/eligibility decision on lowered IR,
   not hand-built nodes, so the classification the executor actually sees is the
   one under test.

4. **Retire the test-only pipeline representation.** Its segmentation
   implementation, role classifier, and hand-built-node tests are deleted.
5. **Synchronize the master plan.** Its *Operator categories* section now names
   `build_operator` + the single eligibility pass as the owner.

## Done when

- One capability vocabulary and one eligibility function exist, exercised from
  the lowerer.
- `build_operator()` is designated as the authoritative hot-path seam; Phase 1
  will activate its eligibility consultation with the parallel executor.
- The master plan's *Operator categories* section describes the seam that
  actually exists.
- `IBEX_THREADS=1` output is byte-for-byte unchanged (this whole plan is a
  single-threaded refactor; it adds no parallelism).

## Non-goals

- No worker pool, no parallel task submission, no parallel operator. Those are
  master-plan Phase 1.
- No change to fusion decisions or plan shape — only *where the eligibility
  decision lives*.
