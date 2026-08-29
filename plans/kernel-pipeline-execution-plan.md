# Kernel-oriented pipeline execution

**Status: in migration.** Phase 0 resolved by disposition; Phase 1 landed
2026-08-22; Phase 2 complete except `KernelContext` (deliberately unbuilt);
Phase 3's handoff/island/raw-thread work is complete, with accounting and
DOP/memory budgets deferred; Phase 4 construction ownership and parallelism
authority are done, while true operator decomposition remains open; Phase 5 has
not started apart from retiring the fused logical node kinds. **Compacted
2026-08-27** — the ~40-entry Phase 2 per-commit diary is in git history at the
pre-compaction commit's parent; the "Where Phase 2 stands" table below is the
current state.

The goal is **feature parity on this architecture**: every shape the old
execution seams supported reaches either a kernel or an explicit, inspectable
fallback. A deliberate replacement for the current execution architecture over
several migrations, not a one-change rewrite. Borrows Umbra's separation of
logical planning / physical pipelines / morsel execution while **rejecting query
JIT** — Ibex's backend stays compiled C++ with a broad library of
template-instantiated vector kernels.

Read `MEASURING.md` and `parallelism-overview.md` first. `chunked-execution-plan.md`
and `chunked-execution` were removed from the tree 2026-08-22 (git history);
this plan owns the architecture that replaces their ad-hoc seams. The R1–R21
canonicalize table is in `include/ibex/ir/canonicalize.hpp`.

## Why

`src/runtime/chunked.cpp` (~13k lines) is the physical planner, most streaming
operator implementations, parallel islands, pipelined stages, and a large set of
operator-specific eligibility rules — all grown together because
`build_operator(const ir::Node&)` lowers logical nodes directly into mutable
`Operator::next()` objects. Three costs: (1) a physical choice has no
representation ("stream this join", "materialize this aggregate" are builder
branches, not inspectable decisions); (2) parallelism is operator-local — a
query can't expose source-to-breaker work as an independently scheduled
pipeline; (3) specialization and semantics are entangled in one file.

## Non-goals

No LLVM / assembler / runtime compilation / bytecode / query JIT. No big-bang
AST replacement, no surface-language change. No general DAG scheduler /
cross-query concurrency / work stealing unless measurements show queued work
requiring it. No loss of pushdown, categorical encodings, null semantics,
source-global selection, deterministic error selection, or byte-identical
output. Order / global aggregate / window / reshape / rich join semantics may
remain real barriers.

## Target architecture

```
parser AST  →  typed logical IR  →  physical plan  →  pipeline executable  →  morsel executor  →  Table / stream sink
```

1. **Preserve the AST, narrow its job** — user syntax / spans / functions /
   blocks / sugar only. Lowering is the one-way boundary: after semantic
   analysis, no runtime component inspects `parser::ast`. Do NOT redesign AST
   ownership / `variant` representation — high-risk, no path to the multicore
   result.
2. **Logical IR is purely logical** — relational meaning, expression/null
   semantics, schema/ordering/unique-key/nullability/origin proofs, optimizer
   rewrites (pushdown, join ordering). It must NOT encode execution accidents
   (`FilterProject`, `TopK`, `is_streamable_inner_join`, a join-key
   representation, worker-safety of a source). Immutable once optimization
   finishes; every node has a stable printable identity; derived properties come
   from one analysis result, not ad-hoc recomputation in builder branches.
3. **Explicit physical plan** — private `runtime::physical`, data-only
   inspectable nodes, the sole owner of algorithm selection and execution
   capability. First vocabulary: Source (`TableScan`/`LazyScan`/`ExternSource`)
   → Pipeline map (`Filter`/`Project`/`Update`/`Rename`, fusable) → Build barrier
   (`HashBuild`/`HashAggregateBuild`/`SortBuild`/`Materialize`) → Probe/output
   (`HashProbe`/`AggregateEmit`/`OrderedMerge`) → Fallback (`MaterializedCall`).
   Each node **declares** (not implies): schema + `TableProperties` transfer;
   streaming/blocking/semi-blocking; partitionability + output-order rule;
   worker-local / partition-owned / query-global state; memory estimate + max
   useful DOP; supported kernel families + a decline reason. `explain physical`
   must print all of it. **A plan that cannot explain why it materialized is not
   an acceptable replacement for the current branches.**
4. **Pipeline units, not a query DAG scheduler** — a pipeline is a maximal chain
   from a partitionable source or barrier output to its next breaker; one
   producer contract, one consumer contract; a breaker owns its build state and
   creates the next pipeline after its build completes. The executor schedules
   numbered morsels using the existing pool + query lease. No nested pool
   submissions (outer DOP wins), no work stealing/futures, no concurrent
   independent branches until DOP partitioning + bandwidth admission are
   measured worthwhile, no raw `std::thread` outside one executor-owned handoff
   API. The benefit is a first-class auditable place to create overlap when an
   algorithm can actually produce independent morsels — not magic.
5. **A broad templated kernel library** — a few composable kernel interfaces,
   many statically compiled specializations. `ColumnView`/`ChunkView` non-owning
   (length, validity, encoding, offsets explicit); `Selection` an explicit
   bitmap/index/range choice, never a hidden `std::vector` convention;
   `OutputWriter` owns pre-sized disjoint ranges or a worker-private buffer,
   variable-width follows count → prefix-sum → scatter; `KernelContext` owns
   scratch / cancellation / RNG stream / profiling counters, never query-global
   mutable state. **Closed dispatch at pipeline construction, not virtual calls
   per row.** Kernel families shared across operators — the same gather /
   validity / scatter / hash / expression kernels serve filter output, join
   assembly, sort gather, aggregate emission.
6. **Ownership-oriented breakers** — follow state ownership, not universal
   streaming. Hash join: one immutable-after-build state; probe streams
   source-order morsels; ordered merge reconstructs probe order. Hash aggregate:
   fixed logical hash partitions own key maps + slots; partition count
   **data-derived / fixed, never core-count-derived**. Order/distinct/window
   start as explicit barriers with reused kernels. `MaterializedCall` is honest
   and profiled — preserves median/quantile/EWMA/predicates/reshape until a
   physical implementation exists.

## Where this stands (re-verified 2026-08-29)

**Done:** Phase 1 (physical plan exists, inspectable); Phase 2 (map kernels
ported, fusion is physical, fused node kinds retired as an execution concern);
Phase 3 items 1/3/4 (one executor-owned ordered handoff, islands dissolved into
a pipeline mode, no raw-thread branch concurrency); Phase 4 **construction
ownership and parallelism authority** (every breaker PDS-H reaches is built
from the plan — backlog 116→6 breakers, plan describes 97% of real-work nodes;
Distinct, streaming Join, and streaming Aggregate read resolved fan-out policy
from the plan).

**The 97% still flatters it:** the streaming inner join is shaped as explicit
`HashBuild` and `HashProbe` nodes across a typed runtime-oriented barrier. The
hash-aggregate fallback now has typed discovery, accumulation, final-ordering,
and emission plan nodes. A serial coordinator invokes all four: discovery
publishes a bounded per-chunk transfer that accumulation consumes before the
source advances, while owned/async one-pass kernels publish an explicit fused
result. Each node has its own execution-profile row. Semi/anti retains its
separate streaming operator.

### Next, in order

1. **Split the join operator, orientation resolved at run time — DONE
   2026-08-25** (`49188c71`, `c2d32f1a`, `a2569785`, `433eb6df`, `8cb4e936`).
   Orientation is a value a build phase returns (`JoinOrientation` /
   `JoinBuildOutcome`); the plan carries both children as pipelines +
   `orientation=runtime` and stops asserting a build side it doesn't choose (the
   actual blocker — two pipelines can't be scheduled around a wrong decision).
   Head table partitioned, fill morsel-parallel, no locks/merge, bit-identical
   to serial by construction. `run_build()` is a phase entry point run at
   plan-execution time. Measured: q21 −8.5% from the parallel fill; scheduled
   invocation geomean −1.9% over 22 queries (q21 +5.1%, q19 +5.5% reported not
   averaged). Kill switches `IBEX_JOIN_BUILD_SERIAL` / `IBEX_JOIN_BUILD_LAZY`.
2. **Make the probe a step inside a map pipeline — DONE structurally.** The
   eligible probe is now admitted into `build_morsel_worker_chain` and installed
   before the row-local map steps; `build_pipeline_from_input` extracts the
   probe side rather than materializing join output first. This preserves the
   intended source → probe → map worker shape, including swapped-mode coverage.
   `HashProbe` is now also an explicit physical-plan and executor node; this
   still carries no end-to-end performance claim.
   Preconditions met: build is a scheduled phase (`8cb4e936`), build side is
   jointly owned so N probes share one (`6df9a966`), probe is its own operator
   (`177b7a93`, `JoinProbeOperator`). The physical promotion is complete; the
   opt-in map-chain fusion remains the measured construction described here.
   - **Coverage is the finding.** PDS-H join modes: 28 `Stream`, 12
     `Precomputed`, 11 `Swapped`. `Precomputed`/`Swapped` materialize both sides
     and emit one table — no probe pipeline to give. `Swapped` (a third of
     joins) is **the un-Umbra shape**. A parallel map pipeline on a join is only
     8 of 22 queries and the intersection with `Stream` is nearly empty.
   - **Performance argument needed before scheduling.** The one first given
     (`probe_parallel_workers`' `on_worker_pool_thread()` veto serializes nested
     joins) was **measured false** — 0 of 52 declines on PDS-H (35 admit, 17
     too_small, 0 nested). No join probe in PDS-H ever runs on a pool thread
     because joins aren't inside parallel pipelines. `assemble_output` also does
     NOT dominate (14% of join self-time, ≤10.5% of any query's wall) —
     retiring the standing "assemble_output dominates" note. So the fusion
     argument is **structural, not performance**.
   - **Deferred-probe threshold regression — DONE 2026-08-29.** The focused
     test starts with a 70k-row deferred right, publishes a build-side
     membership filter that resolves it to one row, then exercises the
     below-`kStreamRightThreshold` BuildRight path after the 20k-row left was
     drained. It asserts filter publication, parallel probe activation, and
     serial/parallel structural equality.
3. **Phase 4 aggregate decomposition** — discovery / per-partition slots / final
   ordering / emission as phases. **Determinism blocker cleared 2026-08-27**
   (guard test landed). The preflight is complete: all four structural-node
   policies appear in `explain physical`; aggregate fan-out gates read their
   node's resolved policy; `AggregateColumnMapping` binds known schemas
   during planning and lazy/open schemas once at execution; and physical-plan
   mutation tests prove mapped positions, typed edges, and worker ceilings are
   consumed or rejected. **Structural plan slice DONE 2026-08-29:** the hash
   fallback carries typed Discovery → Accumulation → FinalOrdering → Emission
   nodes and ownership edges; `explain physical` renders the chain and the
   executor rejects missing, redirected, or mistyped edges. **Serial lifecycle
   slice DONE 2026-08-29:** final ordering is no longer triggered implicitly by
   emission; a coordinator drains the input, invokes the deterministic ordering
   merge, then permits output construction. Discovery and accumulation remain
   deliberately fused only where owned/async or specialized categorical kernels
   produce final aggregate state in one pass.
   **Discovery-transfer/accounting slice DONE 2026-08-29:** ordinary paths pass
   group IDs, positional aggregate entries, row count, and any seeded-First mask
   through one chunk-bounded `AggregateDiscoveryTransfer`; fused kernels return
   an explicit fused marker. The coordinator invokes Discovery and Accumulation
   separately before advancing the source, and all four structural nodes have
   independent profile rows. Release A/B over `groupagg,multi,events` (7
   interleaved repeats, 3 timed iterations, pinned core) found all nine query
   deltas noise; final total +0.22%, geometric speedup 0.992×.
   **Structural fan-out authority DONE 2026-08-29:** the coarse `partition` /
   `finalize` records are removed. Discovery, Accumulation, FinalOrdering, and
   Emission each carry and supply their own policy; explain renders all four,
   and profile-backed mutations prove the executor consumes their individual
   worker ceilings. Data-derived morsel/partition counts and specialization
   thresholds remain next to the kernels that can observe them. Release A/B
   against the preceding commit over `groupagg,multi,events` (7 interleaved
   repeats, 3 timed iterations, pinned core) classified all nine deltas as
   noise; total +0.53%, geometric speedup 0.998×.
4. **Port `Tail` / `TopK` / `FilterHead` / `FilterTail` — DONE.** Same
   single-operator shape as Order/Head: `plan_physical` marks each migrated,
   `build_physical_{tail,topk,filter_head_tail}` construct them (moved verbatim
   from the per-kind switch), the switch branches are deleted, and `explain
   physical` renders `Breaker(<kind>)  serial (single-operator breaker, no
   fan-out point)`. TopK stays a serial bounded-heap select by design. No
   behaviour change.
5. **Phase 5 item 1 — split `chunked.cpp` by ownership** — easier now that the
   fan-out policy is outside the operators.
6. **Sweep process-global plan counters in tests** — one test passed while its
   premise was false (`physical_materialized_calls` is process-wide, other tests
   in the binary bump it). Others may lean the same way.
7. **Phase 3 item 5 — per-pipeline scheduling accounting** — small, worth more
   once the join and aggregate phases have independent identities to attribute.
8. **Phase 3 item 2 — DOP/memory budgets** — analysed and **blocked**
   (`phase3-dop-budget-analysis.md`): the pool is 65% idle with nothing queued,
   so a budget rations a non-scarce resource. Reopen when a multi-producer
   change needs it or `profile_suite.py` shows queues.

**Deliberately not doing:** porting the 6 materializing joins (they're the
intended `MaterializedCall`); a join build-side cost model *as a prerequisite
for the split* (real as a planner-quality question — q12's regression — but not
on the critical path; once the split lands it's a narrow optimization deleting
the `MaterializeOperator(left)` barrier that exists only to measure a row
count); deferred-probe registration selectivity (`build_side_worth_deferring`,
scan-layer, its own schedule).

### Two decisions, routinely conflated

Both called "the join cost model", different files, different questions:
- **Decision A — deferred-probe registration** (`build_side_worth_deferring`,
  `src/ir/scan_predicates.cpp`): should the build side be eagerly materialized
  so its key bounds push into the probe scan's Parquet decode? Rule
  `build_est.rows * 2 < probe_rows`. A **decode-pruning** question. Scoped in
  `plans/deferred-probe-selectivity-cost-model-plan.md` (deleted `50ff03c8`;
  `git show 50ff03c8^:...`).
- **Decision B — which side is the build side**
  (`ChunkedInnerJoinOperator::initialize`): `n_right <= kStreamRightThreshold`
  (65536) → build right, else materialize left, compare, maybe swap.

Only B was ever what the operator split needed, and B **does not block it**: the
adaptive path already drains the entire left child into a `Table` before
deciding (and the right is already a whole `Table` at the call site), so it's
already barrier-shaped. A non-JIT engine can construct the probe pipeline after
the build barrier for free — Umbra must decide at compile time because it
generates code; Ibex has a freedom it was declining to use. Have `JoinPlan`
carry both children as pipelines + a runtime-resolved orientation.

### Where join time actually goes (2026-08-25)

Joins are 96.7% of q05's wall, 80.2% of q08, 74.9% of q21, 67.1% of q19.

- **`assemble_output` does not dominate** — 14% of join self-time, retiring the
  `project_join_parallelism` "assemble_output dominates" note.
- **The serial hash build is the real cost, concentrated.** Builds are 29% of
  join self-time, but 70% of that is ONE build: **q21 hashes 1,285,828 rows in
  40.0 ms** in a query whose whole wall is 75–79 ms. `build_join_hash_index` is
  a single no-fan-out loop (31 ns/row, `heads.reserve(rows)` sized by row count
  not distinct estimate). Umbra builds morsel-parallel. **This is the join-perf
  target** and it lands naturally after the structural work — a `HashBuild`
  that's its own scheduled phase CAN be morsel-parallel; one buried in
  `initialize()` cannot. (q21 is also
  `project_query_shape_conformance_regression`'s dominant gap and
  `project_q21_is_occupancy_bound` — possibly the same finding from three
  directions; check before working any.)

### The determinism constraint — recorded broken 2026-08-25, RESOLVED 2026-08-27

**Update 2026-08-27:** the divergence below does not reproduce on the current
tree. The serial probe path, the `try_owned` path, and a strict-row-order
reference all agree bit-for-bit at every thread count — verified via
unit-test `==` (`tests/test_interpreter.cpp` "two-key grouped aggregate is
deterministic across thread counts", the guard test this section called for)
and via an interleaved-A/B result hash (1c vs 8c vs base) on q18/q20/q21.
Reconciled by the Aug-25→27 aggregate commits (`04d56853`, `d1cfcaa0`,
`f675397e`, `d5928ee2`). Removing `try_owned`'s schedule gate outright was
tried and reverted (1-core q20/q18 +25%/+40%, correctness byte-identical).
Slice 1 of the decomposition landed on top. Original finding, kept for the
record:

`3f923086`, before any decomposition. `t[select { s = sum(v) }, by { g1, g2 }]`,
400k rows, 221 groups, ~1 ULP of 1.8e12:

```
IBEX_CORES=1        →  1810000090365.2402
IBEX_CORES=2,3,5,8  →  1810000090365.2397   (all agree bit for bit)
```

`IBEX_DISABLE_OWNED_PAIR_AGG=1` at 8c reproduces the 1-core answer, so
`try_owned_pair` is the sole differentiator. Against an exact reference
(ascending-order summation): the **owned-pair path matches 221/221**, the
**serial path differs 219/221** — it is the *serial* path that re-associates
(partitions by row count, merges partials inline, per `e07445ca`'s ungrouped
design), while the owned path sums in plain row order. Both internally
consistent; they disagree, and which runs is gated on `can_fan_out()`. Blast
radius: two-int-key grouped `Sum(Double)`/`Count`, single aggregate, ≥65536 rows
(the q18/q20 shapes). **No test covers this** — the four "deterministic across
thread counts" cases cover update lifting / rank / collect / ungrouped
aggregate, nothing mentions the owned path. Item 3 needs an exact-equality
grouped-path gate written first; it will fail on the current tree until the
serial/owned split is reconciled.

## Migration phases

### Phase 0 — freeze contracts, gain observability — RESOLVED BY DISPOSITION

Phase 1 started directly. Contract documentation → deferred, landed as
`src/runtime/CONTRACTS.md` (2026-08-22): the one-place statement of `Chunk` /
`sequence`/`row_offset` index space / `next()` pull protocol / materialization /
`TableProperties` / source-demand / determinism, each pointing at its owning
header and naming known hardening debt (extern-source dictionary sharing, sink
validity widening). Structured comparator already existed
(`runtime::compare_tables` / `table_compare.hpp`, in CI). Physical-plan debug
dump → landed inside Phase 1 (`explain_physical`). Corpus → covered de facto by
the 1651-test suite + `tests/test_physical_plan.cpp` (fallback-node cases,
plan-shape/counter assertions distinguishing "unsupported physical shape" from
"incorrect result"). Still missing: a properties baseline (folded into Phase 2's
kernel-capability declarations); a named serial-vs-chunked runner comparing
structurally rather than by stdout diff.

### Phase 1 — logical→physical planning beside `build_operator` — LANDED (2026-08-22)

`src/runtime/physical_plan.{hpp,cpp}` — `MapPipeline` (source + top-down map
steps) + `MaterializedCall` with a structured reason, `explain_physical`,
process-wide path counters. `build_operator_impl` consults the planner serially;
a migrated plan composes the identical operator chain through
`build_physical_map_step`. Validated: full ctest + 22/22 answers under both
`IBEX_PARALLEL` settings. Deliberate notes:
- **Serial executor only.** With `parallel` on, the island seam stays the
  pipeline's executor (an island *is* the parallel mode of a map pipeline); a
  chain the seam declines at its root does not route into the planner (the old
  recursion may still form an island around a shorter eligible sub-chain).
- Filter/project fusion is physical: the planner lowers the ordinary tree into
  one fused map step.
- **Update row-locality mirrors the switch, not `execution_capability`** —
  capability declines a bare row-local update for island copy-cost reasons (an
  execution choice the plan must not inherit as a shape decision). `is_map_step`
  duplicates the switch's gate and must move in step with it.
- The plan **borrows the IR** and must not outlive it.

### Phase 2 — migrate row-local execution to reusable kernels — COMPLETE except `KernelContext`

`src/runtime/CONTRACTS.md` + `src/runtime/kernel_types.hpp` (non-owning
`ColumnView<T>` fixed-width, `ChunkView` position-addressed, `Selection` =
`RowRange`/`RowIndices`/`RowBitmap`/`RowWordBlocks`). API lesson banked: validity
is a **raw pointer** on the view, never `const std::optional<ValidityBitmap>&`
(an optional-by-const-ref materializes a temporary and dangles the view).

| Item | State |
|---|---|
| 1. View/selection/validity/output-writer/scratch APIs | Views, `Selection`, fixed-width/bool/string/validity output writers exist and are used. **`KernelContext` does NOT exist** — scratch/cancellation/RNG/counters passed ad hoc. |
| 2. Port filter/project/rename/row-local update kernels | Filter: **every representation** (`kernel_gather.hpp` — `gather_selected` over all four `Selection` shapes; bit-packing helpers moved verbatim from `filter.cpp`, shared with the two-phase filter). Project/rename: `map_chunk` metadata map (shares `ColumnEntry` handles, no `Table` round-trip). Row-local update: one `try_direct_update_field` dispatch (`kernel_update.hpp`) shared by both executors — `DirectFieldPlan` (compiled numeric TREE, temporal, string-length), `DirectPredicatePlan`, `DirectValidityPlan`, `DirectCategoricalPlan`, `DirectStringPlan` (count/prefix/write); multi-field folding + parallel-mode splitting in the kernel; grouped `update …, by k` off an immutable `GroupedRowPlan` (CSR, no per-group `Table`). **Remaining gap: legacy null-handling arms + anything only the general evaluator reaches (a string result has no numeric window to pre-size) still convert to a `Table` in parallel mode.** |
| 3. Static dispatch tables + capability declarations | Landed: `MapKernelCapability` + `MapKernelFactory` stored per step in `physical::Plan`; `ColumnKernelSignature` recorded for resolved scan sources; `build_row_local_map_operator` consumes the capability (one vocabulary across serial + parallel paths). |
| 4. Run the physical map pipeline serially, then on the morsel executor | **DONE** (`32f62261`, `0b4150d6`, `8e31700a`). A breaker is a source kind (`SourceKind::MaterializedInput`); the plan owns `PipelineMode` (Serial / MorselParallel) + `parallel_begin`/`parallel_end`; `analyze_parallel_island` / `ParallelIslandCandidate` **deleted**. |
| 5. Retire `FilterProject`/`FilterUpdateProject` as execution node kinds | **DONE** — the planner fuses both ordinary shapes; canonicalize R5/R6 are deleted and the legacy node kinds/types are deleted. |

Lessons from the port: a `MapStep` naming two nodes (`Filter` + fused `Project`)
must carry the fusion all the way to the worker or `range_filter_head` absorbs
the `Project` while skipping an `Update` between them (right rows, wrong
columns); `kRules` is a fixed `std::array<...,19>` — dropping two entries without
shrinking it left null function pointers, 942 tests segfaulted; a plan field
that reads like a decision (`plan.mode`) is only a capability until an executor
also checks its own context (`exec.parallel`) — a q19 crash under
`IBEX_PARALLEL=0` that all 1752 tests passed.

**`KernelContext` deliberately not built:** its trigger (one shared
scratch/cancellation owner) is unmet — cancellation has an owner in
`interrupt.hpp`, map kernels share no scratch, the ad-hoc scratch belongs to
breaker operators (Phase 4).

### Phase 3 — the executor owns handoffs and DOP

1. **One executor-owned ordered handoff — DONE** (`89a40abf`, `2b960a21`).
   `OrderedChunkRing` replaces the two near-identical rings (morsel executor +
   scan pipeline) — same window/CVs/slots/backpressure, two bug sets to sync.
   The **stronger** failure model is now shared (sequence-ordered, allocation-
   free `record_fault`, worker liveness through an exit guard — the scan
   pipeline's first-writer-wins exception path is gone). Naming followed:
   The prior island executor was replaced by `MorselPipelineOperator`; stats
   keys stayed unchanged because tooling reads them. **Zero "island" occurrences
   in `src/`/`include/`/`tests/`.**
   `PipelinedStageOperator` keeps its raw thread + plain `std::deque` FIFO (cap
   2, single producer) — deliberately not merged (no sequence ordering to
   maintain).
2. **DOP/memory budgets** — analysed and blocked, see above (item 8).
3. **Migrate islands + pipelined scan/stage to the executor** — DONE, islands
   are a pipeline mode.
4. **Eliminate raw-thread construction from join/builder branches — CLOSED, by
   deletion.** Both sites (build overlapped with materialize on a raw thread)
   measured worse (q09 +57%, then +47.5% under a since-removed helper-thread
   budget; q10 ~−3% didn't survive widening) and were reverted. `chunked.cpp:10002`
   / `:13066` carry the measurements. Branch concurrency needs a **cost-aware**
   gate, not a thread-count one. The only remaining non-pool thread is
   `PipelinedStageOperator`'s (item 1's subject).
5. **Per-pipeline scheduling accounting** — not started, worth more after the
   join/aggregate splits give it phases to attribute to.

**Concurrency-ownership inventory:** raw threads — `WorkerPool` (sanctioned) +
`PipelinedStageOperator` (long-lived, blocks on ring backpressure,
`StageThreadScope` for the profiler). Bounded handoffs — the two sequence rings
(now `OrderedChunkRing`) + the stage FIFO + the pool's own. 41 `pool.submit`
sites (`chunked.cpp` 25) — DOP is seized there; `WorkerPool::submit` calls
`invariant_violation` from a pool thread and 29 sites check
`on_worker_pool_thread()` first (what makes nested parallelism a crash not a
deadlock).

### Phase 4 — migrate the high-value breakers

**Backlog** (`IBEX_PLAN_STATS=1`, PDS-H SF-1, 8 cores): after all
construction-ownership commits — plans 233, pipelines 180, fallbacks 53 (47 bare
`Scan`). **Plan describes 180 of 186 real-work nodes (97%)**, up from 38%. The 6
remaining are materializing joins (`nulls equal` / `expect` / non-equi
predicates — porting them ports the semantics, not the construction).

**Construction ownership DONE** (Join streaming `f5610646`, Aggregate `902d6941`,
Order `ececc75f`, Head/Distinct `49ca33c1`). **Parallelism authority is also
DONE**: Distinct, streaming Join, and streaming Aggregate receive resolved
`BreakerParallelism` from the plan rather than deriving their worker caps and
fan-out permission privately. **Decomposition remains open** — the operators
are still largely unchanged; the branches moved into `build_physical_join` /
`build_physical_aggregate` rather than dissolving into pipeline stages, so the
exit criterion ("fast paths no longer depend on special builder branches") is
**not met**.

**The decomposition target is specified in
[`src/runtime/PARALLELISM.md`](../src/runtime/PARALLELISM.md), "Target:
parallelism as a plan decision"** — the `BreakerParallelism` descriptor, the
planner-vs-operator split (same one `JoinPlan` already made), the `explain
physical` format, the observability-before-authority slicing, and the sequence
(Distinct → Order/TopK → Join → Aggregate; the determinism reconciliation is
complete, so the remaining blocker is structural decomposition).

*Method note (decided the outcome twice):* each port = name the builder's own
predicates + de-duplicate, have the planner **relay** them, have the seam
consume the plan, move construction. A planner that *restates* the gates
produced a wrong classification within the hour (`plan_join` + two-key Int64,
`6de3956d`) and an equivalence probe built from the same reading agreed with the
mistake. Order/Head/Distinct skipped the first three steps — no eligibility gate
at all (a one-valued strategy enum would be ceremony).

1. **Hash join** — construction DONE; **data side DONE** (`5918b5cc`, `8a644381`,
   `f6a1a632` — build returns an immutable `JoinHashIndex`; `JoinProbe` consumes
   one via `shared_ptr<const>` so writing build state during a probe is a
   compile error); **map-pipeline probe fusion DONE**; **explicit physical
   `HashBuild`/`HashProbe` DONE 2026-08-29**. The build produces a move-only
   `HashProbeInput` whose variant fixes Stream / Swapped / Precomputed
   orientation; the physical probe consumes it, and the temporary coordinator
   is discarded at the barrier. Semi/anti deliberately retains its separate
   streaming operator. **Column binding follow-up DONE 2026-08-29:**
   `JoinColumnMapping` resolves mapped left/right keys to positions together
   with the authoritative output plan; known closed schemas bind in the
   physical planner, lazy/unknown schemas bind once at the concrete barrier,
   and probe kernels no longer look columns up by textual key per chunk. NOT
   blocked on a cost model.
2. **Hash aggregate** — construction, positional column binding, fan-out
   authority, physical-plan mutation coverage, and the four-node structural
   hash-fallback chain DONE. Serial orchestration, the bounded
   discovery→accumulation transfer, fused-result marker, final-ordering/emission
   split, and per-node accounting are also DONE. The former determinism blocker
   is resolved. `StreamingSorted` is the historical name for an adaptive
   strategy: sorted group-at-a-time when possible, hash fallback otherwise
   (including ordinary generated tables). Each structural node now owns its
   fan-out policy. The next step is extracting this completed aggregate family
   from `chunked.cpp` behind the existing physical planner/executor seam.
3. **Distinct + ordered** — construction DONE; `Tail`/`TopK`/`FilterHead`/
   `FilterTail` ported too (see "Next" item 4). The whole Head/Tail/TopK/Filter*
   family and Distinct/Order now leave the per-kind switch.
4. Delete the `chunked.cpp` classes only after the physical path handles every
   supported shape and the fallback is mutation-tested. Join and aggregate no
   longer block extraction; begin with the aggregate family as the next slice.

### Phase 5 — retire the monolith, simplify IR

1. Split by ownership: `physical_planner`, `pipeline_executor`, `kernels/`, one
   file/family per breaker.
2. Move logical fusion/selection out of `ir::NodeKind` — **DONE** for
   `FilterProject` / `FilterUpdateProject`: both legacy types and their
   compatibility lowering are deleted.
3. Remove obsolete `build_operator` recursion; migrate `interpret_node` to an
   explicit physical fallback adapter.
4. Make planner / executor / kernel tests independently runnable.

Exit: `chunked.cpp` no longer exists as a monolithic execution/planning unit.

### Follow-up sequence

1. **Deferred-probe threshold regression — DONE 2026-08-29.** Force a deferred
   right side to resolve below `kStreamRightThreshold`, then assert serial and
   parallel byte-identity and that the deferred build/probe path is reached.
2. **Explicit physical `HashBuild` and `HashProbe` nodes — DONE 2026-08-29.**
   The data-only plan has distinct typed nodes connected by a
   `RuntimeOrientedBuildOutput` edge; both retain the candidate inputs, and
   `build_physical_join` consumes their policies. At execution the build moves
   a Stream / Swapped / Precomputed `HashProbeInput` across that edge and the
   probe owns all downstream work; the enclosing coordinator is discarded.
   Edge mutations are rejected by the same validator execution calls, and
   materializing plus semi/anti shapes carry no inner-join edge. The immediate
   name-resolution audit is also complete: the edge carries one
   `JoinColumnMapping` (positional keys + output provenance), resolved at plan
   time when possible and once at execution otherwise.
3. Split aggregate execution at its existing ownership boundaries — discovery /
   partition accumulation / final ordering / emission — first with serial
   orchestration and plan-shape/accounting tests, then admit fan-out one phase
   at a time with byte-identity checks. The typed plan shape and edge-mutation
   tests, serial orchestration, bounded discovery transfer, fused marker, and
   independent profile accounting are complete. Each structural node now owns
   and supplies its fan-out policy, with byte-identity and profile-backed
   worker-ceiling mutations. This step is complete; extraction in item 5 is
   next.
4. Add per-phase scheduling accounting only after steps 2–3 provide stable
   pipeline identities. Keep DOP/memory budgeting blocked unless those changes
   produce measured queue contention or a multi-producer consumer.
5. Move the resulting planner, executor, kernels, join, and aggregate families
   out of `chunked.cpp`; replace residual recursion with the explicit physical
   fallback adapter, preserving mutation-tested `MaterializedCall` coverage.

## Acceptance gates (every phase, before the next starts)

Full parser/IR suite; byte-identical serial + parallel across chunk grains
(null / string / categorical / empty-schema / error / interruption); mutation
tests proving a migrated query reaches the physical implementation and
unsupported semantics reach the fallback; plan-shape assertions (pushdowns,
properties, barriers, fallbacks inspectable); measurements per `MEASURING.md`
(discriminating micro-case → interleaved A/B → PDS-H at pinned 1/2/4/8 cores; no
performance result without a byte-identity check + accounting closure). Bar:
**no regression** for Phase 1 and a migrated serial map path; a multicore
improvement accepted only when end-to-end wall time improves under the paired
protocol, not pool work or a kernel microbenchmark.

## Risks

| Risk | Mitigation |
|---|---|
| A "physical IR" mirrors every logical node | Begin with source / map pipeline / hash build+probe / materialize / fallback only. |
| Template explosion | Specialize on representation + null/selection policy first; runtime params for uncommon expression details; measure compile size/time per family. |
| Kernel extraction regresses the tuned path | Port existing kernels behind the interface before changing algorithms; keep a direct fast path where the abstraction demonstrably costs. |
| A new executor becomes an unmeasured scheduler project | Preserve the pool + outermost-wins; require queue/occupancy evidence before work stealing or branch concurrency. |
| Fallback hides most plans forever | Every fallback explicit in `explain physical`, profiled, backlog keyed by measured cost. |

## Relation to Umbra

Umbra's relevant design is the staged path AST → relational algebra → execution
IR → pipeline-oriented morsel execution, not its machine-code backends. Ibex
substitutes a precompiled templated/vector-kernel backend for Umbra IR + Flying
Start/LLVM — a conscious trade: low implementation and query-startup complexity,
plan/execution separation for a composable kernel library. See Kersten, Leis,
Neumann, *Tidy Tuples and Flying Start* (VLDB Journal 2021).
