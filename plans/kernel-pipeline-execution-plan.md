# Kernel-oriented pipeline execution

Status: **in migration** (was: proposed architecture plan). Phase 0 resolved
by disposition, Phase 1 landed 2026-08-22, Phase 2 in progress — its
"Where Phase 2 stands" table is the current, item-by-item state. Phases 3-5
not started. The goal is FEATURE PARITY on this architecture: every shape the
old execution seams supported reaches either a kernel or an explicit,
inspectable fallback. This is a deliberate replacement for the current
execution architecture over several migrations, not a promise to rewrite the
runtime in one change. It borrows Umbra's useful separation of
logical planning, physical pipelines, and morsel execution, while explicitly
rejecting query JIT/code generation. Ibex's execution backend remains compiled
C++ with a broad library of specialized, template-instantiated vector kernels.

Read [MEASURING.md](../MEASURING.md) and
[parallelism-overview.md](parallelism-overview.md) first.
`chunked-execution-plan.md` — formerly the third input here — was removed
from the tree on 2026-08-22 (in git history); this plan owns
the *architecture* that replaces its ad-hoc execution seams and does not
invalidate its correctness contracts, chunked-source work, or
existing measured kernel improvements. Its three open items were absorbed:
the extern chunked-source contract (stable schema, ownership, categorical
dictionaries, EOF/error signalling) belongs with Phase 0/2's contract
documentation and bigger-than-ram's Phase 4;
`MaterializeOperator`'s schema/dictionary/validity-across-chunks hardening
is Phase 2's validity-API work; the materializing breakers are Phase 4/5's
own list. Its R1–R21 canonicalize table is documented in-source
(`include/ibex/ir/canonicalize.hpp`).

## Why make this change

`src/runtime/chunked.cpp` is the physical planner, the implementation of most
streaming operators, the home of parallel islands and pipelined stages, and a
large set of operator-specific eligibility rules. Those responsibilities have
grown together because `build_operator(const ir::Node&)` lowers logical nodes
directly into mutable `Operator::next()` objects.

That coupling has three costs:

1. **A physical choice has no representation.** “Stream this two-key join”,
   “materialize this aggregate”, and “run this row-local chain as an island”
   are branches inside the builder rather than inspectable plan decisions.
2. **Parallelism is an operator-local afterthought.** A kernel can fan out, but
   a query cannot naturally expose source-to-breaker work as an independently
   scheduled pipeline. The measured idle pool is therefore usually empty, not
   queued behind a poor scheduler.
3. **Specialisation and semantics are entangled.** One source file decides
   logical eligibility, output ordering, source safety, execution mode,
   pipeline buffering, and the typed fast path. Adding one fast path makes
   every other concern harder to audit.

The goal is not a generic task graph, work stealing, or JIT. It is a physical
plan composed of explicit pipeline stages, each executing a carefully selected
C++ kernel over a vector/morsel. The result should make both serial and
multi-core execution easier to improve without weakening Ibex's byte-identical
answer contract.

## Non-goals

* No LLVM, assembler, runtime C++ compilation, bytecode, or query JIT.
* No big-bang AST replacement and no surface-language change.
* No general DAG scheduler, cross-query concurrency, or work stealing unless
  later measurements show queued work that requires it.
* No loss of pushdown, categorical encodings, null semantics, source-global
  selection, deterministic error selection, or byte-identical output.
* No automatic removal of all materialization: order, global aggregate, window,
  reshape, and rich join semantics may remain real barriers.

## Target architecture

```text
parser AST
    |  parse, spans, syntax sugar, functions and blocks
    v
typed logical IR
    |  relational semantics, schema/properties, optimizer rewrites
    v
physical plan
    |  algorithms, pipelines, barriers, source demand, DOP/memory policy
    v
pipeline executable
    |  kernel selection and typed state layouts
    v
morsel executor
    |  source partitions, bounded handoffs, cancellation, ordered merge
    v
Table / stream sink
```

### 1. Preserve the AST; narrow its job

The parser AST should continue to represent user syntax, source spans,
functions, blocks, imports, effects, and syntactic sugar. It is not the place
to represent execution pipelines. The existing lowering phase should be made
the one-way boundary: after semantic analysis, no runtime component inspects
`parser::ast`.

Do not redesign AST ownership or replace its `variant` representation as part
of this project. That is a high-risk language/compiler migration with no
credible path to the multicore result. Reconsider it only if a concrete
language feature exposes a lowering limitation.

### 2. Make logical IR purely logical

The logical IR owns relational meaning:

* scans, filters, projects, joins, aggregates, order, windows, reshape, model;
* expression and null semantics;
* schema, ordering, unique-key, nullability, and origin proofs;
* optimizer rewrites such as projection/predicate pushdown and join ordering.

It must not encode execution accidents such as `FilterProject`, `TopK`,
`is_streamable_inner_join`, a particular join key representation, or whether a
source is safe to call from a worker. Existing fused logical node kinds may
remain as compatibility input during migration, but the optimizer should
eventually express fusion as a physical pipeline choice.

The logical plan is immutable once optimization finishes. Give every node a
stable, printable identity and make its derived properties available through a
single analysis result, rather than recomputing ad-hoc facts in physical
builder branches.

### 3. Add an explicit physical plan

Introduce a private `runtime::physical` plan with data-only, inspectable nodes.
It is the sole owner of algorithm selection and execution capability.

Suggested first vocabulary:

| Class | Physical nodes | Contract |
|---|---|---|
| Source | `TableScan`, `LazyScan`, `ExternSource` | produces numbered morsels and source-global positions |
| Pipeline map | `Filter`, `Project`, `Update`, `Rename` | vector in → vector out; fusable with adjacent maps |
| Build barrier | `HashBuild`, `HashAggregateBuild`, `SortBuild`, `Materialize` | consumes a stream before the dependent phase starts |
| Probe/output | `HashProbe`, `AggregateEmit`, `OrderedMerge` | consumes morsels using immutable or partition-owned build state |
| Fallback | `MaterializedCall` | explicit adapter for as-yet-unmigrated semantics |

Each node declares, rather than implies:

* input/output schema and `TableProperties` transfer;
* streaming, blocking, or semi-blocking behavior;
* partitionability and output-order rule;
* worker-local, partition-owned, and query-global state;
* memory estimate/limit and maximum useful DOP;
* supported kernel families and a reason when the fast path declines.

`explain physical` (initially a test/debug formatter) must print this plan,
including barriers, selected kernels, DOP allocation, source demand/pushdowns,
and fallback reasons. A plan that cannot explain why it materialized is not an
acceptable replacement for the current branches.

### 4. Pipeline units, not a query DAG scheduler

A pipeline is a maximal chain from a partitionable source or a barrier output
to its next breaker. A pipeline has one producer contract and one consumer
contract; a breaker owns its build state and creates the next pipeline after
its build phase completes.

The executor schedules numbered morsels for a pipeline using the existing
process pool and query lease. Workers dynamically claim source ranges, run the
pipeline's selected map kernel, and publish ordered results to a bounded
handoff. The caller remains the ordered consumer. A pipeline is free to be
serial when its cost model declines fan-out.

This is intentionally smaller than a general scheduler:

* no nested pool submissions—outer pipeline DOP wins;
* no work stealing or futures;
* no concurrent independent branches until explicit DOP partitioning and
  bandwidth admission are measured worthwhile;
* no raw `std::thread` escape hatches outside one executor-owned handoff API.

The immediate benefit is not magical overlap. It is that the query has a
first-class, auditable location to create it when an algorithm actually can
produce independent morsels.

### 5. A broad, templated kernel library

Replace query-specific generated code with a small number of composable kernel
interfaces and many statically compiled specializations.

```cpp
template <class Type, class NullPolicy, class Selection, class Expr>
void filter_project_kernel(const ColumnView<Type, NullPolicy>& input,
                           Selection& selection, OutputWriter& output,
                           const Expr& expr, KernelContext& context);
```

The exact signatures may differ; the ownership model must not:

* `ColumnView`/`ChunkView` are non-owning and carry length, validity, encoding,
  and offsets explicitly.
* `Selection` is an explicit bitmap/index/range choice, never a hidden
  `std::vector` convention.
* `OutputWriter` owns pre-sized disjoint output ranges or a worker-private
  buffer; variable-width output follows count → prefix sum → scatter.
* `KernelContext` owns scratch, cancellation checks, deterministic RNG stream,
  and profiling counters, never query-global mutable state.

Start with closed dispatch at pipeline construction, not virtual calls per row:

1. Select data representation (fixed width, bool bitmap, string,
   categorical), null policy, expression shape, and selection shape once.
2. Construct a typed `PipelineKernel` closure/function pointer for that
   combination.
3. Call it per morsel. It may call two or three composable kernels, but no
   logical-node visitor or `variant` dispatch belongs in the row loop.

Kernel families should be shared across operators. For example, the same
gather, validity, scatter, hash, and expression kernels serve filter output,
join assembly, sort gather, and aggregate emission. This formalizes the
successful convergence work already done around gather behavior.

### 6. Ownership-oriented breakers

The initial physical algorithms should follow state ownership, not an attempt
to make every node universally streaming:

* **Hash join:** one build-side state, immutable after build; probe pipeline
  streams source-order morsels; each morsel emits its own match segment;
  ordered merge reconstructs probe order.
* **Hash aggregate:** fixed logical hash partitions own key maps and aggregate
  slots; workers claim partitions/morsels; deterministic first-row metadata is
  retained for final P-way ordering. Partition count is data-derived/fixed,
  never core-count-derived.
* **Order/distinct/window:** begin as explicit barriers with reused kernels;
  migrate only once their cross-morsel state and output rules are specified.
* **Fallback semantics:** `MaterializedCall` is honest and profiled. It
  preserves rich operators (median, quantile, EWMA, predicates, reshape, etc.)
  until a physical implementation is complete.

## Where this stands, and what is left (2026-08-24)

One ordered list, because the per-phase status below is spread across five
sections and the headline number (97% of real-work nodes described by the plan)
flatters it: that measures who CONSTRUCTS operators, not how they are shaped.

**Done.**

* Phase 1 — physical plan exists and is inspectable.
* Phase 2 — map kernels ported, fusion is physical, fused node kinds retired as
  an execution concern. `KernelContext` deliberately not built: its stated
  trigger (one shared scratch/cancellation owner) is still unmet.
* Phase 3 items 1, 3, 4 — one ordered handoff owned by the executor, islands
  dissolved into a pipeline mode, no raw-thread branch concurrency.
* Phase 4 construction ownership — every breaker PDS-H reaches is built from
  the plan. Backlog 116 -> 6 breakers.

**Next, in the order I would take them.**

1. **Split the join operator, with the orientation resolved at run time.**
   Re-ordered ahead of the aggregate on 2026-08-25; the previous text called
   this "blocked on the run-time build-side choice", and that was an
   assumption rather than a constraint. See "The build-side choice does not
   block the split" below. No cardinality estimate, no behaviour change:
   the same `n_right <= kStreamRightThreshold` / materialize-and-compare
   logic relocated into the build phase, which publishes the index *and*
   which side it indexed. This is `8a644381`'s pattern applied to the
   operator instead of the data.
2. **Make the probe a step inside a map pipeline.** What (1) exists for:
   morsels come from the probe source, the filters and projections above the
   join fuse into the same morsel loop instead of materializing between
   them, and one build can feed several probes (the index is already
   `shared_ptr<const>`). **Needs a performance argument before it is
   scheduled:** the one this list first gave it -- that
   `probe_parallel_workers`' `on_worker_pool_thread()` veto is serializing
   nested joins -- was measured and is false on PDS-H (0 of 52 declines).
   See "The build-side choice does not block the split" below.
3. **Phase 4 decomposition of the aggregate** — discovery / per-partition
   slots / final ordering / emission. Constraint inherited from `e07445ca`:
   any partition-owned aggregate must keep the merge order
   thread-count-invariant — serial, one core and eight must agree bit for
   bit. **Measured 2026-08-25: this constraint is already violated on the
   current tree, before any decomposition work.** See "The determinism
   constraint is already broken" below; it has to be reconciled first, and
   there is no test that would have caught it.
4. **Port `Tail`, `TopK`, `FilterHead`, `FilterTail`.** Same single-operator
   shape as Order/Head/Distinct, half an hour each, and it removes the
   asymmetry of ported and unported siblings sharing one switch.
5. **Phase 5 item 1 — split `chunked.cpp` by ownership.** It is ~13k lines and
   now holds planner-owned builders next to the per-kind switch they replaced;
   the split is easier the more of Phase 4 has landed, not less.
6. **Sweep the process-global plan counters in tests.** One test
   ("Fallback queries keep their existing executor") was PASSING while its
   premise was false, because `physical_materialized_calls` is process-wide and
   other tests in the same binary bump it. ctest never flagged it. Others may
   lean on the same counters the same way.
7. **Phase 3 item 5 — per-pipeline scheduling accounting.** Small, and worth
   more after (1)-(3) give it phases to attribute to.
8. **Phase 3 item 2 — DOP/memory budgets.** Analysed and blocked, see
   `phase3-dop-budget-analysis.md`: the pool is 65% idle with nothing queued,
   so a budget rations a resource that is not scarce. Reopen only when a
   multi-producer change needs it, or if `profile_suite.py` starts showing
   queues.

**Deliberately not doing.**

* Porting the 6 materializing joins. They are the explicit `MaterializedCall`
  the plan wants, not a gap.
* A cost model for join build-side selection *as a prerequisite for the
  split*. Real as a planner-quality question (it is q12's diagnosed
  regression), and `plan_join` records today's choice rather than pretending
  to make one — but it is not on this migration's critical path. Once (1)
  lands it becomes a narrow optimization with one target: deleting the
  `MaterializeOperator(left)` barrier that exists purely to measure a row
  count. Sequenced after (2), measurable on its own.
* Deferred-probe registration selectivity (`build_side_worth_deferring`,
  `src/ir/scan_predicates.cpp`). A DIFFERENT decision from the build-side
  choice — see "Two decisions, routinely conflated" below. Scan-layer work,
  its own schedule, not this plan's.

### Two decisions, routinely conflated (2026-08-25)

Two things in the tree are both called "the join cost model". They live in
different files, answer different questions, and have different failure
modes. Keeping them apart is what re-ordered the list above.

**Decision A — deferred-probe registration.** `build_side_worth_deferring`,
`src/ir/scan_predicates.cpp`. Should the build side be eagerly materialized
so its key bounds can be pushed into the probe scan's Parquet decode? Current
rule: `build_est.rows * 2 < probe_rows`. This is a DECODE-PRUNING question.
It is what `plans/deferred-probe-selectivity-cost-model-plan.md` scopes with
its `column_origin_of` domain-size ratio — that file was deleted from the
tree in `50ff03c8` ("Clean up plans in the tree") and reads from
`git show 50ff03c8^:plans/deferred-probe-selectivity-cost-model-plan.md`.

**Decision B — which side is the build side.**
`ChunkedInnerJoinOperator::initialize`, `src/runtime/chunked.cpp`. Rule:
`n_right <= kStreamRightThreshold` (65536) build on right, else materialize
left, compare `n_left < n_right`, possibly swap.

Only B is what the operator split ever needed. Landing A would leave the
split exactly as blocked as it is today, so "blocked on the join cost model"
was pointing at the wrong artifact.

### The build-side choice does not block the split (2026-08-25)

What the adaptive path actually does when it "cannot decide statically":

```cpp
auto left_res = MaterializeOperator(std::move(left_)).run();   // chunked.cpp
```

It drains the entire left child into a `Table` BEFORE deciding, and the right
side is already a whole `Table` by construction at the call site. In the
large-right case the operator pays TWO full barriers, and the orientation is
chosen after one side is fully materialized. The adaptivity is therefore
already barrier-shaped: it is not an obstacle to `HashBuild + HashProbe`
across a barrier.

The one thing that genuinely wants a plan-time decision is naming the probe
pipeline's SOURCE ahead of time. Ibex does not JIT — constructing the probe
pipeline is allocating operator objects, which can happen after the build
barrier completes at a cost of nothing. Umbra must decide at compile time
because it generates code; this engine has a freedom Umbra does not and was
declining to use it.

So the blocker was an assumption — *"the physical plan must name the build
side statically"* — not a constraint. Have `JoinPlan` carry both children as
pipelines plus a runtime-resolved orientation, the same way `plan_join`
already RELAYS a branch rather than pretending to decide it, and the
two-operator structure lands with no cardinality estimate at all.

**The parallelism gap this exposes, which is the actual prize.**
`JoinProbe::probe_parallel_workers` (`chunked.cpp`) declines fan-out when
`on_worker_pool_thread()`, per the no-nested-submission rule. So Ibex
parallelizes INSIDE operators, and a join nested under any other fan-out runs
its probe serially. Umbra parallelizes the PIPELINE; the operator is serial
code run once per morsel, so a probe under another parallel region is
parallel by construction and the veto has nothing to veto. That — not a
better build-side choice — is what the split buys, and it is why item (2)
above is the one that matters.

*Premise MEASURED 2026-08-25, and it does not hold.* `probe_parallel_workers`
was instrumented to record why it declined, over all 22 PDS-H SF-1 queries at
8 cores (`IBEX_CORES=8`, `ibex_eval` per query; instrumentation reverted, not
committed):

```
35  ADMIT
17  too_small     (n < kMinProbeRows = 16384)
 0  nested        (on_worker_pool_thread())
 0  no_fanout
```

Zero. The `on_worker_pool_thread()` veto never fires for a join probe on this
workload, so "it silently serializes every join nested under another fan-out"
was wrong as a statement about what is happening today. It is latent, not
active, and the reason it is latent is itself the finding: **no join probe in
PDS-H ever runs on a pool thread**, because joins are not inside parallel
pipelines at all. Their probes are parallel (35 of 52 admit) purely through
an intra-operator fan-out that works only because the operator sits on the
main thread.

That leaves the Umbra gap real but differently shaped, and item (2)'s payoff
restated honestly: not "unlock parallelism a veto is blocking", but that a
probe inside a map pipeline fuses with the filters and projections above it
instead of materializing between them, and that one build can feed several
probes. Both are unmeasured. Before (2) is scheduled on a performance
argument, one of those two needs its own measurement -- the parallelism
argument as originally written does not survive contact with the counter.

### Where join time actually goes (2026-08-25)

Measured, then stopped -- structure first, perf second. `IBEX_PROFILE_OPERATORS`
plus temporary timers inside `build_join_hash_index` and `assemble_output`,
PDS-H SF-1 at 8 cores, 11 join-carrying queries (instrumentation reverted).

Joins are worth the attention: they are 96.7% of q05's wall, 80.2% of q08,
74.9% of q21, 67.1% of q19, 64.9% of q04, 64.1% of q09.

Two results, one of which retires a standing note.

* **`assemble_output` does not dominate.** [[project_join_parallelism]] records
  "assemble_output dominates" from the probe-threading work. It is 14% of join
  self time across these 11 queries and never more than 10.5% of any query's
  wall; on the two most join-dominated queries, q05 and q08, it is 3.9% and
  1.3% of wall. Fusing the probe into a map pipeline to avoid materializing
  between the probe and the maps above it is therefore a STRUCTURAL argument,
  not a performance one -- which is consistent with the veto measurement above,
  and is why item (2) carries no performance claim.
* **The serial hash build is the real cost, and it is concentrated.** Builds are
  29% of join self time over the 11 queries, but 70% of that total is ONE build:
  q21 hashes 1,285,828 rows in **40.0 ms**, in a query whose whole wall is
  75-79 ms. `build_join_hash_index` is a single loop with no fan-out (31 ns/row,
  including a `heads.reserve(rows)` sized by row count rather than by any
  distinct estimate). Umbra builds its hash table morsel-parallel; this engine
  builds it on one thread.

That is the join-perf target, and it lands naturally after the structural work
rather than before it: a `HashBuild` that is its own scheduled phase over the
build side's pipeline is a thing that CAN be morsel-parallel, where a build
buried in `initialize()` is not. Note also that q21 is [[project_query_shape_conformance_regression]]'s
dominant remaining gap (2.33x, 91% of it) -- these may be the same finding seen
from two directions, which is worth checking before either is worked.

### The determinism constraint is already broken (2026-08-25)

Measured on `3f923086`, `build-release`, before any decomposition work. Same
query, same data, `t[select { s = sum(v) }, by { g1, g2 }]`, 400k rows, 221
groups, values chosen so re-association is visible (~1 ULP of 1.8e12):

```
IBEX_CORES=1        ->  1810000090365.2402
IBEX_CORES=2,3,5,8  ->  1810000090365.2397    (all four agree bit for bit)
```

`IBEX_DISABLE_OWNED_PAIR_AGG=1` at 8 cores reproduces the 1-core answer
exactly, so `try_owned_pair` is the sole differentiator. Checked against an
independent exact reference (plain ascending-order summation over the same
input, all 221 groups): the owned-pair path matches in 221/221, the serial
path differs in 219/221. It is the SERIAL path that re-associates — it
partitions by row count and merges partials inline, exactly as `e07445ca`
designed for the ungrouped case — while the owned path bypasses that merge
and sums in plain row order. Both are internally consistent; they disagree
with each other, and which one runs is gated on `can_fan_out()`.

What IS invariant: inside the owned path, `part_count` varies 2/2/4/8 across
`IBEX_CORES=2/3/5/8` (itself contrary to this plan's "partition count is
data-derived, never core-count-derived") and the answer does not move. The
P-way merge on `first_rows` and the ascending scatter both hold. The defect
is one level up — not inside partition-owned aggregation, but in the choice
of whether to use it.

Blast radius: two-int-key grouped `Sum(Double)` or `Count`, single aggregate,
>= 65536 rows — `try_owned_pair`'s gates, i.e. the q18/q20 shapes. Single-int-key
grouped sum is identical across 1/2/8 cores.

No test covers this. The four `deterministic across thread counts` cases in
`tests/test_interpreter.cpp` cover grouped *update* lifting, rank, collect
aggregates and the *ungrouped* aggregate; nothing in `tests/` mentions the
owned path at all. So item (3) needs an exact-equality gate on the grouped
path written first, and it will fail on the current tree until the
serial/owned split is reconciled — a pre-existing divergence, not something
the decomposition introduces.

## Migration phases

### Phase 0 — freeze contracts and gain observability

**Status: resolved by disposition (2026-08-22), not by execution.** Phase 1
was started directly; this records, item by item, what that skipped, what
already existed, and where the remainder lives — so the phase gate reads as
a decision rather than an accident.

1. **Contract documentation — deferred, now Phase 2's first deliverable.**
   The knowledge exists (parallelism-overview Part 1, the removed
   chunked-execution-plan, the `operator.hpp`/`morsel.hpp` doc comments)
   but not in one place.
   Writing it down is the input to Phase 2's first step (extracting the
   `ChunkView`/selection/validity/output-writer APIs *is* stating the
   contracts), so it lands there as a commit, not here as a promise.
2. **Structured comparator — the artifact already existed.**
   `runtime::compare_tables` (`table_compare.hpp`) is the authoritative
   predicate for schema, metadata, values, validity, and categorical
   code/dictionary backing, and the structured parity gate runs it in CI.
   This also retires serial-parity-comparator-plan.md's (now only in git history)
   deliverable — its README status was stale. Still missing, and folded into
   Phase 2's per-representation gates: a named serial-vs-chunked *runner*
   (same query under `IBEX_PARALLEL=0`/`1`, compared structurally rather
   than by stdout diff — the byte-diff practice stays, but a disagreement
   should print a `TableMismatch`, not a wall of rendered text).
3. **Physical-plan debug dump — landed inside Phase 1** as
   `physical::explain_physical`, including fallback reasons; the test
   formatter is exactly the "changes no runtime behavior" prototype this
   item asked for.
4. **Corpus — covered de facto, not formally.** The 1651-test suite spans
   row-local maps, every column representation, the empty-schema carrier,
   multi-chunk grains, joins, aggregates, and order;
   `tests/test_physical_plan.cpp` added the fallback-node cases and the
   plan-shape/counter assertions that distinguish "unsupported physical
   shape" (a recorded `MaterializedCall` reason) from "incorrect result"
   (a failing byte-identity check). Organizing a named corpus remains
   optional until Phase 2's kernel ports need a fixed regression set.

Exit criterion, honestly assessed: plan capture exists for Phase 1 shapes
(`explain_physical`); a properties baseline does not, and becomes part of
Phase 2's kernel-capability declarations. No performance claim.

Original items, for the record:

1. Document `Chunk`, `Morsel`, `Operator::next()`, materialization, source
   selection/index space, and `TableProperties` contracts in one place.
2. Add structured serial-vs-chunked comparison for values, schema, validity,
   categorical backing, ordering, time index, and error selection.
3. Add a physical-plan debug dump prototype for a small read-only logical-plan
   walker; it must change no runtime behavior.
4. Establish a corpus spanning row-local maps, all column representations,
   empty input/schema carrier, multi-chunk source grains, joins, aggregates,
   order, window, and a fallback node.

### Phase 1 — introduce logical-to-physical planning beside `build_operator`

**Status: landed (2026-08-22).** `src/runtime/physical_plan.{hpp,cpp}` define
the Phase 1 vocabulary — `MapPipeline` (source + top-down map steps) and
`MaterializedCall` with a structured reason — plus `explain_physical` and
process-wide path counters. `build_operator_impl` consults the planner
serially; a migrated plan composes the identical operator chain through
`build_physical_map_step`, with per-step profile entries nested exactly as
the per-kind switch's recursion produced them. Validated: full `ctest`
(1651, including interpreter/codegen parity) and 22/22 `check_answers.py`
under both `IBEX_PARALLEL` settings. Implementation notes, all deliberate:

* **Serial executor only.** With `parallel` on, the island seam stays the
  pipeline's executor (an island *is* the parallel mode of a map pipeline),
  and a chain the seam declines at its root does not route into the planner:
  the old recursion may still form an island around a shorter eligible
  sub-chain (an ineligible outer predicate does not make an inner projection
  ineligible), which a whole-chain plan would silently serialize.
* **Fused kinds are first-class steps.** Canonicalize R5/R6 mean the tree
  that exists is `FilterProject`/`FilterUpdateProject`, not
  `Project(Filter(...))`; the planner lowers the tree as built.
* **Update row-locality mirrors the switch, not `execution_capability`.**
  Capability also declines a bare row-local update, but for island copy-cost
  reasons (updates parallelize inside the operator); that is an execution
  choice the plan must not inherit as a shape decision. `is_map_step`
  duplicates the switch's own gate (no guard, no `by`, no tuple fields, all
  fields `is_row_local_update_expr`) and must move in step with it.
* **The plan borrows the IR** (like `ParallelIslandCandidate`) and must not
  outlive it; `Plan::root`/`steps`/`source_node` are non-owning.
* Source classification (`TableScan`/`LazyScan`/`ExternSource`) and the
  three `FallbackReason`s cover the shapes the tests exercise; the vocabulary
  grows when Phase 2 kernels need more than `const ir::Node*` steps.

1. Define the physical node types and immutable `PhysicalPlan` arena/value
   ownership.
2. Lower only `Scan → Filter → Project/Rename → row-local Update` into the
   physical plan; all other nodes produce explicit `MaterializedCall` barriers.
3. Implement `explain physical` tests covering selected maps, pushdown demand,
   materialization reasons, properties, and decline reasons.
4. Keep `build_operator` as the executor for the fallback; it receives the
   *lowered physical plan*, not a raw logical node, on the migrated path.

Exit: simple queries use the new planner and have exactly the old outputs and
source pushdowns, with no required performance win.

### Phase 2 — migrate row-local execution to reusable kernels

**Open; items 1–3 are substantially landed for the filter/map/update families,
items 4–5 are untouched. See "Where Phase 2 stands" below for the item-by-item
state and the gaps that assessment does NOT claim.** The opening deliverable — the contract documentation Phase 0's
disposition promised — landed as `src/runtime/CONTRACTS.md` (2026-08-22):
the one-place statement of the `Chunk`, `sequence`/`row_offset` index space,
`next()` pull protocol, materialization, `TableProperties`, source/demand,
and determinism contracts, each pointing at its owning header and naming the
known hardening debt (extern-source dictionary sharing, sink validity
widening). Item 1's first slice also landed (2026-08-22):
`src/runtime/kernel_types.hpp` — non-owning `ColumnView<T>` (fixed-width
representations only, per the one-at-a-time order; bool/string views come
with their own access shapes), `ChunkView` (position-addressed, carrying the
source index space), and the explicit `Selection` variant
(`RowRange`/`RowIndices`/`RowBitmap`), with tests. One API lesson already
banked: validity is a raw pointer on the view, never
`const std::optional<ValidityBitmap>&` — an optional-by-const-ref silently
materializes a temporary from a bare bitmap and dangles the view. Resume
point: `OutputWriter` + `KernelContext`, then the first kernel port
(fixed-width filter gather) onto the views, one representation at a time.

**First kernel port landed (2026-08-22, `c2debfa6`).**
`kernel_gather.hpp` adds `OutputSpan<T>` (the fixed-width OutputWriter: a
pre-sized disjoint window of a count/prefix-sum-owned buffer) and
`gather_selected(ColumnView, Selection, OutputSpan)` with closed
compile-time dispatch over four selection shapes — `RowRange` (one
contiguous copy), `RowIndices`, `RowBitmap`, and `RowWordBlocks`, the
engine's native 64-row mask, added to the vocabulary for exactly this
port. `filter_table`'s fixed-width and categorical-code arms now call the
kernel; string/bool arms and the shared-word validity gather are the next
representations. `ColumnView`'s constraint became trivially-copyable
value types rather than "a `Column<T>` exists" — categorical codes are a
flat int32 array with no `Column<uint32_t>`, and that gather IS the
fixed-width kernel over codes. Gates: ctest 1657/1657, answers 22/22
both `IBEX_PARALLEL` settings, and the MEASURING.md interleaved A/B
(88 cases) read **-4.71% total / 1.055× geomean, every case `noise`** —
neutral, as a port of the identical loop must be.

**Bool and string representations landed (2026-08-22, `29d6759c`).**
The bit-packing helpers moved verbatim from filter.cpp's anonymous
namespace into `kernel_gather.hpp` (one definition, shared with the
two-phase filter — the I4 lesson applied at extraction time).
`BoolView` hides the internal-words/external-bytes split behind
`source_word(start_row)`; `BoolOutputSpan` is a zero-filled pre-sized
bit window under the shared-word atomic-OR rule. `StringView`/
`StringOutputSpan` carry offsets+chars with `char_base` for the
prefix-summed parallel window; the kernel writes only each row's END
offset, the contract both the serial presize and the window chain rely
on. The filter's string and bool arms now call the kernels — **every
representation of the filter gather is on the kernel vocabulary**.
Gates: ctest 1661/1661, answers 22/22 both settings, A/B −2.33% total /
1.013× geomean with the one non-noise case an improvement
(sort_symbol_price −4.06%) and zero regressions. One build lesson: the
kernel header includes `<immintrin.h>` for `_pext_u64` — the dev build
dirs don't define `__BMI2__`, the A/B's release config does, so a
guarded intrinsic that compiles at home can still break the A/B build.
Next: the validity gather (skip-false monotonic writes), then the
project/rename maps onto `ChunkView`, then the item-3 dispatch table.

**Validity representation ported (2026-08-22, `94ee0249`).**
`ValidityView` now gives the kernel vocabulary the same owned-word versus
external-byte/bit-offset normalization as `BoolView`, and
`gather_selected_validity` compacts it through every `Selection` shape.  Its
destination follows the deliberate parallel rule: it is zero-filled and the
kernel only ORs true bits, leaving false bits unwritten.  Thus adjacent output
windows retain the shared-word atomic-OR safety rule without a clear/write
race.  The filter no longer owns a validity row loop; it constructs the view
and calls this kernel beside the other representations.  Unit coverage pins
selected false bits, an externally offset Arrow-compatible source, and two
adjoining output windows.  Gates: debug ctest 1663/1663 and the release gather
tests (30,228 assertions) pass; the 8-repeat, interleaved A/B against
`HEAD~1` measured +0.32% total / 0.999× geomean over 88 cases, with every case
classified `noise` (and byte-identical output).  Next: project/rename maps
onto `ChunkView`, then the item-3 dispatch table.

**Project and rename maps ported (2026-08-22, `12ff7d4e`).**
`ChunkView` now exposes position-resolved entries, lookup, and properties;
`map_chunk` is the shared metadata-only map kernel.  It copies `ColumnEntry`
handles (therefore shares column payload and validity), applies output labels,
and carries `sequence`/`row_offset` unchanged.  `ChunkedProjectOperator` no
longer round-trips through `Table`; it resolves the selected positions over the
view and derives its dropped-key properties before calling the kernel.
`ChunkedRenameOperator` uses the same map with relabeled properties.  The
kernel test pins shared ownership, validity, labels, and morsel identity.
Gates: debug ctest 1664/1664 and optimized kernel-map tests pass; the
8-repeat interleaved A/B against `HEAD~1` measured −0.39% total / 1.005×
geomean over 88 cases.  Next: Phase 2 item 3's static dispatch table and
capability declarations.

**Phase 2 item 3, first slice (2026-08-22, pending performance gate).**
`MapKernelCapability` declares the closed construction-time family:
filter gather, metadata map, row-local update, filter-project gather, and
filter-update-project gather.  The unconditional members live in one static
table; the conditional Update member remains explicitly shape-checked.  Every
migrated physical-plan step stores its selected capability, giving the plan an
auditable dispatch choice before an operator is constructed.  Physical-plan
tests pin the representative filter-project, metadata-map, and row-local
update choices.  Gate so far: debug ctest 1664/1664.  Next: route the shared
map-operator construction through this capability table, then run the
interleaved A/B.

**Phase 2 item 3, construction routing (2026-08-22, `d738c07d`).**
`build_row_local_map_operator` now consumes
`MapKernelCapability`, the same capability selected into `physical::Plan`,
rather than re-admitting nodes through its own node-kind switch.  This is the
shared factory used by both serial physical-plan composition and parallel
islands, so the construction-time selection is now one vocabulary across both
paths.  Gates: physical serial execution and parallel pipeline tests, then
debug ctest 1664/1664; the 8-repeat interleaved A/B against `HEAD~1` measured
+0.06% total / 0.999× geomean over 88 cases.  This is a neutral dispatch-only
change.  Next: extend capability declarations with representation/null-policy
selection as the physical map executor replaces its remaining chunked adapter.

**Phase 2 item 3, plan-to-factory handoff (2026-08-22, `598b6401`).** The
serial physical composer now passes each capability recorded in
`Plan` directly to the shared map factory; it does not
classify the IR again.  The parallel paths use the same factory through the
one capability-producing wrapper at their construction boundary.  This makes
the plan's dispatch choice executable rather than diagnostic-only.  Gates:
physical execution tests and debug ctest 1664/1664; the 8-repeat interleaved
A/B against `HEAD~1` measured +0.35% total / 0.998× geomean over 88 cases.
This is neutral dispatch-only noise.  Next: representation/null-policy
selection as the physical map executor replaces its remaining chunked adapter.

**Phase 2 item 3, source signatures (2026-08-22, `c43516bb`).**
The physical planner now records a `ColumnKernelSignature` for each resolved
`TableScan` source column: fixed-width, packed-bool, string-slabs, or
categorical-codes, together with all-valid versus nullable policy.  The
signature is a construction-time data fact, not a second type-dispatch path;
the selected map capability remains the execution authority until the physical
executor consumes both inputs.  `explain_physical` renders the compact ordered
signature, so the pending representation-aware selection is inspectable.
Unregistered/lazy and extern sources deliberately carry no signature because
their columns do not exist at planning time.  Tests pin the registered source
shape, a nullable categorical direct signature, and the diagnostic rendering.
Gates: debug physical-plan tests and full ctest 1665/1665; the 8-repeat
interleaved A/B against `HEAD~1` measured +1.19% total / 0.997× geomean over
88 cases (0.999× for ibex-only).  Every case is noise; this metadata-only
slice has no performance signal.  Next: make the map factory consume the
signature when it replaces the remaining chunked adapter.

**Phase 2 item 3, static factory dispatch (2026-08-22, pending performance
gate).** `physical::Plan` now stores a `MapKernelDispatch` for every map step:
the proven capability plus its factory function selected from the closed static
table. The serial physical composer invokes that stored factory directly, so
there is no capability switch at step execution. Parallel-island construction
uses the same table through the compatibility wrapper. Tests pin a non-null
factory for representative filter-project, metadata-map, and row-local-update
plans; physical tests pass 79 assertions and full debug ctest passes
1675/1675. This is construction-only dispatch work, so its automatic check is
informational. Next: consume source signatures in an actual physical map
executor, starting serially.

**Chunk filter-kernel seam (2026-08-22, pending performance gate).** The
ordinary filter and fused filter-project operators now delegate their
Chunk→Table evaluator bridge to `kernel::filter_chunk` and
`kernel::filter_project_chunk`. Those entry points preserve a morsel's
`sequence` and `row_offset`, keeping schema-carrier handling in the operators
while giving the physical executor reusable per-morsel filter kernels. Focused
coverage pins selected values and identity; physical execution coverage passes
48 assertions and full debug ctest passes 1676/1676. Next: extend the seam to
the filter-update-project shape, then replace the bridge internals with the
representation-aware gather path already selected by source signatures.

**Complete filter-family chunk seam (2026-08-22, pending performance gate).**
Fused filter-update-project, filter-head, and filter-tail now compose the same
filter, update, and metadata chunk entry points; tail alone retains its rolling
Table buffer because it is a read-all operation. The 8-repeat A/B against
`HEAD~1` measured +6.32% total / 0.947× geomean over 88 cases (0.946×
ibex-only). This bridge-only change does not alter the filter loops or query
work, so the result is non-actionable measurement variation; retain the wider
`HEAD~10` comparison for context. Canonicalization/physical tests pass 163
assertions and full debug ctest passes 1676/1676. Next: replace the shared
bridge internals with the representation-aware gather path selected by source
signatures.

**Row-local update kernel seam (2026-08-22, `27685ecd`).**
`kernel::update_row_local_chunk` is now the one chunk-level entry point for
the row-local Update capability.  It owns the Chunk→Table evaluator boundary
and restores `sequence`/`row_offset` after the established `update_table`
semantics finish; `ChunkedUpdateOperator` delegates to it rather than owning
that conversion itself.  The conversion pair is shared internal runtime code,
so every remaining chunked operator retains the same representation of table
metadata and zero-column logical rows instead of carrying a second copy of
that bridge.  This is deliberately a seam extraction, not a claim that
`update_table`'s fixed-width, packed-bool, string, categorical, and validity
loops are ported yet.  The new kernel test pins alias buffer sharing and
morsel identity; full debug ctest passes 1666/1666.  The 8-repeat interleaved
A/B against `HEAD~1` measured −0.28% total / 1.000× geomean over 88 cases
(1.004× ibex-only).  The three parse-inclusive cases measured 0.882× in this
sample, but the overall result is neutral and this seam adds no parse-path
work; retain that small subset as a watch item rather than infer causality.
Next: port the metadata-only `alias = existing_column` update fast path onto
`ChunkView`, then take the first representation-specific evaluator path.

**Row-local alias update port (2026-08-22, pending performance gate).**
The single-field, non-lexical `alias = existing_column` form now stays in
`kernel::update_row_local_chunk`: it resolves the source position once through
`ChunkView`, maps shared `ColumnEntry` handles into an appended or replaced
output column, and derives update properties before returning.  It therefore
bypasses the Chunk→Table bridge for every representation while preserving the
existing evaluator for scalar references, multi-field snapshot semantics, and
all computed expressions.  It also declines a TimeFrame time-index write so
the established evaluator keeps its error contract.  Tests pin append sharing,
replacement validity sharing, ordered-key invalidation, and that decline.
Gates: focused kernel-update tests and full debug ctest 1668/1668.  Next: take
a fixed-width computed-field evaluator
path onto views, then run one interleaved A/B.

**All-valid Int64 binary update port (2026-08-22, pending performance gate).**
`kernel::update_row_local_chunk` now evaluates one all-valid Int64
column-pair add/subtract/multiply field directly from `ColumnView`s into a
pre-sized output column.  The fixed shape is intentional: nullable inputs,
mixed types, scalar operands, division/modulo's checked arithmetic, and
multi-field snapshots remain with `update_table`.  The output replaces or
appends the alias and derives properties exactly as an update does.  Unit
coverage pins a multiply result; focused kernel-update tests and full debug
ctest 1669/1669 pass.  Next: verify this first computed fast path, then extend
to nullable fixed-width inputs rather than broadening its semantics blindly.

**Nullable Int64 binary update port (2026-08-22, pending performance gate).**
The same column-pair add/subtract/multiply kernel now accepts nullable Int64
inputs.  Its value loop remains branch-free; when either input has validity it
makes a separate AND pass and retains a bitmap only if an output row is null.
The direct test pins the mixed-null mask; focused kernel-update tests and full
debug ctest 1670/1670 pass.  Division/modulo, scalars, mixed types, and
multi-field snapshots remain delegated to `update_table`.  Next: run one
combined interleaved A/B for the all-valid and nullable computed-field ports.

**Double binary update port (2026-08-22, pending performance gate).**
All-valid and nullable Double column-pair add/subtract/multiply/divide fields
now take the same direct `ChunkView` route; nullable output validity is again
the AND of the operands.  Modulo and mixed/scalar shapes remain on the
established evaluator.  The kernel test pins nullable division, and full debug
ctest passes 1671/1671.  Next: capture the combined computed-update A/B, then
consider scalar operands or checked integer division as separate contracts.

**Int64 literal update port (2026-08-22, pending performance gate).**
The fixed-width kernel now also handles a single Int64 column with an Int64
literal for add/subtract/multiply, including either operand order.  This covers
the common `price * 2` shape while propagating a nullable source bitmap into
owned output validity.  Checked division/modulo, non-Int literals, mixed
types, and multi-field snapshots remain on `update_table`.  Focused kernel
coverage pins nullable multiplication; full debug ctest passes 1672/1672.
Next: run one combined A/B for the computed-update ports, then decide whether
the checked-arithmetic contract is worth extracting.

**Double literal update port (2026-08-22, `8ef3b243`).**
Double columns now accept Int64 or Double literals for add/subtract/multiply/divide
through the view kernel, covering `price * 2`. Computed view kernels are serial-only
for now: parallel updates retain the established evaluator's field scheduling and
accounting contract. Full debug ctest 1673/1673 passes. The combined 8-repeat
interleaved A/B against `HEAD~1` measured −0.22% total / 0.998× geomean over
88 cases (0.997× ibex-only); the three parse-inclusive cases were 1.022×.
This is neutral noise. Next: extract checked integer divide/modulo only with
their established zero-divisor contract, keeping parallel updates delegated.

**Checked Int64 modulo update port (2026-08-22, pending performance gate).**
The Int64 view kernels now use `safe_imod` for column-pair and literal modulo,
preserving the language's zero-divisor-to-zero contract. Int64 division stays
on `update_table`: its inferred result type is Double, so treating it as an
Int64 output would violate the existing evaluator contract. These computed
kernels remain serial-only while parallel updates retain evaluator scheduling
and accounting. Focused kernel coverage pins literal zero-divisor behavior;
full debug ctest passes 1674/1674. The combined 8-repeat A/B against `HEAD~1`
measured +3.90% total / 0.965× geomean over 88 cases (0.964× ibex-only;
0.994× parse-inclusive). This small dispatch-only change has no plausible
per-query hot-path mechanism, so the result is recorded as non-actionable
measurement variation. Next: take a broader row-local update step, then
consolidate map-kernel selection into a construction-time dispatch descriptor.

**Literal assignment update port (2026-08-22, pending performance gate).**
Single-field literal assignments now use the chunk kernel in serial mode. The
kernel broadcasts the literal across the output chunk, replaces any existing
column (including clearing obsolete validity), or appends it, and derives the
same metadata fate as `update_table`. Parallel execution still delegates to
the established evaluator so its scheduling and accounting contract stays
intact. Focused kernel coverage pins replacement of a nullable Int64 column;
full debug ctest passes 1675/1675. Next: consolidate capability selection into
a construction-time dispatch descriptor rather than add more one-off probes.

**Ordered multi-field chunk updates (2026-08-22, `018822da`).** An update's
fields are ordered — a later field reads the chunk every earlier one produced —
so the chunk kernel now folds them one at a time. The direct route stays
all-or-nothing: if any field falls outside the kernel vocabulary, the tentative
chunk is discarded and `update_table` evaluates the ORIGINAL complete update,
so a partially-ported clause can never land half its fields through one
contract and half through another.

**Guarded and variable-width update output (2026-08-22, `31f9d64f`,
`73671829`, `572b48f9`, `55b63c70`).** The output side of the update family
reached the representations the first ports had deferred: guarded (`where …
update`) string writes pack through the count → prefix-sum → scatter contract,
guarded categorical writes go through a planned dictionary with per-source code
remaps, and grouped update/window results scatter variable-width output by
absolute row instead of rebuilding a per-group table. These are the
representation contracts the plan's §5 names, applied to the writer side rather
than the reader side.

**Unary, string-length, and compiled numeric trees (2026-08-22, `859e2153`,
`80543e6a`, `8b2d910f`, `bbc61514`).** The one-off computed-field probes the
earlier entries kept adding (all-valid Int64 pair, nullable pair, Double pair,
Int64 literal, Double literal, checked modulo) were the wrong shape to keep
extending: each was a new admission test for one expression form. They are
replaced by a compiled numeric expression TREE evaluated over `ChunkView`s,
which subsumes them; the superseded helpers were deleted rather than left as a
second route to the same answer.

**One direct-update dispatch, shared by both executors (2026-08-22,
`f6250f21`, `8ae777c2`).** This is the structural result of Phase 2 item 2 for
the update family. The direct-field vocabulary is now stated once, in
`kernel_update.hpp`, as plan/write pairs: `DirectFieldPlan` (numeric tree,
temporal part, string length), `DirectPredicatePlan` (packed bool),
`DirectValidityPlan` (fill_null/coalesce/CASE), `DirectCategoricalPlan`
(dictionary + remaps), and `StringInterpolationPlan` (count/prefix/write). A
plan borrows the IR and owns no destination, so the same plan serves a serial
chunk write and a parallel table-level window write. `try_direct_update_field`
is the single ordered dispatch on the chunk side; `evaluate_field_maybe_parallel`
consumes the identical plans on the table side. The two executors can no longer
disagree about which expressions have a fast path, because there is one place
that decides.

**Grouped update off the per-group Table (2026-08-23, `376ef3e1` … `f4cc2a7d`).**
Tracked in full in [grouped-chunkview-update-plan.md](grouped-chunkview-update-plan.md);
recorded here because it removes one of the update family's two remaining
whole-operator materializations. `update …, by k` used to gather each group into
its own `Table`, run the ordinary evaluator on it, and scatter the result back —
O(groups) table constructions, and the reason high-cardinality grouped updates
scaled badly. It now runs an immutable `GroupedRowPlan` (global row ids + CSR
group rows) with: fixed-width reductions and ordered kernels (`lag`, `cumsum`,
fills) reducing over CSR rows and scattering to absolute rows; every other
aggregate (`median`, `std`, `quantile`, `first`/`last`, string/categorical
aggregates) broadcast from ONE grouped aggregation keyed by group id; and
group state lifted out of surrounding expressions into staging columns so the
residual expression is ordinary row-local work on the direct protocols above.
A wholly row-local `by` clause bypasses grouping entirely. What remains
materialized is now a stated list — `rank`, variable-width ordered state,
`window`-clause `lag`/`lead` — not "everything that is not a bare aggregate".

**Parallel chunk updates on the direct plans (2026-08-23, `edd8f9b9`,
`9474fcb4`).** The gap the status table below named first is closed for every
shape the direct vocabulary covers. `evaluate_field_maybe_parallel` owned the
only implementation of "split one field across worker ranges, each writing its
own window", and all of it took a `Table` -- so a caller holding a `Chunk`
reached it only by converting. That loop is now `kernel::evaluate_field_windows`
over a `PredicateInput`, with `kernel::DirectFieldRoute` as the resolved plan
vocabulary (string / categorical / predicate / validity / fixed-width) so a
caller selects an arm once and cannot pick a different one than the executor
runs. Expressions outside that vocabulary stay with the caller through a
`DirectFieldRangeWriter` hook, because the compiled numeric writers and the
general evaluator genuinely need the table; the hook is invoked at exactly the
point in the per-range order those arms held before.

The chunk kernel then calls it directly: the split runs on the chunk, a declined
split (too few rows, one morsel, a worker thread that must not submit to the
pool) takes the same whole-chunk write the serial route uses, and an error falls
through to the established evaluator, which owns that diagnostic. Fused island
updates run on a worker thread and therefore lose the bridge as well. A field
the route does not name -- a compiled numeric tree, the legacy null-handling
arms -- keeps the bridge deliberately: the table evaluator can still split those
through its own range writer, so declining is how such a field keeps its
parallelism rather than how it loses it. Making the numeric tree range-writable
is what would close the rest.

`chunk_direct_updates` in `IBEX_PARALLEL_STATS` counts the fields the chunk
kernel kept, which is the only thing that distinguishes the two paths: the
bridge answers identically, just via a `Table`. Gates: debug ctest 1738/1738
including the interpreter-vs-transpiled parity case, and `check_answers.py`
22/22 under both `IBEX_PARALLEL` settings. No performance claim -- this removes
a per-chunk metadata bridge, not a row loop.

**The compiled numeric tree as a route arm (2026-08-24, `aea4d347`).** The tree
was the last shape that could evaluate a whole chunk but not one range of it, so
a parallel chunk update over general arithmetic -- `sqrt(x) * 2.0 + pmin(y,
0.5)` -- had no arm, declined the split, and crossed to `update_table` only to
be split there by that file's own compiled tree.

It is now a plan and a range writer like the rest: `DirectNumericTreePlan` holds
the post-order nodes, the root, and the root's type; `write_direct_numeric_tree_range`
fills a caller-positioned window. Node column pointers were already
absolute-row, which is what lets one plan serve any `RowRange` of the input it
was planned against. The arm sits after `fixed_width` in `DirectFieldRoute`, the
order the serial dispatch already used, and is dispatched ahead of the caller's
fallback hook.

Both executors therefore reach it, on purpose: the table splitter now writes
such a field with the kernel tree rather than falling through to
`try_write_compiled_numeric_update_expr`. Gating the arm on "the caller has no
fallback" would have been the smaller change and the wrong one -- it would put
the two executors back into disagreement about which expressions have a fast
path, which is the thing this phase exists to remove. The two trees agree where
both apply (Int `Div` widens to `Double` in each, `Mod` is `safe_imod` /
`std::fmod` in each) and the kernel tree accepts a strict subset: no cast
wrappers, no column-kernel splices. `try_numeric_tree_update` is that same plan
written over the whole range, so one implementation remains rather than two.

Still bridged in parallel mode after this: the legacy null-handling arms outside
the `DirectValidityPlan` vocabulary, anything only the general evaluator reaches
(a string result has no numeric window to pre-size), and multi-field clauses.

Gates: debug ctest 1739/1739, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings, and `IBEX_PARALLEL_STATS=1` on a 5M-row
`sqrt(x) * 2.0 + pmin(y, 0.5)` update reporting `chunk_direct_updates=1` where
it was 0. No performance claim -- a per-chunk metadata bridge removed, not a row
loop.

**Multi-field clauses in parallel mode (2026-08-24, `63d7f8a1`).** Only the
serial path folded a clause's fields, so a two-field update in parallel mode
converted to a `Table` per chunk and let `update_table` perform the same fold
there. `fold_fields` now takes the per-field route as a parameter: serial and
parallel differ only in which route they fold with, and the two rules that
matter -- each field planned against the chunk the previous one produced, and
all-or-nothing so a clause cannot land half its fields in the kernel and half
over the bridge -- are one implementation rather than two.

Each field remains its own parallel batch, as it was under the table evaluator;
the barrier between fields is what the ordering requires. What the fold removes
is the per-chunk `Table` conversion around it.

Gates: `IBEX_PARALLEL_STATS=1` on a 5M-row two-field update reports
`chunk_direct_updates=2` where it was 0, with `parallel_fields=3` unchanged
either way -- the fields were already being split, just over the bridge. Debug
ctest 1740/1740, `check_answers.py` 22/22 under both `IBEX_PARALLEL` settings.
No performance claim.

**Item 4, step 1 — a breaker is a source (2026-08-24, `32f62261`).** The
physical plan and the parallel island have stayed two analyses because the plan
could not describe what the island executes: `plan_physical` refused any chain
that did not bottom out in a scan or chunked extern call, so
`trades[distinct ...][filter ...]` was a `MaterializedCall` naming the whole
subtree, while `analyze_parallel_island` happily held that chain and
materialized its input.

The gap was a missing source kind, not a missing capability. A subtree that is
not scan-like is a pipeline breaker, and a map chain over its output is a
pipeline whose source is that breaker -- the relationship a breaker has to the
pipeline above it, and the one Phase 3 needs the plan to express before an
island can become an execution *mode* rather than a second executor.
`SourceKind::MaterializedInput` names it; `source_node` points at the breaker's
root; `explain_physical` prints `MaterializedInput(Distinct)` so the feeding
operator is inspectable.

Execution is unchanged by construction: `build_physical_map_step` builds its
source through the public `build_operator` -- the same call the per-kind switch
makes for that subtree -- and the step factories are the ones the switch already
uses. The source signature stays empty for a breaker, and `physical_filter_route`
treats empty and null alike, so a filter over a breaker keeps the compatibility
route it had as a fallback. `FallbackReason::NonSourceInput` is gone: with
breakers admitted it could only have described a structurally malformed map
node, which `MalformedMapNode` says directly.

A design note worth keeping, because the first attempt at this item went the
wrong way: the target is to *dissolve* the island, not to derive it. Deriving a
`ParallelIslandCandidate` from the plan and feeding the existing island builder
was tried and abandoned before it was committed -- it keeps the island as the
unit of parallelism and makes the planner depend on it, which is the opposite of
the Phase 3 exit criterion.

Gates: debug ctest 1742/1742, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings, and an interleaved A/B against `HEAD~1`
(`core,null,reshape,groupagg`, 9 repeats) at geomean 1.000, median -0.18%, every
query `noise` -- the no-regression bar this phase sets for a construction-path
change.

Remaining for item 4: (3) give `build_parallel_island` the `Plan` and delete
`analyze_parallel_island` / `ParallelIslandCandidate`. `build_pipelined_scan`
stays a source-side streaming mode until Phase 3 item 3.

**Item 4, step 2 — the plan decides the mode (2026-08-24, `0b4150d6`).** The
rules that say whether a map chain may run over morsels lived inside
`analyze_parallel_island`, which reached them by walking the IR itself. They now
belong to the plan: `PipelineMode` (Serial / MorselParallel), `SerialOnlyReason`,
and `parallel_steps` -- the length of the leading run of steps that may run in
parallel -- resolved once from steps the planner has already peeled.

The rules are relocated, not changed: a leading run of `ParallelMap` steps, each
subset-evaluable, and not a chain of nothing but Project/Rename. A bare
row-local `Update` still bounds the prefix rather than joining it, so
`parallel_input_node` names the step below the prefix (or the source when the
whole chain is parallel) -- the same boundary the island seam produces today by
rooting a shorter chain. `explain_physical` prints the mode and, when serial,
why.

The equivalence evidence, which is what step 3 rests on: a temporary assertion
at the live seam compared the plan's verdict, prefix length, step identity, and
input node against `analyze_parallel_island`'s on every query built in parallel
mode. It held across the full debug suite (1744 cases) and all 22 PDS-H queries
run **on the debug binary** under `IBEX_PARALLEL=1` -- the release build compiles
the assertion out, so running PDS-H there would have proven nothing. The probe
was removed before the commit; the corpus test "Pipeline mode agrees with the
island analysis" keeps a readable subset.

Gates: debug ctest 1744/1744, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings. No performance claim -- no execution path changed.

**Item 4, step 3 — the island is gone (2026-08-24, `8e31700a`).**
`build_operator_impl` had two analyses at one seam: `analyze_parallel_island`
decided parallel execution by walking the IR itself, `plan_physical` decided
serial execution. There is now one plan per node, consulted in both modes, and
its `PipelineMode` is the whole decision. `build_parallel_island` became
`build_map_pipeline_parallel` and takes the `Plan` -- reading
`parallel_input_node` for what to materialize and reversing
`steps[0, parallel_steps)` for the operators. Its internals are unchanged, which
is the point: morsel execution was always a way to run a map pipeline, not a
separate kind of plan. `analyze_parallel_island`, `ParallelIslandCandidate`, and
`ParallelEligibilityReason` are deleted.

**Known limitation, deliberate and documented at the seam.** In parallel mode a
migrated pipeline whose own mode is `Serial` does not take the serial physical
composer. Such a chain can still contain a parallel pipeline *below* the step
that bounds it -- `df[filter ...][update ...]`, where the root Update is not a
parallel step but the Filter under it is -- and the per-kind recursion finds it
by re-planning at each node. Consuming the whole chain there would swallow that
inner pipeline. `parallel_steps` models a leading run from the root only;
modelling a serial tail over a parallel prefix inside one plan is Phase 3 work,
and is the remaining reason the recursion still matters.

Naming debt: the operator classes and counters are still called islands
(`ParallelIslandOperator`, `IslandWorkerChain`, `island_grain`,
`parallel_islands`/`serial_islands` in `IBEX_PARALLEL_STATS`). The abstraction
is gone; the identifiers are Phase 5's file-ownership split, and the stats line
is read by tooling, so neither was renamed here.

Gates: debug ctest 1744/1744, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings, and an interleaved A/B over `core,filter,null,groupagg`
at 9 repeats -- this reroutes construction for every query in parallel mode --
at geomean 1.006, total -0.25%, every query `noise`.

**Item 5, step 1 — fusion becomes physical (2026-08-24, `d3d4ae7c`,
`918be2d3`).** Item 5 asks for the fused execution node kinds to be retired
"once physical-pipeline fusion proves equivalent", and that fusion did not
exist: canonicalize R5 rewrote `Project(Filter(x))` into a `FilterProject`
*node* and the planner recognized the kind. Deleting R5 would have deleted the
fusion, not relocated it.

Two commits. `d3d4ae7c` replaced the plan's `steps` + `kernel_dispatch` pair of
position-indexed vectors with one `MapStep` carrying node, capability, and
factory together — a step had to stop being addressable as a bare node pointer
before it could name two nodes. `918be2d3` then fuses in the planner: the step
names the `Filter` as its node and the `Project` as `fused`, and resolves the
same `FilterProjectGather` kernel. No new kernel was needed —
`ChunkedFilterProjectOperator` only ever wanted a predicate and a column list.

The fusion is carried, not merely planned: `MapStep` replaces bare node
pointers through the parallel executors too, so `range_filter_head` absorbs a
fused project into the morsel source exactly as it absorbs a `FilterProject`
node's column list. A step that lost its `fused` partner between the planner
and a worker would have produced correct rows with the projection silently
dropped — right answers, wrong columns — which is why the plumbing goes all the
way rather than stopping at the serial composer.

It fires only on IR that reaches the runtime unfused today, since both the
compiled and REPL paths canonicalize (`repl.cpp` runs it explicitly, and that
was itself a fix). The tests therefore build the unfused tree by hand and check
the plan shape, execution matching the canonicalized `FilterProject` byte for
byte in both modes, and a 40k-row case that actually morselizes with
`parallel_islands == 1` asserted so a silent serial fallback cannot pass.

Gates: debug ctest 1747/1747, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings. No performance claim -- no shape in the current
pipeline reaches the new fusion yet.

**Remaining for item 5**: `Project(Update(Filter(x)))` (canonicalize R6) needs a
step naming three nodes, then R5/R6 can be removed from canonicalize and the
node kinds retired from the IR. That last step moves work off canonicalize's
path for every query and touches ~25 files across `ir/`, `parser/`, `codegen/`,
and `runtime/`, so it wants its own A/B rather than riding along here.

**Item 5 — fusion leaves the tree (2026-08-24, `7c8936d5`).** With physical
fusion in place for both shapes, canonicalize R5 and R6 are gone: the IR no
longer encodes an execution decision, and the planner is the single place that
decides a chain runs as one gather pass. The fused node kinds stay
constructible and handled — nothing in the optimizer produces them now, but
tools and serialized trees still name them, so deleting the types is the Phase 5
cleanup this plan already scopes separately.

Two failures the removal surfaced, both worth recording because neither was in
the removal itself:

* `kRules` is a fixed-size `std::array<..., 19>`. Dropping two entries without
  shrinking it left two value-initialized (null) function pointers, which the
  rewrite driver called — 942 tests segfaulted at once.
* `range_filter_head` absorbed a fused step's `Project` into the morsel source
  while skipping the `Update` between them, so the projection named a column
  nothing had computed. The bug was introduced with three-node fusion
  (`8156caac`) and was unreachable until canonicalize stopped fusing; the
  existing island E2E test caught it immediately. A head may absorb a filter and
  the projection directly above it, and nothing else.

The tests that asserted the rewrites now assert what canonicalize leaves behind,
and the two physical fusion cases build their fused-node reference directly
rather than asking canonicalize for one — which makes them a sharper equivalence
check than before, since the two sides no longer share a producer.

Gates: debug ctest 1749/1749, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings, and an interleaved A/B over `core,filter,groupagg,null`
at 9 repeats: total +2.67%, geomean 0.991, every query `noise`. That total looked
directional (15 of 22 slower), so a `--replica-control` run of the same suite
against **itself** was measured: total -1.29% at geomean 0.993. That is what the
harness does with identical binaries, and this change is not distinguishable
from it.

**Phase 3 opening — the plan owns the run's position (2026-08-24, `bc8025b9`).**
Item 4 left one crutch, documented at the time: `parallel_steps` measured a run
starting at the root, so a chain whose root was not parallel-eligible was
declared serial and the per-kind recursion re-planned node by node until it
stumbled on the eligible sub-chain. `df[filter ...][update ...]` is the shape.

The plan now records `parallel_begin`/`parallel_end`. One plan describes the
whole chain: the run, whatever runs serially above it, and the input beneath it.
`resolve_pipeline_mode` searches top-down for the outermost such run — the
existing policy, not a new one, since the recursion took the first island it
found on the way down.

The seam's special case goes with it. Every migrated plan now reaches the
composer, which hands the run off at its boundary and composes the rest above
it; there is no longer an arrangement of map nodes that only one of the two
paths can express.

**A bug the answer gate caught and the suite did not.** The handoff first tested
only `plan.mode`, so a *serial* execution of a plan whose mode is
`MorselParallel` built the morsel pipeline anyway. All 1752 tests passed; q19
died under `IBEX_PARALLEL=0` on `eval_numeric_update_blocks_into: missing double
output`. The mode says what a pipeline *may* do and `exec.parallel` says what
this run does — the condition needs both. Worth remembering as a shape of
mistake: a plan field that reads like a decision is only a capability until an
executor consults its own context.

Gates: debug ctest 1752/1752, `check_answers.py` 22/22 under both
`IBEX_PARALLEL` settings, interleaved A/B over `core,filter,null,groupagg` at 9
repeats: total -1.86%, geomean 1.011, every query `noise`.

**Phase 3 — the pipelined scan becomes a source strategy (2026-08-24,
`79d40dab`).** The construction seam chose between streaming a decomposable
deferred scan and materialize-then-morselize, which meant the choice could only
be offered to a chain that was parallel end to end: `bc8025b9` had to gate it on
`parallel_begin == 0 && parallel_end == steps.size()`, since the composer's
serial tail is not something `build_pipelined_scan` models.

That choice belongs to the run — it is a property of what feeds the run, not of
the query's root. `build_map_pipeline_parallel` now makes it, so a run with
serial steps above it streams its source too: `df[filter ...][update ...]` over
a decomposable source used to decode the whole thing and morselize, and now
streams with the update composed over the reassembled output. **The construction
seam has no pipeline special case left at all** — it plans, and hands every
migrated plan to the composer.

Gates: debug ctest 1753/1753 (new case: the streamed shape under a serial step,
asserting `pipelined_scans == 1`, in-order values, and the serial answer), 22/22
answers both modes, micro A/B at +0.25% (all noise), and a PDS-H A/B over all 22
queries at 8 cores: geomean 0.9985, byte-identical output, no verdict surviving
Holm-Bonferroni. Worth stating plainly: the micro suites are CSV-backed and
barely touch this path, and PDS-H did not move because those queries were
already parallel end to end — the removed gate rarely bound them. The result
here is a capability and a simplification, not a speedup.

**Phase 3 item 1 — one owner of the ordered handoff (2026-08-24, `89a40abf`,
`2b960a21`).** Two implementations of the same shape existed: the morsel
executor's ordered merge ring and the scan pipeline's unit ring — same window,
same two condition variables, same `sequence % window` slots, same backpressure
rule, two sets of bugs to keep in sync. `OrderedChunkRing` is that shape once:
producers `acquire` a sequence, `publish` into its slot, and `producer_exited`
on the way out; the consumer `take`s the sequence it needs next; both waits keep
their `RingWaitScope`, without which produced-ahead reads as work and occupancy
overstates the machine.

The two failure models were **not** equivalent, so the stronger one is now
shared. The morsel executor ordered failures by sequence deliberately, with an
allocation-free `record_fault` for when allocation is what failed; the scan
pipeline's exception path was first-writer-wins, so two failing units reported
whichever message won the race. It is sequence-ordered now, and its workers
track liveness through the same exit guard, which turns "a worker died" into an
error rather than a hang there too.

Then the naming followed the structure (`2b960a21`): `ParallelIslandOperator` →
`MorselPipelineOperator`, `ParallelIslandStats` → `ParallelPipelineStats`,
`parallel_islands`/`serial_islands` → `parallel_pipelines`/`serial_pipelines`,
error messages and the `IBEX_PARALLEL_STATS` prefix with them (nothing outside
the repo parses that line — checked before changing it; its keys are untouched).
**Zero occurrences of "island" remain in `src/`, `include/`, or `tests/`,
comments included** — a name that survives only in prose teaches the next reader
the wrong model. What was ever specific to an island was never the parallelism,
only the handoff; with that extracted, the class is a pipeline executed over
morsels and is now called one.

Remaining under item 1: `PipelinedStageOperator` still owns a raw thread and a
plain bounded FIFO (`std::deque`, capacity 2, single producer). It is
deliberately not merged into `OrderedChunkRing` — it has no sequence ordering to
maintain, and merging two things because both have two condition variables is
how the duplication above was created in the first place.

**Concurrency-ownership inventory (2026-08-24).** Taken from the tree before
starting Phase 3 items 1-4, since those items are phrased as "the executor is
the only thing allowed to X" and the first question is what does X today.

*Raw thread construction* — two sites, and only one is in the execution path:

| Site | What it is |
|---|---|
| `WorkerPool` (`worker_pool.cpp:105-111`) | The shared pool itself: `std::jthread`, or `std::thread` with an explicit join where Apple's libc++ lacks jthread. Sanctioned by definition. |
| `PipelinedStageOperator::start()` (`chunked.cpp:12280`) | One long-lived producer thread per stage, parked on the consumer's ring backpressure. Deliberately not a pool worker — it is long-lived and blocks, which a fixed-size pool cannot host — and it declares itself with `StageThreadScope` so the profiler attributes its work rather than charging the caller. |

**Phase 3 item 4 ("eliminate raw-thread construction from individual
join/builder branches") is already satisfied.** The two branch-concurrency
sites it names are gone; what remains at those spots are comments recording why
(`chunked.cpp:10002` and `:13066`): both overlapped a build with a materialize
on a raw thread, both measured worse (q09 +57% then +47.5% under a budget; q10's
~-3% did not survive widening), and both were reverted. The conclusion recorded
there is that branch concurrency needs a cost-aware gate rather than a
thread-count one. Item 4 should be closed by citing that, not by doing work.

*Bounded producer/consumer handoffs* — three, all operator classes in
`chunked.cpp`, plus the pool's own:

| Owner | Shape |
|---|---|
| `ParallelIslandOperator` (class `:10670`, cvs `:10977`) | Sequence-indexed ring (`ring_` + `ring_ready_`, window 2, `released_`, `active_workers_`). Ordered reassembly by morsel sequence. |
| `PipelinedScanOperator` (class `:11928`, cvs `:12198`) | The same shape over `expected<Chunk, string>`, ordered by unit sequence, plus worker-failure propagation and categorical state carriers. |
| `PipelinedStageOperator` (cvs `:12385`) | **Not** a sequence ring: a `std::deque<Chunk>` FIFO capped at 2, ordered by having a single producer. Owns the raw thread above. |
| `WorkerPool` (`worker_pool.cpp:35, 116`) | Batch-done and work-available; the pool's own machinery. |

The first two are near-identical and share the invariant that actually matters
(ordered reassembly by sequence); the third is a plain bounded queue and should
not be merged into them just because it also has two condition variables. That
is the shape of Phase 3 item 1: one reusable ordered-handoff component owned by
the executor, used by the two sequence rings, with the stage's FIFO left as what
it is.

*Fan-out sites* — 41 `pool.submit` calls: `chunked.cpp` 25, `update.cpp` 7,
`kernel_update.cpp` 4, and one each in `aggregate/join/filter/sort/lazy_table`.
These are not handoffs, but they are where DOP is seized, so they are item 2's
surface. The current policy is outermost-wins, enforced rather than documented:
`WorkerPool::submit` calls `invariant_violation` if called from a pool thread
(`worker_pool.cpp:406-411`), and 29 call sites ask `on_worker_pool_thread()`
first and run serially instead. Any budget work has to keep that check working,
because it is what makes nested parallelism a crash rather than a deadlock.

**Where Phase 2 stands (2026-08-24).** Written from the tree, not from a
per-commit A/B log; the entries above are the itemized history.

| Item | State |
|---|---|
| 1. Extract view/selection/validity/output-writer/scratch APIs | Views, `Selection`, and the fixed-width/bool/string/validity output writers exist and are used. **`KernelContext` does not exist** — scratch, cancellation, RNG stream, and profiling counters are still passed ad hoc. |
| 2. Port filter/project/rename/row-local update kernels | Filter: every representation. Project/rename: metadata map. Row-local update: the direct-plan family above, plus multi-field ordering, and (since `9474fcb4`) the same plans in parallel mode, split by the kernel itself. Since `aea4d347` the compiled numeric tree is a route arm too, so general arithmetic splits in the kernel. **Remaining gap: the legacy null-handling arms and anything only the general evaluator reaches (a string result has no numeric window to pre-size) still convert to a `Table` in parallel mode**, because only the table evaluator has a range writer for those. Multi-field clauses fold in the kernel too since `63d7f8a1`. |
| 3. Static dispatch tables and capability declarations | Landed: `MapKernelCapability` + `MapKernelFactory` stored per step in `physical::Plan`, with `ColumnKernelSignature` recorded for resolved scan sources. |
| 4. Run the physical map pipeline serially, then on the morsel executor | **DONE** (`32f62261`, `0b4150d6`, `8e31700a`): one plan per node decides both modes, and the island analysis is deleted. Earlier state, kept for context: | Serial only, but the plan can now describe every shape the island executes: since `32f62261` a map chain over a breaker is a `MaterializedInput` pipeline rather than a fallback. The morsel executor is still reached through the island seam. Step 2 (`0b4150d6`) gave the plan its own `PipelineMode`; step 3 (`8e31700a`) deleted the island analysis and handed `build_map_pipeline_parallel` the plan. |
| 5. Retire `FilterProject`/`FilterUpdateProject` as execution node kinds | **DONE as an execution concern** (`918be2d3`, `8156caac`, `7c8936d5`): the planner fuses both shapes, canonicalize's R5/R6 are deleted, and nothing in the optimizer produces the fused kinds. The types themselves remain for tools and serialized trees — deleting them is Phase 5 item 2. |

Current gates on the tree: full debug `ctest` 1749/1749, and PDS-H
`check_answers.py` 22/22 under both `IBEX_PARALLEL` settings. No
performance claim is made for this block: the entries it summarizes were
gated individually when they landed, and it is not a fresh A/B.

Phase 2 is complete except `KernelContext`, which is **not** started
deliberately: its stated
trigger (one shared scratch/cancellation owner) is not met. Cancellation
already has an owner in `interrupt.hpp`, and the map kernels share no scratch —
the ad-hoc scratch that exists belongs to breaker operators, which is Phase 4.
Building it before a caller needs it is the ceremony this plan's own risk table
warns about. Phase 3 has started: the
serial-tail-over-parallel-run modelling landed in `bc8025b9`, and the pipelined
scan became the run's own source strategy in `79d40dab`. Both things item 4
deliberately left are done, and the construction seam now holds no pipeline
special case. What remains in Phase 3 is its own list: the executor owning
bounded handoffs and DOP budgets (items 1-2), raw-thread construction removed
from join/builder branches (item 4), and per-pipeline scheduling accounting
(item 5). The row-local update family's
remaining bridge users are the legacy null-handling arms and anything only the
general evaluator reaches, both of which are shape gaps rather than mode gaps
now.

1. Extract `ChunkView`, selection, validity, output-writer, and scratch APIs.
2. Port filter/project/rename/row-local update kernels one representation at a
   time, preserving the current fast kernels rather than rewriting them.
3. Build static dispatch tables at pipeline construction. Add explicit kernel
   capability declarations and unit tests for dispatch choice.
4. Run the physical map pipeline serially first; only then use the existing
   morsel executor for it.
5. Retire `FilterProject`/`FilterUpdateProject` as execution node kinds once
   physical-pipeline fusion proves equivalent.

Exit: every migrated map has one kernel family used by both serial and parallel
execution; no physical `next()` implementation duplicates its row loop.

### Phase 3 — make the executor own handoffs and DOP

1. Make the physical pipeline executor the only implementation allowed to own
   a bounded producer/consumer handoff or spawn a helper thread.
2. Represent pipeline DOP and memory budget in `ExecutionContext` child
   budgets; an inner kernel observes the allocation and cannot seize the full
   pool.
3. Migrate existing parallel islands and pipelined scan/stage mechanisms to
   the executor without changing their eligibility policy.
4. Eliminate raw-thread construction from individual join/builder branches.
   **CLOSED (2026-08-24) — already satisfied, by deletion rather than by
   migration.** Both sites this names are gone: each overlapped a build with a
   materialize on a raw `std::thread`, each measured worse (q09 +57%, then
   +47.5% under a since-removed helper-thread budget; q10's ~-3% did not
   survive widening), and each was reverted. `chunked.cpp:10002` and `:13066`
   carry the measurements. The finding to keep is that branch concurrency needs
   a cost-aware gate, not a thread-count one — so re-opening this item means
   proposing such a gate, not removing threads. The only remaining non-pool
   thread in the runtime belongs to `PipelinedStageOperator` and is item 1's
   subject, not this one.
5. Track per-pipeline runnable time, worker capacity, queue/backpressure time,
   and ordered-merge time. Keep the existing accounting closure invariant.

Exit: the old parallel-island abstraction is an implementation mode of a
physical map pipeline, not a second executor.

### Phase 4 — migrate the high-value breakers

**The backlog, measured (2026-08-24, `15db9f6d`).** `IBEX_PLAN_STATS=1` prints
it; this is PDS-H SF-1 at 8 cores, summed over the 22 queries:

```
plan stats: plans=233 pipelines=70 fallbacks=163
            not_map_chain=116 empty_chain=47 malformed=0
```

47 of the 163 are a bare `Scan` (`EmptyChain` — the root is a source, there is
no map work to migrate). So the physical plan describes **70 of the 186 nodes
that carry real work, 38%**, and the backlog is **116 breakers**:

| kind | count (2026-08-24, first reading) | after item 1 |
|---|---|---|
| Join | 59 | **6** |
| Aggregate | 30 | 30 |
| Order | 18 | 18 |
| Head | 5 | 5 |
| Distinct | 4 | 4 |

Join and Aggregate were 77% of the first reading, so the a-priori ordering below
survived contact with measurement — evidence rather than assertion.

**Item 1 landed (`f5610646`) and the column moved: Join 59 -> 6.** Plans 233,
pipelines 70 -> 123, fallbacks 163 -> 110, so the plan now describes 123 of the
186 nodes carrying real work (66%, up from 38%). The backlog is 63 breakers, and
**Aggregate is now the largest at 48%**.

The 6 remaining joins are the materializing ones: they still fall back and still
count. A test asserts that asymmetry -- a streaming join must be `migrated`, a
materializing one must not -- because a count that went to zero would mean the
metric was measuring the label rather than the port.

Re-run the numbers before starting each item rather than trusting this table.

Order is driven by measured serial time and semantic completeness, not by
operator count.

**Status (2026-08-24): ownership done, decomposition not started.** These two
halves are separated deliberately, because the first is easily mistaken for the
second and the measured backlog only reports the first.

*What landed.* The physical plan decides and a plan-owned builder constructs,
for every breaker PDS-H reaches:

| kind | before | after | commit |
|---|---|---|---|
| Join (streaming) | 59 | 6 | `f5610646` |
| Aggregate | 30 | 0 | `902d6941` |
| Order | 18 | 0 | `ececc75f` |
| Head, Distinct | 9 | 0 | `49ca33c1` |

Plans 233, pipelines 70 -> 180, fallbacks 163 -> 53 (47 of them bare `Scan`
with nothing to migrate). So the plan describes **180 of the 186 nodes carrying
real work, 97%**, against 38% when the backlog was first measured. The
remaining 6 are materializing joins, unported on purpose: they carry semantics
the streaming operators do not implement (`nulls equal`, `expect`, non-equi
predicates), so porting them means porting the semantics, not the construction.

*What did NOT land, and is what items 1-2 actually ask for.* The operators are
unchanged. A join is still one `ChunkedInnerJoinOperator`, not a `HashBuild`
feeding a separate `HashProbe` across a barrier; the aggregate is still one
operator, not discovery / per-partition slots / final ordering / emission as
distinct phases. Consequences that are invisible in the backlog number:

* a probe cannot be a step inside a map pipeline, so it cannot fuse with the
  filters or projections above it;
* one build cannot feed several probes;
* the aggregate's phases cannot be scheduled or measured separately, which is
  what [[project_high_cardinality_groupby_gap]] and the partition-owned work
  need.

The exit criterion below is therefore NOT met. "Inspectable physical plans" is
satisfied; "fast paths no longer depend on special builder branches" is not --
the branches moved into `build_physical_join` / `build_physical_aggregate`
rather than dissolving into pipeline stages.

*Method note, since it decided the outcome twice.* Each port ran: name the
builder's own predicates and de-duplicate them; have the planner RELAY them;
have the seam consume the plan; move construction. Writing a planner that
restates the gates instead of relaying them produced a wrong classification
within the hour (`plan_join` and two-key Int64 joins, `6de3956d`), and an
equivalence probe built from the same reading agreed with the mistake. Order,
Head and Distinct skipped the first three steps because they have no
eligibility gate at all -- a one-valued strategy enum would be the ceremony the
risk table warns about.

1. **Hash join:** ~~migrate the supported single-key/two-Int64-key streaming
   paths~~ (construction: DONE, `f5610646`) — express them as `HashBuild +
   HashProbe` across a barrier: **data side DONE (`5918b5cc`, `8a644381`,
   `f6a1a632`), operator side NEXT.** The build is a phase
   that returns an immutable `JoinHashIndex` (`build_join_hash_index` /
   `build_join_pair_index`), and `JoinProbe` is the type that consumes one:
   index, key list, output-name plans, per-worker scratch, probe-chunk
   validity and dictionary. The probe reads the build through
   `shared_ptr<const>`, so writing to build state during a probe is a compile
   error, and a probe can be constructed next to a build it did not run.
   What is NOT done is two scheduled operators. **Corrected 2026-08-25:**
   this previously read "blocked on a cost model", on the reasoning that
   `ChunkedInnerJoinOperator` picks its build side at RUN time
   (`n_right <= kStreamRightThreshold`, else materialize left and compare,
   possibly swapping roles) and that a `HashBuild + HashProbe` pair needs
   that decision at plan time. The second half does not follow — the
   adaptive path already materializes a whole side before choosing, so it is
   already barrier-shaped, and a non-JIT engine can construct the probe
   pipeline after the build barrier for free. See "The build-side choice does
   not block the split" above. The cardinality work
   [[project_deferred_probe_selectivity_scoping]] scopes answers a different
   question again (Decision A there). All other join semantics remain behind
   `MaterializedCall`, which is the intended end state for them.
2. **Hash aggregate:** ~~migrate the current paths~~ (construction: DONE,
   `902d6941`) — express discovery, per-partition slots, final ordering and
   emission as distinct physical phases: NOT STARTED. Median/quantile/EWMA
   fallback is explicit and still counted. Blocked-first, not blocked: the
   thread-count-invariance this decomposition is required to preserve is
   already violated on the current tree by `try_owned_pair` vs the serial
   path, with no test covering it — see "The determinism constraint is
   already broken" above. Reconcile that before decomposing, or the
   decomposition inherits a divergence it will be blamed for.
3. **Distinct and ordered operations:** construction DONE (`ececc75f`,
   `49ca33c1`). `Tail`, `TopK`, `FilterHead` and `FilterTail` are the same
   single-operator shape and are NOT ported, so they now sit in the per-kind
   switch beside ported siblings — an asymmetry worth closing cheaply.
4. Delete the corresponding `chunked.cpp` classes only after the physical path
   handles every previously supported shape and the fallback is mutation-tested.
   Blocked on 1-2: the classes are still the implementation.

Exit (unchanged, and not yet met): q09/q18/q20-shaped plans have inspectable
physical plans and their existing fast paths no longer depend on special
builder branches.

### Phase 5 — retire the monolith and simplify IR

1. Split implementation by ownership: `physical_planner`, `pipeline_executor`,
   `kernels/`, and one file/family per breaker.
2. Move logical fusion/selection out of `ir::NodeKind`; leave compatibility
   lowering only until serialized tests and tools no longer need it.
3. Remove obsolete `build_operator` recursion and migrate `interpret_node` to
   an explicit physical fallback adapter.
4. Make planner/executor/kernel tests independently runnable.

Exit: `chunked.cpp` no longer exists as a monolithic execution/planning unit;
logical planning, physical selection, kernel behavior, and scheduling have
separate owners.

## Acceptance gates

Every phase must meet all of these before the next one starts:

* Full parser/IR test suite, because logical/physical boundary changes touch
  lowering and plan semantics.
* Byte-identical serial and parallel results across multiple chunk grains,
  including null, string, categorical, empty-schema, error, and interruption
  cases.
* Mutation tests proving a migrated query reaches the physical implementation
  and proving unsupported semantics reach its fallback.
* Plan-shape assertions: pushdowns, output properties, barriers, and fallbacks
  are all inspectable and expected.
* Measurements follow `MEASURING.md`: profile a discriminating micro-case
  first; then compare interleaved A/B builds; finally validate relevant PDS-H
  queries at pinned 1/2/4/8 cores. Report no performance result without a
  byte-identity check and accounting closure.

The performance bar is initially **no regression** for Phase 1 and a migrated
serial map path. A multicore improvement is accepted only when it improves
end-to-end wall time under the paired protocol, not merely pool work or a
kernel microbenchmark. The goal is more useful parallel work, not a higher
thread count.

## Risks and decisions to make early

| Risk | Decision / mitigation |
|---|---|
| A “physical IR” mirrors every logical node and adds ceremony | Begin with only source, map pipeline, hash build/probe, materialize, and fallback. Add nodes only when an algorithm needs a different lifecycle. |
| Template explosion harms build time/binary size | Specialize on representation and null/selection policy first; use runtime parameters for uncommon expression details; measure compile size/time per family. |
| Kernel extraction regresses the current tuned path | Port existing kernels behind the new interface before changing algorithms; retain a direct fast path where the abstraction demonstrably costs. |
| A new executor becomes an unmeasured scheduler project | Preserve the current pool and outermost-wins policy. Require queue/occupancy evidence before adding work stealing or branch concurrency. |
| Fallback hides most plans forever | Make every fallback explicit in `explain physical`, profiled, and covered by a migration backlog keyed by its measured cost. |
| Logical/physical split loses source pushdowns | Source demand is a physical-plan input derived once from logical analysis; assert plan shape and reader calls in tests. |

## First implementation slice

Do not start by moving a join or aggregate. The first change should be a
behavior-preserving vertical slice:

```text
logical Scan → Filter → Project
      ↓
physical Lazy/TableScan → MapPipeline(Filter, Project)
      ↓
existing serial chunk execution
```

It proves all new ownership boundaries with manageable semantics, exposes the
physical plan, reuses the existing vector kernels, and permits a direct
serial-vs-parallel morsel comparison later. Only after that slice has held its
correctness and no-regression gates should it replace the map-island executor
or touch a pipeline breaker.

## Relation to Umbra

Umbra's relevant design is the staged path from AST through relational algebra
and an execution IR to pipeline-oriented morsel execution, not its machine-code
backends. Ibex substitutes a precompiled templated/vector-kernel backend for
Umbra IR + Flying Start/LLVM generation. This is a conscious trade: it retains
low implementation and query-startup complexity while gaining the plan and
execution separation needed to make a broad kernel library composable.

See Kersten, Leis, and Neumann, *Tidy Tuples and Flying Start* (VLDB Journal,
2021), especially its architecture and compilation-pipeline discussion:
<https://link.springer.com/article/10.1007/s00778-020-00643-4>.
