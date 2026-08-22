# Kernel-oriented pipeline execution

Status: **proposed architecture plan.** This is a deliberate replacement for
the current execution architecture over several migrations, not a promise to
rewrite the runtime in one change. It borrows Umbra's useful separation of
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

**Open.** The opening deliverable — the contract documentation Phase 0's
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
`Plan::kernel_capabilities` directly to the shared map factory; it does not
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
full debug ctest passes 1674/1674. Next: run one combined A/B, then move to
the next bounded row-local shape only if it has a similarly clear output and
validity contract.

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
5. Track per-pipeline runnable time, worker capacity, queue/backpressure time,
   and ordered-merge time. Keep the existing accounting closure invariant.

Exit: the old parallel-island abstraction is an implementation mode of a
physical map pipeline, not a second executor.

### Phase 4 — migrate the high-value breakers

Order is driven by measured serial time and semantic completeness, not by
operator count.

1. **Hash join:** migrate the existing supported single-key/two-Int64-key
   streaming paths as `HashBuild + HashProbe`; retain all other join semantics
   behind `MaterializedCall`.
2. **Hash aggregate:** migrate the current partition-owned paths and express
   discovery, per-partition slots, final ordering, and output emission as
   distinct physical phases. Keep median/quantile/EWMA fallback explicit.
3. **Distinct and ordered operations:** use the established kernel/selection
   library; migrate only where their global state has a complete contract.
4. Delete the corresponding `chunked.cpp` classes only after the physical path
   handles every previously supported shape and the fallback is mutation-tested.

Exit: q09/q18/q20-shaped plans have inspectable physical plans and their
existing fast paths no longer depend on special builder branches.

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
