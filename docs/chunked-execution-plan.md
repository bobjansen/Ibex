# Chunked Execution Roadmap

## Current Status

Ibex now has a real pull-based chunk pipeline, not just scaffolding:

- `Operator`, `TableSourceOperator`, and `MaterializeOperator` exist as the
  execution substrate.
- `build_operator()` can execute a query through that substrate and only falls
  back to `interpret_node()` where the chunked path is missing or not yet worth
  specializing.
- Native chunk-preserving operators exist for:
  - `Scan` via `TableSourceOperator`
  - `ExternCall` via `chunked_table_func`
  - `Filter`
  - `Project`
  - `Rename`
  - `Aggregate` for the current streamable subset (`Count`, `Sum`, `Min`,
    `Max`, `Mean` on numeric inputs)
- `plan_pipelines()` exists and already classifies nodes into `Source`,
  `Passthrough`, and `Breaker` segments.

That means the first milestone is no longer “make chunked execution exist”.
The next milestones are about removing avoidable materialization points,
hardening semantics, and exploiting the pipeline plan for fusion.

## Coverage Today

### Native chunked path

| Node Kind   | Current handling |
|-------------|------------------|
| `Scan`      | Source |
| `ExternCall`| Source when `chunked_table_func` is registered |
| `Filter`    | Per-chunk passthrough |
| `Project`   | Per-chunk passthrough |
| `Rename`    | Per-chunk passthrough |
| `Aggregate` | Streaming implementation for the supported subset |

### Chunk-aware but materializing

These nodes already run inside `build_operator()`, but they still drain their
child operator into a full `Table` before continuing:

| Node Kind | Current handling |
|-----------|------------------|
| `Distinct` | materialize child -> `distinct_table` |
| `Order` | materialize child -> `order_table` |
| `Head` | materialize child -> `head_table` |
| `Tail` | materialize child -> `tail_table` |
| `Columns` | materialize child -> `columns_table` |
| `Melt` | materialize child -> `melt_table` |
| `Dcast` | materialize child -> `dcast_table` |
| `Cov` | materialize child -> `cov_table` |
| `Corr` | materialize child -> `corr_table` |
| `Transpose` | materialize child -> `transpose_table` |
| `Update` | materialize child -> `update_table` |
| `Resample` | materialize child -> `resample_table` |
| `Window` | materialize child -> `windowed_update_table` |
| `AsTimeframe` | materialize child -> sort + mark time index |
| `Join` | materialize both children -> `join_table_impl` |
| `Matmul` | materialize both children -> `matmul_table` |
| `Model` | materialize child -> `fit_model` |

### Full-table fallback

These still fall through to `interpret_node()` and are wrapped back into a
`TableSourceOperator`:

| Node Kind | Current handling |
|-----------|------------------|
| `Construct` | full-table fallback |
| `Stream` | full-table fallback |
| `Program` | full-table fallback |

## Immediate Goal

Keep the chunked path as the default execution substrate, even when an operator
must temporarily materialize. That gives us one place to add instrumentation,
fusion, chunk sizing, and external streaming sources.

Concretely, this means:

1. Finish coverage in `build_operator()` for every node kind.
2. Centralize the “materialize child, run table function, re-wrap as source”
   pattern so breaker implementations stay small and consistent.
3. Narrow the set of nodes that still need `interpret_node()` fallback.

## Phase 1: Finish `build_operator()` Coverage

### 1.1 Eliminate the remaining fall-through nodes

Add explicit `build_operator()` branches for:

- `Construct`
- `Stream`
- `Program`

These can still use full-table logic initially. The important part is that
coverage becomes explicit rather than hiding in the final fallback branch.

That gives three benefits:

- clearer error handling per node kind
- a single audit point for chunked-execution coverage
- easier profiling later, because every materialization boundary is named

### 1.2 Factor the materialize-wrapper helper

`build_operator()` currently repeats the same pattern for many breaker nodes:

- recursively build the child operator
- materialize it with `MaterializeOperator`
- call a table-level implementation
- re-wrap the result in `TableSourceOperator`

Extract helpers along these lines:

- `run_unary_breaker(child, fn)`
- `run_binary_breaker(left, right, fn)`

The helpers should own the common error plumbing and make it obvious which
nodes are:

- native streaming
- chunk-aware but materializing
- still on full-table fallback

### 1.3 Tighten node-role invariants

`classify_node()` and `build_operator()` should stay aligned.

Add a small invariant test layer so when a new `ir::NodeKind` is introduced,
we fail fast unless:

- it is classified in `classify_node()`
- it has an explicit `build_operator()` strategy

## Phase 2: Harden the Existing Native Operators

Before adding more native operators, the current ones need stronger semantics
and tests.

### 2.1 Filter / project / rename correctness

Add tests for:

- zero-row chunks being skipped correctly by `Filter`
- metadata preservation across passthrough operators
- rename failures surfacing deterministically
- mixed multi-chunk inputs, not just single-chunk tables

### 2.2 Aggregate correctness envelope

The streaming aggregate is the most important native breaker today. Expand
coverage around:

- nullable aggregate inputs
- empty input
- grouped and ungrouped aggregations
- categorical group keys across many chunks
- rejection paths for unsupported aggregate shapes

This is also the point to document the exact supported subset in code comments
and user-facing docs once the behavior is stable.

### 2.3 Materialization constraints

`MaterializeOperator` currently assumes:

- identical schema across chunks
- shared categorical dictionaries across chunks
- no multi-chunk validity bitmap support

Those assumptions are acceptable for now, but they should be explicit
engineering constraints, not accidental behavior. The roadmap should treat
them as tracked limitations to remove deliberately.

## Phase 3: Native Streaming Breakers With Clear Payoff

Not every breaker deserves a chunk-native implementation. Prioritize the ones
that either unlock major memory wins or enable obvious pipeline continuation.

### Good candidates

- `Head`
  - global `head(n)` can stop upstream early
  - grouped `head` is more complex but still bounded
- `Distinct`
  - streaming hash-set implementation is conceptually close to aggregate
- `Update`
  - only for the subset of row-local expressions that do not require whole-table
    context
- `AsTimeframe`
  - only if we can separate “mark time index” from “sort to enforce ordering”

### Poor near-term candidates

- `Order`
- `Join`
- `Window`
- `Resample`
- `Matmul`
- `Model`

These are real breakers and can remain materializing until the rest of the
pipeline is mature.

## Phase 4: External Streaming Sources

The existing `chunked_table_func` hook is the right seam for streaming readers.
The next step is to treat it as a first-class source API rather than an
implementation detail used by `read_csv`.

### Source API direction

- keep `ExternRegistry::register_chunked_table(...)` as the runtime entrypoint
- document the chunk contract:
  - stable schema across chunks
  - ownership and lifetime rules
  - categorical dictionary expectations
  - EOF and error signaling
- add tests that drive a multi-chunk extern source through native operators

### ADBC plugin

Once the source contract is stable, an `adbc` plugin becomes straightforward:

- open a driver / connection from plugin config
- execute SQL
- stream Arrow `RecordBatch` objects as Ibex chunks
- preserve zero-copy conversions where the column layout already matches

This is the cleanest path to PostgreSQL, DuckDB, Snowflake, BigQuery, and
other ADBC-backed systems without building bespoke readers per backend.

## Phase 5: Pipeline-Driven Fusion

`plan_pipelines()` already identifies source / passthrough / breaker segments.
That should become the planning input for fused execution.

### First fusion target

Fuse:

- `Scan -> Filter`
- `Scan -> Project`
- `Scan -> Filter -> Project`
- `Scan -> Filter -> Rename -> Project`

### Non-goals for the first fusion pass

- crossing breaker boundaries
- fusing joins or windows
- general-purpose JIT

### Implementation sketch

1. Use `plan_pipelines()` to identify a segment whose sink is a passthrough.
2. Lower that segment into a single row loop over the source chunk.
3. Apply predicates and projected expressions without constructing intermediate
   `Chunk` objects.
4. Materialize once at the segment output.

The point is not just speed. Fusion also reduces allocator churn and creates a
single place to add SIMD-friendly expression lowering later.

## Validation Plan

Every phase above should leave behind tests, not just code.

Minimum regression coverage:

- operator unit tests for chunk boundaries and EOF behavior
- interpreter tests that run the same query through single-chunk and multi-chunk
  sources and compare results
- pipeline-planner tests for representative IR shapes
- extern-source tests for chunked plugin readers

Benchmark gates should focus on `build-release/` only and compare:

- full-table interpreter path
- chunked unfused path
- fused passthrough path once it exists

## Success Criteria

The roadmap is complete when:

- every `ir::NodeKind` has an explicit `build_operator()` strategy
- the chunked path is the normal execution substrate for table queries
- materialization points are intentional, named, and measurable
- external readers can stream chunks into native operators
- passthrough pipelines can execute without intermediate chunk wrappers
