# dplyr Backend Plan

## Status

**Initial supported release implemented.** The `ribex` package now provides the
lazy source, terminal operations, schema inspection, core single-table verbs,
`distinct`, `count`/`tally`, deterministic expression translation, and the
three explicit fallback policies described below. `inner_join()` and
`left_join()` currently use the documented one-way local fallback rather than a
native join node. Native joins and windows remain Phase 3 follow-up work; the
explicit R UDF barrier and constrained closure inlining remain the post-MVP
Phases 5 and 6.

## Summary

Make Ibex a lazy dplyr backend hosted by `ribex`:

```r
result <- ibex_tbl(trades) |>
  filter(price > 10) |>
  mutate(notional = price * size) |>
  group_by(symbol) |>
  summarise(total = sum(notional), .groups = "drop") |>
  arrange(desc(total)) |>
  collect()
```

An `ibex_tbl` records dplyr verbs and expressions without executing them.
`show_query()` renders the equivalent Ibex program, `compute()` materializes it
inside an Ibex session, and `collect()` returns an R tibble/data frame through
the existing Arrow C Data path.

Use a native dplyr backend rather than pretending Ibex is a SQL dialect. The
backend may borrow concepts from dbplyr's lazy-query representation, but its
output is Ibex syntax/IR and its semantic constraints are Ibex's, not SQL's.
The closer implementation models are dtplyr and Arrow's R query objects: S3
methods append lazy steps, and an explicit terminal operation executes them.

Arbitrary R UDFs are not scalar kernels inside the Ibex runtime. Initially, an
unsupported R call ends native execution and falls back to local dplyr under an
explicit policy. A later opt-in `ibex_r_udf()` node may split a plan into native
segments separated by a vectorized, main-R-thread callback. Such a node is an
optimizer barrier, is interpreter-only, and can never run on an Ibex worker.

## Decisions and Non-goals

### Decisions

1. **Implement in `r/ribex`, not in core dplyr or dbplyr.** Add dplyr, rlang,
   tidyselect, tibble, and vctrs as package dependencies only as each becomes
   necessary. Keep the C++ runtime usable without R.
2. **Own an Ibex-specific lazy plan.** Do not render fake SQL or depend on
   dbplyr's SQL query nesting rules. Store enough schema and grouping metadata
   to implement dplyr methods and lower them directly to Ibex operations.
3. **Render Ibex source for the MVP.** The current `ribex` execution API already
   accepts source strings. Keep a structured R-side plan until execution, then
   render once and submit it to the existing parser/lowerer. Do not concatenate
   user data into source text: captured R constants cross through scalar
   bindings.
4. **Make capability detection deterministic.** Every call is either translated,
   explicitly handled as an R barrier, or rejected/fallen back before native
   execution begins. Never pass an unknown R function name through in the hope
   that an Ibex extern happens to exist.
5. **Keep fallback visible.** An automatic pull into R emits one concise warning
   identifying the unsupported call and native prefix. Users can select
   `fallback = "error"` to prohibit it. Sources whose size is unknown or which
   are lazy/plugin-backed require explicit permission before collection.
6. **Once automatic fallback occurs, stay local.** Return an ordinary tibble and
   let all remaining verbs use dplyr. Do not silently bounce the table between R
   and Ibex several times.
7. **R callbacks execute only on the R main thread.** Ibex may use worker threads
   within native segments, but the `.Call` must return before R evaluates a
   closure. No `SEXP`, R allocation, error signaling, or closure evaluation is
   allowed in `WorkerPool` tasks.
8. **Preserve standalone Ibex.** Plans containing R callbacks are marked
   host-bound and rejected by C++23 transpilation with a precise diagnostic.
   Ordinary translated dplyr plans remain expressible as normal Ibex source.

### Non-goals

- Reimplement all of dplyr in the first release.
- Guarantee byte-for-byte equality with R for floating-point algorithms where
  Ibex deliberately specifies different behavior. Differences must be tested
  and documented, not hidden.
- Compile arbitrary R language semantics, environments, reflection, side
  effects, or dynamic dispatch into C++.
- Invoke an R function once per row.
- Add R-specific state to `ExternRegistry` or the plugin ABI.
- Make an R UDF appear pure to the optimizer. An R call is volatile unless a
  future, separately specified compiler proves otherwise.

## Existing Foundation

The current tree already supplies most of the expensive interop work:

- `r/ribex/R/ribex.R` exposes one-shot and persistent-session evaluation.
- R tables can be bound by copy, while supported nanoarrow objects are exported
  under an independent Arrow C Data ownership lease and adopted by Ibex.
- Results can remain nanoarrow-backed or become ordinary R data frames.
- The runtime has schema inference, typed expressions, explicit grouping,
  ordering, joins, windows, and parallel execution islands.
- Backtick-quoted Ibex identifiers cover non-syntactic R column names, provided
  the renderer implements escaping centrally.

Important gaps for a dplyr backend are:

- no lazy R-side query object or dplyr S3 methods;
- no stable `ribex` API to ask a session/table for its schema without executing
  a user-visible query;
- no R-to-Ibex expression translation registry;
- no dplyr-vs-Ibex semantic compatibility table;
- no plan segmentation or host callback representation; and
- `ExternFn` accepts scalar arguments and is the wrong abstraction for an
  R-owned, vector-returning closure.

## User-facing Objects and API

### `ibex_tbl`

Provisional constructor:

```r
ibex_tbl(
  x,
  session = create_session(),
  name = NULL,
  fallback = c("warn", "error", "collect")
)
```

`x` may initially be a data frame, tibble, or nanoarrow-compatible in-memory
table. A later constructor may refer to a named table already resident in an
Ibex session. The object contains no materialized preview copied merely for
printing.

Minimum logical fields:

```text
session          persistent ribex session external pointer
source           collision-proof Ibex binding name and source locality
schema           ordered names, Ibex types, nullability, categorical/time metadata
steps            immutable list of Ibex-specific lazy plan nodes
groups           current dplyr grouping columns and .drop policy
ordering         known ordering, if any
captured_scalars generated scalar names and their R values/types
fallback_policy  error/warn/collect plus source-size permission
host_bound       true once an explicit R callback is present
```

R objects are copy-on-modify descriptions of a plan. Several derived pipelines
may share one immutable source/session without mutating each other's steps.
Session lifetime must dominate every lazy object that references it.

### Terminal and diagnostic methods

- `collect.ibex_tbl()` executes and returns a tibble by default.
- `compute.ibex_tbl()` executes into a fresh session binding and returns another
  `ibex_tbl`, allowing later native verbs without transferring values to R.
- `show_query.ibex_tbl()` renders the generated Ibex with generated scalar
  values redacted or represented as typed placeholders.
- `explain.ibex_tbl()` may later expose translated steps, fallback barriers,
  inferred schema, and the native physical plan.
- `print.ibex_tbl()` shows source, dimensions when cheaply known, columns/types,
  grouping, and a short query preview; it must not execute an unknown-size
  source merely to print rows.
- `as.data.frame()`, `as_tibble()`, and `pull()` force collection explicitly.

### Source and scalar naming

Use one renderer-owned quoting routine for all column names. Escape embedded
backticks according to the Ibex lexer; never assume R names are syntactic Ibex
identifiers. Generated table and scalar binding names use a reserved,
collision-checked prefix.

Captured lexical constants are assigned generated scalar bindings and emitted
with Ibex's `^name` scope escape so a same-named column cannot capture them.
Reject unsupported captured objects before execution. Strings, dates,
timestamps, logicals, integers, and doubles use the existing scalar bridge;
extend the bridge only behind tests for ownership and exact type behavior.

## Lazy Plan and Translation

### R-side plan nodes

Keep quosures only at the dplyr capture boundary. Convert them immediately into
an Ibex-specific expression tree so unsupported syntax is discovered at the
verb that introduced it, not much later in `collect()`.

Initial node families:

```text
Source
Filter(predicates)
Project(fields)
Update(fields)
Group(keys, drop)
Aggregate(fields, keys)
Order(keys)
Distinct(keys, keep_all)
Join(kind, right, keys/predicate, suffix, na_matches)
Limit(kind, n, ties/order)
HostUdf(...)                 later, explicitly opt-in only
```

Grouping is metadata on the lazy plan until a consuming verb requires `by`.
`ungroup()` removes it without executing. A translated `summarise()` emits an
Ibex `select` with `by`; grouped `mutate()` must use Ibex's grouped/broadcast
semantics only after differential tests establish equivalence.

### Expression registry

Maintain separate registries for scalar, aggregate, window/ordered, and special
dplyr expressions. Each entry declares:

- accepted arity and named arguments;
- input/output type rule;
- whether it is valid in filter, update, grouped aggregate, or window context;
- null/`na.rm` behavior and any semantic deviation;
- renderer/lowering callback; and
- whether captured scalar arguments must be compile-time constants.

Start with operators and functions needed by ordinary analytical pipelines:

- arithmetic, comparison, boolean operations, and unary negation;
- `if_else`, `case_when`, `coalesce`, `between`, and null predicates;
- `sum`, `mean`, `min`, `max`, `count`/`n`, `n_distinct`, `first`, and `last`;
- `lag`, `lead`, ranks, and cumulative/rolling functions where Ibex semantics
  have a documented match;
- basic numeric, string, date, and timestamp operations already built into
  Ibex; and
- `desc`, `.data[[...]]`, and controlled `.env` scalar capture as translation
  forms rather than runtime functions.

Do not infer support from an R function's spelling alone. Namespace-qualified
calls such as `base::mean` and `dplyr::if_else` resolve through registered,
explicit identities. Masked user functions with the same name must not be
mistaken for translations.

### Rendering and execution

For the first implementation, lower the structured R plan into one generated
Ibex program and use `session_eval()`. The renderer may fuse adjacent compatible
steps into one bracket block, but correctness comes before minimizing syntax.
Let the normal Ibex parser, lowerer, canonicalizer, and runtime remain the
authorities for type checking and optimization.

Rendering rules:

- quote identifiers and string literals through central, tested functions;
- pass data and captured values through bindings rather than textual
  interpolation;
- keep generated names deterministic within a plan for readable
  `show_query()` output;
- preserve verb order whenever dplyr observes it;
- make every forced materialization visible in diagnostics; and
- translate native errors back to an R condition containing the dplyr verb and
  expression that produced the Ibex fragment.

If source rendering later becomes a measured bottleneck or prevents precise
source mapping, add a C ABI plan builder. It is not an MVP prerequisite.

## Semantic Compatibility Contract

Before claiming support for a verb/function, record and test these dimensions:

1. **Types.** R integer is 32-bit while Ibex `Int64` is 64-bit; R double,
   logical, character, factor/categorical, `Date`, and `POSIXct` require explicit
   mappings. Use vctrs prototypes for R-facing result contracts.
2. **Missing values.** Distinguish R `NA`, IEEE `NaN`, and Ibex null. Test
   comparisons, filters, boolean logic, aggregates, and `na.rm` separately.
3. **Vector sizes and recycling.** Only translations with well-defined Ibex
   equivalents are native. Reject unsupported R recycling rather than quietly
   changing results.
4. **Grouping.** Preserve key order, empty-group behavior, `.drop`, `.groups`,
   grouped mutate broadcast, and regrouping after renamed/removed keys.
5. **Ordering.** dplyr does not promise implicit database order, while an
   in-memory input has observable row order. Track when Ibex preserves,
   establishes, or invalidates ordering; require explicit order for verbs whose
   meaning depends on it.
6. **Joins.** Specify key coercion, output order, duplicate expansion, suffixes,
   `multiple`, `relationship`, and `na_matches`. Unsupported options fail at
   plan construction.
7. **Names.** Support non-syntactic and reserved names through quoted
   identifiers, plus dplyr name repair and join suffix behavior.
8. **Time zones and categoricals.** Do not claim preservation beyond the
   metadata actually retained by the native operators. Existing ribex timezone
   propagation limitations remain visible until fixed independently.
9. **Empty inputs.** Cover zero rows, zero selected columns, all-null columns,
   empty groups, and aggregates over empty data.
10. **Side effects and randomness.** Only explicit Ibex functions receive Ibex
    semantics. An R function that reads globals, uses RNG, performs I/O, or
    mutates state is never silently translated as pure.

Where exact equivalence is impossible but useful native behavior exists,
document the difference in the function capability table and make it available
from an R help page.

## Unsupported Expressions and Automatic Fallback

Fallback is a plan-construction decision:

1. Translate every preceding supported step into the native prefix.
2. If policy permits, execute and collect that prefix once.
3. Emit one warning naming the unsupported function/expression and the fact
   that subsequent work is local R execution.
4. Replay the current verb against the collected tibble using the original
   quosures and environments.
5. Return the local result, not another `ibex_tbl`.

Policy:

- `fallback = "error"`: always fail with the unsupported expression and a
  suggestion to call `collect()` explicitly.
- `fallback = "warn"` (default for in-memory R sources): collect once with a
  warning.
- `fallback = "collect"`: collect without a warning; still expose the boundary
  through `show_query()` before execution.
- Unknown-size, streaming, or plugin/lazy sources default to error even under
  `"warn"`; require a separate explicit opt-in to pull them into R.

Do not partially execute the current verb in Ibex if one of its expressions is
unsupported. That could reorder side effects or change grouped evaluation.

## Explicit R UDF Barrier (Post-MVP)

Provisional API:

```r
ibex_r_udf(
  fn,
  ...,
  .ptype,
  .kind = c("vector", "aggregate")
)
```

This is distinct from automatic fallback: it permits native execution to resume
after a deliberately declared R stage.

### Execution model

The R-level executor splits the lazy plan at every `HostUdf`:

```text
Ibex native segment
  -> Arrow C Data result
  -> R main-thread vector/group callback
  -> validate and bind callback result
  -> next Ibex native segment
```

Each native segment is a complete `.Call` that returns control to R. Only then
does ordinary R code invoke the closure. The callback result is converted to a
supported Arrow/R table representation and rebound for the following native
segment. Column liveness analysis should avoid crossing unused columns, but the
first correct implementation may carry all live output columns across the
barrier.

Never implement this by capturing a `SEXP` in `ExternRegistry`, calling R from
`eval_expr`, or dispatching a callback from the process worker pool.

### Callback contract

- `.ptype` is required and determines the static Ibex output type before
  execution.
- `vector` returns length one or the current ungrouped/group size according to
  documented dplyr recycling rules.
- Grouped `vector` evaluation calls R once per group in deterministic group
  order; it must not pretend that one whole-column call has grouped semantics.
- `aggregate` returns exactly one value per group. Multi-row results belong to a
  future `reframe`/table-UDF contract, not this node.
- Inputs and results are validated for type, length, names, nulls, and supported
  nested structure before the next segment begins.
- R errors and interrupts abort the whole mixed plan cleanly. No C++ exception
  crosses R's error machinery, and no partially computed session binding is
  presented as a successful `compute()` result.
- The closure remains strongly reachable from the R lazy-plan object. Native
  code does not own or preserve it between calls.
- The node is volatile and order-sensitive. Optimizations may occur within each
  native segment but never across the callback boundary.
- `show_query()` prints an unmistakable `R UDF BARRIER` with function label,
  kind, prototype, and the columns crossing it.
- Transpilation and execution outside an embedded R process reject the plan.

Whole-column calls make expensive R functions usable but do not promise a
speedup over dplyr for the callback itself. The value is that filters,
projections, joins, and aggregations around it can still run natively.

## Simple R Function Inlining (Optional Follow-up)

After the explicit translation registry is stable, support a deliberately small
class of pure closures without an R callback:

```r
scale_by <- function(x) (x - centre) / spread

ibex_tbl(df) |>
  mutate(z = scale_by(x))
```

Inlining is allowed only when the closure body is a single expression composed
entirely of registered translations, formal arguments, and supported immutable
captured scalars. Substitute formals into the expression tree, bind captured
values as typed scalars, and type-check the expanded Ibex expression normally.

Reject control flow, assignment, `...`, dynamic lookup, reflection, promises
with side effects, mutable captures, package calls without a registered
translation, and calls whose identity cannot be established. This is syntactic
translation of a constrained expression, not compilation of arbitrary R.

A public registration API for package authors may follow:

```r
register_ibex_translation(
  package = "mypackage",
  function = "winsorise",
  translator = ...,
  semantics = ...
)
```

Registration must describe types and semantics; it cannot merely substitute a
function name.

## Implementation Phases

### Phase 0 — Contract and package scaffolding

1. Add a versioned dplyr dependency and define the supported dplyr version
   window in `DESCRIPTION` and CI.
2. Write the type/null/group/order/join compatibility matrix before adding verb
   methods.
3. Add a read-only native schema endpoint for a bound table/session result,
   including ordered column names, type, nullability, categoricals, time zone,
   ordering, and time-index metadata where available.
4. Add central R helpers for Ibex identifier and string escaping, generated
   names, scalar capture, source maps, and typed diagnostics.
5. Define immutable `ibex_tbl`, plan-node, and expression-node constructors with
   invariant tests.

**Gate:** construct and print a lazy source, inspect schema, render a no-op
query, and collect it with values/metadata equivalent to the existing direct
`eval_ibex()` path.

### Phase 1 — Lazy source and terminal operations

1. Implement `ibex_tbl()`, `collect()`, `compute()`, `show_query()`, `print()`,
   `as.data.frame()`, and `as_tibble()`.
2. Bind input once per session and keep its ownership lease alive across lazy
   derivations.
3. Ensure two branches from one `ibex_tbl` are independent and repeatable.
4. Make session reset/finalization invalidate dependent lazy objects with a
   clear R error rather than a stale native pointer failure.
5. Provide test helpers that execute the same pipeline in dplyr and Ibex and
   compare data, types, names, order, groups, and missing values.

**Gate:** lazy identity pipelines are ownership-safe under R GC, repeated
collection, session finalization, and nanoarrow slice inputs.

### Phase 2 — Core single-table verbs

Implement in this order:

1. `select`, `rename`, `relocate`, and tidyselect resolution against stored
   schema;
2. `filter` with arithmetic/comparison/boolean/null translations;
3. `mutate` and `transmute` with scalar capture and sequential dplyr field
   visibility;
4. `group_by`, `ungroup`, and `summarise` for the initial aggregate registry;
5. `arrange`, `desc`, `head`/`slice_head` where ordering semantics are defined;
6. `count`, `tally`, and common verb wrappers in terms of the primitives.

Unsupported arguments fail at capture time. Do not advertise a generic until
its normal argument matrix has either an implementation or a precise error.

**Gate:** the core differential suite matches local dplyr for supported cases,
and every unsupported expression fails before any native prefix executes.

### Phase 3 — Joins, sets, distinct, and window coverage

1. Add equijoins one kind at a time, starting with `inner_join` and `left_join`.
2. Add `semi_join`, `anti_join`, `right_join` only when output order and suffix
   behavior are specified and tested.
3. Add `distinct`, set operations, ranks, lag/lead, and supported cumulative or
   rolling translations.
4. Add `across`, `if_any`, `if_all`, and tidyselect-driven expansion as R-side
   compile-time rewrites, analogous to Ibex's built-in `map` expansion.
5. Add `copy_to()` or session-resident source constructors only after lifecycle
   semantics are settled.

**Gate:** a representative multi-table analytical pipeline stays lazy through
`collect()` and its rendered Ibex is readable and deterministic.

### Phase 4 — Fallback policy

1. Implement unsupported-expression classification and the three policies.
2. Replay the complete current dplyr verb locally after collecting the native
   prefix.
3. Mark source locality/size knowledge and protect lazy/plugin-backed inputs
   from accidental collection.
4. Ensure warnings occur once, identify the boundary, and remain suppressible by
   the explicit `"collect"` policy.
5. Test that later verbs are ordinary local dplyr and never silently re-enter
   Ibex.

**Gate:** unsupported R functions produce either a correct one-time fallback or
a pre-execution error, with no partial current-verb execution.

### Phase 5 — Explicit vectorized R UDF barrier

1. Add the R-side `HostUdf` expression and required `.ptype` validation.
2. Split execution into native/R/native segments without adding R callbacks to
   the core runtime.
3. Implement ungrouped vector UDFs, then grouped vector UDFs, then scalar
   aggregate UDFs as separate slices.
4. Add column liveness across barriers and measure copy/ownership behavior.
5. Add interrupt, R error, GC, side-effect ordering, RNG, and session rollback
   tests.
6. Make `show_query`, `explain`, and codegen diagnostics expose host binding.

**Gate:** run native segments with `IBEX_PARALLEL=1` while proving that every R
closure executes serially on the calling R thread; results match dplyr for the
declared UDF kind and prototype.

### Phase 6 — Constrained closure inlining and extension API

1. Inline single-expression pure closures over the registered expression set.
2. Capture immutable lexical scalars through typed bindings.
3. Add an opt-in package translation-registration API with semantic metadata.
4. Keep rejection conservative and diagnostics specific.

**Gate:** an inlined closure yields the same generated Ibex as the equivalent
hand-written expression and needs no R callback at execution.

## Testing Strategy

### R unit and differential tests

Add `testthat` coverage for:

- plan immutability, generated-name collisions, identifier/string escaping,
  quosure environments, namespace-qualified calls, and masked function names;
- every supported verb/function across grouped and ungrouped data;
- integer/double coercion, factors, dates, timestamps, `NA`, `NaN`, nullable
  Arrow slices, empty inputs, and non-syntactic column names;
- ordering, ties, duplicate join keys, null join keys, suffixes, and group
  metadata;
- fallback warning/error behavior and exactly-once collection; and
- UDF result type/length errors, closure GC lifetime, side effects, interrupts,
  and grouped call count/order.

Use a reusable differential harness:

```text
ordinary tibble -> dplyr pipeline -> expected
same data       -> ibex_tbl pipeline -> collect -> actual
compare values + types + names + row order + groups + missingness
```

Do not use tolerant value-only comparison where exact type, validity, or order
is part of the contract.

### Native and integration tests

- Extend Arrow C Data tests only if new ownership shapes are introduced.
- Exercise session schema inspection and stale-session diagnostics at the C API
  boundary.
- Run existing parser/runtime/parity/e2e tests after any native API change.
- Run `R CMD check` for `ribex` in CI against the supported R/dplyr versions.
- Verify R callbacks under parallel-enabled native segments with a main-thread
  assertion or test hook.
- Rebuild bundled plugins after any public runtime/header change.

## Performance and Benchmarking

Measure three distinct costs in release builds:

1. **Plan overhead:** microbenchmark capture/rendering for 1, 5, 20, and 100
   verbs independently of data size.
2. **Native execution:** compare the same supported pipelines through direct
   Ibex, `ibex_tbl`, dplyr, and Polars at the repository's standard scales.
   The dplyr layer should add near-constant planning overhead, not row-scaled
   work.
3. **Boundary cost:** measure data-frame copy input, nanoarrow adoption,
   collection, automatic fallback, and explicit R-UDF barriers separately.

For changes to a hot native execution path, follow the repository rule: build
`build-release/` and use `benchmarking/compare_ibex_git.sh` against the
pre-change baseline before completion. R-only plan construction changes do not
need a native hot-path claim, but still need package-level overhead benchmarks.
When publishing comparisons, keep both single-threaded and default
multi-threaded Polars results.

## Documentation

For the first backend release, update:

- `r/ribex/README.md` with installation, lifecycle, fallback, and examples;
- R help pages/vignettes with the supported verb/function matrix;
- `README.md`, `docs/index.html`, and `docs/reference.html` with the dplyr entry
  point and the native-versus-R execution boundary; and
- benchmark documentation with dplyr-backend numbers distinct from direct Ibex
  syntax numbers.

Document `show_query()` output in every introductory example. State prominently
that ordinary unknown R functions do not run inside worker kernels, automatic
fallback stops native execution, and explicit R UDF barriers are
interpreter-only.

This plan should not require new Ibex language semantics. If implementation
does add syntax, built-ins, type-system behavior, or observable native semantics,
update both `SPEC.md` and `docs/index.html` in the same change as required by
the project workflow.

## Completion Criteria

The backend is ready for an initial supported release when:

- a documented core set of dplyr verbs remains lazy until `collect()`;
- `show_query()` produces valid, readable Ibex with safe identifier and scalar
  handling;
- supported pipelines pass differential value/type/null/order/group tests;
- unsupported expressions deterministically error or cross one visible
  fallback boundary;
- session and Arrow ownership remain safe under GC and repeated collection;
- no R API call can occur on an Ibex worker thread;
- direct Ibex execution has no material performance regression; and
- documentation distinguishes native translations, local fallback, explicit R
  barriers, and standalone-transpilable plans.

The later R-UDF milestone is complete only when vector, grouped-vector, and
aggregate contracts are independently tested; callbacks run solely on the main
R thread; native optimization cannot cross the barrier; and codegen rejects the
host-bound plan clearly.
