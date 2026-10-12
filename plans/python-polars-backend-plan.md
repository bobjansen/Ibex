# Python package: Polars 2.0 API compatibility and execution options

Status: independent q06/q01 slices implemented, 2026-10-12. The initial
Engine-only proposal has been broadened to weigh two architectures; the
Engine prototype remains open.

2026-10-12: independent q06 slice implemented under `python/ibex/`, with
immutable expressions/pipelines and Arrow-backed result containers. The new
native `execute_pipeline` endpoint uses the shared REPL evaluator; this slice
renders source with typed scalar bindings rather than exposing typed IR.
Grouping/q01 has since landed; the Engine prototype remains open. Standalone packaging,
Polars dtype descriptors and broader API compatibility remain follow-up work.
Validation: unchanged upstream q06 passes against the reference at SF-1 and
SF-10 with matching names, shape, Float64 output and revenue within the
documented tolerance. Each run invokes the native endpoint once. Ten focused
tests cover execution boundaries, null/empty reductions, date bounds,
ownership and unsupported behavior; existing bridge/notebook smoke tests pass.

q01 follow-up: grouped sums/means/UInt32 row counts and stable column sorting
with per-key null placement are implemented. Original q01 passes at SF-1
for Parquet and in-memory inputs and SF-10 for Parquet. In-memory binding is
retained and low-cardinality strings are prepared once using the existing
categorical threshold; this avoids the initial raw-import route's repeated
conversion and plain-string grouping costs. Seventeen focused tests pass,
including nullable keys, empty groups, source ownership and dictionary reuse.
Release measurements vary input size, group count and thread budget; see
`python/q01-performance.md`. Warm results are competitive, but first use
including input preparation remains slower in the recorded local case.
Priorities before expanding the API: investigate first-use allocations and
prepare only demanded input columns; keep preparation costs visible.

## Assessment after the first slices

Continue exploring an independent frontend, with a bounded scope. It already
preserves a native strength: grouped mean over prepared in-memory tables is
about 3.1x faster at one core and 1.6x at eight than the categorical-key
reference in the recorded 16-million-row case. q01 remains roughly tied.
These results justify another experiment, not a claim of general superiority
or a commitment to recreating the entire Polars API.

Prioritize demand-based input preparation and first-use allocations, with
repeatable cold/warm measurements. The eight-core grouped-mean first-use
cost is substantially worse despite its warm-query win, so the intended
workload must remain repeated analytics on resident inputs until that improves.
Retain native ownership and semantic conformance as gates alongside speed.

Then compare the planned small Engine frontend on the same native endpoint.
Choose between owning API compatibility and reusing Polars' frontend based on
maintenance effort, integration stability and complete-pipeline performance.
Broader joins/windows and standalone packaging follow evidence from that
comparison.

## Goal and acceptance target

Build a Python package exposing the Polars 2.0 programming model with Ibex
execution and lazy pipeline construction. The user's original intent is an
independent package with compatible API, while a backend for real Polars
objects is also an avenue worth weighing. Choose the architecture after a
small comparison rather than assuming the Engine approach is the product.

The first complete compatibility milestone is all 22 upstream Polars PDS-H
query bodies, with correct results and no Polars execution fallback. This is
an initial subset of Polars 2.0 compatibility, not a claim to implement its
entire API. Publish exactly which methods, options, types and semantics work.

Independent-package usage (proposed package name):

```python
import ibex as pl

result = (
    pl.scan_parquet("trades.parquet")
    .filter(pl.col("price") > 10)
    .group_by("symbol")
    .agg((pl.col("price") * pl.col("size")).sum().alias("total"))
    .collect()
)
```

Here `LazyFrame`, `Expr`, `DataFrame` and `Series` are Ibex-owned classes;
Polars is a development reference and optional interoperability dependency.
User query bodies stay the same after changing `import polars as pl` to
`import ibex as pl`. Resolve distribution/module naming and any conflicts
before packaging; `ibex` is an illustrative name, not a reserved package.

Engine alternative:

```python
import polars as pl
from ibex_polars import IbexEngine
from queries.polars.q1 import q

result = q().collect(engine=IbexEngine())

# For existing application code using collect() with its default engine:
pl.Config.set_engine_affinity(IbexEngine())
```

With the Engine approach, query bodies and imports remain unchanged;
backend selection is a launcher/application configuration change. Upstream
PDS-H explicitly chooses an engine in
`queries.polars.utils.obtain_engine_config()`, so affinity alone does not
change its benchmark executor. Our launcher must override that configuration
function before invoking the upstream module.

With an independent package, redirect the query modules' `pl` binding and
provide Ibex inputs or override their table-loader utilities in a dedicated
test launcher. Keep the upstream files unchanged on disk and the reference
Polars run isolated. Do not replace `sys.modules["polars"]` process-wide:
the benchmark utilities and result checker also use real Polars. Test the
literal import-change workflow separately. Compatibility claims must state
which imports/configuration changed; query-body equivalence is the target.

Define eager/lazy behavior explicitly. Polars-compatible `DataFrame` methods
should remain eager; `LazyFrame` methods build plans and `collect` executes.
Offer lazy scans as the default recommended pipeline entry point. Making all
`DataFrame` operations lazy would change observable Polars behavior and needs
a separately named API. An Engine only sees work that reaches a lazy plan;
it cannot recover eager work already executed by Polars.

## Weigh the architectures before committing

| Question | Independent compatible package | Engine for real Polars |
|---|---|---|
| User-facing identity | Ibex package; import change | Polars package; executor selection |
| Expression parsing and selectors | Implement and maintain compatible behavior | Reuse Polars frontend behavior |
| Planning boundary | Own typed expression/plan graph | Translate Polars plan through its integration seam |
| Semantic work | API behavior plus execution semantics | Execution semantics plus plan translation |
| Independence | Core runtime can work without Polars installed | Polars is a required frontend dependency |
| Version exposure | Track public Polars 2.0 API/behavior | Track Engine and private plan APIs as well |
| Eager frames and results | Own frame/series implementation and Arrow interop | Real Polars frames; eager work stays in Polars |
| Whole-pipeline optimization | Ibex receives its own complete graph | Establish interaction between Polars and Ibex optimization |
| Main uncertainty | Breadth/cost of faithful API compatibility | Stability and coverage of plan extraction |

The Engine route may reduce frontend work; it is not automatically quicker
overall until extraction and semantic adaptation are demonstrated. The
independent route better matches the original product intent, but a familiar
method surface is much easier than full behavioral compatibility.

Use q06, q01 and small semantic fixtures for a bounded prototype of each
route. Probe shared plans, reduction-in-filter and filtered aggregates early.
Compare code/maintenance burden, installation dependencies, correctness
gaps, translation overhead and ability to preserve whole-pipeline execution.
Record a decision in this plan with evidence and remaining uncertainties.
Do not require two production frontends for the first release. Keep the
native plan endpoint reusable so a second frontend can be added later.

## Findings and existing foundations

- The repo's environment currently has Polars 2.0.0. Its `pl.Engine` requires
  `name`, `collect`, and `execute`; configured affinity accepts engine objects.
  Polars explicitly marks the custom-engine interface unstable. See the
  [upstream engine implementation](https://github.com/pola-rs/polars/blob/main/py-polars/src/polars/lazyframe/engine.py).
- Local `LazyFrame._ldf.visit()` returns a `NodeTraverser` exposing nodes,
  expressions, schemas and dtypes. This is a private integration seam to
  investigate, not an established compatibility guarantee.
- Polars' GPU engine uses a post-optimization callback. Investigate that path
  alongside direct visitation, especially optimizer flags and shared plans.
- Plan serialization is version-dependent; do not use serialized plans or
  rendered `explain()` text as a stable interchange contract. See
  [Polars serialization documentation](https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.serialize.html).
- `python/ibex_pyarrow_module.cpp` already imports/exports Arrow tables and
  binds typed scalars. Reuse that ownership code. Its source-evaluation path
  needs an audit before it becomes this backend's execution entry point.
- `r/ibex/` provides useful precedents for lazy bindings, schema inspection,
  explicit fallback and compatibility documentation.
- `benchmarking/tpch/bench_pdsh.py` already launches upstream modules unchanged;
  `check_against_polars.py` already compares answers at the measured scale.
- The inspected PDS-H checkout is commit
  `401908a307f133a2dffbe3f5e0e0fb2b50ea310b`. Record the actual revision in every
  test/benchmark run; do not treat this inspected revision as a permanent pin.

## Architecture

Independent frontend:

```text
Polars-compatible Python calls
    -> Ibex-owned immutable Expr and LazyFrame graph
    -> typed Ibex plan and source bindings
    -> existing Ibex optimization and physical execution
    -> Ibex DataFrame/Series backed by native or Arrow storage
```

Engine frontend:

```text
unchanged Polars Python query
    -> Polars lazy logical plan
    -> version-specific plan adapter
    -> typed Ibex plan and source bindings
    -> existing Ibex optimization and physical execution
    -> Arrow export -> Polars DataFrame
```

Use one translation of the complete reachable plan, with no intermediate
collection. Preserve shared subplans as a graph; evaluate their execution and
caching policy explicitly. A reused relation must not become a premature
Python materialization or an accidental exponential expansion.

Prefer typed lowering into existing Ibex IR. Keep Polars-specific defaults and
semantic adaptations in the frontend; extend Ibex IR/runtime only when needed
to represent a real missing operation. Do not route through SQL.

A short source-rendering prototype is acceptable to test integration, but
must carry scalars as typed bindings, escape identifiers correctly, and use
the complete pipeline planning path. Decide whether to retain it only after
checking semantic coverage, planning overhead and maintenance cost. Avoid
building a second optimizer or maintaining handwritten PDS-H translations.

## Phase 0: inventory and compare prototypes

Common work:

1. Freeze Polars 2.0.0 as the initial behavioral reference and record the PDS-H
   revision. Inventory calls, positional/keyword signatures, defaults,
   overloads and returned types across all upstream query functions.
2. Build a method/option/type coverage matrix. Include `pl.col.name` access,
   top-level aggregates, strings as column references versus literals,
   keyword aliases, expression operators and `str`/`dt` namespaces. Measure
   API usage by executing query construction, not only searching source.
3. Establish the native whole-plan execution endpoint and execution evidence
   shared by both prototypes. No data decoding during lazy construction
   (metadata reads are allowed); no hidden reference-engine execution.

Independent-package prototype:

1. Implement immutable expression nodes, literal normalization and operator
   overloads, plus lazy scan/filter/select/reduction/group/sort nodes.
   `bool(expr)` raises a useful error; Python `and`/`or` cannot build a plan.
2. Normalize supported Polars call forms before lowering. Keep column binding,
   output names, expression cardinality and schema inference explicit.
3. Run q06 and q01 with an import/binding change only. Return an Ibex frame
   with schema, shape and Arrow conversion; do not return a raw Arrow table
   under a compatibility claim that promises DataFrame methods.
4. Verify the core package imports and runs without Polars installed. Use
   Polars solely in a separate reference/conformance environment.

Engine prototype:

1. Pin an exact development Polars version and isolate private API access in
   one adapter module. Record visitor version information and reject unknown
   versions before execution. Decide the supported version range using tests.
2. Compare direct visitation with the GPU-style optimized callback on tiny
   scan/filter/aggregate/join plans. Establish how optimization flags apply,
   whether traversal opens sources, and how the root result reaches `collect`.
   Check raw and optimized node shapes: an optimized plan may expose nodes
   absent from the user-facing query.
3. Implement a minimal `IbexEngine` skeleton and translate a Parquet
   scan/filter/project. Return a real Polars frame through Arrow. Implement
   `execute` using the supported result wrapper for the pinned version.
4. Add execution evidence: native invocation count and a translated-plan report.
   Verify no reference-engine collection executes the query and no file data
   is decoded during construction or translation (metadata reads are allowed).
5. Enumerate nodes/functions/options for all 22 upstream plans and write a
   per-query coverage matrix. Run the query functions themselves; Python text
   searches are insufficient because optimization changes plan shapes.

Exit: a documented choice based on comparable q06/q01 prototypes and
semantic probes. For the Engine route, document extraction coverage and
version policy. For the independent route, document API coverage and own
frame/series scope. Both must demonstrate execution of a complete native
pipeline and identify gaps before the 22-query implementation begins.

## Phase 1: native execution and semantic contract

Expose a narrow native endpoint for validated plans, source bindings, schema,
explanation and execution. Reuse existing optimizer/runtime entry points;
audit which passes the current Python host runs rather than assuming it is
equivalent to the CLI or R host. Keep the interface independent of Polars so
other Python frontends and R can reuse it where practical.

If the independent package is chosen, define the Python object model here:
`LazyFrame`, `LazyGroupBy`, `Expr`, conditional builders, type descriptors,
`DataFrame` and `Series`. Expressions are immutable; schema inspection may
resolve metadata without executing rows. Eager frame methods dispatch one
native plan per call; `.lazy()` retains its input ownership. Provide explicit
`to_arrow`, `to_pandas` and optional `to_polars` conversion boundaries, and
the result inspection/indexing methods actually used by supported workflows.
Avoid requiring real Polars to implement result containers.

For Parquet inputs, pass paths/schema/scan options to Ibex lazy readers;
do not collect through Polars to create an input table. Initially support
local benchmark files and reject unsupported dataset/cloud/scan options.
For in-memory inputs, use Arrow adoption with explicit buffer lifetimes and
document copying paths. Keep Python owners alive until execution completes.
Release the GIL only around native work that cannot touch Python objects;
propagate cancellation and exceptions without returning partial results.

Write a compatibility contract covering:

- Ordered names and exact output dtypes, integer promotions/overflow, division,
  casts, date units, null versus NaN, categorical representation and rounding.
- Null predicate behavior, `len` versus non-null `count`, empty/all-null
  aggregates, distinct counts, and expression scalar broadcasting.
- Simultaneous `with_columns` evaluation against the input schema, aliases,
  selectors and computed group keys; avoid accidental sequential updates.
- Join key coalescing, suffixes, output layout, duplicate multiplicity, null
  equality and requested ordering. Never infer uniqueness from PDS-H data.
- Sorting directions, null placement, stable-order requests, limits and ties.
- Regex `str.contains` semantics (not SQL LIKE), string slice indexing,
  date extraction, `is_between` bounds and literal-list membership.

Unsupported calls, nodes, flags and options fail before any data execution, naming
the operation and location. Strict execution is the MVP default. A future
explicit whole-query fallback may be offered, but benchmark runs require
strict mode and proof of native execution. Arbitrary Python UDFs, async,
background collection, batch collection and sinks can initially raise clear
unsupported errors. Do not silently ignore optimizer flags or callbacks.
For the independent package, document which `collect(engine=...)` values are
accepted and what they mean; accepting a Polars engine label must not silently
pretend to run that executor. Adapt upstream benchmark executor options
in the launcher, outside the unchanged query bodies.

## Phase 2: cover upstream PDS-H incrementally

Implement capabilities by query family; confirm this order against Phase 0's
actual API/plan inventory:

| Milestone | Initial queries | Main capabilities |
|---|---|---|
| A | q06, q01 | Scans, predicates, date literals, arithmetic, global/grouped reductions, aliases, sort |
| B | q03, q05, q10 | Inner joins, computed aggregates, multi-key grouping, descending sort and limit |
| C | q04, q13, q16, q18, q22 | Semi/anti/left/cross joins, null-sensitive counts, distinct, regex, string slice |
| D | q02, q11, q15, q17, q20, q21 | Shared relations, reduction broadcasting in predicates, nested aggregates and filtered aggregates |
| E | All remaining queries | Complete dates, conditionals, membership, rounding and observed optimized node coverage |

Each milestone adds unchanged upstream query tests and small adversarial
semantic fixtures. Map a plan shape generally; no query-number branches or
rewrites to the existing handwritten `.ibex` benchmark queries. q15's maximum
inside a filter and q21's filtered distinct aggregate deserve early probes:
they may expose runtime gaps despite broader TPC-H support.

## Phase 3: conformance and benchmark integration

Create Python API/semantic conformance tests and a PDS-H runner with a
`--pdsh-root` option. Run the same upstream lazy query with Polars streaming
and Ibex over identical files. Compare names, column order and dtypes exactly;
compare values with documented, query-appropriate float tolerances and tie
rules. Reuse the existing answer checker's ordering knowledge, but do not
inherit its relaxed column-name matching or blanket monetary tolerance as
the new backend's compatibility contract.

Test null keys, duplicates, empty/all-null inputs, Unicode strings, rounding
boundaries and shared plans separately. Test construction without execution,
repeated collection, source lifetime, native errors and unsupported-feature
rejection. For an Engine, test unsupported-version rejection too. For an
independent frontend, test signatures, returned object types, overloads,
expression naming/cardinality and eager/lazy behavior against the frozen
Polars reference. Integration tests assert native execution for every query.

Extend the benchmark launcher with a distinct framework name identifying
the selected frontend (`pdsh-ibex-api` or `pdsh-ibex-engine`). For an Engine,
override upstream engine configuration in the child launcher before module
execution. For an independent frontend, bind the module's `pl` and supply
Ibex table loaders, then invoke its unchanged `q()` function through our own
timing/result adapter. Do not pretend the upstream preload and version
reporting utilities work unchanged with our independent package. Validate
sample counts and exit status; upstream catches query exceptions. Check
results before measuring. Keep direct Ibex DSL timings as a separate series.

Follow `MEASURING.md`: release builds only, one-thread and matched multicore
comparisons against Polars streaming, plus its default thread setting for
headroom. Record Polars/PDS-H/Ibex revisions, input scale, CPU budgets and
fresh/warm policy. Report plan translation, native execution and output
conversion separately as diagnostics, and include all three in end-to-end
collection timings. Never cache results between timed collections. Any
prepared-plan cache needs separate cold/warm results. Benchmark hot-path
runtime edits before/after with `benchmarking/compare_ibex_git.sh`.

Exit: 22/22 unchanged query bodies pass at SF-1 and at least one larger scale,
with no fallback; reports identify adapter overhead and runtime differences.
Performance improvements are measured outcomes, not a correctness gate or
an assumption of speed from laziness.

## Phase 4: package and documentation

Package the chosen frontend and its native dependency as an installable
distribution; keep the existing notebook bridge working. Resolve the name
before release. Start with a Linux wheel and clean environment installation
test, then document the platform support matrix. An independent package must
install/run without Polars; expose interoperability as an optional extra.
Publish the reference API version and compatibility matrix (and supported
runtime Polars versions for an Engine), setup, a PDS-H example showing the
exact import/configuration change, ownership and unsupported behavior.
Update README.md, SPEC.md's Python integration appendix and docs/index.html
to describe implemented behavior only, with portable paths.

## First implementation slice and required checks

Start with the independent q06 slice and common native execution endpoint,
then add q01. Build the Engine prototype afterwards against that same native
endpoint for a sequential comparison, as agreed on 2026-10-12.
Record the architectural decision before expanding to the full suite. This
provides reviewable initial changes and a concrete basis for weighing the
two approaches without requiring full Polars compatibility first.

Run focused Python conformance tests and relevant native tests. Public
header/runtime changes require plugin rebuilds via
`scripts/ibex-plugin-build.sh`; parser/lexer/AST/IR/codegen changes require the
full applicable CTest suite under project instructions. Keep a per-query
coverage table updated until the 22-query milestone is complete.
