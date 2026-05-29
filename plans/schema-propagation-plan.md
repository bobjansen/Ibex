# Schema Propagation and Static Schema Checking

## Goal

Compute the column schema of every DataFrame/TimeFrame value at compile time by
propagating schemas bottom-up through the IR, so that schema mismatches are
caught at lower time instead of at runtime. Pair this with a runtime-validated
**schema ascription** (`as`) that converts the unknown schema of an I/O source
into a statically known one, letting static analysis resume past the I/O
boundary.

This is the prerequisite that unblocks moving the `DataFrame<Schema>` argument
and return contracts (today validated at runtime in `validate_table_type`) to
compile-time checking where the schema is statically known.

## Why this is worth doing

The language already bills itself as statically typed and transpiles to C++,
but table schemas are currently only known at runtime. A schema pass closes
that gap. The per-operator derivation rules already exist on paper in
SPEC.md Section 8.x ("Schema Derivation" table); the work is implementing them
as a real pass, not inventing semantics.

## Current State

- IR is a tree of `ir::Node` (`include/ibex/ir/node.hpp`) with `children()`;
  nodes carry **no schema information** today.
- Node kinds cover the full surface: `ScanNode`, `FilterNode`, `ProjectNode`,
  `DistinctNode`, `OrderNode`, `HeadNode`/`TailNode`, `AggregateNode`,
  `UpdateNode`, `RenameNode`, `JoinNode`, `WindowNode`, `MeltNode`, `DcastNode`,
  `CovNode`, `CorrNode`, `TransposeNode`, `MatmulNode`, `ModelNode`,
  `ConstructNode`, `ExternCallNode`, plus fused nodes (`FilterProjectNode`,
  `FilterUpdateProjectNode`, `TopKNode`, ...).
- SPEC.md Section 3.3 defines schema types; the derivation table (search
  "Schema Derivation") specifies output schema per clause.
- Schema contracts are validated at runtime by `validate_table_type`
  (`src/repl/repl.cpp`), used for function arguments, returns, and typed `let`
  bindings. See [[project-named-arguments]] for the related "where contracts
  bind" map.
- Bare `DataFrame` (e.g. from `read_csv`) has an implementation-inferred schema
  per SPEC 3.2 — i.e. statically **Unknown**.

## Design

### 1. The `SchemaInfo` lattice

```
SchemaInfo = Known(ordered list of { name, optional<ScalarType> })
           | Unknown        // ⊥: anything; defeats static checking downstream
```

(Implemented refinement: each field's type is `optional` — a column may be
*known to exist* with its type *not yet inferred*. This lets `Known` carry the
column-name set even before expression type inference exists, which is what
unlocks early "missing column" detection without waiting for Stage 3.)

`Unknown` is the top element: any operator fed an `Unknown` input generally
produces `Unknown`, unless the operator fully determines its output regardless
of input (rare). Tracking exact `Known` schemas (not just a lower bound) is
required because downstream operators need column *types*, not just presence.

Note the distinction from the function-argument contract, which is a
**minimum-required** (lower-bound) check: a `Known` schema from propagation is
exact; contracts then check that the required columns are a subset of it.

### 2. The `schema_of` pass

A bottom-up walk: `schema_of(node) -> SchemaInfo`, memoized per `NodeId`.
Realizes the SPEC derivation table. Representative rules:

- `ScanNode` / source → declared schema if the source declares one, else
  `Unknown`.
- `FilterNode`, `OrderNode`, `HeadNode`, `TailNode` → input schema unchanged.
- `ProjectNode` (select) → `Known` = listed fields (types inferred from input +
  expression inference, see §4).
- `UpdateNode` → input `S ∪ new fields`.
- `DistinctNode` → listed fields.
- `RenameNode` → input with columns relabelled.
- `AggregateNode` (+ `by`) → group keys + aggregate output columns.
- `JoinNode` → `A ∪ B`.
- `ConstructNode` (`Table { ... }`) → `Known` directly from the literal.
- `DcastNode`, `TransposeNode`, `MatmulNode` → **data-dependent output
  columns** → `Unknown` (these are the natural ascription points, §3).

Where both sides of a check are `Known`, mismatches become lower-time errors.
Where either is `Unknown`, fall back to the existing runtime
`validate_table_type` — same error shape, just deferred.

### 3. Schema ascription (`as`) — the escape hatch for ⊥

A runtime-checked ascription that injects a `Known` schema at a boundary:

```
let t = read_csv("trades.csv") as { date: Date, px: Float64 };
```

- **Runtime:** validate the table against the asserted schema (reuse
  `validate_table_type`); error if it does not conform.
- **Static:** from the ascription node downward, `schema_of` returns
  `Known(asserted schema)`, so the rest of the pipeline is statically
  checkable.

This is the standard checked-cast / type-guard pattern: it converts runtime
knowledge into static knowledge. It is also the recovery point for the
data-dependent operators (`dcast`/`transpose`) — ascribe their output to
re-enter static analysis.

Two complementary forms:
- inline `expr as { fields }` ascription at the use site (flexible — one
  `read_csv` serves many files);
- declared reader return schema: `extern fn read_typed(...) -> DataFrame<{...}>`
  (clean when a reader always yields the same shape).

### 4. Expression-level type inference

To type derived columns (`select { x = price * 2 }`), the pass needs to infer
the result `ScalarType` of an expression given the input column types: literals,
arithmetic/comparison operators, built-in/extern call return types, casts. This
is a second inference layer beneath the schema pass and is the other substantial
chunk of work.

### 5. Where the pass lives

A new checker pass over the lowered IR, run after `lower` and before/with the
existing effect checking (`src/parser/effects.cpp`). It annotates nodes (or a
side table keyed by `NodeId`) with `SchemaInfo` and reports lower-time schema
errors. Fused nodes must derive schema consistently with their unfused
equivalents (or run the pass before fusion).

## What this unlocks

1. **Compile-time `DataFrame<Schema>` contracts.** Function argument and return
   mismatches are reported at lower time when the caller's schema is `Known`,
   not after partial execution. (Resolves the open question in
   `plans/udf-dataframe-plan.md`.)
2. **Typed errors before any data is touched** — referencing a missing/renamed
   column in `select`/`update`/`filter` becomes a lower-time error instead of a
   runtime one, with the column known statically.
3. **Return-schema inference and documentation** (udf-dataframe-plan Phase 4) —
   once output schemas are computed, helpers can declare and have verified
   `-> DataFrame<{...}>` returns.
4. **Safer optimizer/fusion** — fusion and reordering rewrites can rely on known
   schemas instead of re-deriving column sets ad hoc.
5. **Codegen specialization** — known column types let the emitter pick concrete
   `ibex::ops` instantiations instead of dynamic dispatch (ties into the fused
   codegen item in [[project-execution-roadmap]]).
6. **A foundation for pushdown** — static schemas are a precondition for
   translating pipelines to SQL/ADBC backends, where column types must be known
   up front ([[project-execution-roadmap]]).

## Staging

1. **`SchemaInfo` + `schema_of` for determinate operators.** — **DONE.**
   Implemented in `include/ibex/ir/schema.hpp` + `src/ir/schema.cpp`, tested in
   `tests/test_ir_schema.cpp`. `ir::infer_schema(node, sources)` propagates
   bottom-up. Covered: Scan/ExternCall (resolve from a `SourceSchemas` env, else
   `Unknown`), filter/order/head/tail/as_timeframe/window/distinct passthrough,
   project (fixes the column set even over an `Unknown` child), rename, update
   (with trivial column-ref/literal type inference), aggregate (keys + outputs;
   count→Int64, the always-double aggregates→Float64, min/max/first/last preserve
   input type, sum/mean deferred), join (A ∪ B with key dedup), construct,
   columns. Everything else returns `Unknown` (the sound default): resample,
   melt, dcast, cov, corr, transpose, matmul, model, stream, and the fused
   nodes. No surface-syntax change. Not yet wired into any error-reporting site —
   that is Stage 4. Strict-g++ clean.
2. **Schema ascription `as`.** — **DONE.** Surface syntax
   `expr as DataFrame<{...}>` (KeywordAs in the lexer; postfix in
   `parse_postfix`; `AscribeExpr` in the AST). Lowers to a new `ir::AscribeNode`
   (NodeKind::Ascribe) carrying the schema as `ir::SchemaField`s. The interpreter
   validates the child table against the schema (minimum-required-columns: each
   listed column must exist with a matching type) and passes it through; missing
   or wrong-type columns error. `infer_schema(Ascribe)` returns `Known(schema)`,
   which is the boundary that defeats `Unknown`. Classified as a pipeline Breaker
   so the chunked path routes through `interpret_node`. Codegen emits the child
   as a transparent identity (does not yet re-validate — noted in SPEC §3.6).
   Documented in SPEC §3.6. Tested in test_parser/test_lower/test_e2e/
   test_ir_schema. (Ascription is now exact-with-`*` and reader return schemas
   feed the source env — see stage 6.)
3. **Expression type inference** for select/update-derived columns. — **DONE.**
   `expr_type` in `src/ir/schema.cpp` now resolves binary arithmetic (with
   numeric promotion; `/` → Float64), scalar casts (`Int64(x)` etc.), and the
   common columnar/rolling built-ins (`abs`/`cumsum`/`lag`/`rolling_*` preserve
   or produce their documented type). Aggregate Sum/Mean are resolved (Sum
   preserves the input type, Mean is Float64). Anything uncertain stays
   `nullopt`, keeping results sound. The runtime `infer_expr_type`
   (interpreter.cpp) remains authoritative; the static pass mirrors its
   unambiguous cases (intentional partial duplication, noted in the source).
   Full per-builtin parity is a later refinement.
4. **Promote contracts to static.** — **DONE (for the ascription contract).**
   `lower_ascribe` runs `infer_schema` on the lowered base; when the input
   schema is `Known`, an ascription the input provably cannot satisfy (missing
   required column, or a provably different column type) is a lower-time
   `LowerError`. When the input is `Unknown` (e.g. an I/O source) the check
   falls back to the interpreter's runtime validation. Documented in SPEC §3.6;
   tested in test_lower (static rejection) and test_e2e (runtime fallback).

   Function argument/return `DataFrame<Schema>` contracts remain runtime-checked
   in the REPL: user functions are evaluated dynamically (bodies are not lowered
   as a unit and arguments are eager runtime tables), so there is no static
   caller schema to check against today. Promoting those to compile time depends
   on whole-program schema flow / UDF transpilation — a later architectural step.
   The `as` ascription is the supported way to assert a schema statically in the
   meantime.

5. **Clause-level column-reference validation.** — **DONE.**
   `ir::check_column_refs` (src/ir/schema.cpp) walks the lowered IR and, where
   the input schema is `Known`, rejects references to absent columns. Wired into
   `lower()` (before the optimizer fuses nodes) and `lower_expr()` (the REPL
   path). `Unknown` inputs fall back to runtime validation. Documented in
   SPEC §6.6; tested in test_lower and test_repl.

   The column-only positions (`select`/`order`/`rename` targets, `by` group
   keys, aggregate source columns) are always checked. `filter` and computed
   `select`/`update` expressions are also checked **when the caller supplies the
   complete set of in-scope binding names** (`LowerContext::lexical_names`,
   populated by the REPL from its registries): a reference is flagged only when
   it is neither a column nor an in-scope binding, since the interpreter resolves
   those positions against operand-columns ∪ scalars. The check confirmed the
   resolver only consults columns+scalars there, so the binding set is complete
   and the pass does not false-positive (verified by the full suite). The
   compile path (whole-program `lower()`) leaves expression checking off, as it
   does not assemble the in-scope binding set. `Count`/computed-input aggregates
   are skipped.

6. **Exact schemas, wildcards, and reader return schemas.** — **DONE.**
   Declared schemas are now **exact/closed by default** with an opt-in `*`
   wildcard for "extras allowed" (`DataFrame<{a: Int, *}>`). Added:
   - `SchemaType::open` (parser) + `*` parsing; `ir::SchemaInfo::is_open()` and
     `AscribeNode::open()`; openness propagates in `infer_schema` (literals/
     projections/aggregates close the set; passthrough/update/rename inherit;
     join open if either side is).
   - `check_column_refs` only runs missing-column checks on a **closed** Known
     input, so wildcard schemas never false-positive.
   - `as` ascription is exact: besides required+type checks it now forbids
     unlisted extra columns (statically when the input is closed-Known, and at
     runtime in the interpreter) unless `*` is given.
   - Declared **reader return schemas** feed the source env
     (`build_source_schemas` from the extern decls), so a typed reader call like
     `read_typed(...)[select { typo }]` is checked at lower time. A reader that
     may yield more columns declares `-> DataFrame<{..., *}>`.

   Distinction documented: reader/ascription schemas are exact (assert concrete
   data shape), while function-parameter `DataFrame<Schema>` contracts stay
   minimum-required (reusability) — SPEC §3.3/§3.6/§10.3. Tested in test_lower
   (reader + ascription, exact vs wildcard) and test_repl.

## Non-Goals (initial waves)

- Inferring schemas through `dcast`/`transpose`/`matmul` without an ascription.
- Row-count / ordering-constraint propagation (separate concern from column
  schema; ordering is already tracked as metadata per SPEC 3.2).
- Dependent or value-indexed schemas.
- Removing the runtime `validate_table_type` path — it remains the fallback for
  `Unknown` and the implementation of the ascription check.

7. **Schemas across `let` bindings.** — **DONE.** The REPL builds an exact
   (closed) `ir::SchemaInfo` from each in-scope table in the runtime registry
   (`table_schema_info` in repl.cpp) and passes them via
   `LowerContext::source_schemas`. The `Lowerer` overlays these binding schemas
   on the declared reader schemas (`Lowerer::source_schemas()`, bindings shadow
   readers) and uses the combined env in both `lower_ascribe` and the
   `check_column_refs` pass. A reference to a column a let-bound table lacks is
   now a lower-time error (was runtime). Tested in test_repl; demo/schema
   updated to the natural let-based flow.

8. **More operators in `infer_schema`.** — **DONE.**
   - `melt` → id columns (types from input) + `variable: String` + `value`
     (common measure type when determinable); closed.
   - `cov` / `corr` → `column: String` + one `Float64` per numeric input column;
     requires a fully-typed closed input (else `Unknown`).
   - `resample` → time-bucket column (named after the input's time index) +
     group keys + aggregate outputs; **closed** when the input's time index is
     known, otherwise open (see stage 9).

   `dcast`/`transpose`/`matmul`/`model` remain `Unknown` (genuinely
   data-dependent — see Non-Goals). Tested in test_ir_schema.

9. **Time index in `SchemaInfo`.** — **DONE.** `SchemaInfo` carries an optional
   `time_index` (the designated TimeFrame time-column name). `as_timeframe` sets
   it (and promotes that column to `Timestamp`); passthrough ops preserve it;
   `update` keeps it; `rename` remaps it; `project` keeps it only if the column
   is retained; `aggregate`/`join`/`melt`/`cov`/`corr`/`construct` clear it.
   `resample` reads it to name the output bucket column, so a resample over a
   time-indexed input is now a **closed** schema (was open). The `TimeFrame`
   *type kind* is intentionally kept distinct from `DataFrame` (it carries
   invariants the schema does not). Tested in test_ir_schema.

## Follow-ups (post-merge candidates)

- **Named schema aliases** (`type X = { ... }`) to avoid repeating column lists.
- **Time index from declared `TimeFrame<S>`** sources/ascriptions: infer the
  index as the sole `Timestamp` field. (Today only `as_timeframe` sets it; the
  ascription/reader-declared TimeFrame case leaves it unset.)

Note on the two paths: cross-`let` checking works on **both** execution paths,
by different mechanisms. The whole-program `lower()` / `ibex_compile` path
inlines a clone of each let-bound subtree at its use site (`lower_identifier`),
so schemas flow through `infer_schema` directly — no binding-schema env needed.
The REPL/script path keeps let-bound tables as runtime values and re-reads them
as opaque `ScanNode`s, so it supplies their schemas via
`LowerContext::source_schemas` (built from the runtime registry, stage 7).

## Open Questions

- Store `SchemaInfo` on the node, or in a side table keyed by `NodeId`? Side
  table avoids churning `node.hpp` and survives rewrites better.
- Run the pass before or after optimizer fusion? Before is simpler (fewer node
  kinds to handle); after lets fusion benefit from schemas.
- Ascription surface syntax: `expr as { fields }` vs a built-in
  `assert_schema(expr, { fields })`. `as` reads better and matches the cast
  family already in the lexer.
- Should a declared-but-violated reader schema fail eagerly at the source call,
  or only where a downstream consumer relies on it? (Eager is clearer.)
