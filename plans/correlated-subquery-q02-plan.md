---
name: correlated_subquery_q02
description: "Add a Q2-first correlated scalar-subquery feature: one-level outer(column), scalar(table), and decorrelation to aggregate plus join"
metadata:
  node_type: memory
  type: project
---

# Q2-first correlated scalar subquery plan

Status: **COMPLETE** (July 2026). `outer(column)` / `scalar(table_expr)` parse,
lower, decorrelate, interpret, and code-generate; Q2 is in the executable
benchmark corpus and passes the official SF-1 answer check.

## What was built, and the one deliberate deviation

Phases 3 and 4 below call for a temporary `CorrelatedApplyNode` in the IR plus a
pass that eliminates it. **That node was not built.** The decorrelation happens
in the lowerer instead, in `Lowerer::lower_filter` (`src/parser/lower.cpp`),
which already has everything the pass would have needed: the AST of the
subquery, the outer scope, and the lowered outer input. It emits `Aggregate` →
`Rename` → `Join(Left)` → `Filter` → `Project` directly.

The node would have bought nothing and cost real surface area: an IR node that
must never survive lowering has to be defended against in schema validation,
join pushdown, canonicalization, the interpreter, the chunked operators, and the
emitter — six places that can leak. Building the plan straight from the AST means
there is no artifact to leak, and phase 5's requirement (only pre-existing nodes
reach the runtime) holds by construction rather than by six assertions. Every
other constraint the plan sets — no nested-loop Apply, no second read of a shared
source, elimination before schema validation — is met.

Two rules the plan did not anticipate, both discovered by building it:

- **A filter must not widen its input.** The left join adds the generated
  `__ibex_scalar_N` column, so the plan ends in a `Project` back to the outer
  query's columns. Naming them requires a statically known, closed outer schema;
  without one the lowerer errors rather than silently returning a wider table.
- **`scalar` was already taken.** The REPL has a two-argument
  `scalar(table, column)` value extractor. The subquery is the *one*-argument
  form; arity disambiguates, and no lexer change was needed. `outer` needed one
  — it is already a hard keyword (`outer join`), so it never reaches the
  identifier path and a call named `outer` is unforgeable by user code.

Landed: SPEC 5.7 + grammar, `docs/reference.html`,
`examples/correlated_subquery.ibex`, `benchmarking/tpch/queries/q02.ibex`,
13 tests (parser/lower/interpreter/REPL) + a parity case. Q2 at SF-1: 136ms vs
polars-st 54ms / polars-mt 77ms.

---

## Original plan (as written)

## Goal and boundary

Implement the smallest correlated-subquery feature needed to promote TPC-H
Q2 from `benchmarking/tpch/queries/proposed/q02.ibex` into the executable
benchmark corpus.

The target is a one-level equality-correlated scalar aggregate. It is not a
general row-by-row Apply operator, and it does not initially include `exists`,
`in`, arbitrary scalar subqueries, or nested correlations.

The representative Q2 shape is:

```ibex
ps_supplycost == scalar(
    inner_plan[
        filter ps_partkey == outer(p_partkey) && r_name == "EUROPE",
        select { minimum_supplycost = min(ps_supplycost) }
    ]
)
```

It means: retain an outer candidate row only when its `ps_supplycost` equals
the minimum European supply cost for that outer row's `p_partkey`.

## V1 language semantics

- `outer(column)` captures a column from the immediately enclosing relational
  query scope. It is a correlation boundary, not a table alias or a re-read.
- Bare columns in a subquery resolve only in that subquery's local scope; they
  never implicitly fall through to the outer scope.
- `scalar(table_expr)` accepts a table-producing expression that has exactly
  one output column. In this first implementation it must be an aggregate
  shape that produces at most one result per captured key.
- Correlation predicates must be equality comparisons between a local inner
  column and `outer(outer_column)`.
- V1 only permits the scalar result in a filter comparison. This covers Q2 and
  makes type, cardinality, and null behavior explicit before allowing general
  scalar expressions.
- When no inner rows match a captured key, the scalar value is null. A
  comparison with it cannot pass a filter.
- V1 supports only one enclosing relational scope. Reject deeper captures with
  a diagnostic. If required later, add explicit depth syntax such as
  `outer(2, column)` rather than nesting `outer(...)` calls.

Examples that must be rejected initially:

```ibex
outer(p_partkey);                  // no enclosing subquery scope
scalar(part[select { a, b }]);     // multiple scalar-result columns
scalar(lineitem[filter l_x > outer(p_x)]);  // non-equality capture
update { x = scalar(...) };        // scalar placement not yet supported
```

## Why decorrelation, not nested loops

Q2 must compile to existing set-at-a-time operations:

```text
inner partsupp/supplier/nation/region
  filter r_name == "EUROPE"
  aggregate by ps_partkey: min(ps_supplycost) as __ibex_scalar_0
  rename ps_partkey -> p_partkey
  left join outer candidate rows on p_partkey
  filter ps_supplycost == __ibex_scalar_0
```

The left join preserves SQL scalar-subquery behavior for outer keys with no
matching inner group. The following filter then drops the null comparison.

The implementation must never execute the inner plan once per outer row.
Likewise, using a `let`-bound source twice denotes source reuse: Q17's
`lineitem` binding must not become a second `read_parquet` operation after
decorrelation.

## Implementation phases

### 1. Specify and document the feature

Write the grammar, scope, cardinality, and null rules above into both
`SPEC.md` and `docs/index.html`. Add the supported Q2 syntax to an `.ibex`
example. The implementation and public language docs must land together.

### 2. Parse and validate outer captures

Primary locations:

- `include/ibex/parser/ast.hpp`
- `include/ibex/parser/lexer.hpp`
- `src/parser/lexer.cpp`
- `src/parser/parser.cpp`
- `src/parser/lower.cpp`

Add an AST form for `outer(column)`. `scalar(...)` may remain call-shaped in
the parser, but its lowering must recognize a table-producing argument rather
than trying to lower it as an ordinary scalar expression.

Maintain a stack of relational column scopes while lowering a scalar
subquery. Resolve bare identifiers against the current scope and resolve the
`outer` argument against the immediately enclosing scope. Validate the scalar
subquery's one-column output and report errors with source-level wording.

### 3. Add temporary correlation IR

Introduce an internal `CorrelatedApplyNode` (or equivalent) that owns:

- the outer input;
- the inner plan;
- the generated scalar result column name;
- captured inner/outer key pairs;
- scalar aggregate metadata.

This node is a lowering artifact. It must be eliminated before ordinary
schema validation, join pushdown, canonicalization, interpreter execution,
chunked execution, or C++ code generation. Add defensive errors in those
consumers in case one leaks through.

### 4. Decorrelate the Q2 shape

Add a dedicated pass after lowering and before column-reference checking and
`push_filters_into_joins` in `src/parser/lower.cpp`'s pipeline.

The pass should:

1. split the inner filter into supported capture equalities and local terms;
2. retain only the local terms in the inner input;
3. aggregate the inner input by the captured inner key;
4. rename/project that key to the outer key name;
5. left-join it to the outer input;
6. expose a collision-proof generated scalar column, e.g.
   `__ibex_scalar_0`;
7. replace the original scalar comparison with a normal filter expression.

Do not quietly produce a theta join or a nested-loop fallback for unsupported
correlation predicates. Return an error explaining the currently supported
Q2 shape instead.

### 5. Integrate with existing planning and execution

The rewritten plan should contain only existing `Join`, `Filter`, `Project` /
`Rename`, and `Aggregate` nodes. This deliberately reuses existing runtime and
codegen paths:

- `src/ir/schema.cpp` and column-reference validation;
- `src/ir/join_pushdown.cpp`;
- canonicalization/optimization;
- `src/runtime/interpreter.cpp` and chunked operators;
- `src/codegen/emitter.cpp`.

Verify generated plans preserve source sharing. If the current binding model
clones scans rather than materializing/reusing them, solve that explicitly
before claiming Q17-style source reuse.

### 6. Test and promote Q2

Add tests for:

- parser acceptance and diagnostics for `outer` / `scalar`;
- lexical scope resolution and unknown outer columns;
- scalar one-column validation;
- Q2 decorrelation shape, including no remaining `CorrelatedApplyNode`;
- no-match/null behavior, duplicate inner rows, and generated-name
  collisions;
- interpreter and C++ codegen parity for the rewritten plan;
- no duplicated external source read for a shared binding.

Then add executable `benchmarking/tpch/queries/q02.ibex`, add Q2 to:

- `benchmarking/tpch/bench_ibex.py`;
- `benchmarking/tpch/bench_polars.py`;
- `benchmarking/tpch/check_answers.py`;
- `benchmarking/tpch/print_table.py`.

Run the official SF-1 answer check and benchmark only against `build-release/`.
Report both default Polars and `POLARS_MAX_THREADS=1` results.

## Follow-on increments

After Q2 is correct and fast:

1. `exists` / `not exists` lowered to semi/anti joins (Q4, Q22);
2. uncorrelated `scalar(...)` values (Q11, Q22);
3. uncorrelated `in` / `not in`, with SQL null semantics (Q16, Q18);
4. correlated scalar aggregates with composite capture keys (Q20);
5. only then consider deeper correlation or broader scalar-expression
   placement.

