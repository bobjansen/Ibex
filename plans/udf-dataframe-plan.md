# User Functions and DataFrame-Typed Arguments

## Goal

Make user-defined functions the normal way to package reusable Ibex query
patterns such as:

- `second_highest_salary(employee)`
- `nth_highest_salary(employee, n)`
- `department_top_three_salaries(employee, department)`
- small table-to-table cleanup and enrichment helpers

The near-term target is not a full higher-order function system. The target is
practical, explicit, reusable query helpers that work well with `DataFrame`
inputs and outputs.

## Current State

Ibex already has the surface syntax for top-level functions:

```ibex
fn build_features(df: DataFrame) -> DataFrame effects {} {
    df[update { notional = price * size }];
}
```

What exists today:

- top-level `fn ... -> ... { ... }` syntax
- explicit parameter and return types
- optional effect annotations
- REPL/runtime evaluation of user-defined functions
- `DataFrame<Schema>` syntax in types

Useful implementation detail already present:

- bare `DataFrame` means no column requirements
- `DataFrame<Schema>` validation already behaves like "these columns are
  required; extra columns are allowed"

That existing behavior is the right foundation for the longer-term
"optionally specify required columns" goal.

## Desired Shape

### 1. Practical reusable table helpers

The primary target shape is:

```ibex
fn nth_highest_salary(employee: DataFrame, n: Int) -> DataFrame {
    ...
}
```

and later:

```ibex
fn nth_highest_salary(employee: DataFrame<{ salary: Int }>, n: Int) -> DataFrame {
    ...
}
```

The first form is the minimal ergonomic win.
The second form is the future typed-contract win.

### 2. Typed DataFrame arguments with optional required columns

The intended model should be:

- `DataFrame`
  Means any table.
- `DataFrame<{}>`
  Equivalent to unconstrained table for now.
- `DataFrame<{ salary: Int }>`
  Means the function requires at least column `salary: Int`.
- `DataFrame<{ departmentId: Int, salary: Int }>`
  Means both columns must exist with those types.

Extra columns should remain allowed.

This is important because many reusable query helpers only need a subset of a
table schema, and forcing exact-schema matches would make them much less useful.

## Non-Goals for V1

Do not solve these in the first implementation wave:

- nested function declarations
- closures / outer-scope capture
- recursion or mutual recursion
- higher-order functions
- exact-schema matching for table arguments
- dynamic return-schema inference in function signatures
- dynamic output column names in function types

## Recommended Implementation Phases

## Phase 1: Make user functions a first-class everyday feature

Target:

- top-level pure and effect-annotated functions are stable and documented
- scalar and `DataFrame` parameters work reliably
- scalar and `DataFrame` returns work reliably

Requirements:

- ~~collect function signatures before evaluating bodies~~ — done; `execute_statements`
  pre-passes top-level `fn` declarations into the registry before walking the batch
- ~~support forward references between top-level function definitions~~ — done;
  covered by forward-reference tests in `tests/test_repl.cpp`
- keep eager argument evaluation
- preserve the current "last expression is the return value" rule
- improve diagnostics for arity/type mismatches

Success criteria:

- LeetCode-style examples can be written as helpers instead of one-off scripts
- functions can call other functions
- table-to-table helper functions are routine, not special

## Phase 2: Clarify evaluation model and optimizer boundary

Decision:

- keep the current runtime/REPL evaluation path as the baseline
- avoid introducing a new complex runtime call mechanism if simple lowering or
  statement-level evaluation is sufficient

Questions to settle:

- when can function bodies be inlined safely?
- when should effect annotations block inlining/reordering?
- how should errors report function call stacks?

Recommendation:

- treat function calls as a semantic boundary first
- optimize/integrate later once correctness and ergonomics are solid

## Phase 3: DataFrame contract types

This is the feature that unlocks "I want to optionally specify the columns that
are required for DataFrame typed arguments."

Target syntax:

```ibex
fn second_highest_salary(employee: DataFrame<{ salary: Int }>) -> DataFrame {
    ...
}
```

Semantics:

- every declared field is required
- field types must match
- extra columns are ignored and preserved unless the function body drops them
- validation happens at call time and should produce a clear missing/wrong-type
  message

This matches the validation model already implemented for `DataFrame<Schema>`
arguments in the REPL/runtime.

Design choice:

- treat the schema annotation as a minimum required schema, not an exact schema

Why:

- reusable helpers should compose across wider tables
- this matches common dataframe practice better than exact matching
- it aligns with the existing validation helper

## Phase 4: Return-side table typing

Once argument-side table contracts are in place, decide how much to require for
return types.

Options:

1. Keep `-> DataFrame` as the common case.
2. Allow `-> DataFrame<{ nth_highest_salary: Int }>` when the return schema is
   stable and useful to document.

Recommendation:

- support bare `-> DataFrame` first
- add optional return-schema annotations later
- do not block useful functions on precise return typing

This matters because many helpers reshape data significantly, and forcing exact
return-schema declarations too early would make iteration slower.

## Phase 5: Clause-expression integration

Current spec text says user-defined functions are evaluated at the statement
level in the REPL/runtime, while function calls inside DataFrame clauses are
resolved against built-ins or externs.

Longer term, we should decide whether user-defined functions can also appear
inside clause expressions, for example:

```ibex
trades[update { score = clip(zscore(ret), -3.0, 3.0) }]
```

Possible staging:

- allow scalar-returning user functions in scalar expressions first
- table-returning functions remain statement-level first
- only later consider full expression-level integration

This is valuable, but it is not required to unlock the initial corpus of query
helpers.

## Recommended Priorities

1. Stabilize user-defined function calls for table-shaped helpers.
2. Document and rely on bare `DataFrame` arguments first.
3. Add required-column contracts via `DataFrame<Schema>` argument annotations.
4. Add optional return-side table schema annotations.
5. Revisit scalar-expression call integration after the above is solid.

## Motivating Examples

These are good litmus tests for the feature set:

### Example A: table in, table out

```ibex
fn top_three_salaries(employee: DataFrame) -> DataFrame {
    let distinct_salaries = employee[distinct { departmentId, salary }];
    distinct_salaries[order { salary desc }, head 3, by departmentId];
}
```

### Example B: table plus scalar

```ibex
fn nth_highest_salary(employee: DataFrame, n: Int) -> DataFrame {
    let top_n = employee[distinct { salary }, order { salary desc }, head n];
    top_n;
}
```

### Example C: required input columns

```ibex
fn nth_highest_salary(employee: DataFrame<{ salary: Int }>, n: Int) -> DataFrame {
    let top_n = employee[distinct { salary }, order { salary desc }, head n];
    top_n;
}
```

The third form is the end state to optimize for.

## Open Questions

- Should `DataFrame<{ salary: Int }>` be the exact user-facing syntax, or
  should function signatures prefer a named schema alias later?
- Should wrong-type table arguments fail at parse/lower time when statically
  known, or always at runtime call validation?
- Should function overloading by `DataFrame` schema ever be allowed?
  Recommendation: no, not in the first several iterations.
- Should return-schema annotations participate in type inference, or remain
  documentation-plus-validation only?

## Recommendation

Build this feature around one simple principle:

`DataFrame` arguments should be reusable by default, and schema annotations
should mean "requires these columns", not "must match exactly".`

That gives the right ergonomics for query helpers while still allowing stronger
contracts where they are useful.
