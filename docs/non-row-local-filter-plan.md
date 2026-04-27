# Non-Row-Local Filter Plan

Context:
- `rank(...)` now exists in `update`, including grouped `by` support and the
  explicit `rank(order { ... })` form.
- A remaining ergonomics gap is that `filter` still effectively prefers
  row-local expressions, so patterns like `num == lag(num, 1)` require a
  preceding `update`.

Target language rule:
- Any pure expression should be allowed in `filter`.
- The final `filter` expression must evaluate to a boolean-shaped result.
- Block context such as `by`, `order`, and later `window` should be visible to
  those expressions.
- Aggregates that change cardinality should still stay out of plain `filter`.

Recommended implementation order:

1. `filter` with table-aware value expressions
- Detect supported non-row-local pure subexpressions in `filter`.
- Lower them into `FilterCall` IR nodes.
- Evaluate those calls once as vector-valued filter operands.
- Keep result schemas unchanged; no temporary columns are exposed.

Implemented first slice:

```ibex
logs[filter num == lag(num, 1) && num == lag(num, 2)]
```

works directly, with `lag(...)` / `lead(...)` evaluated over current row order.

2. Then generalize the same mechanism to `select`
- Allow non-row-local pure expressions directly in computed `select` fields.

3. Then extend context-sensitive support
- `rank(...)` in `filter` / `select` with surrounding `by`
- explicit `order { ... }` context
- later `window` / rolling functions

Design stance:
- The direct `FilterCall` approach avoids exposing hidden temp columns.
- It gives high language leverage for relatively little code while preserving
  the existing vectorized filter evaluator shape.
- Later optimization can fuse common cases once the semantics are in place.

Good motivating examples:

```ibex
logs[filter num == lag(num, 1) && num == lag(num, 2)]
```

```ibex
employee[
    filter rank(salary, method = dense, ascending = false) <= 3,
    by departmentId
]
```

```ibex
tf[
    window 5m,
    filter price > rolling_mean(price)
]
```
