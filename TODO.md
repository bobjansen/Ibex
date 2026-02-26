# Todo list

## NULL semantics (proposal):
1. Type system and syntax:
    - Add nullable scalar syntax `T?` (e.g. `Int?`, `Timestamp?`).
    - Allow nullable columns/tables via schema fields and type inference.
    - Add `null` literal (typed by context; explicit casts if ambiguous).

2. Runtime representation:
    - Store nullability as validity bitmap per column (Arrow-style), not sentinel values.
    - Keep current column buffers for values; nullness is tracked separately.
    - Preserve bitmap through copy/project/filter/join/update operations.

3. Expression semantics:
    - Arithmetic with null yields null.
    - Comparisons with null yield null (not false).
    - Boolean logic uses three-valued logic (true/false/null).
    - In `filter`, only rows evaluating to true are kept; false and null are dropped.

4. Aggregation semantics:
    - `sum/mean/min/max` ignore null inputs; result is null if all inputs are null.
    - `count()` keeps current row-count behavior.
    - Add `count(col)` (or `count_non_null(col)`) for non-null count explicitly.
    - Define `first/last` behavior with nulls and add non-null variants if needed.

5. Join semantics:
    - For unmatched rows in left/asof joins, populate right-side columns with null (not type defaults like 0/"").
    - Equality keys with null should not match by default (SQL-style).
    - Document any null-safe equality operator separately if added later.

6. Ordering and window semantics:
    - Define default null sort order (`nulls last` for ascending).
    - Add explicit `nulls first/nulls last` syntax later if needed.
    - Rolling/window functions should ignore null values unless function requires otherwise.

7. I/O and REPL:
    - CSV/parquet loaders should preserve source nulls (empty field/NA handling policy must be explicit).
    - REPL should print null values as `null` consistently.
    - `:schema` should show nullability in types (e.g. `price: Float64?`).

8. Rollout plan:
    - Phase 1: validity bitmap + left join/asof unmatched rows become null + REPL display.
    - Phase 2: nullable expressions + 3VL in filter.
    - Phase 3: aggregate semantics + I/O null parsing controls.
