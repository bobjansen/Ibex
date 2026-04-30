# Canonicalize follow-ups

Ideas for further leverage on the IR rewrite machinery introduced with
R5/R6 (`FilterProjectNode`, `FilterUpdateProjectNode`). Ordered by
payoff vs. risk.

## 1. Fuse `Rename` into neighbours

`Rename` is pure metadata. Rules:

- `Project(Rename(x))` → `Project(x)` with names rewritten.
- `FilterProject(Rename(x))` → `FilterProject(x)` with predicate and
  columns remapped.

Drops a whole operator from many pipelines and removes the Rename
branch in `build_operator`.

## 2. Push `Filter` under `Update` when safe

`Filter(Update(x))` → `Update(Filter(x))` when the predicate only
references columns not redefined by the update. Lets R6 fire on more
sources — today R6 only triggers when the input was already written
as `Project(Update(Filter))`.

## 3. Constant-fold / dead-key elimination

- Drop `Order` keys proven constant by a preceding equality `Filter`.
- Collapse `Head(Head(x))` / `Tail(Tail(x))` to the tighter bound.

## 4. Consolidate the rewrite driver

`rewrite_root` is a hand-rolled while-changed loop with per-rule
`continue`. Extract each rule into an
`std::optional<NodePtr> try_rule(NodePtr)` and iterate a fixed array
of them. Makes rule additions one-liners and gives a clean hook for
per-rule counts in `OptimizationStats`.

## 5. Test coverage for rule composition

Only R3∘R1 is covered today. Add tests for R1∘R5, R2∘R5, R4∘R5 as
cheap insurance against future rules breaking fixpoint convergence.
