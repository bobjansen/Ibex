#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/optimizer.hpp>

namespace ibex::ir {

/// Canonicalize a query IR by applying algebraic identities until fixpoint.
///
/// Rewrites applied (bottom-up, to fixpoint at each node):
///   R1. `Filter(Order(x))`  → `Order(Filter(x))`   — Filter preserves order (SPEC §9),
///                                                    so sorting a filtered set is
///                                                    observably identical and smaller.
///   R2. `Project(Order(x))` → `Order(Project(x))`  — when all Order keys are preserved
///                                                    by the projection.
///   R3. `Order(Rename(x))`  → `Rename(Order(x))`   — Rename is a metadata bijection;
///                                                    Order keys are remapped new→old.
///   R4. `Head(Project(x))`  → `Project(Head(x))`   — Project/Rename preserve row count
///        `Head(Rename(x))`  → `Rename(Head(x))`      and order; same for Tail.
///        `Tail(Project(x))` → `Project(Tail(x))`     Only applied when group_by is
///        `Tail(Rename(x))`  → `Rename(Tail(x))`      empty (matches current Filter-head
///                                                    fusion precondition).
///   R5. `Project(Filter(x))`         → `FilterProject(x)`        — fused node so
///                                                                   build_operator dispatches
///                                                                   on NodeKind rather than
///                                                                   pattern-matching the shape.
///   R6. `Project(Update(Filter(x)))` → `FilterUpdateProject(x)`  — fired only when the
///                                                                   Update is row-local
///                                                                   (no tuple_fields, no
///                                                                   group_by, no cross-row
///                                                                   callees).
///   R7. `Head(Filter(x))`            → `FilterHead(x)`            — only when the Head has
///                                                                   no group_by.
///   R8. `Tail(Filter(x))`            → `FilterTail(x)`            — only when the Tail has
///                                                                   no group_by.
///   R9. `Rename(Rename(x))`          → single `Rename(x)` composed; identity pairs dropped.
///  R10. Rename with no entries (or all identity) is dropped.
///  R11. `Filter(Rename(x))`          → `Rename(Filter'(x))`       — predicate column refs
///                                                                   remapped new→old; bubbles
///                                                                   Rename toward the root so
///                                                                   it composes via R9.
///  R12. `Filter(Update(x))`          → `Update(Filter(x))`        — only when the Update is
///                                                                   row-local AND the predicate
///                                                                   references no column the
///                                                                   Update produces. Feeds R6.
///  R13. `Head(m, Head(n, x))`        → `Head(min(m,n), x)`         — both group_by empty.
///  R14. `Tail(m, Tail(n, x))`        → `Tail(min(m,n), x)`         — both group_by empty.
///  R15. `Order(Filter(... AND col==K AND ..., x))` drops `col` from Order keys (pinned by
///       equality). If all keys drop, the Order is removed entirely.
///  R16. `Head(Order(x))` → `TopK(keys, n, First, x)` and `Tail(Order(x))` → `TopK(..., Last, x)`
///       — fused node so runtime + codegen both use partial heap-select (O(n log k)) instead
///       of full sort + truncate. Preserves any group_by on the limit.
///  R18. `Filter(pred, Aggregate(group_by, aggs, x))` → `Aggregate(group_by, aggs, Filter(pred,
///  x))`
///       when `pred` only references columns in `group_by` (not aggregation aliases). Group_by
///       columns survive the aggregate unchanged, so the filter is equivalent above or below;
///       below it usually eliminates many more rows. HAVING-style predicates over agg aliases
///       are left in place.
///  R17. Predicate simplification on `Filter(pred, x)`: boolean identity/absorption
///       (`x AND true → x`, `x OR false → x`, etc.), double-negation (`NOT NOT x → x`),
///       literal-only comparison and arithmetic folding (`5 == 5 → true`, `2 + 3 → 5`),
///       and `IsNull/IsNotNull` over a literal. If `pred` reduces to `true`, the Filter
///       is dropped; if to `false`, it becomes `Head(0, x)`.
///  R19. `Filter(p1, Filter(p2, x))` → `Filter(p1 AND p2, x)` — merges adjacent
///       filters so downstream rules see one combined predicate (richer column
///       refs, more fusion/push opportunities).
///  R20. `Aggregate(gb, aggs, x)` → `Aggregate(gb, aggs, Project(needed, x))` —
///       inserts a column-pruning Project below the aggregate, where `needed`
///       is the union of `gb` and each agg's input column. Skipped when `x` is
///       already Project / FilterProject / FilterUpdateProject (avoids redundant
///       projection and rule-driver loops). The Project then composes with R5
///       (FilterProject fusion) when `x` is a Filter.
///  R21. `Project(c, Project(_, x))` → `Project(c, x)` (and same for
///       `FilterProject(c, p, Project(_, x))`). The outer node's columns are a
///       subset of the inner Project's, so dropping the inner Project is sound
///       and removes a redundant schema pass — particularly useful after R20
///       inserts a pruning Project that R5 fuses with into a FilterProject.
///
/// Pure on IR: takes ownership and returns the rewritten tree. The emitted
/// operator tree is identical to what `build_operator` would produce via its
/// existing pattern-matching branches.
[[nodiscard]] auto canonicalize(NodePtr root) -> NodePtr;

/// PassManager entry-point.
class CanonicalizePass final : public OptimizationPass {
   public:
    auto run(NodePtr root, const OptimizationContext& context, OptimizationStats& stats) const
        -> NodePtr override;
};

}  // namespace ibex::ir
