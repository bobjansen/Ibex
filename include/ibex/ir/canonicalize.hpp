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
