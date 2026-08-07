#include <ibex/ir/node.hpp>
#include <ibex/ir/pending_order.hpp>

namespace ibex::ir {

namespace {

/// The join an `order`'s keys ultimately describe, looking through operators
/// that neither move a row relative to its neighbours nor change a column's
/// name. Both halves matter: the row-order half is why the join's claim still
/// holds up here, and the naming half is why the `order`'s key names are still
/// the join's key names when they arrive.
///
/// Deliberately absent:
///   - `Rename` changes a name, so the keys would have to be remapped on the
///     way down. Sound to add, just not free.
///   - `Update` can overwrite a key's values. The rows have not moved, but a
///     claim about the old values says nothing about the new ones.
///   - `Aggregate`, `Melt`, `Dcast`, `Window`, `Resample` rebuild rows.
///   - `Order` itself: the nearer one is the one that decides, and it will have
///     been annotated on its own.
///
/// `Project` keeps its cut, `Filter`/`Distinct`/`Head`/`Tail` only remove rows,
/// and `Ascribe` asserts. The fused kinds are included exactly when every
/// operator they fuse is.
auto join_below(Node& node) -> JoinNode* {
    switch (node.kind()) {
        case NodeKind::Join:
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            return static_cast<JoinNode*>(&node);
        case NodeKind::Filter:
        case NodeKind::Project:
        case NodeKind::Distinct:
        case NodeKind::Head:
        case NodeKind::Tail:
        case NodeKind::Ascribe:
        case NodeKind::FilterProject:
        case NodeKind::FilterHead:
        case NodeKind::FilterTail:
            break;
        default:
            return nullptr;
    }
    if (node.children().empty() || node.children().front() == nullptr) {
        return nullptr;
    }
    return join_below(*node.mutable_children().front());
}

}  // namespace

void annotate_pending_orders(Node& root) {
    if (root.kind() == NodeKind::Program) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        auto& program = static_cast<ProgramNode&>(root);
        for (const auto& pre : program.mutable_preamble()) {
            if (pre != nullptr) {
                annotate_pending_orders(*pre);
            }
        }
        if (program.mutable_main_node() != nullptr) {
            annotate_pending_orders(*program.mutable_main_node());
        }
        return;
    }

    for (const auto& child : root.mutable_children()) {
        if (child != nullptr) {
            annotate_pending_orders(*child);
        }
    }

    if (root.kind() != NodeKind::Order || root.children().empty() ||
        root.children().front() == nullptr) {
        return;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& order = static_cast<const OrderNode&>(root);
    if (order.keys().empty()) {
        // A bare `order` sorts by the whole schema, resolved against the input
        // at execution. There is nothing to hand the join that it could compare
        // against an ordering claim.
        return;
    }
    if (auto* join = join_below(*root.mutable_children().front())) {
        join->set_pending_order(order.keys());
    }
}

}  // namespace ibex::ir
