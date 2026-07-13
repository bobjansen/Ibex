#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/join_pushdown.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <robin_hood.h>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {

namespace {

// Fresh NodeIds for the Filter nodes this pass synthesizes. Seeded at
// push_filters_into_joins() entry to (max id in tree + 1) so new ids never
// collide with existing ones; same convention as canonicalize.cpp.
auto next_id_counter() -> std::uint64_t& {
    thread_local std::uint64_t next_id = 0;
    return next_id;
}

auto fresh_id() -> NodeId {
    return NodeId{next_id_counter()++};
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)
// Node kind is checked immediately before every downcast below.

void collect_max_id(Node& n, std::uint64_t& m) {
    m = std::max(m, n.id().value);
    for (const auto& c : n.mutable_children()) {
        if (c) {
            collect_max_id(*c, m);
        }
    }
    if (n.kind() == NodeKind::Program) {
        auto& prog = static_cast<ProgramNode&>(n);
        for (const auto& p : prog.mutable_preamble()) {
            if (p) {
                collect_max_id(*p, m);
            }
        }
        if (prog.mutable_main_node()) {
            collect_max_id(*prog.mutable_main_node(), m);
        }
    }
}

/// Flatten the top-level AND spine of `expr` into individual conjuncts.
void collect_conjuncts(const Expr& expr, std::vector<const Expr*>& out) {
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node);
        logical != nullptr && logical->op == LogicalOp::And && logical->left != nullptr &&
        logical->right != nullptr) {
        collect_conjuncts(*logical->left, out);
        collect_conjuncts(*logical->right, out);
        return;
    }
    out.push_back(&expr);
}

/// Fold conjuncts back into one left-associated AND chain.
auto and_combine(std::vector<Expr> parts) -> Expr {
    Expr acc = std::move(parts.front());
    for (std::size_t i = 1; i < parts.size(); ++i) {
        acc = Expr{.node = LogicalExpr{.op = LogicalOp::And,
                                       .left = make_expr_ptr(std::move(acc)),
                                       .right = make_expr_ptr(std::move(parts[i]))}};
    }
    return acc;
}

/// Where one conjunct may be evaluated. `Above` is always sound.
enum class Destination : std::uint8_t { Above, Left, Right, BothSides };

auto classify(const Expr& conjunct, const SchemaInfo& left, const SchemaInfo& right,
              const std::vector<std::string>& keys) -> Destination {
    // A conjunct whose value depends on rows other than its own (rolling, lag,
    // rank, aggregates, ...) means something different below the join.
    if (!is_subset_evaluable_expr(conjunct)) {
        return Destination::Above;
    }
    robin_hood::unordered_set<std::string> refs;
    collect_expr_column_refs(conjunct, refs);
    // No refs: literal-only, or one this pass gains nothing by moving.
    if (refs.empty()) {
        return Destination::Above;
    }

    auto is_key = [&](const std::string& name) {
        return std::ranges::find(keys, name) != keys.end();
    };

    // Above the join a name shared by both sides resolves to the LEFT column,
    // so refs found in the left schema always license a left push.
    if (left.is_known() && std::ranges::all_of(refs, [&](const std::string& name) {
            return left.find(name) != nullptr;
        })) {
        // All refs are join keys present on both sides: keys are equal on
        // every surviving row, so the conjunct may pre-filter BOTH inputs.
        if (right.is_known() && std::ranges::all_of(refs, [&](const std::string& name) {
                return is_key(name) && right.find(name) != nullptr;
            })) {
            return Destination::BothSides;
        }
        return Destination::Left;
    }

    // A right push must prove no ref silently reads a left column instead:
    // every ref the left side also produces must be a join key (equal on both
    // sides), and "left does not produce it" is only provable on a closed
    // Known left schema.
    if (right.is_known() && left.is_known() && !left.is_open() &&
        std::ranges::all_of(refs, [&](const std::string& name) {
            return right.find(name) != nullptr && (left.find(name) == nullptr || is_key(name));
        })) {
        return Destination::Right;
    }

    return Destination::Above;
}

/// Rewrite one `Filter(pred, Join(a, b))` root. Children are assumed already
/// processed. Returns the (possibly unchanged) root.
auto rewrite_filter_over_join(NodePtr node, const SourceSchemas& sources) -> NodePtr {
    auto& filter = static_cast<FilterNode&>(*node);
    auto& join = static_cast<JoinNode&>(*node->mutable_children().front());
    const JoinKind kind = join.kind();
    if (kind != JoinKind::Inner && kind != JoinKind::Left && kind != JoinKind::Right) {
        return node;  // Outer/Semi/Anti/Cross/Asof: see the header's safety table.
    }
    if (join.children().size() != 2 || join.children()[0] == nullptr ||
        join.children()[1] == nullptr) {
        return node;
    }

    const SchemaInfo left_schema = infer_schema(*join.children()[0], sources);
    const SchemaInfo right_schema = infer_schema(*join.children()[1], sources);
    if (!left_schema.is_known() && !right_schema.is_known()) {
        return node;  // nothing is provable about either side
    }

    std::vector<const Expr*> conjuncts;
    collect_conjuncts(filter.predicate(), conjuncts);

    std::vector<Expr> left_parts;
    std::vector<Expr> right_parts;
    std::vector<Expr> kept_parts;
    for (const Expr* conjunct : conjuncts) {
        Destination dest = classify(*conjunct, left_schema, right_schema, join.keys());
        // Join-kind gating: only the non-null-supplying side may pre-filter.
        if (kind == JoinKind::Left && dest != Destination::Left) {
            dest = dest == Destination::BothSides ? Destination::Left : Destination::Above;
        } else if (kind == JoinKind::Right && dest != Destination::Right) {
            // A key conjunct must stay above too: an unmatched right row's
            // output key column is left-owned and null-extended.
            dest = Destination::Above;
        }
        switch (dest) {
            case Destination::Left:
                left_parts.push_back(*conjunct);
                break;
            case Destination::Right:
                right_parts.push_back(*conjunct);
                break;
            case Destination::BothSides:
                left_parts.push_back(*conjunct);
                right_parts.push_back(*conjunct);
                break;
            case Destination::Above:
                kept_parts.push_back(*conjunct);
                break;
        }
    }
    if (left_parts.empty() && right_parts.empty()) {
        return node;
    }

    NodePtr join_owned = std::move(node->mutable_children().front());
    node->mutable_children().clear();
    NodePtr left_child = std::move(join_owned->mutable_children()[0]);
    NodePtr right_child = std::move(join_owned->mutable_children()[1]);
    join_owned->mutable_children().clear();

    auto wrap = [&sources](NodePtr child, std::vector<Expr> parts) -> NodePtr {
        if (parts.empty()) {
            return child;
        }
        const bool child_is_join = child->kind() == NodeKind::Join;
        auto pushed = std::make_unique<FilterNode>(fresh_id(), and_combine(std::move(parts)));
        pushed->add_child(std::move(child));
        if (child_is_join) {
            // The receiving side is itself a join: push through it too. Each
            // conjunct only ever moves downward, so this terminates.
            return rewrite_filter_over_join(std::move(pushed), sources);
        }
        return pushed;
    };
    join_owned->add_child(wrap(std::move(left_child), std::move(left_parts)));
    join_owned->add_child(wrap(std::move(right_child), std::move(right_parts)));

    if (kept_parts.empty()) {
        return join_owned;
    }
    auto kept = std::make_unique<FilterNode>(fresh_id(), and_combine(std::move(kept_parts)));
    kept->add_child(std::move(join_owned));
    return kept;
}

auto walk(NodePtr node, const SourceSchemas& sources) -> NodePtr {
    if (!node) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), sources);
    }
    if (node->kind() == NodeKind::Program) {
        auto& prog = static_cast<ProgramNode&>(*node);
        if (prog.mutable_main_node()) {
            prog.mutable_main_node() = walk(std::move(prog.mutable_main_node()), sources);
        }
        for (auto& pre : prog.mutable_preamble()) {
            pre = walk(std::move(pre), sources);
        }
    }
    if (node->kind() == NodeKind::Filter && node->children().size() == 1 &&
        node->children().front() != nullptr && node->children().front()->kind() == NodeKind::Join) {
        return rewrite_filter_over_join(std::move(node), sources);
    }
    return node;
}
// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace

auto push_filters_into_joins(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    if (root) {
        std::uint64_t max_id = 0;
        collect_max_id(*root, max_id);
        next_id_counter() = max_id + 1;
    }
    return walk(std::move(root), sources);
}

}  // namespace ibex::ir
