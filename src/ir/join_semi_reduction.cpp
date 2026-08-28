// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/distinct_key_reduction.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/join_output.hpp>
#include <ibex/ir/join_semi_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/required_columns.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

auto fresh_id(std::uint64_t& next) -> NodeId {
    return NodeId{next++};
}

void collect_max_id(Node& n, std::uint64_t& m) {
    m = std::max(m, n.id().value);
    for (const auto& child : n.mutable_children()) {
        if (child) {
            collect_max_id(*child, m);
        }
    }
    if (n.kind() == NodeKind::Program) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        auto& prog = static_cast<ProgramNode&>(n);
        for (const auto& pre : prog.mutable_preamble()) {
            if (pre) {
                collect_max_id(*pre, m);
            }
        }
        if (prog.mutable_main_node()) {
            collect_max_id(*prog.mutable_main_node(), m);
        }
    }
}

/// Names a schema lists. Empty when the schema cannot be enumerated, which
/// every caller here treats as "prove nothing".
auto field_names(const SchemaInfo& schema) -> std::vector<std::string> {
    std::vector<std::string> names;
    if (!schema.is_known()) {
        return names;
    }
    names.reserve(schema.fields().size());
    for (const auto& field : schema.fields()) {
        names.push_back(field.name);
    }
    return names;
}

auto contains(const std::vector<std::string>& names, const std::string& name) -> bool {
    return std::find(names.begin(), names.end(), name) != names.end();
}

auto field_views(const SchemaInfo& schema) -> std::vector<std::string_view> {
    std::vector<std::string_view> names;
    names.reserve(schema.fields().size());
    for (const auto& field : schema.fields()) {
        names.push_back(field.name);
    }
    return names;
}

/// Flatten/fold the top-level AND spine without changing evaluation order.
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

auto and_combine(std::vector<Expr> parts) -> Expr {
    Expr result = std::move(parts.front());
    for (std::size_t i = 1; i < parts.size(); ++i) {
        result = Expr{.node = LogicalExpr{.op = LogicalOp::And,
                                          .left = make_expr_ptr(std::move(result)),
                                          .right = make_expr_ptr(std::move(parts[i]))}};
    }
    return result;
}

/// True when no column that exists on `dropped` but not on `kept` is read above
/// the join. A shared name resolves to the kept side and is equal on surviving
/// rows, so it survives the reduction untouched.
auto dropped_side_is_unread(const ColumnDemand& demand, const std::vector<std::string>& dropped,
                            const std::vector<std::string>& kept) -> bool {
    if (demand.all) {
        return false;
    }
    for (const auto& name : demand.names) {
        if (contains(dropped, name) && !contains(kept, name)) {
            return false;
        }
    }
    return true;
}

/// The join shapes whose pair count and column set this pass can reason about.
auto reducible_shape(const JoinNode& join) -> bool {
    return join.kind() == JoinKind::Inner && !join.predicate().has_value() &&
           !join.keys().empty() && join.take() == MatchSelection::All &&
           join.expect().left == JoinMultiplicity::Many &&
           join.expect().right == JoinMultiplicity::Many && join.null_match() == NullMatch::Never &&
           !join.suffix().present && join.children().size() == 2;
}

/// A semi/anti join asks only whether a right key exists. Removing a distinct
/// directly over that side cannot change the answer, regardless of which
/// non-key columns the distinct also compared.
void drop_redundant_right_distinct(JoinNode& join) {
    if ((join.kind() != JoinKind::Semi && join.kind() != JoinKind::Anti) ||
        join.mutable_children().size() != 2 || join.mutable_children()[1] == nullptr ||
        join.mutable_children()[1]->kind() != NodeKind::Distinct) {
        return;
    }
    NodePtr distinct = std::move(join.mutable_children()[1]);
    if (distinct->mutable_children().size() != 1 ||
        distinct->mutable_children().front() == nullptr) {
        join.mutable_children()[1] = std::move(distinct);
        return;
    }
    join.mutable_children()[1] = std::move(distinct->mutable_children().front());
}

auto default_existence_join(const JoinNode& join) -> bool {
    return join.kind() == JoinKind::Left && !join.predicate().has_value() && !join.keys().empty() &&
           join.take() == MatchSelection::All && join.expect().left == JoinMultiplicity::Many &&
           join.expect().right == JoinMultiplicity::Many && join.null_match() == NullMatch::Never &&
           !join.suffix().present && join.children().size() == 2 && join.children()[0] != nullptr &&
           join.children()[1] != nullptr;
}

auto marker_is_unmatched_right(const Expr& expr, const JoinNode& join, const SchemaInfo& right,
                               const Node& right_plan, const std::vector<JoinOutputColumn>& output)
    -> bool {
    const auto* is_null = std::get_if<IsNullExpr>(&expr.node);
    if (is_null == nullptr || is_null->negated || is_null->operand == nullptr) {
        return false;
    }
    const auto* column = std::get_if<ColumnRef>(&is_null->operand->node);
    if (column == nullptr || column->lexical) {
        return false;
    }
    const auto found = std::ranges::find_if(output, [&](const JoinOutputColumn& candidate) {
        return candidate.side == JoinOutputSide::Right && candidate.name == column->name;
    });
    if (found == output.end() || found->source_index >= right.fields().size()) {
        return false;
    }
    // A declared/derived non-null right value is null exactly on the padded
    // rows. A right key is also sufficient under `nulls never`: any MATCHED
    // right key was necessarily non-null even if its source column is nullable.
    if (right.fields()[found->source_index].non_null() || found->is_key) {
        return true;
    }
    const std::string& marker_name = right.fields()[found->source_index].name;
    return std::ranges::any_of(join.keys(), [&](const JoinKey& key) {
        return columns_have_same_value(right_plan, marker_name, key.right);
    });
}

auto demand_reads_right_output(const ColumnDemand& demand,
                               const std::vector<JoinOutputColumn>& output) -> bool {
    if (demand.all) {
        return true;
    }
    return std::ranges::any_of(output, [&](const JoinOutputColumn& column) {
        return column.side == JoinOutputSide::Right && demand.names.contains(column.name);
    });
}

auto expression_reads_right_output(const Expr& expr, const std::vector<JoinOutputColumn>& output)
    -> bool {
    robin_hood::unordered_set<std::string> refs;
    collect_expr_column_refs(expr, refs);
    return std::ranges::any_of(output, [&](const JoinOutputColumn& column) {
        return column.side == JoinOutputSide::Right && refs.contains(column.name);
    });
}

/// `filter is_null(r_nonnull)` over a left join is an anti join when the right
/// columns are not otherwise observed:
///
///     Filter(is_null(r), LeftJoin(L, R))  ->  AntiJoin(L, R)
///
/// The demand check is what makes the schema change safe. The filter itself is
/// allowed to read the marker; its PARENT must not read any right output.
auto rewrite_left_null_filter_to_anti(NodePtr node, const SourceSchemas& sources,
                                      const std::map<const Node*, ColumnDemand>& filter_demand)
    -> NodePtr {
    auto& filter = static_cast<FilterNode&>(*node);
    auto& join = static_cast<JoinNode&>(*node->mutable_children().front());
    if (!default_existence_join(join)) {
        return node;
    }
    const auto demand = filter_demand.find(node.get());
    if (demand == filter_demand.end()) {
        return node;
    }
    const SchemaInfo left = infer_schema(*join.children()[0], sources);
    const SchemaInfo right = infer_schema(*join.children()[1], sources);
    if (!left.is_known() || left.is_open() || !right.is_known() || right.is_open()) {
        return node;
    }
    const auto output = plan_join_output(join.kind(), join.keys(), field_views(left),
                                         field_views(right), join.suffix());
    if (!output.has_value() || demand_reads_right_output(demand->second, *output)) {
        return node;
    }

    std::vector<const Expr*> conjuncts;
    collect_conjuncts(filter.predicate(), conjuncts);
    std::vector<Expr> kept;
    bool found_marker = false;
    for (const Expr* conjunct : conjuncts) {
        if (marker_is_unmatched_right(*conjunct, join, right, *join.children()[1], *output)) {
            found_marker = true;
            continue;
        }
        // The anti join emits only left columns. A residual right predicate
        // would become ill-typed (and generally is not equivalent), so decline
        // the entire rewrite rather than moving it speculatively.
        if (expression_reads_right_output(*conjunct, *output)) {
            return node;
        }
        kept.push_back(*conjunct);
    }
    if (!found_marker) {
        return node;
    }

    const auto id = join.id();
    const auto keys = join.keys();
    const auto pending_order = join.pending_order();
    auto children = std::move(join.mutable_children());
    auto anti = std::make_unique<JoinNode>(id, JoinKind::Anti, keys);
    anti->set_pending_order(pending_order);
    anti->add_child(std::move(children[0]));
    anti->add_child(std::move(children[1]));
    drop_redundant_right_distinct(*anti);
    if (kept.empty()) {
        return anti;
    }
    auto residual = std::make_unique<FilterNode>(filter.id(), and_combine(std::move(kept)));
    residual->add_child(std::move(anti));
    return residual;
}

auto swapped_keys(const std::vector<JoinKey>& keys) -> std::vector<JoinKey> {
    std::vector<JoinKey> out;
    out.reserve(keys.size());
    for (const auto& key : keys) {
        out.emplace_back(key.right, key.left);
    }
    return out;
}

auto make_semi(NodePtr kept, NodePtr dropped, std::vector<JoinKey> keys, std::uint64_t& next)
    -> NodePtr {
    auto semi = std::make_unique<JoinNode>(fresh_id(next), JoinKind::Semi, std::move(keys));
    semi->add_child(std::move(kept));
    semi->add_child(std::move(dropped));
    return semi;
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)
// Node kind is checked immediately before every downcast below.

auto rewrite_join(NodePtr node, const SourceSchemas& sources,
                  const std::map<const Node*, ColumnDemand>& demand, std::uint64_t& next)
    -> NodePtr {
    const auto& join = static_cast<const JoinNode&>(*node);
    if (!reducible_shape(join)) {
        return node;
    }
    const auto found = demand.find(node.get());
    if (found == demand.end() || found->second.all) {
        return node;
    }

    const SchemaInfo left = infer_schema(*join.children()[0], sources);
    const SchemaInfo right = infer_schema(*join.children()[1], sources);
    // A missing-column test is only sound on a closed, Known schema: an open
    // one may carry columns this pass cannot see, and would silently drop them.
    if (!left.is_known() || left.is_open() || !right.is_known() || right.is_open()) {
        return node;
    }
    const std::vector<std::string> left_names = field_names(left);
    const std::vector<std::string> right_names = field_names(right);

    // Prefer dropping the right: it keeps the retained side, the key
    // orientation and the output column order exactly as they were.
    if (right.is_unique_within(right_join_key_names(join.keys())) &&
        dropped_side_is_unread(found->second, right_names, left_names)) {
        auto children = std::move(node->mutable_children());
        return make_semi(std::move(children[0]), std::move(children[1]), join.keys(), next);
    }
    if (left.is_unique_within(left_join_key_names(join.keys())) &&
        dropped_side_is_unread(found->second, left_names, right_names)) {
        auto children = std::move(node->mutable_children());
        return make_semi(std::move(children[1]), std::move(children[0]), swapped_keys(join.keys()),
                         next);
    }
    return node;
}

auto walk(NodePtr node, const SourceSchemas& sources,
          const std::map<const Node*, ColumnDemand>& demand,
          const std::map<const Node*, ColumnDemand>& filter_demand, std::uint64_t& next)
    -> NodePtr {
    if (!node) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), sources, demand, filter_demand, next);
    }
    if (node->kind() == NodeKind::Program) {
        auto& prog = static_cast<ProgramNode&>(*node);
        if (prog.mutable_main_node()) {
            prog.mutable_main_node() =
                walk(std::move(prog.mutable_main_node()), sources, demand, filter_demand, next);
        }
        for (auto& pre : prog.mutable_preamble()) {
            pre = walk(std::move(pre), sources, demand, filter_demand, next);
        }
    }
    if (node->kind() == NodeKind::Filter && node->children().size() == 1 &&
        node->children().front() != nullptr && node->children().front()->kind() == NodeKind::Join) {
        return rewrite_left_null_filter_to_anti(std::move(node), sources, filter_demand);
    }
    if (node->kind() == NodeKind::Join) {
        NodePtr rewritten = rewrite_join(std::move(node), sources, demand, next);
        if (rewritten->kind() == NodeKind::Join) {
            drop_redundant_right_distinct(static_cast<JoinNode&>(*rewritten));
        }
        return rewritten;
    }
    return node;
}

// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace

auto reduce_inner_joins_to_semi(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    if (!root) {
        return root;
    }
    // Computed once, against the untouched tree: `join_output_demand` keys on
    // node address, and this pass replaces the very nodes it asks about. Every
    // lookup below happens before that node is replaced, and a reduction only
    // removes columns its own entry proved unread -- so no ancestor's demand
    // can be invalidated by a descendant's rewrite.
    const std::map<const Node*, ColumnDemand> demand = join_output_demand(*root);
    const std::map<const Node*, ColumnDemand> filter_demand = filter_output_demand(*root);
    std::uint64_t next = 0;
    collect_max_id(*root, next);
    ++next;
    return walk(std::move(root), sources, demand, filter_demand, next);
}

}  // namespace ibex::ir
