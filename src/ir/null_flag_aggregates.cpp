// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/ir/null_flag_aggregates.hpp>
#include <ibex/ir/nullability.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

auto is_zero_literal(const Expr& expr) -> bool {
    const auto* literal = std::get_if<Literal>(&expr.node);
    if (literal == nullptr) {
        return false;
    }
    const auto* value = std::get_if<std::int64_t>(&literal->value);
    return value != nullptr && *value == 0;
}

/// Whether `expr` is 0 on every row of `input`: `Int64(is_null(e))` with `e`
/// never null, or a `case` whose every value is such a flag or a literal 0.
// NOLINTNEXTLINE(misc-no-recursion)
auto is_zero_flag(const Expr& expr, const SchemaInfo& input) -> bool {
    const auto* call = std::get_if<CallExpr>(&expr.node);
    if (call == nullptr || !call->named_args.empty()) {
        return false;
    }
    if (call->callee == "Int64") {
        if (call->args.size() != 1 || call->args[0] == nullptr) {
            return false;
        }
        const auto* flag = std::get_if<IsNullExpr>(&call->args[0]->node);
        return flag != nullptr && !flag->negated && flag->operand != nullptr &&
               expr_nullability(*flag->operand, input) == Nullability::Never;
    }
    if (call->callee == "__case") {
        // condition, value, ..., else value
        if (call->args.size() < 3 || call->args.size() % 2 == 0) {
            return false;
        }
        for (std::size_t i = 1; i < call->args.size(); i += 2) {
            if (call->args[i] == nullptr) {
                return false;
            }
            const Expr& value = *call->args[i];
            if (!is_zero_literal(value) && !is_zero_flag(value, input)) {
                return false;
            }
        }
        const Expr& otherwise = *call->args.back();
        return call->args.back() != nullptr &&
               (is_zero_literal(otherwise) || is_zero_flag(otherwise, input));
    }
    return false;
}

/// Replace every column reference to a name in `zeros` with the literal 0.
// NOLINTNEXTLINE(misc-no-recursion)
void substitute_zeros(Expr& expr, const std::set<std::string>& zeros) {
    const auto visit_ptr = [&](ExprPtr& ptr) {
        if (ptr != nullptr) {
            substitute_zeros(*ptr, zeros);
        }
    };
    if (auto* ref = std::get_if<ColumnRef>(&expr.node)) {
        if (!ref->lexical && zeros.contains(ref->name)) {
            expr.node = Literal{.value = std::int64_t{0}};
        }
    } else if (auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
        visit_ptr(binary->left);
        visit_ptr(binary->right);
    } else if (auto* call = std::get_if<CallExpr>(&expr.node)) {
        for (auto& arg : call->args) {
            visit_ptr(arg);
        }
        for (auto& named : call->named_args) {
            visit_ptr(named.value);
        }
    } else if (auto* compare = std::get_if<CompareExpr>(&expr.node)) {
        visit_ptr(compare->left);
        visit_ptr(compare->right);
    } else if (auto* logical = std::get_if<LogicalExpr>(&expr.node)) {
        visit_ptr(logical->left);
        visit_ptr(logical->right);
    } else if (auto* is_null = std::get_if<IsNullExpr>(&expr.node)) {
        visit_ptr(is_null->operand);
    }
}

auto is_plain_update(const Node& node) -> bool {
    if (node.kind() != NodeKind::Update || node.children().size() != 1 ||
        node.children().front() == nullptr) {
        return false;
    }
    const auto& update = node_cast<UpdateNode>(node);
    return update.group_by().empty() && update.guard() == nullptr && update.tuple_fields().empty();
}

/// Rewrite the Update(Aggregate([Project...] Update(input))) chain rooted at
/// `post_slot` in place. Projections the column pruning put between the
/// aggregate and its pre-aggregate update are crossed.
void fold(NodePtr& post_slot, const SourceSchemas& sources) {
    if (!is_plain_update(*post_slot)) {
        return;
    }
    NodePtr& aggregate_slot = post_slot->mutable_children().front();
    if (aggregate_slot->kind() != NodeKind::Aggregate || aggregate_slot->children().size() != 1 ||
        aggregate_slot->children().front() == nullptr) {
        return;
    }
    const auto& aggregate = node_cast<AggregateNode>(*aggregate_slot);
    // An ungrouped aggregate over no rows has one row whose `max` is null.
    if (aggregate.group_by().empty()) {
        return;
    }
    std::vector<NodePtr*> projections;
    NodePtr* pre_slot = &aggregate_slot->mutable_children().front();
    while ((*pre_slot)->kind() == NodeKind::Project && (*pre_slot)->children().size() == 1 &&
           (*pre_slot)->children().front() != nullptr) {
        projections.push_back(pre_slot);
        pre_slot = &(*pre_slot)->mutable_children().front();
    }
    if (!is_plain_update(**pre_slot)) {
        return;
    }
    const auto& pre = node_cast<UpdateNode>(**pre_slot);
    const SchemaInfo input = infer_schema(*(*pre_slot)->children().front(), sources);
    if (!input.is_known()) {
        return;
    }
    std::set<std::string> zero_flags;
    for (const auto& field : pre.fields()) {
        if (is_zero_flag(field.expr, input)) {
            zero_flags.insert(field.alias);
        }
    }
    if (zero_flags.empty()) {
        return;
    }

    std::set<std::string> folded;
    std::vector<AggSpec> kept;
    for (const auto& spec : aggregate.aggregations()) {
        if ((spec.func == AggFunc::Max || spec.func == AggFunc::Min) && !spec.is_count &&
            !spec.column.lexical && zero_flags.contains(spec.column.name)) {
            folded.insert(spec.alias);
        } else {
            kept.push_back(spec);
        }
    }
    if (folded.empty()) {
        return;
    }
    const auto& post = node_cast<UpdateNode>(*post_slot);
    // The post-update must not already assign a folded name: it is about to.
    if (std::ranges::any_of(post.fields(),
                            [&](const FieldSpec& field) { return folded.contains(field.alias); })) {
        return;
    }

    // Whatever still reads a pre-aggregate column keeps it computed.
    std::set<std::string> read;
    for (const auto& key : aggregate.group_by()) {
        read.insert(key.name);
    }
    for (const auto& spec : kept) {
        read.insert(spec.column.name);
        read.insert(spec.paired.name);
    }
    std::set<std::string> dropped;
    std::vector<FieldSpec> pre_fields;
    for (const auto& field : pre.fields()) {
        if (zero_flags.contains(field.alias) && !read.contains(field.alias)) {
            dropped.insert(field.alias);
        } else {
            pre_fields.push_back(field);
        }
    }

    // Each folded aggregate's column stays, as the literal it always held, so
    // nothing above the update can tell. Fields read it as the literal too.
    std::vector<FieldSpec> post_fields;
    post_fields.reserve(post.fields().size() + folded.size());
    for (const auto& alias : folded) {
        post_fields.push_back(
            FieldSpec{.alias = alias, .expr = Expr{.node = Literal{.value = std::int64_t{0}}}});
    }
    for (const auto& field : post.fields()) {
        post_fields.push_back(field);
        substitute_zeros(post_fields.back().expr, folded);
    }

    // Rebuild bottom-up: the pre-update, the projections over it, the
    // aggregate, then the post-update.
    NodePtr below = std::move((*pre_slot)->mutable_children().front());
    if (!pre_fields.empty()) {
        auto rebuilt = std::make_unique<UpdateNode>((*pre_slot)->id(), std::move(pre_fields));
        rebuilt->add_child(std::move(below));
        below = std::move(rebuilt);
    }
    for (auto it = projections.rbegin(); it != projections.rend(); ++it) {
        std::vector<ColumnRef> columns;
        for (const auto& column : node_cast<ProjectNode>(***it).columns()) {
            if (!dropped.contains(column.name)) {
                columns.push_back(column);
            }
        }
        auto rebuilt = std::make_unique<ProjectNode>((**it)->id(), std::move(columns));
        rebuilt->add_child(std::move(below));
        below = std::move(rebuilt);
    }
    auto rebuilt_aggregate = std::make_unique<AggregateNode>(aggregate_slot->id(),
                                                             aggregate.group_by(), std::move(kept));
    rebuilt_aggregate->add_child(std::move(below));
    auto rebuilt_post = std::make_unique<UpdateNode>(post_slot->id(), std::move(post_fields));
    rebuilt_post->add_child(std::move(rebuilt_aggregate));
    post_slot = std::move(rebuilt_post);
}

// NOLINTNEXTLINE(misc-no-recursion)
void walk(NodePtr& node, const SourceSchemas& sources) {
    if (node == nullptr) {
        return;
    }
    for (auto& child : node->mutable_children()) {
        walk(child, sources);
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = node_cast<ProgramNode>(*node);
        walk(program.mutable_main_node(), sources);
        for (auto& pre : program.mutable_preamble()) {
            walk(pre, sources);
        }
        return;
    }
    if (node->kind() == NodeKind::Update) {
        fold(node, sources);
    }
}

}  // namespace

auto fold_constant_null_flag_aggregates(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    walk(root, sources);
    return root;
}

}  // namespace ibex::ir
