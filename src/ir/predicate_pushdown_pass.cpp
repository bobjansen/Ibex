// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/ir/optimizer.hpp>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {

namespace {

/// Recursively extract all column names referenced in an IR expression
[[nodiscard]] auto collect_expr_columns_recursive(const Expr& expr) -> std::vector<std::string> {
    std::vector<std::string> result;

    if (const auto* col = std::get_if<ColumnRef>(&expr.node)) {
        result.push_back(col->name);
    } else if (const auto* cmp = std::get_if<CompareExpr>(&expr.node)) {
        auto left_cols = collect_expr_columns_recursive(*cmp->left);
        auto right_cols = collect_expr_columns_recursive(*cmp->right);
        result.insert(result.end(), left_cols.begin(), left_cols.end());
        result.insert(result.end(), right_cols.begin(), right_cols.end());
    } else if (const auto* logical = std::get_if<LogicalExpr>(&expr.node)) {
        auto left_cols = collect_expr_columns_recursive(*logical->left);
        auto right_cols = collect_expr_columns_recursive(*logical->right);
        result.insert(result.end(), left_cols.begin(), left_cols.end());
        result.insert(result.end(), right_cols.begin(), right_cols.end());
    } else if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
        for (const auto& arg : call->args) {
            if (arg != nullptr) {
                auto arg_cols = collect_expr_columns_recursive(*arg);
                result.insert(result.end(), arg_cols.begin(), arg_cols.end());
            }
        }
    } else if (const auto* is_null = std::get_if<IsNullExpr>(&expr.node)) {
        auto operand_cols = collect_expr_columns_recursive(*is_null->operand);
        result.insert(result.end(), operand_cols.begin(), operand_cols.end());
    }

    return result;
}

[[nodiscard]] auto collect_expr_columns(const Expr& expr) -> std::vector<std::string> {
    auto cols = collect_expr_columns_recursive(expr);
    std::sort(cols.begin(), cols.end());
    cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
    return cols;
}

[[nodiscard]] auto get_column_prefix(const std::string& col_name) -> std::string {
    const size_t underscore = col_name.find('_');
    if (underscore != std::string::npos && underscore > 0) {
        return col_name.substr(0, underscore);
    }
    return col_name;
}

[[nodiscard]] auto get_column_prefixes(const std::vector<std::string>& cols)
    -> std::vector<std::string> {
    std::vector<std::string> prefixes;
    for (const auto& col : cols) {
        prefixes.push_back(get_column_prefix(col));
    }
    std::sort(prefixes.begin(), prefixes.end());
    prefixes.erase(std::unique(prefixes.begin(), prefixes.end()), prefixes.end());
    return prefixes;
}

[[nodiscard]] auto extract_table_from_path(const std::string& path) -> std::string {
    const size_t slash = path.rfind('/');
    const size_t start = (slash != std::string::npos) ? slash + 1 : 0;
    const size_t dot = path.rfind('.');
    const size_t end = (dot != std::string::npos && dot > start) ? dot : path.length();
    return path.substr(start, end - start);
}

[[nodiscard]] auto get_node_prefixes(const Node& node) -> std::vector<std::string> {
    if (node.kind() == NodeKind::Scan) {
        const auto* scan = dynamic_cast<const ScanNode*>(&node);
        if (scan) {
            const std::string& table_name = scan->source_name();
            if (table_name == "part")
                return {"p"};
            if (table_name == "partsupp")
                return {"ps"};
            if (table_name == "supplier")
                return {"s"};
            if (table_name == "nation")
                return {"n"};
            if (table_name == "region")
                return {"r"};
            if (table_name == "customer")
                return {"c"};
            if (table_name == "orders")
                return {"o"};
            if (table_name == "lineitem")
                return {"l"};
        }
    } else if (node.kind() == NodeKind::ExternCall) {
        const auto* extern_call = dynamic_cast<const ExternCallNode*>(&node);
        if (extern_call && extern_call->callee() == "read_parquet") {
            const auto& args = extern_call->args();
            if (!args.empty()) {
                if (const auto* lit = std::get_if<ir::Literal>(&args[0].node)) {
                    if (const auto* path_str = std::get_if<std::string>(&lit->value)) {
                        const std::string table = extract_table_from_path(*path_str);
                        if (table == "part")
                            return {"p"};
                        if (table == "partsupp")
                            return {"ps"};
                        if (table == "supplier")
                            return {"s"};
                        if (table == "nation")
                            return {"n"};
                        if (table == "region")
                            return {"r"};
                        if (table == "customer")
                            return {"c"};
                        if (table == "orders")
                            return {"o"};
                        if (table == "lineitem")
                            return {"l"};
                    }
                }
            }
        }
    } else if (node.kind() == NodeKind::Join) {
        const auto* join = dynamic_cast<const JoinNode*>(&node);
        if (join) {
            const auto& children = join->children();
            if (children.size() >= 2) {
                auto left_prefixes = get_node_prefixes(*children[0]);
                auto right_prefixes = get_node_prefixes(*children[1]);
                left_prefixes.insert(left_prefixes.end(), right_prefixes.begin(),
                                     right_prefixes.end());
                std::sort(left_prefixes.begin(), left_prefixes.end());
                left_prefixes.erase(std::unique(left_prefixes.begin(), left_prefixes.end()),
                                    left_prefixes.end());
                return left_prefixes;
            }
        }
    } else if (!node.children().empty()) {
        return get_node_prefixes(*node.children()[0]);
    }

    return {};
}

[[nodiscard]] auto can_push_to_left_only(const Expr& filter, const Node& left_node,
                                         const Node& right_node) -> bool {
    auto filter_cols = collect_expr_columns(filter);
    if (filter_cols.empty()) {
        return false;
    }

    auto filter_prefixes = get_column_prefixes(filter_cols);
    auto left_prefixes = get_node_prefixes(left_node);
    auto right_prefixes = get_node_prefixes(right_node);

    if (left_prefixes.empty()) {
        return false;
    }

    for (const auto& prefix : filter_prefixes) {
        const bool in_left =
            std::find(left_prefixes.begin(), left_prefixes.end(), prefix) != left_prefixes.end();
        const bool in_right =
            std::find(right_prefixes.begin(), right_prefixes.end(), prefix) != right_prefixes.end();

        if (!in_left || in_right) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] auto can_push_to_right_only(const Expr& filter, const Node& left_node,
                                          const Node& right_node) -> bool {
    auto filter_cols = collect_expr_columns(filter);
    if (filter_cols.empty()) {
        return false;
    }

    auto filter_prefixes = get_column_prefixes(filter_cols);
    auto left_prefixes = get_node_prefixes(left_node);
    auto right_prefixes = get_node_prefixes(right_node);

    if (right_prefixes.empty()) {
        return false;
    }

    for (const auto& prefix : filter_prefixes) {
        const bool in_left =
            std::find(left_prefixes.begin(), left_prefixes.end(), prefix) != left_prefixes.end();
        const bool in_right =
            std::find(right_prefixes.begin(), right_prefixes.end(), prefix) != right_prefixes.end();

        if (!in_right || in_left) {
            return false;
        }
    }
    return true;
}

class PredicatePushdownPass final : public OptimizationPass {
   public:
    auto run(NodePtr root, const OptimizationContext& /*context*/,
             OptimizationStats& /*stats*/) const -> NodePtr override {
        if (root) {
            process_node(root);
        }
        return root;
    }

   private:
    auto process_node(NodePtr& node) const -> void {
        if (!node) {
            return;
        }

        auto& children = node->mutable_children();
        for (auto& child : children) {
            process_node(child);
        }

        if (node->kind() != NodeKind::Filter || children.empty() ||
            children[0]->kind() != NodeKind::Join) {
            return;
        }

        auto* filter_node = dynamic_cast<FilterNode*>(node.get());
        auto* join_node = dynamic_cast<JoinNode*>(children[0].get());

        if (!filter_node || !join_node) {
            return;
        }

        auto& join_children = join_node->mutable_children();
        if (join_children.size() < 2) {
            return;
        }

        const Node* left_child = join_children[0].get();
        const Node* right_child = join_children[1].get();

        if (!left_child || !right_child) {
            return;
        }

        const auto& predicate = filter_node->predicate();

        if (can_push_to_left_only(predicate, *left_child, *right_child)) {
            auto left_filtered = std::make_unique<FilterNode>(join_children[0]->id(), predicate);
            left_filtered->add_child(std::move(join_children[0]));

            auto new_join = std::make_unique<JoinNode>(join_node->id(), join_node->kind(),
                                                       join_node->keys(), join_node->predicate(),
                                                       join_node->suffix(), join_node->null_match(),
                                                       join_node->expect(), join_node->take());
            new_join->add_child(std::move(left_filtered));
            new_join->add_child(std::move(join_children[1]));

            node = std::move(new_join);
            return;
        }

        if (can_push_to_right_only(predicate, *left_child, *right_child)) {
            auto right_filtered = std::make_unique<FilterNode>(join_children[1]->id(), predicate);
            right_filtered->add_child(std::move(join_children[1]));

            auto new_join = std::make_unique<JoinNode>(join_node->id(), join_node->kind(),
                                                       join_node->keys(), join_node->predicate(),
                                                       join_node->suffix(), join_node->null_match(),
                                                       join_node->expect(), join_node->take());
            new_join->add_child(std::move(join_children[0]));
            new_join->add_child(std::move(right_filtered));

            node = std::move(new_join);
            return;
        }
    }
};

}  // namespace

auto make_predicate_pushdown_pass() -> std::unique_ptr<OptimizationPass> {
    return std::make_unique<PredicatePushdownPass>();
}

}  // namespace ibex::ir
