// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/node.hpp>

#include <algorithm>
#include <expected>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {

ColumnNameMap::ColumnNameMap(std::span<const RenameSpec> renames)
    : renames_(renames.begin(), renames.end()) {}

auto ColumnNameMap::output_name(std::string_view input) const -> std::string {
    const auto found = std::ranges::find(renames_, input, &RenameSpec::old_name);
    return found == renames_.end() ? std::string(input) : found->new_name;
}

auto ColumnNameMap::input_name(std::string_view output) const -> std::string {
    const auto found = std::ranges::find(renames_, output, &RenameSpec::new_name);
    return found == renames_.end() ? std::string(output) : found->old_name;
}

auto ColumnNameMap::validate() const -> std::expected<void, std::string> {
    robin_hood::unordered_set<std::string> sources;
    robin_hood::unordered_set<std::string> targets;
    sources.reserve(renames_.size());
    targets.reserve(renames_.size());
    for (const auto& spec : renames_) {
        if (!sources.insert(spec.old_name).second) {
            return std::unexpected("rename: source column appears more than once: " +
                                   spec.old_name);
        }
        if (!targets.insert(spec.new_name).second) {
            return std::unexpected("rename: target column appears more than once: " +
                                   spec.new_name);
        }
    }
    return {};
}

auto ColumnNameMap::validate_input(std::span<const std::string_view> input_names) const
    -> std::expected<void, std::string> {
    if (auto valid = validate(); !valid.has_value()) {
        return valid;
    }
    const robin_hood::unordered_set<std::string_view> input(input_names.begin(), input_names.end());
    robin_hood::unordered_set<std::string_view> moving_sources;
    moving_sources.reserve(renames_.size());
    for (const auto& spec : renames_) {
        if (spec.new_name != spec.old_name) {
            moving_sources.insert(spec.old_name);
        }
    }
    for (const auto& spec : renames_) {
        if (!input.contains(spec.old_name)) {
            return std::unexpected("rename: column not found: " + spec.old_name);
        }
        if (spec.new_name != spec.old_name && input.contains(spec.new_name) &&
            !moving_sources.contains(spec.new_name)) {
            return std::unexpected("rename: target column already exists: " + spec.new_name);
        }
    }
    return {};
}

void ColumnNameMap::remap_expr_to_input(Expr& expr) const {
    std::visit(
        [&](auto& node) {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                if (!node.lexical) {
                    node.name = input_name(node.name);
                }
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                remap_expr_to_input(*node.left);
                remap_expr_to_input(*node.right);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                remap_expr_to_input(*node.left);
                if (node.right != nullptr) {
                    remap_expr_to_input(*node.right);
                }
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (auto& arg : node.args) {
                    remap_expr_to_input(*arg);
                }
                for (auto& named : node.named_args) {
                    if (named.value != nullptr) {
                        remap_expr_to_input(*named.value);
                    }
                }
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                remap_expr_to_input(*node.operand);
            }
        },
        expr.node);
}

auto ColumnNameMap::compose(std::span<const RenameSpec> outer, std::span<const RenameSpec> inner)
    -> std::vector<RenameSpec> {
    const ColumnNameMap inner_map(inner);
    robin_hood::unordered_set<std::string> used_inner_targets;
    used_inner_targets.reserve(inner.size());
    std::vector<RenameSpec> out;
    out.reserve(outer.size() + inner.size());
    for (const auto& spec : outer) {
        const std::string old = inner_map.input_name(spec.old_name);
        if (old != spec.old_name) {
            used_inner_targets.insert(spec.old_name);
        }
        if (spec.new_name != old) {
            out.push_back(RenameSpec{.new_name = spec.new_name, .old_name = old});
        }
    }
    for (const auto& spec : inner) {
        if (!used_inner_targets.contains(spec.new_name) && spec.new_name != spec.old_name) {
            out.push_back(spec);
        }
    }
    return out;
}

}  // namespace ibex::ir
