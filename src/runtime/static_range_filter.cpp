// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "static_range_filter.hpp"

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <variant>

namespace ibex::runtime {

namespace {

auto inverted_compare(ir::CompareOp op) -> ir::CompareOp {
    switch (op) {
        case ir::CompareOp::Lt:
            return ir::CompareOp::Gt;
        case ir::CompareOp::Le:
            return ir::CompareOp::Ge;
        case ir::CompareOp::Gt:
            return ir::CompareOp::Lt;
        case ir::CompareOp::Ge:
            return ir::CompareOp::Le;
        default:
            return op;
    }
}

struct IntegerLiteral {
    std::int64_t value = 0;
    bool is_date = false;
};

auto integer_literal(const ir::Expr& expr) -> std::optional<IntegerLiteral> {
    const auto* literal = std::get_if<ir::Literal>(&expr.node);
    if (literal == nullptr) {
        return std::nullopt;
    }
    if (const auto* integer = std::get_if<std::int64_t>(&literal->value)) {
        return IntegerLiteral{.value = *integer, .is_date = false};
    }
    if (const auto* date = std::get_if<Date>(&literal->value)) {
        return IntegerLiteral{.value = date->days, .is_date = true};
    }
    return std::nullopt;
}

/// Whether the ordinary filter compares these operands, so answering the
/// comparison from raw int64 bits cannot change what the query means.
auto comparable(const Table& schema, const std::string& column, bool date_literal) -> bool {
    const auto* entry = schema.find_entry(column);
    if (entry == nullptr) {
        return false;
    }
    if (std::holds_alternative<Column<Date>>(*entry->column)) {
        return true;
    }
    return !date_literal && std::holds_alternative<Column<std::int64_t>>(*entry->column);
}

}  // namespace

auto static_range_filter(const std::vector<ir::Expr>& conjuncts, const Table& schema)
    -> std::optional<std::pair<std::string, DynamicScanFilter>> {
    if (conjuncts.empty()) {
        return std::nullopt;
    }
    std::optional<std::string> name;
    bool any_date_literal = false;
    DynamicScanFilter filter;
    for (const auto& expr : conjuncts) {
        const auto* comparison = std::get_if<ir::CompareExpr>(&expr.node);
        if (comparison == nullptr || comparison->left == nullptr || comparison->right == nullptr ||
            comparison->op == ir::CompareOp::Ne) {
            return std::nullopt;
        }
        const auto* column = ir::as_column_ref(*comparison->left);
        auto literal = integer_literal(*comparison->right);
        auto op = comparison->op;
        if (column == nullptr || !literal.has_value()) {
            column = ir::as_column_ref(*comparison->right);
            literal = integer_literal(*comparison->left);
            op = inverted_compare(op);
        }
        if (column == nullptr || column->lexical || !literal.has_value()) {
            return std::nullopt;
        }
        if (name.has_value() && *name != column->name) {
            return std::nullopt;
        }
        name = column->name;
        any_date_literal = any_date_literal || literal->is_date;
        std::int64_t value = literal->value;
        switch (op) {
            case ir::CompareOp::Eq:
                filter.min = filter.min.has_value() ? std::max(*filter.min, value) : value;
                filter.max = filter.max.has_value() ? std::min(*filter.max, value) : value;
                break;
            case ir::CompareOp::Le:
                filter.max = filter.max.has_value() ? std::min(*filter.max, value) : value;
                break;
            case ir::CompareOp::Ge:
                filter.min = filter.min.has_value() ? std::max(*filter.min, value) : value;
                break;
            case ir::CompareOp::Lt:
                if (value == std::numeric_limits<std::int64_t>::min()) {
                    return std::nullopt;
                }
                --value;
                filter.max = filter.max.has_value() ? std::min(*filter.max, value) : value;
                break;
            case ir::CompareOp::Gt:
                if (value == std::numeric_limits<std::int64_t>::max()) {
                    return std::nullopt;
                }
                ++value;
                filter.min = filter.min.has_value() ? std::max(*filter.min, value) : value;
                break;
            case ir::CompareOp::Ne:
                return std::nullopt;
        }
    }
    if (!name.has_value() || !comparable(schema, *name, any_date_literal)) {
        return std::nullopt;
    }
    return std::pair{std::move(*name), std::move(filter)};
}

}  // namespace ibex::runtime
