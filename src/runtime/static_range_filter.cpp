// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "static_range_filter.hpp"

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>
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

/// One `column <op> literal` comparison, normalized so the column is on the
/// left.
struct Term {
    std::string column;
    ir::CompareOp op = ir::CompareOp::Eq;
    IntegerLiteral literal;
};

auto as_term(const ir::Expr& expr) -> std::optional<Term> {
    const auto* comparison = std::get_if<ir::CompareExpr>(&expr.node);
    if (comparison == nullptr || comparison->left == nullptr || comparison->right == nullptr) {
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
    return Term{.column = column->name, .op = op, .literal = *literal};
}

/// Whether the ordinary filter compares these operands, so answering the
/// comparison from raw int64 bits cannot change what the query means.
auto comparable(const Table& schema, const Term& term) -> bool {
    const auto* entry = schema.find_entry(term.column);
    if (entry == nullptr) {
        return false;
    }
    if (std::holds_alternative<Column<Date>>(*entry->column)) {
        return true;
    }
    return !term.literal.is_date && std::holds_alternative<Column<std::int64_t>>(*entry->column);
}

/// Fold `term` into `filter`. False when the interval cannot express it (`!=`,
/// or a strict bound at the int64 extreme), and the term stays a conjunct.
auto absorb(const Term& term, DynamicScanFilter& filter) -> bool {
    std::int64_t value = term.literal.value;
    switch (term.op) {
        case ir::CompareOp::Eq:
            filter.min = filter.min.has_value() ? std::max(*filter.min, value) : value;
            filter.max = filter.max.has_value() ? std::min(*filter.max, value) : value;
            return true;
        case ir::CompareOp::Le:
            filter.max = filter.max.has_value() ? std::min(*filter.max, value) : value;
            return true;
        case ir::CompareOp::Ge:
            filter.min = filter.min.has_value() ? std::max(*filter.min, value) : value;
            return true;
        case ir::CompareOp::Lt:
            if (value == std::numeric_limits<std::int64_t>::min()) {
                return false;
            }
            --value;
            filter.max = filter.max.has_value() ? std::min(*filter.max, value) : value;
            return true;
        case ir::CompareOp::Gt:
            if (value == std::numeric_limits<std::int64_t>::max()) {
                return false;
            }
            ++value;
            filter.min = filter.min.has_value() ? std::max(*filter.min, value) : value;
            return true;
        case ir::CompareOp::Ne:
            return false;
    }
    return false;
}

}  // namespace

auto split_static_range(const std::vector<ir::Expr>& conjuncts, const Table& schema)
    -> std::optional<StaticRange> {
    std::vector<std::optional<Term>> terms;
    terms.reserve(conjuncts.size());
    for (const auto& conjunct : conjuncts) {
        terms.push_back(as_term(conjunct));
    }

    // The first column the reader can answer.
    std::optional<std::string> column;
    for (const auto& term : terms) {
        if (term.has_value() && comparable(schema, *term)) {
            column = term->column;
            break;
        }
    }
    if (!column.has_value()) {
        return std::nullopt;
    }

    StaticRange out;
    out.column = *column;
    bool absorbed_any = false;
    for (std::size_t i = 0; i < conjuncts.size(); ++i) {
        const auto& term = terms[i];
        if (term.has_value() && term->column == *column) {
            if (!comparable(schema, *term)) {
                return std::nullopt;
            }
            if (absorb(*term, out.filter)) {
                absorbed_any = true;
                continue;
            }
        }
        out.rest.push_back(conjuncts[i]);
    }
    if (!absorbed_any) {
        return std::nullopt;
    }
    return out;
}

}  // namespace ibex::runtime
