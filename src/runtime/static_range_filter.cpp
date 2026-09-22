// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "static_range_filter.hpp"

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <variant>
#include <vector>

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

/// A literal the interval can be built from.
struct RangeLiteral {
    enum class Kind : std::uint8_t { Int, Date, Double };
    Kind kind = Kind::Int;
    std::int64_t value = 0;  // Int and Date
    double real = 0.0;       // Double
};

auto range_literal(const ir::Expr& expr) -> std::optional<RangeLiteral> {
    const auto* literal = std::get_if<ir::Literal>(&expr.node);
    if (literal == nullptr) {
        return std::nullopt;
    }
    if (const auto* integer = std::get_if<std::int64_t>(&literal->value)) {
        return RangeLiteral{.kind = RangeLiteral::Kind::Int, .value = *integer};
    }
    if (const auto* date = std::get_if<Date>(&literal->value)) {
        return RangeLiteral{.kind = RangeLiteral::Kind::Date, .value = date->days};
    }
    if (const auto* real = std::get_if<double>(&literal->value)) {
        return RangeLiteral{.kind = RangeLiteral::Kind::Double, .real = *real};
    }
    return std::nullopt;
}

/// One `column <op> literal` comparison, normalized so the column is on the
/// left.
struct Term {
    std::string column;
    ir::CompareOp op = ir::CompareOp::Eq;
    RangeLiteral literal;
};

auto as_term(const ir::Expr& expr) -> std::optional<Term> {
    const auto* comparison = std::get_if<ir::CompareExpr>(&expr.node);
    if (comparison == nullptr || comparison->left == nullptr || comparison->right == nullptr) {
        return std::nullopt;
    }
    const auto* column = ir::as_column_ref(*comparison->left);
    auto literal = range_literal(*comparison->right);
    auto op = comparison->op;
    if (column == nullptr || !literal.has_value()) {
        column = ir::as_column_ref(*comparison->right);
        literal = range_literal(*comparison->left);
        op = inverted_compare(op);
    }
    if (column == nullptr || column->lexical || !literal.has_value()) {
        return std::nullopt;
    }
    return Term{.column = column->name, .op = op, .literal = *literal};
}

/// Whether the ordinary filter compares these operands, so answering the
/// comparison from raw int64 bits cannot change what the query means. A Date
/// literal needs a Date column and a Double literal an Int column; an Int
/// literal is compared with either.
auto comparable(const Table& schema, const Term& term) -> bool {
    const auto* entry = schema.find_entry(term.column);
    if (entry == nullptr) {
        return false;
    }
    const bool is_date = std::holds_alternative<Column<Date>>(*entry->column);
    const bool is_int = std::holds_alternative<Column<std::int64_t>>(*entry->column);
    switch (term.literal.kind) {
        case RangeLiteral::Kind::Int:
            return is_date || is_int;
        case RangeLiteral::Kind::Date:
            return is_date;
        case RangeLiteral::Kind::Double:
            return is_int;
    }
    return false;
}

/// Below this magnitude every integer bound derived from a double literal is
/// exactly representable, and rounding an int64 to double is monotone, so
/// comparing the column with the literal agrees with comparing it with the
/// derived integer bound whether the comparison is exact or goes through
/// double. Beyond it the literal stays an ordinary conjunct.
constexpr double kExactDoubleLimit = 9007199254740992.0;  // 2^53

/// Fold a comparison against a double literal into `filter` as integer bounds.
auto absorb_real(ir::CompareOp op, double x, DynamicScanFilter& filter) -> bool {
    if (!std::isfinite(x) || std::fabs(x) >= kExactDoubleLimit) {
        return false;
    }
    const auto floor_x = static_cast<std::int64_t>(std::floor(x));
    const auto ceil_x = static_cast<std::int64_t>(std::ceil(x));
    const auto raise_min = [&](std::int64_t v) {
        filter.min = filter.min.has_value() ? std::max(*filter.min, v) : v;
    };
    const auto lower_max = [&](std::int64_t v) {
        filter.max = filter.max.has_value() ? std::min(*filter.max, v) : v;
    };
    switch (op) {
        case ir::CompareOp::Gt:
            raise_min(floor_x + 1);
            return true;
        case ir::CompareOp::Ge:
            raise_min(ceil_x);
            return true;
        case ir::CompareOp::Lt:
            lower_max(ceil_x - 1);
            return true;
        case ir::CompareOp::Le:
            lower_max(floor_x);
            return true;
        case ir::CompareOp::Eq:
            if (floor_x == ceil_x) {
                raise_min(floor_x);
                lower_max(floor_x);
            } else {
                // No integer equals a fractional literal: an empty interval.
                filter.min = std::numeric_limits<std::int64_t>::max();
                filter.max = std::numeric_limits<std::int64_t>::min();
            }
            return true;
        case ir::CompareOp::Ne:
            return false;
    }
    return false;
}

/// Fold `term` into `filter`. False when the interval cannot express it (`!=`,
/// or a strict bound at the int64 extreme), and the term stays a conjunct.
auto absorb(const Term& term, DynamicScanFilter& filter) -> bool {
    if (term.literal.kind == RangeLiteral::Kind::Double) {
        return absorb_real(term.op, term.literal.real, filter);
    }
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
