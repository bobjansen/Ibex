// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Decimal arithmetic and comparison kernels shared by the per-row evaluator
// (expr.cpp) and the vectorized one (filter.cpp). One implementation of each
// rule, so the two paths cannot disagree on a result type, an overflow, or a
// rounding. Semantics: plans/decimal-plan.md.

#include <ibex/core/decimal.hpp>
#include <ibex/ir/node.hpp>

#include <compare>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>

#include "interpreter_internal.hpp"

namespace ibex::runtime {

/// Static result type of a decimal expression. Only meaningful once
/// `infer_expr_type` has said Decimal for the same expression. Defined in
/// expr.cpp next to `infer_expr_type`, which it mirrors.
[[nodiscard]] auto infer_decimal_type(const ir::Expr& expr, const Table& input,
                                      const ScalarRegistry* scalars)
    -> std::expected<DecimalType, std::string>;

/// True for a Float64 literal, the only double the decimal rules admit: it is
/// read back exactly from its shortest round-trip text (`10.5` is `10.5`).
[[nodiscard]] inline auto is_float_literal(const ir::Expr& expr) -> bool {
    const auto* lit = std::get_if<ir::Literal>(&expr.node);
    return lit != nullptr && std::holds_alternative<double>(lit->value);
}

[[nodiscard]] inline auto decimal_overflow(DecimalType t) -> std::string {
    return "decimal overflow: result does not fit " + decimal::type_name(t);
}

/// The decimal a numeric operand denotes when it meets a decimal. Int64 is
/// exact as Decimal(19, 0); a double is taken as the literal it was written as
/// (the type checker rejects a double that is not a literal).
[[nodiscard]] inline auto decimal_operand(const ExprValue& v)
    -> std::expected<DecimalValue, std::string> {
    if (const auto* d = std::get_if<DecimalValue>(&v)) {
        return *d;
    }
    if (const auto* i = std::get_if<std::int64_t>(&v)) {
        return DecimalValue{.units = Int128{*i}, .type = decimal::kInt64Type};
    }
    if (const auto* f = std::get_if<double>(&v)) {
        return decimal::literal_from_double(*f);
    }
    return std::unexpected("expected a numeric operand for Decimal arithmetic");
}

/// Result type of `a op b` for decimals, or an error for `%` and for a
/// product whose scale would exceed 38.
[[nodiscard]] inline auto decimal_arith_type(ir::ArithmeticOp op, DecimalType a, DecimalType b)
    -> std::expected<DecimalType, std::string> {
    switch (op) {
        case ir::ArithmeticOp::Add:
        case ir::ArithmeticOp::Sub:
            return decimal::add_result_type(a, b);
        case ir::ArithmeticOp::Mul:
            if (!decimal::mul_scale_ok(a, b)) {
                return std::unexpected("Decimal multiplication: result scale " +
                                       std::to_string(a.scale + b.scale) +
                                       " exceeds 38; cast an operand to a smaller scale first");
            }
            return decimal::mul_result_type(a, b);
        case ir::ArithmeticOp::Div:
            return std::unexpected("internal: Decimal division yields Float64");
        case ir::ArithmeticOp::Mod:
            return std::unexpected("'%' is not defined for Decimal");
    }
    return std::unexpected("unsupported Decimal operator");
}

/// `a op b` in units of `rt` (the precomputed result type). `sa`/`sb` are the
/// operand scales. Add/Sub align both to `rt.scale`; Mul multiplies units,
/// whose scale is already `sa + sb == rt.scale`.
[[nodiscard]] inline auto decimal_arith_units(ir::ArithmeticOp op, Int128 a, int sa, Int128 b,
                                              int sb, DecimalType rt, Int128& out) -> bool {
    switch (op) {
        case ir::ArithmeticOp::Add:
        case ir::ArithmeticOp::Sub: {
            Int128 x = 0;
            Int128 y = 0;
            if (!decimal::rescale(a, sa, rt.scale, x) || !decimal::rescale(b, sb, rt.scale, y)) {
                return false;
            }
            const bool ok = op == ir::ArithmeticOp::Add ? decimal::checked_add(x, y, out)
                                                        : decimal::checked_sub(x, y, out);
            return ok && decimal::fits(out, rt.precision);
        }
        case ir::ArithmeticOp::Mul:
            return decimal::checked_mul(a, b, out) && decimal::fits(out, rt.precision);
        default:
            return false;
    }
}

/// Row-level `a op b` where at least one side is a decimal.
[[nodiscard]] inline auto decimal_arith(ir::ArithmeticOp op, const ExprValue& left,
                                        const ExprValue& right)
    -> std::expected<ExprValue, std::string> {
    auto a = decimal_operand(left);
    if (!a) {
        return std::unexpected(a.error());
    }
    auto b = decimal_operand(right);
    if (!b) {
        return std::unexpected(b.error());
    }
    if (op == ir::ArithmeticOp::Div) {
        // `/` has one result type (SPEC 3.1). IEEE rules for a zero divisor.
        return ExprValue{decimal::to_double(a->units, a->type.scale) /
                         decimal::to_double(b->units, b->type.scale)};
    }
    auto rt = decimal_arith_type(op, a->type, b->type);
    if (!rt) {
        return std::unexpected(rt.error());
    }
    Int128 out = 0;
    if (!decimal_arith_units(op, a->units, a->type.scale, b->units, b->type.scale, *rt, out)) {
        return std::unexpected(decimal_overflow(*rt));
    }
    return ExprValue{DecimalValue{.units = out, .type = *rt}};
}

/// The units a computed value holds in a `Decimal(target)` column: decimals
/// rescaled (half away from zero), Int64 exactly. Too large is an error.
[[nodiscard]] inline auto fit_decimal(const ExprValue& v, DecimalType target)
    -> std::expected<Int128, std::string> {
    auto d = decimal_operand(v);
    if (!d) {
        return std::unexpected(d.error());
    }
    Int128 out = 0;
    if (!decimal::rescale(d->units, d->type.scale, target.scale, out) ||
        !decimal::fits(out, target.precision)) {
        return std::unexpected("decimal overflow: " + decimal::to_string(*d) + " does not fit " +
                               decimal::type_name(target));
    }
    return out;
}

[[nodiscard]] inline auto compare_holds(ir::CompareOp op, std::strong_ordering ord) -> bool {
    switch (op) {
        case ir::CompareOp::Eq:
            return ord == 0;
        case ir::CompareOp::Ne:
            return ord != 0;
        case ir::CompareOp::Lt:
            return ord < 0;
        case ir::CompareOp::Le:
            return ord <= 0;
        case ir::CompareOp::Gt:
            return ord > 0;
        case ir::CompareOp::Ge:
            return ord >= 0;
    }
    return false;
}

/// A column read as decimal units at some scale: Decimal columns at their
/// own scale, Int64 columns at scale 0. Anything else is not decimal-numeric.
struct DecimalColumnView {
    const Decimal* dec = nullptr;
    const std::int64_t* i64 = nullptr;
    DecimalType type;

    [[nodiscard]] auto units(std::size_t i) const -> Int128 {
        return dec != nullptr ? dec[i].units : Int128{i64[i]};
    }
};

[[nodiscard]] inline auto decimal_column_view(const ColumnValue& col, std::size_t off)
    -> std::optional<DecimalColumnView> {
    if (const auto* d = std::get_if<Column<Decimal>>(&col)) {
        return DecimalColumnView{
            .dec = d->data() + off, .i64 = nullptr, .type = decimal_type_of(*d)};
    }
    if (const auto* i = std::get_if<Column<std::int64_t>>(&col)) {
        return DecimalColumnView{
            .dec = nullptr, .i64 = i->data() + off, .type = decimal::kInt64Type};
    }
    return std::nullopt;
}

[[nodiscard]] inline auto is_row_valid(const ValidityBitmap* v, std::size_t off, std::size_t i)
    -> bool {
    return v == nullptr || (*v)[off + i];
}

/// `lhs op rhs` over `n` rows, one side at least a Decimal column. Null rows
/// (either side invalid) are skipped: an imported null slot's payload is
/// undefined and must never be able to raise a spurious overflow.
[[nodiscard]] inline auto decimal_arith_columns(ir::ArithmeticOp op, const ColumnValue& lhs,
                                                std::size_t lhs_off, const ValidityBitmap* lv,
                                                const ColumnValue& rhs, std::size_t rhs_off,
                                                const ValidityBitmap* rv, std::size_t n)
    -> std::expected<ColumnValue, std::string> {
    auto a = decimal_column_view(lhs, lhs_off);
    auto b = decimal_column_view(rhs, rhs_off);
    if (!a || !b) {
        return std::unexpected(
            "Decimal arithmetic needs Decimal or Int64 operands; cast a Float64 operand with "
            "Decimal(x, precision, scale)");
    }
    if (op == ir::ArithmeticOp::Div) {
        Column<double> out;
        out.resize(n);
        double* op_out = out.data();
        for (std::size_t i = 0; i < n; ++i) {
            if (!is_row_valid(lv, lhs_off, i) || !is_row_valid(rv, rhs_off, i)) {
                op_out[i] = 0.0;
                continue;
            }
            op_out[i] = decimal::to_double(a->units(i), a->type.scale) /
                        decimal::to_double(b->units(i), b->type.scale);
        }
        return ColumnValue{std::move(out)};
    }
    auto rt = decimal_arith_type(op, a->type, b->type);
    if (!rt) {
        return std::unexpected(rt.error());
    }
    Column<Decimal> out = make_decimal_column(*rt);
    out.resize(n);
    Decimal* dst = out.data();
    for (std::size_t i = 0; i < n; ++i) {
        if (!is_row_valid(lv, lhs_off, i) || !is_row_valid(rv, rhs_off, i)) {
            dst[i] = Decimal{};
            continue;
        }
        Int128 r = 0;
        if (!decimal_arith_units(op, a->units(i), a->type.scale, b->units(i), b->type.scale, *rt,
                                 r)) {
            return std::unexpected(decimal_overflow(*rt));
        }
        dst[i] = Decimal{r};
    }
    return ColumnValue{std::move(out)};
}

/// A join/semi-join key pair that cannot be matched on raw units: two Decimal
/// columns at different scales. Rescaling one side per join would hide a cost
/// and an overflow; asking for the cast keeps both visible and the semantics
/// exact. Nullopt when the pair is fine (including any non-Decimal pair).
[[nodiscard]] inline auto decimal_key_scale_mismatch(const ColumnValue& left,
                                                     const ColumnValue& right,
                                                     const std::string& left_name,
                                                     const std::string& right_name)
    -> std::optional<std::string> {
    const auto* l = std::get_if<Column<Decimal>>(&left);
    const auto* r = std::get_if<Column<Decimal>>(&right);
    if (l == nullptr || r == nullptr) {
        return std::nullopt;
    }
    const DecimalType lt = decimal_type_of(*l);
    const DecimalType rt = decimal_type_of(*r);
    if (lt.scale == rt.scale) {
        return std::nullopt;
    }
    return "join key scale mismatch: left '" + left_name + "' is " + decimal::type_name(lt) +
           " but right '" + right_name + "' is " + decimal::type_name(rt) +
           "; cast one side to the other's scale with Decimal(x, precision, scale)";
}

/// Exact `col op rhs` into `mp`, `col` a Decimal or Int64 column. Returns
/// false when `col` is neither (the caller reports the type error).
///
/// The common shape -- the literal's scale no finer than the column's -- is
/// rescaled once and compared on raw units. A literal too large to rescale
/// exceeds every value the column can hold, so its sign decides every row.
inline auto decimal_compare_col_scalar(ir::CompareOp op, const ColumnValue& col, std::size_t off,
                                       const DecimalValue& rhs, std::uint8_t* mp, std::size_t n)
    -> bool {
    auto view = decimal_column_view(col, off);
    if (!view) {
        return false;
    }
    const int sc = view->type.scale;
    const int sr = rhs.type.scale;
    if (sr <= sc) {
        Int128 r = 0;
        if (!decimal::rescale(rhs.units, sr, sc, r)) {
            const auto ord =
                rhs.units < 0 ? std::strong_ordering::greater : std::strong_ordering::less;
            const std::uint8_t v = compare_holds(op, ord) ? 1 : 0;
            for (std::size_t i = 0; i < n; ++i) {
                mp[i] = v;
            }
            return true;
        }
        for (std::size_t i = 0; i < n; ++i) {
            const Int128 l = view->units(i);
            const auto ord =
                l == r ? std::strong_ordering::equal
                       : (l < r ? std::strong_ordering::less : std::strong_ordering::greater);
            mp[i] = compare_holds(op, ord) ? 1 : 0;
        }
        return true;
    }
    for (std::size_t i = 0; i < n; ++i) {
        mp[i] = compare_holds(op, decimal::compare(view->units(i), sc, rhs.units, sr)) ? 1 : 0;
    }
    return true;
}

/// Exact `lhs op rhs` over two columns, each Decimal or Int64 and at least
/// one Decimal. Returns false when either side is not decimal-numeric.
inline auto decimal_compare_columns(ir::CompareOp op, const ColumnValue& lhs, std::size_t lhs_off,
                                    const ColumnValue& rhs, std::size_t rhs_off, std::uint8_t* mp,
                                    std::size_t n) -> bool {
    auto a = decimal_column_view(lhs, lhs_off);
    auto b = decimal_column_view(rhs, rhs_off);
    if (!a || !b) {
        return false;
    }
    const int sa = a->type.scale;
    const int sb = b->type.scale;
    for (std::size_t i = 0; i < n; ++i) {
        mp[i] = compare_holds(op, decimal::compare(a->units(i), sa, b->units(i), sb)) ? 1 : 0;
    }
    return true;
}

}  // namespace ibex::runtime
