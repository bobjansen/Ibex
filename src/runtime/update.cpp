// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// update.cpp — update/select field application: the scalar fast paths
// (compiled numeric expression trees, SIMD unary/pmin/pmax kernels), plain,
// grouped, windowed, and guarded update paths.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/safe_arith.hpp>
#include <ibex/runtime/table_format.hpp>
#include <ibex/runtime/table_properties.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <expected>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "kernel_filter.hpp"
#include "kernel_gather.hpp"
#include "kernel_update.hpp"
#include "zorro.hpp"

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "interpreter_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

struct FastOperand {
    bool is_column = false;
    const ColumnValue* column = nullptr;
    ScalarValue literal;
    ExprType kind = ExprType::Int;
};

auto resolve_fast_operand(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::optional<FastOperand> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        // `^name` skips column scope and resolves in the scalar registry below.
        if (const auto* source = col->lexical ? nullptr : input.find(col->name);
            source != nullptr) {
            return FastOperand{
                .is_column = true,
                .column = source,
                .literal = ScalarValue{},
                .kind = expr_type_for_column(*source),
            };
        }
        if (scalars != nullptr) {
            if (auto it = scalars->find(col->name); it != scalars->end()) {
                return FastOperand{
                    .is_column = false,
                    .column = nullptr,
                    .literal = it->second,
                    .kind = scalar_kind_from_value(it->second),
                };
            }
        }
        return std::nullopt;
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        const ScalarValue value = scalar_from_literal(*lit);
        return FastOperand{
            .is_column = false,
            .column = nullptr,
            .literal = value,
            .kind = scalar_kind_from_value(value),
        };
    }
    return std::nullopt;
}

auto apply_int_op(ir::ArithmeticOp op, std::int64_t lhs, std::int64_t rhs) -> std::int64_t {
    switch (op) {
        case ir::ArithmeticOp::Add:
            return lhs + rhs;
        case ir::ArithmeticOp::Sub:
            return lhs - rhs;
        case ir::ArithmeticOp::Mul:
            return lhs * rhs;
        case ir::ArithmeticOp::Div:
            return safe_idiv(lhs, rhs);
        case ir::ArithmeticOp::Mod:
            return safe_imod(lhs, rhs);
    }
    return 0;
}

auto apply_double_op(ir::ArithmeticOp op, double lhs, double rhs) -> double {
    switch (op) {
        case ir::ArithmeticOp::Add:
            return lhs + rhs;
        case ir::ArithmeticOp::Sub:
            return lhs - rhs;
        case ir::ArithmeticOp::Mul:
            return lhs * rhs;
        case ir::ArithmeticOp::Div:
            return lhs / rhs;
        case ir::ArithmeticOp::Mod:
            return std::fmod(lhs, rhs);
    }
    return 0.0;
}

auto get_int_value(const FastOperand& op, std::size_t row) -> std::int64_t {
    if (!op.is_column) {
        if (const auto* int_value = std::get_if<std::int64_t>(&op.literal)) {
            return *int_value;
        }
        if (const auto* date_value = std::get_if<Date>(&op.literal)) {
            return date_value->days;
        }
        if (const auto* ts_value = std::get_if<Timestamp>(&op.literal)) {
            return ts_value->nanos;
        }
        return static_cast<std::int64_t>(std::get<double>(op.literal));
    }
    if (const auto* int_col = std::get_if<Column<std::int64_t>>(op.column)) {
        return (*int_col)[row];
    }
    if (const auto* double_col = std::get_if<Column<double>>(op.column)) {
        return static_cast<std::int64_t>((*double_col)[row]);
    }
    if (const auto* date_col = std::get_if<Column<Date>>(op.column)) {
        return date_col->operator[](row).days;
    }
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(op.column)) {
        return ts_col->operator[](row).nanos;
    }
    invariant_violation("get_int_value: unexpected operand column type");
}

auto get_double_value(const FastOperand& op, std::size_t row) -> double {
    if (!op.is_column) {
        if (const auto* int_value = std::get_if<std::int64_t>(&op.literal)) {
            return static_cast<double>(*int_value);
        }
        if (const auto* date_value = std::get_if<Date>(&op.literal)) {
            return static_cast<double>(date_value->days);
        }
        if (const auto* ts_value = std::get_if<Timestamp>(&op.literal)) {
            return static_cast<double>(ts_value->nanos);
        }
        return std::get<double>(op.literal);
    }
    if (const auto* int_col = std::get_if<Column<std::int64_t>>(op.column)) {
        return static_cast<double>((*int_col)[row]);
    }
    if (const auto* double_col = std::get_if<Column<double>>(op.column)) {
        return (*double_col)[row];
    }
    if (const auto* date_col = std::get_if<Column<Date>>(op.column)) {
        return static_cast<double>(date_col->operator[](row).days);
    }
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(op.column)) {
        return static_cast<double>(ts_col->operator[](row).nanos);
    }
    invariant_violation("get_double_value: unexpected operand column type");
}

auto try_fast_update_binary(const ir::Expr& expr, const Table& input, RowRange range,
                            ExprType output_kind, const ScalarRegistry* scalars)
    -> std::optional<ColumnValue> {
    // `rows` is the output width; `begin` offsets every read of an input column
    // so the result is dense for the range.
    const std::size_t rows = range.count;
    const std::size_t begin = range.begin;
    const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node);
    if (bin == nullptr) {
        return std::nullopt;
    }
    auto left = resolve_fast_operand(*bin->left, input, scalars);
    if (!left) {
        return std::nullopt;
    }
    auto right = resolve_fast_operand(*bin->right, input, scalars);
    if (!right) {
        return std::nullopt;
    }
    if (left->kind == ExprType::String || right->kind == ExprType::String ||
        left->kind == ExprType::Date || right->kind == ExprType::Date ||
        left->kind == ExprType::Timestamp || right->kind == ExprType::Timestamp) {
        return std::nullopt;
    }
    // Helper: dispatch on (op × layout) OUTSIDE the inner loop so each resulting
    // loop body is a branch-free array kernel that the compiler can auto-vectorize.
    // `run` receives a stateless lambda (unique type per op) and executes the
    // appropriate col/col, col/scalar, or scalar/col loop.
    auto make_double_result = [&](auto op_fn, const double* lp, double ls, const double* rp,
                                  double rs) -> ColumnValue {
        Column<double> out;
        out.resize_for_overwrite(rows);
        double* dst = out.data();
        if (lp && rp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rp[i]);
        } else if (lp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rs);
        } else {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(ls, rp[i]);
        }
        return ColumnValue{std::move(out)};
    };
    auto make_int_result = [&](auto op_fn, const std::int64_t* lp, std::int64_t ls,
                               const std::int64_t* rp, std::int64_t rs) -> ColumnValue {
        Column<std::int64_t> out;
        out.resize_for_overwrite(rows);
        std::int64_t* dst = out.data();
        if (lp && rp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rp[i]);
        } else if (lp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(lp[i], rs);
        } else {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = op_fn(ls, rp[i]);
        }
        return ColumnValue{std::move(out)};
    };

    if (output_kind == ExprType::Double) {
        if (!left->is_column && !right->is_column) {
            const double value =
                apply_double_op(bin->op, get_double_value(*left, 0), get_double_value(*right, 0));
            Column<double> out;
            out.assign(rows, value);
            return ColumnValue{std::move(out)};
        }
        // Hoist all variant/type dispatch outside the inner loop.
        // Falls back to nullptr + scalar=0 for int-typed columns (uncommon path
        // handled by the fallback reserve+push_back loop below).
        const double* lp = (left->is_column && left->kind == ExprType::Double)
                               ? std::get<Column<double>>(*left->column).data() + begin
                               : nullptr;
        const double* rp = (right->is_column && right->kind == ExprType::Double)
                               ? std::get<Column<double>>(*right->column).data() + begin
                               : nullptr;
        const double ls = left->is_column ? 0.0 : get_double_value(*left, 0);
        const double rs = right->is_column ? 0.0 : get_double_value(*right, 0);
        // Only take the SIMD path when every used operand resolved to a raw pointer.
        const bool left_ok = !left->is_column || lp != nullptr;
        const bool right_ok = !right->is_column || rp != nullptr;
        if (left_ok && right_ok) {
            // Dispatch on op once, outside the loop, so each kernel is branch-free.
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return make_double_result([](double a, double b) { return a + b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Sub:
                    return make_double_result([](double a, double b) { return a - b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Mul:
                    return make_double_result([](double a, double b) { return a * b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Div:
                    return make_double_result([](double a, double b) { return a / b; }, lp, ls, rp,
                                              rs);
                case ir::ArithmeticOp::Mod:
                    return make_double_result([](double a, double b) { return std::fmod(a, b); },
                                              lp, ls, rp, rs);
            }
        }
        // Fallback: handles int-column inputs that need cast-to-double.
        Column<double> out;
        out.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row)
            out.push_back(apply_double_op(bin->op, get_double_value(*left, begin + row),
                                          get_double_value(*right, begin + row)));
        return ColumnValue{std::move(out)};
    }
    if (output_kind == ExprType::Int) {
        if (!left->is_column && !right->is_column) {
            std::int64_t value =
                apply_int_op(bin->op, get_int_value(*left, 0), get_int_value(*right, 0));
            Column<std::int64_t> out;
            out.assign(rows, value);
            return ColumnValue{std::move(out)};
        }
        const std::int64_t* lp = (left->is_column && left->kind == ExprType::Int)
                                     ? std::get<Column<std::int64_t>>(*left->column).data() + begin
                                     : nullptr;
        const std::int64_t* rp = (right->is_column && right->kind == ExprType::Int)
                                     ? std::get<Column<std::int64_t>>(*right->column).data() + begin
                                     : nullptr;
        std::int64_t ls = left->is_column ? 0 : get_int_value(*left, 0);
        std::int64_t rs = right->is_column ? 0 : get_int_value(*right, 0);
        const bool left_ok = !left->is_column || lp != nullptr;
        const bool right_ok = !right->is_column || rp != nullptr;
        if (left_ok && right_ok) {
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a + b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Sub:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a - b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Mul:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a * b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Div:
                    return make_int_result(
                        [](std::int64_t a, std::int64_t b) { return safe_idiv(a, b); }, lp, ls, rp,
                        rs);
                case ir::ArithmeticOp::Mod:
                    return make_int_result(
                        [](std::int64_t a, std::int64_t b) { return safe_imod(a, b); }, lp, ls, rp,
                        rs);
            }
        }
        Column<std::int64_t> out;
        out.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row)
            out.push_back(apply_int_op(bin->op, get_int_value(*left, begin + row),
                                       get_int_value(*right, begin + row)));
        return ColumnValue{std::move(out)};
    }
    return std::nullopt;
}

/// Write the simple arithmetic subset directly into a caller-owned dense
/// range.  `evaluate_field_maybe_parallel` owns one full output column already;
/// allocating a temporary column for every morsel and copying it into that
/// output throws away the zero-copy property that makes field-level splitting
/// worthwhile.  Keep this deliberately narrower than the fused tree below:
/// decline is cheap and retains the established evaluator for nested calls.
auto try_write_fast_update_binary(const ir::Expr& expr, const Table& input, RowRange range,
                                  ExprType output_kind, const ScalarRegistry* scalars,
                                  std::int64_t* dst_int, double* dst_double) -> bool {
    const auto kind = output_kind == ExprType::Int ? kernel::FixedWidthNumericKind::Int
                                                   : kernel::FixedWidthNumericKind::Double;
    const PredicateInput predicate_input(input);
    const auto direct = kernel::try_plan_direct_numeric_field(expr, predicate_input, scalars);
    if (direct.has_value() && direct->numeric_kind == kind &&
        kernel::write_direct_field_range(*direct, predicate_input, range, scalars,
                                         {.numeric = {.ints = dst_int, .doubles = dst_double}})) {
        return true;
    }
    const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node);
    if (bin == nullptr || (output_kind != ExprType::Int && output_kind != ExprType::Double)) {
        return false;
    }
    auto left = resolve_fast_operand(*bin->left, input, scalars);
    auto right = resolve_fast_operand(*bin->right, input, scalars);
    if (!left.has_value() || !right.has_value() || left->kind == ExprType::String ||
        right->kind == ExprType::String || left->kind == ExprType::Date ||
        right->kind == ExprType::Date || left->kind == ExprType::Timestamp ||
        right->kind == ExprType::Timestamp) {
        return false;
    }

    const std::size_t rows = range.count;
    const std::size_t begin = range.begin;
    if (output_kind == ExprType::Double) {
        if (dst_double == nullptr) {
            return false;
        }
        const double* lp = (left->is_column && left->kind == ExprType::Double)
                               ? std::get<Column<double>>(*left->column).data() + begin
                               : nullptr;
        const double* rp = (right->is_column && right->kind == ExprType::Double)
                               ? std::get<Column<double>>(*right->column).data() + begin
                               : nullptr;
        if ((!left->is_column || lp != nullptr) && (!right->is_column || rp != nullptr)) {
            const double ls = left->is_column ? 0.0 : get_double_value(*left, 0);
            const double rs = right->is_column ? 0.0 : get_double_value(*right, 0);
            auto write = [&](auto op_fn) {
                if (lp != nullptr && rp != nullptr) {
                    for (std::size_t i = 0; i < rows; ++i)
                        dst_double[i] = op_fn(lp[i], rp[i]);
                } else if (lp != nullptr) {
                    for (std::size_t i = 0; i < rows; ++i)
                        dst_double[i] = op_fn(lp[i], rs);
                } else if (rp != nullptr) {
                    for (std::size_t i = 0; i < rows; ++i)
                        dst_double[i] = op_fn(ls, rp[i]);
                } else {
                    for (std::size_t i = 0; i < rows; ++i)
                        dst_double[i] = op_fn(ls, rs);
                }
            };
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    write([](double a, double b) { return a + b; });
                    break;
                case ir::ArithmeticOp::Sub:
                    write([](double a, double b) { return a - b; });
                    break;
                case ir::ArithmeticOp::Mul:
                    write([](double a, double b) { return a * b; });
                    break;
                case ir::ArithmeticOp::Div:
                    write([](double a, double b) { return a / b; });
                    break;
                case ir::ArithmeticOp::Mod:
                    write([](double a, double b) { return std::fmod(a, b); });
                    break;
            }
            return true;
        }
        for (std::size_t row = 0; row < rows; ++row) {
            dst_double[row] = apply_double_op(bin->op, get_double_value(*left, begin + row),
                                              get_double_value(*right, begin + row));
        }
        return true;
    }

    if (dst_int == nullptr) {
        return false;
    }
    const std::int64_t* lp = (left->is_column && left->kind == ExprType::Int)
                                 ? std::get<Column<std::int64_t>>(*left->column).data() + begin
                                 : nullptr;
    const std::int64_t* rp = (right->is_column && right->kind == ExprType::Int)
                                 ? std::get<Column<std::int64_t>>(*right->column).data() + begin
                                 : nullptr;
    if ((!left->is_column || lp != nullptr) && (!right->is_column || rp != nullptr)) {
        const std::int64_t ls = left->is_column ? 0 : get_int_value(*left, 0);
        const std::int64_t rs = right->is_column ? 0 : get_int_value(*right, 0);
        auto write = [&](auto op_fn) {
            if (lp != nullptr && rp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    dst_int[i] = op_fn(lp[i], rp[i]);
            } else if (lp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    dst_int[i] = op_fn(lp[i], rs);
            } else if (rp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    dst_int[i] = op_fn(ls, rp[i]);
            } else {
                for (std::size_t i = 0; i < rows; ++i)
                    dst_int[i] = op_fn(ls, rs);
            }
        };
        switch (bin->op) {
            case ir::ArithmeticOp::Add:
                write([](std::int64_t a, std::int64_t b) { return a + b; });
                break;
            case ir::ArithmeticOp::Sub:
                write([](std::int64_t a, std::int64_t b) { return a - b; });
                break;
            case ir::ArithmeticOp::Mul:
                write([](std::int64_t a, std::int64_t b) { return a * b; });
                break;
            case ir::ArithmeticOp::Div:
                write([](std::int64_t a, std::int64_t b) { return safe_idiv(a, b); });
                break;
            case ir::ArithmeticOp::Mod:
                write([](std::int64_t a, std::int64_t b) { return safe_imod(a, b); });
                break;
        }
        return true;
    }
    for (std::size_t row = 0; row < rows; ++row) {
        dst_int[row] = apply_int_op(bin->op, get_int_value(*left, begin + row),
                                    get_int_value(*right, begin + row));
    }
    return true;
}

}  // namespace

// Pure double→double row-wise math builtins (sqrt/log/exp/trig + the
// type-preserving abs/floor/ceil/trunc when applied to a Double). Looked up by
// name so they can be compiled into the no-variant numeric fast path instead of
// the per-row scalar registry. Returns nullptr for names not in this set.
using UnaryDoubleFn = double (*)(double);
namespace {

auto lookup_unary_double_fn(std::string_view name) -> UnaryDoubleFn {
    static const robin_hood::unordered_map<std::string_view, UnaryDoubleFn> table = {
        {"sqrt", [](double x) { return std::sqrt(x); }},
        {"log", [](double x) { return std::log(x); }},
        {"exp", [](double x) { return std::exp(x); }},
        {"log2", [](double x) { return std::log2(x); }},
        {"log10", [](double x) { return std::log10(x); }},
        {"sin", [](double x) { return std::sin(x); }},
        {"cos", [](double x) { return std::cos(x); }},
        {"tan", [](double x) { return std::tan(x); }},
        {"asin", [](double x) { return std::asin(x); }},
        {"acos", [](double x) { return std::acos(x); }},
        {"atan", [](double x) { return std::atan(x); }},
        {"sinh", [](double x) { return std::sinh(x); }},
        {"cosh", [](double x) { return std::cosh(x); }},
        {"tanh", [](double x) { return std::tanh(x); }},
        {"abs", [](double x) { return std::fabs(x); }},
        {"floor", [](double x) { return std::floor(x); }},
        {"ceil", [](double x) { return std::ceil(x); }},
        {"trunc", [](double x) { return std::trunc(x); }},
    };
    auto it = table.find(name);
    return it == table.end() ? nullptr : it->second;
}

// abs/floor/ceil/trunc preserve the argument type (Int stays Int); only these
// may take an Int argument on the fast path. The transcendentals always widen
// to Double, so an Int argument is cast.
auto unary_fn_is_type_preserving(std::string_view name) -> bool {
    return name == "abs" || name == "floor" || name == "ceil" || name == "trunc";
}

struct NumericUpdateNode {
    enum class Kind : std::uint8_t {
        IntColumn,
        DoubleColumn,
        IntLiteral,
        DoubleLiteral,
        Op,
        Min,          ///< pmin(left, right) — element-wise minimum
        Max,          ///< pmax(left, right) — element-wise maximum
        UnaryDouble,  ///< dbl_fn(child) — row-wise double→double math (child = left)
        UnaryToInt,   ///< int_fn(child as double) — round(x, mode): Double → Int (child = left)
    };

    Kind kind = Kind::IntLiteral;
    ExprType type = ExprType::Int;
    ir::ArithmeticOp op = ir::ArithmeticOp::Add;
    std::uint32_t left = 0;
    std::uint32_t right = 0;
    const std::int64_t* i64 = nullptr;
    const double* dbl = nullptr;
    std::int64_t int_lit = 0;
    double dbl_lit = 0.0;
    UnaryDoubleFn dbl_fn = nullptr;
    std::int64_t (*int_fn)(double) = nullptr;
};

// round(x, mode) → Int64. mode is a bare identifier; resolve it to a kernel at
// compile time. Mirrors apply_round() exactly. Returns nullptr for bad modes.
auto lookup_round_int_fn(std::string_view mode) -> std::int64_t (*)(double) {
    if (mode == "nearest") {
        return [](double v) { return static_cast<std::int64_t>(std::llround(v)); };
    }
    if (mode == "bankers") {
        return [](double v) { return static_cast<std::int64_t>(std::llrint(v)); };
    }
    if (mode == "floor") {
        return [](double v) { return static_cast<std::int64_t>(std::floor(v)); };
    }
    if (mode == "ceil") {
        return [](double v) { return static_cast<std::int64_t>(std::ceil(v)); };
    }
    if (mode == "trunc") {
        return [](double v) { return static_cast<std::int64_t>(std::trunc(v)); };
    }
    return nullptr;
}

// Materialize `src` as a Column<int64_t> (want == Int) or Column<double>
// (want == Double), converting a bool/int/double source elementwise. Bool maps
// to 0/1, which is exactly what `Int64(like(...))` means. Returns nullopt for a
// source that is not a numeric-or-bool column.
auto to_numeric_column(const ColumnValue& src, ExprType want) -> std::optional<ColumnValue> {
    const auto build = [&](auto&& read, std::size_t n) -> ColumnValue {
        if (want == ExprType::Int) {
            Column<std::int64_t> out;
            out.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                out.push_back(static_cast<std::int64_t>(read(i)));
            }
            return ColumnValue{std::move(out)};
        }
        Column<double> out;
        out.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            out.push_back(static_cast<double>(read(i)));
        }
        return ColumnValue{std::move(out)};
    };
    if (const auto* c = std::get_if<Column<bool>>(&src)) {
        return build([&](std::size_t i) { return (*c)[i] ? 1 : 0; }, c->size());
    }
    if (const auto* c = std::get_if<Column<std::int64_t>>(&src)) {
        return build([&](std::size_t i) { return (*c)[i]; }, c->size());
    }
    if (const auto* c = std::get_if<Column<double>>(&src)) {
        return build([&](std::size_t i) { return (*c)[i]; }, c->size());
    }
    return std::nullopt;
}

// A sub-expression that is not natively numeric but produces a numeric column
// via a column KERNEL -- most usefully `Int64(like(col, "pat"))`, whose pattern
// compiles once instead of per row. Evaluate it once here and splice it in as an
// Int/DoubleColumn leaf so the surrounding arithmetic still vectorizes, instead
// of dropping the whole enclosing expression to the per-row evaluator.
//
// Recursion-safe by construction: the only kernels reached
// (`use_column_kernel`) take column/literal args, so this never re-enters the
// numeric compiler on the same node. The materialized column is owned by
// `temps`, whose heap buffers outlive the block evaluation (moving a Column on a
// `temps` reallocation preserves its data pointer).
// Splice counter. File-local in this host TU on purpose — see the declaration
// in interpreter_internal.hpp for why it must not be an inline header variable.
std::atomic<std::uint64_t> g_column_kernel_splices{0};

auto try_splice_column_leaf(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars,
                            std::vector<NumericUpdateNode>& nodes, std::vector<ColumnValue>& temps,
                            RowRange range) -> std::optional<std::uint32_t> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || !call->named_args.empty()) {
        return std::nullopt;
    }
    // The spliced kernel (`like`, a numeric cast) runs over the WHOLE table, so
    // under a partial range this leaf would re-evaluate the entire column once
    // per morsel — the O(morsels x rows) shape that has bitten this path
    // before. Decline instead; the caller falls back to the per-row loop, which
    // is linear in the range.
    if (!range.is_whole(input.rows())) {
        return std::nullopt;
    }

    // Either `Cast(kernel_call(...))` or a bare `kernel_call(...)` whose result
    // is already numeric. The cast fixes the leaf type; otherwise the kernel's
    // own result type does.
    const ir::CallExpr* kernel_call = call;
    std::optional<ExprType> want;
    if ((call->callee == "Int" || call->callee == "Int64") && call->args.size() == 1) {
        want = ExprType::Int;
        kernel_call = std::get_if<ir::CallExpr>(&call->args[0]->node);
    } else if (call->callee == "Float64" && call->args.size() == 1) {
        want = ExprType::Double;
        kernel_call = std::get_if<ir::CallExpr>(&call->args[0]->node);
    }
    if (kernel_call == nullptr) {
        return std::nullopt;
    }

    const BuiltinFn* fn = find_builtin(kernel_call->callee);
    if (fn == nullptr || !use_column_kernel(*fn, *kernel_call)) {
        return std::nullopt;
    }
    // Splice a kernel only if its result validity is exactly its inputs'
    // validity. The numeric fast path recomputes the field's validity with
    // `collect_expr_validity`, which ANDs the input columns' masks; a kernel that
    // *changes* nulls would then be re-nulled on the rows it just resolved. The
    // null-handling kernels (fill_null/coalesce/float_clean) are exactly those.
    // A `switch` with no `default` makes a newly added kernel a compile error
    // here -- it must state its validity behaviour rather than default into the
    // fast (and possibly wrong) path.
    const auto preserves_input_validity = [](ScalarKernel kernel) {
        switch (kernel) {
            case ScalarKernel::Like:
            case ScalarKernel::StringLength:
            case ScalarKernel::NumericCast:
                return true;
            case ScalarKernel::FillNull:
            case ScalarKernel::Coalesce:
            case ScalarKernel::FloatClean:
            case ScalarKernel::None:
                return false;
        }
        // Unreachable: the switch is exhaustive (a new ScalarKernel is a
        // -Wswitch compile error above). Loud rather than a silent `false`.
        invariant_violation("preserves_input_validity: unhandled ScalarKernel");
    };
    if (!preserves_input_validity(fn->scalar_kernel)) {
        return std::nullopt;
    }
    const ColumnEvalFn kernel = column_eval_of(*fn);
    if (kernel == nullptr) {
        return std::nullopt;
    }
    const ColumnEvalCtx ctx{.scalars = scalars, .externs = nullptr, .window = std::nullopt};
    auto col = kernel(*kernel_call, input, input.rows(), ctx);
    if (!col.has_value()) {
        return std::nullopt;
    }
    // No cast wrapper: keep the kernel's own numeric type; a bool result with no
    // cast is not a numeric leaf, so decline (the per-row path handles it).
    if (!want.has_value()) {
        if (std::holds_alternative<Column<std::int64_t>>(col->column)) {
            want = ExprType::Int;
        } else if (std::holds_alternative<Column<double>>(col->column)) {
            want = ExprType::Double;
        } else {
            return std::nullopt;
        }
    }

    auto numeric = to_numeric_column(col->column, *want);
    if (!numeric.has_value()) {
        return std::nullopt;
    }
    temps.push_back(std::move(*numeric));

    NumericUpdateNode node;
    node.type = *want;
    if (*want == ExprType::Int) {
        node.kind = NumericUpdateNode::Kind::IntColumn;
        node.i64 = std::get<Column<std::int64_t>>(temps.back()).data();
    } else {
        node.kind = NumericUpdateNode::Kind::DoubleColumn;
        node.dbl = std::get<Column<double>>(temps.back()).data();
    }
    nodes.push_back(node);
    g_column_kernel_splices.fetch_add(1, std::memory_order_relaxed);
    return static_cast<std::uint32_t>(nodes.size() - 1);
}

auto try_compile_numeric_update_expr(const ir::Expr& expr, const Table& input,
                                     const ScalarRegistry* scalars,
                                     std::vector<NumericUpdateNode>& nodes,
                                     std::vector<ColumnValue>& temps, RowRange range)
    -> std::optional<std::uint32_t> {
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left =
            try_compile_numeric_update_expr(*bin->left, input, scalars, nodes, temps, range);
        if (!left.has_value()) {
            return std::nullopt;
        }
        auto right =
            try_compile_numeric_update_expr(*bin->right, input, scalars, nodes, temps, range);
        if (!right.has_value()) {
            return std::nullopt;
        }

        const auto left_type = nodes[*left].type;
        const auto right_type = nodes[*right].type;
        NumericUpdateNode node;
        node.kind = NumericUpdateNode::Kind::Op;
        node.op = bin->op;
        node.left = *left;
        node.right = *right;
        node.type = bin->op == ir::ArithmeticOp::Div || left_type == ExprType::Double ||
                            right_type == ExprType::Double
                        ? ExprType::Double
                        : ExprType::Int;
        nodes.push_back(node);
        return static_cast<std::uint32_t>(nodes.size() - 1);
    }

    // round(x, mode) → Int: compile the value child and wrap in a UnaryToInt
    // node whose kernel is fixed by the (compile-time) mode identifier.
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if (call->callee == "round" && call->args.size() == 2 && call->named_args.empty()) {
            const auto* moderef = std::get_if<ir::ColumnRef>(&call->args[1]->node);
            auto kern = moderef != nullptr ? lookup_round_int_fn(moderef->name) : nullptr;
            if (kern == nullptr) {
                return std::nullopt;
            }
            auto child = try_compile_numeric_update_expr(*call->args[0], input, scalars, nodes,
                                                         temps, range);
            if (!child.has_value()) {
                return std::nullopt;
            }
            NumericUpdateNode node;
            node.kind = NumericUpdateNode::Kind::UnaryToInt;
            node.type = ExprType::Int;
            node.left = *child;
            node.int_fn = kern;
            nodes.push_back(node);
            return static_cast<std::uint32_t>(nodes.size() - 1);
        }
    }

    // pmin / pmax over numeric args: fold the (variadic) argument list into a
    // left-associative chain of Min/Max nodes so the elementwise clip lands on
    // this no-variant fast path instead of the per-row scalar-registry loop.
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        const bool is_min = call->callee == "pmin";
        const bool is_max = call->callee == "pmax";
        if ((is_min || is_max) && call->args.size() >= 2 && call->named_args.empty()) {
            std::optional<std::uint32_t> acc;
            for (const auto& arg : call->args) {
                auto a = try_compile_numeric_update_expr(*arg, input, scalars, nodes, temps, range);
                if (!a.has_value()) {
                    return std::nullopt;  // non-numeric arg (string/date/…): slow path
                }
                if (!acc.has_value()) {
                    acc = a;
                    continue;
                }
                NumericUpdateNode node;
                node.kind = is_min ? NumericUpdateNode::Kind::Min : NumericUpdateNode::Kind::Max;
                node.left = *acc;
                node.right = *a;
                node.type =
                    (nodes[*acc].type == ExprType::Double || nodes[*a].type == ExprType::Double)
                        ? ExprType::Double
                        : ExprType::Int;
                nodes.push_back(node);
                acc = static_cast<std::uint32_t>(nodes.size() - 1);
            }
            return acc;
        }
        // Unary double→double math (sqrt/log/exp/…, and abs/floor/ceil/trunc on
        // a Double argument): compile the child, wrap in a UnaryDouble node.
        if (call->args.size() == 1 && call->named_args.empty()) {
            if (auto fn = lookup_unary_double_fn(call->callee)) {
                auto child = try_compile_numeric_update_expr(*call->args[0], input, scalars, nodes,
                                                             temps, range);
                if (!child.has_value()) {
                    return std::nullopt;
                }
                // abs/floor/ceil/trunc keep an Int argument Int — leave those to
                // the generic path (this fast node always yields Double).
                if (unary_fn_is_type_preserving(call->callee) &&
                    nodes[*child].type != ExprType::Double) {
                    return std::nullopt;
                }
                NumericUpdateNode node;
                node.kind = NumericUpdateNode::Kind::UnaryDouble;
                node.type = ExprType::Double;
                node.left = *child;
                node.dbl_fn = fn;
                nodes.push_back(node);
                return static_cast<std::uint32_t>(nodes.size() - 1);
            }
        }
        // A column-kernel call (e.g. `Int64(like(col, "pat"))`): evaluate it once
        // into a numeric column leaf rather than dropping the whole enclosing
        // expression to the per-row evaluator.
        return try_splice_column_leaf(expr, input, scalars, nodes, temps, range);
    }

    auto operand = resolve_fast_operand(expr, input, scalars);
    if (!operand.has_value()) {
        return std::nullopt;
    }
    if (operand->kind != ExprType::Int && operand->kind != ExprType::Double) {
        return std::nullopt;
    }

    NumericUpdateNode node;
    node.type = operand->kind;
    if (operand->is_column) {
        if (operand->kind == ExprType::Int) {
            node.kind = NumericUpdateNode::Kind::IntColumn;
            // Leaves are read as `node.i64[block_offset + i]` with a block
            // offset relative to the range, so advancing the base pointer here
            // is all the range costs the tree.
            node.i64 = std::get<Column<std::int64_t>>(*operand->column).data() + range.begin;
        } else {
            node.kind = NumericUpdateNode::Kind::DoubleColumn;
            node.dbl = std::get<Column<double>>(*operand->column).data() + range.begin;
        }
    } else {
        if (operand->kind == ExprType::Int) {
            node.kind = NumericUpdateNode::Kind::IntLiteral;
            node.int_lit = get_int_value(*operand, 0);
        } else {
            node.kind = NumericUpdateNode::Kind::DoubleLiteral;
            node.dbl_lit = get_double_value(*operand, 0);
        }
    }

    nodes.push_back(node);
    return static_cast<std::uint32_t>(nodes.size() - 1);
}

constexpr std::size_t kNumericUpdateBlockRows = 256;
constexpr std::uint8_t kNumericEvalInt = 1U;
constexpr std::uint8_t kNumericEvalDouble = 2U;

// NumericUpdateNode is post-order. Evaluate it in small blocks so operator
// dispatch happens per node/block and each inner loop can auto-vectorize.
template <typename T>
struct NumericBlockValue {
    const T* data = nullptr;  // nullptr means broadcast `scalar`.
    T scalar{};
};

auto mark_numeric_double_subtree(const std::vector<NumericUpdateNode>& nodes, std::uint32_t idx,
                                 std::vector<std::uint8_t>& modes) -> void {
    if ((modes[idx] & kNumericEvalDouble) != 0U) {
        return;
    }
    modes[idx] |= kNumericEvalDouble;
    const auto& node = nodes[idx];
    switch (node.kind) {
        case NumericUpdateNode::Kind::Op:
        case NumericUpdateNode::Kind::Min:
        case NumericUpdateNode::Kind::Max:
            mark_numeric_double_subtree(nodes, node.left, modes);
            mark_numeric_double_subtree(nodes, node.right, modes);
            break;
        case NumericUpdateNode::Kind::UnaryDouble:
        case NumericUpdateNode::Kind::UnaryToInt:
            mark_numeric_double_subtree(nodes, node.left, modes);
            break;
        case NumericUpdateNode::Kind::IntColumn:
        case NumericUpdateNode::Kind::DoubleColumn:
        case NumericUpdateNode::Kind::IntLiteral:
        case NumericUpdateNode::Kind::DoubleLiteral:
            break;
    }
}

auto mark_numeric_int_subtree(const std::vector<NumericUpdateNode>& nodes, std::uint32_t idx,
                              std::vector<std::uint8_t>& modes) -> void {
    if ((modes[idx] & kNumericEvalInt) != 0U) {
        return;
    }
    modes[idx] |= kNumericEvalInt;
    const auto& node = nodes[idx];
    switch (node.kind) {
        case NumericUpdateNode::Kind::Op:
        case NumericUpdateNode::Kind::Min:
        case NumericUpdateNode::Kind::Max:
            mark_numeric_int_subtree(nodes, node.left, modes);
            mark_numeric_int_subtree(nodes, node.right, modes);
            break;
        case NumericUpdateNode::Kind::UnaryToInt:
            // round-like nodes evaluate their input with the Double semantics of
            // the old recursive evaluator, even when the child is Int-typed.
            mark_numeric_double_subtree(nodes, node.left, modes);
            break;
        case NumericUpdateNode::Kind::IntColumn:
        case NumericUpdateNode::Kind::IntLiteral:
            break;
        case NumericUpdateNode::Kind::DoubleColumn:
        case NumericUpdateNode::Kind::DoubleLiteral:
        case NumericUpdateNode::Kind::UnaryDouble:
            invariant_violation("mark_numeric_int_subtree: unexpected double node");
    }
}

template <typename T, typename Fn>
auto eval_numeric_binary_block(T* dst, NumericBlockValue<T> lhs, NumericBlockValue<T> rhs,
                               std::size_t count, Fn&& fn) -> void {
    if (lhs.data != nullptr && rhs.data != nullptr) {
        for (std::size_t i = 0; i < count; ++i) {
            dst[i] = fn(lhs.data[i], rhs.data[i]);
        }
    } else if (lhs.data != nullptr) {
        for (std::size_t i = 0; i < count; ++i) {
            dst[i] = fn(lhs.data[i], rhs.scalar);
        }
    } else if (rhs.data != nullptr) {
        for (std::size_t i = 0; i < count; ++i) {
            dst[i] = fn(lhs.scalar, rhs.data[i]);
        }
    } else {
        std::fill_n(dst, count, fn(lhs.scalar, rhs.scalar));
    }
}

auto eval_numeric_double_node_block(const NumericUpdateNode& node, std::uint32_t idx,
                                    std::vector<NumericBlockValue<double>>& values, double* dst,
                                    std::size_t row_offset, std::size_t count) -> void {
    auto& value = values[idx];
    switch (node.kind) {
        case NumericUpdateNode::Kind::IntColumn:
            for (std::size_t i = 0; i < count; ++i) {
                dst[i] = static_cast<double>(node.i64[row_offset + i]);
            }
            value = NumericBlockValue<double>{.data = dst};
            return;
        case NumericUpdateNode::Kind::DoubleColumn:
            value = NumericBlockValue<double>{.data = node.dbl + row_offset};
            return;
        case NumericUpdateNode::Kind::IntLiteral:
            value = NumericBlockValue<double>{.scalar = static_cast<double>(node.int_lit)};
            return;
        case NumericUpdateNode::Kind::DoubleLiteral:
            value = NumericBlockValue<double>{.scalar = node.dbl_lit};
            return;
        case NumericUpdateNode::Kind::Op: {
            const auto lhs = values[node.left];
            const auto rhs = values[node.right];
            value = NumericBlockValue<double>{.data = dst};
            switch (node.op) {
                case ir::ArithmeticOp::Add:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](double a, double b) { return a + b; });
                    return;
                case ir::ArithmeticOp::Sub:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](double a, double b) { return a - b; });
                    return;
                case ir::ArithmeticOp::Mul:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](double a, double b) { return a * b; });
                    return;
                case ir::ArithmeticOp::Div:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](double a, double b) { return a / b; });
                    return;
                case ir::ArithmeticOp::Mod:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](double a, double b) { return std::fmod(a, b); });
                    return;
            }
            invariant_violation("eval_numeric_double_node_block: unknown arithmetic op");
        }
        case NumericUpdateNode::Kind::Min:
            eval_numeric_binary_block(dst, values[node.left], values[node.right], count,
                                      [](double a, double b) { return b < a ? b : a; });
            value = NumericBlockValue<double>{.data = dst};
            return;
        case NumericUpdateNode::Kind::Max:
            eval_numeric_binary_block(dst, values[node.left], values[node.right], count,
                                      [](double a, double b) { return b > a ? b : a; });
            value = NumericBlockValue<double>{.data = dst};
            return;
        case NumericUpdateNode::Kind::UnaryDouble: {
            const auto src = values[node.left];
            if (src.data == nullptr) {
                value = NumericBlockValue<double>{.scalar = node.dbl_fn(src.scalar)};
            } else {
                for (std::size_t i = 0; i < count; ++i) {
                    dst[i] = node.dbl_fn(src.data[i]);
                }
                value = NumericBlockValue<double>{.data = dst};
            }
            return;
        }
        case NumericUpdateNode::Kind::UnaryToInt: {
            const auto src = values[node.left];
            if (src.data == nullptr) {
                value = NumericBlockValue<double>{.scalar =
                                                      static_cast<double>(node.int_fn(src.scalar))};
            } else {
                for (std::size_t i = 0; i < count; ++i) {
                    dst[i] = static_cast<double>(node.int_fn(src.data[i]));
                }
                value = NumericBlockValue<double>{.data = dst};
            }
            return;
        }
    }
    invariant_violation("eval_numeric_double_node_block: unknown node kind");
}

auto eval_numeric_int_node_block(const NumericUpdateNode& node, std::uint32_t idx,
                                 std::vector<NumericBlockValue<std::int64_t>>& int_values,
                                 const std::vector<NumericBlockValue<double>>& double_values,
                                 std::int64_t* dst, std::size_t row_offset, std::size_t count)
    -> void {
    auto& value = int_values[idx];
    switch (node.kind) {
        case NumericUpdateNode::Kind::IntColumn:
            value = NumericBlockValue<std::int64_t>{.data = node.i64 + row_offset};
            return;
        case NumericUpdateNode::Kind::IntLiteral:
            value = NumericBlockValue<std::int64_t>{.scalar = node.int_lit};
            return;
        case NumericUpdateNode::Kind::Op: {
            const auto lhs = int_values[node.left];
            const auto rhs = int_values[node.right];
            value = NumericBlockValue<std::int64_t>{.data = dst};
            switch (node.op) {
                case ir::ArithmeticOp::Add:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](std::int64_t a, std::int64_t b) { return a + b; });
                    return;
                case ir::ArithmeticOp::Sub:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](std::int64_t a, std::int64_t b) { return a - b; });
                    return;
                case ir::ArithmeticOp::Mul:
                    eval_numeric_binary_block(dst, lhs, rhs, count,
                                              [](std::int64_t a, std::int64_t b) { return a * b; });
                    return;
                case ir::ArithmeticOp::Div:
                    eval_numeric_binary_block(
                        dst, lhs, rhs, count,
                        [](std::int64_t a, std::int64_t b) { return safe_idiv(a, b); });
                    return;
                case ir::ArithmeticOp::Mod:
                    eval_numeric_binary_block(
                        dst, lhs, rhs, count,
                        [](std::int64_t a, std::int64_t b) { return safe_imod(a, b); });
                    return;
            }
            invariant_violation("eval_numeric_int_node_block: unknown arithmetic op");
        }
        case NumericUpdateNode::Kind::Min:
            eval_numeric_binary_block(dst, int_values[node.left], int_values[node.right], count,
                                      [](std::int64_t a, std::int64_t b) { return b < a ? b : a; });
            value = NumericBlockValue<std::int64_t>{.data = dst};
            return;
        case NumericUpdateNode::Kind::Max:
            eval_numeric_binary_block(dst, int_values[node.left], int_values[node.right], count,
                                      [](std::int64_t a, std::int64_t b) { return b > a ? b : a; });
            value = NumericBlockValue<std::int64_t>{.data = dst};
            return;
        case NumericUpdateNode::Kind::UnaryToInt: {
            const auto src = double_values[node.left];
            if (src.data == nullptr) {
                value = NumericBlockValue<std::int64_t>{.scalar = node.int_fn(src.scalar)};
            } else {
                for (std::size_t i = 0; i < count; ++i) {
                    dst[i] = node.int_fn(src.data[i]);
                }
                value = NumericBlockValue<std::int64_t>{.data = dst};
            }
            return;
        }
        case NumericUpdateNode::Kind::DoubleColumn:
        case NumericUpdateNode::Kind::DoubleLiteral:
        case NumericUpdateNode::Kind::UnaryDouble:
            invariant_violation("eval_numeric_int_node_block: unexpected double node");
    }
    invariant_violation("eval_numeric_int_node_block: unknown node kind");
}

/// Evaluate a compiled numeric tree into one caller-owned dense range.  The
/// same block/scratch schedule serves both the ordinary materialising evaluator
/// and parallel update windows; only ownership of the root column differs.
auto eval_numeric_update_blocks_into(const std::vector<NumericUpdateNode>& nodes,
                                     std::uint32_t root, std::size_t rows, ExprType output_kind,
                                     std::int64_t* out_int, double* out_double) -> void {
    if (rows == 0) {
        // An empty range is a legitimate shape, not a missing destination: a
        // filter that selects nothing, or a zero-row morsel. `Column<T>::
        // resize_for_overwrite(0)` leaves `data()` null, so the output-pointer
        // checks below would report an absent destination for a write that has
        // nothing to write. Returning here also skips the scratch allocation,
        // which no block would have used. Before this function was split out of
        // `eval_numeric_update_blocks` the empty case fell out of the block loop
        // never running; the split turned it into an abort.
        return;
    }
    std::vector<std::uint8_t> modes(nodes.size(), 0U);
    if (output_kind == ExprType::Double) {
        mark_numeric_double_subtree(nodes, root, modes);
    } else {
        mark_numeric_int_subtree(nodes, root, modes);
    }

    const bool needs_double = std::ranges::any_of(
        modes, [](std::uint8_t mode) { return (mode & kNumericEvalDouble) != 0U; });
    const bool needs_int = std::ranges::any_of(
        modes, [](std::uint8_t mode) { return (mode & kNumericEvalInt) != 0U; });
    std::vector<double> double_scratch(needs_double ? nodes.size() * kNumericUpdateBlockRows : 0U);
    std::vector<std::int64_t> int_scratch(needs_int ? nodes.size() * kNumericUpdateBlockRows : 0U);
    std::vector<NumericBlockValue<double>> double_values(nodes.size());
    std::vector<NumericBlockValue<std::int64_t>> int_values(nodes.size());

    if (output_kind == ExprType::Double) {
        if (out_double == nullptr) {
            invariant_violation("eval_numeric_update_blocks_into: missing double output");
        }
        for (std::size_t offset = 0; offset < rows; offset += kNumericUpdateBlockRows) {
            const std::size_t count = std::min(kNumericUpdateBlockRows, rows - offset);
            for (std::uint32_t idx = 0; idx <= root; ++idx) {
                if ((modes[idx] & kNumericEvalDouble) == 0U) {
                    continue;
                }
                double* dst = idx == root ? out_double + offset
                                          : double_scratch.data() + (static_cast<std::size_t>(idx) *
                                                                     kNumericUpdateBlockRows);
                eval_numeric_double_node_block(nodes[idx], idx, double_values, dst, offset, count);
            }
            const auto root_value = double_values[root];
            double* out_block = out_double + offset;
            if (root_value.data == nullptr) {
                std::fill_n(out_block, count, root_value.scalar);
            } else if (root_value.data != out_block) {
                std::copy_n(root_value.data, count, out_block);
            }
        }
        return;
    }

    if (out_int == nullptr) {
        invariant_violation("eval_numeric_update_blocks_into: missing int output");
    }
    for (std::size_t offset = 0; offset < rows; offset += kNumericUpdateBlockRows) {
        const std::size_t count = std::min(kNumericUpdateBlockRows, rows - offset);
        for (std::uint32_t idx = 0; idx <= root; ++idx) {
            if ((modes[idx] & kNumericEvalDouble) != 0U) {
                double* dst = double_scratch.data() +
                              (static_cast<std::size_t>(idx) * kNumericUpdateBlockRows);
                eval_numeric_double_node_block(nodes[idx], idx, double_values, dst, offset, count);
            }
            if ((modes[idx] & kNumericEvalInt) != 0U) {
                std::int64_t* dst =
                    idx == root ? out_int + offset
                                : int_scratch.data() +
                                      (static_cast<std::size_t>(idx) * kNumericUpdateBlockRows);
                eval_numeric_int_node_block(nodes[idx], idx, int_values, double_values, dst, offset,
                                            count);
            }
        }
        const auto root_value = int_values[root];
        std::int64_t* out_block = out_int + offset;
        if (root_value.data == nullptr) {
            std::fill_n(out_block, count, root_value.scalar);
        } else if (root_value.data != out_block) {
            std::copy_n(root_value.data, count, out_block);
        }
    }
}

auto eval_numeric_update_blocks(const std::vector<NumericUpdateNode>& nodes, std::uint32_t root,
                                std::size_t rows, ExprType output_kind) -> ColumnValue {
    if (output_kind == ExprType::Double) {
        Column<double> out;
        out.resize_for_overwrite(rows);
        eval_numeric_update_blocks_into(nodes, root, rows, output_kind, nullptr, out.data());
        return ColumnValue{std::move(out)};
    }
    Column<std::int64_t> out;
    out.resize_for_overwrite(rows);
    eval_numeric_update_blocks_into(nodes, root, rows, output_kind, out.data(), nullptr);
    return ColumnValue{std::move(out)};
}

// SIMD fast path for the hot 2-argument `pmin`/`pmax` shape (e.g. the
// winsorising clip `pmin(price, 500.0)`), mirroring try_fast_update_binary's
// branch-free array kernels. Only fires when both operands resolve to a raw
// pointer/scalar of the *same* numeric category (both Double or both Int) — a
// mixed int/double clip or a nested pmin(pmax(...)) falls through to the generic
// blocked NumericUpdateNode evaluator. The compare order (`b < a ? b : a`) matches the
// scalar pmin builtin for NaN/tie parity.
auto try_fast_update_pminmax(const ir::Expr& expr, const Table& input, RowRange range,
                             ExprType output_kind, const ScalarRegistry* scalars)
    -> std::optional<ColumnValue> {
    // `rows` is the output width; `begin` offsets every read of an input column
    // so the result is dense for the range.
    const std::size_t rows = range.count;
    const std::size_t begin = range.begin;
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr) {
        return std::nullopt;
    }
    const bool is_min = call->callee == "pmin";
    const bool is_max = call->callee == "pmax";
    if ((!is_min && !is_max) || call->args.size() != 2 || !call->named_args.empty()) {
        return std::nullopt;
    }
    auto left = resolve_fast_operand(*call->args[0], input, scalars);
    auto right = resolve_fast_operand(*call->args[1], input, scalars);
    if (!left || !right || left->kind != output_kind || right->kind != output_kind) {
        return std::nullopt;  // mixed int/double or non-numeric: generic path
    }

    auto run = [&](auto* dst, auto lp, auto ls, auto rp, auto rs) {
        using V = std::decay_t<decltype(ls)>;
        auto pick = [is_min](V a, V b) -> V {
            if (is_min) {
                return b < a ? b : a;
            }
            return b > a ? b : a;
        };
        if (lp && rp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = pick(lp[i], rp[i]);
        } else if (lp) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = pick(lp[i], rs);
        } else {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = pick(ls, rp[i]);
        }
    };

    if (output_kind == ExprType::Double) {
        const double* lp =
            left->is_column ? std::get<Column<double>>(*left->column).data() + begin : nullptr;
        const double* rp =
            right->is_column ? std::get<Column<double>>(*right->column).data() + begin : nullptr;
        Column<double> out;
        out.resize_for_overwrite(rows);
        run(out.data(), lp, left->is_column ? 0.0 : get_double_value(*left, 0), rp,
            right->is_column ? 0.0 : get_double_value(*right, 0));
        return ColumnValue{std::move(out)};
    }
    if (output_kind == ExprType::Int) {
        const std::int64_t* lp = left->is_column
                                     ? std::get<Column<std::int64_t>>(*left->column).data() + begin
                                     : nullptr;
        const std::int64_t* rp = right->is_column
                                     ? std::get<Column<std::int64_t>>(*right->column).data() + begin
                                     : nullptr;
        Column<std::int64_t> out;
        out.resize_for_overwrite(rows);
        run(out.data(), lp, left->is_column ? std::int64_t{0} : get_int_value(*left, 0), rp,
            right->is_column ? std::int64_t{0} : get_int_value(*right, 0));
        return ColumnValue{std::move(out)};
    }
    return std::nullopt;
}

}  // namespace

// Vectorised transcendentals via libmvec — the same mechanism zorro uses for the
// RNG normal/exponential paths. Fills dst[i] = fn(src[i]) in 4-wide AVX2 chunks +
// a scalar tail, ~5–10× the scalar libm tree-walk. Returns false (caller falls
// back to the scalar tree-walk) when the build lacks AVX2/libmvec or `name` has
// no kernel here, so non-x86/glibc targets stay correct.
#if defined(__AVX2__) && defined(ZORRO_USE_LIBMVEC)
// glibc AVX2 packed-double symbols (one arg). Every name below is also a
// registered scalar builtin, so the SIMD body and the scalar tail/fallback agree.
// NOLINTBEGIN(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp) — these
// are glibc's fixed vector-ABI symbol names; the spelling is not ours to choose.
extern "C" {

__m256d _ZGVdN4v_log2(__m256d) noexcept;
__m256d _ZGVdN4v_log10(__m256d) noexcept;
__m256d _ZGVdN4v_exp(__m256d) noexcept;
__m256d _ZGVdN4v_sin(__m256d) noexcept;
__m256d _ZGVdN4v_cos(__m256d) noexcept;
__m256d _ZGVdN4v_tan(__m256d) noexcept;
__m256d _ZGVdN4v_asin(__m256d) noexcept;
__m256d _ZGVdN4v_acos(__m256d) noexcept;
__m256d _ZGVdN4v_atan(__m256d) noexcept;
__m256d _ZGVdN4v_sinh(__m256d) noexcept;
__m256d _ZGVdN4v_cosh(__m256d) noexcept;
__m256d _ZGVdN4v_tanh(__m256d) noexcept;
}
// NOLINTEND(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
namespace {

struct SimdKernel {
    std::string_view name;
    __m256d (*vec)(__m256d) noexcept;  // 4-wide AVX2 body
    double (*scalar)(double);          // matching libm scalar for the tail
};

}  // namespace
// Non-capturing lambdas decay to function pointers; std::log etc. are overloaded,
// so wrap each to pin the double overload without an explicit cast per row.
const std::array<SimdKernel, 13> kSimdKernels = {
    {
        {.name = "log", .vec = _ZGVdN4v_log, .scalar = [](double x) { return std::log(x); }},
        {.name = "log2", .vec = _ZGVdN4v_log2, .scalar = [](double x) { return std::log2(x); }},
        {.name = "log10", .vec = _ZGVdN4v_log10, .scalar = [](double x) { return std::log10(x); }},
        {.name = "exp", .vec = _ZGVdN4v_exp, .scalar = [](double x) { return std::exp(x); }},
        {.name = "sin", .vec = _ZGVdN4v_sin, .scalar = [](double x) { return std::sin(x); }},
        {.name = "cos", .vec = _ZGVdN4v_cos, .scalar = [](double x) { return std::cos(x); }},
        {.name = "tan", .vec = _ZGVdN4v_tan, .scalar = [](double x) { return std::tan(x); }},
        {.name = "asin", .vec = _ZGVdN4v_asin, .scalar = [](double x) { return std::asin(x); }},
        {.name = "acos", .vec = _ZGVdN4v_acos, .scalar = [](double x) { return std::acos(x); }},
        {.name = "atan", .vec = _ZGVdN4v_atan, .scalar = [](double x) { return std::atan(x); }},
        {.name = "sinh", .vec = _ZGVdN4v_sinh, .scalar = [](double x) { return std::sinh(x); }},
        {.name = "cosh", .vec = _ZGVdN4v_cosh, .scalar = [](double x) { return std::cosh(x); }},
        {.name = "tanh", .vec = _ZGVdN4v_tanh, .scalar = [](double x) { return std::tanh(x); }},
    },
};
namespace {

auto find_simd_kernel(std::string_view name) -> const SimdKernel* {
    for (const auto& k : kSimdKernels) {
        if (k.name == name) {
            return &k;
        }
    }
    return nullptr;
}
auto simd_transcendental(std::string_view name, const double* src, double* dst, std::size_t n)
    -> bool {
    const SimdKernel* k = find_simd_kernel(name);
    if (k == nullptr) {
        return false;
    }
    std::size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        _mm256_storeu_pd(dst + i, k->vec(_mm256_loadu_pd(src + i)));
    }
    for (; i < n; ++i) {
        dst[i] = k->scalar(src[i]);
    }
    return true;
}
// True iff simd_transcendental has a kernel for `name` on this build — lets the
// unary fast path gate on it before materialising the argument.
auto simd_transcendental_supported(std::string_view name) -> bool {
    return find_simd_kernel(name) != nullptr;
}

}  // namespace
#else
auto simd_transcendental(std::string_view, const double*, double*, std::size_t) -> bool {
    return false;
}
auto simd_transcendental_supported(std::string_view) -> bool {
    return false;
}
#endif

// Packed IEEE sqrt over a column: vsqrtpd on AVX2 chunks + a scalar tail.
// std::sqrt sets errno on a negative argument, so without -fno-math-errno the
// auto-vectorizer keeps the f(column) loop on scalar vsqrtsd (+ an errno-domain
// branch) — ~2× slower than the packed form. Calling _mm256_sqrt_pd directly
// sidesteps that for the whole-column shape without flipping errno semantics
// TU-wide (which would pessimize the round→int64 loops). Bit-identical to
// std::sqrt for finite inputs; sqrt(<0) is NaN either way, only errno differs,
// which ibex never reads after a math builtin.
#ifdef __AVX2__
namespace {

void simd_sqrt(const double* src, double* dst, std::size_t n) noexcept {
    std::size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        _mm256_storeu_pd(dst + i, _mm256_sqrt_pd(_mm256_loadu_pd(src + i)));
    }
    for (; i < n; ++i) {
        dst[i] = std::sqrt(src[i]);
    }
}

}  // namespace
#else
void simd_sqrt(const double* src, double* dst, std::size_t n) noexcept {
    for (std::size_t i = 0; i < n; ++i) {
        dst[i] = std::sqrt(src[i]);
    }
}
#endif

// Forward declaration: try_fast_update_unary materialises a computed log/exp
// argument through the full numeric fast path, which is defined just below it.

namespace {

// SIMD fast path for the hot unary-over-column shapes: the auto-vectorisable
// double→double ops (sqrt/abs/floor/ceil/trunc) directly over a Double column,
// round(col, mode) → Int, and libmvec log/exp (which also accept a fast-
// computable argument, materialised first). Dispatching on the op outside the
// loop keeps each body a direct, inlinable kernel (vsqrtpd/vandpd/vroundpd) or a
// libmvec chunk. Remaining transcendentals (trig/hyperbolic) fall through to the
// scalar tree-walk.
auto try_fast_update_unary(const ir::Expr& expr, const Table& input, RowRange range,
                           ExprType output_kind, const ScalarRegistry* scalars)
    -> std::optional<ColumnValue> {
    // `rows` is the output width; `begin` offsets every read of an input column
    // so the result is dense for the range.
    const std::size_t rows = range.count;
    const std::size_t begin = range.begin;
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || !call->named_args.empty()) {
        return std::nullopt;
    }

    if (call->callee == "round" && call->args.size() == 2 && output_kind == ExprType::Int) {
        const auto* moderef = std::get_if<ir::ColumnRef>(&call->args[1]->node);
        auto arg = resolve_fast_operand(*call->args[0], input, scalars);
        if (moderef == nullptr || !arg || !arg->is_column || arg->kind != ExprType::Double) {
            return std::nullopt;
        }
        const double* src = std::get<Column<double>>(*arg->column).data() + begin;
        Column<std::int64_t> out;
        out.resize_for_overwrite(rows);
        std::int64_t* dst = out.data();
        auto run = [&](auto k) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = k(src[i]);
        };
        const auto& m = moderef->name;
        if (m == "nearest") {
            // Round half away from zero, branchless so the loop vectorises
            // (trunc/fabs/copysign → vroundpd/vandpd/vorpd) instead of calling
            // libm llround per element. Exact: frac = v - trunc(v) is computed
            // without error for |v| < 2^52, and |frac| >= 0.5 is the exact
            // away-from-zero test (avoids the floor(v+0.5) double-rounding bug at
            // v = nextafter(0.5, 0)). For |v| >= 2^52 v is already integral, frac
            // is 0, and the result is v — matching llround.
            run([](double v) {
                const double t = std::trunc(v);
                const double frac = v - t;
                return static_cast<std::int64_t>(std::fabs(frac) >= 0.5 ? t + std::copysign(1.0, v)
                                                                        : t);
            });
        } else if (m == "bankers") {
            run([](double v) { return static_cast<std::int64_t>(std::llrint(v)); });
        } else if (m == "floor") {
            run([](double v) { return static_cast<std::int64_t>(std::floor(v)); });
        } else if (m == "ceil") {
            run([](double v) { return static_cast<std::int64_t>(std::ceil(v)); });
        } else if (m == "trunc") {
            run([](double v) { return static_cast<std::int64_t>(std::trunc(v)); });
        } else {
            return std::nullopt;
        }
        return ColumnValue{std::move(out)};
    }

    // Transcendentals via libmvec: accept a bare Double column or any
    // fast-computable numeric argument (materialised to a temp column first).
    // simd_transcendental returns false for names it has no kernel for and for
    // builds lacking AVX2/libmvec, so the caller falls back to the scalar
    // tree-walk. (sqrt is handled by the dedicated vsqrtpd path below.)
    if (call->args.size() == 1 && output_kind == ExprType::Double &&
        simd_transcendental_supported(call->callee)) {
        auto arg = resolve_fast_operand(*call->args[0], input, scalars);
        ColumnValue owned;  // backing store if the argument is computed
        const double* src = nullptr;
        if (arg && arg->is_column && arg->kind == ExprType::Double) {
            src = std::get<Column<double>>(*arg->column).data() + begin;
        } else {
            // Ranged: the temp comes back dense for the range, so the
            // `owned` pointer below is read at offset 0, not `begin`.
            auto materialised = try_fast_update_numeric_expr(*call->args[0], input, range,
                                                             ExprType::Double, scalars);
            if (!materialised || !std::holds_alternative<Column<double>>(*materialised)) {
                return std::nullopt;  // non-double / non-fast arg: tree-walk
            }
            owned = std::move(*materialised);
            src = std::get<Column<double>>(owned).data();
        }
        Column<double> out;
        out.resize_for_overwrite(rows);
        if (!simd_transcendental(call->callee, src, out.data(), rows)) {
            return std::nullopt;  // no SIMD kernel on this target: tree-walk
        }
        return ColumnValue{std::move(out)};
    }

    if (call->args.size() == 1 && output_kind == ExprType::Double) {
        auto arg = resolve_fast_operand(*call->args[0], input, scalars);
        if (!arg || !arg->is_column || arg->kind != ExprType::Double) {
            return std::nullopt;
        }
        const double* src = std::get<Column<double>>(*arg->column).data() + begin;
        Column<double> out;
        out.resize_for_overwrite(rows);
        double* dst = out.data();
        auto run = [&](auto k) {
            for (std::size_t i = 0; i < rows; ++i)
                dst[i] = k(src[i]);
        };
        const auto& f = call->callee;
        if (f == "sqrt") {
            simd_sqrt(src, dst, rows);
        } else if (f == "abs") {
            run([](double x) { return std::fabs(x); });
        } else if (f == "floor") {
            run([](double x) { return std::floor(x); });
        } else if (f == "ceil") {
            run([](double x) { return std::ceil(x); });
        } else if (f == "trunc") {
            run([](double x) { return std::trunc(x); });
        } else {
            return std::nullopt;  // transcendentals: tree-walk (libm-bound anyway)
        }
        return ColumnValue{std::move(out)};
    }
    return std::nullopt;
}

}  // namespace

auto column_kernel_splice_count() -> std::uint64_t {
    return g_column_kernel_splices.load(std::memory_order_relaxed);
}

void reset_column_kernel_splice_count() {
    g_column_kernel_splices.store(0, std::memory_order_relaxed);
}

auto try_fast_update_numeric_expr(const ir::Expr& expr, const Table& input, RowRange range,
                                  ExprType output_kind, const ScalarRegistry* scalars)
    -> std::optional<ColumnValue> {
    const std::size_t rows = range.count;
    if (auto fast = try_fast_update_binary(expr, input, range, output_kind, scalars);
        fast.has_value()) {
        return fast;
    }
    if (auto fast = try_fast_update_pminmax(expr, input, range, output_kind, scalars);
        fast.has_value()) {
        return fast;
    }
    if (auto fast = try_fast_update_unary(expr, input, range, output_kind, scalars);
        fast.has_value()) {
        return fast;
    }
    if (output_kind != ExprType::Int && output_kind != ExprType::Double) {
        return std::nullopt;
    }

    std::vector<NumericUpdateNode> nodes;
    nodes.reserve(8);
    // Owns any columns materialized by `try_splice_column_leaf` (e.g. a compiled
    // `like`). Must outlive `eval_numeric_update_blocks`, which reads leaf data
    // pointers into these; reserved so pushes never reallocate mid-compile.
    std::vector<ColumnValue> temps;
    temps.reserve(8);
    auto root = try_compile_numeric_update_expr(expr, input, scalars, nodes, temps, range);
    if (!root.has_value() || nodes[*root].type != output_kind) {
        return std::nullopt;
    }

    return eval_numeric_update_blocks(nodes, *root, rows, output_kind);
}

/// The parallel field splitter owns a full destination column before it starts
/// its morsels.  Compile the same range-native numeric tree used by the serial
/// evaluator, but let its root write straight into this morsel's disjoint
/// output window.  A decline is intentional: callers keep the semantic
/// fallback for any expression the numeric compiler does not own.
auto try_write_compiled_numeric_update_expr(const ir::Expr& expr, const Table& input,
                                            RowRange range, ExprType output_kind,
                                            const ScalarRegistry* scalars, std::int64_t* dst_int,
                                            double* dst_double) -> bool {
    if ((output_kind != ExprType::Int && output_kind != ExprType::Double) ||
        (output_kind == ExprType::Int && dst_int == nullptr) ||
        (output_kind == ExprType::Double && dst_double == nullptr)) {
        return false;
    }
    std::vector<NumericUpdateNode> nodes;
    nodes.reserve(8);
    std::vector<ColumnValue> temps;
    temps.reserve(8);
    const auto root = try_compile_numeric_update_expr(expr, input, scalars, nodes, temps, range);
    if (!root.has_value() || nodes[*root].type != output_kind) {
        return false;
    }
    eval_numeric_update_blocks_into(nodes, *root, range.count, output_kind, dst_int, dst_double);
    return true;
}

namespace {

// Append a computed field column, carrying its validity bitmap when present.
auto add_computed_column(Table& table, const std::string& alias, ComputedColumn col) -> void {
    if (col.validity.has_value()) {
        table.add_column(alias, std::move(col.column), std::move(*col.validity));
    } else {
        table.add_column(alias, std::move(col.column));
    }
}

}  // namespace

// Like update_table but passes the window clause's duration to the shared
// field evaluator, so rolling aggregates without a per-call window use it.
// An order-dependent field (lag/lead/cum*/rolling_*/fill_*) reads rows other
// than its own, so it only means anything when the adjacent row is the one the
// author meant. A `by` clause upstream establishes that some keys partition the
// rows; after that, an UNPARTITIONED order-dependent call reads across a
// partition into another group's rows.
//
// Both grouped layouts are hazardous and the check does not distinguish them:
// `window` + `by` emits group-major runs (only each run's last row reads
// across), while `resample` + `by` emits interleaved rows (every row does).
//
// The producers record the keys; this consults them. Per-group evaluation
// builds fresh slices that inherit no grouping, so the correctly-partitioned
// case passes without this needing to know about `by` at all.
auto check_row_order(const Table& input, const std::vector<ir::FieldSpec>& fields)
    -> std::expected<void, std::string> {
    if (input.rows() < 2) {
        return {};
    }
    std::string fn;
    for (const auto& field : fields) {
        fn = ir::find_order_dependent_call(field.expr);
        if (!fn.empty()) {
            break;
        }
    }
    if (fn.empty()) {
        return {};
    }
    if (!input.grouped_by().empty()) {
        std::string keys;
        for (const auto& key : input.grouped_by()) {
            if (!keys.empty()) {
                keys += ", ";
            }
            keys += key;
        }
        return std::unexpected(
            fn + ": depends on the row order, but an upstream `by " + keys +
            "` grouped the rows, so the adjacent row can belong to a different group. Add `by " +
            keys + "` here so " + fn +
            " stops at the group edge, or `order` the table first to state the order you intend.");
    }
    return {};
}

auto windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                           ir::Duration duration, const ScalarRegistry* scalars,
                           const ExternRegistry* externs, const ExecutionContext& exec,
                           bool aligned) -> std::expected<Table, std::string> {
    Table output = std::move(input);
    const std::size_t rows = output.rows();
    if (!output.time_index().has_value()) {
        return std::unexpected("window: requires a TimeFrame");
    }
    if (auto ok = check_row_order(output, fields); !ok) {
        return std::unexpected(ok.error());
    }
    // Reject mutation of the time index column
    for (const auto& field : fields) {
        if (field.alias == *output.time_index()) {
            return std::unexpected("cannot update time index column: " + field.alias);
        }
    }
    for (const auto& field : fields) {
        if (std::holds_alternative<ir::RankExpr>(field.expr.node)) {
            return std::unexpected("rank(): not supported inside windowed update");
        }
        if (const auto* col_ref = std::get_if<ir::ColumnRef>(&field.expr.node)) {
            const auto* entry = col_ref->lexical ? nullptr : output.find_entry(col_ref->name);
            if (entry != nullptr) {
                // `alias = other_column` renames rather than computes, so the
                // two names can share one buffer under the copy-on-write
                // invariant. Deep-copying here moved 26MB per renamed key
                // column on a q03-shaped scan — the join-key alignment idiom
                // (`select { o_orderkey = l_orderkey }`) hits this on nearly
                // every query.
                output.add_column_from(field.alias, *entry);
                continue;
            }
            if (scalars != nullptr) {
                if (auto it = scalars->find(col_ref->name); it != scalars->end()) {
                    output.add_column(field.alias, broadcast_scalar_column(it->second, rows));
                    continue;
                }
            }
            return std::unexpected("unknown column '" + col_ref->name + "'");
        }
        // Shared field evaluator; ctx.window carries the enclosing `window`
        // clause's duration as the rolling_* fallback (a per-call window
        // overrides it).
        auto col = evaluate_field(field.expr, output, RowRange::whole(output.rows()),
                                  ColumnEvalCtx{.scalars = scalars,
                                                .externs = externs,
                                                .window = duration,
                                                .window_aligned = aligned,
                                                .exec = &exec});
        if (!col) {
            return std::unexpected(col.error());
        }
        add_computed_column(output, field.alias, std::move(*col));
    }
    normalize_time_index(output);
    return output;
}

/// Clear one bit of a shared validity bitmap.
///
/// Groups scatter to disjoint ROWS, but a bitmap packs 64 rows into a word, so
/// two groups can meet inside one word — and unlike a filter's output, the rows
/// a group owns are scattered rather than contiguous, so there is no "only the
/// two edge words are shared" structure to exploit here. Every write must be
/// atomic. It is sound because these writes only ever turn bits OFF: AND is
/// commutative and associative, so the interleaving cannot matter.
inline void clear_validity_bit(std::uint64_t* words, std::size_t bit) noexcept {
    const std::uint64_t mask = ~(std::uint64_t{1} << (bit % 64));
#ifdef __cpp_lib_atomic_ref
    std::atomic_ref<std::uint64_t>(words[bit / 64]).fetch_and(mask, std::memory_order_relaxed);
#else
    // Apple's libc++ (macOS clang-werror leg) doesn't ship std::atomic_ref yet.
    // std::atomic<uint64_t> is a lock-free integral specialization and therefore
    // layout-compatible with a plain uint64_t, so a reinterpret_cast view gives
    // the same fetch-AND without needing atomic_ref.
    reinterpret_cast<std::atomic<std::uint64_t>*>(&words[bit / 64])
        ->fetch_and(mask, std::memory_order_relaxed);
#endif
}

/// How many workers should split the per-row bucketing loop, or 0 for serial.
///
/// Unlike the group loop below, this does NOT cap at the group count: it splits
/// row ranges, so a two-symbol table parallelises as readily as a thousand-symbol
/// one. That matters because bucketing is the phase that does not care how many
/// groups there are — it is one hash and compare per ROW.
[[nodiscard]] auto bucketing_worker_count(const ExecutionContext& exec, std::size_t rows)
    -> std::size_t {
    if (on_worker_pool_thread() || !exec.can_fan_out() || rows < exec.parallel_min_rows) {
        return 0;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    // Enough rows per worker that the per-worker hash index is worth building.
    constexpr std::size_t kMinRowsPerWorker = 32768;
    const std::size_t workers =
        std::min({budget, pool_size, std::max<std::size_t>(rows / kMinRowsPerWorker, 1)});
    return workers < 2 ? 0 : workers;
}

/// How many workers should share a grouped windowed update's groups, or 0 to
/// run them serially.
///
/// Parallelism here is across GROUPS, not row ranges: each group's rolling
/// buffer must not cross a group boundary, so a group is the natural
/// indivisible unit. That also caps the speedup at the group count, which is
/// The columns a grouped update must scatter back into its full-size output.
///
/// A field introducing a NEW column is appended to the sub-result, so it sits
/// past `first_new_idx`. A field OVERWRITING an existing column mutates it in
/// place inside the slice instead, so it never appears there. Collecting only
/// the appended ones is why an overwrite used to be computed, made visible to
/// later fields in the same block, and then silently dropped from the result --
/// while the ungrouped paths applied it.
[[nodiscard]] auto written_field_names_for(const Table& first, std::size_t first_new_idx,
                                           const std::vector<ir::FieldSpec>& fields)
    -> std::vector<std::string> {
    std::vector<std::string> names;
    names.reserve(fields.size());
    for (std::size_t c = first_new_idx; c < first.columns.size(); ++c) {
        names.push_back(first.columns[c].name);
    }
    for (const auto& field : fields) {
        const bool appended = std::ranges::find(names, field.alias) != names.end();
        if (!appended && first.find(field.alias) != nullptr) {
            names.push_back(field.alias);
        }
    }
    return names;
}

/// why a two-symbol table gains nothing however many cores are free.
[[nodiscard]] auto grouped_window_worker_count(const ExecutionContext& exec,
                                               std::size_t remaining_groups, std::size_t rows,
                                               const Table& first_sub,
                                               const std::vector<std::string>& written_field_names,
                                               const std::vector<ir::FieldSpec>& fields)
    -> std::size_t {
    // `run_group` is reached from a worker only through this function, but the
    // pipeline executor can call the whole update from a pool thread; submitting
    // from there deadlocks the pool.
    if (on_worker_pool_thread() || !exec.can_fan_out() || remaining_groups < 2) {
        return 0;
    }
    if (rows < exec.parallel_min_rows) {
        return 0;
    }
    // An extern call or a Generator would not merely be slower out of order —
    // it would answer differently. See `is_group_parallel_safe_expr`.
    for (const auto& field : fields) {
        if (!ir::is_group_parallel_safe_expr(field.expr)) {
            return 0;
        }
    }
    // A bool output column is bit-packed, and a group's rows are scattered, so
    // two groups writing one word would lose bits. Validity has the same shape
    // but is monotone and handled atomically; a bool VALUE is not monotone, so
    // it is refused instead. Rolling/aggregate fields are numeric in practice.
    for (const auto& fname : written_field_names) {
        const auto* sample = first_sub.find(fname);
        if (sample != nullptr && std::holds_alternative<Column<bool>>(*sample)) {
            return 0;
        }
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t workers = std::min({budget, pool_size, remaining_groups});
    return workers < 2 ? 0 : workers;
}

/// Native reductions have no cross-group state: each worker can reduce and
/// scatter a complete CSR group independently. Validity lands later, after
/// workers have published which groups were all-null, so packed bitmap bytes
/// are never concurrently modified.
[[nodiscard]] auto grouped_reduction_worker_count(const ExecutionContext& exec,
                                                  std::size_t group_count, std::size_t rows)
    -> std::size_t {
    if (on_worker_pool_thread() || !exec.can_fan_out() || group_count < 2 ||
        rows < exec.parallel_min_rows) {
        return 0;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t workers = std::min({budget, pool_size, group_count});
    return workers < 2 ? 0 : workers;
}

/// Row indices bucketed by group, in CSR form.
///
/// CSR — "compressed sparse row", the sparse-matrix layout the name is
/// borrowed from — means two arrays instead of a container per group: one flat
/// buffer holding every element once, and an offsets array marking where each
/// group's run begins. Group `g` owns `flat[offsets[g] .. offsets[g + 1])`,
/// ascending, so `offsets` has one more entry than there are groups and the
/// last one is the total. Reading a group is a pointer and a length; there is
/// no per-group allocation and nothing to chase.
///
/// One flat buffer rather than a vector per group. The per-group vectors it
/// replaces cost an allocation each, and — because a parallel scatter must
/// write by index rather than append — could only be filled concurrently after
/// a `resize` that value-initialised every element. Writing 16MB of zeroes to
/// overwrite them immediately is what made the previous attempt at threading
/// this slower than the serial loop it replaced.
struct GroupedRows {
    std::unique_ptr<std::size_t[]> flat;
    std::vector<std::size_t> offsets;  // group_count + 1 entries

    [[nodiscard]] auto group_count() const noexcept -> std::size_t {
        return offsets.empty() ? 0 : offsets.size() - 1;
    }
    [[nodiscard]] auto operator[](std::size_t g) const noexcept -> std::span<const std::size_t> {
        return {flat.get() + offsets[g], offsets[g + 1] - offsets[g]};
    }
};

/// Assign every row a dense group id, writing them into `row_gid`, and return
/// the group count. Ids are in order of first appearance, at any thread count.
///
/// The key is hashed and compared IN PLACE against the candidate group's
/// stored key, so a `Key` is built once per GROUP rather than once per row.
/// Boxing one per row — a heap vector of ScalarValue, built only to probe the
/// index and then thrown away — measured ~60% of a grouped-window query's
/// runtime, dwarfing the window it exists to organize.
///
/// Shared by both grouped update paths. It lived inside the windowed one until
/// a profile showed the plain `by` update still boxing a key per row: 8M rows
/// of `rank/lag/cumsum by symbol` spent ~17% of the whole window suite in
/// KeyHash plus the string hashing under it, for a Categorical key whose codes
/// are already dense.
///
/// `row_gid` is left uninitialised by callers: every path below assigns all
/// `rows` entries before anything reads one, so value-initialising first would
/// be an 8MB memset at 2M rows whose every byte is overwritten. A path that
/// ever wrote only some rows would read garbage rather than a zero, so keep
/// them total.
[[nodiscard]] auto assign_group_ids(const std::vector<const ColumnValue*>& group_columns,
                                    const std::vector<const ValidityBitmap*>& group_validity,
                                    std::size_t rows, const ExecutionContext& exec,
                                    std::span<std::uint32_t> row_gid) -> std::size_t {
    // A single Categorical key needs no hashing at all. Its codes are already
    // dense dictionary indices, so resolve each CODE to a group id once — 252
    // of them for `by symbol` — and the per-row work becomes one array read.
    // The in-place key path below still hashes the dictionary string a row at
    // a time (`KeyCol::text` -> `hash_key_row` -> `_Hash_bytes`), which
    // measured ~24% of the window suite once the boxing above it was gone.
    //
    // Resolved through the dictionary text rather than by using the code as
    // the group id: nothing forbids two codes carrying the same string (a
    // remap can produce that), and those must land in one group.
    if (group_columns.size() == 1) {
        if (const auto* cat = std::get_if<Column<Categorical>>(group_columns[0])) {
            constexpr std::uint32_t kUnset = std::numeric_limits<std::uint32_t>::max();
            const ValidityBitmap* validity = group_validity[0];
            const auto& dict = cat->dictionary();
            std::vector<std::uint32_t> code_gid(dict.size(), kUnset);
            robin_hood::unordered_flat_map<std::string_view, std::uint32_t> by_text;
            std::uint32_t next_gid = 0;
            std::uint32_t null_gid = kUnset;  // nulls form one group of their own
            for (std::size_t r = 0; r < rows; ++r) {
                if (validity != nullptr && !(*validity)[r]) {
                    if (null_gid == kUnset) {
                        null_gid = next_gid++;
                    }
                    row_gid[r] = null_gid;
                    continue;
                }
                const auto code = static_cast<std::size_t>(cat->code_at(r));
                std::uint32_t gid = code_gid[code];
                if (gid == kUnset) {
                    // Filled lazily in row order, so ids stay in order of first
                    // appearance — the same numbering the paths below produce.
                    auto [it, inserted] = by_text.emplace(std::string_view{dict[code]}, next_gid);
                    if (inserted) {
                        ++next_gid;
                    }
                    gid = it->second;
                    code_gid[code] = gid;
                }
                row_gid[r] = gid;
            }
            return next_gid;
        }
    }

    // Bucket rows by group key — the row indices land in original
    // (time-sorted) order within each group, which is the precondition the
    // single-buffer rolling implementation relies on.
    //
    // The key is hashed and compared IN PLACE against the candidate group's
    // stored key, so a `Key` is built once per GROUP rather than once per row.
    // Boxing one per row — a heap vector of ScalarValue, built only to probe
    // the index and then thrown away — measured ~60% of a grouped-window
    // query's runtime, dwarfing the window it exists to organize. This is the
    // same treatment group-by and distinct already had.
    std::vector<KeyCol> key_cols;
    key_cols.reserve(group_columns.size());
    for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
        auto col = make_key_col(*group_columns[ci], group_validity[ci]);
        if (!col.has_value()) {
            key_cols.clear();  // a type the in-place path cannot resolve
            break;
        }
        key_cols.push_back(*col);
    }

    // Every grouping path below produces the same two things: a group id per
    // row, and a group count. The CSR bucketing that turns them into per-group
    // row lists is then shared.
    //
    // Left uninitialised: every path below assigns all `rows` entries before
    // anything reads one, so value-initialising first would be an 8MB memset at
    // 2M rows whose every byte is overwritten. A path that ever wrote only some
    // rows would read garbage here rather than a zero, so keep them total.
    std::size_t group_count = 0;
    if (key_cols.size() == group_columns.size()) {
        std::vector<Key> group_keys;

        auto make_key_at = [&](std::size_t r) {
            Key key;
            key.values.reserve(group_columns.size());
            for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
                push_key_value(key, *group_columns[ci], group_validity[ci], r);
            }
            return key;
        };

        const std::size_t bucket_workers = bucketing_worker_count(exec, rows);
        if (bucket_workers < 2) {
            KeyRowIndex index;
            for (std::size_t r = 0; r < rows; ++r) {
                row_gid[r] = index.find_or_insert(group_keys, key_cols, r, [&] {
                    group_keys.push_back(make_key_at(r));
                    return static_cast<std::uint32_t>(group_keys.size() - 1);
                });
            }
        } else {
            // Group each row range independently, then reconcile. Every worker
            // numbers groups from zero in its own range, so the reconcile step
            // maps local ids onto global ones — it is O(workers x groups),
            // never O(rows), which is what makes the split pay.
            //
            // Global ids are handed out by walking workers in row order and
            // each worker's groups in first-appearance order, so a group's id
            // is still "order of first appearance in the table" exactly as the
            // serial loop produces. Nothing downstream depends on that today,
            // but a numbering that changed with the thread count would be a
            // trap for whatever does next.
            const std::size_t span = (rows + bucket_workers - 1) / bucket_workers;
            struct Local {
                std::vector<Key> keys;
                std::vector<std::uint32_t> remap;  // local id -> global id
            };
            std::vector<Local> locals(bucket_workers);
            {
                auto batch =
                    process_worker_pool().submit(bucket_workers, [&](std::size_t w) noexcept {
                        const std::size_t lo = w * span;
                        const std::size_t hi = std::min(lo + span, rows);
                        if (lo >= hi) {
                            return;
                        }
                        KeyRowIndex index;
                        auto& local = locals[w];
                        for (std::size_t r = lo; r < hi; ++r) {
                            row_gid[r] = index.find_or_insert(local.keys, key_cols, r, [&] {
                                local.keys.push_back(make_key_at(r));
                                return static_cast<std::uint32_t>(local.keys.size() - 1);
                            });
                        }
                    });
                batch.wait();
            }

            robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> global_index;
            for (auto& local : locals) {
                local.remap.resize(local.keys.size());
                for (std::size_t l = 0; l < local.keys.size(); ++l) {
                    auto [it, inserted] = global_index.emplace(
                        local.keys[l], static_cast<std::uint32_t>(group_keys.size()));
                    if (inserted) {
                        group_keys.push_back(std::move(local.keys[l]));
                    }
                    local.remap[l] = it->second;
                }
            }

            {
                auto batch =
                    process_worker_pool().submit(bucket_workers, [&](std::size_t w) noexcept {
                        const std::size_t lo = w * span;
                        const std::size_t hi = std::min(lo + span, rows);
                        const auto& remap = locals[w].remap;
                        for (std::size_t r = lo; r < hi; ++r) {
                            row_gid[r] = remap[row_gid[r]];
                        }
                    });
                batch.wait();
            }
        }

        group_count = group_keys.size();
    } else {
        // Fallback for a key column `make_key_col` cannot resolve. No built-in
        // type reaches it today; it exists so an added column kind degrades to
        // the slower grouping rather than to a wrong answer.
        robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> group_index;
        for (std::size_t r = 0; r < rows; ++r) {
            Key key;
            key.values.reserve(group_columns.size());
            for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
                push_key_value(key, *group_columns[ci], group_validity[ci], r);
            }
            auto [it, inserted] =
                group_index.emplace(std::move(key), static_cast<std::uint32_t>(group_count));
            if (inserted) {
                ++group_count;
            }
            row_gid[r] = it->second;
        }
    }

    return group_count;
}

/// Bucket `row_gid` into CSR form.
///
/// Both passes — the histogram and the scatter — are split across workers when
/// the group count is small enough for a private histogram per worker. Each
/// worker owns a row range and writes only into the slice its own tally
/// reserved, so no two workers touch the same element and no atomics are
/// needed.
///
/// Group `g`'s region receives worker ranges in row order, so its row indices
/// come out ascending — the same order a serial append produces, and the order
/// the single-buffer rolling implementation requires. Getting this backwards
/// would not be slower, it would silently un-sort each group.
[[nodiscard]] auto build_grouped_rows(std::span<const std::uint32_t> row_gid,
                                      std::size_t group_count, const ExecutionContext& exec)
    -> GroupedRows {
    const std::size_t rows = row_gid.size();
    GroupedRows out;
    out.offsets.assign(group_count + 1, 0);
    out.flat = std::make_unique_for_overwrite<std::size_t[]>(rows);

    // The private histograms are a workers x groups matrix, so threading only
    // pays while that stays small. High cardinality keeps the serial pass,
    // which is also where it is least penalised: with many groups the
    // increments hit many different counters, and the dependency chain that
    // makes a few-group histogram slow breaks up on its own.
    constexpr std::size_t kMaxHistogramCells = 1U << 16U;
    std::size_t workers = bucketing_worker_count(exec, rows);
    if (workers >= 2 && workers * group_count > kMaxHistogramCells) {
        workers = 0;
    }

    if (workers < 2) {
        for (std::size_t r = 0; r < rows; ++r) {
            ++out.offsets[row_gid[r] + 1];
        }
        for (std::size_t g = 0; g < group_count; ++g) {
            out.offsets[g + 1] += out.offsets[g];
        }
        std::vector<std::size_t> cursor(out.offsets.begin(), out.offsets.end() - 1);
        for (std::size_t r = 0; r < rows; ++r) {
            out.flat[cursor[row_gid[r]]++] = r;
        }
        return out;
    }

    const std::size_t span = (rows + workers - 1) / workers;
    // Row-major by worker: the inner loop over rows touches one worker's row of
    // the matrix, so that is the axis worth keeping contiguous.
    std::vector<std::size_t> hist(workers * group_count, 0);
    auto& pool = process_worker_pool();
    {
        auto batch = pool.submit(workers, [&](std::size_t w) noexcept {
            const std::size_t lo = w * span;
            const std::size_t hi = std::min(lo + span, rows);
            std::size_t* tally = hist.data() + (w * group_count);
            for (std::size_t r = lo; r < hi; ++r) {
                ++tally[row_gid[r]];
            }
        });
        batch.wait();
    }

    // Exclusive prefix in (group, worker) order, turning each tally into that
    // worker's write cursor for that group. Groups are contiguous in `flat`,
    // and workers within a group are in row order.
    std::size_t running = 0;
    for (std::size_t g = 0; g < group_count; ++g) {
        out.offsets[g] = running;
        for (std::size_t w = 0; w < workers; ++w) {
            std::size_t& cell = hist[(w * group_count) + g];
            const std::size_t n = cell;
            cell = running;
            running += n;
        }
    }
    out.offsets[group_count] = running;

    {
        auto batch = pool.submit(workers, [&](std::size_t w) noexcept {
            const std::size_t lo = w * span;
            const std::size_t hi = std::min(lo + span, rows);
            std::size_t* cursor = hist.data() + (w * group_count);
            for (std::size_t r = lo; r < hi; ++r) {
                out.flat[cursor[row_gid[r]]++] = r;
            }
        });
        batch.wait();
    }
    return out;
}

/// Immutable, table-global grouping state shared by grouped update stages.
/// `row_gid_storage` owns one deterministic id per absolute input row; `rows`
/// owns the CSR inverse mapping used by every later group-local reader and
/// scatter.
struct GroupedRowPlan {
    std::unique_ptr<std::uint32_t[]> row_gid_storage;
    GroupedRows rows;

    [[nodiscard]] auto row_gid(std::size_t count) const noexcept -> std::span<const std::uint32_t> {
        return {row_gid_storage.get(), count};
    }
};

/// Resolve group keys once and make their global row ownership explicit. No
/// grouped kernel may assign ids independently per chunk: equal keys can recur
/// in later chunks and must retain this same id.
auto make_grouped_row_plan(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                           const ExecutionContext& exec)
    -> std::expected<GroupedRowPlan, std::string> {
    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("update + by: unknown group key '" + key.name +
                                   "' (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(column);
    }
    const std::size_t row_count = input.rows();
    GroupedRowPlan plan;
    plan.row_gid_storage = std::make_unique_for_overwrite<std::uint32_t[]>(row_count);
    const auto validity = collect_key_validity(input, group_by);
    const std::size_t group_count =
        assign_group_ids(group_columns, validity, row_count, exec,
                         std::span<std::uint32_t>{plan.row_gid_storage.get(), row_count});
    plan.rows = build_grouped_rows(plan.row_gid(row_count), group_count, exec);
    return plan;
}

/// The first grouped-native reduction slice.  It deliberately accepts only
/// independent, bare numeric reductions: once a field can observe a preceding
/// field (or carries a scalar expression around an aggregate), the established
/// per-group table evaluator remains the semantic owner.
enum class NativeGroupedReduction : std::uint8_t {
    SumInt,
    SumDouble,
    Mean,
    MinInt,
    MinDouble,
    MaxInt,
    MaxDouble,
    CountRows,
    CountNonNull,
};

struct NativeGroupedReductionField {
    std::string alias;
    NativeGroupedReduction reduction = NativeGroupedReduction::CountRows;
    const ColumnEntry* source = nullptr;
};

/// The fixed-width reduction a callee runs over one source column, or nullopt
/// when the pair is outside the native contract. Type dispatch only: the
/// callers own the alias, the declaration-order question, and where the source
/// column came from.
auto native_reduction_for(std::string_view callee, const ColumnEntry& source)
    -> std::optional<NativeGroupedReduction> {
    // Counting non-null values reads the validity bitmap and never a value, so
    // it is fixed-width whatever the column holds.
    if (callee == "count") {
        return NativeGroupedReduction::CountNonNull;
    }
    const bool is_int = std::holds_alternative<Column<std::int64_t>>(*source.column);
    if (!is_int && !std::holds_alternative<Column<double>>(*source.column)) {
        return std::nullopt;
    }
    if (callee == "mean") {
        return NativeGroupedReduction::Mean;
    }
    if (callee == "sum") {
        return is_int ? NativeGroupedReduction::SumInt : NativeGroupedReduction::SumDouble;
    }
    if (callee == "min") {
        return is_int ? NativeGroupedReduction::MinInt : NativeGroupedReduction::MinDouble;
    }
    if (callee == "max") {
        return is_int ? NativeGroupedReduction::MaxInt : NativeGroupedReduction::MaxDouble;
    }
    return std::nullopt;
}

/// Classify one call as a native group reduction over a column `input` already
/// holds. Shared by the bare-field planner and by the aggregate-subterm lifter,
/// so the two cannot disagree about what the native contract covers.
auto classify_native_grouped_reduction(const ir::CallExpr& call, const Table& input)
    -> std::optional<NativeGroupedReductionField> {
    if (!call.named_args.empty()) {
        return std::nullopt;
    }
    if (call.callee != "sum" && call.callee != "mean" && call.callee != "min" &&
        call.callee != "max" && call.callee != "count") {
        return std::nullopt;
    }
    NativeGroupedReductionField item;
    if (call.callee == "count" && call.args.empty()) {
        item.reduction = NativeGroupedReduction::CountRows;
        return item;
    }
    if (call.args.size() != 1) {
        return std::nullopt;
    }
    const auto* ref = ir::as_column_ref(*call.args[0]);
    if (ref == nullptr) {
        return std::nullopt;
    }
    item.source = input.find_entry(ref->name);
    if (item.source == nullptr) {
        return std::nullopt;
    }
    const auto reduction = native_reduction_for(call.callee, *item.source);
    if (!reduction.has_value()) {
        return std::nullopt;
    }
    item.reduction = *reduction;
    return item;
}

/// Reduce one planned field over the CSR groups and broadcast each group's
/// value back to all of its absolute rows. Workers claim complete groups, so
/// their fixed-width writes are disjoint; the all-null markers they publish are
/// merged into one validity bitmap serially afterwards, which keeps packed
/// bitmap bytes out of the concurrent path.
auto compute_grouped_reduction_broadcast(const NativeGroupedReductionField& item,
                                         const GroupedRows& group_rows, std::size_t rows,
                                         std::size_t workers)
    -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
    const auto for_each_group = [&](const auto& body) {
        if (workers < 2) {
            for (std::size_t group = 0; group < group_rows.group_count(); ++group) {
                body(group);
            }
            return;
        }
        std::atomic<std::size_t> next_group{0};
        auto batch = process_worker_pool().submit(workers, [&](std::size_t) noexcept {
            while (true) {
                const std::size_t group = next_group.fetch_add(1, std::memory_order_relaxed);
                if (group >= group_rows.group_count()) {
                    return;
                }
                body(group);
            }
        });
        batch.wait();
    };

    if (item.reduction == NativeGroupedReduction::CountRows ||
        item.reduction == NativeGroupedReduction::CountNonNull) {
        Column<std::int64_t> result;
        result.resize(rows);
        const auto* validity =
            item.source != nullptr && item.source->validity ? &*item.source->validity : nullptr;
        for_each_group([&](std::size_t group) noexcept {
            std::int64_t count = 0;
            for (const auto row : group_rows[group]) {
                if (item.reduction == NativeGroupedReduction::CountRows || validity == nullptr ||
                    (*validity)[row]) {
                    ++count;
                }
            }
            for (const auto row : group_rows[group]) {
                result[row] = count;
            }
        });
        return {ColumnValue{std::move(result)}, std::nullopt};
    }

    std::vector<std::uint8_t> all_null(group_rows.group_count(), 0U);
    const auto reduce =
        [&]<typename Source, typename Result>(const Column<Source>& source) -> ColumnValue {
        const auto* validity = item.source->validity ? &*item.source->validity : nullptr;
        Column<Result> result;
        result.resize(rows);
        for_each_group([&](std::size_t group) noexcept {
            Result value{};
            std::size_t count = 0;
            for (const auto row : group_rows[group]) {
                if (validity != nullptr && !(*validity)[row]) {
                    continue;
                }
                const Result next = static_cast<Result>(source[row]);
                if (item.reduction == NativeGroupedReduction::MinInt ||
                    item.reduction == NativeGroupedReduction::MinDouble) {
                    value = count == 0 ? next : std::min(value, next);
                } else if (item.reduction == NativeGroupedReduction::MaxInt ||
                           item.reduction == NativeGroupedReduction::MaxDouble) {
                    value = count == 0 ? next : std::max(value, next);
                } else {
                    value += next;
                }
                ++count;
            }
            if (item.reduction == NativeGroupedReduction::Mean && count != 0) {
                value /= static_cast<Result>(count);
            }
            all_null[group] = count == 0 ? 1U : 0U;
            for (const auto row : group_rows[group]) {
                result[row] = value;
            }
        });
        return ColumnValue{std::move(result)};
    };

    ColumnValue column = [&] {
        if (item.reduction == NativeGroupedReduction::Mean) {
            if (const auto* source = std::get_if<Column<std::int64_t>>(&*item.source->column)) {
                return reduce.template operator()<std::int64_t, double>(*source);
            }
            return reduce.template operator()<double, double>(
                std::get<Column<double>>(*item.source->column));
        }
        if (item.reduction == NativeGroupedReduction::SumInt ||
            item.reduction == NativeGroupedReduction::MinInt ||
            item.reduction == NativeGroupedReduction::MaxInt) {
            return reduce.template operator()<std::int64_t, std::int64_t>(
                std::get<Column<std::int64_t>>(*item.source->column));
        }
        return reduce.template operator()<double, double>(
            std::get<Column<double>>(*item.source->column));
    }();

    std::optional<ValidityBitmap> output_validity;
    for (std::size_t group = 0; group < group_rows.group_count(); ++group) {
        if (all_null[group] == 0U) {
            continue;
        }
        if (!output_validity.has_value()) {
            output_validity = ValidityBitmap(rows, true);
        }
        for (const auto row : group_rows[group]) {
            output_validity->set(row, false);
        }
    }
    return {std::move(column), std::move(output_validity)};
}

/// The properties a grouped update lands with: the rows never move, and every
/// written alias loses whatever the old column of that name claimed.
auto derive_grouped_update_properties(const Table& output, const std::vector<ir::FieldSpec>& fields)
    -> TableProperties {
    return TableProperties::derive(
        table_properties_of(output),
        [&](const std::string& name) -> KeyFate {
            const bool overwritten = std::ranges::any_of(
                fields, [&](const ir::FieldSpec& field) { return field.alias == name; });
            return overwritten ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve);
}

/// Return a fully scattered update result when every field belongs to the
/// fixed-width native reduction contract. A null-producing numeric reduction
/// owns one output validity bitmap; count is never nullable. The group CSR
/// rows are read directly, and every group writes disjoint absolute output
/// rows.
auto try_native_grouped_reductions(const Table& input, const std::vector<ir::FieldSpec>& fields,
                                   const GroupedRows& group_rows, const ExecutionContext& exec)
    -> std::expected<std::optional<Table>, std::string> {
    std::vector<NativeGroupedReductionField> plan;
    plan.reserve(fields.size());
    for (const auto& field : fields) {
        const auto* call = std::get_if<ir::CallExpr>(&field.expr.node);
        if (call == nullptr) {
            return std::optional<Table>{};
        }
        auto item = classify_native_grouped_reduction(*call, input);
        if (!item.has_value()) {
            return std::optional<Table>{};
        }
        if (std::ranges::any_of(
                plan, [&](const auto& existing) { return existing.alias == field.alias; })) {
            return std::optional<Table>{};
        }
        // A source that is also an output alias relies on update's
        // declaration-order visibility.  Keep that contract on the
        // materialized evaluator until this protocol has dependencies.
        if (item->source != nullptr && std::ranges::any_of(fields, [&](const auto& candidate) {
                return candidate.alias == item->source->name;
            })) {
            return std::optional<Table>{};
        }
        item->alias = field.alias;
        plan.push_back(std::move(*item));
    }
    if (plan.empty()) {
        return std::optional<Table>{};
    }

    const std::size_t rows = input.rows();
    const std::size_t workers =
        grouped_reduction_worker_count(exec, group_rows.group_count(), rows);
    Table output = input;
    for (const auto& item : plan) {
        auto [column, validity] =
            compute_grouped_reduction_broadcast(item, group_rows, rows, workers);
        if (validity.has_value()) {
            output.add_column(item.alias, std::move(column), std::move(*validity));
        } else {
            output.add_column(item.alias, std::move(column));
        }
    }
    apply_table_properties(output, derive_grouped_update_properties(output, fields));
    return std::optional<Table>{std::move(output)};
}

enum class NativeGroupedOrderedKind : std::uint8_t {
    Lag,
    Lead,
    CumSum,
    CumProd,
    FillForward,
    FillBackward,
};

/// The ordered group kernel a call names, with the row offset `lag`/`lead`
/// carry. Call shape only — the caller resolves the source column, which is why
/// the bare-field planner and the lifter can share this without agreeing on
/// where that column comes from.
struct NativeGroupedOrderedCall {
    NativeGroupedOrderedKind kind = NativeGroupedOrderedKind::Lag;
    std::size_t offset = 0;
};

auto classify_native_grouped_ordered(const ir::CallExpr& call)
    -> std::optional<NativeGroupedOrderedCall> {
    if (!call.named_args.empty() || call.args.empty() || call.args[0] == nullptr) {
        return std::nullopt;
    }
    if (call.callee == "lag" || call.callee == "lead") {
        if (call.args.size() != 2 || call.args[1] == nullptr) {
            return std::nullopt;
        }
        const auto* literal = std::get_if<ir::Literal>(&call.args[1]->node);
        const auto* value =
            literal == nullptr ? nullptr : std::get_if<std::int64_t>(&literal->value);
        if (value == nullptr || *value < 0) {
            return std::nullopt;
        }
        return NativeGroupedOrderedCall{.kind = call.callee == "lag"
                                                    ? NativeGroupedOrderedKind::Lag
                                                    : NativeGroupedOrderedKind::Lead,
                                        .offset = static_cast<std::size_t>(*value)};
    }
    if (call.args.size() != 1) {
        return std::nullopt;
    }
    if (call.callee == "cumsum") {
        return NativeGroupedOrderedCall{.kind = NativeGroupedOrderedKind::CumSum, .offset = 0};
    }
    if (call.callee == "cumprod") {
        return NativeGroupedOrderedCall{.kind = NativeGroupedOrderedKind::CumProd, .offset = 0};
    }
    if (call.callee == "fill_forward") {
        return NativeGroupedOrderedCall{.kind = NativeGroupedOrderedKind::FillForward, .offset = 0};
    }
    if (call.callee == "fill_backward") {
        return NativeGroupedOrderedCall{.kind = NativeGroupedOrderedKind::FillBackward,
                                        .offset = 0};
    }
    return std::nullopt;
}

/// True when the ordered kernels can read this column at all: they carry a
/// state chain of the column's own type, which the fixed-width numerics are.
auto ordered_source_supported(const ColumnEntry& source) -> bool {
    return std::holds_alternative<Column<std::int64_t>>(*source.column) ||
           std::holds_alternative<Column<double>>(*source.column);
}

/// Walk each CSR group in original row order and scatter to absolute output
/// rows. A worker owns its group's complete state chain, so nothing crosses a
/// group boundary; the byte validity staging avoids concurrent writes to packed
/// bitmap words and is merged only after the worker barrier.
auto compute_grouped_ordered_column(const NativeGroupedOrderedCall& ordered,
                                    const ColumnEntry& source, const GroupedRows& group_rows,
                                    std::size_t rows, std::size_t workers)
    -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
    const auto kind = ordered.kind;
    const auto offset = ordered.offset;
    std::vector<std::uint8_t> invalid(rows, 0U);
    const auto write = [&]<typename T>(const Column<T>& values) -> ColumnValue {
        Column<T> result;
        result.resize(rows);
        const auto* source_validity = source.validity ? &*source.validity : nullptr;
        const auto run_group = [&](std::size_t group) noexcept {
            const auto indices = group_rows[group];
            if (kind == NativeGroupedOrderedKind::Lag || kind == NativeGroupedOrderedKind::Lead) {
                for (std::size_t local = 0; local < indices.size(); ++local) {
                    const bool in_bounds = kind == NativeGroupedOrderedKind::Lag
                                               ? local >= offset
                                               : local + offset < indices.size();
                    const std::size_t row = indices[local];
                    if (!in_bounds) {
                        result[row] = T{};
                        invalid[row] = 1U;
                    } else {
                        const std::size_t source_local =
                            kind == NativeGroupedOrderedKind::Lag ? local - offset : local + offset;
                        result[row] = values[indices[source_local]];
                    }
                }
                return;
            }
            if (kind == NativeGroupedOrderedKind::CumSum ||
                kind == NativeGroupedOrderedKind::CumProd) {
                T accumulator = kind == NativeGroupedOrderedKind::CumProd ? T{1} : T{};
                for (const auto row : indices) {
                    if (kind == NativeGroupedOrderedKind::CumProd) {
                        accumulator *= values[row];
                    } else {
                        accumulator += values[row];
                    }
                    result[row] = accumulator;
                }
                return;
            }
            const bool forward = kind == NativeGroupedOrderedKind::FillForward;
            T carry{};
            bool have_carry = false;
            for (std::size_t pos = 0; pos < indices.size(); ++pos) {
                const std::size_t local = forward ? pos : indices.size() - 1 - pos;
                const std::size_t row = indices[local];
                const bool valid = source_validity == nullptr || (*source_validity)[row];
                if (valid) {
                    result[row] = values[row];
                    carry = values[row];
                    have_carry = true;
                } else if (have_carry) {
                    result[row] = carry;
                } else {
                    result[row] = T{};
                    invalid[row] = 1U;
                }
            }
        };
        if (workers < 2) {
            for (std::size_t group = 0; group < group_rows.group_count(); ++group) {
                run_group(group);
            }
        } else {
            std::atomic<std::size_t> next_group{0};
            auto batch = process_worker_pool().submit(workers, [&](std::size_t) noexcept {
                while (true) {
                    const std::size_t group = next_group.fetch_add(1, std::memory_order_relaxed);
                    if (group >= group_rows.group_count()) {
                        return;
                    }
                    run_group(group);
                }
            });
            batch.wait();
        }
        return ColumnValue{std::move(result)};
    };

    ColumnValue column =
        std::holds_alternative<Column<std::int64_t>>(*source.column)
            ? write.template operator()<std::int64_t>(
                  std::get<Column<std::int64_t>>(*source.column))
            : write.template operator()<double>(std::get<Column<double>>(*source.column));
    std::optional<ValidityBitmap> validity;
    if (std::ranges::any_of(invalid, [](auto value) { return value != 0U; })) {
        validity = ValidityBitmap(rows, true);
        for (std::size_t row = 0; row < rows; ++row) {
            if (invalid[row] != 0U) {
                validity->set(row, false);
            }
        }
    }
    return {std::move(column), std::move(validity)};
}

/// One group-state call lifted out of a field expression -- an aggregate, or an
/// ordered kernel like `lag`/`cumsum`. Its per-row result is written into
/// `column`, a staging column the rewritten row-local expression then reads
/// like any other input.
struct LiftedGroupState {
    std::string column;
    std::string callee;
    /// The call as written, kept whole because an aggregate's later arguments
    /// are parameters rather than data — `quantile(x, 0.9)`'s 0.9. Its first
    /// argument is replaced by `source_column` when the spec is built.
    ir::CallExpr call;
    /// The column the reduction reads. Empty only for bare `count()`, which
    /// reads no column at all.
    std::string source_column;
    /// Set when the argument is an expression rather than a column reference:
    /// it is evaluated into `source_column` before the reduction runs.
    std::optional<ir::Expr> source_expr;
    /// Set for an ordered group kernel; absent for an aggregate. The two differ
    /// only in how `column` is filled: an aggregate broadcasts one value to the
    /// group's rows, an ordered kernel writes a distinct value per row.
    std::optional<NativeGroupedOrderedCall> ordered;
};

/// A staging column name that no column of `table` already holds. The prefix is
/// not a valid identifier in the surface language, so a collision means an
/// earlier stage of this same rewrite, not user data.
auto unique_staging_name(const Table& table, std::string_view prefix, std::size_t& counter)
    -> std::string {
    while (true) {
        std::string name = std::string(prefix) + std::to_string(counter++);
        if (table.find_entry(name) == nullptr) {
            return name;
        }
    }
}

/// Rewrite `expr` in place, replacing every aggregate and ordered group call
/// with a reference to the staging column that will hold its result. Returns
/// false when one of them is outside what the native group protocols compute,
/// which leaves the whole field to the materialized evaluator.
///
/// Ordered state is lifted for the same reason aggregates are: `lag(x, 1)` is a
/// value per row that the CSR walk already produces, and once it is a column
/// the expression around it -- `log(price / lag(price, 1))`, the log-return
/// shape -- is ordinary row-local work.
///
/// Aggregates over the same bare column collapse onto one staging column, so
/// `x / sum(x) - sum(x)` reduces once. Aggregates over an expression do not:
/// deciding that two argument trees are the same expression needs a structural
/// comparison the IR does not offer, and computing one twice is a bounded cost
/// against wrongly sharing two different reductions.
auto lift_group_state(ir::Expr& expr, const Table& input, std::size_t& counter,
                      std::vector<LiftedGroupState>& lifted) -> bool {
    return std::visit(
        [&](auto& node) -> bool {
            using NodeT = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<NodeT, ir::ColumnRef> ||
                          std::is_same_v<NodeT, ir::Literal>) {
                return true;
            } else if constexpr (std::is_same_v<NodeT, ir::RankExpr>) {
                return false;
            } else if constexpr (std::is_same_v<NodeT, ir::IsNullExpr>) {
                return node.operand != nullptr &&
                       lift_group_state(*node.operand, input, counter, lifted);
            } else if constexpr (std::is_same_v<NodeT, ir::BinaryExpr> ||
                                 std::is_same_v<NodeT, ir::CompareExpr> ||
                                 std::is_same_v<NodeT, ir::LogicalExpr>) {
                if (node.left == nullptr || !lift_group_state(*node.left, input, counter, lifted)) {
                    return false;
                }
                // `Not` is the one binary-shaped node with no right operand.
                return node.right == nullptr ||
                       lift_group_state(*node.right, input, counter, lifted);
            } else if constexpr (std::is_same_v<NodeT, ir::CallExpr>) {
                const auto ordered = classify_native_grouped_ordered(node);
                if (!ordered.has_value() && !ir::is_aggregate_func(node.callee)) {
                    for (auto& arg : node.args) {
                        if (arg == nullptr || !lift_group_state(*arg, input, counter, lifted)) {
                            return false;
                        }
                    }
                    for (auto& named : node.named_args) {
                        if (named.value == nullptr ||
                            !lift_group_state(*named.value, input, counter, lifted)) {
                            return false;
                        }
                    }
                    return true;
                }
                if (!node.named_args.empty()) {
                    return false;
                }
                // Every aggregate the group-by operator can name is liftable:
                // the fixed-width family reduces over the CSR rows, and the
                // rest is one grouped aggregation broadcast back. `count()`
                // names no column and so has no `AggSpec`, but it is the
                // simplest CSR reduction there is. An ordered kernel was
                // already classified above.
                const bool bare_count = node.callee == "count" && node.args.empty();
                if (!ordered.has_value() && !bare_count &&
                    !parse_aggregate_func(node.callee).has_value()) {
                    return false;
                }
                LiftedGroupState item{.column = {},
                                      .callee = node.callee,
                                      .call = node,
                                      .source_column = {},
                                      .source_expr = {},
                                      .ordered = ordered};
                if (node.args.empty()) {
                    if (!bare_count) {
                        return false;
                    }
                } else {
                    if (node.args[0] == nullptr) {
                        return false;
                    }
                    const ir::Expr& argument = *node.args[0];
                    // A nested aggregate has no group to reduce over, and an
                    // order-dependent or unknown call inside the argument is
                    // exactly what the materialized evaluator still owns.
                    if (!ir::is_row_local_update_expr(argument) ||
                        expr_contains_aggregate_call(argument)) {
                        return false;
                    }
                    if (const auto* ref = ir::as_column_ref(argument); ref != nullptr) {
                        const auto* entry = input.find_entry(ref->name);
                        if (entry == nullptr ||
                            (ordered.has_value() && !ordered_source_supported(*entry))) {
                            return false;
                        }
                        item.source_column = ref->name;
                    } else {
                        item.source_expr = argument;
                        item.source_column = unique_staging_name(input, "__grp_agg_arg_", counter);
                    }
                }
                // Two calls collapse onto one staging column only when they
                // agree on everything that makes them a different computation:
                // the same kernel over the same column, and the same parameter
                // -- `quantile`'s fraction, `lag`'s offset.
                const auto same = std::ranges::find_if(lifted, [&](const auto& existing) {
                    if (existing.source_expr.has_value() || existing.callee != item.callee ||
                        existing.source_column != item.source_column ||
                        existing.ordered.has_value() != item.ordered.has_value()) {
                        return false;
                    }
                    if (item.ordered.has_value()) {
                        return existing.ordered->offset == item.ordered->offset;
                    }
                    return existing.call.args.size() <= 1 && node.args.size() <= 1;
                });
                if (same != lifted.end()) {
                    expr.node = ir::ColumnRef{.name = same->column};
                    return true;
                }
                item.column = unique_staging_name(input, "__grp_agg_", counter);
                expr.node = ir::ColumnRef{.name = item.column};
                lifted.push_back(std::move(item));
                return true;
            } else {
                return false;
            }
        },
        expr.node);
}

/// The admission test for the lifted group-state path, and the rewrite it
/// implies. A field qualifies when every aggregate and ordered group call in it
/// becomes a staging column AND what is left is an ordinary row-local
/// expression -- which is the whole contract, asked of the rewritten
/// expression rather than guessed at from the original. An extern, a rolling
/// call, or a `rank` survives the rewrite and fails it here.
auto plan_lifted_group_state(const ir::Expr& expr, const Table& input, ir::Expr& rewritten,
                             std::size_t& counter, std::vector<LiftedGroupState>& lifted) -> bool {
    rewritten = expr;
    if (!lift_group_state(rewritten, input, counter, lifted) || lifted.empty()) {
        return false;
    }
    return ir::is_row_local_update_expr(rewritten) && !expr_contains_aggregate_call(rewritten);
}

/// Return `table` without the staging columns named in `staged`. `Table` has no
/// column removal by design, so the surviving entries move into a fresh table
/// in their original order, sharing their storage rather than copying it.
auto without_staging_columns(Table table, const std::vector<std::string>& staged) -> Table {
    Table result;
    result.logical_rows = table.logical_rows;
    for (auto& entry : table.columns) {
        if (std::ranges::find(staged, entry.name) != staged.end()) {
            continue;
        }
        result.add_column_shared(entry.name, std::move(entry.column), std::move(entry.validity));
    }
    result.set_properties(table.properties());
    return result;
}

/// Broadcast the aggregates that are not in the fixed-width CSR family —
/// `median`, `std`, `quantile`, `first`, `last`, a string `min` — by running
/// the grouped aggregate operator itself once and gathering its per-group
/// result back onto the rows.
///
/// The key it aggregates by is the group id, not the user's `by` columns. That
/// is what makes this general: the operator never sees a multi-key, string, or
/// null-bearing key tuple, only one dense Int64, and the answer comes back
/// already addressed by the same ids the CSR rows use. It also means the
/// aggregate semantics here are not a reimplementation — they are the same
/// kernels `select ..., by ...` runs, so the two cannot answer differently.
///
/// Declines (rather than fails) when the operator will not take the call: the
/// materialized evaluator runs the identical aggregate and owns the diagnostic.
auto broadcast_general_group_aggregates(const Table& staged,
                                        std::span<const LiftedGroupState* const> items,
                                        std::span<const std::uint32_t> row_gid,
                                        std::size_t group_count, std::size_t& counter,
                                        const ExecutionContext& exec)
    -> std::expected<std::optional<std::vector<ColumnEntry>>, std::string> {
    const std::size_t rows = row_gid.size();
    Table keyed;
    const std::string key = unique_staging_name(staged, "__grp_key_", counter);
    Column<std::int64_t> gid;
    gid.resize(rows);
    for (std::size_t r = 0; r < rows; ++r) {
        gid[r] = static_cast<std::int64_t>(row_gid[r]);
    }
    keyed.add_column(key, std::move(gid));

    std::vector<ir::AggSpec> specs;
    specs.reserve(items.size());
    for (const auto* item : items) {
        const auto* source = staged.find_entry(item->source_column);
        if (source == nullptr) {
            return std::optional<std::vector<ColumnEntry>>{};
        }
        keyed.add_column_from(item->source_column, *source);
        ir::CallExpr call = item->call;
        if (call.args.empty()) {
            return std::optional<std::vector<ColumnEntry>>{};  // only bare count(), never general
        }
        call.args[0] = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = item->source_column}});
        auto spec = aggregate_call_to_spec(call, item->column);
        if (!spec.has_value() || !spec->has_value()) {
            return std::optional<std::vector<ColumnEntry>>{};
        }
        specs.push_back(std::move(**spec));
    }

    auto aggregated = aggregate_table(keyed, {ir::ColumnRef{.name = key}}, specs, &exec);
    if (!aggregated.has_value() || aggregated->rows() != group_count) {
        return std::optional<std::vector<ColumnEntry>>{};
    }
    const auto* key_column = aggregated->find(key);
    if (key_column == nullptr) {
        return std::optional<std::vector<ColumnEntry>>{};
    }
    const auto* key_values = std::get_if<Column<std::int64_t>>(key_column);
    if (key_values == nullptr) {
        return std::optional<std::vector<ColumnEntry>>{};
    }
    // The operator is free to emit its groups in any order, so the ids come
    // back as data and are read, never assumed.
    std::vector<std::size_t> position(group_count, 0);
    for (std::size_t i = 0; i < group_count; ++i) {
        const auto id = (*key_values)[i];
        if (id < 0 || static_cast<std::size_t>(id) >= group_count) {
            return std::optional<std::vector<ColumnEntry>>{};
        }
        position[static_cast<std::size_t>(id)] = i;
    }
    std::vector<std::size_t> indices(rows);
    for (std::size_t r = 0; r < rows; ++r) {
        indices[r] = position[row_gid[r]];
    }

    std::vector<ColumnEntry> broadcast;
    broadcast.reserve(items.size());
    for (const auto* item : items) {
        const auto* result = aggregated->find_entry(item->column);
        if (result == nullptr) {
            return std::optional<std::vector<ColumnEntry>>{};
        }
        ColumnEntry entry{.name = item->column,
                          .column = std::make_shared<ColumnValue>(
                              gather_column(*result->column, indices.data(), rows, &exec)),
                          .validity = std::nullopt};
        if (result->validity.has_value()) {
            ValidityBitmap validity(rows, true);
            for (std::size_t r = 0; r < rows; ++r) {
                validity.set(r, (*result->validity)[indices[r]]);
            }
            entry.validity = std::move(validity);
        }
        broadcast.push_back(std::move(entry));
    }
    return std::optional<std::vector<ColumnEntry>>{std::move(broadcast)};
}

/// A field that MIXES an aggregate with row-local terms — `x - mean(x)`,
/// `x / sum(x)`, `sum(a * b)` — is the canonical grouped update, and until now
/// no native gate matched it: its root is not a bare aggregate call, so the
/// whole clause fell to the per-group gather-and-rebuild evaluator.
///
/// Nothing here needs a new aggregate kernel. Each aggregate subterm is a group
/// reduction that either the CSR path or the grouped aggregate operator already
/// owns, and once its broadcast value is a column, what remains of the
/// expression is row-local by construction and belongs to `update_table`'s
/// direct ChunkView output protocols. So the field is executed as: stage any
/// aggregate arguments, reduce and broadcast, then evaluate the residual
/// expression over original-order rows.
///
/// The staging columns live only on a local table that shares the input's
/// column storage; the caller's table never sees them.
auto try_native_grouped_aggregate_expr(const Table& input, const ir::FieldSpec& field,
                                       const GroupedRowPlan& grouped, const ScalarRegistry* scalars,
                                       const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<std::optional<Table>, std::string> {
    ir::Expr rewritten;
    std::size_t counter = 0;
    std::vector<LiftedGroupState> lifted;
    if (!plan_lifted_group_state(field.expr, input, rewritten, counter, lifted)) {
        return std::optional<Table>{};
    }

    std::vector<std::string> staged_names;
    staged_names.reserve(lifted.size() * 2);
    Table staged = input;
    for (const auto& item : lifted) {
        if (!item.source_expr.has_value()) {
            continue;
        }
        staged_names.push_back(item.source_column);
        auto argument =
            update_table(std::move(staged),
                         {ir::FieldSpec{.alias = item.source_column, .expr = *item.source_expr}},
                         scalars, externs, exec);
        if (!argument.has_value()) {
            return std::unexpected(std::move(argument.error()));
        }
        staged = std::move(*argument);
    }

    const std::size_t rows = input.rows();
    const GroupedRows& group_rows = grouped.rows;
    const std::size_t workers =
        grouped_reduction_worker_count(exec, group_rows.group_count(), rows);
    std::vector<const LiftedGroupState*> general;
    for (const auto& item : lifted) {
        if (item.ordered.has_value()) {
            const auto* source = staged.find_entry(item.source_column);
            if (source == nullptr || !ordered_source_supported(*source)) {
                return std::optional<Table>{};
            }
            auto [column, validity] =
                compute_grouped_ordered_column(*item.ordered, *source, group_rows, rows, workers);
            staged_names.push_back(item.column);
            if (validity.has_value()) {
                staged.add_column(item.column, std::move(column), std::move(*validity));
            } else {
                staged.add_column(item.column, std::move(column));
            }
            continue;
        }
        NativeGroupedReductionField plan{.alias = item.column};
        if (item.source_column.empty()) {
            plan.reduction = NativeGroupedReduction::CountRows;
        } else {
            plan.source = staged.find_entry(item.source_column);
            if (plan.source == nullptr) {
                return std::optional<Table>{};
            }
            // A staged argument's type is only known once it has been
            // evaluated, so the fixed-width question is asked here rather than
            // when the aggregate was lifted. Anything outside that family is
            // still native — it goes to the grouped aggregate operator below.
            const auto reduction = native_reduction_for(item.callee, *plan.source);
            if (!reduction.has_value()) {
                general.push_back(&item);
                continue;
            }
            plan.reduction = *reduction;
        }
        auto [column, validity] =
            compute_grouped_reduction_broadcast(plan, group_rows, rows, workers);
        staged_names.push_back(item.column);
        if (validity.has_value()) {
            staged.add_column(item.column, std::move(column), std::move(*validity));
        } else {
            staged.add_column(item.column, std::move(column));
        }
    }
    if (!general.empty()) {
        auto broadcast = broadcast_general_group_aggregates(
            staged, general, grouped.row_gid(rows), group_rows.group_count(), counter, exec);
        if (!broadcast.has_value()) {
            return std::unexpected(std::move(broadcast.error()));
        }
        if (!broadcast->has_value()) {
            return std::optional<Table>{};
        }
        for (auto& entry : **broadcast) {
            staged_names.push_back(entry.name);
            staged.add_column_shared(entry.name, std::move(entry.column),
                                     std::move(entry.validity));
        }
    }

    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->grouped_lifted_group_state.fetch_add(lifted.size(),
                                                                  std::memory_order_relaxed);
    }
    // A field that is nothing but one aggregate — `m = median(x)` — has already
    // been computed: its staging column IS the answer, addressed by absolute
    // row. Landing it directly hands the caller that column instead of
    // evaluating a whole-column copy of it.
    if (const auto* ref = ir::as_column_ref(rewritten);
        ref != nullptr && std::ranges::find(staged_names, ref->name) != staged_names.end()) {
        ColumnEntry landed = *staged.find_entry(ref->name);
        Table output = without_staging_columns(std::move(staged), staged_names);
        output.add_column_shared(field.alias, std::move(landed.column), std::move(landed.validity));
        apply_table_properties(output, derive_grouped_update_properties(output, {field}));
        return std::optional<Table>{std::move(output)};
    }

    auto evaluated =
        update_table(std::move(staged), {ir::FieldSpec{.alias = field.alias, .expr = rewritten}},
                     scalars, externs, exec);
    if (!evaluated.has_value()) {
        return std::unexpected(std::move(evaluated.error()));
    }
    Table output = without_staging_columns(std::move(*evaluated), staged_names);
    apply_table_properties(output, derive_grouped_update_properties(output, {field}));
    return std::optional<Table>{std::move(output)};
}

/// Fixed-width, bare ordered kernels over one CSR group.
auto try_native_grouped_ordered_field(const Table& input, const std::vector<ir::FieldSpec>& fields,
                                      const GroupedRows& group_rows, const ExecutionContext& exec)
    -> std::expected<std::optional<Table>, std::string> {
    if (fields.size() != 1) {
        return std::optional<Table>{};
    }
    const auto& field = fields.front();
    const auto* call = std::get_if<ir::CallExpr>(&field.expr.node);
    if (call == nullptr) {
        return std::optional<Table>{};
    }
    const auto ordered = classify_native_grouped_ordered(*call);
    if (!ordered.has_value()) {
        return std::optional<Table>{};
    }
    const auto* ref = ir::as_column_ref(*call->args[0]);
    if (ref == nullptr) {
        return std::optional<Table>{};
    }
    const auto* entry = input.find_entry(ref->name);
    if (entry == nullptr || !ordered_source_supported(*entry)) {
        return std::optional<Table>{};
    }

    const std::size_t rows = input.rows();
    const std::size_t workers =
        grouped_reduction_worker_count(exec, group_rows.group_count(), rows);
    auto [column, validity] =
        compute_grouped_ordered_column(*ordered, *entry, group_rows, rows, workers);
    Table output = input;
    if (validity.has_value()) {
        output.add_column(field.alias, std::move(column), std::move(*validity));
    } else {
        output.add_column(field.alias, std::move(column));
    }
    apply_table_properties(output, derive_grouped_update_properties(output, fields));
    return std::optional<Table>{std::move(output)};
}

/// A cheap admission check for the ordered mixed-field dispatcher below. The
/// full planner remains authoritative; this merely avoids splitting a wholly
/// materialized update into one group pass per field.
[[nodiscard]] auto is_grouped_direct_field_candidate(const Table& input, const ir::FieldSpec& field)
    -> bool {
    if (ir::is_row_local_update_expr(field.expr) && !expr_contains_aggregate_call(field.expr)) {
        return true;
    }
    // A field mixing group state into row-local terms is admitted on exactly
    // the test its executor uses -- a trial lift over a copy of the expression
    // -- rather than on a looser proxy. A clause of nothing but fields the
    // lifter declines must keep its single shared group pass instead of paying
    // one group plan per staged field for nothing.
    {
        ir::Expr probe;
        std::size_t counter = 0;
        std::vector<LiftedGroupState> lifted;
        if (plan_lifted_group_state(field.expr, input, probe, counter, lifted)) {
            return true;
        }
    }
    const auto* call = std::get_if<ir::CallExpr>(&field.expr.node);
    if (call == nullptr || !call->named_args.empty()) {
        return false;
    }
    if (call->callee == "lag" || call->callee == "lead" || call->callee == "cumsum" ||
        call->callee == "cumprod" || call->callee == "fill_forward" ||
        call->callee == "fill_backward") {
        return true;
    }
    if (call->callee == "count" && call->args.empty()) {
        return true;
    }
    if ((call->callee != "sum" && call->callee != "mean" && call->callee != "min" &&
         call->callee != "max" && call->callee != "count") ||
        call->args.size() != 1) {
        return false;
    }
    const auto* ref = ir::as_column_ref(*call->args[0]);
    if (ref == nullptr || ref->name == field.alias) {
        return false;
    }
    const auto* source = input.find(ref->name);
    // A preceding materialized field may create this source. Treat that as a
    // candidate so the ordered dispatcher gets a chance to land it first; the
    // single-field native planner remains the type/error authority afterwards.
    return source == nullptr || std::holds_alternative<Column<std::int64_t>>(*source) ||
           std::holds_alternative<Column<double>>(*source);
}

/// A TimeFrame's index read as plain integer ticks: nanos for a Timestamp
/// column, days for a Date one. Both are single-field structs over their
/// integer, so this is a view rather than a copy — the whole point, since the
/// alternative at 5M rows is a 40MB widening pass to compare two numbers.
///
/// The two widths stay separate rather than being normalised to nanos: a Date
/// is int32 days, and the multiply would be a per-access cost paid for nothing.
/// Only ordering is ever asked of it, and ordering is the same either way.
class TimeIndexTicks {
   public:
    /// Absent when the table has no time index, or when its index is not one of
    /// the two integer-backed temporal types — a case the callers read as
    /// "cannot reason about this" rather than as a failure.
    [[nodiscard]] static auto of(const Table& table) -> std::optional<TimeIndexTicks> {
        if (!table.time_index().has_value()) {
            return std::nullopt;
        }
        const auto* column = table.find(*table.time_index());
        if (column == nullptr) {
            return std::nullopt;
        }
        TimeIndexTicks ticks;
        if (const auto* ts = std::get_if<Column<Timestamp>>(column)) {
            static_assert(sizeof(Timestamp) == sizeof(std::int64_t));
            ticks.nanos_ = reinterpret_cast<const std::int64_t*>(ts->data());
            return ticks;
        }
        if (const auto* date = std::get_if<Column<Date>>(column)) {
            static_assert(sizeof(Date) == sizeof(std::int32_t));
            ticks.days_ = reinterpret_cast<const std::int32_t*>(date->data());
            return ticks;
        }
        return std::nullopt;
    }

    [[nodiscard]] auto operator[](std::size_t row) const noexcept -> std::int64_t {
        return nanos_ != nullptr ? nanos_[row] : std::int64_t{days_[row]};
    }

    [[nodiscard]] auto is_sorted(std::size_t rows) const noexcept -> bool {
        return nanos_ != nullptr ? std::is_sorted(nanos_, nanos_ + rows)
                                 : std::is_sorted(days_, days_ + rows);
    }

    /// A nanosecond span expressed in this index's own ticks, rounded UP. Every
    /// caller is sizing a lookback, where too long costs work and too short
    /// costs correctness.
    [[nodiscard]] auto ticks_from_nanos_ceil(std::int64_t nanos) const noexcept -> std::int64_t {
        if (nanos_ != nullptr) {
            return nanos;
        }
        static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
        return (nanos + kNsPerDay - 1) / kNsPerDay;
    }

   private:
    const std::int64_t* nanos_ = nullptr;
    const std::int32_t* days_ = nullptr;
};

/// Does the table's stated ordering already prove that each group's rows are
/// time-ascending?
///
/// The exact rule: the ordering must reach the time index ascending after
/// nothing but grouping keys. Rows ordered by (k, ts) sit in one time-ascending
/// run per `k`; rows ordered by (ts) are time-ascending in every subset at all,
/// group or not. The direction of the *preceding* keys is irrelevant — they only
/// have to be keys this call also partitions by, so that a change in one of them
/// is a change of group rather than a jump backwards inside one.
///
/// Anything else — a key that is not grouped by here, a descending time index,
/// no ordering claim at all — is not a proof, and the caller falls back to
/// reading the data.
[[nodiscard]] auto ordering_proves_group_time_order(const Table& input,
                                                    const std::vector<ir::ColumnRef>& group_by)
    -> bool {
    const auto& ordering = input.ordering();
    if (!ordering.has_value() || !input.time_index().has_value()) {
        return false;
    }
    for (const auto& key : *ordering) {
        if (key.name == *input.time_index()) {
            return key.ascending;
        }
        if (std::ranges::none_of(group_by, [&](const ir::ColumnRef& group_key) {
                return group_key.name == key.name;
            })) {
            return false;
        }
    }
    return false;  // the ordering ran out before it reached the time index
}

/// A duration window over a group whose rows are NOT time-ascending reads a
/// later row into an earlier row's window and drops a row that belongs there.
/// It does so silently, which is the whole reason this check exists: the answer
/// is wrong, not absent.
///
/// The hazard is not hypothetical, and it is not user error either. This very
/// operator emits its rows GROUP-MAJOR while keeping the time index, so a second
/// grouped window over a *different* key sees a table that is a valid TimeFrame,
/// is genuinely ordered, and is still time-scrambled within the new groups.
///
/// Cost is paid only when it must be: a stated ordering proves the property for
/// free, and a globally ascending index proves it with one sequential pass —
/// far cheaper than the strided per-group walk, and the common case by a wide
/// margin. The per-group walk is the last resort, and is serial so that the
/// group it names never depends on thread timing.
[[nodiscard]] auto check_grouped_time_order(const Table& input,
                                            const std::vector<ir::ColumnRef>& group_by,
                                            const GroupedRows& group_rows)
    -> std::expected<void, std::string> {
    const std::size_t rows = input.rows();
    if (rows < 2 || ordering_proves_group_time_order(input, group_by)) {
        return {};
    }
    const auto ticks = TimeIndexTicks::of(input);
    if (!ticks.has_value()) {
        return {};  // not an integer-backed index; nothing to compare
    }
    if (ticks->is_sorted(rows)) {
        return {};
    }
    for (std::size_t group = 0; group < group_rows.group_count(); ++group) {
        const auto indices = group_rows[group];
        for (std::size_t local = 1; local < indices.size(); ++local) {
            if ((*ticks)[indices[local]] >= (*ticks)[indices[local - 1]]) {
                continue;
            }
            std::string keys;
            std::string values;
            for (const auto& key : group_by) {
                if (!keys.empty()) {
                    keys += ", ";
                    values += ", ";
                }
                keys += key.name;
                const auto* entry = input.find_entry(key.name);
                values +=
                    key.name + "=" +
                    (entry == nullptr ? std::string("?") : format_cell(*entry, indices[local]));
            }
            return std::unexpected(
                "window + by: rows are not time-ascending within group (" + values + "): row " +
                std::to_string(indices[local]) + " is earlier in time than row " +
                std::to_string(indices[local - 1]) +
                ", which precedes it in this group — a duration window over them would read a "
                "later row into an earlier row's window"
                "\n  hint: `order { " +
                keys + ", " + *input.time_index() +
                " }` first. An upstream grouped `window`/`resample` leaves its rows group-major, "
                "so they are time-ascending only within ITS keys, not within these.");
        }
    }
    return {};
}

/// Cut one group's row list into pieces of at least `target` rows, each ending
/// on a window-bucket boundary. `bucket_of(row)` gives the bucket index of a
/// row; it is non-decreasing along `idx` because rows arrive time-sorted, which
/// is what lets each cut be a binary search instead of a scan.
template <typename BucketOf>
void split_at_bucket_bounds(std::span<const std::size_t> idx, std::size_t target,
                            BucketOf bucket_of, std::vector<std::span<const std::size_t>>& out) {
    const std::size_t m = idx.size();
    std::size_t p = 0;
    while (p < m) {
        if (m - p <= target) {  // the tail is too short to be worth cutting again
            out.push_back(idx.subspan(p));
            return;
        }
        // Nominal cut at p + target, then pushed forward to the end of whatever
        // bucket that row belongs to. Pushing forward (never back) keeps every
        // piece at least `target` rows long.
        const std::size_t q = p + target;
        const std::int64_t b = bucket_of(idx[q - 1]);
        std::size_t lo = q;
        std::size_t hi = m;
        while (lo < hi) {
            const std::size_t mid = lo + ((hi - lo) / 2);
            if (bucket_of(idx[mid]) <= b) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        out.push_back(idx.subspan(p, lo - p));
        p = lo;
    }
}

/// One unit of grouped-window work: the rows it is responsible for, plus the
/// rows immediately before them it must evaluate anyway to arrive at the right
/// state — the halo.
///
/// Halo rows are evaluated and then DISCARDED. They belong to the preceding
/// piece of the same group, which emits them itself, so every output row still
/// has exactly one writer. A whole group (or a cut on an aligned bucket
/// boundary) needs no halo and carries `halo == 0`.
struct WindowTask {
    /// The rows this task emits results for.
    std::span<const std::size_t> rows;
    /// How many rows immediately before `rows`, within the same group, must be
    /// evaluated to reach the correct state at the first emitted row.
    std::size_t halo = 0;
};

/// Build the work list for a grouped windowed update: normally one item per
/// group, but a group may be cut into several pieces.
///
/// Parallelism across groups caps the speedup at the group count — three
/// symbols leave thirteen of sixteen cores idle, which is exactly the shape a
/// per-symbol OHLC bar query has. Under an *aligned* window each row's value
/// depends only on rows in its own bucket (see `is_bucket_local_window_expr`),
/// so a group's rows can be cut at any bucket boundary and the pieces run
/// independently: no halo, no overlap, and each piece's rolling state starts
/// where the bucket would have reset it anyway.
///
/// A *trailing* window spans `(t - dur, t]` and straddles any boundary we might
/// pick, so its pieces cannot be independent for free. They can still be
/// independent for a KNOWN PRICE: every field's reach behind a row is bounded by
/// `expr_window_lookback`, so handing a piece the rows within that bound before
/// its first row — its halo — makes its results identical to the whole group's.
/// The halo rows are evaluated for their state and then dropped, since the
/// preceding piece emits them.
///
/// "Identical" there is mathematical, not bitwise, and the same caveat already
/// applies to the aligned cut below. The rolling kernels carry a running
/// accumulator that is never reset — values are added on entry and subtracted on
/// exit — so a piece reaches a row by a different sequence of additions than the
/// whole group does. Floating-point addition is not associative, so a float
/// aggregate can differ in its last bits. Measured on a deliberately awkward
/// input, the halo cut diverges by ~2e-11 relative and the aligned cut, shipped
/// long before it, by ~1e-10. Neither reorders, drops, or recomputes a window's
/// CONTENTS, which is the property the halo actually has to guarantee.
///
/// Splitting is skipped entirely once there are already at least `budget`
/// groups, so the high-cardinality case is untouched *by construction* rather
/// than by tuning. It is also skipped when no cut lands inside the group — a
/// window as long as the data has one bucket and nothing to divide.
[[nodiscard]] auto build_window_tasks(const GroupedRows& group_rows, const Table& input,
                                      ir::Duration duration, bool aligned,
                                      const std::vector<ir::FieldSpec>& fields,
                                      const ExecutionContext& exec, std::size_t rows)
    -> std::vector<WindowTask> {
    std::vector<WindowTask> tasks;
    tasks.reserve(group_rows.group_count());
    for (std::size_t g = 0; g < group_rows.group_count(); ++g) {
        tasks.push_back(WindowTask{.rows = group_rows[g], .halo = 0});
    }

    if (on_worker_pool_thread() || !exec.can_fan_out() || rows < exec.parallel_min_rows) {
        return tasks;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t workers = std::min(budget, pool_size);
    if (workers < 2 || group_rows.group_count() >= workers) {
        return tasks;  // enough groups to keep the pool busy already
    }

    // Aim past the worker count so a group whose buckets divide unevenly can
    // still be balanced by work stealing, but keep each piece large enough that
    // its own gather and scatter dominate the fixed per-piece cost.
    constexpr std::size_t kMinSplitRows = 32768;
    const std::size_t target = std::max(rows / (workers * 4), kMinSplitRows);

    // An aligned clause whose every field is bucket-local is the better cut when
    // it is available: the boundary itself resets the rolling state, so the
    // pieces need no halo at all.
    const bool bucket_local =
        aligned && std::ranges::all_of(fields, [](const ir::FieldSpec& field) {
            return ir::is_bucket_local_window_expr(field.expr);
        });
    if (bucket_local) {
        const auto* tcv = input.find(*input.time_index());
        std::vector<std::span<const std::size_t>> split;
        // Each piece holds at least `target` rows, so this bound is never exceeded.
        split.reserve((rows / target) + tasks.size());
        bool cut = false;
        if (const auto* ts = std::get_if<Column<Timestamp>>(tcv)) {
            const std::int64_t unit = duration.count();
            if (unit > 0) {
                auto bucket_of = [&](std::size_t r) {
                    return window_bucket_index((*ts)[r].nanos, unit);
                };
                for (const auto& task : tasks) {
                    split_at_bucket_bounds(task.rows, target, bucket_of, split);
                }
                cut = true;
            }
        } else if (const auto* dt = std::get_if<Column<Date>>(tcv)) {
            static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
            const std::int64_t unit = duration.count() / kNsPerDay;
            if (unit > 0) {
                auto bucket_of = [&](std::size_t r) {
                    return window_bucket_index((*dt)[r].days, unit);
                };
                for (const auto& task : tasks) {
                    split_at_bucket_bounds(task.rows, target, bucket_of, split);
                }
                cut = true;
            }
        }
        if (cut && split.size() > tasks.size()) {
            std::vector<WindowTask> out;
            out.reserve(split.size());
            for (const auto& piece : split) {
                out.push_back(WindowTask{.rows = piece, .halo = 0});
            }
            return out;
        }
        return tasks;  // nothing divided; keep the plainer work list
    }

    // Otherwise: cut anywhere, and pay a halo. Every field must state a bound.
    std::optional<ir::WindowLookback> lookback = ir::WindowLookback{};
    for (const auto& field : fields) {
        const auto field_bound = ir::expr_window_lookback(field.expr, duration.count());
        if (!field_bound.has_value()) {
            return tasks;  // reads arbitrarily far back; only a whole group is safe
        }
        lookback->nanos = std::max(lookback->nanos, field_bound->nanos);
        lookback->rows = std::max(lookback->rows, field_bound->rows);
    }
    const auto ticks = TimeIndexTicks::of(input);
    if (!ticks.has_value()) {
        return tasks;
    }
    // The lookback is stated in nanoseconds; a Date index counts days. Round the
    // conversion UP — a halo one tick too long is merely wasted work, one tick
    // too short is a wrong answer.
    const std::int64_t tick_lookback = ticks->ticks_from_nanos_ceil(lookback->nanos);

    std::vector<WindowTask> split;
    split.reserve((rows / target) + tasks.size());
    std::size_t halo_rows = 0;
    for (const auto& task : tasks) {
        const auto idx = task.rows;
        for (std::size_t begin = 0; begin < idx.size();) {
            const std::size_t remaining = idx.size() - begin;
            // Absorb a short tail rather than leaving a piece that cannot repay
            // its own halo.
            const std::size_t len = remaining <= target + (target / 2) ? remaining : target;
            std::size_t halo = 0;
            if (begin > 0) {
                // How far back the piece must reach, in rows: the count window's
                // bound directly, and for the duration bound the first row whose
                // tick still falls inside it. The group's rows are
                // time-ascending (established before this runs), so that row is
                // a binary search rather than a scan.
                halo = std::min(lookback->rows, begin);
                if (tick_lookback > 0) {
                    const std::int64_t first = (*ticks)[idx[begin]];
                    std::size_t lo = 0;
                    std::size_t hi = begin;
                    while (lo < hi) {
                        const std::size_t mid = lo + ((hi - lo) / 2);
                        if ((*ticks)[idx[mid]] <= first - tick_lookback) {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    halo = std::max(halo, begin - lo);
                }
            }
            halo_rows += halo;
            split.push_back(WindowTask{.rows = idx.subspan(begin, len), .halo = halo});
            begin += len;
        }
    }
    if (split.size() <= tasks.size()) {
        return tasks;  // nothing divided
    }
    // A window long enough — or ticks dense enough — that the halos rival the
    // data itself turns the split into a loss: the same rows would be gathered
    // and evaluated several times over. One quarter is the point past which the
    // extra evaluation outweighs the cores it buys, and refusing here is what
    // keeps the decision honest for shapes this cannot help.
    if (halo_rows > rows / 4) {
        return tasks;
    }
    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->window_halo_pieces.fetch_add(split.size() - tasks.size(),
                                                          std::memory_order_relaxed);
    }
    return split;
}

/// Per-group windowed update: partition the input by `group_by`, run the
/// regular `windowed_update_table` on each per-group slice, then scatter the
/// new field columns back into a single full-sized output. The rolling
/// buffer therefore never crosses group boundaries.
///
/// Input rows are assumed time-sorted globally (precondition of TimeFrame),
/// which means within each group the sub-table is also time-sorted.
auto grouped_windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                   ir::Duration duration,
                                   const std::vector<ir::ColumnRef>& group_by,
                                   const ScalarRegistry* scalars, const ExternRegistry* externs,
                                   const ExecutionContext& exec, bool aligned)
    -> std::expected<Table, std::string> {
    if (group_by.empty()) {
        return windowed_update_table(std::move(input), fields, duration, scalars, externs, exec,
                                     aligned);
    }
    if (!input.time_index().has_value()) {
        return std::unexpected("window: requires a TimeFrame");
    }
    for (const auto& field : fields) {
        if (field.alias == *input.time_index()) {
            return std::unexpected("cannot update time index column: " + field.alias);
        }
    }

    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* col = input.find(key.name);
        if (col == nullptr) {
            return std::unexpected("window + by: unknown group key '" + key.name +
                                   "' (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(col);
    }
    // Nulls in a group key form their own group; without this a null key would
    // merge into the genuine zero/empty group.
    const auto group_validity = collect_key_validity(input, group_by);

    const std::size_t rows = input.rows();
    if (rows == 0) {
        return windowed_update_table(std::move(input), fields, duration, scalars, externs, exec,
                                     aligned);
    }

    // Bucket rows by group key — the row indices land in original
    // (time-sorted) order within each group, which is the precondition the
    // single-buffer rolling implementation relies on.
    auto row_gid_buf = std::make_unique_for_overwrite<std::uint32_t[]>(rows);
    const std::span<std::uint32_t> row_gid{row_gid_buf.get(), rows};
    const std::size_t group_count =
        assign_group_ids(group_columns, group_validity, rows, exec, row_gid);
    const GroupedRows group_rows = build_grouped_rows(row_gid, group_count, exec);

    // Every window kernel below — and the halo boundaries `build_window_tasks`
    // computes by binary search — reads each group's rows as a time-ascending
    // sequence. Establish that before anything relies on it.
    if (auto ordered = check_grouped_time_order(input, group_by, group_rows); !ordered) {
        return std::unexpected(std::move(ordered.error()));
    }

    // The unit of work is a piece of a group, not necessarily a whole one:
    // `build_window_tasks` may cut a group into several. Everything below treats
    // a piece exactly as it treats a group — the rows it EMITS are disjoint from
    // every other task's, which is all that independence requires. What a piece
    // additionally READS is its halo, and only the gather widens for it.
    const auto tasks = build_window_tasks(group_rows, input, duration, aligned, fields, exec, rows);

    // ── output row order ─────────────────────────────────────────────────
    // A grouped window emits its rows GROUP-MAJOR: all of group 0, then group
    // 1, and so on, each group still time-ascending. It does NOT file results
    // back into the rows they came from.
    //
    // Why: the per-group gather and the write-back are both strided -- a group
    // owns 1-in-`group_count` rows -- so filing results back pays cache-line
    // amplification twice. Permuting once up front makes both halves
    // sequential. Measured at 5M rows: 1.21x at 8 groups, 1.45x at 20, 1.63x at
    // 100. Below the crossover it costs about 9% (0.91x at 3 groups), and that
    // is accepted deliberately: gating on `group_count` would buy the 9% back
    // at the price of an output order that changes when a user's data grows
    // past a threshold. One rule beats two, especially when the second one is
    // invisible to a test suite whose fixtures are all small.
    //
    // Legal because SPEC's ordering rules have a grouped update DROP the
    // ordering constraint. The result is a well-formed TimeFrame keyed
    // (group keys, time) -- strictly MORE than the old path recorded, since it
    // claimed nothing at all.
    //
    // The decision lives here, not in the planner: `group_count` is exact and
    // already computed, whereas `distinct_estimate` covers only integer columns
    // with footer stats and declines on the string group key this is usually
    // about. Nor in the lowerer as a synthesized `order`, which would pay a
    // full radix sort to rediscover the grouping bucketing just produced.
    std::vector<std::size_t> perm;
    std::vector<std::size_t> task_offset(tasks.size() + 1, 0);
    perm.reserve(rows);
    for (std::size_t g = 0; g < tasks.size(); ++g) {
        task_offset[g + 1] = task_offset[g] + tasks[g].rows.size();
        perm.insert(perm.end(), tasks[g].rows.begin(), tasks[g].rows.end());
    }
    // `perm` is a permutation of [0, rows), so it is the identity exactly when
    // it is ascending -- i.e. the input is ALREADY group-major. That is the
    // common case for an explicit `order symbol` upstream, and skipping the
    // copy is what stops this operator permuting a second time for nothing.
    const bool needs_permute = !std::ranges::is_sorted(perm);
    Table permuted;
    if (needs_permute) {
        // The order this actually produces, asserted for the metadata: the
        // group keys, then time within each group.
        std::vector<ir::OrderKey> ordering;
        ordering.reserve(group_by.size() + 1);
        for (const auto& key : group_by) {
            ordering.push_back(ir::OrderKey{.name = key.name, .ascending = true});
        }
        if (input.time_index().has_value()) {
            ordering.push_back(ir::OrderKey{.name = *input.time_index(), .ascending = true});
        }
        permuted = permute_table_rows(input, perm, std::move(ordering), exec);
    }
    const Table& slice_source = needs_permute ? permuted : input;
    // Task `g` owns rows [task_offset[g], task_offset[g + 1]) of `slice_source`,
    // and its results land in the same run. Contiguous either way: when the
    // permutation was skipped it is because those rows were already there.
    std::vector<std::size_t> out_positions(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        out_positions[i] = i;
    }
    auto out_slot = [&](std::size_t g) -> std::span<const std::size_t> {
        return {out_positions.data() + task_offset[g], tasks[g].rows.size()};
    };
    // What the task must READ: its own rows preceded by its halo. The halo rows
    // are the tail of the preceding piece of the same group, and `perm` lays the
    // pieces of a group down in order, so they sit immediately before this
    // task's run — the read slot is simply the write slot extended backwards.
    auto eval_slot = [&](std::size_t g) -> std::span<const std::size_t> {
        return {out_positions.data() + task_offset[g] - tasks[g].halo,
                tasks[g].rows.size() + tasks[g].halo};
    };

    // Only the columns the window expressions actually read need to be gathered
    // into each per-group slice. The gather is strided -- a group's rows are
    // 1-in-`group_count` through the table -- so every column carried along
    // costs a full cache line per row it does not use. `symbol` is the standard
    // case: it is a group key, hence constant within the slice, hence gathered
    // once per row to be read never.
    //
    // Two things must stay in the set beyond the expression references:
    //   * the time index, which the windowing itself needs; and
    //   * any column a field would OVERWRITE. `windowed_update_table` replaces
    //     a same-named column in place instead of appending, and the
    //     append-only assumption below (`first_new_idx`) is what turns its
    //     result back into a list of new fields. Drop such a column from the
    //     slice and the field appends instead, which would then add a DUPLICATE
    //     to `output` -- it already carries every input column.
    robin_hood::unordered_set<std::string> needed;
    if (input.time_index().has_value()) {
        needed.insert(*input.time_index());
    }
    for (const auto& field : fields) {
        ir::collect_expr_column_refs(field.expr, needed);
        needed.insert(field.alias);  // no-op unless it names an input column
    }
    std::vector<const ColumnEntry*> slice_columns;
    slice_columns.reserve(slice_source.columns.size());
    for (const auto& entry : slice_source.columns) {
        if (needed.contains(entry.name)) {
            slice_columns.push_back(&entry);
        }
    }

    auto run_group =
        [&](std::span<const std::size_t> row_idx) -> std::expected<Table, std::string> {
        Table sub;
        for (const auto* entry_ptr : slice_columns) {
            const auto& entry = *entry_ptr;
            // Serial by decision, not omission: `run_group` is itself one task
            // of a per-group fan-out, so a nested split would only oversubscribe
            // (and `for_row_ranges` would refuse it anyway), and a single
            // group's slice is far below the row floor that makes a split pay.
            ColumnValue gathered =
                gather_column(*entry.column, row_idx.data(), row_idx.size(), nullptr);
            // Carry each input column's validity into the per-group slice — else a
            // rolling/lag field over a nullable input column (e.g. a computed
            // log-return whose first per-symbol row is null) would see a slice
            // with no validity bitmap and read the undefined null payload.
            if (entry.validity.has_value()) {
                ValidityBitmap vb(row_idx.size(), true);
                for (std::size_t k = 0; k < row_idx.size(); ++k) {
                    vb.set(k, (*entry.validity)[row_idx[k]]);
                }
                sub.add_column(entry.name, std::move(gathered), std::move(vb));
            } else {
                sub.add_column(entry.name, std::move(gathered));
            }
        }
        // A per-group slice: it inherits the time index, but deliberately NOT
        // the grouping -- a single group has no boundary to read across, and
        // claiming one would make the row-order guard reject a correct
        // unpartitioned call inside the slice.
        if (input.time_index().has_value()) {
            sub.set_properties(TableProperties::time_frame(*input.time_index()));
        }
        return windowed_update_table(std::move(sub), fields, duration, scalars, externs, exec,
                                     aligned);
    };

    // Run the first group to learn the new field column types/names.
    auto first = run_group(eval_slot(0));
    if (!first.has_value()) {
        return std::unexpected(first.error());
    }
    // The slice carries only `slice_columns`, so that -- not the full input
    // width -- is where the appended fields begin.
    const std::size_t first_new_idx = slice_columns.size();
    auto written_field_names = written_field_names_for(*first, first_new_idx, fields);
    if (written_field_names.empty()) {
        return std::unexpected("window: grouped update produced no columns");
    }

    std::vector<bool> variable_output(written_field_names.size(), false);
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        const auto* sample = first->find(written_field_names[f]);
        if (sample == nullptr) {
            return std::unexpected("window: missing new column '" + written_field_names[f] +
                                   "' in sub-result");
        }
        variable_output[f] = std::holds_alternative<Column<std::string>>(*sample) ||
                             std::holds_alternative<Column<Categorical>>(*sample);
    }
    struct VariableTaskPiece {
        std::vector<std::optional<ColumnEntry>> fields;
    };
    std::vector<VariableTaskPiece> variable_pieces(
        tasks.size(), VariableTaskPiece{.fields = std::vector<std::optional<ColumnEntry>>(
                                            written_field_names.size())});

    // Allocate fixed-width output columns at full size. Variable-width results
    // stay task-local until every window has completed; assembling them once in
    // output order preserves their packed storage and dictionary contracts.
    //
    // Shares column pointers with its source, exactly as the time-major path
    // shares them with `input`. NOT a move: `slice_source` still refers to
    // `permuted`, and the remaining groups are cut from it below.
    Table output = needs_permute ? permuted : input;
    auto allocate_full = [&](const ColumnValue& sample) -> std::expected<ColumnValue, std::string> {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                              std::is_same_v<ColT, Column<Categorical>>) {
                    return std::unexpected(
                        "window: variable-width result reached fixed-width path");
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                } else if constexpr (std::is_trivially_default_constructible_v<
                                         typename ColT::value_type>) {
                    // The groups partition every row, so each output element is
                    // written by exactly one scatter. Value-initialising first
                    // would be a second full pass over the column for nothing —
                    // at 5M rows and four computed fields that was ~35ms of
                    // memset, all of it serial.
                    ColT out;
                    out.resize_for_overwrite(rows);
                    return ColumnValue{std::move(out)};
                } else {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                }
            },
            sample);
    };
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (variable_output[f]) {
            continue;
        }
        auto full = allocate_full(*first->find(written_field_names[f]));
        if (!full.has_value()) {
            return std::unexpected(full.error());
        }
        output.add_column(written_field_names[f], std::move(full.value()));
    }

    // `skip` is the task's halo: rows the sub-result computed for their state
    // and which the preceding piece owns. Everything downstream of the window
    // itself reads the sub-result from there.
    auto scatter_into = [](ColumnValue& dst, const ColumnValue& src,
                           std::span<const std::size_t> indices,
                           std::size_t skip) -> std::optional<std::string> {
        return std::visit(
            [&](auto& dcol) -> std::optional<std::string> {
                using DT = std::decay_t<decltype(dcol)>;
                const DT* scol = std::get_if<DT>(&src);
                if (scol == nullptr) {
                    return "window: type mismatch in grouped scatter";
                }
                if constexpr (std::is_same_v<DT, Column<std::string>> ||
                              std::is_same_v<DT, Column<Categorical>>) {
                    return "window: variable-width scatter reached fixed-width path";
                } else if constexpr (std::is_same_v<DT, Column<bool>>) {
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dcol.set(indices[i], (*scol)[skip + i]);
                    }
                } else {
                    auto* dp = dcol.data();
                    const auto* sp = scol->data() + skip;
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dp[indices[i]] = sp[i];
                    }
                }
                return std::nullopt;
            },
            dst);
    };

    // Lazy-allocated per-field validity bitmaps. We only construct one if at
    // least one group's sub-result has a validity bitmap for that field —
    // most pure-arithmetic outputs stay all-valid and pay nothing. The
    // laziness is what decides the OUTPUT REPRESENTATION (bitmap vs no
    // bitmap), so it has to survive parallelism unchanged rather than being
    // traded for a simpler eager allocation.
    std::vector<std::optional<ValidityBitmap>> output_validity(written_field_names.size());

    // Allocation happens at most once per field, so a mutex around just that is
    // uncontended in practice; the bit writes then proceed outside it. The
    // vector is pre-sized and never resized, so the pointer stays valid.
    std::mutex validity_mutex;
    auto ensure_validity = [&](std::size_t f_idx) -> ValidityBitmap* {
        const std::scoped_lock lock(validity_mutex);
        if (!output_validity[f_idx].has_value()) {
            output_validity[f_idx] = ValidityBitmap(rows, true);
        }
        return &*output_validity[f_idx];
    };

    auto scatter_validity = [&](std::size_t f_idx, const Table& sub_table,
                                std::span<const std::size_t> indices, std::size_t skip) {
        const auto* sub_entry = sub_table.find_entry(written_field_names[f_idx]);
        if (sub_entry == nullptr || !sub_entry->validity.has_value()) {
            return;
        }
        auto* out_bm = ensure_validity(f_idx);
        const auto& sub_bm = *sub_entry->validity;
        auto* words = out_bm->words_data();
        const auto* validity_bytes = sub_bm.buffer_data();
        const std::size_t validity_offset = sub_bm.buffer_offset() + skip;
        for (std::size_t i = 0; i < indices.size(); ++i) {
            const std::size_t bit = validity_offset + i;
            if (((validity_bytes[bit / 8] >> (bit % 8)) & 0x01U) == 0U) {
                clear_validity_bit(words, indices[i]);
            }
        }
    };

    // Resolved once: `find` walks a hash map, and every group would otherwise
    // repeat that per field.
    std::vector<ColumnValue*> dst_columns(written_field_names.size(), nullptr);
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (!variable_output[f]) {
            dst_columns[f] = output.find(written_field_names[f]);
        }
    }

    // One group's whole contribution: run the windowed update over its slice,
    // then scatter the new columns into the rows it owns. Groups own disjoint
    // rows, which is exactly what makes them independent of each other.
    auto scatter_group = [&](std::size_t task, const Table& sub,
                             std::span<const std::size_t> indices) -> std::optional<std::string> {
        const std::size_t skip = tasks[task].halo;
        for (std::size_t f = 0; f < written_field_names.size(); ++f) {
            const auto* src = sub.find_entry(written_field_names[f]);
            if (src == nullptr) {
                return "window: missing column '" + written_field_names[f] +
                       "' in grouped sub-result";
            }
            if (variable_output[f]) {
                variable_pieces[task].fields[f] = *src;
                continue;
            }
            if (auto err = scatter_into(*dst_columns[f], *src->column, indices, skip)) {
                return err;
            }
            scatter_validity(f, sub, indices, skip);
        }
        return std::nullopt;
    };

    if (auto err = scatter_group(0, *first, out_slot(0))) {
        return std::unexpected(*err);
    }

    const std::size_t workers = grouped_window_worker_count(exec, tasks.size() - 1, rows, *first,
                                                            written_field_names, fields);
    if (workers < 2) {
        for (std::size_t g = 1; g < tasks.size(); ++g) {
            auto sub = run_group(eval_slot(g));
            if (!sub.has_value()) {
                return std::unexpected(sub.error());
            }
            if (auto err = scatter_group(g, *sub, out_slot(g))) {
                return std::unexpected(*err);
            }
        }
    } else {
        if (exec.parallel_stats != nullptr) {
            exec.parallel_stats->parallel_group_windows.fetch_add(1, std::memory_order_relaxed);
        }
        // Groups are claimed from one cursor rather than pre-assigned: group
        // sizes are data-dependent and can differ by orders of magnitude, so a
        // static split would leave workers idle behind the largest group.
        std::atomic<std::size_t> cursor{1};
        std::mutex error_mutex;
        std::optional<std::string> failure;
        std::size_t failure_group = 0;
        auto& pool = process_worker_pool();
        {
            auto batch = pool.submit(workers, [&](std::size_t) noexcept {
                while (true) {
                    const std::size_t g = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (g >= tasks.size()) {
                        return;
                    }
                    {
                        // Abandon only groups above a recorded failure, so the
                        // reported error never depends on thread timing.
                        const std::scoped_lock lock(error_mutex);
                        if (failure.has_value() && failure_group < g) {
                            return;
                        }
                    }
                    std::optional<std::string> err;
                    try {
                        auto sub = run_group(eval_slot(g));
                        if (!sub.has_value()) {
                            err = std::move(sub.error());
                        } else {
                            err = scatter_group(g, *sub, out_slot(g));
                        }
                    } catch (const std::exception& e) {
                        err = std::string("window + by: worker exception: ") + e.what();
                    } catch (...) {
                        err = std::string("window + by: worker threw a non-standard exception");
                    }
                    if (err.has_value()) {
                        const std::scoped_lock lock(error_mutex);
                        if (!failure.has_value() || g < failure_group) {
                            failure = std::move(err);
                            failure_group = g;
                        }
                    }
                }
            });
            batch.wait();
        }
        if (failure.has_value()) {
            return std::unexpected(std::move(*failure));
        }
    }

    // Attach the lazy validity bitmaps to their output column entries.
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (!output_validity[f].has_value()) {
            continue;
        }
        auto idx_it = output.index.find(written_field_names[f]);
        if (idx_it != output.index.end()) {
            output.columns[idx_it->second].validity = std::move(output_validity[f]);
        }
    }

    // Tasks already describe the final group-major order, so walk their
    // contiguous output runs rather than reconstructing original row indices.
    // This is also what preserves categorical dictionary insertion order.
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (!variable_output[f]) {
            continue;
        }
        const bool has_validity = std::ranges::any_of(variable_pieces, [&](const auto& piece) {
            return piece.fields[f]->validity.has_value();
        });
        if (std::holds_alternative<Column<std::string>>(*variable_pieces[0].fields[f]->column)) {
            std::size_t total_chars = 0;
            for (std::size_t task = 0; task < tasks.size(); ++task) {
                const auto& source =
                    std::get<Column<std::string>>(*variable_pieces[task].fields[f]->column);
                // From the halo, not from zero: the leading rows of this
                // sub-result belong to the preceding piece, which emits them.
                for (std::size_t local = tasks[task].halo; local < source.size(); ++local) {
                    const std::size_t count =
                        source.offsets_data()[local + 1] - source.offsets_data()[local];
                    if (count > std::numeric_limits<std::uint32_t>::max() - total_chars) {
                        return std::unexpected("window + by: string output exceeds uint32 offsets");
                    }
                    total_chars += count;
                }
            }
            Column<std::string> result;
            result.resize_for_gather(rows, total_chars);
            auto* offsets = result.offsets_data();
            char* chars = result.chars_data();
            offsets[0] = 0;
            std::uint32_t cursor = 0;
            std::size_t row = 0;
            std::optional<ValidityBitmap> validity;
            if (has_validity) {
                validity = ValidityBitmap(rows, true);
            }
            for (std::size_t task = 0; task < tasks.size(); ++task) {
                const auto& entry = *variable_pieces[task].fields[f];
                const auto& source = std::get<Column<std::string>>(*entry.column);
                for (std::size_t local = tasks[task].halo; local < source.size(); ++local, ++row) {
                    const auto begin = source.offsets_data()[local];
                    const auto end = source.offsets_data()[local + 1];
                    if (end != begin) {
                        std::memcpy(chars + cursor, source.chars_data() + begin, end - begin);
                    }
                    cursor += end - begin;
                    offsets[row + 1] = cursor;
                    if (validity.has_value() && entry.validity.has_value()) {
                        validity->set(row, (*entry.validity)[local]);
                    }
                }
            }
            if (validity.has_value()) {
                output.add_column(written_field_names[f], ColumnValue{std::move(result)},
                                  std::move(*validity));
            } else {
                output.add_column(written_field_names[f], ColumnValue{std::move(result)});
            }
            continue;
        }

        using CategoricalColumn = Column<Categorical>;
        using Code = CategoricalColumn::code_type;
        constexpr Code kUnmapped = -1;
        CategoricalColumn result;
        result.reserve(rows);
        std::vector<std::vector<Code>> remaps(tasks.size());
        for (std::size_t task = 0; task < tasks.size(); ++task) {
            const auto& source =
                std::get<CategoricalColumn>(*variable_pieces[task].fields[f]->column);
            remaps[task].assign(source.dictionary_size(), kUnmapped);
        }
        std::optional<ValidityBitmap> validity;
        if (has_validity) {
            validity = ValidityBitmap(rows, true);
        }
        std::size_t row = 0;
        for (std::size_t task = 0; task < tasks.size(); ++task) {
            const auto& entry = *variable_pieces[task].fields[f];
            const auto& source = std::get<CategoricalColumn>(*entry.column);
            auto& remap = remaps[task];
            for (std::size_t local = tasks[task].halo; local < source.size(); ++local, ++row) {
                const Code input_code = source.code_at(local);
                if (input_code >= 0 && static_cast<std::size_t>(input_code) < remap.size() &&
                    remap[static_cast<std::size_t>(input_code)] != kUnmapped) {
                    result.push_code(remap[static_cast<std::size_t>(input_code)]);
                } else {
                    result.push_back(source[local]);
                    if (input_code >= 0 && static_cast<std::size_t>(input_code) < remap.size()) {
                        remap[static_cast<std::size_t>(input_code)] =
                            result.code_at(result.size() - 1);
                    }
                }
                if (validity.has_value() && entry.validity.has_value()) {
                    validity->set(row, (*entry.validity)[local]);
                }
            }
        }
        if (validity.has_value()) {
            output.add_column(written_field_names[f], ColumnValue{std::move(result)},
                              std::move(*validity));
        } else {
            output.add_column(written_field_names[f], ColumnValue{std::move(result)});
        }
    }

    // State what the rows actually are: one contiguous run per group, ordered
    // (group keys..., time). The permuting branch asserted exactly that when it
    // built `permuted`; skipping the permutation means the input was ALREADY
    // group-major, so its own ordering stands.
    //
    // The grouping claim matters for the same reason: an unpartitioned lag/lead
    // downstream would read across a run boundary. It holds whether or not the
    // permutation ran. The condition is the group count -- a single group has no
    // boundary to read across, and claiming a grouping there would reject a
    // correct unpartitioned lead.
    //
    // Both land through `apply_table_properties`, which sets `grouped_by` before
    // `normalize_time_index` consults it, so the ordering asserted here survives
    // instead of being rewritten to "time index ascending" (false for
    // group-major rows).
    std::vector<std::string> grouping;
    if (tasks.size() > 1) {
        grouping.reserve(group_by.size());
        for (const auto& key : group_by) {
            grouping.push_back(key.name);
        }
    }
    // The fate applies the overwrite rule, exactly as the ungrouped update
    // does: a field that rewrites a column's values leaves it present but no
    // longer a valid key, so any ordering naming it is void. `Preserve` is the
    // ROW story (an update adds no rows, removes none, moves none); the column
    // story is the fate's, and stating only the first would let a stale key
    // survive its own overwrite.
    apply_table_properties(
        output,
        TableProperties::derive(
            TableProperties::recovered(needs_permute ? permuted.ordering() : input.ordering(),
                                       output.time_index(), std::move(grouping)),
            [&](const std::string& name) -> KeyFate {
                const bool overwritten = std::ranges::any_of(
                    fields, [&](const ir::FieldSpec& f) { return f.alias == name; });
                return overwritten ? KeyFate::overwritten() : KeyFate::kept(name);
            },
            RowTransform::Preserve));
    return output;
}

namespace {

/// Evaluate one update field across worker threads, or serially when that is
/// not worthwhile or not safe.
///
/// This is where a 1:1 operator wants its parallelism, rather than in a
/// morsel pipeline. `update_table` builds its output by *moving* the input
/// (`Table output = std::move(input)`), so a passthrough column costs nothing —
/// which means an pipeline's per-morsel gather and its merge concat are not "one
/// copy too many" for this shape, they are pure overhead invented by
/// morselization. Splitting only the field computation leaves the zero-copy
/// passthrough intact and adds one copy of the computed column, instead of two
/// copies of the entire table.
///
/// Declines, falling back to a single whole-range evaluation, when:
///   - parallelism is off, the table is small, or the pool has one thread;
///   - the expression is not `is_range_native_expr` — evaluating it per range
///     would re-read the whole table per range (see that function);
///   - the result cannot be written into a fixed-width window or the supported
///     string interpolation slab. Categorical results still need their
///     per-piece dictionaries merged, and a wrong merge is silent.
auto evaluate_field_maybe_parallel(const ir::Expr& expr, const Table& table,
                                   const ColumnEvalCtx& ctx, const ExecutionContext& exec)
    -> std::expected<ComputedColumn, std::string> {
    const std::size_t rows = table.rows();
    const auto whole = [&] { return evaluate_field(expr, table, RowRange::whole(rows), ctx); };
    const PredicateInput direct_input(table);
    // The categorical arm is planned before the size gates because the
    // whole-range route below needs the same output dictionary the split route
    // would build, and building that twice is the one part of planning that is
    // not free. The other arms are planned only if this field is actually going
    // to be split.
    kernel::DirectFieldRoute route;
    route.categorical = kernel::try_plan_direct_categorical_field(expr, direct_input, ctx.scalars);
    const auto whole_categorical = [&]() -> std::expected<ComputedColumn, std::string> {
        if (!route.categorical.has_value()) {
            return whole();
        }
        std::vector<Column<Categorical>::code_type> codes(rows, 0);
        auto validity = kernel::write_direct_categorical_field_range(
            *route.categorical, direct_input, RowRange::whole(rows), ctx.scalars,
            {.codes = codes.data(), .begin = 0, .count = rows});
        if (!validity.has_value()) {
            return std::unexpected(std::move(validity.error()));
        }
        Column<Categorical> categorical(route.categorical->dictionary, route.categorical->index,
                                        std::move(codes));
        return ComputedColumn{.column = ColumnValue{std::move(categorical)},
                              .validity = std::move(*validity)};
    };

    // Reentrancy: the pipeline's fused FilterUpdateProject operator calls
    // update_table from a worker thread, so this can be reached on one.
    // Submitting from there deadlocks the pool (WorkerPool::submit aborts
    // rather than let it happen), and the morsel is already one worker's share
    // of the table — splitting it again would only oversubscribe.
    if (on_worker_pool_thread()) {
        return whole_categorical();
    }
    if (!exec.can_fan_out() || rows < exec.parallel_min_rows) {
        return whole_categorical();
    }
    auto inferred = infer_expr_type(expr, table, ctx.scalars, ctx.externs);
    if (!inferred.has_value()) {
        return whole();
    }
    if (!route.categorical.has_value()) {
        route = kernel::plan_direct_field(expr, direct_input, ctx.scalars);
    }
    // The fallback below evaluates one range at a time, so an expression that
    // is not range-native would re-read the whole table per range; and a result
    // this splitter cannot pre-size a destination for has no window to write.
    if ((!is_range_native_expr(expr) && !route.has_plan()) ||
        (inferred.value() != ExprType::Int && inferred.value() != ExprType::Double &&
         inferred.value() != ExprType::Bool &&
         (inferred.value() != ExprType::Categorical || !route.categorical.has_value()) &&
         !route.string.has_value()) ||
        (inferred.value() == ExprType::Bool && !route.predicate.has_value())) {
        return whole();
    }

    // Everything the direct vocabulary does not cover: the compiled numeric
    // writers first, then the general evaluator with a copy into the window.
    // This is the one part of a split field that needs the Table itself, which
    // is why it is the caller's to supply.
    std::atomic<bool> direct_numeric_writer{false};
    const kernel::DirectFieldRangeWriter fallback = [&](RowRange range,
                                                        kernel::NumericOutputSpan window)
        -> std::expected<std::optional<ValidityBitmap>, std::string> {
        if (try_write_fast_update_binary(expr, table, range, inferred.value(), ctx.scalars,
                                         window.ints, window.doubles) ||
            try_write_compiled_numeric_update_expr(expr, table, range, inferred.value(),
                                                   ctx.scalars, window.ints, window.doubles)) {
            direct_numeric_writer.store(true, std::memory_order_relaxed);
            return collect_expr_validity(expr, PredicateInput(table), range);
        }
        auto piece = evaluate_field(expr, table, range, ctx);
        if (!piece.has_value()) {
            return std::unexpected(std::move(piece.error()));
        }
        // `infer_expr_type` chose the destination, so a morsel of another type
        // means the two disagree. Report it rather than throwing out of a
        // worker.
        const void* src = nullptr;
        if (window.ints != nullptr) {
            if (const auto* ints = std::get_if<Column<std::int64_t>>(&piece->column)) {
                src = ints->data();
            }
        } else if (const auto* doubles = std::get_if<Column<double>>(&piece->column)) {
            src = doubles->data();
        }
        if (src == nullptr) {
            return std::unexpected(
                std::string("evaluate_field_maybe_parallel: morsel column type does not match "
                            "the inferred result type"));
        }
        void* at = window.ints != nullptr ? static_cast<void*>(window.ints)
                                          : static_cast<void*>(window.doubles);
        std::memcpy(at, src, range.count * sizeof(std::int64_t));
        return std::move(piece->validity);
    };

    auto windows =
        kernel::evaluate_field_windows(expr, route, direct_input, inferred.value(), ctx.scalars,
                                       exec, &fallback, &direct_numeric_writer);
    if (!windows.has_value()) {
        return std::unexpected(std::move(windows.error()));
    }
    if (!windows->has_value()) {
        return whole_categorical();  // the split was declined, not attempted
    }
    return std::move(**windows);
}

}  // namespace

auto update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                  const ExecutionContext& exec) -> std::expected<Table, std::string> {
    Table output = std::move(input);
    if (output.time_index().has_value()) {
        if (auto ok = check_row_order(output, fields); !ok) {
            return std::unexpected(ok.error());
        }
        for (const auto& field : fields) {
            if (field.alias == *output.time_index()) {
                return std::unexpected("cannot update time index column: " + field.alias);
            }
        }
    }
    const std::size_t rows = output.rows();
    for (const auto& field : fields) {
        if (const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node)) {
            auto res = evaluate_rank_column(output, *rank, {}, exec);
            if (!res) {
                return std::unexpected(res.error());
            }
            add_computed_column(output, field.alias, std::move(*res));
            continue;
        }
        if (const auto* col_ref = std::get_if<ir::ColumnRef>(&field.expr.node)) {
            const auto* entry = col_ref->lexical ? nullptr : output.find_entry(col_ref->name);
            if (entry != nullptr) {
                // `alias = other_column` renames rather than computes, so the
                // two names can share one buffer under the copy-on-write
                // invariant. Deep-copying here moved 26MB per renamed key
                // column on a q03-shaped scan — the join-key alignment idiom
                // (`select { o_orderkey = l_orderkey }`) hits this on nearly
                // every query.
                output.add_column_from(field.alias, *entry);
                continue;
            }
            if (scalars != nullptr) {
                if (auto it = scalars->find(col_ref->name); it != scalars->end()) {
                    output.add_column(field.alias, broadcast_scalar_column(it->second, rows));
                    continue;
                }
            }
            return std::unexpected("unknown column '" + col_ref->name + "'");
        }
        // Everything else — whole-column builtins, vectorized validity-aware
        // fields, the numeric fast path, the per-row loop — goes through the
        // shared field evaluator. No enclosing `window` clause here, so
        // ctx.window stays empty (only a per-call window arg can supply a
        // rolling span).
        auto col = evaluate_field_maybe_parallel(
            field.expr, output,
            ColumnEvalCtx{
                .scalars = scalars, .externs = externs, .window = std::nullopt, .exec = &exec},
            exec);
        if (!col) {
            return std::unexpected(col.error());
        }
        add_computed_column(output, field.alias, std::move(*col));
    }
    // A field that writes a sort key's values in place invalidates it as a key
    // even though the column stays present (the overwrite rule); the time index
    // cannot be updated (rejected above), so it always survives. The column loop
    // never touches the metadata fields, so `output` still carries the input's.
    apply_table_properties(
        output, TableProperties::derive(
                    table_properties_of(output),
                    [&](const std::string& name) -> KeyFate {
                        const bool overwritten = std::ranges::any_of(
                            fields, [&](const ir::FieldSpec& f) { return f.alias == name; });
                        return overwritten ? KeyFate::overwritten() : KeyFate::kept(name);
                    },
                    RowTransform::Preserve));
    return output;
}

struct GuardedWriteTarget {
    std::optional<std::size_t> existing;
};

/// Guarded writes retain the physical representation of their target: unmatched
/// rows still come from that target.  Strings are the language-level value type
/// of a categorical expression, so encode them before the representation check
/// when they are being written into an existing categorical column.
auto preserve_categorical_target(const ColumnEntry* target, std::shared_ptr<ColumnValue> values)
    -> std::shared_ptr<ColumnValue> {
    if (target == nullptr || !std::holds_alternative<Column<Categorical>>(*target->column) ||
        !std::holds_alternative<Column<std::string>>(*values)) {
        return values;
    }

    const auto& strings = std::get<Column<std::string>>(*values);
    Column<Categorical> categorical;
    categorical.reserve(strings.size());
    for (std::size_t row = 0; row < strings.size(); ++row) {
        categorical.push_back(strings[row]);
    }
    return std::make_shared<ColumnValue>(std::move(categorical));
}

/// The one numeric representation change permitted by a guarded write is the
/// standard Int-to-Double promotion. It gives the matched floating values and
/// untouched integer values the same representation as a mixed numeric CASE.
auto promote_integer_target(const std::shared_ptr<ColumnValue>& target,
                            const std::shared_ptr<ColumnValue>& values)
    -> std::shared_ptr<ColumnValue> {
    if (target == nullptr || !std::holds_alternative<Column<std::int64_t>>(*target) ||
        !std::holds_alternative<Column<double>>(*values)) {
        return target;
    }

    const auto& integers = std::get<Column<std::int64_t>>(*target);
    Column<double> widened;
    widened.resize_for_overwrite(integers.size());
    for (std::size_t row = 0; row < integers.size(); ++row) {
        widened[row] = static_cast<double>(integers[row]);
    }
    return std::make_shared<ColumnValue>(std::move(widened));
}

auto is_guarded_numeric_promotion(const ColumnValue& target, const ColumnValue& values) -> bool {
    return std::holds_alternative<Column<std::int64_t>>(target) &&
           std::holds_alternative<Column<double>>(values);
}

/// Validate the stable side of a guarded write before its variant-specific
/// scatter starts. A guarded update interleaves new values with old ones, so an
/// existing target cannot change representation except for the standard
/// Int-to-Double promotion above. This also keeps the time-index write
/// prohibition at the one point guarded fields land.
auto prepare_guarded_write(const Table& output, const std::string& alias, const ColumnValue& values)
    -> std::expected<GuardedWriteTarget, std::string> {
    if (output.time_index().has_value() && alias == *output.time_index()) {
        return std::unexpected("cannot update time index column: " + alias);
    }
    if (const auto it = output.index.find(alias); it != output.index.end()) {
        if (output.columns[it->second].column->index() != values.index() &&
            !is_guarded_numeric_promotion(*output.columns[it->second].column, values)) {
            return std::unexpected("guarded update cannot change the type of existing column '" +
                                   alias + "'");
        }
        return GuardedWriteTarget{.existing = it->second};
    }
    return GuardedWriteTarget{};
}

/// The sole metadata landing point for a guarded field. Its target was checked
/// before the representation-specific merge, which guarantees that the mixed
/// old/new column is type-stable when it reaches this writer.
auto write_guarded_update(Table& output, const std::string& alias, GuardedWriteTarget target,
                          ColumnValue values, std::optional<ValidityBitmap> validity) -> void {
    if (target.existing.has_value()) {
        output.replace_column(*target.existing, std::move(values), std::move(validity));
    } else if (validity.has_value()) {
        output.add_column(alias, std::move(values), std::move(*validity));
    } else {
        output.add_column(alias, std::move(values));
    }
    apply_table_properties(output, TableProperties::derive(
                                       table_properties_of(output),
                                       [&](const std::string& name) -> KeyFate {
                                           return name == alias ? KeyFate::overwritten()
                                                                : KeyFate::kept(name);
                                       },
                                       RowTransform::Preserve));
}

/// Execute a guarded update `where <predicate> update { ... }`: rows matching
/// the predicate get the field assignments; non-matching rows keep their values
/// (a new column is null off-mask). Each field is evaluated where it is needed —
/// row-local (Scalar-only, `is_subset_evaluable_expr`) fields on just the
/// matching rows (gather/scatter); non-row-local fields (lag/rolling/rank/...)
/// over the full table, then selected by the mask. Both produce the same result;
/// row-locality only decides where the work happens.
auto apply_guarded_update(Table input, const ir::UpdateNode& update, const ScalarRegistry* scalars,
                          const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    if (!update.group_by().empty()) {
        return std::unexpected("guarded update (where ... update) does not support `by` yet");
    }
    if (!update.tuple_fields().empty()) {
        return std::unexpected(
            "guarded update (where ... update) does not support tuple-bound fields yet");
    }
    const std::size_t n = input.rows();

    // Mask: a row matches iff the predicate is true AND not null.
    auto mask = compute_mask(*update.guard(), input, scalars, RowRange::whole(n));
    if (!mask) {
        return std::unexpected(mask.error());
    }
    // Fold the 3VL validity into the mask bytes. Every consumer below asks the
    // same question — "true and not null?" — so answering it once turns each of
    // them into a single dense byte read instead of a byte read plus a
    // conditional bit probe, and lets the literal path below vectorize.
    if (mask->valid.has_value()) {
        const std::uint8_t* mask_valid = mask->valid->data();
        std::uint8_t* bytes = mask->value.data();
        for (std::size_t i = 0; i < n; ++i) {
            bytes[i] = static_cast<std::uint8_t>(bytes[i] != 0 && mask_valid[i] != 0);
        }
        mask->valid.reset();
    }
    const std::uint8_t* matched_bytes = mask->value.data();

    // Common CASE-WHEN shape: replacing values in an existing, non-null,
    // fixed-width column with a literal. The general guarded-update path below
    // is deliberately able to evaluate arbitrary row-local expressions on a
    // gathered subset, but that is pure overhead for `where price > x update
    // { price = y }`: it builds an index, gathers every input column, broadcasts
    // the literal, deep-copies the old column, and scatters into another full
    // column. Copy the required output column once and apply the mask directly.
    // Restrict this to a single assignment so the snapshot semantics of a
    // multi-field guarded update remain unchanged.
    if (update.fields().size() == 1) {
        const auto& field = update.fields().front();
        const auto* literal = std::get_if<ir::Literal>(&field.expr.node);
        const ColumnEntry* old_entry = input.find_entry(field.alias);
        if (literal != nullptr && old_entry != nullptr) {
            ColumnValue replacement =
                broadcast_scalar_column(scalar_from_literal(*literal), std::size_t{1});
            auto replacement_values = preserve_categorical_target(
                old_entry, std::make_shared<ColumnValue>(std::move(replacement)));
            auto old_values = promote_integer_target(old_entry->column, replacement_values);
            auto target = prepare_guarded_write(input, field.alias, *replacement_values);
            if (!target.has_value()) {
                return std::unexpected(std::move(target.error()));
            }

            using LiteralResult = std::pair<ColumnValue, std::optional<ValidityBitmap>>;
            const ValidityBitmap* old_validity =
                old_entry->validity.has_value() ? &*old_entry->validity : nullptr;
            auto result = std::visit(
                [&](const auto& old_col) -> std::optional<LiteralResult> {
                    using Col = std::decay_t<decltype(old_col)>;
                    if constexpr (std::is_same_v<Col, Column<std::string>> ||
                                  std::is_same_v<Col, Column<Categorical>>) {
                        // Flat strings cannot be overwritten in place, and a
                        // string literal is not dictionary-compatible with a
                        // categorical column. Keep their established path.
                        return std::nullopt;
                    } else {
                        const auto& scalar_col = std::get<Col>(*replacement_values);
                        const auto value = scalar_col[0];

                        Col out;
                        if constexpr (is_dense_column_v<Col>) {
                            // Select each output element from the guard in one
                            // pass. Copying the old column and then scattering
                            // the literal into it writes every element once and
                            // then rewrites the matched ones; this writes each
                            // exactly once, and because the guard is a dense
                            // byte array with no branch left in the body it
                            // vectorizes into a blend. Resolving the storage up
                            // front also drops the per-element re-test of
                            // whether the column holds adopted (Arrow) buffers,
                            // which cost more than the store it guarded.
                            using Value = typename Col::value_type;
                            if constexpr (std::is_trivially_default_constructible_v<Value>) {
                                out.resize_for_overwrite(n);
                            } else {
                                // `Timestamp`/`Date` default their member, so
                                // they cannot skip the fill; the select below
                                // still writes every slot exactly once.
                                out.resize(n);
                            }
                            typename Col::value_type* dst = out.data();
                            const typename Col::value_type* src = old_col.data();
                            for (std::size_t i = 0; i < n; ++i) {
                                dst[i] = matched_bytes[i] != 0 ? value : src[i];
                            }
                        } else {
                            // `Column<bool>` reaches here and is bit-packed, so
                            // it keeps copy-then-scatter through the indexed
                            // accessor.
                            out = old_col;
                            for (std::size_t i = 0; i < n; ++i) {
                                if (matched_bytes[i] != 0) {
                                    out[i] = value;
                                }
                            }
                        }

                        if (old_validity == nullptr) {
                            return LiteralResult{ColumnValue{std::move(out)}, std::nullopt};
                        }

                        // A nullable target still fits this shape: a non-null
                        // literal makes every matched row valid, and the rest
                        // keep the old value and the old validity bit —
                        // `where is_null(x) update { x = <lit> }` is a common
                        // idiom. Merge the two a word at a time rather than
                        // copying the old bitmap and setting a bit per matched
                        // row.
                        ValidityBitmap out_valid;
                        out_valid.resize(n);
                        bool any_invalid = false;
                        if (!old_validity->is_external()) {
                            constexpr std::size_t kBits = 64;
                            const std::uint64_t* src = old_validity->words_data();
                            std::uint64_t* dst = out_valid.words_data();
                            const std::size_t words = (n + kBits - 1) / kBits;
                            for (std::size_t w = 0; w < words; ++w) {
                                const std::size_t base = w * kBits;
                                const std::size_t len = std::min(kBits, n - base);
                                std::uint64_t bits = 0;
                                for (std::size_t b = 0; b < len; ++b) {
                                    bits |= static_cast<std::uint64_t>(matched_bytes[base + b] != 0)
                                            << b;
                                }
                                const std::uint64_t all = len == kBits
                                                              ? ~std::uint64_t{0}
                                                              : ((std::uint64_t{1} << len) - 1);
                                const std::uint64_t merged = (src[w] | bits) & all;
                                dst[w] = merged;
                                any_invalid = any_invalid || merged != all;
                            }
                        } else {
                            // An adopted Arrow bitmap may sit at a bit offset
                            // and need not be word-aligned; read it bit by bit.
                            for (std::size_t i = 0; i < n; ++i) {
                                if (matched_bytes[i] != 0 || (*old_validity)[i]) {
                                    out_valid.set(i, true);
                                } else {
                                    any_invalid = true;
                                }
                            }
                        }
                        return LiteralResult{
                            ColumnValue{std::move(out)},
                            any_invalid ? std::optional<ValidityBitmap>{std::move(out_valid)}
                                        : std::nullopt};
                    }
                },
                *old_values);
            if (result.has_value()) {
                Table output = std::move(input);
                write_guarded_update(output, field.alias, *target, std::move(result->first),
                                     std::move(result->second));
                return output;
            }
        }
    }

    std::vector<std::size_t> matched_idx;
    matched_idx.reserve(n / 8);
    for (std::size_t i = 0; i < n; ++i) {
        if (matched_bytes[i] != 0) {
            matched_idx.push_back(i);
        }
    }

    Table output = std::move(input);
    std::optional<Table> sub;  // matching rows of the original columns (built lazily)
    robin_hood::unordered_set<std::string> subset_refs;
    for (const auto& field : update.fields()) {
        if (ir::is_subset_evaluable_expr(field.expr)) {
            ir::collect_expr_column_refs(field.expr, subset_refs);
        }
    }

    for (const auto& field : update.fields()) {
        // Retain the old column's shared storage rather than deep-copying it.
        // replace_column() reseats the output handle only after the result has
        // been built, so this remains a stable snapshot of the old values.
        const ColumnEntry* old_entry = output.find_entry(field.alias);
        std::shared_ptr<ColumnValue> old_col = old_entry != nullptr ? old_entry->column : nullptr;
        const ValidityBitmap* old_valid = old_entry != nullptr && old_entry->validity.has_value()
                                              ? &*old_entry->validity
                                              : nullptr;

        // Evaluate the field. Subset-evaluable fields run on the matching rows
        // only; the rest run over the full table.
        const bool subset = ir::is_subset_evaluable_expr(field.expr);
        std::shared_ptr<ColumnValue> new_vals;
        std::optional<ValidityBitmap> new_valid;
        {
            if (subset && !sub.has_value()) {
                // A subset expression only needs the columns it references.
                // Avoid gathering unrelated payload columns (often the bulk
                // of a wide table) merely to evaluate one guarded assignment.
                Table subset_source;
                for (const auto& entry : output.columns) {
                    if (subset_refs.contains(entry.name)) {
                        subset_source.add_column_from(entry.name, entry);
                    }
                }
                if (subset_source.columns.empty()) {
                    subset_source.logical_rows = matched_idx.size();
                    sub = std::move(subset_source);
                } else {
                    sub = gather_rows(subset_source, matched_idx);
                }
            }
            Table src_in = subset ? Table{*sub} : Table{output};
            auto upd = update_table(std::move(src_in), {field}, scalars, externs, exec);
            if (!upd) {
                return std::unexpected(upd.error());
            }
            const ColumnEntry* e = upd->find_entry(field.alias);
            new_vals = e->column;
            new_valid = e->validity;
        }

        new_vals = preserve_categorical_target(old_entry, std::move(new_vals));
        old_col = promote_integer_target(old_col, new_vals);

        auto target = prepare_guarded_write(output, field.alias, *new_vals);
        if (!target.has_value()) {
            return std::unexpected(std::move(target.error()));
        }

        // Build the result column by pushing per row: matched -> new value
        // (subset values are aligned with matched_idx; full values indexed by i),
        // non-matched -> old value, or null for a new column.
        auto [result_col, result_valid] = std::visit(
            [&](const auto& src) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
                using Col = std::decay_t<decltype(src)>;
                const Col* oldc = old_col != nullptr ? &std::get<Col>(*old_col) : nullptr;

                // The overwhelmingly common case has valid values on both
                // arms. Start with the required copy of the old output column,
                // then scatter only matching rows into it. This avoids a
                // branch and a validity-bit write for every input row.
                if constexpr (!std::is_same_v<Col, Column<std::string>> &&
                              !std::is_same_v<Col, Column<Categorical>>) {
                    if (oldc != nullptr && old_valid == nullptr && !new_valid.has_value()) {
                        Col out = *oldc;
                        // Resolve both ends once; see `ColumnAppender`. Only
                        // `Column<bool>` reaches this branch without a dense
                        // buffer, so it keeps the indexed path.
                        typename Col::value_type* out_values = nullptr;
                        if constexpr (is_dense_column_v<Col>) {
                            out_values = out.data();
                        }
                        auto scatter = [&](std::size_t dst, std::size_t s) {
                            if constexpr (is_dense_column_v<Col>) {
                                out_values[dst] = src[s];
                            } else {
                                out[dst] = src[s];
                            }
                        };
                        if (subset) {
                            for (std::size_t k = 0; k < matched_idx.size(); ++k) {
                                scatter(matched_idx[k], k);
                            }
                        } else {
                            for (const std::size_t i : matched_idx) {
                                scatter(i, i);
                            }
                        }
                        return {ColumnValue{std::move(out)}, std::nullopt};
                    }

                    // A guarded assignment that CREATES a column. Every
                    // non-matching row is null by definition, so there is no
                    // old value to interleave and the row loop below is doing
                    // nothing but re-deriving `matched_idx`. Touch the matched
                    // rows only — for a selective guard that is a small
                    // fraction of the table.
                    if (oldc == nullptr) {
                        Col out;
                        // Value-initialized on purpose: the payload under a
                        // null is never read by Ibex, but it is still handed
                        // to Arrow on export, so it must not be garbage.
                        out.resize(n);
                        ValidityBitmap valid(n, false);
                        bool any_invalid = matched_idx.size() < n;
                        typename Col::value_type* out_values = nullptr;
                        if constexpr (is_dense_column_v<Col>) {
                            out_values = out.data();
                        }
                        for (std::size_t k = 0; k < matched_idx.size(); ++k) {
                            const std::size_t i = matched_idx[k];
                            const std::size_t si = subset ? k : i;
                            if (new_valid.has_value() && !(*new_valid)[si]) {
                                any_invalid = true;
                                continue;
                            }
                            if constexpr (is_dense_column_v<Col>) {
                                out_values[i] = src[si];
                            } else {
                                out[i] = src[si];
                            }
                            valid.set(i, true);
                        }
                        return {ColumnValue{std::move(out)},
                                any_invalid ? std::optional<ValidityBitmap>{std::move(valid)}
                                            : std::nullopt};
                    }
                }

                if constexpr (std::is_same_v<Col, Column<std::string>>) {
                    // Strings cannot be scattered through a flat value buffer,
                    // but their final source is known for every output row.
                    // Count the selected slabs first, then write the one
                    // pre-sized packed column. This retains the guarded
                    // snapshot rule while avoiding one allocation per row.
                    std::size_t total_chars = 0;
                    std::size_t source_row = 0;
                    bool fits_offsets = true;
                    for (std::size_t i = 0; i < n; ++i) {
                        std::string_view value;
                        if (matched_bytes[i] != 0) {
                            const std::size_t si = subset ? source_row++ : i;
                            value = src[si];
                        } else if (oldc != nullptr) {
                            value = (*oldc)[i];
                        }
                        if (value.size() >
                            std::numeric_limits<std::uint32_t>::max() - total_chars) {
                            fits_offsets = false;
                            break;
                        }
                        total_chars += value.size();
                    }
                    if (fits_offsets) {
                        Col out;
                        out.resize_for_gather(n, total_chars);
                        auto* offsets = out.offsets_data();
                        char* chars = out.chars_data();
                        offsets[0] = 0;
                        std::uint32_t cursor = 0;
                        source_row = 0;
                        ValidityBitmap valid(n, true);
                        bool any_invalid = false;
                        for (std::size_t i = 0; i < n; ++i) {
                            std::string_view value;
                            bool is_valid = false;
                            if (matched_bytes[i] != 0) {
                                const std::size_t si = subset ? source_row++ : i;
                                value = src[si];
                                is_valid = !new_valid.has_value() || (*new_valid)[si];
                            } else if (oldc != nullptr) {
                                value = (*oldc)[i];
                                is_valid = old_valid == nullptr || (*old_valid)[i];
                            }
                            if (!value.empty()) {
                                std::memcpy(chars + cursor, value.data(), value.size());
                            }
                            cursor += static_cast<std::uint32_t>(value.size());
                            offsets[i + 1] = cursor;
                            valid.set(i, is_valid);
                            any_invalid = any_invalid || !is_valid;
                        }
                        return {ColumnValue{std::move(out)},
                                any_invalid ? std::optional<ValidityBitmap>{std::move(valid)}
                                            : std::nullopt};
                    }
                }

                if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                    // The old and replacement categoricals can carry distinct
                    // dictionaries. Rebuild the result dictionary in final-row
                    // order (the established appender contract), but memoize
                    // each input code's output code. Repeated categories then
                    // copy one code instead of hashing their label per row.
                    using Code = Col::code_type;
                    constexpr Code kUnmapped = -1;
                    Col out;
                    out.reserve(n);
                    std::vector<Code> old_codes(
                        oldc != nullptr ? oldc->dictionary_size() : std::size_t{0}, kUnmapped);
                    std::vector<Code> new_codes(src.dictionary_size(), kUnmapped);
                    std::optional<Code> empty_code;
                    const auto append_remapped = [&](const Col& source, std::size_t row,
                                                     std::vector<Code>& remap) {
                        const Code input_code = source.code_at(row);
                        if (input_code >= 0 &&
                            static_cast<std::size_t>(input_code) < remap.size() &&
                            remap[static_cast<std::size_t>(input_code)] != kUnmapped) {
                            out.push_code(remap[static_cast<std::size_t>(input_code)]);
                            return;
                        }
                        out.push_back(source[row]);
                        if (input_code >= 0 &&
                            static_cast<std::size_t>(input_code) < remap.size()) {
                            remap[static_cast<std::size_t>(input_code)] =
                                out.code_at(out.size() - 1);
                        }
                    };

                    ValidityBitmap valid(n, true);
                    bool any_invalid = false;
                    std::size_t source_row = 0;
                    for (std::size_t i = 0; i < n; ++i) {
                        bool is_valid = false;
                        if (matched_bytes[i] != 0) {
                            const std::size_t si = subset ? source_row++ : i;
                            append_remapped(src, si, new_codes);
                            is_valid = !new_valid.has_value() || (*new_valid)[si];
                        } else if (oldc != nullptr) {
                            append_remapped(*oldc, i, old_codes);
                            is_valid = old_valid == nullptr || (*old_valid)[i];
                        } else {
                            if (!empty_code.has_value()) {
                                out.push_back(std::string_view{});
                                empty_code = out.code_at(out.size() - 1);
                            } else {
                                out.push_code(*empty_code);
                            }
                        }
                        valid.set(i, is_valid);
                        any_invalid = any_invalid || !is_valid;
                    }
                    return {ColumnValue{std::move(out)},
                            any_invalid ? std::optional<ValidityBitmap>{std::move(valid)}
                                        : std::nullopt};
                }

                Col out;
                ColumnAppender<Col> appender(out, n);
                ValidityBitmap valid(n, true);
                bool any_invalid = false;
                std::size_t k = 0;  // running index into `src` for the subset case
                for (std::size_t i = 0; i < n; ++i) {
                    if (matched_bytes[i] != 0) {
                        const std::size_t si = subset ? k++ : i;
                        appender.push(src[si]);
                        const bool v = !new_valid.has_value() || (*new_valid)[si];
                        valid.set(i, v);
                        any_invalid = any_invalid || !v;
                    } else if (oldc != nullptr) {
                        appender.push((*oldc)[i]);
                        const bool v = old_valid == nullptr || (*old_valid)[i];
                        valid.set(i, v);
                        any_invalid = any_invalid || !v;
                    } else {
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            appender.push(std::string_view{});
                        } else {
                            appender.push(typename Col::value_type{});
                        }
                        valid.set(i, false);
                        any_invalid = true;
                    }
                }
                return {
                    ColumnValue{std::move(out)},
                    any_invalid ? std::optional<ValidityBitmap>{std::move(valid)} : std::nullopt};
            },
            *new_vals);

        write_guarded_update(output, field.alias, *target, std::move(result_col),
                             std::move(result_valid));
    }
    return output;
}

/// A `by` clause is semantically inert for a wholly row-local update: every
/// field reads only its own absolute row, and `update_table` already owns the
/// direct ChunkView output protocols (numeric windows, predicates, strings,
/// categoricals, temporal parts, and validity). Bypassing group discovery for
/// such a clause is stronger than gathering CSR windows only to evaluate the
/// same rows again, and it retains the normal ordered field writer.
[[nodiscard]] auto is_group_inert_update(const std::vector<ir::FieldSpec>& fields) -> bool {
    return std::ranges::all_of(fields, [](const ir::FieldSpec& field) {
        return ir::is_row_local_update_expr(field.expr) &&
               !expr_contains_aggregate_call(field.expr);
    });
}

/// Per-group update, over a grouping that has already been discovered: fields
/// the native CSR protocols own are reduced and scattered by absolute row, and
/// whatever is left partitions the input by `group_by`, runs the regular
/// `update_table` on each per-group slice, and scatters the new field columns
/// back into a single full-sized output. Ordered ops like `lag`, `lead`,
/// `cumsum`, and `fill_forward` therefore see only their group's rows; pure
/// arithmetic gives the same answer per row regardless.
///
/// Taking the plan rather than building one is what lets the ordered staging
/// loop below run field by field without paying for group discovery again: a
/// stage only ever ADDS a column, so the row count and the key columns — and
/// therefore the group ids and their CSR rows — are the same for every stage.
auto grouped_update_table_with_plan(Table input, const std::vector<ir::FieldSpec>& fields,
                                    const std::vector<ir::ColumnRef>& group_by,
                                    const GroupedRowPlan& grouped, const ScalarRegistry* scalars,
                                    const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    if (is_group_inert_update(fields)) {
        return update_table(std::move(input), fields, scalars, externs, exec);
    }
    const std::size_t rows = input.rows();
    const std::size_t group_count = grouped.rows.group_count();
    const GroupedRows& group_rows = grouped.rows;
    const auto row_gid = grouped.row_gid(rows);

    if (auto ordered = try_native_grouped_ordered_field(input, fields, group_rows, exec);
        !ordered) {
        return std::unexpected(ordered.error());
    } else if (ordered->has_value()) {
        return std::move(**ordered);
    }
    if (auto native = try_native_grouped_reductions(input, fields, group_rows, exec); !native) {
        return std::unexpected(native.error());
    } else if (native->has_value()) {
        return std::move(**native);
    }
    if (fields.size() == 1) {
        // Aggregates mixed into a row-local expression: lift each reduction to
        // a broadcast staging column and evaluate the remainder directly.
        if (auto lifted = try_native_grouped_aggregate_expr(input, fields.front(), grouped, scalars,
                                                            externs, exec);
            !lifted) {
            return std::unexpected(lifted.error());
        } else if (lifted->has_value()) {
            return std::move(**lifted);
        }
    }

    // A mixed clause must retain update's declaration-order visibility: a
    // materialized field can feed a following native reduction, and a native
    // reduction can feed a following row-local field. Execute it as ordered
    // single-field stages, so each stage sees exactly its predecessors' landed
    // columns. Single supported reductions still take the CSR-native branch
    // above; fields without a native candidate stay together on the established
    // materialized path.
    if (fields.size() > 1 && std::ranges::any_of(fields, [&](const auto& field) {
            return is_grouped_direct_field_candidate(input, field);
        })) {
        Table output = std::move(input);
        for (const auto& field : fields) {
            auto staged = grouped_update_table_with_plan(std::move(output), {field}, group_by,
                                                         grouped, scalars, externs, exec);
            if (!staged) {
                return std::unexpected(staged.error());
            }
            output = std::move(*staged);
        }
        return output;
    }

    auto run_group =
        [&](std::span<const std::size_t> row_idx) -> std::expected<Table, std::string> {
        Table sub;
        for (const auto& entry : input.columns) {
            // Serial for the same reason as `grouped_windowed_update_table`'s
            // slice gather: one group's rows, under a per-group fan-out.
            ColumnValue gathered =
                gather_column(*entry.column, row_idx.data(), row_idx.size(), nullptr);
            // Carry each input column's validity into the per-group slice —
            // else an aggregate over a nullable column accumulates the null
            // cells' undefined payloads instead of skipping them (mirrors
            // grouped_windowed_update_table).
            if (entry.validity.has_value()) {
                ValidityBitmap vb(row_idx.size(), true);
                for (std::size_t k = 0; k < row_idx.size(); ++k) {
                    vb.set(k, (*entry.validity)[row_idx[k]]);
                }
                sub.add_column(entry.name, std::move(gathered), std::move(vb));
            } else {
                sub.add_column(entry.name, std::move(gathered));
            }
        }
        // A per-group slice: it inherits the time index, but deliberately NOT
        // the grouping -- a single group has no boundary to read across, and
        // claiming one would make the row-order guard reject a correct
        // unpartitioned call inside the slice.
        if (input.time_index().has_value()) {
            sub.set_properties(TableProperties::time_frame(*input.time_index()));
        }

        std::vector<ir::FieldSpec> pending_row_fields;
        auto flush_pending = [&] -> std::expected<void, std::string> {
            if (pending_row_fields.empty()) {
                return {};
            }
            auto updated = update_table(std::move(sub), pending_row_fields, scalars, externs, exec);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            sub = std::move(updated.value());
            pending_row_fields.clear();
            return {};
        };

        for (const auto& field : fields) {
            auto aggregate = broadcast_aggregate_column(sub, field, scalars);
            if (!aggregate) {
                return std::unexpected(aggregate.error());
            }
            if (!aggregate->has_value()) {
                pending_row_fields.push_back(field);
                continue;
            }
            if (auto flushed = flush_pending(); !flushed) {
                return std::unexpected(flushed.error());
            }
            if ((*aggregate)->validity.has_value()) {
                sub.add_column(field.alias, std::move((*aggregate)->column),
                               std::move(*(*aggregate)->validity));
            } else {
                sub.add_column(field.alias, std::move((*aggregate)->column));
            }
        }
        if (auto flushed = flush_pending(); !flushed) {
            return std::unexpected(flushed.error());
        }
        return sub;
    };

    auto first = run_group(group_rows[0]);
    if (!first.has_value()) {
        return std::unexpected(first.error());
    }
    const std::size_t first_new_idx = input.columns.size();
    auto written_field_names = written_field_names_for(*first, first_new_idx, fields);
    if (written_field_names.empty()) {
        return std::unexpected("update: grouped update produced no columns");
    }

    std::vector<bool> variable_output(written_field_names.size(), false);
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        const auto* sample = first->find(written_field_names[f]);
        if (sample == nullptr) {
            return std::unexpected("update: missing new column '" + written_field_names[f] +
                                   "' in sub-result");
        }
        variable_output[f] = std::holds_alternative<Column<std::string>>(*sample) ||
                             std::holds_alternative<Column<Categorical>>(*sample);
    }
    struct VariableGroupPiece {
        std::vector<std::optional<ColumnEntry>> fields;
    };
    std::vector<VariableGroupPiece> variable_pieces(
        group_count, VariableGroupPiece{.fields = std::vector<std::optional<ColumnEntry>>(
                                            written_field_names.size())});

    Table output = input;
    auto allocate_full = [&](const ColumnValue& sample) -> std::expected<ColumnValue, std::string> {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                } else {
                    ColT out;
                    out.resize(rows);
                    return ColumnValue{std::move(out)};
                }
            },
            sample);
    };
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (variable_output[f]) {
            continue;
        }
        auto full = allocate_full(*first->find(written_field_names[f]));
        if (!full.has_value()) {
            return std::unexpected(full.error());
        }
        output.add_column(written_field_names[f], std::move(full.value()));
    }

    auto scatter_into = [](ColumnValue& dst, const ColumnValue& src,
                           std::span<const std::size_t> indices) -> std::optional<std::string> {
        return std::visit(
            [&](auto& dcol) -> std::optional<std::string> {
                using DT = std::decay_t<decltype(dcol)>;
                const DT* scol = std::get_if<DT>(&src);
                if (scol == nullptr) {
                    return "update: type mismatch in grouped scatter";
                }
                if constexpr (std::is_same_v<DT, Column<std::string>> ||
                              std::is_same_v<DT, Column<Categorical>>) {
                    return "update: variable-width grouped scatter reached fixed-width path";
                } else if constexpr (std::is_same_v<DT, Column<bool>>) {
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dcol.set(indices[i], (*scol)[i]);
                    }
                } else {
                    auto* dp = dcol.data();
                    const auto* sp = scol->data();
                    for (std::size_t i = 0; i < indices.size(); ++i) {
                        dp[indices[i]] = sp[i];
                    }
                }
                return std::nullopt;
            },
            dst);
    };

    // Same lazy validity scatter as `grouped_windowed_update_table` —
    // ordered ops like `lag(c, 1)` produce a per-group validity bitmap with
    // the first `n` rows marked null; we OR those into the output's column.
    std::vector<std::optional<ValidityBitmap>> output_validity(written_field_names.size());

    auto scatter_validity = [&](std::size_t f_idx, const Table& sub_table,
                                std::span<const std::size_t> indices) {
        const auto* sub_entry = sub_table.find_entry(written_field_names[f_idx]);
        if (sub_entry == nullptr || !sub_entry->validity.has_value()) {
            return;
        }
        if (!output_validity[f_idx].has_value()) {
            output_validity[f_idx] = ValidityBitmap(rows, true);
        }
        const auto& sub_bm = *sub_entry->validity;
        auto& out_bm = *output_validity[f_idx];
        const auto* validity_bytes = sub_bm.buffer_data();
        const std::size_t validity_offset = sub_bm.buffer_offset();
        for (std::size_t i = 0; i < indices.size(); ++i) {
            const std::size_t bit = validity_offset + i;
            if (((validity_bytes[bit / 8] >> (bit % 8)) & 0x01U) == 0U) {
                out_bm.set(indices[i], false);
            }
        }
    };

    auto scatter_group = [&](std::size_t group, const Table& sub) -> std::optional<std::string> {
        for (std::size_t f = 0; f < written_field_names.size(); ++f) {
            const auto& fname = written_field_names[f];
            const auto* src = sub.find_entry(fname);
            if (src == nullptr) {
                return "update: missing column '" + fname + "' in grouped sub-result";
            }
            if (variable_output[f]) {
                variable_pieces[group].fields[f] = *src;
                continue;
            }
            auto* dst = output.find(fname);
            if (auto err = scatter_into(*dst, *src->column, group_rows[group])) {
                return err;
            }
            scatter_validity(f, sub, group_rows[group]);
        }
        return std::nullopt;
    };

    if (auto err = scatter_group(0, *first)) {
        return std::unexpected(*err);
    }
    for (std::size_t g = 1; g < group_count; ++g) {
        auto sub = run_group(group_rows[g]);
        if (!sub.has_value()) {
            return std::unexpected(sub.error());
        }
        if (auto err = scatter_group(g, *sub)) {
            return std::unexpected(*err);
        }
    }
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (!output_validity[f].has_value()) {
            continue;
        }
        auto idx_it = output.index.find(written_field_names[f]);
        if (idx_it != output.index.end()) {
            output.columns[idx_it->second].validity = std::move(output_validity[f]);
        }
    }

    // Group slices are group-major, while the result must remain in original
    // row order. Record each row's position within its group once so the two
    // variable-width assemblers can walk the final order directly.
    std::vector<std::size_t> group_offset(rows);
    for (std::size_t group = 0; group < group_count; ++group) {
        for (std::size_t local = 0; local < group_rows[group].size(); ++local) {
            group_offset[group_rows[group][local]] = local;
        }
    }
    for (std::size_t f = 0; f < written_field_names.size(); ++f) {
        if (!variable_output[f]) {
            continue;
        }
        const bool has_validity = std::ranges::any_of(variable_pieces, [&](const auto& piece) {
            return piece.fields[f]->validity.has_value();
        });
        if (std::holds_alternative<Column<std::string>>(*variable_pieces[0].fields[f]->column)) {
            std::size_t total_chars = 0;
            for (std::size_t row = 0; row < rows; ++row) {
                const std::size_t group = row_gid[row];
                const auto& source =
                    std::get<Column<std::string>>(*variable_pieces[group].fields[f]->column);
                const auto local = group_offset[row];
                const std::size_t count =
                    source.offsets_data()[local + 1] - source.offsets_data()[local];
                if (count > std::numeric_limits<std::uint32_t>::max() - total_chars) {
                    return std::unexpected("update + by: string output exceeds uint32 offsets");
                }
                total_chars += count;
            }
            Column<std::string> result;
            result.resize_for_gather(rows, total_chars);
            auto* offsets = result.offsets_data();
            char* chars = result.chars_data();
            offsets[0] = 0;
            std::uint32_t cursor = 0;
            std::optional<ValidityBitmap> validity;
            if (has_validity) {
                validity = ValidityBitmap(rows, true);
            }
            for (std::size_t row = 0; row < rows; ++row) {
                const std::size_t group = row_gid[row];
                const auto& entry = *variable_pieces[group].fields[f];
                const auto& source = std::get<Column<std::string>>(*entry.column);
                const auto local = group_offset[row];
                const auto begin = source.offsets_data()[local];
                const auto end = source.offsets_data()[local + 1];
                if (end != begin) {
                    std::memcpy(chars + cursor, source.chars_data() + begin, end - begin);
                }
                cursor += end - begin;
                offsets[row + 1] = cursor;
                if (validity.has_value() && entry.validity.has_value()) {
                    validity->set(row, (*entry.validity)[local]);
                }
            }
            if (validity.has_value()) {
                output.add_column(written_field_names[f], ColumnValue{std::move(result)},
                                  std::move(*validity));
            } else {
                output.add_column(written_field_names[f], ColumnValue{std::move(result)});
            }
            continue;
        }

        using CategoricalColumn = Column<Categorical>;
        using Code = CategoricalColumn::code_type;
        constexpr Code kUnmapped = -1;
        CategoricalColumn result;
        result.reserve(rows);
        std::vector<std::vector<Code>> remaps(group_count);
        for (std::size_t group = 0; group < group_count; ++group) {
            const auto& source =
                std::get<CategoricalColumn>(*variable_pieces[group].fields[f]->column);
            remaps[group].assign(source.dictionary_size(), kUnmapped);
        }
        std::optional<ValidityBitmap> validity;
        if (has_validity) {
            validity = ValidityBitmap(rows, true);
        }
        for (std::size_t row = 0; row < rows; ++row) {
            const std::size_t group = row_gid[row];
            const auto& entry = *variable_pieces[group].fields[f];
            const auto& source = std::get<CategoricalColumn>(*entry.column);
            const auto local = group_offset[row];
            const Code input_code = source.code_at(local);
            auto& remap = remaps[group];
            if (input_code >= 0 && static_cast<std::size_t>(input_code) < remap.size() &&
                remap[static_cast<std::size_t>(input_code)] != kUnmapped) {
                result.push_code(remap[static_cast<std::size_t>(input_code)]);
            } else {
                result.push_back(source[local]);
                if (input_code >= 0 && static_cast<std::size_t>(input_code) < remap.size()) {
                    remap[static_cast<std::size_t>(input_code)] = result.code_at(result.size() - 1);
                }
            }
            if (validity.has_value() && entry.validity.has_value()) {
                validity->set(row, (*entry.validity)[local]);
            }
        }
        if (validity.has_value()) {
            output.add_column(written_field_names[f], ColumnValue{std::move(result)},
                              std::move(*validity));
        } else {
            output.add_column(written_field_names[f], ColumnValue{std::move(result)});
        }
    }
    normalize_time_index(output);
    return output;
}

auto grouped_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                          const std::vector<ir::ColumnRef>& group_by, const ScalarRegistry* scalars,
                          const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    if (group_by.empty()) {
        return update_table(std::move(input), fields, scalars, externs, exec);
    }
    if (input.time_index().has_value()) {
        for (const auto& field : fields) {
            if (field.alias == *input.time_index()) {
                return std::unexpected("cannot update time index column: " + field.alias);
            }
        }
    }
    if (input.rows() == 0) {
        return update_table(std::move(input), fields, scalars, externs, exec);
    }
    // Asked before the grouping is discovered, so a row-local clause never pays
    // for a group plan it would not read. `grouped_update_table_with_plan` asks
    // the same question again per staged field, where the plan already exists.
    if (is_group_inert_update(fields)) {
        return update_table(std::move(input), fields, scalars, externs, exec);
    }
    auto grouped = make_grouped_row_plan(input, group_by, exec);
    if (!grouped.has_value()) {
        return std::unexpected(std::move(grouped.error()));
    }
    return grouped_update_table_with_plan(std::move(input), fields, group_by, *grouped, scalars,
                                          externs, exec);
}

}  // namespace ibex::runtime
