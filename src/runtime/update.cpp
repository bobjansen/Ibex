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
#include <expected>
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
        if (const auto* source = input.find(col->name); source != nullptr) {
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
    ColumnEvalCtx ctx{.scalars = scalars, .externs = nullptr, .window = std::nullopt};
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

auto eval_numeric_update_blocks(const std::vector<NumericUpdateNode>& nodes, std::uint32_t root,
                                std::size_t rows, ExprType output_kind) -> ColumnValue {
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
        Column<double> out;
        out.resize_for_overwrite(rows);
        for (std::size_t offset = 0; offset < rows; offset += kNumericUpdateBlockRows) {
            const std::size_t count = std::min(kNumericUpdateBlockRows, rows - offset);
            for (std::uint32_t idx = 0; idx <= root; ++idx) {
                if ((modes[idx] & kNumericEvalDouble) == 0U) {
                    continue;
                }
                double* dst = idx == root ? out.data() + offset
                                          : double_scratch.data() + static_cast<std::size_t>(idx) *
                                                                        kNumericUpdateBlockRows;
                eval_numeric_double_node_block(nodes[idx], idx, double_values, dst, offset, count);
            }
            const auto root_value = double_values[root];
            double* out_block = out.data() + offset;
            if (root_value.data == nullptr) {
                std::fill_n(out_block, count, root_value.scalar);
            } else if (root_value.data != out_block) {
                std::copy_n(root_value.data, count, out_block);
            }
        }
        return ColumnValue{std::move(out)};
    }

    Column<std::int64_t> out;
    out.resize_for_overwrite(rows);
    for (std::size_t offset = 0; offset < rows; offset += kNumericUpdateBlockRows) {
        const std::size_t count = std::min(kNumericUpdateBlockRows, rows - offset);
        for (std::uint32_t idx = 0; idx <= root; ++idx) {
            if ((modes[idx] & kNumericEvalDouble) != 0U) {
                double* dst =
                    double_scratch.data() + static_cast<std::size_t>(idx) * kNumericUpdateBlockRows;
                eval_numeric_double_node_block(nodes[idx], idx, double_values, dst, offset, count);
            }
            if ((modes[idx] & kNumericEvalInt) != 0U) {
                std::int64_t* dst = idx == root
                                        ? out.data() + offset
                                        : int_scratch.data() + static_cast<std::size_t>(idx) *
                                                                   kNumericUpdateBlockRows;
                eval_numeric_int_node_block(nodes[idx], idx, int_values, double_values, dst, offset,
                                            count);
            }
        }
        const auto root_value = int_values[root];
        std::int64_t* out_block = out.data() + offset;
        if (root_value.data == nullptr) {
            std::fill_n(out_block, count, root_value.scalar);
        } else if (root_value.data != out_block) {
            std::copy_n(root_value.data, count, out_block);
        }
    }
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
    if (!input.grouped_by.empty()) {
        std::string keys;
        for (const auto& key : input.grouped_by) {
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
    if (!output.time_index.has_value()) {
        return std::unexpected("window: requires a TimeFrame");
    }
    if (auto ok = check_row_order(output, fields); !ok) {
        return std::unexpected(ok.error());
    }
    // Reject mutation of the time index column
    for (const auto& field : fields) {
        if (field.alias == *output.time_index) {
            return std::unexpected("cannot update time index column: " + field.alias);
        }
    }
    for (const auto& field : fields) {
        if (std::holds_alternative<ir::RankExpr>(field.expr.node)) {
            return std::unexpected("rank(): not supported inside windowed update");
        }
        if (const auto* col_ref = std::get_if<ir::ColumnRef>(&field.expr.node)) {
            const auto* entry = output.find_entry(col_ref->name);
            if (entry != nullptr) {
                // `alias = other_column` renames rather than computes, so the
                // two names can share one buffer under the copy-on-write
                // invariant. Deep-copying here moved 26MB per renamed key
                // column on a q03-shaped scan — the join-key alignment idiom
                // (`select { o_orderkey = l_orderkey }`) hits this on nearly
                // every query.
                output.add_column_shared(field.alias, entry->column, entry->validity);
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
    if (on_worker_pool_thread() || !exec.parallel || rows < exec.parallel_min_rows) {
        return 0;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
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
/// why a two-symbol table gains nothing however many cores are free.
[[nodiscard]] auto grouped_window_worker_count(const ExecutionContext& exec,
                                               std::size_t remaining_groups, std::size_t rows,
                                               const Table& first_sub,
                                               const std::vector<std::string>& new_field_names,
                                               const std::vector<ir::FieldSpec>& fields)
    -> std::size_t {
    // `run_group` is reached from a worker only through this function, but the
    // island executor can call the whole update from a pool thread; submitting
    // from there deadlocks the pool.
    if (on_worker_pool_thread() || !exec.parallel || remaining_groups < 2) {
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
    for (const auto& fname : new_field_names) {
        const auto* sample = first_sub.find(fname);
        if (sample != nullptr && std::holds_alternative<Column<bool>>(*sample)) {
            return 0;
        }
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t workers = std::min({budget, pool_size, remaining_groups});
    return workers < 2 ? 0 : workers;
}

/// Row indices bucketed by group, in CSR form: `flat` holds every row index
/// once, and group `g` owns `flat[offsets[g] .. offsets[g + 1])`, ascending.
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

/// Build the work list for a grouped windowed update: normally one item per
/// group, but under an aligned window a group may be cut into several.
///
/// Parallelism across groups caps the speedup at the group count — three
/// symbols leave thirteen of sixteen cores idle, which is exactly the shape a
/// per-symbol OHLC bar query has. Under an *aligned* window each row's value
/// depends only on rows in its own bucket (see `is_bucket_local_window_expr`),
/// so a group's rows can be cut at any bucket boundary and the pieces run
/// independently: no halo, no overlap, and each piece's rolling state starts
/// where the bucket would have reset it anyway.
///
/// Splitting is skipped entirely once there are already at least `budget`
/// groups, so the high-cardinality case is untouched *by construction* rather
/// than by tuning. It is also skipped when no cut lands inside the group — a
/// window as long as the data has one bucket and nothing to divide.
[[nodiscard]] auto build_window_tasks(const GroupedRows& group_rows, const Table& input,
                                      ir::Duration duration, bool aligned,
                                      const std::vector<ir::FieldSpec>& fields,
                                      const ExecutionContext& exec, std::size_t rows)
    -> std::vector<std::span<const std::size_t>> {
    std::vector<std::span<const std::size_t>> tasks;
    tasks.reserve(group_rows.group_count());
    for (std::size_t g = 0; g < group_rows.group_count(); ++g) {
        tasks.push_back(group_rows[g]);
    }

    if (!aligned || on_worker_pool_thread() || !exec.parallel || rows < exec.parallel_min_rows) {
        return tasks;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t workers = std::min(budget, pool_size);
    if (workers < 2 || group_rows.group_count() >= workers) {
        return tasks;  // enough groups to keep the pool busy already
    }
    for (const auto& field : fields) {
        if (!ir::is_bucket_local_window_expr(field.expr)) {
            return tasks;
        }
    }

    // Aim past the worker count so a group whose buckets divide unevenly can
    // still be balanced by work stealing, but keep each piece large enough that
    // its own gather and scatter dominate the fixed per-piece cost.
    constexpr std::size_t kMinSplitRows = 32768;
    const std::size_t target = std::max(rows / (workers * 4), kMinSplitRows);

    const auto* tcv = input.find(*input.time_index);
    std::vector<std::span<const std::size_t>> split;
    // Each piece holds at least `target` rows, so this bound is never exceeded.
    split.reserve((rows / target) + tasks.size());
    if (const auto* ts = std::get_if<Column<Timestamp>>(tcv)) {
        const std::int64_t unit = duration.count();
        if (unit <= 0) {
            return tasks;
        }
        auto bucket_of = [&](std::size_t r) { return window_bucket_index((*ts)[r].nanos, unit); };
        for (const auto& task : tasks) {
            split_at_bucket_bounds(task, target, bucket_of, split);
        }
    } else if (const auto* dt = std::get_if<Column<Date>>(tcv)) {
        static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
        const std::int64_t unit = duration.count() / kNsPerDay;
        if (unit <= 0) {
            return tasks;
        }
        auto bucket_of = [&](std::size_t r) { return window_bucket_index((*dt)[r].days, unit); };
        for (const auto& task : tasks) {
            split_at_bucket_bounds(task, target, bucket_of, split);
        }
    } else {
        return tasks;
    }

    if (split.size() <= tasks.size()) {
        return tasks;  // nothing divided; keep the plainer work list
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
    if (!input.time_index.has_value()) {
        return std::unexpected("window: requires a TimeFrame");
    }
    for (const auto& field : fields) {
        if (field.alias == *input.time_index) {
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
    auto row_gid_buf = std::make_unique_for_overwrite<std::uint32_t[]>(rows);
    const std::span<std::uint32_t> row_gid{row_gid_buf.get(), rows};
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

    const GroupedRows group_rows = build_grouped_rows(row_gid, group_count, exec);

    // The unit of work is a span of row indices, not necessarily a whole group:
    // under an aligned window `build_window_tasks` may cut one group into
    // several pieces. Everything below treats a piece exactly as it treats a
    // group, and for the same reason — disjoint rows, and no expression that
    // reads across the cut.
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
        task_offset[g + 1] = task_offset[g] + tasks[g].size();
        perm.insert(perm.end(), tasks[g].begin(), tasks[g].end());
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
        if (input.time_index.has_value()) {
            ordering.push_back(ir::OrderKey{.name = *input.time_index, .ascending = true});
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
        return {out_positions.data() + task_offset[g], tasks[g].size()};
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
    if (input.time_index.has_value()) {
        needed.insert(*input.time_index);
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
            ColumnValue gathered = gather_column(*entry.column, row_idx.data(), row_idx.size());
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
        sub.time_index = input.time_index;
        return windowed_update_table(std::move(sub), fields, duration, scalars, externs, exec,
                                     aligned);
    };

    // Run the first group to learn the new field column types/names.
    auto first = run_group(out_slot(0));
    if (!first.has_value()) {
        return std::unexpected(first.error());
    }
    // The slice carries only `slice_columns`, so that -- not the full input
    // width -- is where the appended fields begin.
    const std::size_t first_new_idx = slice_columns.size();
    if (first->columns.size() <= first_new_idx) {
        return std::unexpected("window: grouped update produced no new columns");
    }
    std::vector<std::string> new_field_names;
    new_field_names.reserve(first->columns.size() - first_new_idx);
    for (std::size_t c = first_new_idx; c < first->columns.size(); ++c) {
        new_field_names.push_back(first->columns[c].name);
    }

    // Allocate output's new columns at full size, with the same types as the
    // first sub-result. Strings/categoricals would need a different scatter
    // strategy (per-row write isn't free for flat-buffer strings); rolling /
    // lag / fill ops produce numeric columns in practice, so reject the
    // string/categorical case explicitly until that's needed.
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
                        "window + by: scatter of string/Categorical results not yet implemented");
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
    for (const auto& fname : new_field_names) {
        const auto* sample = first->find(fname);
        if (sample == nullptr) {
            return std::unexpected("window: missing new column '" + fname + "' in sub-result");
        }
        auto full = allocate_full(*sample);
        if (!full.has_value()) {
            return std::unexpected(full.error());
        }
        output.add_column(fname, std::move(full.value()));
    }

    auto scatter_into = [](ColumnValue& dst, const ColumnValue& src,
                           std::span<const std::size_t> indices) -> std::optional<std::string> {
        return std::visit(
            [&](auto& dcol) -> std::optional<std::string> {
                using DT = std::decay_t<decltype(dcol)>;
                const DT* scol = std::get_if<DT>(&src);
                if (scol == nullptr) {
                    return "window: type mismatch in grouped scatter";
                }
                if constexpr (std::is_same_v<DT, Column<std::string>> ||
                              std::is_same_v<DT, Column<Categorical>>) {
                    return "window: scatter for string/Categorical not implemented";
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

    // Lazy-allocated per-field validity bitmaps. We only construct one if at
    // least one group's sub-result has a validity bitmap for that field —
    // most pure-arithmetic outputs stay all-valid and pay nothing. The
    // laziness is what decides the OUTPUT REPRESENTATION (bitmap vs no
    // bitmap), so it has to survive parallelism unchanged rather than being
    // traded for a simpler eager allocation.
    std::vector<std::optional<ValidityBitmap>> output_validity(new_field_names.size());

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
                                std::span<const std::size_t> indices) {
        const auto* sub_entry = sub_table.find_entry(new_field_names[f_idx]);
        if (sub_entry == nullptr || !sub_entry->validity.has_value()) {
            return;
        }
        auto* out_bm = ensure_validity(f_idx);
        const auto& sub_bm = *sub_entry->validity;
        auto* words = out_bm->words_data();
        const auto* validity_bytes = sub_bm.buffer_data();
        const std::size_t validity_offset = sub_bm.buffer_offset();
        for (std::size_t i = 0; i < indices.size(); ++i) {
            const std::size_t bit = validity_offset + i;
            if (((validity_bytes[bit / 8] >> (bit % 8)) & 0x01U) == 0U) {
                clear_validity_bit(words, indices[i]);
            }
        }
    };

    // Resolved once: `find` walks a hash map, and every group would otherwise
    // repeat that per field.
    std::vector<ColumnValue*> dst_columns;
    dst_columns.reserve(new_field_names.size());
    for (const auto& fname : new_field_names) {
        dst_columns.push_back(output.find(fname));
    }

    // One group's whole contribution: run the windowed update over its slice,
    // then scatter the new columns into the rows it owns. Groups own disjoint
    // rows, which is exactly what makes them independent of each other.
    auto scatter_group = [&](const Table& sub,
                             std::span<const std::size_t> indices) -> std::optional<std::string> {
        for (std::size_t f = 0; f < new_field_names.size(); ++f) {
            const auto* src = sub.find(new_field_names[f]);
            if (src == nullptr) {
                return "window: missing column '" + new_field_names[f] + "' in grouped sub-result";
            }
            if (auto err = scatter_into(*dst_columns[f], *src, indices)) {
                return err;
            }
            scatter_validity(f, sub, indices);
        }
        return std::nullopt;
    };

    if (auto err = scatter_group(*first, out_slot(0))) {
        return std::unexpected(*err);
    }

    const std::size_t workers =
        grouped_window_worker_count(exec, tasks.size() - 1, rows, *first, new_field_names, fields);
    if (workers < 2) {
        for (std::size_t g = 1; g < tasks.size(); ++g) {
            auto sub = run_group(out_slot(g));
            if (!sub.has_value()) {
                return std::unexpected(sub.error());
            }
            if (auto err = scatter_group(*sub, out_slot(g))) {
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
                        auto sub = run_group(out_slot(g));
                        if (!sub.has_value()) {
                            err = std::move(sub.error());
                        } else {
                            err = scatter_group(*sub, out_slot(g));
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
    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        if (!output_validity[f].has_value()) {
            continue;
        }
        auto idx_it = output.index.find(new_field_names[f]);
        if (idx_it != output.index.end()) {
            output.columns[idx_it->second].validity = std::move(output_validity[f]);
        }
    }

    // `normalize_time_index` rewrites any ordering to "time index ascending"
    // whenever a time index survives, which for group-major rows is simply
    // FALSE -- and not cosmetically so, since sort elision reads `ordering` and
    // would drop a downstream `order <time index>` as a no-op. Restore what the
    // rows actually are, the same way `order_table` does after its own gather.
    auto true_ordering = needs_permute ? permuted.ordering : input.ordering;
    normalize_time_index(output);
    if (true_ordering.has_value()) {
        output.ordering = std::move(true_ordering);
    }
    // Record the grouping for the same reason the ordering is restored above:
    // the rows are one contiguous run per group, so an unpartitioned lag/lead
    // downstream would read across a boundary. This holds whether or not the
    // permutation ran -- skipping it means the input was ALREADY group-major,
    // which is the same hazard. The condition is the group count: a single
    // group has no boundary to read across, and claiming a grouping there
    // would reject a correct unpartitioned lead.
    if (tasks.size() > 1) {
        output.grouped_by.clear();
        output.grouped_by.reserve(group_by.size());
        for (const auto& key : group_by) {
            output.grouped_by.push_back(key.name);
        }
    }
    return output;
}

namespace {

/// Evaluate one update field across worker threads, or serially when that is
/// not worthwhile or not safe.
///
/// This is where a 1:1 operator wants its parallelism, rather than in a
/// morsel island. `update_table` builds its output by *moving* the input
/// (`Table output = std::move(input)`), so a passthrough column costs nothing —
/// which means an island's per-morsel gather and its merge concat are not "one
/// copy too many" for this shape, they are pure overhead invented by
/// morselization. Splitting only the field computation leaves the zero-copy
/// passthrough intact and adds one copy of the computed column, instead of two
/// copies of the entire table.
///
/// Declines, falling back to a single whole-range evaluation, when:
///   - parallelism is off, the table is small, or the pool has one thread;
///   - the expression is not `is_range_native_expr` — evaluating it per range
///     would re-read the whole table per range (see that function);
///   - the result is not Int64/Double. Other types are not harder in principle,
///     but a Categorical result would need its per-piece dictionaries merged,
///     and a wrong merge is silent. Numeric covers the case this exists for.
auto evaluate_field_maybe_parallel(const ir::Expr& expr, const Table& table,
                                   const ColumnEvalCtx& ctx, const ExecutionContext& exec)
    -> std::expected<ComputedColumn, std::string> {
    const std::size_t rows = table.rows();
    const auto whole = [&] { return evaluate_field(expr, table, RowRange::whole(rows), ctx); };

    // Reentrancy: the island's fused FilterUpdateProject operator calls
    // update_table from a worker thread, so this can be reached on one.
    // Submitting from there deadlocks the pool (WorkerPool::submit aborts
    // rather than let it happen), and the morsel is already one worker's share
    // of the table — splitting it again would only oversubscribe.
    if (on_worker_pool_thread()) {
        return whole();
    }
    if (!exec.parallel || rows < exec.parallel_min_rows || !is_range_native_expr(expr)) {
        return whole();
    }
    auto inferred = infer_expr_type(expr, table, ctx.scalars, ctx.externs);
    if (!inferred.has_value() ||
        (inferred.value() != ExprType::Int && inferred.value() != ExprType::Double)) {
        return whole();
    }

    // Same derivation as an island's, so the two parallel paths partition
    // alike. Note a zero `parallel_grain` now means "derive", not "one row per
    // morsel" — reading it directly here would have split a 20M-row update into
    // 20M tasks.
    const std::size_t grain = island_grain(exec, rows);
    const std::size_t morsels = (rows + grain - 1) / grain;
    auto& pool = process_worker_pool();
    const std::size_t threads =
        std::min(morsels, exec.parallel_threads != 0 ? exec.parallel_threads : pool.size());
    if (threads < 2 || morsels < 2) {
        return whole();
    }

    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
    }

    // One slot per morsel, each written by exactly one worker — no lock, and
    // the result order is positional rather than dependent on completion order.
    std::vector<std::expected<ComputedColumn, std::string>> pieces(morsels);
    std::atomic<std::size_t> cursor{0};
    auto batch = pool.submit(threads, [&](std::size_t) {
        while (true) {
            const std::size_t index = cursor.fetch_add(1, std::memory_order_relaxed);
            if (index >= morsels) {
                return;
            }
            const std::size_t begin = index * grain;
            const std::size_t count = std::min(grain, rows - begin);
            pieces[index] =
                evaluate_field(expr, table, RowRange{.begin = begin, .count = count}, ctx);
        }
    });
    batch.wait();

    // Lowest failing morsel wins, so the reported error does not depend on
    // thread timing — the same rule the island merger uses.
    for (auto& piece : pieces) {
        if (!piece.has_value()) {
            return std::unexpected(piece.error());
        }
    }

    const bool any_validity =
        std::ranges::any_of(pieces, [](const auto& p) { return p->validity.has_value(); });
    ColumnValue out = inferred.value() == ExprType::Int ? ColumnValue{Column<std::int64_t>{}}
                                                        : ColumnValue{Column<double>{}};
    std::visit(
        [&](auto& dst) {
            using ColT = std::decay_t<decltype(dst)>;
            // Only the two numeric alternatives are constructed above; the rest
            // exist so the visit compiles over the whole variant.
            if constexpr (std::is_same_v<ColT, Column<std::int64_t>> ||
                          std::is_same_v<ColT, Column<double>>) {
                dst.resize_for_overwrite(rows);
                std::size_t at = 0;
                for (auto& piece : pieces) {
                    const auto& src = std::get<ColT>(piece->column);
                    std::memcpy(dst.data() + at, src.data(),
                                src.size() * sizeof(typename ColT::value_type));
                    at += src.size();
                }
            } else {
                invariant_violation("evaluate_field_maybe_parallel: non-numeric result column");
            }
        },
        out);

    std::optional<ValidityBitmap> validity;
    if (any_validity) {
        ValidityBitmap merged(rows, true);
        std::size_t at = 0;
        for (auto& piece : pieces) {
            const std::size_t n = std::visit([](const auto& c) { return c.size(); }, piece->column);
            if (piece->validity.has_value()) {
                for (std::size_t i = 0; i < n; ++i) {
                    merged.set(at + i, (*piece->validity)[i]);
                }
            }
            at += n;
        }
        validity = std::move(merged);
    }
    return ComputedColumn{.column = std::move(out), .validity = std::move(validity)};
}

}  // namespace

auto update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                  const ExecutionContext& exec) -> std::expected<Table, std::string> {
    Table output = std::move(input);
    if (output.time_index.has_value()) {
        if (auto ok = check_row_order(output, fields); !ok) {
            return std::unexpected(ok.error());
        }
        for (const auto& field : fields) {
            if (field.alias == *output.time_index) {
                return std::unexpected("cannot update time index column: " + field.alias);
            }
        }
    }
    std::size_t rows = output.rows();
    for (const auto& field : fields) {
        if (const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node)) {
            auto res = evaluate_rank_column(output, *rank, {});
            if (!res) {
                return std::unexpected(res.error());
            }
            add_computed_column(output, field.alias, std::move(*res));
            continue;
        }
        if (const auto* col_ref = std::get_if<ir::ColumnRef>(&field.expr.node)) {
            const auto* entry = output.find_entry(col_ref->name);
            if (entry != nullptr) {
                // `alias = other_column` renames rather than computes, so the
                // two names can share one buffer under the copy-on-write
                // invariant. Deep-copying here moved 26MB per renamed key
                // column on a q03-shaped scan — the join-key alignment idiom
                // (`select { o_orderkey = l_orderkey }`) hits this on nearly
                // every query.
                output.add_column_shared(field.alias, entry->column, entry->validity);
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
        output, derive_table_properties(
                    table_properties_of(output),
                    [&](const std::string& name) -> std::optional<std::string> {
                        const bool overwritten = std::ranges::any_of(
                            fields, [&](const ir::FieldSpec& f) { return f.alias == name; });
                        return overwritten ? std::nullopt : std::optional<std::string>{name};
                    },
                    RowTransform::Preserve));
    return output;
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
        if (literal != nullptr && old_entry != nullptr && !old_entry->validity.has_value()) {
            ColumnValue replacement =
                broadcast_scalar_column(scalar_from_literal(*literal), std::size_t{1});
            if (old_entry->column->index() != replacement.index()) {
                return std::unexpected(
                    "guarded update cannot change the type of existing column '" + field.alias +
                    "'");
            }

            auto result = std::visit(
                [&](const auto& old_col) -> std::optional<ColumnValue> {
                    using Col = std::decay_t<decltype(old_col)>;
                    if constexpr (std::is_same_v<Col, Column<std::string>> ||
                                  std::is_same_v<Col, Column<Categorical>>) {
                        // Flat strings cannot be overwritten in place, and a
                        // string literal is not dictionary-compatible with a
                        // categorical column. Keep their established path.
                        return std::nullopt;
                    } else {
                        Col out = old_col;
                        const auto& scalar_col = std::get<Col>(replacement);
                        const auto value = scalar_col[0];
                        if (mask->valid.has_value()) {
                            const auto& valid = *mask->valid;
                            for (std::size_t i = 0; i < n; ++i) {
                                if (mask->value[i] != 0 && valid[i]) {
                                    out[i] = value;
                                }
                            }
                        } else {
                            for (std::size_t i = 0; i < n; ++i) {
                                if (mask->value[i] != 0) {
                                    out[i] = value;
                                }
                            }
                        }
                        return ColumnValue{std::move(out)};
                    }
                },
                *old_entry->column);
            if (result.has_value()) {
                const auto pos = input.index.at(field.alias);
                Table output = std::move(input);
                output.replace_column(pos, std::move(*result));
                return output;
            }
        }
    }

    std::vector<std::size_t> matched_idx;
    matched_idx.reserve(n / 8);
    for (std::size_t i = 0; i < n; ++i) {
        const bool m = mask->value[i] != 0 && (!mask->valid.has_value() || (*mask->valid)[i] != 0);
        if (m) {
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
        const std::shared_ptr<ColumnValue> old_col =
            old_entry != nullptr ? old_entry->column : nullptr;
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
                        subset_source.add_column_shared(entry.name, entry.column, entry.validity);
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

        // A guarded assignment may not change the type of an existing column —
        // non-matching rows must keep their old (same-type) values.
        if (old_col != nullptr && old_col->index() != new_vals->index()) {
            return std::unexpected("guarded update cannot change the type of existing column '" +
                                   field.alias + "'");
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
                        if (subset) {
                            for (std::size_t k = 0; k < matched_idx.size(); ++k) {
                                out[matched_idx[k]] = src[k];
                            }
                        } else {
                            for (const std::size_t i : matched_idx) {
                                out[i] = src[i];
                            }
                        }
                        return {ColumnValue{std::move(out)}, std::nullopt};
                    }
                }

                Col out;
                out.reserve(n);
                ValidityBitmap valid(n, true);
                bool any_invalid = false;
                std::size_t k = 0;  // running index into `src` for the subset case
                for (std::size_t i = 0; i < n; ++i) {
                    const bool matches =
                        mask->value[i] != 0 && (!mask->valid.has_value() || (*mask->valid)[i] != 0);
                    if (matches) {
                        const std::size_t si = subset ? k++ : i;
                        out.push_back(src[si]);
                        const bool v = !new_valid.has_value() || (*new_valid)[si];
                        valid.set(i, v);
                        any_invalid = any_invalid || !v;
                    } else if (oldc != nullptr) {
                        out.push_back((*oldc)[i]);
                        const bool v = old_valid == nullptr || (*old_valid)[i];
                        valid.set(i, v);
                        any_invalid = any_invalid || !v;
                    } else {
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            out.push_back(std::string_view{});
                        } else {
                            out.push_back(typename Col::value_type{});
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

        if (auto it = output.index.find(field.alias); it != output.index.end()) {
            output.replace_column(it->second, std::move(result_col), std::move(result_valid));
        } else if (result_valid.has_value()) {
            output.add_column(field.alias, std::move(result_col), std::move(*result_valid));
        } else {
            output.add_column(field.alias, std::move(result_col));
        }
    }
    return output;
}

/// Per-group update: partition the input by `group_by`, run the regular
/// `update_table` on each per-group slice, then scatter the new field
/// columns back into a single full-sized output. Ordered ops like `lag`,
/// `lead`, `cumsum`, and `fill_forward` therefore see only their group's
/// rows; pure arithmetic gives the same answer per row regardless.
auto grouped_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                          const std::vector<ir::ColumnRef>& group_by, const ScalarRegistry* scalars,
                          const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    if (group_by.empty()) {
        return update_table(std::move(input), fields, scalars, externs, exec);
    }
    if (input.time_index.has_value()) {
        for (const auto& field : fields) {
            if (field.alias == *input.time_index) {
                return std::unexpected("cannot update time index column: " + field.alias);
            }
        }
    }

    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* col = input.find(key.name);
        if (col == nullptr) {
            return std::unexpected("update + by: unknown group key '" + key.name +
                                   "' (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(col);
    }
    // Nulls in a group key form their own group; without this a null key would
    // merge into the genuine zero/empty group.
    const auto group_validity = collect_key_validity(input, group_by);

    const std::size_t rows = input.rows();
    if (rows == 0) {
        return update_table(std::move(input), fields, scalars, externs, exec);
    }

    robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> group_index;
    std::vector<std::vector<std::size_t>> group_rows;
    for (std::size_t r = 0; r < rows; ++r) {
        Key key;
        key.values.reserve(group_columns.size());
        for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
            push_key_value(key, *group_columns[ci], group_validity[ci], r);
        }
        auto [it, inserted] =
            group_index.emplace(std::move(key), static_cast<std::uint32_t>(group_rows.size()));
        if (inserted) {
            group_rows.emplace_back();
        }
        group_rows[it->second].push_back(r);
    }

    auto run_group =
        [&](const std::vector<std::size_t>& row_idx) -> std::expected<Table, std::string> {
        Table sub;
        for (const auto& entry : input.columns) {
            ColumnValue gathered = gather_column(*entry.column, row_idx.data(), row_idx.size());
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
        sub.time_index = input.time_index;

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
    if (first->columns.size() <= first_new_idx) {
        return std::unexpected("update: grouped update produced no new columns");
    }
    std::vector<std::string> new_field_names;
    new_field_names.reserve(first->columns.size() - first_new_idx);
    for (std::size_t c = first_new_idx; c < first->columns.size(); ++c) {
        new_field_names.push_back(first->columns[c].name);
    }

    Table output = input;
    auto allocate_full = [&](const ColumnValue& sample) -> std::expected<ColumnValue, std::string> {
        return std::visit(
            [&](const auto& col) -> std::expected<ColumnValue, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                              std::is_same_v<ColT, Column<Categorical>>) {
                    return std::unexpected(
                        "update + by: scatter of string/Categorical results not yet implemented");
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
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
    for (const auto& fname : new_field_names) {
        const auto* sample = first->find(fname);
        if (sample == nullptr) {
            return std::unexpected("update: missing new column '" + fname + "' in sub-result");
        }
        auto full = allocate_full(*sample);
        if (!full.has_value()) {
            return std::unexpected(full.error());
        }
        output.add_column(fname, std::move(full.value()));
    }

    auto scatter_into = [](ColumnValue& dst, const ColumnValue& src,
                           const std::vector<std::size_t>& indices) -> std::optional<std::string> {
        return std::visit(
            [&](auto& dcol) -> std::optional<std::string> {
                using DT = std::decay_t<decltype(dcol)>;
                const DT* scol = std::get_if<DT>(&src);
                if (scol == nullptr) {
                    return "update: type mismatch in grouped scatter";
                }
                if constexpr (std::is_same_v<DT, Column<std::string>> ||
                              std::is_same_v<DT, Column<Categorical>>) {
                    return "update: scatter for string/Categorical not implemented";
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
    std::vector<std::optional<ValidityBitmap>> output_validity(new_field_names.size());

    auto scatter_validity = [&](std::size_t f_idx, const Table& sub_table,
                                const std::vector<std::size_t>& indices) {
        const auto* sub_entry = sub_table.find_entry(new_field_names[f_idx]);
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

    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        const auto& fname = new_field_names[f];
        auto* dst = output.find(fname);
        const auto* src = first->find(fname);
        if (auto err = scatter_into(*dst, *src, group_rows[0])) {
            return std::unexpected(*err);
        }
        scatter_validity(f, *first, group_rows[0]);
    }

    for (std::size_t g = 1; g < group_rows.size(); ++g) {
        auto sub = run_group(group_rows[g]);
        if (!sub.has_value()) {
            return std::unexpected(sub.error());
        }
        for (std::size_t f = 0; f < new_field_names.size(); ++f) {
            const auto& fname = new_field_names[f];
            auto* dst = output.find(fname);
            const auto* src = sub->find(fname);
            if (src == nullptr) {
                return std::unexpected("update: missing column '" + fname +
                                       "' in grouped sub-result");
            }
            if (auto err = scatter_into(*dst, *src, group_rows[g])) {
                return std::unexpected(*err);
            }
            scatter_validity(f, *sub, group_rows[g]);
        }
    }
    for (std::size_t f = 0; f < new_field_names.size(); ++f) {
        if (!output_validity[f].has_value()) {
            continue;
        }
        auto idx_it = output.index.find(new_field_names[f]);
        if (idx_it != output.index.end()) {
            output.columns[idx_it->second].validity = std::move(output_validity[f]);
        }
    }
    normalize_time_index(output);
    return output;
}

}  // namespace ibex::runtime
