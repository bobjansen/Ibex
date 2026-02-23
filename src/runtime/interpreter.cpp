#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <robin_hood.h>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ibex::runtime {

namespace {

auto is_simple_identifier(std::string_view name) -> bool {
    if (name.empty()) {
        return false;
    }
    auto is_alpha = [](unsigned char ch) -> bool {
        return std::isalpha(ch) != 0;
    };
    auto is_alnum = [](unsigned char ch) -> bool {
        return std::isalnum(ch) != 0;
    };
    unsigned char first = static_cast<unsigned char>(name.front());
    if (!is_alpha(first) && first != '_') {
        return false;
    }
    for (std::size_t i = 1; i < name.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(name[i]);
        if (!is_alnum(ch) && ch != '_') {
            return false;
        }
    }
    return true;
}

auto format_columns(const Table& table) -> std::string {
    if (table.columns.empty()) {
        return "<none>";
    }
    std::string out;
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        const auto& name = table.columns[i].name;
        if (is_simple_identifier(name)) {
            out.append(name);
        } else {
            out.push_back('`');
            out.append(name);
            out.push_back('`');
        }
    }
    return out;
}

auto format_scalars(const ScalarRegistry& scalars) -> std::string {
    if (scalars.empty()) {
        return "<none>";
    }
    std::vector<std::string_view> names;
    names.reserve(scalars.size());
    for (const auto& entry : scalars) {
        names.emplace_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

auto format_tables(const TableRegistry& registry) -> std::string {
    if (registry.empty()) {
        return "<none>";
    }
    std::vector<std::string_view> names;
    names.reserve(registry.size());
    for (const auto& entry : registry) {
        names.emplace_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

auto column_size(const ColumnValue& column) -> std::size_t {
    return std::visit([](const auto& col) { return col.size(); }, column);
}

auto append_value(ColumnValue& out, const ColumnValue& src, std::size_t index) -> void {
    std::visit(
        [&](auto& dst_col) {
            using ColType = std::decay_t<decltype(dst_col)>;
            const auto* src_col = std::get_if<ColType>(&src);
            if (src_col == nullptr) {
                throw std::runtime_error("column type mismatch");
            }
            dst_col.push_back((*src_col)[index]);
        },
        out);
}

auto make_empty_like(const ColumnValue& src) -> ColumnValue {
    return std::visit(
        [](const auto& col) -> ColumnValue {
            using ColType = std::decay_t<decltype(col)>;
            return ColType{};
        },
        src);
}

auto compare_value(const ColumnValue& column, std::size_t index, const ir::FilterPredicate& pred,
                   const std::variant<std::int64_t, double, std::string>& value) -> bool {
    return std::visit(
        [&](const auto& col) -> bool {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* v = std::get_if<std::int64_t>(&value)) {
                    switch (pred.op) {
                        case ir::CompareOp::Eq:
                            return col[index] == *v;
                        case ir::CompareOp::Ne:
                            return col[index] != *v;
                        case ir::CompareOp::Lt:
                            return col[index] < *v;
                        case ir::CompareOp::Le:
                            return col[index] <= *v;
                        case ir::CompareOp::Gt:
                            return col[index] > *v;
                        case ir::CompareOp::Ge:
                            return col[index] >= *v;
                    }
                }
                if (const auto* v = std::get_if<double>(&value)) {
                    double lhs = static_cast<double>(col[index]);
                    switch (pred.op) {
                        case ir::CompareOp::Eq:
                            return lhs == *v;
                        case ir::CompareOp::Ne:
                            return lhs != *v;
                        case ir::CompareOp::Lt:
                            return lhs < *v;
                        case ir::CompareOp::Le:
                            return lhs <= *v;
                        case ir::CompareOp::Gt:
                            return lhs > *v;
                        case ir::CompareOp::Ge:
                            return lhs >= *v;
                    }
                }
                return false;
            } else if constexpr (std::is_same_v<ValueType, double>) {
                double lhs = col[index];
                if (const auto* v = std::get_if<double>(&value)) {
                    switch (pred.op) {
                        case ir::CompareOp::Eq:
                            return lhs == *v;
                        case ir::CompareOp::Ne:
                            return lhs != *v;
                        case ir::CompareOp::Lt:
                            return lhs < *v;
                        case ir::CompareOp::Le:
                            return lhs <= *v;
                        case ir::CompareOp::Gt:
                            return lhs > *v;
                        case ir::CompareOp::Ge:
                            return lhs >= *v;
                    }
                }
                if (const auto* v = std::get_if<std::int64_t>(&value)) {
                    double rhs = static_cast<double>(*v);
                    switch (pred.op) {
                        case ir::CompareOp::Eq:
                            return lhs == rhs;
                        case ir::CompareOp::Ne:
                            return lhs != rhs;
                        case ir::CompareOp::Lt:
                            return lhs < rhs;
                        case ir::CompareOp::Le:
                            return lhs <= rhs;
                        case ir::CompareOp::Gt:
                            return lhs > rhs;
                        case ir::CompareOp::Ge:
                            return lhs >= rhs;
                    }
                }
                return false;
            } else if constexpr (std::is_same_v<ValueType, std::string>) {
                if (const auto* v = std::get_if<std::string>(&value)) {
                    switch (pred.op) {
                        case ir::CompareOp::Eq:
                            return col[index] == *v;
                        case ir::CompareOp::Ne:
                            return col[index] != *v;
                        case ir::CompareOp::Lt:
                            return col[index] < *v;
                        case ir::CompareOp::Le:
                            return col[index] <= *v;
                        case ir::CompareOp::Gt:
                            return col[index] > *v;
                        case ir::CompareOp::Ge:
                            return col[index] >= *v;
                    }
                }
                return false;
            } else {
                return false;
            }
        },
        column);
}

auto resolve_filter_value(const ir::FilterPredicate& predicate, const ScalarRegistry* scalars)
    -> std::expected<std::variant<std::int64_t, double, std::string>, std::string> {
    if (const auto* literal = std::get_if<std::int64_t>(&predicate.value)) {
        return *literal;
    }
    if (const auto* literal = std::get_if<double>(&predicate.value)) {
        return *literal;
    }
    if (const auto* literal = std::get_if<std::string>(&predicate.value)) {
        return *literal;
    }
    const auto* ref = std::get_if<ir::FilterPredicate::ScalarRef>(&predicate.value);
    if (ref == nullptr) {
        return std::unexpected("filter predicate value not supported");
    }
    if (scalars == nullptr) {
        return std::unexpected("unknown scalar in filter: " + ref->name);
    }
    auto it = scalars->find(ref->name);
    if (it == scalars->end()) {
        return std::unexpected("unknown scalar in filter: " + ref->name +
                               " (available: " + format_scalars(*scalars) + ")");
    }
    if (const auto* int_value = std::get_if<std::int64_t>(&it->second)) {
        return *int_value;
    }
    if (const auto* double_value = std::get_if<double>(&it->second)) {
        return *double_value;
    }
    if (const auto* str_value = std::get_if<std::string>(&it->second)) {
        return *str_value;
    }
    return std::unexpected("scalar type not supported in filter");
}

auto filter_table(const Table& input, const ir::FilterPredicate& predicate,
                  const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    const auto* predicate_column = input.find(predicate.column.name);
    if (predicate_column == nullptr) {
        return std::unexpected("filter column not found: " + predicate.column.name +
                               " (available: " + format_columns(input) + ")");
    }
    auto resolved = resolve_filter_value(predicate, scalars);
    if (!resolved) {
        return std::unexpected(resolved.error());
    }
    std::size_t rows = column_size(*predicate_column);

    Table output;
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(*entry.column));
    }

    for (std::size_t row = 0; row < rows; ++row) {
        if (!compare_value(*predicate_column, row, predicate, resolved.value())) {
            continue;
        }
        for (auto& entry : output.columns) {
            const auto* source = input.find(entry.name);
            if (source == nullptr) {
                return std::unexpected("filter column missing: " + entry.name);
            }
            append_value(*entry.column, *source, row);
        }
    }
    return output;
}

auto project_table(const Table& input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Table, std::string> {
    Table output;
    for (const auto& col : columns) {
        const auto* source = input.find(col.name);
        if (source == nullptr) {
            return std::unexpected("select column not found: " + col.name +
                                   " (available: " + format_columns(input) + ")");
        }
        output.add_column(col.name, *source);
    }
    return output;
}

using ScalarValue = std::variant<std::int64_t, double, std::string>;

struct Key {
    std::vector<ScalarValue> values;
};

struct KeyHash {
    auto operator()(const Key& key) const -> std::size_t {
        std::size_t seed = 0;
        auto hash_combine = [&](std::size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        for (const auto& value : key.values) {
            std::size_t h = std::visit(
                [](const auto& v) { return std::hash<std::decay_t<decltype(v)>>{}(v); }, value);
            hash_combine(h);
        }
        return seed;
    }
};

struct KeyEq {
    auto operator()(const Key& a, const Key& b) const -> bool { return a.values == b.values; }
};

enum class ExprType : std::uint8_t {
    Int,
    Double,
    String,
};

using ExprValue = std::variant<std::int64_t, double, std::string>;

struct AggSlot {
    ir::AggFunc func = ir::AggFunc::Sum;
    ExprType kind = ExprType::Int;
    bool has_value = false;
    std::int64_t count = 0;
    std::int64_t int_value = 0;
    double double_value = 0.0;
    double sum = 0.0;
    ScalarValue first_value;
    ScalarValue last_value;
};

struct AggState {
    std::vector<AggSlot> slots;
};

auto expr_type_for_column(const ColumnValue& column) -> ExprType {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return ExprType::Double;
    }
    return ExprType::String;
}

auto scalar_from_column(const ColumnValue& column, std::size_t row) -> ScalarValue {
    return std::visit([&](const auto& col) -> ScalarValue { return col[row]; }, column);
}

auto distinct_table(const Table& input) -> std::expected<Table, std::string> {
    if (input.columns.empty()) {
        return input;
    }
    std::size_t rows = input.rows();
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(*entry.column));
    }
    for (auto& entry : output.columns) {
        std::visit([&](auto& col) { col.reserve(rows); }, *entry.column);
    }

    robin_hood::unordered_flat_set<Key, KeyHash, KeyEq> seen;
    seen.reserve(rows);

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(input.columns.size());
        for (const auto& entry : input.columns) {
            key.values.push_back(scalar_from_column(*entry.column, row));
        }
        if (!seen.insert(std::move(key)).second) {
            continue;
        }
        for (std::size_t col = 0; col < input.columns.size(); ++col) {
            append_value(*output.columns[col].column, *input.columns[col].column, row);
        }
    }
    return output;
}

auto append_scalar(ColumnValue& column, const ScalarValue& value) -> void {
    std::visit(
        [&](auto& col) {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value);
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(static_cast<std::int64_t>(*double_value));
                } else {
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ValueType, double>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(static_cast<double>(*int_value));
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(*double_value);
                } else {
                    throw std::runtime_error("type mismatch");
                }
            } else if constexpr (std::is_same_v<ValueType, std::string>) {
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    throw std::runtime_error("type mismatch");
                }
            }
        },
        column);
}

struct FastOperand {
    bool is_column = false;
    const ColumnValue* column = nullptr;
    ScalarValue literal;
    ExprType kind = ExprType::Int;
};

auto scalar_kind_from_value(const ScalarValue& value) -> ExprType {
    if (std::holds_alternative<std::int64_t>(value)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<double>(value)) {
        return ExprType::Double;
    }
    return ExprType::String;
}

auto resolve_fast_operand(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::optional<FastOperand> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (const auto* source = input.find(col->name); source != nullptr) {
            return FastOperand{true, source, ScalarValue{}, expr_type_for_column(*source)};
        }
        if (scalars != nullptr) {
            if (auto it = scalars->find(col->name); it != scalars->end()) {
                return FastOperand{false, nullptr, it->second, scalar_kind_from_value(it->second)};
            }
        }
        return std::nullopt;
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        return FastOperand{false, nullptr, lit->value, scalar_kind_from_value(lit->value)};
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
            return lhs / rhs;
        case ir::ArithmeticOp::Mod:
            return lhs % rhs;
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
        return static_cast<std::int64_t>(std::get<double>(op.literal));
    }
    if (const auto* int_col = std::get_if<Column<std::int64_t>>(op.column)) {
        return (*int_col)[row];
    }
    if (const auto* double_col = std::get_if<Column<double>>(op.column)) {
        return static_cast<std::int64_t>((*double_col)[row]);
    }
    throw std::runtime_error("type mismatch");
}

auto get_double_value(const FastOperand& op, std::size_t row) -> double {
    if (!op.is_column) {
        if (const auto* int_value = std::get_if<std::int64_t>(&op.literal)) {
            return static_cast<double>(*int_value);
        }
        return std::get<double>(op.literal);
    }
    if (const auto* int_col = std::get_if<Column<std::int64_t>>(op.column)) {
        return static_cast<double>((*int_col)[row]);
    }
    if (const auto* double_col = std::get_if<Column<double>>(op.column)) {
        return (*double_col)[row];
    }
    throw std::runtime_error("type mismatch");
}

auto try_fast_update_binary(const ir::Expr& expr, const Table& input, std::size_t rows,
                            ExprType output_kind, const ScalarRegistry* scalars)
    -> std::optional<ColumnValue> {
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
    if (left->kind == ExprType::String || right->kind == ExprType::String) {
        return std::nullopt;
    }
    // Helper: dispatch on (op × layout) OUTSIDE the inner loop so each resulting
    // loop body is a branch-free array kernel that the compiler can auto-vectorize.
    // `run` receives a stateless lambda (unique type per op) and executes the
    // appropriate col/col, col/scalar, or scalar/col loop.
    auto make_double_result = [&](auto op_fn, const double* lp, double ls, const double* rp,
                                  double rs) -> ColumnValue {
        Column<double> out;
        out.resize(rows);
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
        out.resize(rows);
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
            double value =
                apply_double_op(bin->op, get_double_value(*left, 0), get_double_value(*right, 0));
            Column<double> out;
            out.assign(rows, value);
            return ColumnValue{std::move(out)};
        }
        // Hoist all variant/type dispatch outside the inner loop.
        // Falls back to nullptr + scalar=0 for int-typed columns (uncommon path
        // handled by the fallback reserve+push_back loop below).
        const double* lp = (left->is_column && left->kind == ExprType::Double)
                               ? std::get<Column<double>>(*left->column).data()
                               : nullptr;
        const double* rp = (right->is_column && right->kind == ExprType::Double)
                               ? std::get<Column<double>>(*right->column).data()
                               : nullptr;
        double ls = left->is_column ? 0.0 : get_double_value(*left, 0);
        double rs = right->is_column ? 0.0 : get_double_value(*right, 0);
        // Only take the SIMD path when every used operand resolved to a raw pointer.
        bool left_ok = !left->is_column || lp != nullptr;
        bool right_ok = !right->is_column || rp != nullptr;
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
            out.push_back(apply_double_op(bin->op, get_double_value(*left, row),
                                          get_double_value(*right, row)));
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
                                     ? std::get<Column<std::int64_t>>(*left->column).data()
                                     : nullptr;
        const std::int64_t* rp = (right->is_column && right->kind == ExprType::Int)
                                     ? std::get<Column<std::int64_t>>(*right->column).data()
                                     : nullptr;
        std::int64_t ls = left->is_column ? 0 : get_int_value(*left, 0);
        std::int64_t rs = right->is_column ? 0 : get_int_value(*right, 0);
        bool left_ok = !left->is_column || lp != nullptr;
        bool right_ok = !right->is_column || rp != nullptr;
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
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a / b; }, lp,
                                           ls, rp, rs);
                case ir::ArithmeticOp::Mod:
                    return make_int_result([](std::int64_t a, std::int64_t b) { return a % b; }, lp,
                                           ls, rp, rs);
            }
        }
        Column<std::int64_t> out;
        out.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row)
            out.push_back(
                apply_int_op(bin->op, get_int_value(*left, row), get_int_value(*right, row)));
        return ColumnValue{std::move(out)};
    }
    return std::nullopt;
}

auto default_scalar_for_column(const ColumnValue& column) -> ScalarValue {
    return std::visit(
        [](const auto& col) -> ScalarValue {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                return std::int64_t{0};
            } else if constexpr (std::is_same_v<ValueType, double>) {
                return 0.0;
            } else {
                return std::string{};
            }
        },
        column);
}

auto column_kind(const ColumnValue& column) -> ExprType {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return ExprType::Double;
    }
    return ExprType::String;
}

auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                     const std::vector<std::string>& keys) -> std::expected<Table, std::string> {
    if (kind == ir::JoinKind::Asof) {
        return std::unexpected("asof join is not supported yet");
    }
    if (keys.empty()) {
        return std::unexpected("join requires at least one key");
    }

    std::vector<const ColumnValue*> left_keys;
    std::vector<const ColumnValue*> right_keys;
    left_keys.reserve(keys.size());
    right_keys.reserve(keys.size());
    for (const auto& key : keys) {
        const auto* left_col = left.find(key);
        if (left_col == nullptr) {
            return std::unexpected("join key not found in left: " + key +
                                   " (available: " + format_columns(left) + ")");
        }
        const auto* right_col = right.find(key);
        if (right_col == nullptr) {
            return std::unexpected("join key not found in right: " + key +
                                   " (available: " + format_columns(right) + ")");
        }
        if (column_kind(*left_col) != column_kind(*right_col)) {
            return std::unexpected("join key type mismatch for " + key);
        }
        left_keys.push_back(left_col);
        right_keys.push_back(right_col);
    }

    std::unordered_set<std::string> key_set(keys.begin(), keys.end());

    Table output;
    output.columns.reserve(left.columns.size() + right.columns.size());

    std::unordered_set<std::string> out_names;
    out_names.reserve(left.columns.size() + right.columns.size());

    for (const auto& entry : left.columns) {
        out_names.insert(entry.name);
        output.add_column(entry.name, make_empty_like(*entry.column));
    }

    struct RightOut {
        const ColumnValue* column = nullptr;
        std::size_t out_index = 0;
    };
    std::vector<RightOut> right_out;
    right_out.reserve(right.columns.size());
    for (const auto& entry : right.columns) {
        if (key_set.contains(entry.name)) {
            continue;
        }
        std::string name = entry.name;
        while (out_names.contains(name)) {
            name += "_right";
        }
        out_names.insert(name);
        output.add_column(name, make_empty_like(*entry.column));
        right_out.push_back(RightOut{entry.column.get(), output.columns.size() - 1});
    }

    std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_index;
    right_index.reserve(right.rows());
    for (std::size_t r = 0; r < right.rows(); ++r) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : right_keys) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        right_index[key].push_back(r);
    }

    auto append_left_row = [&](std::size_t row) {
        for (std::size_t i = 0; i < left.columns.size(); ++i) {
            append_value(*output.columns[i].column, *left.columns[i].column, row);
        }
    };

    auto append_right_row = [&](std::size_t row) {
        for (const auto& item : right_out) {
            append_value(*output.columns[item.out_index].column, *item.column, row);
        }
    };

    auto append_right_defaults = [&]() {
        for (const auto& item : right_out) {
            append_scalar(*output.columns[item.out_index].column,
                          default_scalar_for_column(*item.column));
        }
    };

    for (std::size_t l = 0; l < left.rows(); ++l) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : left_keys) {
            key.values.push_back(scalar_from_column(*col, l));
        }
        auto it = right_index.find(key);
        if (it == right_index.end()) {
            if (kind == ir::JoinKind::Left) {
                append_left_row(l);
                append_right_defaults();
            }
            continue;
        }
        for (auto r : it->second) {
            append_left_row(l);
            append_right_row(r);
        }
    }

    return output;
}

auto aggregate_table(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                     const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("group-by column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        group_columns.push_back(column);
    }

    std::vector<const ColumnValue*> agg_columns;
    agg_columns.reserve(aggregations.size());
    for (const auto& agg : aggregations) {
        if (agg.func == ir::AggFunc::Count) {
            agg_columns.push_back(nullptr);
            continue;
        }
        const auto* column = input.find(agg.column.name);
        if (column == nullptr) {
            return std::unexpected("aggregate column not found: " + agg.column.name +
                                   " (available: " + format_columns(input) + ")");
        }
        agg_columns.push_back(column);
    }

    struct AggPlanItem {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
        const Column<std::int64_t>* int_col = nullptr;
        const Column<double>* dbl_col = nullptr;
        const Column<std::string>* str_col = nullptr;
    };

    std::vector<AggPlanItem> plan;
    plan.reserve(aggregations.size());
    bool numeric_only = true;
    for (std::size_t i = 0; i < aggregations.size(); ++i) {
        const auto& agg = aggregations[i];
        AggPlanItem item;
        item.func = agg.func;
        if (agg.func == ir::AggFunc::Count) {
            item.kind = ExprType::Int;
        } else {
            item.kind = expr_type_for_column(*agg_columns[i]);
            if (const auto* int_col = std::get_if<Column<std::int64_t>>(agg_columns[i])) {
                item.int_col = int_col;
            } else if (const auto* dbl_col = std::get_if<Column<double>>(agg_columns[i])) {
                item.dbl_col = dbl_col;
            } else if (const auto* str_col = std::get_if<Column<std::string>>(agg_columns[i])) {
                item.str_col = str_col;
            }
        }

        if (item.kind == ExprType::String &&
            (agg.func == ir::AggFunc::Sum || agg.func == ir::AggFunc::Mean ||
             agg.func == ir::AggFunc::Min || agg.func == ir::AggFunc::Max)) {
            return std::unexpected("string aggregation not supported");
        }

        if (agg.func == ir::AggFunc::First || agg.func == ir::AggFunc::Last) {
            // numeric First/Last are handled in the fast path; only fall back for strings
            if (item.kind == ExprType::String) {
                numeric_only = false;
            }
        } else if (agg.func != ir::AggFunc::Count && agg.func != ir::AggFunc::Sum &&
                   agg.func != ir::AggFunc::Mean && agg.func != ir::AggFunc::Min &&
                   agg.func != ir::AggFunc::Max) {
            numeric_only = false;
        }
        if (item.kind == ExprType::String) {
            numeric_only = false;
        }

        plan.push_back(item);
    }

    auto make_state = [&]() -> AggState {
        AggState state;
        state.slots.reserve(aggregations.size());
        for (std::size_t i = 0; i < plan.size(); ++i) {
            AggSlot slot;
            slot.func = plan[i].func;
            slot.kind = plan[i].kind;
            state.slots.push_back(slot);
        }
        return state;
    };

    auto update_state = [&](AggState& state, std::size_t row) -> std::optional<std::string> {
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto& agg = aggregations[i];
            AggSlot& slot = state.slots[i];
            if (agg.func == ir::AggFunc::Count) {
                slot.count += 1;
                continue;
            }
            const ColumnValue& column = *agg_columns[i];
            if (agg.func == ir::AggFunc::First) {
                if (!slot.has_value) {
                    slot.first_value = scalar_from_column(column, row);
                }
                slot.has_value = true;
                continue;
            }
            if (agg.func == ir::AggFunc::Last) {
                slot.last_value = scalar_from_column(column, row);
                slot.has_value = true;
                continue;
            }

            if (slot.kind == ExprType::Int) {
                std::int64_t value = 0;
                if (std::holds_alternative<Column<std::int64_t>>(column)) {
                    value = std::get<std::int64_t>(scalar_from_column(column, row));
                } else {
                    value = static_cast<std::int64_t>(
                        std::get<double>(scalar_from_column(column, row)));
                }
                switch (agg.func) {
                    case ir::AggFunc::Sum:
                        slot.int_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += static_cast<double>(value);
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                        break;
                }
                slot.has_value = true;
            } else {
                double value = 0.0;
                if (std::holds_alternative<Column<double>>(column)) {
                    value = std::get<double>(scalar_from_column(column, row));
                } else {
                    value = static_cast<double>(
                        std::get<std::int64_t>(scalar_from_column(column, row)));
                }
                switch (agg.func) {
                    case ir::AggFunc::Sum:
                        slot.double_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += value;
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                        break;
                }
                slot.has_value = true;
            }
        }
        return std::nullopt;
    };

    auto update_state_numeric = [&](AggState& state,
                                    std::size_t row) -> std::optional<std::string> {
        for (std::size_t i = 0; i < plan.size(); ++i) {
            const auto& item = plan[i];
            AggSlot& slot = state.slots[i];
            if (item.func == ir::AggFunc::Count) {
                slot.count += 1;
                continue;
            }
            if (item.func == ir::AggFunc::First) {
                if (!slot.has_value) {
                    if (item.int_col != nullptr) {
                        slot.int_value = (*item.int_col)[row];
                    } else if (item.dbl_col != nullptr) {
                        slot.double_value = (*item.dbl_col)[row];
                    }
                    slot.has_value = true;
                }
                continue;
            }
            if (item.func == ir::AggFunc::Last) {
                if (item.int_col != nullptr) {
                    slot.int_value = (*item.int_col)[row];
                } else if (item.dbl_col != nullptr) {
                    slot.double_value = (*item.dbl_col)[row];
                }
                slot.has_value = true;
                continue;
            }

            if (slot.kind == ExprType::Int) {
                std::int64_t value = 0;
                if (item.int_col != nullptr) {
                    value = (*item.int_col)[row];
                } else if (item.dbl_col != nullptr) {
                    value = static_cast<std::int64_t>((*item.dbl_col)[row]);
                }

                switch (item.func) {
                    case ir::AggFunc::Sum:
                        slot.int_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += static_cast<double>(value);
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.int_value) {
                            slot.int_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                        break;
                }
                slot.has_value = true;
            } else {
                double value = 0.0;
                if (item.dbl_col != nullptr) {
                    value = (*item.dbl_col)[row];
                } else if (item.int_col != nullptr) {
                    value = static_cast<double>((*item.int_col)[row]);
                }
                switch (item.func) {
                    case ir::AggFunc::Sum:
                        slot.double_value += value;
                        break;
                    case ir::AggFunc::Mean:
                        slot.sum += value;
                        slot.count += 1;
                        break;
                    case ir::AggFunc::Min:
                        if (!slot.has_value || value < slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Max:
                        if (!slot.has_value || value > slot.double_value) {
                            slot.double_value = value;
                        }
                        break;
                    case ir::AggFunc::Count:
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                        break;
                }
                slot.has_value = true;
            }
        }
        return std::nullopt;
    };

    auto build_output = [&]() -> std::expected<Table, std::string> {
        Table output;
        for (const auto& key : group_by) {
            const auto* column = input.find(key.name);
            if (column == nullptr) {
                return std::unexpected("group-by column not found: " + key.name);
            }
            output.add_column(key.name, make_empty_like(*column));
        }
        for (const auto& agg : aggregations) {
            ColumnValue column;
            switch (agg.func) {
                case ir::AggFunc::Count:
                    column = Column<std::int64_t>{};
                    break;
                case ir::AggFunc::Mean:
                    column = Column<double>{};
                    break;
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max: {
                    const auto* input_col = input.find(agg.column.name);
                    if (input_col == nullptr) {
                        return std::unexpected("aggregate column not found: " + agg.column.name);
                    }
                    if (std::holds_alternative<Column<double>>(*input_col)) {
                        column = Column<double>{};
                    } else {
                        column = Column<std::int64_t>{};
                    }
                    break;
                }
                case ir::AggFunc::First:
                case ir::AggFunc::Last: {
                    const auto* input_col = input.find(agg.column.name);
                    if (input_col == nullptr) {
                        return std::unexpected("aggregate column not found: " + agg.column.name);
                    }
                    column = make_empty_like(*input_col);
                    break;
                }
            }
            output.add_column(agg.alias, std::move(column));
        }
        return output;
    };

    auto append_agg_values = [&](Table& output,
                                 const AggState& state) -> std::optional<std::string> {
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto& agg = aggregations[i];
            auto* column = output.find(agg.alias);
            if (column == nullptr) {
                return "missing aggregate column in output";
            }
            const AggSlot& slot = state.slots[i];
            switch (agg.func) {
                case ir::AggFunc::Count:
                    append_scalar(*column, slot.count);
                    break;
                case ir::AggFunc::Mean:
                    if (slot.count == 0) {
                        append_scalar(*column, 0.0);
                    } else {
                        append_scalar(*column, slot.sum / static_cast<double>(slot.count));
                    }
                    break;
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                    if (slot.kind == ExprType::Double) {
                        append_scalar(*column, slot.double_value);
                    } else {
                        append_scalar(*column, slot.int_value);
                    }
                    break;
                case ir::AggFunc::First:
                    if (slot.kind == ExprType::Int) {
                        append_scalar(*column, slot.int_value);
                    } else if (slot.kind == ExprType::Double) {
                        append_scalar(*column, slot.double_value);
                    } else {
                        append_scalar(*column, slot.first_value);
                    }
                    break;
                case ir::AggFunc::Last:
                    if (slot.kind == ExprType::Int) {
                        append_scalar(*column, slot.int_value);
                    } else if (slot.kind == ExprType::Double) {
                        append_scalar(*column, slot.double_value);
                    } else {
                        append_scalar(*column, slot.last_value);
                    }
                    break;
            }
        }
        return std::nullopt;
    };

    std::size_t rows = input.rows();
    if (group_by.size() == 1) {
        const ColumnValue& key_column = *group_columns.front();
        if (std::holds_alternative<Column<std::string>>(key_column)) {
            const auto& col = std::get<Column<std::string>>(key_column);

            // Pass 1: Assign group IDs. One hash lookup per row, with a sorted-run shortcut
            // that skips the lookup whenever the current key equals the previous one.
            robin_hood::unordered_flat_map<std::string_view, std::uint32_t> key_to_id;
            key_to_id.reserve(64);
            std::vector<std::string_view> order;
            order.reserve(64);
            std::vector<AggState> states;
            states.reserve(64);
            std::vector<std::uint32_t> group_ids(rows);
            {
                std::string_view prev_key;
                std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
                for (std::size_t row = 0; row < rows; ++row) {
                    std::string_view key{col[row]};
                    std::uint32_t gid;
                    if (key == prev_key) {
                        gid = prev_gid;
                    } else {
                        auto it = key_to_id.find(key);
                        if (it == key_to_id.end()) {
                            gid = static_cast<std::uint32_t>(states.size());
                            key_to_id.emplace(key, gid);
                            order.push_back(key);
                            states.push_back(make_state());
                        } else {
                            gid = it->second;
                        }
                        prev_key = key;
                        prev_gid = gid;
                    }
                    group_ids[row] = gid;
                }
            }

            auto n_groups = static_cast<std::uint32_t>(states.size());
            const std::uint32_t* gids = group_ids.data();

            // Pass 2: Per-aggregation column scans with flat accumulator arrays.
            // n_groups is typically small, so the flat accumulators stay in L1 cache,
            // and the inner loops are simple scatter-updates: accum[gids[row]] op= data[row].
            for (std::size_t agg_i = 0; agg_i < plan.size(); ++agg_i) {
                const auto& item = plan[agg_i];

                if (item.func == ir::AggFunc::Count) {
                    std::vector<std::int64_t> acc(n_groups, 0);
                    for (std::size_t row = 0; row < rows; ++row) {
                        acc[gids[row]]++;
                    }
                    for (std::uint32_t g = 0; g < n_groups; ++g) {
                        states[g].slots[agg_i].count = acc[g];
                    }
                    continue;
                }

                if (item.func == ir::AggFunc::First) {
                    std::vector<bool> found(n_groups, false);
                    if (item.int_col != nullptr) {
                        std::vector<std::int64_t> acc(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = (*item.int_col)[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            states[g].slots[agg_i].int_value = acc[g];
                            states[g].slots[agg_i].has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = data[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            states[g].slots[agg_i].double_value = acc[g];
                            states[g].slots[agg_i].has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            std::uint32_t g = gids[row];
                            if (!found[g]) {
                                acc[g] = (*item.str_col)[row];
                                found[g] = true;
                            }
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            states[g].slots[agg_i].first_value = std::move(acc[g]);
                            states[g].slots[agg_i].has_value = true;
                        }
                    }
                    continue;
                }

                if (item.func == ir::AggFunc::Last) {
                    if (item.int_col != nullptr) {
                        std::vector<std::int64_t> acc(n_groups, 0);
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] = (*item.int_col)[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            states[g].slots[agg_i].int_value = acc[g];
                            states[g].slots[agg_i].has_value = true;
                        }
                    } else if (item.dbl_col != nullptr) {
                        std::vector<double> acc(n_groups, 0.0);
                        const double* data = item.dbl_col->data();
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] = data[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            states[g].slots[agg_i].double_value = acc[g];
                            states[g].slots[agg_i].has_value = true;
                        }
                    } else if (item.str_col != nullptr) {
                        std::vector<std::string> acc(n_groups);
                        for (std::size_t row = 0; row < rows; ++row) {
                            acc[gids[row]] = (*item.str_col)[row];
                        }
                        for (std::uint32_t g = 0; g < n_groups; ++g) {
                            states[g].slots[agg_i].last_value = std::move(acc[g]);
                            states[g].slots[agg_i].has_value = true;
                        }
                    }
                    continue;
                }

                if (item.dbl_col != nullptr) {
                    const double* data = item.dbl_col->data();
                    switch (item.func) {
                        case ir::AggFunc::Sum: {
                            std::vector<double> acc(n_groups, 0.0);
                            for (std::size_t row = 0; row < rows; ++row) {
                                acc[gids[row]] += data[row];
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].double_value = acc[g];
                                states[g].slots[agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Mean: {
                            std::vector<double> acc(n_groups, 0.0);
                            std::vector<std::int64_t> counts(n_groups, 0);
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                acc[g] += data[row];
                                counts[g]++;
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].sum = acc[g];
                                states[g].slots[agg_i].count = counts[g];
                            }
                            break;
                        }
                        case ir::AggFunc::Min: {
                            std::vector<double> acc(n_groups,
                                                    std::numeric_limits<double>::infinity());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] < acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].double_value = acc[g];
                                states[g].slots[agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Max: {
                            std::vector<double> acc(n_groups,
                                                    -std::numeric_limits<double>::infinity());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] > acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].double_value = acc[g];
                                states[g].slots[agg_i].has_value = true;
                            }
                            break;
                        }
                        default:
                            break;
                    }
                } else if (item.int_col != nullptr) {
                    const std::int64_t* data = item.int_col->data();
                    switch (item.func) {
                        case ir::AggFunc::Sum: {
                            std::vector<std::int64_t> acc(n_groups, 0);
                            for (std::size_t row = 0; row < rows; ++row) {
                                acc[gids[row]] += data[row];
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].int_value = acc[g];
                                states[g].slots[agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Mean: {
                            std::vector<double> acc(n_groups, 0.0);
                            std::vector<std::int64_t> counts(n_groups, 0);
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                acc[g] += static_cast<double>(data[row]);
                                counts[g]++;
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].sum = acc[g];
                                states[g].slots[agg_i].count = counts[g];
                            }
                            break;
                        }
                        case ir::AggFunc::Min: {
                            std::vector<std::int64_t> acc(n_groups,
                                                          std::numeric_limits<std::int64_t>::max());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] < acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].int_value = acc[g];
                                states[g].slots[agg_i].has_value = true;
                            }
                            break;
                        }
                        case ir::AggFunc::Max: {
                            std::vector<std::int64_t> acc(n_groups,
                                                          std::numeric_limits<std::int64_t>::min());
                            for (std::size_t row = 0; row < rows; ++row) {
                                std::uint32_t g = gids[row];
                                if (data[row] > acc[g]) {
                                    acc[g] = data[row];
                                }
                            }
                            for (std::uint32_t g = 0; g < n_groups; ++g) {
                                states[g].slots[agg_i].int_value = acc[g];
                                states[g].slots[agg_i].has_value = true;
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            }

            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                append_scalar(*column, std::string(order[i]));
                if (auto err = append_agg_values(*output, states[i])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<std::int64_t>>(key_column)) {
            const auto& col = std::get<Column<std::int64_t>>(key_column);
            robin_hood::unordered_flat_map<std::int64_t, std::size_t> index;
            index.reserve(rows);
            std::vector<std::int64_t> order;
            order.reserve(rows);
            std::vector<AggState> states;
            states.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                std::int64_t key = col[row];
                auto it = index.find(key);
                std::size_t slot_index = 0;
                if (it == index.end()) {
                    slot_index = states.size();
                    index.emplace(key, slot_index);
                    order.push_back(key);
                    states.push_back(make_state());
                } else {
                    slot_index = it->second;
                }
                if (auto err = (numeric_only ? update_state_numeric(states[slot_index], row)
                                             : update_state(states[slot_index], row))) {
                    return std::unexpected(*err);
                }
            }
            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                append_scalar(*column, order[i]);
                if (auto err = append_agg_values(*output, states[i])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<double>>(key_column)) {
            const auto& col = std::get<Column<double>>(key_column);
            robin_hood::unordered_flat_map<double, std::size_t> index;
            index.reserve(rows);
            std::vector<double> order;
            order.reserve(rows);
            std::vector<AggState> states;
            states.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                double key = col[row];
                auto it = index.find(key);
                std::size_t slot_index = 0;
                if (it == index.end()) {
                    slot_index = states.size();
                    index.emplace(key, slot_index);
                    order.push_back(key);
                    states.push_back(make_state());
                } else {
                    slot_index = it->second;
                }
                if (auto err = (numeric_only ? update_state_numeric(states[slot_index], row)
                                             : update_state(states[slot_index], row))) {
                    return std::unexpected(*err);
                }
            }
            auto output = build_output();
            if (!output.has_value()) {
                return std::unexpected(output.error());
            }
            for (std::size_t i = 0; i < order.size(); ++i) {
                auto* column = output->find(group_by.front().name);
                if (column == nullptr) {
                    return std::unexpected("missing group-by column in output");
                }
                append_scalar(*column, order[i]);
                if (auto err = append_agg_values(*output, states[i])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
    }

    // Multi-column GROUP BY: reuse a single Key vector across rows (no per-row
    // heap allocation), reserve a sensible up-front capacity rather than rows,
    // and use a robin_hood flat map (Key → AggState) so the map stays in L2.
    //
    // Precompute typed column pointers so the hot loop uses direct typed access
    // instead of a ColumnValue variant dispatch (std::visit) per row × column.
    enum class ColKind : std::uint8_t { Int, Double, Str };
    struct ColPtr {
        ColKind kind;
        union {
            const std::int64_t* int_ptr;
            const double* dbl_ptr;
            const Column<std::string>* str_ptr;
        };
    };
    std::vector<ColPtr> col_ptrs(group_columns.size());
    for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
        std::visit(
            [&](const auto& col) {
                using T = typename std::decay_t<decltype(col)>::value_type;
                if constexpr (std::is_same_v<T, std::int64_t>) {
                    col_ptrs[ci] = {ColKind::Int, {}};
                    col_ptrs[ci].int_ptr = col.data();
                } else if constexpr (std::is_same_v<T, double>) {
                    col_ptrs[ci] = {ColKind::Double, {}};
                    col_ptrs[ci].dbl_ptr = col.data();
                } else {
                    col_ptrs[ci] = {ColKind::Str, {}};
                    col_ptrs[ci].str_ptr = &col;
                }
            },
            *group_columns[ci]);
    }

    // Pre-initialise reuse_key slots to the correct variant alternative so the
    // hot loop can use std::get<T> assignment (in-place overwrite, no
    // destroy+construct) instead of emplace<T>.
    robin_hood::unordered_flat_map<Key, AggState, KeyHash, KeyEq> groups;
    std::vector<Key> order;
    groups.reserve(1024);
    order.reserve(1024);
    Key reuse_key;
    reuse_key.values.resize(group_columns.size());
    for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
        switch (col_ptrs[ci].kind) {
            case ColKind::Int:
                reuse_key.values[ci].emplace<std::int64_t>();
                break;
            case ColKind::Double:
                reuse_key.values[ci].emplace<double>();
                break;
            case ColKind::Str:
                reuse_key.values[ci].emplace<std::string>();
                break;
        }
    }
    for (std::size_t row = 0; row < rows; ++row) {
        for (std::size_t ci = 0; ci < group_columns.size(); ++ci) {
            const ColPtr& cp = col_ptrs[ci];
            switch (cp.kind) {
                case ColKind::Int:
                    std::get<std::int64_t>(reuse_key.values[ci]) = cp.int_ptr[row];
                    break;
                case ColKind::Double:
                    std::get<double>(reuse_key.values[ci]) = cp.dbl_ptr[row];
                    break;
                case ColKind::Str:
                    std::get<std::string>(reuse_key.values[ci]) = (*cp.str_ptr)[row];
                    break;
            }
        }

        auto it = groups.find(reuse_key);
        if (it == groups.end()) {
            order.push_back(reuse_key);
            it = groups.emplace(reuse_key, make_state()).first;
        }

        if (auto err = (numeric_only ? update_state_numeric(it->second, row)
                                     : update_state(it->second, row))) {
            return std::unexpected(*err);
        }
    }

    auto output = build_output();
    if (!output.has_value()) {
        return std::unexpected(output.error());
    }
    for (const auto& key : order) {
        for (std::size_t i = 0; i < group_by.size(); ++i) {
            auto* column = output->find(group_by[i].name);
            if (column == nullptr) {
                return std::unexpected("missing group-by column in output");
            }
            append_scalar(*column, key.values[i]);
        }
        const AggState& state = groups.find(key)->second;
        if (auto err = append_agg_values(*output, state)) {
            return std::unexpected(*err);
        }
    }

    return output;
}

auto infer_expr_type(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars,
                     const ExternRegistry* externs) -> std::expected<ExprType, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* source = input.find(col->name);
        if (source == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    if (std::holds_alternative<std::int64_t>(it->second)) {
                        return ExprType::Int;
                    }
                    if (std::holds_alternative<double>(it->second)) {
                        return ExprType::Double;
                    }
                    return ExprType::String;
                }
            }
            return std::unexpected("unknown column in expression: " + col->name +
                                   " (available: " + format_columns(input) + ")");
        }
        return expr_type_for_column(*source);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        if (std::holds_alternative<std::int64_t>(lit->value)) {
            return ExprType::Int;
        }
        if (std::holds_alternative<double>(lit->value)) {
            return ExprType::Double;
        }
        return ExprType::String;
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = infer_expr_type(*bin->left, input, scalars, externs);
        if (!left) {
            return left;
        }
        auto right = infer_expr_type(*bin->right, input, scalars, externs);
        if (!right) {
            return right;
        }
        if (left.value() == ExprType::String || right.value() == ExprType::String) {
            return std::unexpected("string arithmetic not supported");
        }
        if (bin->op == ir::ArithmeticOp::Div) {
            return ExprType::Double;
        }
        if (left.value() == ExprType::Double || right.value() == ExprType::Double) {
            return ExprType::Double;
        }
        return ExprType::Int;
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if (externs == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        const auto* fn = externs->find(call->callee);
        if (fn == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        if (fn->kind != ExternReturnKind::Scalar || !fn->scalar_kind.has_value()) {
            return std::unexpected("function not usable in expression: " + call->callee);
        }
        for (const auto& arg : call->args) {
            auto arg_type = infer_expr_type(*arg, input, scalars, externs);
            if (!arg_type) {
                return arg_type;
            }
        }
        switch (fn->scalar_kind.value()) {
            case ScalarKind::Int:
                return ExprType::Int;
            case ScalarKind::Double:
                return ExprType::Double;
            case ScalarKind::String:
                return ExprType::String;
        }
    }
    return std::unexpected("unsupported expression");
}

auto eval_expr(const ir::Expr& expr, const Table& input, std::size_t row,
               const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<ExprValue, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* source = input.find(col->name);
        if (source == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    return it->second;
                }
            }
            return std::unexpected("unknown column in expression: " + col->name +
                                   " (available: " + format_columns(input) + ")");
        }
        return std::visit([&](const auto& column) -> ExprValue { return column[row]; }, *source);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        return lit->value;
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = eval_expr(*bin->left, input, row, scalars, externs);
        if (!left) {
            return left;
        }
        auto right = eval_expr(*bin->right, input, row, scalars, externs);
        if (!right) {
            return right;
        }
        if (std::holds_alternative<std::string>(left.value()) ||
            std::holds_alternative<std::string>(right.value())) {
            return std::unexpected("string arithmetic not supported");
        }
        auto to_double = [](const ExprValue& v) -> double {
            if (const auto* i = std::get_if<std::int64_t>(&v)) {
                return static_cast<double>(*i);
            }
            return std::get<double>(v);
        };
        bool want_double = bin->op == ir::ArithmeticOp::Div ||
                           std::holds_alternative<double>(left.value()) ||
                           std::holds_alternative<double>(right.value());
        if (want_double) {
            double lhs = to_double(left.value());
            double rhs = to_double(right.value());
            switch (bin->op) {
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
        } else {
            std::int64_t lhs = std::get<std::int64_t>(left.value());
            std::int64_t rhs = std::get<std::int64_t>(right.value());
            switch (bin->op) {
                case ir::ArithmeticOp::Add:
                    return lhs + rhs;
                case ir::ArithmeticOp::Sub:
                    return lhs - rhs;
                case ir::ArithmeticOp::Mul:
                    return lhs * rhs;
                case ir::ArithmeticOp::Div:
                    return lhs / rhs;
                case ir::ArithmeticOp::Mod:
                    return lhs % rhs;
            }
        }
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if (externs == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        const auto* fn = externs->find(call->callee);
        if (fn == nullptr) {
            return std::unexpected("unknown function in expression: " + call->callee);
        }
        if (fn->kind != ExternReturnKind::Scalar) {
            return std::unexpected("function not usable in expression: " + call->callee);
        }
        std::vector<ExprValue> arg_values;
        arg_values.reserve(call->args.size());
        for (const auto& arg : call->args) {
            auto value = eval_expr(*arg, input, row, scalars, externs);
            if (!value) {
                return value;
            }
            arg_values.push_back(std::move(value.value()));
        }
        auto result = fn->func(arg_values);
        if (!result) {
            return std::unexpected(result.error());
        }
        if (auto* scalar = std::get_if<ScalarValue>(&result.value())) {
            return *scalar;
        }
        return std::unexpected("function returned table in expression: " + call->callee);
    }
    return std::unexpected("unsupported expression");
}

auto update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                  const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string> {
    Table output = std::move(input);
    std::size_t rows = output.rows();
    for (const auto& field : fields) {
        auto inferred = infer_expr_type(field.expr, output, scalars, externs);
        if (!inferred) {
            return std::unexpected(inferred.error());
        }
        if (auto fast = try_fast_update_binary(field.expr, output, rows, inferred.value(), scalars);
            fast.has_value()) {
            output.add_column(field.alias, std::move(fast.value()));
            continue;
        }
        ColumnValue new_column;
        switch (inferred.value()) {
            case ExprType::Int:
                new_column = Column<std::int64_t>{};
                break;
            case ExprType::Double:
                new_column = Column<double>{};
                break;
            case ExprType::String:
                new_column = Column<std::string>{};
                break;
        }
        std::visit([&](auto& col) { col.reserve(rows); }, new_column);
        for (std::size_t row = 0; row < rows; ++row) {
            auto value = eval_expr(field.expr, output, row, scalars, externs);
            if (!value) {
                return std::unexpected(value.error());
            }
            std::visit(
                [&](auto& col) {
                    using ColType = std::decay_t<decltype(col)>;
                    using ValueType = typename ColType::value_type;
                    if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(*int_value);
                        } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                            col.push_back(static_cast<std::int64_t>(*double_value));
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    } else if constexpr (std::is_same_v<ValueType, double>) {
                        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
                            col.push_back(static_cast<double>(*int_value));
                        } else if (const auto* double_value = std::get_if<double>(&value.value())) {
                            col.push_back(*double_value);
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    } else if constexpr (std::is_same_v<ValueType, std::string>) {
                        if (const auto* v = std::get_if<std::string>(&value.value())) {
                            col.push_back(*v);
                        } else {
                            throw std::runtime_error("type mismatch");
                        }
                    }
                },
                new_column);
        }
        output.add_column(field.alias, std::move(new_column));
    }
    return output;
}

// NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
auto interpret_node(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string> {
    switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = static_cast<const ir::ScanNode&>(node);
            auto it = registry.find(scan.source_name());
            if (it == registry.end()) {
                return std::unexpected("unknown table: " + scan.source_name() +
                                       " (available: " + format_tables(registry) + ")");
            }
            return it->second;
        }
        case ir::NodeKind::Filter: {
            const auto& filter = static_cast<const ir::FilterNode&>(node);
            if (filter.children().empty()) {
                return std::unexpected("filter node missing child");
            }
            auto child = interpret_node(*filter.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return filter_table(child.value(), filter.predicate(), scalars);
        }
        case ir::NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            if (project.children().empty()) {
                return std::unexpected("project node missing child");
            }
            auto child = interpret_node(*project.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return project_table(child.value(), project.columns());
        }
        case ir::NodeKind::Distinct: {
            if (node.children().empty()) {
                return std::unexpected("distinct node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return distinct_table(child.value());
        }
        case ir::NodeKind::Update: {
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            if (update.children().empty()) {
                return std::unexpected("update node missing child");
            }
            if (!update.group_by().empty()) {
                return std::unexpected("grouped update not supported in interpreter");
            }
            auto child = interpret_node(*update.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return update_table(std::move(child.value()), update.fields(), scalars, externs);
        }
        case ir::NodeKind::Aggregate: {
            const auto& agg = static_cast<const ir::AggregateNode&>(node);
            if (agg.children().empty()) {
                return std::unexpected("aggregate node missing child");
            }
            // Fast path: Aggregate(Scan) — pass the registry table by const ref to skip the copy.
            const ir::Node& child_node = *agg.children().front();
            if (child_node.kind() == ir::NodeKind::Scan) {
                const auto& scan = static_cast<const ir::ScanNode&>(child_node);
                auto it = registry.find(scan.source_name());
                if (it == registry.end()) {
                    return std::unexpected("unknown table: " + scan.source_name());
                }
                return aggregate_table(it->second, agg.group_by(), agg.aggregations());
            }
            auto child = interpret_node(child_node, registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return aggregate_table(child.value(), agg.group_by(), agg.aggregations());
        }
        case ir::NodeKind::Window:
            return std::unexpected("window not supported in interpreter");
        case ir::NodeKind::ExternCall: {
            const auto& ec = static_cast<const ir::ExternCallNode&>(node);
            if (externs == nullptr) {
                return std::unexpected("extern call with no registry: " + ec.callee());
            }
            const auto* fn = externs->find(ec.callee());
            if (fn == nullptr) {
                return std::unexpected("unknown extern function: " + ec.callee());
            }
            if (fn->kind != ExternReturnKind::Table) {
                return std::unexpected("extern function does not return a table: " + ec.callee());
            }
            ExternArgs args;
            args.reserve(ec.args().size());
            for (const auto& arg : ec.args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val)
                    return std::unexpected(val.error());
                args.push_back(std::move(val.value()));
            }
            auto result = fn->func(args);
            if (!result)
                return std::unexpected(result.error());
            if (auto* table = std::get_if<Table>(&result.value())) {
                return std::move(*table);
            }
            return std::unexpected("extern function did not return a table: " + ec.callee());
        }
        case ir::NodeKind::Join: {
            const auto& join = static_cast<const ir::JoinNode&>(node);
            if (join.children().size() != 2) {
                return std::unexpected("join node expects exactly two children");
            }
            auto left = interpret_node(*join.children()[0], registry, scalars, externs);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = interpret_node(*join.children()[1], registry, scalars, externs);
            if (!right) {
                return std::unexpected(right.error());
            }
            return join_table_impl(left.value(), right.value(), join.kind(), join.keys());
        }
    }
    return std::unexpected("unknown node kind");
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace

void Table::add_column(std::string name, ColumnValue column) {
    if (auto it = index.find(name); it != index.end()) {
        // Reseat the shared_ptr rather than mutating shared data (copy-on-write).
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        return;
    }
    std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{.name = std::move(name),
                                  .column = std::make_shared<ColumnValue>(std::move(column))});
    index[columns.back().name] = pos;
}

auto Table::find(const std::string& name) -> ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return columns[it->second].column.get();
    }
    return nullptr;
}

auto Table::find(const std::string& name) const -> const ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return columns[it->second].column.get();
    }
    return nullptr;
}

auto Table::rows() const noexcept -> std::size_t {
    if (columns.empty()) {
        return 0;
    }
    return column_size(*columns.front().column);
}

auto interpret(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
               const ExternRegistry* externs) -> std::expected<Table, std::string> {
    return interpret_node(node, registry, scalars, externs);
}

auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                 const std::vector<std::string>& keys) -> std::expected<Table, std::string> {
    return join_table_impl(left, right, kind, keys);
}

auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string> {
    if (table.rows() != 1) {
        return std::unexpected("scalar() requires exactly one row");
    }
    const auto* col = table.find(column);
    if (col == nullptr) {
        return std::unexpected("column not found: " + column);
    }
    return scalar_from_column(*col, 0);
}

}  // namespace ibex::runtime
