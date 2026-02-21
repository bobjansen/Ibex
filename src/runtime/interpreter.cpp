#include <ibex/runtime/interpreter.hpp>

#include <cmath>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ibex::runtime {

namespace {

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

auto compare_value(const ColumnValue& column, std::size_t index, const ir::FilterPredicate& pred)
    -> bool {
    return std::visit(
        [&](const auto& col) -> bool {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* v = std::get_if<std::int64_t>(&pred.value)) {
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
                if (const auto* v = std::get_if<double>(&pred.value)) {
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
                if (const auto* v = std::get_if<double>(&pred.value)) {
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
                if (const auto* v = std::get_if<std::int64_t>(&pred.value)) {
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
                if (const auto* v = std::get_if<std::string>(&pred.value)) {
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

auto filter_table(const Table& input, const ir::FilterPredicate& predicate)
    -> std::expected<Table, std::string> {
    const auto* predicate_column = input.find(predicate.column.name);
    if (predicate_column == nullptr) {
        return std::unexpected("filter column not found: " + predicate.column.name);
    }
    std::size_t rows = column_size(*predicate_column);

    Table output;
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(entry.column));
    }

    for (std::size_t row = 0; row < rows; ++row) {
        if (!compare_value(*predicate_column, row, predicate)) {
            continue;
        }
        for (auto& entry : output.columns) {
            const auto* source = input.find(entry.name);
            if (source == nullptr) {
                return std::unexpected("filter column missing: " + entry.name);
            }
            append_value(entry.column, *source, row);
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
            return std::unexpected("select column not found: " + col.name);
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

auto aggregate_table(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                     const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    std::vector<const ColumnValue*> group_columns;
    group_columns.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("group-by column not found: " + key.name);
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
            return std::unexpected("aggregate column not found: " + agg.column.name);
        }
        agg_columns.push_back(column);
    }

    auto make_state = [&]() -> AggState {
        AggState state;
        state.slots.reserve(aggregations.size());
        for (std::size_t i = 0; i < aggregations.size(); ++i) {
            const auto& agg = aggregations[i];
            AggSlot slot;
            slot.func = agg.func;
            if (agg.func == ir::AggFunc::Count) {
                slot.kind = ExprType::Int;
            } else {
                slot.kind = expr_type_for_column(*agg_columns[i]);
            }
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
            if (slot.kind == ExprType::String &&
                (agg.func == ir::AggFunc::Sum || agg.func == ir::AggFunc::Mean ||
                 agg.func == ir::AggFunc::Min || agg.func == ir::AggFunc::Max)) {
                return "string aggregation not supported";
            }
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

    auto append_agg_values = [&](Table& output, const AggState& state)
        -> std::optional<std::string> {
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
                    append_scalar(*column, slot.first_value);
                    break;
                case ir::AggFunc::Last:
                    append_scalar(*column, slot.last_value);
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
            std::unordered_map<std::string_view, std::size_t> index;
            index.reserve(rows);
            std::vector<std::string_view> order;
            order.reserve(rows);
            std::vector<AggState> states;
            states.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                std::string_view key{col[row]};
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
                if (auto err = update_state(states[slot_index], row)) {
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
                append_scalar(*column, std::string(order[i]));
                if (auto err = append_agg_values(*output, states[i])) {
                    return std::unexpected(*err);
                }
            }
            return output;
        }
        if (std::holds_alternative<Column<std::int64_t>>(key_column)) {
            const auto& col = std::get<Column<std::int64_t>>(key_column);
            std::unordered_map<std::int64_t, std::size_t> index;
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
                if (auto err = update_state(states[slot_index], row)) {
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
            std::unordered_map<double, std::size_t> index;
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
                if (auto err = update_state(states[slot_index], row)) {
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

    std::unordered_map<Key, AggState, KeyHash, KeyEq> groups;
    std::vector<Key> order;
    groups.reserve(rows);
    order.reserve(rows);
    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_columns.size());
        for (const auto* column : group_columns) {
            key.values.push_back(scalar_from_column(*column, row));
        }

        auto it = groups.find(key);
        if (it == groups.end()) {
            AggState state = make_state();
            order.push_back(key);
            it = groups.emplace(order.back(), std::move(state)).first;
        }

        if (auto err = update_state(it->second, row)) {
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
        const AggState& state = groups.at(key);
        if (auto err = append_agg_values(*output, state)) {
            return std::unexpected(*err);
        }
    }

    return output;
}

auto infer_expr_type(const ir::Expr& expr, const Table& input, const ScalarRegistry* scalars)
    -> std::expected<ExprType, std::string> {
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
            return std::unexpected("unknown column in expression: " + col->name);
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
        auto left = infer_expr_type(*bin->left, input, scalars);
        if (!left) {
            return left;
        }
        auto right = infer_expr_type(*bin->right, input, scalars);
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
    return std::unexpected("unsupported expression");
}

using ExprValue = std::variant<std::int64_t, double, std::string>;

auto eval_expr(const ir::Expr& expr, const Table& input, std::size_t row,
               const ScalarRegistry* scalars) -> std::expected<ExprValue, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* source = input.find(col->name);
        if (source == nullptr) {
            if (scalars != nullptr) {
                if (auto it = scalars->find(col->name); it != scalars->end()) {
                    return it->second;
                }
            }
            return std::unexpected("unknown column in expression: " + col->name);
        }
        return std::visit([&](const auto& column) -> ExprValue { return column[row]; }, *source);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        return lit->value;
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = eval_expr(*bin->left, input, row, scalars);
        if (!left) {
            return left;
        }
        auto right = eval_expr(*bin->right, input, row, scalars);
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
    return std::unexpected("unsupported expression");
}

auto update_table(const Table& input, const std::vector<ir::FieldSpec>& fields,
                  const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    Table output = input;
    std::size_t rows = input.rows();
    for (const auto& field : fields) {
        auto inferred = infer_expr_type(field.expr, input, scalars);
        if (!inferred) {
            return std::unexpected(inferred.error());
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
        for (std::size_t row = 0; row < rows; ++row) {
            auto value = eval_expr(field.expr, input, row, scalars);
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
auto interpret_node(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string> {
    switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = static_cast<const ir::ScanNode&>(node);
            auto it = registry.find(scan.source_name());
            if (it == registry.end()) {
                return std::unexpected("unknown table: " + scan.source_name());
            }
            return it->second;
        }
        case ir::NodeKind::Filter: {
            const auto& filter = static_cast<const ir::FilterNode&>(node);
            if (filter.children().empty()) {
                return std::unexpected("filter node missing child");
            }
            auto child = interpret_node(*filter.children().front(), registry, scalars);
            if (!child) {
                return std::unexpected(child.error());
            }
            return filter_table(child.value(), filter.predicate());
        }
        case ir::NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            if (project.children().empty()) {
                return std::unexpected("project node missing child");
            }
            auto child = interpret_node(*project.children().front(), registry, scalars);
            if (!child) {
                return std::unexpected(child.error());
            }
            return project_table(child.value(), project.columns());
        }
        case ir::NodeKind::Update: {
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            if (update.children().empty()) {
                return std::unexpected("update node missing child");
            }
            if (!update.group_by().empty()) {
                return std::unexpected("grouped update not supported in interpreter");
            }
            auto child = interpret_node(*update.children().front(), registry, scalars);
            if (!child) {
                return std::unexpected(child.error());
            }
            return update_table(child.value(), update.fields(), scalars);
        }
        case ir::NodeKind::Aggregate: {
            const auto& agg = static_cast<const ir::AggregateNode&>(node);
            if (agg.children().empty()) {
                return std::unexpected("aggregate node missing child");
            }
            auto child = interpret_node(*agg.children().front(), registry, scalars);
            if (!child) {
                return std::unexpected(child.error());
            }
            return aggregate_table(child.value(), agg.group_by(), agg.aggregations());
        }
        case ir::NodeKind::Window:
            return std::unexpected("window not supported in interpreter");
    }
    return std::unexpected("unknown node kind");
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace

void Table::add_column(std::string name, ColumnValue column) {
    if (auto it = index.find(name); it != index.end()) {
        columns[it->second].column = std::move(column);
        return;
    }
    std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{.name = std::move(name), .column = std::move(column)});
    index[columns.back().name] = pos;
}

auto Table::find(const std::string& name) -> ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return &columns[it->second].column;
    }
    return nullptr;
}

auto Table::find(const std::string& name) const -> const ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return &columns[it->second].column;
    }
    return nullptr;
}

auto Table::rows() const noexcept -> std::size_t {
    if (columns.empty()) {
        return 0;
    }
    return column_size(columns.front().column);
}

auto interpret(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string> {
    return interpret_node(node, registry, scalars);
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
