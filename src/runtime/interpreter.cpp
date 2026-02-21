#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cmath>
#include <stdexcept>

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

auto compare_value(const ColumnValue& column, std::size_t index, const ir::FilterPredicate& pred) -> bool {
    return std::visit(
        [&](const auto& col) -> bool {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = typename ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* v = std::get_if<std::int64_t>(&pred.value)) {
                    switch (pred.op) {
                    case ir::CompareOp::Eq: return col[index] == *v;
                    case ir::CompareOp::Ne: return col[index] != *v;
                    case ir::CompareOp::Lt: return col[index] < *v;
                    case ir::CompareOp::Le: return col[index] <= *v;
                    case ir::CompareOp::Gt: return col[index] > *v;
                    case ir::CompareOp::Ge: return col[index] >= *v;
                    }
                }
                if (const auto* v = std::get_if<double>(&pred.value)) {
                    double lhs = static_cast<double>(col[index]);
                    switch (pred.op) {
                    case ir::CompareOp::Eq: return lhs == *v;
                    case ir::CompareOp::Ne: return lhs != *v;
                    case ir::CompareOp::Lt: return lhs < *v;
                    case ir::CompareOp::Le: return lhs <= *v;
                    case ir::CompareOp::Gt: return lhs > *v;
                    case ir::CompareOp::Ge: return lhs >= *v;
                    }
                }
                return false;
            } else if constexpr (std::is_same_v<ValueType, double>) {
                double lhs = col[index];
                if (const auto* v = std::get_if<double>(&pred.value)) {
                    switch (pred.op) {
                    case ir::CompareOp::Eq: return lhs == *v;
                    case ir::CompareOp::Ne: return lhs != *v;
                    case ir::CompareOp::Lt: return lhs < *v;
                    case ir::CompareOp::Le: return lhs <= *v;
                    case ir::CompareOp::Gt: return lhs > *v;
                    case ir::CompareOp::Ge: return lhs >= *v;
                    }
                }
                if (const auto* v = std::get_if<std::int64_t>(&pred.value)) {
                    double rhs = static_cast<double>(*v);
                    switch (pred.op) {
                    case ir::CompareOp::Eq: return lhs == rhs;
                    case ir::CompareOp::Ne: return lhs != rhs;
                    case ir::CompareOp::Lt: return lhs < rhs;
                    case ir::CompareOp::Le: return lhs <= rhs;
                    case ir::CompareOp::Gt: return lhs > rhs;
                    case ir::CompareOp::Ge: return lhs >= rhs;
                    }
                }
                return false;
            } else if constexpr (std::is_same_v<ValueType, std::string>) {
                if (const auto* v = std::get_if<std::string>(&pred.value)) {
                    switch (pred.op) {
                    case ir::CompareOp::Eq: return col[index] == *v;
                    case ir::CompareOp::Ne: return col[index] != *v;
                    case ir::CompareOp::Lt: return col[index] < *v;
                    case ir::CompareOp::Le: return col[index] <= *v;
                    case ir::CompareOp::Gt: return col[index] > *v;
                    case ir::CompareOp::Ge: return col[index] >= *v;
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

enum class ExprType : std::uint8_t {
    Int,
    Double,
    String,
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

auto infer_expr_type(const ir::Expr& expr, const Table& input) -> std::expected<ExprType, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* source = input.find(col->name);
        if (source == nullptr) {
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
        auto left = infer_expr_type(*bin->left, input);
        if (!left) {
            return left;
        }
        auto right = infer_expr_type(*bin->right, input);
        if (!right) {
            return right;
        }
        if (left.value() == ExprType::String || right.value() == ExprType::String) {
            return std::unexpected("string arithmetic not supported");
        }
        if (left.value() == ExprType::Double || right.value() == ExprType::Double) {
            return ExprType::Double;
        }
        return ExprType::Int;
    }
    return std::unexpected("unsupported expression");
}

using ExprValue = std::variant<std::int64_t, double, std::string>;

auto eval_expr(const ir::Expr& expr, const Table& input, std::size_t row)
    -> std::expected<ExprValue, std::string> {
    if (const auto* col = std::get_if<ir::ColumnRef>(&expr.node)) {
        const auto* source = input.find(col->name);
        if (source == nullptr) {
            return std::unexpected("unknown column in expression: " + col->name);
        }
        return std::visit(
            [&](const auto& column) -> ExprValue {
                return column[row];
            },
            *source);
    }
    if (const auto* lit = std::get_if<ir::Literal>(&expr.node)) {
        return lit->value;
    }
    if (const auto* bin = std::get_if<ir::BinaryExpr>(&expr.node)) {
        auto left = eval_expr(*bin->left, input, row);
        if (!left) {
            return left;
        }
        auto right = eval_expr(*bin->right, input, row);
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
        bool want_double = std::holds_alternative<double>(left.value()) ||
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

auto update_table(const Table& input, const std::vector<ir::FieldSpec>& fields)
    -> std::expected<Table, std::string> {
    Table output = input;
    std::size_t rows = input.rows();
    for (const auto& field : fields) {
        auto inferred = infer_expr_type(field.expr, input);
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
            auto value = eval_expr(field.expr, input, row);
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

auto interpret_node(const ir::Node& node, const TableRegistry& registry)
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
        auto child = interpret_node(*filter.children().front(), registry);
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
        auto child = interpret_node(*project.children().front(), registry);
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
        auto child = interpret_node(*update.children().front(), registry);
        if (!child) {
            return std::unexpected(child.error());
        }
        return update_table(child.value(), update.fields());
    }
    case ir::NodeKind::Aggregate:
        return std::unexpected("aggregate not supported in interpreter");
    case ir::NodeKind::Window:
        return std::unexpected("window not supported in interpreter");
    }
    return std::unexpected("unknown node kind");
}

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

auto interpret(const ir::Node& node, const TableRegistry& registry)
    -> std::expected<Table, std::string> {
    return interpret_node(node, registry);
}

}  // namespace ibex::runtime
