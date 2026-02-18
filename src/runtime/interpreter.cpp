#include <ibex/runtime/interpreter.hpp>

#include <algorithm>

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

auto update_table(const Table& input, const std::vector<ir::FieldSpec>& fields)
    -> std::expected<Table, std::string> {
    Table output = input;
    for (const auto& field : fields) {
        const auto* source = input.find(field.column.name);
        if (source == nullptr) {
            return std::unexpected("update column not found: " + field.column.name);
        }
        output.add_column(field.alias, *source);
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
