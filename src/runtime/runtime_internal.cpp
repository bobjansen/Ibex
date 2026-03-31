#include "runtime_internal.hpp"

#include <cctype>
#include <limits>
#include <stdexcept>

namespace ibex::runtime {

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

auto normalize_time_index(Table& table) -> void {
    if (!table.time_index.has_value()) {
        return;
    }
    if (table.ordering.has_value() && table.ordering->size() == 1 &&
        table.ordering->front().name == *table.time_index && table.ordering->front().ascending) {
        return;
    }
    table.ordering = std::vector<ir::OrderKey>{{.name = *table.time_index, .ascending = true}};
}

auto int64_to_date_checked(std::int64_t value) -> Date {
    if (value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
        throw std::runtime_error("date out of range");
    }
    return Date{static_cast<std::int32_t>(value)};
}

auto scalar_from_column(const ColumnValue& column, std::size_t row) -> ScalarValue {
    return std::visit(
        [&](const auto& col) -> ScalarValue {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColType, Column<Categorical>> ||
                          std::is_same_v<ColType, Column<std::string>>) {
                return std::string(col[row]);
            } else {
                return col[row];
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
    if (std::holds_alternative<Column<bool>>(column)) {
        return ExprType::Bool;
    }
    if (std::holds_alternative<Column<Date>>(column)) {
        return ExprType::Date;
    }
    if (std::holds_alternative<Column<Timestamp>>(column)) {
        return ExprType::Timestamp;
    }
    return ExprType::String;
}

}  // namespace ibex::runtime
