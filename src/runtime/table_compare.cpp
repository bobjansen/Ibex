#include <ibex/runtime/table_compare.hpp>

#include <cmath>
#include <format>
#include <type_traits>

namespace ibex::runtime {

auto TableMismatch::message() const -> std::string {
    return std::format("table mismatch at {}: expected {}, actual {}", location, expected, actual);
}

namespace {

auto mismatch(std::string location, std::string expected, std::string actual)
    -> std::optional<TableMismatch> {
    return TableMismatch{std::move(location), std::move(expected), std::move(actual)};
}

auto validity_mismatch(const ColumnEntry& expected, const ColumnEntry& actual, std::size_t column)
    -> std::optional<TableMismatch> {
    if (expected.validity.has_value() != actual.validity.has_value()) {
        return mismatch(std::format("column {} validity backing", column),
                        expected.validity ? "bitmap" : "all-valid",
                        actual.validity ? "bitmap" : "all-valid");
    }
    if (!expected.validity)
        return std::nullopt;
    if (expected.validity->size() != actual.validity->size()) {
        return mismatch(std::format("column {} validity size", column),
                        std::to_string(expected.validity->size()),
                        std::to_string(actual.validity->size()));
    }
    for (std::size_t row = 0; row < expected.validity->size(); ++row) {
        if ((*expected.validity)[row] != (*actual.validity)[row]) {
            return mismatch(std::format("column {} row {} validity", column, row),
                            (*expected.validity)[row] ? "valid" : "null",
                            (*actual.validity)[row] ? "valid" : "null");
        }
    }
    return std::nullopt;
}

template <typename T>
auto same_value(const T& lhs, const T& rhs) -> bool {
    return lhs == rhs;
}

template <>
auto same_value(const double& lhs, const double& rhs) -> bool {
    return lhs == rhs || (std::isnan(lhs) && std::isnan(rhs));
}

template <typename T>
auto compare_column_values(const Column<T>& expected, const Column<T>& actual, std::size_t column)
    -> std::optional<TableMismatch> {
    if (expected.size() != actual.size()) {
        return mismatch(std::format("column {} row count", column), std::to_string(expected.size()),
                        std::to_string(actual.size()));
    }
    for (std::size_t row = 0; row < expected.size(); ++row) {
        if (!same_value(expected[row], actual[row])) {
            return mismatch(std::format("column {} row {} value", column, row), "different value",
                            "different value");
        }
    }
    return std::nullopt;
}

auto compare_column_values(const Column<Categorical>& expected, const Column<Categorical>& actual,
                           std::size_t column) -> std::optional<TableMismatch> {
    if (expected.dictionary() != actual.dictionary()) {
        return mismatch(std::format("column {} categorical dictionary", column),
                        "different dictionary", "different dictionary");
    }
    if (expected.size() != actual.size()) {
        return mismatch(std::format("column {} row count", column), std::to_string(expected.size()),
                        std::to_string(actual.size()));
    }
    for (std::size_t row = 0; row < expected.size(); ++row) {
        if (expected.code_at(row) != actual.code_at(row)) {
            return mismatch(std::format("column {} row {} categorical code", column, row),
                            std::to_string(expected.code_at(row)),
                            std::to_string(actual.code_at(row)));
        }
    }
    return std::nullopt;
}

auto same_ordering(const std::optional<std::vector<ir::OrderKey>>& expected,
                   const std::optional<std::vector<ir::OrderKey>>& actual) -> bool {
    if (expected.has_value() != actual.has_value())
        return false;
    if (!expected)
        return true;
    if (expected->size() != actual->size())
        return false;
    for (std::size_t i = 0; i < expected->size(); ++i) {
        if ((*expected)[i].name != (*actual)[i].name ||
            (*expected)[i].ascending != (*actual)[i].ascending)
            return false;
    }
    return true;
}

}  // namespace

auto compare_tables(const Table& expected, const Table& actual) -> std::optional<TableMismatch> {
    if (expected.columns.size() != actual.columns.size())
        return mismatch("schema column count", std::to_string(expected.columns.size()),
                        std::to_string(actual.columns.size()));
    if (!same_ordering(expected.ordering, actual.ordering))
        return mismatch("ordering", "different ordering", "different ordering");
    if (expected.time_index != actual.time_index)
        return mismatch("time_index", expected.time_index.value_or("<none>"),
                        actual.time_index.value_or("<none>"));
    if (expected.columns.empty() && expected.logical_rows != actual.logical_rows)
        return mismatch("logical_rows", std::to_string(expected.logical_rows.value_or(0)),
                        std::to_string(actual.logical_rows.value_or(0)));
    for (std::size_t col = 0; col < expected.columns.size(); ++col) {
        const auto& lhs = expected.columns[col];
        const auto& rhs = actual.columns[col];
        if (lhs.name != rhs.name)
            return mismatch(std::format("column {} name", col), lhs.name, rhs.name);
        if (lhs.column->index() != rhs.column->index())
            return mismatch(std::format("column {} type", col), std::to_string(lhs.column->index()),
                            std::to_string(rhs.column->index()));
        if (auto m = validity_mismatch(lhs, rhs, col))
            return m;
        auto m = std::visit(
            [&](const auto& lhs_col) -> std::optional<TableMismatch> {
                using T = std::decay_t<decltype(lhs_col)>;
                return compare_column_values(lhs_col, std::get<T>(*rhs.column), col);
            },
            *lhs.column);
        if (m)
            return m;
    }
    return std::nullopt;
}

}  // namespace ibex::runtime
