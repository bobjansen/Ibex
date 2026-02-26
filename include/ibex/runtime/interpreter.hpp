#pragma once

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

namespace ibex::runtime {

enum class ScalarKind : std::uint8_t {
    Int,
    Double,
    String,
    Date,
    Timestamp,
};

using ColumnValue = std::variant<Column<std::int64_t>, Column<double>, Column<std::string>,
                                 Column<Categorical>, Column<Date>, Column<Timestamp>>;
using ScalarValue = std::variant<std::int64_t, double, std::string, Date, Timestamp>;

struct ColumnEntry {
    std::string name;
    std::shared_ptr<ColumnValue> column;
    // Validity bitmap: true = valid (not null), false = null.
    // nullopt means every row is valid — the common case, with zero overhead.
    std::optional<std::vector<bool>> validity;
};

/// Returns true if row `row` of `entry` is null.
[[nodiscard]] inline auto is_null(const ColumnEntry& entry, std::size_t row) -> bool {
    return entry.validity.has_value() && !(*entry.validity)[row];
}

struct Table {
    std::vector<ColumnEntry> columns;
    std::unordered_map<std::string, std::size_t> index;
    std::optional<std::vector<ir::OrderKey>> ordering;
    std::optional<std::string> time_index;

    void add_column(std::string name, ColumnValue column);
    /// Add a column with an explicit validity bitmap (true = valid, false = null).
    void add_column(std::string name, ColumnValue column, std::vector<bool> validity);
    [[nodiscard]] auto find(const std::string& name) -> ColumnValue*;
    [[nodiscard]] auto find(const std::string& name) const -> const ColumnValue*;
    [[nodiscard]] auto find_entry(const std::string& name) const -> const ColumnEntry*;
    [[nodiscard]] auto rows() const noexcept -> std::size_t;
};

using TableRegistry = std::unordered_map<std::string, Table>;
using ScalarRegistry = std::unordered_map<std::string, ScalarValue>;

/// Interpret an IR node tree against a table registry.
class ExternRegistry;

[[nodiscard]] auto interpret(const ir::Node& node, const TableRegistry& registry,
                             const ScalarRegistry* scalars = nullptr,
                             const ExternRegistry* externs = nullptr)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                               const std::vector<std::string>& keys)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string>;

}  // namespace ibex::runtime
