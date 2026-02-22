#pragma once

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>

#include <expected>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>

namespace ibex::runtime {

enum class ScalarKind : std::uint8_t {
    Int,
    Double,
    String,
};

using ColumnValue = std::variant<Column<std::int64_t>, Column<double>, Column<std::string>>;
using ScalarValue = std::variant<std::int64_t, double, std::string>;

struct ColumnEntry {
    std::string name;
    std::shared_ptr<ColumnValue> column;
};

struct Table {
    std::vector<ColumnEntry> columns;
    std::unordered_map<std::string, std::size_t> index;

    void add_column(std::string name, ColumnValue column);
    [[nodiscard]] auto find(const std::string& name) -> ColumnValue*;
    [[nodiscard]] auto find(const std::string& name) const -> const ColumnValue*;
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
