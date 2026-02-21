#pragma once

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>

#include <expected>
#include <string>
#include <unordered_map>
#include <variant>

namespace ibex::runtime {

using ColumnValue = std::variant<Column<std::int64_t>, Column<double>, Column<std::string>>;

struct ColumnEntry {
    std::string name;
    ColumnValue column;
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

/// Interpret an IR node tree against a table registry.
[[nodiscard]] auto interpret(const ir::Node& node, const TableRegistry& registry)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
