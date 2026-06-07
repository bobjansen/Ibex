#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <optional>
#include <string>
#include <vector>

namespace ibex::runtime {

[[nodiscard]] auto cov_table(const Table& input) -> std::expected<Table, std::string>;
[[nodiscard]] auto corr_table(const Table& input) -> std::expected<Table, std::string>;
[[nodiscard]] auto transpose_table(const Table& input) -> std::expected<Table, std::string>;
[[nodiscard]] auto matmul_table(const Table& left, const Table& right)
    -> std::expected<Table, std::string>;
/// Vertically concatenate `tables` (rbind). Every operand must expose the same
/// set of columns by name and type as the first; the result carries the first
/// operand's column order.
///
/// When `merge_key` is set, the operands are assumed already sorted ascending by
/// that column (the TimeFrame invariant) and their rows are k-way **merged** in
/// time order — one output allocation, no post-hoc sort. The key column must be
/// Timestamp, Date, or Int64. When `merge_key` is null the rows of all operands
/// are simply appended in operand order.
[[nodiscard]] auto rbind_table(const std::vector<const Table*>& tables,
                               const std::optional<std::string>& merge_key = std::nullopt)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto melt_table(const Table& input, const std::vector<std::string>& id_columns,
                              const std::vector<std::string>& measure_columns)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto dcast_table(const Table& input, const std::string& pivot_column,
                               const std::string& value_column,
                               const std::vector<std::string>& row_keys)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
