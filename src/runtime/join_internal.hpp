#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <string>
#include <vector>

namespace ibex::runtime {

[[nodiscard]] auto join_table_no_predicate(const Table& left, const Table& right, ir::JoinKind kind,
                                           const std::vector<std::string>& keys)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
