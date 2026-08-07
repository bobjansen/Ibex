#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <string>
#include <string_view>
#include <vector>

#include "runtime_internal.hpp"

namespace ibex::runtime {

/// Column names of `table`, in order — the input form expected by
/// `ir::plan_join_output()`.
[[nodiscard]] inline auto table_column_names(const Table& table) -> std::vector<std::string_view> {
    std::vector<std::string_view> names;
    names.reserve(table.columns.size());
    for (const auto& entry : table.columns) {
        names.emplace_back(entry.name);
    }
    return names;
}

using PredicateMaskEvaluator = std::expected<Mask, std::string> (*)(const ir::Expr& predicate,
                                                                    const Table& table,
                                                                    const ScalarRegistry* scalars,
                                                                    RowRange rows);

/// `pending_order` is what an `order` directly above this join will ask for
/// (`ir::JoinNode::pending_order`), or empty. It can only shift which side is
/// indexed, never the rows or their names -- see `choose_build_side` in
/// join.cpp for the trade.
[[nodiscard]] auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                                   const std::vector<ir::JoinKey>& keys, const ir::Expr* predicate,
                                   const ScalarRegistry* scalars,
                                   PredicateMaskEvaluator mask_evaluator,
                                   const ir::JoinSuffixPolicy& suffix = {},
                                   const std::vector<ir::OrderKey>& pending_order = {},
                                   ir::NullMatch null_match = ir::NullMatch::Never)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
