// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <optional>
#include <span>
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
[[nodiscard]] auto join_table_impl(
    const Table& left, const Table& right, ir::JoinKind kind, const std::vector<ir::JoinKey>& keys,
    const ir::Expr* predicate, const ScalarRegistry* scalars, PredicateMaskEvaluator mask_evaluator,
    const ir::JoinSuffixPolicy& suffix = {}, const std::vector<ir::OrderKey>& pending_order = {},
    ir::NullMatch null_match = ir::NullMatch::Never, const ir::JoinExpect& expect = {},
    ir::MatchSelection take = ir::MatchSelection::All, const ExecutionContext* exec = nullptr)
    -> std::expected<Table, std::string>;

/// Which column the fused left-join aggregate should read, given the `Update`
/// nodes between the aggregate and the join, ordered aggregate-first. Returns
/// nullopt when the rewrite must decline.
///
/// The rewrite aggregates the JOIN's output directly and never builds the
/// nodes between, so an update that *computes* a value the aggregate reads
/// cannot be waved through: its column does not exist down there, and the
/// column it was derived from holds different values. Exactly one update shape
/// is safe, and it is the one this fast path exists for -- `count(col)` lowers
/// to `Update{alias = Int64(col is not null)} + sum(alias)`, a 0/1 flag whose
/// grouped sum is what `left_join_count_table` computes from the join
/// structure itself. Anything else declines, and the ordinary path runs the
/// whole chain.
[[nodiscard]] auto fused_left_join_counted_column(
    const ir::AggregateNode& aggregate, std::span<const ir::UpdateNode* const> skipped_updates)
    -> std::optional<std::string>;

/// Physical rewrite for a unique-left-key left join followed by
/// count(right_column) grouped by that key. Returns nullopt when the shape or
/// runtime proofs are insufficient and the ordinary join must be used.
[[nodiscard]] auto left_join_count_table(const ir::JoinNode& join,
                                         const ir::AggregateNode& aggregate, const Table& left,
                                         const Table& right, std::string_view counted_column)
    -> std::optional<Table>;

}  // namespace ibex::runtime
