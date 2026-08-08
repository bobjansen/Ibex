<<<<<<< HEAD
// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

=======
>>>>>>> 5ab6755 (Make the nullability rules single-sourced and testable)
#pragma once

#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>

#include <robin_hood.h>
#include <string>
#include <vector>

namespace ibex::ir {

class SchemaInfo;

/// The rules deciding whether a column can hold a null, kept apart from
/// `infer_schema` because they are a body of semantics rather than a step of
/// schema propagation: each one is an argument about what an operator does to
/// values, and each is checked against the runtime that actually does it.
///
/// Every rule here answers `Nullability::Maybe` for anything it cannot argue,
/// so an unmodelled expression or operator costs precision and never soundness.
/// Adding a rule means adding an argument; see `plans/joins.md` for the ones
/// already made.

/// Whether a computed field can hold a null, given the schema it reads.
[[nodiscard]] auto expr_nullability(const Expr& expr, const SchemaInfo& input) -> Nullability;

/// Whether an aggregate's result column can hold a null.
///
/// `grouped` is load-bearing: an aggregate emits one row per group that
/// occurred, so a grouped aggregate's every row summarizes at least one input
/// row. An ungrouped one emits its single row over an empty input too, where
/// `min` has no value to return however null-free its column is.
[[nodiscard]] auto agg_nullability(const AggSpec& agg, const SchemaInfo& input, bool grouped)
    -> Nullability;

/// The columns `predicate` proves null-free in the rows a `filter` keeps.
///
/// `filter` keeps a row only when its predicate is *true*, and Ibex is
/// three-valued: null is not true, so a row whose predicate went null is
/// dropped along with the rows that went false. Every column the predicate had
/// to read to reach true is therefore present in every surviving row.
[[nodiscard]] auto filter_proved_non_null(const Expr& predicate)
    -> robin_hood::unordered_set<std::string>;

/// Set each planned join output column's nullability in `out`, which must be
/// the fields built from `plan` in order.
void apply_join_nullability(const JoinNode& join, const SchemaInfo& left, const SchemaInfo& right,
                            const std::vector<JoinOutputColumn>& plan,
                            std::vector<SchemaField>& out);

}  // namespace ibex::ir
