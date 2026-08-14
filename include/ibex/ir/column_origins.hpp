// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <optional>
#include <robin_hood.h>
#include <string>

namespace ibex::ir {

/// The base-table column one output column's values are taken from, unchanged.
struct ColumnOrigin {
    std::string source;  ///< Source instance name, as a `ScanNode` names it.
    std::string column;  ///< That source's own name for the column.

    friend auto operator==(const ColumnOrigin&, const ColumnOrigin&) -> bool = default;
};

/// Output column name -> where its values came from. A column with no entry has
/// no single origin, which is the sound answer for anything computed.
using ColumnOriginMap = robin_hood::unordered_map<std::string, ColumnOrigin>;

/// Trace each of `node`'s output columns back to the base column it came from.
///
/// **Why this is derived rather than stored.** Names are the IR's column
/// identity, and they are user-visible semantics — `select { c_nationkey =
/// n_nationkey }` renames because the query said to. Column IDs would have to
/// sit *alongside* names rather than replace them, in every node, through the
/// parser and lowerer and out into a name-keyed runtime `Table`. So identity
/// stays nominal and provenance is recomputed, the same choice `infer_schema`
/// and `required_columns` already make: each walks the plan composing rename
/// maps, one upward and one downward.
///
/// What this adds is a THIRD such walk that answers the question neither does:
/// not "what is this column called here" but "which base column is it". A pass
/// that needs that — a functional dependency, where a key proved unique in one
/// base table still determines its other columns after a join has fanned them
/// out — cannot get it by matching names, because the name it is holding may
/// have been assigned three renames ago.
///
/// **Only value-preserving operators propagate.** A projection of a bare column
/// reference carries its origin; `x = a + b` does not, and reports none. Filter,
/// order, head/tail and distinct only drop or reorder rows, so every column
/// keeps its origin. A grouped aggregate's KEY columns keep theirs — the output
/// holds one of the values that grouped — while its aggregate outputs are
/// computed and have none.
///
/// Join output naming comes from `plan_join_output`, the single authority on
/// it, so the suffix policy and folded same-name keys are handled where they
/// are defined rather than restated here. That is the drift `JoinOutputColumn`
/// exists to prevent.
///
/// `sources` supplies each source's column list, exactly as `infer_schema`
/// takes it; a source that is absent contributes no origins.
[[nodiscard]] auto column_origins(const Node& node, const SourceSchemas& sources = {})
    -> ColumnOriginMap;

/// The origin of `column` in `node`'s output, or nullopt when it has none.
[[nodiscard]] auto column_origin_of(const Node& node, const std::string& column,
                                    const SourceSchemas& sources = {})
    -> std::optional<ColumnOrigin>;

}  // namespace ibex::ir
