// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Fold away grouped `max`/`min` aggregates of a proved-zero null flag.
///
/// Polars' `n_unique` counts a null as one more value, and SQL-style
/// `count_distinct` skips it, so the Polars adapter lowers `n_unique(x)` to
///
///     count_distinct(x) + coalesce(max(Int64(is_null(x))), 0)
///
/// (and a filtered `n_unique` to the same with the flag under a `case`). When
/// `x` is proved never null the flag is 0 on every row, and a grouped
/// aggregate has no empty group, so the `max` is 0 for every group -- yet it
/// costs a per-row update and an aggregate (adapter PDS-H q21: ~50 ms at 8
/// cores). The aggregate is dropped, the trailing update assigns its column
/// the literal 0 (so nothing above can tell) and reads it as that literal,
/// and the pre-aggregate update stops computing the flag.
///
/// Matches the shape the lowering of a computed aggregate field builds,
/// `Update(Aggregate(Update(input)))`, crossing any projections the column
/// pruning put between the aggregate and the pre-aggregate update. Both
/// updates must be plain (no group, guard or tuple fields) and the aggregate
/// grouped. A flag is `Int64(is_null(e))` with `e` proved never null over the
/// update's input, or a `case` whose every value is such a flag or the
/// literal 0. Declines silently whenever anything is unproven.
[[nodiscard]] auto fold_constant_null_flag_aggregates(NodePtr root, const SourceSchemas& sources)
    -> NodePtr;

}  // namespace ibex::ir
