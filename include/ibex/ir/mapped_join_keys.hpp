// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Mark an unobservable mapped key as folded in the join's output.
///
/// Several optimizer passes need one stable name for a key across a join chain:
/// filter and semi/anti pushdown, inner-chain costing, physical reordering, and
/// deferred scan-side filtering. Without normalization that made a large part
/// of the optimizer needlessly spelling-sensitive: measured at SF-1, PDS-H q03
/// ran 13-16% slower purely for being spelled
/// `on { o_orderkey = l_orderkey }`.
///
/// Those passes are not wrong. What was missing is a shared logical key name.
/// This pass preserves each input's native spelling and records that the right
/// key folds into the left key's output column:
///
///     Join(Inner, keys=[c_custkey = o_custkey])
///       Scan customer
///       Scan orders
///
/// becomes the same tree with `fold_output=true` on that JoinKey. Execution
/// still reads `o_custkey` from orders; the optimizer uses `c_custkey` as the
/// folded key's logical output name.
///
/// It also removes a cost the passes never had anything to do with: SPEC 12.3
/// keeps BOTH key columns in a mapped join's output, so the join materialises a
/// column the same-named form folds away. On q13 — where no pass declines at
/// all — that alone is 4%.
///
/// A key is rewritten only when the fold is **unobservable**:
///
///   - **Inner, Left, Semi or Anti.** Inner: the keys are equal on every
///     surviving row. Left: the folded column takes the left row's value, which
///     is what an unmatched left row already carries. Semi/Anti emit no right
///     columns at all. **Right and Outer are excluded** — their folded key
///     column is filled from the right row for unmatched right rows, so folding
///     would change the values of the column that survives.
///   - **Nothing above the join reads the right key column** (`join_output_demand`).
///     Reconstructing it would need an order-restoring projection, and under a
///     Left join it is not even a copy of the left key on unmatched rows.
///   - **Neither name collides across the two sides**: the left key's name is
///     absent from the right child's schema (else folding would hide a distinct
///     column under that name) and the right key's name is absent from the left
///     child (else the fold changes which columns the suffix policy renames).
///     Both schemas must be Known.
///
/// Anything unproven is left mapped, which is exactly today's behaviour.
///
/// Pure on IR: takes ownership and returns the rewritten tree.
[[nodiscard]] auto normalize_mapped_join_keys(NodePtr root, const SourceSchemas& sources)
    -> NodePtr;

}  // namespace ibex::ir
