// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Normalize `on { left_key = right_key }` into a same-named join.
///
/// Five optimizer passes decline outright on a join whose keys are not
/// same-named — filter pushdown and semi/anti pushdown
/// (`src/ir/join_pushdown.cpp`), the reorder cost model
/// (`src/ir/join_reorder.cpp`), join ordering (`src/ir/join_order.cpp`), and
/// deferrable probe scans (`src/ir/scan_predicates.cpp`). Each has its own good
/// reason, but the effect was that writing a join the mapped way silently cost
/// most of the join optimizer: measured at SF-1, PDS-H q03 ran 13-16% slower
/// purely for being spelled `on { o_orderkey = l_orderkey }`.
///
/// Those passes are not wrong. What was missing is that nothing normalized
/// their input. This pass renames the right side's key column to the left's,
/// which turns the mapped form into the shape they already handle:
///
///     Join(Inner, keys=[c_custkey = o_custkey])   Join(Inner, keys=[c_custkey])
///       Scan customer                       ->      Scan customer
///       Scan orders                                 Rename(o_custkey -> c_custkey)
///                                                     Scan orders
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
///     absent from the right child's schema (else the rename duplicates a name
///     inside it) and the right key's name is absent from the left child's (else
///     the fold changes which columns the suffix policy renames). Both schemas
///     must be Known.
///
/// Anything unproven is left mapped, which is exactly today's behaviour.
///
/// Pure on IR: takes ownership and returns the rewritten tree.
[[nodiscard]] auto normalize_mapped_join_keys(NodePtr root, const SourceSchemas& sources)
    -> NodePtr;

}  // namespace ibex::ir
