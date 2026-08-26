// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

namespace ibex::ir {

/// Dedup on one copy of a duplicated column, and re-add the rest afterwards.
///
/// `DistinctNode` carries no key list: it deduplicates on every column of its
/// input. So a plan that presents the SAME value under two names pays for both.
/// PDS-H q22 does exactly that — it needs `o_custkey` under the joining side's
/// name, so it selects `{ c_custkey = o_custkey, o_custkey }` and then dedups
/// on the pair:
///
///     distinct { c_custkey, o_custkey }   ->   project { c_custkey, o_custkey }
///                                              update  { o_custkey = c_custkey }
///                                              distinct
///                                              project { c_custkey }
///
/// The duplication moves ABOVE the dedup, which is exact: a column that carries
/// the same value as another cannot split a duplicate group, so the two plans
/// agree row for row, and first-occurrence order is preserved because the input
/// row order and the equality classes are both unchanged. The projection on top
/// restores the original column order, which the update would otherwise change.
///
/// Halving q22's dedup key also halves the store: two `Int64` columns pack into
/// a 16-byte key and one into 8, so the reduced form both hashes less and holds
/// the 200k-entry set in half the memory.
///
/// **Two columns are the same value only when they are provably the same
/// value.** Sharing a `ColumnOrigin` is NOT enough: a self-join's two sides
/// name the same base column while pairing different rows. This walks the plan
/// instead, from the dedup down through the nodes that neither compute nor
/// combine (project, filter, rename, and an update field that is a bare column
/// copy), and stops at everything else — a join included. Two columns match
/// only when that walk lands them on the same name at the same node.
///
/// Declines silently and completely whenever the shape is anything else.
[[nodiscard]] auto reduce_duplicate_distinct_columns(NodePtr root) -> NodePtr;

}  // namespace ibex::ir
