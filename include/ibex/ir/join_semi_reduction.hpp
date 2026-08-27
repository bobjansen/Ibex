// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Existence-join reductions.
///
/// An inner join whose other side is *proved* unique on the join keys and
/// contributes no column the plan reads is a semi join wearing an inner join's
/// costs. It builds an index, probes it, and materializes a wide result to
/// carry columns nobody looks at.
///
///   Join(Inner, k, X, Y)  ->  Join(Semi, k, X, Y)     Y unique on k, no Y column read
///   Join(Inner, k, X, Y)  ->  Join(Semi, k', Y, X)    X unique on k, no X column read
///   Filter(is_null(r), Join(Left, k, X, Y)) -> Join(Anti, k, X, Y)
///
/// PDS-H q21 is the motivating shape. Its `q1` binding joins a grouped
/// aggregate -- unique on `l_orderkey` by construction -- to late line items,
/// purely to carry `n_supp_by_order`, which the filter directly above consumes
/// and nothing below ever reads. The join materializes 7.3M rows at SF-2 and is
/// the single largest serial block in the suite (`build_self_ms=285`). Written
/// as a semi join by hand, q21 measures 636ms -> 526ms at eight cores and
/// 1162ms -> 996ms at one, byte-identical answers.
///
/// **Why the row count is preserved.** A semi join emits each retained row at
/// most once. An inner join emits one output row per matching PAIR, so the two
/// agree exactly when the dropped side can contribute at most one match per
/// retained row -- which is what uniqueness on the join keys means.
/// `SchemaInfo::is_unique_within` is a proof, not an estimate: an `Aggregate`
/// fixes its group keys whatever it reads.
///
/// **Why the columns are the same.** `join_output_demand` reports what the plan
/// reads from this join's own output. A name present on BOTH sides resolves to
/// the retained side above the join and is equal on surviving rows by
/// construction, so only names exclusive to the dropped side can be lost --
/// and those are exactly what this pass requires to be unread. That test also
/// covers a mapped key (`on { a = b }`), whose right-hand column survives in the
/// output and lives on one side only.
///
/// The anti form additionally proves that `r` is a right-side value which is
/// non-null on every match, and that no right output is observed above the
/// filter. A `Distinct` immediately on the right of a semi/anti join is removed:
/// existence is unchanged by duplicate right rows. PDS-H q22 is the motivating
/// shape for both reductions.
///
/// Conservative on everything else. The inner reduction is declined for a predicate
/// (non-equi), `take`, a declared `expect` (its pair count is what gets
/// checked), `nulls equal`, a suffix policy (it renames the very columns this
/// pass reasons about), an open or Unknown schema on either side, and any join
/// this pass's demand map does not cover. `pending_order` is dropped when the
/// inner rewrite changes orientation: it is a cost hint naming the old
/// orientation, and per SPEC.md Section 5.6 row order is outside the join
/// contract either way. The left-to-anti rewrite preserves it because the
/// orientation does not change.
///
/// Runs after `push_filters_into_joins` and `push_semi_joins_down` -- it wants
/// the tree those leave behind, and the semi join it produces is a candidate
/// for nothing further. Pure on IR: takes ownership and returns the rewritten
/// tree.
[[nodiscard]] auto reduce_inner_joins_to_semi(NodePtr root, const SourceSchemas& sources)
    -> NodePtr;

}  // namespace ibex::ir
