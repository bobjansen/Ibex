// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

namespace ibex::ir {

/// Tell each join what a following `order` is going to ask for, by writing the
/// order's keys into `JoinNode::pending_order`.
///
/// A join chooses which side to index from the two row counts, and a path that
/// indexes the right and scans the left emits the left rows in their own order.
/// When the left already carries that order, choosing that side turns "join,
/// then sort the output" into "join". Choosing it costs something too -- the
/// side sizes are why the other one was preferred -- so the executor makes the
/// trade with both row counts in hand. This pass only supplies the missing
/// half: what the query is about to ask for.
///
/// It is a cost hint end to end. A join's row order is outside the contract
/// (SPEC.md Section 5.6), the `order` above it still runs and still guarantees
/// the order, and a consumer that ignores the annotation computes exactly the
/// same rows. That is what lets the transpiler skip it.
///
/// The `order` need not be the join's immediate parent. Canonicalize lifts
/// `order` above a filter (R1) and above a key-preserving projection (R2),
/// which puts those operators *between* the two, so the search looks through
/// the operators that neither move a row relative to its neighbours nor rename
/// a column -- see `join_below` in the .cpp for the list and for what is
/// deliberately left out.
void annotate_pending_orders(Node& root);

}  // namespace ibex::ir
