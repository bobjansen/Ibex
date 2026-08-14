// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Drop group keys that a surviving key already determines.
///
/// PDS-H q10 groups on seven columns and gets 37967 groups — the same 37967
/// that `by { c_custkey }` alone produces, about ten times faster. The other
/// six columns cannot split a group because `c_custkey` is unique in
/// `customer`, so every one of them is constant within it. Grouping on them
/// widens the key for nothing: a wider key hashes more, compares more, and
/// (once it stops fitting a packed integer) falls off the fast paths entirely.
///
/// The rewrite groups on the determining subset and recovers the rest with
/// `first()`, which is exact precisely because they are constant per group:
///
///     by { c_custkey, c_name, ... }        by { c_custkey }
///     select { revenue = sum(x) }     ->   select { revenue = sum(x),
///                                                   c_name = first(c_name), ... }
///                                          project { c_custkey, c_name, ..., revenue }
///
/// The projection restores the original column ORDER, which the aggregate would
/// otherwise change (keys first, then aggregates). Row order needs no repair:
/// groups are emitted in first-occurrence order, and since the reduced and
/// original keys are in bijection, the two orders coincide.
///
/// **Three facts have to line up, and all three are proofs.** A base column
/// proved unique (`SchemaInfo::unique_keys`), the base column each group key
/// came from (`column_origins`), and the join edges that carry a dependency
/// from one table to another. q10 needs all three and the last transitively:
/// `c_custkey` determines `c_nationkey` inside `customer`, and the join
/// `c_nationkey = n_nationkey` carries that on to `n_name` because
/// `n_nationkey` is unique in `nation`.
///
/// Declines silently and completely whenever anything is unproven, which is the
/// pre-existing behaviour.
[[nodiscard]] auto reduce_functionally_dependent_group_keys(NodePtr root,
                                                            const SourceSchemas& sources)
    -> NodePtr;

}  // namespace ibex::ir
