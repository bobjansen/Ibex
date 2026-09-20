// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>

namespace ibex::ir {

/// Restrict a grouped aggregate to the keys the join above it probes with.
///
///     Join(Left|Inner, on k = g, X, Aggregate(by g, ...))
///       ->
///     Join(Left|Inner, on k = g, X, Aggregate(by g, ...)
///                                     Join(Semi, on g = k, <input>, Project(k)(X)))
///
/// The aggregate otherwise groups its WHOLE input while the join reads only
/// the groups `X` keys into. A semi join keeps every input row whose key
/// appears in `X`, so each surviving group keeps ALL of its rows and its
/// aggregate is unchanged; the groups it drops could never have been matched.
///
/// A decorrelated `scalar(...)` subquery is the shape this was written for,
/// but nothing here knows that: it matches the plan, so a join written by hand
/// against a grouped aggregate gets the same treatment.
///
/// **What it costs.** `X` is evaluated a second time to supply the keys, so
/// this pays only when `X` keys into fewer distinct values than the aggregate
/// would otherwise group -- the saving is groups never built, so the estimate
/// that decides it counts GROUPS, not rows. Without one for both sides the
/// rewrite is declined rather than guessed at.
///
/// **What it requires.** `X` must be replayable — evaluated twice and give the
/// same rows. Run this AFTER `hoist_extern_sources`: a reader is an ExternCall
/// until then, indistinguishable from a plugin, and would be refused.
[[nodiscard]] auto restrict_aggregates_to_probed_keys(NodePtr root, const SourceStats& stats)
    -> NodePtr;

}  // namespace ibex::ir
