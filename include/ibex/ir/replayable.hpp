// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

#include <cstdint>

namespace ibex::ir {

/// A deep copy of `node`'s subtree, or null when any part of it must not be
/// evaluated a second time.
///
/// A rewrite that clones a subtree is really asking whether it may be replayed.
/// Cloning is not the cost it looks like -- the clone is evaluated, so anything
/// the subtree does, it does twice, and anything it draws, it draws again. A
/// plan that reads `rand_uniform` into a key column answers one way in the
/// original and another in the copy, and a rewrite that compares the two is
/// then comparing two different tables.
///
/// Copying and the safety test are ONE switch, so a kind that cannot be copied
/// is exactly a kind that must not be replayed and the two cannot drift apart.
/// It is an allow-list: a node kind nobody has classified is refused rather
/// than silently assumed pure, and so is a call whose callee is not a known
/// built-in, since an extern is left unclassified on purpose (see
/// `BuiltinFunctionInfo`) and planning must not assume it is reproducible.
/// `FnKind::Generator` (`rand_*`) is refused outright.
///
/// Note that a reader is an `ExternCall` until `hoist_extern_sources` turns it
/// into a `Scan`, so a caller running before that point will be refused a plan
/// that only reads a file.
///
/// This says nothing about COST. A subtree can be perfectly replayable and
/// still far too expensive to evaluate twice; that is the caller's judgement.
///
/// `next` supplies fresh node ids and is advanced.
[[nodiscard]] auto clone_replayable_subplan(const Node& node, std::uint64_t& next) -> NodePtr;

/// True if `expr` may be evaluated a second time for the same reasons.
[[nodiscard]] auto is_replayable_expr(const Expr& expr) -> bool;

}  // namespace ibex::ir
