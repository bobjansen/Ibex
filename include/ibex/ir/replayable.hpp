// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

namespace ibex::ir {

/// True if `node`'s subtree may be evaluated a SECOND time and be relied on to
/// produce the same rows.
///
/// A rewrite that clones a subtree is really asking for this. Cloning is not
/// the cost it looks like -- the clone is evaluated, so anything the subtree
/// does, it does twice, and anything it draws, it draws again. A plan that
/// reads `rand_uniform` into a key column answers one way in the original and
/// another in the copy, and a rewrite that compares the two is then comparing
/// two different tables.
///
/// The test is an ALLOW-list, so a node kind nobody has thought about is
/// unsafe rather than silently assumed pure, and the same for a call whose
/// callee is not a known built-in: an extern is left unclassified on purpose
/// (see `BuiltinFunctionInfo`) and planning must not assume it is
/// reproducible. `FnKind::Generator` (`rand_*`) is refused outright.
///
/// This says nothing about COST. A subtree can be perfectly replayable and
/// still far too expensive to evaluate twice; that is the caller's judgement.
[[nodiscard]] auto is_replayable_subplan(const Node& node) -> bool;

/// True if `expr` may be evaluated a second time for the same reasons.
[[nodiscard]] auto is_replayable_expr(const Expr& expr) -> bool;

}  // namespace ibex::ir
