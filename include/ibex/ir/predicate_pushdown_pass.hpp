// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/optimizer.hpp>

#include <memory>

namespace ibex::ir {

/// Create an IR optimization pass that pushes filter predicates down past joins.
///
/// Detects patterns like `Filter(expr)(Join(...))` and moves the filter to execute
/// on one of the join operands when the filter references only columns from that operand.
/// This reduces intermediate cardinality before expensive joins.
[[nodiscard]] auto make_predicate_pushdown_pass() -> std::unique_ptr<OptimizationPass>;

}  // namespace ibex::ir
