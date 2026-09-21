// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::runtime {

/// A conjunction made solely of literal comparisons on one integer-like source
/// column, as an inclusive interval a reader can decide while it decodes the
/// key. nullopt means "not this shape", and the ordinary filter path stands.
///
/// `schema` types the column, because the reader compares raw int64 bits and so
/// must agree with what the ordinary filter would do with the same operands: an
/// Int literal is compared with an Int or Date column, a Date literal only with
/// a Date column. Anything else (a Date literal against an Int column, a
/// Double or String column) is left to the ordinary path, which reports the
/// type error or evaluates the comparison itself, rather than being answered
/// here as though the operands were compatible.
[[nodiscard]] auto static_range_filter(const std::vector<ir::Expr>& conjuncts, const Table& schema)
    -> std::optional<std::pair<std::string, DynamicScanFilter>>;

}  // namespace ibex::runtime
