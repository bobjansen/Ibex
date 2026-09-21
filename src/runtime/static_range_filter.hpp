// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <optional>
#include <string>
#include <vector>

namespace ibex::runtime {

/// The literal-range part of a conjunction, as an inclusive interval a reader
/// can decide while it decodes one integer-like key, plus every conjunct that
/// interval does not express.
struct StaticRange {
    std::string column;
    DynamicScanFilter filter;
    /// Conjuncts left over, in their original order. Empty when the interval is
    /// the whole predicate.
    std::vector<ir::Expr> rest;
};

/// Split `conjuncts` into the comparisons of one integer-like column against
/// literals and the rest. The column is the first one with such a comparison
/// that the reader can answer; its other comparisons are folded into the same
/// interval. nullopt means no such column, and the ordinary filter path stands.
///
/// `schema` types the column, because the reader compares raw int64 bits and so
/// must agree with what the ordinary filter would do with the same operands: an
/// Int literal is compared with an Int or Date column, a Date literal only with
/// a Date column, and a Double literal with an Int column, where it becomes the
/// integer bound it is equivalent to (`i > 5.5` is `i >= 6`; a fractional `==`
/// is an empty interval; a literal too large to stay exact is left alone).
/// Anything else (a Date literal against an Int column, a Double or String
/// column) is never absorbed into the interval. When a column
/// is chosen and another comparison on it has incompatible operands the whole
/// split declines, so the ordinary path can report the type error rather than
/// have it hidden by the rows the interval already removed.
[[nodiscard]] auto split_static_range(const std::vector<ir::Expr>& conjuncts, const Table& schema)
    -> std::optional<StaticRange>;

}  // namespace ibex::runtime
