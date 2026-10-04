// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <string>
#include <vector>

namespace ibex::ir {

/// Equality join predicates as join keys.
///
/// `A join B on a == b` is a predicate (theta) join by the grammar, and a
/// predicate join runs as a nested loop. When the predicate is nothing but
/// equalities between a left and a right column, it means exactly what
/// `on { a = b }` means, which runs as a hash join:
///
///   Join(kind, keys = {}, predicate = l1 == r1 && l2 == r2)
///     -> Join(kind, keys = {l1 = r1, l2 = r2}, no predicate)
///
/// **Same rows.** A null on either side makes `==` null, which no join keeps;
/// an equality key never matches a null either.
///
/// **Same columns.** A mapped key pair keeps both columns, as a predicate join
/// does. A pair with ONE name on both sides (`left(a) == right(a)`) would fold
/// into one output column as a key, so it is never rewritten.
///
/// **Same comparisons.** Only when both columns' types are known and equal, and
/// equality means the same thing to `==` and to a hash match: Int32, Int64,
/// Bool, Date, Timestamp, String and Categorical (compatible with each other),
/// and Decimal of one precision and scale. Floating-point is excluded: `==`
/// never matches NaN, a hash match on the bits can. Differing types are
/// excluded: `==` compares Int64 with Float64, an equality key refuses them.
///
/// A predicate that is all such equalities but cannot be rewritten -- types
/// unknown when the plan is made, differing, floating-point, or one shared name
/// -- stays a nested loop and earns a warning naming the reason and the fix.
/// A predicate with any other term (`a == b && x < y`) is a genuine theta join
/// and is left alone, silently.
struct JoinPredicateKeyRewrite {
    NodePtr plan;
    /// One line per join left as a nested loop that looked like an equi-join.
    std::vector<std::string> warnings;
};

[[nodiscard]] auto equality_predicates_to_join_keys(NodePtr root, const SourceSchemas& sources = {})
    -> JoinPredicateKeyRewrite;

}  // namespace ibex::ir
