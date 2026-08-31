// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Fuse the `distinct` + `count()` idiom into a single `count_distinct`
/// aggregate.
///
///   Aggregate(count() as c, by K)
///     Distinct
///       Project(K + {v})              ->  Aggregate(count_distinct(v) as c, by K)
///                                           Project(K + {v})
///
/// This is exactly what `t[distinct { K..., v }][select { c = count() }, by K]`
/// lowers to, and it is how `count(distinct v)` was expressed before the
/// aggregate existed. The two forms are semantically identical -- the fused one
/// discovers the groups and counts distinct values in one grouped pass instead
/// of a Distinct breaker followed by a second grouping pass. PDS-H q16 is the
/// motivating shape.
///
/// Strictly structural: the extra projected column must not be a group key, the
/// only aggregate must be a column-less `count()`, and the Distinct must sit
/// directly between the Aggregate and a Project that names exactly the group
/// keys plus one more column. Also declined unless the counted column is
/// proved null-free: the idiom keeps a null value as its own distinct pair
/// (so `count()` includes it), while `count_distinct` excludes nulls the SQL
/// way, so the two only agree when the column cannot be null. Pure on IR:
/// takes ownership and returns the rewritten tree.
[[nodiscard]] auto fuse_distinct_count_to_count_distinct(NodePtr root,
                                                         const SourceSchemas& sources = {})
    -> NodePtr;

}  // namespace ibex::ir
