#pragma once

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Reorder safe inner-join chains directly below order-insensitive aggregates.
/// The rewrite is deliberately narrow: it requires known schemas and source
/// sizes, permits duplicate names only for equijoin keys, and leaves First/
/// Last aggregates untouched because they observe input order.
[[nodiscard]] auto reorder_inner_joins_for_aggregates(NodePtr root, const SourceStats& stats)
    -> NodePtr;

}  // namespace ibex::ir
