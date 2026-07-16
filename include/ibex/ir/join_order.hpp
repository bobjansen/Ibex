#pragma once

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <cstddef>
#include <optional>
#include <vector>

namespace ibex::ir {

/// A cost-driven order over the leaf relations of a left-deep inner equijoin
/// chain. Indices refer to the chain's original left-to-right leaf order.
/// `nullopt` means the tree is not a safe/known join graph yet.
[[nodiscard]] auto choose_inner_join_order(const Node& root, const SourceSchemas& schemas,
                                           const SourceRowCounts& row_counts)
    -> std::optional<std::vector<std::size_t>>;

}  // namespace ibex::ir
