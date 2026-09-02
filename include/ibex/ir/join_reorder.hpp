// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <cstddef>
#include <string_view>

namespace ibex::ir {

/// Reorder safe inner-join chains directly below order-insensitive aggregates.
/// The rewrite is deliberately narrow: it requires known schemas and source
/// sizes, permits duplicate names only for equijoin keys, and leaves First/
/// Last aggregates untouched because they observe input order.
[[nodiscard]] auto reorder_inner_joins_for_aggregates(NodePtr root, const SourceStats& stats)
    -> NodePtr;

/// Reorder the inner-join chain reachable through row-wise nodes at the root.
/// The caller must have proved that input encounter order is unobservable.
[[nodiscard]] auto reorder_inner_joins_for_order_insensitive_root(NodePtr root,
                                                                  const SourceStats& stats)
    -> NodePtr;

struct BindingOrderUses {
    std::size_t count = 0;
    bool all_order_insensitive = true;
};

/// Find scans of `binding` and prove whether each one's encounter order is
/// erased by an order-insensitive aggregate before an order-sensitive operator
/// can observe it.
[[nodiscard]] auto binding_order_uses(const Node& root, std::string_view binding)
    -> BindingOrderUses;

}  // namespace ibex::ir
