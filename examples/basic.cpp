// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/format.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/runtime/ops.hpp>

auto main() -> int {
    // Create a column of prices
    ibex::Column<double> prices{100.5, 200.3, 50.0, 175.8, 320.1};

    ibex::formatting::print("=== Column operations ===\n");
    ibex::formatting::print("prices: {} elements\n", prices.size());

    // Filter: keep prices above 100
    auto expensive = prices.filter([](double p) { return p > 100.0; });
    ibex::formatting::print("prices > 100: {} elements\n", expensive.size());

    // Transform: convert to basis points
    auto bps = prices.transform([](double p) { return p * 100.0; });
    ibex::formatting::print("first price in bps: {}\n", bps[0]);

    // Build a simple IR plan
    ibex::formatting::print("\n=== IR builder ===\n");

    ibex::ir::Builder builder;

    auto scan = builder.scan("trades");
    auto filter = builder.filter(ibex::ops::filter_cmp(
        ibex::ir::CompareOp::Gt, ibex::ops::filter_col("price"), ibex::ops::filter_dbl(100.0)));
    filter->add_child(std::move(scan));

    auto project = builder.project({
        ibex::ir::ColumnRef{.name = "symbol"},
        ibex::ir::ColumnRef{.name = "price"},
    });
    project->add_child(std::move(filter));

    ibex::formatting::print("plan root: node id={}, kind={}\n", project->id().value,
               static_cast<int>(project->kind()));
    ibex::formatting::print("plan has {} children\n", project->children().size());

    return 0;
}
