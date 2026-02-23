#include <ibex/core/column.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/runtime/ops.hpp>

#include <fmt/core.h>

auto main() -> int {
    // Create a column of prices
    ibex::Column<double> prices{100.5, 200.3, 50.0, 175.8, 320.1};

    fmt::print("=== Column operations ===\n");
    fmt::print("prices: {} elements\n", prices.size());

    // Filter: keep prices above 100
    auto expensive = prices.filter([](double p) { return p > 100.0; });
    fmt::print("prices > 100: {} elements\n", expensive.size());

    // Transform: convert to basis points
    auto bps = prices.transform([](double p) { return p * 100.0; });
    fmt::print("first price in bps: {}\n", bps[0]);

    // Build a simple IR plan
    fmt::print("\n=== IR builder ===\n");

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

    fmt::print("plan root: node id={}, kind={}\n", project->id(),
               static_cast<int>(project->kind()));
    fmt::print("plan has {} children\n", project->children().size());

    return 0;
}
