// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <string>

#include "physical_plan.hpp"

namespace {

using namespace ibex;

auto require_ir(const char* source) -> ir::NodePtr {
    auto parsed = parser::parse(source);
    if (!parsed.has_value()) {
        FAIL("parse failed: " + std::string(source));
    }
    auto lowered = parser::lower(*parsed);
    if (!lowered.has_value()) {
        FAIL("lower failed: " + std::string(source));
    }
    return std::move(lowered.value());
}

auto trades_registry() -> runtime::TableRegistry {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});
    table.add_column("ok", Column<bool>{true, false, true});
    runtime::TableRegistry registry;
    registry.emplace("trades", table);
    return registry;
}

auto serial_exec() -> runtime::ExecutionContext {
    runtime::ExecutionContext exec;
    exec.parallel = false;
    return exec;
}

/// The plan borrows the IR it was lowered from (like
/// ParallelIslandCandidate) — keep the tree alive alongside the plan.
auto serial_plan(const char* source) -> std::pair<ir::NodePtr, runtime::physical::Plan> {
    auto ir = require_ir(source);
    auto plan = runtime::physical::plan_physical(*ir, trades_registry(), nullptr);
    return {std::move(ir), std::move(plan)};
}

}  // namespace

TEST_CASE("Physical plan lowers filter+select into a fused map step", "[physical][plan]") {
    // Canonicalize R5 fuses Project(Filter(x)) into FilterProject before the
    // planner ever runs, so the plan's step vocabulary has to know the fused
    // kinds — the plan describes the tree that exists, not the one written.
    const auto [plan_tree, plan] = serial_plan("trades[filter price > 15, select { price }];");
    REQUIRE(plan.migrated);
    REQUIRE(plan.steps.size() == 1);
    REQUIRE(plan.steps.front()->kind() == ir::NodeKind::FilterProject);
    REQUIRE(plan.kernel_dispatch.size() == 1);
    REQUIRE(plan.kernel_dispatch.front().capability ==
            runtime::MapKernelCapability::FilterProjectGather);
    REQUIRE(plan.kernel_dispatch.front().factory != nullptr);
    REQUIRE(plan.source == runtime::physical::SourceKind::TableScan);
    REQUIRE(plan.source_node != nullptr);
    REQUIRE(plan.source_node->kind() == ir::NodeKind::Scan);
    REQUIRE(plan.source_signature ==
            std::vector{runtime::ColumnKernelSignature{runtime::ColumnRepresentation::FixedWidth,
                                                       runtime::KernelNullPolicy::AllValid},
                        runtime::ColumnKernelSignature{runtime::ColumnRepresentation::StringSlabs,
                                                       runtime::KernelNullPolicy::AllValid},
                        runtime::ColumnKernelSignature{runtime::ColumnRepresentation::PackedBool,
                                                       runtime::KernelNullPolicy::AllValid}});
}

TEST_CASE("Physical plan lowers select-only and row-local update chains", "[physical][plan]") {
    const auto [project_tree, project] = serial_plan("trades[select { price }];");
    REQUIRE(project.migrated);
    REQUIRE(project.steps.size() == 1);
    REQUIRE(project.steps.front()->kind() == ir::NodeKind::Project);
    REQUIRE(project.kernel_dispatch.size() == 1);
    REQUIRE(project.kernel_dispatch.front().capability ==
            runtime::MapKernelCapability::MetadataMap);
    REQUIRE(project.kernel_dispatch.front().factory != nullptr);

    const auto [update_tree, update] = serial_plan("trades[update { doubled = price * 2 }];");
    REQUIRE(update.migrated);
    REQUIRE(update.steps.size() == 1);
    REQUIRE(update.steps.front()->kind() == ir::NodeKind::Update);
    REQUIRE(update.kernel_dispatch.size() == 1);
    REQUIRE(update.kernel_dispatch.front().capability ==
            runtime::MapKernelCapability::RowLocalUpdate);
    REQUIRE(update.kernel_dispatch.front().factory != nullptr);
}

TEST_CASE("Physical plan classifies an unregistered scan as LazyScan", "[physical][plan]") {
    // Planning is read-only: a source the registry cannot resolve still has a
    // plan, which is the whole point of inspectability without execution.
    auto ir = require_ir("unknown_src[filter price > 15];");
    const auto plan = runtime::physical::plan_physical(*ir, trades_registry(), nullptr);
    REQUIRE(plan.migrated);
    REQUIRE(plan.source == runtime::physical::SourceKind::LazyScan);
}

TEST_CASE("Column kernel signatures capture storage representation and null policy",
          "[physical][plan]") {
    const runtime::ColumnValue categorical =
        Column<Categorical>{{"A", "B"}, {std::int32_t{0}, std::int32_t{1}}};
    const std::optional<runtime::ValidityBitmap> nullable{runtime::ValidityBitmap{true, false}};

    REQUIRE(runtime::column_kernel_signature(categorical, nullable) ==
            runtime::ColumnKernelSignature{runtime::ColumnRepresentation::CategoricalCodes,
                                           runtime::KernelNullPolicy::Nullable});
}

TEST_CASE("Physical plan declines breakers and grouped updates with reasons", "[physical][plan]") {
    const auto [order_tree, order] = serial_plan("trades[order { price }];");
    REQUIRE_FALSE(order.migrated);
    REQUIRE(order.reason == runtime::physical::FallbackReason::NotMapChain);

    const auto [aggregate_tree, aggregate] =
        serial_plan("trades[select { total = sum(price) }, by { symbol }];");
    REQUIRE_FALSE(aggregate.migrated);
    REQUIRE(aggregate.reason == runtime::physical::FallbackReason::NotMapChain);

    // A grouped update is a barrier even though a bare update is a map: the
    // walk stops at the root, so there are no steps and no source.
    const auto [grouped_tree, grouped] =
        serial_plan("trades[update { total = sum(price) }, by symbol];");
    REQUIRE_FALSE(grouped.migrated);
    REQUIRE(grouped.reason == runtime::physical::FallbackReason::NotMapChain);

    // A map chain whose input is another operator, not a source.
    const auto [over_distinct_tree, over_distinct] = serial_plan(
        "trades[distinct { symbol, price }]"
        "[filter price > 5];");
    REQUIRE(over_distinct.migrated == false);
    REQUIRE(over_distinct.reason == runtime::physical::FallbackReason::NonSourceInput);
    REQUIRE(over_distinct.steps.size() == 1);
    REQUIRE(over_distinct.steps.front()->kind() == ir::NodeKind::Filter);

    // A bare source has no map work to migrate.
    const auto [bare_tree, bare] = serial_plan("trades;");
    REQUIRE_FALSE(bare.migrated);
    REQUIRE(bare.reason == runtime::physical::FallbackReason::EmptyChain);
}

TEST_CASE("explain_physical renders pipelines and fallback reasons", "[physical][explain]") {
    const auto [migrated_tree, migrated] =
        serial_plan("trades[filter price > 15, select { price }];");
    const std::string text = runtime::physical::explain_physical(migrated);
    REQUIRE(text.find("MapPipeline") != std::string::npos);
    REQUIRE(text.find("  FilterProject\n") != std::string::npos);
    REQUIRE(text.find("source: TableScan(trades)") != std::string::npos);
    REQUIRE(text.find("  source signature: fixed-width/all-valid string-slabs/all-valid "
                      "packed-bool/all-valid\n") != std::string::npos);

    const auto [fallback_tree, fallback] = serial_plan("trades[order { price }];");
    const std::string declined = runtime::physical::explain_physical(fallback);
    REQUIRE(declined.find("MaterializedCall(root is not a row-local map)") != std::string::npos);
}

TEST_CASE("Migrated pipelines execute through the physical planner serially",
          "[physical][execute]") {
    const auto registry = trades_registry();
    auto ir = require_ir("trades[filter price > 15, select { price }];");
    const runtime::ExecutionContext exec = serial_exec();

    const auto before = runtime::physical::physical_map_pipelines();
    const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    // The path fired — a pipeline and its pre-planner construction are
    // indistinguishable from the outside, so the counter is the proof.
    REQUIRE(runtime::physical::physical_map_pipelines() > before);

    REQUIRE(result->columns.size() == 1);
    const auto* price = result->find("price");
    REQUIRE(price != nullptr);
    const auto* ints = std::get_if<Column<std::int64_t>>(price);
    REQUIRE(ints != nullptr);
    REQUIRE(ints->size() == 2);
    REQUIRE((*ints)[0] == 20);
    REQUIRE((*ints)[1] == 30);
}

TEST_CASE("Migrated pipelines produce identical results in serial and parallel",
          "[physical][execute]") {
    const auto registry = trades_registry();
    auto ir = require_ir(
        "trades[filter ok, update { doubled = price * 2 }]"
        "[select { doubled }];");

    runtime::ExecutionContext serial = serial_exec();
    runtime::ExecutionContext parallel;  // defaults: parallel on
    parallel.parallel = true;

    const auto serial_result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto parallel_result =
        runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(serial_result.has_value());
    REQUIRE(parallel_result.has_value());

    const auto& s = *serial_result;
    const auto& p = *parallel_result;
    REQUIRE(s.columns.size() == p.columns.size());
    REQUIRE(s.rows() == p.rows());
    const auto* s_col = std::get_if<Column<std::int64_t>>(s.find("doubled"));
    const auto* p_col = std::get_if<Column<std::int64_t>>(p.find("doubled"));
    REQUIRE(s_col != nullptr);
    REQUIRE(p_col != nullptr);
    REQUIRE(s_col->size() == p_col->size());
    for (std::size_t i = 0; i < s_col->size(); ++i) {
        REQUIRE((*s_col)[i] == (*p_col)[i]);
    }
}

TEST_CASE("Migrated filter keeps the empty input's schema carrier", "[physical][execute]") {
    runtime::Table empty;
    empty.add_column("price", Column<std::int64_t>{});
    empty.add_column("symbol", Column<std::string>{});
    runtime::TableRegistry registry;
    registry.emplace("trades", empty);

    auto ir = require_ir("trades[filter price > 15, select { price }];");
    const runtime::ExecutionContext exec = serial_exec();
    const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);
    REQUIRE(result->rows() == 0);
    REQUIRE(result->find("price") != nullptr);
}

TEST_CASE("Fallback queries keep their existing executor under the planner",
          "[physical][execute]") {
    const auto registry = trades_registry();
    auto ir = require_ir("trades[select { total = sum(price) }, by { symbol }];");
    const runtime::ExecutionContext exec = serial_exec();

    const auto before = runtime::physical::physical_materialized_calls();
    const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    REQUIRE(runtime::physical::physical_materialized_calls() > before);

    // Grouped output in first-occurrence order: A then B.
    const auto* symbols = std::get_if<Column<std::string>>(result->find("symbol"));
    const auto* totals = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(symbols != nullptr);
    REQUIRE(totals != nullptr);
    REQUIRE(symbols->size() == 2);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*totals)[0] == 40);
    REQUIRE((*totals)[1] == 20);
}

TEST_CASE("Migrated pipelines handle every column representation", "[physical][execute]") {
    runtime::Table table;
    table.add_column("i", Column<std::int64_t>{1, 2, 3});
    table.add_column("d", Column<double>{1.5, 2.5, 3.5});
    table.add_column("s", Column<std::string>{"x", "y", "z"});
    table.add_column("b", Column<bool>{false, true, true});
    runtime::TableRegistry registry;
    registry.emplace("wide", table);

    auto ir = require_ir("wide[filter b, select { i, d, s, b }];");
    const runtime::ExecutionContext exec = serial_exec();
    const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    REQUIRE(result->columns.size() == 4);
    const auto* i = std::get_if<Column<std::int64_t>>(result->find("i"));
    const auto* d = std::get_if<Column<double>>(result->find("d"));
    const auto* s = std::get_if<Column<std::string>>(result->find("s"));
    const auto* b = std::get_if<Column<bool>>(result->find("b"));
    REQUIRE(i != nullptr);
    REQUIRE(d != nullptr);
    REQUIRE(s != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE((*i)[0] == 2);
    REQUIRE((*i)[1] == 3);
    REQUIRE((*d)[0] == 2.5);
    REQUIRE((*s)[1] == "z");
    REQUIRE((*b)[0]);
}
