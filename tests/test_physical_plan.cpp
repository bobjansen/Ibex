// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
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

/// The plan borrows the IR it was lowered from — keep the tree alive
/// alongside the plan.
auto serial_plan(const char* source) -> std::pair<ir::NodePtr, runtime::physical::Plan> {
    auto ir = require_ir(source);
    auto plan = runtime::physical::plan_physical(*ir, trades_registry(), nullptr);
    return {std::move(ir), std::move(plan)};
}

}  // namespace

TEST_CASE("Physical plan lowers filter+select into a fused map step", "[physical][plan]") {
    // Canonicalize leaves Project(Filter(x)) alone; the planner fuses it into
    // one step, so the plan's step vocabulary still resolves the fused kernel —
    // the plan describes execution, not the shape of the tree.
    const auto [plan_tree, plan] = serial_plan("trades[filter price > 15, select { price }];");
    REQUIRE(plan.migrated);
    REQUIRE(plan.steps.size() == 1);
    REQUIRE(plan.steps.front().node->kind() == ir::NodeKind::Filter);
    REQUIRE(plan.steps.front().fused_project != nullptr);
    REQUIRE(plan.steps.front().capability == runtime::MapKernelCapability::FilterProjectGather);
    REQUIRE(plan.steps.front().factory != nullptr);
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
    REQUIRE(project.steps.front().node->kind() == ir::NodeKind::Project);
    REQUIRE(project.steps.front().capability == runtime::MapKernelCapability::MetadataMap);
    REQUIRE(project.steps.front().factory != nullptr);

    const auto [update_tree, update] = serial_plan("trades[update { doubled = price * 2 }];");
    REQUIRE(update.migrated);
    REQUIRE(update.steps.size() == 1);
    REQUIRE(update.steps.front().node->kind() == ir::NodeKind::Update);
    REQUIRE(update.steps.front().capability == runtime::MapKernelCapability::RowLocalUpdate);
    REQUIRE(update.steps.front().factory != nullptr);
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

    // A bare source has no map work to migrate.
    const auto [bare_tree, bare] = serial_plan("trades;");
    REQUIRE_FALSE(bare.migrated);
    REQUIRE(bare.reason == runtime::physical::FallbackReason::EmptyChain);
}

// A map chain over a breaker is a pipeline whose source is that breaker's
// materialized output -- the relationship a breaker has to the pipeline above
// it -- rather than a reason to decline. The breaker itself keeps the existing
// executor; the plan describes the chain over it.
TEST_CASE("A map chain over a breaker plans as a materialized-input pipeline", "[physical][plan]") {
    const auto [tree, plan] = serial_plan(
        "trades[distinct { symbol, price }]"
        "[filter price > 5];");
    REQUIRE(plan.migrated);
    REQUIRE(plan.source == runtime::physical::SourceKind::MaterializedInput);
    REQUIRE(plan.steps.size() == 1);
    REQUIRE(plan.steps.front().node->kind() == ir::NodeKind::Filter);
    REQUIRE(plan.source_node != nullptr);
    REQUIRE(plan.source_node->kind() == ir::NodeKind::Distinct);
    // No registered-scan signature to prove a representation with, so the
    // filter keeps the compatibility route exactly as it did when this shape
    // was a fallback.
    REQUIRE(plan.source_signature.empty());

    const std::string text = runtime::physical::explain_physical(plan);
    REQUIRE(text.find("source: MaterializedInput(Distinct)") != std::string::npos);
}

// The rules the plan applies when it decides its own mode, over the shapes that
// exercise each one. This case was written as a differential check against the
// island analysis while both existed; with that analysis deleted it states
// the expected verdicts directly, which is what it was proving all along.
// Fusion as a physical choice: a Project directly over a Filter is one gather
// pass whether or not canonicalize rewrote the tree into a FilterProject node.
// The IR here is built by hand precisely because canonicalize R5 would fuse it
// first — this is the shape that reaches the runtime when a caller lowers
// without the full optimizer, and the shape that remains once R5 is retired.
TEST_CASE("The planner fuses a project over a filter", "[physical][plan][fusion]") {
    auto scan = std::make_unique<ir::ScanNode>(ir::NodeId{3}, "trades");
    auto filter = std::make_unique<ir::FilterNode>(
        ir::NodeId{2},
        ir::Expr{.node = ir::CompareExpr{
                     .op = ir::CompareOp::Gt,
                     .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                     .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 0}})}});
    filter->add_child(std::move(scan));
    auto project = std::make_unique<ir::ProjectNode>(
        ir::NodeId{1}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "price"}});
    project->add_child(std::move(filter));
    const ir::NodePtr tree = std::move(project);

    const auto plan = runtime::physical::plan_physical(*tree, trades_registry(), nullptr);
    REQUIRE(plan.migrated);
    // One step, not two: the filter is the step, the project rides on it.
    REQUIRE(plan.steps.size() == 1);
    REQUIRE(plan.steps.front().capability == runtime::MapKernelCapability::FilterProjectGather);
    REQUIRE(plan.steps.front().node->kind() == ir::NodeKind::Filter);
    REQUIRE(plan.steps.front().fused_project != nullptr);
    REQUIRE(plan.steps.front().fused_project->kind() == ir::NodeKind::Project);
    REQUIRE(plan.source == runtime::physical::SourceKind::TableScan);
    REQUIRE(runtime::physical::explain_physical(plan).find("Filter+Project(fused)") !=
            std::string::npos);
}

// The fused step has to execute, not merely plan: same rows, same columns as
// the canonicalized FilterProject the optimizer would have produced.
TEST_CASE("A fused step executes like the fused node", "[physical][execute][fusion]") {
    const auto registry = trades_registry();

    auto scan = std::make_unique<ir::ScanNode>(ir::NodeId{3}, "trades");
    auto filter = std::make_unique<ir::FilterNode>(
        ir::NodeId{2},
        ir::Expr{.node = ir::CompareExpr{
                     .op = ir::CompareOp::Gt,
                     .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                     .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 15}})}});
    filter->add_child(std::move(scan));
    auto project = std::make_unique<ir::ProjectNode>(
        ir::NodeId{1}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "price"}});
    project->add_child(std::move(filter));
    const ir::NodePtr unfused = std::move(project);

    // The reference is the fused IR node itself, built directly: canonicalize
    // no longer produces one, and comparing against it is exactly the
    // equivalence claim -- planner fusion computes what the fused kind did.
    ir::Builder builder;
    auto canonical = builder.filter_project(
        ir::Expr{.node = ir::CompareExpr{.op = ir::CompareOp::Gt,
                                         .left = ir::make_expr_ptr(
                                             ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                                         .right = ir::make_expr_ptr(ir::Expr{
                                             .node = ir::Literal{.value = std::int64_t{15}}})}},
        std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "price"}});
    canonical->add_child(builder.scan("trades"));
    REQUIRE(canonical->kind() == ir::NodeKind::FilterProject);

    for (const bool parallel : {false, true}) {
        INFO("parallel: " << parallel);
        runtime::ExecutionContext exec;
        exec.parallel = parallel;

        const auto fused = runtime::interpret(*unfused, registry, nullptr, nullptr, nullptr, exec);
        const auto reference =
            runtime::interpret(*canonical, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(fused.has_value());
        REQUIRE(reference.has_value());
        REQUIRE(fused->columns.size() == 1);
        REQUIRE(fused->columns.front().name == "price");
        const auto* fused_price = std::get_if<Column<std::int64_t>>(fused->find("price"));
        const auto* reference_price = std::get_if<Column<std::int64_t>>(reference->find("price"));
        REQUIRE(fused_price != nullptr);
        REQUIRE(reference_price != nullptr);
        CHECK(std::vector<std::int64_t>(fused_price->begin(), fused_price->end()) ==
              std::vector<std::int64_t>(reference_price->begin(), reference_price->end()));
        CHECK(std::vector<std::int64_t>(fused_price->begin(), fused_price->end()) ==
              std::vector<std::int64_t>{20, 30});
    }
}

// The three-node shape canonicalize R6 rewrites: one gather pass that filters,
// updates, and projects. Built by hand for the same reason as the two-node
// case — canonicalize would fuse it into a node first.
TEST_CASE("The planner fuses a project over an update over a filter", "[physical][plan][fusion]") {
    const auto build = [] {
        auto scan = std::make_unique<ir::ScanNode>(ir::NodeId{4}, "trades");
        auto filter = std::make_unique<ir::FilterNode>(
            ir::NodeId{3},
            ir::Expr{
                .node = ir::CompareExpr{
                    .op = ir::CompareOp::Gt,
                    .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                    .right = ir::make_expr_ptr(
                        ir::Expr{.node = ir::Literal{.value = std::int64_t{15}}})}});
        filter->add_child(std::move(scan));
        std::vector<ir::FieldSpec> fields;
        fields.push_back(
            {.alias = "doubled",
             .expr = ir::Expr{
                 .node = ir::BinaryExpr{
                     .op = ir::ArithmeticOp::Mul,
                     .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                     .right = ir::make_expr_ptr(
                         ir::Expr{.node = ir::Literal{.value = std::int64_t{2}}})}}});
        auto update = std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(fields));
        update->add_child(std::move(filter));
        auto project = std::make_unique<ir::ProjectNode>(
            ir::NodeId{1}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "doubled"}});
        project->add_child(std::move(update));
        return ir::NodePtr{std::move(project)};
    };
    const ir::NodePtr tree = build();

    const auto registry = trades_registry();
    const auto plan = runtime::physical::plan_physical(*tree, registry, nullptr);
    REQUIRE(plan.migrated);
    REQUIRE(plan.steps.size() == 1);
    REQUIRE(plan.steps.front().capability ==
            runtime::MapKernelCapability::FilterUpdateProjectGather);
    REQUIRE(plan.steps.front().node->kind() == ir::NodeKind::Filter);
    REQUIRE(plan.steps.front().fused_update != nullptr);
    REQUIRE(plan.steps.front().fused_project != nullptr);
    REQUIRE(runtime::physical::explain_physical(plan).find("Filter+Update+Project(fused)") !=
            std::string::npos);

    // And it computes what the fused IR node computes. That node is built
    // directly: canonicalize no longer produces one, and this comparison is
    // exactly the equivalence claim -- planner fusion computes what the fused
    // kind did.
    ir::Builder builder;
    std::vector<ir::FieldSpec> reference_fields;
    reference_fields.push_back(
        {.alias = "doubled",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{2}}})}}});
    auto canonical = builder.filter_update_project(
        ir::Expr{.node = ir::CompareExpr{.op = ir::CompareOp::Gt,
                                         .left = ir::make_expr_ptr(
                                             ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                                         .right = ir::make_expr_ptr(ir::Expr{
                                             .node = ir::Literal{.value = std::int64_t{15}}})}},
        std::move(reference_fields), std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "doubled"}});
    canonical->add_child(builder.scan("trades"));
    REQUIRE(canonical->kind() == ir::NodeKind::FilterUpdateProject);
    for (const bool parallel : {false, true}) {
        INFO("parallel: " << parallel);
        runtime::ExecutionContext exec;
        exec.parallel = parallel;
        const auto fused = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, exec);
        const auto reference =
            runtime::interpret(*canonical, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(fused.has_value());
        REQUIRE(reference.has_value());
        const auto* fused_doubled = std::get_if<Column<std::int64_t>>(fused->find("doubled"));
        const auto* reference_doubled =
            std::get_if<Column<std::int64_t>>(reference->find("doubled"));
        REQUIRE(fused_doubled != nullptr);
        REQUIRE(reference_doubled != nullptr);
        CHECK(fused->columns.size() == reference->columns.size());
        CHECK(std::vector<std::int64_t>(fused_doubled->begin(), fused_doubled->end()) ==
              std::vector<std::int64_t>(reference_doubled->begin(), reference_doubled->end()));
        CHECK(std::vector<std::int64_t>(fused_doubled->begin(), fused_doubled->end()) ==
              std::vector<std::int64_t>{40, 60});
    }
}

// The fused step through real morsels, which is where fusion could go wrong
// without the answer changing shape: `range_filter_head` absorbs a fused
// project into the morsel source exactly as it absorbs a FilterProject node's
// column list, and the ordered merger reassembles the pieces.
TEST_CASE("A fused step runs over morsels", "[physical][execute][fusion][parallel]") {
    constexpr std::size_t kRows = 40'000;
    Column<std::int64_t> price;
    Column<std::string> symbol;
    for (std::size_t r = 0; r < kRows; ++r) {
        price.push_back(static_cast<std::int64_t>(r));
        symbol.push_back(r % 2 == 0 ? "A" : "B");
    }
    runtime::Table table;
    table.add_column("price", std::move(price));
    table.add_column("symbol", std::move(symbol));
    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    const auto build = []() {
        auto scan = std::make_unique<ir::ScanNode>(ir::NodeId{3}, "trades");
        auto filter = std::make_unique<ir::FilterNode>(
            ir::NodeId{2},
            ir::Expr{
                .node = ir::CompareExpr{
                    .op = ir::CompareOp::Gt,
                    .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                    .right = ir::make_expr_ptr(
                        ir::Expr{.node = ir::Literal{.value = std::int64_t{19'999}}})}});
        filter->add_child(std::move(scan));
        auto project = std::make_unique<ir::ProjectNode>(
            ir::NodeId{1}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "price"}});
        project->add_child(std::move(filter));
        return ir::NodePtr{std::move(project)};
    };
    const ir::NodePtr tree = build();

    runtime::ExecutionContext serial;
    serial.parallel = false;
    runtime::ParallelIslandStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel = true;
    parallel.parallel_threads = 4;
    parallel.parallel_min_rows = 0;
    parallel.parallel_min_cells = 0;
    parallel.parallel_grain = 4'096;
    parallel.parallel_stats = &stats;

    const auto a = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, serial);
    const auto b = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(a.has_value());
    REQUIRE(b.has_value());
    // The projection really was applied per morsel, not forgotten by one of
    // them: one column out, and the same rows either way.
    REQUIRE(a->columns.size() == 1);
    REQUIRE(b->columns.size() == 1);
    REQUIRE(a->rows() == kRows - 20'000);
    const auto* serial_price = std::get_if<Column<std::int64_t>>(a->find("price"));
    const auto* parallel_price = std::get_if<Column<std::int64_t>>(b->find("price"));
    REQUIRE(serial_price != nullptr);
    REQUIRE(parallel_price != nullptr);
    CHECK(std::vector<std::int64_t>(serial_price->begin(), serial_price->end()) ==
          std::vector<std::int64_t>(parallel_price->begin(), parallel_price->end()));
    // It took the parallel path rather than quietly falling back.
    CHECK(stats.parallel_islands.load() == 1);
}

TEST_CASE("Pipeline mode applies the parallel-map rules", "[physical][plan][parallel]") {
    using runtime::physical::PipelineMode;
    using runtime::physical::SerialOnlyReason;

    struct Case {
        const char* source;
        bool parallel;
        std::size_t parallel_steps;
        SerialOnlyReason reason;
    };
    const Case cases[] = {
        {"trades[filter price > 5];", true, 1, SerialOnlyReason::None},
        {"trades[filter price > 15, select { price }];", true, 1, SerialOnlyReason::None},
        {"trades[filter price > 5][rename { p = price }];", true, 2, SerialOnlyReason::None},
        // Over a breaker: a pipeline since 32f62261, and one that may run over
        // morsels — the breaker's output is materialized either way.
        {"trades[distinct { symbol, price }][filter price > 5];", true, 1, SerialOnlyReason::None},
        {"trades[select { total = sum(price) }, by { symbol }][filter total > 5];", true, 1,
         SerialOnlyReason::None},
        // Metadata-only: nothing per-row to spread.
        {"trades[select { price }];", false, 0, SerialOnlyReason::NoRowWork},
        {"trades[rename { p = price }];", false, 0, SerialOnlyReason::NoRowWork},
        // A bare row-local update is a map step and deliberately not a parallel
        // one, so it bounds the prefix instead of joining it.
        {"trades[update { p2 = price * 2 }];", false, 0, SerialOnlyReason::NotParallelMap},
        {"trades[update { p2 = price * 2 }][filter p2 > 5];", true, 1, SerialOnlyReason::None},
    };

    for (const auto& test : cases) {
        INFO(test.source);
        const auto [tree, plan] = serial_plan(test.source);
        REQUIRE(plan.migrated);
        REQUIRE((plan.mode == PipelineMode::MorselParallel) == test.parallel);
        REQUIRE(plan.parallel_steps == test.parallel_steps);
        REQUIRE(plan.serial_reason == test.reason);
        REQUIRE((runtime::physical::parallel_input_node(plan) != nullptr) == test.parallel);
    }

    // Roots that are no pipeline at all never claim a mode.
    for (const char* source : {"trades[order { price }];", "trades;"}) {
        INFO(source);
        const auto [tree, plan] = serial_plan(source);
        REQUIRE_FALSE(plan.migrated);
        REQUIRE(plan.mode == PipelineMode::Serial);
    }
}

TEST_CASE("A pipeline's mode names the step that bounds it", "[physical][plan][parallel]") {
    const auto [tree, plan] = serial_plan("trades[update { p2 = price * 2 }][filter p2 > 5];");
    REQUIRE(plan.migrated);
    REQUIRE(plan.mode == runtime::physical::PipelineMode::MorselParallel);
    REQUIRE(plan.steps.size() == 2);
    // Only the filter may run over morsels; the update below it is the
    // boundary, and executes serially as it does today.
    REQUIRE(plan.parallel_steps == 1);
    REQUIRE(plan.steps.front().node->kind() == ir::NodeKind::Filter);
    const ir::Node* input = runtime::physical::parallel_input_node(plan);
    REQUIRE(input != nullptr);
    REQUIRE(input->kind() == ir::NodeKind::Update);

    const auto [metadata_tree, metadata] = serial_plan("trades[select { price }];");
    REQUIRE(metadata.migrated);
    REQUIRE(metadata.mode == runtime::physical::PipelineMode::Serial);
    REQUIRE(metadata.serial_reason == runtime::physical::SerialOnlyReason::NoRowWork);
    REQUIRE(runtime::physical::parallel_input_node(metadata) == nullptr);
    REQUIRE(runtime::physical::explain_physical(metadata).find(
                "mode: serial(metadata-only chain, no per-row work)") != std::string::npos);
}

TEST_CASE("explain_physical renders pipelines and fallback reasons", "[physical][explain]") {
    const auto [migrated_tree, migrated] =
        serial_plan("trades[filter price > 15, select { price }];");
    const std::string text = runtime::physical::explain_physical(migrated);
    REQUIRE(text.find("MapPipeline") != std::string::npos);
    REQUIRE(text.find("  Filter+Project(fused)\n") != std::string::npos);
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

// The execution-level half of the materialized-input plan: the chain runs
// through the physical planner, and the breaker below it is built by the
// existing executor as it always was.
TEST_CASE("A pipeline over a breaker executes through the physical planner",
          "[physical][execute]") {
    const auto registry = trades_registry();
    auto ir = require_ir(
        "trades[distinct { symbol, price }]"
        "[filter price > 5];");
    const runtime::ExecutionContext exec = serial_exec();

    const auto before = runtime::physical::physical_map_pipelines();
    const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    REQUIRE(runtime::physical::physical_map_pipelines() > before);
    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE(std::ranges::all_of(*prices, [](std::int64_t price) { return price > 5; }));
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
