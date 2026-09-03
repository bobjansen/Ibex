// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
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
    exec.parallel_threads = 1;
    return exec;
}

/// The plan borrows the IR it was lowered from — keep the tree alive
/// alongside the plan.
auto serial_plan(const char* source) -> std::pair<ir::NodePtr, runtime::physical::Plan> {
    auto ir = require_ir(source);
    auto plan = runtime::physical::plan_physical(*ir, trades_registry(), nullptr);
    return {std::move(ir), std::move(plan)};
}

auto execute_physical_plan(const runtime::physical::Plan& plan, const ir::Node& root,
                           const runtime::TableRegistry& registry,
                           const runtime::ExecutionContext& exec)
    -> std::expected<runtime::Table, std::string> {
    auto op = runtime::build_operator_from_physical_plan(plan, root, registry, nullptr, nullptr,
                                                         exec, nullptr);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    return runtime::materialize_operator(std::move(*op));
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
    // Order is migrated now -- `build_physical_order` builds it -- so the
    // breaker standing in for "declines" here is one that still does.
    const auto [order_tree, order] = serial_plan("trades[order { price }];");
    REQUIRE(order.migrated);

    // `melt` is the declining stand-in, chosen because it is still unported --
    // not because it was convenient. Order, Head, Distinct and the Head/Tail/TopK
    // family each played this role and each stopped declining when it was ported,
    // breaking these tests in turn. When `melt` is ported this must move again;
    // pick the next one the same way, from what the migration has not reached.
    const auto [melt_tree, melt] = serial_plan("trades[melt symbol];");
    REQUIRE_FALSE(melt.migrated);
    REQUIRE(melt.reason == runtime::physical::FallbackReason::NotMapChain);

    // Distinct is migrated now, like Order above.
    const auto [distinct_tree, distinct] = serial_plan("trades[distinct symbol];");
    REQUIRE(distinct.migrated);

    // A streamable aggregate is migrated now -- `build_physical_aggregate`
    // builds it -- so the breaker that still declines is one needing every
    // value at once.
    const auto [streaming_tree, streaming] =
        serial_plan("trades[select { total = sum(price) }, by { symbol }];");
    REQUIRE(streaming.migrated);
    REQUIRE(streaming.aggregate.strategy == runtime::physical::AggregateStrategy::StreamingSorted);

    const auto [aggregate_tree, aggregate] =
        serial_plan("trades[select { total = median(price) }, by { symbol }];");
    REQUIRE_FALSE(aggregate.migrated);
    REQUIRE(aggregate.reason == runtime::physical::FallbackReason::NotMapChain);
    REQUIRE(aggregate.aggregate.strategy == runtime::physical::AggregateStrategy::MaterializeAll);

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
// pipeline analysis while both existed; with that analysis deleted it states
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
    REQUIRE(plan.steps.front().filter_predicate != nullptr);
    REQUIRE(plan.steps.front().project_columns != nullptr);
    REQUIRE(plan.source == runtime::physical::SourceKind::TableScan);
    REQUIRE(runtime::physical::explain_physical(plan).find("Filter+Project(fused)") !=
            std::string::npos);
}

TEST_CASE("A fused step executes correctly", "[physical][execute][fusion]") {
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

    for (const bool parallel : {false, true}) {
        INFO("parallel: " << parallel);
        runtime::ExecutionContext exec;
        exec.parallel_threads = (parallel) ? 0 : 1;

        const auto fused = runtime::interpret(*unfused, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(fused.has_value());
        REQUIRE(fused->columns.size() == 1);
        REQUIRE(fused->columns.front().name == "price");
        const auto* fused_price = std::get_if<Column<std::int64_t>>(fused->find("price"));
        REQUIRE(fused_price != nullptr);
        CHECK(std::vector<std::int64_t>(fused_price->begin(), fused_price->end()) ==
              std::vector<std::int64_t>{20, 30});
    }
}

// One gather pass filters, updates, and projects.
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
    REQUIRE(plan.steps.front().filter_predicate != nullptr);
    REQUIRE(plan.steps.front().update_fields != nullptr);
    REQUIRE(plan.steps.front().project_columns != nullptr);
    REQUIRE(runtime::physical::explain_physical(plan).find("Filter+Update+Project(fused)") !=
            std::string::npos);

    for (const bool parallel : {false, true}) {
        INFO("parallel: " << parallel);
        runtime::ExecutionContext exec;
        exec.parallel_threads = (parallel) ? 0 : 1;
        const auto fused = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(fused.has_value());
        const auto* fused_doubled = std::get_if<Column<std::int64_t>>(fused->find("doubled"));
        REQUIRE(fused_doubled != nullptr);
        CHECK(std::vector<std::int64_t>(fused_doubled->begin(), fused_doubled->end()) ==
              std::vector<std::int64_t>{40, 60});
    }
}

// A run below a serial step has to actually reach the workers, not merely be
// described. `df[filter ...][update ...]` puts the update above the run: the
// filter's morsels fan out, the ordered merger reassembles them, and the update
// runs once over the finished table.
TEST_CASE("A parallel run below a serial step still reaches the workers",
          "[physical][execute][parallel]") {
    constexpr std::size_t kRows = 40'000;
    Column<std::int64_t> price;
    for (std::size_t r = 0; r < kRows; ++r) {
        price.push_back(static_cast<std::int64_t>(r));
    }
    runtime::Table table;
    table.add_column("price", std::move(price));
    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 19999][update { doubled = price * 2 }];");
    const auto plan = runtime::physical::plan_physical(*ir, registry, nullptr);
    REQUIRE(plan.migrated);
    REQUIRE(plan.mode == runtime::physical::PipelineMode::MorselParallel);
    REQUIRE(plan.parallel_begin == 1);

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 4;
    parallel.parallel_min_rows = 0;
    parallel.parallel_min_cells = 0;
    parallel.parallel_grain = 4'096;
    parallel.parallel_stats = &stats;

    const auto a = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto b = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(a.has_value());
    REQUIRE(b.has_value());
    REQUIRE(a->rows() == kRows - 20'000);
    const auto* serial_doubled = std::get_if<Column<std::int64_t>>(a->find("doubled"));
    const auto* parallel_doubled = std::get_if<Column<std::int64_t>>(b->find("doubled"));
    REQUIRE(serial_doubled != nullptr);
    REQUIRE(parallel_doubled != nullptr);
    CHECK(std::vector<std::int64_t>(serial_doubled->begin(), serial_doubled->end()) ==
          std::vector<std::int64_t>(parallel_doubled->begin(), parallel_doubled->end()));
    // The filter below the update fanned out; a silent serial fallback would
    // leave this at zero.
    CHECK(stats.parallel_pipelines.load() == 1);
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
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
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
    CHECK(stats.parallel_pipelines.load() == 1);
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
        {.source = "trades[filter price > 5];",
         .parallel = true,
         .parallel_steps = 1,
         .reason = SerialOnlyReason::None},
        {.source = "trades[filter price > 15, select { price }];",
         .parallel = true,
         .parallel_steps = 1,
         .reason = SerialOnlyReason::None},
        {.source = "trades[filter price > 5][rename { p = price }];",
         .parallel = true,
         .parallel_steps = 2,
         .reason = SerialOnlyReason::None},
        // Over a breaker: a pipeline since 32f62261, and one that may run over
        // morsels — the breaker's output is materialized either way.
        {.source = "trades[distinct { symbol, price }][filter price > 5];",
         .parallel = true,
         .parallel_steps = 1,
         .reason = SerialOnlyReason::None},
        {.source = "trades[select { total = sum(price) }, by { symbol }][filter total > 5];",
         .parallel = true,
         .parallel_steps = 1,
         .reason = SerialOnlyReason::None},
        // Metadata-only: nothing per-row to spread.
        {.source = "trades[select { price }];",
         .parallel = false,
         .parallel_steps = 0,
         .reason = SerialOnlyReason::NoRowWork},
        {.source = "trades[rename { p = price }];",
         .parallel = false,
         .parallel_steps = 0,
         .reason = SerialOnlyReason::NoRowWork},
        // A bare row-local update is a map step and deliberately not a parallel
        // one, so it bounds the prefix instead of joining it.
        {.source = "trades[update { p2 = price * 2 }];",
         .parallel = false,
         .parallel_steps = 0,
         .reason = SerialOnlyReason::NotParallelMap},
        {.source = "trades[update { p2 = price * 2 }][filter p2 > 5];",
         .parallel = true,
         .parallel_steps = 1,
         .reason = SerialOnlyReason::None},
    };

    for (const auto& test : cases) {
        INFO(test.source);
        const auto [tree, plan] = serial_plan(test.source);
        REQUIRE(plan.migrated);
        REQUIRE((plan.mode == PipelineMode::MorselParallel) == test.parallel);
        REQUIRE(plan.parallel_step_count() == test.parallel_steps);
        REQUIRE(plan.serial_reason == test.reason);
        REQUIRE((runtime::physical::parallel_input_node(plan) != nullptr) == test.parallel);
    }

    // Roots that are no pipeline at all never claim a mode. A migrated
    // single-operator breaker (Tail) and an unmigrated one (melt) both qualify:
    // neither has `steps`, so `mode` stays `Serial` regardless.
    for (const char* source : {"trades[tail 2];", "trades[melt symbol];", "trades;"}) {
        INFO(source);
        const auto [tree, plan] = serial_plan(source);
        REQUIRE(plan.steps.empty());
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
    REQUIRE(plan.parallel_step_count() == 1);
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

    const auto [fallback_tree, fallback] = serial_plan("trades[melt symbol];");
    const std::string declined = runtime::physical::explain_physical(fallback);
    REQUIRE(declined.find("MaterializedCall(Melt: root is not a row-local map)") !=
            std::string::npos);

    // A migrated single-operator breaker renders as a Breaker line, not a
    // pipeline and not a MaterializedCall.
    const auto [tail_tree, tail] = serial_plan("trades[tail 2];");
    const std::string tail_text = runtime::physical::explain_physical(tail);
    REQUIRE(tail_text.find("Breaker(Tail)") != std::string::npos);
    REQUIRE(tail_text.find("serial (single-operator breaker, no fan-out point)") !=
            std::string::npos);
    REQUIRE(tail_text.find("MapPipeline") == std::string::npos);
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

    const runtime::ExecutionContext serial = serial_exec();
    const runtime::ExecutionContext parallel;  // defaults: parallel on

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

TEST_CASE("The plan classifies a join and builds the streaming ones", "[physical][join]") {
    // Phase 4 item 1. The plan classifies every join, and now BUILDS the
    // streaming ones -- `build_physical_join` owns their construction, so they
    // leave the migration backlog (Join 59 -> 6 across PDS-H; the 6 are the
    // materializing joins, which still fall back and still count).
    //
    // The classification was proven equal to the builder's own branches by a
    // temporary assertion at the seam, across the full suite and all 22 PDS-H
    // queries, before any of it was allowed to route anything.
    using runtime::physical::JoinBranch;
    using runtime::physical::JoinDeclineReason;
    using runtime::physical::JoinStrategy;

    const auto plan_of = [](const char* src) {
        auto ir = require_ir(src);
        REQUIRE(ir->kind() == ir::NodeKind::Join);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return runtime::physical::plan_join(static_cast<const ir::JoinNode&>(*ir));
    };

    SECTION("a single-key inner join streams, right side builds") {
        auto ir = require_ir("(a join b on k);");
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto plan = runtime::physical::plan_join(static_cast<const ir::JoinNode&>(*ir));
        CHECK(plan.describes);
        CHECK(plan.strategy == JoinStrategy::StreamingProbe);
        CHECK(plan.decline == JoinDeclineReason::None);
        CHECK(plan.key_count == 1);
        // The plan names the two inputs in textual order and says nothing
        // about which one is hashed: that is resolved by the build phase at
        // run time, so a plan asserting it would be false every time the
        // operator swaps. Compared against this node's own children -- a
        // second `require_ir` would build a different tree whose pointers
        // could never match.
        REQUIRE(ir->children().size() == 2);
        CHECK(plan.left_input == ir->children()[0].get());
        CHECK(plan.right_input == ir->children()[1].get());
    }

    SECTION("two Int64 keys stream as the pair shape when the schema proves it") {
        // The classification that the first version of this planner got wrong.
        // It declined every two-key join, while the builder streams the Int64
        // pair through `is_streamable_pair_int_join` -- so the plan claimed
        // `MaterializeBoth` for a join that streams, and consuming that would
        // have rerouted q09. Ascriptions make the key types provable; without
        // them the same query declines, which the section above pins.
        const auto plan = plan_of(
            "(a as DataFrame<{ k1: Int64, k2: Int64 }> "
            " join b as DataFrame<{ k1: Int64, k2: Int64 }> on { k1, k2 });");
        CHECK(plan.strategy == JoinStrategy::StreamingProbe);
        CHECK(plan.branch == JoinBranch::PairIntInner);
        CHECK(plan.decline == JoinDeclineReason::None);
        CHECK(plan.key_count == 2);
    }

    SECTION("semi and anti stream on the same terms") {
        CHECK(plan_of("(a semi join b on k);").branch == JoinBranch::SemiAnti);
        CHECK(plan_of("(a anti join b on k);").branch == JoinBranch::SemiAnti);
    }

    SECTION("the branch is named, so the seam never infers it from key count") {
        // The seam dispatches on this. It used to deduce the operator from
        // `key_count == 1` vs `== 2`, which held only while StreamingProbe
        // implied a full gate had passed -- a coupling nothing would have
        // reported if a later gate broke it.
        CHECK(plan_of("(a join b on k);").branch == JoinBranch::SingleKeyInner);
        CHECK(plan_of("(a left join b on k);").branch == JoinBranch::None);
        // None exactly when both sides materialize, in both directions.
        for (const char* src : {"(a join b on k);", "(a semi join b on k);",
                                "(a left join b on k);", "(a join b on x < y);"}) {
            CAPTURE(src);
            const auto plan = plan_of(src);
            CHECK((plan.branch == JoinBranch::None) ==
                  (plan.strategy == JoinStrategy::MaterializeBoth));
        }
    }

    SECTION("every decline is named, not a bare conjunction") {
        CHECK(plan_of("(a left join b on k);").decline == JoinDeclineReason::UnsupportedKind);
        CHECK(plan_of("(a join b on x < y);").decline == JoinDeclineReason::HasPredicate);
        // Two keys decline for a schema reason, not a count one: the pair path
        // takes exactly two keys, but only when both are provably Int64 on both
        // sides. With no schema here, they are not provable.
        CHECK(plan_of("(a join b on { k1, k2 });").decline ==
              JoinDeclineReason::KeyTypesUnsupported);
        CHECK(plan_of("(a join b on { k1, k2, k3 });").decline == JoinDeclineReason::MultipleKeys);
        CHECK(plan_of("(a join b on k nulls equal);").decline == JoinDeclineReason::NullsEqual);
        for (const char* src :
             {"(a left join b on k);", "(a join b on x < y);", "(a join b on { k1, k2 });",
              "(a join b on { k1, k2, k3 });", "(a join b on k nulls equal);"}) {
            CAPTURE(src);
            const auto plan = plan_of(src);
            CHECK(plan.strategy == JoinStrategy::MaterializeBoth);
            // A declined join names no inputs: the materializing path has no
            // build/probe shape at all, so naming sides would suggest one.
            CHECK(plan.left_input == nullptr);
            CHECK(plan.right_input == nullptr);
        }
    }

    SECTION("a streaming join is a migrated plan; a materializing one is not") {
        const runtime::TableRegistry empty;
        auto streaming = require_ir("(a join b on k);");
        const auto sp = runtime::physical::plan_physical(*streaming, empty, nullptr);
        // `build_physical_join` builds this one, not the per-kind switch, which
        // is what takes it out of the migration backlog.
        CHECK(sp.migrated);
        CHECK(sp.join.describes);
        CHECK(sp.join.branch == JoinBranch::SingleKeyInner);

        auto materializing = require_ir("(a left join b on k);");
        const auto mp = runtime::physical::plan_physical(*materializing, empty, nullptr);
        // Still a fallback, and still counted as one. If this became migrated
        // too the backlog would be measuring the label rather than the port.
        CHECK_FALSE(mp.migrated);
        CHECK(mp.join.describes);
        const std::string text = runtime::physical::explain_physical(mp);
        CHECK(text.find("MaterializedCall") != std::string::npos);
        CHECK(text.find("Join(MaterializeBoth") != std::string::npos);
    }
}

TEST_CASE("The plan classifies an aggregate by relaying the builder's predicates",
          "[physical][aggregate]") {
    // Phase 4 item 2, step 2. Every field is relayed from
    // `plan_fused_left_join_count` and `aggregate_is_streamable` -- the same
    // calls the builder makes -- so there is no independent judgement here to
    // be wrong. A temporary assertion at the seam compared all three outcomes,
    // the fused join identity, and the negative case across the full suite and
    // all 22 PDS-H queries with zero disagreements.
    using runtime::physical::AggregateStrategy;

    const auto plan_of = [](const char* src) {
        auto ir = require_ir(src);
        REQUIRE(ir->kind() == ir::NodeKind::Aggregate);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        return runtime::physical::plan_aggregate(static_cast<const ir::AggregateNode&>(*ir));
    };

    SECTION("incremental aggregations stream") {
        for (const char* src :
             {"t[select { n = count() }, by k];", "t[select { s = sum(v) }, by k];",
              "t[select { a = mean(v), d = std(v) }, by k];",
              "t[select { f = first(v), l = last(v) }, by k];"}) {
            CAPTURE(src);
            CHECK(plan_of(src).strategy == AggregateStrategy::StreamingSorted);
        }
    }

    SECTION("aggregations needing every value materialize") {
        // Median and quantile cannot be combined from partials; ewma is
        // row-order coupled.
        for (const char* src :
             {"t[select { m = median(v) }, by k];", "t[select { q = quantile(v, 0.9) }, by k];"}) {
            CAPTURE(src);
            CHECK(plan_of(src).strategy == AggregateStrategy::MaterializeAll);
        }
    }

    SECTION("one unstreamable aggregation makes the whole node materialize") {
        // The predicate is all-of, not any-of: partials for the streamable
        // columns would not help if one column still needs every value.
        CHECK(plan_of("t[select { s = sum(v), m = median(v) }, by k];").strategy ==
              AggregateStrategy::MaterializeAll);
    }

    SECTION("the join+aggregate fusion names the join it consumes") {
        // Two logical nodes, one physical step. `fused_join` is what makes that
        // a property of the plan rather than a walk each builder repeats.
        auto ir = require_ir("(l left join r on k)[select { n = count(v) }, by k];");
        REQUIRE(ir->kind() == ir::NodeKind::Aggregate);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto plan =
            runtime::physical::plan_aggregate(static_cast<const ir::AggregateNode&>(*ir));
        CHECK(plan.strategy == AggregateStrategy::FusedLeftJoinCount);
        REQUIRE(plan.fused_join != nullptr);
        CHECK(plan.fused_join->kind() == ir::NodeKind::Join);
        CHECK_FALSE(plan.counted_column.empty());
    }

    SECTION("an aggregate over a plain child names no fused join") {
        const auto plan = plan_of("t[select { s = sum(v) }, by k];");
        CHECK(plan.fused_join == nullptr);
        CHECK(plan.counted_column.empty());
    }
}

TEST_CASE("The fallback backlog is counted by kind, in every mode", "[physical][execute]") {
    // The backlog is what orders Phase 4, so it has to be trustworthy in two
    // ways this pins down.
    //
    // 1. It attributes to the NODE KIND, not just a total. A bare count cannot
    //    say whether to port the join or the aggregate first.
    // 2. It counts the same in both execution modes. The call used to sit
    //    behind `if (!exec.can_fan_out())`, so at two cores or more the backlog
    //    read as empty -- a migration counter reporting nothing on the
    //    configuration everything is measured on. Equal deltas here are also
    //    what rules out double counting now that the gate is gone: the seam
    //    visits each node once per build, whatever the budget.
    const auto registry = trades_registry();
    // Median still falls back -- it needs every value at once -- so it still
    // counts. A `sum` would count nothing now that streaming aggregates build
    // from the plan, which is exactly what the backlog is meant to track.
    auto ir = require_ir("trades[select { total = median(price) }, by { symbol }];");

    const auto measure = [&](std::size_t threads) {
        runtime::ExecutionContext exec;
        exec.parallel_threads = threads;
        const auto before = runtime::physical::physical_fallbacks_for(ir::NodeKind::Aggregate);
        const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        return runtime::physical::physical_fallbacks_for(ir::NodeKind::Aggregate) - before;
    };

    const std::uint64_t serial_delta = measure(1);
    const std::uint64_t parallel_delta = measure(4);
    CHECK(serial_delta >= 1);
    CHECK(serial_delta == parallel_delta);

    // A kind the query does not contain must not move, or the attribution is
    // recording something other than the node it names.
    const auto joins_before = runtime::physical::physical_fallbacks_for(ir::NodeKind::Join);
    (void)measure(1);
    CHECK(runtime::physical::physical_fallbacks_for(ir::NodeKind::Join) == joins_before);

    CHECK(runtime::physical::node_kind_name(ir::NodeKind::Aggregate) == "Aggregate");
    CHECK(runtime::physical::node_kind_name(ir::NodeKind::Scan) == "Scan");
}

TEST_CASE("A migrated aggregate executes through the plan and still answers right",
          "[physical][execute]") {
    const auto registry = trades_registry();
    auto ir = require_ir("trades[select { total = sum(price) }, by { symbol }];");
    const runtime::ExecutionContext exec = serial_exec();

    // This used to assert the opposite -- that the query raised the fallback
    // counter -- and it kept passing after `sum` became migrated only because
    // that counter is process-global and other tests in the same binary bump
    // it. A test that no longer tests its own premise, passing quietly. The
    // pipeline counter is the one this query now moves.
    const auto before = runtime::physical::physical_map_pipelines();
    const auto result = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    REQUIRE(runtime::physical::physical_map_pipelines() > before);

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

TEST_CASE("The plan describes the distinct dedup fan-out phase", "[physical][breaker]") {
    SECTION("one phase, named, with the packed-key policy") {
        const auto [tree, plan] = serial_plan("trades[distinct { price, symbol }];");
        REQUIRE(plan.migrated);
        REQUIRE(plan.breaker_phases.size() == 1);
        const auto& phase = plan.breaker_phases.front();
        REQUIRE(phase.name == "dedup");
        REQUIRE(phase.parallelism.row_floor == (1U << 15U));
        REQUIRE(phase.parallelism.breaker_max_workers == 64);
        REQUIRE(phase.parallelism.strategy == runtime::physical::PartitionStrategy::PackedKey);
        // Not resolved yet — no ExecutionContext ran here.
        REQUIRE(phase.parallelism.worker_cap == 0);
    }

    SECTION("a bare registered scan gets an exact table row estimate") {
        const auto [tree, plan] = serial_plan("trades[distinct { price }];");
        REQUIRE(plan.breaker_phases.size() == 1);
        const auto& est = plan.breaker_phases.front().parallelism.estimate;
        REQUIRE(est.source == runtime::physical::RowEstimate::Source::TableExact);
        REQUIRE(est.rows == 3);
    }

    SECTION("a filter under the distinct makes the count unknowable — no estimate") {
        const auto [tree, plan] = serial_plan("trades[filter price > 5][distinct { price }];");
        REQUIRE(plan.breaker_phases.size() == 1);
        REQUIRE(plan.breaker_phases.front().parallelism.estimate.source ==
                runtime::physical::RowEstimate::Source::None);
    }

    SECTION("explain physical prints the phase") {
        const auto [tree, plan] = serial_plan("trades[distinct { price, symbol }];");
        const std::string text = runtime::physical::explain_physical(plan);
        REQUIRE(text.find("Breaker(Distinct)") != std::string::npos);
        REQUIRE(text.find("dedup:") != std::string::npos);
        REQUIRE(text.find("floor 32768") != std::string::npos);
        REQUIRE(text.find("packed-key") != std::string::npos);
    }

    SECTION("distinct_dedup_parallelism is the one definition of the policy") {
        const auto bp = runtime::physical::distinct_dedup_parallelism(
            {.rows = 999, .source = runtime::physical::RowEstimate::Source::ChildExact});
        REQUIRE(bp.row_floor == (1U << 15U));
        REQUIRE(bp.breaker_max_workers == 64);
        REQUIRE(bp.strategy == runtime::physical::PartitionStrategy::PackedKey);
        REQUIRE(bp.estimate.rows == 999);
    }
}

TEST_CASE("The distinct operator reads the plan: parallel output equals serial",
          "[physical][breaker][execute]") {
    // A table over the 32768 fan-out floor, with heavy key repetition so the
    // parallel packed-key partitions and the serial set must agree on both
    // which rows survive and their first-occurrence order.
    constexpr std::int64_t kRows = 60000;
    runtime::Table table;
    {
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        for (std::int64_t r = 0; r < kRows; ++r) {
            a.push_back(r % 137);
            b.push_back(r % 89);
        }
        table.add_column("a", std::move(a));
        table.add_column("b", std::move(b));
    }
    runtime::TableRegistry registry;
    registry.emplace("big", table);

    auto ir = require_ir("big[distinct { a, b }];");

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;

    const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(s->rows() == p->rows());
    const auto& sa = std::get<Column<std::int64_t>>(*s->find_entry("a")->column);
    const auto& pa = std::get<Column<std::int64_t>>(*p->find_entry("a")->column);
    const auto& sb = std::get<Column<std::int64_t>>(*s->find_entry("b")->column);
    const auto& pb = std::get<Column<std::int64_t>>(*p->find_entry("b")->column);
    for (std::size_t i = 0; i < s->rows(); ++i) {
        REQUIRE(sa[i] == pa[i]);
        REQUIRE(sb[i] == pb[i]);
    }
}

TEST_CASE("resolve_breaker_parallelism is the one place the worker cap is computed",
          "[physical][breaker]") {
    using runtime::physical::BreakerParallelism;
    using runtime::physical::FanOutDecline;
    using runtime::physical::resolve_breaker_parallelism;
    using runtime::physical::RowEstimate;

    SECTION("single core declines") {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 1;
        BreakerParallelism bp{.row_floor = 32768, .breaker_max_workers = 64};
        resolve_breaker_parallelism(bp, exec, 8);
        REQUIRE(bp.decline == FanOutDecline::SingleCore);
        REQUIRE(bp.worker_cap == 1);
    }

    SECTION("a confident estimate under the floor declines") {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        BreakerParallelism bp{.row_floor = 32768,
                              .breaker_max_workers = 64,
                              .estimate = {.rows = 100, .source = RowEstimate::Source::Footer}};
        resolve_breaker_parallelism(bp, exec, 8);
        REQUIRE(bp.decline == FanOutDecline::BelowFloor);
        REQUIRE(bp.worker_cap == 1);
    }

    SECTION("the cap is the min of budget, pool, and the breaker's own ceiling") {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 6;
        BreakerParallelism bp{.row_floor = 32768, .breaker_max_workers = 4};
        resolve_breaker_parallelism(bp, exec, 8);
        REQUIRE(bp.decline == FanOutDecline::None);
        REQUIRE(bp.worker_cap == 4);  // min(6, 8, 4)
    }

    SECTION("a phase with no floor of its own inherits the shared parallel_min_rows") {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.parallel_min_rows = 65536;
        BreakerParallelism bp{};  // row_floor 0
        resolve_breaker_parallelism(bp, exec, 8);
        REQUIRE(bp.row_floor == 65536);
    }
}

TEST_CASE("The plan describes order's sort fan-out and explain physical is not silent",
          "[physical][breaker]") {
    SECTION("one 'sort' phase, row-range, no floor of its own") {
        const auto [tree, plan] = serial_plan("trades[order { price }];");
        REQUIRE(plan.migrated);
        REQUIRE(plan.breaker_phases.size() == 1);
        const auto& phase = plan.breaker_phases.front();
        REQUIRE(phase.name == "sort");
        REQUIRE(phase.parallelism.strategy == runtime::physical::PartitionStrategy::RowRange);
        REQUIRE(phase.parallelism.row_floor == 0);  // inherits parallel_min_rows at resolve
        REQUIRE(phase.parallelism.breaker_max_workers == 0);  // no per-breaker ceiling
        REQUIRE(phase.parallelism.estimate.source == runtime::physical::RowEstimate::Source::None);
    }

    SECTION("explain physical renders Breaker(Order), not MapPipeline") {
        const auto [tree, plan] = serial_plan("trades[order { price }];");
        const std::string text = runtime::physical::explain_physical(plan);
        REQUIRE(text.find("Breaker(Order)") != std::string::npos);
        REQUIRE(text.find("sort:") != std::string::npos);
        REQUIRE(text.find("row-range") != std::string::npos);
        REQUIRE(text.find("MapPipeline") == std::string::npos);
        REQUIRE(text.find("TableScan()") == std::string::npos);  // the old mislabel
    }

    SECTION("a migrated head is a serial single-operator breaker, not a MapPipeline") {
        const auto [tree, plan] = serial_plan("trades[head 2];");
        REQUIRE(plan.migrated);
        REQUIRE(plan.breaker_phases.empty());
        const std::string text = runtime::physical::explain_physical(plan);
        REQUIRE(text.find("Breaker(Head)") != std::string::npos);
        REQUIRE(text.find("serial (single-operator breaker") != std::string::npos);
        REQUIRE(text.find("MapPipeline") == std::string::npos);
    }

    SECTION("Tail / TopK / FilterHead / FilterTail are migrated single-operator breakers") {
        struct Case {
            const char* source;
            const char* kind;
        };
        for (const Case c :
             {Case{.source = "trades[tail 2];", .kind = "Breaker(Tail)"},
              Case{.source = "trades[order { price }][head 2];", .kind = "Breaker(TopK)"},
              Case{.source = "trades[filter price > 10][head 2];", .kind = "Breaker(FilterHead)"},
              Case{.source = "trades[filter price > 10][tail 2];",
                   .kind = "Breaker(FilterTail)"}}) {
            INFO(c.source);
            const auto [tree, plan] = serial_plan(c.source);
            REQUIRE(plan.migrated);
            REQUIRE(plan.steps.empty());
            REQUIRE(plan.breaker_phases.empty());
            const std::string text = runtime::physical::explain_physical(plan);
            REQUIRE(text.find(c.kind) != std::string::npos);
            REQUIRE(text.find("serial (single-operator breaker") != std::string::npos);
            REQUIRE(text.find("MapPipeline") == std::string::npos);
            REQUIRE(text.find("MaterializedCall") == std::string::npos);
        }
    }
}

TEST_CASE("Migrated Tail / TopK / FilterHead / FilterTail execute through the plan",
          "[physical][breaker][execute]") {
    const auto registry = trades_registry();
    // trades has 3 rows: price {10,20,30} (Int64), symbol {A,B,A}.
    for (const char* source :
         {"trades[tail 2];", "trades[order { price }][head 2];",
          "trades[filter price > 15][head 2];", "trades[filter price > 15][tail 2];"}) {
        INFO(source);
        auto ir = require_ir(source);
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
        const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(s.has_value());
        REQUIRE(p.has_value());
        REQUIRE(s->rows() == 2);
        REQUIRE(s->rows() == p->rows());
        const auto& sp = std::get<Column<std::int64_t>>(*s->find_entry("price")->column);
        const auto& pp = std::get<Column<std::int64_t>>(*p->find_entry("price")->column);
        REQUIRE(sp[0] == pp[0]);
        REQUIRE(sp[1] == pp[1]);
    }
}

TEST_CASE("The plan describes a streaming join's two fan-out phases", "[physical][breaker]") {
    SECTION("explicit HashBuild feeds HashProbe with runtime orientation") {
        const auto [tree, plan] = serial_plan("(a join b on k);");
        REQUIRE(plan.migrated);
        REQUIRE(plan.join.strategy == runtime::physical::JoinStrategy::StreamingProbe);
        REQUIRE(plan.streaming_join.has_value());
        REQUIRE(plan.breaker_phases.empty());

        const auto& nodes = *plan.streaming_join;
        REQUIRE(nodes.build.left_input == plan.join.left_input);
        REQUIRE(nodes.build.right_input == plan.join.right_input);
        REQUIRE(nodes.probe.left_input == plan.join.left_input);
        REQUIRE(nodes.probe.right_input == plan.join.right_input);
        REQUIRE(nodes.build.output == runtime::physical::JoinDataKind::RuntimeOrientedBuildOutput);
        REQUIRE(nodes.probe.build_input == nodes.build.output);

        const auto& build = nodes.build.parallelism;
        REQUIRE(build.strategy == runtime::physical::PartitionStrategy::HeadTable);
        REQUIRE(build.row_floor == (1U << 17U));  // chunked.cpp build_partitions
        REQUIRE(build.breaker_max_workers == 64);

        const auto& probe = nodes.probe.parallelism;
        REQUIRE(probe.strategy == runtime::physical::PartitionStrategy::Range);
        REQUIRE(probe.row_floor == (1U << 14U));  // probe_parallel_workers
        REQUIRE(probe.breaker_max_workers == 64);
    }

    SECTION("known renamed inputs resolve mapped keys to physical positions") {
        runtime::Table left;
        left.add_column("value", Column<std::int64_t>{10, 20});
        left.add_column("id", Column<std::int64_t>{1, 2});
        runtime::Table right;
        right.add_column("other", Column<std::int64_t>{30, 40});
        right.add_column("id", Column<std::int64_t>{2, 3});
        runtime::TableRegistry registry;
        registry.emplace("left_t", std::move(left));
        registry.emplace("right_t", std::move(right));
        auto tree = require_ir(
            "(left_t[rename { left_id = id }] join "
            "right_t[rename { right_id = id }] on { left_id = right_id });");
        const auto plan = runtime::physical::plan_physical(*tree, registry, nullptr);
        REQUIRE(plan.streaming_join.has_value());
        REQUIRE(plan.streaming_join->columns.has_value());
        REQUIRE(plan.streaming_join->columns->keys.size() == 1);
        CHECK(plan.streaming_join->columns->keys[0].left_index == 1);
        CHECK(plan.streaming_join->columns->keys[0].right_index == 1);
        CHECK(plan.streaming_join->columns->output[1].name == "left_id");
        CHECK(plan.streaming_join->columns->output[3].name == "right_id");
        const std::string text = runtime::physical::explain_physical(plan);
        CHECK(text.find("columns: resolved  left[1]=right[1]  output=4") != std::string::npos);
    }

    SECTION("unknown input schemas defer the same mapping to execution") {
        const auto [tree, plan] = serial_plan("(a join b on k);");
        REQUIRE(plan.streaming_join.has_value());
        REQUIRE_FALSE(plan.streaming_join->columns.has_value());
        CHECK(runtime::physical::explain_physical(plan).find("columns: deferred") !=
              std::string::npos);
    }

    SECTION("explain physical renders the two nodes and their typed edge") {
        const auto [tree, plan] = serial_plan("(a join b on k);");
        const std::string text = runtime::physical::explain_physical(plan);
        REQUIRE(text.find("Breaker(Join)") != std::string::npos);
        REQUIRE(text.find("StreamingProbe") != std::string::npos);
        REQUIRE(text.find("HashBuild.RuntimeOrientedBuildOutput -> HashProbe.build_input") !=
                std::string::npos);
        REQUIRE(text.find("HashBuild:") != std::string::npos);
        REQUIRE(text.find("HashProbe:") != std::string::npos);
        REQUIRE(text.find("head-table") != std::string::npos);
        REQUIRE(text.find("MapPipeline") == std::string::npos);
    }

    SECTION("mutating either end of the build output edge is rejected") {
        const auto [tree, plan] = serial_plan("(a join b on k);");
        REQUIRE(plan.streaming_join.has_value());
        auto nodes = *plan.streaming_join;
        REQUIRE_FALSE(runtime::physical::validate_streaming_join_edge(nodes).has_value());

        nodes.probe.build_input = runtime::physical::JoinDataKind::None;
        auto error = runtime::physical::validate_streaming_join_edge(nodes);
        REQUIRE(error.has_value());
        REQUIRE(error->find("does not consume HashBuild") != std::string::npos);

        nodes = *plan.streaming_join;
        nodes.probe.right_input = nodes.probe.left_input;
        error = runtime::physical::validate_streaming_join_edge(nodes);
        REQUIRE(error.has_value());
        REQUIRE(error->find("candidate inputs disagree") != std::string::npos);
    }

    SECTION("a materializing join carries no fan-out phases") {
        const auto [tree, plan] = serial_plan("(a left join b on k);");
        REQUIRE_FALSE(plan.streaming_join.has_value());
        REQUIRE(plan.breaker_phases.empty());
    }

    SECTION("semi join retains its separate streaming operator") {
        const auto [tree, plan] = serial_plan("(a semi join b on k);");
        REQUIRE(plan.migrated);
        REQUIRE(plan.join.branch == runtime::physical::JoinBranch::SemiAnti);
        REQUIRE_FALSE(plan.streaming_join.has_value());
    }
}

TEST_CASE("The plan describes a hash aggregate's structural fan-out policies",
          "[physical][breaker]") {
    SECTION("the hash fallback has four typed structural phases") {
        const auto [tree, plan] =
            serial_plan("trades[select { total = sum(price) }, by { symbol }];");
        REQUIRE(plan.hash_aggregate.has_value());
        const auto& nodes = *plan.hash_aggregate;
        REQUIRE(nodes.discovery.source == tree->children().front().get());
        REQUIRE(nodes.discovery.input == runtime::physical::AggregateDataKind::InputChunks);
        REQUIRE(nodes.discovery.output == runtime::physical::AggregateDataKind::DiscoveredGroups);
        REQUIRE(nodes.accumulation.input == nodes.discovery.output);
        REQUIRE(nodes.accumulation.output ==
                runtime::physical::AggregateDataKind::AccumulatedGroups);
        REQUIRE(nodes.final_ordering.input == nodes.accumulation.output);
        REQUIRE(nodes.final_ordering.output == runtime::physical::AggregateDataKind::OrderedGroups);
        REQUIRE(nodes.emission.input == nodes.final_ordering.output);
        REQUIRE(nodes.emission.output == runtime::physical::AggregateDataKind::OutputChunks);
        REQUIRE_FALSE(runtime::physical::validate_hash_aggregate_edges(nodes).has_value());
    }

    SECTION("each structural node owns its fan-out policy") {
        const auto [tree, plan] =
            serial_plan("trades[select { total = sum(price) }, by { symbol }];");
        REQUIRE(plan.migrated);
        REQUIRE(plan.aggregate.strategy == runtime::physical::AggregateStrategy::StreamingSorted);
        REQUIRE(plan.breaker_phases.empty());
        REQUIRE(plan.hash_aggregate.has_value());
        const auto& nodes = *plan.hash_aggregate;
        REQUIRE(nodes.discovery.parallelism.strategy ==
                runtime::physical::PartitionStrategy::RadixHash);
        REQUIRE(nodes.discovery.parallelism.row_floor == (1U << 18U));
        REQUIRE(nodes.discovery.parallelism.breaker_max_workers == 64);
        REQUIRE(nodes.accumulation.parallelism.strategy ==
                runtime::physical::PartitionStrategy::Morsel);
        REQUIRE(nodes.accumulation.parallelism.row_floor == (1U << 17U));
        REQUIRE(nodes.accumulation.parallelism.breaker_max_workers == 64);
        REQUIRE(nodes.final_ordering.parallelism.strategy ==
                runtime::physical::PartitionStrategy::Owned);
        REQUIRE(nodes.final_ordering.parallelism.row_floor == (1U << 17U));
        REQUIRE(nodes.final_ordering.parallelism.breaker_max_workers == 64);
        REQUIRE(nodes.emission.parallelism.strategy ==
                runtime::physical::PartitionStrategy::ColumnRange);
        REQUIRE(nodes.emission.parallelism.row_floor == 0);
        REQUIRE(nodes.emission.parallelism.breaker_max_workers == 0);
    }

    SECTION("explain physical renders the strategy line and both phases") {
        const auto [tree, plan] =
            serial_plan("trades[select { total = sum(price) }, by { symbol }];");
        const std::string text = runtime::physical::explain_physical(plan);
        REQUIRE(text.find("Breaker(Aggregate)") != std::string::npos);
        REQUIRE(text.find("Discovery:") != std::string::npos);
        REQUIRE(text.find("Accumulation:") != std::string::npos);
        REQUIRE(text.find("FinalOrdering:") != std::string::npos);
        REQUIRE(text.find("Emission:") != std::string::npos);
        REQUIRE(text.find("Discovery -> Accumulation -> FinalOrdering -> Emission") !=
                std::string::npos);
        REQUIRE(
            text.find("InputChunks -> DiscoveredGroups -> AccumulatedGroups -> OrderedGroups -> "
                      "OutputChunks") != std::string::npos);
        REQUIRE(text.find("radix-hash") != std::string::npos);
        REQUIRE(text.find("MapPipeline") == std::string::npos);
    }

    SECTION("a registered scan gives all four nodes its exact input-row bound") {
        const auto [tree, plan] =
            serial_plan("trades[select { total = sum(price) }, by { symbol }];");
        REQUIRE(plan.hash_aggregate.has_value());
        for (const auto* policy : {&plan.hash_aggregate->discovery.parallelism,
                                   &plan.hash_aggregate->accumulation.parallelism,
                                   &plan.hash_aggregate->final_ordering.parallelism,
                                   &plan.hash_aggregate->emission.parallelism}) {
            REQUIRE(policy->estimate.source == runtime::physical::RowEstimate::Source::TableExact);
            REQUIRE(policy->estimate.rows == 3);
        }
        const std::string text = runtime::physical::explain_physical(plan);
        REQUIRE(text.find("input estimate 3 rows (table)") != std::string::npos);
    }

    SECTION("declared source schemas bind aggregate columns without executing the source") {
        auto tree = require_ir("ticks[select { total = sum(volume) }, by { symbol }];");
        const ir::SourceSchemas schemas{{
            "ticks",
            ir::SchemaInfo::known({{.name = "timestamp", .type = ir::ColumnType::Timestamp},
                                   {.name = "symbol", .type = ir::ColumnType::String},
                                   {.name = "price", .type = ir::ColumnType::Float64},
                                   {.name = "volume", .type = ir::ColumnType::Int64}}),
        }};
        const runtime::TableRegistry empty;
        const auto plan = runtime::physical::plan_physical(*tree, empty, nullptr, schemas);
        REQUIRE(plan.aggregate.columns.has_value());
        // Required-column projection narrows the physical child to
        // [symbol, volume], so the mapping describes that input rather than
        // the generator's wider source layout.
        REQUIRE(plan.aggregate.columns->group_by == std::vector<std::size_t>{0});
        REQUIRE(plan.aggregate.columns->aggregate_inputs ==
                std::vector<std::optional<std::size_t>>{1});
    }

    SECTION("a fused left-join count carries no fan-out phases") {
        // COUNT(*) grouped by the left key over a left join fuses to one step
        // that runs whole-table -- no ChunkedAggregateOperator, nothing to fan.
        const auto [tree, plan] = serial_plan(
            "(orders as DataFrame<{ o_orderkey: Int64 }> "
            " left join lineitem as DataFrame<{ l_orderkey: Int64 }> "
            " on { o_orderkey = l_orderkey })"
            "[select { n = count() }, by { o_orderkey }];");
        if (plan.aggregate.strategy == runtime::physical::AggregateStrategy::FusedLeftJoinCount) {
            REQUIRE(plan.breaker_phases.empty());
            REQUIRE_FALSE(plan.hash_aggregate.has_value());
        }
    }
}

TEST_CASE("Physical HashBuild and HashProbe consume the resolved join column mapping",
          "[physical][breaker][execute][join]") {
    runtime::Table left;
    left.add_column("value", Column<std::int64_t>{10, 20, 30});
    left.add_column("id", Column<std::int64_t>{1, 2, 3});
    runtime::Table right;
    right.add_column("other", Column<std::int64_t>{200, 300, 400});
    right.add_column("id", Column<std::int64_t>{2, 3, 4});
    runtime::TableRegistry registry;
    registry.emplace("left_t", std::move(left));
    registry.emplace("right_t", std::move(right));
    auto tree = require_ir(
        "(left_t[rename { left_id = id }] join "
        "right_t[rename { right_id = id }] on { left_id = right_id });");

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 4;
    const auto s = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, serial);
    const auto p = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(s->rows() == 2);
    REQUIRE(p->rows() == s->rows());
    const auto& left_ids = std::get<Column<std::int64_t>>(*s->find("left_id"));
    const auto& right_ids = std::get<Column<std::int64_t>>(*s->find("right_id"));
    const auto& parallel_left_ids = std::get<Column<std::int64_t>>(*p->find("left_id"));
    const auto& parallel_right_ids = std::get<Column<std::int64_t>>(*p->find("right_id"));
    CHECK(left_ids[0] == 2);
    CHECK(left_ids[1] == 3);
    CHECK(right_ids[0] == 2);
    CHECK(right_ids[1] == 3);
    CHECK(parallel_left_ids[0] == left_ids[0]);
    CHECK(parallel_left_ids[1] == left_ids[1]);
    CHECK(parallel_right_ids[0] == right_ids[0]);
    CHECK(parallel_right_ids[1] == right_ids[1]);

    auto plan = runtime::physical::plan_physical(*tree, registry, nullptr);
    REQUIRE(plan.streaming_join.has_value());
    REQUIRE(plan.streaming_join->columns.has_value());

    SECTION("a mutated output position is rejected at the concrete boundary") {
        plan.streaming_join->columns->output.back().source_index = 0;
        const auto result = execute_physical_plan(plan, *tree, registry, serial);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("join column mapping") != std::string::npos);
    }

    SECTION("a narrower physical input layout rebinds the complete mapping once") {
        auto& columns = *plan.streaming_join->columns;
        columns.left_input_names.insert(columns.left_input_names.begin(), "predicate_only");
        for (auto& key : columns.keys) {
            ++key.left_index;
        }
        for (auto& output : columns.output) {
            if (output.side == ir::JoinOutputSide::Left) {
                ++output.source_index;
            }
        }
        const auto result = execute_physical_plan(plan, *tree, registry, serial);
        REQUIRE(result.has_value());
        REQUIRE(result->rows() == s->rows());
        const auto& rebound_left_ids = std::get<Column<std::int64_t>>(*result->find("left_id"));
        const auto& rebound_right_ids = std::get<Column<std::int64_t>>(*result->find("right_id"));
        CHECK(rebound_left_ids[0] == left_ids[0]);
        CHECK(rebound_left_ids[1] == left_ids[1]);
        CHECK(rebound_right_ids[0] == right_ids[0]);
        CHECK(rebound_right_ids[1] == right_ids[1]);
    }
}

TEST_CASE("Physical aggregate consumes its column mapping and rejects mutations",
          "[physical][breaker][execute][aggregate]") {
    runtime::Table input;
    input.add_column("unused", Column<std::int64_t>{90, 91, 92});
    input.add_column("g", Column<std::int64_t>{1, 1, 2});
    input.add_column("v", Column<std::int64_t>{10, 20, 7});
    runtime::TableRegistry registry;
    registry.emplace("input", std::move(input));
    auto tree = require_ir("input[select { s = sum(v) }, by { g }];");
    auto plan = runtime::physical::plan_physical(*tree, registry, nullptr);
    REQUIRE(plan.aggregate.columns.has_value());
    REQUIRE(plan.aggregate.columns->group_by == std::vector<std::size_t>{0});
    REQUIRE(plan.aggregate.columns->aggregate_inputs == std::vector<std::optional<std::size_t>>{1});

    const auto expected = execute_physical_plan(plan, *tree, registry, serial_exec());
    REQUIRE(expected.has_value());
    REQUIRE(expected->rows() == 2);
    const auto& expected_s = std::get<Column<std::int64_t>>(*expected->find("s"));
    CHECK(expected_s[0] == 30);
    CHECK(expected_s[1] == 7);

    SECTION("a mutated group-key position is rejected at the concrete boundary") {
        plan.aggregate.columns->group_by[0] = 1;
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("group-by column mapping") != std::string::npos);
    }

    SECTION("a mutated aggregate-input position is rejected at the concrete boundary") {
        plan.aggregate.columns->aggregate_inputs[0] = 0;
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("aggregate input column mapping") != std::string::npos);
    }

    SECTION("a deliberately deferred mapping binds once and produces the same result") {
        plan.aggregate.columns.reset();
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE(result.has_value());
        REQUIRE(result->rows() == expected->rows());
        const auto& result_s = std::get<Column<std::int64_t>>(*result->find("s"));
        CHECK(result_s[0] == expected_s[0]);
        CHECK(result_s[1] == expected_s[1]);
    }

    SECTION("a narrower physical child layout is rebound once at the aggregate boundary") {
        // Model a lazy scan that consumed a predicate-only leading column and
        // therefore emitted [g, v] although logical inference saw
        // [predicate_only, g, v]. The positions are valid for that logical
        // schema but stale for the concrete breaker input.
        plan.aggregate.columns->input_names = {"predicate_only", "g", "v"};
        plan.aggregate.columns->group_by = {1};
        plan.aggregate.columns->aggregate_inputs = {2};
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE(result.has_value());
        REQUIRE(result->rows() == expected->rows());
        const auto& result_s = std::get<Column<std::int64_t>>(*result->find("s"));
        CHECK(result_s[0] == expected_s[0]);
        CHECK(result_s[1] == expected_s[1]);
    }

    SECTION("the serial executor accounts each structural phase independently") {
        auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/1,
                                                                        /*report=*/false);
        auto exec = serial_exec();
        exec.execution_profile = profile;
        const auto result = execute_physical_plan(plan, *tree, registry, exec);
        REQUIRE(result.has_value());

        const auto rows = profile->snapshot();
        for (const std::string_view label : {"Aggregate.Discovery", "Aggregate.Accumulation",
                                             "Aggregate.FinalOrdering", "Aggregate.Emission"}) {
            const auto phase =
                std::ranges::find_if(rows, [&](const auto& row) { return row.label == label; });
            REQUIRE(phase != rows.end());
            CHECK(phase->next_self_ns > 0);
        }
    }

    SECTION("mutating a structural phase edge is rejected") {
        REQUIRE(plan.hash_aggregate.has_value());
        plan.hash_aggregate->final_ordering.input =
            runtime::physical::AggregateDataKind::DiscoveredGroups;
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("Accumulation -> FinalOrdering") != std::string::npos);
    }

    SECTION("removing the structural phase chain is rejected") {
        plan.hash_aggregate.reset();
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("no hash-fallback phase chain") != std::string::npos);
    }

    SECTION("mutating the discovery input is rejected") {
        REQUIRE(plan.hash_aggregate.has_value());
        plan.hash_aggregate->discovery.source = tree.get();
        const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("Discovery input") != std::string::npos);
    }
}

TEST_CASE("Open aggregate schemas defer positions but authorize only declared names",
          "[physical][breaker][execute][aggregate]") {
    ir::Builder builder;
    auto tree = builder.aggregate(
        {ir::ColumnRef{.name = "g"}},
        {ir::AggSpec{.func = ir::AggFunc::Sum, .column = {.name = "v"}, .alias = "s"}});
    tree->add_child(builder.scan("open_input"));

    const ir::SourceSchemas open{{
        "open_input",
        ir::SchemaInfo::known({{.name = "g", .type = ir::ColumnType::Int64},
                               {.name = "v", .type = ir::ColumnType::Int64}},
                              /*open=*/true),
    }};
    const runtime::TableRegistry none;
    const auto plan = runtime::physical::plan_physical(*tree, none, nullptr, open);
    REQUIRE(plan.migrated);
    REQUIRE_FALSE(plan.aggregate.columns.has_value());

    runtime::Table input;
    input.add_column("extra", Column<std::int64_t>{4, 5, 6});
    input.add_column("g", Column<std::int64_t>{1, 1, 2});
    input.add_column("v", Column<std::int64_t>{10, 20, 7});
    runtime::TableRegistry registry;
    registry.emplace("open_input", std::move(input));
    const auto result = execute_physical_plan(plan, *tree, registry, serial_exec());
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    const auto& sums = std::get<Column<std::int64_t>>(*result->find("s"));
    CHECK(sums[0] == 30);
    CHECK(sums[1] == 7);
}

TEST_CASE("The aggregate reads the plan: parallel output equals serial and the fan-out fires",
          "[physical][breaker][execute]") {
    // A high-cardinality single-Int64 group-by: past kPairOwnedMinRows so the
    // partition-owned path's `partition` phase fires, and over 131072 groups so
    // its co-ranking `finalize` merge fans out too. Both now read their worker
    // cap and fan-out permission from the plan.
    constexpr std::int64_t kRows = 500'000;
    constexpr std::int64_t kKeys = 200'000;  // 2.5 rows/group, order-sensitive sum
    runtime::TableRegistry registry;
    {
        runtime::Table t;
        Column<std::int64_t> g;
        Column<double> v;
        for (std::int64_t r = 0; r < kRows; ++r) {
            g.push_back(r % kKeys);
            v.push_back(1e9 + static_cast<double>(r % 991) * 0.125);
        }
        t.add_column("g", std::move(g));
        t.add_column("v", std::move(v));
        registry.emplace("big", t);
    }

    auto ir = require_ir("big[select { s = sum(v) }, by { g }];");

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.parallel_min_rows = 0;
    parallel.parallel_stats = &stats;
    auto parallel_profile =
        std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/8, /*report=*/false);
    parallel.execution_profile = parallel_profile;

    const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(stats.parallel_aggregate_partitions.load() > 0);  // the radix partition phase fired
    REQUIRE(stats.parallel_aggregate_finalizes.load() > 0);   // and its merge phase
    const auto profile_tasks = [](const auto& rows, std::string_view label) {
        const auto row = std::ranges::find_if(
            rows, [&](const auto& candidate) { return candidate.label == label; });
        REQUIRE(row != rows.end());
        return row->pool_tasks;
    };
    const auto parallel_rows = parallel_profile->snapshot();
    REQUIRE(profile_tasks(parallel_rows, "Aggregate.Discovery") > 0);
    REQUIRE(profile_tasks(parallel_rows, "Aggregate.FinalOrdering") > 0);
    REQUIRE(profile_tasks(parallel_rows, "Aggregate.Emission") > 0);
    REQUIRE(s->rows() == p->rows());
    REQUIRE(s->rows() == static_cast<std::size_t>(kKeys));

    // Byte-identical: group order (first occurrence) and the float sums.
    const auto& sg = std::get<Column<std::int64_t>>(*s->find_entry("g")->column);
    const auto& pg = std::get<Column<std::int64_t>>(*p->find_entry("g")->column);
    const auto& sv = std::get<Column<double>>(*s->find_entry("s")->column);
    const auto& pv = std::get<Column<double>>(*p->find_entry("s")->column);
    for (std::size_t i = 0; i < s->rows(); ++i) {
        REQUIRE(sg[i] == pg[i]);
        REQUIRE(sv[i] == pv[i]);
    }

    // Execute an explicitly mutated copy through the same physical executor.
    // A worker ceiling of one on each structural node must suppress fan-out;
    // if the builder recreated factory defaults, these counters would fire.
    auto capped_plan = runtime::physical::plan_physical(*ir, registry, nullptr);
    REQUIRE(capped_plan.hash_aggregate.has_value());
    capped_plan.hash_aggregate->discovery.parallelism.breaker_max_workers = 1;
    capped_plan.hash_aggregate->accumulation.parallelism.breaker_max_workers = 1;
    capped_plan.hash_aggregate->final_ordering.parallelism.breaker_max_workers = 1;
    capped_plan.hash_aggregate->emission.parallelism.breaker_max_workers = 1;
    runtime::ParallelPipelineStats capped_stats;
    runtime::ExecutionContext capped_exec;
    capped_exec.parallel_threads = 8;
    capped_exec.parallel_min_rows = 0;
    capped_exec.parallel_stats = &capped_stats;
    auto capped_profile =
        std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/8, /*report=*/false);
    capped_exec.execution_profile = capped_profile;
    const auto capped = execute_physical_plan(capped_plan, *ir, registry, capped_exec);
    REQUIRE(capped.has_value());
    REQUIRE(capped_stats.parallel_aggregate_partitions.load() == 0);
    REQUIRE(capped_stats.parallel_aggregate_finalizes.load() == 0);
    const auto capped_rows = capped_profile->snapshot();
    for (const std::string_view label : {"Aggregate.Discovery", "Aggregate.Accumulation",
                                         "Aggregate.FinalOrdering", "Aggregate.Emission"}) {
        REQUIRE(profile_tasks(capped_rows, label) == 0);
    }
    REQUIRE(capped->rows() == s->rows());
    const auto& capped_g = std::get<Column<std::int64_t>>(*capped->find_entry("g")->column);
    const auto& capped_v = std::get<Column<double>>(*capped->find_entry("s")->column);
    for (std::size_t i = 0; i < s->rows(); ++i) {
        REQUIRE(capped_g[i] == sg[i]);
        REQUIRE(capped_v[i] == sv[i]);
    }
}

TEST_CASE("Aggregate Accumulation consumes its structural-node worker ceiling",
          "[physical][breaker][execute]") {
    constexpr std::int64_t kRows = 300'000;
    runtime::Table input;
    Column<std::int64_t> groups;
    Column<double> values;
    groups.reserve(kRows);
    values.reserve(kRows);
    for (std::int64_t row = 0; row < kRows; ++row) {
        groups.push_back(row % 8);
        values.push_back(static_cast<double>(row % 97));
    }
    input.add_column("g", std::move(groups));
    input.add_column("v", std::move(values));
    runtime::TableRegistry registry;
    registry.emplace("many", std::move(input));
    auto tree = require_ir("many[select { s = sum(v), n = count() }, by { g }];");

    const auto tasks_for = [](const auto& profile, std::string_view label) {
        const auto rows = profile->snapshot();
        const auto row = std::ranges::find_if(
            rows, [&](const auto& candidate) { return candidate.label == label; });
        REQUIRE(row != rows.end());
        return row->pool_tasks;
    };
    const auto run = [&](runtime::physical::Plan plan) {
        auto profile =
            std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/8, /*report=*/false);
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.parallel_min_rows = 0;
        exec.execution_profile = profile;
        auto result = execute_physical_plan(plan, *tree, registry, exec);
        REQUIRE(result.has_value());
        return std::pair{std::move(*result), std::move(profile)};
    };

    auto baseline_plan = runtime::physical::plan_physical(*tree, registry, nullptr);
    auto [baseline, baseline_profile] = run(baseline_plan);
    REQUIRE(tasks_for(baseline_profile, "Aggregate.Discovery") > 0);
    REQUIRE(tasks_for(baseline_profile, "Aggregate.Accumulation") > 0);

    REQUIRE(baseline_plan.hash_aggregate.has_value());
    baseline_plan.hash_aggregate->accumulation.parallelism.breaker_max_workers = 1;
    auto [capped, capped_profile] = run(std::move(baseline_plan));
    REQUIRE(tasks_for(capped_profile, "Aggregate.Discovery") > 0);
    REQUIRE(tasks_for(capped_profile, "Aggregate.Accumulation") == 0);
    REQUIRE(capped.rows() == baseline.rows());
    const auto& capped_g = std::get<Column<std::int64_t>>(*capped.find("g"));
    const auto& baseline_g = std::get<Column<std::int64_t>>(*baseline.find("g"));
    const auto& capped_s = std::get<Column<double>>(*capped.find("s"));
    const auto& baseline_s = std::get<Column<double>>(*baseline.find("s"));
    const auto& capped_n = std::get<Column<std::int64_t>>(*capped.find("n"));
    const auto& baseline_n = std::get<Column<std::int64_t>>(*baseline.find("n"));
    for (std::size_t row = 0; row < baseline.rows(); ++row) {
        CHECK(capped_g[row] == baseline_g[row]);
        CHECK(capped_s[row] == baseline_s[row]);
        CHECK(capped_n[row] == baseline_n[row]);
    }
}

TEST_CASE("The join operator reads the hash-build plan: parallel output equals serial",
          "[physical][breaker][execute]") {
    // Both sides over the 131072 hash-build floor and over kStreamRightThreshold
    // (65536), so the operator materializes the left, indexes the right, and its
    // build fans out -- the path build_partitions now takes from build_plan_.
    // Heavy key repetition so the partitioned and serial hash builds must agree
    // on chain order (the join's output row order is a contract).
    constexpr std::int64_t kLeft = 200000;
    constexpr std::int64_t kRight = 160000;
    constexpr std::int64_t kKeys = 40000;
    runtime::TableRegistry registry;
    {
        runtime::Table left;
        Column<std::int64_t> lk;
        Column<std::int64_t> lv;
        for (std::int64_t r = 0; r < kLeft; ++r) {
            lk.push_back(r % kKeys);
            lv.push_back(r);
        }
        left.add_column("k", std::move(lk));
        left.add_column("lv", std::move(lv));
        registry.emplace("big_left", left);

        runtime::Table right;
        Column<std::int64_t> rk;
        Column<std::int64_t> rv;
        for (std::int64_t r = 0; r < kRight; ++r) {
            rk.push_back(r % kKeys);
            rv.push_back(r * 10);
        }
        right.add_column("k", std::move(rk));
        right.add_column("rv", std::move(rv));
        registry.emplace("big_right", right);
    }

    auto ir = require_ir("(big_left join big_right on k)[order { lv, rv }];");

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.parallel_stats = &stats;

    const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(stats.parallel_hash_builds.load() > 0);  // the fan-out path fired
    REQUIRE(s->rows() == p->rows());
    REQUIRE(s->rows() == static_cast<std::size_t>(kLeft) / kKeys * kRight);
    for (const char* col : {"lv", "rv"}) {
        const auto& sc = std::get<Column<std::int64_t>>(*s->find_entry(col)->column);
        const auto& pc = std::get<Column<std::int64_t>>(*p->find_entry(col)->column);
        for (std::size_t i = 0; i < s->rows(); ++i) {
            REQUIRE(sc[i] == pc[i]);
        }
    }
}

TEST_CASE("The join operator reads the probe plan: parallel output equals serial",
          "[physical][breaker][execute]") {
    // n_left < n_right with no pending order drives the swapped path: left is
    // indexed, the right side is scanned once through probe_ranges_parallel over
    // its whole row count. With the right side (200000) above the 16384 probe
    // floor the probe fans out -- the axis probe_parallel_workers now reads from
    // probe_plan_. Swapped mode emits in right-scan order, which the parallel
    // probe preserves exactly, so the two outputs are byte-identical with no
    // `order` to normalise them.
    constexpr std::int64_t kLeft = 120000;
    constexpr std::int64_t kRight = 200000;
    constexpr std::int64_t kKeys = 25000;
    runtime::TableRegistry registry;
    {
        runtime::Table left;
        Column<std::int64_t> lk;
        Column<std::int64_t> lv;
        for (std::int64_t r = 0; r < kLeft; ++r) {
            lk.push_back(r % kKeys);
            lv.push_back(r);
        }
        left.add_column("k", std::move(lk));
        left.add_column("lv", std::move(lv));
        registry.emplace("probe_left", left);

        runtime::Table right;
        Column<std::int64_t> rk;
        Column<std::int64_t> rv;
        for (std::int64_t r = 0; r < kRight; ++r) {
            rk.push_back(r % kKeys);
            rv.push_back(r * 10);
        }
        right.add_column("k", std::move(rk));
        right.add_column("rv", std::move(rv));
        registry.emplace("probe_right", right);
    }

    auto ir = require_ir("(probe_left join probe_right on k);");

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.parallel_stats = &stats;

    const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(stats.parallel_probes.load() > 0);  // the probe fan-out axis fired
    REQUIRE(s->rows() == p->rows());
    REQUIRE(s->rows() == static_cast<std::size_t>(kRight) / kKeys * kLeft);
    for (const char* col : {"lv", "rv"}) {
        const auto& sc = std::get<Column<std::int64_t>>(*s->find_entry(col)->column);
        const auto& pc = std::get<Column<std::int64_t>>(*p->find_entry(col)->column);
        for (std::size_t i = 0; i < s->rows(); ++i) {
            REQUIRE(sc[i] == pc[i]);
        }
    }
}
