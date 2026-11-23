// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/env.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/pipeline.hpp>
#include <ibex/runtime/table_compare.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "aggregate_chunked_internal.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "physical_plan.hpp"

namespace {

using namespace ibex;

/// Aggregates that see every input chunk on its own: no streamed batch joins
/// them (`stream_batch_rows`), so a test's chunk transitions reach the
/// aggregate as written.
auto unbatched_aggregates() -> runtime::AggregatePhasePolicy {
    runtime::AggregatePhasePolicy policy;
    policy.stream_batch_rows = 0;
    return policy;
}

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

TEST_CASE("Bound table schemas enable composite join planning", "[physical][plan][join]") {
    using runtime::physical::JoinBranch;
    using runtime::physical::JoinStrategy;
    runtime::Table left;
    left.add_column("k1", Column<std::int64_t>{1, 1, 2});
    left.add_column("k2", Column<std::int64_t>{2, 3, 4});
    runtime::Table right;
    right.add_column("k1", Column<std::int64_t>{1, 2});
    right.add_column("k2", Column<std::int64_t>{3, 4});
    runtime::TableRegistry registry{{"a", left}, {"b", right}};
    for (const char* source :
         {"a join b on { k1, k2 };", "a semi join b on { k1, k2 };", "a anti join b on { k1, k2 };",
          "(a[select { x = k1, y = k2 }]) join "
          "(b[select { x = k1, y = k2 }]) on { x, y };"}) {
        CAPTURE(source);
        auto node = require_ir(source);
        const auto unknown = runtime::physical::plan_physical(*node, {}, nullptr);
        CHECK(unknown.join.strategy == JoinStrategy::MaterializeBoth);
        const auto plan = runtime::physical::plan_physical(*node, registry, nullptr);
        CHECK(plan.join.strategy == JoinStrategy::StreamingProbe);
        CHECK(plan.join.branch != JoinBranch::None);
        auto result = runtime::interpret(*node, registry);
        REQUIRE(result.has_value());
        CHECK(result->rows() == (std::string(source).find("anti") != std::string::npos ? 1 : 2));
    }
    SECTION("non-integer bound keys still materialize") {
        runtime::Table floating;
        floating.add_column("k1", Column<double>{1., 2.});
        floating.add_column("k2", Column<double>{3., 4.});
        registry.insert_or_assign("b", std::move(floating));
        auto node = require_ir("a join b on { k1, k2 };");
        const auto plan = runtime::physical::plan_physical(*node, registry, nullptr);
        CHECK(plan.join.strategy == JoinStrategy::MaterializeBoth);
    }
}

TEST_CASE("Deferred lazy sources give the planner their key types", "[physical][plan]") {
    // A query built without ascriptions (the Polars adapter's) reads lazy
    // sources the registry never holds. Their schemas used to be unknown to
    // the planner, so a two-key Int64 join could not prove its key types and
    // materialized both sides: Q09's 60M-row lineitem probe decoded whole.
    using runtime::physical::JoinBranch;
    using runtime::physical::JoinStrategy;
    const auto lazy = [](bool string_keys) {
        runtime::Table schema;
        if (string_keys) {
            schema.add_column("k1", Column<std::string>{});
        } else {
            schema.add_column("k1", Column<std::int64_t>{});
        }
        schema.add_column("k2", Column<std::int64_t>{});
        return std::make_shared<runtime::LazyTable>(
            std::move(schema), 10,
            [](const std::vector<std::string>&,
               const runtime::Selection*) -> std::expected<runtime::Table, std::string> {
                return std::unexpected("planning only");
            });
    };
    const auto deferred = [&](bool string_keys) {
        runtime::DeferredScanRegistry registry;
        for (const char* name : {"a", "b"}) {
            runtime::DeferredScan scan;
            scan.lazy = lazy(string_keys);
            registry.emplace(name, std::move(scan));
        }
        return registry;
    };
    auto node = require_ir("a join b on { k1, k2 };");
    const auto ints = deferred(false);
    const auto typed = runtime::physical::plan_physical(
        *node, {}, nullptr, runtime::physical::deferred_source_schemas(&ints));
    CHECK(typed.join.strategy == JoinStrategy::StreamingProbe);
    CHECK(typed.join.branch == JoinBranch::PairIntInner);
    CHECK(runtime::physical::plan_physical(*node, {}, nullptr).join.strategy ==
          JoinStrategy::MaterializeBoth);
    // A string column's in-memory form is settled at decode, so it is never
    // claimed: the same join over string keys proves nothing.
    const auto strings = deferred(true);
    const auto untyped = runtime::physical::plan_physical(
        *node, {}, nullptr, runtime::physical::deferred_source_schemas(&strings));
    CHECK(untyped.join.strategy == JoinStrategy::MaterializeBoth);
}

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
// described. `df[filter ...][update ...]` admits the update into the run only
// for a consumer that takes its morsels as they come (`stream_only_updates`);
// a materialized result takes it back out, so the filter's morsels fan out,
// are reassembled, and the update runs once over the finished table.
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
    REQUIRE(plan.parallel_begin == 0);
    REQUIRE(plan.stream_only_updates == 1);

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 4;
    parallel.parallel_min_rows = 0;
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

// The fused filter→update→project through real morsels. The morsel source
// evaluates the range-native filter in place and gathers only what the update
// and the projection read; the update and projection then run per morsel.
// Every column must come out as the serial run's: a gathered passthrough, a
// nullable Bool, the update's column, and one the update reads but the
// projection drops.
TEST_CASE("A fused filter, update and project step runs over morsels",
          "[physical][execute][fusion][parallel]") {
    constexpr std::size_t kRows = 40'000;
    Column<std::int64_t> price;
    Column<double> qty;
    Column<std::string> symbol;
    Column<bool> flag;
    runtime::ValidityBitmap flag_valid;
    flag_valid.assign(kRows, true);
    for (std::size_t r = 0; r < kRows; ++r) {
        price.push_back(static_cast<std::int64_t>((r * 7919) % kRows));
        qty.push_back(static_cast<double>(r % 13) + 0.5);
        symbol.push_back(r % 3 == 0 ? "A" : "B");
        flag.push_back(r % 2 == 0);
        flag_valid.set(r, r % 5 != 0);
    }
    runtime::Table table;
    table.add_column("price", std::move(price));
    table.add_column("qty", std::move(qty));
    table.add_column("symbol", std::move(symbol));
    table.add_column("flag", std::move(flag));
    table.columns.back().validity = std::move(flag_valid);
    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    const auto ir = require_ir(
        "trades[filter price > 19999][update { notional = Float64(price) * qty }]"
        "[select { symbol, notional, flag, price }];");
    const auto plan = runtime::physical::plan_physical(*ir, registry, nullptr);
    REQUIRE(plan.migrated);
    REQUIRE(plan.steps.size() == 1);
    REQUIRE(plan.steps.front().capability ==
            runtime::MapKernelCapability::FilterUpdateProjectGather);
    REQUIRE(plan.mode == runtime::physical::PipelineMode::MorselParallel);

    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 4;
    parallel.parallel_min_rows = 0;
    parallel.parallel_grain = 4'096;
    parallel.parallel_stats = &stats;

    const auto a = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto b = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(a.has_value());
    REQUIRE(b.has_value());
    REQUIRE(a->rows() == kRows - 20'000);
    REQUIRE(b->columns.size() == 4);
    const auto mismatch = runtime::compare_tables(*a, *b);
    INFO((mismatch.has_value() ? mismatch->message() : std::string{}));
    CHECK_FALSE(mismatch.has_value());
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

    SECTION("two-key semi and anti stream when both keys are provably Int64") {
        // q20's shape: `probed_key_restriction` pushes a two-key semi join
        // under an aggregate. It used to decline for its key count alone and
        // materialize 7.28M lineitem rows into `join_table_impl`.
        for (const char* src :
             {"(a as DataFrame<{ k1: Int64, k2: Int64 }> "
              " semi join b as DataFrame<{ k1: Int64, k2: Int64 }> on { k1, k2 });",
              "(a as DataFrame<{ k1: Int64, k2: Int64 }> "
              " anti join b as DataFrame<{ k1: Int64, k2: Int64 }> on { k1, k2 });"}) {
            CAPTURE(src);
            const auto plan = plan_of(src);
            CHECK(plan.strategy == JoinStrategy::StreamingProbe);
            CHECK(plan.branch == JoinBranch::SemiAnti);
            CHECK(plan.decline == JoinDeclineReason::None);
            CHECK(plan.key_count == 2);
        }
        // Unprovable or non-Int64 key types still decline, for the type.
        CHECK(plan_of("(a semi join b on { k1, k2 });").decline ==
              JoinDeclineReason::KeyTypesUnsupported);
        const auto typed = plan_of(
            "(a as DataFrame<{ k1: Int64, k2: String }> "
            " semi join b as DataFrame<{ k1: Int64, k2: String }> on { k1, k2 });");
        CHECK(typed.strategy == JoinStrategy::MaterializeBoth);
        CHECK(typed.decline == JoinDeclineReason::KeyTypesUnsupported);
        CHECK(plan_of("(a semi join b on { k1, k2, k3 });").decline ==
              JoinDeclineReason::MultipleKeys);
    }

    SECTION("the branch is named, so the seam never infers it from key count") {
        // The seam dispatches on this. It used to deduce the operator from
        // `key_count == 1` vs `== 2`, which held only while StreamingProbe
        // implied a full gate had passed -- a coupling nothing would have
        // reported if a later gate broke it.
        CHECK(plan_of("(a join b on k);").branch == JoinBranch::SingleKeyInner);
        CHECK(plan_of("(a left join b on k);").branch == JoinBranch::SingleKeyLeft);
        CHECK(plan_of("(a outer join b on k);").branch == JoinBranch::None);
        // None exactly when both sides materialize, in both directions.
        for (const char* src :
             {"(a join b on k);", "(a semi join b on k);", "(a left join b on k);",
              "(a outer join b on k);", "(a join b on x < y);"}) {
            CAPTURE(src);
            const auto plan = plan_of(src);
            CHECK((plan.branch == JoinBranch::None) ==
                  (plan.strategy == JoinStrategy::MaterializeBoth));
        }
    }

    SECTION("every decline is named, not a bare conjunction") {
        CHECK(plan_of("(a outer join b on k);").decline == JoinDeclineReason::UnsupportedKind);
        CHECK(plan_of("(a join b on x < y);").decline == JoinDeclineReason::HasPredicate);
        // The left join streams on one key only, with no pair path.
        CHECK(plan_of("(a left join b on { k1, k2 });").decline == JoinDeclineReason::MultipleKeys);
        CHECK(plan_of("(a left join b on k nulls equal);").decline ==
              JoinDeclineReason::NullsEqual);
        // Two keys decline for a schema reason, not a count one: the pair path
        // takes exactly two keys, but only when both are provably Int64 on both
        // sides. With no schema here, they are not provable.
        CHECK(plan_of("(a join b on { k1, k2 });").decline ==
              JoinDeclineReason::KeyTypesUnsupported);
        CHECK(plan_of("(a join b on { k1, k2, k3 });").decline == JoinDeclineReason::MultipleKeys);
        CHECK(plan_of("(a join b on k nulls equal);").decline == JoinDeclineReason::NullsEqual);
        for (const char* src : {"(a outer join b on k);", "(a join b on x < y);",
                                "(a join b on { k1, k2 });", "(a join b on { k1, k2, k3 });",
                                "(a join b on k nulls equal);", "(a left join b on { k1, k2 });"}) {
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
        auto left = require_ir("(a left join b on k);");
        const auto lp = runtime::physical::plan_physical(*left, empty, nullptr);
        CHECK(lp.migrated);
        CHECK(lp.join.branch == JoinBranch::SingleKeyLeft);
        CHECK(lp.streaming_join.has_value());

        auto materializing = require_ir("(a outer join b on k);");
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

    SECTION("the fusion sees through plain copies of the join's columns") {
        // The Polars adapter renames every join column through a select (an
        // update of plain copies), so the group key and the counted column
        // reach the aggregate under other names.
        auto ir = require_ir(
            "(l left join r on k)[select { g = k, c = v }][select { n = count(c) }, by g];");
        REQUIRE(ir->kind() == ir::NodeKind::Aggregate);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto plan =
            runtime::physical::plan_aggregate(static_cast<const ir::AggregateNode&>(*ir));
        CHECK(plan.strategy == AggregateStrategy::FusedLeftJoinCount);
        CHECK(plan.counted_column == "v");
        CHECK(plan.group_source == "k");
        // A computed key is not a copy of the join's key.
        auto computed = require_ir(
            "(l left join r on k)[select { g = k + 1, c = v }][select { n = count(c) }, by g];");
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        CHECK(runtime::physical::plan_aggregate(static_cast<const ir::AggregateNode&>(*computed))
                  .strategy != AggregateStrategy::FusedLeftJoinCount);
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
        REQUIRE(build.row_floor == (1U << 17U));  // join_chunked.cpp build_partitions
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
        const auto [tree, plan] = serial_plan("(a outer join b on k);");
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
        REQUIRE(nodes.discovery.parallelism.row_floor == (1U << 15U));
        REQUIRE(nodes.discovery.parallelism.breaker_max_workers == 64);
        REQUIRE(nodes.accumulation.parallelism.strategy ==
                runtime::physical::PartitionStrategy::Morsel);
        REQUIRE(nodes.accumulation.parallelism.row_floor == (1U << 17U));
        REQUIRE(nodes.accumulation.parallelism.breaker_max_workers == 64);
        REQUIRE(nodes.final_ordering.parallelism.strategy ==
                runtime::physical::PartitionStrategy::Owned);
        REQUIRE(nodes.final_ordering.parallelism.row_floor == (1U << 16U));
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

    SECTION("a lazy scan gives the aggregate its footer row count") {
        // A streamed Parquet scan is a deferred lazy source, not a registered
        // table. Its footer's row count is the aggregate's input estimate,
        // through row-preserving steps only, and only when the planner is
        // handed the deferred sources.
        runtime::Table schema;
        schema.add_column("k", Column<std::int64_t>{});
        schema.add_column("v", Column<double>{});
        auto lazy = std::make_shared<runtime::LazyTable>(
            std::move(schema), 4'000,
            [](const std::vector<std::string>&,
               const runtime::Selection*) -> std::expected<runtime::Table, std::string> {
                return std::unexpected("planning only: nothing decodes");
            });
        runtime::DeferredScanRegistry deferred;
        deferred.emplace("lazy", runtime::DeferredScan{.lazy = std::move(lazy),
                                                       .conjuncts = {},
                                                       .demand = {"k", "v"},
                                                       .demand_all = false,
                                                       .key_column = {},
                                                       .filter = nullptr});
        const runtime::TableRegistry registry;
        const auto schemas = runtime::physical::deferred_source_schemas(&deferred);
        const auto plan_of = [&](const char* source, const runtime::DeferredScanRegistry* scans) {
            auto tree = require_ir(source);
            auto plan = runtime::physical::plan_physical(*tree, registry, nullptr, schemas, scans);
            return std::pair{std::move(tree), std::move(plan)};
        };
        const auto [tree, plan] = plan_of(
            "lazy[update { w = v * 2.0 }][select { total = sum(w) }, by { k }];", &deferred);
        REQUIRE(plan.hash_aggregate.has_value());
        const auto& estimate = plan.hash_aggregate->discovery.parallelism.estimate;
        REQUIRE(estimate.source == runtime::physical::RowEstimate::Source::Footer);
        REQUIRE(estimate.rows == 4'000);
        REQUIRE(runtime::physical::explain_physical(plan).find(
                    "input estimate 4000 rows (footer)") != std::string::npos);
        const auto [unknown_tree, without_scans] =
            plan_of("lazy[select { total = sum(v) }, by { k }];", nullptr);
        REQUIRE(without_scans.hash_aggregate.has_value());
        REQUIRE(without_scans.hash_aggregate->discovery.parallelism.estimate.source ==
                runtime::physical::RowEstimate::Source::None);
        const auto [filtered_tree, filtered] =
            plan_of("lazy[filter v > 1.0][select { total = sum(v) }, by { k }];", &deferred);
        REQUIRE(filtered.hash_aggregate.has_value());
        REQUIRE(filtered.hash_aggregate->discovery.parallelism.estimate.source ==
                runtime::physical::RowEstimate::Source::None);
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

TEST_CASE("The hot Int64 sum splits one big chunk into row-range jobs and stays exact",
          "[physical][breaker][execute]") {
    // A registry table reaches the aggregate as ONE chunk. The async hot sum
    // path ran one job per chunk, so that chunk ran on one worker; it is now
    // cut into ~1M-row jobs. The keys are clustered -- runs of 3 that straddle
    // every job boundary -- with nulls in the summed column, and the last
    // rows revisit early keys so the cold merge leaves its in-order fast path.
    // Group order (first occurrence) and every sum must match a serial run.
    constexpr std::int64_t kRows = 4'200'000;
    runtime::TableRegistry registry;
    {
        runtime::Table t;
        Column<std::int64_t> k;
        Column<double> v;
        std::vector<bool> valid;
        for (std::int64_t r = 0; r < kRows; ++r) {
            // Spaced 4 apart: a span over rows/2 keeps the dense-int path out.
            k.push_back(r < kRows - 1000 ? (r / 3) * 4 : (r % 977) * 4);
            v.push_back(static_cast<double>(r % 101) * 0.25);
            valid.push_back(r % 17 != 0);
        }
        t.add_column("k", std::move(k));
        t.add_column("v", std::move(v), runtime::ValidityBitmap(valid));
        registry.emplace("t", t);
    }
    auto ir = require_ir("t[select { s = sum(v) }, by k];");
    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.parallel_stats = &stats;
    auto profile =
        std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/8, /*report=*/false);
    parallel.execution_profile = profile;
    const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(stats.parallel_aggregate_partitions.load() > 0);  // the hot path ran
    REQUIRE(s->rows() == p->rows());
    const auto& sk = std::get<Column<std::int64_t>>(*s->find_entry("k")->column);
    const auto& pk = std::get<Column<std::int64_t>>(*p->find_entry("k")->column);
    const auto& sv = std::get<Column<double>>(*s->find_entry("s")->column);
    const auto& pv = std::get<Column<double>>(*p->find_entry("s")->column);
    for (std::size_t i = 0; i < s->rows(); ++i) {
        REQUIRE(sk[i] == pk[i]);
        REQUIRE(sv[i] == pv[i]);
    }
    const auto rows = profile->snapshot();
    const auto discovery = std::ranges::find_if(
        rows, [](const auto& row) { return row.label == "Aggregate.Discovery"; });
    REQUIRE(discovery != rows.end());
    // The stage counts the hot jobs plus the cold merge's one task per
    // partition (the largest power of two within the worker cap).
    const std::size_t cap = std::min<std::size_t>(8, runtime::process_worker_pool().size());
    std::size_t parts = 1;
    while (parts * 2 <= cap) {
        parts *= 2;
    }
    CHECK(discovery->pool_tasks >= parts + 4);  // 4 jobs, not 1
}

TEST_CASE("Sorted Int64 keys group exactly, serial and parallel, against a hand reference",
          "[physical][breaker][execute]") {
    // Three aggregates keep the hot sum path out, and keys spaced 2 apart keep
    // the dense path out, so a sorted chunk reaches the sorted numbering -- on
    // one core inline, on eight across the pool. Runs of 7 cross the ranges'
    // and the 300k-row chunks' boundaries; in the chunked section the last
    // chunk revisits earlier keys, so the later paths must pick up the groups
    // the sorted path numbered. `first(v)` reads each new group's first row.
    // Both runs are checked against a hand-built first-occurrence reference.
    const auto check = [](std::int64_t rows, bool unsorted_tail) {
        runtime::Table t;
        Column<std::int64_t> k;
        Column<double> v;
        std::vector<std::int64_t> order;
        struct Want {
            std::int64_t count = 0;
            double sum = 0.0;
            double first = 0.0;
        };
        std::map<std::int64_t, Want> want;
        for (std::int64_t r = 0; r < rows; ++r) {
            const bool tail = unsorted_tail && r >= rows - 300'000;
            const std::int64_t key = tail ? ((r * 7919) % 50'000) * 2 : (r / 7) * 2;
            const double val = static_cast<double>(r % 101) * 0.25;
            k.push_back(key);
            v.push_back(val);
            auto [it, fresh] = want.try_emplace(key);
            if (fresh) {
                order.push_back(key);
                it->second.first = val;
            }
            it->second.count += 1;
            it->second.sum += val;
        }
        t.add_column("k", std::move(k));
        t.add_column("v", std::move(v));
        runtime::TableRegistry registry;
        registry.emplace("t", t);
        auto ir = require_ir("t[select { s = sum(v), c = count(), f = first(v) }, by k];");
        for (const std::size_t threads : {std::size_t{1}, std::size_t{8}}) {
            runtime::ExecutionContext exec;
            exec.parallel_threads = threads;
            const auto out = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
            REQUIRE(out.has_value());
            REQUIRE(out->rows() == order.size());
            const auto& ok = std::get<Column<std::int64_t>>(*out->find_entry("k")->column);
            const auto& oc = std::get<Column<std::int64_t>>(*out->find_entry("c")->column);
            const auto& os = std::get<Column<double>>(*out->find_entry("s")->column);
            const auto& of = std::get<Column<double>>(*out->find_entry("f")->column);
            for (std::size_t i = 0; i < order.size(); ++i) {
                const auto& w = want.at(order[i]);
                REQUIRE(ok[i] == order[i]);
                REQUIRE(oc[i] == w.count);
                REQUIRE(os[i] == Catch::Approx(w.sum));
                REQUIRE(of[i] == w.first);
            }
        }
    };
    SECTION("one chunk") {
        check(1'200'000, false);
    }
    SECTION("300k-row chunks, the last one unsorted") {
        const char* saved = std::getenv("IBEX_CHUNK_ROWS");
        const std::optional<std::string> restore =
            saved == nullptr ? std::nullopt : std::optional<std::string>(saved);
        runtime::set_env("IBEX_CHUNK_ROWS", "300000");
        check(1'500'000, true);
        if (restore.has_value()) {
            runtime::set_env("IBEX_CHUNK_ROWS", *restore);
        } else {
            runtime::unset_env("IBEX_CHUNK_ROWS");
        }
    }
}

TEST_CASE("Sorted Int64 pair keys group exactly, serial and parallel, against a hand reference",
          "[physical][breaker][execute]") {
    // {a, b} sorted lexicographically, as lineitem's {l_orderkey,
    // l_linenumber}: runs of 3 per pair, b cycling 1..4 within each a. Both
    // the serial loop (now deferring its index while pairs only increase) and
    // the parallel sorted numbering must give first-occurrence order, counts
    // and sums equal to a hand-built reference. In the chunked section the last
    // chunk revisits earlier pairs, so the deferred index must be built from
    // the groups numbered so far.
    const auto check = [](std::int64_t rows, bool unsorted_tail) {
        runtime::Table t;
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        Column<double> v;
        std::vector<std::pair<std::int64_t, std::int64_t>> order;
        std::map<std::pair<std::int64_t, std::int64_t>, std::pair<std::int64_t, double>> agg;
        for (std::int64_t r = 0; r < rows; ++r) {
            const bool tail = unsorted_tail && r >= rows - 300'000;
            const std::int64_t run = tail ? (r * 7919) % 40'000 : r / 3;
            const std::int64_t ka = (run / 4) * 3;
            const std::int64_t kb = 1 + (run % 4);
            const double val = static_cast<double>(r % 101) * 0.25;
            a.push_back(ka);
            b.push_back(kb);
            v.push_back(val);
            auto [it, fresh] = agg.try_emplace({ka, kb}, 0, 0.0);
            if (fresh) {
                order.emplace_back(ka, kb);
            }
            it->second.first += 1;
            it->second.second += val;
        }
        t.add_column("a", std::move(a));
        t.add_column("b", std::move(b));
        t.add_column("v", std::move(v));
        runtime::TableRegistry registry;
        registry.emplace("t", t);
        const auto ir = require_ir("t[select { s = sum(v), c = count() }, by { a, b }];");
        for (const std::size_t threads : {std::size_t{1}, std::size_t{8}}) {
            runtime::ExecutionContext exec;
            exec.parallel_threads = threads;
            const auto out = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
            REQUIRE(out.has_value());
            REQUIRE(out->rows() == order.size());
            const auto& oa = std::get<Column<std::int64_t>>(*out->find_entry("a")->column);
            const auto& ob = std::get<Column<std::int64_t>>(*out->find_entry("b")->column);
            const auto& oc = std::get<Column<std::int64_t>>(*out->find_entry("c")->column);
            const auto& os = std::get<Column<double>>(*out->find_entry("s")->column);
            for (std::size_t i = 0; i < order.size(); ++i) {
                REQUIRE(oa[i] == order[i].first);
                REQUIRE(ob[i] == order[i].second);
                const auto& want = agg.at(order[i]);
                REQUIRE(oc[i] == want.first);
                REQUIRE(os[i] == Catch::Approx(want.second));
            }
        }
    };
    SECTION("one chunk") {
        check(1'200'000, false);
    }
    SECTION("300k-row chunks, the last one unsorted") {
        const char* saved = std::getenv("IBEX_CHUNK_ROWS");
        const std::optional<std::string> restore =
            saved == nullptr ? std::nullopt : std::optional<std::string>(saved);
        runtime::set_env("IBEX_CHUNK_ROWS", "300000");
        check(1'500'000, true);
        if (restore.has_value()) {
            runtime::set_env("IBEX_CHUNK_ROWS", *restore);
        } else {
            runtime::unset_env("IBEX_CHUNK_ROWS");
        }
    }
}

TEST_CASE("Streamed sorted keys accumulate across workers, bit-identical to one thread",
          "[physical][breaker][execute]") {
    // 300k-row chunks of a sorted key with runs of 1-5 rows. Per chunk the
    // private-partials accumulation sizes its state by every group so far and
    // the owned-ranges one needs a 1M-row chunk, so these chunks accumulated
    // on one thread. The sorted numbering's gids never decrease, so each chunk
    // now splits into row ranges cut where the gid changes. The sum's values
    // are not dyadic, so a group split across workers, or summed out of row
    // order, would not match the one-thread run bit for bit.
    constexpr std::int64_t kRows = 1'200'000;
    runtime::Table t;
    {
        Column<std::int64_t> k;
        Column<double> v;
        std::int64_t key = 0;
        for (std::int64_t r = 0; r < kRows; ++r) {
            if (r % 5 == 0 || r % 7 == 0) {
                key += 4;  // spaced 4 apart: keeps the dense-int path out
            }
            k.push_back(key);
            v.push_back(static_cast<double>(r % 1013) * 0.1);
        }
        t.add_column("k", std::move(k));
        t.add_column("v", std::move(v));
    }
    runtime::TableRegistry registry;
    registry.emplace("t", t);
    const auto ir = require_ir("t[select { s = sum(v), c = count() }, by k];");
    const char* saved = std::getenv("IBEX_CHUNK_ROWS");
    const std::optional<std::string> restore =
        saved == nullptr ? std::nullopt : std::optional<std::string>(saved);
    runtime::set_env("IBEX_CHUNK_ROWS", "300000");
    runtime::ExecutionContext serial;
    serial.parallel_threads = 1;
    runtime::ParallelPipelineStats stats;
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.aggregate_phase_policy = unbatched_aggregates();
    parallel.parallel_stats = &stats;
    const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
    const auto before = runtime::ascending_gid_accumulations().load();
    const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, parallel);
    const auto ascending = runtime::ascending_gid_accumulations().load() - before;
    if (restore.has_value()) {
        runtime::set_env("IBEX_CHUNK_ROWS", *restore);
    } else {
        runtime::unset_env("IBEX_CHUNK_ROWS");
    }
    REQUIRE(s.has_value());
    REQUIRE(p.has_value());
    REQUIRE(s->rows() == p->rows());
    const auto& sk = std::get<Column<std::int64_t>>(*s->find_entry("k")->column);
    const auto& pk = std::get<Column<std::int64_t>>(*p->find_entry("k")->column);
    const auto& ss = std::get<Column<double>>(*s->find_entry("s")->column);
    const auto& ps = std::get<Column<double>>(*p->find_entry("s")->column);
    const auto& sc = std::get<Column<std::int64_t>>(*s->find_entry("c")->column);
    const auto& pc = std::get<Column<std::int64_t>>(*p->find_entry("c")->column);
    for (std::size_t i = 0; i < s->rows(); ++i) {
        REQUIRE(sk[i] == pk[i]);
        REQUIRE(ss[i] == ps[i]);
        REQUIRE(sc[i] == pc[i]);
    }
    if (std::min<std::size_t>(8, runtime::process_worker_pool().size()) >= 2) {
        CHECK(ascending >= 4);  // every 300k-row chunk split by ascending gid
    }
}

TEST_CASE("Group arrays grown past their own mapping keep every group exact",
          "[physical][breaker][execute]") {
    // 5M distinct Int64 pairs, streamed in 1M-row chunks: the group keys (80 MB)
    // and the two aggregate slots per group grow chunk by chunk past the size
    // at which they move into a mapping of their own and then grow by
    // `mremap`. Every group must come out with its own key, count and sum, in
    // first-occurrence order.
    constexpr std::int64_t kRows = 5'000'000;
    runtime::Table t;
    {
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        Column<std::int64_t> v;
        for (std::int64_t r = 0; r < kRows; ++r) {
            a.push_back((r / 4) * 3);
            b.push_back(r % 4);
            v.push_back(r);
        }
        t.add_column("a", std::move(a));
        t.add_column("b", std::move(b));
        t.add_column("v", std::move(v));
    }
    runtime::TableRegistry registry;
    registry.emplace("t", t);
    const auto ir = require_ir("t[select { s = sum(v), c = count() }, by { a, b }];");
    const char* saved = std::getenv("IBEX_CHUNK_ROWS");
    const std::optional<std::string> restore =
        saved == nullptr ? std::nullopt : std::optional<std::string>(saved);
    runtime::set_env("IBEX_CHUNK_ROWS", "1000000");
    for (const std::size_t threads : {std::size_t{1}, std::size_t{8}}) {
        CAPTURE(threads);
        runtime::ExecutionContext exec;
        exec.parallel_threads = threads;
        const auto out = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(out.has_value());
        REQUIRE(out->rows() == static_cast<std::size_t>(kRows));
        const auto& oa = std::get<Column<std::int64_t>>(*out->find_entry("a")->column);
        const auto& ob = std::get<Column<std::int64_t>>(*out->find_entry("b")->column);
        const auto& os = std::get<Column<std::int64_t>>(*out->find_entry("s")->column);
        const auto& oc = std::get<Column<std::int64_t>>(*out->find_entry("c")->column);
        for (std::int64_t r = 0; r < kRows; ++r) {
            const auto i = static_cast<std::size_t>(r);
            if (oa[i] != (r / 4) * 3 || ob[i] != r % 4 || os[i] != r || oc[i] != 1) {
                FAIL("group " << r << ": a=" << oa[i] << " b=" << ob[i] << " s=" << os[i]
                              << " c=" << oc[i]);
            }
        }
    }
    if (restore.has_value()) {
        runtime::set_env("IBEX_CHUNK_ROWS", *restore);
    } else {
        runtime::unset_env("IBEX_CHUNK_ROWS");
    }
}

TEST_CASE("Categorical keys past the dense cell space partition their discovery",
          "[physical][breaker][execute]") {
    // 100 x 50,000 dictionary entries is 5M cells, past the dense limit. Such
    // keys used to stay on the Categorical path and fall to its serial hash
    // index (PARALLELISM.md I3); two now take the pair path and three the
    // packed path, which partition discovery across workers. Groups, their
    // first-occurrence order, decoded keys and sums must match a hand
    // reference.
    constexpr std::int64_t kRows = 600'000;
    constexpr std::int32_t kA = 100;
    constexpr std::int32_t kB = 50'000;
    constexpr std::int32_t kC = 7;
    const auto dictionary = [](const char* prefix, std::int32_t n) {
        std::vector<std::string> out;
        out.reserve(static_cast<std::size_t>(n));
        for (std::int32_t i = 0; i < n; ++i) {
            out.push_back(std::string(prefix) + std::to_string(i));
        }
        return out;
    };
    Column<Categorical>::codes_storage a_codes;
    Column<Categorical>::codes_storage b_codes;
    Column<Categorical>::codes_storage c_codes;
    Column<std::int64_t> v;
    for (std::int64_t r = 0; r < kRows; ++r) {
        a_codes.push_back(static_cast<std::int32_t>(r % kA));
        b_codes.push_back(static_cast<std::int32_t>((r * 7919) % kB));
        c_codes.push_back(static_cast<std::int32_t>((r / 3) % kC));
        v.push_back(r % 101);
    }
    runtime::Table t;
    t.add_column("a", Column<Categorical>{dictionary("a", kA), a_codes});
    t.add_column("b", Column<Categorical>{dictionary("b", kB), b_codes});
    t.add_column("c", Column<Categorical>{dictionary("c", kC), c_codes});
    t.add_column("v", std::move(v));
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    const auto check = [&](const char* query, bool three) {
        CAPTURE(query);
        using Key = std::tuple<std::int32_t, std::int32_t, std::int32_t>;
        std::vector<Key> order;
        std::map<Key, std::int64_t> sums;
        for (std::int64_t r = 0; r < kRows; ++r) {
            const auto i = static_cast<std::size_t>(r);
            const Key key{a_codes[i], b_codes[i], three ? c_codes[i] : 0};
            auto [it, fresh] = sums.try_emplace(key, 0);
            if (fresh) {
                order.push_back(key);
            }
            it->second += r % 101;
        }
        const auto ir = require_ir(query);
        for (const std::size_t threads : {std::size_t{1}, std::size_t{8}}) {
            CAPTURE(threads);
            runtime::ParallelPipelineStats stats;
            runtime::ExecutionContext exec;
            exec.parallel_threads = threads;
            exec.parallel_stats = &stats;
            const auto out = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
            REQUIRE(out.has_value());
            REQUIRE(out->rows() == order.size());
            const auto& oa = std::get<Column<Categorical>>(*out->find_entry("a")->column);
            const auto& ob = std::get<Column<Categorical>>(*out->find_entry("b")->column);
            const auto& os = std::get<Column<std::int64_t>>(*out->find_entry("s")->column);
            for (std::size_t g = 0; g < order.size(); ++g) {
                const auto& [ka, kb, kc] = order[g];
                REQUIRE(oa[g] == "a" + std::to_string(ka));
                REQUIRE(ob[g] == "b" + std::to_string(kb));
                if (three) {
                    const auto& oc = std::get<Column<Categorical>>(*out->find_entry("c")->column);
                    REQUIRE(oc[g] == "c" + std::to_string(kc));
                }
                REQUIRE(os[g] == sums.at(order[g]));
            }
            if (threads == 8 &&
                std::min<std::size_t>(8, runtime::process_worker_pool().size()) >= 2) {
                CHECK(stats.parallel_aggregate_partitions.load() > 0);
            }
        }
    };
    SECTION("two keys") {
        check("t[select { s = sum(v) }, by { a, b }];", false);
    }
    SECTION("three keys") {
        check("t[select { s = sum(v) }, by { a, b, c }];", true);
    }
}

TEST_CASE("A one-sum Int64 pair group-by over one whole-table chunk takes the radix path",
          "[physical][breaker][execute]") {
    // One sum over unsorted, high-cardinality Int64 pairs (1.5M groups in
    // 2.2M rows; a spread 1000 apart keeps the pair-dense path out). Handed
    // over as ONE chunk, the async pair sum ran it as a single job and the
    // owned path kept one big table per worker; both lost to the radix path,
    // which now takes a first chunk of 2M rows or more. Streamed 300k-row
    // chunks must still take the async path, whose owned finalize is the
    // only FinalOrdering work a one-sum radix aggregate never does. Either
    // way the groups, their first-occurrence order and every sum must match
    // a hand-built reference.
    constexpr std::int64_t kRows = 2'200'000;
    runtime::Table t;
    std::vector<std::pair<std::int64_t, std::int64_t>> order;
    std::map<std::pair<std::int64_t, std::int64_t>, double> sums;
    {
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        Column<double> v;
        for (std::int64_t r = 0; r < kRows; ++r) {
            const std::int64_t ka = ((r * 7919) % 500'000) * 1000;
            const std::int64_t kb = r % 3;
            const double val = static_cast<double>(r % 101) * 0.25;
            a.push_back(ka);
            b.push_back(kb);
            v.push_back(val);
            auto [it, fresh] = sums.try_emplace({ka, kb}, 0.0);
            if (fresh) {
                order.emplace_back(ka, kb);
            }
            it->second += val;
        }
        t.add_column("a", std::move(a));
        t.add_column("b", std::move(b));
        t.add_column("v", std::move(v));
    }
    runtime::TableRegistry registry;
    registry.emplace("t", t);
    auto ir = require_ir("t[select { s = sum(v) }, by { a, b }];");
    const bool can_fan_out = std::min<std::size_t>(8, runtime::process_worker_pool().size()) >= 2;
    // Runs on 1 and 8 threads; returns the 8-thread FinalOrdering pool tasks.
    const auto check = [&] -> std::size_t {
        std::size_t final_ordering_tasks = 0;
        for (const std::size_t threads : {std::size_t{1}, std::size_t{8}}) {
            runtime::ExecutionContext exec;
            exec.parallel_threads = threads;
            const auto profile = std::make_shared<runtime::ExecutionProfileState>(
                /*worker_budget=*/threads, /*report=*/false);
            exec.execution_profile = profile;
            const auto out = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
            REQUIRE(out.has_value());
            REQUIRE(out->rows() == order.size());
            const auto& oa = std::get<Column<std::int64_t>>(*out->find_entry("a")->column);
            const auto& ob = std::get<Column<std::int64_t>>(*out->find_entry("b")->column);
            const auto& os = std::get<Column<double>>(*out->find_entry("s")->column);
            for (std::size_t i = 0; i < order.size(); ++i) {
                REQUIRE(oa[i] == order[i].first);
                REQUIRE(ob[i] == order[i].second);
                REQUIRE(os[i] == sums.at(order[i]));  // quarters: every sum is exact
            }
            if (threads == 8) {
                for (const auto& row : profile->snapshot()) {
                    if (row.label == "Aggregate.FinalOrdering") {
                        final_ordering_tasks += row.pool_tasks;
                    }
                }
            }
        }
        return final_ordering_tasks;
    };
    SECTION("one chunk goes to the radix path") {
        CHECK(check() == 0);
    }
    SECTION("streamed chunks keep the async pair sum") {
        const char* saved = std::getenv("IBEX_CHUNK_ROWS");
        const std::optional<std::string> restore =
            saved == nullptr ? std::nullopt : std::optional<std::string>(saved);
        runtime::set_env("IBEX_CHUNK_ROWS", "300000");
        const std::size_t tasks = check();
        if (restore.has_value()) {
            runtime::set_env("IBEX_CHUNK_ROWS", *restore);
        } else {
            runtime::unset_env("IBEX_CHUNK_ROWS");
        }
        if (can_fan_out) {
            CHECK(tasks > 0);
        }
    }
}

TEST_CASE("Partitioned discovery over run heads matches serial on clustered keys",
          "[physical][breaker][execute]") {
    // Two aggregates keep the hot sum path out, so this one-chunk table goes
    // through the radix partitioned discovery, which now hashes only a run's
    // first row and copies its gid along the run. Runs of varying length
    // cross every range boundary, and the middle third revisits keys from
    // the first, so a later run of a known key must find its group again.
    constexpr std::int64_t kRows = 900'000;
    runtime::TableRegistry registry;
    {
        runtime::Table t;
        Column<std::int64_t> k;
        Column<double> v;
        for (std::int64_t r = 0; r < kRows; ++r) {
            const std::int64_t run = r / (1 + (r % 7 == 0 ? 0 : 3));
            const std::int64_t key = (r / 300'000 == 1) ? (run % 5'000) * 4 : run * 4;
            k.push_back(key);
            v.push_back(static_cast<double>(r % 101) * 0.25);
        }
        t.add_column("k", std::move(k));
        t.add_column("v", std::move(v));
        registry.emplace("t", t);
    }
    const auto ir = require_ir("t[select { s = sum(v), c = count() }, by k];");
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
    REQUIRE(stats.parallel_aggregate_partitions.load() > 0);  // the radix path ran
    REQUIRE(s->rows() == p->rows());
    const auto& sk = std::get<Column<std::int64_t>>(*s->find_entry("k")->column);
    const auto& pk = std::get<Column<std::int64_t>>(*p->find_entry("k")->column);
    const auto& sv = std::get<Column<double>>(*s->find_entry("s")->column);
    const auto& pv = std::get<Column<double>>(*p->find_entry("s")->column);
    const auto& sc = std::get<Column<std::int64_t>>(*s->find_entry("c")->column);
    const auto& pc = std::get<Column<std::int64_t>>(*p->find_entry("c")->column);
    for (std::size_t i = 0; i < s->rows(); ++i) {
        REQUIRE(sk[i] == pk[i]);
        REQUIRE(sv[i] == pv[i]);
        REQUIRE(sc[i] == pc[i]);
    }
}

TEST_CASE("A known input size below the radix floor still lets the owned discovery fan out",
          "[physical][breaker][execute]") {
    // Discovery's plan floor declines the phase when a confident input
    // estimate is below it, and a registry table always gives one. The floor
    // used to be the radix path's 262144, so a 100k-row table grouped on an
    // Int64 key or an Int64 pair never reached the partition-owned path,
    // whose own gate (kIntOwnedMinRows / kPairOwnedMinRows, 65536) admits it.
    // At 40k rows that own gate still declines.
    const auto run = [](std::int64_t rows, const char* query) {
        runtime::TableRegistry registry;
        runtime::Table t;
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        Column<double> v;
        for (std::int64_t r = 0; r < rows; ++r) {
            a.push_back((r * 7919) % (rows / 2));  // ~2 rows per key
            b.push_back(r % 3);
            v.push_back(static_cast<double>(r % 991) * 0.125);
        }
        t.add_column("a", std::move(a));
        t.add_column("b", std::move(b));
        t.add_column("v", std::move(v));
        registry.emplace("t", t);
        auto ir = require_ir(query);
        runtime::ParallelPipelineStats stats;
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.parallel_stats = &stats;
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        const auto p = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, exec);
        const auto s = runtime::interpret(*ir, registry, nullptr, nullptr, nullptr, serial);
        REQUIRE(p.has_value());
        REQUIRE(s.has_value());
        REQUIRE(p->rows() == s->rows());
        const auto& ps = std::get<Column<double>>(*p->find_entry("s")->column);
        const auto& ss = std::get<Column<double>>(*s->find_entry("s")->column);
        for (std::size_t i = 0; i < p->rows(); ++i) {
            REQUIRE(ps[i] == ss[i]);
        }
        return stats.parallel_aggregate_partitions.load();
    };
    SECTION("single Int64 key") {
        CHECK(run(100'000, "t[select { s = sum(v) }, by { a }];") > 0);
        CHECK(run(40'000, "t[select { s = sum(v) }, by { a }];") == 0);
    }
    SECTION("Int64 pair key") {
        CHECK(run(100'000, "t[select { s = sum(v) }, by { a, b }];") > 0);
        CHECK(run(40'000, "t[select { s = sum(v) }, by { a, b }];") == 0);
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
        // Eight keys spread wider than the dense array takes, so the groups
        // come from hash discovery and Accumulation is a separate node.
        groups.push_back((row % 8) * 1'000'003);
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

TEST_CASE("Aggregate discovery policy derives phase admission from every strategy",
          "[physical][aggregate][discovery-policy]") {
    using Policy = runtime::AggregateDiscoveryPolicy;
    const std::vector<std::size_t Policy::*> floors{
        &Policy::radix_min_rows, &Policy::packed_min_rows, &Policy::int_owned_min_rows,
        &Policy::pair_owned_min_rows, &Policy::sorted_min_rows};
    CHECK(runtime::physical::aggregate_discovery_parallelism().row_floor ==
          Policy{}.admission_floor());
    for (const auto field : floors) {
        Policy policy;
        for (const auto other : floors)
            policy.*other = 1'000'000;
        policy.*field = 1234;
        CHECK(policy.valid());
        CHECK(runtime::physical::aggregate_discovery_parallelism({}, policy).row_floor == 1234);
        policy.*field = 0;
        CHECK_FALSE(policy.valid());
    }
}

TEST_CASE("Aggregate discovery runtime overrides admit paths below the default plan floor",
          "[physical][aggregate][discovery-policy]") {
    using Policy = runtime::AggregateDiscoveryPolicy;
    struct Strategy {
        const char* name;
        std::size_t Policy::* floor;
        const char* keys;
        bool sorted = false;
    };
    const std::vector<Strategy> strategies{{"owned integer", &Policy::int_owned_min_rows, "a"},
                                           {"owned pair", &Policy::pair_owned_min_rows, "a, b"},
                                           {"radix", &Policy::radix_min_rows, "a, b"},
                                           {"packed", &Policy::packed_min_rows, "a, b, c"},
                                           {"sorted", &Policy::sorted_min_rows, "a", true}};
    for (const auto& strategy : strategies) {
        INFO(strategy.name);
        runtime::Table table;
        Column<std::int64_t> a, b, c;
        for (std::int64_t row = 0; row < 20'000; ++row) {
            a.push_back(strategy.sorted
                            ? row * 1'000'003
                            : static_cast<std::int64_t>(static_cast<std::uint64_t>(row) *
                                                        0x9e3779b97f4a7c15ULL));
            b.push_back(row % 991);
            c.push_back(row % 3);
        }
        table.add_column("a", std::move(a));
        table.add_column("b", std::move(b));
        table.add_column("c", std::move(c));
        runtime::TableRegistry registry;
        registry.emplace("input", std::move(table));
        const std::string query =
            "input[select { n = count() }, by { " + std::string(strategy.keys) + " }];";
        auto tree = require_ir(query.c_str());
        const auto plan = runtime::physical::plan_physical(*tree, registry, nullptr);
        REQUIRE(plan.hash_aggregate.has_value());
        CHECK(plan.hash_aggregate->discovery.parallelism.row_floor > 20'000);
        const auto reference = execute_physical_plan(plan, *tree, registry, serial_exec());
        REQUIRE(reference.has_value());
        for (const bool lowered : {false, true}) {
            INFO("lowered=" << lowered);
            runtime::ExecutionContext exec;
            exec.parallel_threads = 8;
            runtime::ParallelPipelineStats stats;
            exec.parallel_stats = &stats;
            exec.aggregate_discovery_policy.emplace();
            (*exec.aggregate_discovery_policy).*strategy.floor = lowered ? 1000 : 1'000'000;
            exec.aggregate_discovery_policy->sorted_range_rows = 1000;
            const auto result = execute_physical_plan(plan, *tree, registry, exec);
            REQUIRE(result.has_value());
            const auto mismatch = runtime::compare_tables(*reference, *result);
            INFO((mismatch.has_value() ? mismatch->message() : "tables agree"));
            CHECK_FALSE(mismatch.has_value());
            CHECK((stats.parallel_aggregate_partitions.load() > 0) == lowered);
        }
    }
}

TEST_CASE("Aggregate discovery rejects zero floors and grains before execution",
          "[physical][aggregate][discovery-policy]") {
    using Policy = runtime::AggregateDiscoveryPolicy;
    auto tree = require_ir("trades[select { n = count() }, by price];");
    for (const auto field :
         {&Policy::packed_min_rows, &Policy::key_fill_range_groups, &Policy::sorted_range_rows,
          &Policy::async_hot_job_rows, &Policy::pair_owned_max_first_chunk_rows}) {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.aggregate_discovery_policy.emplace();
        (*exec.aggregate_discovery_policy).*field = 0;
        const auto result =
            runtime::interpret(*tree, trades_registry(), nullptr, nullptr, nullptr, exec);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("aggregate discovery policy") != std::string::npos);
    }
}

TEST_CASE("Aggregate discovery policy controls async job grain and pair strategy cutoff",
          "[physical][aggregate][discovery-policy]") {
    runtime::Table table;
    Column<std::int64_t> a, b, v;
    for (std::int64_t row = 0; row < 40'000; ++row) {
        a.push_back((row / 3) * 1'000'003);
        b.push_back(row % 17);
        v.push_back(row % 11);
    }
    table.add_column("a", std::move(a));
    table.add_column("b", std::move(b));
    table.add_column("v", std::move(v));
    runtime::TableRegistry registry;
    registry.emplace("input", std::move(table));
    auto tree = require_ir("input[select { s = sum(v) }, by a];");
    const auto reference =
        runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, serial_exec());
    REQUIRE(reference.has_value());
    const auto tasks = [&](std::size_t job_rows) {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.aggregate_discovery_policy.emplace();
        exec.aggregate_discovery_policy->int_owned_min_rows = 1000;
        exec.aggregate_discovery_policy->async_hot_job_rows = job_rows;
        exec.execution_profile = std::make_shared<runtime::ExecutionProfileState>(8, false);
        const auto result = runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        CHECK_FALSE(runtime::compare_tables(*reference, *result).has_value());
        const auto rows = exec.execution_profile->snapshot();
        const auto row = std::ranges::find_if(
            rows, [](const auto& r) { return r.label == "Aggregate.Discovery"; });
        REQUIRE(row != rows.end());
        return row->pool_tasks;
    };
    // A 1000-row grain makes 40 jobs, the default grain one. Jobs run on at
    // most `cap` lanes (pool tasks), and each of the first `cap` submits finds
    // fewer than `cap` lanes active and starts one, so 40 jobs take between
    // min(cap, 40) and 40 pool tasks while one job takes exactly one.
    const std::size_t cap = std::min<std::size_t>(8, runtime::process_worker_pool().size());
    REQUIRE(cap >= 2);
    CHECK(tasks(1U << 20U) == 1);
    const std::size_t split = tasks(1000);
    CHECK(split >= cap);
    CHECK(split <= 40);

    auto pair = require_ir("input[select { n = count() }, by { a, b }];");
    for (const std::size_t cutoff : {1U, 2'097'152U}) {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.aggregate_discovery_policy.emplace();
        exec.aggregate_discovery_policy->pair_owned_min_rows = 1000;
        exec.aggregate_discovery_policy->pair_owned_max_first_chunk_rows = cutoff;
        runtime::ParallelPipelineStats stats;
        exec.parallel_stats = &stats;
        const auto result = runtime::interpret(*pair, registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        CHECK((stats.parallel_aggregate_partitions.load() > 0) == (cutoff > 1));
    }
}

namespace {
auto phase_policy_input(std::size_t rows, std::size_t groups, bool sorted, bool dense)
    -> runtime::TableRegistry {
    runtime::Table table;
    Column<std::int64_t> a, b, c, v;
    for (std::size_t row = 0; row < rows; ++row) {
        const auto group = sorted ? row * groups / rows : row % groups;
        a.push_back(dense    ? static_cast<std::int64_t>(group)
                    : sorted ? static_cast<std::int64_t>(group * 1'000'003)
                             : static_cast<std::int64_t>(group * 0x9e3779b97f4a7c15ULL));
        b.push_back(static_cast<std::int64_t>(group % 17));
        c.push_back(static_cast<std::int64_t>(group % 3));
        v.push_back(static_cast<std::int64_t>(row % 101));
    }
    table.add_column("a", std::move(a));
    table.add_column("b", std::move(b));
    table.add_column("c", std::move(c));
    table.add_column("v", std::move(v));
    runtime::TableRegistry registry;
    registry.emplace("input", std::move(table));
    return registry;
}

auto checked_policy_tasks(const ir::Node& tree, const runtime::TableRegistry& registry,
                          runtime::ExecutionContext exec, std::string_view phase) -> std::uint64_t {
    auto serial = exec;
    serial.parallel_threads = 1;
    serial.execution_profile.reset();
    const auto reference = runtime::interpret(tree, registry, nullptr, nullptr, nullptr, serial);
    INFO((reference ? "reference succeeds" : reference.error()));
    REQUIRE(reference.has_value());
    exec.parallel_threads = 8;
    exec.execution_profile = std::make_shared<runtime::ExecutionProfileState>(8, false);
    const auto result = runtime::interpret(tree, registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    const auto mismatch = runtime::compare_tables(*reference, *result);
    INFO((mismatch ? mismatch->message() : "tables agree"));
    CHECK_FALSE(mismatch.has_value());
    std::uint64_t tasks = 0;
    for (const auto& row : exec.execution_profile->snapshot())
        if (phase.empty() || row.label == phase)
            tasks += row.pool_tasks;
    return tasks;
}

auto disabled_aggregate_phase_policy() -> runtime::AggregatePhasePolicy {
    runtime::AggregatePhasePolicy policy;
    policy.accumulation_morsel_rows = 1'000'000;
    policy.sorted_min_rows = 1'000'000;
    policy.dense_gid_min_rows = 1'000'000;
    policy.owned_range_min_rows = 1'000'000;
    policy.slot_init_min_bytes = 1'000'000'000;
    return policy;
}
}  // namespace

TEST_CASE("Aggregate phase floors derive from strategy policy and reject invalid grains",
          "[physical][aggregate][phase-policy]") {
    using Policy = runtime::AggregatePhasePolicy;
    Policy policy;
    CHECK(runtime::physical::aggregate_accumulation_parallelism().row_floor == 131072);
    CHECK(runtime::physical::aggregate_final_ordering_parallelism().row_floor == 65536);
    for (const auto field : {&Policy::accumulation_morsel_rows, &Policy::sorted_min_rows,
                             &Policy::dense_gid_min_rows, &Policy::owned_range_min_rows}) {
        auto tuned = policy;
        tuned.*field = 1000;
        CHECK(runtime::physical::aggregate_accumulation_parallelism({}, tuned).row_floor ==
              (field == &Policy::accumulation_morsel_rows ? 2000 : 1000));
    }
    policy.accumulation_morsel_rows = std::numeric_limits<std::size_t>::max();
    CHECK(policy.accumulation_floor() == 262144);  // doubling must not overflow
    const auto tree = require_ir("trades[select { n = count() }, by price];");
    for (const auto field :
         {&Policy::accumulation_morsel_rows, &Policy::sorted_min_rows, &Policy::sorted_range_rows,
          &Policy::dense_gid_min_rows, &Policy::owned_range_min_rows,
          &Policy::owned_range_min_groups, &Policy::owned_range_morsel_rows,
          &Policy::owned_range_window_rows, &Policy::slot_init_min_bytes, &Policy::merge_min_groups,
          &Policy::merge_groups_per_worker, &Policy::ordered_min_runs,
          &Policy::ordered_runs_per_worker}) {
        runtime::ExecutionContext exec;
        exec.aggregate_phase_policy.emplace();
        (*exec.aggregate_phase_policy).*field = 0;
        const auto result =
            runtime::interpret(*tree, trades_registry(), nullptr, nullptr, nullptr, exec);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("aggregate phase policy") != std::string::npos);
    }
}

TEST_CASE("Accumulation overrides admit private sorted and dense strategies below old phase floor",
          "[physical][aggregate][phase-policy]") {
    using Policy = runtime::AggregatePhasePolicy;
    struct Case {
        const char* name;
        std::size_t Policy::* floor;
        std::size_t groups;
        bool sorted;
        bool dense;
    };
    for (const auto& c :
         std::vector<Case>{{"private", &Policy::accumulation_morsel_rows, 8, false, true},
                           {"sorted", &Policy::sorted_min_rows, 10000, true, false},
                           {"dense", &Policy::dense_gid_min_rows, 10000, false, true}}) {
        INFO(c.name);
        const auto registry = phase_policy_input(20000, c.groups, c.sorted, c.dense);
        const auto tree = require_ir("input[select { n = count(), s = sum(v) }, by a];");
        for (const bool lowered : {false, true}) {
            INFO(lowered);
            runtime::ExecutionContext exec;
            exec.aggregate_phase_policy = disabled_aggregate_phase_policy();
            (*exec.aggregate_phase_policy).*c.floor = lowered ? 1000 : 1'000'000;
            exec.aggregate_phase_policy->sorted_range_rows = 1000;
            if (c.sorted) {
                exec.aggregate_discovery_policy.emplace();
                exec.aggregate_discovery_policy->sorted_min_rows = 1000;
                exec.aggregate_discovery_policy->sorted_range_rows = 1000;
            }
            const auto tasks = checked_policy_tasks(*tree, registry, exec,
                                                    c.sorted ? "Aggregate.Accumulation" : "");
            CHECK((tasks > 0) == lowered);
        }
    }
}

TEST_CASE("Owned-range thresholds and grains control work without changing row order",
          "[physical][aggregate][phase-policy]") {
    const auto registry = phase_policy_input(100000, 50000, false, false);
    const auto tree = require_ir("input[select { n = count(), s = sum(v) }, by a];");
    runtime::ExecutionContext exec;
    exec.aggregate_phase_policy = disabled_aggregate_phase_policy();
    exec.aggregate_phase_policy->owned_range_min_rows = 1000;
    exec.aggregate_phase_policy->owned_range_min_groups = 1000;
    exec.aggregate_phase_policy->owned_range_morsel_rows = 5000;
    exec.aggregate_phase_policy->owned_range_window_rows = 11000;  // partial last morsel/window
    CHECK(checked_policy_tasks(*tree, registry, exec, "Aggregate.Accumulation") > 0);
    exec.aggregate_phase_policy->owned_range_min_groups = 1'000'000;
    CHECK(checked_policy_tasks(*tree, registry, exec, "Aggregate.Accumulation") == 0);
}

TEST_CASE("FinalOrdering uses merge and ordered-run floors before plan admission",
          "[physical][aggregate][phase-policy]") {
    for (const bool sorted : {false, true}) {
        INFO(sorted);
        const auto registry = phase_policy_input(20000, sorted ? 5000 : 20000, sorted, false);
        const auto tree = require_ir("input[select { n = count() }, by a];");
        for (const bool lowered : {false, true}) {
            runtime::ExecutionContext exec;
            exec.aggregate_discovery_policy.emplace();
            exec.aggregate_discovery_policy->int_owned_min_rows = 1000;
            exec.aggregate_phase_policy.emplace();
            exec.aggregate_phase_policy->merge_min_groups = lowered ? 1000 : 1'000'000;
            exec.aggregate_phase_policy->ordered_min_runs = lowered ? 1000 : 1'000'000;
            exec.aggregate_phase_policy->merge_groups_per_worker = 1000;
            exec.aggregate_phase_policy->ordered_runs_per_worker = 1000;
            CHECK((checked_policy_tasks(*tree, registry, exec, "Aggregate.FinalOrdering") > 0) ==
                  lowered);
        }
    }
    // Regression: 75,904 ordered runs clear the kernel's 64k floor but the old
    // 128k phase floor vetoed it when the table's exact row count was known.
    auto registry = phase_policy_input(80000, 80000, true, false);
    Column<std::int64_t> clustered;
    for (std::int64_t row = 0; row < 80000; ++row)
        clustered.push_back((row < 8192 ? row / 2 : row - 4096) * 1'000'003);
    runtime::Table clustered_table;
    clustered_table.add_column("a", std::move(clustered));
    registry.at("input") = std::move(clustered_table);
    const auto tree = require_ir("input[select { n = count() }, by a];");
    CHECK(checked_policy_tasks(*tree, registry, {}, "Aggregate.FinalOrdering") > 0);
}

TEST_CASE("Join threshold overrides reach build probe and concatenate",
          "[physical][join][phase-policy]") {
    auto registry = phase_policy_input(20000, 20000, false, false);
    auto other = phase_policy_input(10000, 5000, false, false).at("input");
    for (auto& entry : other.columns)
        if (entry.name != "a")
            entry.name = "other_" + entry.name;
    registry.emplace("other", std::move(other));
    const auto tree = require_ir("(input join other on a);");
    runtime::ExecutionContext exec;
    runtime::ParallelPipelineStats stats;
    exec.parallel_stats = &stats;
    exec.join_threshold_policy.emplace();
    exec.join_threshold_policy->build_min_rows = 1000;
    exec.join_threshold_policy->probe_min_rows = 1000;
    exec.join_threshold_policy->concat_min_rows = 1000;
    const auto with_concat = checked_policy_tasks(*tree, registry, exec, "");
    CHECK(stats.parallel_hash_builds.load() > 0);
    CHECK(stats.parallel_probes.load() > 0);
    exec.join_threshold_policy->concat_min_rows = 1'000'000;
    const auto without_concat = checked_policy_tasks(*tree, registry, exec, "");
    CHECK(with_concat > without_concat);
    exec.join_threshold_policy->build_min_rows = 1'000'000;
    exec.join_threshold_policy->probe_min_rows = 1'000'000;
    stats.parallel_hash_builds.store(0);
    stats.parallel_probes.store(0);
    checked_policy_tasks(*tree, registry, exec, "");
    CHECK(stats.parallel_hash_builds.load() == 0);
    CHECK(stats.parallel_probes.load() == 0);
}

TEST_CASE("Semi right passes and radix strategies consume their runtime thresholds",
          "[physical][phase-policy]") {
    SECTION("semi right intersection") {
        auto registry = phase_policy_input(20000, 20000, false, false);
        const auto right = phase_policy_input(100000, 100000, false, false);
        registry.emplace("other", right.at("input"));
        const auto tree = require_ir("(input semi join other on a);");
        runtime::ExecutionContext exec;
        exec.join_threshold_policy.emplace();
        exec.join_threshold_policy->probe_min_rows = 1'000'000;
        exec.join_threshold_policy->semi_right_min_rows = 1000;
        const auto admitted = checked_policy_tasks(*tree, registry, exec, "");
        exec.join_threshold_policy->semi_right_min_rows = 1'000'000;
        CHECK(admitted > checked_policy_tasks(*tree, registry, exec, ""));
    }
    SECTION("radix strategy switch") {
        const auto registry = phase_policy_input(20000, 20000, false, false);
        const auto tree = require_ir("input[order { a }];");
        runtime::ExecutionContext exec;
        exec.parallel_min_rows = 1000;
        exec.radix_sort_min_rows = 1000;
        const auto msd = checked_policy_tasks(*tree, registry, exec, "");
        exec.radix_sort_min_rows = 1'000'000;
        CHECK(msd > checked_policy_tasks(*tree, registry, exec, ""));
    }
}

TEST_CASE("Join and sort reject invalid runtime thresholds", "[physical][phase-policy]") {
    const auto tree = require_ir("trades;");
    using Policy = runtime::JoinThresholdPolicy;
    for (const auto field : {&Policy::build_min_rows, &Policy::probe_min_rows,
                             &Policy::concat_min_rows, &Policy::semi_right_min_rows}) {
        runtime::ExecutionContext exec;
        exec.join_threshold_policy.emplace();
        (*exec.join_threshold_policy).*field = 0;
        const auto result =
            runtime::interpret(*tree, trades_registry(), nullptr, nullptr, nullptr, exec);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("thresholds must be positive") != std::string::npos);
    }
    runtime::ExecutionContext exec;
    exec.radix_sort_min_rows = 0;
    CHECK_FALSE(
        runtime::interpret(*tree, trades_registry(), nullptr, nullptr, nullptr, exec).has_value());
}

TEST_CASE("Aggregate grains change submitted work and preserve results",
          "[physical][aggregate][phase-policy]") {
    SECTION("private morsels") {
        const auto registry = phase_policy_input(20000, 8, false, true);
        const auto tree = require_ir("input[select { n = count(), s = sum(v) }, by a];");
        runtime::ExecutionContext exec;
        exec.aggregate_phase_policy = disabled_aggregate_phase_policy();
        exec.aggregate_phase_policy->accumulation_morsel_rows = 1000;
        const auto small = checked_policy_tasks(*tree, registry, exec, "");
        exec.aggregate_phase_policy->accumulation_morsel_rows = 5000;
        CHECK(small > checked_policy_tasks(*tree, registry, exec, ""));
    }
    SECTION("sorted ranges") {
        const auto registry = phase_policy_input(20000, 10000, true, false);
        const auto tree = require_ir("input[select { n = count(), s = sum(v) }, by a];");
        runtime::ExecutionContext exec;
        exec.aggregate_discovery_policy.emplace();
        exec.aggregate_discovery_policy->sorted_min_rows = 1000;
        exec.aggregate_phase_policy = disabled_aggregate_phase_policy();
        exec.aggregate_phase_policy->sorted_min_rows = 1000;
        exec.aggregate_phase_policy->sorted_range_rows = 1000;
        const auto small = checked_policy_tasks(*tree, registry, exec, "Aggregate.Accumulation");
        exec.aggregate_phase_policy->sorted_range_rows = 10000;
        CHECK(small > checked_policy_tasks(*tree, registry, exec, "Aggregate.Accumulation"));
    }
    SECTION("key copies") {
        const auto registry = phase_policy_input(20000, 20000, false, false);
        const auto tree = require_ir("input[select { n = count() }, by { a, b, c }];");
        runtime::ExecutionContext exec;
        exec.aggregate_discovery_policy.emplace();
        exec.aggregate_discovery_policy->packed_min_rows = 1000;
        exec.aggregate_discovery_policy->key_fill_range_groups = 1000;
        const auto small = checked_policy_tasks(*tree, registry, exec, "Aggregate.Discovery");
        exec.aggregate_discovery_policy->key_fill_range_groups = 1'000'000;
        CHECK(small > checked_policy_tasks(*tree, registry, exec, "Aggregate.Discovery"));
    }
    SECTION("slot initialization") {
        const auto registry = phase_policy_input(20000, 20000, false, false);
        const auto tree = require_ir("input[select { n = count(), s = sum(v) }, by a];");
        runtime::ExecutionContext exec;
        exec.aggregate_discovery_policy.emplace();
        exec.aggregate_discovery_policy->radix_min_rows = 1000;
        exec.aggregate_phase_policy = disabled_aggregate_phase_policy();
        // Admit the phase; this input is unsorted and has no dense domain.
        exec.aggregate_phase_policy->sorted_min_rows = 1000;
        exec.aggregate_phase_policy->slot_init_min_bytes = 10000;
        const auto small = checked_policy_tasks(*tree, registry, exec, "");
        exec.aggregate_phase_policy->slot_init_min_bytes = 1'000'000'000;
        CHECK(small > checked_policy_tasks(*tree, registry, exec, ""));
    }
    SECTION("final ordering grains") {
        for (const bool sorted : {false, true}) {
            const auto registry = phase_policy_input(20000, sorted ? 5000 : 20000, sorted, false);
            const auto tree = require_ir("input[select { n = count() }, by a];");
            runtime::ExecutionContext exec;
            exec.aggregate_discovery_policy.emplace();
            exec.aggregate_discovery_policy->int_owned_min_rows = 1000;
            exec.aggregate_phase_policy.emplace();
            exec.aggregate_phase_policy->merge_min_groups = 1000;
            exec.aggregate_phase_policy->ordered_min_runs = 1000;
            exec.aggregate_phase_policy->merge_groups_per_worker = 1000;
            exec.aggregate_phase_policy->ordered_runs_per_worker = 1000;
            const auto small =
                checked_policy_tasks(*tree, registry, exec, "Aggregate.FinalOrdering");
            exec.aggregate_phase_policy->merge_groups_per_worker = 50000;
            exec.aggregate_phase_policy->ordered_runs_per_worker = 50000;
            CHECK(small > checked_policy_tasks(*tree, registry, exec, "Aggregate.FinalOrdering"));
        }
    }
}

TEST_CASE("Remaining operator grains reject invalid values", "[physical][operator-grains]") {
    using Policy = runtime::OperatorGrainPolicy;
    const auto tree = require_ir("trades;");
    for (const auto field :
         {&Policy::filter_bounds_rows, &Policy::update_bucket_rows, &Policy::window_split_rows,
          &Policy::decimal_partial_rows, &Policy::map_min_rows, &Policy::map_max_rows,
          &Policy::map_morsels_per_worker}) {
        runtime::ExecutionContext exec;
        exec.operator_grain_policy.*field = 0;
        const auto result =
            runtime::interpret(*tree, trades_registry(), nullptr, nullptr, nullptr, exec);
        REQUIRE_FALSE(result.has_value());
        CHECK(result.error().find("operator grain policy") != std::string::npos);
    }
    runtime::ExecutionContext exec;
    exec.operator_grain_policy.map_min_rows = exec.operator_grain_policy.map_max_rows + 1;
    CHECK_FALSE(
        runtime::interpret(*tree, trades_registry(), nullptr, nullptr, nullptr, exec).has_value());
}

TEST_CASE("Fused filter bounds consume the configured grain", "[physical][operator-grains]") {
    const auto registry = phase_policy_input(20000, 16, false, true);
    auto tree = require_ir("input[filter a >= 3];");
    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(ir::node_cast<ir::FilterNode>(*tree).take_predicate());
    for (const auto grain : {1000U, 1'000'000U}) {
        runtime::ExecutionContext exec;
        exec.parallel_threads = 8;
        exec.parallel_min_rows = 0;
        exec.operator_grain_policy.filter_bounds_rows = grain;
        auto profile = std::make_shared<runtime::ExecutionProfileState>(8, false);
        auto* entry = profile->stage("bounds");
        {
            const runtime::ExecutionProfileScope scope(entry, runtime::ProfilePhase::Next);
            const auto result =
                runtime::filter_selection(registry.at("input"), conjuncts, exec, nullptr);
            REQUIRE(result.has_value());
            std::vector<std::size_t> want;
            for (std::size_t row = 0; row < 20000; ++row)
                if (row % 16 >= 3)
                    want.push_back(row);
            CHECK(std::vector<std::size_t>(result->begin(), result->end()) == want);
        }
        CHECK((profile->snapshot().front().pool_tasks > 0) == (grain == 1000));
    }
}

TEST_CASE("Grouped update bucketing respects its grain", "[physical][operator-grains]") {
    const auto registry = phase_policy_input(20000, 8, false, false);
    const auto tree = require_ir("input[update { s = cumsum(v) }, by a];");
    runtime::ExecutionContext exec;
    exec.parallel_min_rows = 0;
    exec.operator_grain_policy.update_bucket_rows = 1000;
    const auto small = checked_policy_tasks(*tree, registry, exec, "");
    exec.operator_grain_policy.update_bucket_rows = 1'000'000;
    CHECK(small > checked_policy_tasks(*tree, registry, exec, ""));
}

TEST_CASE("Grouped window split grain changes actual halo pieces", "[physical][operator-grains]") {
    auto registry = phase_policy_input(20000, 2, false, true);
    Column<Timestamp> ts;
    for (std::int64_t row = 0; row < 20000; ++row)
        ts.push_back(Timestamp{row});
    registry.at("input").add_column("ts", std::move(ts));
    registry.at("input").set_properties(runtime::TableProperties::time_frame("ts"));
    const auto tree = require_ir("input[window 40ns, by a, update { s = rolling_sum(v) }];");
    for (const auto grain : std::vector<std::size_t>{
             1000, 1'000'000, (std::numeric_limits<std::size_t>::max() / 3) * 2 + 2}) {
        runtime::ExecutionContext exec;
        exec.parallel_min_rows = 0;
        exec.operator_grain_policy.window_split_rows = grain;
        runtime::ParallelPipelineStats stats;
        exec.parallel_stats = &stats;
        checked_policy_tasks(*tree, registry, exec, "");
        CHECK((stats.window_halo_pieces.load() > 0) == (grain == 1000));
    }
}

TEST_CASE("Decimal partials respect query budget and grain",
          "[physical][decimal][operator-grains]") {
    auto registry = phase_policy_input(20000, 8, false, true);
    auto d = runtime::make_decimal_column(DecimalType{.precision = 10, .scale = 2});
    for (std::int64_t row = 0; row < 20000; ++row)
        d.push_back(Decimal{row % 101 - 50});
    registry.at("input").add_column("d", std::move(d));
    runtime::ValidityBitmap validity(20000, true);
    validity.set(123, false);
    registry.at("input").columns.back().validity = std::move(validity);
    const auto tree = require_ir("input[select { s = sum(d), m = mean(d) }, by a];");
    for (const auto budget : {1U, 2U, 4U}) {
        for (const auto grain : {1000U, 1'000'000U}) {
            runtime::ExecutionContext exec;
            exec.parallel_threads = budget;
            exec.operator_grain_policy.decimal_partial_rows = grain;
            exec.execution_profile =
                std::make_shared<runtime::ExecutionProfileState>(budget, false);
            const auto result =
                runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, exec);
            REQUIRE(result.has_value());
            auto serial = exec;
            serial.parallel_threads = 1;
            serial.execution_profile.reset();
            const auto reference =
                runtime::interpret(*tree, registry, nullptr, nullptr, nullptr, serial);
            REQUIRE(reference.has_value());
            CHECK_FALSE(runtime::compare_tables(*reference, *result).has_value());
            std::uint64_t tasks = 0;
            for (const auto& row : exec.execution_profile->snapshot())
                tasks += row.pool_tasks;
            CHECK(tasks == (budget > 1 && grain == 1000 ? budget * 2 : 0));
        }
    }
}

TEST_CASE("Map grain band and target are runtime policy", "[physical][operator-grains]") {
    runtime::ExecutionContext exec;
    exec.parallel_threads = 2;
    exec.operator_grain_policy.map_min_rows = 100;
    exec.operator_grain_policy.map_max_rows = 10000;
    exec.operator_grain_policy.map_morsels_per_worker = 2;
    CHECK(runtime::morsel_grain(exec, 20000) == 5000);
    exec.operator_grain_policy.map_morsels_per_worker = std::numeric_limits<std::size_t>::max();
    CHECK(runtime::morsel_grain(exec, 20000) == 100);  // no multiplication overflow
    exec.operator_grain_policy.map_min_rows = 300;
    CHECK(runtime::morsel_grain(exec, 20000) == 300);
    exec.operator_grain_policy.map_morsels_per_worker = 1;
    exec.operator_grain_policy.map_max_rows = 500;
    CHECK(runtime::morsel_grain(exec, 20000) == 500);
    exec.parallel_grain = 17;
    CHECK(runtime::morsel_grain(exec, 20000) == 17);  // explicit query override wins
}
