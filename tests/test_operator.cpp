#include <ibex/ir/builder.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

TEST_CASE("TableSourceOperator emits one chunk then EOF") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(table));

    auto first = source->next();
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());
    REQUIRE(first.value()->rows() == 3);
    REQUIRE(first.value()->columns.size() == 2);

    auto second = source->next();
    REQUIRE(second.has_value());
    REQUIRE_FALSE(second.value().has_value());
}

TEST_CASE("MaterializeOperator round-trips a Table through the operator scaffold") {
    runtime::Table input;
    input.add_column("price", Column<std::int64_t>{10, 20, 30});
    input.add_column("symbol", Column<std::string>{"A", "B", "A"});

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(input));
    runtime::MaterializeOperator sink{std::move(source)};

    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.rows() == 3);
    REQUIRE(out.columns.size() == 2);

    const auto* price = out.find("price");
    REQUIRE(price != nullptr);
    const auto* price_col = std::get_if<Column<std::int64_t>>(price);
    REQUIRE(price_col != nullptr);
    REQUIRE(price_col->size() == 3);
    REQUIRE((*price_col)[0] == 10);
    REQUIRE((*price_col)[1] == 20);
    REQUIRE((*price_col)[2] == 30);

    const auto* symbol = out.find("symbol");
    REQUIRE(symbol != nullptr);
    const auto* symbol_col = std::get_if<Column<std::string>>(symbol);
    REQUIRE(symbol_col != nullptr);
    REQUIRE((*symbol_col)[0] == "A");
    REQUIRE((*symbol_col)[1] == "B");
    REQUIRE((*symbol_col)[2] == "A");
}

TEST_CASE("MaterializeOperator preserves Table ordering and time_index") {
    runtime::Table input;
    input.add_column("ts", Column<std::int64_t>{1, 2, 3});
    input.add_column("value", Column<double>{1.5, 2.5, 3.5});
    input.ordering = std::vector<ir::OrderKey>{ir::OrderKey{.name = "ts", .ascending = true}};
    input.time_index = std::string{"ts"};

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(input));
    runtime::MaterializeOperator sink{std::move(source)};

    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.ordering.has_value());
    REQUIRE(out.ordering->size() == 1);
    REQUIRE((*out.ordering)[0].name == "ts");
    REQUIRE(out.time_index.has_value());
    REQUIRE(*out.time_index == "ts");
}

namespace {

auto make_int_chunk(const std::string& name, std::vector<std::int64_t> values) -> runtime::Chunk {
    runtime::Chunk chunk;
    runtime::ColumnEntry entry;
    entry.name = name;
    entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
    auto& col = std::get<Column<std::int64_t>>(*entry.column);
    col.reserve(values.size());
    for (auto v : values) {
        col.push_back(v);
    }
    chunk.columns.push_back(std::move(entry));
    return chunk;
}

class VectorSource final : public runtime::Operator {
   public:
    explicit VectorSource(std::vector<runtime::Chunk> chunks) : chunks_(std::move(chunks)) {}

    auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
        if (pos_ >= chunks_.size()) {
            return std::optional<runtime::Chunk>{};
        }
        return std::optional<runtime::Chunk>{std::move(chunks_[pos_++])};
    }

   private:
    std::vector<runtime::Chunk> chunks_;
    std::size_t pos_ = 0;
};

}  // namespace

TEST_CASE("MaterializeOperator concatenates multi-chunk int streams") {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(make_int_chunk("x", {1, 2, 3}));
    chunks.push_back(make_int_chunk("x", {4, 5}));
    chunks.push_back(make_int_chunk("x", {6, 7, 8, 9}));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE(result.has_value());
    REQUIRE(result.value().rows() == 9);

    const auto* col = std::get_if<Column<std::int64_t>>(result.value().find("x"));
    REQUIRE(col != nullptr);
    for (std::int64_t i = 1; i <= 9; ++i) {
        REQUIRE((*col)[static_cast<std::size_t>(i - 1)] == i);
    }
}

TEST_CASE("MaterializeOperator rejects chunk schema mismatches") {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(make_int_chunk("x", {1, 2}));

    // Second chunk has a different column name.
    chunks.push_back(make_int_chunk("y", {3, 4}));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("schema mismatch") != std::string::npos);
}

TEST_CASE("MaterializeOperator returns an empty Table when the source is empty") {
    class EmptySource final : public runtime::Operator {
       public:
        auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
            return std::optional<runtime::Chunk>{};
        }
    };

    runtime::MaterializeOperator sink{std::make_unique<EmptySource>()};
    auto result = sink.run();
    REQUIRE(result.has_value());
    REQUIRE(result.value().columns.empty());
    REQUIRE(result.value().rows() == 0);
}

// ── Pipeline planner tests ──────────────────────────────────────────────────

TEST_CASE("classify_node returns correct roles", "[pipeline]") {
    CHECK(runtime::classify_node(ir::NodeKind::Scan) == runtime::PipelineRole::Source);
    CHECK(runtime::classify_node(ir::NodeKind::ExternCall) == runtime::PipelineRole::Source);
    CHECK(runtime::classify_node(ir::NodeKind::Construct) == runtime::PipelineRole::Source);
    CHECK(runtime::classify_node(ir::NodeKind::Filter) == runtime::PipelineRole::Passthrough);
    CHECK(runtime::classify_node(ir::NodeKind::Project) == runtime::PipelineRole::Passthrough);
    CHECK(runtime::classify_node(ir::NodeKind::Rename) == runtime::PipelineRole::Passthrough);
    CHECK(runtime::classify_node(ir::NodeKind::Update) == runtime::PipelineRole::Breaker);
    CHECK(runtime::classify_node(ir::NodeKind::Aggregate) == runtime::PipelineRole::Breaker);
    CHECK(runtime::classify_node(ir::NodeKind::Order) == runtime::PipelineRole::Breaker);
    CHECK(runtime::classify_node(ir::NodeKind::Distinct) == runtime::PipelineRole::Breaker);
    CHECK(runtime::classify_node(ir::NodeKind::Join) == runtime::PipelineRole::Breaker);
}

TEST_CASE("plan_pipelines: scan-only produces one segment", "[pipeline]") {
    ir::Builder b;
    auto scan = b.scan("prices");

    auto plan = runtime::plan_pipelines(*scan);
    REQUIRE(plan.segments.size() == 1);
    REQUIRE(plan.segments[0].size() == 1);
    REQUIRE(plan.segments[0].source()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("plan_pipelines: scan → filter → project is one segment", "[pipeline]") {
    ir::Builder b;
    auto scan = b.scan("prices");
    auto filter = b.filter(ibex::ops::filter_cmp(ir::CompareOp::Gt, ibex::ops::filter_col("price"),
                                                 ibex::ops::filter_dbl(100.0)));
    filter->add_child(std::move(scan));
    auto project = b.project({ir::ColumnRef{.name = "price"}});
    project->add_child(std::move(filter));

    auto plan = runtime::plan_pipelines(*project);
    REQUIRE(plan.segments.size() == 1);
    REQUIRE(plan.segments[0].size() == 3);
    REQUIRE(plan.segments[0].source()->kind() == ir::NodeKind::Scan);
    REQUIRE(plan.segments[0].nodes[1]->kind() == ir::NodeKind::Filter);
    REQUIRE(plan.segments[0].sink()->kind() == ir::NodeKind::Project);
}

TEST_CASE("plan_pipelines: scan → filter → aggregate splits into two segments", "[pipeline]") {
    ir::Builder b;
    auto scan = b.scan("prices");
    auto filter = b.filter(ibex::ops::filter_cmp(ir::CompareOp::Gt, ibex::ops::filter_col("price"),
                                                 ibex::ops::filter_dbl(0.0)));
    filter->add_child(std::move(scan));
    auto agg = b.aggregate(
        {ir::ColumnRef{.name = "symbol"}},
        {ir::AggSpec{
            .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "price"}, .alias = "total"}});
    agg->add_child(std::move(filter));

    auto plan = runtime::plan_pipelines(*agg);
    REQUIRE(plan.segments.size() == 2);

    // Segment 0: scan → filter (the upstream pipeline)
    REQUIRE(plan.segments[0].size() == 2);
    REQUIRE(plan.segments[0].source()->kind() == ir::NodeKind::Scan);
    REQUIRE(plan.segments[0].sink()->kind() == ir::NodeKind::Filter);

    // Segment 1: aggregate (the breaker)
    REQUIRE(plan.segments[1].size() == 1);
    REQUIRE(plan.segments[1].source()->kind() == ir::NodeKind::Aggregate);
}

TEST_CASE("plan_pipelines: scan → agg → order produces three segments", "[pipeline]") {
    ir::Builder b;
    auto scan = b.scan("prices");
    auto agg = b.aggregate(
        {ir::ColumnRef{.name = "symbol"}},
        {ir::AggSpec{
            .func = ir::AggFunc::Count, .column = ir::ColumnRef{.name = "symbol"}, .alias = "n"}});
    agg->add_child(std::move(scan));
    auto order = b.order({ir::OrderKey{.name = "symbol", .ascending = true}});
    order->add_child(std::move(agg));

    auto plan = runtime::plan_pipelines(*order);
    REQUIRE(plan.segments.size() == 3);

    REQUIRE(plan.segments[0].size() == 1);
    REQUIRE(plan.segments[0].source()->kind() == ir::NodeKind::Scan);

    REQUIRE(plan.segments[1].size() == 1);
    REQUIRE(plan.segments[1].source()->kind() == ir::NodeKind::Aggregate);

    REQUIRE(plan.segments[2].size() == 1);
    REQUIRE(plan.segments[2].source()->kind() == ir::NodeKind::Order);
}
