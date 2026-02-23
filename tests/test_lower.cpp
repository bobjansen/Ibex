#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

namespace {

using namespace ibex;

auto require_parse(const char* source) -> parser::Program {
    auto result = parser::parse(source);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

template <typename T>
const T* as_node(const ir::Node* node) {
    return dynamic_cast<const T*>(node);
}

}  // namespace

TEST_CASE("Lower filter and select to IR") {
    auto program = require_parse("df[filter price > 10, select { price }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 1);
    REQUIRE(project->columns()[0].name == "price");

    REQUIRE(project->children().size() == 1);
    const auto* filter = as_node<ir::FilterNode>(project->children()[0].get());
    REQUIRE(filter != nullptr);
    REQUIRE(filter->predicate().column.name == "price");

    REQUIRE(filter->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(filter->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lower grouped aggregation to IR") {
    auto program = require_parse("df[select { symbol, total = sum(price) }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* agg = as_node<ir::AggregateNode>(result->get());
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    REQUIRE(agg->group_by()[0].name == "symbol");
    REQUIRE(agg->aggregations().size() == 1);
    REQUIRE(agg->aggregations()[0].alias == "total");
    REQUIRE(agg->aggregations()[0].column.name == "price");
    REQUIRE(agg->aggregations()[0].func == ir::AggFunc::Sum);
}

TEST_CASE("Lower update with by to IR") {
    auto program = require_parse("df[update { avg = price }, by symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* update = as_node<ir::UpdateNode>(result->get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "avg");
    const auto* expr = std::get_if<ir::ColumnRef>(&update->fields()[0].expr.node);
    REQUIRE(expr != nullptr);
    REQUIRE(expr->name == "price");
    REQUIRE(update->group_by().size() == 1);
    REQUIRE(update->group_by()[0].name == "symbol");
}

TEST_CASE("Lower computed select without aggregation") {
    auto program = require_parse("df[select { x = price * price }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 1);
    REQUIRE(project->columns()[0].name == "x");

    REQUIRE(project->children().size() == 1);
    const auto* update = as_node<ir::UpdateNode>(project->children()[0].get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "x");
}

TEST_CASE("Lower distinct to IR") {
    auto program = require_parse("df[distinct symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* distinct = as_node<ir::DistinctNode>(result->get());
    REQUIRE(distinct != nullptr);
    REQUIRE(distinct->children().size() == 1);
    const auto* project = as_node<ir::ProjectNode>(distinct->children()[0].get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 1);
    REQUIRE(project->columns()[0].name == "symbol");
}

TEST_CASE("Lower order to IR") {
    auto program = require_parse("df[order symbol];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* order = as_node<ir::OrderNode>(result->get());
    REQUIRE(order != nullptr);
    REQUIRE(order->keys().size() == 1);
    REQUIRE(order->keys()[0].name == "symbol");
    REQUIRE(order->keys()[0].ascending);
    REQUIRE(order->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(order->children()[0].get());
    REQUIRE(scan != nullptr);
}

TEST_CASE("Lowering rejects computed group keys") {
    auto program = require_parse("df[by { yr = year(ts) }];");
    auto result = parser::lower(program);
    REQUIRE_FALSE(result.has_value());
}
