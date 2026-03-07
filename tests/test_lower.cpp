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
    // Predicate is a FilterCmp with a FilterColumn on the left referencing "price".
    const auto* cmp = std::get_if<ibex::ir::FilterCmp>(&filter->predicate().node);
    REQUIRE(cmp != nullptr);
    const auto* col = std::get_if<ibex::ir::FilterColumn>(&cmp->left->node);
    REQUIRE(col != nullptr);
    REQUIRE(col->name == "price");

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

TEST_CASE("Lower rename to IR") {
    auto program = require_parse("df[rename { cost = price }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* rename = as_node<ir::RenameNode>(result->get());
    REQUIRE(rename != nullptr);
    REQUIRE(rename->renames().size() == 1);
    REQUIRE(rename->renames()[0].new_name == "cost");
    REQUIRE(rename->renames()[0].old_name == "price");
    REQUIRE(rename->children().size() == 1);
    const auto* scan = as_node<ir::ScanNode>(rename->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "df");
}

TEST_CASE("Lower rename with multiple renames") {
    auto program = require_parse("df[rename { cost = price, amount = qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* rename = as_node<ir::RenameNode>(result->get());
    REQUIRE(rename != nullptr);
    REQUIRE(rename->renames().size() == 2);
    REQUIRE(rename->renames()[0].new_name == "cost");
    REQUIRE(rename->renames()[0].old_name == "price");
    REQUIRE(rename->renames()[1].new_name == "amount");
    REQUIRE(rename->renames()[1].old_name == "qty");
}

TEST_CASE("Lower filter + order pipeline") {
    auto program = require_parse("df[filter price > 10, order { price asc }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    // Order is on top of filter
    const auto* order = as_node<ir::OrderNode>(result->get());
    REQUIRE(order != nullptr);
    REQUIRE(order->keys().size() == 1);
    REQUIRE(order->keys()[0].name == "price");
    REQUIRE(order->keys()[0].ascending);

    REQUIRE(order->children().size() == 1);
    const auto* filter = as_node<ir::FilterNode>(order->children()[0].get());
    REQUIRE(filter != nullptr);
}

TEST_CASE("Lower update without group-by") {
    auto program = require_parse("df[update { total = price * qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* update = as_node<ir::UpdateNode>(result->get());
    REQUIRE(update != nullptr);
    REQUIRE(update->fields().size() == 1);
    REQUIRE(update->fields()[0].alias == "total");
    REQUIRE(update->group_by().empty());
}

TEST_CASE("Lower join to IR") {
    auto program = require_parse("a join b on key;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Inner);
    REQUIRE(join->keys().size() == 1);
    REQUIRE(join->keys()[0] == "key");
    REQUIRE(join->children().size() == 2);
}

TEST_CASE("Lower left join to IR") {
    auto program = require_parse("a left join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Left);
}

TEST_CASE("Lower right join to IR") {
    auto program = require_parse("a right join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Right);
}

TEST_CASE("Lower outer join to IR") {
    auto program = require_parse("a outer join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Outer);
}
TEST_CASE("Lower semi join to IR") {
    auto program = require_parse("a semi join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Semi);
}

TEST_CASE("Lower anti join to IR") {
    auto program = require_parse("a anti join b on id;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Anti);
}

TEST_CASE("Lower cross join to IR") {
    auto program = require_parse("a cross join b;");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Cross);
    REQUIRE(join->keys().empty());
}

TEST_CASE("Lower asof join to IR") {
    auto program = require_parse("a asof join b on {ts, symbol};");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* join = as_node<ir::JoinNode>(result->get());
    REQUIRE(join != nullptr);
    REQUIRE(join->kind() == ir::JoinKind::Asof);
    REQUIRE(join->keys().size() == 2);
}

TEST_CASE("Lower select with multiple plain columns") {
    auto program = require_parse("df[select { price, symbol, qty }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 3);
    REQUIRE(project->columns()[0].name == "price");
    REQUIRE(project->columns()[1].name == "symbol");
    REQUIRE(project->columns()[2].name == "qty");
}

TEST_CASE("Lower order with multiple keys") {
    auto program = require_parse("df[order { symbol asc, price desc }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* order = as_node<ir::OrderNode>(result->get());
    REQUIRE(order != nullptr);
    REQUIRE(order->keys().size() == 2);
    REQUIRE(order->keys()[0].name == "symbol");
    REQUIRE(order->keys()[0].ascending);
    REQUIRE(order->keys()[1].name == "price");
    REQUIRE_FALSE(order->keys()[1].ascending);
}

TEST_CASE("Lower distinct with braces") {
    auto program = require_parse("df[distinct { a, b, c }];");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* distinct = as_node<ir::DistinctNode>(result->get());
    REQUIRE(distinct != nullptr);
    REQUIRE(distinct->children().size() == 1);
    const auto* project = as_node<ir::ProjectNode>(distinct->children()[0].get());
    REQUIRE(project != nullptr);
    REQUIRE(project->columns().size() == 3);
}

TEST_CASE("Lower let-bound table reuse preserves full child pipeline") {
    auto program = require_parse(R"(
let enriched = trades[update { x = price * 2 }];
enriched[filter x > 10, select { x }];
)");
    auto result = parser::lower(program);
    REQUIRE(result.has_value());

    const auto* project = as_node<ir::ProjectNode>(result->get());
    REQUIRE(project != nullptr);
    REQUIRE(project->children().size() == 1);

    const auto* filter = as_node<ir::FilterNode>(project->children()[0].get());
    REQUIRE(filter != nullptr);
    REQUIRE(filter->children().size() == 1);

    const auto* update = as_node<ir::UpdateNode>(filter->children()[0].get());
    REQUIRE(update != nullptr);
    REQUIRE(update->children().size() == 1);
    REQUIRE(update->children()[0] != nullptr);

    const auto* scan = as_node<ir::ScanNode>(update->children()[0].get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "trades");
}
