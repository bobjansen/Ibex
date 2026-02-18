#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>

#include <catch2/catch_test_macros.hpp>

TEST_CASE("Builder creates nodes with unique IDs", "[ir][builder]") {
    ibex::ir::Builder builder;

    auto scan = builder.scan("prices");
    auto filter = builder.filter(ibex::ir::FilterPredicate{
        .column = {.name = "price"},
        .op = ibex::ir::CompareOp::Gt,
        .value = 100.0,
    });

    REQUIRE(scan->id() != filter->id());
    REQUIRE(scan->kind() == ibex::ir::NodeKind::Scan);
    REQUIRE(filter->kind() == ibex::ir::NodeKind::Filter);
}

TEST_CASE("ScanNode stores source name", "[ir][scan]") {
    ibex::ir::Builder builder;
    auto node = builder.scan("trades");

    auto* scan = dynamic_cast<ibex::ir::ScanNode*>(node.get());
    REQUIRE(scan != nullptr);
    REQUIRE(scan->source_name() == "trades");
}

TEST_CASE("FilterNode stores predicate", "[ir][filter]") {
    ibex::ir::Builder builder;

    auto node = builder.filter(ibex::ir::FilterPredicate{
        .column = {.name = "volume"},
        .op = ibex::ir::CompareOp::Ge,
        .value = std::int64_t{1000},
    });

    auto* filter_node = dynamic_cast<ibex::ir::FilterNode*>(node.get());
    REQUIRE(filter_node != nullptr);
    REQUIRE(filter_node->predicate().column.name == "volume");
    REQUIRE(filter_node->predicate().op == ibex::ir::CompareOp::Ge);
}

TEST_CASE("ProjectNode stores column list", "[ir][project]") {
    ibex::ir::Builder builder;

    auto node = builder.project({
        ibex::ir::ColumnRef{.name = "symbol"},
        ibex::ir::ColumnRef{.name = "price"},
    });

    auto* proj = dynamic_cast<ibex::ir::ProjectNode*>(node.get());
    REQUIRE(proj != nullptr);
    REQUIRE(proj->columns().size() == 2);
    REQUIRE(proj->columns()[0].name == "symbol");
}

TEST_CASE("AggregateNode stores group-by and aggregations", "[ir][aggregate]") {
    ibex::ir::Builder builder;

    auto node = builder.aggregate(
        {ibex::ir::ColumnRef{.name = "symbol"}},
        {ibex::ir::AggSpec{
            .func = ibex::ir::AggFunc::Sum,
            .column = {.name = "volume"},
            .alias = "total_volume",
        }});

    auto* agg = dynamic_cast<ibex::ir::AggregateNode*>(node.get());
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    REQUIRE(agg->aggregations().size() == 1);
    REQUIRE(agg->aggregations()[0].alias == "total_volume");
}

TEST_CASE("Nodes can form a tree via add_child", "[ir][tree]") {
    ibex::ir::Builder builder;

    auto scan = builder.scan("orders");
    auto filter = builder.filter(ibex::ir::FilterPredicate{
        .column = {.name = "status"},
        .op = ibex::ir::CompareOp::Eq,
        .value = std::string{"filled"},
    });

    auto scan_id = scan->id();
    filter->add_child(std::move(scan));

    REQUIRE(filter->children().size() == 1);
    REQUIRE(filter->children()[0]->id() == scan_id);
}
