#include <ibex/ir/node.hpp>
#include <ibex/ir/required_columns.hpp>
#include <ibex/ir/scan_predicates.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto col(std::string name) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}});
}

auto refs(std::vector<std::string> names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> out;
    out.reserve(names.size());
    for (auto& name : names) {
        out.push_back(ir::ColumnRef{.name = std::move(name), .source = {0}});
    }
    return out;
}

auto make_scan(std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(ir::NodeId{1}, std::move(name));
}

/// `<column> > 0`
auto gt_zero(std::string name) -> ir::Expr {
    auto lit = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}});
    return ir::Expr{.node = ir::CompareExpr{.op = ir::CompareOp::Gt,
                                            .left = col(std::move(name)),
                                            .right = std::move(lit)}};
}

auto with_child(ir::NodePtr parent, ir::NodePtr child) -> ir::NodePtr {
    parent->add_child(std::move(child));
    return parent;
}

}  // namespace

TEST_CASE("required_columns: a bare scan demands every column", "[ir][required_columns]") {
    auto plan = make_scan("t");
    auto demand = ir::required_columns(*plan);

    REQUIRE(demand.contains("t"));
    CHECK(demand.at("t").all);
}

TEST_CASE("required_columns: project fixes the demand to its own columns",
          "[ir][required_columns]") {
    auto plan = with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{2}, refs({"a", "b"})),
                           make_scan("t"));
    auto demand = ir::required_columns(*plan);

    REQUIRE(demand.contains("t"));
    CHECK_FALSE(demand.at("t").all);
    CHECK(demand.at("t").names == std::set<std::string>{"a", "b"});
}

TEST_CASE("required_columns: a filter adds the columns its predicate reads",
          "[ir][required_columns]") {
    auto plan = with_child(
        std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"a"})),
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("b")), make_scan("t")));
    auto demand = ir::required_columns(*plan);

    // `b` is never in the output, but the scan must still supply it.
    CHECK(demand.at("t").names == std::set<std::string>{"a", "b"});
}

TEST_CASE("required_columns: aggregate demands its group keys and aggregated columns",
          "[ir][required_columns]") {
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(ir::AggSpec{
        .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "amount"}, .alias = "total"});
    auto plan = with_child(
        std::make_unique<ir::AggregateNode>(ir::NodeId{2}, refs({"region"}), std::move(aggs)),
        make_scan("t"));
    auto demand = ir::required_columns(*plan);

    CHECK_FALSE(demand.at("t").all);
    CHECK(demand.at("t").names == std::set<std::string>{"region", "amount"});
}

TEST_CASE("required_columns: count() alone demands no column", "[ir][required_columns]") {
    // count() carries no ColumnRef, so an unfiltered row count reads nothing —
    // the row count comes from the source's metadata.
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(
        ir::AggSpec{.func = ir::AggFunc::Count, .column = ir::ColumnRef{.name = ""}, .alias = "n"});
    auto plan =
        with_child(std::make_unique<ir::AggregateNode>(ir::NodeId{2}, refs({}), std::move(aggs)),
                   make_scan("t"));
    auto demand = ir::required_columns(*plan);

    CHECK_FALSE(demand.at("t").all);
    CHECK(demand.at("t").names.empty());
}

TEST_CASE("required_columns: order adds its sort keys", "[ir][required_columns]") {
    std::vector<ir::OrderKey> keys;
    keys.push_back(ir::OrderKey{.name = "ts", .ascending = true});
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"a"})),
                   with_child(std::make_unique<ir::OrderNode>(ir::NodeId{2}, std::move(keys)),
                              make_scan("t")));
    auto demand = ir::required_columns(*plan);

    CHECK(demand.at("t").names == std::set<std::string>{"a", "ts"});
}

TEST_CASE("required_columns: update reads its inputs, not the names it produces",
          "[ir][required_columns]") {
    std::vector<ir::FieldSpec> fields;
    fields.push_back(
        ir::FieldSpec{.alias = "b", .expr = ir::Expr{.node = ir::ColumnRef{.name = "a"}}});
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"b"})),
                   with_child(std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(fields)),
                              make_scan("t")));
    auto demand = ir::required_columns(*plan);

    // `b` is produced by the update, so it is not demanded from the scan; `a` is.
    CHECK(demand.at("t").names == std::set<std::string>{"a"});
}

TEST_CASE("required_columns: rename maps a demanded name back to its source name",
          "[ir][required_columns]") {
    std::vector<ir::RenameSpec> renames;
    renames.push_back(ir::RenameSpec{.new_name = "b", .old_name = "a"});
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"b"})),
                   with_child(std::make_unique<ir::RenameNode>(ir::NodeId{2}, std::move(renames)),
                              make_scan("t")));
    auto demand = ir::required_columns(*plan);

    CHECK(demand.at("t").names == std::set<std::string>{"a"});
}

TEST_CASE("required_columns: join demands its keys from both sides", "[ir][required_columns]") {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{2}, ir::JoinKind::Inner,
                                               std::vector<std::string>{"id"});
    join->add_child(make_scan("left"));
    join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{9}, "right"));
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"a"})), std::move(join));
    auto demand = ir::required_columns(*plan);

    // The demand is the union across both sides; a name absent from one side's
    // schema is simply not read there.
    CHECK(demand.at("left").names == std::set<std::string>{"a", "id"});
    CHECK(demand.at("right").names == std::set<std::string>{"a", "id"});
}

TEST_CASE("required_columns: distinct cannot be narrowed", "[ir][required_columns]") {
    // Distinct de-duplicates over every input column: narrowing its input would
    // change which rows survive, not just which columns come back.
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"a"})),
                   with_child(std::make_unique<ir::DistinctNode>(ir::NodeId{2}), make_scan("t")));
    auto demand = ir::required_columns(*plan);

    CHECK(demand.at("t").all);
}

TEST_CASE("required_columns: an unmodelled node widens rather than under-reading",
          "[ir][required_columns]") {
    // Cov reads every numeric column and names none of them. The pass must not
    // conclude the scan is unused.
    auto plan = with_child(std::make_unique<ir::CovNode>(ir::NodeId{2}), make_scan("t"));
    auto demand = ir::required_columns(*plan);

    REQUIRE(demand.contains("t"));
    CHECK(demand.at("t").all);
}

TEST_CASE("required_columns: a source the plan never scans is absent", "[ir][required_columns]") {
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{2}, refs({"a"})), make_scan("t"));
    auto demand = ir::required_columns(*plan);

    CHECK_FALSE(demand.contains("other"));
}

TEST_CASE("scan_predicates: splits row-local conjuncts directly above a scan",
          "[ir][scan_predicates]") {
    ir::Expr predicate{.node = ir::LogicalExpr{.op = ir::LogicalOp::And,
                                               .left = ir::make_expr_ptr(gt_zero("a")),
                                               .right = ir::make_expr_ptr(gt_zero("b"))}};
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"payload"})),
                   with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, std::move(predicate)),
                              make_scan("t")));

    auto predicates = ir::scan_predicates(*plan);
    REQUIRE(predicates.contains("t"));
    CHECK(predicates.at("t").size() == 2);
}

TEST_CASE("scan_predicates: rejects a filter containing a non-local conjunct",
          "[ir][scan_predicates]") {
    ir::CallExpr lag;
    lag.callee = "lag";
    lag.args.push_back(col("a"));
    ir::Expr non_local{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Gt,
            .left = ir::make_expr_ptr(ir::Expr{.node = std::move(lag)}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})}};
    ir::Expr predicate{.node = ir::LogicalExpr{.op = ir::LogicalOp::And,
                                               .left = ir::make_expr_ptr(std::move(non_local)),
                                               .right = ir::make_expr_ptr(gt_zero("b"))}};
    auto plan = with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, std::move(predicate)),
                           make_scan("t"));

    auto predicates = ir::scan_predicates(*plan);
    CHECK_FALSE(predicates.contains("t"));
}

TEST_CASE("scan_predicates: repeated source scans are not selected globally",
          "[ir][scan_predicates]") {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Inner,
                                               std::vector<std::string>{"id"});
    join->add_child(
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t")));
    join->add_child(with_child(std::make_unique<ir::FilterNode>(ir::NodeId{4}, gt_zero("b")),
                               std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")));

    CHECK_FALSE(ir::scan_predicates(*join).contains("t"));
}

TEST_CASE("scan_predicates: reaches a scan through a column-only projection",
          "[ir][scan_predicates]") {
    auto plan = with_child(
        std::make_unique<ir::FilterNode>(ir::NodeId{3}, gt_zero("predicate")),
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{2}, refs({"predicate", "payload"})),
                   make_scan("t")));

    auto predicates = ir::scan_predicates(*plan);
    REQUIRE(predicates.contains("t"));
    CHECK(predicates.at("t").size() == 1);
}

TEST_CASE("scan_predicates: removes a fully applied fused filter while retaining projection",
          "[ir][scan_predicates]") {
    auto plan = std::make_unique<ir::FilterProjectNode>(ir::NodeId{2}, gt_zero("predicate"),
                                                        refs({"payload"}));
    plan->add_child(make_scan("t"));

    auto rewritten = ir::remove_applied_scan_filters(std::move(plan), {"t"});
    REQUIRE(rewritten->kind() == ir::NodeKind::Project);
    const auto& project = static_cast<const ir::ProjectNode&>(*rewritten);
    REQUIRE(project.columns().size() == 1);
    CHECK(project.columns().front().name == "payload");
    REQUIRE(rewritten->children().size() == 1);
    REQUIRE(rewritten->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("split_scan_instances: repeated scans get per-instance identity",
          "[ir][scan_predicates]") {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Inner,
                                               std::vector<std::string>{"id"});
    join->add_child(
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t")));
    join->add_child(with_child(std::make_unique<ir::FilterNode>(ir::NodeId{4}, gt_zero("b")),
                               std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")));

    auto split = ir::split_scan_instances(std::move(join), {"t"});
    REQUIRE(split.instances == std::map<std::string, std::string>{{"t#1", "t"}, {"t#2", "t"}});

    // With per-instance identity, each scan's filter becomes pushable, and
    // demand is tracked per scan.
    auto predicates = ir::scan_predicates(*split.plan);
    REQUIRE(predicates.contains("t#1"));
    REQUIRE(predicates.contains("t#2"));
    CHECK(predicates.at("t#1").size() == 1);
    CHECK(predicates.at("t#2").size() == 1);

    auto demand = ir::required_columns(*split.plan);
    REQUIRE(demand.contains("t#1"));
    REQUIRE(demand.contains("t#2"));
}

TEST_CASE("split_scan_instances: a source scanned once keeps its name", "[ir][scan_predicates]") {
    auto plan =
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t"));

    auto split = ir::split_scan_instances(std::move(plan), {"t"});
    CHECK(split.instances.empty());
    REQUIRE(ir::scan_predicates(*split.plan).contains("t"));
}

TEST_CASE("split_scan_instances: only named sources are split", "[ir][scan_predicates]") {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Inner,
                                               std::vector<std::string>{"id"});
    join->add_child(make_scan("t"));
    join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t"));

    // `t` is not offered for splitting (it is not a lazy source), so the plan
    // is untouched.
    auto split = ir::split_scan_instances(std::move(join), {"other"});
    CHECK(split.instances.empty());
    CHECK_FALSE(ir::scan_predicates(*split.plan).contains("t"));
}

TEST_CASE("scan_predicates: does not remove a non-local filter", "[ir][scan_predicates]") {
    ir::CallExpr lag;
    lag.callee = "lag";
    lag.args.push_back(col("a"));
    ir::Expr predicate{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Gt,
            .left = ir::make_expr_ptr(ir::Expr{.node = std::move(lag)}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})}};
    auto plan = with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, std::move(predicate)),
                           make_scan("t"));

    auto rewritten = ir::remove_applied_scan_filters(std::move(plan), {"t"});
    CHECK(rewritten->kind() == ir::NodeKind::Filter);
}

namespace {

auto inner_join(ir::NodePtr left, ir::NodePtr right, std::string key) -> ir::NodePtr {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{20}, ir::JoinKind::Inner,
                                               std::vector<std::string>{std::move(key)});
    join->add_child(std::move(left));
    join->add_child(std::move(right));
    return join;
}

}  // namespace

TEST_CASE("deferrable_probe_scans: bare right-side scan of an inner join is eligible",
          "[ir][scan_predicates][deferred_scan]") {
    auto plan =
        inner_join(make_scan("build"), std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"), "id");

    auto deferrable = ir::deferrable_probe_scans(*plan, {"t"});
    REQUIRE(deferrable.contains("t"));
    CHECK(deferrable.at("t").key_column == "id");
    // The left (build) side is never deferrable.
    CHECK_FALSE(ir::deferrable_probe_scans(*plan, {"build"}).contains("build"));
}

TEST_CASE("deferrable_probe_scans: reaches the scan through project and rename, mapping the key",
          "[ir][scan_predicates][deferred_scan]") {
    // Join key `o_orderkey`; the scan calls it `l_orderkey`.
    auto rename = std::make_unique<ir::RenameNode>(
        ir::NodeId{3},
        std::vector<ir::RenameSpec>{{.new_name = "o_orderkey", .old_name = "l_orderkey"}});
    auto plan = inner_join(
        make_scan("build"),
        with_child(std::move(rename),
                   with_child(std::make_unique<ir::ProjectNode>(
                                  ir::NodeId{2}, refs({"l_orderkey", "l_extendedprice"})),
                              std::make_unique<ir::ScanNode>(ir::NodeId{4}, "lineitem"))),
        "o_orderkey");

    auto deferrable = ir::deferrable_probe_scans(*plan, {"lineitem"});
    REQUIRE(deferrable.contains("lineitem"));
    CHECK(deferrable.at("lineitem").key_column == "l_orderkey");
}

TEST_CASE("deferrable_probe_scans: a residual filter in the chain blocks eligibility",
          "[ir][scan_predicates][deferred_scan]") {
    auto plan = inner_join(make_scan("build"),
                           with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")),
                                      std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")),
                           "id");

    CHECK(ir::deferrable_probe_scans(*plan, {"t"}).empty());
}

TEST_CASE("deferrable_probe_scans: a scan consumed twice is never deferred",
          "[ir][scan_predicates][deferred_scan]") {
    // `t` is both the probe side and (filtered) the build side — reducing the
    // probe instance would be unsound without per-instance identity.
    auto plan =
        inner_join(make_scan("t"), std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"), "id");

    CHECK(ir::deferrable_probe_scans(*plan, {"t"}).empty());
}

TEST_CASE("deferrable_probe_scans: non-inner, multi-key, and predicate joins are ineligible",
          "[ir][scan_predicates][deferred_scan]") {
    auto left_join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Left,
                                                    std::vector<std::string>{"id"});
    left_join->add_child(make_scan("build"));
    left_join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));
    CHECK(ir::deferrable_probe_scans(*left_join, {"t"}).empty());

    auto two_key = std::make_unique<ir::JoinNode>(ir::NodeId{6}, ir::JoinKind::Inner,
                                                  std::vector<std::string>{"id", "id2"});
    two_key->add_child(make_scan("build"));
    two_key->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));
    CHECK(ir::deferrable_probe_scans(*two_key, {"t"}).empty());

    auto with_pred = std::make_unique<ir::JoinNode>(ir::NodeId{7}, ir::JoinKind::Inner,
                                                    std::vector<std::string>{"id"}, gt_zero("a"));
    with_pred->add_child(make_scan("build"));
    with_pred->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));
    CHECK(ir::deferrable_probe_scans(*with_pred, {"t"}).empty());
}

TEST_CASE("deferrable_probe_scans: a project that drops the join key blocks eligibility",
          "[ir][scan_predicates][deferred_scan]") {
    auto plan =
        inner_join(make_scan("build"),
                   with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{2}, refs({"payload"})),
                              std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")),
                   "id");

    CHECK(ir::deferrable_probe_scans(*plan, {"t"}).empty());
}

TEST_CASE("deferrable_probe_scans: only offered sources are returned",
          "[ir][scan_predicates][deferred_scan]") {
    auto plan =
        inner_join(make_scan("build"), std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"), "id");

    CHECK(ir::deferrable_probe_scans(*plan, {"other"}).empty());
}
