// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/required_columns.hpp>
#include <ibex/ir/scan_predicates.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
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
                                               std::vector<ir::JoinKey>{"id"});
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

TEST_CASE("required_columns: mapped join demands each side's own key", "[ir][required_columns]") {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{2}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{{"left_id", "right_id"}});
    join->add_child(make_scan("left"));
    join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{9}, "right"));
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"a"})), std::move(join));
    auto demand = ir::required_columns(*plan);

    CHECK(demand.at("left").names == std::set<std::string>{"a", "left_id"});
    CHECK(demand.at("right").names == std::set<std::string>{"a", "right_id"});
}

TEST_CASE("required_columns: a folded logical key maps to both native inputs",
          "[ir][required_columns]") {
    auto join = std::make_unique<ir::JoinNode>(
        ir::NodeId{2}, ir::JoinKind::Inner,
        std::vector<ir::JoinKey>{{"left_id", "right_id", true, "id"}});
    join->add_child(make_scan("left"));
    join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{9}, "right"));
    auto plan =
        with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"id"})), std::move(join));
    auto demand = ir::required_columns(*plan);

    CHECK(demand.at("left").names == std::set<std::string>{"left_id"});
    CHECK(demand.at("right").names == std::set<std::string>{"right_id"});
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
                                               std::vector<ir::JoinKey>{"id"});
    join->add_child(
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t")));
    join->add_child(with_child(std::make_unique<ir::FilterNode>(ir::NodeId{4}, gt_zero("b")),
                               std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")));

    CHECK_FALSE(ir::scan_predicates(*join).contains("t"));
}

TEST_CASE("scan_predicates_by_occurrence: keeps two occurrences of one source apart",
          "[ir][scan_predicates]") {
    // The same shape the name-keyed map has to reject wholesale: one source,
    // two scans, each wanting different rows. Per occurrence the two predicates
    // are separable -- which is what the later phases of
    // plans/per-occurrence-scan-selections-plan.md need.
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{"id"});
    join->add_child(
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t")));
    join->add_child(with_child(std::make_unique<ir::FilterNode>(ir::NodeId{4}, gt_zero("b")),
                               std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")));

    const auto occurrences = ir::scan_predicates_by_occurrence(*join);
    REQUIRE(occurrences.size() == 2);
    CHECK(occurrences[0].source == "t");
    CHECK(occurrences[1].source == "t");
    // Distinct scan identities, and one conjunct each rather than both pooled.
    CHECK(occurrences[0].scan != occurrences[1].scan);
    CHECK(occurrences[0].conjuncts.size() == 1);
    CHECK(occurrences[1].conjuncts.size() == 1);

    // The name-keyed view still declines, byte for byte as before: the table
    // registry cannot give two occurrences of one name different rows.
    CHECK_FALSE(ir::scan_predicates(*join).contains("t"));
}

TEST_CASE("scan_predicates_by_occurrence: records an unfiltered occurrence",
          "[ir][scan_predicates]") {
    // An occurrence that wants the whole table is exactly the one a pushed
    // filter would corrupt, so it must be visible even with no conjuncts.
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{"id"});
    join->add_child(
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t")));
    join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t"));

    const auto occurrences = ir::scan_predicates_by_occurrence(*join);
    REQUIRE(occurrences.size() == 2);
    const auto filtered = std::ranges::count_if(
        occurrences, [](const ir::ScanOccurrence& o) { return !o.conjuncts.empty(); });
    CHECK(filtered == 1);
    CHECK_FALSE(ir::scan_predicates(*join).contains("t"));
}

TEST_CASE("scan_predicates_by_occurrence: agrees with the name-keyed map on one occurrence",
          "[ir][scan_predicates]") {
    // `projected_scan` reaches a scan through column-only Project/Rename nodes,
    // never through another Filter, so with the stack intact only the inner
    // filter is a candidate. A real plan never reaches this pass in that shape:
    // canonicalize R19 (`try_filter_merge`) has already rewritten
    // `Filter(p1, Filter(p2, x))` into `Filter(p1 AND p2, x)`, and then both
    // conjuncts push. This builds the IR by hand precisely to bypass that and
    // exercise the single-occurrence path directly.
    //
    // What is pinned is the Phase 1 claim: wherever the name-keyed map still
    // answers, the per-occurrence view agrees with it exactly.
    auto plan = with_child(
        std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")),
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{3}, gt_zero("b")), make_scan("t")));

    const auto occurrences = ir::scan_predicates_by_occurrence(*plan);
    REQUIRE(occurrences.size() == 1);
    CHECK(occurrences.front().source == "t");
    CHECK(occurrences.front().conjuncts.size() == 1);

    const auto predicates = ir::scan_predicates(*plan);
    REQUIRE(predicates.contains("t"));
    CHECK(predicates.at("t").size() == occurrences.front().conjuncts.size());
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

TEST_CASE("scan_predicates: removes a fully applied filter while retaining projection",
          "[ir][scan_predicates]") {
    auto filter = with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("predicate")),
                             make_scan("t"));
    auto plan = with_child(std::make_unique<ir::ProjectNode>(ir::NodeId{3}, refs({"payload"})),
                           std::move(filter));

    auto rewritten = ir::remove_applied_scan_filters(std::move(plan), {"t"});
    REQUIRE(rewritten->kind() == ir::NodeKind::Project);
    const auto& project = static_cast<const ir::ProjectNode&>(*rewritten);
    REQUIRE(project.columns().size() == 1);
    CHECK(project.columns().front().name == "payload");
    REQUIRE(rewritten->children().size() == 1);
    REQUIRE(rewritten->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("scan_predicates: a repeated scan is not pushed and is counted",
          "[ir][scan_predicates]") {
    // A self-join: each side filters `t` differently. Neither filter is
    // pushable (applying one occurrence's selection to the shared decode would
    // be unsound), so the source is decoded whole once and the filters stay as
    // plan nodes.
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{"id"});
    join->add_child(
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t")));
    join->add_child(with_child(std::make_unique<ir::FilterNode>(ir::NodeId{4}, gt_zero("b")),
                               std::make_unique<ir::ScanNode>(ir::NodeId{3}, "t")));

    CHECK_FALSE(ir::scan_predicates(*join).contains("t"));
    CHECK(ir::scan_source_counts(*join).at("t") == 2);
}

TEST_CASE("scan_predicates: a source scanned once is pushed", "[ir][scan_predicates]") {
    auto plan =
        with_child(std::make_unique<ir::FilterNode>(ir::NodeId{2}, gt_zero("a")), make_scan("t"));

    REQUIRE(ir::scan_predicates(*plan).contains("t"));
    CHECK(ir::scan_source_counts(*plan).at("t") == 1);
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
                                               std::vector<ir::JoinKey>{std::move(key)});
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

TEST_CASE("deferrable_probe_scans: an unfiltered build side is declined",
          "[ir][scan_predicates][deferred_scan]") {
    // The row-count gate alone says yes -- 1.5M build rows against 12M probe
    // rows clears `build * 2 < probe` three times over. But every probe key
    // references a real build key, so the published filter rejects nothing and
    // the whole apparatus is a pure loss. What separates this from a genuinely
    // reduced build side is COVERAGE of the key's domain: this build side still
    // carries all 1.5M rows of its own table.
    const ir::SourceRowCounts rows{{"orders", 1500000}, {"lineitem", 12000000}};
    ir::SourceSchemas schemas;
    schemas.emplace(
        "orders",
        ir::SchemaInfo::known({ir::SchemaField{
            .name = "id", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe}}));
    schemas.emplace(
        "lineitem",
        ir::SchemaInfo::known({ir::SchemaField{
            .name = "id", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe}}));

    auto plan = inner_join(std::make_unique<ir::ScanNode>(ir::NodeId{1}, "orders"),
                           std::make_unique<ir::ScanNode>(ir::NodeId{2}, "lineitem"), "id");
    CHECK_FALSE(
        ir::deferrable_probe_scans(*plan, {"lineitem"}, rows, schemas).contains("lineitem"));
}

TEST_CASE("deferrable_probe_scans: a filter absorbed into the build scan still counts",
          "[ir][scan_predicates][deferred_scan]") {
    // Same shape, but the build side's filter was fused into its scan and its
    // Filter node deleted. Without the absorbed selectivity the build side
    // reads back its whole table and is indistinguishable from the case above;
    // with it, the build side is a fraction of `orders` and deferring pays.
    const ir::SourceRowCounts rows{{"orders", 1500000}, {"lineitem", 12000000}};
    ir::SourceSchemas schemas;
    schemas.emplace(
        "orders",
        ir::SchemaInfo::known({ir::SchemaField{
            .name = "id", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe}}));
    schemas.emplace(
        "lineitem",
        ir::SchemaInfo::known({ir::SchemaField{
            .name = "id", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe}}));

    auto plan = inner_join(std::make_unique<ir::ScanNode>(ir::NodeId{1}, "orders"),
                           std::make_unique<ir::ScanNode>(ir::NodeId{2}, "lineitem"), "id");
    const std::map<std::string, double> absorbed{{"orders", 0.05}};
    CHECK(ir::deferrable_probe_scans(*plan, {"lineitem"}, rows, schemas, absorbed)
              .contains("lineitem"));
}

TEST_CASE("deferrable_probe_scans: the no-absorbed and with-absorbed calls can disagree "
          "on a small probe -- guards the prove_unique_columns caching window",
          "[ir][scan_predicates][deferred_scan]") {
    // repl.cpp's `optimize_and_execute_plan` calls deferrable_probe_scans TWICE:
    //   1. before the join reorder, with absorbed={}, to decide which join-key
    //      columns `prove_unique_columns` may leave in the LazyTable cache_
    //      (a cached key column disables the fused dynamic-key scan, so a
    //      column that WILL be a deferred probe must stay uncached).
    //   2. after the reorder and filter absorption, with the real selectivity
    //      map, to register the DeferredScan for real.
    // These disagree exactly here: without the absorbed 0.05 the build side
    // reads back its whole `dim` table and the size gate declines the probe, so
    // call 1 lets `prove_unique_columns` cache `probe.k`; call 2 then wants a
    // fused dynamic-key scan on that now-cached column and silently gets a
    // whole-table decode instead.
    //
    // It stays a BOUNDED regression only because `prove_unique_columns` has a
    // ~1M-row cap: `probe` here is small enough (500k) to be proven and cached,
    // but the deferred probes that actually matter (PDS-H lineitem/orders) are
    // far over the cap and never touched by the proof. Raising that cap, or
    // caching probe keys, or sharing a LazyTable across plans, widens the
    // window -- and should light this test up as the spec of the gap.
    const ir::SourceRowCounts rows{{"dim", 400000}, {"probe", 500000}};
    ir::SourceSchemas schemas;
    schemas.emplace("dim", ir::SchemaInfo::known({ir::SchemaField{
                               .name = "k",
                               .type = ir::ColumnType::Int64,
                               .nulls = ir::Nullability::Maybe}}));
    schemas.emplace("probe", ir::SchemaInfo::known({ir::SchemaField{
                                 .name = "k",
                                 .type = ir::ColumnType::Int64,
                                 .nulls = ir::Nullability::Maybe}}));

    auto plan = inner_join(std::make_unique<ir::ScanNode>(ir::NodeId{1}, "dim"),
                           std::make_unique<ir::ScanNode>(ir::NodeId{2}, "probe"), "k");

    // Call 1 (what `prove_unique_columns` sees): declined -> `probe.k` is cached.
    CHECK_FALSE(ir::deferrable_probe_scans(*plan, {"probe"}, rows, schemas).contains("probe"));
    // Call 2 (the real registration): eligible -> wants a fused scan on `probe.k`.
    const std::map<std::string, double> absorbed{{"dim", 0.10}};
    CHECK(ir::deferrable_probe_scans(*plan, {"probe"}, rows, schemas, absorbed).contains("probe"));
}

TEST_CASE("deferrable_probe_scans: mapped join key defers under the right-side name",
          "[ir][scan_predicates][deferred_scan]") {
    // Q2's p_partkey = ps_partkey join must defer partsupp with its physical
    // ps_partkey name; the runtime maps the left build keys to that name.
    auto plan = std::make_unique<ir::JoinNode>(
        ir::NodeId{20}, ir::JoinKind::Inner,
        std::vector<ir::JoinKey>{ir::JoinKey{"p_partkey", "ps_partkey"}});
    plan->add_child(make_scan("part"));
    plan->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "partsupp"));

    auto deferrable = ir::deferrable_probe_scans(*plan, {"partsupp"});
    REQUIRE(deferrable.contains("partsupp"));
    CHECK(deferrable.at("partsupp").key_column == "ps_partkey");
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

TEST_CASE("deferrable_probe_scans: non-inner, 3+-key, and predicate joins are ineligible",
          "[ir][scan_predicates][deferred_scan]") {
    auto left_join = std::make_unique<ir::JoinNode>(ir::NodeId{5}, ir::JoinKind::Left,
                                                    std::vector<ir::JoinKey>{"id"});
    left_join->add_child(make_scan("build"));
    left_join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));
    CHECK(ir::deferrable_probe_scans(*left_join, {"t"}).empty());

    // Two keys is eligible (see the positive case below, "deferred scan
    // filtering for two-key joins" -- the runtime pushes a filter over the
    // FIRST component only). Three or more stays out of scope: the runtime's
    // `is_streamable_pair_int_join` only ever recognizes exactly two.
    auto three_key = std::make_unique<ir::JoinNode>(ir::NodeId{6}, ir::JoinKind::Inner,
                                                    std::vector<ir::JoinKey>{"id", "id2", "id3"});
    three_key->add_child(make_scan("build"));
    three_key->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));
    CHECK(ir::deferrable_probe_scans(*three_key, {"t"}).empty());

    auto with_pred = std::make_unique<ir::JoinNode>(ir::NodeId{7}, ir::JoinKind::Inner,
                                                    std::vector<ir::JoinKey>{"id"}, gt_zero("a"));
    with_pred->add_child(make_scan("build"));
    with_pred->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));
    CHECK(ir::deferrable_probe_scans(*with_pred, {"t"}).empty());
}

TEST_CASE("deferrable_probe_scans: a two-key inner join is eligible on its first component",
          "[ir][scan_predicates][deferred_scan]") {
    auto two_key = std::make_unique<ir::JoinNode>(ir::NodeId{6}, ir::JoinKind::Inner,
                                                  std::vector<ir::JoinKey>{"id", "id2"});
    two_key->add_child(make_scan("build"));
    two_key->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{2}, "t"));

    auto deferrable = ir::deferrable_probe_scans(*two_key, {"t"});
    REQUIRE(deferrable.contains("t"));
    CHECK(deferrable.at("t").key_column == "id");
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
