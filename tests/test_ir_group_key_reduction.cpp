// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/group_key_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto scan(std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(ir::NodeId{1}, std::move(name));
}

auto scan_id(std::string name, std::uint64_t id) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(ir::NodeId{id}, std::move(name));
}

/// A source schema, optionally with `unique` proved unique.
auto source(std::vector<std::string> names, const std::string& unique = {}) -> ir::SchemaInfo {
    std::vector<ir::SchemaField> fields;
    fields.reserve(names.size());
    for (auto& name : names) {
        fields.push_back(ir::SchemaField{.name = std::move(name), .type = std::nullopt});
    }
    auto info = ir::SchemaInfo::known(std::move(fields));
    if (!unique.empty()) {
        info.add_unique_key(ir::UniqueKey{unique});
    }
    return info;
}

auto refs(std::vector<std::string> names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> out;
    out.reserve(names.size());
    for (auto& name : names) {
        out.push_back(ir::ColumnRef{.name = std::move(name), .source = {0}});
    }
    return out;
}

auto sum_of(std::string column, std::string alias) -> std::vector<ir::AggSpec> {
    return {ir::AggSpec{.func = ir::AggFunc::Sum,
                        .column = ir::ColumnRef{.name = std::move(column)},
                        .alias = std::move(alias)}};
}

auto aggregate(std::vector<std::string> keys, ir::NodePtr input) -> ir::NodePtr {
    auto node = std::make_unique<ir::AggregateNode>(ir::NodeId{9}, refs(std::move(keys)),
                                                    sum_of("v", "total"));
    node->add_child(std::move(input));
    return node;
}

/// Wrap `input` in `order { by desc } head n` -- the top-k the payload lift
/// requires above the aggregate.
auto topk(ir::NodePtr input, const std::string& by, std::size_t n) -> ir::NodePtr {
    auto order = std::make_unique<ir::OrderNode>(
        ir::NodeId{20}, std::vector<ir::OrderKey>{ir::OrderKey{.name = by, .ascending = false}});
    order->add_child(std::move(input));
    auto head = std::make_unique<ir::HeadNode>(ir::NodeId{21}, n);
    head->add_child(std::move(order));
    return head;
}

/// The same top-k as canonicalize fuses it (R16): one `TopK` node.
auto fused_topk(ir::NodePtr input, const std::string& by, std::size_t n,
                ir::TopKNode::KeepMode mode = ir::TopKNode::KeepMode::First) -> ir::NodePtr {
    auto node = std::make_unique<ir::TopKNode>(
        ir::NodeId{22}, std::vector<ir::OrderKey>{ir::OrderKey{.name = by, .ascending = false}}, n,
        std::vector<ir::ColumnRef>{}, mode);
    node->add_child(std::move(input));
    return node;
}

/// The JoinNode in `plan`, or nullptr -- the lift's re-fetch.
// NOLINTNEXTLINE(misc-no-recursion)
auto find_join(const ir::Node& plan) -> const ir::JoinNode* {
    if (plan.kind() == ir::NodeKind::Join) {
        return static_cast<const ir::JoinNode*>(&plan);
    }
    for (const auto& child : plan.children()) {
        if (child != nullptr) {
            if (const auto* found = find_join(*child)) {
                return found;
            }
        }
    }
    return nullptr;
}

/// The AggregateNode in `plan`, wherever the pass left it.
auto find_aggregate(const ir::Node& plan) -> const ir::AggregateNode* {
    if (plan.kind() == ir::NodeKind::Aggregate) {
        return static_cast<const ir::AggregateNode*>(&plan);
    }
    for (const auto& child : plan.children()) {
        if (child != nullptr) {
            if (const auto* found = find_aggregate(*child)) {
                return found;
            }
        }
    }
    return nullptr;
}

}  // namespace

TEST_CASE("group key reduction drops keys a unique key determines") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_phone", "v"}, "c_custkey"));

    auto plan = aggregate({"c_custkey", "c_name", "c_phone"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_custkey");
    // The dropped keys come back as `first()`, exact because they are constant
    // within a group.
    REQUIRE(agg->aggregations().size() == 3);
    CHECK(agg->aggregations()[1].func == ir::AggFunc::First);
    CHECK(agg->aggregations()[1].alias == "c_name");
}

TEST_CASE("group key reduction restores the original column order") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto plan = aggregate({"c_custkey", "c_name"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    // The aggregate emits keys then aggregates, so without a projection the
    // reduction would reorder the result for a positional reader.
    REQUIRE(plan->kind() == ir::NodeKind::Project);
    const auto& columns = static_cast<const ir::ProjectNode&>(*plan).columns();
    REQUIRE(columns.size() == 3);
    CHECK(columns[0].name == "c_custkey");
    CHECK(columns[1].name == "c_name");
    CHECK(columns[2].name == "total");
}

TEST_CASE("group key reduction carries a dependency across a join, transitively") {
    // q10's shape: c_custkey -> c_nationkey inside customer, then across the
    // join to n_name because n_nationkey is unique in nation.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_nationkey", "v"}, "c_custkey"));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Inner, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"c_custkey", "c_name", "n_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_custkey");
}

TEST_CASE("group key reduction declines without a proved unique key") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}));  // nothing proved

    auto plan = aggregate({"c_custkey", "c_name"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction declines across a right join") {
    // An unmatched RIGHT row carries a null left key, so two such rows share a
    // key value while differing in the right table's columns.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_nationkey", "v"}, "c_custkey"));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Right, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"c_custkey", "n_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction does not carry a dependency backwards across a left join") {
    // Although c_custkey is unique, n_nationkey does not determine customer
    // columns after a left join: every unmatched customer has the same null
    // n_nationkey while its c_name may differ. Dropping c_name would merge all
    // those unmatched groups.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_custkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Left, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"n_nationkey", "c_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction carries a dependency forwards across a left join") {
    // The sound direction remains useful: one left key identifies at most one
    // unique right row, with unmatched keys consistently producing nulls.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_nationkey", "v"}));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Left, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"c_nationkey", "n_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_nationkey");
}

TEST_CASE("group key reduction declines when a key has no traceable origin") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    // A computed key has no base column, so nothing can be proved about it.
    std::vector<ir::FieldSpec> fields;
    fields.push_back(ir::FieldSpec{
        .alias = "derived",
        .expr = ir::Expr{
            .node = ir::BinaryExpr{
                .op = ir::ArithmeticOp::Add,
                .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "c_custkey"}}),
                .right = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "v"}})}}});
    auto update = std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(fields));
    update->add_child(scan("customer"));

    auto plan = aggregate({"c_custkey", "derived"}, std::move(update));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction distinguishes self-join occurrences by scan identity") {
    // A self-join on a NON-unique column `k`. `t.pk` is unique in the source,
    // but a non-unique-key self-join fans a single left row out over many right
    // rows, so grouping by (left pk, right name) is a real grouping and the
    // right name must be KEPT. Identifying a base column by source NAME alone
    // conflates the two scans: "pk is unique -> it determines every column of
    // t" then reaches the right occurrence's name and collapses it to first().
    // Scan-node identity keeps the two occurrences apart.
    ir::SourceSchemas sources;
    sources.emplace("t", source({"k", "pk", "name", "v"}, "pk"));

    std::vector<ir::RenameSpec> renames{
        ir::RenameSpec{.new_name = "k2", .old_name = "k"},
        ir::RenameSpec{.new_name = "pk2", .old_name = "pk"},
        ir::RenameSpec{.new_name = "name2", .old_name = "name"},
        ir::RenameSpec{.new_name = "v2", .old_name = "v"},
    };
    auto right = std::make_unique<ir::RenameNode>(ir::NodeId{2}, std::move(renames));
    right->add_child(scan_id("t", 20));

    std::vector<ir::JoinKey> keys{ir::JoinKey{std::string{"k"}, std::string{"k2"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Inner, std::move(keys));
    join->add_child(scan_id("t", 10));
    join->add_child(std::move(right));

    auto plan = aggregate({"pk", "name2"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    CHECK(agg->group_by().size() == 2);
}

TEST_CASE("group key reduction never reduces to no key at all") {
    // Two keys that are the same base column: one is redundant, but dropping
    // both would turn a grouped aggregate into an ungrouped one.
    ir::SourceSchemas sources;
    sources.emplace("t", source({"a", "v"}, "a"));

    std::vector<ir::RenameSpec> renames{ir::RenameSpec{.new_name = "b", .old_name = "a"}};
    auto rename = std::make_unique<ir::RenameNode>(ir::NodeId{2}, std::move(renames));
    rename->add_child(scan("t"));

    auto plan = aggregate({"b"}, std::move(rename));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    CHECK(agg->group_by().size() == 1);
}

TEST_CASE("group key proof candidates include only traceable multi-key aggregate columns") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}));

    auto plan = aggregate({"c_custkey", "c_name"}, scan("customer"));
    const auto candidates = ir::group_key_proof_candidates(*plan, sources);

    REQUIRE(candidates.contains("customer"));
    CHECK(candidates.at("customer") == std::set<std::string>{"c_custkey", "c_name"});

    auto single_key = aggregate({"c_custkey"}, scan("customer"));
    CHECK(ir::group_key_proof_candidates(*single_key, sources).empty());
}

TEST_CASE("group key proof candidates must determine the complete group key") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_nationkey", "v"}));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Inner, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto reducible = aggregate({"c_custkey", "c_name", "n_name"}, std::move(join));
    const auto candidates = ir::group_key_proof_candidates(*reducible, sources);
    REQUIRE(candidates.contains("customer"));
    CHECK(candidates.at("customer") == std::set<std::string>{"c_custkey", "c_name"});

    sources.emplace("orders", source({"o_orderkey", "o_orderdate"}));
    std::vector<ir::JoinKey> unrelated_keys{
        ir::JoinKey{std::string{"c_custkey"}, std::string{"o_orderkey"}}};
    auto unrelated = std::make_unique<ir::JoinNode>(ir::NodeId{4}, ir::JoinKind::Inner,
                                                    std::move(unrelated_keys));
    unrelated->add_child(scan("customer"));
    unrelated->add_child(scan("orders"));
    auto partial = aggregate({"c_custkey", "c_name", "o_orderdate"}, std::move(unrelated));
    CHECK(ir::group_key_proof_candidates(*partial, sources).empty());
}

TEST_CASE("payload lift moves determined columns above a top-k") {
    // q10's shape: the reduction turns c_name/c_phone into first() aggregates,
    // and the lift then moves them out of the aggregate entirely so the scan
    // stops decoding them -- they are needed only for the surviving rows.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_phone", "v"}, "c_custkey"));

    auto plan = topk(aggregate({"c_custkey", "c_name", "c_phone"}, scan("customer")), "total", 20);
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_custkey");
    // Only the real aggregate survives; both first()s were lifted.
    REQUIRE(agg->aggregations().size() == 1);
    CHECK(agg->aggregations().front().alias == "total");

    const auto* join = find_join(*plan);
    REQUIRE(join != nullptr);
    REQUIRE(join->keys().size() == 1);
    CHECK(join->keys().front().left == "c_custkey");
    CHECK(join->keys().front().right == "c_custkey");
    // Folded, or the key would collide with the re-fetch side's own copy.
    CHECK(join->keys().front().fold_output);

    // The caller-visible column order must survive the new shape.
    REQUIRE(plan->kind() == ir::NodeKind::Project);
    const auto& columns = static_cast<const ir::ProjectNode&>(*plan).columns();
    REQUIRE(columns.size() == 4);
    CHECK(columns[0].name == "c_custkey");
    CHECK(columns[1].name == "c_name");
    CHECK(columns[2].name == "c_phone");
    CHECK(columns[3].name == "total");
}

TEST_CASE("payload lift re-sorts after the re-fetch join") {
    // A join's row order is outside the contract (SPEC 5.6), so the ordering the
    // top-k established has to be re-established above the join.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto plan = topk(aggregate({"c_custkey", "c_name"}, scan("customer")), "total", 20);
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Project);
    const auto& under_project = *plan->children().front();
    REQUIRE(under_project.kind() == ir::NodeKind::Order);
    const auto& keys = static_cast<const ir::OrderNode&>(under_project).keys();
    REQUIRE(keys.size() == 1);
    CHECK(keys.front().name == "total");
    CHECK_FALSE(keys.front().ascending);
    CHECK(under_project.children().front()->kind() == ir::NodeKind::Join);
}

TEST_CASE("payload lift declines without a top-k") {
    // The structural gate: with no head, every group survives and the re-fetch
    // would join as many rows as the aggregate produced.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto plan = aggregate({"c_custkey", "c_name"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    CHECK(find_join(*plan) == nullptr);
    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    // c_name stays a first() aggregate, the pre-existing behaviour.
    REQUIRE(agg->aggregations().size() == 2);
}

TEST_CASE("payload lift declines when the top-k sorts on a lifted column") {
    // Sorting on c_name picks a different k, and no re-sort above the join can
    // recover rows the head already discarded.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto plan = topk(aggregate({"c_custkey", "c_name"}, scan("customer")), "c_name", 20);
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    CHECK(find_join(*plan) == nullptr);
}

TEST_CASE("payload lift fires on the fused TopK canonicalize produces") {
    // A canonicalized plan has no Head over an Order: R16 fused them into a
    // TopK. The lift matched only the unfused pair, so q10 lost it -- and ran
    // +34% slower -- whenever its plan went through the optimizer.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_phone", "v"}, "c_custkey"));

    auto plan =
        fused_topk(aggregate({"c_custkey", "c_name", "c_phone"}, scan("customer")), "total", 20);
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    REQUIRE(agg->aggregations().size() == 1);
    CHECK(agg->aggregations().front().alias == "total");
    REQUIRE(find_join(*plan) != nullptr);

    // Same output columns, and the TopK's own order re-established above the
    // re-fetch join.
    REQUIRE(plan->kind() == ir::NodeKind::Project);
    const auto& columns = static_cast<const ir::ProjectNode&>(*plan).columns();
    REQUIRE(columns.size() == 4);
    CHECK(columns[0].name == "c_custkey");
    CHECK(columns[3].name == "total");
    const auto& under_project = *plan->children().front();
    REQUIRE(under_project.kind() == ir::NodeKind::Order);
    const auto& keys = static_cast<const ir::OrderNode&>(under_project).keys();
    REQUIRE(keys.size() == 1);
    CHECK(keys.front().name == "total");
    CHECK_FALSE(keys.front().ascending);
    const auto& join = *under_project.children().front();
    REQUIRE(join.kind() == ir::NodeKind::Join);
    CHECK(join.children().front()->kind() == ir::NodeKind::TopK);
}

TEST_CASE("payload lift over a TopK declines on a lifted sort key or a tail") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto sorts_on_lifted =
        fused_topk(aggregate({"c_custkey", "c_name"}, scan("customer")), "c_name", 20);
    sorts_on_lifted =
        ir::reduce_functionally_dependent_group_keys(std::move(sorts_on_lifted), sources);
    CHECK(find_join(*sorts_on_lifted) == nullptr);

    auto keeps_last = fused_topk(aggregate({"c_custkey", "c_name"}, scan("customer")), "total", 20,
                                 ir::TopKNode::KeepMode::Last);
    keeps_last = ir::reduce_functionally_dependent_group_keys(std::move(keeps_last), sources);
    CHECK(find_join(*keeps_last) == nullptr);
}

namespace {

/// `aggregate({keys}, Update{extra = <reads>}(Project{cols}(scan)))` -- the
/// shape canonicalize leaves below q10's aggregate: a projection under the
/// update that computes the aggregate's input.
auto aggregate_over_projected_update(const std::vector<std::string>& keys,
                                     const std::vector<std::string>& projected,
                                     const std::string& update_reads) -> ir::NodePtr {
    std::vector<ir::ColumnRef> cols;
    for (const auto& name : projected) {
        cols.push_back(ir::ColumnRef{.name = name});
    }
    auto project = std::make_unique<ir::ProjectNode>(ir::NodeId{3}, std::move(cols));
    project->add_child(scan("customer"));
    std::vector<ir::FieldSpec> fields;
    fields.push_back(ir::FieldSpec{.alias = "extra",
                                   .expr = ir::Expr{.node = ir::ColumnRef{.name = update_reads}}});
    auto update = std::make_unique<ir::UpdateNode>(ir::NodeId{4}, std::move(fields));
    update->add_child(std::move(project));
    return aggregate(keys, std::move(update));
}

/// The first ProjectNode under the plan's aggregate.
// NOLINTNEXTLINE(misc-no-recursion)
auto project_below(const ir::Node& node) -> const ir::ProjectNode* {
    if (node.kind() == ir::NodeKind::Project) {
        return static_cast<const ir::ProjectNode*>(&node);
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            if (const auto* found = project_below(*child); found != nullptr) {
                return found;
            }
        }
    }
    return nullptr;
}

auto names_of(const ir::ProjectNode& project) -> std::vector<std::string> {
    std::vector<std::string> out;
    for (const auto& col : project.columns()) {
        out.push_back(col.name);
    }
    return out;
}

}  // namespace

TEST_CASE("payload lift drops lifted columns from a projection below the aggregate") {
    // Projection pushdown decodes whatever a projection names, so a projection
    // below the aggregate that still lists c_name keeps the scan decoding it --
    // and the joins carrying it -- after the lift moved it above the top-k.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto plan = fused_topk(
        aggregate_over_projected_update({"c_custkey", "c_name"}, {"c_custkey", "c_name", "v"}, "v"),
        "total", 20);
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);
    REQUIRE(find_join(*plan) != nullptr);
    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    const auto* project = project_below(*agg);
    REQUIRE(project != nullptr);
    CHECK(names_of(*project) == std::vector<std::string>{"c_custkey", "v"});
}

TEST_CASE("payload lift keeps a lifted column a node below the aggregate still reads") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    // The update between the aggregate and the projection reads c_name.
    auto plan = fused_topk(aggregate_over_projected_update({"c_custkey", "c_name"},
                                                           {"c_custkey", "c_name", "v"}, "c_name"),
                           "total", 20);
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);
    REQUIRE(find_join(*plan) != nullptr);
    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    const auto* project = project_below(*agg);
    REQUIRE(project != nullptr);
    CHECK(names_of(*project) == std::vector<std::string>{"c_custkey", "c_name", "v"});
}
