// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/probed_key_restriction.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto make_scan(ir::NodeId id, std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(id, std::move(name));
}

/// `facts` is the big table the aggregate groups; `probes` is the small one
/// the join keys into. The statistics say `facts` holds 400k distinct `k` and
/// `probes` 25 -- the shape the restriction exists for.
auto stats() -> ir::SourceStats {
    ir::SourceStats s;
    s.rows["facts"] = 1'000'000;
    s.rows["probes"] = 25;
    s.schemas["facts"] = ir::SchemaInfo::known(
        {{.name = "k", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe},
         {.name = "v", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe}});
    s.schemas["probes"] = ir::SchemaInfo::known(
        {{.name = "k", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe}});
    s.distinct["facts"]["k"] = 400'000;
    s.distinct["probes"]["k"] = 25;
    return s;
}

auto aggregate_over_facts() -> ir::NodePtr {
    auto agg = std::make_unique<ir::AggregateNode>(
        ir::NodeId{2}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "k"}},
        std::vector<ir::AggSpec>{ir::AggSpec{
            .func = ir::AggFunc::Min, .column = ir::ColumnRef{.name = "v"}, .alias = "m"}});
    agg->add_child(make_scan(ir::NodeId{1}, "facts"));
    return agg;
}

/// Join(kind, on k, <probe>, Aggregate(by k) over facts).
auto plan(ir::JoinKind kind, ir::NodePtr probe) -> ir::NodePtr {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{10}, kind,
                                               std::vector<ir::JoinKey>{ir::JoinKey{"k"}});
    join->add_child(std::move(probe));
    join->add_child(aggregate_over_facts());
    return join;
}

/// The Semi join the pass inserts, or null.
auto restriction_of(const ir::Node& root) -> const ir::JoinNode* {
    if (const auto* join = dynamic_cast<const ir::JoinNode*>(&root);
        join != nullptr && join->kind() == ir::JoinKind::Semi) {
        return join;
    }
    for (const auto& child : root.children()) {
        if (child != nullptr) {
            if (const auto* found = restriction_of(*child)) {
                return found;
            }
        }
    }
    return nullptr;
}

}  // namespace

TEST_CASE("probed key restriction: a join over a grouped aggregate restricts it",
          "[ir][probed-keys]") {
    auto root = ir::restrict_aggregates_to_probed_keys(
        plan(ir::JoinKind::Left, make_scan(ir::NodeId{3}, "probes")), stats());
    const auto* semi = restriction_of(*root);
    REQUIRE(semi != nullptr);
    // The aggregate's input on the left, the probe side's name on the right.
    REQUIRE(semi->keys().size() == 1);
    CHECK(semi->keys()[0].left == "k");
    CHECK(semi->keys()[0].right == "k");
    // It sits under the aggregate, not above it: restricting the aggregate's
    // OUTPUT would drop groups the join still has to see as unmatched.
    const auto* aggregate = dynamic_cast<const ir::AggregateNode*>(root->children()[1].get());
    REQUIRE(aggregate != nullptr);
    CHECK(aggregate->children()[0]->kind() == ir::NodeKind::Join);
}

TEST_CASE("probed key restriction: it is idempotent", "[ir][probed-keys]") {
    auto once = ir::restrict_aggregates_to_probed_keys(
        plan(ir::JoinKind::Left, make_scan(ir::NodeId{3}, "probes")), stats());
    // A second run must not wrap the restriction in another one.
    auto twice = ir::restrict_aggregates_to_probed_keys(std::move(once), stats());
    const auto* aggregate = dynamic_cast<const ir::AggregateNode*>(twice->children()[1].get());
    REQUIRE(aggregate != nullptr);
    const auto* semi = dynamic_cast<const ir::JoinNode*>(aggregate->children()[0].get());
    REQUIRE(semi != nullptr);
    CHECK(semi->kind() == ir::JoinKind::Semi);
    CHECK(semi->children()[0]->kind() == ir::NodeKind::Scan);  // not another Semi
}

TEST_CASE("probed key restriction: declined when the probe keys into as much as it saves",
          "[ir][probed-keys]") {
    // The probe side is the fact table itself, so it keys into every group the
    // aggregate would build and the restriction removes nothing -- while still
    // paying for a second evaluation of the probe side.
    auto root = ir::restrict_aggregates_to_probed_keys(
        plan(ir::JoinKind::Left, make_scan(ir::NodeId{3}, "facts")), stats());
    CHECK(restriction_of(*root) == nullptr);
}

TEST_CASE("probed key restriction: declined without statistics", "[ir][probed-keys]") {
    // No distinct counts, so nothing says the restriction removes any group.
    // Declining costs a rewrite; guessing costs a second evaluation of a probe
    // side that may dwarf what it saves.
    auto root = ir::restrict_aggregates_to_probed_keys(
        plan(ir::JoinKind::Left, make_scan(ir::NodeId{3}, "probes")), ir::SourceStats{});
    CHECK(restriction_of(*root) == nullptr);
}

TEST_CASE("probed key restriction: only Left and Inner joins", "[ir][probed-keys]") {
    // A Right or Outer join emits rows the probe side never matched, whose
    // aggregate values would then depend on groups the restriction removed.
    for (const ir::JoinKind kind : {ir::JoinKind::Right, ir::JoinKind::Outer}) {
        auto root = ir::restrict_aggregates_to_probed_keys(
            plan(kind, make_scan(ir::NodeId{3}, "probes")), stats());
        CHECK(restriction_of(*root) == nullptr);
    }
    auto inner = ir::restrict_aggregates_to_probed_keys(
        plan(ir::JoinKind::Inner, make_scan(ir::NodeId{3}, "probes")), stats());
    CHECK(restriction_of(*inner) != nullptr);
}

TEST_CASE("probed key restriction: declined when null keys match each other",
          "[ir][probed-keys][regression]") {
    // The inserted semi join uses `nulls never`, which would remove the
    // aggregate's null-keyed group that a `nulls equal` join must still match.
    auto join = std::make_unique<ir::JoinNode>(
        ir::NodeId{10}, ir::JoinKind::Left, std::vector<ir::JoinKey>{ir::JoinKey{"k"}},
        std::nullopt, ir::JoinSuffixPolicy{}, ir::NullMatch::Equal);
    join->add_child(make_scan(ir::NodeId{3}, "probes"));
    join->add_child(aggregate_over_facts());
    auto root = ir::restrict_aggregates_to_probed_keys(std::move(join), stats());
    CHECK(restriction_of(*root) == nullptr);
}

TEST_CASE("probed key restriction: declined when the probe side cannot be replayed",
          "[ir][probed-keys]") {
    // Collecting the keys evaluates the probe side a second time. An extern
    // call is unclassified on purpose, so nothing says a second call answers
    // the same way -- and a plan that draws from an RNG certainly does not.
    auto extern_probe = std::make_unique<ir::ExternCallNode>(ir::NodeId{3}, "read_something",
                                                             std::vector<ir::Expr>{});
    auto root = ir::restrict_aggregates_to_probed_keys(
        plan(ir::JoinKind::Left, std::move(extern_probe)), stats());
    CHECK(restriction_of(*root) == nullptr);
}
