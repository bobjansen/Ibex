// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/join_pushdown.hpp>
#include <ibex/ir/mapped_join_keys.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto make_scan(ir::NodeId id, std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(id, std::move(name));
}

auto make_join(ir::NodeId id, ir::JoinKind kind, std::vector<ir::JoinKey> keys, ir::NodePtr left,
               ir::NodePtr right) -> ir::NodePtr {
    auto join = std::make_unique<ir::JoinNode>(id, kind, std::move(keys));
    join->add_child(std::move(left));
    join->add_child(std::move(right));
    return join;
}

/// A Project above the join, which is what bounds the demand at all: with the
/// join as the plan root every column is demanded and nothing can be folded.
auto make_project(ir::NodeId id, std::vector<std::string> names, ir::NodePtr child) -> ir::NodePtr {
    std::vector<ir::ColumnRef> columns;
    columns.reserve(names.size());
    for (auto& name : names) {
        columns.push_back(ir::ColumnRef{.name = std::move(name)});
    }
    auto project = std::make_unique<ir::ProjectNode>(id, std::move(columns));
    project->add_child(std::move(child));
    return project;
}

/// `a` = {ak, ax}, `b` = {bk, bx}: disjoint names, so a join on `ak = bk` is
/// mapped and nothing collides. `d` also carries `ak`, and `e` also carries
/// `bk` — the two collision shapes.
auto test_sources() -> ir::SourceSchemas {
    ir::SourceSchemas sources;
    sources.insert_or_assign(
        "a", ir::SchemaInfo::known({{.name = "ak", .type = ir::ColumnType::Int64},
                                    {.name = "ax", .type = ir::ColumnType::Int64}}));
    sources.insert_or_assign(
        "b", ir::SchemaInfo::known({{.name = "bk", .type = ir::ColumnType::Int64},
                                    {.name = "bx", .type = ir::ColumnType::Int64}}));
    sources.insert_or_assign(
        "d", ir::SchemaInfo::known({{.name = "ak", .type = ir::ColumnType::Int64},
                                    {.name = "bk", .type = ir::ColumnType::Int64}}));
    sources.insert_or_assign(
        "e", ir::SchemaInfo::known({{.name = "ak", .type = ir::ColumnType::Int64},
                                    {.name = "bk", .type = ir::ColumnType::Int64},
                                    {.name = "ex", .type = ir::ColumnType::Int64}}));
    return sources;
}

/// Project(names, Join(kind, keys, left_source, right_source)).
auto project_over_join(ir::JoinKind kind, std::vector<ir::JoinKey> keys, std::string left_source,
                       std::string right_source, std::vector<std::string> projected)
    -> ir::NodePtr {
    return make_project(
        {1}, std::move(projected),
        make_join({2}, kind, std::move(keys), make_scan({3}, std::move(left_source)),
                  make_scan({4}, std::move(right_source))));
}

auto join_of(const ir::Node& root) -> const ir::JoinNode& {
    const ir::Node* node = root.children().front().get();
    REQUIRE(node->kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    return static_cast<const ir::JoinNode&>(*node);
}

/// The rename specs sitting directly under the join's right child, or an empty
/// vector when that child is not a Rename.
auto right_renames(const ir::JoinNode& join) -> std::vector<ir::RenameSpec> {
    const ir::Node& right = *join.children()[1];
    if (right.kind() != ir::NodeKind::Rename) {
        return {};
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    return static_cast<const ir::RenameNode&>(right).renames();
}

auto key_pairs(const ir::JoinNode& join) -> std::vector<std::pair<std::string, std::string>> {
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(join.keys().size());
    for (const auto& key : join.keys()) {
        out.emplace_back(key.left, key.right);
    }
    return out;
}

}  // namespace

TEST_CASE("mapped join keys: an unread right key folds into the left's name",
          "[ir][mapped_join_keys]") {
    auto plan = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"ak", "bk"}}, "a", "b", {"ak", "ax", "bx"}),
        test_sources());

    const auto& join = join_of(*plan);
    CHECK(ir::join_keys_are_same_named(join.keys()));
    CHECK(key_pairs(join) == std::vector<std::pair<std::string, std::string>>{{"ak", "ak"}});

    const auto renames = right_renames(join);
    REQUIRE(renames.size() == 1);
    CHECK(renames[0].old_name == "bk");
    CHECK(renames[0].new_name == "ak");
    // The rename is inserted, not substituted: the right scan is still there.
    CHECK(join.children()[1]->children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("mapped join keys: a right key the plan reads is left alone", "[ir][mapped_join_keys]") {
    // `bk` is projected, so folding it into `ak` would drop a column the plan
    // reads. SPEC 12.3 keeps both, and so does this.
    auto plan = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"ak", "bk"}}, "a", "b", {"ak", "bk"}),
        test_sources());

    const auto& join = join_of(*plan);
    CHECK_FALSE(ir::join_keys_are_same_named(join.keys()));
    CHECK(right_renames(join).empty());
}

TEST_CASE("mapped join keys: a left join folds, a right join does not", "[ir][mapped_join_keys]") {
    // Left: the folded column takes the left row's value, which is what an
    // unmatched left row already carries — nothing observable changes.
    auto left = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Left, {{"ak", "bk"}}, "a", "b", {"ak", "ax", "bx"}),
        test_sources());
    CHECK(ir::join_keys_are_same_named(join_of(*left).keys()));

    // Right and Outer fill the surviving key column from the RIGHT row when a
    // right row goes unmatched, so folding would change its values.
    for (const auto kind : {ir::JoinKind::Right, ir::JoinKind::Outer}) {
        auto plan = ir::normalize_mapped_join_keys(
            project_over_join(kind, {{"ak", "bk"}}, "a", "b", {"ak", "ax", "bx"}), test_sources());
        CHECK_FALSE(ir::join_keys_are_same_named(join_of(*plan).keys()));
        CHECK(right_renames(join_of(*plan)).empty());
    }
}

TEST_CASE("mapped join keys: semi and anti joins fold", "[ir][mapped_join_keys]") {
    // Their output carries no right columns at all, so the fold cannot be seen
    // whatever the projection asks for.
    for (const auto kind : {ir::JoinKind::Semi, ir::JoinKind::Anti}) {
        auto plan = ir::normalize_mapped_join_keys(
            project_over_join(kind, {{"ak", "bk"}}, "a", "b", {"ak", "ax"}), test_sources());
        CHECK(ir::join_keys_are_same_named(join_of(*plan).keys()));
        REQUIRE(right_renames(join_of(*plan)).size() == 1);
    }
}

TEST_CASE("mapped join keys: a name that collides across the sides is left alone",
          "[ir][mapped_join_keys]") {
    // `d` already has an `ak`, so renaming its `bk` to `ak` would duplicate a
    // name inside the right child.
    auto into_right = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"ak", "bk"}}, "a", "d", {"ak", "ax"}),
        test_sources());
    CHECK_FALSE(ir::join_keys_are_same_named(join_of(*into_right).keys()));

    // `e` (the LEFT side here) already has a `bk`, so the join's suffix policy
    // is already renaming one of the two; folding would change which.
    auto from_left = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"ak", "bk"}}, "e", "b", {"ak", "ex"}),
        test_sources());
    CHECK_FALSE(ir::join_keys_are_same_named(join_of(*from_left).keys()));
}

TEST_CASE("mapped join keys: an unknown side schema proves nothing", "[ir][mapped_join_keys]") {
    ir::SourceSchemas partial = test_sources();
    partial.erase("b");
    auto plan = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"ak", "bk"}}, "a", "b", {"ak", "ax", "bx"}),
        partial);
    CHECK_FALSE(ir::join_keys_are_same_named(join_of(*plan).keys()));
}

TEST_CASE("mapped join keys: a same-named join is not touched", "[ir][mapped_join_keys]") {
    auto plan = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"ak"}}, "a", "d", {"ak"}), test_sources());
    const auto& join = join_of(*plan);
    CHECK(ir::join_keys_are_same_named(join.keys()));
    CHECK(right_renames(join).empty());
    CHECK(join.children()[1]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("mapped join keys: a multi-key join folds only the mapped key",
          "[ir][mapped_join_keys]") {
    ir::SourceSchemas sources;
    sources.insert_or_assign(
        "l", ir::SchemaInfo::known({{.name = "shared", .type = ir::ColumnType::Int64},
                                    {.name = "ak", .type = ir::ColumnType::Int64},
                                    {.name = "ax", .type = ir::ColumnType::Int64}}));
    sources.insert_or_assign(
        "r", ir::SchemaInfo::known({{.name = "shared", .type = ir::ColumnType::Int64},
                                    {.name = "bk", .type = ir::ColumnType::Int64},
                                    {.name = "bx", .type = ir::ColumnType::Int64}}));

    auto plan = ir::normalize_mapped_join_keys(
        project_over_join(ir::JoinKind::Inner, {{"shared"}, {"ak", "bk"}}, "l", "r",
                          {"shared", "ax", "bx"}),
        sources);

    const auto& join = join_of(*plan);
    CHECK(key_pairs(join) ==
          std::vector<std::pair<std::string, std::string>>{{"shared", "shared"}, {"ak", "ak"}});
    const auto renames = right_renames(join);
    REQUIRE(renames.size() == 1);
    CHECK(renames[0].old_name == "bk");
}

TEST_CASE("mapped join keys: both spellings reach the same pushdown decision",
          "[ir][mapped_join_keys]") {
    // The regression guard the whole change exists for. `push_filters_into_joins`
    // runs the normalizer first, so a filter on the right side must descend
    // whichever way the join is spelled — before this, the mapped spelling kept
    // its filter above the join and silently joined unfiltered inputs.
    // Two right-hand sources identical but for the key's name, so the only
    // difference between the two plans is the spelling of the join.
    ir::SourceSchemas sources = test_sources();
    sources.insert_or_assign(
        "rn", ir::SchemaInfo::known({{.name = "ak", .type = ir::ColumnType::Int64},
                                     {.name = "bx", .type = ir::ColumnType::Int64}}));
    sources.insert_or_assign(
        "rm", ir::SchemaInfo::known({{.name = "bk", .type = ir::ColumnType::Int64},
                                     {.name = "bx", .type = ir::ColumnType::Int64}}));

    const auto build = [](std::vector<ir::JoinKey> keys, std::string right_source) {
        auto join = make_join({2}, ir::JoinKind::Inner, std::move(keys), make_scan({3}, "a"),
                              make_scan({4}, std::move(right_source)));
        auto predicate =
            ir::Expr{.node = ir::CompareExpr{
                         .op = ir::CompareOp::Gt,
                         .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "bx"}}),
                         .right = ir::make_expr_ptr(
                             ir::Expr{.node = ir::Literal{.value = std::int64_t{5}}})}};
        auto filter = std::make_unique<ir::FilterNode>(ir::NodeId{1}, std::move(predicate));
        filter->add_child(std::move(join));
        return make_project({0}, {"ak", "ax", "bx"}, std::move(filter));
    };

    const auto pushed_side = [](const ir::Node& root) -> std::string {
        // Project(Join(a, Filter(b))) once the conjunct has descended.
        const ir::Node* node = root.children().front().get();
        REQUIRE(node->kind() == ir::NodeKind::Join);
        const ir::Node& right = *node->children()[1];
        // The mapped spelling gains a Rename between the join and the filter.
        const ir::Node* below =
            right.kind() == ir::NodeKind::Rename ? right.children().front().get() : &right;
        return below->kind() == ir::NodeKind::Filter ? "filtered" : "unfiltered";
    };

    auto same_named = ir::push_filters_into_joins(build({{"ak", "ak"}}, "rn"), sources);
    auto mapped = ir::push_filters_into_joins(build({{"ak", "bk"}}, "rm"), sources);

    CHECK(pushed_side(*same_named) == "filtered");
    CHECK(pushed_side(*mapped) == "filtered");
}
