#include <ibex/ir/join_output.hpp>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>
#include <vector>

using ibex::ir::JoinKey;
using ibex::ir::JoinKind;
using ibex::ir::JoinOutputSide;
using ibex::ir::plan_join_output;

namespace {

auto views(const std::vector<std::string>& names) -> std::vector<std::string_view> {
    return {names.begin(), names.end()};
}

// Output names only — the shape every consumer of the plan must agree on.
auto planned_names(JoinKind kind, const std::vector<JoinKey>& keys,
                   const std::vector<std::string>& left, const std::vector<std::string>& right)
    -> std::vector<std::string> {
    const auto left_views = views(left);
    const auto right_views = views(right);
    std::vector<std::string> out;
    for (const auto& column : plan_join_output(kind, keys, left_views, right_views)) {
        out.push_back(column.name);
    }
    return out;
}

}  // namespace

TEST_CASE("join output plan: a same-name key yields one output column", "[ir][join][schema]") {
    const auto out =
        planned_names(JoinKind::Inner, {{"id", "id"}}, {"id", "left_val"}, {"id", "right_val"});
    REQUIRE(out == std::vector<std::string>{"id", "left_val", "right_val"});
}

TEST_CASE("join output plan: a mapped key keeps both native columns", "[ir][join][schema]") {
    const auto out = planned_names(JoinKind::Inner, {{"left_id", "right_id"}}, {"left_id", "val"},
                                   {"right_id", "other"});
    REQUIRE(out == std::vector<std::string>{"left_id", "val", "right_id", "other"});
}

TEST_CASE("join output plan: semi and anti joins return the left columns only",
          "[ir][join][schema]") {
    for (const JoinKind kind : {JoinKind::Semi, JoinKind::Anti}) {
        const auto out = planned_names(kind, {{"id", "id"}}, {"id", "val"}, {"id", "other"});
        REQUIRE(out == std::vector<std::string>{"id", "val"});
    }
}

TEST_CASE("join output plan: colliding right columns take repeated _right suffixes",
          "[ir][join][schema]") {
    // "val" is taken, and so is the "val_right" the first suffix would produce,
    // so the second right column has to keep suffixing.
    const auto out = planned_names(JoinKind::Inner, {{"id", "id"}}, {"id", "val", "val_right"},
                                   {"id", "val", "val_right"});
    REQUIRE(out == std::vector<std::string>{"id", "val", "val_right", "val_right_right",
                                            "val_right_right_right"});
}

TEST_CASE("join output plan: a cross join emits every column of both sides", "[ir][join][schema]") {
    const auto out = planned_names(JoinKind::Cross, {}, {"a", "b"}, {"b", "c"});
    REQUIRE(out == std::vector<std::string>{"a", "b", "b_right", "c"});
}

TEST_CASE("join output plan: left columns come first and keep their source order",
          "[ir][join][schema]") {
    const std::vector<std::string> left{"id", "a", "b"};
    const std::vector<std::string> right{"id", "c"};
    const auto left_views = views(left);
    const auto right_views = views(right);
    const auto plan = plan_join_output(JoinKind::Left, {{"id", "id"}}, left_views, right_views);

    REQUIRE(plan.size() == 4);
    for (std::size_t i = 0; i < left.size(); ++i) {
        CHECK(plan[i].side == JoinOutputSide::Left);
        CHECK(plan[i].source_index == i);
    }
    CHECK(plan[3].side == JoinOutputSide::Right);
    CHECK(plan[3].source_index == 1);  // right "c"; the shared key was skipped
    CHECK(plan[3].name == "c");
}

TEST_CASE("join output plan: an empty right side leaves the left schema untouched",
          "[ir][join][schema]") {
    const auto out = planned_names(JoinKind::Left, {{"id", "id"}}, {"id", "val"}, {});
    REQUIRE(out == std::vector<std::string>{"id", "val"});
}
