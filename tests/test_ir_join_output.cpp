// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/join_output.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <string>
#include <string_view>
#include <vector>

using Catch::Matchers::ContainsSubstring;
using ibex::ir::JoinKey;
using ibex::ir::JoinKind;
using ibex::ir::JoinOutputSide;
using ibex::ir::JoinSuffixPolicy;
using ibex::ir::plan_join_output;

namespace {

auto views(const std::vector<std::string>& names) -> std::vector<std::string_view> {
    return {names.begin(), names.end()};
}

auto plan_of(JoinKind kind, const std::vector<JoinKey>& keys, const std::vector<std::string>& left,
             const std::vector<std::string>& right, const JoinSuffixPolicy& suffix = {}) {
    const auto left_views = views(left);
    const auto right_views = views(right);
    return plan_join_output(kind, keys, left_views, right_views, suffix);
}

// Output names only — the shape every consumer of the plan must agree on.
auto planned_names(JoinKind kind, const std::vector<JoinKey>& keys,
                   const std::vector<std::string>& left, const std::vector<std::string>& right,
                   const JoinSuffixPolicy& suffix = {}) -> std::vector<std::string> {
    const auto plan = plan_of(kind, keys, left, right, suffix);
    REQUIRE(plan.has_value());
    std::vector<std::string> out;
    for (const auto& column : *plan) {
        out.push_back(column.name);
    }
    return out;
}

auto suffixes(std::string left, std::string right) -> JoinSuffixPolicy {
    return JoinSuffixPolicy{.present = true, .left = std::move(left), .right = std::move(right)};
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

TEST_CASE("join output plan: an explicitly folded mapped key keeps native inputs",
          "[ir][join][schema]") {
    const auto plan = plan_of(JoinKind::Inner, {{"id", "right_id", true}}, {"id", "left_value"},
                              {"right_id", "right_value"});
    REQUIRE(plan.has_value());
    REQUIRE(plan->size() == 3);
    CHECK((*plan)[0].name == "id");
    CHECK((*plan)[0].folded_peer_index == 0);
    CHECK((*plan)[1].name == "left_value");
    CHECK((*plan)[2].name == "right_value");
}

TEST_CASE("join output plan: a folded key can preserve a logical label", "[ir][join][schema]") {
    const auto plan = plan_of(JoinKind::Inner, {{"left_native", "right_native", true, "id"}},
                              {"left_native", "left_value"}, {"right_native", "right_value"});
    REQUIRE(plan.has_value());
    REQUIRE(plan->size() == 3);
    CHECK((*plan)[0].name == "id");
    CHECK((*plan)[0].source_index == 0);
    CHECK((*plan)[0].folded_peer_index == 0);
}

TEST_CASE("join output plan: semi and anti joins return the left columns only",
          "[ir][join][schema]") {
    for (const JoinKind kind : {JoinKind::Semi, JoinKind::Anti}) {
        const auto out = planned_names(kind, {{"id", "id"}}, {"id", "val"}, {"id", "other"});
        REQUIRE(out == std::vector<std::string>{"id", "val"});
    }
}

// Replaces "colliding right columns take repeated _right suffixes". Automatic
// suffixing is gone: the same mechanism served deliberate wide-table renames
// and accidental overlaps, and only the first is worth guessing at.
TEST_CASE("join output plan: a non-key collision without a suffix clause is an error",
          "[ir][join][schema]") {
    const auto plan = plan_of(JoinKind::Inner, {{"id", "id"}}, {"id", "val"}, {"id", "val"});
    REQUIRE_FALSE(plan.has_value());
    CHECK_THAT(plan.error(), ContainsSubstring("\"val\""));
    CHECK_THAT(plan.error(), ContainsSubstring("both inputs"));
    CHECK_THAT(plan.error(), ContainsSubstring("suffix"));
}

TEST_CASE("join output plan: a suffix clause renames both sides of a collision",
          "[ir][join][schema]") {
    const auto out = planned_names(JoinKind::Inner, {{"id", "id"}}, {"id", "val"}, {"id", "val"},
                                   suffixes("_old", "_new"));
    REQUIRE(out == std::vector<std::string>{"id", "val_old", "val_new"});
}

TEST_CASE("join output plan: suffixes apply to collisions only", "[ir][join][schema]") {
    // "keep" exists on one side only, so it is untouched; the shared key folds
    // and is not a collision either.
    const auto out = planned_names(JoinKind::Inner, {{"id", "id"}}, {"id", "val", "keep"},
                                   {"id", "val", "other"}, suffixes("_l", "_r"));
    REQUIRE(out == std::vector<std::string>{"id", "val_l", "keep", "val_r", "other"});
}

TEST_CASE("join output plan: an empty suffix leaves that side alone", "[ir][join][schema]") {
    const auto out = planned_names(JoinKind::Inner, {{"id", "id"}}, {"id", "val"}, {"id", "val"},
                                   suffixes("", "_right"));
    REQUIRE(out == std::vector<std::string>{"id", "val", "val_right"});
}

TEST_CASE("join output plan: a suffixed name that still collides is an error",
          "[ir][join][schema]") {
    SECTION("the suffixed name lands on an existing column") {
        // "val" + "_right" is already a left column, so renaming does not
        // separate them. The old planner would have kept suffixing.
        const auto plan = plan_of(JoinKind::Inner, {{"id", "id"}}, {"id", "val", "val_right"},
                                  {"id", "val"}, suffixes("", "_right"));
        REQUIRE_FALSE(plan.has_value());
        CHECK_THAT(plan.error(), ContainsSubstring("val_right"));
    }

    SECTION("both suffixes are the same") {
        const auto plan = plan_of(JoinKind::Inner, {{"id", "id"}}, {"id", "val"}, {"id", "val"},
                                  suffixes("", ""));
        REQUIRE_FALSE(plan.has_value());
        CHECK_THAT(plan.error(), ContainsSubstring("\"val\""));
    }
}

TEST_CASE("join output plan: semi and anti cannot collide", "[ir][join][schema]") {
    // The right side is not emitted, so an overlapping name is no obstacle and
    // no suffix clause is needed to accept the join.
    for (const JoinKind kind : {JoinKind::Semi, JoinKind::Anti}) {
        const auto out = planned_names(kind, {{"id", "id"}}, {"id", "val"}, {"id", "val"});
        REQUIRE(out == std::vector<std::string>{"id", "val"});
    }
}

// Replaces the old cross-join case, which relied on "b" being auto-renamed.
TEST_CASE("join output plan: a cross join emits every column of both sides", "[ir][join][schema]") {
    SECTION("disjoint names need no clause") {
        const auto out = planned_names(JoinKind::Cross, {}, {"a", "b"}, {"c", "d"});
        REQUIRE(out == std::vector<std::string>{"a", "b", "c", "d"});
    }

    SECTION("a cross join has no keys to fold, so a shared name collides") {
        const auto plan = plan_of(JoinKind::Cross, {}, {"a", "b"}, {"b", "c"});
        REQUIRE_FALSE(plan.has_value());
    }

    SECTION("with a clause both sides are renamed") {
        const auto out =
            planned_names(JoinKind::Cross, {}, {"a", "b"}, {"b", "c"}, suffixes("_l", "_r"));
        REQUIRE(out == std::vector<std::string>{"a", "b_l", "b_r", "c"});
    }
}

TEST_CASE("join output plan: left columns come first and keep their source order",
          "[ir][join][schema]") {
    const std::vector<std::string> left{"id", "a", "b"};
    const std::vector<std::string> right{"id", "c"};
    const auto planned = plan_of(JoinKind::Left, {{"id", "id"}}, left, right);
    REQUIRE(planned.has_value());
    const auto& plan = *planned;

    REQUIRE(plan.size() == 4);
    for (std::size_t i = 0; i < left.size(); ++i) {
        CHECK(plan[i].side == JoinOutputSide::Left);
        CHECK(plan[i].source_index == i);
    }
    CHECK(plan[3].side == JoinOutputSide::Right);
    CHECK(plan[3].source_index == 1);  // right "c"; the shared key was skipped
    CHECK(plan[3].name == "c");
}

TEST_CASE("join output plan: source indices survive renaming", "[ir][join][schema]") {
    // A renamed column must still point at the column it came from, or every
    // consumer gathers the wrong data under the right name.
    const auto planned = plan_of(JoinKind::Inner, {{"id", "id"}}, {"id", "val"}, {"id", "val"},
                                 suffixes("_l", "_r"));
    REQUIRE(planned.has_value());
    const auto& plan = *planned;
    REQUIRE(plan.size() == 3);
    CHECK(plan[1].name == "val_l");
    CHECK(plan[1].side == JoinOutputSide::Left);
    CHECK(plan[1].source_index == 1);
    CHECK(plan[2].name == "val_r");
    CHECK(plan[2].side == JoinOutputSide::Right);
    CHECK(plan[2].source_index == 1);
}

TEST_CASE("join output plan: an empty right side leaves the left schema untouched",
          "[ir][join][schema]") {
    const auto out = planned_names(JoinKind::Left, {{"id", "id"}}, {"id", "val"}, {});
    REQUIRE(out == std::vector<std::string>{"id", "val"});
}
