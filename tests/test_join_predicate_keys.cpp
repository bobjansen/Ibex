// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// `on a == b` runs as a hash join where it means what `on { a = b }` means
// (ir::equality_predicates_to_join_keys), and warns where it has to stay a
// nested loop.

#include <ibex/ir/join_predicate_keys.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto lower_src(std::string_view src) -> ir::NodePtr {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    return std::move(*lowered);
}

auto find_join(const ir::Node& node) -> const ir::JoinNode* {
    if (node.kind() == ir::NodeKind::Join) {
        return &ir::node_cast<ir::JoinNode>(node);
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            if (const auto* join = find_join(*child)) {
                return join;
            }
        }
    }
    return nullptr;
}

auto key_pairs(const ir::JoinNode& join) -> std::vector<std::pair<std::string, std::string>> {
    std::vector<std::pair<std::string, std::string>> out;
    for (const auto& key : join.keys()) {
        out.emplace_back(key.left, key.right);
    }
    return out;
}

/// Lower `src` (which `parser::lower` already rewrites) and run the pass once
/// more to collect the warnings for whatever it left as a predicate.
auto warnings_for(std::string_view src) -> std::vector<std::string> {
    return ir::equality_predicates_to_join_keys(lower_src(src)).warnings;
}

constexpr std::string_view kLeft = "Table { i = [1, 2, 3, null], v = [10, 20, 30, 40] }";
constexpr std::string_view kRight = "Table { j = [1, 2, 2, 9, null], w = [10, 20, 21, 90, 99] }";

/// `<kLeft> <kind> <kRight> on <on>`.
auto join_src(std::string_view kind, std::string_view on, std::string_view tail = ";")
    -> std::string {
    std::string src(kLeft);
    src += ' ';
    src += kind;
    src += ' ';
    src += kRight;
    src += " on ";
    src += on;
    src += tail;
    return src;
}

}  // namespace

TEST_CASE("join predicates: an all-equality predicate over one type becomes keys",
          "[ir][join][predicate_keys]") {
    SECTION("one pair") {
        auto plan = lower_src(join_src("join", "i == j"));
        const auto* join = find_join(*plan);
        REQUIRE(join != nullptr);
        CHECK_FALSE(join->predicate().has_value());
        CHECK(key_pairs(*join) == std::vector<std::pair<std::string, std::string>>{{"i", "j"}});
    }
    SECTION("several pairs, written right side first") {
        auto plan = lower_src(join_src("join", "j == i && w == v"));
        const auto* join = find_join(*plan);
        REQUIRE(join != nullptr);
        CHECK_FALSE(join->predicate().has_value());
        CHECK(key_pairs(*join) ==
              std::vector<std::pair<std::string, std::string>>{{"i", "j"}, {"v", "w"}});
    }
    SECTION("explicit sides") {
        auto plan = lower_src(join_src("semi join", "right(j) == left(i)"));
        const auto* join = find_join(*plan);
        REQUIRE(join != nullptr);
        CHECK_FALSE(join->predicate().has_value());
        CHECK(key_pairs(*join) == std::vector<std::pair<std::string, std::string>>{{"i", "j"}});
    }
    CHECK(warnings_for(join_src("join", "i == j")).empty());
}

TEST_CASE("join predicates: the rewrite returns what the predicate join returned",
          "[ir][join][predicate_keys]") {
    // Null keys (never matched), a duplicate right key, and unmatched rows on
    // both sides, for every kind the rewrite covers.
    const runtime::TableRegistry none;
    for (const char* kind :
         {"join", "left join", "right join", "outer join", "semi join", "anti join"}) {
        CAPTURE(kind);
        const std::string_view kind_name = kind;
        const bool left_only = kind_name == "semi join" || kind_name == "anti join";
        const std::string_view order =
            left_only ? ")[order { i, v }];" : ")[order { i, v, j, w }];";
        const auto run = [&](std::string_view on) {
            auto plan = lower_src("(" + join_src(kind, on, order));
            auto out = runtime::interpret(*plan, none, nullptr, nullptr);
            REQUIRE(out.has_value());
            return std::move(*out);
        };
        const auto keyed = run("{ i = j }");
        const auto predicate = run("i == j");
        const auto mismatch = runtime::compare_tables(keyed, predicate);
        CHECK_FALSE(mismatch.has_value());
    }
}

TEST_CASE("join predicates: equality joins that cannot be keys stay and warn",
          "[ir][join][predicate_keys]") {
    SECTION("different types") {
        const std::string src = "Table { i = [1, 2] } join Table { f = [1.0, 2.0] } on i == f;";
        auto plan = lower_src(src);
        CHECK(find_join(*plan)->predicate().has_value());
        const auto warnings = warnings_for(src);
        REQUIRE(warnings.size() == 1);
        CHECK(warnings[0].contains("Int64"));
        CHECK(warnings[0].contains("Float64"));
        CHECK(warnings[0].contains("on { i = f }"));
    }
    SECTION("floating-point") {
        const auto warnings =
            warnings_for("Table { x = [1.0] } join Table { y = [1.0] } on x == y;");
        REQUIRE(warnings.size() == 1);
        CHECK(warnings[0].contains("NaN"));
    }
    SECTION("one name on both sides") {
        const auto warnings = warnings_for(
            "Table { k = [1] } join Table { k = [1] } on left(k) == right(k) "
            "suffix { \"_l\", \"_r\" };");
        REQUIRE(warnings.size() == 1);
        CHECK(warnings[0].contains("fold"));
    }
    SECTION("schemas unknown when the plan is made") {
        // Registry tables: their columns are known only at run time.
        const auto warnings = warnings_for("lhs join rhs on left(i) == right(j);");
        REQUIRE(warnings.size() == 1);
        CHECK(warnings[0].contains("not known"));
    }
}

TEST_CASE("join predicates: a predicate with any other term is left alone, silently",
          "[ir][join][predicate_keys]") {
    const std::string src = join_src("join", "i == j && v < w");
    auto plan = lower_src(src);
    const auto* join = find_join(*plan);
    REQUIRE(join != nullptr);
    CHECK(join->predicate().has_value());
    CHECK(join->keys().empty());
    CHECK(warnings_for(src).empty());
}
