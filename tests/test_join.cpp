#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <vector>

using namespace ibex;

namespace {

auto col_i64(const runtime::Table& t, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

auto col_str(const runtime::Table& t, const std::string& name) -> std::vector<std::string> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<std::string>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

auto interpret_expr(std::string_view src, const runtime::TableRegistry& tables) -> runtime::Table {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE(result.has_value());
    return std::move(*result);
}

}  // namespace

TEST_CASE("join: inner join on single key", "[join]") {
    runtime::Table left;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});
    left.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table right;
    right.add_column("id", Column<std::int64_t>{2, 3, 4});
    right.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("left", std::move(left));
    tables.emplace("right", std::move(right));

    auto out = interpret_expr("left join right on id;", tables);

    CHECK(out.rows() == 2);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 300});
}

TEST_CASE("join: left join preserves left rows", "[join]") {
    runtime::Table left;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});
    left.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table right;
    right.add_column("id", Column<std::int64_t>{2, 3, 4});
    right.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("left", std::move(left));
    tables.emplace("right", std::move(right));

    auto out = interpret_expr("left left join right on id;", tables);

    CHECK(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{0, 200, 300});
}

TEST_CASE("join: multi-key join and duplicate column names", "[join]") {
    runtime::Table left;
    left.add_column("k1", Column<std::int64_t>{1, 1});
    left.add_column("k2", Column<std::string>{"A", "B"});
    left.add_column("val", Column<std::int64_t>{10, 20});

    runtime::Table right;
    right.add_column("k1", Column<std::int64_t>{1, 1});
    right.add_column("k2", Column<std::string>{"A", "B"});
    right.add_column("val", Column<std::int64_t>{100, 200});

    runtime::TableRegistry tables;
    tables.emplace("left", std::move(left));
    tables.emplace("right", std::move(right));

    auto out = interpret_expr("left join right on {k1, k2};", tables);

    CHECK(out.rows() == 2);
    CHECK(out.find("k1") != nullptr);
    CHECK(out.find("k2") != nullptr);
    CHECK(out.find("val") != nullptr);
    CHECK(out.find("val_right") != nullptr);
    CHECK(out.find("k1_right") == nullptr);
    CHECK(out.find("k2_right") == nullptr);
    CHECK(col_i64(out, "val") == std::vector<std::int64_t>{10, 20});
    CHECK(col_i64(out, "val_right") == std::vector<std::int64_t>{100, 200});
    CHECK(col_str(out, "k2") == std::vector<std::string>{"A", "B"});
}
