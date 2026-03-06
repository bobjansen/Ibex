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
    std::vector<std::string> out;
    out.reserve(values->size());
    for (const auto& v : *values) {
        out.emplace_back(v);
    }
    return out;
}

auto col_ts_nanos(const runtime::Table& t, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<Timestamp>>(col);
    REQUIRE(values != nullptr);
    std::vector<std::int64_t> out;
    out.reserve(values->size());
    for (const auto& v : *values) {
        out.push_back(v.nanos);
    }
    return out;
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

auto interpret_error(std::string_view src, const runtime::TableRegistry& tables) -> std::string {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE_FALSE(result.has_value());
    return result.error();
}

}  // namespace

TEST_CASE("join: inner join on single key", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on id;", tables);

    CHECK(out.rows() == 2);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 300});
}

TEST_CASE("join: left join preserves left rows", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs left join rhs on id;", tables);

    CHECK(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{0, 200, 300});
}

TEST_CASE("join: multi-key join and duplicate column names", "[join]") {
    runtime::Table lhs;
    lhs.add_column("k1", Column<std::int64_t>{1, 1});
    lhs.add_column("k2", Column<std::string>{"A", "B"});
    lhs.add_column("val", Column<std::int64_t>{10, 20});

    runtime::Table rhs;
    rhs.add_column("k1", Column<std::int64_t>{1, 1});
    rhs.add_column("k2", Column<std::string>{"A", "B"});
    rhs.add_column("val", Column<std::int64_t>{100, 200});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on {k1, k2};", tables);

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

TEST_CASE("join: asof join matches latest right row at-or-before left time", "[join]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{10}, Timestamp{20}, Timestamp{30}});
    lhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    lhs.add_column("lval", Column<std::int64_t>{1, 2, 3});
    lhs.time_index = "ts";

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{5}, Timestamp{20}, Timestamp{25}});
    rhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    rhs.add_column("rval", Column<std::int64_t>{50, 200, 250});
    rhs.time_index = "ts";

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on {ts, symbol};", tables);

    CHECK(out.rows() == 3);
    REQUIRE(out.time_index.has_value());
    CHECK(*out.time_index == "ts");
    CHECK(col_ts_nanos(out, "ts") == std::vector<std::int64_t>{10, 20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{50, 200, 250});
}

TEST_CASE("join: asof join preserves left rows and fills right defaults", "[join]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}, Timestamp{2}});
    lhs.add_column("symbol", Column<std::string>{"A", "B"});
    lhs.add_column("lval", Column<std::int64_t>{10, 20});
    lhs.time_index = "ts";

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{2}});
    rhs.add_column("symbol", Column<std::string>{"A"});
    rhs.add_column("rval", Column<std::int64_t>{99});
    rhs.time_index = "ts";

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on {ts, symbol};", tables);

    CHECK(out.rows() == 2);
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{0, 0});
}

TEST_CASE("join: asof join requires time index key in on-list", "[join]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    lhs.add_column("symbol", Column<std::string>{"A"});
    lhs.time_index = "ts";

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    rhs.add_column("symbol", Column<std::string>{"A"});
    rhs.time_index = "ts";

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto error = interpret_error("lhs asof join rhs on symbol;", tables);
    CHECK(error.find("include the time index") != std::string::npos);
}


TEST_CASE("join: semi join keeps matching left rows only", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3, 4});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30, 40});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 2, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 201, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs semi join rhs on id;", tables);

    CHECK(out.rows() == 2);
    CHECK(out.find("rval") == nullptr);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 4});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 40});
}

TEST_CASE("join: anti join keeps non-matching left rows only", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3, 4});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30, 40});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs anti join rhs on id;", tables);

    CHECK(out.rows() == 2);
    CHECK(out.find("rval") == nullptr);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 30});
}

TEST_CASE("join: outer join row count and key values", "[join]") {
    // lhs: id {1, 2, 3},  lval {10, 20, 30}
    // rhs: id {2, 3, 4},  rval {200, 300, 400}
    // outer join on id → 4 rows:
    //   id=1 (left-only), id=2 (matched), id=3 (matched), id=4 (right-only)
    runtime::Table lhs;
    lhs.add_column("id",   Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id",   Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs outer join rhs on id;", tables);

    REQUIRE(out.rows() == 4);
    CHECK(col_i64(out, "id")   == std::vector<std::int64_t>{1, 2, 3, 4});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 30, 0});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{0, 200, 300, 400});
}

TEST_CASE("join: outer join null semantics — left-only rows null right columns", "[join]") {
    // lhs: id {1, 2},  name {"alice", "bob"}
    // rhs: id {2, 3},  score {20.0, 30.0}
    // Row order: left rows first (left-table order), then unmatched right rows.
    //   row 0 → id=1, left-only  → score NULL
    //   row 1 → id=2, matched    → score 20.0
    //   row 2 → id=3, right-only → name NULL
    runtime::Table lhs;
    lhs.add_column("id",   Column<std::int64_t>{1, 2});
    lhs.add_column("name", Column<std::string>{"alice", "bob"});

    runtime::Table rhs;
    rhs.add_column("id",    Column<std::int64_t>{2, 3});
    rhs.add_column("score", Column<double>{20.0, 30.0});

    auto result = runtime::join_tables(lhs, rhs, ir::JoinKind::Outer, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;

    REQUIRE(t.rows() == 3);
    CHECK(col_i64(t, "id") == std::vector<std::int64_t>{1, 2, 3});

    const auto& name_entry  = t.columns[t.index.at("name")];
    const auto& score_entry = t.columns[t.index.at("score")];

    // row 0: id=1, left-only → name valid, score null
    CHECK_FALSE(runtime::is_null(name_entry,  0));
    CHECK(      runtime::is_null(score_entry, 0));

    // row 1: id=2, matched → both valid
    CHECK_FALSE(runtime::is_null(name_entry,  1));
    CHECK_FALSE(runtime::is_null(score_entry, 1));

    // row 2: id=3, right-only → name null, score valid
    CHECK(      runtime::is_null(name_entry,  2));
    CHECK_FALSE(runtime::is_null(score_entry, 2));
}

TEST_CASE("join: outer join disjoint tables — all rows unmatched", "[join]") {
    // lhs: id {1}, rhs: id {2} — no matches at all
    // 2 rows total; left row gets null rval, right row gets null lval
    runtime::Table lhs;
    lhs.add_column("id",   Column<std::int64_t>{1});
    lhs.add_column("lval", Column<std::int64_t>{10});

    runtime::Table rhs;
    rhs.add_column("id",   Column<std::int64_t>{2});
    rhs.add_column("rval", Column<std::int64_t>{20});

    auto result = runtime::join_tables(lhs, rhs, ir::JoinKind::Outer, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;

    REQUIRE(t.rows() == 2);
    CHECK(col_i64(t, "id") == std::vector<std::int64_t>{1, 2});

    const auto& lval_entry = t.columns[t.index.at("lval")];
    const auto& rval_entry = t.columns[t.index.at("rval")];

    // row 0: id=1, left-only → lval valid, rval null
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK(      runtime::is_null(rval_entry, 0));

    // row 1: id=2, right-only → lval null, rval valid
    CHECK(      runtime::is_null(lval_entry, 1));
    CHECK_FALSE(runtime::is_null(rval_entry, 1));
}

TEST_CASE("join: outer join identical tables — all rows matched, no nulls", "[join]") {
    // When both tables have the same keys, every row matches → no nulls
    runtime::Table lhs;
    lhs.add_column("id",   Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id",   Column<std::int64_t>{1, 2, 3});
    rhs.add_column("rval", Column<std::int64_t>{100, 200, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs outer join rhs on id;", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id")   == std::vector<std::int64_t>{1, 2, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{100, 200, 300});

    // No validity bitmaps should be set when there are no nulls
    const auto& lval_entry = out.columns[out.index.at("lval")];
    const auto& rval_entry = out.columns[out.index.at("rval")];
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK_FALSE(runtime::is_null(rval_entry, 0));
}

TEST_CASE("join: right join preserves right rows", "[join]") {
    // lhs: id {1, 2, 3},  lval {10, 20, 30}
    // rhs: id {2, 3, 4},  rval {200, 300, 400}
    // right join on id → 3 rows: id=2, id=3, id=4
    //   id=4 has no left match → lval null
    runtime::Table lhs;
    lhs.add_column("id",   Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id",   Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs right join rhs on id;", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id")   == std::vector<std::int64_t>{2, 3, 4});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 300, 400});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 30, 0});

    const auto& lval_entry = out.columns[out.index.at("lval")];
    // row 0 (id=2) and row 1 (id=3) matched → lval not null
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK_FALSE(runtime::is_null(lval_entry, 1));
    // row 2 (id=4) is right-only → lval null
    CHECK(runtime::is_null(lval_entry, 2));
}

TEST_CASE("join: cross join returns cartesian product", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});

    runtime::Table rhs;
    rhs.add_column("group", Column<std::string>{"A", "B", "C"});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs cross join rhs;", tables);

    CHECK(out.rows() == 6);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 1, 1, 2, 2, 2});
    CHECK(col_str(out, "group") == std::vector<std::string>{"A", "B", "C", "A", "B", "C"});
}
