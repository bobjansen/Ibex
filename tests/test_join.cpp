// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
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

/// A program that must fail to parse or lower; returns the message.
auto interpret_error_at_parse(std::string_view src) -> std::string {
    auto parsed = parser::parse(src);
    if (!parsed.has_value()) {
        return parsed.error().format();
    }
    auto lowered = parser::lower(*parsed);
    REQUIRE_FALSE(lowered.has_value());
    return lowered.error().message;
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

TEST_CASE("join: mapped key pair retains both key columns", "[join]") {
    runtime::Table lhs;
    lhs.add_column("left_id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("right_id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on {left_id = right_id};", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "left_id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_i64(out, "right_id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 300});
}

TEST_CASE("join: inner join on Int64 key preserves duplicate matches", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});
    lhs.add_column("lval", Column<std::int64_t>{10, 20});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 2, 3});
    rhs.add_column("rval", Column<std::int64_t>{200, 201, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on id;", tables);

    CHECK(out.rows() == 2);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 2});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 20});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 201});
}

TEST_CASE("join: inner join preserves left row order when left side is smaller", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{2, 1});
    lhs.add_column("lval", Column<std::int64_t>{20, 10});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 1, 2, 3});
    rhs.add_column("rval", Column<std::int64_t>{100, 200, 101, 201, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on id;", tables);

    CHECK(out.rows() == 4);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 2, 1, 1});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 20, 10, 10});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 201, 100, 101});
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

// Row order is outside the join contract on purpose (SPEC.md §5.6): the engine
// picks a build side by size, and that choice is observable. This
// small-left/large-right path emits matched rows in the probe (right) side's
// scan order (cheaper: no reassembly pass, and it preserves locality for any
// downstream join that probes this output), with unmatched left rows appended.
//
// So this test and its siblings below pin *this path's* behaviour, not a
// promise to callers. A strategy change may legitimately rewrite them —
// what it may not do is make the order depend on something a caller cannot
// see while some other test still asserts the old one. Callers that need an
// order write `order`.
TEST_CASE("join: left join emits matches in right-scan order when left side is smaller", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{2, 1, 4});
    lhs.add_column("lval", Column<std::int64_t>{20, 10, 40});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 1, 3, 5});
    rhs.add_column("rval", Column<std::int64_t>{100, 200, 101, 300, 500});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs left join rhs on id;", tables);

    CHECK(out.rows() == 4);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 1, 4});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 10, 40});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{100, 200, 101, 0});

    const auto& rval_entry = out.columns[out.index.at("rval")];
    CHECK_FALSE(runtime::is_null(rval_entry, 0));
    CHECK_FALSE(runtime::is_null(rval_entry, 1));
    CHECK_FALSE(runtime::is_null(rval_entry, 2));
    CHECK(runtime::is_null(rval_entry, 3));
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

    // Both same-name keys fold, so neither needs the clause; only the non-key
    // `val` collides and is renamed on both sides.
    auto out = interpret_expr(R"(lhs join rhs on {k1, k2} suffix { "_l", "_r" };)", tables);

    CHECK(out.rows() == 2);
    CHECK(out.find("k1") != nullptr);
    CHECK(out.find("k2") != nullptr);
    CHECK(out.find("val_l") != nullptr);
    CHECK(out.find("val_r") != nullptr);
    CHECK(out.find("val") == nullptr);
    CHECK(out.find("k1_l") == nullptr);
    CHECK(out.find("k1_r") == nullptr);
    CHECK(out.find("k2_l") == nullptr);
    CHECK(out.find("k2_r") == nullptr);
    CHECK(col_i64(out, "val_l") == std::vector<std::int64_t>{10, 20});
    CHECK(col_i64(out, "val_r") == std::vector<std::int64_t>{100, 200});
    CHECK(col_str(out, "k2") == std::vector<std::string>{"A", "B"});
}

TEST_CASE("join: a non-key collision without a suffix clause is rejected", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});
    lhs.add_column("val", Column<std::int64_t>{10, 20});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2});
    rhs.add_column("val", Column<std::int64_t>{100, 200});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    const auto err = interpret_error("lhs join rhs on id;", tables);
    CHECK(err.find("\"val\"") != std::string::npos);
    CHECK(err.find("both inputs") != std::string::npos);
    CHECK(err.find("suffix") != std::string::npos);
}

TEST_CASE("join: asof join matches latest right row at-or-before left time", "[join]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{10}, Timestamp{20}, Timestamp{30}});
    lhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    lhs.add_column("lval", Column<std::int64_t>{1, 2, 3});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{5}, Timestamp{20}, Timestamp{25}});
    rhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    rhs.add_column("rval", Column<std::int64_t>{50, 200, 250});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on {ts, symbol};", tables);

    CHECK(out.rows() == 3);
    REQUIRE(out.time_index().has_value());
    CHECK(*out.time_index() == "ts");
    CHECK(col_ts_nanos(out, "ts") == std::vector<std::int64_t>{10, 20, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{50, 200, 250});
}

TEST_CASE("join: asof join time-only key (no equality keys)", "[join][asof]") {
    // Exercises the no-equality-key fast path: only the time index is joined on.
    runtime::Table lhs;
    lhs.add_column("ts",
                   Column<Timestamp>{Timestamp{10}, Timestamp{20}, Timestamp{30}, Timestamp{40}});
    lhs.add_column("lval", Column<std::int64_t>{1, 2, 3, 4});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    // Right times: 5, 20, 25 — note row at 40 has no right strictly... at-or-before.
    rhs.add_column("ts", Column<Timestamp>{Timestamp{5}, Timestamp{20}, Timestamp{25}});
    rhs.add_column("rval", Column<std::int64_t>{50, 200, 250});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on ts;", tables);

    CHECK(out.rows() == 4);
    REQUIRE(out.time_index().has_value());
    CHECK(*out.time_index() == "ts");
    CHECK(col_ts_nanos(out, "ts") == std::vector<std::int64_t>{10, 20, 30, 40});
    // ts=10 -> latest right <=10 is 5 (50); ts=20 -> 20 (200);
    // ts=30 -> 25 (250); ts=40 -> 25 (250).
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{50, 200, 250, 250});
}

TEST_CASE("join: asof join time-only key with an Int time index", "[join][asof]") {
    // An Int (non-Timestamp) time index exercises the buffer-materialisation
    // fallback in as_int64_view — Timestamp reads the column storage directly,
    // but Int/Date must convert into a temporary int64 array first.
    runtime::Table lhs;
    lhs.add_column("ts", Column<std::int64_t>{10, 20, 30, 40});
    lhs.add_column("lval", Column<std::int64_t>{1, 2, 3, 4});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<std::int64_t>{5, 20, 25});
    rhs.add_column("rval", Column<std::int64_t>{50, 200, 250});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on ts;", tables);

    CHECK(out.rows() == 4);
    REQUIRE(out.time_index().has_value());
    CHECK(*out.time_index() == "ts");
    CHECK(col_i64(out, "ts") == std::vector<std::int64_t>{10, 20, 30, 40});
    // Same at-or-before semantics as the Timestamp case.
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{50, 200, 250, 250});
}

TEST_CASE("join: asof join time-only key, left before all right rows fills default",
          "[join][asof]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}, Timestamp{100}});
    lhs.add_column("lval", Column<std::int64_t>{7, 8});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{50}});
    rhs.add_column("rval", Column<std::int64_t>{99});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on ts;", tables);

    CHECK(out.rows() == 2);
    // ts=1 has no right at-or-before -> default 0; ts=100 -> 50 (99).
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{0, 99});
}

TEST_CASE("join: asof join two equality keys (generic grouped path)", "[join][asof]") {
    // Two equality keys (symbol + venue) exercise the generic multi-key path,
    // not the single-key fast path. venue must discriminate the match.
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{10}, Timestamp{20}, Timestamp{30}});
    lhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    lhs.add_column("venue", Column<std::string>{"X", "Y", "X"});
    lhs.add_column("lval", Column<std::int64_t>{1, 2, 3});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{5}, Timestamp{8}, Timestamp{20}});
    rhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    rhs.add_column("venue", Column<std::string>{"X", "Y", "X"});
    rhs.add_column("rval", Column<std::int64_t>{50, 80, 200});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on {ts, symbol, venue};", tables);

    CHECK(out.rows() == 3);
    // (10,A,X)->X@5=50; (20,A,Y)->Y@8=80; (30,A,X)->X@20=200 (not Y).
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{50, 80, 200});
}

TEST_CASE("join: asof join preserves left rows and fills right defaults", "[join]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}, Timestamp{2}});
    lhs.add_column("symbol", Column<std::string>{"A", "B"});
    lhs.add_column("lval", Column<std::int64_t>{10, 20});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{2}});
    rhs.add_column("symbol", Column<std::string>{"A"});
    rhs.add_column("rval", Column<std::int64_t>{99});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

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
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    rhs.add_column("symbol", Column<std::string>{"A"});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto error = interpret_error("lhs asof join rhs on symbol;", tables);
    CHECK(error.find("include the time index") != std::string::npos);
    // Diagnostic content: name what was provided and suggest the fix.
    CHECK(error.find("got:  on symbol") != std::string::npos);
    CHECK(error.find("on {ts, symbol}") != std::string::npos);
}

TEST_CASE("join: asof error names which side is a DataFrame and suggests as_timeframe",
          "[join][asof][diagnostic]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}, Timestamp{2}});
    lhs.add_column("symbol", Column<std::string>{"A", "A"});
    // No time_index — lhs is a DataFrame.

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    rhs.add_column("symbol", Column<std::string>{"A"});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto error = interpret_error("lhs asof join rhs on {ts, symbol};", tables);
    CHECK(error.find("requires both sides to be TimeFrame") != std::string::npos);
    CHECK(error.find("left side is a DataFrame") != std::string::npos);
    CHECK(error.find("right is TimeFrame on 'ts'") != std::string::npos);
    // Suggests a concrete fix using the discoverable Timestamp column.
    CHECK(error.find("as_timeframe(left, \"ts\")") != std::string::npos);
}

TEST_CASE("join: asof error reports both sides when neither is a TimeFrame",
          "[join][asof][diagnostic]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    lhs.add_column("symbol", Column<std::string>{"A"});

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    rhs.add_column("symbol", Column<std::string>{"A"});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto error = interpret_error("lhs asof join rhs on {ts, symbol};", tables);
    CHECK(error.find("neither side has been promoted") != std::string::npos);
    CHECK(error.find("as_timeframe(left, \"ts\")") != std::string::npos);
    CHECK(error.find("as_timeframe(right, \"ts\")") != std::string::npos);
}

TEST_CASE("join: asof error requests a mapped time-index pair", "[join][asof][diagnostic]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}});
    lhs.add_column("symbol", Column<std::string>{"A"});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("event_time", Column<Timestamp>{Timestamp{1}});
    rhs.add_column("symbol", Column<std::string>{"A"});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("event_time"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto error = interpret_error("lhs asof join rhs on symbol;", tables);
    CHECK(error.find("time index 'ts' (left)") != std::string::npos);
    CHECK(error.find("'event_time' (right)") != std::string::npos);
    CHECK(error.find("ts = event_time") != std::string::npos);
}

TEST_CASE("join: asof supports differently named time indexes", "[join][asof]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{1}, Timestamp{3}});
    lhs.add_column("lval", Column<std::int64_t>{10, 30});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("event_time", Column<Timestamp>{Timestamp{1}, Timestamp{2}});
    rhs.add_column("rval", Column<std::int64_t>{100, 200});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("event_time"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs asof join rhs on {ts = event_time};", tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 30});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{100, 200});
    REQUIRE(out.find("event_time") != nullptr);
}

TEST_CASE("join: asof error names which side is unsorted", "[join][asof][diagnostic]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<Timestamp>{Timestamp{3}, Timestamp{1}, Timestamp{2}});
    lhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    lhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::Table rhs;
    rhs.add_column("ts", Column<Timestamp>{Timestamp{1}, Timestamp{2}, Timestamp{3}});
    rhs.add_column("symbol", Column<std::string>{"A", "A", "A"});
    rhs.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto error = interpret_error("lhs asof join rhs on {ts, symbol};", tables);
    CHECK(error.find("left is not sorted ascending") != std::string::npos);
    CHECK(error.find("look-ahead bias") != std::string::npos);
    CHECK(error.find("[order ts]") != std::string::npos);
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

TEST_CASE("join: semi join preserves left row order when left side is smaller", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{2, 1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{20, 10, 21, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 1, 2, 4});
    rhs.add_column("rval", Column<std::int64_t>{100, 200, 101, 201, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs semi join rhs on id;", tables);

    CHECK(out.rows() == 3);
    CHECK(out.find("rval") == nullptr);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 1, 2});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 10, 21});
}

// The streaming semi/anti operator evaluates its predicate across the worker
// pool once a chunk is large enough, and stitches the per-range index lists back
// together. Every smaller case above stays under that gate, so this is the only
// one that reaches the threaded path -- and what it is really checking is that
// the stitched result is still the left's own row ORDER, not just its row count.
TEST_CASE("join: semi and anti keep left order across a threaded predicate", "[join]") {
    constexpr std::int64_t kRows = 300000;  // over the operator's fan-out gate
    Column<std::int64_t> ids;
    Column<std::int64_t> lvals;
    ids.reserve(kRows);
    lvals.reserve(kRows);
    for (std::int64_t i = 0; i < kRows; ++i) {
        // Descending, so a result in ascending key order would be visibly wrong
        // rather than accidentally right.
        ids.push_back(kRows - 1 - i);
        lvals.push_back((kRows - 1 - i) * 10);
    }
    Column<std::int64_t> rids;
    for (std::int64_t i = 0; i < kRows; i += 3) {
        rids.push_back(i);
    }

    runtime::Table lhs;
    lhs.add_column("id", std::move(ids));
    lhs.add_column("lval", std::move(lvals));
    runtime::Table rhs;
    rhs.add_column("id", std::move(rids));

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    std::vector<std::int64_t> want_semi;
    std::vector<std::int64_t> want_anti;
    for (std::int64_t i = 0; i < kRows; ++i) {
        const std::int64_t id = kRows - 1 - i;
        (id % 3 == 0 ? want_semi : want_anti).push_back(id);
    }

    auto semi = interpret_expr("lhs semi join rhs on id;", tables);
    CHECK(col_i64(semi, "id") == want_semi);
    CHECK(semi.rows() == want_semi.size());

    auto anti = interpret_expr("lhs anti join rhs on id;", tables);
    CHECK(col_i64(anti, "id") == want_anti);
    CHECK(anti.rows() == want_anti.size());

    // The payload column must be carried through the same stitching as the key.
    const auto lval = col_i64(semi, "lval");
    REQUIRE(lval.size() == want_semi.size());
    for (std::size_t i = 0; i < lval.size(); ++i) {
        REQUIRE(lval[i] == want_semi[i] * 10);
    }
}

TEST_CASE("join: dense integer intersection preserves semi and anti membership", "[join]") {
    constexpr std::int64_t kLeftRows = 20'000;
    constexpr std::int64_t kRightRows = 80'000;  // over the swapped-build gate
    Column<std::int64_t> left_ids;
    left_ids.reserve(kLeftRows);
    for (std::int64_t row = 0; row < kLeftRows; ++row) {
        left_ids.push_back(row - (kLeftRows / 2));
    }
    Column<std::int64_t> right_ids;
    right_ids.reserve(kRightRows);
    for (std::int64_t row = 0; row < kRightRows; ++row) {
        // Every even key in the left's moderately dense range, repeated.
        right_ids.push_back(-(kLeftRows / 2) + 2 * (row % (kLeftRows / 2)));
    }
    runtime::Table lhs;
    lhs.add_column("id", std::move(left_ids));
    runtime::Table rhs;
    rhs.add_column("id", std::move(right_ids));
    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto semi = interpret_expr("lhs semi join rhs on id;", tables);
    auto anti = interpret_expr("lhs anti join rhs on id;", tables);
    REQUIRE(semi.rows() == static_cast<std::size_t>(kLeftRows / 2));
    REQUIRE(anti.rows() == static_cast<std::size_t>(kLeftRows / 2));
    const auto semi_ids = col_i64(semi, "id");
    const auto anti_ids = col_i64(anti, "id");
    for (std::size_t row = 0; row < semi_ids.size(); ++row) {
        CHECK(semi_ids[row] % 2 == 0);
        CHECK(anti_ids[row] % 2 != 0);
    }
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

TEST_CASE("join: anti join preserves left row order when left side is smaller", "[join]") {
    runtime::Table lhs;
    lhs.add_column("key", Column<std::string>{"B", "A", "C", "A"});
    lhs.add_column("lval", Column<std::int64_t>{20, 10, 30, 11});

    runtime::Table rhs;
    rhs.add_column("key", Column<std::string>{"A", "A", "D", "E", "F"});
    rhs.add_column("rval", Column<std::int64_t>{100, 101, 400, 500, 600});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs anti join rhs on key;", tables);

    CHECK(out.rows() == 2);
    CHECK(out.find("rval") == nullptr);
    CHECK(col_str(out, "key") == std::vector<std::string>{"B", "C"});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 30});
}

TEST_CASE("join: anti join keeps all left rows when smaller left side has no string-key matches",
          "[join]") {
    runtime::Table lhs;
    lhs.add_column("key", Column<std::string>{"K000000", "K000001", "K000002", "K000003"});
    lhs.add_column("lval", Column<std::int64_t>{10, 11, 12, 13});

    runtime::Table rhs;
    rhs.add_column("key", Column<std::string>{"Z000000", "Z000001", "Z000002", "Z000003", "Z000004",
                                              "Z000005"});
    rhs.add_column("rval", Column<std::int64_t>{100, 101, 102, 103, 104, 105});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs anti join rhs on key;", tables);

    CHECK(out.rows() == 4);
    CHECK(out.find("rval") == nullptr);
    CHECK(col_str(out, "key") ==
          std::vector<std::string>{"K000000", "K000001", "K000002", "K000003"});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 11, 12, 13});
}

TEST_CASE("join: multi-key semi join preserves left row order when left side is smaller",
          "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 1, 2});
    lhs.add_column("bucket", Column<std::int64_t>{10, 20, 10});
    lhs.add_column("lval", Column<std::int64_t>{100, 200, 300});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 1, 2, 3});
    rhs.add_column("bucket", Column<std::int64_t>{20, 20, 10, 30});
    rhs.add_column("rval", Column<std::int64_t>{500, 501, 600, 700});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs semi join rhs on {id, bucket};", tables);

    CHECK(out.rows() == 2);
    CHECK(out.find("rval") == nullptr);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2});
    CHECK(col_i64(out, "bucket") == std::vector<std::int64_t>{20, 10});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{200, 300});
}

TEST_CASE("join: outer join row count and key values", "[join]") {
    // lhs: id {1, 2, 3},  lval {10, 20, 30}
    // rhs: id {2, 3, 4},  rval {200, 300, 400}
    // outer join on id -> 4 rows:
    //   id=1 (left-only), id=2 (matched), id=3 (matched), id=4 (right-only)
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs outer join rhs on id;", tables);

    REQUIRE(out.rows() == 4);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3, 4});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 30, 0});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{0, 200, 300, 400});
}

// See the comment on "left join emits matches in right-scan order..." above:
// row order is outside the join contract. Matched rows come out in the probe
// (right) side's scan order; unmatched left rows are appended, then unmatched
// right rows.
TEST_CASE("join: outer join emits matches in right-scan order when left side is smaller",
          "[join]") {
    runtime::Table lhs;
    lhs.add_column("key", Column<std::string>{"B", "A", "D"});
    lhs.add_column("lval", Column<std::int64_t>{20, 10, 40});

    runtime::Table rhs;
    rhs.add_column("key", Column<std::string>{"A", "C", "B", "E"});
    rhs.add_column("rval", Column<std::int64_t>{100, 300, 200, 500});

    auto result = runtime::join_tables(lhs, rhs, ir::JoinKind::Outer, {"key"});
    REQUIRE(result.has_value());
    auto& t = *result;

    REQUIRE(t.rows() == 5);
    CHECK(col_str(t, "key") == std::vector<std::string>{"A", "B", "D", "C", "E"});
    CHECK(col_i64(t, "lval") == std::vector<std::int64_t>{10, 20, 40, 0, 0});
    CHECK(col_i64(t, "rval") == std::vector<std::int64_t>{100, 200, 0, 300, 500});

    const auto& lval_entry = t.columns[t.index.at("lval")];
    const auto& rval_entry = t.columns[t.index.at("rval")];
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK_FALSE(runtime::is_null(lval_entry, 1));
    CHECK_FALSE(runtime::is_null(lval_entry, 2));
    CHECK(runtime::is_null(lval_entry, 3));
    CHECK(runtime::is_null(lval_entry, 4));
    CHECK_FALSE(runtime::is_null(rval_entry, 0));
    CHECK_FALSE(runtime::is_null(rval_entry, 1));
    CHECK(runtime::is_null(rval_entry, 2));
    CHECK_FALSE(runtime::is_null(rval_entry, 3));
    CHECK_FALSE(runtime::is_null(rval_entry, 4));
}

TEST_CASE("join: outer join null semantics - left-only rows null right columns", "[join]") {
    // lhs: id {1, 2},  name {"alice", "bob"}
    // rhs: id {2, 3},  score {20.0, 30.0}
    // Row order: left rows first (left-table order), then unmatched right rows.
    //   row 0 -> id=1, left-only  -> score NULL
    //   row 1 -> id=2, matched    -> score 20.0
    //   row 2 -> id=3, right-only -> name NULL
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});
    lhs.add_column("name", Column<std::string>{"alice", "bob"});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3});
    rhs.add_column("score", Column<double>{20.0, 30.0});

    auto result = runtime::join_tables(lhs, rhs, ir::JoinKind::Outer, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;

    REQUIRE(t.rows() == 3);
    CHECK(col_i64(t, "id") == std::vector<std::int64_t>{1, 2, 3});

    const auto& name_entry = t.columns[t.index.at("name")];
    const auto& score_entry = t.columns[t.index.at("score")];

    // row 0: id=1, left-only -> name valid, score null
    CHECK_FALSE(runtime::is_null(name_entry, 0));
    CHECK(runtime::is_null(score_entry, 0));

    // row 1: id=2, matched -> both valid
    CHECK_FALSE(runtime::is_null(name_entry, 1));
    CHECK_FALSE(runtime::is_null(score_entry, 1));

    // row 2: id=3, right-only -> name null, score valid
    CHECK(runtime::is_null(name_entry, 2));
    CHECK_FALSE(runtime::is_null(score_entry, 2));
}

TEST_CASE("join: outer join disjoint tables - all rows unmatched", "[join]") {
    // lhs: id {1}, rhs: id {2} - no matches at all
    // 2 rows total; left row gets null rval, right row gets null lval
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1});
    lhs.add_column("lval", Column<std::int64_t>{10});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2});
    rhs.add_column("rval", Column<std::int64_t>{20});

    auto result = runtime::join_tables(lhs, rhs, ir::JoinKind::Outer, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;

    REQUIRE(t.rows() == 2);
    CHECK(col_i64(t, "id") == std::vector<std::int64_t>{1, 2});

    const auto& lval_entry = t.columns[t.index.at("lval")];
    const auto& rval_entry = t.columns[t.index.at("rval")];

    // row 0: id=1, left-only -> lval valid, rval null
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK(runtime::is_null(rval_entry, 0));

    // row 1: id=2, right-only -> lval null, rval valid
    CHECK(runtime::is_null(lval_entry, 1));
    CHECK_FALSE(runtime::is_null(rval_entry, 1));
}

TEST_CASE("join: outer join on a mapped key nulls each side's own key column", "[join]") {
    // A mapped key keeps both native columns, so nothing folds and an unmatched
    // row has no peer to fill its own key from -- unlike the same-name case
    // above, where one column carries whichever side turned up.
    runtime::Table lhs;
    lhs.add_column("lid", Column<std::int64_t>{1});
    lhs.add_column("lval", Column<std::int64_t>{10});

    runtime::Table rhs;
    rhs.add_column("rid", Column<std::int64_t>{2});
    rhs.add_column("rval", Column<std::int64_t>{20});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs outer join rhs on {lid = rid};", tables);

    REQUIRE(out.rows() == 2);
    const auto& lid = out.columns[out.index.at("lid")];
    const auto& rid = out.columns[out.index.at("rid")];

    // row 0: the left row, unmatched -> its own key present, the right's null.
    CHECK_FALSE(runtime::is_null(lid, 0));
    CHECK(runtime::is_null(rid, 0));

    // row 1: the right row, unmatched -> the mirror image.
    CHECK(runtime::is_null(lid, 1));
    CHECK_FALSE(runtime::is_null(rid, 1));
}

TEST_CASE("join: outer join identical tables - all rows matched, no nulls", "[join]") {
    // When both tables have the same keys, every row matches -> no nulls
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    rhs.add_column("rval", Column<std::int64_t>{100, 200, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs outer join rhs on id;", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
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
    // right join on id -> 3 rows: id=2, id=3, id=4
    //   id=4 has no left match -> lval null
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("lval", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("rval", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs right join rhs on id;", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3, 4});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{200, 300, 400});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{20, 30, 0});

    const auto& lval_entry = out.columns[out.index.at("lval")];
    // row 0 (id=2) and row 1 (id=3) matched -> lval not null
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK_FALSE(runtime::is_null(lval_entry, 1));
    // row 2 (id=4) is right-only -> lval null
    CHECK(runtime::is_null(lval_entry, 2));
}

// See the comment on "left join emits matches in right-scan order..." above:
// row order is outside the join contract. Matches come out in right-scan
// order, then unmatched right rows are appended.
TEST_CASE(
    "join: right join emits matches in right-scan order, then unmatched right rows, when left "
    "side is smaller",
    "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{2, 1});
    lhs.add_column("lval", Column<std::int64_t>{20, 10});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 3, 2, 4, 1});
    rhs.add_column("rval", Column<std::int64_t>{100, 300, 200, 400, 101});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs right join rhs on id;", tables);

    REQUIRE(out.rows() == 5);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 1, 3, 4});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{10, 20, 10, 0, 0});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{100, 200, 101, 300, 400});

    const auto& lval_entry = out.columns[out.index.at("lval")];
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK_FALSE(runtime::is_null(lval_entry, 1));
    CHECK_FALSE(runtime::is_null(lval_entry, 2));
    CHECK(runtime::is_null(lval_entry, 3));
    CHECK(runtime::is_null(lval_entry, 4));
}

// See the comment on "left join emits matches in right-scan order..." above:
// row order is outside the join contract. Matches come out in right-scan
// order, unmatched left rows next, then unmatched right rows.
TEST_CASE("join: multi-key outer join emits matches in right-scan order when left side is smaller",
          "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{2, 1});
    lhs.add_column("bucket", Column<std::int64_t>{10, 20});
    lhs.add_column("lval", Column<std::int64_t>{200, 100});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 3, 2, 4});
    rhs.add_column("bucket", Column<std::int64_t>{20, 30, 10, 40});
    rhs.add_column("rval", Column<std::int64_t>{500, 700, 600, 800});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs outer join rhs on {id, bucket};", tables);

    REQUIRE(out.rows() == 4);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3, 4});
    CHECK(col_i64(out, "bucket") == std::vector<std::int64_t>{20, 10, 30, 40});
    CHECK(col_i64(out, "lval") == std::vector<std::int64_t>{100, 200, 0, 0});
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{500, 600, 700, 800});

    const auto& lval_entry = out.columns[out.index.at("lval")];
    CHECK_FALSE(runtime::is_null(lval_entry, 0));
    CHECK_FALSE(runtime::is_null(lval_entry, 1));
    CHECK(runtime::is_null(lval_entry, 2));
    CHECK(runtime::is_null(lval_entry, 3));
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

// --- Non-equijoin / theta join tests -----------------------------------------

TEST_CASE("non-equijoin: inner join on inequality predicate", "[join][non-equijoin]") {
    // left.a < right.b  ->  only pairs where a < b
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 3, 5});

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{2, 4});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // Pairs: (1,2)OK (1,4)OK (3,4)OK (3,2)X (5,2)X (5,4)X  -> 3 rows
    auto out = interpret_expr("lhs join rhs on a < b;", tables);

    CHECK(out.rows() == 3);
    CHECK(col_i64(out, "a") == std::vector<std::int64_t>{1, 1, 3});
    CHECK(col_i64(out, "b") == std::vector<std::int64_t>{2, 4, 4});
}

TEST_CASE("non-equijoin: inner join compound predicate (range)", "[join][non-equijoin]") {
    // Pairs where lo <= val && val < hi
    runtime::Table ticks;
    ticks.add_column("val", Column<std::int64_t>{1, 5, 10, 15});

    runtime::Table windows;
    windows.add_column("lo", Column<std::int64_t>{0, 8});
    windows.add_column("hi", Column<std::int64_t>{6, 12});

    runtime::TableRegistry tables;
    tables.emplace("ticks", std::move(ticks));
    tables.emplace("windows", std::move(windows));

    // val=1:  [0,6)OK [8,12)X  -> 1 row
    // val=5:  [0,6)OK [8,12)X  -> 1 row
    // val=10: [0,6)X [8,12)OK  -> 1 row
    // val=15: [0,6)X [8,12)X  -> 0 rows
    auto out = interpret_expr("ticks join windows on lo <= val && val < hi;", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "val") == std::vector<std::int64_t>{1, 5, 10});
    CHECK(col_i64(out, "lo") == std::vector<std::int64_t>{0, 0, 8});
    CHECK(col_i64(out, "hi") == std::vector<std::int64_t>{6, 6, 12});
}

TEST_CASE("non-equijoin: inner join no matches yields empty table", "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{5, 6});

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{1, 2});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on a < b;", tables);
    CHECK(out.rows() == 0);
}

TEST_CASE("non-equijoin: left join preserves unmatched left rows", "[join][non-equijoin]") {
    // left rows with no match get null-padded right columns
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 10});  // 10 won't match anything

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{2, 3});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // a=1: b=2OK b=3OK -> 2 rows; a=10: no match -> 1 null-padded row
    auto out = interpret_expr("lhs left join rhs on a < b;", tables);

    REQUIRE(out.rows() == 3);
    auto a_vals = col_i64(out, "a");
    CHECK(a_vals == std::vector<std::int64_t>{1, 1, 10});

    // Row 3 (a=10) should have null in b
    const auto* b_entry = out.find_entry("b");
    REQUIRE(b_entry != nullptr);
    CHECK(b_entry->validity.has_value());
    CHECK((*b_entry->validity)[0] == true);
    CHECK((*b_entry->validity)[1] == true);
    CHECK((*b_entry->validity)[2] == false);  // null - no match for a=10
}

TEST_CASE("non-equijoin: semi join keeps left rows with at least one match",
          "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 5, 10});

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{3, 6});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // a=1: 1<3OK -> keep (once); a=5: 5<6OK -> keep; a=10: 10<3X 10<6X -> drop
    auto out = interpret_expr("lhs semi join rhs on a < b;", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "a") == std::vector<std::int64_t>{1, 5});
    // Semi join output contains only left columns
    CHECK(out.find("b") == nullptr);
}

TEST_CASE("non-equijoin: anti join keeps left rows with no match", "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 5, 10});

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{3, 6});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // a=1: 1<3OK -> drop; a=5: 5<6OK -> drop; a=10: no match -> keep
    auto out = interpret_expr("lhs anti join rhs on a < b;", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "a") == std::vector<std::int64_t>{10});
}

TEST_CASE("non-equijoin: right join preserves unmatched right rows", "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 2});

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{5, 0});  // b=0: no left row has a > 0? No: a>0 always

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // Predicate: a < b
    // a=1,b=5: 1<5OK; a=2,b=5: 2<5OK; a=1,b=0: 1<0X; a=2,b=0: 2<0X
    // Matched right rows: b=5 (matched); b=0 (unmatched -> null-padded left)
    auto out = interpret_expr("lhs right join rhs on a < b;", tables);

    REQUIRE(out.rows() == 3);  // 2 matches for b=5, 1 null-padded row for b=0
    auto b_vals = col_i64(out, "b");
    // Row order: matched rows first (left-table order), then unmatched right rows
    CHECK(b_vals[0] == 5);
    CHECK(b_vals[1] == 5);
    CHECK(b_vals[2] == 0);

    // b=0 row has null left column
    const auto* a_entry = out.find_entry("a");
    REQUIRE(a_entry != nullptr);
    CHECK(a_entry->validity.has_value());
    CHECK((*a_entry->validity)[2] == false);
}

TEST_CASE("non-equijoin: not-equal predicate", "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("x", Column<std::int64_t>{1, 2});

    runtime::Table rhs;
    rhs.add_column("y", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // x=1: y=2OK y=3OK; x=2: y=1OK y=3OK -> 4 rows
    auto out = interpret_expr("lhs join rhs on x != y;", tables);
    CHECK(out.rows() == 4);
}

// A right side above the chunked join's stream threshold (65,536 rows) makes
// the operator materialize the left side, build its hash index there, and
// probe with the right — the "swapped" path, which every other join test in
// this file is too small to reach. Row order is outside the join contract,
// and this path emits matches in right-scan (probe) order rather than
// reassembling by left row (see the class comment on
// ChunkedInnerJoinOperator), so the assertions below pin the exact emission
// order (right row ascending, then left row ascending within a key's chain)
// against an independent reference, not just the row count.
TEST_CASE("join: swapped-mode inner join emits in right-scan order", "[join]") {
    constexpr std::size_t kLeftRows = 200;
    constexpr std::size_t kRightRows = 70000;  // > kStreamRightThreshold
    constexpr std::int64_t kLeftNullRow = 7;
    constexpr std::int64_t kRightNullRow = 50;

    // Left keys repeat (two rows per key), so each probe walks a chain rather
    // than a single entry. Right keys span twice the left key range, so half of
    // the probes miss.
    std::vector<std::int64_t> left_keys;
    std::vector<std::int64_t> left_vals;
    left_keys.reserve(kLeftRows);
    left_vals.reserve(kLeftRows);
    runtime::ValidityBitmap left_validity(kLeftRows, true);
    for (std::size_t i = 0; i < kLeftRows; ++i) {
        left_keys.push_back(static_cast<std::int64_t>(i % 100));
        left_vals.push_back(static_cast<std::int64_t>(1000 + i));
    }
    left_validity.set(static_cast<std::size_t>(kLeftNullRow), false);

    std::vector<std::int64_t> right_keys;
    std::vector<std::int64_t> right_vals;
    right_keys.reserve(kRightRows);
    right_vals.reserve(kRightRows);
    runtime::ValidityBitmap right_validity(kRightRows, true);
    for (std::size_t i = 0; i < kRightRows; ++i) {
        right_keys.push_back(static_cast<std::int64_t>(i % 200));
        right_vals.push_back(static_cast<std::int64_t>(i));
    }
    right_validity.set(static_cast<std::size_t>(kRightNullRow), false);

    // Reference: for each right row in ascending (scan) order, every
    // matching left row in ascending order (the build-side hash chain
    // enumerates ascending left-row order). A null key matches nothing on
    // either side.
    std::vector<std::vector<std::size_t>> lefts_by_key(200);
    for (std::size_t l = 0; l < kLeftRows; ++l) {
        if (l == static_cast<std::size_t>(kLeftNullRow)) {
            continue;
        }
        lefts_by_key[static_cast<std::size_t>(left_keys[l])].push_back(l);
    }
    std::vector<std::int64_t> want_lval;
    std::vector<std::int64_t> want_rval;
    for (std::size_t r = 0; r < kRightRows; ++r) {
        if (r == static_cast<std::size_t>(kRightNullRow)) {
            continue;
        }
        for (const std::size_t l : lefts_by_key[static_cast<std::size_t>(right_keys[r])]) {
            want_lval.push_back(left_vals[l]);
            want_rval.push_back(right_vals[r]);
        }
    }
    // Each matched left row pulls ~350 right rows: the chains are real.
    REQUIRE(want_lval.size() > kLeftRows * 300);

    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{std::move(left_keys)}, std::move(left_validity));
    lhs.add_column("lval", Column<std::int64_t>{std::move(left_vals)});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{std::move(right_keys)}, std::move(right_validity));
    rhs.add_column("rval", Column<std::int64_t>{std::move(right_vals)});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on id;", tables);

    CHECK(out.rows() == want_lval.size());
    CHECK(col_i64(out, "lval") == want_lval);
    CHECK(col_i64(out, "rval") == want_rval);
}

// --- Output schema agreement -------------------------------------------------
//
// IR schema inference and the executors derive their output column list from
// the same planner (`ir::plan_join_output`). These tests pin that agreement on
// real data: whatever the interpreter materializes must be exactly what a
// caller planning against the inferred schema was told to expect.

namespace {

// Column names of `t`, in output order.
auto column_names(const runtime::Table& t) -> std::vector<std::string> {
    std::vector<std::string> out;
    out.reserve(t.columns.size());
    for (const auto& entry : t.columns) {
        out.push_back(entry.name);
    }
    return out;
}

// Names of the inferred output schema of `src`, in declaration order.
auto inferred_names(std::string_view src, const ir::SourceSchemas& sources)
    -> std::vector<std::string> {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    const ir::SchemaInfo schema = ir::infer_schema(*lowered.value(), sources);
    REQUIRE(schema.is_known());
    std::vector<std::string> out;
    out.reserve(schema.fields().size());
    for (const auto& field : schema.fields()) {
        out.push_back(field.name);
    }
    return out;
}

}  // namespace

TEST_CASE("join: inferred output names match the materialized ones for every kind",
          "[join][schema]") {
    // "val" collides. The clause renames both sides of it; "val_right" is a
    // left-only column and so is left alone, which is the distinction between
    // "suffix the collisions" and "suffix the right side".
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("val", Column<std::int64_t>{10, 20, 30});
    lhs.add_column("val_right", Column<std::int64_t>{11, 21, 31});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("val", Column<std::int64_t>{200, 300, 400});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    const ir::SourceSchemas sources{
        {"lhs", ir::SchemaInfo::known({{.name = "id", .type = ir::ColumnType::Int64},
                                       {.name = "val", .type = ir::ColumnType::Int64},
                                       {.name = "val_right", .type = ir::ColumnType::Int64}})},
        {"rhs", ir::SchemaInfo::known({{.name = "id", .type = ir::ColumnType::Int64},
                                       {.name = "val", .type = ir::ColumnType::Int64}})},
    };

    const std::vector<std::string> both{"id", "val_l", "val_right", "val_r"};
    // Semi and anti emit no right columns, so nothing collides and the clause
    // has nothing to apply -- the left side keeps its own spelling.
    const std::vector<std::string> left_only{"id", "val", "val_right"};

    struct Case {
        std::string_view src;
        const std::vector<std::string>* want;
    };
    const std::vector<Case> cases{
        {.src = R"(lhs join rhs on id suffix { "_l", "_r" };)", .want = &both},
        {.src = R"(lhs left join rhs on id suffix { "_l", "_r" };)", .want = &both},
        {.src = R"(lhs right join rhs on id suffix { "_l", "_r" };)", .want = &both},
        {.src = R"(lhs outer join rhs on id suffix { "_l", "_r" };)", .want = &both},
        {.src = R"(lhs semi join rhs on id suffix { "_l", "_r" };)", .want = &left_only},
        {.src = R"(lhs anti join rhs on id suffix { "_l", "_r" };)", .want = &left_only},
    };

    for (const auto& test : cases) {
        INFO(test.src);
        CHECK(column_names(interpret_expr(test.src, tables)) == *test.want);
        CHECK(inferred_names(test.src, sources) == *test.want);
    }
}

TEST_CASE("join: inferred output names match the materialized ones for mapped keys",
          "[join][schema]") {
    // Differently named keys are both retained; the colliding non-key is
    // suffixed.
    runtime::Table lhs;
    lhs.add_column("left_id", Column<std::int64_t>{1, 2});
    lhs.add_column("val", Column<std::int64_t>{10, 20});

    runtime::Table rhs;
    rhs.add_column("right_id", Column<std::int64_t>{2, 3});
    rhs.add_column("val", Column<std::int64_t>{200, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    const ir::SourceSchemas sources{
        {"lhs", ir::SchemaInfo::known({{.name = "left_id", .type = ir::ColumnType::Int64},
                                       {.name = "val", .type = ir::ColumnType::Int64}})},
        {"rhs", ir::SchemaInfo::known({{.name = "right_id", .type = ir::ColumnType::Int64},
                                       {.name = "val", .type = ir::ColumnType::Int64}})},
    };

    const std::string_view src =
        R"(lhs join rhs on { left_id = right_id } suffix { "", "_right" };)";
    const std::vector<std::string> want{"left_id", "val", "right_id", "val_right"};
    CHECK(column_names(interpret_expr(src, tables)) == want);
    CHECK(inferred_names(src, sources) == want);
}

TEST_CASE("join: a mapped key does not fold, so an equal name on both sides collides",
          "[join][schema]") {
    // `on { a = b }` keeps both key columns, unlike a same-name key. Nothing
    // here is a collision, so no clause is needed -- the inverse of the case
    // above, where the non-key `val` did collide.
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 2});
    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{2, 3});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    const ir::SourceSchemas sources{
        {"lhs", ir::SchemaInfo::known({{.name = "a", .type = ir::ColumnType::Int64}})},
        {"rhs", ir::SchemaInfo::known({{.name = "b", .type = ir::ColumnType::Int64}})},
    };

    const std::string_view src = "lhs join rhs on { a = b };";
    const std::vector<std::string> want{"a", "b"};
    CHECK(column_names(interpret_expr(src, tables)) == want);
    CHECK(inferred_names(src, sources) == want);
}

TEST_CASE("join: inferred output names match the materialized ones for a cross join",
          "[join][schema]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});
    lhs.add_column("val", Column<std::int64_t>{10, 20});

    runtime::Table rhs;
    rhs.add_column("val", Column<std::int64_t>{200, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    const ir::SourceSchemas sources{
        {"lhs", ir::SchemaInfo::known({{.name = "id", .type = ir::ColumnType::Int64},
                                       {.name = "val", .type = ir::ColumnType::Int64}})},
        {"rhs", ir::SchemaInfo::known({{.name = "val", .type = ir::ColumnType::Int64}})},
    };

    // A cross join has no keys, so nothing folds and `val` simply collides.
    const std::string_view src = R"(lhs cross join rhs suffix { "", "_right" };)";
    const std::vector<std::string> want{"id", "val", "val_right"};
    CHECK(column_names(interpret_expr(src, tables)) == want);
    CHECK(inferred_names(src, sources) == want);
}

// The chunked join's swapped mode (right side above the 65,536-row stream
// threshold) assembles its output from the materialized left side rather than
// a probe chunk, so it names right columns through its own call into the
// planner. This pins that route on the same repeated-collision case as the
// stream-mode test above.
TEST_CASE("join: swapped-mode inner join names colliding right columns like the planner",
          "[join][schema]") {
    constexpr std::size_t kLeftRows = 8;
    constexpr std::size_t kRightRows = 70000;  // > kStreamRightThreshold

    std::vector<std::int64_t> left_keys;
    std::vector<std::int64_t> left_val;
    std::vector<std::int64_t> left_val_right;
    for (std::size_t i = 0; i < kLeftRows; ++i) {
        left_keys.push_back(static_cast<std::int64_t>(i));
        left_val.push_back(static_cast<std::int64_t>(10 + i));
        left_val_right.push_back(static_cast<std::int64_t>(100 + i));
    }

    std::vector<std::int64_t> right_keys;
    std::vector<std::int64_t> right_val;
    for (std::size_t i = 0; i < kRightRows; ++i) {
        right_keys.push_back(static_cast<std::int64_t>(i));
        right_val.push_back(static_cast<std::int64_t>(i));
    }

    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{std::move(left_keys)});
    lhs.add_column("val", Column<std::int64_t>{std::move(left_val)});
    lhs.add_column("val_right", Column<std::int64_t>{std::move(left_val_right)});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{std::move(right_keys)});
    rhs.add_column("val", Column<std::int64_t>{std::move(right_val)});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // Both suffixes are non-empty, so this also pins the *left* rename on this
    // route. The swapped path emits left columns from the materialized left
    // side rather than the plan, and took only right names from the planner
    // until a suffix clause made left renaming possible at all.
    auto out = interpret_expr(R"(lhs join rhs on id suffix { "_l", "_r" };)", tables);

    CHECK(out.rows() == kLeftRows);
    CHECK(column_names(out) == std::vector<std::string>{"id", "val_l", "val_right", "val_r"});
    CHECK(col_i64(out, "val_l") == std::vector<std::int64_t>{10, 11, 12, 13, 14, 15, 16, 17});
    CHECK(col_i64(out, "val_r") == std::vector<std::int64_t>{0, 1, 2, 3, 4, 5, 6, 7});
}

// --- Side-qualified predicate references -------------------------------------

TEST_CASE("non-equijoin: left(col) and right(col) name the same column on both sides",
          "[join][non-equijoin]") {
    // Every column name exists on both sides, so an unqualified predicate has
    // nothing to resolve against — the qualifiers are the only way to say it.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("v", Column<std::int64_t>{10, 20, 30});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    rhs.add_column("v", Column<std::int64_t>{15, 5, 35});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // A theta join has no keys, so nothing folds: both `id` and `v` collide
    // and the clause is what makes the output nameable. Predicate resolution
    // is independent of it — the qualifiers read the inputs, not the output.
    // left.v < right.v -> (10,15) (10,35) (20,35) (30,35)
    auto out =
        interpret_expr(R"(lhs join rhs on left(v) < right(v) suffix { "", "_right" };)", tables);
    CHECK(out.rows() == 4);
    CHECK(col_i64(out, "v") == std::vector<std::int64_t>{10, 10, 20, 30});
    CHECK(col_i64(out, "v_right") == std::vector<std::int64_t>{15, 35, 35, 35});

    // The mirror image, to prove the sides are not simply swapped somewhere.
    // 10 > {5}; 20 > {15, 5}; 30 > {15, 5} — right rows in their own order.
    auto flipped =
        interpret_expr(R"(lhs join rhs on left(v) > right(v) suffix { "", "_right" };)", tables);
    CHECK(flipped.rows() == 5);
    CHECK(col_i64(flipped, "v") == std::vector<std::int64_t>{10, 20, 20, 30, 30});
    CHECK(col_i64(flipped, "v_right") == std::vector<std::int64_t>{5, 15, 5, 15, 5});
}

TEST_CASE("non-equijoin: an unqualified name resolves against whichever side has it",
          "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("ts", Column<std::int64_t>{5, 15, 25});

    runtime::Table rhs;
    rhs.add_column("lo", Column<std::int64_t>{0, 20});
    rhs.add_column("hi", Column<std::int64_t>{10, 30});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on ts >= lo && ts < hi;", tables);
    CHECK(out.rows() == 2);
    CHECK(col_i64(out, "ts") == std::vector<std::int64_t>{5, 25});
    CHECK(col_i64(out, "lo") == std::vector<std::int64_t>{0, 20});

    // Mixing the two forms is fine.
    auto mixed = interpret_expr("lhs join rhs on left(ts) >= lo && ts < right(hi);", tables);
    CHECK(mixed.rows() == 2);
    CHECK(col_i64(mixed, "ts") == std::vector<std::int64_t>{5, 25});
}

TEST_CASE("non-equijoin: an ambiguous unqualified name is rejected", "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("v", Column<std::int64_t>{1, 2});

    runtime::Table rhs;
    rhs.add_column("v", Column<std::int64_t>{1, 2});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    const std::string err = interpret_error("lhs join rhs on v < v;", tables);
    CHECK(err.find("exists in both inputs") != std::string::npos);
    CHECK(err.find("left(v)") != std::string::npos);
    CHECK(err.find("right(v)") != std::string::npos);
}

TEST_CASE("non-equijoin: a qualifier naming a column the side lacks is rejected",
          "[join][non-equijoin]") {
    runtime::Table lhs;
    lhs.add_column("a", Column<std::int64_t>{1, 2});

    runtime::Table rhs;
    rhs.add_column("b", Column<std::int64_t>{1, 2});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    CHECK(interpret_error("lhs join rhs on left(b) < right(b);", tables)
              .find("the left input has no column") != std::string::npos);
    CHECK(interpret_error("lhs join rhs on left(a) < right(a);", tables)
              .find("the right input has no column") != std::string::npos);
}

// ── Row order carried over from the left input ────────────────────────────
//
// A join still promises no row order. What these pin is that when a path
// happens to emit the left rows in their own order, the result SAYS so, which
// is what lets a following `order` skip the sort.

namespace {

// An ordering claim as (name, ascending) pairs, for comparison in tests.
auto ordering_of(const runtime::Table& t) -> std::vector<std::pair<std::string, bool>> {
    std::vector<std::pair<std::string, bool>> out;
    if (t.ordering().has_value()) {
        for (const auto& key : *t.ordering()) {
            out.emplace_back(key.name, key.ascending);
        }
    }
    return out;
}

// A left side deliberately larger than the right, so the executor indexes the
// right and scans the left — the path that emits in left-row order.
auto big_left_registry() -> runtime::TableRegistry {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{5, 4, 3, 2, 1});
    lhs.add_column("lval", Column<std::int64_t>{50, 40, 30, 20, 10});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3});
    rhs.add_column("rval", Column<std::int64_t>{200, 300});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));
    return tables;
}

}  // namespace

TEST_CASE("join: a left-scan path carries the left's ordering into the output", "[join][order]") {
    auto tables = big_left_registry();
    auto out = interpret_expr("lhs[order { id asc }] join rhs on id;", tables);

    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(ordering_of(out) == std::vector<std::pair<std::string, bool>>{{"id", true}});
}

TEST_CASE("join: an unordered left carries nothing", "[join][order]") {
    auto tables = big_left_registry();
    auto out = interpret_expr("lhs join rhs on id;", tables);
    CHECK(ordering_of(out).empty());
}

TEST_CASE("join: duplicate matches stay adjacent, so the claim still holds", "[join][order]") {
    // Two right rows per left key: the left row is emitted twice, together.
    // A non-strict ordering tolerates that -- (1, 1, 3) is still ascending.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{3, 1, 2, 4, 5});
    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 1, 3, 3});
    rhs.add_column("rval", Column<std::int64_t>{10, 11, 30, 31});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs[order { id asc }] join rhs on id;", tables);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 1, 3, 3});
    CHECK(ordering_of(out) == std::vector<std::pair<std::string, bool>>{{"id", true}});
}

TEST_CASE("join: a right join's unmatched rows void the claim", "[join][order]") {
    // Unmatched right rows are appended with null left columns, so the left's
    // ordering says nothing about where they belong. `id = 9` is the one that
    // matters: without it every right row matches and the order does survive.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{5, 4, 3, 2, 1});
    lhs.add_column("lval", Column<std::int64_t>{50, 40, 30, 20, 10});
    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 9});
    rhs.add_column("rval", Column<std::int64_t>{200, 900});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs[order { id asc }] right join rhs on id;", tables);
    REQUIRE(out.rows() == 2);
    CHECK(ordering_of(out).empty());
}

TEST_CASE("join: a right join whose right rows all match keeps the claim", "[join][order]") {
    // The companion to the case above: nothing is appended, so every emitted
    // row is a left row in left order and the claim stands. Which of the two
    // happens is a property of the data, which is why the claim is proved from
    // the emitted rows rather than from the join kind.
    auto tables = big_left_registry();
    auto out = interpret_expr("lhs[order { id asc }] right join rhs on id;", tables);
    CHECK(ordering_of(out) == std::vector<std::pair<std::string, bool>>{{"id", true}});
}

TEST_CASE("join: a semi join keeps the left ordering", "[join][order]") {
    // Left columns only, emitted in left-row order: the claim survives whole.
    auto tables = big_left_registry();
    auto out = interpret_expr("lhs[order { id asc }] semi join rhs on id;", tables);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(ordering_of(out) == std::vector<std::pair<std::string, bool>>{{"id", true}});
}

TEST_CASE("join: a suffixed key column is claimed under its output name", "[join][order]") {
    // `lval` collides and takes the left suffix, so the claim has to be
    // restated in the output's names or it names a column that is not there.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{3, 1, 2, 4, 5});
    lhs.add_column("lval", Column<std::int64_t>{30, 10, 20, 40, 50});
    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2});
    rhs.add_column("lval", Column<std::int64_t>{100, 200});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out =
        interpret_expr(R"(lhs[order { lval asc }] join rhs on id suffix { "_l", "_r" };)", tables);
    CHECK(ordering_of(out) == std::vector<std::pair<std::string, bool>>{{"lval_l", true}});
}

TEST_CASE("join: an ordering key the output drops takes the claim with it", "[join][order]") {
    // An anti join emits left columns only, but the left was ordered by a
    // column projected away before the join, so nothing can be claimed.
    auto tables = big_left_registry();
    auto out =
        interpret_expr("lhs[order { lval asc }][select { id }] anti join rhs on id;", tables);
    CHECK(ordering_of(out).empty());
}

TEST_CASE("join: a following order over a carried claim returns the same rows", "[join][order]") {
    // The elision is invisible by construction -- what it must not do is change
    // the answer. Compare it against the same query over an unordered left,
    // which sorts for real.
    auto tables = big_left_registry();
    auto elided =
        interpret_expr("(lhs[order { id asc }] join rhs on id)[order { id asc }];", tables);
    auto sorted = interpret_expr("(lhs join rhs on id)[order { id asc }];", tables);

    CHECK(col_i64(elided, "id") == col_i64(sorted, "id"));
    CHECK(col_i64(elided, "lval") == col_i64(sorted, "lval"));
    CHECK(col_i64(elided, "rval") == col_i64(sorted, "rval"));
}

// ── Build-side selection with a pending `order` ───────────────────────────
//
// A join indexes the smaller side. When an `order` above it wants the order
// the left already carries, indexing the right instead delivers that order and
// the sort disappears. These pin the choice through its one observable
// consequence: whether the result claims the ordering.

TEST_CASE("join: a pending order flips the build side when the ratio allows", "[join][order]") {
    // Left is the smaller side, so the default is to index it and scan the
    // right -- which loses left-row order. The pending `order` asks for exactly
    // what the ordered left carries, and 3 rows against 9 is inside the ratio,
    // so the join indexes the right instead and the order survives.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{3, 1, 2});
    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
    rhs.add_column("rval", Column<std::int64_t>{10, 20, 30, 40, 50, 60, 70, 80, 90});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("(lhs[order { id asc }] join rhs on id)[order { id asc }];", tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    CHECK(ordering_of(out) == std::vector<std::pair<std::string, bool>>{{"id", true}});
}

TEST_CASE("join: a pending order for something else does not flip the build side",
          "[join][order]") {
    // The left is ordered by `id` but the query wants `rval`, which the left
    // does not have at all. Nothing to preserve, so the default choice stands
    // and the `order` sorts for real -- which it must still get right.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{3, 1, 2});
    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
    rhs.add_column("rval", Column<std::int64_t>{90, 80, 70, 60, 50, 40, 30, 20, 10});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out =
        interpret_expr("(lhs[order { id asc }] join rhs on id)[order { rval asc }];", tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "rval") == std::vector<std::int64_t>{70, 80, 90});
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{3, 2, 1});
}

TEST_CASE("join: the answer is the same whichever side is indexed", "[join][order]") {
    // The whole point is that this is a cost decision. Run the same query with
    // and without the pending order in the plan and compare the rows, since
    // the two take different paths through the executor.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{3, 1, 2});
    lhs.add_column("lval", Column<std::int64_t>{30, 10, 20});
    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
    rhs.add_column("rval", Column<std::int64_t>{10, 20, 30, 40, 50, 60, 70, 80, 90});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto hinted =
        interpret_expr("(lhs[order { id asc }] join rhs on id)[order { id asc }];", tables);
    auto plain = interpret_expr("(lhs join rhs on id)[order { id asc }];", tables);

    CHECK(col_i64(hinted, "id") == col_i64(plain, "id"));
    CHECK(col_i64(hinted, "lval") == col_i64(plain, "lval"));
    CHECK(col_i64(hinted, "rval") == col_i64(plain, "rval"));
}

// ── Source nulls survive the join ─────────────────────────────────────────

TEST_CASE("join: a null in a left column stays null through the join", "[join]") {
    // The materialized join gathered its columns without their validity, so a
    // null came out as the type's zero. A zero was chosen for `v` deliberately
    // elsewhere in these tests; here the point is that `null` and `0` must not
    // become the same value just because the rows were rearranged.
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("g", Column<std::int64_t>{1, 1, 1});
    runtime::ValidityBitmap lv(3, true);
    lv.set(1, false);
    lhs.add_column("v", Column<std::int64_t>{7, 0, 9}, lv);

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    rhs.add_column("g", Column<std::int64_t>{1, 1, 1});
    rhs.add_column("w", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // Two keys, so this is not one of the streamable single-key shapes and
    // takes the materialized join.
    auto out = interpret_expr("lhs join rhs on { id, g };", tables);
    REQUIRE(out.rows() == 3);
    const auto* entry = out.find_entry("v");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0]);
    CHECK_FALSE((*entry->validity)[1]);
    CHECK((*entry->validity)[2]);
}

TEST_CASE("join: a null in a right column stays null through the join", "[join]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("g", Column<std::int64_t>{1, 1, 1});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    rhs.add_column("g", Column<std::int64_t>{1, 1, 1});
    runtime::ValidityBitmap rv(3, true);
    rv.set(2, false);
    rhs.add_column("w", Column<std::int64_t>{10, 20, 0}, rv);

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("lhs join rhs on { id, g };", tables);
    REQUIRE(out.rows() == 3);
    const auto* entry = out.find_entry("w");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0]);
    CHECK((*entry->validity)[1]);
    CHECK_FALSE((*entry->validity)[2]);
}

TEST_CASE("join: a null in a folded key stays null through the join", "[join]") {
    // The same defect as the two above, in the one column they cannot reach:
    // a folded key is built per-row from whichever side is present, and that
    // loop copied the value without the validity that went with it. A key can
    // be null in an emitted row -- an outer join keeps an unmatched row and
    // its own null key -- so the null came back as a genuine 0.
    runtime::Table lhs;
    runtime::ValidityBitmap lv(2, true);
    lv.set(1, false);
    lhs.add_column("id", Column<std::int64_t>{1, 0}, lv);
    lhs.add_column("lval", Column<std::int64_t>{10, 20});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{9});
    rhs.add_column("rval", Column<std::int64_t>{90});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    // Nothing matches, so all three rows are unmatched and each key comes from
    // its own side -- including the left row whose key is null.
    auto out = interpret_expr("lhs outer join rhs on { id };", tables);
    REQUIRE(out.rows() == 3);
    const auto* entry = out.find_entry("id");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());

    // Find the row the null key belongs to by its payload: the join promises
    // no order, and a null read as 0 would sort among the values.
    const auto lvals = col_i64(out, "lval");
    std::size_t null_row = out.rows();
    for (std::size_t i = 0; i < lvals.size(); ++i) {
        if (lvals[i] == 20) {
            null_row = i;
        }
    }
    REQUIRE(null_row < out.rows());
    CHECK_FALSE((*entry->validity)[null_row]);

    // And the two rows with real keys keep them.
    for (std::size_t i = 0; i < out.rows(); ++i) {
        if (i != null_row) {
            CHECK((*entry->validity)[i]);
        }
    }
}

// ── `nulls equal` ─────────────────────────────────────────────────────────
//
// Every case pits a null against a GENUINE 0 in the same key column. That is
// the point: a null cell holds its type's zero, so a test whose keys avoid 0
// passes even when the two are conflated.

namespace {

// Keys 0, null, 5, null on the left; 0, null, 7 on the right.
auto null_key_registry() -> runtime::TableRegistry {
    runtime::Table lhs;
    runtime::ValidityBitmap lv(4, true);
    lv.set(1, false);
    lv.set(3, false);
    lhs.add_column("k", Column<std::int64_t>{0, 0, 5, 0}, lv);
    lhs.add_column("lv", Column<std::int64_t>{100, 101, 102, 103});

    runtime::Table rhs;
    runtime::ValidityBitmap rv(3, true);
    rv.set(1, false);
    rhs.add_column("k", Column<std::int64_t>{0, 0, 7}, rv);
    rhs.add_column("rv", Column<std::int64_t>{200, 201, 202});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));
    return tables;
}

}  // namespace

TEST_CASE("join: by default a null key matches nothing", "[join][nulls]") {
    auto tables = null_key_registry();
    auto out = interpret_expr("lhs join rhs on k;", tables);
    // Only the genuine zeros pair up.
    CHECK(col_i64(out, "lv") == std::vector<std::int64_t>{100});
    CHECK(col_i64(out, "rv") == std::vector<std::int64_t>{200});
}

TEST_CASE("join: `nulls equal` pairs nulls with nulls only", "[join][nulls]") {
    auto tables = null_key_registry();
    auto out = interpret_expr("(lhs join rhs on k nulls equal)[order { lv asc }];", tables);
    // 100 is the genuine 0 and pairs only with 200, the other genuine 0.
    // 101 and 103 are the null keys and pair only with 201, the null on the
    // right. 102 (k = 5) and 202 (k = 7) match nothing either way.
    CHECK(col_i64(out, "lv") == std::vector<std::int64_t>{100, 101, 103});
    CHECK(col_i64(out, "rv") == std::vector<std::int64_t>{200, 201, 201});
}

TEST_CASE("join: `nulls equal` keeps the key column null in the output", "[join][nulls]") {
    auto tables = null_key_registry();
    auto out = interpret_expr("(lhs join rhs on k nulls equal)[order { lv asc }];", tables);
    const auto* entry = out.find_entry("k");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0]);        // the genuine 0
    CHECK_FALSE((*entry->validity)[1]);  // matched because both were null
    CHECK_FALSE((*entry->validity)[2]);
}

TEST_CASE("join: a left join under `nulls equal` matches instead of filling", "[join][nulls]") {
    // Without the policy a null-keyed left row survives with null right
    // columns; with it, the row finds its partner and carries real values.
    auto tables = null_key_registry();
    auto plain = interpret_expr("(lhs left join rhs on k)[order { lv asc }];", tables);
    CHECK(plain.rows() == 4);
    const auto* plain_rv = plain.find_entry("rv");
    REQUIRE(plain_rv != nullptr);
    REQUIRE(plain_rv->validity.has_value());
    CHECK_FALSE((*plain_rv->validity)[1]);  // null key, unmatched

    auto equal = interpret_expr("(lhs left join rhs on k nulls equal)[order { lv asc }];", tables);
    CHECK(equal.rows() == 4);
    const auto* equal_rv = equal.find_entry("rv");
    REQUIRE(equal_rv != nullptr);
    CHECK(col_i64(equal, "rv")[1] == 201);
    if (equal_rv->validity.has_value()) {
        CHECK((*equal_rv->validity)[1]);
    }
}

TEST_CASE("join: semi and anti follow the policy", "[join][nulls]") {
    auto tables = null_key_registry();
    // Semi: without the policy only the genuine 0 survives; with it the two
    // null-keyed rows do too.
    CHECK(col_i64(interpret_expr("lhs semi join rhs on k;", tables), "lv") ==
          std::vector<std::int64_t>{100});
    CHECK(col_i64(interpret_expr("(lhs semi join rhs on k nulls equal)[order { lv asc }];", tables),
                  "lv") == std::vector<std::int64_t>{100, 101, 103});
    // Anti is the complement of the same question.
    CHECK(col_i64(interpret_expr("(lhs anti join rhs on k)[order { lv asc }];", tables), "lv") ==
          std::vector<std::int64_t>{101, 102, 103});
    CHECK(col_i64(interpret_expr("(lhs anti join rhs on k nulls equal)[order { lv asc }];", tables),
                  "lv") == std::vector<std::int64_t>{102});
}

TEST_CASE("join: a composite key needs every component to match", "[join][nulls]") {
    // (1, null) matches (1, null) but not (1, 0): the null component pairs only
    // with a null in that same component.
    runtime::Table lhs;
    runtime::ValidityBitmap lv(3, true);
    lv.set(0, false);
    lhs.add_column("a", Column<std::int64_t>{1, 1, 2});
    lhs.add_column("b", Column<std::int64_t>{0, 0, 0}, lv);
    lhs.add_column("lv", Column<std::int64_t>{10, 11, 12});

    runtime::Table rhs;
    runtime::ValidityBitmap rv(2, true);
    rv.set(0, false);
    rhs.add_column("a", Column<std::int64_t>{1, 1});
    rhs.add_column("b", Column<std::int64_t>{0, 0}, rv);
    rhs.add_column("rv", Column<std::int64_t>{20, 21});

    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("(lhs join rhs on { a, b } nulls equal)[order { lv asc }];", tables);
    // lv 10 is (1, null) -> rv 20, the (1, null) on the right.
    // lv 11 is (1, 0)    -> rv 21, the (1, 0). The two must not cross.
    CHECK(col_i64(out, "lv") == std::vector<std::int64_t>{10, 11});
    CHECK(col_i64(out, "rv") == std::vector<std::int64_t>{20, 21});
}

TEST_CASE("join: `nulls` is rejected where there are no keys to apply it to", "[join][nulls]") {
    auto tables = null_key_registry();
    auto cross = interpret_error_at_parse("lhs cross join rhs nulls equal;");
    CHECK(cross.find("cross join") != std::string::npos);
    // A pure predicate join has no equijoin keys either, and the clause does
    // not redefine how the predicate compares nulls.
    auto theta = interpret_error_at_parse("lhs join rhs on left(k) < right(k) nulls equal;");
    CHECK(theta.find("equijoin keys") != std::string::npos);
}

TEST_CASE("join: `nulls` takes only equal or never", "[join][nulls]") {
    auto err = interpret_error_at_parse("lhs join rhs on k nulls sometimes;");
    CHECK(err.find("'equal' or 'never'") != std::string::npos);
}

TEST_CASE("join: `nulls` is not a reserved word", "[join][nulls]") {
    // `read_csv(path, nulls: String = "", ...)` names a parameter `nulls`, so
    // reserving it would have broken the shipped csv reader. It is matched in
    // the join trailer position only.
    runtime::Table t;
    t.add_column("nulls", Column<std::int64_t>{1, 2});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    auto out = interpret_expr("t[filter nulls > 1];", tables);
    CHECK(col_i64(out, "nulls") == std::vector<std::int64_t>{2});
}

// ── `expect L:R` ──────────────────────────────────────────────────────────
//
// L is how many LEFT rows may match one right row, R how many RIGHT rows may
// match one left row. It is checked against the pairs actually emitted, so one
// rule covers every join kind and every path through the executor.

namespace {

// Two orders share customer 10; customer 20 has one. So n:1 holds and 1:1 does
// not, and the violation is on the right side of the join.
auto orders_registry() -> runtime::TableRegistry {
    runtime::Table orders;
    orders.add_column("id", Column<std::int64_t>{1, 2, 3});
    orders.add_column("cust", Column<std::int64_t>{10, 10, 20});

    runtime::Table customers;
    customers.add_column("cust", Column<std::int64_t>{10, 20});
    customers.add_column("tier", Column<std::int64_t>{7, 8});

    runtime::TableRegistry tables;
    tables.emplace("orders", std::move(orders));
    tables.emplace("customers", std::move(customers));
    return tables;
}

}  // namespace

TEST_CASE("join: expect n:1 holds when each left row matches once", "[join][expect]") {
    auto tables = orders_registry();
    auto out =
        interpret_expr("(orders join customers on cust expect n:1)[order { id asc }];", tables);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    CHECK(col_i64(out, "tier") == std::vector<std::int64_t>{7, 7, 8});
}

TEST_CASE("join: expect 1:1 fails when a right row matches twice", "[join][expect]") {
    auto tables = orders_registry();
    auto err = interpret_error("orders join customers on cust expect 1:1;", tables);
    CHECK(err.find("expect 1:1") != std::string::npos);
    CHECK(err.find("right row 0") != std::string::npos);
}

TEST_CASE("join: expect 1:n fails on the same data, naming the other side", "[join][expect]") {
    // 1:n says each right row matches at most one left row — the half of 1:1
    // that this data breaks.
    auto tables = orders_registry();
    auto err = interpret_error("orders join customers on cust expect 1:n;", tables);
    CHECK(err.find("right row 0") != std::string::npos);
    CHECK(err.find("more than one row on the left") != std::string::npos);
}

TEST_CASE("join: expect n:1 fails when a left row matches twice", "[join][expect]") {
    // Reverse the shape: now the LEFT row is the one with two partners.
    runtime::Table lhs;
    lhs.add_column("k", Column<std::int64_t>{1});
    runtime::Table rhs;
    rhs.add_column("k", Column<std::int64_t>{1, 1});
    rhs.add_column("v", Column<std::int64_t>{5, 6});
    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto err = interpret_error("lhs join rhs on k expect n:1;", tables);
    CHECK(err.find("left row 0") != std::string::npos);
    CHECK(err.find("more than one row on the right") != std::string::npos);
}

TEST_CASE("join: expect n:n asserts nothing", "[join][expect]") {
    auto tables = orders_registry();
    auto out = interpret_expr("orders join customers on cust expect n:n;", tables);
    CHECK(out.rows() == 3);
}

TEST_CASE("join: an unmatched row is not a repeat", "[join][expect]") {
    // A left join pads unmatched left rows. Those match nothing, so they cannot
    // break a declaration about matching more than once.
    runtime::Table lhs;
    lhs.add_column("k", Column<std::int64_t>{1, 2, 3});
    runtime::Table rhs;
    rhs.add_column("k", Column<std::int64_t>{2});
    rhs.add_column("v", Column<std::int64_t>{20});
    runtime::TableRegistry tables;
    tables.emplace("lhs", std::move(lhs));
    tables.emplace("rhs", std::move(rhs));

    auto out = interpret_expr("(lhs left join rhs on k expect n:1)[order { k asc }];", tables);
    CHECK(out.rows() == 3);
}

TEST_CASE("join: expect is rejected where there are no keys", "[join][expect]") {
    auto err = interpret_error_at_parse("lhs cross join rhs expect n:1;");
    CHECK(err.find("has none") != std::string::npos);
}

TEST_CASE("join: expect takes only 1 or n", "[join][expect]") {
    auto err = interpret_error_at_parse("lhs join rhs on k expect 2:1;");
    CHECK(err.find("`1` or `n`") != std::string::npos);
}

// ── `take first` / `last` / `any` ─────────────────────────────────────────

namespace {

// Two quotes for "a", one for "b". The quotes are deliberately NOT in ts order
// as written, so a rule that silently used input order would be visible.
auto quotes_registry() -> runtime::TableRegistry {
    runtime::Table trades;
    trades.add_column("sym", Column<std::string>{"a", "b"});

    runtime::Table quotes;
    quotes.add_column("sym", Column<std::string>{"a", "a", "b"});
    quotes.add_column("px", Column<std::int64_t>{10, 11, 20});
    quotes.add_column("ts", Column<std::int64_t>{3, 1, 5});

    runtime::TableRegistry tables;
    tables.emplace("trades", std::move(trades));
    tables.emplace("quotes", std::move(quotes));
    return tables;
}

}  // namespace

TEST_CASE("join: take first reads the right value's stated order", "[join][take]") {
    auto tables = quotes_registry();
    // Ordered by ts, the earliest quote for "a" is px 11 (ts 1) — not the one
    // that comes first in the quotes table as written.
    auto out = interpret_expr(
        "(trades join quotes[order { ts asc }] on sym take first)[order { sym asc }];", tables);
    CHECK(col_i64(out, "px") == std::vector<std::int64_t>{11, 20});
}

TEST_CASE("join: take last reads the same order from the far end", "[join][take]") {
    auto tables = quotes_registry();
    auto out = interpret_expr(
        "(trades join quotes[order { ts asc }] on sym take last)[order { sym asc }];", tables);
    CHECK(col_i64(out, "px") == std::vector<std::int64_t>{10, 20});
}

TEST_CASE("join: reversing the order swaps first and last", "[join][take]") {
    // `last` is a convenience, not a separate concept: the order decides.
    auto tables = quotes_registry();
    auto desc = interpret_expr(
        "(trades join quotes[order { ts desc }] on sym take first)[order { sym asc }];", tables);
    CHECK(col_i64(desc, "px") == std::vector<std::int64_t>{10, 20});
}

TEST_CASE("join: the order can be stated once on a binding", "[join][take]") {
    // The claim travels with the value, so a filter in between keeps it and
    // nothing has to be repeated at the join.
    auto tables = quotes_registry();
    auto out = interpret_expr(R"(
let ordered = quotes[order { ts asc }];
let live = ordered[filter px > 0];
(trades join live on sym take first)[order { sym asc }];
)",
                              tables);
    CHECK(col_i64(out, "px") == std::vector<std::int64_t>{11, 20});
}

TEST_CASE("join: take first refuses a right input with no stated order", "[join][take]") {
    auto tables = quotes_registry();
    auto err = interpret_error("trades join quotes on sym take first;", tables);
    CHECK(err.find("state an order") != std::string::npos);
    CHECK(err.find("take any") != std::string::npos);
}

TEST_CASE("join: take any needs no order and keeps one match", "[join][take]") {
    // Which one is unspecified, so this asserts only the row count and that the
    // surviving row is one of the two candidates.
    auto tables = quotes_registry();
    auto out = interpret_expr("(trades join quotes on sym take any)[order { sym asc }];", tables);
    REQUIRE(out.rows() == 2);
    const auto px = col_i64(out, "px");
    CHECK((px[0] == 10 || px[0] == 11));
    CHECK(px[1] == 20);
}

TEST_CASE("join: take any gives the same answer twice", "[join][take]") {
    // Unspecified but stable: the contract does not say which row, but the same
    // input rows must give the same output row, or no test over such a query
    // could exist.
    auto tables = quotes_registry();
    auto first = interpret_expr("(trades join quotes on sym take any)[order { sym asc }];", tables);
    auto again = interpret_expr("(trades join quotes on sym take any)[order { sym asc }];", tables);
    CHECK(col_i64(first, "px") == col_i64(again, "px"));
}

TEST_CASE("join: take leaves an unmatched row alone", "[join][take]") {
    // A left join's padding has no match to choose between, so it survives.
    auto tables = quotes_registry();
    auto out = interpret_expr(R"(
let extra = Table { sym = ["a", "b", "c"] };
(extra left join quotes[order { ts asc }] on sym take first)[order { sym asc }];
)",
                              tables);
    CHECK(out.rows() == 3);
}

TEST_CASE("join: expect still describes the matching, not what take kept", "[join][take]") {
    // `take first` would satisfy every `expect n:1` by construction if the
    // declaration were checked after the drop, so it is checked before.
    auto tables = quotes_registry();
    auto err = interpret_error("trades join quotes[order { ts asc }] on sym expect n:1 take first;",
                               tables);
    CHECK(err.find("expect n:1") != std::string::npos);
}

TEST_CASE("join: take is rejected where there is nothing to choose", "[join][take]") {
    CHECK(interpret_error_at_parse("lhs cross join rhs take first;").find("no keys") !=
          std::string::npos);
    CHECK(interpret_error_at_parse("lhs semi join rhs on k take first;").find("at most once") !=
          std::string::npos);
}

TEST_CASE("join: take takes only first, last or any", "[join][take]") {
    auto err = interpret_error_at_parse("lhs join rhs on k take some;");
    CHECK(err.find("'first', 'last' or 'any'") != std::string::npos);
}
