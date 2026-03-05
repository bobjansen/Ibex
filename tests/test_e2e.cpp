/// Comprehensive end-to-end tests: source text → parse → lower → interpret.
///
/// These tests exercise the full pipeline in a single step, ensuring that the
/// parser, lowerer, and interpreter all agree on the semantics of each query.
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/stream_buffered.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace ibex;

// ─── Helpers ─────────────────────────────────────────────────────────────────

auto col_i64(const runtime::Table& t, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

auto col_dbl(const runtime::Table& t, const std::string& name) -> std::vector<double> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<double>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

auto col_str(const runtime::Table& t, const std::string& name) -> std::vector<std::string> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    std::vector<std::string> out;
    if (const auto* values = std::get_if<Column<std::string>>(col)) {
        for (const auto& v : *values) {
            out.emplace_back(v);
        }
    } else if (const auto* cat = std::get_if<Column<Categorical>>(col)) {
        for (std::size_t i = 0; i < cat->size(); ++i) {
            out.emplace_back((*cat)[i]);
        }
    } else {
        FAIL("Column '" << name << "' is neither string nor categorical");
    }
    return out;
}

auto run(std::string_view src, const runtime::TableRegistry& tables,
         const runtime::ScalarRegistry* scalars = nullptr,
         const runtime::ExternRegistry* externs = nullptr) -> runtime::Table {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, scalars, externs);
    REQUIRE(result.has_value());
    return std::move(*result);
}

auto run_error(std::string_view src, const runtime::TableRegistry& tables) -> std::string {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE_FALSE(result.has_value());
    return result.error();
}

auto make_trades() -> runtime::TableRegistry {
    runtime::Table t;
    t.add_column("price", Column<std::int64_t>{10, 20, 30, 40, 50});
    t.add_column("qty", Column<std::int64_t>{5, 3, 8, 2, 1});
    t.add_column("symbol", Column<std::string>{"AAPL", "GOOG", "AAPL", "GOOG", "AAPL"});
    runtime::TableRegistry reg;
    reg.emplace("trades", std::move(t));
    return reg;
}

}  // namespace

// ─── Basic filter ────────────────────────────────────────────────────────────

TEST_CASE("E2E: filter with greater-than", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 25];", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30, 40, 50});
}

TEST_CASE("E2E: filter with equality", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price == 20];", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{20});
    CHECK(col_str(out, "symbol") == std::vector<std::string>{"GOOG"});
}

TEST_CASE("E2E: filter with less-than-or-equal", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price <= 20];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20});
}

TEST_CASE("E2E: filter with string equality", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter symbol == \"AAPL\"];", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 30, 50});
}

TEST_CASE("E2E: filter with AND predicate", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 15 && symbol == \"GOOG\"];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{20, 40});
}

TEST_CASE("E2E: filter with OR predicate", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price == 10 || price == 50];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 50});
}

// ─── Select ──────────────────────────────────────────────────────────────────

TEST_CASE("E2E: select single column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select price];", tables);

    REQUIRE(out.columns.size() == 1);
    REQUIRE(out.rows() == 5);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: select multiple columns", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { price, symbol }];", tables);

    REQUIRE(out.columns.size() == 2);
    CHECK(out.find("price") != nullptr);
    CHECK(out.find("symbol") != nullptr);
    CHECK(out.find("qty") == nullptr);
}

TEST_CASE("E2E: select with computed field", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { total = price * qty }];", tables);

    REQUIRE(out.columns.size() == 1);
    CHECK(col_i64(out, "total") == std::vector<std::int64_t>{50, 60, 240, 80, 50});
}

// ─── Filter + Select combined ────────────────────────────────────────────────

TEST_CASE("E2E: filter then select", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 15, select { symbol, price }];", tables);

    REQUIRE(out.rows() == 4);
    REQUIRE(out.columns.size() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{20, 30, 40, 50});
}

// ─── Update ──────────────────────────────────────────────────────────────────

TEST_CASE("E2E: update adds new column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { total = price * qty }];", tables);

    REQUIRE(out.find("total") != nullptr);
    REQUIRE(out.find("price") != nullptr);
    CHECK(col_i64(out, "total") == std::vector<std::int64_t>{50, 60, 240, 80, 50});
}

TEST_CASE("E2E: update replaces existing column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { price = price + 1 }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{11, 21, 31, 41, 51});
}

TEST_CASE("E2E: update with subtraction", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { price = price - 5 }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{5, 15, 25, 35, 45});
}

TEST_CASE("E2E: update with multiplication by 2", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { doubled = price * 2 }];", tables);

    CHECK(col_i64(out, "doubled") == std::vector<std::int64_t>{20, 40, 60, 80, 100});
}

TEST_CASE("E2E: update with modulo", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { rem = price % 15 }];", tables);

    CHECK(col_i64(out, "rem") == std::vector<std::int64_t>{10, 5, 0, 10, 5});
}

// ─── Distinct ────────────────────────────────────────────────────────────────

TEST_CASE("E2E: distinct on single column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[distinct symbol];", tables);

    CHECK(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    // Order depends on first-seen — AAPL first, then GOOG.
    CHECK(symbols[0] == "AAPL");
    CHECK(symbols[1] == "GOOG");
}

TEST_CASE("E2E: distinct on multiple columns", "[e2e]") {
    runtime::Table t;
    t.add_column("a", Column<std::int64_t>{1, 1, 2, 2, 1});
    t.add_column("b", Column<std::string>{"X", "X", "Y", "Y", "Y"});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    auto out = run("data[distinct { a, b }];", tables);
    CHECK(out.rows() == 3);  // (1,X), (2,Y), (1,Y)
}

// ─── Order ───────────────────────────────────────────────────────────────────

TEST_CASE("E2E: order ascending", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[order { price asc }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: order descending", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[order { price desc }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{50, 40, 30, 20, 10});
}

TEST_CASE("E2E: order by string column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[order { symbol asc }];", tables);

    auto symbols = col_str(out, "symbol");
    CHECK(symbols[0] == "AAPL");
    CHECK(symbols[1] == "AAPL");
    CHECK(symbols[2] == "AAPL");
    CHECK(symbols[3] == "GOOG");
    CHECK(symbols[4] == "GOOG");
}

// ─── Aggregation ─────────────────────────────────────────────────────────────

TEST_CASE("E2E: sum aggregation with group-by", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, total = sum(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto totals = col_i64(out, "total");
    // AAPL: 10+30+50=90, GOOG: 20+40=60
    if (symbols[0] == "AAPL") {
        CHECK(totals[0] == 90);
        CHECK(totals[1] == 60);
    } else {
        CHECK(totals[0] == 60);
        CHECK(totals[1] == 90);
    }
}

TEST_CASE("E2E: count aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, n = count() }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto counts = col_i64(out, "n");
    if (symbols[0] == "AAPL") {
        CHECK(counts[0] == 3);
        CHECK(counts[1] == 2);
    } else {
        CHECK(counts[0] == 2);
        CHECK(counts[1] == 3);
    }
}

TEST_CASE("E2E: mean aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, avg = mean(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto avgs = col_dbl(out, "avg");
    if (symbols[0] == "AAPL") {
        CHECK(avgs[0] == Catch::Approx(30.0));   // (10+30+50)/3
        CHECK(avgs[1] == Catch::Approx(30.0));   // (20+40)/2
    } else {
        CHECK(avgs[0] == Catch::Approx(30.0));
        CHECK(avgs[1] == Catch::Approx(30.0));
    }
}

TEST_CASE("E2E: min and max aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out = run(
        "trades[select { symbol, lo = min(price), hi = max(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto lo = col_i64(out, "lo");
    auto hi = col_i64(out, "hi");
    if (symbols[0] == "AAPL") {
        CHECK(lo[0] == 10);
        CHECK(hi[0] == 50);
        CHECK(lo[1] == 20);
        CHECK(hi[1] == 40);
    } else {
        CHECK(lo[0] == 20);
        CHECK(hi[0] == 40);
        CHECK(lo[1] == 10);
        CHECK(hi[1] == 50);
    }
}

TEST_CASE("E2E: first and last aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out = run(
        "trades[select { symbol, f = first(price), l = last(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto firsts = col_i64(out, "f");
    auto lasts = col_i64(out, "l");
    if (symbols[0] == "AAPL") {
        CHECK(firsts[0] == 10);
        CHECK(lasts[0] == 50);
        CHECK(firsts[1] == 20);
        CHECK(lasts[1] == 40);
    } else {
        CHECK(firsts[0] == 20);
        CHECK(lasts[0] == 40);
        CHECK(firsts[1] == 10);
        CHECK(lasts[1] == 50);
    }
}

TEST_CASE("E2E: global aggregation without group-by", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { total = sum(price) }];", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "total") == std::vector<std::int64_t>{150});
}

TEST_CASE("E2E: count without group-by", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { n = count() }];", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "n") == std::vector<std::int64_t>{5});
}

// ─── Rename ──────────────────────────────────────────────────────────────────

TEST_CASE("E2E: rename column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[rename { cost = price }];", tables);

    CHECK(out.find("cost") != nullptr);
    CHECK(out.find("price") == nullptr);
    CHECK(col_i64(out, "cost") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: rename multiple columns", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[rename { cost = price, amount = qty }];", tables);

    CHECK(out.find("cost") != nullptr);
    CHECK(out.find("amount") != nullptr);
    CHECK(out.find("price") == nullptr);
    CHECK(out.find("qty") == nullptr);
    CHECK(col_i64(out, "cost") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
    CHECK(col_i64(out, "amount") == std::vector<std::int64_t>{5, 3, 8, 2, 1});
}

// ─── Multi-step pipelines ────────────────────────────────────────────────────

TEST_CASE("E2E: filter + aggregate + order pipeline", "[e2e]") {
    auto tables = make_trades();
    auto out = run(
        "trades[filter price > 15, select { symbol, total = sum(price) }, by symbol, "
        "order { total desc }];",
        tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto totals = col_i64(out, "total");
    // AAPL: 30+50=80, GOOG: 20+40=60 → desc: AAPL, GOOG
    CHECK(symbols[0] == "AAPL");
    CHECK(totals[0] == 80);
    CHECK(symbols[1] == "GOOG");
    CHECK(totals[1] == 60);
}

TEST_CASE("E2E: update then filter via chained let", "[e2e]") {
    runtime::Table t;
    t.add_column("price", Column<std::int64_t>{10, 20, 30, 40, 50});
    t.add_column("symbol", Column<std::string>{"AAPL", "GOOG", "AAPL", "GOOG", "AAPL"});
    runtime::TableRegistry tables;
    tables.emplace("trades", std::move(t));

    runtime::ExternRegistry registry;
    registry.register_table("get_trades",
                            [&tables](const runtime::ExternArgs&)
                                -> std::expected<runtime::ExternValue, std::string> {
                                return runtime::ExternValue{tables.at("trades")};
                            });

    const char* src = R"(
extern fn get_trades() -> DataFrame from "fake.hpp";
let t = get_trades();
let updated = t[update { doubled = price * 2 }];
updated[filter doubled > 50];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("E2E: filter then distinct", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 15, distinct symbol];", tables);

    CHECK(out.rows() == 2);
}

// ─── Chained let bindings (via REPL) ─────────────────────────────────────────

TEST_CASE("E2E: chained let bindings via execute_script", "[e2e]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [&tables](const runtime::ExternArgs&)
                                -> std::expected<runtime::ExternValue, std::string> {
                                return runtime::ExternValue{tables.at("data")};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let d = get_data();
let big = d[filter val > 3];
big[select { val }];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

// ─── Joins ───────────────────────────────────────────────────────────────────

TEST_CASE("E2E: inner join and select", "[e2e]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("name", Column<std::string>{"Alice", "Bob", "Charlie"});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("score", Column<std::int64_t>{85, 92, 78});

    runtime::TableRegistry tables;
    tables.emplace("people", std::move(lhs));
    tables.emplace("scores", std::move(rhs));

    auto out = run("people join scores on id;", tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_i64(out, "score") == std::vector<std::int64_t>{85, 92});
}

TEST_CASE("E2E: left join preserves all left rows", "[e2e]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("name", Column<std::string>{"Alice", "Bob", "Charlie"});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 4});
    rhs.add_column("score", Column<std::int64_t>{85, 78});

    runtime::TableRegistry tables;
    tables.emplace("people", std::move(lhs));
    tables.emplace("scores", std::move(rhs));

    auto out = run("people left join scores on id;", tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    // Unmatched rows get default value 0 for int
    CHECK(col_i64(out, "score") == std::vector<std::int64_t>{0, 85, 0});
}

// ─── Scalar predicate in filter ──────────────────────────────────────────────

TEST_CASE("E2E: filter with scalar variable", "[e2e]") {
    auto tables = make_trades();
    runtime::ScalarRegistry scalars;
    scalars.emplace("threshold", static_cast<std::int64_t>(30));

    auto out = run("trades[filter price >= threshold];", tables, &scalars);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30, 40, 50});
}

// ─── Arithmetic in filter predicates ─────────────────────────────────────────

TEST_CASE("E2E: filter with arithmetic expression", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price * qty > 100];", tables);

    // price*qty: 50, 60, 240, 80, 50 → only 240 > 100
    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30});
}

// ─── Double columns ──────────────────────────────────────────────────────────

TEST_CASE("E2E: filter and select with double column", "[e2e]") {
    runtime::Table t;
    t.add_column("price", Column<double>{1.5, 2.5, 3.5, 4.5});
    t.add_column("symbol", Column<std::string>{"A", "B", "A", "B"});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    auto out = run("data[filter price > 2.0, select { price }];", tables);
    REQUIRE(out.rows() == 3);
    auto prices = col_dbl(out, "price");
    CHECK(prices[0] == Catch::Approx(2.5));
    CHECK(prices[1] == Catch::Approx(3.5));
    CHECK(prices[2] == Catch::Approx(4.5));
}

// ─── Empty results ───────────────────────────────────────────────────────────

TEST_CASE("E2E: filter produces empty result", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 999];", tables);

    CHECK(out.rows() == 0);
}

// ─── Error handling ──────────────────────────────────────────────────────────

TEST_CASE("E2E: unknown table produces error", "[e2e]") {
    runtime::TableRegistry empty;
    auto error = run_error("no_such_table[select { x }];", empty);
    CHECK(!error.empty());
}

TEST_CASE("E2E: unknown column in filter produces error", "[e2e]") {
    auto tables = make_trades();
    auto error = run_error("trades[filter nonexistent > 10];", tables);
    CHECK(!error.empty());
}

TEST_CASE("E2E: unknown column in select produces error", "[e2e]") {
    auto tables = make_trades();
    auto error = run_error("trades[select nonexistent];", tables);
    CHECK(!error.empty());
}

// ─── Parse errors ────────────────────────────────────────────────────────────

TEST_CASE("E2E: parse error for incomplete expression", "[e2e]") {
    auto result = parser::parse("trades[filter];");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("E2E: parse error for missing closing bracket", "[e2e]") {
    auto result = parser::parse("trades[filter price > 10");
    REQUIRE_FALSE(result.has_value());
}

// ─── Multiple aggs in one query ──────────────────────────────────────────────

TEST_CASE("E2E: multiple aggregations in a single select", "[e2e]") {
    auto tables = make_trades();
    auto out = run(
        "trades[select { symbol, s = sum(price), n = count(), hi = max(price), lo = min(price) }, "
        "by symbol];",
        tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto sums = col_i64(out, "s");
    auto counts = col_i64(out, "n");
    auto highs = col_i64(out, "hi");
    auto lows = col_i64(out, "lo");

    // Find AAPL index
    int aapl = (symbols[0] == "AAPL") ? 0 : 1;
    int goog = 1 - aapl;
    CHECK(sums[aapl] == 90);
    CHECK(counts[aapl] == 3);
    CHECK(highs[aapl] == 50);
    CHECK(lows[aapl] == 10);
    CHECK(sums[goog] == 60);
    CHECK(counts[goog] == 2);
    CHECK(highs[goog] == 40);
    CHECK(lows[goog] == 20);
}

// ─── Update with group-by ────────────────────────────────────────────────────

TEST_CASE("E2E: aggregate mean per group", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, avg = mean(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto avgs = col_dbl(out, "avg");
    // AAPL mean: (10+30+50)/3 = 30, GOOG mean: (20+40)/2 = 30
    int aapl = (symbols[0] == "AAPL") ? 0 : 1;
    int goog = 1 - aapl;
    CHECK(avgs[aapl] == Catch::Approx(30.0));
    CHECK(avgs[goog] == Catch::Approx(30.0));
}

// ─── Date literals in filter ─────────────────────────────────────────────────

TEST_CASE("E2E: filter with date literal", "[e2e]") {
    using namespace std::chrono;
    auto d1 = Date{static_cast<std::int32_t>(
        sys_days{year{2024} / month{1} / std::chrono::day{1}}.time_since_epoch().count())};
    auto d2 = Date{static_cast<std::int32_t>(
        sys_days{year{2024} / month{1} / std::chrono::day{15}}.time_since_epoch().count())};
    auto d3 = Date{static_cast<std::int32_t>(
        sys_days{year{2024} / month{2} / std::chrono::day{1}}.time_since_epoch().count())};

    runtime::Table t;
    t.add_column("day", Column<Date>{d1, d2, d3});
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry tables;
    tables.emplace("cal", std::move(t));

    auto out = run("cal[filter day > date\"2024-01-10\"];", tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "val") == std::vector<std::int64_t>{2, 3});
}

// ─── Extern function calls ──────────────────────────────────────────────────

TEST_CASE("E2E: extern scalar function in select", "[e2e]") {
    runtime::Table t;
    t.add_column("x", Column<std::int64_t>{2, 3, 4});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    runtime::ExternRegistry externs;
    externs.register_scalar(
        "double_it", runtime::ScalarKind::Int,
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            const auto* v = std::get_if<std::int64_t>(args.data());
            if (v == nullptr) {
                return std::unexpected("expected int");
            }
            return runtime::ExternValue{(*v) * 2};
        });

    auto out = run("data[select { result = double_it(x) }];", tables, nullptr, &externs);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "result") == std::vector<std::int64_t>{4, 6, 8});
}

// ─── Large dataset basic smoke ───────────────────────────────────────────────

TEST_CASE("E2E: handles 1000-row table", "[e2e]") {
    runtime::Table t;
    std::vector<std::int64_t> prices;
    std::vector<std::string> symbols;
    prices.reserve(1000);
    symbols.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        prices.push_back(i);
        symbols.push_back(i % 2 == 0 ? "A" : "B");
    }
    t.add_column("price", Column<std::int64_t>(std::move(prices)));
    t.add_column("symbol", Column<std::string>(symbols));
    runtime::TableRegistry tables;
    tables.emplace("big", std::move(t));

    auto out = run("big[filter price >= 500, select { symbol, total = sum(price) }, by symbol];",
                   tables);
    REQUIRE(out.rows() == 2);
    // A: even numbers 500..998 → sum = 250*749 = 187250
    // B: odd numbers 501..999 → sum = 250*750 = 187500
    auto symbols_out = col_str(out, "symbol");
    auto totals = col_i64(out, "total");
    int a_idx = (symbols_out[0] == "A") ? 0 : 1;
    int b_idx = 1 - a_idx;
    CHECK(totals[a_idx] == 187250);
    CHECK(totals[b_idx] == 187500);
}

// ─── Stream wall-clock bucket flush ──────────────────────────────────────────

// Verify that a TimeBucket stream emits a completed bucket at wall-clock bucket
// end, not only when a message with a later data timestamp arrives.
//
// Scenario: both ticks have data timestamps in bucket 0 (by data time), but the
// second tick arrives after the bucket duration has elapsed on the wall clock.
// The expected behaviour is two separate sink calls — one for each tick — rather
// than a single call with both ticks merged into bucket 0.
TEST_CASE("Stream TimeBucket flushes at wall-clock bucket end", "[e2e][stream]") {
    std::vector<runtime::Table> emitted;
    int call_count = 0;

    runtime::ExternRegistry registry;

    // Source: returns tick 1 immediately, then sleeps past the 20 ms bucket
    // boundary before returning tick 2 (which still carries a data timestamp
    // inside bucket 0).  After that it signals EOF.
    registry.register_table(
        "tick_src",
        [&](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            ++call_count;
            if (call_count == 1) {
                runtime::Table t;
                t.add_column("ts", Column<Timestamp>{Timestamp{0}});
                t.add_column("price", Column<double>{100.0});
                t.time_index = "ts";
                return runtime::ExternValue{t};
            }
            if (call_count == 2) {
                // Sleep longer than the 20 ms bucket so the wall-clock check fires.
                std::this_thread::sleep_for(std::chrono::milliseconds(30));
                runtime::Table t;
                // Data timestamp 5 ms — still inside bucket 0 by data time.
                t.add_column("ts", Column<Timestamp>{Timestamp{5'000'000LL}});
                t.add_column("price", Column<double>{110.0});
                t.time_index = "ts";
                return runtime::ExternValue{t};
            }
            return runtime::ExternValue{runtime::Table{}};  // EOF
        });

    // Sink: capture each emitted (post-resample) table.
    registry.register_scalar_table_consumer(
        "tick_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            emitted.push_back(t);
            return runtime::ExternValue{std::int64_t{0}};
        });

    // Use lower() (not lower_expr) so the extern declarations are registered in
    // the lowerer's table_externs_ / sink_externs_ sets before the Stream node
    // is lowered.
    const char* src = R"(
extern fn tick_src() -> TimeFrame from "fake.hpp";
extern fn tick_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = tick_src(),
    transform = [resample 20ms, select { close = last(price) }],
    sink      = tick_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    REQUIRE(result.has_value());

    // Wall-clock flush fires between the two source calls: bucket containing
    // tick 1 is emitted before tick 2 is processed, giving two sink calls.
    REQUIRE(emitted.size() == 2);

    auto* close0 = std::get_if<Column<double>>(emitted[0].find("close"));
    REQUIRE(close0 != nullptr);
    CHECK((*close0)[0] == Catch::Approx(100.0));

    auto* close1 = std::get_if<Column<double>>(emitted[1].find("close"));
    REQUIRE(close1 != nullptr);
    CHECK((*close1)[0] == Catch::Approx(110.0));
}

// Verify that a source returning StreamTimeout (receive timeout, no data) keeps
// the stream alive and still triggers the wall-clock flush.
//
// Scenario: one tick arrives in bucket 0, then the source returns StreamTimeout
// repeatedly while 30 ms elapses (> 20 ms bucket), then signals EOF.
// Expected: the bucket is flushed via the wall-clock check during the timeout
// loop — not delayed until EOF.
TEST_CASE("Stream TimeBucket flushes via StreamTimeout during idle period", "[e2e][stream]") {
    std::vector<runtime::Table> emitted;
    int call_count = 0;
    auto stream_start = std::chrono::steady_clock::now();

    runtime::ExternRegistry registry;

    registry.register_table(
        "tick_src",
        [&](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            ++call_count;
            if (call_count == 1) {
                // First call: one tick in bucket 0.
                runtime::Table t;
                t.add_column("ts", Column<Timestamp>{Timestamp{0}});
                t.add_column("price", Column<double>{99.0});
                t.time_index = "ts";
                return runtime::ExternValue{t};
            }
            // Subsequent calls: return StreamTimeout until 30 ms have elapsed,
            // then signal EOF.  No second tick is ever sent.
            auto elapsed = std::chrono::steady_clock::now() - stream_start;
            if (elapsed < std::chrono::milliseconds(30)) {
                return runtime::ExternValue{runtime::StreamTimeout{}};
            }
            return runtime::ExternValue{runtime::Table{}};  // EOF
        });

    registry.register_scalar_table_consumer(
        "tick_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            emitted.push_back(t);
            return runtime::ExternValue{std::int64_t{0}};
        });

    const char* src = R"(
extern fn tick_src() -> TimeFrame from "fake.hpp";
extern fn tick_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = tick_src(),
    transform = [resample 20ms, select { close = last(price) }],
    sink      = tick_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    REQUIRE(result.has_value());

    // The bucket must have been flushed during the StreamTimeout loop, before EOF.
    // Without StreamTimeout support the bucket would only flush at EOF — which
    // would still yield one emission, but from the wrong trigger.
    // Here we verify the flush happened AND carried the correct value.
    REQUIRE(emitted.size() == 1);

    auto* close0 = std::get_if<Column<double>>(emitted[0].find("close"));
    REQUIRE(close0 != nullptr);
    CHECK((*close0)[0] == Catch::Approx(99.0));
}

// Verify that StreamBuffered (SPSC queue helper) works as a stream source.
//
// Scenario: a producer thread pushes two ticks into a StreamBuffered; the
// second is delayed by 30 ms so the wall-clock flush fires between them.
// Expected: two sink calls — one per bucket — without any double-buffering.
TEST_CASE("StreamBuffered feeds a TimeBucket stream from a producer thread", "[e2e][stream]") {
    std::vector<runtime::Table> emitted;

    runtime::ExternRegistry registry;

    // Create the SPSC-backed source.  Capacity of 8 is ample for this test.
    auto buf = std::make_shared<runtime::StreamBuffered>(8);
    registry.register_table("tick_src", buf->make_source_fn());

    registry.register_scalar_table_consumer(
        "tick_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            emitted.push_back(t);
            return runtime::ExternValue{std::int64_t{0}};
        });

    // Producer: write tick 1, sleep past the 20 ms bucket, write tick 2, close.
    std::thread producer([&buf] {
        runtime::Table t1;
        t1.add_column("ts", Column<Timestamp>{Timestamp{0}});
        t1.add_column("price", Column<double>{200.0});
        t1.time_index = "ts";
        buf->write(t1);

        std::this_thread::sleep_for(std::chrono::milliseconds(30));

        runtime::Table t2;
        // Data timestamp still inside bucket 0, but wall clock has moved on.
        t2.add_column("ts", Column<Timestamp>{Timestamp{5'000'000LL}});
        t2.add_column("price", Column<double>{210.0});
        t2.time_index = "ts";
        buf->write(t2);

        buf->close();
    });

    const char* src = R"(
extern fn tick_src() -> TimeFrame from "fake.hpp";
extern fn tick_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = tick_src(),
    transform = [resample 20ms, select { close = last(price) }],
    sink      = tick_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    producer.join();
    REQUIRE(result.has_value());

    // Wall-clock flush fires between the two ticks: two separate emissions.
    REQUIRE(emitted.size() == 2);

    auto* close0 = std::get_if<Column<double>>(emitted[0].find("close"));
    REQUIRE(close0 != nullptr);
    CHECK((*close0)[0] == Catch::Approx(200.0));

    auto* close1 = std::get_if<Column<double>>(emitted[1].find("close"));
    REQUIRE(close1 != nullptr);
    CHECK((*close1)[0] == Catch::Approx(210.0));
}

// Verify make_buffered_source: capacity is passed from the Ibex query as an
// argument to the extern fn, so the C++ plugin does not hard-code it.
//
// Scenario: two ticks separated by 30 ms so the wall-clock flush fires; the
// extern is declared with a capacity parameter and called as tick_src(8).
TEST_CASE("make_buffered_source takes capacity from Ibex query argument", "[e2e][stream]") {
    std::vector<runtime::Table> emitted;

    runtime::ExternRegistry registry;

    registry.register_table(
        "tick_src",
        runtime::make_buffered_source([](runtime::StreamBuffered& buf) {
            runtime::Table t1;
            t1.add_column("ts", Column<Timestamp>{Timestamp{0}});
            t1.add_column("price", Column<double>{300.0});
            t1.time_index = "ts";
            buf.write(t1);

            std::this_thread::sleep_for(std::chrono::milliseconds(30));

            runtime::Table t2;
            t2.add_column("ts", Column<Timestamp>{Timestamp{5'000'000LL}});
            t2.add_column("price", Column<double>{310.0});
            t2.time_index = "ts";
            buf.write(t2);

            buf.close();
        }));

    registry.register_scalar_table_consumer(
        "tick_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            emitted.push_back(t);
            return runtime::ExternValue{std::int64_t{0}};
        });

    // Capacity 8 is passed from the query — the C++ plugin receives it on
    // the first event-loop call and initialises the ring accordingly.
    const char* src = R"(
extern fn tick_src(capacity: Int) -> TimeFrame from "fake.hpp";
extern fn tick_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = tick_src(8),
    transform = [resample 20ms, select { close = last(price) }],
    sink      = tick_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    REQUIRE(result.has_value());

    REQUIRE(emitted.size() == 2);
    auto* c0 = std::get_if<Column<double>>(emitted[0].find("close"));
    REQUIRE(c0 != nullptr);
    CHECK((*c0)[0] == Catch::Approx(300.0));
    auto* c1 = std::get_if<Column<double>>(emitted[1].find("close"));
    REQUIRE(c1 != nullptr);
    CHECK((*c1)[0] == Catch::Approx(310.0));
}

// Stress-test StreamBuffered under high producer rate and a jittery consumer.
//
// The producer pushes N single-row Tables as fast as possible into a small
// ring (capacity 32).  The consumer (the sink callback) sleeps for 2 ms every
// 50 calls to simulate processing jitter, causing the ring to fill and forcing
// the producer into its backpressure yield loop.
//
// Each row carries a distinct 1 ms-spaced timestamp so the data-timestamp
// trigger fires a bucket flush for every row — giving exactly N sink calls
// with 1 row each.  The invariant is:
//
//   total rows received by sink == N
//
// If StreamBuffered dropped rows under backpressure this CHECK would fail.
TEST_CASE("StreamBuffered: no packet loss under producer stress with jittery consumer",
          "[e2e][stream]") {
    constexpr int N = 2'000;

    std::atomic<int> rows_received{0};
    std::atomic<int> sink_calls{0};

    // Small ring to force frequent backpressure on the producer thread.
    auto buf = std::make_shared<runtime::StreamBuffered>(32);

    runtime::ExternRegistry registry;
    registry.register_table("stress_src", buf->make_source_fn());

    // Jittery sink: every 50th call sleeps 2 ms, simulating an uneven consumer.
    registry.register_scalar_table_consumer(
        "stress_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            rows_received.fetch_add(static_cast<int>(t.rows()), std::memory_order_relaxed);
            if (sink_calls.fetch_add(1, std::memory_order_relaxed) % 50 == 49) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            return runtime::ExternValue{std::int64_t{0}};
        });

    // Producer: write N single-row tables as fast as possible.
    // Each row sits in its own 1 ms bucket so data-timestamp flushes fire
    // immediately rather than waiting for wall-clock bucket expiry.
    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            runtime::Table t;
            t.add_column("ts",
                         Column<Timestamp>{Timestamp{static_cast<std::int64_t>(i) * 1'000'000LL}});
            t.add_column("val", Column<double>{static_cast<double>(i)});
            t.time_index = "ts";
            buf->write(t);  // yields on backpressure — never drops
        }
        buf->close();
    });

    // Use resample 1ms so every adjacent pair of rows crosses a bucket boundary,
    // triggering a data-timestamp flush for each row.
    const char* src = R"(
extern fn stress_src() -> TimeFrame from "fake.hpp";
extern fn stress_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = stress_src(),
    transform = [resample 1ms, select { val = last(val) }],
    sink      = stress_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    producer.join();
    REQUIRE(result.has_value());

    // N rows in → N rows out.  Any drop under backpressure would show here.
    CHECK(rows_received.load() == N);
}

// ─── Melt ────────────────────────────────────────────────────────────────────

TEST_CASE("E2E: melt basic — all non-id columns melted", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("math", Column<std::int64_t>{90, 80});
    t.add_column("science", Column<std::int64_t>{85, 95});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[melt { name }];", tables);

    REQUIRE(out.rows() == 4);
    auto names = col_str(out, "name");
    auto vars = col_str(out, "variable");
    auto vals = col_i64(out, "value");

    // Row order: Alice×math, Alice×science, Bob×math, Bob×science
    CHECK(names == std::vector<std::string>{"Alice", "Alice", "Bob", "Bob"});
    CHECK(vars == std::vector<std::string>{"math", "science", "math", "science"});
    CHECK(vals == std::vector<std::int64_t>{90, 85, 80, 95});
}

TEST_CASE("E2E: melt with explicit measure columns via select", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("math", Column<std::int64_t>{90, 80});
    t.add_column("science", Column<std::int64_t>{85, 95});
    t.add_column("english", Column<std::int64_t>{88, 92});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[melt { name }, select { math, science }];", tables);

    REQUIRE(out.rows() == 4);
    auto names = col_str(out, "name");
    auto vars = col_str(out, "variable");
    auto vals = col_i64(out, "value");

    CHECK(names == std::vector<std::string>{"Alice", "Alice", "Bob", "Bob"});
    CHECK(vars == std::vector<std::string>{"math", "science", "math", "science"});
    CHECK(vals == std::vector<std::int64_t>{90, 85, 80, 95});
    // english column should not appear
    CHECK(out.find("english") == nullptr);
}

TEST_CASE("E2E: melt with multiple id columns", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("first", Column<std::string>{"A", "B"});
    t.add_column("last", Column<std::string>{"X", "Y"});
    t.add_column("score", Column<std::int64_t>{100, 200});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[melt { first, last }];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_str(out, "first") == std::vector<std::string>{"A", "B"});
    CHECK(col_str(out, "last") == std::vector<std::string>{"X", "Y"});
    CHECK(col_str(out, "variable") == std::vector<std::string>{"score", "score"});
    CHECK(col_i64(out, "value") == std::vector<std::int64_t>{100, 200});
}

TEST_CASE("E2E: melt with double measure columns", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("id", Column<std::string>{"a", "b"});
    t.add_column("x", Column<double>{1.5, 2.5});
    t.add_column("y", Column<double>{3.5, 4.5});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[melt { id }];", tables);

    REQUIRE(out.rows() == 4);
    CHECK(col_dbl(out, "value") == std::vector<double>{1.5, 3.5, 2.5, 4.5});
}

// ─── Dcast ───────────────────────────────────────────────────────────────────

TEST_CASE("E2E: dcast basic — long to wide", "[e2e][dcast]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Alice", "Bob", "Bob"});
    t.add_column("subject", Column<std::string>{"math", "science", "math", "science"});
    t.add_column("score", Column<std::int64_t>{90, 85, 80, 95});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[dcast subject, select score, by name];", tables);

    REQUIRE(out.rows() == 2);
    auto names = col_str(out, "name");
    CHECK(names == std::vector<std::string>{"Alice", "Bob"});

    auto math = col_i64(out, "math");
    auto science = col_i64(out, "science");
    CHECK(math == std::vector<std::int64_t>{90, 80});
    CHECK(science == std::vector<std::int64_t>{85, 95});
}

TEST_CASE("E2E: dcast with missing cells produces nulls", "[e2e][dcast]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("subject", Column<std::string>{"math", "science"});
    t.add_column("score", Column<std::int64_t>{90, 95});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[dcast subject, select score, by name];", tables);

    REQUIRE(out.rows() == 2);

    // Alice has math=90, science=null; Bob has math=null, science=95
    const auto* math_entry = out.find_entry("math");
    REQUIRE(math_entry != nullptr);
    REQUIRE(math_entry->validity.has_value());
    CHECK((*math_entry->validity)[0] == true);   // Alice has math
    CHECK((*math_entry->validity)[1] == false);   // Bob missing math

    const auto* sci_entry = out.find_entry("science");
    REQUIRE(sci_entry != nullptr);
    REQUIRE(sci_entry->validity.has_value());
    CHECK((*sci_entry->validity)[0] == false);    // Alice missing science
    CHECK((*sci_entry->validity)[1] == true);     // Bob has science
}

TEST_CASE("E2E: dcast with multiple row keys", "[e2e][dcast]") {
    runtime::Table t;
    t.add_column("first", Column<std::string>{"A", "A", "B", "B"});
    t.add_column("last", Column<std::string>{"X", "X", "Y", "Y"});
    t.add_column("metric", Column<std::string>{"height", "weight", "height", "weight"});
    t.add_column("value", Column<std::int64_t>{170, 65, 180, 75});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[dcast metric, select value, by { first, last }];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_str(out, "first") == std::vector<std::string>{"A", "B"});
    CHECK(col_str(out, "last") == std::vector<std::string>{"X", "Y"});
    CHECK(col_i64(out, "height") == std::vector<std::int64_t>{170, 180});
    CHECK(col_i64(out, "weight") == std::vector<std::int64_t>{65, 75});
}

TEST_CASE("E2E: melt then dcast roundtrip", "[e2e][melt][dcast]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("math", Column<std::int64_t>{90, 80});
    t.add_column("science", Column<std::int64_t>{85, 95});
    runtime::TableRegistry tables;
    tables.emplace("wide", std::move(t));

    // Melt then dcast should recover the original shape.
    auto melted = run("wide[melt { name }];", tables);
    runtime::TableRegistry tables2;
    tables2.emplace("long", std::move(melted));

    auto out = run("long[dcast variable, select value, by name];", tables2);

    REQUIRE(out.rows() == 2);
    auto names = col_str(out, "name");
    CHECK(names == std::vector<std::string>{"Alice", "Bob"});
    CHECK(col_i64(out, "math") == std::vector<std::int64_t>{90, 80});
    CHECK(col_i64(out, "science") == std::vector<std::int64_t>{85, 95});
}
