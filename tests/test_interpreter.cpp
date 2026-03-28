#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/rng.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <limits>
#include <sstream>

namespace {

using namespace ibex;

auto require_program(const char* source) -> parser::Program {
    auto result = parser::parse(source);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

auto require_ir(const char* source) -> ir::NodePtr {
    auto program = require_program(source);
    auto lowered = parser::lower(program);
    REQUIRE(lowered.has_value());
    return std::move(lowered.value());
}

auto date_from_ymd(int y, unsigned m, unsigned d) -> Date {
    using namespace std::chrono;
    auto day_point = sys_days{year{y} / month{m} / std::chrono::day{d}};
    return Date{static_cast<std::int32_t>(day_point.time_since_epoch().count())};
}

auto ts_from_nanos(std::int64_t nanos) -> Timestamp {
    return Timestamp{nanos};
}

}  // namespace

TEST_CASE("Interpret filter + select") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 15, select { price }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_col != nullptr);
    REQUIRE(price_ints->size() == 2);
    REQUIRE((*price_ints)[0] == 20);
    REQUIRE((*price_ints)[1] == 30);
}

TEST_CASE("Interpret filter with scalar predicate") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("t", static_cast<std::int64_t>(15));

    auto ir = require_ir("trades[filter price > t, select { price }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(price_ints->size() == 2);
    REQUIRE((*price_ints)[0] == 20);
    REQUIRE((*price_ints)[1] == 30);
}

TEST_CASE("Interpret filter with date literal") {
    runtime::Table table;
    table.add_column("day", Column<Date>{
                                date_from_ymd(2024, 1, 1),
                                date_from_ymd(2024, 1, 2),
                                date_from_ymd(2024, 1, 3),
                            });

    runtime::TableRegistry registry;
    registry.emplace("calendar", table);

    auto ir = require_ir("calendar[filter day >= date\"2024-01-02\", select { day }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    const auto* day_col = result->find("day");
    REQUIRE(day_col != nullptr);
    const auto* days = std::get_if<Column<Date>>(day_col);
    REQUIRE(days != nullptr);
    REQUIRE(days->size() == 2);
    REQUIRE((*days)[0].days == date_from_ymd(2024, 1, 2).days);
    REQUIRE((*days)[1].days == date_from_ymd(2024, 1, 3).days);
}

TEST_CASE("Interpret update alias") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{5, 7});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { p = price }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);

    const auto* price_col = result->find("price");
    const auto* alias_col = result->find("p");
    REQUIRE(price_col != nullptr);
    REQUIRE(alias_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    const auto* alias_ints = std::get_if<Column<std::int64_t>>(alias_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(alias_ints != nullptr);
    REQUIRE(price_ints->size() == alias_ints->size());
    REQUIRE((*alias_ints)[0] == 5);
    REQUIRE((*alias_ints)[1] == 7);
}

TEST_CASE("Interpret update with arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { price = price + 1 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(price_ints->size() == 3);
    REQUIRE((*price_ints)[0] == 2);
    REQUIRE((*price_ints)[1] == 3);
    REQUIRE((*price_ints)[2] == 4);
}

TEST_CASE("Interpret distinct") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 10, 20, 20});
    table.add_column("symbol", Column<std::string>{"A", "A", "B", "B"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[distinct { symbol, price }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);

    const auto* symbol_col = result->find("symbol");
    const auto* price_col = result->find("price");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(price_col != nullptr);
    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* prices = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices != nullptr);
    REQUIRE(symbols->size() == 2);
    REQUIRE(prices->size() == 2);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*prices)[1] == 20);
}

TEST_CASE("Interpret order") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{20, 10, 20});
    table.add_column("symbol", Column<std::string>{"B", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[order { price asc, symbol asc }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* price_col = result->find("price");
    const auto* symbol_col = result->find("symbol");
    REQUIRE(price_col != nullptr);
    REQUIRE(symbol_col != nullptr);
    const auto* prices = std::get_if<Column<std::int64_t>>(price_col);
    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    REQUIRE(prices != nullptr);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*symbols)[1] == "A");
    REQUIRE((*prices)[2] == 20);
    REQUIRE((*symbols)[2] == "B");
}

TEST_CASE("Interpret select with function call") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ExternRegistry externs;
    externs.register_scalar(
        "square", runtime::ScalarKind::Int,
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("square() expects 1 argument");
            }
            const auto* value = std::get_if<std::int64_t>(args.data());
            if (value == nullptr) {
                return std::unexpected("square() expects int argument");
            }
            return runtime::ExternValue{(*value) * (*value)};
        });

    auto ir = require_ir("trades[select { foo = square(price) }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);

    const auto* foo_col = result->find("foo");
    REQUIRE(foo_col != nullptr);
    const auto* foo_ints = std::get_if<Column<std::int64_t>>(foo_col);
    REQUIRE(foo_ints != nullptr);
    REQUIRE(foo_ints->size() == 2);
    REQUIRE((*foo_ints)[0] == 4);
    REQUIRE((*foo_ints)[1] == 9);
}

TEST_CASE("Interpret grouped aggregation") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "C"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, total = sum(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* total_col = result->find("total");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(total_col != nullptr);

    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* totals = std::get_if<Column<std::int64_t>>(total_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(totals != nullptr);
    REQUIRE(symbols->size() == 3);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*totals)[0] == 40);
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*totals)[1] == 20);
    REQUIRE((*symbols)[2] == "C");
    REQUIRE((*totals)[2] == 25);
}

TEST_CASE("Interpret aggregate arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "C"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, avg = sum(price) / count() }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* avg_col = result->find("avg");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(avg_col != nullptr);

    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* avgs = std::get_if<Column<double>>(avg_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(avgs != nullptr);
    REQUIRE(symbols->size() == 3);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*avgs)[0] == Catch::Approx(20.0));
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*avgs)[1] == Catch::Approx(20.0));
    REQUIRE((*symbols)[2] == "C");
    REQUIRE((*avgs)[2] == Catch::Approx(25.0));
}

TEST_CASE("Interpret first and last aggregation") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir(
        "trades[select { symbol, first_price = first(price), last_price = last(price) }, by "
        "symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbol_col = result->find("symbol");
    const auto* first_col = result->find("first_price");
    const auto* last_col = result->find("last_price");
    REQUIRE(symbol_col != nullptr);
    REQUIRE(first_col != nullptr);
    REQUIRE(last_col != nullptr);

    const auto* symbols = std::get_if<Column<std::string>>(symbol_col);
    const auto* firsts = std::get_if<Column<std::int64_t>>(first_col);
    const auto* lasts = std::get_if<Column<std::int64_t>>(last_col);
    REQUIRE(symbols != nullptr);
    REQUIRE(firsts != nullptr);
    REQUIRE(lasts != nullptr);

    REQUIRE(symbols->size() == 2);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*firsts)[0] == 10);
    REQUIRE((*lasts)[0] == 25);
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*firsts)[1] == 20);
    REQUIRE((*lasts)[1] == 20);
}

TEST_CASE("Extract scalar from single-row table") {
    runtime::Table table;
    table.add_column("total", Column<std::int64_t>{42});

    auto result = runtime::extract_scalar(table, "total");
    REQUIRE(result.has_value());
    REQUIRE(std::get<std::int64_t>(result.value()) == 42);
}

TEST_CASE("Extract scalar errors on multi-row table") {
    runtime::Table table;
    table.add_column("total", Column<std::int64_t>{1, 2});

    auto result = runtime::extract_scalar(table, "total");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Interpret update with scalar reference") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    runtime::ScalarRegistry scalars;
    scalars.emplace("offset", std::int64_t{10});

    auto ir = require_ir("trades[update { price = price + offset }];");
    auto result = runtime::interpret(*ir, registry, &scalars);
    REQUIRE(result.has_value());

    const auto* price_col = result->find("price");
    REQUIRE(price_col != nullptr);
    const auto* price_ints = std::get_if<Column<std::int64_t>>(price_col);
    REQUIRE(price_ints != nullptr);
    REQUIRE(price_ints->size() == 3);
    REQUIRE((*price_ints)[0] == 11);
    REQUIRE((*price_ints)[1] == 12);
    REQUIRE((*price_ints)[2] == 13);
}

TEST_CASE("Interpret order descending") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 30, 20});
    table.add_column("symbol", Column<std::string>{"A", "B", "C"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[order { price desc }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 30);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 10);
}

TEST_CASE("Interpret order on empty table") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[order { price asc }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Interpret distinct on empty table") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[distinct { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Interpret order then distinct") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{20, 10, 20, 10});
    table.add_column("symbol", Column<std::string>{"B", "A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // order first so distinct result order is deterministic
    auto ir = require_ir("trades[order { price asc, symbol asc }, distinct { price, symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    const auto* symbols = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(prices != nullptr);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices->size() == 2);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*symbols)[1] == "B");
}

TEST_CASE("Interpret distinct preserves first occurrence without order") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B", "A", "C", "B"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[distinct { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* symbols = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(symbols != nullptr);
    REQUIRE(symbols->size() == 3);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*symbols)[2] == "C");
}

TEST_CASE("Interpret filter on empty table") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 10];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Interpret aggregate on single row") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{42});
    table.add_column("symbol", Column<std::string>{"X"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { symbol, total = sum(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* totals = std::get_if<Column<std::int64_t>>(result->find("total"));
    REQUIRE(totals != nullptr);
    REQUIRE(totals->size() == 1);
    REQUIRE((*totals)[0] == 42);
}

TEST_CASE("Interpret update with double arithmetic") {
    runtime::Table table;
    table.add_column("price", Column<double>{1.5, 2.5, 3.5});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[update { price_x2 = price * 2 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = std::get_if<Column<double>>(result->find("price_x2"));
    REQUIRE(col != nullptr);
    REQUIRE(col->size() == 3);
    REQUIRE((*col)[0] == Catch::Approx(3.0));
    REQUIRE((*col)[1] == Catch::Approx(5.0));
    REQUIRE((*col)[2] == Catch::Approx(7.0));
}

TEST_CASE("Interpret update rejects out-of-range int64 to Date coercion") {
    runtime::Table table;
    table.add_column("id", Column<std::int64_t>{1});

    runtime::TableRegistry registry;
    registry.emplace("rows", table);

    runtime::ExternRegistry externs;
    externs.register_scalar(
        "bad_date", runtime::ScalarKind::Date,
        [](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{runtime::ScalarValue{
                static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max()) + 1}};
        });

    auto ir = require_ir("rows[update { d = bad_date() }];");
    REQUIRE_THROWS(runtime::interpret(*ir, registry, nullptr, &externs));
}

TEST_CASE("Interpret compound filter: AND") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{5, 15, 25});
    table.add_column("qty", Column<std::int64_t>{3, 8, 2});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // price > 10 && qty < 5 → only row with price=25, qty=2 passes
    auto ir = require_ir("trades[filter price > 10 && qty < 5];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 25);
}

TEST_CASE("Interpret compound filter: OR") {
    runtime::Table table;
    table.add_column("symbol", Column<std::string>{"A", "B", "C"});
    table.add_column("price", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // symbol == "A" || symbol == "C"
    auto ir = require_ir("trades[filter symbol == \"A\" || symbol == \"C\"];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* syms = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(syms != nullptr);
    REQUIRE((*syms)[0] == "A");
    REQUIRE((*syms)[1] == "C");
}

TEST_CASE("Interpret compound filter: arithmetic in predicate") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 60, 40});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // price * 2 > 100 → only price=60 passes
    auto ir = require_ir("trades[filter price * 2 > 100];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 60);
}

TEST_CASE("Interpret compound filter: NOT") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // !(price > 15) → price=10 passes
    auto ir = require_ir("trades[filter !(price > 15)];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
}

TEST_CASE("Interpret compound filter: three-way AND") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{5, 15, 25, 35});
    table.add_column("qty", Column<std::int64_t>{1, 4, 3, 2});
    table.add_column("flag", Column<std::int64_t>{1, 1, 0, 1});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    // price > 10 && qty < 5 && flag == 1 → price=15 (qty=4,flag=1) and price=35 (qty=2,flag=1)
    auto ir = require_ir("trades[filter price > 10 && qty < 5 && flag == 1];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* prices = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 15);
    REQUIRE((*prices)[1] == 35);
}

// ─── TimeFrame / as_timeframe tests ──────────────────────────────────────────

TEST_CASE("as_timeframe on Timestamp column sets time_index and sorts ascending") {
    runtime::Table table;
    table.add_column("ts",
                     Column<Timestamp>{ts_from_nanos(300), ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{30, 10, 20});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "ts");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");

    const auto* ts_col = std::get_if<Column<Timestamp>>(result->find("ts"));
    REQUIRE(ts_col != nullptr);
    REQUIRE((*ts_col)[0].nanos == 100);
    REQUIRE((*ts_col)[1].nanos == 200);
    REQUIRE((*ts_col)[2].nanos == 300);
}

TEST_CASE("as_timeframe on Date column sets time_index and sorts ascending") {
    runtime::Table table;
    table.add_column("day", Column<Date>{date_from_ymd(2024, 1, 3), date_from_ymd(2024, 1, 1),
                                         date_from_ymd(2024, 1, 2)});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "day");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "day");

    const auto* day_col = std::get_if<Column<Date>>(result->find("day"));
    REQUIRE(day_col != nullptr);
    REQUIRE((*day_col)[0].days == date_from_ymd(2024, 1, 1).days);
    REQUIRE((*day_col)[1].days == date_from_ymd(2024, 1, 2).days);
    REQUIRE((*day_col)[2].days == date_from_ymd(2024, 1, 3).days);
}

TEST_CASE("as_timeframe on non-existent column returns error") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "missing");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("not found") != std::string::npos);
}

TEST_CASE("as_timeframe on non-timestamp column returns error") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 20.0, 30.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "price");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("must be Timestamp, Date, or Int") != std::string::npos);
}

TEST_CASE("as_timeframe on Int column treats values as nanoseconds") {
    runtime::Table table;
    // 0 ns, 1s, 2s
    table.add_column("ts", Column<std::int64_t>{0, 1'000'000'000LL, 2'000'000'000LL});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir(R"(as_timeframe(data, "ts");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index == "ts");
    REQUIRE(result->rows() == 3);
    // ts column should now be Timestamp
    REQUIRE(result->find("ts") != nullptr);
    REQUIRE(std::holds_alternative<Column<Timestamp>>(*result->find("ts")));
}

TEST_CASE("Filter on TimeFrame preserves time_index") {
    runtime::Table table;
    table.add_column("ts",
                     Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200), ts_from_nanos(300)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[filter val > 15];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
    REQUIRE(result->rows() == 2);
}

TEST_CASE("Project keeping timestamp col preserves time_index") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[select { ts, val }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
}

TEST_CASE("Project dropping timestamp col clears time_index") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[select { val }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE_FALSE(result->time_index.has_value());
}

TEST_CASE("Order by non-time-col on TimeFrame returns error") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[order val asc];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("order on TimeFrame") != std::string::npos);
}

TEST_CASE("Window on plain DataFrame returns TimeFrame error") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // window + update on a plain DataFrame (no time_index) → error
    auto ir = require_ir("data[window 5m, update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a TimeFrame") != std::string::npos);
}

TEST_CASE("Window with no update clause returns unsupported error") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(100), ts_from_nanos(200)});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // window without any update node → "only 'update' is currently supported"
    auto ir = require_ir("data[window 5m];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("only 'update'") != std::string::npos);
}

// ─── lag / lead tests ─────────────────────────────────────────────────────────

TEST_CASE("lag(val, 1) on TimeFrame shifts values and fills default at start") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { prev = lag(val, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* prev = std::get_if<Column<std::int64_t>>(result->find("prev"));
    REQUIRE(prev != nullptr);
    REQUIRE((*prev)[0] == 0);  // default
    REQUIRE((*prev)[1] == 10);
    REQUIRE((*prev)[2] == 20);
}

TEST_CASE("lead(val, 1) on TimeFrame shifts values and fills default at end") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { nxt = lead(val, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* nxt = std::get_if<Column<std::int64_t>>(result->find("nxt"));
    REQUIRE(nxt != nullptr);
    REQUIRE((*nxt)[0] == 20);
    REQUIRE((*nxt)[1] == 30);
    REQUIRE((*nxt)[2] == 0);  // default
}

TEST_CASE("lag(val, 0) is identity") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1)});
    table.add_column("val", Column<std::int64_t>{42, 99});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { same = lag(val, 0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* same = std::get_if<Column<std::int64_t>>(result->find("same"));
    REQUIRE(same != nullptr);
    REQUIRE((*same)[0] == 42);
    REQUIRE((*same)[1] == 99);
}

TEST_CASE("lag on non-TimeFrame returns error") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { prev = lag(val, 1) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a TimeFrame") != std::string::npos);
}

// ─── cumsum / cumprod tests ───────────────────────────────────────────────────

TEST_CASE("cumsum on Int column produces running sum") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3, 4});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cs = std::get_if<Column<std::int64_t>>(result->find("cs"));
    REQUIRE(cs != nullptr);
    REQUIRE((*cs)[0] == 1);
    REQUIRE((*cs)[1] == 3);
    REQUIRE((*cs)[2] == 6);
    REQUIRE((*cs)[3] == 10);
}

TEST_CASE("cumsum on Float column produces running sum") {
    runtime::Table table;
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cs = std::get_if<Column<double>>(result->find("cs"));
    REQUIRE(cs != nullptr);
    REQUIRE((*cs)[0] == Catch::Approx(1.0));
    REQUIRE((*cs)[1] == Catch::Approx(3.0));
    REQUIRE((*cs)[2] == Catch::Approx(6.0));
}

TEST_CASE("cumprod on Int column produces running product") {
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{1, 2, 3, 4});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cp = cumprod(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cp = std::get_if<Column<std::int64_t>>(result->find("cp"));
    REQUIRE(cp != nullptr);
    REQUIRE((*cp)[0] == 1);
    REQUIRE((*cp)[1] == 2);
    REQUIRE((*cp)[2] == 6);
    REQUIRE((*cp)[3] == 24);
}

TEST_CASE("cumprod on Float column produces running product") {
    runtime::Table table;
    table.add_column("val", Column<double>{2.0, 3.0, 4.0});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cp = cumprod(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cp = std::get_if<Column<double>>(result->find("cp"));
    REQUIRE(cp != nullptr);
    REQUIRE((*cp)[0] == Catch::Approx(2.0));
    REQUIRE((*cp)[1] == Catch::Approx(6.0));
    REQUIRE((*cp)[2] == Catch::Approx(24.0));
}

TEST_CASE("cumsum on TimeFrame works without window clause") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* cs = std::get_if<Column<std::int64_t>>(result->find("cs"));
    REQUIRE(cs != nullptr);
    REQUIRE((*cs)[0] == 10);
    REQUIRE((*cs)[1] == 30);
    REQUIRE((*cs)[2] == 60);
}

TEST_CASE("cumsum on non-numeric column returns error") {
    runtime::Table table;
    table.add_column("val", Column<std::string>{"a", "b", "c"});

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { cs = cumsum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("must be numeric") != std::string::npos);
}

TEST_CASE("rolling_sum outside window clause returns error") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1)});
    table.add_column("val", Column<std::int64_t>{10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a window clause") != std::string::npos);
}

// ─── rolling aggregate tests ──────────────────────────────────────────────────
// Timestamps: 0ns, 1ns, 2ns  Values: 10, 20, 30
// Window 1ns: [t-1, t]
//   row 0 (t=0): [0-1,0]=[-1,0] → only row 0
//   row 1 (t=1): [0,1]          → rows 0,1
//   row 2 (t=2): [1,2]          → rows 1,2

TEST_CASE("rolling_sum with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(*result->time_index == "ts");

    const auto* s = std::get_if<Column<std::int64_t>>(result->find("s"));
    REQUIRE(s != nullptr);
    REQUIRE((*s)[0] == 10);  // row 0 only
    REQUIRE((*s)[1] == 30);  // rows 0+1
    REQUIRE((*s)[2] == 50);  // rows 1+2
}

TEST_CASE("rolling_mean with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { m = rolling_mean(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    REQUIRE((*m)[0] == Catch::Approx(10.0));
    REQUIRE((*m)[1] == Catch::Approx(15.0));
    REQUIRE((*m)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_count with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { c = rolling_count() }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* c = std::get_if<Column<std::int64_t>>(result->find("c"));
    REQUIRE(c != nullptr);
    REQUIRE((*c)[0] == 1);
    REQUIRE((*c)[1] == 2);
    REQUIRE((*c)[2] == 2);
}

TEST_CASE("rolling_min and rolling_max with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{30, 10, 20});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir_min = require_ir("data[window 1ns, update { mn = rolling_min(val) }];");
    auto res_min = runtime::interpret(*ir_min, registry);
    REQUIRE(res_min.has_value());
    const auto* mn = std::get_if<Column<std::int64_t>>(res_min->find("mn"));
    REQUIRE(mn != nullptr);
    REQUIRE((*mn)[0] == 30);  // row 0 only
    REQUIRE((*mn)[1] == 10);  // min(30,10)
    REQUIRE((*mn)[2] == 10);  // min(10,20)

    auto ir_max = require_ir("data[window 1ns, update { mx = rolling_max(val) }];");
    auto res_max = runtime::interpret(*ir_max, registry);
    REQUIRE(res_max.has_value());
    const auto* mx = std::get_if<Column<std::int64_t>>(res_max->find("mx"));
    REQUIRE(mx != nullptr);
    REQUIRE((*mx)[0] == 30);  // row 0 only
    REQUIRE((*mx)[1] == 30);  // max(30,10)
    REQUIRE((*mx)[2] == 20);  // max(10,20)
}

// ─── resample tests ───────────────────────────────────────────────────────────

TEST_CASE("resample basic OHLC — 3 two-minute buckets") {
    // 6 ticks: t=0,1,2 in bucket 0; t=3,4,5 in bucket 1; t=6 in bucket 2
    // (using minute-scale nanos: 1 min = 60e9 ns)
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0 * min_ns), ts_from_nanos(1 * min_ns),
                                             ts_from_nanos(2 * min_ns), ts_from_nanos(3 * min_ns),
                                             ts_from_nanos(4 * min_ns), ts_from_nanos(5 * min_ns)});
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0, 50.0, 60.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("tf", table);

    auto ir = require_ir(
        R"(tf[resample 2m, select { open = first(price), high = max(price), low = min(price), close = last(price) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    // 3 buckets: [0..1], [2..3], [4..5]
    REQUIRE(result->rows() == 3);
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");

    const auto* open_col = result->find("open");
    const auto* close_col = result->find("close");
    const auto* high_col = result->find("high");
    const auto* low_col = result->find("low");
    REQUIRE(open_col != nullptr);
    REQUIRE(close_col != nullptr);
    REQUIRE(high_col != nullptr);
    REQUIRE(low_col != nullptr);

    // bucket 0: rows 0,1 → open=10, close=20, low=10, high=20
    const auto& opens = std::get<Column<double>>(*open_col);
    const auto& closes = std::get<Column<double>>(*close_col);
    const auto& highs = std::get<Column<double>>(*high_col);
    const auto& lows = std::get<Column<double>>(*low_col);
    REQUIRE(opens[0] == 10.0);
    REQUIRE(closes[0] == 20.0);
    REQUIRE(highs[0] == 20.0);
    REQUIRE(lows[0] == 10.0);

    // bucket 1: rows 2,3 → open=30, close=40
    REQUIRE(opens[1] == 30.0);
    REQUIRE(closes[1] == 40.0);

    // bucket 2: rows 4,5 → open=50, close=60
    REQUIRE(opens[2] == 50.0);
    REQUIRE(closes[2] == 60.0);
}

TEST_CASE("resample with by — one bucket per (bucket, symbol)") {
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1 * min_ns),
                                             ts_from_nanos(0), ts_from_nanos(1 * min_ns)});
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "B", "B"});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("tf", table);

    auto ir = require_ir(R"(tf[resample 1m, select { close = last(price) }, by symbol];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    // 2 symbols × 2 buckets = 4 rows
    REQUIRE(result->rows() == 4);
    REQUIRE(result->find("close") != nullptr);
    REQUIRE(result->find("symbol") != nullptr);
}

TEST_CASE("resample error on non-timeframe") {
    runtime::Table table;
    table.add_column("ts", Column<std::int64_t>{0, 1, 2});
    table.add_column("price", Column<double>{1.0, 2.0, 3.0});
    // No time_index set

    runtime::TableRegistry registry;
    registry.emplace("plain", table);

    auto ir = require_ir(R"(plain[resample 1m, select { close = last(price) }];)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("requires a TimeFrame") != std::string::npos);
}

TEST_CASE("rolling_sum preserves other columns and time_index") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<std::int64_t>{1, 2, 3});
    table.add_column("label", Column<std::string>{"a", "b", "c"});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_sum(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
    // original columns still present
    REQUIRE(result->find("val") != nullptr);
    REQUIRE(result->find("label") != nullptr);
    REQUIRE(result->find("s") != nullptr);
}

// ─── rolling_median / rolling_std / rolling_ewma ─────────────────────────────
// Same 3-row, 1ns-window setup used by the other rolling tests:
//   ts: 0ns, 1ns, 2ns   val: 10, 20, 30
//   window 1ns → [t-1, t]
//     row 0: only row 0      → {10}
//     row 1: rows 0..1       → {10,20}
//     row 2: rows 1..2       → {20,30}

TEST_CASE("rolling_median with 1ns window") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { m = rolling_median(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    // row 0: median({10}) = 10
    CHECK((*m)[0] == Catch::Approx(10.0));
    // row 1: median({10,20}) = 15   (even count → average of two middle values)
    CHECK((*m)[1] == Catch::Approx(15.0));
    // row 2: median({20,30}) = 25
    CHECK((*m)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_median odd window size") {
    // 5 timestamps, window 2ns → row 2 sees {10,20,30} (odd count)
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3), ts_from_nanos(4)});
    table.add_column("val", Column<double>{10.0, 30.0, 20.0, 40.0, 50.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 2ns, update { m = rolling_median(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* m = std::get_if<Column<double>>(result->find("m"));
    REQUIRE(m != nullptr);
    // row 2 (t=2): window [0,2] → {10,30,20} sorted {10,20,30} → median = 20
    CHECK((*m)[2] == Catch::Approx(20.0));
    // row 3 (t=3): window [1,3] → {30,20,40} sorted {20,30,40} → median = 30
    CHECK((*m)[3] == Catch::Approx(30.0));
}

TEST_CASE("rolling_std with 1ns window") {
    // row 0: {10}      → n<2, stddev = 0.0
    // row 1: {10,20}   → mean=15, M2=50, sample std = sqrt(50/1)=sqrt(50)≈7.071
    // row 2: {20,30}   → mean=25, M2=50, sample std = sqrt(50/1)=sqrt(50)≈7.071
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_std(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(s != nullptr);
    // single-element window → 0.0 (undefined sample stddev)
    CHECK((*s)[0] == Catch::Approx(0.0));
    // {10,20}: sample std = sqrt(50) ≈ 7.0711
    CHECK((*s)[1] == Catch::Approx(7.0711).epsilon(1e-3));
    // {20,30}: sample std = sqrt(50) ≈ 7.0711
    CHECK((*s)[2] == Catch::Approx(7.0711).epsilon(1e-3));
}

TEST_CASE("rolling_std larger window") {
    // {0,2,4}: mean=2, M2=8, sample std = sqrt(8/2) = 2.0
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3), ts_from_nanos(4)});
    table.add_column("val", Column<double>{0.0, 2.0, 4.0, 6.0, 8.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // window 2ns: row 2 sees {0,2,4}
    auto ir = require_ir("data[window 2ns, update { s = rolling_std(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(s != nullptr);
    // row 2: {0,2,4} → sample std = 2.0
    CHECK((*s)[2] == Catch::Approx(2.0));
}

TEST_CASE("rolling_ewma with 1ns window") {
    // alpha = 0.5
    // row 0: window {10}     → ewma = 10
    // row 1: window {10,20}  → ewma starts 10, then 0.5*20 + 0.5*10 = 15
    // row 2: window {20,30}  → ewma starts 20, then 0.5*30 + 0.5*20 = 25
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { e = rolling_ewma(val, 0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e = std::get_if<Column<double>>(result->find("e"));
    REQUIRE(e != nullptr);
    CHECK((*e)[0] == Catch::Approx(10.0));
    CHECK((*e)[1] == Catch::Approx(15.0));
    CHECK((*e)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_ewma larger window") {
    // window 2ns, alpha = 0.5
    // row 0 (t=0): {10}            → ewma = 10
    // row 1 (t=1): {10, 20}        → 10 → 0.5*20+0.5*10 = 15
    // row 2 (t=2): {10, 20, 30}    → 10 → 15 → 0.5*30+0.5*15 = 22.5
    // row 3 (t=3): {20, 30, 40}    → 20 → 0.5*30+0.5*20=25 → 0.5*40+0.5*25 = 32.5
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 2ns, update { e = rolling_ewma(val, 0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e = std::get_if<Column<double>>(result->find("e"));
    REQUIRE(e != nullptr);
    CHECK((*e)[0] == Catch::Approx(10.0));
    CHECK((*e)[1] == Catch::Approx(15.0));
    CHECK((*e)[2] == Catch::Approx(22.5));
    CHECK((*e)[3] == Catch::Approx(32.5));
}

// ─── Phase 1 null semantics ───────────────────────────────────────────────────

TEST_CASE("null: right join unmatched rows produce null left columns", "[null][join]") {
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2});
    left.add_column("name", Column<std::string>{"alice", "bob"});

    right.add_column("id", Column<std::int64_t>{2, 3});
    right.add_column("score", Column<double>{20.0, 30.0});

    auto result = runtime::join_tables(left, right, ir::JoinKind::Right, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 2);

    const auto& name_entry = t.columns[t.index.at("name")];
    CHECK_FALSE(runtime::is_null(name_entry, 0));
    CHECK(runtime::is_null(name_entry, 1));

    const auto& id_col = std::get<Column<std::int64_t>>(*t.columns[t.index.at("id")].column);
    CHECK(id_col[0] == 2);
    CHECK(id_col[1] == 3);
}

TEST_CASE("null: outer join unmatched rows produce nulls on both sides", "[null][join]") {
    // left:  id {1, 2},  name  {"alice", "bob"}
    // right: id {2, 3},  score {20.0, 30.0}
    //
    // Full outer join on id emits left rows in left-table order, then any
    // unmatched right rows:
    //   row 0 → id=1, name="alice" (left-only)  →  score is NULL
    //   row 1 → id=2, name="bob"  (matched)     →  score=20.0
    //   row 2 → id=3              (right-only)  →  name is NULL, score=30.0
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2});
    left.add_column("name", Column<std::string>{"alice", "bob"});

    right.add_column("id", Column<std::int64_t>{2, 3});
    right.add_column("score", Column<double>{20.0, 30.0});

    auto result = runtime::join_tables(left, right, ir::JoinKind::Outer, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 3);

    const auto& name_entry = t.columns[t.index.at("name")];
    const auto& score_entry = t.columns[t.index.at("score")];

    // row 0: id=1, left-only → name valid, score null
    CHECK_FALSE(runtime::is_null(name_entry, 0));
    CHECK(runtime::is_null(score_entry, 0));

    // row 1: id=2, matched  → both valid
    CHECK_FALSE(runtime::is_null(name_entry, 1));
    CHECK_FALSE(runtime::is_null(score_entry, 1));

    // row 2: id=3, right-only → name null, score valid
    CHECK(runtime::is_null(name_entry, 2));
    CHECK_FALSE(runtime::is_null(score_entry, 2));
}
TEST_CASE("null: left join unmatched rows produce null right columns", "[null][join]") {
    runtime::Table left, right;
    Column<std::int64_t> lid;
    lid.push_back(1);
    lid.push_back(2);
    lid.push_back(3);
    left.add_column("id", std::move(lid));

    Column<std::int64_t> rid;
    rid.push_back(1);
    rid.push_back(3);
    right.add_column("id", std::move(rid));
    Column<double> scores;
    scores.push_back(10.0);
    scores.push_back(30.0);
    right.add_column("score", std::move(scores));

    auto result = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 3);

    const auto& score_entry = t.columns[t.index.at("score")];
    CHECK_FALSE(runtime::is_null(score_entry, 0));  // id=1 matched
    CHECK(runtime::is_null(score_entry, 1));        // id=2 unmatched → null
    CHECK_FALSE(runtime::is_null(score_entry, 2));  // id=3 matched
}

TEST_CASE("null: validity bitmap propagates through filter", "[null]") {
    runtime::Table t;
    t.add_column("id", Column<std::int64_t>{1, 2, 3});
    t.add_column("val", Column<double>{1.0, 2.0, 3.0}, std::vector<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    // Filter: keep id >= 1 (all rows) — bitmap must survive unchanged.
    auto ir = require_ir("t[filter id >= 1];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto& val_entry = result->columns[result->index.at("val")];
    CHECK_FALSE(runtime::is_null(val_entry, 0));  // row 0 was valid
    CHECK(runtime::is_null(val_entry, 1));        // row 1 was null → still null
    CHECK_FALSE(runtime::is_null(val_entry, 2));  // row 2 was valid
}

TEST_CASE("null: validity bitmap propagates through project", "[null]") {
    runtime::Table t;
    t.add_column("id", Column<std::int64_t>{1, 2});
    t.add_column("val", Column<double>{1.0, 2.0}, std::vector<bool>{true, false});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[select { val }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& val_entry = result->columns[result->index.at("val")];
    CHECK_FALSE(runtime::is_null(val_entry, 0));
    CHECK(runtime::is_null(val_entry, 1));
}

TEST_CASE("null: print displays null for null rows", "[null]") {
    runtime::Table left, right;
    Column<std::int64_t> lid;
    lid.push_back(1);
    lid.push_back(2);
    left.add_column("id", std::move(lid));
    Column<std::int64_t> rid;
    rid.push_back(1);
    right.add_column("id", std::move(rid));
    Column<double> vals;
    vals.push_back(99.0);
    right.add_column("val", std::move(vals));

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    std::ostringstream oss;
    ops::print(*joined, oss);
    const std::string out = oss.str();
    CHECK(out.find("null") != std::string::npos);
    CHECK(out.find("99") != std::string::npos);
}

TEST_CASE("null agg: grouped sum/mean/min/max ignore nulls", "[null][agg]") {
    runtime::Table table;
    table.add_column("g", Column<std::int64_t>{1, 1, 2, 2});
    table.add_column("x", Column<double>{10.0, 0.0, 0.0, 0.0},
                     std::vector<bool>{true, false, false, false});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir(
        "t[select { g, sx = sum(x), mx = mean(x), mn = min(x), xx = max(x), n = count() }, by g, "
        "order g];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& g = std::get<Column<std::int64_t>>(*result->columns[result->index.at("g")].column);
    REQUIRE(g[0] == 1);
    REQUIRE(g[1] == 2);

    const auto& sx_entry = result->columns[result->index.at("sx")];
    const auto& mx_entry = result->columns[result->index.at("mx")];
    const auto& mn_entry = result->columns[result->index.at("mn")];
    const auto& xx_entry = result->columns[result->index.at("xx")];
    const auto& n_entry = result->columns[result->index.at("n")];

    const auto& sx = std::get<Column<double>>(*sx_entry.column);
    const auto& mx = std::get<Column<double>>(*mx_entry.column);
    const auto& mn = std::get<Column<double>>(*mn_entry.column);
    const auto& xx = std::get<Column<double>>(*xx_entry.column);
    const auto& n = std::get<Column<std::int64_t>>(*n_entry.column);

    CHECK(sx[0] == Catch::Approx(10.0));
    CHECK(mx[0] == Catch::Approx(10.0));
    CHECK(mn[0] == Catch::Approx(10.0));
    CHECK(xx[0] == Catch::Approx(10.0));
    CHECK_FALSE(runtime::is_null(sx_entry, 0));
    CHECK_FALSE(runtime::is_null(mx_entry, 0));
    CHECK_FALSE(runtime::is_null(mn_entry, 0));
    CHECK_FALSE(runtime::is_null(xx_entry, 0));

    CHECK(runtime::is_null(sx_entry, 1));
    CHECK(runtime::is_null(mx_entry, 1));
    CHECK(runtime::is_null(mn_entry, 1));
    CHECK(runtime::is_null(xx_entry, 1));

    CHECK(n[0] == 2);
    CHECK(n[1] == 2);
}

TEST_CASE("null agg: global all-null aggregate returns null", "[null][agg]") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{0, 0, 0}, std::vector<bool>{false, false, false});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir(
        "t[select { sx = sum(x), mx = mean(x), mn = min(x), xx = max(x), n = count() }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto& sx_entry = result->columns[result->index.at("sx")];
    const auto& mx_entry = result->columns[result->index.at("mx")];
    const auto& mn_entry = result->columns[result->index.at("mn")];
    const auto& xx_entry = result->columns[result->index.at("xx")];
    const auto& n_entry = result->columns[result->index.at("n")];
    const auto& n = std::get<Column<std::int64_t>>(*n_entry.column);

    CHECK(runtime::is_null(sx_entry, 0));
    CHECK(runtime::is_null(mx_entry, 0));
    CHECK(runtime::is_null(mn_entry, 0));
    CHECK(runtime::is_null(xx_entry, 0));
    CHECK(n[0] == 3);
}

TEST_CASE("print: doubles use mixed precision formatting", "[print]") {
    runtime::Table t;
    t.add_column("a", Column<double>{0.1 + 0.2});
    t.add_column("b", Column<double>{1.3100000000000165});
    t.add_column("c", Column<double>{1.0441991347356203e13});

    std::ostringstream oss;
    ops::print(t, oss);
    const std::string out = oss.str();

    CHECK(out.find("0.3") != std::string::npos);
    CHECK(out.find("1.31") != std::string::npos);
    CHECK(out.find("1.044199e13") != std::string::npos);
}

TEST_CASE("null: left join fan-out (duplicate right keys)", "[null][join]") {
    // left: id = {1, 2, 3}
    // right: id = {1, 1, 3}  — id=1 appears twice, id=2 missing
    // expected output: 4 rows (id=1 ×2, id=2 ×1 null, id=3 ×1)
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 1, 3});
    right.add_column("val", Column<double>{10.0, 11.0, 30.0});

    auto result = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(result.has_value());
    auto& t = *result;
    REQUIRE(t.rows() == 4);

    // Rows: (1,10), (1,11), (2,null), (3,30)
    const auto& id_col = std::get<Column<std::int64_t>>(*t.columns[t.index.at("id")].column);
    const auto& val_entry = t.columns[t.index.at("val")];
    const auto& val_col = std::get<Column<double>>(*val_entry.column);

    CHECK(id_col[0] == 1);
    CHECK_FALSE(runtime::is_null(val_entry, 0));
    CHECK(val_col[0] == 10.0);
    CHECK(id_col[1] == 1);
    CHECK_FALSE(runtime::is_null(val_entry, 1));
    CHECK(val_col[1] == 11.0);
    CHECK(id_col[2] == 2);
    CHECK(runtime::is_null(val_entry, 2));
    CHECK(id_col[3] == 3);
    CHECK_FALSE(runtime::is_null(val_entry, 3));
    CHECK(val_col[3] == 30.0);
}

// ─── Phase 2: Nullable Expressions + 3VL Filter tests ────────────────────────

TEST_CASE("null 3vl: arithmetic propagates null", "[null][3vl]") {
    // price column with a validity bitmap: rows 0,2 valid; row 1 null
    runtime::Table table;
    Column<std::int64_t> price_col{10, 0, 30};
    table.add_column("price", std::move(price_col));
    table.columns[table.index.at("price")].validity = std::vector<bool>{true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // select { p2 = price * 2 }
    auto ir = require_ir("t[select { p2 = price * 2 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& p2_entry = result->columns[result->index.at("p2")];
    const auto& p2 = std::get<Column<std::int64_t>>(*p2_entry.column);
    REQUIRE(p2.size() == 3);
    CHECK(p2[0] == 20);
    CHECK(p2[2] == 60);
    // row 1 should be null
    REQUIRE(p2_entry.validity.has_value());
    CHECK((*p2_entry.validity)[0] == true);
    CHECK((*p2_entry.validity)[1] == false);
    CHECK((*p2_entry.validity)[2] == true);
}

TEST_CASE("null 3vl: comparison with null drops row", "[null][3vl]") {
    // sector column where row 1 is null (left join unmatched)
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());
    REQUIRE(joined->rows() == 3);

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { sector == "Tech" } — null rows should be dropped
    auto ir = require_ir("j[filter { sector == \"Tech\" }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    CHECK(result->rows() == 1);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 1);
}

TEST_CASE("null 3vl: IS NULL predicate keeps null rows", "[null][3vl]") {
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { sector is null } — only id=2 (unmatched) should remain
    auto ir = require_ir("j[filter { sector is null }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    CHECK(result->rows() == 1);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 2);
}

TEST_CASE("null 3vl: IS NOT NULL predicate keeps valid rows", "[null][3vl]") {
    runtime::Table left, right;
    left.add_column("id", Column<std::int64_t>{1, 2, 3});

    right.add_column("id", Column<std::int64_t>{1, 3});
    right.add_column("sector", Column<std::string>{"Tech", "Finance"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { sector is not null } — id=1 and id=3 should remain
    auto ir = require_ir("j[filter { sector is not null }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    CHECK(result->rows() == 2);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 1);
    CHECK(id_col[1] == 3);
}

TEST_CASE("null 3vl: OR short-circuit with null — true OR null = true", "[null][3vl]") {
    runtime::Table left, right;
    // id=1: price=10, sector="Tech"  (both known)
    // id=2: price=50, sector=null    (price>0 is TRUE, sector is null → true OR null = true)
    // id=3: price=-1, sector=null    (price>0 is FALSE, sector is null → false OR null = null →
    // drop)
    left.add_column("id", Column<std::int64_t>{1, 2, 3});
    left.add_column("price", Column<std::int64_t>{10, 50, -1});

    right.add_column("id", Column<std::int64_t>{1});
    right.add_column("sector", Column<std::string>{"Tech"});

    auto joined = runtime::join_tables(left, right, ir::JoinKind::Left, {"id"});
    REQUIRE(joined.has_value());
    REQUIRE(joined->rows() == 3);

    runtime::TableRegistry registry;
    registry.emplace("j", *joined);

    // filter { price > 0 || sector is not null }
    auto ir = require_ir("j[filter { price > 0 || sector is not null }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    // id=1: price>0=true, is not null=true  → true OR true = true → keep
    // id=2: price>0=true, is not null=false → true OR null(=false,invalid) = true (valid) → keep
    // id=3: price>0=false, is not null=false → false OR null = null → drop
    CHECK(result->rows() == 2);
    const auto& id_col =
        std::get<Column<std::int64_t>>(*result->columns[result->index.at("id")].column);
    CHECK(id_col[0] == 1);
    CHECK(id_col[1] == 2);
}

TEST_CASE("Interpret rename: basic column renaming") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[rename p = price];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);
    CHECK(result->columns[0].name == "p");
    CHECK(result->columns[1].name == "symbol");
    const auto& p = std::get<Column<std::int64_t>>(*result->columns[0].column);
    CHECK(p[0] == 10);
    CHECK(p[1] == 20);
    CHECK(p[2] == 30);
}

TEST_CASE("Interpret rename: keeps non-renamed columns") {
    runtime::Table table;
    table.add_column("a", Column<std::int64_t>{1, 2});
    table.add_column("b", Column<std::int64_t>{3, 4});
    table.add_column("c", Column<std::int64_t>{5, 6});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[rename x = b];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 3);
    CHECK(result->columns[0].name == "a");
    CHECK(result->columns[1].name == "x");
    CHECK(result->columns[2].name == "c");
}

TEST_CASE("Interpret rename: multiple renames") {
    runtime::Table table;
    table.add_column("foo", Column<std::int64_t>{1, 2});
    table.add_column("bar", Column<std::int64_t>{3, 4});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[rename { x = foo, y = bar }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 2);
    CHECK(result->columns[0].name == "x");
    CHECK(result->columns[1].name == "y");
}

TEST_CASE("Interpret rename: combined with filter and select") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("qty", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 15, rename p = price, select p];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.size() == 1);
    CHECK(result->columns[0].name == "p");
    const auto& p = std::get<Column<std::int64_t>>(*result->columns[0].column);
    CHECK(p[0] == 20);
    CHECK(p[1] == 30);
}

TEST_CASE("Interpret rename: error on missing column") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[rename p = nonexistent];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("nonexistent") != std::string::npos);
}

// ── Statistical aggregation functions ────────────────────────────────────────

TEST_CASE("Interpret median aggregation (odd count)") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 30.0, 20.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 1);
    CHECK((*meds)[0] == Catch::Approx(20.0));
}

TEST_CASE("Interpret median aggregation (even count)") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 1);
    CHECK((*meds)[0] == Catch::Approx(25.0));
}

TEST_CASE("Interpret median aggregation grouped") {
    runtime::Table table;
    table.add_column("price", Column<double>{10.0, 20.0, 30.0, 5.0, 15.0});
    table.add_column("symbol", Column<std::string>{"A", "A", "A", "B", "B"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 2);
    // Group A: {10, 20, 30} → median = 20
    CHECK((*meds)[0] == Catch::Approx(20.0));
    // Group B: {5, 15} → median = 10
    CHECK((*meds)[1] == Catch::Approx(10.0));
}

TEST_CASE("Interpret median on integer column") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{1, 3, 2});
    table.add_column("symbol", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[select { med = median(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* med_col = result->find("med");
    REQUIRE(med_col != nullptr);
    const auto* meds = std::get_if<Column<double>>(med_col);
    REQUIRE(meds != nullptr);
    REQUIRE(meds->size() == 1);
    CHECK((*meds)[0] == Catch::Approx(2.0));
}

TEST_CASE("Interpret sample stddev aggregation") {
    runtime::Table table;
    // {0, 2, 4}: mean=2, M2=8, sample stddev = sqrt(8/2) = 2.0
    table.add_column("v", Column<double>{0.0, 2.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = std(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s_col = result->find("s");
    REQUIRE(s_col != nullptr);
    const auto* sv = std::get_if<Column<double>>(s_col);
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 1);
    CHECK((*sv)[0] == Catch::Approx(2.0));
}

TEST_CASE("Interpret sample stddev grouped") {
    runtime::Table table;
    table.add_column("v", Column<double>{2.0, 4.0, 6.0, 1.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "B", "B"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = std(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s_col = result->find("s");
    REQUIRE(s_col != nullptr);
    const auto* sv = std::get_if<Column<double>>(s_col);
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 2);
    // Group A: {2,4,6} → mean=4, M2=8, sample std = sqrt(8/2) = 2
    CHECK((*sv)[0] == Catch::Approx(2.0));
    // Group B: {1,3} → mean=2, M2=2, sample std = sqrt(2/1) = sqrt(2) ≈ 1.41421356
    CHECK((*sv)[1] == Catch::Approx(1.41421356).epsilon(1e-5));
}

TEST_CASE("Interpret stddev single element is null") {
    runtime::Table table;
    table.add_column("v", Column<double>{5.0});
    table.add_column("grp", Column<std::string>{"A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = std(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // Single element → count < 2 → result should be null
    const auto* s_entry = result->find_entry("s");
    REQUIRE(s_entry != nullptr);
    REQUIRE(s_entry->validity.has_value());
    CHECK((*s_entry->validity)[0] == false);
}

TEST_CASE("Interpret EWMA aggregation") {
    runtime::Table table;
    // alpha=0.5: ewma starts at 1.0, then 0.5*3+0.5*1=2.0, then 0.5*5+0.5*2=3.5
    table.add_column("v", Column<double>{1.0, 3.0, 5.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { e = ewma(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e_col = result->find("e");
    REQUIRE(e_col != nullptr);
    const auto* ev = std::get_if<Column<double>>(e_col);
    REQUIRE(ev != nullptr);
    REQUIRE(ev->size() == 1);
    CHECK((*ev)[0] == Catch::Approx(3.5));
}

TEST_CASE("Interpret EWMA grouped") {
    runtime::Table table;
    // Group A: {1, 3}, alpha=0.5 → 1.0 then 0.5*3+0.5*1 = 2.0
    // Group B: {2, 4}, alpha=0.5 → 2.0 then 0.5*4+0.5*2 = 3.0
    table.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "B", "A", "B"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { e = ewma(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* e_col = result->find("e");
    REQUIRE(e_col != nullptr);
    const auto* ev = std::get_if<Column<double>>(e_col);
    REQUIRE(ev != nullptr);
    REQUIRE(ev->size() == 2);
    CHECK((*ev)[0] == Catch::Approx(2.0));
    CHECK((*ev)[1] == Catch::Approx(3.0));
}

// ─── quantile ─────────────────────────────────────────────────────────────────

TEST_CASE("Interpret quantile aggregation (p=0.5 == median)") {
    runtime::Table table;
    // {10, 20, 30} sorted → p=0.5 → idx=1.0 → 20.0
    table.add_column("v", Column<double>{10.0, 30.0, 20.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { q = quantile(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* q_col = result->find("q");
    REQUIRE(q_col != nullptr);
    const auto* qv = std::get_if<Column<double>>(q_col);
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 1);
    CHECK((*qv)[0] == Catch::Approx(20.0));
}

TEST_CASE("Interpret quantile aggregation (p=0.25 and p=0.75)") {
    runtime::Table table;
    // {1, 2, 3, 4} sorted
    // p=0.25 → idx=0.75 → 1 + 0.75*(2-1) = 1.75
    // p=0.75 → idx=2.25 → 3 + 0.25*(4-3) = 3.25
    table.add_column("v", Column<double>{1.0, 3.0, 2.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir_lo = require_ir("t[select { q = quantile(v, 0.25) }, by grp];");
    auto result_lo = runtime::interpret(*ir_lo, registry);
    REQUIRE(result_lo.has_value());
    const auto* qv_lo = std::get_if<Column<double>>(result_lo->find("q"));
    REQUIRE(qv_lo != nullptr);
    CHECK((*qv_lo)[0] == Catch::Approx(1.75));

    auto ir_hi = require_ir("t[select { q = quantile(v, 0.75) }, by grp];");
    auto result_hi = runtime::interpret(*ir_hi, registry);
    REQUIRE(result_hi.has_value());
    const auto* qv_hi = std::get_if<Column<double>>(result_hi->find("q"));
    REQUIRE(qv_hi != nullptr);
    CHECK((*qv_hi)[0] == Catch::Approx(3.25));
}

TEST_CASE("Interpret quantile aggregation (p=0 and p=1)") {
    runtime::Table table;
    table.add_column("v", Column<double>{5.0, 1.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir_min = require_ir("t[select { q = quantile(v, 0.0) }, by grp];");
    auto result_min = runtime::interpret(*ir_min, registry);
    REQUIRE(result_min.has_value());
    const auto* qv_min = std::get_if<Column<double>>(result_min->find("q"));
    REQUIRE(qv_min != nullptr);
    CHECK((*qv_min)[0] == Catch::Approx(1.0));

    auto ir_max = require_ir("t[select { q = quantile(v, 1.0) }, by grp];");
    auto result_max = runtime::interpret(*ir_max, registry);
    REQUIRE(result_max.has_value());
    const auto* qv_max = std::get_if<Column<double>>(result_max->find("q"));
    REQUIRE(qv_max != nullptr);
    CHECK((*qv_max)[0] == Catch::Approx(5.0));
}

TEST_CASE("Interpret quantile grouped") {
    runtime::Table table;
    // Group A: {1,3,5} → p=0.5 → 3.0
    // Group B: {2,4}   → p=0.5 → idx=0.5 → 2+0.5*(4-2)=3.0
    table.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    table.add_column("grp", Column<std::string>{"A", "B", "A", "B", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { q = quantile(v, 0.5) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* qv = std::get_if<Column<double>>(result->find("q"));
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 2);
    CHECK((*qv)[0] == Catch::Approx(3.0));
    CHECK((*qv)[1] == Catch::Approx(3.0));
}

// ─── skew ──────────────────────────────────────────────────────────────────────

TEST_CASE("Interpret skew aggregation (symmetric → 0)") {
    runtime::Table table;
    // {1, 2, 3} — symmetric around 2, skew = 0
    table.add_column("v", Column<double>{1.0, 2.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = skew(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* s_col = result->find("s");
    REQUIRE(s_col != nullptr);
    const auto* sv = std::get_if<Column<double>>(s_col);
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 1);
    CHECK((*sv)[0] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("Interpret skew aggregation (right-skewed)") {
    runtime::Table table;
    // {1, 1, 1, 4} — right skew
    // mean=1.75, deviations: -0.75,-0.75,-0.75,2.25
    // m2=0.75^2*3+2.25^2 = 1.6875+5.0625=6.75
    // m3=(-0.75)^3*3+(2.25)^3 = -1.265625+11.390625=10.125
    // n=4, skew = (4*sqrt(3)/2) * (10.125 / 6.75^1.5)
    // 6.75^1.5 = sqrt(6.75^3) = sqrt(307.546875) ≈ 17.5371
    // skew = (4*1.73205/2) * (10.125/17.5371) ≈ 3.4641 * 0.5773 ≈ 2.0
    table.add_column("v", Column<double>{1.0, 1.0, 1.0, 4.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = skew(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* sv = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 1);
    // Expected value matches pandas: pandas.Series([1,1,1,4]).skew() ≈ 2.0
    CHECK((*sv)[0] == Catch::Approx(2.0).epsilon(1e-5));
}

TEST_CASE("Interpret skew too few values is null") {
    runtime::Table table;
    // n=2 → skew is undefined (n<3) → null
    table.add_column("v", Column<double>{1.0, 2.0});
    table.add_column("grp", Column<std::string>{"A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { s = skew(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("s");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0] == false);
}

// ─── kurtosis ──────────────────────────────────────────────────────────────────

TEST_CASE("Interpret kurtosis aggregation (normal-like → ~0 excess)") {
    runtime::Table table;
    // {1,2,3,4,5} — excess kurtosis (pandas): ≈ -1.3
    table.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { k = kurtosis(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* k_col = result->find("k");
    REQUIRE(k_col != nullptr);
    const auto* kv = std::get_if<Column<double>>(k_col);
    REQUIRE(kv != nullptr);
    REQUIRE(kv->size() == 1);
    // pandas.Series([1,2,3,4,5]).kurtosis() == -1.2
    CHECK((*kv)[0] == Catch::Approx(-1.2).epsilon(1e-5));
}

TEST_CASE("Interpret kurtosis aggregation (leptokurtic)") {
    runtime::Table table;
    // {0,0,0,0,10} — heavy tail, positive excess kurtosis
    table.add_column("v", Column<double>{0.0, 0.0, 0.0, 0.0, 10.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { k = kurtosis(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* kv = std::get_if<Column<double>>(result->find("k"));
    REQUIRE(kv != nullptr);
    // pandas.Series([0,0,0,0,10]).kurtosis() ≈ 5.0
    CHECK((*kv)[0] == Catch::Approx(5.0).epsilon(1e-4));
}

TEST_CASE("Interpret kurtosis too few values is null") {
    runtime::Table table;
    // n=3 → kurtosis undefined (n<4) → null
    table.add_column("v", Column<double>{1.0, 2.0, 3.0});
    table.add_column("grp", Column<std::string>{"A", "A", "A"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[select { k = kurtosis(v) }, by grp];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("k");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0] == false);
}

// ─── rolling_quantile / rolling_skew / rolling_kurtosis ───────────────────────

TEST_CASE("rolling_quantile with 1ns window") {
    runtime::Table table;
    // {10, 20, 30} — 1ns window = each row sees only itself
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    // 1ns window includes boundary: row 1 sees {10,20}, row 2 sees {20,30}
    // p=0.5: row0→10, row1→15, row2→25
    auto ir = require_ir("data[window 1ns, update { q = rolling_quantile(val, 0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* q_col = result->find("q");
    REQUIRE(q_col != nullptr);
    const auto* qv = std::get_if<Column<double>>(q_col);
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 3);
    CHECK((*qv)[0] == Catch::Approx(10.0));
    CHECK((*qv)[1] == Catch::Approx(15.0));
    CHECK((*qv)[2] == Catch::Approx(25.0));
}

TEST_CASE("rolling_quantile with 2ns window") {
    runtime::Table table;
    // {10, 20, 30, 40} with 2ns window (threshold = t - 2, lo advances when ts < threshold)
    // row 0 (t=0): window {10}        → p=0.25 → 10.0
    // row 1 (t=1): window {10,20}     → idx=0.25 → 12.5
    // row 2 (t=2): window {10,20,30}  → idx=0.5  → 15.0
    // row 3 (t=3): window {20,30,40}  → idx=0.5  → 25.0
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3)});
    table.add_column("val", Column<double>{10.0, 20.0, 30.0, 40.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 2ns, update { q = rolling_quantile(val, 0.25) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* qv = std::get_if<Column<double>>(result->find("q"));
    REQUIRE(qv != nullptr);
    REQUIRE(qv->size() == 4);
    CHECK((*qv)[0] == Catch::Approx(10.0));
    CHECK((*qv)[1] == Catch::Approx(12.5));
    CHECK((*qv)[2] == Catch::Approx(15.0));
    CHECK((*qv)[3] == Catch::Approx(25.0));
}

TEST_CASE("rolling_skew with 1ns window (single element → 0)") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { s = rolling_skew(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* sv = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 3);
    // n<3 → all zeros
    CHECK((*sv)[0] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*sv)[1] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*sv)[2] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("rolling_skew with wide window (symmetric → 0)") {
    runtime::Table table;
    // {1,2,3} fully in window by row 2 — symmetric → skew=0
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 10ns, update { s = rolling_skew(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* sv = std::get_if<Column<double>>(result->find("s"));
    REQUIRE(sv != nullptr);
    REQUIRE(sv->size() == 3);
    // row 2: {1,2,3} symmetric → skew=0
    CHECK((*sv)[2] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("rolling_kurtosis with 1ns window (n<4 → 0)") {
    runtime::Table table;
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 1ns, update { k = rolling_kurtosis(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* kv = std::get_if<Column<double>>(result->find("k"));
    REQUIRE(kv != nullptr);
    REQUIRE(kv->size() == 3);
    CHECK((*kv)[0] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*kv)[1] == Catch::Approx(0.0).margin(1e-10));
    CHECK((*kv)[2] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("rolling_kurtosis wide window") {
    runtime::Table table;
    // {1,2,3,4,5}: excess kurtosis ≈ -1.3 (pandas)
    table.add_column("ts", Column<Timestamp>{ts_from_nanos(0), ts_from_nanos(1), ts_from_nanos(2),
                                             ts_from_nanos(3), ts_from_nanos(4)});
    table.add_column("val", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    table.time_index = "ts";

    runtime::TableRegistry registry;
    registry.emplace("data", table);

    auto ir = require_ir("data[window 100ns, update { k = rolling_kurtosis(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* kv = std::get_if<Column<double>>(result->find("k"));
    REQUIRE(kv != nullptr);
    REQUIRE(kv->size() == 5);
    // row 4: {1,2,3,4,5} → excess kurtosis = -1.2
    CHECK((*kv)[4] == Catch::Approx(-1.2).epsilon(1e-5));
}

// ─── Vectorized RNG ───────────────────────────────────────────────────────────

TEST_CASE("rand_uniform generates correct number of rows in bounds") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(0.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("u");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] >= 0.0);
        CHECK((*dv)[i] < 1.0);
    }
}

TEST_CASE("rand_uniform with integer literals") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(10.0, 20.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("u");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] >= 10.0);
        CHECK((*dv)[i] < 20.0);
    }
}

TEST_CASE("rand_normal generates correct column type and size") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_normal(0.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("n");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 10);
    // Values from N(0,1) are almost certainly in (-10, 10)
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] > -10.0);
        CHECK((*dv)[i] < 10.0);
    }
}

TEST_CASE("rand_student_t generates correct column type and size") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { t_val = rand_student_t(5.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("t_val");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
}

TEST_CASE("rand_gamma generates positive values") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { g = rand_gamma(2.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("g");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] > 0.0);
    }
}

TEST_CASE("rand_exponential generates positive values") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { e = rand_exponential(2.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("e");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 5);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] > 0.0);
    }
}

TEST_CASE("rand_bernoulli generates 0 or 1 integers") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(0.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("b");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 10);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK(((*iv)[i] == 0 || (*iv)[i] == 1));
    }
}

TEST_CASE("rand_bernoulli p=0 always yields 0") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("b");
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 0);
    }
}

TEST_CASE("rand_bernoulli p=1 always yields 1") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("b");
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 1);
    }
}

TEST_CASE("rand_poisson generates non-negative integers") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { p = rand_poisson(3.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("p");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] >= 0);
    }
}

TEST_CASE("rand_int generates integers in [lo, hi]") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { r = rand_int(1, 6) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("r");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 10);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] >= 1);
        CHECK((*iv)[i] <= 6);
    }
}

TEST_CASE("rand_int lo == hi always yields lo") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { r = rand_int(7, 7) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("r");
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 7);
    }
}

TEST_CASE("rand functions produce independent columns") {
    // Two rand_uniform calls in the same query should produce different columns.
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
                                               11, 12, 13, 14, 15, 16, 17, 18, 19, 20});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { a = rand_uniform(0.0, 1.0), b = rand_uniform(0.0, 1.0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* a_col = result->find("a");
    const auto* b_col = result->find("b");
    REQUIRE(a_col != nullptr);
    REQUIRE(b_col != nullptr);
    const auto* av = std::get_if<Column<double>>(a_col);
    const auto* bv = std::get_if<Column<double>>(b_col);
    REQUIRE(av != nullptr);
    REQUIRE(bv != nullptr);
    // Columns should differ (probability of all 20 values matching is astronomically small)
    bool any_differ = false;
    for (std::size_t i = 0; i < av->size(); ++i) {
        if ((*av)[i] != (*bv)[i]) {
            any_differ = true;
            break;
        }
    }
    CHECK(any_differ);
}

TEST_CASE("rand_uniform invalid arguments") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // low >= high should fail
    auto ir = require_ir("t[update { u = rand_uniform(1.0, 0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("rand_normal invalid stddev") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_normal(0.0, 0.0) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

TEST_CASE("rand_bernoulli invalid p") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { b = rand_bernoulli(1.5) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

// ─── seed_rng / reseed_rng ────────────────────────────────────────────────────

// rand_uniform uses zorro::Rng, so seeding requires reseed().
TEST_CASE("reseed produces identical rand_uniform sequence") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(32)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(0.0, 1.0) }];");

    runtime::reseed(0xDEADBEEF);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<double>>(*r1->find("u"));

    runtime::reseed(0xDEADBEEF);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<double>>(*r2->find("u"));

    REQUIRE(c1.size() == c2.size());
    for (std::size_t i = 0; i < c1.size(); ++i) {
        CHECK(c1[i] == c2[i]);
    }
}

TEST_CASE("reseed with different seeds produces different rand_uniform sequences") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(32)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { u = rand_uniform(0.0, 1.0) }];");

    runtime::reseed(1);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<double>>(*r1->find("u"));

    runtime::reseed(2);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<double>>(*r2->find("u"));

    bool any_different = false;
    for (std::size_t i = 0; i < c1.size(); ++i) {
        if (c1[i] != c2[i]) {
            any_different = true;
            break;
        }
    }
    CHECK(any_different);
}

TEST_CASE("reseed produces identical rand_normal sequence") {
    runtime::Table table;
    // 33 rows: exercises the 8-wide main loop (4 iterations = 32 rows) and
    // the 1-element scalar tail.
    table.add_column("x", Column<std::int64_t>(std::vector<std::int64_t>(33)));
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { n = rand_normal(0.0, 1.0) }];");

    runtime::reseed(0xCAFEBABE);
    auto r1 = runtime::interpret(*ir, registry);
    REQUIRE(r1.has_value());
    const auto& c1 = std::get<Column<double>>(*r1->find("n"));

    runtime::reseed(0xCAFEBABE);
    auto r2 = runtime::interpret(*ir, registry);
    REQUIRE(r2.has_value());
    const auto& c2 = std::get<Column<double>>(*r2->find("n"));

    REQUIRE(c1.size() == c2.size());
    for (std::size_t i = 0; i < c1.size(); ++i) {
        CHECK(c1[i] == c2[i]);
    }
}

// ─── rep ─────────────────────────────────────────────────────────────────────

TEST_CASE("rep scalar int fills table rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = rep(42) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 42);
    }
}

TEST_CASE("rep scalar float fills table rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = rep(3.14) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* dv = std::get_if<Column<double>>(col);
    REQUIRE(dv != nullptr);
    REQUIRE(dv->size() == 3);
    for (std::size_t i = 0; i < dv->size(); ++i) {
        CHECK((*dv)[i] == Catch::Approx(3.14));
    }
}

TEST_CASE("rep bool true fills boolean mask column") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { mask = rep(true) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("mask");
    REQUIRE(col != nullptr);
    const auto* bv = std::get_if<Column<bool>>(col);
    REQUIRE(bv != nullptr);
    REQUIRE(bv->size() == 4);
    for (std::size_t i = 0; i < bv->size(); ++i) {
        CHECK((*bv)[i] == true);
    }
}

TEST_CASE("rep bool false fills boolean mask column with false") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { mask = rep(false) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("mask");
    REQUIRE(col != nullptr);
    const auto* bv = std::get_if<Column<bool>>(col);
    REQUIRE(bv != nullptr);
    REQUIRE(bv->size() == 3);
    for (std::size_t i = 0; i < bv->size(); ++i) {
        CHECK((*bv)[i] == false);
    }
}

TEST_CASE("rep named arg times truncates to table rows") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // rep(7, times=100) — scalar, so output is all 7s (length_out defaults to rows)
    auto ir = require_ir("t[update { c = rep(7, times=100) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 7);
    }
}

TEST_CASE("rep named arg length_out overrides row count") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // length_out=3 produces only 3 elements — but update requires same row count,
    // so length_out must equal rows; here we verify via select
    auto ir = require_ir("t[update { c = rep(9, length_out=5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 5);
    for (std::size_t i = 0; i < iv->size(); ++i) {
        CHECK((*iv)[i] == 9);
    }
}

TEST_CASE("rep column reference with each repeats each element") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{10, 20, 30, 40, 50, 60});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // rep(x, each=2) — each element twice, cycling to 6 rows
    // pattern: 10,10,20,20,30,30 (exactly 6 rows)
    auto ir = require_ir("t[update { c = rep(x, each=2) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* col = result->find("c");
    REQUIRE(col != nullptr);
    const auto* iv = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(iv != nullptr);
    REQUIRE(iv->size() == 6);
    CHECK((*iv)[0] == 10);
    CHECK((*iv)[1] == 10);
    CHECK((*iv)[2] == 20);
    CHECK((*iv)[3] == 20);
    CHECK((*iv)[4] == 30);
    CHECK((*iv)[5] == 30);
}

TEST_CASE("rep unknown named argument returns error") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = rep(1, foo=3) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

// ─── fill_null / fill_forward / fill_backward tests ──────────────────────────

TEST_CASE("fill_null replaces null cells with constant (Int)", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{10, 0, 30, 0, 50});
    t.columns[t.index.at("val")].validity = std::vector<bool>{true, false, true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val, 0) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("v2");
    REQUIRE(entry != nullptr);
    // No validity bitmap — all rows are now valid.
    CHECK_FALSE(entry->validity.has_value());
    const auto& v2 = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(v2[0] == 10);
    CHECK(v2[1] == 0);  // was null, filled with 0
    CHECK(v2[2] == 30);
    CHECK(v2[3] == 0);  // was null, filled with 0
    CHECK(v2[4] == 50);
}

TEST_CASE("fill_null replaces null cells with constant (Float)", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<double>{1.0, 0.0, 3.0});
    t.columns[t.index.at("val")].validity = std::vector<bool>{true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val, 99.5) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("v2");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& v2 = std::get<Column<double>>(*entry->column);
    CHECK(v2[0] == Catch::Approx(1.0));
    CHECK(v2[1] == Catch::Approx(99.5));  // was null, filled with 99.5
    CHECK(v2[2] == Catch::Approx(3.0));
}

TEST_CASE("fill_null on column with no nulls is a no-op", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    // No validity bitmap — no nulls.

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val, 99) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("v2");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& v2 = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(v2[0] == 1);
    CHECK(v2[1] == 2);
    CHECK(v2[2] == 3);
}

TEST_CASE("fill_forward carries last valid value forward (LOCF)", "[null][fill]") {
    runtime::Table t;
    //                      valid null  valid null  null  valid
    t.add_column("val", Column<std::int64_t>{10, 0, 20, 0, 0, 30});
    t.columns[t.index.at("val")].validity =
        std::vector<bool>{true, false, true, false, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("fwd");
    REQUIRE(entry != nullptr);
    // No leading nulls — all rows should be valid after fill.
    CHECK_FALSE(entry->validity.has_value());
    const auto& fwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(fwd[0] == 10);
    CHECK(fwd[1] == 10);  // carried from row 0
    CHECK(fwd[2] == 20);
    CHECK(fwd[3] == 20);  // carried from row 2
    CHECK(fwd[4] == 20);  // carried from row 2
    CHECK(fwd[5] == 30);
}

TEST_CASE("fill_forward leaves leading nulls as null", "[null][fill]") {
    runtime::Table t;
    //                      null  null  valid null
    t.add_column("val", Column<std::int64_t>{0, 0, 5, 0});
    t.columns[t.index.at("val")].validity = std::vector<bool>{false, false, true, false};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("fwd");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK(runtime::is_null(*entry, 0));  // leading null — unfillable
    CHECK(runtime::is_null(*entry, 1));  // leading null — unfillable
    CHECK_FALSE(runtime::is_null(*entry, 2));
    CHECK_FALSE(runtime::is_null(*entry, 3));  // filled from row 2
    const auto& fwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(fwd[2] == 5);
    CHECK(fwd[3] == 5);
}

TEST_CASE("fill_forward on column with no nulls is a no-op", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("fwd");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& fwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(fwd[0] == 1);
    CHECK(fwd[1] == 2);
    CHECK(fwd[2] == 3);
}

TEST_CASE("fill_backward carries next valid value backward (NOCB)", "[null][fill]") {
    runtime::Table t;
    //                      valid null  null  valid null  valid
    t.add_column("val", Column<std::int64_t>{10, 0, 0, 20, 0, 30});
    t.columns[t.index.at("val")].validity =
        std::vector<bool>{true, false, false, true, false, true};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bwd = fill_backward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bwd");
    REQUIRE(entry != nullptr);
    // No trailing nulls — all rows should be valid after fill.
    CHECK_FALSE(entry->validity.has_value());
    const auto& bwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(bwd[0] == 10);
    CHECK(bwd[1] == 20);  // carried from row 3
    CHECK(bwd[2] == 20);  // carried from row 3
    CHECK(bwd[3] == 20);
    CHECK(bwd[4] == 30);  // carried from row 5
    CHECK(bwd[5] == 30);
}

TEST_CASE("fill_backward leaves trailing nulls as null", "[null][fill]") {
    runtime::Table t;
    //                      null  valid null  null
    t.add_column("val", Column<std::int64_t>{0, 5, 0, 0});
    t.columns[t.index.at("val")].validity = std::vector<bool>{false, true, false, false};

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bwd = fill_backward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bwd");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK_FALSE(runtime::is_null(*entry, 0));  // filled from row 1
    CHECK_FALSE(runtime::is_null(*entry, 1));
    CHECK(runtime::is_null(*entry, 2));  // trailing null — unfillable
    CHECK(runtime::is_null(*entry, 3));  // trailing null — unfillable
    const auto& bwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(bwd[0] == 5);
    CHECK(bwd[1] == 5);
}

TEST_CASE("fill_backward on column with no nulls is a no-op", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { bwd = fill_backward(val) }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* entry = result->find_entry("bwd");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto& bwd = std::get<Column<std::int64_t>>(*entry->column);
    CHECK(bwd[0] == 1);
    CHECK(bwd[1] == 2);
    CHECK(bwd[2] == 3);
}

TEST_CASE("fill_null wrong argument count returns error", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { v2 = fill_null(val) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
    CHECK(result.error().find("fill_null") != std::string::npos);
}

TEST_CASE("fill_forward wrong argument count returns error", "[null][fill]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[update { fwd = fill_forward(val, val) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
    CHECK(result.error().find("fill_forward") != std::string::npos);
}

TEST_CASE("rep missing positional argument returns error") {
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry registry;
    registry.emplace("t", table);

    // rep() with no positional args should fail
    auto ir = require_ir("t[update { c = rep(times=3) }];");
    auto result = runtime::interpret(*ir, registry);
    CHECK_FALSE(result.has_value());
}

// ─── Table constructor from column vectors ────────────────────────────────────

TEST_CASE("Table constructor creates table from integer columns") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { price = [10, 20, 30], qty = [1, 2, 3] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* price = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[0] == 10);
    REQUIRE((*price)[1] == 20);
    REQUIRE((*price)[2] == 30);

    const auto* qty = std::get_if<Column<std::int64_t>>(result->find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE((*qty)[0] == 1);
    REQUIRE((*qty)[1] == 2);
    REQUIRE((*qty)[2] == 3);
}

TEST_CASE("Table constructor creates table from string and float columns") {
    runtime::TableRegistry registry;
    auto ir = require_ir(R"(Table { symbol = ["A", "B", "C"], price = [1.5, 2.5, 3.5] };)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* symbol = std::get_if<Column<std::string>>(result->find("symbol"));
    REQUIRE(symbol != nullptr);
    REQUIRE((*symbol)[0] == "A");
    REQUIRE((*symbol)[1] == "B");
    REQUIRE((*symbol)[2] == "C");

    const auto* price = std::get_if<Column<double>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[0] == Catch::Approx(1.5));
    REQUIRE((*price)[1] == Catch::Approx(2.5));
    REQUIRE((*price)[2] == Catch::Approx(3.5));
}

TEST_CASE("Table constructor creates table from bool column") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { active = [true, false, true] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* active = std::get_if<Column<bool>>(result->find("active"));
    REQUIRE(active != nullptr);
    REQUIRE((*active)[0] == true);
    REQUIRE((*active)[1] == false);
    REQUIRE((*active)[2] == true);
}

TEST_CASE("Table constructor creates empty table") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->columns.empty());
    REQUIRE(result->rows() == 0);
}

TEST_CASE("Table constructor can be filtered") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { x = [1, 2, 3, 4, 5] }[filter x > 3];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* x = std::get_if<Column<std::int64_t>>(result->find("x"));
    REQUIRE(x != nullptr);
    REQUIRE((*x)[0] == 4);
    REQUIRE((*x)[1] == 5);
}

TEST_CASE("Table constructor mismatched column lengths returns error") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { a = [1, 2, 3], b = [4, 5] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("TimeFrame from Table constructor via as_timeframe") {
    runtime::TableRegistry registry;
    auto ir = require_ir(
        R"(as_timeframe(Table { ts = [1000, 2000, 3000], price = [10, 20, 30] }, "ts");)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->time_index.has_value());
    REQUIRE(*result->time_index == "ts");
    REQUIRE(result->rows() == 3);
}

TEST_CASE("Table constructor with single column") {
    runtime::TableRegistry registry;
    auto ir = require_ir("Table { vals = [42] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* vals = std::get_if<Column<std::int64_t>>(result->find("vals"));
    REQUIRE(vals != nullptr);
    REQUIRE((*vals)[0] == 42);
}

TEST_CASE("Table constructor with column from existing table (single-column select)") {
    // `prices[select { price }]` produces a single-column Table; Table { p = ... } picks it up.
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> price_col;
    price_col.push_back(10);
    price_col.push_back(20);
    price_col.push_back(30);
    src.add_column("price", price_col);
    registry["prices"] = std::move(src);

    auto ir = require_ir("Table { p = prices[select { price }] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* p = std::get_if<Column<std::int64_t>>(result->find("p"));
    REQUIRE(p != nullptr);
    REQUIRE((*p)[0] == 10);
    REQUIRE((*p)[1] == 20);
    REQUIRE((*p)[2] == 30);
}

TEST_CASE("Table constructor with named column from multi-column expression") {
    // The expression returns a multi-column Table; we extract the column matching the def name.
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> price_col;
    price_col.push_back(1);
    price_col.push_back(2);
    Column<std::int64_t> qty_col;
    qty_col.push_back(10);
    qty_col.push_back(20);
    src.add_column("price", price_col);
    src.add_column("qty", qty_col);
    registry["trades"] = std::move(src);

    auto ir = require_ir("Table { price = trades, qty = trades };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* price = std::get_if<Column<std::int64_t>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[0] == 1);
    REQUIRE((*price)[1] == 2);

    const auto* qty = std::get_if<Column<std::int64_t>>(result->find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE((*qty)[0] == 10);
    REQUIRE((*qty)[1] == 20);
}

TEST_CASE("Table constructor mixing literal and expression columns") {
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<double> price_col;
    price_col.push_back(1.5);
    price_col.push_back(2.5);
    price_col.push_back(3.5);
    src.add_column("price", price_col);
    registry["data"] = std::move(src);

    auto ir = require_ir(R"(Table { label = ["a", "b", "c"], price = data[select { price }] };)");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);

    const auto* label = std::get_if<Column<std::string>>(result->find("label"));
    REQUIRE(label != nullptr);
    REQUIRE((*label)[0] == "a");

    const auto* price = std::get_if<Column<double>>(result->find("price"));
    REQUIRE(price != nullptr);
    REQUIRE((*price)[2] == Catch::Approx(3.5));
}

TEST_CASE("Table constructor expression column length mismatch returns error") {
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> col;
    col.push_back(1);
    col.push_back(2);
    src.add_column("x", col);
    registry["data"] = std::move(src);

    // Literal has 3 elements; expression column produces 2 rows → length mismatch.
    auto ir = require_ir("Table { a = [1, 2, 3], b = data[select { x }] };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Table constructor expression column not found in multi-column result returns error") {
    runtime::TableRegistry registry;
    runtime::Table src;
    Column<std::int64_t> col;
    col.push_back(1);
    src.add_column("x", col);
    registry["data"] = std::move(src);

    // "missing" does not exist in data (which has only "x"); multi-column, no name match.
    // data has one column "x" but the def name is "missing" — single-col path renames it,
    // so we test with two columns to exercise the error path.
    runtime::Table two_col;
    Column<std::int64_t> a_col, b_col;
    a_col.push_back(1);
    b_col.push_back(2);
    two_col.add_column("a", a_col);
    two_col.add_column("b", b_col);
    registry["two"] = std::move(two_col);

    auto ir = require_ir("Table { missing = two };");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
}

// ─── Melt (wide → long) ───────────────────────────────────────────────────────

TEST_CASE("melt: basic wide-to-long unpivot", "[melt]") {
    // wide: symbol | open | close
    //       AAPL     100    110
    //       GOOG     200    210
    // long: symbol | variable | value
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("open", Column<std::int64_t>{100, 200});
    wide.add_column("close", Column<std::int64_t>{110, 210});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // 2 rows × 2 measures = 4 output rows
    REQUIRE(result->rows() == 4);

    const auto* sym = result->find("symbol");
    REQUIRE(sym != nullptr);
    const auto* var = result->find("variable");
    REQUIRE(var != nullptr);
    const auto* val = result->find("value");
    REQUIRE(val != nullptr);

    const auto& sym_col = std::get<Column<std::string>>(*sym);
    // variable column is now Categorical (n_measures distinct values)
    const auto& var_col = std::get<Column<Categorical>>(*var);
    const auto& val_col = std::get<Column<std::int64_t>>(*val);

    // Row order: for each input row, iterate measures in column order.
    REQUIRE(sym_col[0] == "AAPL");
    REQUIRE(var_col[0] == "open");
    REQUIRE(val_col[0] == 100);

    REQUIRE(sym_col[1] == "AAPL");
    REQUIRE(var_col[1] == "close");
    REQUIRE(val_col[1] == 110);

    REQUIRE(sym_col[2] == "GOOG");
    REQUIRE(var_col[2] == "open");
    REQUIRE(val_col[2] == 200);

    REQUIRE(sym_col[3] == "GOOG");
    REQUIRE(var_col[3] == "close");
    REQUIRE(val_col[3] == 210);
}

TEST_CASE("melt: select restricts measure columns", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("open", Column<std::int64_t>{100, 200});
    wide.add_column("close", Column<std::int64_t>{110, 210});
    wide.add_column("volume", Column<std::int64_t>{1000, 2000});
    registry["wide"] = std::move(wide);

    // Only melt open and close, leave volume out.
    auto ir = require_ir("wide[melt symbol, select { open, close }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);  // 2 rows × 2 measures

    const auto* var = result->find("variable");
    REQUIRE(var != nullptr);
    // variable column is Categorical
    const auto& var_col = std::get<Column<Categorical>>(*var);
    REQUIRE(var_col[0] == "open");
    REQUIRE(var_col[1] == "close");
}

TEST_CASE("melt: multiple id columns", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("date", Column<std::int64_t>{1, 2});
    wide.add_column("open", Column<double>{100.0, 200.0});
    wide.add_column("close", Column<double>{110.0, 210.0});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt { symbol, date }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);

    REQUIRE(result->find("symbol") != nullptr);
    REQUIRE(result->find("date") != nullptr);
    REQUIRE(result->find("variable") != nullptr);
    REQUIRE(result->find("value") != nullptr);
}

TEST_CASE("melt: id column not found returns error", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("open", Column<std::int64_t>{100});
    registry["wide"] = std::move(wide);

    auto ir = require_ir("wide[melt no_such_col];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("id column not found") != std::string::npos);
}

TEST_CASE("melt: measure type mismatch returns error", "[melt]") {
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("id", Column<std::string>{"A"});
    wide.add_column("int_col", Column<std::int64_t>{1});
    wide.add_column("float_col", Column<double>{1.0});
    registry["wide"] = std::move(wide);

    // Mixing int and float measure columns is not allowed.
    auto ir = require_ir("wide[melt id, select { int_col, float_col }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("same type") != std::string::npos);
}

// ─── Dcast (long → wide) ─────────────────────────────────────────────────────

TEST_CASE("dcast: basic long-to-wide pivot", "[dcast]") {
    // long: variable | value
    //       open       100
    //       close      110
    // wide: open | close
    //       100    110
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("variable", Column<std::string>{"open", "close"});
    lng.add_column("value", Column<std::int64_t>{100, 110});
    registry["lng"] = std::move(lng);

    // by {} means no row keys (all rows form one group)
    auto ir = require_ir("lng[dcast variable, select value, by {}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);

    const auto* open_col = result->find("open");
    REQUIRE(open_col != nullptr);
    const auto* close_col = result->find("close");
    REQUIRE(close_col != nullptr);

    REQUIRE(std::get<Column<std::int64_t>>(*open_col)[0] == 100);
    REQUIRE(std::get<Column<std::int64_t>>(*close_col)[0] == 110);
}

TEST_CASE("dcast: round-trips melt", "[dcast][melt]") {
    // wide → melt → dcast should reproduce the original wide table structure.
    runtime::TableRegistry registry;
    runtime::Table wide;
    wide.add_column("symbol", Column<std::string>{"AAPL", "GOOG"});
    wide.add_column("open", Column<std::int64_t>{100, 200});
    wide.add_column("close", Column<std::int64_t>{110, 210});
    registry["wide"] = std::move(wide);

    // Melt first.
    auto melted_ir = require_ir("wide[melt symbol];");
    auto melted = runtime::interpret(*melted_ir, registry);
    REQUIRE(melted.has_value());
    registry["long"] = std::move(*melted);

    // Then dcast with symbol as row key.
    auto ir = require_ir("long[dcast variable, select value, by { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    REQUIRE(result->find("open") != nullptr);
    REQUIRE(result->find("close") != nullptr);

    const auto& open_col = std::get<Column<std::int64_t>>(*result->find("open"));
    REQUIRE(open_col[0] == 100);
    REQUIRE(open_col[1] == 200);
}

TEST_CASE("dcast: missing cell filled with null", "[dcast]") {
    // Two symbols; AAPL has both open/close, GOOG only has open.
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("symbol", Column<std::string>{"AAPL", "AAPL", "GOOG"});
    lng.add_column("variable", Column<std::string>{"open", "close", "open"});
    lng.add_column("value", Column<std::int64_t>{100, 110, 200});
    registry["lng"] = std::move(lng);

    auto ir = require_ir("lng[dcast variable, select value, by { symbol }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    // close column should have a null for GOOG.
    const auto* close_entry = result->find_entry("close");
    REQUIRE(close_entry != nullptr);
    REQUIRE(close_entry->validity.has_value());

    // Find which row is GOOG.
    const auto& sym_col = std::get<Column<std::string>>(*result->find("symbol"));
    std::size_t goog_row = (sym_col[0] == "GOOG") ? 0 : 1;
    REQUIRE_FALSE((*close_entry->validity)[goog_row]);
}

TEST_CASE("dcast: pivot column not found returns error", "[dcast]") {
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("variable", Column<std::string>{"open"});
    lng.add_column("value", Column<std::int64_t>{100});
    registry["lng"] = std::move(lng);

    auto ir = require_ir("lng[dcast no_such_col, select value, by {}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("pivot column not found") != std::string::npos);
}

TEST_CASE("dcast: value column not found returns error", "[dcast]") {
    runtime::TableRegistry registry;
    runtime::Table lng;
    lng.add_column("variable", Column<std::string>{"open"});
    lng.add_column("value", Column<std::int64_t>{100});
    registry["lng"] = std::move(lng);

    auto ir = require_ir("lng[dcast variable, select no_such_val, by {}];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("value column not found") != std::string::npos);
}

// ─── ExternCall node ─────────────────────────────────────────────────────────

// Helper: extern fn declaration so the lowerer recognises the function.
static constexpr const char* kMakePricesDecl =
    R"(extern fn make_prices(n: Int) -> DataFrame from "prices.hpp"; make_prices(0);)";

TEST_CASE("ExternCall: table-returning extern is resolved and called", "[extern]") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;

    // Register a simple table-returning extern that ignores its argument.
    externs.register_table("make_prices", [](const runtime::ExternArgs&) {
        runtime::Table t;
        t.add_column("price", Column<std::int64_t>{42, 99});
        return std::expected<runtime::ExternValue, std::string>{t};
    });

    auto ir = require_ir(kMakePricesDecl);
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto* price = result->find("price");
    REQUIRE(price != nullptr);
    const auto& prices = std::get<Column<std::int64_t>>(*price);
    REQUIRE(prices[0] == 42);
    REQUIRE(prices[1] == 99);
}

TEST_CASE("ExternCall: no extern registry returns error", "[extern]") {
    runtime::TableRegistry registry;

    // nullptr externs — should get "extern call with no registry" error.
    auto ir = require_ir(kMakePricesDecl);
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("no registry") != std::string::npos);
}

TEST_CASE("ExternCall: unknown extern function returns error", "[extern]") {
    runtime::TableRegistry registry;
    runtime::ExternRegistry externs;  // empty — make_prices not registered

    auto ir = require_ir(kMakePricesDecl);
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("unknown extern function") != std::string::npos);
}

// ─── Grouped update error ────────────────────────────────────────────────────

TEST_CASE("grouped update returns unsupported error", "[update]") {
    runtime::TableRegistry registry;
    runtime::Table trades;
    trades.add_column("symbol", Column<std::string>{"AAPL", "AAPL", "GOOG"});
    trades.add_column("price", Column<std::int64_t>{100, 110, 200});
    registry["trades"] = std::move(trades);

    auto ir = require_ir("trades[update { mean_price = mean(price) }, by symbol];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("grouped update not supported") != std::string::npos);
}

// ─── Filter type coverage: double / string columns ───────────────────────────
// These exercise template specialisations that the all-int64 tests miss.

TEST_CASE("filter on double column vs float literal", "[filter][types]") {
    // Exercises eval_value_vec<FilterColumn><double> and compare_col_scalar<double, double>.
    runtime::Table table;
    table.add_column("price", Column<double>{1.0, 2.5, 3.0});

    runtime::TableRegistry registry;
    registry.emplace("trades", table);

    auto ir = require_ir("trades[filter price > 1.5];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& col = std::get<Column<double>>(*result->find("price"));
    REQUIRE(col[0] == Catch::Approx(2.5));
    REQUIRE(col[1] == Catch::Approx(3.0));
}

TEST_CASE("filter int column vs float literal", "[filter][types]") {
    // Exercises cmp_col_scalar_into<int64, double>: int column compared to a float literal.
    runtime::Table table;
    table.add_column("qty", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter qty > 1.5];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    const auto& col = std::get<Column<std::int64_t>>(*result->find("qty"));
    REQUIRE(col[0] == 2);
    REQUIRE(col[1] == 3);
}

TEST_CASE("filter double column vs int literal", "[filter][types]") {
    // Exercises cmp_col_scalar_into<double, int64>: double column compared to an int literal.
    runtime::Table table;
    table.add_column("price", Column<double>{1.5, 2.5, 0.5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter price > 1];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);
    const auto& col = std::get<Column<double>>(*result->find("price"));
    REQUIRE(col[0] == Catch::Approx(1.5));
    REQUIRE(col[1] == Catch::Approx(2.5));
}

TEST_CASE("filter on string column", "[filter][types]") {
    // Exercises eval_value_vec<FilterColumn><string>.
    runtime::Table table;
    table.add_column("name", Column<std::string>{"Alice", "Bob", "Charlie"});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter name > \"B\"];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& col = std::get<Column<std::string>>(*result->find("name"));
    REQUIRE(col[0] == "Bob");
    REQUIRE(col[1] == "Charlie");
}

TEST_CASE("filter by bare column reference returns not-a-boolean-expression error",
          "[filter][types]") {
    // Exercises compute_mask<FilterColumn> (the else branch — bare column reference
    // is not a boolean predicate; interpreter returns an error).
    runtime::Table table;
    table.add_column("val", Column<std::int64_t>{10, 20, 30});
    table.add_column("is_active", Column<bool>{true, false, true});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[filter is_active];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("not a boolean expression") != std::string::npos);
}

// ─── Mixed-type column arithmetic ─────────────────────────────────────────────
// These exercise arith_into<int64,double>, arith_into<double,int64>,
// and arith_into<double,double> template specialisations.

TEST_CASE("update: int column + double column = double", "[update][types]") {
    runtime::Table table;
    table.add_column("a", Column<std::int64_t>{1, 2, 3});
    table.add_column("b", Column<double>{0.5, 1.5, 2.5});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = a + b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto* c = result->find("c");
    REQUIRE(c != nullptr);
    const auto& col = std::get<Column<double>>(*c);
    REQUIRE(col[0] == Catch::Approx(1.5));
    REQUIRE(col[1] == Catch::Approx(3.5));
    REQUIRE(col[2] == Catch::Approx(5.5));
}

TEST_CASE("update: double column + int column = double", "[update][types]") {
    runtime::Table table;
    table.add_column("a", Column<double>{0.5, 1.5, 2.5});
    table.add_column("b", Column<std::int64_t>{1, 2, 3});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = a + b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& col = std::get<Column<double>>(*result->find("c"));
    REQUIRE(col[0] == Catch::Approx(1.5));
    REQUIRE(col[1] == Catch::Approx(3.5));
    REQUIRE(col[2] == Catch::Approx(5.5));
}

TEST_CASE("update: double column * double column = double", "[update][types]") {
    runtime::Table table;
    table.add_column("a", Column<double>{1.0, 2.0, 3.0});
    table.add_column("b", Column<double>{2.0, 3.0, 4.0});

    runtime::TableRegistry registry;
    registry.emplace("t", table);

    auto ir = require_ir("t[update { c = a * b }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& col = std::get<Column<double>>(*result->find("c"));
    REQUIRE(col[0] == Catch::Approx(2.0));
    REQUIRE(col[1] == Catch::Approx(6.0));
    REQUIRE(col[2] == Catch::Approx(12.0));
}

// ─── Matrix Operations ────────────────────────────────────────────────────────

TEST_CASE("cov: diagonal equals variance", "[cov][matrix]") {
    // x = [1, 2, 3, 4, 5], y = [2, 4, 6, 8, 10]  (y = 2x)
    // var(x) = 2.5, var(y) = 10.0, cov(x,y) = 5.0
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[cov];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);
    REQUIRE(result->find("column") != nullptr);
    REQUIRE(result->find("x") != nullptr);
    REQUIRE(result->find("y") != nullptr);

    const auto& x_col = std::get<Column<double>>(*result->find("x"));
    const auto& y_col = std::get<Column<double>>(*result->find("y"));

    // Row 0 = covariances with x; row 1 = covariances with y
    REQUIRE(x_col[0] == Catch::Approx(2.5));   // var(x)
    REQUIRE(y_col[0] == Catch::Approx(5.0));   // cov(x, y)
    REQUIRE(x_col[1] == Catch::Approx(5.0));   // cov(y, x)
    REQUIRE(y_col[1] == Catch::Approx(10.0));  // var(y)
}

TEST_CASE("cov: drops non-numeric columns silently", "[cov][matrix]") {
    runtime::Table t;
    t.add_column("label", Column<std::string>{"a", "b", "c"});
    t.add_column("v", Column<double>{1.0, 2.0, 3.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[cov];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // Only the "v" column remains; label column is dropped.
    REQUIRE(result->find("v") != nullptr);
    REQUIRE(result->find("label") == nullptr);
    REQUIRE(result->rows() == 1);
}

TEST_CASE("cov: integer columns are widened to double", "[cov][matrix]") {
    runtime::Table t;
    t.add_column("a", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[cov];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->find("a") != nullptr);
    const auto& a_col = std::get<Column<double>>(*result->find("a"));
    REQUIRE(a_col[0] == Catch::Approx(2.5));  // var([1,2,3,4,5])
}

TEST_CASE("corr: diagonal equals 1.0", "[corr][matrix]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});  // perfect correlation
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[corr];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);
    const auto& x_col = std::get<Column<double>>(*result->find("x"));
    const auto& y_col = std::get<Column<double>>(*result->find("y"));

    REQUIRE(x_col[0] == Catch::Approx(1.0));  // corr(x, x) = 1
    REQUIRE(y_col[1] == Catch::Approx(1.0));  // corr(y, y) = 1
    REQUIRE(x_col[1] == Catch::Approx(1.0));  // perfect positive correlation
    REQUIRE(y_col[0] == Catch::Approx(1.0));
}

TEST_CASE("corr: off-diagonal in [-1, 1]", "[corr][matrix]") {
    runtime::Table t;
    t.add_column("a", Column<double>{1.0, 2.0, 3.0, 4.0});
    t.add_column("b", Column<double>{4.0, 3.0, 2.0, 1.0});  // perfect negative correlation
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[corr];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    const auto& a_col = std::get<Column<double>>(*result->find("a"));
    const auto& b_col = std::get<Column<double>>(*result->find("b"));
    REQUIRE(a_col[0] == Catch::Approx(1.0));   // corr(a, a)
    REQUIRE(b_col[1] == Catch::Approx(1.0));   // corr(b, b)
    REQUIRE(b_col[0] == Catch::Approx(-1.0));  // perfect negative: corr(a, b)
    REQUIRE(a_col[1] == Catch::Approx(-1.0));  // symmetric
}

TEST_CASE("transpose: basic numeric float64", "[transpose][matrix]") {
    // Input: 3 rows × 2 cols (a, b)
    // After transpose: 2 rows × 3 cols (r0, r1, r2) + leading "column" label
    runtime::Table t;
    t.add_column("a", Column<double>{1.0, 2.0, 3.0});
    t.add_column("b", Column<double>{4.0, 5.0, 6.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[transpose];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);  // 2 original columns → 2 rows
    // Without a label column: output columns are "column", "r0", "r1", "r2"
    REQUIRE(result->find("column") != nullptr);
    REQUIRE(result->find("r0") != nullptr);
    REQUIRE(result->find("r1") != nullptr);
    REQUIRE(result->find("r2") != nullptr);

    const auto& label = std::get<Column<std::string>>(*result->find("column"));
    REQUIRE(label[0] == "a");
    REQUIRE(label[1] == "b");

    const auto& r0 = std::get<Column<double>>(*result->find("r0"));
    REQUIRE(r0[0] == Catch::Approx(1.0));  // a[0]
    REQUIRE(r0[1] == Catch::Approx(4.0));  // b[0]

    const auto& r1 = std::get<Column<double>>(*result->find("r1"));
    REQUIRE(r1[0] == Catch::Approx(2.0));  // a[1]
    REQUIRE(r1[1] == Catch::Approx(5.0));  // b[1]
}

TEST_CASE("transpose: uses string label column for output column names", "[transpose][matrix]") {
    runtime::Table t;
    t.add_column("symbol", Column<std::string>{"AAPL", "MSFT"});
    t.add_column("open", Column<double>{100.0, 200.0});
    t.add_column("close", Column<double>{110.0, 210.0});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[transpose];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);  // 2 data columns (open, close)
    REQUIRE(result->find("AAPL") != nullptr);
    REQUIRE(result->find("MSFT") != nullptr);

    const auto& aapl = std::get<Column<double>>(*result->find("AAPL"));
    REQUIRE(aapl[0] == Catch::Approx(100.0));  // open[AAPL]
    REQUIRE(aapl[1] == Catch::Approx(110.0));  // close[AAPL]
}

TEST_CASE("transpose: mixed-type columns returns error", "[transpose][matrix]") {
    runtime::Table t;
    t.add_column("a", Column<double>{1.0, 2.0});
    t.add_column("b", Column<std::int64_t>{3, 4});
    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[transpose];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("mixed types") != std::string::npos);
}

TEST_CASE("matmul: basic (2x2) * (2x2)", "[matmul][matrix]") {
    // A = [[1, 2], [3, 4]]  →  columns: c0=[1,3], c1=[2,4]
    // B = [[5, 6], [7, 8]]  →  columns: c0=[5,7], c1=[6,8]
    // C = A*B = [[1*5+2*7, 1*6+2*8], [3*5+4*7, 3*6+4*8]] = [[19, 22], [43, 50]]
    runtime::Table a, b;
    a.add_column("c0", Column<double>{1.0, 3.0});
    a.add_column("c1", Column<double>{2.0, 4.0});
    b.add_column("c0", Column<double>{5.0, 7.0});
    b.add_column("c1", Column<double>{6.0, 8.0});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("matmul(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 2);
    REQUIRE(result->find("c0") != nullptr);
    REQUIRE(result->find("c1") != nullptr);

    const auto& out_c0 = std::get<Column<double>>(*result->find("c0"));
    const auto& out_c1 = std::get<Column<double>>(*result->find("c1"));

    REQUIRE(out_c0[0] == Catch::Approx(19.0));
    REQUIRE(out_c0[1] == Catch::Approx(43.0));
    REQUIRE(out_c1[0] == Catch::Approx(22.0));
    REQUIRE(out_c1[1] == Catch::Approx(50.0));
}

TEST_CASE("matmul: identity matrix leaves operand unchanged", "[matmul][matrix]") {
    // A = [[1], [2], [3]]  (3-row, 1-col vector)
    // I = [[1]]             (1x1 identity)
    // Result = [[1], [2], [3]]
    runtime::Table a, identity;
    a.add_column("v", Column<double>{1.0, 2.0, 3.0});
    identity.add_column("v", Column<double>{1.0});

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("identity", identity);

    auto ir = require_ir("matmul(a, identity);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    REQUIRE(result->rows() == 3);
    const auto& v = std::get<Column<double>>(*result->find("v"));
    REQUIRE(v[0] == Catch::Approx(1.0));
    REQUIRE(v[1] == Catch::Approx(2.0));
    REQUIRE(v[2] == Catch::Approx(3.0));
}

TEST_CASE("matmul: inner dimension mismatch returns error", "[matmul][matrix]") {
    runtime::Table a, b;
    a.add_column("c0", Column<double>{1.0, 2.0});
    a.add_column("c1", Column<double>{3.0, 4.0});         // A is 2 rows × 2 cols
    b.add_column("only", Column<double>{5.0, 6.0, 7.0});  // B has 3 rows ≠ 2 cols of A

    runtime::TableRegistry registry;
    registry.emplace("a", a);
    registry.emplace("b", b);

    auto ir = require_ir("matmul(a, b);");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("inner dimensions") != std::string::npos);
}

// ─── Model Specification Tests ───────────────────────────────────────────────

TEST_CASE("model: OLS simple regression", "[model]") {
    // y = 2*x + 1 (perfect linear relationship)
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());

    // Coefficients table: (intercept) and x
    REQUIRE(result->rows() == 2);
    REQUIRE(result->find("term") != nullptr);
    REQUIRE(result->find("estimate") != nullptr);

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));

    // With intercept: y = 1 + 2*x
    REQUIRE(std::string(terms[0]) == "(intercept)");
    REQUIRE(std::string(terms[1]) == "x");
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));

    // ModelResult should have R² = 1.0 for perfect fit
    REQUIRE(model_out.r_squared == Catch::Approx(1.0));
    REQUIRE(model_out.n_obs == 5);
    REQUIRE(model_out.n_params == 2);
}

TEST_CASE("model: OLS multiple regression", "[model]") {
    // y = 1 + 2*x1 + 3*x2
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("x2", Column<double>{0.0, 1.0, 0.0, 1.0, 0.0});
    t.add_column("y", Column<double>{3.0, 8.0, 7.0, 12.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);  // intercept + x1 + x2

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));  // intercept
    REQUIRE(estimates[1] == Catch::Approx(2.0));  // x1
    REQUIRE(estimates[2] == Catch::Approx(3.0));  // x2
}

TEST_CASE("model: OLS no intercept", "[model]") {
    // y = 2*x (through origin)
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{2.0, 4.0, 6.0, 8.0, 10.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x - 1, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 1);  // only x, no intercept

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(std::string(terms[0]) == "x");
    REQUIRE(estimates[0] == Catch::Approx(2.0));
}

TEST_CASE("model: OLS with filter", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0, 100.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0, 999.0});
    t.add_column("id", Column<std::int64_t>{1, 2, 3, 4, 5, 1000});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    // Filter out the outlier before fitting
    auto ir = require_ir("t[filter id < 1000, model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));  // intercept
    REQUIRE(estimates[1] == Catch::Approx(2.0));  // x
    REQUIRE(model_out.n_obs == 5);
}

TEST_CASE("model: OLS dot notation (all predictors)", "[model]") {
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("x2", Column<double>{5.0, 4.0, 3.0, 2.0, 1.0});
    t.add_column("y", Column<double>{6.0, 6.0, 6.0, 6.0, 6.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ ., method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 3);  // intercept + x1 + x2
}

TEST_CASE("model: OLS interaction term", "[model]") {
    // y = 1 + 2*x1 + 3*x2 + 4*x1*x2, with well-spread data
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0});
    t.add_column("x2", Column<double>{1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0});
    // y = 1 + 2*x1 + 3*x2 + 4*x1*x2
    t.add_column("y", Column<double>{10.0, 16.0, 22.0, 17.0, 27.0, 37.0, 24.0, 38.0, 52.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 + x2 + x1:x2, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);  // intercept + x1 + x2 + x1:x2

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    REQUIRE(std::string(terms[3]) == "x1:x2");

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0).margin(0.01));  // intercept
    REQUIRE(estimates[1] == Catch::Approx(2.0).margin(0.01));  // x1
    REQUIRE(estimates[2] == Catch::Approx(3.0).margin(0.01));  // x2
    REQUIRE(estimates[3] == Catch::Approx(4.0).margin(0.01));  // x1:x2
}

TEST_CASE("model: OLS crossing operator (*)", "[model]") {
    // y ~ x1 * x2 should expand to y ~ x1 + x2 + x1:x2
    runtime::Table t;
    t.add_column("x1", Column<double>{1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0});
    t.add_column("x2", Column<double>{1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0});
    t.add_column("y", Column<double>{10.0, 16.0, 22.0, 17.0, 27.0, 37.0, 24.0, 38.0, 52.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x1 * x2, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 4);  // intercept + x1 + x2 + x1:x2
}

TEST_CASE("model: ridge regression", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ridge, lambda = 0.1 }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    // Ridge should shrink coefficients slightly toward zero compared to OLS
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    // With small lambda, should be close to OLS (intercept ≈ 1, slope ≈ 2)
    REQUIRE(estimates[1] == Catch::Approx(2.0).margin(0.2));
}

TEST_CASE("model: WLS with weights", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});
    t.add_column("w", Column<double>{1.0, 1.0, 1.0, 1.0, 1.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = wls, weights = w }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // With equal weights, WLS = OLS
    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));
}

TEST_CASE("model: LightBM plugin method", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    runtime::ExternRegistry externs;
    externs.register_scalar_table_consumer(
        "model_lightbm", runtime::ScalarKind::Int,
        [](const runtime::Table& design_matrix,
           const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            REQUIRE(args.size() == 5);
            REQUIRE(std::get<std::string>(args[0]) == "__response");
            REQUIRE(std::get<std::string>(args[1]) == "iterations");
            REQUIRE(std::get<std::int64_t>(args[2]) == 250);
            REQUIRE(std::get<std::string>(args[3]) == "learning_rate");
            REQUIRE(std::get<double>(args[4]) == Catch::Approx(0.04));

            const auto* x = std::get_if<Column<double>>(design_matrix.find("x"));
            const auto* y = std::get_if<Column<double>>(design_matrix.find("__response"));
            REQUIRE(x != nullptr);
            REQUIRE(y != nullptr);
            REQUIRE(x->size() == y->size());

            runtime::Table out;
            out.add_column("term", Column<std::string>{"(intercept)", "x"});
            out.add_column("estimate", Column<double>{1.0, 2.0});
            return runtime::ExternValue{std::move(out)};
        });

    auto ir =
        require_ir("t[model { y ~ x, method = lightbm, iterations = 250, learning_rate = 0.04 }];");
    auto result = runtime::interpret(*ir, registry, nullptr, &externs);
    REQUIRE(result.has_value());

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));
}

TEST_CASE("model: LightBM method requires plugin import/registration", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = lightbm }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("plugin") != std::string::npos);
}

TEST_CASE("model: ModelResult accessor tables", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    runtime::ModelResult model_out;
    auto result = runtime::interpret(*ir, registry, nullptr, nullptr, &model_out);
    REQUIRE(result.has_value());

    // Summary table has std_error, t_stat, p_value columns
    REQUIRE(model_out.summary.find("std_error") != nullptr);
    REQUIRE(model_out.summary.find("t_stat") != nullptr);
    REQUIRE(model_out.summary.find("p_value") != nullptr);

    // Fitted values
    REQUIRE(model_out.fitted_values.rows() == 5);
    const auto& fitted = std::get<Column<double>>(*model_out.fitted_values.find("fitted"));
    REQUIRE(fitted[0] == Catch::Approx(3.0));   // 1 + 2*1
    REQUIRE(fitted[4] == Catch::Approx(11.0));  // 1 + 2*5

    // Residuals (should be ~0 for perfect fit)
    REQUIRE(model_out.residuals.rows() == 5);
    const auto& resid = std::get<Column<double>>(*model_out.residuals.find("residual"));
    REQUIRE(resid[0] == Catch::Approx(0.0).margin(1e-10));
    REQUIRE(resid[4] == Catch::Approx(0.0).margin(1e-10));
}

TEST_CASE("model: integer columns widened", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<std::int64_t>{1, 2, 3, 4, 5});
    t.add_column("y", Column<std::int64_t>{3, 5, 7, 9, 11});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());
    REQUIRE(result->rows() == 2);

    const auto& estimates = std::get<Column<double>>(*result->find("estimate"));
    REQUIRE(estimates[0] == Catch::Approx(1.0));
    REQUIRE(estimates[1] == Catch::Approx(2.0));
}

TEST_CASE("model: dummy encoding for string columns", "[model]") {
    runtime::Table t;
    t.add_column("region", Column<std::string>{"East", "West", "North", "East", "West", "North"});
    t.add_column("y", Column<double>{10.0, 20.0, 30.0, 12.0, 22.0, 32.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ region, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE(result.has_value());

    // With intercept: reference level (East) absorbed → intercept + region_West + region_North
    REQUIRE(result->rows() == 3);

    const auto& terms = std::get<Column<std::string>>(*result->find("term"));
    REQUIRE(std::string(terms[0]) == "(intercept)");
    // The dummy columns should be region_West and region_North (East is reference)
}

TEST_CASE("model: error on unknown method", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    t.add_column("y", Column<double>{3.0, 5.0, 7.0, 9.0, 11.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = unknown_method }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("unknown method") != std::string::npos);
}

TEST_CASE("model: error on missing response column", "[model]") {
    runtime::Table t;
    t.add_column("x", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});

    runtime::TableRegistry registry;
    registry.emplace("t", t);

    auto ir = require_ir("t[model { y ~ x, method = ols }];");
    auto result = runtime::interpret(*ir, registry);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("response column not found") != std::string::npos);
}
