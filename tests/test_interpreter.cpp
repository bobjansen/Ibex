#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <limits>

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
