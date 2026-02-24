#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>

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
