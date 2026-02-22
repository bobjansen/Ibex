#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

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
