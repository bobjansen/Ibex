#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

TEST_CASE("TableSourceOperator emits one chunk then EOF") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(table));

    auto first = source->next();
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());
    REQUIRE(first.value()->rows() == 3);
    REQUIRE(first.value()->columns.size() == 2);

    auto second = source->next();
    REQUIRE(second.has_value());
    REQUIRE_FALSE(second.value().has_value());
}

TEST_CASE("MaterializeOperator round-trips a Table through the operator scaffold") {
    runtime::Table input;
    input.add_column("price", Column<std::int64_t>{10, 20, 30});
    input.add_column("symbol", Column<std::string>{"A", "B", "A"});

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(input));
    runtime::MaterializeOperator sink{std::move(source)};

    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.rows() == 3);
    REQUIRE(out.columns.size() == 2);

    const auto* price = out.find("price");
    REQUIRE(price != nullptr);
    const auto* price_col = std::get_if<Column<std::int64_t>>(price);
    REQUIRE(price_col != nullptr);
    REQUIRE(price_col->size() == 3);
    REQUIRE((*price_col)[0] == 10);
    REQUIRE((*price_col)[1] == 20);
    REQUIRE((*price_col)[2] == 30);

    const auto* symbol = out.find("symbol");
    REQUIRE(symbol != nullptr);
    const auto* symbol_col = std::get_if<Column<std::string>>(symbol);
    REQUIRE(symbol_col != nullptr);
    REQUIRE((*symbol_col)[0] == "A");
    REQUIRE((*symbol_col)[1] == "B");
    REQUIRE((*symbol_col)[2] == "A");
}

TEST_CASE("MaterializeOperator preserves Table ordering and time_index") {
    runtime::Table input;
    input.add_column("ts", Column<std::int64_t>{1, 2, 3});
    input.add_column("value", Column<double>{1.5, 2.5, 3.5});
    input.ordering = std::vector<ir::OrderKey>{ir::OrderKey{.name = "ts", .ascending = true}};
    input.time_index = std::string{"ts"};

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(input));
    runtime::MaterializeOperator sink{std::move(source)};

    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.ordering.has_value());
    REQUIRE(out.ordering->size() == 1);
    REQUIRE((*out.ordering)[0].name == "ts");
    REQUIRE(out.time_index.has_value());
    REQUIRE(*out.time_index == "ts");
}

TEST_CASE("MaterializeOperator returns an empty Table when the source is empty") {
    class EmptySource final : public runtime::Operator {
       public:
        auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
            return std::optional<runtime::Chunk>{};
        }
    };

    runtime::MaterializeOperator sink{std::make_unique<EmptySource>()};
    auto result = sink.run();
    REQUIRE(result.has_value());
    REQUIRE(result.value().columns.empty());
    REQUIRE(result.value().rows() == 0);
}
