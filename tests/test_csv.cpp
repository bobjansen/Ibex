#include <catch2/catch_test_macros.hpp>

#include <csv.hpp>
#include <filesystem>
#include <fstream>

TEST_CASE("Read simple CSV") {
    auto path = std::filesystem::temp_directory_path() / "ibex_test.csv";
    {
        std::ofstream out(path);
        out << "price,symbol\n";
        out << "10,A\n";
        out << "20,B\n";
        out << "30,A\n";
    }

    auto table = read_csv(path.string());
    const auto* price_col = table.find("price");
    const auto* symbol_col = table.find("symbol");
    REQUIRE(price_col != nullptr);
    REQUIRE(symbol_col != nullptr);
    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(price_col);
    const auto* symbols = std::get_if<ibex::Column<std::string>>(symbol_col);
    REQUIRE(prices != nullptr);
    REQUIRE(symbols != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 30);
    REQUIRE((*symbols)[0] == "A");
    REQUIRE((*symbols)[1] == "B");
    REQUIRE((*symbols)[2] == "A");
}
