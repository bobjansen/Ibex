#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <csv.hpp>
#include <filesystem>
#include <fstream>

namespace {

auto write_csv(const std::filesystem::path& path, const char* content) {
    std::ofstream out(path);
    out << content;
}

auto tmp(const char* name) -> std::filesystem::path {
    return std::filesystem::temp_directory_path() / name;
}

}  // namespace

TEST_CASE("Read simple CSV - int and string columns") {
    auto path = tmp("ibex_test_simple.csv");
    write_csv(path, "price,symbol\n10,A\n20,B\n30,A\n");

    auto table = read_csv(path.string());
    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    const auto* symbols = std::get_if<ibex::Column<std::string>>(table.find("symbol"));
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

TEST_CASE("Read CSV - float64 column detection") {
    auto path = tmp("ibex_test_float.csv");
    write_csv(path, "price,qty\n1.5,10\n2.25,20\n0.5,5\n");

    auto table = read_csv(path.string());
    const auto* prices = std::get_if<ibex::Column<double>>(table.find("price"));
    const auto* qtys = std::get_if<ibex::Column<std::int64_t>>(table.find("qty"));
    REQUIRE(prices != nullptr);
    REQUIRE(qtys != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == Catch::Approx(1.5));
    REQUIRE((*prices)[1] == Catch::Approx(2.25));
    REQUIRE((*prices)[2] == Catch::Approx(0.5));
    REQUIRE((*qtys)[0] == 10);
}

TEST_CASE("Read CSV - large int64 values") {
    auto path = tmp("ibex_test_int64.csv");
    // Value exceeds int32 range
    write_csv(path, "ts\n9999999999\n1000000000\n");

    auto table = read_csv(path.string());
    const auto* ts = std::get_if<ibex::Column<std::int64_t>>(table.find("ts"));
    REQUIRE(ts != nullptr);
    REQUIRE((*ts)[0] == 9999999999LL);
    REQUIRE((*ts)[1] == 1000000000LL);
}

TEST_CASE("Read CSV - single column") {
    auto path = tmp("ibex_test_single.csv");
    write_csv(path, "value\n1\n2\n3\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 3);
    const auto* col = std::get_if<ibex::Column<std::int64_t>>(table.find("value"));
    REQUIRE(col != nullptr);
    REQUIRE((*col)[0] == 1);
    REQUIRE((*col)[2] == 3);
}

TEST_CASE("Read CSV - mixed numeric/non-numeric falls back to string") {
    auto path = tmp("ibex_test_mixed.csv");
    // "price" column has a non-numeric value → whole column must become string
    write_csv(path, "price\n10\n20\nN/A\n30\n");

    auto table = read_csv(path.string());
    const auto* col = std::get_if<ibex::Column<std::string>>(table.find("price"));
    REQUIRE(col != nullptr);
    REQUIRE(col->size() == 4);
    REQUIRE((*col)[0] == "10");
    REQUIRE((*col)[2] == "N/A");
}

TEST_CASE("Read CSV - single data row") {
    auto path = tmp("ibex_test_onerow.csv");
    write_csv(path, "a,b\n42,hello\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 1);
    const auto* a = std::get_if<ibex::Column<std::int64_t>>(table.find("a"));
    const auto* b = std::get_if<ibex::Column<std::string>>(table.find("b"));
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE((*a)[0] == 42);
    REQUIRE((*b)[0] == "hello");
}

TEST_CASE("Read CSV - trailing comma produces empty last field") {
    auto path = tmp("ibex_test_trailing.csv");
    write_csv(path, "a,b,c\n1,2,\n3,4,\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    // "c" column has empty strings → parsed as string type
    const auto* c = std::get_if<ibex::Column<std::string>>(table.find("c"));
    REQUIRE(c != nullptr);
    REQUIRE((*c)[0] == "");
    REQUIRE((*c)[1] == "");
}

TEST_CASE("Read CSV - RFC 4180 quoted fields with embedded commas") {
    auto path = tmp("ibex_test_quoted.csv");
    // "name" column has commas inside quotes — old parser would split incorrectly
    write_csv(path, "id,name,score\n1,\"Smith, John\",95\n2,\"Doe, Jane\",87\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    const auto* ids = std::get_if<ibex::Column<std::int64_t>>(table.find("id"));
    const auto* names = std::get_if<ibex::Column<std::string>>(table.find("name"));
    const auto* scores = std::get_if<ibex::Column<std::int64_t>>(table.find("score"));
    REQUIRE(ids != nullptr);
    REQUIRE(names != nullptr);
    REQUIRE(scores != nullptr);
    REQUIRE((*names)[0] == "Smith, John");
    REQUIRE((*names)[1] == "Doe, Jane");
    REQUIRE((*ids)[0] == 1);
    REQUIRE((*scores)[0] == 95);
}

TEST_CASE("Read CSV - RFC 4180 escaped quotes inside quoted field") {
    auto path = tmp("ibex_test_escaped_quotes.csv");
    // "" inside a quoted field represents a literal quote character
    write_csv(path, "msg\n\"say \"\"hello\"\"\"\n\"world\"\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    const auto* msgs = std::get_if<ibex::Column<std::string>>(table.find("msg"));
    REQUIRE(msgs != nullptr);
    REQUIRE((*msgs)[0] == "say \"hello\"");
    REQUIRE((*msgs)[1] == "world");
}
