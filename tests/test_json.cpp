#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <filesystem>
#include <fstream>
#include <json.hpp>
#include <string>

namespace {

auto write_file(const std::filesystem::path& path, const char* content) {
    std::ofstream out(path);
    out << content;
}

auto tmp(const char* name) -> std::filesystem::path {
    return std::filesystem::temp_directory_path() / name;
}

auto get_string_at(const ibex::runtime::Table& table, const char* name, std::size_t row)
    -> std::string {
    if (const auto* col = std::get_if<ibex::Column<std::string>>(table.find(name))) {
        return std::string((*col)[row]);
    }
    if (const auto* col = std::get_if<ibex::Column<ibex::Categorical>>(table.find(name))) {
        return std::string((*col)[row]);
    }
    return {};
}

auto is_null_at(const ibex::runtime::Table& table, const char* name, std::size_t row) -> bool {
    const auto* entry = table.find_entry(name);
    REQUIRE(entry != nullptr);
    return ibex::runtime::is_null(*entry, row);
}

}  // namespace

// ---------------------------------------------------------------------------
// read_json tests
// ---------------------------------------------------------------------------

TEST_CASE("Read JSON - int and string columns from array") {
    auto path = tmp("ibex_test_json_simple.json");
    write_file(
        path, R"([{"price":10,"symbol":"A"},{"price":20,"symbol":"B"},{"price":30,"symbol":"A"}])");

    auto table = read_json(path.string());
    REQUIRE(table.rows() == 3);

    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 30);
    REQUIRE(get_string_at(table, "symbol", 0) == "A");
    REQUIRE(get_string_at(table, "symbol", 1) == "B");
    REQUIRE(get_string_at(table, "symbol", 2) == "A");
}

TEST_CASE("Read JSON - float64 column detection") {
    auto path = tmp("ibex_test_json_float.json");
    write_file(path, R"([{"price":1.5,"qty":10},{"price":2.25,"qty":20}])");

    auto table = read_json(path.string());
    const auto* prices = std::get_if<ibex::Column<double>>(table.find("price"));
    const auto* qtys = std::get_if<ibex::Column<std::int64_t>>(table.find("qty"));
    REQUIRE(prices != nullptr);
    REQUIRE(qtys != nullptr);
    REQUIRE((*prices)[0] == Catch::Approx(1.5));
    REQUIRE((*prices)[1] == Catch::Approx(2.25));
    REQUIRE((*qtys)[0] == 10);
}

TEST_CASE("Read JSON - mixed int/float widens to double") {
    auto path = tmp("ibex_test_json_widen.json");
    write_file(path, R"([{"val":1},{"val":2.5},{"val":3}])");

    auto table = read_json(path.string());
    const auto* vals = std::get_if<ibex::Column<double>>(table.find("val"));
    REQUIRE(vals != nullptr);
    REQUIRE(vals->size() == 3);
    REQUIRE((*vals)[0] == Catch::Approx(1.0));
    REQUIRE((*vals)[1] == Catch::Approx(2.5));
    REQUIRE((*vals)[2] == Catch::Approx(3.0));
}

TEST_CASE("Read JSON - boolean column") {
    auto path = tmp("ibex_test_json_bool.json");
    write_file(path, R"([{"flag":true},{"flag":false},{"flag":true}])");

    auto table = read_json(path.string());
    const auto* flags = std::get_if<ibex::Column<bool>>(table.find("flag"));
    REQUIRE(flags != nullptr);
    REQUIRE(flags->size() == 3);
    REQUIRE((*flags)[0] == true);
    REQUIRE((*flags)[1] == false);
    REQUIRE((*flags)[2] == true);
}

TEST_CASE("Read JSON - null values create validity bitmap") {
    auto path = tmp("ibex_test_json_null.json");
    write_file(path, R"([{"a":1,"b":"x"},{"a":null,"b":"y"},{"a":3,"b":null}])");

    auto table = read_json(path.string());
    REQUIRE(table.rows() == 3);

    const auto* a = std::get_if<ibex::Column<std::int64_t>>(table.find("a"));
    REQUIRE(a != nullptr);
    REQUIRE((*a)[0] == 1);
    REQUIRE((*a)[2] == 3);
    REQUIRE_FALSE(is_null_at(table, "a", 0));
    REQUIRE(is_null_at(table, "a", 1));
    REQUIRE_FALSE(is_null_at(table, "a", 2));

    REQUIRE(get_string_at(table, "b", 0) == "x");
    REQUIRE(get_string_at(table, "b", 1) == "y");
    REQUIRE_FALSE(is_null_at(table, "b", 0));
    REQUIRE_FALSE(is_null_at(table, "b", 1));
    REQUIRE(is_null_at(table, "b", 2));
}

TEST_CASE("Read JSON - missing keys treated as null") {
    auto path = tmp("ibex_test_json_missing.json");
    write_file(path, R"([{"a":1,"b":2},{"a":3}])");

    auto table = read_json(path.string());
    REQUIRE(table.rows() == 2);

    const auto* b = std::get_if<ibex::Column<std::int64_t>>(table.find("b"));
    REQUIRE(b != nullptr);
    REQUIRE((*b)[0] == 2);
    REQUIRE_FALSE(is_null_at(table, "b", 0));
    REQUIRE(is_null_at(table, "b", 1));
}

TEST_CASE("Read JSON - JSON-Lines format") {
    auto path = tmp("ibex_test_jsonl.jsonl");
    write_file(path, "{\"x\":1,\"y\":\"a\"}\n{\"x\":2,\"y\":\"b\"}\n{\"x\":3,\"y\":\"c\"}\n");

    auto table = read_json(path.string());
    REQUIRE(table.rows() == 3);

    const auto* x = std::get_if<ibex::Column<std::int64_t>>(table.find("x"));
    REQUIRE(x != nullptr);
    REQUIRE((*x)[0] == 1);
    REQUIRE((*x)[1] == 2);
    REQUIRE((*x)[2] == 3);
    REQUIRE(get_string_at(table, "y", 0) == "a");
    REQUIRE(get_string_at(table, "y", 1) == "b");
    REQUIRE(get_string_at(table, "y", 2) == "c");
}

TEST_CASE("Read JSON - single object becomes one-row table") {
    auto path = tmp("ibex_test_json_single.json");
    write_file(path, R"({"name":"Alice","age":30})");

    auto table = read_json(path.string());
    REQUIRE(table.rows() == 1);
    REQUIRE(get_string_at(table, "name", 0) == "Alice");

    const auto* age = std::get_if<ibex::Column<std::int64_t>>(table.find("age"));
    REQUIRE(age != nullptr);
    REQUIRE((*age)[0] == 30);
}

TEST_CASE("Read JSON - large int64 values") {
    auto path = tmp("ibex_test_json_int64.json");
    write_file(path, R"([{"ts":9999999999},{"ts":1000000000}])");

    auto table = read_json(path.string());
    const auto* ts = std::get_if<ibex::Column<std::int64_t>>(table.find("ts"));
    REQUIRE(ts != nullptr);
    REQUIRE((*ts)[0] == 9999999999LL);
    REQUIRE((*ts)[1] == 1000000000LL);
}

TEST_CASE("Read JSON - empty array produces empty table") {
    auto path = tmp("ibex_test_json_empty.json");
    write_file(path, "[]");

    auto table = read_json(path.string());
    REQUIRE(table.rows() == 0);
}

TEST_CASE("Read JSON - missing path reports file not found") {
    auto path = tmp("ibex_test_json_missing_file.json");
    std::filesystem::remove(path);

    REQUIRE_THROWS_WITH(read_json(path.string()),
                        Catch::Matchers::ContainsSubstring("read_json: file not found:") &&
                            Catch::Matchers::ContainsSubstring(path.string()));
}

// ---------------------------------------------------------------------------
// write_json tests
// ---------------------------------------------------------------------------

TEST_CASE("Write JSON - int and string columns round-trip") {
    auto in_path = tmp("ibex_test_json_write_in.json");
    write_file(in_path, R"([{"price":10,"symbol":"A"},{"price":20,"symbol":"B"}])");

    auto original = read_json(in_path.string());
    auto out_path = tmp("ibex_test_json_write_out.json");
    auto rows_written = write_json(original, out_path.string());
    REQUIRE(rows_written == 2);

    auto reread = read_json(out_path.string());
    REQUIRE(reread.rows() == 2);
    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(reread.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE(get_string_at(reread, "symbol", 0) == "A");
    REQUIRE(get_string_at(reread, "symbol", 1) == "B");
}

TEST_CASE("Write JSON - double column round-trip") {
    ibex::runtime::Table table;
    table.add_column("x", ibex::Column<double>({1.5, 2.25}));
    table.add_column("y", ibex::Column<double>({0.5, 3.0}));

    auto out_path = tmp("ibex_test_json_write_double.json");
    auto rows_written = write_json(table, out_path.string());
    REQUIRE(rows_written == 2);

    auto reread = read_json(out_path.string());
    const auto* x = std::get_if<ibex::Column<double>>(reread.find("x"));
    const auto* y = std::get_if<ibex::Column<double>>(reread.find("y"));
    REQUIRE(x != nullptr);
    REQUIRE(y != nullptr);
    REQUIRE((*x)[0] == Catch::Approx(1.5));
    REQUIRE((*x)[1] == Catch::Approx(2.25));
    REQUIRE((*y)[0] == Catch::Approx(0.5));
    REQUIRE((*y)[1] == Catch::Approx(3.0));
}

TEST_CASE("Write JSON - bool column round-trip") {
    ibex::runtime::Table table;
    table.add_column("flag", ibex::Column<bool>({true, false, true}));

    auto out_path = tmp("ibex_test_json_write_bool.json");
    auto rows_written = write_json(table, out_path.string());
    REQUIRE(rows_written == 3);

    auto reread = read_json(out_path.string());
    const auto* flags = std::get_if<ibex::Column<bool>>(reread.find("flag"));
    REQUIRE(flags != nullptr);
    REQUIRE((*flags)[0] == true);
    REQUIRE((*flags)[1] == false);
    REQUIRE((*flags)[2] == true);
}

TEST_CASE("Write JSON - null values round-trip") {
    ibex::runtime::Table table;
    ibex::Column<std::int64_t> col({10, 0, 30});
    std::vector<bool> validity = {true, false, true};
    table.add_column("val", std::move(col), std::move(validity));

    auto out_path = tmp("ibex_test_json_write_null.json");
    write_json(table, out_path.string());

    auto reread = read_json(out_path.string());
    REQUIRE(reread.rows() == 3);
    const auto* vals = std::get_if<ibex::Column<std::int64_t>>(reread.find("val"));
    REQUIRE(vals != nullptr);
    REQUIRE((*vals)[0] == 10);
    REQUIRE((*vals)[2] == 30);
    REQUIRE_FALSE(is_null_at(reread, "val", 0));
    REQUIRE(is_null_at(reread, "val", 1));
    REQUIRE_FALSE(is_null_at(reread, "val", 2));
}

TEST_CASE("Write JSON - empty table writes empty array") {
    ibex::runtime::Table table;
    table.add_column("a", ibex::Column<std::int64_t>{});

    auto out_path = tmp("ibex_test_json_write_empty.json");
    auto rows_written = write_json(table, out_path.string());
    REQUIRE(rows_written == 0);

    // File should contain an empty JSON array.
    std::ifstream f(out_path);
    std::string content((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    REQUIRE(content.find("[]") != std::string::npos);
}

TEST_CASE("Write JSON - CSV to JSON cross-format round-trip") {
    // Build a table programmatically and write as JSON, then read back.
    ibex::runtime::Table table;
    table.add_column("name", ibex::Column<std::string>({"Smith, John", "Doe, Jane"}));
    table.add_column("score", ibex::Column<std::int64_t>({95, 87}));

    auto out_path = tmp("ibex_test_json_cross.json");
    auto rows_written = write_json(table, out_path.string());
    REQUIRE(rows_written == 2);

    auto reread = read_json(out_path.string());
    REQUIRE(reread.rows() == 2);
    REQUIRE(get_string_at(reread, "name", 0) == "Smith, John");
    REQUIRE(get_string_at(reread, "name", 1) == "Doe, Jane");
    const auto* scores = std::get_if<ibex::Column<std::int64_t>>(reread.find("score"));
    REQUIRE(scores != nullptr);
    REQUIRE((*scores)[0] == 95);
    REQUIRE((*scores)[1] == 87);
}
