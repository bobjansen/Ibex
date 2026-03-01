#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <csv.hpp>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

auto write_csv(const std::filesystem::path& path, const char* content) {
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

TEST_CASE("Read simple CSV - int and string columns") {
    auto path = tmp("ibex_test_simple.csv");
    write_csv(path, "price,symbol\n10,A\n20,B\n30,A\n");

    auto table = read_csv(path.string());
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
    REQUIRE(table.rows() == 4);
    REQUIRE(get_string_at(table, "price", 0) == "10");
    REQUIRE(get_string_at(table, "price", 2) == "N/A");
}

TEST_CASE("Read CSV - single data row") {
    auto path = tmp("ibex_test_onerow.csv");
    write_csv(path, "a,b\n42,hello\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 1);
    const auto* a = std::get_if<ibex::Column<std::int64_t>>(table.find("a"));
    REQUIRE(a != nullptr);
    REQUIRE((*a)[0] == 42);
    REQUIRE(get_string_at(table, "b", 0) == "hello");
}

TEST_CASE("Read CSV - trailing comma produces empty last field") {
    auto path = tmp("ibex_test_trailing.csv");
    write_csv(path, "a,b,c\n1,2,\n3,4,\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    REQUIRE(get_string_at(table, "c", 0) == "");
    REQUIRE(get_string_at(table, "c", 1) == "");
}

TEST_CASE("Read CSV - RFC 4180 quoted fields with embedded commas") {
    auto path = tmp("ibex_test_quoted.csv");
    // "name" column has commas inside quotes — old parser would split incorrectly
    write_csv(path, "id,name,score\n1,\"Smith, John\",95\n2,\"Doe, Jane\",87\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    const auto* ids = std::get_if<ibex::Column<std::int64_t>>(table.find("id"));
    const auto* scores = std::get_if<ibex::Column<std::int64_t>>(table.find("score"));
    REQUIRE(ids != nullptr);
    REQUIRE(scores != nullptr);
    REQUIRE(get_string_at(table, "name", 0) == "Smith, John");
    REQUIRE(get_string_at(table, "name", 1) == "Doe, Jane");
    REQUIRE((*ids)[0] == 1);
    REQUIRE((*scores)[0] == 95);
}

TEST_CASE("Read CSV - RFC 4180 escaped quotes inside quoted field") {
    auto path = tmp("ibex_test_escaped_quotes.csv");
    // "" inside a quoted field represents a literal quote character
    write_csv(path, "msg\n\"say \"\"hello\"\"\"\n\"world\"\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    REQUIRE(get_string_at(table, "msg", 0) == "say \"hello\"");
    REQUIRE(get_string_at(table, "msg", 1) == "world");
}

TEST_CASE("Read CSV - nullable parsing via null spec") {
    auto path = tmp("ibex_test_nullable_parse.csv");
    write_csv(path, "price,qty,note\n10,1,ok\n,2,NA\n30,NA,\n");

    auto table = read_csv(path.string(), "<empty>,NA");
    REQUIRE(table.rows() == 3);

    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    const auto* qtys = std::get_if<ibex::Column<std::int64_t>>(table.find("qty"));
    const auto* notes = std::get_if<ibex::Column<std::string>>(table.find("note"));
    REQUIRE(prices != nullptr);
    REQUIRE(qtys != nullptr);
    REQUIRE(notes != nullptr);

    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 0);  // null payload
    REQUIRE((*prices)[2] == 30);
    REQUIRE_FALSE(is_null_at(table, "price", 0));
    REQUIRE(is_null_at(table, "price", 1));
    REQUIRE_FALSE(is_null_at(table, "price", 2));

    REQUIRE((*qtys)[0] == 1);
    REQUIRE((*qtys)[1] == 2);
    REQUIRE((*qtys)[2] == 0);  // null payload
    REQUIRE_FALSE(is_null_at(table, "qty", 0));
    REQUIRE_FALSE(is_null_at(table, "qty", 1));
    REQUIRE(is_null_at(table, "qty", 2));

    REQUIRE((*notes)[0] == "ok");
    REQUIRE((*notes)[1] == "");  // null payload
    REQUIRE((*notes)[2] == "");  // null payload
    REQUIRE_FALSE(is_null_at(table, "note", 0));
    REQUIRE(is_null_at(table, "note", 1));
    REQUIRE(is_null_at(table, "note", 2));
}

// ---------------------------------------------------------------------------
// write_csv tests
// ---------------------------------------------------------------------------

TEST_CASE("Write CSV - int and string columns round-trip") {
    auto path = tmp("ibex_test_write_simple.csv");
    write_csv(path, "price,symbol\n10,A\n20,B\n30,A\n");

    auto original = read_csv(path.string());
    auto out_path = tmp("ibex_test_write_simple_out.csv");
    auto rows_written = write_csv(original, out_path.string());
    REQUIRE(rows_written == 3);

    auto reread = read_csv(out_path.string());
    REQUIRE(reread.rows() == 3);
    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(reread.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 30);
    REQUIRE(get_string_at(reread, "symbol", 0) == "A");
    REQUIRE(get_string_at(reread, "symbol", 1) == "B");
    REQUIRE(get_string_at(reread, "symbol", 2) == "A");
}

TEST_CASE("Write CSV - double column round-trip") {
    auto path = tmp("ibex_test_write_double.csv");
    write_csv(path, "x,y\n1.5,2.25\n0.5,3.0\n");

    auto original = read_csv(path.string());
    auto out_path = tmp("ibex_test_write_double_out.csv");
    auto rows_written = write_csv(original, out_path.string());
    REQUIRE(rows_written == 2);

    auto reread = read_csv(out_path.string());
    const auto* x = std::get_if<ibex::Column<double>>(reread.find("x"));
    const auto* y = std::get_if<ibex::Column<double>>(reread.find("y"));
    REQUIRE(x != nullptr);
    REQUIRE(y != nullptr);
    REQUIRE((*x)[0] == Catch::Approx(1.5));
    REQUIRE((*x)[1] == Catch::Approx(0.5));
    REQUIRE((*y)[0] == Catch::Approx(2.25));
    REQUIRE((*y)[1] == Catch::Approx(3.0));
}

TEST_CASE("Write CSV - fields with commas are quoted") {
    // Build a table that has a string column containing commas.
    ibex::runtime::Table table;
    table.add_column("name", ibex::Column<std::string>({"Smith, John", "Doe, Jane"}));
    table.add_column("score", ibex::Column<std::int64_t>({95, 87}));

    auto out_path = tmp("ibex_test_write_quoted.csv");
    auto rows_written = write_csv(table, out_path.string());
    REQUIRE(rows_written == 2);

    // Read back and verify the commas survived the round-trip.
    auto reread = read_csv(out_path.string());
    REQUIRE(reread.rows() == 2);
    REQUIRE(get_string_at(reread, "name", 0) == "Smith, John");
    REQUIRE(get_string_at(reread, "name", 1) == "Doe, Jane");
    const auto* scores = std::get_if<ibex::Column<std::int64_t>>(reread.find("score"));
    REQUIRE(scores != nullptr);
    REQUIRE((*scores)[0] == 95);
    REQUIRE((*scores)[1] == 87);
}

TEST_CASE("Write CSV - fields with double-quotes are escaped") {
    ibex::runtime::Table table;
    table.add_column("msg", ibex::Column<std::string>({"say \"hello\"", "world"}));

    auto out_path = tmp("ibex_test_write_escaped.csv");
    write_csv(table, out_path.string());

    auto reread = read_csv(out_path.string());
    REQUIRE(get_string_at(reread, "msg", 0) == "say \"hello\"");
    REQUIRE(get_string_at(reread, "msg", 1) == "world");
}

TEST_CASE("Write CSV - null values written as empty fields") {
    auto src_path = tmp("ibex_test_write_nulls_src.csv");
    write_csv(src_path, "price,note\n10,ok\n,NA\n30,\n");

    auto original = read_csv(src_path.string(), "<empty>,NA");
    auto out_path = tmp("ibex_test_write_nulls_out.csv");
    auto rows_written = write_csv(original, out_path.string());
    REQUIRE(rows_written == 3);

    // Re-read with the same null spec to verify round-trip.
    auto reread = read_csv(out_path.string(), "<empty>,NA");
    REQUIRE(reread.rows() == 3);
    REQUIRE_FALSE(is_null_at(reread, "price", 0));
    REQUIRE(is_null_at(reread, "price", 1));
    REQUIRE_FALSE(is_null_at(reread, "price", 2));
    REQUIRE_FALSE(is_null_at(reread, "note", 0));
    REQUIRE(is_null_at(reread, "note", 1));
    REQUIRE(is_null_at(reread, "note", 2));
}

TEST_CASE("Write CSV - empty table writes only header") {
    ibex::runtime::Table table;
    table.add_column("a", ibex::Column<std::int64_t>{});
    table.add_column("b", ibex::Column<std::string>{});

    auto out_path = tmp("ibex_test_write_empty.csv");
    auto rows_written = write_csv(table, out_path.string());
    REQUIRE(rows_written == 0);

    // File should contain only the header line.
    std::ifstream f(out_path);
    std::string line;
    REQUIRE(std::getline(f, line));
    REQUIRE(line == "a,b");
    REQUIRE_FALSE(std::getline(f, line));  // no data rows
}
