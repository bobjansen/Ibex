#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <sqlite.hpp>
#include <filesystem>
#include <string>

namespace {

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

/// Create a temporary SQLite database with a given SQL setup script.
auto make_db(const char* filename, const char* setup_sql) -> std::filesystem::path {
    auto path = tmp(filename);
    std::filesystem::remove(path);

    sqlite3* db = nullptr;
    sqlite3_open(path.c_str(), &db);
    char* err = nullptr;
    sqlite3_exec(db, setup_sql, nullptr, nullptr, &err);
    sqlite3_free(err);
    sqlite3_close(db);
    return path;
}

}  // namespace

// ---------------------------------------------------------------------------
// read_sqlite tests
// ---------------------------------------------------------------------------

TEST_CASE("Read SQLite - integer and text columns") {
    auto path = make_db("ibex_test_sqlite_basic.db",
                        "CREATE TABLE t (price INTEGER, symbol TEXT);"
                        "INSERT INTO t VALUES (10, 'A');"
                        "INSERT INTO t VALUES (20, 'B');"
                        "INSERT INTO t VALUES (30, 'A');");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 3);

    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 30);
    REQUIRE(get_string_at(table, "symbol", 0) == "A");
    REQUIRE(get_string_at(table, "symbol", 1) == "B");
    REQUIRE(get_string_at(table, "symbol", 2) == "A");
}

TEST_CASE("Read SQLite - real column detected as double") {
    auto path = make_db("ibex_test_sqlite_real.db",
                        "CREATE TABLE t (x REAL, y INTEGER);"
                        "INSERT INTO t VALUES (1.5, 10);"
                        "INSERT INTO t VALUES (2.25, 20);"
                        "INSERT INTO t VALUES (0.5, 5);");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 3);

    const auto* x = std::get_if<ibex::Column<double>>(table.find("x"));
    const auto* y = std::get_if<ibex::Column<std::int64_t>>(table.find("y"));
    REQUIRE(x != nullptr);
    REQUIRE(y != nullptr);
    REQUIRE((*x)[0] == Catch::Approx(1.5));
    REQUIRE((*x)[1] == Catch::Approx(2.25));
    REQUIRE((*x)[2] == Catch::Approx(0.5));
    REQUIRE((*y)[0] == 10);
    REQUIRE((*y)[2] == 5);
}

TEST_CASE("Read SQLite - mixed integer and real column promoted to double") {
    // SQLite allows different types per row; a column with both INTEGER and REAL
    // values should be inferred as double.
    auto path = make_db("ibex_test_sqlite_mixed_num.db",
                        "CREATE TABLE t (val NUMERIC);"
                        "INSERT INTO t VALUES (1);"
                        "INSERT INTO t VALUES (2.5);"
                        "INSERT INTO t VALUES (3);");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 3);

    const auto* col = std::get_if<ibex::Column<double>>(table.find("val"));
    REQUIRE(col != nullptr);
    REQUIRE((*col)[0] == Catch::Approx(1.0));
    REQUIRE((*col)[1] == Catch::Approx(2.5));
    REQUIRE((*col)[2] == Catch::Approx(3.0));
}

TEST_CASE("Read SQLite - nullable integer column") {
    auto path = make_db("ibex_test_sqlite_nullable_int.db",
                        "CREATE TABLE t (price INTEGER, qty INTEGER);"
                        "INSERT INTO t VALUES (10, 1);"
                        "INSERT INTO t VALUES (NULL, 2);"
                        "INSERT INTO t VALUES (30, NULL);");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 3);

    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[2] == 30);
    REQUIRE_FALSE(is_null_at(table, "price", 0));
    REQUIRE(is_null_at(table, "price", 1));
    REQUIRE_FALSE(is_null_at(table, "price", 2));

    const auto* qty = std::get_if<ibex::Column<std::int64_t>>(table.find("qty"));
    REQUIRE(qty != nullptr);
    REQUIRE_FALSE(is_null_at(table, "qty", 0));
    REQUIRE_FALSE(is_null_at(table, "qty", 1));
    REQUIRE(is_null_at(table, "qty", 2));
}

TEST_CASE("Read SQLite - nullable text column") {
    auto path = make_db("ibex_test_sqlite_nullable_text.db",
                        "CREATE TABLE t (name TEXT, tag TEXT);"
                        "INSERT INTO t VALUES ('Alice', 'dev');"
                        "INSERT INTO t VALUES ('Bob', NULL);"
                        "INSERT INTO t VALUES (NULL, 'qa');");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 3);

    REQUIRE(get_string_at(table, "name", 0) == "Alice");
    REQUIRE(get_string_at(table, "name", 1) == "Bob");
    REQUIRE_FALSE(is_null_at(table, "name", 0));
    REQUIRE_FALSE(is_null_at(table, "name", 1));
    REQUIRE(is_null_at(table, "name", 2));

    REQUIRE_FALSE(is_null_at(table, "tag", 0));
    REQUIRE(is_null_at(table, "tag", 1));
    REQUIRE_FALSE(is_null_at(table, "tag", 2));
}

TEST_CASE("Read SQLite - empty table returns zero rows") {
    auto path = make_db("ibex_test_sqlite_empty.db",
                        "CREATE TABLE t (id INTEGER, name TEXT);");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 0);
    // Columns should still be present (schema from column metadata).
    // SQLite doesn't expose schema without rows via SELECT *, so we just
    // verify the row count is 0.
}

TEST_CASE("Read SQLite - large int64 values") {
    auto path = make_db("ibex_test_sqlite_int64.db",
                        "CREATE TABLE t (ts INTEGER);"
                        "INSERT INTO t VALUES (9999999999);"
                        "INSERT INTO t VALUES (1000000000);");

    auto table = read_sqlite(path.string(), "t");
    REQUIRE(table.rows() == 2);

    const auto* ts = std::get_if<ibex::Column<std::int64_t>>(table.find("ts"));
    REQUIRE(ts != nullptr);
    REQUIRE((*ts)[0] == 9999999999LL);
    REQUIRE((*ts)[1] == 1000000000LL);
}

// ---------------------------------------------------------------------------
// write_sqlite tests
// ---------------------------------------------------------------------------

TEST_CASE("Write SQLite - integer and string columns round-trip") {
    auto path = tmp("ibex_test_sqlite_write_basic.db");
    std::filesystem::remove(path);

    ibex::runtime::Table table;
    table.add_column("price", ibex::Column<std::int64_t>({10, 20, 30}));
    table.add_column("symbol", ibex::Column<std::string>({"A", "B", "A"}));

    auto rows_written = write_sqlite(table, path.string(), "trades");
    REQUIRE(rows_written == 3);

    auto reread = read_sqlite(path.string(), "trades");
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

TEST_CASE("Write SQLite - double column round-trip") {
    auto path = tmp("ibex_test_sqlite_write_double.db");
    std::filesystem::remove(path);

    ibex::runtime::Table table;
    table.add_column("x", ibex::Column<double>({1.5, 2.25, 0.5}));

    auto rows_written = write_sqlite(table, path.string(), "data");
    REQUIRE(rows_written == 3);

    auto reread = read_sqlite(path.string(), "data");
    REQUIRE(reread.rows() == 3);

    const auto* x = std::get_if<ibex::Column<double>>(reread.find("x"));
    REQUIRE(x != nullptr);
    REQUIRE((*x)[0] == Catch::Approx(1.5));
    REQUIRE((*x)[1] == Catch::Approx(2.25));
    REQUIRE((*x)[2] == Catch::Approx(0.5));
}

TEST_CASE("Write SQLite - null values round-trip") {
    auto path = tmp("ibex_test_sqlite_write_nulls.db");
    std::filesystem::remove(path);

    ibex::runtime::Table table;
    {
        ibex::Column<std::int64_t> price_col({10, 0, 30});
        std::vector<bool> price_valid = {true, false, true};
        table.add_column("price", std::move(price_col), std::move(price_valid));
    }
    {
        ibex::Column<std::string> note_col({"ok", "", ""});
        std::vector<bool> note_valid = {true, false, false};
        table.add_column("note", std::move(note_col), std::move(note_valid));
    }

    auto rows_written = write_sqlite(table, path.string(), "events");
    REQUIRE(rows_written == 3);

    auto reread = read_sqlite(path.string(), "events");
    REQUIRE(reread.rows() == 3);

    REQUIRE_FALSE(is_null_at(reread, "price", 0));
    REQUIRE(is_null_at(reread, "price", 1));
    REQUIRE_FALSE(is_null_at(reread, "price", 2));

    REQUIRE_FALSE(is_null_at(reread, "note", 0));
    REQUIRE(is_null_at(reread, "note", 1));
    REQUIRE(is_null_at(reread, "note", 2));
}

TEST_CASE("Write SQLite - overwrites existing table") {
    auto path = tmp("ibex_test_sqlite_write_overwrite.db");
    std::filesystem::remove(path);

    ibex::runtime::Table first;
    first.add_column("v", ibex::Column<std::int64_t>({1, 2, 3}));
    write_sqlite(first, path.string(), "t");

    ibex::runtime::Table second;
    second.add_column("v", ibex::Column<std::int64_t>({99, 100}));
    auto rows_written = write_sqlite(second, path.string(), "t");
    REQUIRE(rows_written == 2);

    auto reread = read_sqlite(path.string(), "t");
    REQUIRE(reread.rows() == 2);

    const auto* v = std::get_if<ibex::Column<std::int64_t>>(reread.find("v"));
    REQUIRE(v != nullptr);
    REQUIRE((*v)[0] == 99);
    REQUIRE((*v)[1] == 100);
}

TEST_CASE("Write SQLite - empty table creates schema only") {
    auto path = tmp("ibex_test_sqlite_write_empty.db");
    std::filesystem::remove(path);

    ibex::runtime::Table table;
    table.add_column("a", ibex::Column<std::int64_t>{});
    table.add_column("b", ibex::Column<std::string>{});

    auto rows_written = write_sqlite(table, path.string(), "empty_tbl");
    REQUIRE(rows_written == 0);

    // Table should exist but have zero rows.
    auto reread = read_sqlite(path.string(), "empty_tbl");
    REQUIRE(reread.rows() == 0);
}

TEST_CASE("Write SQLite - categorical column stored as text") {
    auto path = tmp("ibex_test_sqlite_write_categorical.db");
    std::filesystem::remove(path);

    ibex::runtime::Table table;
    table.add_column("side",
                     ibex::Column<ibex::Categorical>({"buy", "sell"}, {0, 1, 0, 0, 1}));
    table.add_column("qty", ibex::Column<std::int64_t>({100, 200, 150, 120, 80}));

    auto rows_written = write_sqlite(table, path.string(), "orders");
    REQUIRE(rows_written == 5);

    auto reread = read_sqlite(path.string(), "orders");
    REQUIRE(reread.rows() == 5);
    REQUIRE(get_string_at(reread, "side", 0) == "buy");
    REQUIRE(get_string_at(reread, "side", 1) == "sell");
    REQUIRE(get_string_at(reread, "side", 2) == "buy");
}
