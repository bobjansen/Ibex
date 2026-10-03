// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The ADBC client as a C++ library, called the way a compiled program calls it:
// through adbc.hpp, with no plugin and no REPL. test_adbc.cpp covers the same
// functions behind the language; this checks the library on its own, since
// that is what a program built by ibex_compile links.
//
// Built only when IBEX_BUILD_ADBC=ON and the SQLite ADBC driver is installed.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>

#include "adbc.hpp"

namespace {

namespace stdfs = std::filesystem;

constexpr std::string_view kSqliteDriver = IBEX_ADBC_SQLITE_DRIVER;

/// A throwaway SQLite database file, removed with the object.
class TempDatabase {
   public:
    TempDatabase() {
        static std::atomic<unsigned> counter{0};
        std::error_code ec;
        path_ = stdfs::temp_directory_path(ec) /
                ("ibex_adbc_client_" + std::to_string(std::random_device{}()) + "_" +
                 std::to_string(counter++) + ".db");
    }
    ~TempDatabase() {
        std::error_code ec;
        stdfs::remove(path_, ec);
    }
    TempDatabase(const TempDatabase&) = delete;
    auto operator=(const TempDatabase&) -> TempDatabase& = delete;

    [[nodiscard]] auto uri() const -> std::string { return "file:" + path_.generic_string(); }

   private:
    stdfs::path path_;
};

auto int_column(const ibex::runtime::Table& table, const char* name) -> std::vector<std::int64_t> {
    const auto* column = std::get_if<ibex::Column<std::int64_t>>(table.find(name));
    REQUIRE(column != nullptr);
    return {column->begin(), column->end()};
}

auto numbers(std::initializer_list<std::int64_t> values) -> ibex::runtime::Table {
    ibex::Column<std::int64_t> column;
    for (const auto value : values) {
        column.push_back(value);
    }
    ibex::runtime::Table table;
    table.add_column("x", std::move(column));
    return table;
}

const ibex::runtime::Table kNoParams;

}  // namespace

TEST_CASE("adbc.hpp: adbc::read runs one query on a connection of its own", "[adbc_client]") {
    const auto table =
        ibex::ext::adbc::read(kSqliteDriver, "", "select 1 as x union all select 2", "");
    CHECK(int_column(table, "x") == std::vector<std::int64_t>{1, 2});
}

TEST_CASE("adbc.hpp: a connection executes, writes and queries", "[adbc_client]") {
    const TempDatabase database;
    const auto db = ibex::ext::adbc::connect(kSqliteDriver, database.uri(), "");

    CHECK(ibex::ext::adbc::execute(db, "create table t (x integer)", kNoParams) == 0);
    CHECK(ibex::ext::adbc::execute(db, "insert into t values (10), (20)", kNoParams) == 2);
    // Bulk-write three more rows, then read all five back through the same connection.
    CHECK(ibex::ext::adbc::write(db, numbers({30, 40, 50}), "t", "append") == 3);
    const auto rows = ibex::ext::adbc::query(db, "select x from t order by x", kNoParams);
    CHECK(int_column(rows, "x") == std::vector<std::int64_t>{10, 20, 30, 40, 50});

    // Parameters bind by position, one execution per row.
    const auto filtered =
        ibex::ext::adbc::query(db, "select x from t where x > ? order by x", numbers({30}));
    CHECK(int_column(filtered, "x") == std::vector<std::int64_t>{40, 50});

    const auto listing = ibex::ext::adbc::tables(db);
    CHECK(listing.rows() >= 1);
    CHECK(ibex::ext::adbc::close(db) == 1);
    CHECK(ibex::ext::adbc::close(db) == 0);
}

TEST_CASE("adbc.hpp: a transaction rolls back what it wrote", "[adbc_client]") {
    const TempDatabase database;
    const auto db = ibex::ext::adbc::connect(kSqliteDriver, database.uri(), "");
    (void)ibex::ext::adbc::execute(db, "create table t (x integer)", kNoParams);

    CHECK(ibex::ext::adbc::begin(db) == 1);
    (void)ibex::ext::adbc::execute(db, "insert into t values (1)", kNoParams);
    CHECK(ibex::ext::adbc::rollback(db) == 1);
    CHECK(ibex::ext::adbc::query(db, "select x from t", kNoParams).rows() == 0);

    CHECK(ibex::ext::adbc::begin(db) == 1);
    (void)ibex::ext::adbc::execute(db, "insert into t values (2)", kNoParams);
    CHECK(ibex::ext::adbc::commit(db) == 1);
    CHECK(int_column(ibex::ext::adbc::query(db, "select x from t", kNoParams), "x") ==
          std::vector<std::int64_t>{2});
}

TEST_CASE("adbc.hpp: copies of a connection alias one connection", "[adbc_client]") {
    const TempDatabase database;
    const auto db = ibex::ext::adbc::connect(kSqliteDriver, database.uri(), "");
    const ibex::ext::adbc::Connection alias = db;
    (void)ibex::ext::adbc::execute(db, "create table t (x integer)", kNoParams);

    // Closing through one name closes the connection for the other.
    CHECK(ibex::ext::adbc::close(db) == 1);
    CHECK_THROWS_WITH(ibex::ext::adbc::query(alias, "select 1 as x", kNoParams),
                      Catch::Matchers::ContainsSubstring("closed"));
}

TEST_CASE("adbc.hpp: failures throw the message the REPL prints", "[adbc_client]") {
    SECTION("a connection that was never opened") {
        const ibex::ext::adbc::Connection none;
        CHECK_THROWS_WITH(ibex::ext::adbc::query(none, "select 1", kNoParams),
                          Catch::Matchers::ContainsSubstring("the first argument must be an "
                                                             "adbc::Connection"));
    }
    SECTION("a driver that is not there") {
        CHECK_THROWS_AS(ibex::ext::adbc::connect("/nonexistent/driver.so", "", ""),
                        std::runtime_error);
    }
    SECTION("a statement that does not run") {
        const TempDatabase database;
        const auto db = ibex::ext::adbc::connect(kSqliteDriver, database.uri(), "");
        CHECK_THROWS_WITH(ibex::ext::adbc::execute(db, "not sql", kNoParams),
                          Catch::Matchers::ContainsSubstring("adbc::execute"));
    }
    SECTION("an unknown write mode") {
        const TempDatabase database;
        const auto db = ibex::ext::adbc::connect(kSqliteDriver, database.uri(), "");
        CHECK_THROWS_WITH(ibex::ext::adbc::write(db, numbers({1}), "t", "sideways"),
                          Catch::Matchers::ContainsSubstring("unknown mode"));
    }
}
