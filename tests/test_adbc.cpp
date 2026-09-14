// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// ADBC integration tests. The built `adbc` plugin is loaded the way a user
// loads it -- `import "adbc";` in a REPL session -- and reads a throwaway
// SQLite database. The database is also seeded through `read_adbc` (one
// statement per call), which keeps this binary free of any ADBC link: the
// driver manager lives only in the plugin, as it does in the ibex tool.
//
// Built only when IBEX_BUILD_ADBC=ON and the SQLite ADBC driver is installed.

#include <ibex/core/column.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>

namespace {

namespace stdfs = std::filesystem;

constexpr std::string_view kPluginDir = IBEX_ADBC_PLUGIN_DIR;
constexpr std::string_view kSqliteDriver = IBEX_ADBC_SQLITE_DRIVER;

/// Keeps this process's temp files apart from a concurrent test run's.
auto process_token() -> const std::string& {
    static const std::string token = std::to_string(std::random_device{}());
    return token;
}

// Environment access that also works on Windows, where the driver manager reads
// the process environment (which _putenv_s updates) and std::getenv is deprecated.
auto get_env(const char* name) -> std::optional<std::string> {
#ifdef _WIN32
    char* value = nullptr;
    std::size_t size = 0;
    if (_dupenv_s(&value, &size, name) != 0 || value == nullptr) {
        return std::nullopt;
    }
    std::string result(value);
    std::free(value);
    return result;
#else
    const char* value = std::getenv(name);
    if (value == nullptr) {
        return std::nullopt;
    }
    return std::string(value);
#endif
}

void set_env(const char* name, const std::string& value) {
#ifdef _WIN32
    _putenv_s(name, value.c_str());
#else
    setenv(name, value.c_str(), 1);
#endif
}

void unset_env(const char* name) {
#ifdef _WIN32
    _putenv_s(name, "");  // an empty value removes the variable
#else
    unsetenv(name);
#endif
}

/// A SQLite database file that lives for one test.
class SqliteDb {
   public:
    SqliteDb() {
        static std::atomic<int> counter{0};
        const std::string file = "ibex_adbc_test_" + process_token() + "_" +
                                 std::to_string(counter.fetch_add(1)) + ".sqlite";
        path_ = stdfs::temp_directory_path() / file;
        std::error_code ec;
        stdfs::remove(path_, ec);
    }
    ~SqliteDb() {
        std::error_code ec;
        stdfs::remove(path_, ec);
    }
    SqliteDb(const SqliteDb&) = delete;
    SqliteDb& operator=(const SqliteDb&) = delete;
    SqliteDb(SqliteDb&&) = delete;
    SqliteDb& operator=(SqliteDb&&) = delete;

    /// Forward slashes on every platform: the path is embedded in Ibex string
    /// literals, where a backslash is an escape.
    [[nodiscard]] auto path() const -> std::string { return path_.generic_string(); }

   private:
    stdfs::path path_;
};

/// `text` as an Ibex string literal. Test paths and SQL contain no quotes or
/// backslashes.
auto ibex_str(std::string_view text) -> std::string {
    return "\"" + std::string(text) + "\"";
}

/// `read_adbc(<sqlite driver>, <db>, <sql>[, <options>])` as Ibex source.
auto read_call(const SqliteDb& db, std::string_view sql,
               std::optional<std::string_view> options = std::nullopt) -> std::string {
    std::string call =
        "read_adbc(" + ibex_str(kSqliteDriver) + ", " + ibex_str(db.path()) + ", " + ibex_str(sql);
    if (options.has_value()) {
        call += ", " + ibex_str(*options);
    }
    return call + ")";
}

auto string_args(std::initializer_list<std::string_view> values) -> ibex::runtime::ExternArgs {
    ibex::runtime::ExternArgs args;
    args.reserve(values.size());
    for (const auto value : values) {
        args.emplace_back(std::string(value));
    }
    return args;
}

/// A REPL session with the adbc plugin imported.
struct AdbcSession {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session;

    AdbcSession() : session(config(), registry) {
        const auto imported = session.execute("import \"adbc\";");
        INFO(imported.error);
        REQUIRE(imported.ok);
        REQUIRE(registry.find("read_adbc") != nullptr);
    }

    static auto config() -> ibex::repl::ReplConfig {
        ibex::repl::ReplConfig cfg;
        cfg.persistent_history = false;
        cfg.plugin_search_paths = {std::string(kPluginDir)};
        return cfg;
    }

    [[nodiscard]] auto function() const -> const ibex::runtime::ExternFunction& {
        const auto* fn = registry.find("read_adbc");
        REQUIRE(fn != nullptr);
        return *fn;
    }

    /// Run statements that return no result set, one connection each.
    void exec(const SqliteDb& db, std::initializer_list<std::string_view> statements) {
        for (const auto sql : statements) {
            const auto r = session.execute(read_call(db, sql) + ";");
            INFO(sql);
            INFO(r.error);
            REQUIRE(r.ok);
        }
    }
};

/// Five trades; row 4 has a NULL qty and row 5 a NULL symbol.
void seed_trades(AdbcSession& s, const SqliteDb& db) {
    s.exec(db, {
                   "create table trades (id integer, symbol text, qty integer, px real)",
                   "insert into trades values (1, 'AAPL', 10, 150.0)",
                   "insert into trades values (2, 'MSFT', 5, 300.0)",
                   "insert into trades values (3, 'AAPL', 20, 151.0)",
                   "insert into trades values (4, 'MSFT', null, 301.0)",
                   "insert into trades values (5, null, 7, 99.5)",
               });
}

auto column_size(const ibex::runtime::ColumnEntry& entry) -> std::size_t {
    return std::visit([](const auto& column) { return column.size(); }, *entry.column);
}

auto ints(const ibex::runtime::Table& table, const std::string& name) -> std::vector<std::int64_t> {
    const auto* column = std::get_if<ibex::Column<std::int64_t>>(table.find(name));
    REQUIRE(column != nullptr);
    return {column->begin(), column->end()};
}

auto contains(const std::string& haystack, std::string_view needle) -> bool {
    return haystack.find(needle) != std::string::npos;
}

}  // namespace

TEST_CASE("read_adbc accepts the documented three-argument form", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    const auto r =
        s.session.execute(read_call(db, "select id, symbol from trades order by id") + ";");
    INFO(r.error);
    REQUIRE(r.ok);
    REQUIRE(r.table.has_value());
    CHECK(r.table->rows() == 5);
    CHECK(ints(*r.table, "id") == std::vector<std::int64_t>{1, 2, 3, 4, 5});
    CHECK(r.table->find("symbol") != nullptr);
}

TEST_CASE("read_adbc reads every batch and feeds an Ibex aggregation", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);
    const std::string_view two_rows = "stmt.adbc.sqlite.query.batch_rows=2";

    SECTION("the chunked source yields one chunk per driver batch") {
        auto source = s.function().chunked_table_func(
            string_args({kSqliteDriver, db.path(), "select id from trades order by id", two_rows}));
        INFO((source.has_value() ? std::string{} : source.error()));
        REQUIRE(source.has_value());
        std::vector<std::size_t> chunk_rows;
        while (true) {
            auto chunk = (*source)->next();
            REQUIRE(chunk.has_value());
            if (!chunk->has_value()) {
                break;
            }
            REQUIRE((*chunk)->columns.size() == 1);
            chunk_rows.push_back(column_size((*chunk)->columns.front()));
        }
        CHECK(chunk_rows == std::vector<std::size_t>{2, 2, 1});
    }

    SECTION("the REPL aggregates across batches") {
        const auto bound =
            s.session.execute("let t = " +
                              read_call(db,
                                        "select symbol, qty, px from trades "
                                        "where symbol is not null and qty is not null",
                                        two_rows) +
                              ";");
        INFO(bound.error);
        REQUIRE(bound.ok);
        const auto r =
            s.session.execute("t[select { notional = sum(qty * px) }, by symbol, order symbol];");
        INFO(r.error);
        REQUIRE(r.ok);
        REQUIRE(r.table.has_value());
        REQUIRE(r.table->rows() == 2);
        const auto* notional = std::get_if<ibex::Column<double>>(r.table->find("notional"));
        REQUIRE(notional != nullptr);
        CHECK((*notional)[0] == (10 * 150.0) + (20 * 151.0));  // AAPL
        CHECK((*notional)[1] == 5 * 300.0);                    // MSFT
    }
}

TEST_CASE("read_adbc carries SQL NULLs as validity", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    const auto r =
        s.session.execute(read_call(db, "select id, symbol, qty from trades order by id") + ";");
    INFO(r.error);
    REQUIRE(r.ok);
    REQUIRE(r.table.has_value());

    const auto* qty = r.table->find_entry("qty");
    REQUIRE(qty != nullptr);
    REQUIRE(qty->validity.has_value());
    CHECK((*qty->validity)[0]);
    CHECK_FALSE((*qty->validity)[3]);

    const auto* symbol = r.table->find_entry("symbol");
    REQUIRE(symbol != nullptr);
    REQUIRE(symbol->validity.has_value());
    CHECK((*symbol->validity)[3]);
    CHECK_FALSE((*symbol->validity)[4]);
}

TEST_CASE("read_adbc keeps the schema of an empty result", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);
    const std::string_view empty_sql = "select id, symbol from trades where id < 0";

    SECTION("through the REPL, columns can still be selected") {
        REQUIRE(s.session.execute("let e = " + read_call(db, empty_sql) + ";").ok);
        const auto r = s.session.execute("e[select { id }];");
        INFO(r.error);
        REQUIRE(r.ok);
        REQUIRE(r.table.has_value());
        CHECK(r.table->rows() == 0);
        CHECK(r.table->find("id") != nullptr);
    }

    SECTION("the materialized path returns both columns") {
        auto value = s.function().func(string_args({kSqliteDriver, db.path(), empty_sql}));
        INFO((value.has_value() ? std::string{} : value.error()));
        REQUIRE(value.has_value());
        const auto* table = std::get_if<ibex::runtime::Table>(&*value);
        REQUIRE(table != nullptr);
        CHECK(table->rows() == 0);
        CHECK(table->columns.size() == 2);
        CHECK(table->find("id") != nullptr);
        CHECK(table->find("symbol") != nullptr);
    }
}

TEST_CASE("read_adbc materialized and chunked paths agree", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    auto value = s.function().func(
        string_args({kSqliteDriver, db.path(), "select id from trades order by id",
                     "stmt.adbc.sqlite.query.batch_rows=2"}));
    INFO((value.has_value() ? std::string{} : value.error()));
    REQUIRE(value.has_value());
    const auto* table = std::get_if<ibex::runtime::Table>(&*value);
    REQUIRE(table != nullptr);
    CHECK(ints(*table, "id") == std::vector<std::int64_t>{1, 2, 3, 4, 5});
}

TEST_CASE("read_adbc applies connection options before and after init", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    // The documented example from docs/io.html: extension loading can only be
    // enabled on an open SQLite connection.
    for (const std::string_view options : {"conn.post.adbc.connection.autocommit=true",
                                           "stmt.adbc.sqlite.query.batch_rows=10000;"
                                           "conn.post.adbc.sqlite.load_extension.enabled=true"}) {
        INFO(options);
        const auto r =
            s.session.execute(read_call(db, "select count(*) as n from trades", options) + ";");
        INFO(r.error);
        REQUIRE(r.ok);
        REQUIRE(r.table.has_value());
        CHECK(ints(*r.table, "n") == std::vector<std::int64_t>{5});
    }
}

TEST_CASE("read_adbc reports errors instead of failing silently", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    SECTION("an unloadable driver") {
        const auto r = s.session.execute(
            "read_adbc(\"/nonexistent/libadbc_driver_nothing.so\", \"\", \"select 1\");");
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "read_adbc"));
    }
    SECTION("a malformed options string") {
        const auto r = s.session.execute(read_call(db, "select 1 as x", "novalue") + ";");
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "key=value"));
    }
    SECTION("an option the driver rejects names the option") {
        const auto r = s.session.execute(
            read_call(db, "select 1 as x", "stmt.adbc.sqlite.query.batch_rows=notanumber") + ";");
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "adbc.sqlite.query.batch_rows"));
    }
    SECTION("invalid SQL") {
        const auto r = s.session.execute(read_call(db, "select * from no_such_table") + ";");
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "no_such_table"));
    }
    SECTION("repeated failures leave the session usable") {
        // Each of these fails after AdbcDatabaseNew, the path that used to skip
        // AdbcDatabaseRelease. Under the sanitizer build (CI) a leak here fails
        // the run; without it this at least proves teardown is clean.
        for (int i = 0; i < 50; ++i) {
            const auto r = s.session.execute(
                read_call(db, "select 1 as x", "db.no_such_database_option=1") + ";");
            CHECK_FALSE(r.ok);
        }
        const auto r = s.session.execute(read_call(db, "select count(*) as n from trades") + ";");
        INFO(r.error);
        REQUIRE(r.ok);
        CHECK(ints(*r.table, "n") == std::vector<std::int64_t>{5});
    }
}

namespace {

/// A throwaway ADBC_DRIVER_PATH directory; restores the previous value on exit.
class ManifestDir {
   public:
    ManifestDir() {
        dir_ = stdfs::temp_directory_path() / ("ibex_adbc_manifests_" + process_token());
        stdfs::create_directories(dir_);
        previous_ = get_env("ADBC_DRIVER_PATH");
        set_env("ADBC_DRIVER_PATH", dir_.string());
    }
    ~ManifestDir() {
        if (previous_.has_value()) {
            set_env("ADBC_DRIVER_PATH", *previous_);
        } else {
            unset_env("ADBC_DRIVER_PATH");
        }
        std::error_code ec;
        stdfs::remove_all(dir_, ec);
    }
    ManifestDir(const ManifestDir&) = delete;
    ManifestDir& operator=(const ManifestDir&) = delete;
    ManifestDir(ManifestDir&&) = delete;
    ManifestDir& operator=(ManifestDir&&) = delete;

    /// Write `<name>.toml` pointing at `library` (single-path form: any platform).
    void add(std::string_view name, std::string_view library) const {
        std::ofstream manifest(dir_ / (std::string(name) + ".toml"));
        manifest << "manifest_version = 1\n"
                 << "name = 'Ibex test driver'\n\n"
                 << "[Driver]\n"
                 << "shared = '" << library << "'\n";
    }

   private:
    stdfs::path dir_;
    std::optional<std::string> previous_;
};

}  // namespace

TEST_CASE("read_adbc resolves a bare driver name through a manifest", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);
    const ManifestDir manifests;
    manifests.add("ibex_test_sqlite", kSqliteDriver);

    SECTION("a manifest on ADBC_DRIVER_PATH names the driver") {
        const auto r = s.session.execute("read_adbc(\"ibex_test_sqlite\", " + ibex_str(db.path()) +
                                         ", \"select count(*) as n from trades\");");
        INFO(r.error);
        REQUIRE(r.ok);
        CHECK(ints(*r.table, "n") == std::vector<std::int64_t>{5});
    }
    SECTION("an unknown name fails and says which name") {
        const auto r = s.session.execute("read_adbc(\"ibex_no_such_driver\", " +
                                         ibex_str(db.path()) + ", \"select 1\");");
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "ibex_no_such_driver"));
    }
}
