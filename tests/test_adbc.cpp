// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// ADBC integration tests. The built `adbc` plugin is loaded the way a user
// loads it -- `import "adbc";` in a REPL session -- and reads a throwaway
// SQLite database. The database is also seeded through `adbc_read` (one
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
#include <chrono>
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
#include <thread>
#include <variant>
#include <vector>

#include "exe_path.hpp"

namespace {

namespace stdfs = std::filesystem;

// Where the build put the plugin and found the SQLite driver. These are the
// build machine's paths; the helpers below also cover running the binary
// elsewhere, such as the Windows CI artifact on a user's machine.
constexpr std::string_view kBuildPluginDir = IBEX_ADBC_PLUGIN_DIR;
constexpr std::string_view kBuildSqliteDriver = IBEX_ADBC_SQLITE_DRIVER;

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

/// Where to look for the adbc plugin: next to this binary first (the Windows
/// artifact ships them together), then the build's plugin output directory.
auto plugin_search_paths() -> std::vector<std::string> {
    std::vector<std::string> paths;
    paths.reserve(2);
    if (const auto exe_dir = ibex::tools::executable_directory(); !exe_dir.empty()) {
        paths.push_back(exe_dir.string());
    }
    paths.emplace_back(kBuildPluginDir);
    return paths;
}

/// The SQLite driver library: $IBEX_ADBC_SQLITE_DRIVER, else the build's path,
/// else (Windows) where install_adbc_driver.ps1 installs it by default. Forward
/// slashes, since the path is embedded in Ibex string literals.
auto sqlite_driver() -> const std::string& {
    static const std::string driver = [] {
        if (auto env = get_env("IBEX_ADBC_SQLITE_DRIVER"); env.has_value() && !env->empty()) {
            return stdfs::path(*env).generic_string();
        }
        std::error_code ec;
        if (stdfs::exists(stdfs::path(kBuildSqliteDriver), ec)) {
            return std::string(kBuildSqliteDriver);
        }
#ifdef _WIN32
        if (auto local = get_env("LOCALAPPDATA"); local.has_value()) {
            const auto installed =
                stdfs::path(*local) / "ADBC" / "Drivers" / "sqlite" / "adbc_driver_sqlite.dll";
            if (stdfs::exists(installed, ec)) {
                return installed.generic_string();
            }
        }
#endif
        return std::string(kBuildSqliteDriver);
    }();
    return driver;
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

/// `adbc_read(<sqlite driver>, <db>, <sql>[, <options>])` as Ibex source.
auto read_call(const SqliteDb& db, std::string_view sql,
               std::optional<std::string_view> options = std::nullopt) -> std::string {
    std::string call = "adbc_read(" + ibex_str(sqlite_driver()) + ", " + ibex_str(db.path()) +
                       ", " + ibex_str(sql);
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
        REQUIRE(registry.find("adbc_read") != nullptr);
    }

    static auto config() -> ibex::repl::ReplConfig {
        ibex::repl::ReplConfig cfg;
        cfg.persistent_history = false;
        cfg.plugin_search_paths = plugin_search_paths();
        return cfg;
    }

    [[nodiscard]] auto function() const -> const ibex::runtime::ExternFunction& {
        const auto* fn = registry.find("adbc_read");
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

TEST_CASE("adbc_read accepts the documented three-argument form", "[adbc]") {
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

TEST_CASE("adbc_read reads every batch and feeds an Ibex aggregation", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);
    const std::string_view two_rows = "stmt.adbc.sqlite.query.batch_rows=2";

    SECTION("the chunked source yields one chunk per driver batch") {
        auto source = s.function().chunked_table_func(string_args(
            {sqlite_driver(), db.path(), "select id from trades order by id", two_rows}));
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

TEST_CASE("adbc_read carries SQL NULLs as validity", "[adbc]") {
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

TEST_CASE("adbc_read keeps the schema of an empty result", "[adbc]") {
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
        auto value = s.function().func(string_args({sqlite_driver(), db.path(), empty_sql}));
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

TEST_CASE("adbc_read materialized and chunked paths agree", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    auto value = s.function().func(
        string_args({sqlite_driver(), db.path(), "select id from trades order by id",
                     "stmt.adbc.sqlite.query.batch_rows=2"}));
    INFO((value.has_value() ? std::string{} : value.error()));
    REQUIRE(value.has_value());
    const auto* table = std::get_if<ibex::runtime::Table>(&*value);
    REQUIRE(table != nullptr);
    CHECK(ints(*table, "id") == std::vector<std::int64_t>{1, 2, 3, 4, 5});
}

TEST_CASE("adbc_read applies connection options before and after init", "[adbc]") {
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

TEST_CASE("adbc_read reports errors instead of failing silently", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);

    SECTION("a driver path that does not exist is named, whole") {
        const auto r = s.session.execute(
            "adbc_read(\"/nonexistent/libadbc_driver_nothing.so\", \"\", \"select 1\");");
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "adbc_read"));
        CHECK(
            contains(r.error, "driver library not found: /nonexistent/libadbc_driver_nothing.so"));
        // Left to the driver manager, a second dlopen for an invented name.
        CHECK_FALSE(contains(r.error, ".so.so"));
    }
    SECTION("a Windows drive path is not read as a driver:uri prefix") {
        // ADBC reads `C:` as a scheme and reported "Could not load `C`".
        const auto r = s.session.execute(
            "adbc_read(\"C:/nonexistent/adbc_driver_nothing.dll\", \"\", \"select 1\");");
        CHECK_FALSE(r.ok);
        CHECK(
            contains(r.error, "driver library not found: C:/nonexistent/adbc_driver_nothing.dll"));
        CHECK_FALSE(contains(r.error, "`C`"));
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

TEST_CASE("adbc_read resolves a bare driver name through a manifest", "[adbc]") {
    SqliteDb db;
    AdbcSession s;
    seed_trades(s, db);
    const ManifestDir manifests;
    manifests.add("ibex_test_sqlite", sqlite_driver());

    SECTION("a manifest on ADBC_DRIVER_PATH names the driver") {
        const auto r = s.session.execute("adbc_read(\"ibex_test_sqlite\", " + ibex_str(db.path()) +
                                         ", \"select count(*) as n from trades\");");
        INFO(r.error);
        REQUIRE(r.ok);
        CHECK(ints(*r.table, "n") == std::vector<std::int64_t>{5});
    }
    SECTION("an unknown name fails, says how to install one and where it looked") {
        // A session keeps an error's first line only, so the verdict and what
        // to do must both be on it.
        const auto r = s.session.execute("adbc_read(\"ibex_no_such_driver\", " +
                                         ibex_str(db.path()) + ", \"select 1\");");
        INFO(r.error);
        CHECK_FALSE(r.ok);
        CHECK(contains(r.error, "ADBC driver `ibex_no_such_driver` not found"));
        CHECK(contains(r.error, "install_adbc_driver"));
        // The full message keeps the driver manager's own search list.
        const auto direct =
            s.function().func(string_args({"ibex_no_such_driver", db.path(), "select 1"}));
        REQUIRE_FALSE(direct.has_value());
        INFO(direct.error());
        CHECK(contains(direct.error(), "Searched for a driver manifest in:"));
        CHECK(contains(direct.error(), "ADBC_DRIVER_PATH"));
    }
}

TEST_CASE("adbc_connect keeps one connection across queries", "[adbc][connection]") {
    AdbcSession s;
    SqliteDb db;
    seed_trades(s, db);
    const auto connect = s.session.execute("let db = adbc_connect(" + ibex_str(sqlite_driver()) +
                                           ", " + ibex_str(db.path()) + ");");
    INFO(connect.error);
    REQUIRE(connect.ok);

    // A temporary table exists only on the connection that made it.
    const auto temp = s.session.execute(
        "adbc_query(db, \"create temp table big as select id from trades where qty > 6\");");
    INFO(temp.error);
    REQUIRE(temp.ok);
    const auto reused = s.session.execute("adbc_query(db, \"select id from big order by id\");");
    INFO(reused.error);
    REQUIRE(reused.ok);
    REQUIRE(reused.table.has_value());
    CHECK(ints(*reused.table, "id") == std::vector<std::int64_t>{1, 3, 5});

    const auto one_off = s.session.execute(read_call(db, "select id from big") + ";");
    REQUIRE_FALSE(one_off.ok);
    CHECK(contains(one_off.error, "no such table"));

    // A query result is an ordinary table operand.
    const auto grouped = s.session.execute(
        "adbc_query(db, \"select symbol, qty from trades where symbol is not null\")"
        "[select { total = sum(qty) }, by symbol, order symbol];");
    INFO(grouped.error);
    REQUIRE(grouped.ok);
    REQUIRE(grouped.table.has_value());
    CHECK(grouped.table->rows() == 2);
}

TEST_CASE("adbc_close closes every alias once", "[adbc][connection]") {
    AdbcSession s;
    SqliteDb db;
    seed_trades(s, db);
    REQUIRE(s.session
                .execute("let db = adbc_connect(" + ibex_str(sqlite_driver()) + ", " +
                         ibex_str(db.path()) + ");\nlet alias = db;")
                .ok);

    const auto first = s.session.execute("adbc_close(alias);");
    REQUIRE(first.ok);
    REQUIRE(first.scalar.has_value());
    CHECK(std::get<std::int64_t>(*first.scalar) == 1);
    const auto second = s.session.execute("adbc_close(db);");
    REQUIRE(second.ok);
    CHECK(std::get<std::int64_t>(*second.scalar) == 0);

    const auto after = s.session.execute("adbc_query(db, \"select 1 as x\");");
    REQUIRE_FALSE(after.ok);
    CHECK(contains(after.error, "adbc_query: connection is closed"));
}

TEST_CASE("adbc_connect reports failures and misuse", "[adbc][connection]") {
    AdbcSession s;
    SqliteDb db;

    const auto missing = s.session.execute("let db = adbc_connect(\"ibex_no_such_driver\", \"\");");
    REQUIRE_FALSE(missing.ok);
    CHECK(contains(missing.error, "adbc_connect: ADBC driver `ibex_no_such_driver` not found"));

    const auto bad_option = s.session.execute("let db = adbc_connect(" + ibex_str(sqlite_driver()) +
                                              ", " + ibex_str(db.path()) + ", \"conn.nope=1\");");
    REQUIRE_FALSE(bad_option.ok);
    CHECK(contains(bad_option.error, "adbc_connect: AdbcConnectionInit failed"));

    const auto not_a_connection = s.session.execute("adbc_query(\"x\", \"select 1\");");
    REQUIRE_FALSE(not_a_connection.ok);
    CHECK(contains(not_a_connection.error, "expects a binding of type AdbcConnection"));
}

TEST_CASE("Functions take, open and return ADBC connections", "[adbc][connection]") {
    AdbcSession s;
    SqliteDb db;
    seed_trades(s, db);
    const auto declared = s.session.execute(
        "fn big_ids(mutable c: AdbcConnection) -> DataFrame {\n"
        "    adbc_query(c, \"select id from big order by id\");\n"
        "}\n"
        "fn open_db(uri: String) -> AdbcConnection {\n"
        "    adbc_connect(" +
        ibex_str(sqlite_driver()) +
        ", uri);\n"
        "}\n"
        "fn trade_count(uri: String) -> DataFrame {\n"
        "    let c = open_db(uri);\n"
        "    adbc_query(c, \"select count(*) as n from trades\");\n"
        "}\n"
        "let db = open_db(" +
        ibex_str(db.path()) + ");");
    INFO(declared.error);
    REQUIRE(declared.ok);

    // The function sees the caller's connection, and so its temporary table.
    REQUIRE(s.session
                .execute("adbc_query(db, \"create temp table big as select id from trades "
                         "where qty > 6\");")
                .ok);
    const auto shared = s.session.execute("big_ids(db);");
    INFO(shared.error);
    REQUIRE(shared.ok);
    CHECK(ints(*shared.table, "id") == std::vector<std::int64_t>{1, 3, 5});

    // A connection a function opens for itself is its own.
    const auto own = s.session.execute("trade_count(" + ibex_str(db.path()) + ");");
    INFO(own.error);
    REQUIRE(own.ok);
    CHECK(ints(*own.table, "n") == std::vector<std::int64_t>{5});

    const auto closed = s.session.execute("adbc_close(db);");
    REQUIRE(closed.ok);
    CHECK(std::get<std::int64_t>(*closed.scalar) == 1);
}

// Reusable connections against a real PostgreSQL server. Runs only when
// IBEX_TEST_POSTGRES_URI is set (the ADBC workflow's postgres service, or a
// local `docker run -e POSTGRES_PASSWORD=ibex -p 55432:5432 postgres:17`);
// IBEX_TEST_POSTGRES_DRIVER overrides the driver name. The server itself says
// which backend serves each query (pg_backend_pid) and whether a closed
// connection's backend is gone (pg_stat_activity).
TEST_CASE("adbc_connect against PostgreSQL", "[adbc][connection][postgresql]") {
    const auto uri = get_env("IBEX_TEST_POSTGRES_URI");
    if (!uri.has_value() || uri->empty()) {
        SKIP("IBEX_TEST_POSTGRES_URI is not set");
    }
    const std::string driver = get_env("IBEX_TEST_POSTGRES_DRIVER").value_or("postgresql");
    AdbcSession s;
    const auto exec = [&](const std::string& source) {
        auto result = s.session.execute(source);
        INFO(source);
        INFO(result.error);
        REQUIRE(result.ok);
        return result;
    };
    const auto pid_of = [&](const std::string& conn) {
        const auto r =
            exec("adbc_query(" + conn + ", \"select pg_backend_pid()::bigint as pid\");");
        REQUIRE(r.table.has_value());
        return ints(*r.table, "pid").at(0);
    };
    // Whether backend `pid` is still connected, seen from connection `via`.
    // A backend exits shortly after its client disconnects, so wait for it.
    const auto backend_gone = [&](const std::string& via, std::int64_t pid) {
        for (int attempt = 0; attempt < 50; ++attempt) {
            const auto r = exec("adbc_query(" + via +
                                ", \"select count(*) as n from pg_stat_activity where pid = " +
                                std::to_string(pid) + "\");");
            if (ints(*r.table, "n").at(0) == 0) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return false;
    };

    exec("let db = adbc_connect(" + ibex_str(driver) + ", " + ibex_str(*uri) +
         ");\n"
         "let other = adbc_connect(" +
         ibex_str(driver) + ", " + ibex_str(*uri) +
         ");\n"
         "fn pid_via(mutable c: AdbcConnection) -> DataFrame {\n"
         "    adbc_query(c, \"select pg_backend_pid()::bigint as pid\");\n"
         "}\n"
         "fn own_pid(uri: String) -> DataFrame {\n"
         "    let c = adbc_connect(" +
         ibex_str(driver) +
         ", uri);\n"
         "    adbc_query(c, \"select pg_backend_pid()::bigint as pid\");\n"
         "}\n"
         "fn open_db(uri: String) -> AdbcConnection {\n"
         "    adbc_connect(" +
         ibex_str(driver) +
         ", uri);\n"
         "}\n"
         "let ready = 1;");

    // One backend per connection, the same one for every query on it.
    const auto db_pid = pid_of("db");
    CHECK(pid_of("db") == db_pid);
    const auto other_pid = pid_of("other");
    CHECK(other_pid != db_pid);

    // Session state lives on the connection that made it.
    exec(
        "adbc_query(db, \"create temp table ibex_conn_t as select generate_series(1, 3)::bigint "
        "as id\");");
    const auto temp = exec("adbc_query(db, \"select id from ibex_conn_t order by id\");");
    CHECK(ints(*temp.table, "id") == std::vector<std::int64_t>{1, 2, 3});
    const auto elsewhere = s.session.execute("adbc_query(other, \"select id from ibex_conn_t\");");
    CHECK_FALSE(elsewhere.ok);
    const auto one_off = s.session.execute("adbc_read(" + ibex_str(driver) + ", " + ibex_str(*uri) +
                                           ", \"select id from ibex_conn_t\");");
    CHECK_FALSE(one_off.ok);

    // A failed query leaves the connection, and its session state, usable.
    const auto failed = s.session.execute("adbc_query(db, \"select * from ibex_no_such_table\");");
    CHECK_FALSE(failed.ok);
    const auto after = exec("adbc_query(db, \"select count(*) as n from ibex_conn_t\");");
    CHECK(ints(*after.table, "n") == std::vector<std::int64_t>{3});

    // A function taking the connection uses the caller's backend.
    const auto via = exec("pid_via(db);");
    CHECK(ints(*via.table, "pid").at(0) == db_pid);

    // A function's own connection is a new backend, gone once it returns.
    const auto own = exec("own_pid(" + ibex_str(*uri) + ");");
    const auto own_pid = ints(*own.table, "pid").at(0);
    CHECK(own_pid != db_pid);
    CHECK(backend_gone("db", own_pid));

    // A returned connection stays open for the caller until its binding goes.
    exec("let kept = open_db(" + ibex_str(*uri) + ");");
    const auto kept_pid = pid_of("kept");
    const auto alive =
        exec("adbc_query(db, \"select count(*) as n from pg_stat_activity where pid = " +
             std::to_string(kept_pid) + "\");");
    CHECK(ints(*alive.table, "n") == std::vector<std::int64_t>{1});
    exec("let kept = 0;");
    CHECK(backend_gone("db", kept_pid));

    // Closing through an alias closes the connection for every binding.
    exec("let alias = other;");
    const auto closed = exec("adbc_close(alias);");
    CHECK(std::get<std::int64_t>(*closed.scalar) == 1);
    const auto on_closed = s.session.execute("adbc_query(other, \"select 1 as x\");");
    CHECK_FALSE(on_closed.ok);
    CHECK(contains(on_closed.error, "connection is closed"));
    CHECK(backend_gone("db", other_pid));
}
