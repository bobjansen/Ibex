// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// ADBC integration tests. The built `adbc` plugin is loaded the way a user
// loads it -- `import "adbc";` in a REPL session -- and reads a throwaway
// SQLite database. The database is also seeded through the plugin
// (`adbc_execute`), which keeps this binary free of any ADBC link: the
// driver manager lives only in the plugin, as it does in the ibex tool.
//
// Built only when IBEX_BUILD_ADBC=ON and the SQLite ADBC driver is installed.

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
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

    /// Run statements that return no result set, on one connection.
    void exec(const SqliteDb& db, std::initializer_list<std::string_view> statements) {
        const auto opened =
            session.execute("let seed_conn = adbc_connect(" + ibex_str(sqlite_driver()) + ", " +
                            ibex_str(db.path()) + ");");
        INFO(opened.error);
        REQUIRE(opened.ok);
        for (const auto sql : statements) {
            const auto r = session.execute("adbc_execute(seed_conn, " + ibex_str(sql) + ");");
            INFO(sql);
            INFO(r.error);
            REQUIRE(r.ok);
        }
        REQUIRE(session.execute("adbc_close(seed_conn);").ok);
    }

    /// A plugin function by name.
    [[nodiscard]] auto plugin(const std::string& name) const
        -> const ibex::runtime::ExternFunction& {
        const auto* fn = registry.find(name);
        REQUIRE(fn != nullptr);
        REQUIRE(fn->func);
        return *fn;
    }

    /// A connection opened by calling the plugin directly.
    [[nodiscard]] auto connect(std::string_view driver, std::string_view uri) const
        -> ibex::runtime::ResourcePtr {
        auto opened = plugin("adbc_connect").func(string_args({driver, uri}));
        INFO((opened.has_value() ? std::string{} : opened.error()));
        REQUIRE(opened.has_value());
        auto* resource = std::get_if<ibex::runtime::ResourcePtr>(&*opened);
        REQUIRE(resource != nullptr);
        return *resource;
    }

    /// `adbc_write(conn, table, target, mode)`, called on the plugin directly.
    [[nodiscard]] auto write(const ibex::runtime::ResourcePtr& conn, ibex::runtime::Table table,
                             std::string_view target, std::string_view mode) const
        -> std::expected<std::int64_t, std::string> {
        ibex::runtime::ExternArgs args;
        args.push_resource(conn);
        args.push_table(std::make_shared<const ibex::runtime::Table>(std::move(table)));
        args.emplace_back(std::string(target));
        args.emplace_back(std::string(mode));
        auto written = plugin("adbc_write").func(args);
        if (!written) {
            return std::unexpected(written.error());
        }
        return std::get<std::int64_t>(std::get<ibex::runtime::ScalarValue>(*written));
    }

    /// `adbc_query(conn, sql)`, called on the plugin directly.
    [[nodiscard]] auto query(const ibex::runtime::ResourcePtr& conn, std::string_view sql) const
        -> ibex::runtime::Table {
        ibex::runtime::ExternArgs args;
        args.push_resource(conn);
        args.emplace_back(std::string(sql));
        auto result = plugin("adbc_query").func(args);
        INFO(sql);
        INFO((result.has_value() ? std::string{} : result.error()));
        REQUIRE(result.has_value());
        auto* table = std::get_if<ibex::runtime::Table>(&*result);
        REQUIRE(table != nullptr);
        return std::move(*table);
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

auto doubles(const ibex::runtime::Table& table, const std::string& name) -> std::vector<double> {
    const auto* column = std::get_if<ibex::Column<double>>(table.find(name));
    REQUIRE(column != nullptr);
    return {column->begin(), column->end()};
}

auto strings(const ibex::runtime::Table& table, const std::string& name)
    -> std::vector<std::string> {
    const auto* column = std::get_if<ibex::Column<std::string>>(table.find(name));
    REQUIRE(column != nullptr);
    std::vector<std::string> values;
    values.reserve(column->size());
    for (std::size_t i = 0; i < column->size(); ++i) {
        values.emplace_back((*column)[i]);
    }
    return values;
}

/// Which rows of column `name` are null.
auto nulls(const ibex::runtime::Table& table, const std::string& name) -> std::vector<bool> {
    const auto* entry = table.find_entry(name);
    REQUIRE(entry != nullptr);
    std::vector<bool> result;
    result.reserve(column_size(*entry));
    for (std::size_t i = 0; i < column_size(*entry); ++i) {
        result.push_back(ibex::runtime::is_null(*entry, i));
    }
    return result;
}

/// One column of every Ibex type but Decimal, three rows, row 2 null in all
/// but `i`. 2026-01-02 is day 20455 and 1999-12-31 day 10956; the timestamp
/// is 2026-01-02 03:04:05.123456789.
auto typed_table() -> ibex::runtime::Table {
    using ibex::runtime::ValidityBitmap;
    const ValidityBitmap middle_null{true, false, true};
    ibex::runtime::Table table;
    table.add_column("i", ibex::Column<std::int64_t>{1, 0, -3});
    table.add_column("f", ibex::Column<double>{1.5, 0.0, -2.25}, middle_null);
    table.add_column("b", ibex::Column<bool>{true, false, false}, middle_null);
    table.add_column("s", ibex::Column<std::string>{"a", "", "c"}, middle_null);
    ibex::Column<ibex::Categorical> sym;
    sym.push_back("AAPL");
    sym.push_back("MSFT");
    sym.push_back("AAPL");
    table.add_column("sym", std::move(sym), middle_null);
    table.add_column(
        "d", ibex::Column<ibex::Date>{{ibex::Date{20455}, ibex::Date{0}, ibex::Date{10956}}},
        middle_null);
    table.add_column("ts",
                     ibex::Column<ibex::Timestamp>{{ibex::Timestamp{1767323045123456789},
                                                    ibex::Timestamp{0}, ibex::Timestamp{0}}},
                     middle_null);
    return table;
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
    SECTION("a column Ibex has no type for is named, with a SQL cast") {
        const auto r = s.session.execute(read_call(db, "select 1 as id, x'0102' as blob") + ";");
        REQUIRE_FALSE(r.ok);
        CHECK(contains(r.error,
                       "adbc_read: batch import failed: column `blob`: Arrow binary has "
                       "no Ibex column type; cast it in the query, e.g. "
                       "CAST(blob AS TEXT)"));
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

TEST_CASE("adbc_execute runs statements that return no rows", "[adbc][write]") {
    AdbcSession s;
    SqliteDb db;
    const auto exec = [&](const std::string& source) {
        auto result = s.session.execute(source);
        INFO(source);
        INFO(result.error);
        REQUIRE(result.ok);
        return result;
    };
    exec("let db = adbc_connect(" + ibex_str(sqlite_driver()) + ", " + ibex_str(db.path()) + ");");
    exec("adbc_execute(db, \"create table t (id integer, qty integer)\");");

    const auto inserted =
        exec("adbc_execute(db, \"insert into t values (1, 10), (2, 20), (3, 30)\");");
    REQUIRE(inserted.scalar.has_value());
    CHECK(std::get<std::int64_t>(*inserted.scalar) == 3);
    const auto updated = exec("adbc_execute(db, \"update t set qty = qty + 1 where id > 1\");");
    CHECK(std::get<std::int64_t>(*updated.scalar) == 2);
    const auto deleted = exec("adbc_execute(db, \"delete from t where id = 3\");");
    CHECK(std::get<std::int64_t>(*deleted.scalar) == 1);

    const auto rows = exec("adbc_query(db, \"select qty from t order by id\");");
    CHECK(ints(*rows.table, "qty") == std::vector<std::int64_t>{10, 21});

    // A failed statement says which function failed and leaves the
    // connection usable.
    const auto bad = s.session.execute("adbc_execute(db, \"insert into nope values (1)\");");
    REQUIRE_FALSE(bad.ok);
    CHECK(contains(bad.error, "adbc_execute:"));
    CHECK(contains(bad.error, "no such table"));
    exec("adbc_execute(db, \"delete from t\");");

    exec("adbc_close(db);");
    const auto closed = s.session.execute("adbc_execute(db, \"delete from t\");");
    REQUIRE_FALSE(closed.ok);
    CHECK(contains(closed.error, "adbc_execute: connection is closed"));
}

TEST_CASE("adbc_write stores every Ibex column type in SQLite", "[adbc][write]") {
    AdbcSession s;
    SqliteDb db;
    const auto conn = s.connect(sqlite_driver(), db.path());

    const auto written = s.write(conn, typed_table(), "typed", "create");
    INFO((written.has_value() ? std::string{} : written.error()));
    REQUIRE(written.has_value());
    CHECK(*written == 3);

    // SQLite has no bool, date or timestamp type: the driver stores 0/1 and
    // ISO 8601 text, and a query reads back what SQLite holds.
    const auto back = s.query(conn, "select * from typed order by rowid");
    CHECK(ints(back, "i") == std::vector<std::int64_t>{1, 0, -3});
    CHECK(doubles(back, "f").at(0) == 1.5);
    CHECK(doubles(back, "f").at(2) == -2.25);
    CHECK(ints(back, "b").at(0) == 1);
    CHECK(ints(back, "b").at(2) == 0);
    CHECK(strings(back, "s").at(2) == "c");
    CHECK(strings(back, "sym").at(0) == "AAPL");
    CHECK(strings(back, "sym").at(2) == "AAPL");
    CHECK(strings(back, "d").at(0) == "2026-01-02");
    CHECK(strings(back, "d").at(2) == "1999-12-31");
    CHECK(strings(back, "ts").at(0) == "2026-01-02T03:04:05.123456789");
    const std::vector<bool> middle{false, true, false};
    for (const auto* name : {"f", "b", "s", "sym", "d", "ts"}) {
        INFO(name);
        CHECK(nulls(back, name) == middle);
    }
    CHECK(nulls(back, "i") == std::vector<bool>{false, false, false});

    // The SQLite driver has no decimal type; its refusal names the type.
    ibex::runtime::Table amounts;
    auto amount = ibex::runtime::make_decimal_column({.precision = 12, .scale = 2});
    amount.push_back(ibex::Decimal{150});
    amounts.add_column("amount", std::move(amount));
    const auto refused = s.write(conn, std::move(amounts), "amounts", "create");
    REQUIRE_FALSE(refused.has_value());
    CHECK(contains(refused.error(), "adbc_write:"));
    CHECK(contains(refused.error(), "decimal128"));
}

TEST_CASE("adbc_write modes create, append, replace and create_append", "[adbc][write]") {
    AdbcSession s;
    SqliteDb db;
    const auto conn = s.connect(sqlite_driver(), db.path());
    const auto ids = [](std::initializer_list<std::int64_t> values) {
        ibex::runtime::Table table;
        table.add_column("id", ibex::Column<std::int64_t>(values));
        return table;
    };
    const auto stored = [&] { return ints(s.query(conn, "select id from t order by id"), "id"); };

    REQUIRE(s.write(conn, ids({1, 2}), "t", "create").has_value());
    CHECK(stored() == std::vector<std::int64_t>{1, 2});

    const auto again = s.write(conn, ids({9}), "t", "create");
    REQUIRE_FALSE(again.has_value());
    CHECK(contains(again.error(), "already exists"));

    REQUIRE(s.write(conn, ids({3}), "t", "append").has_value());
    CHECK(stored() == std::vector<std::int64_t>{1, 2, 3});

    const auto missing = s.write(conn, ids({1}), "no_such_table", "append");
    REQUIRE_FALSE(missing.has_value());
    CHECK(contains(missing.error(), "no such table"));

    REQUIRE(s.write(conn, ids({7, 8}), "t", "replace").has_value());
    CHECK(stored() == std::vector<std::int64_t>{7, 8});

    REQUIRE(s.write(conn, ids({1}), "fresh", "create_append").has_value());
    REQUIRE(s.write(conn, ids({2}), "fresh", "create_append").has_value());
    CHECK(ints(s.query(conn, "select id from fresh order by id"), "id") ==
          std::vector<std::int64_t>{1, 2});

    const auto bogus = s.write(conn, ids({1}), "t", "upsert");
    REQUIRE_FALSE(bogus.has_value());
    CHECK(contains(bogus.error(), "adbc_write: unknown mode 'upsert'"));

    // An empty table still creates its columns.
    ibex::runtime::Table empty;
    empty.add_column("id", ibex::Column<std::int64_t>{});
    empty.add_column("name", ibex::Column<std::string>{});
    const auto created = s.write(conn, std::move(empty), "empty_t", "create");
    REQUIRE(created.has_value());
    CHECK(*created == 0);
    const auto columns =
        s.query(conn, "select name from pragma_table_info('empty_t') order by cid");
    CHECK(strings(columns, "name") == std::vector<std::string>{"id", "name"});
}

TEST_CASE("A failed adbc_write writes nothing and keeps the connection", "[adbc][write]") {
    AdbcSession s;
    SqliteDb db;
    s.exec(db, {"create table t (id integer primary key)"});
    const auto conn = s.connect(sqlite_driver(), db.path());
    ibex::runtime::Table duplicate;
    duplicate.add_column("id", ibex::Column<std::int64_t>{1, 2, 2, 3});

    const auto failed = s.write(conn, std::move(duplicate), "t", "append");
    REQUIRE_FALSE(failed.has_value());
    CHECK(contains(failed.error(), "UNIQUE constraint failed"));
    CHECK(ints(s.query(conn, "select count(*) as n from t"), "n") == std::vector<std::int64_t>{0});

    ibex::runtime::Table fine;
    fine.add_column("id", ibex::Column<std::int64_t>{1, 2});
    REQUIRE(s.write(conn, std::move(fine), "t", "append").has_value());
    CHECK(ints(s.query(conn, "select count(*) as n from t"), "n") == std::vector<std::int64_t>{2});
}

TEST_CASE("adbc_write takes any table expression, also inside functions", "[adbc][write]") {
    AdbcSession s;
    SqliteDb db;
    seed_trades(s, db);
    const auto exec = [&](const std::string& source) {
        auto result = s.session.execute(source);
        INFO(source);
        INFO(result.error);
        REQUIRE(result.ok);
        return result;
    };
    exec("let db = adbc_connect(" + ibex_str(sqlite_driver()) + ", " + ibex_str(db.path()) + ");");

    // A query over a query result, written back to the same database.
    exec(
        "let totals = adbc_query(db, \"select symbol, qty from trades\")"
        "[filter qty > 6, select { total = sum(qty) }, by symbol];");
    const auto written = exec("adbc_write(db, totals, \"totals\");");
    CHECK(std::get<std::int64_t>(*written.scalar) == 2);
    // Inline, the query clause would run on a resource call's result inside
    // another resource call's argument: refused before anything runs.
    const auto inline_query = s.session.execute(
        "adbc_write(db, adbc_query(db, \"select qty from trades\")[filter qty > 6], "
        "\"inline_t\");");
    REQUIRE_FALSE(inline_query.ok);
    CHECK(contains(inline_query.error, "adbc_query can be called only as"));
    const auto totals = exec("adbc_query(db, \"select total from totals order by total\");");
    CHECK(ints(*totals.table, "total") == std::vector<std::int64_t>{7, 30});

    // The mode defaults to create.
    exec("let small = Table { id = [1, 2] };");
    exec("adbc_write(db, small, \"small\");");
    const auto again = s.session.execute("adbc_write(db, small, \"small\");");
    REQUIRE_FALSE(again.ok);
    CHECK(contains(again.error, "already exists"));

    // A function taking the connection and a table.
    exec(
        "fn save(mutable c: AdbcConnection, df: DataFrame, name: String) -> Int {\n"
        "    adbc_write(c, df, name, \"replace\");\n"
        "}\n"
        "let saved = save(db, small[update { id = id * 10 }], \"small\");");
    const auto ids = exec("adbc_query(db, \"select id from small order by id\");");
    CHECK(ints(*ids.table, "id") == std::vector<std::int64_t>{10, 20});

    const auto scalar = s.session.execute("adbc_write(db, 5, \"x\");");
    REQUIRE_FALSE(scalar.ok);

    exec("adbc_close(db);");
    const auto closed = s.session.execute("adbc_write(db, small, \"small\", \"replace\");");
    REQUIRE_FALSE(closed.ok);
    CHECK(contains(closed.error, "adbc_write: connection is closed"));
}

TEST_CASE("adbc_query and adbc_execute bind a parameter table", "[adbc][params]") {
    AdbcSession s;
    SqliteDb db;
    const auto exec = [&](const std::string& source) {
        auto result = s.session.execute(source);
        INFO(source);
        INFO(result.error);
        REQUIRE(result.ok);
        return result;
    };
    exec("let db = adbc_connect(" + ibex_str(sqlite_driver()) + ", " + ibex_str(db.path()) + ");");
    exec("adbc_execute(db, \"create table t (id integer, name text, px real)\");");

    // One prepared statement, one execution per row; the counts add up. A
    // quote in a value is data, not SQL.
    const auto inserted = exec(
        "adbc_execute(db, \"insert into t values (?, ?, ?)\", "
        "Table { id = [1, 2, 3], name = [\"O'Hara\", null, \"c\"], px = [1.5, 2.5, null] });");
    CHECK(std::get<std::int64_t>(*inserted.scalar) == 3);
    const auto stored = exec("adbc_query(db, \"select id, name, px from t order by id\");");
    CHECK(strings(*stored.table, "name").at(0) == "O'Hara");
    CHECK(nulls(*stored.table, "name") == std::vector<bool>{false, true, false});
    CHECK(nulls(*stored.table, "px") == std::vector<bool>{false, false, true});

    // A query binds by position; its result is an ordinary table.
    const auto one = exec(
        "adbc_query(db, \"select id from t where id >= ? and name is not null order by id\", "
        "Table { lo = [2] });");
    CHECK(ints(*one.table, "id") == std::vector<std::int64_t>{3});

    // Several parameter rows: the results follow one another, in row order.
    const auto many = exec(
        "adbc_query(db, \"select id, ? as tag from t where id = ?\", "
        "Table { tag = [\"third\", \"first\"], id = [3, 1] });");
    CHECK(ints(*many.table, "id") == std::vector<std::int64_t>{3, 1});
    CHECK(strings(*many.table, "tag") == std::vector<std::string>{"third", "first"});

    // No parameter rows: nothing runs.
    const auto none = exec(
        "adbc_query(db, \"select id from t where id = ?\", Table { id = [1] }[filter id > 5]);");
    CHECK(none.table->rows() == 0);
    const auto no_update = exec(
        "adbc_execute(db, \"delete from t where id = ?\", Table { id = [1] }[filter id > 5]);");
    CHECK(std::get<std::int64_t>(*no_update.scalar) == 0);

    // The driver checks the parameter count; the connection survives.
    const auto mismatch =
        s.session.execute("adbc_query(db, \"select ? as a\", Table { a = [1], b = [2] });");
    REQUIRE_FALSE(mismatch.ok);
    CHECK(contains(mismatch.error, "adbc_query:"));
    CHECK(contains(mismatch.error, "parameter count mismatch"));

    // A function can pass a parameter table through.
    exec(
        "fn by_id(mutable c: AdbcConnection, ids: DataFrame) -> DataFrame {\n"
        "    adbc_query(c, \"select name from t where id = ?\", ids);\n"
        "}\n"
        "let named = by_id(db, Table { id = [3, 1] });");
    const auto named = exec("named;");
    CHECK(strings(*named.table, "name") == std::vector<std::string>{"c", "O'Hara"});
}

TEST_CASE("adbc_begin, adbc_commit and adbc_rollback", "[adbc][transaction]") {
    AdbcSession s;
    SqliteDb db;
    const auto exec = [&](const std::string& source) {
        auto result = s.session.execute(source);
        INFO(source);
        INFO(result.error);
        REQUIRE(result.ok);
        return result;
    };
    const auto int_of = [](const ibex::repl::ExecutionResult& result) {
        REQUIRE(result.scalar.has_value());
        return std::get<std::int64_t>(*result.scalar);
    };
    const std::string connect =
        "adbc_connect(" + ibex_str(sqlite_driver()) + ", " + ibex_str(db.path()) + ")";
    exec("let db = " + connect + ";\nlet other = " + connect + ";");
    exec("adbc_execute(db, \"create table t (id integer)\");");
    // Rows `other` sees: only what `db` has committed.
    const auto committed_ids = [&] {
        return ints(*exec("adbc_query(other, \"select id from t order by id\");").table, "id");
    };

    SECTION("commit makes a transaction's rows visible to other connections") {
        CHECK(int_of(exec("adbc_begin(db);")) == 1);
        exec("adbc_execute(db, \"insert into t values (1), (2)\");");
        CHECK(int_of(exec("adbc_write(db, Table { id = [3] }, \"t\", \"append\");")) == 1);
        CHECK(ints(*exec("adbc_query(db, \"select id from t order by id\");").table, "id") ==
              std::vector<std::int64_t>{1, 2, 3});
        CHECK(committed_ids().empty());
        CHECK(int_of(exec("adbc_commit(db);")) == 1);
        CHECK(committed_ids() == std::vector<std::int64_t>{1, 2, 3});

        // Back in autocommit: a statement is visible as soon as it ran.
        exec("adbc_execute(db, \"insert into t values (4)\");");
        CHECK(committed_ids() == std::vector<std::int64_t>{1, 2, 3, 4});
    }

    SECTION("rollback discards a transaction's statements") {
        exec("adbc_execute(db, \"insert into t values (1)\");");
        exec("adbc_begin(db);");
        exec("adbc_execute(db, \"insert into t values (2)\");");
        exec("adbc_execute(db, \"delete from t where id = 1\");");
        CHECK(int_of(exec("adbc_rollback(db);")) == 1);
        CHECK(ints(*exec("adbc_query(db, \"select id from t order by id\");").table, "id") ==
              std::vector<std::int64_t>{1});

        // Back in autocommit.
        exec("adbc_execute(db, \"insert into t values (7)\");");
        CHECK(committed_ids() == std::vector<std::int64_t>{1, 7});

        // Nothing open: rollback says so, commit refuses.
        CHECK(int_of(exec("adbc_rollback(db);")) == 0);
        const auto commit = s.session.execute("adbc_commit(db);");
        REQUIRE_FALSE(commit.ok);
        CHECK(contains(commit.error, "adbc_commit: no transaction is open"));
    }

    SECTION("a transaction does not nest") {
        exec("adbc_begin(db);");
        exec("adbc_execute(db, \"insert into t values (1)\");");
        const auto again = s.session.execute("adbc_begin(db);");
        REQUIRE_FALSE(again.ok);
        CHECK(contains(again.error, "adbc_begin: a transaction is already open"));
        // The first transaction is still open and intact.
        exec("adbc_commit(db);");
        CHECK(committed_ids() == std::vector<std::int64_t>{1});
    }

    SECTION("a failed statement means the transaction can only roll back") {
        // SQLite would commit the statements that worked; Ibex rolls back, as
        // PostgreSQL does, so a script means the same on every driver.
        exec("adbc_begin(db);");
        exec("adbc_execute(db, \"insert into t values (1)\");");
        const auto bad = s.session.execute("adbc_execute(db, \"insert into nope values (1)\");");
        REQUIRE_FALSE(bad.ok);
        exec("adbc_execute(db, \"insert into t values (2)\");");
        const auto commit = s.session.execute("adbc_commit(db);");
        REQUIRE_FALSE(commit.ok);
        CHECK(contains(commit.error, "adbc_commit: a statement in the transaction failed"));
        CHECK(committed_ids().empty());

        // Also a failed query or write, and only inside the transaction: a
        // failure before adbc_begin does not carry over.
        for (const std::string failing :
             {"adbc_query(db, \"select * from nope\");",
              "adbc_write(db, Table { id = [1] }, \"t\", \"create\");"}) {
            REQUIRE_FALSE(s.session.execute(failing).ok);
            exec("adbc_begin(db);");
            exec("adbc_execute(db, \"insert into t values (3)\");");
            REQUIRE_FALSE(s.session.execute(failing).ok);
            REQUIRE_FALSE(s.session.execute("adbc_commit(db);").ok);
            CHECK(committed_ids().empty());
        }
        exec("adbc_begin(db);");
        exec("adbc_execute(db, \"insert into t values (4)\");");
        exec("adbc_commit(db);");
        CHECK(committed_ids() == std::vector<std::int64_t>{4});
    }

    SECTION("closing a connection rolls back, never commits") {
        exec("adbc_begin(db);");
        exec("adbc_execute(db, \"insert into t values (1)\");");
        CHECK(int_of(exec("adbc_close(db);")) == 1);
        CHECK(committed_ids().empty());
        const auto after = s.session.execute("adbc_begin(db);");
        REQUIRE_FALSE(after.ok);
        CHECK(contains(after.error, "adbc_begin: connection is closed"));
    }

    SECTION("dropping the last binding rolls back") {
        exec("adbc_begin(db);");
        exec("adbc_execute(db, \"insert into t values (1)\");");
        exec("let db = 0;");
        CHECK(committed_ids().empty());
    }

    SECTION("a function can wrap a transaction") {
        exec(
            "fn load(mutable c: AdbcConnection, rows: DataFrame) -> Int {\n"
            "    adbc_begin(c);\n"
            "    adbc_execute(c, \"delete from t\");\n"
            "    adbc_write(c, rows, \"t\", \"append\");\n"
            "    adbc_commit(c);\n"
            "}\n"
            "load(db, Table { id = [5, 6] });");
        CHECK(committed_ids() == std::vector<std::int64_t>{5, 6});
    }

    SECTION("autocommit cannot be set through options") {
        const auto rejected = s.session.execute("adbc_connect(" + ibex_str(sqlite_driver()) + ", " +
                                                ibex_str(db.path()) +
                                                ", \"conn.adbc.connection.autocommit=false\");");
        REQUIRE_FALSE(rejected.ok);
        CHECK(contains(rejected.error, "adbc.connection.autocommit option only accepts true"));
        CHECK(contains(rejected.error, "adbc_begin"));
    }
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

// adbc_write and adbc_execute against PostgreSQL; gated like the test above.
// Unlike SQLite, PostgreSQL has a column type for every Ibex type.
TEST_CASE("Transactions against PostgreSQL", "[adbc][transaction][postgresql]") {
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
    const std::string connect = "adbc_connect(" + ibex_str(driver) + ", " + ibex_str(*uri) + ")";
    exec("let db = " + connect + ";\nlet other = " + connect + ";");
    exec("adbc_execute(db, \"drop table if exists ibex_txn\");");
    exec("adbc_execute(db, \"create table ibex_txn (id bigint primary key)\");");
    const auto committed_ids = [&] {
        return ints(*exec("adbc_query(other, \"select id from ibex_txn order by id\");").table,
                    "id");
    };

    exec("adbc_begin(db);");
    exec("adbc_execute(db, \"insert into ibex_txn values (1)\");");
    exec("adbc_write(db, Table { id = [2] }, \"ibex_txn\", \"append\");");
    CHECK(committed_ids().empty());
    exec("adbc_commit(db);");
    CHECK(committed_ids() == std::vector<std::int64_t>{1, 2});

    exec("adbc_begin(db);");
    exec("adbc_execute(db, \"delete from ibex_txn\");");
    exec("adbc_rollback(db);");
    CHECK(committed_ids() == std::vector<std::int64_t>{1, 2});

    // A failed statement aborts a PostgreSQL transaction. Its COMMIT then
    // rolls back, and adbc_commit must say so rather than report success.
    exec("adbc_begin(db);");
    exec("adbc_execute(db, \"insert into ibex_txn values (3)\");");
    const auto duplicate =
        s.session.execute("adbc_execute(db, \"insert into ibex_txn values (1)\");");
    REQUIRE_FALSE(duplicate.ok);
    const auto aborted =
        s.session.execute("adbc_execute(db, \"insert into ibex_txn values (4)\");");
    REQUIRE_FALSE(aborted.ok);
    const auto commit = s.session.execute("adbc_commit(db);");
    REQUIRE_FALSE(commit.ok);
    CHECK(contains(commit.error,
                   "adbc_commit: a statement in the transaction failed, so it "
                   "was rolled back and nothing was committed"));
    CHECK(committed_ids() == std::vector<std::int64_t>{1, 2});
    // Either way the connection is back in autocommit and usable.
    exec("adbc_execute(db, \"insert into ibex_txn values (5)\");");
    CHECK(committed_ids() == std::vector<std::int64_t>{1, 2, 5});

    // Closing with a transaction open rolls it back.
    exec("adbc_begin(db);");
    exec("adbc_execute(db, \"insert into ibex_txn values (6)\");");
    exec("adbc_close(db);");
    CHECK(committed_ids() == std::vector<std::int64_t>{1, 2, 5});

    exec("adbc_execute(other, \"drop table ibex_txn\");");
}

TEST_CASE("adbc_write and adbc_execute against PostgreSQL", "[adbc][write][postgresql]") {
    const auto uri = get_env("IBEX_TEST_POSTGRES_URI");
    if (!uri.has_value() || uri->empty()) {
        SKIP("IBEX_TEST_POSTGRES_URI is not set");
    }
    const std::string driver = get_env("IBEX_TEST_POSTGRES_DRIVER").value_or("postgresql");
    AdbcSession s;
    const auto conn = s.connect(driver, *uri);
    const auto execute = [&](std::string_view sql) {
        ibex::runtime::ExternArgs args;
        args.push_resource(conn);
        args.emplace_back(std::string(sql));
        auto result = s.plugin("adbc_execute").func(args);
        INFO(sql);
        INFO((result.has_value() ? std::string{} : result.error()));
        REQUIRE(result.has_value());
        return std::get<std::int64_t>(std::get<ibex::runtime::ScalarValue>(*result));
    };

    auto table = typed_table();
    auto amount = ibex::runtime::make_decimal_column({.precision = 12, .scale = 2});
    amount.push_back(ibex::Decimal{1234});
    amount.push_back(ibex::Decimal{0});
    amount.push_back(ibex::Decimal{-5});
    table.add_column("amount", std::move(amount), ibex::runtime::ValidityBitmap{true, false, true});
    const auto written = s.write(conn, std::move(table), "ibex_write_typed", "replace");
    INFO((written.has_value() ? std::string{} : written.error()));
    REQUIRE(written.has_value());
    CHECK(*written == 3);

    const auto types = s.query(conn,
                               "select data_type from information_schema.columns where "
                               "table_name = 'ibex_write_typed' order by ordinal_position");
    CHECK(strings(types, "data_type") ==
          std::vector<std::string>{"bigint", "double precision", "boolean", "text", "text", "date",
                                   "timestamp without time zone", "numeric"});

    // Every type reads back as itself, except numeric, which the driver hands
    // over as text; timestamps keep PostgreSQL's microseconds.
    const auto back = s.query(conn,
                              "select i, f, b, s, sym, d, ts, amount::text as amount from "
                              "ibex_write_typed order by i desc");
    CHECK(ints(back, "i") == std::vector<std::int64_t>{1, 0, -3});
    CHECK(doubles(back, "f").at(2) == -2.25);
    const auto* flags = std::get_if<ibex::Column<bool>>(back.find("b"));
    REQUIRE(flags != nullptr);
    CHECK((*flags)[0]);
    CHECK_FALSE((*flags)[2]);
    CHECK(strings(back, "sym").at(2) == "AAPL");
    const auto* days = std::get_if<ibex::Column<ibex::Date>>(back.find("d"));
    REQUIRE(days != nullptr);
    CHECK((*days)[0].days == 20455);
    CHECK((*days)[2].days == 10956);
    const auto* stamps = std::get_if<ibex::Column<ibex::Timestamp>>(back.find("ts"));
    REQUIRE(stamps != nullptr);
    CHECK((*stamps)[0].nanos == 1767323045123456000);
    CHECK(strings(back, "amount").at(0) == "12.34");
    CHECK(strings(back, "amount").at(2) == "-0.05");
    const std::vector<bool> middle{false, true, false};
    for (const auto* name : {"f", "b", "s", "sym", "d", "ts", "amount"}) {
        INFO(name);
        CHECK(nulls(back, name) == middle);
    }

    // Affected rows for DML; PostgreSQL's driver reports none (-1) for DDL.
    CHECK(execute("update ibex_write_typed set f = 0 where i > -3") == 2);
    CHECK(execute("drop table if exists ibex_write_pk") == -1);
    execute("create table ibex_write_pk (id bigint primary key)");

    // A failed COPY writes nothing, and the connection stays usable.
    ibex::runtime::Table duplicate;
    duplicate.add_column("id", ibex::Column<std::int64_t>{1, 2, 2, 3});
    const auto failed = s.write(conn, std::move(duplicate), "ibex_write_pk", "append");
    REQUIRE_FALSE(failed.has_value());
    CHECK(contains(failed.error(), "duplicate key"));
    CHECK(ints(s.query(conn, "select count(*)::bigint as n from ibex_write_pk"), "n") ==
          std::vector<std::int64_t>{0});

    execute("drop table ibex_write_pk");
    execute("drop table ibex_write_typed");

    // Parameters: every Ibex type binds, Categorical and Decimal included,
    // and several rows run as one prepared statement.
    execute("drop table if exists ibex_params");
    execute(
        "create table ibex_params (i bigint, f double precision, b boolean, s text, "
        "sym text, d date, ts timestamp, amount numeric(12, 2))");
    auto params = typed_table();
    auto param_amount = ibex::runtime::make_decimal_column({.precision = 12, .scale = 2});
    param_amount.push_back(ibex::Decimal{1234});
    param_amount.push_back(ibex::Decimal{0});
    param_amount.push_back(ibex::Decimal{-5});
    params.add_column("amount", std::move(param_amount),
                      ibex::runtime::ValidityBitmap{true, false, true});
    {
        ibex::runtime::ExternArgs args;
        args.push_resource(conn);
        args.emplace_back(
            std::string("insert into ibex_params values ($1, $2, $3, $4, $5, $6, $7, $8)"));
        args.push_table(std::make_shared<const ibex::runtime::Table>(std::move(params)));
        auto inserted = s.plugin("adbc_execute").func(args);
        INFO((inserted.has_value() ? std::string{} : inserted.error()));
        REQUIRE(inserted.has_value());
    }
    const auto bound = s.query(conn,
                               "select i, f, b, s, sym, d, ts, amount::text as amount from "
                               "ibex_params order by i desc");
    CHECK(ints(bound, "i") == std::vector<std::int64_t>{1, 0, -3});
    CHECK(strings(bound, "sym").at(2) == "AAPL");
    CHECK(strings(bound, "amount").at(0) == "12.34");
    const auto* bound_days = std::get_if<ibex::Column<ibex::Date>>(bound.find("d"));
    REQUIRE(bound_days != nullptr);
    CHECK((*bound_days)[2].days == 10956);
    const auto* bound_stamps = std::get_if<ibex::Column<ibex::Timestamp>>(bound.find("ts"));
    REQUIRE(bound_stamps != nullptr);
    CHECK((*bound_stamps)[0].nanos == 1767323045123456000);
    for (const auto* name : {"f", "b", "s", "sym", "d", "ts", "amount"}) {
        INFO(name);
        CHECK(nulls(bound, name) == middle);
    }
    {
        ibex::runtime::Table lookup;
        lookup.add_column("i", ibex::Column<std::int64_t>{-3, 1});
        ibex::runtime::ExternArgs args;
        args.push_resource(conn);
        args.emplace_back(std::string("select i, s from ibex_params where i = $1"));
        args.push_table(std::make_shared<const ibex::runtime::Table>(std::move(lookup)));
        auto found = s.plugin("adbc_query").func(args);
        REQUIRE(found.has_value());
        const auto& rows = std::get<ibex::runtime::Table>(*found);
        CHECK(ints(rows, "i") == std::vector<std::int64_t>{-3, 1});
        CHECK(strings(rows, "s") == std::vector<std::string>{"c", "a"});
    }
    {
        // No parameter rows: nothing runs, and the driver still describes the
        // result's columns.
        ibex::runtime::Table no_rows;
        no_rows.add_column("i", ibex::Column<std::int64_t>{});
        ibex::runtime::ExternArgs args;
        args.push_resource(conn);
        args.emplace_back(std::string("select i, s from ibex_params where i = $1"));
        args.push_table(std::make_shared<const ibex::runtime::Table>(std::move(no_rows)));
        auto found = s.plugin("adbc_query").func(args);
        REQUIRE(found.has_value());
        const auto& rows = std::get<ibex::runtime::Table>(*found);
        CHECK(rows.rows() == 0);
        CHECK(rows.find("i") != nullptr);
        CHECK(rows.find("s") != nullptr);
    }
    execute("drop table ibex_params");

    // uuid reads as its canonical text; time has no Ibex type, and the error
    // names the column and the cast.
    const auto uuids = s.query(conn,
                               "select 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid as u "
                               "union all select null::uuid");
    CHECK(strings(uuids, "u").at(0) == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    CHECK(nulls(uuids, "u") == std::vector<bool>{false, true});
    ibex::runtime::ExternArgs time_args;
    time_args.push_resource(conn);
    time_args.emplace_back(std::string("select '12:30'::time as t"));
    const auto times = s.plugin("adbc_query").func(time_args);
    REQUIRE_FALSE(times.has_value());
    CHECK(contains(times.error(),
                   "column `t`: Arrow time64[us] has no Ibex column type; cast it "
                   "in the query, e.g. CAST(t AS TEXT)"));
}
