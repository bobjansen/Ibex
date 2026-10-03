// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The ADBC client library: sessions, statements, driver quirks, discovery, and
// the typed C++ API of adbc_client.hpp. Built as `libibex_adbc.a`, which the
// `adbc` plugin and compiled programs both link.

#include "adbc_client.hpp"

#include <ibex/core/text.hpp>
#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/operator.hpp>

#include <algorithm>
#include <array>
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <deque>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "adbc_objects.hpp"
#include "adbc_options.hpp"

namespace {

using ibex::adbc::OptionList;
using ibex::adbc::ParsedOptions;

auto release_adbc_error(AdbcError* error) noexcept -> void {
    if (error != nullptr && error->release != nullptr) {
        error->release(error);
        error->release = nullptr;
    }
}

auto format_adbc_error(std::string_view context, AdbcStatusCode status, const AdbcError& error)
    -> std::string {
    std::string message(context);
    message += " failed";
    if (error.message != nullptr && std::strlen(error.message) > 0) {
        message += ": ";
        message += error.message;
    } else {
        message += " (status ";
        message += std::to_string(static_cast<int>(status));
        message += ")";
    }
    return message;
}

template <typename Fn>
auto call_adbc(std::string_view context, Fn&& fn) -> std::expected<void, std::string> {
    AdbcError error{};
    const AdbcStatusCode status = fn(&error);
    if (status != ADBC_STATUS_OK) {
        std::string message = format_adbc_error(context, status, error);
        release_adbc_error(&error);
        return std::unexpected(std::move(message));
    }
    release_adbc_error(&error);
    return {};
}

/// Drivers `scripts/install_adbc_driver.{sh,ps1}` can install. Keep in step
/// with the scripts' driver tables.
constexpr std::array<std::string_view, 4> kInstallableDrivers{"sqlite", "postgresql", "duckdb",
                                                              "mysql"};

/// Whether `driver` names a library file, which the driver manager loads
/// as-is, rather than a name it resolves through manifests and the loader's
/// search path.
auto looks_like_path(std::string_view driver) -> bool {
    if (driver.find('/') != std::string_view::npos || driver.find('\\') != std::string_view::npos) {
        return true;
    }
    return driver.ends_with(".so") || driver.ends_with(".dylib") || driver.ends_with(".dll");
}

/// A driver given as a path that does not exist, reported before the driver
/// manager sees it. Left to the manager, a missing path produced two `dlopen`
/// failures, the second for a name it invented (`lib/nope/x.so.so`), and a
/// Windows path lost everything after the drive letter: ADBC reads `C:` as a
/// `driver:uri` prefix and reported "Could not load `C`".
auto missing_driver_file(const std::string& driver) -> std::optional<std::string> {
    if (!looks_like_path(driver)) {
        return std::nullopt;
    }
    std::error_code ec;
    if (std::filesystem::exists(std::filesystem::path(driver), ec)) {
        return std::nullopt;
    }
    return "driver library not found: " + driver +
           " (a path is loaded as given; pass a bare driver name such as \"sqlite\" to "
           "use an installed driver instead)";
}

/// Rewrite the driver manager's "Could not load" for a bare driver name. The
/// first line says what happened and what to do, because consumers that show
/// one line (`ReplSession`, and so the R and Python bridges) keep only that;
/// the driver manager's own list of where it looked for manifests follows.
auto explain_driver_not_found(std::string_view driver, std::string_view manager_message)
    -> std::string {
    std::string message = "ADBC driver `" + std::string(driver) + "` not found; ";
    const bool installable =
        std::ranges::find(kInstallableDrivers, driver) != kInstallableDrivers.end();
    if (installable) {
        message += "install it with `scripts/install_adbc_driver.sh " + std::string(driver) +
                   "` (Linux, macOS) or `powershell -ExecutionPolicy Bypass -File "
                   "scripts\\install_adbc_driver.ps1 " +
                   std::string(driver) + "` (Windows)";
    } else {
        message += "scripts/install_adbc_driver.sh (and .ps1) install ";
        for (std::size_t i = 0; i < kInstallableDrivers.size(); ++i) {
            message += i == 0 ? "" : ", ";
            message += kInstallableDrivers[i];
        }
        message +=
            "; for another driver, install it with an ADBC driver manifest, or pass the "
            "path to its library";
    }
    constexpr std::string_view kSearched = "Also searched these paths for manifests:";
    if (const auto at = manager_message.find(kSearched); at != std::string_view::npos) {
        message += "\nSearched for a driver manifest in:";
        message += manager_message.substr(at + kSearched.size());
    }
    return message;
}

/// An import error with SQL advice: a column Ibex has no type for can be cast
/// in the query itself. No one cast type works everywhere: MySQL rejects
/// `TEXT` in a cast, and PostgreSQL's `CHAR` is `char(1)`, so both are named.
auto with_sql_advice(std::string message) -> std::string {
    constexpr std::string_view kColumn = "column `";
    const auto advice = message.rfind(ibex::interop::kUnsupportedColumnAdvice);
    const auto start = message.find(kColumn);
    if (advice == std::string::npos || start == std::string::npos) {
        return message;
    }
    const auto name_start = start + kColumn.size();
    const auto name_end = message.find('`', name_start);
    if (name_end == std::string::npos) {
        return message;
    }
    const std::string name = message.substr(name_start, name_end - name_start);
    message.replace(advice, ibex::interop::kUnsupportedColumnAdvice.size(),
                    "cast it in the query, e.g. CAST(" + name + " AS TEXT) (AS CHAR on MySQL)");
    return message;
}

template <typename Handle, typename Setter>
auto apply_adbc_options(std::string_view context, Handle* handle, const OptionList& options,
                        Setter&& setter) -> std::expected<void, std::string> {
    for (const auto& [key, value] : options) {
        const std::string where = std::string(context) + "(" + key + ")";
        auto status = call_adbc(where, [&](AdbcError* error) {
            return setter(handle, key.c_str(), value.c_str(), error);
        });
        if (!status) {
            return status;
        }
    }
    return {};
}

/// Where a driver departs from what the other drivers do, worked around so
/// that a script means the same on each. Found from the vendor name the
/// driver reports for itself, not the name it was loaded by, which may be a
/// path.
struct DriverQuirks {
    /// Binds one parameter row per execution (DuckDB): the plugin runs a
    /// parameter table row by row itself.
    bool one_param_row = false;
    /// Commits a bulk ingest batch by batch (MySQL): outside a transaction a
    /// failed write would keep the batches before the failing one, so the
    /// plugin wraps it in one.
    bool batched_ingest = false;
    /// Leaves the last result of a COPY unread, after a bulk ingest and after
    /// a query alike (PostgreSQL: one PQgetResult where libpq wants them read
    /// to the end, in ADBC 24 and on main as of 2026-10-02). libpq then
    /// reports the connection busy, and the driver skips the BEGIN of the
    /// next transaction, whose first statement then commits on its own and
    /// survives a rollback. adbc::begin runs a query first, which makes libpq
    /// discard the result.
    bool leaves_result_unread = false;
    /// Drops the error of a failed bulk ingest (DuckDB 1.5.6: the appender
    /// flushes in its destructor, whose error is discarded, typically a
    /// constraint violation such as a duplicate key) and reports the rows as
    /// written when none are. Inside a transaction the failure aborts it, so
    /// the plugin ingests in one and checks it with a query afterwards.
    bool ingest_hides_failure = false;
};

auto quirks_of_vendor(std::string vendor) -> DriverQuirks {
    std::ranges::transform(vendor, vendor.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return {.one_param_row = vendor.find("duckdb") != std::string::npos,
            .batched_ingest = vendor.find("mysql") != std::string::npos ||
                              vendor.find("mariadb") != std::string::npos,
            .leaves_result_unread = vendor.find("postgresql") != std::string::npos,
            .ingest_hides_failure = vendor.find("duckdb") != std::string::npos};
}

/// One open ADBC database and connection: the value behind an Ibex
/// `adbc::Connection`. Shared by every binding of it and by the query running on
/// it (a statement lease), so the handles outlive whichever of them drops
/// first. Allows one active statement at a time.
///
/// Tables imported from a query may keep zero-copy buffers whose release
/// callbacks live in the driver library. The pinned driver manager never
/// unloads a driver (`ManagedLibrary::Release` is a no-op, apache/arrow-adbc#204),
/// so those buffers stay valid after `adbc::close` and after this object is gone.
class AdbcSession final : public ibex::runtime::Resource {
   public:
    static constexpr std::string_view kTypeName = "adbc::Connection";

    static auto open(const std::string& driver, const std::string& uri,
                     const ParsedOptions& options)
        -> std::expected<std::shared_ptr<AdbcSession>, std::string> {
        auto session = std::shared_ptr<AdbcSession>(new AdbcSession());
        session->statement_options_ = options.statement;
        auto init = session->init(driver, uri, options);
        if (!init) {
            // `session` is destroyed here, releasing whatever init acquired.
            return std::unexpected(init.error());
        }
        session->quirks_ = quirks_of_vendor(session->vendor_name());
        return session;
    }

    AdbcSession(const AdbcSession&) = delete;
    AdbcSession& operator=(const AdbcSession&) = delete;
    AdbcSession(AdbcSession&&) noexcept = delete;
    AdbcSession& operator=(AdbcSession&&) noexcept = delete;

    ~AdbcSession() override { (void)release_handles(); }

    [[nodiscard]] auto type_name() const noexcept -> std::string_view override { return kTypeName; }

    /// Start a statement. Fails when the connection is closed or another
    /// statement is still running on it.
    auto acquire_lease() -> std::expected<void, std::string> {
        if (closed_) {
            return std::unexpected("connection is closed");
        }
        if (busy_) {
            return std::unexpected("connection busy: another query on it has not finished");
        }
        busy_ = true;
        return {};
    }

    /// End a statement. A close requested while it ran releases the handles now.
    void release_lease() noexcept {
        busy_ = false;
        if (closed_) {
            (void)release_handles();
        }
    }

    /// Mark the connection closed; every binding of it rejects new queries.
    /// Returns false when it already was. The handles are released now, or when
    /// the running statement ends.
    auto close() -> std::expected<bool, std::string> {
        if (closed_) {
            return false;
        }
        closed_ = true;
        if (busy_) {
            return true;
        }
        auto released = release_handles();
        if (!released) {
            return std::unexpected(released.error());
        }
        return true;
    }

    /// Run `fn` holding the statement lease, so that nothing else runs on the
    /// connection meanwhile and a closed connection is refused.
    template <typename Fn>
    auto with_lease(Fn&& fn) -> std::expected<void, std::string> {
        if (auto lease = acquire_lease(); !lease) {
            return lease;
        }
        auto result = fn();
        release_lease();
        return result;
    }

    /// Start a transaction: turn autocommit off. Fails when one is open.
    auto begin() -> std::expected<void, std::string> {
        return with_lease([&]() -> std::expected<void, std::string> {
            if (in_transaction_) {
                return std::unexpected("a transaction is already open");
            }
            if (quirks_.leaves_result_unread) {
                // Still in autocommit, so it opens nothing (see DriverQuirks).
                (void)run_trivial_query();
            }
            auto status = set_autocommit(ADBC_OPTION_VALUE_DISABLED);
            if (status) {
                in_transaction_ = true;
                statement_failed_ = false;
            }
            return status;
        });
    }

    /// Commit the open transaction and return to autocommit. A transaction in
    /// which a statement failed is rolled back instead, on every driver:
    /// PostgreSQL would do so anyway, but reports a successful COMMIT. When
    /// the commit itself fails the transaction is rolled back too, so the
    /// connection never stays in a transaction the script believes has ended.
    auto commit() -> std::expected<void, std::string> {
        return with_lease([&]() -> std::expected<void, std::string> {
            if (!in_transaction_) {
                return std::unexpected("no transaction is open; start one with adbc::begin");
            }
            if (statement_failed_) {
                auto rolled_back = end_with_rollback();
                return std::unexpected(
                    std::string("a statement in the transaction failed, so it was rolled back "
                                "and nothing was committed") +
                    (rolled_back ? "" : "; rolling back failed too: " + rolled_back.error()));
            }
            in_transaction_ = false;
            auto committed = call_adbc("AdbcConnectionCommit", [&](AdbcError* error) {
                return AdbcConnectionCommit(&connection_, error);
            });
            if (!committed) {
                auto rolled_back = end_with_rollback();
                return std::unexpected(
                    committed.error() +
                    (rolled_back ? "; the transaction was rolled back"
                                 : "; rolling it back failed too: " + rolled_back.error()));
            }
            return set_autocommit(ADBC_OPTION_VALUE_ENABLED);
        });
    }

    /// Roll back the open transaction and return to autocommit. Returns false
    /// when no transaction was open.
    auto rollback() -> std::expected<bool, std::string> {
        bool was_open = false;
        auto rolled_back = with_lease([&]() -> std::expected<void, std::string> {
            was_open = in_transaction_;
            return was_open ? end_with_rollback() : std::expected<void, std::string>{};
        });
        if (!rolled_back) {
            return std::unexpected(rolled_back.error());
        }
        return was_open;
    }

    /// Run `write`, a bulk ingest on this connection, in a transaction of its
    /// own when the driver needs one for a failed write to write nothing, or
    /// to report its failure, and none is open (see `DriverQuirks`). On MySQL
    /// `write` must not run DDL, which would end the transaction. Called
    /// holding the statement lease.
    template <typename Fn>
    auto ingest_atomically(Fn&& write) -> std::expected<std::int64_t, std::string> {
        auto checked = [&]() -> std::expected<std::int64_t, std::string> {
            auto rows = write();
            if (rows && quirks_.ingest_hides_failure) {
                if (auto probe = run_trivial_query(); !probe) {
                    return std::unexpected(
                        "the driver reported the write as done, but writing the rows failed "
                        "(typically a constraint such as a duplicate key; the driver drops "
                        "the reason): " +
                        probe.error());
                }
            }
            return rows;
        };
        if (!(quirks_.batched_ingest || quirks_.ingest_hides_failure) || in_transaction_) {
            return checked();
        }
        if (auto off = set_autocommit(ADBC_OPTION_VALUE_DISABLED); !off) {
            return std::unexpected(off.error());
        }
        auto rows = checked();
        auto ended = rows ? call_adbc("AdbcConnectionCommit",
                                      [&](AdbcError* error) {
                                          return AdbcConnectionCommit(&connection_, error);
                                      })
                          : std::expected<void, std::string>{};
        if (!rows || !ended) {
            auto rolled_back = call_adbc("AdbcConnectionRollback", [&](AdbcError* error) {
                return AdbcConnectionRollback(&connection_, error);
            });
            (void)set_autocommit(ADBC_OPTION_VALUE_ENABLED);
            return std::unexpected(
                (rows ? ended.error() : rows.error()) +
                (rolled_back ? "" : "; rolling back failed too: " + rolled_back.error()));
        }
        if (auto on = set_autocommit(ADBC_OPTION_VALUE_ENABLED); !on) {
            return std::unexpected(on.error());
        }
        return rows;
    }

    /// Record that a query or statement on this connection failed. Inside a
    /// transaction, that leaves adbc::commit only able to roll back.
    void statement_failed() noexcept {
        if (in_transaction_) {
            statement_failed_ = true;
        }
    }

    [[nodiscard]] auto connection() noexcept -> AdbcConnection* { return &connection_; }
    [[nodiscard]] auto quirks() const noexcept -> const DriverQuirks& { return quirks_; }
    [[nodiscard]] auto in_transaction() const noexcept -> bool { return in_transaction_; }
    [[nodiscard]] auto statement_options() const noexcept -> const OptionList& {
        return statement_options_;
    }

   private:
    /// Run `SELECT 1` on the connection, without a result stream.
    auto run_trivial_query() -> std::expected<void, std::string> {
        AdbcStatement statement{};
        auto status = call_adbc("AdbcStatementNew", [&](AdbcError* error) {
            return AdbcStatementNew(&connection_, &statement, error);
        });
        if (!status) {
            return status;
        }
        status = call_adbc("AdbcStatementSetSqlQuery", [&](AdbcError* error) {
            return AdbcStatementSetSqlQuery(&statement, "SELECT 1", error);
        });
        if (status) {
            std::int64_t rows_affected = -1;
            status = call_adbc("AdbcStatementExecuteQuery", [&](AdbcError* error) {
                return AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, error);
            });
        }
        AdbcError error{};
        AdbcStatementRelease(&statement, &error);
        release_adbc_error(&error);
        return status;
    }

    /// The vendor name the driver reports through GetInfo, or "" when it
    /// reports none or the call fails: then no workaround applies.
    auto vendor_name() -> std::string {
        const std::uint32_t codes[] = {ADBC_INFO_VENDOR_NAME};
        ::ArrowArrayStream stream{};
        auto status = call_adbc("AdbcConnectionGetInfo", [&](AdbcError* error) {
            return AdbcConnectionGetInfo(&connection_, codes, 1, &stream, error);
        });
        if (!status) {
            return {};
        }
        const auto stream_guard =
            std::unique_ptr<::ArrowArrayStream, void (*)(::ArrowArrayStream*)>(
                &stream, ibex::interop::release_arrow_stream);
        ::ArrowSchema schema{};
        if (stream.get_schema(&stream, &schema) != 0) {
            return {};
        }
        const auto schema_guard = std::unique_ptr<::ArrowSchema, void (*)(::ArrowSchema*)>(
            &schema, ibex::interop::release_arrow_schema);
        std::string vendor;
        while (true) {
            ::ArrowArray batch{};
            if (stream.get_next(&stream, &batch) != 0 || batch.release == nullptr) {
                break;
            }
            const auto batch_guard = std::unique_ptr<::ArrowArray, void (*)(::ArrowArray*)>(
                &batch, ibex::interop::release_arrow_array);
            (void)ibex::adbc::read_info_strings(
                batch, schema, [&](std::uint32_t code, const std::optional<std::string>& text) {
                    if (code == ADBC_INFO_VENDOR_NAME && text.has_value()) {
                        vendor = *text;
                    }
                });
        }
        return vendor;
    }

    AdbcSession() {
        std::memset(&database_, 0, sizeof(database_));
        std::memset(&connection_, 0, sizeof(connection_));
    }

    auto set_autocommit(const char* value) -> std::expected<void, std::string> {
        return call_adbc("AdbcConnectionSetOption(" ADBC_CONNECTION_OPTION_AUTOCOMMIT ")",
                         [&](AdbcError* error) {
                             return AdbcConnectionSetOption(
                                 &connection_, ADBC_CONNECTION_OPTION_AUTOCOMMIT, value, error);
                         });
    }

    /// Roll back and turn autocommit back on. The transaction counts as
    /// ended whatever this returns; a failure is reported.
    auto end_with_rollback() -> std::expected<void, std::string> {
        in_transaction_ = false;
        auto rolled_back = call_adbc("AdbcConnectionRollback", [&](AdbcError* error) {
            return AdbcConnectionRollback(&connection_, error);
        });
        if (!rolled_back) {
            return rolled_back;
        }
        return set_autocommit(ADBC_OPTION_VALUE_ENABLED);
    }

    /// Connection before database. An open transaction is rolled back first:
    /// closing never commits. Reports the first failure; everything is
    /// released either way.
    auto release_handles() noexcept -> std::expected<void, std::string> {
        std::expected<void, std::string> result;
        if (connection_acquired_ && in_transaction_) {
            in_transaction_ = false;
            result = call_adbc("AdbcConnectionRollback", [&](AdbcError* error) {
                return AdbcConnectionRollback(&connection_, error);
            });
        }
        if (connection_acquired_) {
            connection_acquired_ = false;
            auto status = call_adbc("AdbcConnectionRelease", [&](AdbcError* error) {
                return AdbcConnectionRelease(&connection_, error);
            });
            if (result && !status) {
                result = std::move(status);
            }
        }
        if (database_acquired_) {
            database_acquired_ = false;
            auto status = call_adbc("AdbcDatabaseRelease", [&](AdbcError* error) {
                return AdbcDatabaseRelease(&database_, error);
            });
            if (result && !status) {
                result = std::move(status);
            }
        }
        return result;
    }

    auto init(const std::string& driver, const std::string& uri, const ParsedOptions& options)
        -> std::expected<void, std::string> {
        if (auto missing = missing_driver_file(driver); missing.has_value()) {
            return std::unexpected(std::move(*missing));
        }
        for (const auto* list : {&options.connection, &options.connection_post}) {
            for (const auto& [key, value] : *list) {
                // Turning autocommit off opens a transaction adbc::commit
                // and adbc::close would not know about.
                if (key == ADBC_CONNECTION_OPTION_AUTOCOMMIT &&
                    value != ADBC_OPTION_VALUE_ENABLED) {
                    return std::unexpected(
                        "the " ADBC_CONNECTION_OPTION_AUTOCOMMIT
                        " option only accepts true; start a transaction with adbc::begin and "
                        "end it with adbc::commit or adbc::rollback");
                }
            }
        }
        auto status = call_adbc("AdbcDatabaseNew", [&](AdbcError* error) {
            return AdbcDatabaseNew(&database_, error);
        });
        if (!status) {
            return status;
        }
        // From here on the database must be released on every exit path,
        // including a rejected option below.
        database_acquired_ = true;

        // Let a bare driver name ("sqlite") resolve through driver manifests,
        // as installed by dbc or conda: ADBC_DRIVER_PATH, $CONDA_PREFIX,
        // ~/.config/adbc/drivers, /etc/adbc/drivers. Paths still load as-is.
        status = call_adbc("AdbcDriverManagerDatabaseSetLoadFlags", [&](AdbcError* error) {
            return AdbcDriverManagerDatabaseSetLoadFlags(&database_, ADBC_LOAD_FLAG_DEFAULT, error);
        });
        if (!status) {
            return status;
        }

        status = call_adbc("AdbcDatabaseSetOption(driver)", [&](AdbcError* error) {
            return AdbcDatabaseSetOption(&database_, "driver", driver.c_str(), error);
        });
        if (!status) {
            return status;
        }

        if (!uri.empty()) {
            status = call_adbc("AdbcDatabaseSetOption(uri)", [&](AdbcError* error) {
                return AdbcDatabaseSetOption(&database_, "uri", uri.c_str(), error);
            });
            if (!status) {
                return status;
            }
        }

        if (!options.entrypoint.empty()) {
            status = call_adbc("AdbcDatabaseSetOption(entrypoint)", [&](AdbcError* error) {
                return AdbcDatabaseSetOption(&database_, "entrypoint", options.entrypoint.c_str(),
                                             error);
            });
            if (!status) {
                return status;
            }
        }

        status = apply_adbc_options(
            "AdbcDatabaseSetOption", &database_, options.database,
            [](AdbcDatabase* database, const char* key, const char* value, AdbcError* error) {
                return AdbcDatabaseSetOption(database, key, value, error);
            });
        if (!status) {
            return status;
        }

        status = call_adbc("AdbcDatabaseInit",
                           [&](AdbcError* error) { return AdbcDatabaseInit(&database_, error); });
        if (!status) {
            // The driver is loaded here. A bare name the manager could not
            // resolve gets an explanation; anything else (a library that
            // exists but fails to load, a bad option) keeps its own message.
            if (!looks_like_path(driver) &&
                status.error().find("Could not load `") != std::string::npos) {
                return std::unexpected(explain_driver_not_found(driver, status.error()));
            }
            return status;
        }

        status = call_adbc("AdbcConnectionNew", [&](AdbcError* error) {
            return AdbcConnectionNew(&connection_, error);
        });
        if (!status) {
            return status;
        }
        connection_acquired_ = true;

        const auto set_connection_option = [](AdbcConnection* connection, const char* key,
                                              const char* value, AdbcError* error) {
            return AdbcConnectionSetOption(connection, key, value, error);
        };
        status = apply_adbc_options("AdbcConnectionSetOption", &connection_, options.connection,
                                    set_connection_option);
        if (!status) {
            return status;
        }

        status = call_adbc("AdbcConnectionInit", [&](AdbcError* error) {
            return AdbcConnectionInit(&connection_, &database_, error);
        });
        if (!status) {
            return status;
        }

        return apply_adbc_options("AdbcConnectionSetOption", &connection_, options.connection_post,
                                  set_connection_option);
    }

    AdbcDatabase database_{};
    AdbcConnection connection_{};
    OptionList statement_options_;
    bool database_acquired_ = false;
    bool connection_acquired_ = false;
    bool closed_ = false;
    bool busy_ = false;
    bool in_transaction_ = false;
    bool statement_failed_ = false;
    DriverQuirks quirks_;
};

/// A table exported through Arrow C Data for `AdbcStatementBind`. The driver
/// takes the array over when it binds and may keep it until the statement is
/// released, so the owner releases the statement first; whatever the driver
/// left of either descriptor is released here.
class BoundTable {
   public:
    BoundTable() = default;
    BoundTable(const BoundTable&) = delete;
    BoundTable& operator=(const BoundTable&) = delete;
    BoundTable(BoundTable&&) noexcept = delete;
    BoundTable& operator=(BoundTable&&) noexcept = delete;
    ~BoundTable() {
        ibex::interop::release_arrow_array(&array_);
        ibex::interop::release_arrow_schema(&schema_);
    }

    /// Export `table` and bind it to `statement`.
    auto bind(AdbcStatement* statement, const std::shared_ptr<const ibex::runtime::Table>& table)
        -> std::expected<void, std::string> {
        if (auto exported = ibex::interop::export_table_to_arrow(table, &array_, &schema_);
            !exported) {
            return std::unexpected(exported.error());
        }
        return call_adbc("AdbcStatementBind", [&](AdbcError* error) {
            return AdbcStatementBind(statement, &array_, &schema_, error);
        });
    }

   private:
    ::ArrowArray array_{};
    ::ArrowSchema schema_{};
};

/// Whether a `params` argument asks for binding: the default, `Table {}`, has
/// no columns and binds nothing.
auto has_params(const std::shared_ptr<const ibex::runtime::Table>& params) -> bool {
    return params != nullptr && !params->columns.empty();
}

/// Every table bound to one statement. The driver may keep each until the
/// statement is released, so they live as long as it does.
using BoundTables = std::deque<BoundTable>;

/// Prepare `statement` (its SQL already set).
auto prepare_statement(AdbcStatement* statement) -> std::expected<void, std::string> {
    return call_adbc("AdbcStatementPrepare",
                     [&](AdbcError* error) { return AdbcStatementPrepare(statement, error); });
}

/// Rows `[begin, end)` of `table`, as a table of their own.
auto table_rows(const ibex::runtime::Table& table, std::size_t begin, std::size_t end)
    -> std::shared_ptr<const ibex::runtime::Table> {
    auto chunk = ibex::runtime::make_morsel_chunk(table, begin, end, 0);
    auto out = std::make_shared<ibex::runtime::Table>();
    for (const auto& entry : chunk.columns) {
        out->add_column_from(entry.name, entry);
    }
    return out;
}

/// Whether `params` is run row by row in the plugin rather than bound whole:
/// on a driver that binds one row per execution (see `DriverQuirks`).
auto runs_rows_one_by_one(const AdbcSession& session,
                          const std::shared_ptr<const ibex::runtime::Table>& params) -> bool {
    return session.quirks().one_param_row && has_params(params) && params->rows() != 1;
}

/// Streams one query's result. Holds a statement lease on its session for as
/// long as it lives.
class AdbcSourceOperator final : public ibex::runtime::Operator {
   public:
    static auto create(std::shared_ptr<AdbcSession> session, const std::string& sql,
                       std::string_view function,
                       std::shared_ptr<const ibex::runtime::Table> params = nullptr)
        -> std::expected<ibex::runtime::OperatorPtr, std::string> {
        const std::string prefix = std::string(function) + ": ";
        if (auto lease = session->acquire_lease(); !lease) {
            return std::unexpected(prefix + lease.error());
        }
        auto op = std::unique_ptr<AdbcSourceOperator>(
            new AdbcSourceOperator(std::move(session), std::string(function)));
        auto init = op->init(sql, params);
        if (!init) {
            op->session_->statement_failed();
            // `op` is destroyed here, releasing the statement and the lease.
            return std::unexpected(prefix + init.error());
        }
        return ibex::runtime::OperatorPtr(std::move(op));
    }

    AdbcSourceOperator(const AdbcSourceOperator&) = delete;
    AdbcSourceOperator& operator=(const AdbcSourceOperator&) = delete;
    AdbcSourceOperator(AdbcSourceOperator&&) noexcept = delete;
    AdbcSourceOperator& operator=(AdbcSourceOperator&&) noexcept = delete;

    ~AdbcSourceOperator() override {
        // Children before parents: stream, statement, then the lease on the
        // connection.
        ibex::interop::release_arrow_stream(&stream_);
        ibex::interop::release_arrow_schema(&schema_);
        if (statement_acquired_) {
            AdbcError error{};
            AdbcStatementRelease(&statement_, &error);
            release_adbc_error(&error);
        }
        session_->release_lease();
    }

    [[nodiscard]] auto next()
        -> std::expected<std::optional<ibex::runtime::Chunk>, std::string> override {
        if (finished_) {
            return std::optional<ibex::runtime::Chunk>{};
        }

        if (no_executions_) {
            // Zero parameter rows: nothing ran. The result keeps its columns
            // when the driver could describe them without running.
            finished_ = true;
            if (schema_loaded_) {
                if (auto empty = ibex::interop::empty_table_from_arrow_schema(schema_)) {
                    return make_chunk(std::move(*empty));
                }
            }
            return make_chunk(ibex::runtime::Table{});
        }

        if (!schema_loaded_) {
            const int status = stream_.get_schema(&stream_, &schema_);
            if (status != 0) {
                return fail(stream_error("ADBC stream get_schema", status));
            }
            schema_loaded_ = true;
        }

        while (true) {
            ::ArrowArray batch{};
            const int status = stream_.get_next(&stream_, &batch);
            if (status != 0) {
                return fail(stream_error("ADBC stream get_next", status));
            }
            if (batch.release == nullptr) {
                if (row_params_ != nullptr && next_row_ < row_params_->rows()) {
                    // The next parameter row's results follow this one's.
                    auto ran = next_execution();
                    if (!ran) {
                        return fail(function_ + ": " + ran.error());
                    }
                    continue;
                }
                finished_ = true;
                if (emitted_chunk_) {
                    return std::optional<ibex::runtime::Chunk>{};
                }
                // No rows at all. Emit one empty chunk so the result keeps the
                // query's columns instead of collapsing to a column-less table.
                auto empty = ibex::interop::empty_table_from_arrow_schema(schema_);
                if (!empty) {
                    return fail(function_ +
                                ": result schema import failed: " + with_sql_advice(empty.error()));
                }
                return make_chunk(std::move(*empty));
            }

            auto batch_guard = std::unique_ptr<::ArrowArray, void (*)(::ArrowArray*)>(
                &batch, ibex::interop::release_arrow_array);

            // ADBC record batches use the same Arrow C Data importer as direct
            // Arrow input, including its zero-copy decimal128 `d:p,s` mapping.
            // Keep decimal type interpretation in that shared boundary so the
            // ADBC path cannot drift from Arrow C Data or Parquet semantics.
            auto imported = ibex::interop::adopt_table_from_arrow(&batch, schema_);
            if (!imported) {
                return fail(function_ +
                            ": batch import failed: " + with_sql_advice(imported.error()));
            }
            if (imported->rows() == 0) {
                continue;
            }
            return make_chunk(std::move(*imported));
        }
    }

   private:
    AdbcSourceOperator(std::shared_ptr<AdbcSession> session, std::string function)
        : session_(std::move(session)), function_(std::move(function)) {
        std::memset(&statement_, 0, sizeof(statement_));
        std::memset(&stream_, 0, sizeof(stream_));
        std::memset(&schema_, 0, sizeof(schema_));
    }

    auto fail(std::string message)
        -> std::expected<std::optional<ibex::runtime::Chunk>, std::string> {
        finished_ = true;
        session_->statement_failed();
        return std::unexpected(std::move(message));
    }

    auto make_chunk(ibex::runtime::Table table)
        -> std::expected<std::optional<ibex::runtime::Chunk>, std::string> {
        emitted_chunk_ = true;
        ibex::runtime::Chunk chunk;
        chunk.columns = std::move(table.columns);
        chunk.set_properties(table.properties());
        return std::optional<ibex::runtime::Chunk>{std::move(chunk)};
    }

    auto init(const std::string& sql, const std::shared_ptr<const ibex::runtime::Table>& params)
        -> std::expected<void, std::string> {
        auto status = call_adbc("AdbcStatementNew", [&](AdbcError* error) {
            return AdbcStatementNew(session_->connection(), &statement_, error);
        });
        if (!status) {
            return status;
        }
        statement_acquired_ = true;

        status = apply_adbc_options(
            "AdbcStatementSetOption", &statement_, session_->statement_options(),
            [](AdbcStatement* statement, const char* key, const char* value, AdbcError* error) {
                return AdbcStatementSetOption(statement, key, value, error);
            });
        if (!status) {
            return status;
        }

        status = call_adbc("AdbcStatementSetSqlQuery", [&](AdbcError* error) {
            return AdbcStatementSetSqlQuery(&statement_, sql.c_str(), error);
        });
        if (!status) {
            return status;
        }
        if (has_params(params) && params->rows() == 0) {
            // Zero parameter rows run nothing, so do not execute: given no
            // rows, the SQLite driver returns a stream it cannot describe and
            // leaks its reader. Ask for the result's schema instead; a driver
            // that cannot say gives a column-less empty result.
            no_executions_ = true;
            status = call_adbc("AdbcStatementPrepare", [&](AdbcError* error) {
                return AdbcStatementPrepare(&statement_, error);
            });
            if (!status) {
                return status;
            }
            AdbcError error{};
            schema_loaded_ =
                AdbcStatementExecuteSchema(&statement_, &schema_, &error) == ADBC_STATUS_OK;
            release_adbc_error(&error);
            return {};
        }
        if (has_params(params)) {
            status = prepare_statement(&statement_);
            if (!status) {
                return status;
            }
            if (runs_rows_one_by_one(*session_, params)) {
                row_params_ = params;
                return execute_row();
            }
            status = bound_.emplace_back().bind(&statement_, params);
            if (!status) {
                return status;
            }
        }
        return execute();
    }

    auto execute() -> std::expected<void, std::string> {
        std::int64_t rows_affected = -1;
        return call_adbc("AdbcStatementExecuteQuery", [&](AdbcError* error) {
            return AdbcStatementExecuteQuery(&statement_, &stream_, &rows_affected, error);
        });
    }

    /// Bind parameter row `next_row_` alone and execute it.
    auto execute_row() -> std::expected<void, std::string> {
        auto bound = bound_.emplace_back().bind(&statement_,
                                                table_rows(*row_params_, next_row_, next_row_ + 1));
        ++next_row_;
        if (!bound) {
            return bound;
        }
        return execute();
    }

    /// End the current result stream and start the next parameter row's.
    auto next_execution() -> std::expected<void, std::string> {
        ibex::interop::release_arrow_stream(&stream_);
        ibex::interop::release_arrow_schema(&schema_);
        std::memset(&stream_, 0, sizeof(stream_));
        std::memset(&schema_, 0, sizeof(schema_));
        schema_loaded_ = false;
        if (auto ran = execute_row(); !ran) {
            return ran;
        }
        if (const int status = stream_.get_schema(&stream_, &schema_); status != 0) {
            return std::unexpected(stream_error("ADBC stream get_schema", status));
        }
        schema_loaded_ = true;
        return {};
    }

    [[nodiscard]] auto stream_error(std::string_view context, int status) -> std::string {
        std::string message = function_ + ": ";
        message += context;
        message += " failed";
        if (stream_.get_last_error != nullptr) {
            if (const char* error = stream_.get_last_error(&stream_);
                error != nullptr && std::strlen(error) > 0) {
                message += ": ";
                message += error;
                return message;
            }
        }
        message += " (status ";
        message += std::to_string(status);
        message += ")";
        return message;
    }

    std::shared_ptr<AdbcSession> session_;
    std::string function_;
    // Outlive the statement: the destructor releases the statement first.
    BoundTables bound_;
    AdbcStatement statement_{};
    ::ArrowArrayStream stream_{};
    ::ArrowSchema schema_{};
    bool statement_acquired_ = false;
    bool schema_loaded_ = false;
    bool emitted_chunk_ = false;
    bool finished_ = false;
    bool no_executions_ = false;
    // Parameters run row by row (`runs_rows_one_by_one`), and the next row.
    std::shared_ptr<const ibex::runtime::Table> row_params_;
    std::size_t next_row_ = 0;
};

/// One statement on a session, holding the session's statement lease for as
/// long as it lives. For statements that return no result stream; queries use
/// `AdbcSourceOperator`, which must outlive the statement it reads.
class LeasedStatement {
   public:
    static auto open(std::shared_ptr<AdbcSession> session)
        -> std::expected<std::unique_ptr<LeasedStatement>, std::string> {
        if (auto lease = session->acquire_lease(); !lease) {
            return std::unexpected(lease.error());
        }
        auto statement = std::unique_ptr<LeasedStatement>(new LeasedStatement(std::move(session)));
        auto status = call_adbc("AdbcStatementNew", [&](AdbcError* error) {
            return AdbcStatementNew(statement->session_->connection(), &statement->statement_,
                                    error);
        });
        if (!status) {
            return std::unexpected(status.error());
        }
        statement->acquired_ = true;
        status = apply_adbc_options(
            "AdbcStatementSetOption", &statement->statement_,
            statement->session_->statement_options(),
            [](AdbcStatement* handle, const char* key, const char* value, AdbcError* error) {
                return AdbcStatementSetOption(handle, key, value, error);
            });
        if (!status) {
            return std::unexpected(status.error());
        }
        return statement;
    }

    LeasedStatement(const LeasedStatement&) = delete;
    LeasedStatement& operator=(const LeasedStatement&) = delete;
    LeasedStatement(LeasedStatement&&) noexcept = delete;
    LeasedStatement& operator=(LeasedStatement&&) noexcept = delete;

    ~LeasedStatement() {
        if (acquired_) {
            AdbcError error{};
            AdbcStatementRelease(&statement_, &error);
            release_adbc_error(&error);
        }
        session_->release_lease();
    }

    /// Record a failure on the connection (see `AdbcSession::statement_failed`).
    void failed() noexcept { session_->statement_failed(); }

    auto set_option(const char* key, const char* value) -> std::expected<void, std::string> {
        return call_adbc(std::string("AdbcStatementSetOption(") + key + ")", [&](AdbcError* error) {
            return AdbcStatementSetOption(&statement_, key, value, error);
        });
    }

    auto set_sql(const std::string& sql) -> std::expected<void, std::string> {
        return call_adbc("AdbcStatementSetSqlQuery", [&](AdbcError* error) {
            return AdbcStatementSetSqlQuery(&statement_, sql.c_str(), error);
        });
    }

    [[nodiscard]] auto session() -> AdbcSession& { return *session_; }

    auto prepare() -> std::expected<void, std::string> { return prepare_statement(&statement_); }

    /// Bind `table`: for ingestion, or as parameters of the prepared
    /// statement, one execution per row.
    auto bind(const std::shared_ptr<const ibex::runtime::Table>& table)
        -> std::expected<void, std::string> {
        return bound_.emplace_back().bind(&statement_, table);
    }

    /// Execute without a result stream. The affected-row count, or -1 when
    /// the driver does not report one.
    auto execute_update() -> std::expected<std::int64_t, std::string> {
        std::int64_t rows_affected = -1;
        auto status = call_adbc("AdbcStatementExecuteQuery", [&](AdbcError* error) {
            return AdbcStatementExecuteQuery(&statement_, nullptr, &rows_affected, error);
        });
        if (!status) {
            return std::unexpected(status.error());
        }
        return rows_affected;
    }

   private:
    explicit LeasedStatement(std::shared_ptr<AdbcSession> session) : session_(std::move(session)) {
        std::memset(&statement_, 0, sizeof(statement_));
    }

    std::shared_ptr<AdbcSession> session_;
    // Outlive the statement: the destructor releases the statement first.
    BoundTables bound_;
    AdbcStatement statement_{};
    bool acquired_ = false;
};

/// A string column with nulls, filled row by row.
struct NullableStrings {
    ibex::Column<std::string> values;
    std::vector<bool> valid;

    void push(const std::optional<std::string>& value) {
        values.push_back(value.value_or(std::string{}));
        valid.push_back(value.has_value());
    }

    void add_to(ibex::runtime::Table& table, std::string name) {
        table.add_column(std::move(name), std::move(values), ibex::runtime::ValidityBitmap(valid));
    }
};

/// Why reading a driver's Arrow stream failed.
auto stream_failure(::ArrowArrayStream& stream, std::string_view context, int status)
    -> std::string {
    std::string message = std::string(context) + " failed";
    if (stream.get_last_error != nullptr) {
        if (const char* error = stream.get_last_error(&stream);
            error != nullptr && std::strlen(error) > 0) {
            return message + ": " + error;
        }
    }
    return message + " (status " + std::to_string(status) + ")";
}

/// One row per table and view the connection can see: catalog, schema, table
/// and type, as `AdbcConnectionGetObjects` reports them. SQLite has catalogs
/// (`main`, `temp`) and no schemas; PostgreSQL the reverse, within the one
/// database it is connected to.
auto list_tables(AdbcSession& session) -> std::expected<ibex::runtime::Table, std::string> {
    ::ArrowArrayStream stream{};
    auto status = call_adbc("AdbcConnectionGetObjects", [&](AdbcError* error) {
        return AdbcConnectionGetObjects(session.connection(), ADBC_OBJECT_DEPTH_TABLES, nullptr,
                                        nullptr, nullptr, nullptr, nullptr, &stream, error);
    });
    if (!status) {
        return std::unexpected(status.error());
    }
    const auto stream_guard = std::unique_ptr<::ArrowArrayStream, void (*)(::ArrowArrayStream*)>(
        &stream, ibex::interop::release_arrow_stream);
    ::ArrowSchema schema{};
    if (const int code = stream.get_schema(&stream, &schema); code != 0) {
        return std::unexpected(stream_failure(stream, "AdbcConnectionGetObjects get_schema", code));
    }
    const auto schema_guard = std::unique_ptr<::ArrowSchema, void (*)(::ArrowSchema*)>(
        &schema, ibex::interop::release_arrow_schema);

    NullableStrings catalogs;
    NullableStrings schemas;
    NullableStrings names;
    NullableStrings types;
    while (true) {
        ::ArrowArray batch{};
        if (const int code = stream.get_next(&stream, &batch); code != 0) {
            return std::unexpected(
                stream_failure(stream, "AdbcConnectionGetObjects get_next", code));
        }
        if (batch.release == nullptr) {
            break;
        }
        const auto batch_guard = std::unique_ptr<::ArrowArray, void (*)(::ArrowArray*)>(
            &batch, ibex::interop::release_arrow_array);

        auto walked =
            ibex::adbc::read_object_rows(batch, schema, [&](const ibex::adbc::ObjectRow& row) {
                catalogs.push(row.catalog);
                schemas.push(row.db_schema);
                names.push(row.table);
                types.push(row.type);
            });
        if (!walked) {
            return std::unexpected("AdbcConnectionGetObjects returned an unexpected layout: " +
                                   walked.error());
        }
    }

    ibex::runtime::Table table;
    catalogs.add_to(table, "catalog");
    schemas.add_to(table, "schema");
    names.add_to(table, "table");
    types.add_to(table, "type");
    return table;
}

/// The Ibex type name of an imported column, as a declaration spells it.
auto ibex_type_name(const ibex::runtime::ColumnValue& column) -> std::string {
    return std::visit(
        [](const auto& values) -> std::string {
            using C = std::decay_t<decltype(values)>;
            if constexpr (std::is_same_v<C, ibex::Column<std::int64_t>>) {
                return "Int64";
            } else if constexpr (std::is_same_v<C, ibex::Column<double>>) {
                return "Float64";
            } else if constexpr (std::is_same_v<C, ibex::Column<bool>>) {
                return "Bool";
            } else if constexpr (std::is_same_v<C, ibex::Column<std::string>>) {
                return "String";
            } else if constexpr (std::is_same_v<C, ibex::Column<ibex::Categorical>>) {
                return "Categorical";
            } else if constexpr (std::is_same_v<C, ibex::Column<ibex::Date>>) {
                return "Date";
            } else if constexpr (std::is_same_v<C, ibex::Column<ibex::Timestamp>>) {
                return "Timestamp";
            } else {
                static_assert(std::is_same_v<C, ibex::Column<ibex::Decimal>>);
                const auto type = ibex::runtime::decimal_type_of(values);
                return "Decimal(" + std::to_string(type.precision) + ", " +
                       std::to_string(type.scale) + ")";
            }
        },
        column);
}

/// The Ibex type a query would give a column of this Arrow type, or the
/// reason it has none. Imports a zero-row table of that one field, so the
/// answer is the importer's own, error message included.
auto ibex_type_of(const ::ArrowSchema& field) -> std::expected<std::string, std::string> {
    std::array<::ArrowSchema*, 1> children{const_cast<::ArrowSchema*>(&field)};
    ::ArrowSchema wrapper{};
    wrapper.format = "+s";
    wrapper.name = "";
    wrapper.n_children = 1;
    wrapper.children = children.data();
    wrapper.release = [](::ArrowSchema* schema) { schema->release = nullptr; };
    auto imported = ibex::interop::empty_table_from_arrow_schema(wrapper);
    if (!imported) {
        return std::unexpected(imported.error());
    }
    if (imported->columns.size() != 1) {
        return std::unexpected("imports as " + std::to_string(imported->columns.size()) +
                               " columns");
    }
    return ibex_type_name(*imported->columns.front().column);
}

/// One row per column of a table: its name, Arrow type, the Ibex type a query
/// would give it (null when there is none, with the reason, and the SQL that
/// converts it, in `reason`), and whether it may hold nulls.
auto describe_table(AdbcSession& session, const std::string& table, const std::string& db_schema,
                    const std::string& catalog)
    -> std::expected<ibex::runtime::Table, std::string> {
    ::ArrowSchema schema{};
    auto status = call_adbc("AdbcConnectionGetTableSchema", [&](AdbcError* error) {
        return AdbcConnectionGetTableSchema(
            session.connection(), catalog.empty() ? nullptr : catalog.c_str(),
            db_schema.empty() ? nullptr : db_schema.c_str(), table.c_str(), &schema, error);
    });
    if (!status) {
        return std::unexpected(status.error());
    }
    const auto schema_guard = std::unique_ptr<::ArrowSchema, void (*)(::ArrowSchema*)>(
        &schema, ibex::interop::release_arrow_schema);

    ibex::Column<std::string> names;
    ibex::Column<std::string> arrow_types;
    NullableStrings ibex_types;
    NullableStrings reasons;
    ibex::Column<bool> nullable;
    for (std::int64_t i = 0; i < schema.n_children; ++i) {
        const ::ArrowSchema& field = *schema.children[i];
        const std::string name = field.name != nullptr ? field.name : "";
        names.push_back(name);
        arrow_types.push_back(ibex::interop::describe_arrow_type(field));
        // ARROW_FLAG_NULLABLE: Ibex's Arrow header, included first, defines
        // the structs without the flag macros.
        constexpr std::int64_t kArrowFlagNullable = 2;
        nullable.push_back((field.flags & kArrowFlagNullable) != 0);
        auto type = ibex_type_of(field);
        if (type) {
            ibex_types.push(*type);
            reasons.push(std::nullopt);
            continue;
        }
        // The importer's message starts with the column; the row says that.
        std::string reason = with_sql_advice(type.error());
        const std::string prefix = "column `" + name + "`: ";
        if (reason.starts_with(prefix)) {
            reason.erase(0, prefix.size());
        }
        ibex_types.push(std::nullopt);
        reasons.push(std::move(reason));
    }

    ibex::runtime::Table result;
    result.add_column("column", std::move(names));
    result.add_column("arrow_type", std::move(arrow_types));
    ibex_types.add_to(result, "ibex_type");
    result.add_column("nullable", std::move(nullable));
    reasons.add_to(result, "reason");
    return result;
}

/// Run a metadata call on a session under its statement lease. A failure
/// inside a transaction counts like a failed statement: on PostgreSQL the
/// driver's catalog queries share the transaction.
template <typename Fn>
auto run_metadata(AdbcSession& session, std::string_view function, Fn&& fn)
    -> std::expected<ibex::runtime::Table, std::string> {
    std::optional<ibex::runtime::Table> table;
    auto ran = session.with_lease([&]() -> std::expected<void, std::string> {
        auto result = fn();
        if (!result) {
            session.statement_failed();
            return std::unexpected(result.error());
        }
        table = std::move(*result);
        return {};
    });
    if (!ran) {
        return std::unexpected(std::string(function) + ": " + ran.error());
    }
    return std::move(*table);
}

auto session_of(const ibex::adbc::Connection& db, std::string_view function)
    -> std::expected<std::shared_ptr<AdbcSession>, std::string> {
    auto session = std::dynamic_pointer_cast<AdbcSession>(db.resource());
    if (session == nullptr) {
        return std::unexpected(std::string(function) + ": the first argument must be an " +
                               std::string(AdbcSession::kTypeName));
    }
    return session;
}

auto materialize(std::expected<ibex::runtime::OperatorPtr, std::string> source)
    -> std::expected<ibex::runtime::Table, std::string> {
    if (!source) {
        return std::unexpected(source.error());
    }
    ibex::runtime::MaterializeOperator sink(std::move(*source));
    return sink.run();
}

auto parse_options_of(std::string_view options) -> std::expected<ParsedOptions, std::string> {
    return ibex::adbc::parse_options(options);
}

/// The ADBC ingest mode for `write`'s `mode` argument.
auto ingest_mode(std::string_view mode) -> std::optional<const char*> {
    if (mode == "create") {
        return ADBC_INGEST_OPTION_MODE_CREATE;
    }
    if (mode == "append") {
        return ADBC_INGEST_OPTION_MODE_APPEND;
    }
    if (mode == "replace") {
        return ADBC_INGEST_OPTION_MODE_REPLACE;
    }
    if (mode == "create_append") {
        return ADBC_INGEST_OPTION_MODE_CREATE_APPEND;
    }
    return std::nullopt;
}

/// `begin`, `commit` and `rollback`: 1 when the call changed the transaction state.
template <typename Op>
auto transaction_call(const ibex::adbc::Connection& db, std::string_view function, Op&& op)
    -> std::expected<std::int64_t, std::string> {
    auto session = session_of(db, function);
    if (!session) {
        return std::unexpected(session.error());
    }
    auto changed = op(**session);
    if (!changed) {
        return std::unexpected(std::string(function) + ": " + changed.error());
    }
    return std::int64_t{*changed ? 1 : 0};
}

}  // namespace

namespace ibex::adbc {

auto connect(std::string_view driver, std::string_view uri, std::string_view options)
    -> std::expected<Connection, std::string> {
    auto parsed = parse_options_of(options);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    auto session = AdbcSession::open(std::string(driver), std::string(uri), *parsed);
    if (!session) {
        return std::unexpected("adbc::connect: " + session.error());
    }
    return Connection(runtime::ResourcePtr(std::move(*session)));
}

auto read_source(std::string_view driver, std::string_view uri, std::string_view sql,
                 std::string_view options) -> std::expected<runtime::OperatorPtr, std::string> {
    auto parsed = parse_options_of(options);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    // A connection of its own, released with the source.
    auto session = AdbcSession::open(std::string(driver), std::string(uri), *parsed);
    if (!session) {
        return std::unexpected("adbc::read: " + session.error());
    }
    return AdbcSourceOperator::create(std::move(*session), std::string(sql), "adbc::read");
}

auto read(std::string_view driver, std::string_view uri, std::string_view sql,
          std::string_view options) -> std::expected<runtime::Table, std::string> {
    return materialize(read_source(driver, uri, sql, options));
}

auto query(const Connection& db, std::string_view sql, const TablePtr& params)
    -> std::expected<runtime::Table, std::string> {
    auto session = session_of(db, "adbc::query");
    if (!session) {
        return std::unexpected(session.error());
    }
    return materialize(
        AdbcSourceOperator::create(std::move(*session), std::string(sql), "adbc::query", params));
}

auto execute(const Connection& db, std::string_view sql, const TablePtr& params)
    -> std::expected<std::int64_t, std::string> {
    auto session = session_of(db, "adbc::execute");
    if (!session) {
        return std::unexpected(session.error());
    }
    auto statement = LeasedStatement::open(std::move(*session));
    if (!statement) {
        return std::unexpected("adbc::execute: " + statement.error());
    }
    auto& stmt = **statement;
    auto rows =
        stmt.set_sql(std::string(sql)).and_then([&]() -> std::expected<std::int64_t, std::string> {
            if (!has_params(params)) {
                return stmt.execute_update();
            }
            if (!runs_rows_one_by_one(stmt.session(), params)) {
                return stmt.prepare().and_then([&] { return stmt.bind(params); }).and_then([&] {
                    return stmt.execute_update();
                });
            }
            // One execution per row; the count is unknown if any row's is.
            if (auto prepared = stmt.prepare(); !prepared) {
                return std::unexpected(prepared.error());
            }
            std::int64_t total = 0;
            for (std::size_t row = 0; row < params->rows(); ++row) {
                auto count = stmt.bind(table_rows(*params, row, row + 1)).and_then([&] {
                    return stmt.execute_update();
                });
                if (!count) {
                    return count;
                }
                total = total < 0 || *count < 0 ? -1 : total + *count;
            }
            return total;
        });
    if (!rows) {
        stmt.failed();
        return std::unexpected("adbc::execute: " + rows.error());
    }
    return *rows;
}

auto write(const Connection& db, const TablePtr& table, std::string_view target,
           std::string_view mode) -> std::expected<std::int64_t, std::string> {
    auto session = session_of(db, "adbc::write");
    if (!session) {
        return std::unexpected(session.error());
    }
    if (table == nullptr) {
        return std::unexpected("adbc::write(db, df, table, mode) expects a DataFrame");
    }
    const auto adbc_mode = ingest_mode(mode);
    if (!adbc_mode) {
        return std::unexpected("adbc::write: unknown mode '" + std::string(mode) +
                               "'; expected create, append, replace or create_append");
    }
    const std::string target_name(target);

    // MySQL commits at CREATE TABLE and DROP TABLE, which ends a transaction
    // without a word: the rows after it would be committed one batch at a
    // time, whatever the script does next.
    const bool batched = (*session)->quirks().batched_ingest;
    const bool creates = *adbc_mode != std::string_view(ADBC_INGEST_OPTION_MODE_APPEND);
    if (batched && creates && (*session)->in_transaction()) {
        return std::unexpected(
            "adbc::write: MySQL commits the open transaction when it creates or drops a table, "
            "so inside adbc::begin only mode \"append\" is supported; create the table "
            "before adbc::begin");
    }

    auto statement = LeasedStatement::open(std::move(*session));
    if (!statement) {
        return std::unexpected("adbc::write: " + statement.error());
    }
    auto& stmt = **statement;
    const auto ingest = [&](const char* ingest_mode,
                            const std::shared_ptr<const ibex::runtime::Table>& rows) {
        return stmt.set_option(ADBC_INGEST_OPTION_MODE, ingest_mode)
            .and_then([&] { return stmt.bind(rows); })
            .and_then([&] { return stmt.execute_update(); });
    };
    auto rows =
        stmt.set_option(ADBC_INGEST_OPTION_TARGET_TABLE, target_name.c_str())
            .and_then([&]() -> std::expected<std::int64_t, std::string> {
                if (stmt.session().quirks().ingest_hides_failure) {
                    return stmt.session().ingest_atomically(
                        [&] { return ingest(*adbc_mode, table); });
                }
                if (!batched) {
                    return ingest(*adbc_mode, table);
                }
                // The table's DDL first, through a write of no rows, so
                // that the rows go in one transaction that nothing ends.
                if (creates) {
                    if (auto created = ingest(*adbc_mode, table_rows(*table, 0, 0)); !created) {
                        return created;
                    }
                }
                return stmt.session().ingest_atomically(
                    [&] { return ingest(ADBC_INGEST_OPTION_MODE_APPEND, table); });
            });
    if (!rows) {
        stmt.failed();
        return std::unexpected("adbc::write: " + rows.error());
    }
    // The driver's count, or the table's row count when the driver reports none.
    return *rows >= 0 ? *rows : static_cast<std::int64_t>(table->rows());
}

auto tables(const Connection& db) -> std::expected<runtime::Table, std::string> {
    auto session = session_of(db, "adbc::tables");
    if (!session) {
        return std::unexpected(session.error());
    }
    return run_metadata(**session, "adbc::tables", [&] { return list_tables(**session); });
}

auto table_schema(const Connection& db, std::string_view table, std::string_view schema,
                  std::string_view catalog) -> std::expected<runtime::Table, std::string> {
    auto session = session_of(db, "adbc::table_schema");
    if (!session) {
        return std::unexpected(session.error());
    }
    const std::string table_name(table);
    const std::string schema_name(schema);
    const std::string catalog_name(catalog);
    return run_metadata(**session, "adbc::table_schema", [&] {
        return describe_table(**session, table_name, schema_name, catalog_name);
    });
}

auto begin(const Connection& db) -> std::expected<std::int64_t, std::string> {
    return transaction_call(db, "adbc::begin", [](AdbcSession& session) {
        return session.begin().transform([] { return true; });
    });
}

auto commit(const Connection& db) -> std::expected<std::int64_t, std::string> {
    return transaction_call(db, "adbc::commit", [](AdbcSession& session) {
        return session.commit().transform([] { return true; });
    });
}

auto rollback(const Connection& db) -> std::expected<std::int64_t, std::string> {
    return transaction_call(db, "adbc::rollback",
                            [](AdbcSession& session) { return session.rollback(); });
}

auto close(const Connection& db) -> std::expected<std::int64_t, std::string> {
    auto session = session_of(db, "adbc::close");
    if (!session) {
        return std::unexpected(session.error());
    }
    auto closed = (*session)->close();
    if (!closed) {
        return std::unexpected("adbc::close: " + closed.error());
    }
    return std::int64_t{*closed ? 1 : 0};
}

}  // namespace ibex::adbc
