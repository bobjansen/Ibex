// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Ibex plugin entry point for adbc.hpp.
//
// Build as a shared library alongside adbc.ibex and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   import "adbc";
//   let df = adbc_read("adbc_driver_sqlite", "", "select 1 as x");

#include <ibex/core/text.hpp>
#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <array>
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <cstdint>
#include <cstring>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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
/// with the scripts' `wheel_for` tables.
constexpr std::array<std::string_view, 2> kInstallableDrivers{"sqlite", "postgresql"};

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
/// in the query itself. `CAST(x AS TEXT)` is standard SQL, so it holds for
/// every driver.
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
                    "cast it in the query, e.g. CAST(" + name + " AS TEXT)");
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

/// One open ADBC database and connection: the value behind an Ibex
/// `AdbcConnection`. Shared by every binding of it and by the query running on
/// it (a statement lease), so the handles outlive whichever of them drops
/// first. Allows one active statement at a time.
///
/// Tables imported from a query may keep zero-copy buffers whose release
/// callbacks live in the driver library. The pinned driver manager never
/// unloads a driver (`ManagedLibrary::Release` is a no-op, apache/arrow-adbc#204),
/// so those buffers stay valid after `adbc_close` and after this object is gone.
class AdbcSession final : public ibex::runtime::Resource {
   public:
    static constexpr std::string_view kTypeName = "AdbcConnection";

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

    [[nodiscard]] auto connection() noexcept -> AdbcConnection* { return &connection_; }
    [[nodiscard]] auto statement_options() const noexcept -> const OptionList& {
        return statement_options_;
    }

   private:
    AdbcSession() {
        std::memset(&database_, 0, sizeof(database_));
        std::memset(&connection_, 0, sizeof(connection_));
    }

    /// Connection before database. Reports the first failure; both are
    /// released either way.
    auto release_handles() noexcept -> std::expected<void, std::string> {
        std::expected<void, std::string> result;
        if (connection_acquired_) {
            connection_acquired_ = false;
            result = call_adbc("AdbcConnectionRelease", [&](AdbcError* error) {
                return AdbcConnectionRelease(&connection_, error);
            });
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
};

/// Streams one query's result. Holds a statement lease on its session for as
/// long as it lives.
class AdbcSourceOperator final : public ibex::runtime::Operator {
   public:
    static auto create(std::shared_ptr<AdbcSession> session, const std::string& sql,
                       std::string_view function)
        -> std::expected<ibex::runtime::OperatorPtr, std::string> {
        const std::string prefix = std::string(function) + ": ";
        if (auto lease = session->acquire_lease(); !lease) {
            return std::unexpected(prefix + lease.error());
        }
        auto op = std::unique_ptr<AdbcSourceOperator>(
            new AdbcSourceOperator(std::move(session), std::string(function)));
        auto init = op->init(sql);
        if (!init) {
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

        if (!schema_loaded_) {
            const int status = stream_.get_schema(&stream_, &schema_);
            if (status != 0) {
                finished_ = true;
                return std::unexpected(stream_error("ADBC stream get_schema", status));
            }
            schema_loaded_ = true;
        }

        while (true) {
            ::ArrowArray batch{};
            const int status = stream_.get_next(&stream_, &batch);
            if (status != 0) {
                finished_ = true;
                return std::unexpected(stream_error("ADBC stream get_next", status));
            }
            if (batch.release == nullptr) {
                finished_ = true;
                if (emitted_chunk_) {
                    return std::optional<ibex::runtime::Chunk>{};
                }
                // No rows at all. Emit one empty chunk so the result keeps the
                // query's columns instead of collapsing to a column-less table.
                auto empty = ibex::interop::empty_table_from_arrow_schema(schema_);
                if (!empty) {
                    return std::unexpected(function_ + ": result schema import failed: " +
                                           with_sql_advice(empty.error()));
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
                finished_ = true;
                return std::unexpected(
                    function_ + ": batch import failed: " + with_sql_advice(imported.error()));
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

    auto make_chunk(ibex::runtime::Table table)
        -> std::expected<std::optional<ibex::runtime::Chunk>, std::string> {
        emitted_chunk_ = true;
        ibex::runtime::Chunk chunk;
        chunk.columns = std::move(table.columns);
        chunk.set_properties(table.properties());
        return std::optional<ibex::runtime::Chunk>{std::move(chunk)};
    }

    auto init(const std::string& sql) -> std::expected<void, std::string> {
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

        std::int64_t rows_affected = -1;
        return call_adbc("AdbcStatementExecuteQuery", [&](AdbcError* error) {
            return AdbcStatementExecuteQuery(&statement_, &stream_, &rows_affected, error);
        });
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
    AdbcStatement statement_{};
    ::ArrowArrayStream stream_{};
    ::ArrowSchema schema_{};
    bool statement_acquired_ = false;
    bool schema_loaded_ = false;
    bool emitted_chunk_ = false;
    bool finished_ = false;
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

    /// Bind `array` and `schema`. The driver takes the array over; whatever
    /// it leaves of either is the caller's to release, after this statement.
    auto bind(::ArrowArray* array, ::ArrowSchema* schema) -> std::expected<void, std::string> {
        return call_adbc("AdbcStatementBind", [&](AdbcError* error) {
            return AdbcStatementBind(&statement_, array, schema, error);
        });
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
    AdbcStatement statement_{};
    bool acquired_ = false;
};

auto parse_option_arg(const ibex::runtime::ExternArgs& args, std::size_t index,
                      std::string_view usage) -> std::expected<ParsedOptions, std::string> {
    if (args.size() <= index) {
        return ParsedOptions{};
    }
    const auto* option_spec = std::get_if<std::string>(&args[index]);
    if (option_spec == nullptr) {
        return std::unexpected(std::string(usage) + " expects a string options spec");
    }
    return ibex::adbc::parse_options(*option_spec);
}

auto make_adbc_source(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::OperatorPtr, std::string> {
    if (args.size() != 3 && args.size() != 4) {
        return std::unexpected("adbc_read() expects 3 or 4 string arguments");
    }
    const auto* driver = std::get_if<std::string>(&args[0]);
    const auto* uri = std::get_if<std::string>(&args[1]);
    const auto* sql = std::get_if<std::string>(&args[2]);
    if (driver == nullptr || uri == nullptr || sql == nullptr) {
        return std::unexpected("adbc_read(driver, uri, sql[, options]) expects string arguments");
    }
    auto options = parse_option_arg(args, 3, "adbc_read(driver, uri, sql, options)");
    if (!options) {
        return std::unexpected(options.error());
    }
    // A connection of its own, released with the source.
    auto session = AdbcSession::open(*driver, *uri, *options);
    if (!session) {
        return std::unexpected("adbc_read: " + session.error());
    }
    return AdbcSourceOperator::create(std::move(*session), *sql, "adbc_read");
}

auto materialize(std::expected<ibex::runtime::OperatorPtr, std::string> source)
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    if (!source) {
        return std::unexpected(source.error());
    }
    ibex::runtime::MaterializeOperator sink(std::move(*source));
    auto table = sink.run();
    if (!table) {
        return std::unexpected(table.error());
    }
    return ibex::runtime::ExternValue{std::move(*table)};
}

auto session_arg(const ibex::runtime::ExternArgs& args, std::string_view function)
    -> std::expected<std::shared_ptr<AdbcSession>, std::string> {
    auto session = args.resource_as<AdbcSession>(0);
    if (session == nullptr) {
        return std::unexpected(std::string(function) + ": the first argument must be an " +
                               std::string(AdbcSession::kTypeName));
    }
    return session;
}

auto adbc_connect(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    if (args.size() != 2 && args.size() != 3) {
        return std::unexpected("adbc_connect(driver, uri[, options]) expects 2 or 3 arguments");
    }
    const auto* driver = std::get_if<std::string>(&args[0]);
    const auto* uri = std::get_if<std::string>(&args[1]);
    if (driver == nullptr || uri == nullptr) {
        return std::unexpected("adbc_connect(driver, uri[, options]) expects string arguments");
    }
    auto options = parse_option_arg(args, 2, "adbc_connect(driver, uri, options)");
    if (!options) {
        return std::unexpected(options.error());
    }
    auto session = AdbcSession::open(*driver, *uri, *options);
    if (!session) {
        return std::unexpected("adbc_connect: " + session.error());
    }
    return ibex::runtime::ExternValue{ibex::runtime::ResourcePtr(std::move(*session))};
}

auto adbc_query(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    auto session = session_arg(args, "adbc_query");
    if (!session) {
        return std::unexpected(session.error());
    }
    const auto* sql = args.size() == 2 ? std::get_if<std::string>(&args[1]) : nullptr;
    if (sql == nullptr) {
        return std::unexpected("adbc_query(db, sql) expects a string query");
    }
    return materialize(AdbcSourceOperator::create(std::move(*session), *sql, "adbc_query"));
}

auto adbc_execute(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    auto session = session_arg(args, "adbc_execute");
    if (!session) {
        return std::unexpected(session.error());
    }
    const auto* sql = args.size() == 2 ? std::get_if<std::string>(&args[1]) : nullptr;
    if (sql == nullptr) {
        return std::unexpected("adbc_execute(db, sql) expects a string statement");
    }
    auto statement = LeasedStatement::open(std::move(*session));
    if (!statement) {
        return std::unexpected("adbc_execute: " + statement.error());
    }
    auto rows =
        (*statement)->set_sql(*sql).and_then([&] { return (*statement)->execute_update(); });
    if (!rows) {
        return std::unexpected("adbc_execute: " + rows.error());
    }
    return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{*rows}};
}

/// The ADBC ingest mode for `adbc_write`'s `mode` argument.
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

/// Bulk-ingest a table through ADBC. Returns the rows written: the driver's
/// count, or the table's row count when the driver reports none.
auto adbc_write(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    constexpr std::string_view kUsage = "adbc_write(db, df, table, mode)";
    auto session = session_arg(args, "adbc_write");
    if (!session) {
        return std::unexpected(session.error());
    }
    const auto table = args.size() == 4 ? args.table(1) : nullptr;
    const auto* target = args.size() == 4 ? std::get_if<std::string>(&args[2]) : nullptr;
    const auto* mode = args.size() == 4 ? std::get_if<std::string>(&args[3]) : nullptr;
    if (table == nullptr || target == nullptr || mode == nullptr) {
        return std::unexpected(std::string(kUsage) +
                               " expects a connection, a DataFrame and two strings");
    }
    const auto adbc_mode = ingest_mode(*mode);
    if (!adbc_mode) {
        return std::unexpected("adbc_write: unknown mode '" + *mode +
                               "'; expected create, append, replace or create_append");
    }

    ::ArrowArray array{};
    ::ArrowSchema schema{};
    if (auto exported = ibex::interop::export_table_to_arrow(table, &array, &schema); !exported) {
        return std::unexpected("adbc_write: " + exported.error());
    }
    // The driver may hold what it was bound until the statement is released,
    // so the statement lives in the inner scope below and the exports outlive it.
    const auto release_exports = [&] {
        ibex::interop::release_arrow_array(&array);
        ibex::interop::release_arrow_schema(&schema);
    };
    std::expected<std::int64_t, std::string> rows = std::unexpected(std::string{});
    {
        auto statement = LeasedStatement::open(std::move(*session));
        if (!statement) {
            release_exports();
            return std::unexpected("adbc_write: " + statement.error());
        }
        auto& stmt = **statement;
        rows = stmt.set_option(ADBC_INGEST_OPTION_TARGET_TABLE, target->c_str())
                   .and_then([&] { return stmt.set_option(ADBC_INGEST_OPTION_MODE, *adbc_mode); })
                   .and_then([&] { return stmt.bind(&array, &schema); })
                   .and_then([&] { return stmt.execute_update(); });
    }
    release_exports();
    if (!rows) {
        return std::unexpected("adbc_write: " + rows.error());
    }
    const std::int64_t written = *rows >= 0 ? *rows : static_cast<std::int64_t>(table->rows());
    return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{written}};
}

auto adbc_close(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    auto session = session_arg(args, "adbc_close");
    if (!session) {
        return std::unexpected(session.error());
    }
    auto closed = (*session)->close();
    if (!closed) {
        return std::unexpected("adbc_close: " + closed.error());
    }
    return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{std::int64_t{*closed ? 1 : 0}}};
}

}  // namespace

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("adbc_read", [](const ibex::runtime::ExternArgs& args) {
        return materialize(make_adbc_source(args));
    });
    registry->register_chunked_table("adbc_read",
                                     [](const ibex::runtime::ExternArgs& args)
                                         -> std::expected<ibex::runtime::OperatorPtr, std::string> {
                                         return make_adbc_source(args);
                                     });

    registry->register_resource("adbc_connect", adbc_connect);
    registry->register_table("adbc_query", adbc_query);
    registry->register_scalar("adbc_execute", ibex::runtime::ScalarKind::Int, adbc_execute);
    registry->register_scalar("adbc_write", ibex::runtime::ScalarKind::Int, adbc_write);
    registry->register_scalar("adbc_close", ibex::runtime::ScalarKind::Int, adbc_close);
}
