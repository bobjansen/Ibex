// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Ibex plugin entry point for adbc.hpp.
//
// Build as a shared library alongside adbc.ibex and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   import "adbc";
//   let df = read_adbc("adbc_driver_sqlite", "", "select 1 as x");

#include <ibex/core/text.hpp>
#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <cstdint>
#include <cstring>
#include <expected>
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

class AdbcSourceOperator final : public ibex::runtime::Operator {
   public:
    static auto create(std::string driver, std::string uri, std::string sql, ParsedOptions options)
        -> std::expected<ibex::runtime::OperatorPtr, std::string> {
        auto op = std::unique_ptr<AdbcSourceOperator>(new AdbcSourceOperator());
        auto init = op->init(std::move(driver), std::move(uri), std::move(sql), std::move(options));
        if (!init) {
            // `op` is destroyed here, releasing whatever handles init acquired.
            return std::unexpected("read_adbc: " + init.error());
        }
        return ibex::runtime::OperatorPtr(std::move(op));
    }

    AdbcSourceOperator(const AdbcSourceOperator&) = delete;
    AdbcSourceOperator& operator=(const AdbcSourceOperator&) = delete;
    AdbcSourceOperator(AdbcSourceOperator&&) noexcept = delete;
    AdbcSourceOperator& operator=(AdbcSourceOperator&&) noexcept = delete;

    ~AdbcSourceOperator() override {
        // Children before parents: stream, statement, connection, database.
        ibex::interop::release_arrow_stream(&stream_);
        ibex::interop::release_arrow_schema(&schema_);
        if (statement_acquired_) {
            AdbcError error{};
            AdbcStatementRelease(&statement_, &error);
            release_adbc_error(&error);
        }
        if (connection_acquired_) {
            AdbcError error{};
            AdbcConnectionRelease(&connection_, &error);
            release_adbc_error(&error);
        }
        if (database_acquired_) {
            AdbcError error{};
            AdbcDatabaseRelease(&database_, &error);
            release_adbc_error(&error);
        }
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
                    return std::unexpected("read_adbc: result schema import failed: " +
                                           empty.error());
                }
                return make_chunk(std::move(*empty));
            }

            auto batch_guard = std::unique_ptr<::ArrowArray, void (*)(::ArrowArray*)>(
                &batch, ibex::interop::release_arrow_array);

            auto imported = ibex::interop::adopt_table_from_arrow(&batch, schema_);
            if (!imported) {
                finished_ = true;
                return std::unexpected("read_adbc: batch import failed: " + imported.error());
            }
            if (imported->rows() == 0) {
                continue;
            }
            return make_chunk(std::move(*imported));
        }
    }

   private:
    AdbcSourceOperator() {
        std::memset(&database_, 0, sizeof(database_));
        std::memset(&connection_, 0, sizeof(connection_));
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

    auto init(std::string driver, std::string uri, std::string sql, ParsedOptions options)
        -> std::expected<void, std::string> {
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

        status = apply_adbc_options("AdbcConnectionSetOption", &connection_,
                                    options.connection_post, set_connection_option);
        if (!status) {
            return status;
        }

        status = call_adbc("AdbcStatementNew", [&](AdbcError* error) {
            return AdbcStatementNew(&connection_, &statement_, error);
        });
        if (!status) {
            return status;
        }
        statement_acquired_ = true;

        status = apply_adbc_options(
            "AdbcStatementSetOption", &statement_, options.statement,
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
        status = call_adbc("AdbcStatementExecuteQuery", [&](AdbcError* error) {
            return AdbcStatementExecuteQuery(&statement_, &stream_, &rows_affected, error);
        });
        if (!status) {
            return status;
        }
        return {};
    }

    [[nodiscard]] auto stream_error(std::string_view context, int status) -> std::string {
        std::string message = "read_adbc: ";
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

    AdbcDatabase database_{};
    AdbcConnection connection_{};
    AdbcStatement statement_{};
    ::ArrowArrayStream stream_{};
    ::ArrowSchema schema_{};
    bool database_acquired_ = false;
    bool connection_acquired_ = false;
    bool statement_acquired_ = false;
    bool schema_loaded_ = false;
    bool emitted_chunk_ = false;
    bool finished_ = false;
};

auto make_adbc_source(const ibex::runtime::ExternArgs& args)
    -> std::expected<ibex::runtime::OperatorPtr, std::string> {
    if (args.size() != 3 && args.size() != 4) {
        return std::unexpected("read_adbc() expects 3 or 4 string arguments");
    }
    const auto* driver = std::get_if<std::string>(&args[0]);
    const auto* uri = std::get_if<std::string>(&args[1]);
    const auto* sql = std::get_if<std::string>(&args[2]);
    if (driver == nullptr || uri == nullptr || sql == nullptr) {
        return std::unexpected("read_adbc(driver, uri, sql[, options]) expects string arguments");
    }

    ParsedOptions options;
    if (args.size() == 4) {
        const auto* option_spec = std::get_if<std::string>(&args[3]);
        if (option_spec == nullptr) {
            return std::unexpected(
                "read_adbc(driver, uri, sql, options) expects a string options spec");
        }
        auto parsed = ibex::adbc::parse_options(*option_spec);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        options = std::move(*parsed);
    }

    return AdbcSourceOperator::create(*driver, *uri, *sql, std::move(options));
}

}  // namespace

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("read_adbc",
                             [](const ibex::runtime::ExternArgs& args)
                                 -> std::expected<ibex::runtime::ExternValue, std::string> {
                                 auto source = make_adbc_source(args);
                                 if (!source) {
                                     return std::unexpected(source.error());
                                 }
                                 ibex::runtime::MaterializeOperator sink(std::move(*source));
                                 auto table = sink.run();
                                 if (!table) {
                                     return std::unexpected(table.error());
                                 }
                                 return ibex::runtime::ExternValue{std::move(*table)};
                             });

    registry->register_chunked_table("read_adbc",
                                     [](const ibex::runtime::ExternArgs& args)
                                         -> std::expected<ibex::runtime::OperatorPtr, std::string> {
                                         return make_adbc_source(args);
                                     });
}
