// Ibex plugin entry point for adbc.hpp.
//
// Build as a shared library alongside adbc.ibex and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   import "adbc";
//   let df = read_adbc("adbc_driver_sqlite", "", "select 1 as x");

#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <arrow-adbc/adbc.h>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

struct ParsedAdbcOptions {
    std::string entrypoint;
    std::vector<std::pair<std::string, std::string>> database;
    std::vector<std::pair<std::string, std::string>> connection;
    std::vector<std::pair<std::string, std::string>> statement;
};

auto trim(std::string_view text) -> std::string_view {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0) {
        text.remove_suffix(1);
    }
    return text;
}

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
auto apply_adbc_options(std::string_view context, Handle* handle,
                        const std::vector<std::pair<std::string, std::string>>& options,
                        Setter&& setter) -> std::expected<void, std::string> {
    for (const auto& [key, value] : options) {
        auto status = call_adbc(context, [&](AdbcError* error) {
            return setter(handle, key.c_str(), value.c_str(), error);
        });
        if (!status) {
            return status;
        }
    }
    return {};
}

auto parse_adbc_options(std::string_view spec) -> std::expected<ParsedAdbcOptions, std::string> {
    ParsedAdbcOptions parsed;
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        const std::size_t next = spec.find_first_of(";\n", pos);
        std::string_view item =
            next == std::string_view::npos ? spec.substr(pos) : spec.substr(pos, next - pos);
        item = trim(item);
        if (!item.empty()) {
            const std::size_t eq = item.find('=');
            if (eq == std::string_view::npos || eq == 0 || eq + 1 >= item.size()) {
                return std::unexpected(
                    "read_adbc options must be key=value entries separated by ';' or newlines");
            }
            const std::string_view raw_key = trim(item.substr(0, eq));
            const std::string_view raw_value = trim(item.substr(eq + 1));
            const std::string key(raw_key);
            const std::string value(raw_value);
            if (key == "entrypoint") {
                parsed.entrypoint = value;
            } else if (key.rfind("db.", 0) == 0) {
                parsed.database.emplace_back(key.substr(3), value);
            } else if (key.rfind("conn.", 0) == 0) {
                parsed.connection.emplace_back(key.substr(5), value);
            } else if (key.rfind("stmt.", 0) == 0) {
                parsed.statement.emplace_back(key.substr(5), value);
            } else {
                parsed.database.emplace_back(key, value);
            }
        }
        if (next == std::string_view::npos) {
            break;
        }
        pos = next + 1;
    }
    return parsed;
}

class AdbcSourceOperator final : public ibex::runtime::Operator {
   public:
    static auto create(std::string driver, std::string uri, std::string sql,
                       ParsedAdbcOptions options)
        -> std::expected<ibex::runtime::OperatorPtr, std::string> {
        auto op = std::unique_ptr<AdbcSourceOperator>(new AdbcSourceOperator());
        auto init = op->init(std::move(driver), std::move(uri), std::move(sql), std::move(options));
        if (!init) {
            return std::unexpected(std::move(init.error()));
        }
        return ibex::runtime::OperatorPtr(std::move(op));
    }

    AdbcSourceOperator(const AdbcSourceOperator&) = delete;
    AdbcSourceOperator& operator=(const AdbcSourceOperator&) = delete;
    AdbcSourceOperator(AdbcSourceOperator&&) noexcept = delete;
    AdbcSourceOperator& operator=(AdbcSourceOperator&&) noexcept = delete;

    ~AdbcSourceOperator() override {
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
                return std::optional<ibex::runtime::Chunk>{};
            }

            auto batch_guard = std::unique_ptr<::ArrowArray, void (*)(::ArrowArray*)>(
                &batch, ibex::interop::release_arrow_array);

            auto imported = ibex::interop::import_table_from_arrow(batch, schema_);
            if (!imported) {
                finished_ = true;
                return std::unexpected("ADBC stream batch import failed: " + imported.error());
            }
            if (imported->rows() == 0) {
                continue;
            }

            ibex::runtime::Chunk chunk;
            chunk.columns = std::move(imported->columns);
            chunk.ordering = std::move(imported->ordering);
            chunk.time_index = std::move(imported->time_index);
            return std::optional<ibex::runtime::Chunk>{std::move(chunk)};
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

    auto init(std::string driver, std::string uri, std::string sql, ParsedAdbcOptions options)
        -> std::expected<void, std::string> {
        auto status = call_adbc("AdbcDatabaseNew", [&](AdbcError* error) {
            return AdbcDatabaseNew(&database_, error);
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

        database_acquired_ = true;

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

        status = apply_adbc_options(
            "AdbcConnectionSetOption", &connection_, options.connection,
            [](AdbcConnection* connection, const char* key, const char* value, AdbcError* error) {
                return AdbcConnectionSetOption(connection, key, value, error);
            });
        if (!status) {
            return status;
        }

        status = call_adbc("AdbcConnectionInit", [&](AdbcError* error) {
            return AdbcConnectionInit(&connection_, &database_, error);
        });
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
        std::string message(context);
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

    ParsedAdbcOptions options;
    if (args.size() == 4) {
        const auto* option_spec = std::get_if<std::string>(&args[3]);
        if (option_spec == nullptr) {
            return std::unexpected(
                "read_adbc(driver, uri, sql, options) expects a string options spec");
        }
        auto parsed = parse_adbc_options(*option_spec);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        options = std::move(*parsed);
    }

    return AdbcSourceOperator::create(*driver, *uri, *sql, std::move(options));
}

}  // namespace

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
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
