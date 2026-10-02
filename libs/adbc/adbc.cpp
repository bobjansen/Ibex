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
//
// The ADBC client itself is the `ibex_adbc` library (adbc_client.hpp); this file
// turns the REPL's `ExternArgs` into calls on it and its results into
// `ExternValue`s. Compiled programs call the same library through adbc.hpp.

#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "adbc_client.hpp"

namespace {

using ibex::adbc::Connection;
using ibex::runtime::ExternArgs;
using ibex::runtime::ExternValue;

template <typename T>
auto wrap(std::expected<T, std::string> result) -> std::expected<ExternValue, std::string> {
    if (!result) {
        return std::unexpected(std::move(result.error()));
    }
    if constexpr (std::is_same_v<T, std::int64_t>) {
        return ExternValue{ibex::runtime::ScalarValue{*result}};
    } else {
        return ExternValue{std::move(*result)};
    }
}

/// The string argument at `index`, or null when it is absent or not a string.
auto string_arg(const ExternArgs& args, std::size_t index) -> const std::string* {
    return args.size() > index ? std::get_if<std::string>(&args[index]) : nullptr;
}

/// The optional options spec at `index`: empty when the argument is absent, and
/// a usage error when it is present but not a string.
auto options_arg(const ExternArgs& args, std::size_t index, std::string_view usage)
    -> std::expected<std::string_view, std::string> {
    if (args.size() <= index) {
        return std::string_view{};
    }
    const auto* spec = std::get_if<std::string>(&args[index]);
    if (spec == nullptr) {
        return std::unexpected(std::string(usage) + " expects a string options spec");
    }
    return std::string_view{*spec};
}

auto connection_arg(const ExternArgs& args) -> Connection {
    return Connection(args.resource(0));
}

auto adbc_read_source(const ExternArgs& args)
    -> std::expected<ibex::runtime::OperatorPtr, std::string> {
    if (args.size() != 3 && args.size() != 4) {
        return std::unexpected("adbc_read() expects 3 or 4 string arguments");
    }
    const auto* driver = string_arg(args, 0);
    const auto* uri = string_arg(args, 1);
    const auto* sql = string_arg(args, 2);
    if (driver == nullptr || uri == nullptr || sql == nullptr) {
        return std::unexpected("adbc_read(driver, uri, sql[, options]) expects string arguments");
    }
    auto options = options_arg(args, 3, "adbc_read(driver, uri, sql, options)");
    if (!options) {
        return std::unexpected(options.error());
    }
    return ibex::adbc::read_source(*driver, *uri, *sql, *options);
}

auto adbc_read(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    auto source = adbc_read_source(args);
    if (!source) {
        return std::unexpected(source.error());
    }
    ibex::runtime::MaterializeOperator sink(std::move(*source));
    return wrap(sink.run());
}

auto adbc_connect(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    if (args.size() != 2 && args.size() != 3) {
        return std::unexpected("adbc_connect(driver, uri[, options]) expects 2 or 3 arguments");
    }
    const auto* driver = string_arg(args, 0);
    const auto* uri = string_arg(args, 1);
    if (driver == nullptr || uri == nullptr) {
        return std::unexpected("adbc_connect(driver, uri[, options]) expects string arguments");
    }
    auto options = options_arg(args, 2, "adbc_connect(driver, uri, options)");
    if (!options) {
        return std::unexpected(options.error());
    }
    auto connection = ibex::adbc::connect(*driver, *uri, *options);
    if (!connection) {
        return std::unexpected(connection.error());
    }
    return ExternValue{connection->resource()};
}

/// The arguments of `adbc_query` and `adbc_execute`: `(db, sql[, params])`.
struct SqlCall {
    Connection db;
    const std::string* sql = nullptr;
    ibex::adbc::TablePtr params;
};

auto sql_call(const ExternArgs& args, std::string_view usage)
    -> std::expected<SqlCall, std::string> {
    SqlCall call{.db = connection_arg(args)};
    call.sql = args.size() >= 2 && args.size() <= 3 ? string_arg(args, 1) : nullptr;
    call.params = args.size() == 3 ? args.table(2) : nullptr;
    if (call.sql == nullptr || (args.size() == 3 && call.params == nullptr)) {
        return std::unexpected(std::string(usage));
    }
    return call;
}

auto adbc_query(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    auto call = sql_call(args,
                         "adbc_query(db, sql[, params]) expects a string query and a "
                         "parameter table");
    if (!call) {
        return std::unexpected(call.error());
    }
    return wrap(ibex::adbc::query(call->db, *call->sql, call->params));
}

auto adbc_execute(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    auto call = sql_call(args,
                         "adbc_execute(db, sql[, params]) expects a string statement and "
                         "a parameter table");
    if (!call) {
        return std::unexpected(call.error());
    }
    return wrap(ibex::adbc::execute(call->db, *call->sql, call->params));
}

auto adbc_write(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    constexpr std::string_view kUsage = "adbc_write(db, df, table, mode)";
    const auto table = args.size() == 4 ? args.table(1) : nullptr;
    const auto* target = args.size() == 4 ? string_arg(args, 2) : nullptr;
    const auto* mode = args.size() == 4 ? string_arg(args, 3) : nullptr;
    if (table == nullptr || target == nullptr || mode == nullptr) {
        return std::unexpected(std::string(kUsage) +
                               " expects a connection, a DataFrame and two strings");
    }
    return wrap(ibex::adbc::write(connection_arg(args), table, *target, *mode));
}

auto adbc_tables(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    if (args.size() != 1) {
        return std::unexpected("adbc_tables(db) expects one argument");
    }
    return wrap(ibex::adbc::tables(connection_arg(args)));
}

auto adbc_table_schema(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    const auto string_at = [&](std::size_t i) -> const std::string* {
        return args.size() == 4 ? string_arg(args, i) : nullptr;
    };
    const auto* table = string_at(1);
    const auto* db_schema = string_at(2);
    const auto* catalog = string_at(3);
    if (table == nullptr || db_schema == nullptr || catalog == nullptr) {
        return std::unexpected(
            "adbc_table_schema(db, table, schema, catalog) expects a connection and three "
            "strings");
    }
    return wrap(ibex::adbc::table_schema(connection_arg(args), *table, *db_schema, *catalog));
}

/// `adbc_begin`, `adbc_commit`, `adbc_rollback` and `adbc_close` take the
/// connection and nothing else.
template <typename Fn>
auto connection_only(const ExternArgs& args, std::string_view function, Fn&& fn)
    -> std::expected<ExternValue, std::string> {
    if (args.size() != 1) {
        return std::unexpected(std::string(function) + "(db) expects one argument");
    }
    return wrap(fn(connection_arg(args)));
}

auto adbc_begin(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    return connection_only(args, "adbc_begin", ibex::adbc::begin);
}

auto adbc_commit(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    return connection_only(args, "adbc_commit", ibex::adbc::commit);
}

auto adbc_rollback(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    return connection_only(args, "adbc_rollback", ibex::adbc::rollback);
}

auto adbc_close(const ExternArgs& args) -> std::expected<ExternValue, std::string> {
    return connection_only(args, "adbc_close", ibex::adbc::close);
}

}  // namespace

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("adbc_read", adbc_read);
    registry->register_chunked_table("adbc_read", adbc_read_source);

    registry->register_resource("adbc_connect", adbc_connect);
    registry->register_table("adbc_query", adbc_query);
    registry->register_scalar("adbc_execute", ibex::runtime::ScalarKind::Int, adbc_execute);
    registry->register_scalar("adbc_write", ibex::runtime::ScalarKind::Int, adbc_write);
    registry->register_table("adbc_tables", adbc_tables);
    registry->register_table("adbc_table_schema", adbc_table_schema);
    registry->register_scalar("adbc_begin", ibex::runtime::ScalarKind::Int, adbc_begin);
    registry->register_scalar("adbc_commit", ibex::runtime::ScalarKind::Int, adbc_commit);
    registry->register_scalar("adbc_rollback", ibex::runtime::ScalarKind::Int, adbc_rollback);
    registry->register_scalar("adbc_close", ibex::runtime::ScalarKind::Int, adbc_close);
}
