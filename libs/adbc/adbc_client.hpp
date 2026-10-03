// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The ADBC client as a C++ library: connect to a database through an ADBC
// driver, run queries and statements, bulk-write a table, describe tables, and
// run transactions. The `adbc` plugin (adbc.cpp) is argument parsing over this
// API, and compiled programs call it through adbc.hpp, so both behave alike.
//
// This header names no ADBC type: it pulls in only the Ibex runtime. The ADBC
// driver manager and the Arrow bridge are the library's own concern, linked in
// with `libibex_adbc.a`.
//
// Every function returns the message the plugin has always reported on failure,
// including its `adbc::execute: ` style prefix.

#pragma once

#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace ibex::adbc {

/// The name the `extern type` declaration in adbc.ibex gives a connection.
inline constexpr std::string_view kConnectionTypeName = "adbc::Connection";

using TablePtr = std::shared_ptr<const runtime::Table>;

/// A connection to a database, shared by value: copies alias one connection and
/// the last copy to go away closes it, which is the language's binding rule
/// (`let b = a;` is the same connection). A default-constructed one is not
/// connected and every call on it fails.
class Connection {
   public:
    Connection() = default;
    explicit Connection(runtime::ResourcePtr resource) : resource_(std::move(resource)) {}

    /// The connection as the language's opaque resource.
    [[nodiscard]] auto resource() const noexcept -> const runtime::ResourcePtr& {
        return resource_;
    }
    [[nodiscard]] explicit operator bool() const noexcept { return resource_ != nullptr; }

   private:
    runtime::ResourcePtr resource_;
};

/// Open a connection. `options` is the `key=value;...` spec adbc.ibex describes.
[[nodiscard]] auto connect(std::string_view driver, std::string_view uri, std::string_view options)
    -> std::expected<Connection, std::string>;

/// One query on a connection of its own, closed when the result is read.
[[nodiscard]] auto read(std::string_view driver, std::string_view uri, std::string_view sql,
                        std::string_view options) -> std::expected<runtime::Table, std::string>;

/// `read` as a streaming source, for the chunked executor.
[[nodiscard]] auto read_source(std::string_view driver, std::string_view uri, std::string_view sql,
                               std::string_view options)
    -> std::expected<runtime::OperatorPtr, std::string>;

/// Run a query on `db`. `params` binds the statement's placeholders by column
/// position, one execution per row; null or empty means none.
[[nodiscard]] auto query(const Connection& db, std::string_view sql, const TablePtr& params)
    -> std::expected<runtime::Table, std::string>;

/// Run a statement that returns no rows; the affected row count, or -1 when the
/// driver does not report one.
[[nodiscard]] auto execute(const Connection& db, std::string_view sql, const TablePtr& params)
    -> std::expected<std::int64_t, std::string>;

/// Bulk-insert `table` into `target`. `mode` is create, append, replace or
/// create_append. Returns the rows written.
[[nodiscard]] auto write(const Connection& db, const TablePtr& table, std::string_view target,
                         std::string_view mode) -> std::expected<std::int64_t, std::string>;

/// The tables and views the connection sees.
[[nodiscard]] auto tables(const Connection& db) -> std::expected<runtime::Table, std::string>;

/// One table's columns. An empty `schema` or `catalog` means the driver's default.
[[nodiscard]] auto table_schema(const Connection& db, std::string_view table,
                                std::string_view schema, std::string_view catalog)
    -> std::expected<runtime::Table, std::string>;

/// Transactions. Each returns 1 when the call changed the state, else 0.
[[nodiscard]] auto begin(const Connection& db) -> std::expected<std::int64_t, std::string>;
[[nodiscard]] auto commit(const Connection& db) -> std::expected<std::int64_t, std::string>;
[[nodiscard]] auto rollback(const Connection& db) -> std::expected<std::int64_t, std::string>;

/// Close the connection, for every copy of it. 1 when it was open, else 0.
[[nodiscard]] auto close(const Connection& db) -> std::expected<std::int64_t, std::string>;

}  // namespace ibex::adbc
