// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// C++ entry points for the externs of adbc.ibex, for programs compiled with
// ibex_compile. A script that declares
//
//   import "adbc";
//   let df = adbc_read("sqlite", "file:data.db", "select 1 as x");
//
// transpiles to a call of `adbc_read` below. Link `libibex_adbc.a` with the ADBC
// driver manager and the Arrow bridge (scripts/ibex-build.sh does); the drivers
// themselves are found at run time, as in the REPL.
//
// Each function reports a failure by throwing std::runtime_error with the message
// the REPL would print, as the other bundled plugins' headers do.

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <expected>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "adbc_client.hpp"

/// The language's `AdbcConnection`: copies alias one connection, and the last
/// copy to go away closes it.
using AdbcConnection = ibex::adbc::Connection;

namespace ibex::adbc::detail {

template <typename T>
auto unwrap(std::expected<T, std::string> result) -> T {
    if (!result) {
        throw std::runtime_error(std::move(result.error()));
    }
    return std::move(*result);
}

inline auto params_of(const ibex::runtime::Table& params) -> TablePtr {
    return std::make_shared<const ibex::runtime::Table>(params);
}

}  // namespace ibex::adbc::detail

inline auto adbc_read(std::string_view driver, std::string_view uri, std::string_view sql,
                      std::string_view options) -> ibex::runtime::Table {
    return ibex::adbc::detail::unwrap(ibex::adbc::read(driver, uri, sql, options));
}

inline auto adbc_connect(std::string_view driver, std::string_view uri, std::string_view options)
    -> AdbcConnection {
    return ibex::adbc::detail::unwrap(ibex::adbc::connect(driver, uri, options));
}

inline auto adbc_query(const AdbcConnection& db, std::string_view sql,
                       const ibex::runtime::Table& params) -> ibex::runtime::Table {
    return ibex::adbc::detail::unwrap(
        ibex::adbc::query(db, sql, ibex::adbc::detail::params_of(params)));
}

inline auto adbc_execute(const AdbcConnection& db, std::string_view sql,
                         const ibex::runtime::Table& params) -> std::int64_t {
    return ibex::adbc::detail::unwrap(
        ibex::adbc::execute(db, sql, ibex::adbc::detail::params_of(params)));
}

inline auto adbc_write(const AdbcConnection& db, const ibex::runtime::Table& df,
                       std::string_view table, std::string_view mode) -> std::int64_t {
    return ibex::adbc::detail::unwrap(
        ibex::adbc::write(db, ibex::adbc::detail::params_of(df), table, mode));
}

inline auto adbc_tables(const AdbcConnection& db) -> ibex::runtime::Table {
    return ibex::adbc::detail::unwrap(ibex::adbc::tables(db));
}

inline auto adbc_table_schema(const AdbcConnection& db, std::string_view table,
                              std::string_view schema, std::string_view catalog)
    -> ibex::runtime::Table {
    return ibex::adbc::detail::unwrap(ibex::adbc::table_schema(db, table, schema, catalog));
}

inline auto adbc_begin(const AdbcConnection& db) -> std::int64_t {
    return ibex::adbc::detail::unwrap(ibex::adbc::begin(db));
}

inline auto adbc_commit(const AdbcConnection& db) -> std::int64_t {
    return ibex::adbc::detail::unwrap(ibex::adbc::commit(db));
}

inline auto adbc_rollback(const AdbcConnection& db) -> std::int64_t {
    return ibex::adbc::detail::unwrap(ibex::adbc::rollback(db));
}

inline auto adbc_close(const AdbcConnection& db) -> std::int64_t {
    return ibex::adbc::detail::unwrap(ibex::adbc::close(db));
}
