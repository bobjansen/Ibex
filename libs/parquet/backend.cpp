// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parquet/backend.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstdint>
#include <exception>
#include <expected>
#include <string>
#include <variant>

#include "parquet.hpp"

namespace ibex::parquet {

void register_backend(runtime::ExternRegistry& registry) {
    registry.register_table(
        "parquet::read",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("parquet::read() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("parquet::read() expects a string path");
            }
            try {
                return runtime::ExternValue{ibex::ext::parquet::read(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry.register_lazy_table(
        "parquet::read",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::LazyTablePtr, std::string> {
            if (args.size() != 1) {
                return std::unexpected("parquet::read() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("parquet::read() expects a string path");
            }
            try {
                return read_parquet_lazy(*path);
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry.register_chunked_table(
        "parquet::read",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::OperatorPtr, std::string> {
            if (args.size() != 1) {
                return std::unexpected("parquet::read() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("parquet::read() expects a string path");
            }
            return ChunkedParquetSourceOperator::create(*path);
        });

    registry.register_scalar_table_consumer(
        "parquet::write", runtime::ScalarKind::Int,
        [](const runtime::Table& table,
           const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected(
                    "parquet::write(df, path) expects exactly 1 scalar argument (path)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("parquet::write(df, path) expects a string path");
            }
            try {
                const std::int64_t rows = ibex::ext::parquet::write(table, *path);
                return runtime::ExternValue{runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry.register_library("parquet");
}

}  // namespace ibex::parquet
