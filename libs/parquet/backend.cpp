// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parquet/backend.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <arrow/c/bridge.h>
#include <cstdint>
#include <exception>
#include <expected>
#include <string>
#include <variant>

#include "parquet.hpp"

namespace ibex::parquet {

namespace {

class BackendWriter final : public ChunkWriter {
   public:
    BackendWriter(std::string_view path, std::string_view compression, int level,
                  std::int64_t row_group_size, bool statistics, std::int64_t page_size,
                  bool create_parents, ArrowSchema* schema)
        : writer_(path, compression, level, row_group_size, statistics, page_size, create_parents) {
        if (schema != nullptr) {
            auto imported = arrow::ImportSchema(schema);
            if (!imported.ok()) {
                throw std::invalid_argument(imported.status().ToString());
            }
            writer_.set_schema(std::move(imported).ValueOrDie());
        }
    }

    void push(runtime::Table table) override { writer_.push(std::move(table)); }
    auto finish() -> std::int64_t override { return writer_.finish(); }

   private:
    ext::parquet::Writer writer_;
};

}  // namespace

auto open_writer(std::string_view path, std::string_view compression, std::int64_t level,
                 std::int64_t row_group_size, bool statistics, std::int64_t page_size,
                 bool create_parents, ArrowSchema* schema) -> std::unique_ptr<ChunkWriter> {
    if (!std::in_range<int>(level)) {
        throw std::invalid_argument("parquet::write: compression level out of range");
    }
    return std::make_unique<BackendWriter>(path, compression, static_cast<int>(level),
                                           row_group_size, statistics, page_size, create_parents,
                                           schema);
}

void register_backend(runtime::ExternRegistry& registry) {
#if !defined(IBEX_PARQUET_LOCAL_ONLY)
    registry.register_shutdown_hook(&finalize_s3_if_initialized);
#endif
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

    const runtime::ExternChunkConsumerFn write_chunks =
        [](const runtime::ExternTableProducer& produce,
           const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
        if (args.size() != 1 && args.size() != 5) {
            return std::unexpected("parquet::write: expected a path and optional writer options");
        }
        const auto* path = std::get_if<std::string>(&args[0]);
        if (path == nullptr) {
            return std::unexpected("parquet::write: expected a string path");
        }
        try {
            const bool defaults = args.size() == 1;
            auto writer = open_writer(*path, defaults ? "zstd" : std::get<std::string>(args[1]),
                                      defaults ? 3 : std::get<std::int64_t>(args[2]),
                                      defaults ? 122880 : std::get<std::int64_t>(args[3]),
                                      defaults || std::get<bool>(args[4]), 1024 * 1024, true);
            auto produced = produce([&](runtime::Table chunk) -> std::expected<void, std::string> {
                writer->push(std::move(chunk));
                return {};
            });
            if (!produced) {
                return std::unexpected(std::move(produced.error()));
            }
            return runtime::ExternValue{runtime::ScalarValue{writer->finish()}};
        } catch (const std::exception& error) {
            return std::unexpected(std::string(error.what()));
        }
    };
    registry.register_scalar_table_consumer(
        "parquet::write", runtime::ScalarKind::Int,
        [write_chunks](const runtime::Table& table, const runtime::ExternArgs& args) {
            return write_chunks([&](const runtime::TableChunkSink& sink) { return sink(table); },
                                args);
        },
        write_chunks);

    registry.register_library("parquet");
}

}  // namespace ibex::parquet
