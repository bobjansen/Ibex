#include <ibex/parquet/backend.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <cstdint>
#include <exception>
#include <expected>
#include <string>
#include <variant>

#include "parquet.hpp"

namespace ibex::parquet {

void register_backend(runtime::ExternRegistry& registry) {
    registry.register_table(
        "read_parquet",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("read_parquet() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_parquet() expects a string path");
            }
            try {
                return runtime::ExternValue{read_parquet(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry.register_lazy_table(
        "read_parquet",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::LazyTablePtr, std::string> {
            if (args.size() != 1) {
                return std::unexpected("read_parquet() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_parquet() expects a string path");
            }
            try {
                return read_parquet_lazy(*path);
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry.register_chunked_table(
        "read_parquet",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::OperatorPtr, std::string> {
            if (args.size() != 1) {
                return std::unexpected("read_parquet() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_parquet() expects a string path");
            }
            return ChunkedParquetSourceOperator::create(*path);
        });

    registry.register_scalar_table_consumer(
        "write_parquet", runtime::ScalarKind::Int,
        [](const runtime::Table& table,
           const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected(
                    "write_parquet(df, path) expects exactly 1 scalar argument (path)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("write_parquet(df, path) expects a string path");
            }
            try {
                const std::int64_t rows = write_parquet(table, *path);
                return runtime::ExternValue{runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry.register_library("parquet");
}

}  // namespace ibex::parquet
