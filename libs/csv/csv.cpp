// Ibex plugin entry point for csv.hpp.
//
// Build as a shared library alongside csv.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
//   extern fn write_csv(df: DataFrame, path: String) -> Int from "csv.hpp";

#include "csv.hpp"

#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

namespace {

/// Chunk size for the streaming csv reader. 65536 rows keeps a typical
/// column payload (a handful of int64/double/categorical columns) inside
/// L2 cache on current x86-64 parts.
constexpr std::size_t kChunkedCsvRowsPerChunk = 65536;

/// Returns an OperatorPtr if the args match the chunked fast path
/// (5 args, has_header=false, no null spec, non-empty explicit schema
/// with no Infer entries). Returns std::nullopt otherwise so the caller
/// can fall back to the eager reader.
auto try_make_chunked_csv_source(const ibex::runtime::ExternArgs& args)
    -> std::optional<std::expected<ibex::runtime::OperatorPtr, std::string>> {
    if (args.size() != 5) {
        return std::nullopt;
    }
    const auto* path = std::get_if<std::string>(&args[0]);
    const auto* null_spec = std::get_if<std::string>(&args[1]);
    const auto* delimiter = std::get_if<std::string>(&args[2]);
    const auto* has_header = std::get_if<bool>(&args[3]);
    const auto* schema = std::get_if<std::string>(&args[4]);
    if (path == nullptr || null_spec == nullptr || delimiter == nullptr || has_header == nullptr ||
        schema == nullptr) {
        return std::nullopt;
    }
    if (*has_header || !null_spec->empty() || schema->empty()) {
        return std::nullopt;
    }
    char delim{};
    try {
        delim = csv_parse_delimiter(*delimiter);
    } catch (const std::exception&) {
        return std::nullopt;
    }
    CsvSchemaHint hint;
    try {
        hint = csv_parse_schema(*schema);
    } catch (const std::exception& e) {
        return std::expected<ibex::runtime::OperatorPtr, std::string>{
            std::unexpected(std::string(e.what()))};
    }
    std::vector<std::string> col_names;
    std::vector<CsvColumnKind> col_kinds;
    col_names.reserve(hint.entries.size());
    col_kinds.reserve(hint.entries.size());
    for (std::size_t i = 0; i < hint.entries.size(); ++i) {
        const auto& entry = hint.entries[i];
        if (entry.kind == CsvColumnKind::Infer) {
            return std::nullopt;
        }
        col_names.push_back(entry.name.value_or("col" + std::to_string(i + 1)));
        col_kinds.push_back(entry.kind);
    }
    try {
        auto op = std::make_unique<ChunkedCsvSourceOperator>(
            *path, std::move(col_names), std::move(col_kinds), delim, kChunkedCsvRowsPerChunk);
        return std::expected<ibex::runtime::OperatorPtr, std::string>{std::move(op)};
    } catch (const std::exception& e) {
        return std::expected<ibex::runtime::OperatorPtr, std::string>{
            std::unexpected(std::string(e.what()))};
    }
}

}  // namespace

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "read_csv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() < 1 || args.size() > 5) {
                return std::unexpected("read_csv() expects 1 to 5 arguments");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_csv() expects a string path");
            }
            try {
                if (args.size() == 5) {
                    const auto* null_spec = std::get_if<std::string>(&args[1]);
                    if (null_spec == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header, schema) expects a "
                            "string null spec");
                    }
                    const auto* delimiter = std::get_if<std::string>(&args[2]);
                    if (delimiter == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header, schema) expects a "
                            "string delimiter");
                    }
                    const auto* has_header = std::get_if<bool>(&args[3]);
                    if (has_header == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header, schema) expects a bool "
                            "has_header flag");
                    }
                    const auto* schema = std::get_if<std::string>(&args[4]);
                    if (schema == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header, schema) expects a "
                            "string schema");
                    }
                    return ibex::runtime::ExternValue{
                        read_csv(*path, *null_spec, *delimiter, *has_header, *schema)};
                }
                if (args.size() == 4) {
                    const auto* null_spec = std::get_if<std::string>(&args[1]);
                    if (null_spec == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header) expects a string null "
                            "spec");
                    }
                    const auto* delimiter = std::get_if<std::string>(&args[2]);
                    if (delimiter == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header) expects a string "
                            "delimiter");
                    }
                    const auto* has_header = std::get_if<bool>(&args[3]);
                    if (has_header == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter, has_header) expects a bool "
                            "has_header flag");
                    }
                    return ibex::runtime::ExternValue{
                        read_csv(*path, *null_spec, *delimiter, *has_header)};
                }
                if (args.size() == 3) {
                    const auto* null_spec = std::get_if<std::string>(&args[1]);
                    if (null_spec == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter) expects a string null spec");
                    }
                    const auto* delimiter = std::get_if<std::string>(&args[2]);
                    if (delimiter == nullptr) {
                        return std::unexpected(
                            "read_csv(path, nulls, delimiter) expects a string delimiter");
                    }
                    return ibex::runtime::ExternValue{read_csv(*path, *null_spec, *delimiter)};
                }
                if (args.size() == 2) {
                    const auto* null_spec = std::get_if<std::string>(&args[1]);
                    if (null_spec == nullptr) {
                        return std::unexpected("read_csv(path, nulls) expects a string null spec");
                    }
                    return ibex::runtime::ExternValue{read_csv(*path, *null_spec)};
                }
                return ibex::runtime::ExternValue{read_csv(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry->register_chunked_table(
        "read_csv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::OperatorPtr, std::string> {
            auto maybe_op = try_make_chunked_csv_source(args);
            if (!maybe_op.has_value()) {
                return std::unexpected(
                    "read_csv chunked path requires 5 args with has_header=false, empty "
                    "null spec, and fully typed schema");
            }
            return std::move(*maybe_op);
        });

    registry->register_scalar_table_consumer(
        "write_csv", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected(
                    "write_csv(df, path) expects exactly 1 scalar argument (path)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("write_csv(df, path) expects a string path");
            }
            try {
                std::int64_t rows = write_csv(table, *path);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
