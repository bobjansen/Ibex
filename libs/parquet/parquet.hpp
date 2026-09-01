// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once
// Ibex Parquet library — provides read_parquet() and write_parquet() for use in Ibex scripts.
//
// Reading:
//   extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";
//   let df = read_parquet("data/myfile.parquet");
//   // The local-only R package build accepts local paths only.
//   // The regular plugin additionally supports HTTPS and S3 paths.
//   let public = read_parquet("https://data.example.com/myfile.parquet");
//   let remote = read_parquet("s3://bucket/path/myfile.parquet?region=us-east-1");
//
// Writing:
//   extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";
//   let rows = write_parquet(df, "data/out.parquet");
//
// Compile with: -I$(IBEX_ROOT)/libraries

// Arrow (and curl in the remote-capable build) eventually drags in <windows.h> on this platform
// (via winsock2.h). Without NOMINMAX its min/max macros clobber every
// std::numeric_limits<T>::max()-style call textually, which is exactly what
// breaks arrow/util/compression.h's own use of it -- must be defined before
// any of the includes below, whichever of them happens to pull windows.h in
// first.
#if defined(_WIN32)
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#endif

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/util/formatting.h>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if !defined(IBEX_PARQUET_LOCAL_ONLY)
#include <curl/curl.h>
#endif
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/page_index.h>
#include <parquet/statistics.h>
#include <random>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "dictionary_policy.hpp"

namespace {

#if !defined(IBEX_PARQUET_LOCAL_ONLY)
inline auto is_s3_uri(std::string_view path) -> bool {
    return path.starts_with("s3://");
}

inline auto is_https_uri(std::string_view path) -> bool {
    return path.starts_with("https://");
}

inline void close_and_remove_temp(std::FILE* file, const std::string& path) {
    if (file != nullptr) {
        (void)std::fclose(file);
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

// mkstemp() is POSIX-only; use stdio's "x" exclusive-create flag instead
// (a C11 addition MSVC's fopen and glibc's fopen both support), retrying
// under a random suffix on collision. Portable across POSIX and Windows
// without any #ifdef.
inline auto make_temp_parquet_file() -> std::pair<std::FILE*, std::string> {
    std::error_code ec;
    auto temp_dir = std::filesystem::temp_directory_path(ec);
    if (ec) {
        throw std::runtime_error("read_parquet: failed to locate temp directory: " + ec.message());
    }

    std::random_device rd;
    std::mt19937_64 rng(rd());
    for (int attempt = 0; attempt < 16; ++attempt) {
        std::ostringstream name;
        name << "ibex-parquet-" << std::hex << rng();
        auto path = (temp_dir / name.str()).string();
        if (std::FILE* file = std::fopen(path.c_str(), "wbx")) {
            return {file, path};
        }
    }
    throw std::runtime_error("read_parquet: failed to create temp file: " +
                             std::string(std::strerror(errno)));
}

inline auto write_http_chunk(char* ptr, std::size_t size, std::size_t nmemb, void* userdata)
    -> std::size_t {
    return std::fwrite(ptr, size, nmemb, static_cast<std::FILE*>(userdata));
}

inline auto download_https_to_temp(std::string_view url) -> std::string {
    static const bool curl_initialized = [] {
        return curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK;
    }();
    if (!curl_initialized) {
        throw std::runtime_error("read_parquet: failed to initialize HTTPS client");
    }

    auto [file, temp_path] = make_temp_parquet_file();
    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        close_and_remove_temp(file, temp_path);
        throw std::runtime_error("read_parquet: failed to create HTTPS client");
    }

    std::string url_string{url};
    char error_buffer[CURL_ERROR_SIZE] = {};
    curl_easy_setopt(curl, CURLOPT_URL, url_string.c_str());
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error_buffer);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_http_chunk);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, file);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "ibex-parquet/1");
    if (const char* ca_info = std::getenv("CURL_CA_BUNDLE");
        ca_info != nullptr && ca_info[0] != '\0') {
        curl_easy_setopt(curl, CURLOPT_CAINFO, ca_info);
    } else if (const char* ssl_cert_file = std::getenv("SSL_CERT_FILE");
               ssl_cert_file != nullptr && ssl_cert_file[0] != '\0') {
        curl_easy_setopt(curl, CURLOPT_CAINFO, ssl_cert_file);
    }

    const CURLcode rc = curl_easy_perform(curl);
    long response_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
    curl_easy_cleanup(curl);

    const int close_result = std::fclose(file);
    file = nullptr;

    if (rc != CURLE_OK) {
        close_and_remove_temp(file, temp_path);
        std::string detail = error_buffer[0] != '\0' ? error_buffer : curl_easy_strerror(rc);
        throw std::runtime_error("read_parquet: failed to download '" + url_string + "' (" +
                                 detail + ", HTTP " + std::to_string(response_code) + ")");
    }
    if (close_result != 0) {
        close_and_remove_temp(file, temp_path);
        throw std::runtime_error("read_parquet: failed to finish temp download for '" + url_string +
                                 "': " + std::strerror(errno));
    }

    return temp_path;
}
#endif

inline auto open_parquet_input(std::string_view path)
    -> std::shared_ptr<arrow::io::RandomAccessFile> {
    std::string path_string{path};
#if defined(IBEX_PARQUET_LOCAL_ONLY)
    if (path_string.find("://") != std::string::npos) {
        throw std::runtime_error("read_parquet: this build supports local files only: '" +
                                 path_string + "'");
    }
#else
    if (is_https_uri(path)) {
        auto temp_path = download_https_to_temp(path);
        auto input_result = arrow::io::ReadableFile::Open(temp_path);
        std::error_code remove_ec;
        std::filesystem::remove(temp_path, remove_ec);
        if (!input_result.ok()) {
            throw std::runtime_error("read_parquet: failed to open downloaded '" + path_string +
                                     "' (" + input_result.status().ToString() + ")");
        }
        return input_result.ValueOrDie();
    }
    if (is_s3_uri(path)) {
        std::string object_path;
        auto fs_result = arrow::fs::FileSystemFromUri(path_string, &object_path);
        if (!fs_result.ok()) {
            throw std::runtime_error("read_parquet: failed to resolve object storage path '" +
                                     path_string + "' (" + fs_result.status().ToString() + ")");
        }

        auto input_result = fs_result.ValueOrDie()->OpenInputFile(object_path);
        if (!input_result.ok()) {
            throw std::runtime_error("read_parquet: failed to open '" + path_string + "' (" +
                                     input_result.status().ToString() + ")");
        }
        return input_result.ValueOrDie();
    }
#endif

    std::error_code ec;
    const bool exists = std::filesystem::exists(path_string, ec);
    if (ec) {
        throw std::runtime_error("read_parquet: failed to inspect path '" + path_string +
                                 "': " + ec.message());
    }
    if (!exists) {
        throw std::runtime_error("read_parquet: file not found: '" + path_string + "'");
    }

    auto input_result = arrow::io::ReadableFile::Open(path_string);
    if (!input_result.ok()) {
        throw std::runtime_error("read_parquet: failed to open '" + path_string + "' (" +
                                 input_result.status().ToString() + ")");
    }
    return input_result.ValueOrDie();
}

/// Append one Arrow chunk's values to `out`, mapping each through `convert`.
///
/// Two things make this quicker than the per-row `push_back` it replaces. The
/// destination is sized once, so there is no per-element capacity check or size
/// bookkeeping to stop the compiler vectorizing. And the null test is hoisted
/// out of the loop: Arrow already knows a chunk's null count, and a Parquet
/// column with no nulls — the overwhelmingly common case, and every column in
/// TPC-H — then runs a straight branchless loop over a contiguous buffer.
///
/// A null reads as the zero value, which is what the previous per-row code did.
template <typename ArrowArray, typename Out, typename Convert>
inline void append_converted(const arrow::Array& array, std::vector<Out>& out, Convert convert) {
    const auto& typed = static_cast<const ArrowArray&>(array);
    const auto count = static_cast<std::size_t>(typed.length());
    const std::size_t base = out.size();
    out.resize(base + count);
    if (count == 0) {
        return;
    }

    Out* dst = out.data() + base;
    // Arrow offsets raw_values() by the array's own offset, so this stays
    // correct for a sliced chunk.
    const auto* src = typed.raw_values();

    if (typed.null_count() == 0) {
        for (std::size_t i = 0; i < count; ++i) {
            dst[i] = convert(src[i]);
        }
        return;
    }
    // Unlike `append_same_layout`, this keeps a per-row null check: `convert` does
    // arithmetic (a timestamp rescale multiplies), and Arrow's values buffer is
    // undefined at a null row, so converting one could overflow — which is UB.
    // Copying raw bytes cannot. The value it writes at a null slot is arbitrary;
    // nothing may read it.
    for (std::size_t i = 0; i < count; ++i) {
        dst[i] = typed.IsNull(static_cast<std::int64_t>(i)) ? Out{} : convert(src[i]);
    }
}

/// The same, for a chunk whose Arrow buffer already has the destination's exact
/// layout — `std::int64_t`, `double`, and the single-field `ibex::Date` /
/// `ibex::Timestamp` wrappers.
///
/// This copies straight from Arrow's buffer, which for a trivially copyable
/// element is one memmove. It runs over EVERY row, nulls included, and makes no
/// attempt to blank the null slots: Arrow leaves the values buffer undefined
/// where a row is null, and so do we. Nothing downstream may read a cell whose
/// validity bit is clear — the grouping key masks those slots out of its hash and
/// comparison rather than trusting a producer to have zeroed them. Copying
/// unconditionally keeps a null-bearing column on the same bulk path as a dense
/// one, with no per-row branch either way.
///
/// It deliberately does NOT go through `resize`: resize value-initializes the
/// new elements, so a resize-then-copy writes the destination twice, which is
/// enough to cancel out the gain over the per-row `push_back` this replaces.
template <typename ArrowArray, typename Out>
inline void append_same_layout(const arrow::Array& array, std::vector<Out>& out) {
    using Src = typename ArrowArray::value_type;
    static_assert(sizeof(Src) == sizeof(Out), "same-layout copy needs equal width");
    static_assert(std::is_trivially_copyable_v<Out>);

    const auto& typed = static_cast<const ArrowArray&>(array);
    const auto count = static_cast<std::size_t>(typed.length());
    if (count == 0) {
        return;
    }
    // Arrow offsets raw_values() by the array's own offset, so this stays
    // correct for a sliced chunk.
    const auto* values = reinterpret_cast<const Out*>(typed.raw_values());
    out.insert(out.end(), values, values + count);
}

/// Lift an Arrow column's nulls into an Ibex validity bitmap, or nullopt when the
/// column has none (the common case, which then costs nothing and carries no
/// bitmap at all).
///
/// Both sides mark 1 = valid and index bit i as `i / 8`'s byte, bit `i % 8`,
/// LSB-first — Ibex just views that as little-endian 64-bit words. So the bitmap
/// transfers wholesale rather than a bit at a time, which is what keeps reading a
/// null-bearing column as cheap as a dense one. `CopyBitmap` handles the
/// bit-offsets that arise from a sliced chunk, or from a chunk boundary that does
/// not land on a byte.
inline auto validity_from_arrow(const arrow::ChunkedArray& chunked)
    -> std::optional<ibex::runtime::ValidityBitmap> {
    static_assert(std::endian::native == std::endian::little,
                  "the wholesale bitmap copy assumes Arrow's LSB-first bitmap and "
                  "ValidityBitmap's word layout agree, which holds on little-endian only");

    if (chunked.null_count() == 0) {
        return std::nullopt;
    }

    ibex::runtime::ValidityBitmap validity;
    validity.assign(static_cast<std::size_t>(chunked.length()), true);
    auto* dest = reinterpret_cast<std::uint8_t*>(validity.words_data());

    std::int64_t row = 0;
    for (const auto& chunk : chunked.chunks()) {
        const auto length = chunk->length();
        const auto* bitmap = chunk->null_bitmap_data();
        // A chunk with no nulls of its own may carry no bitmap; its rows stay
        // valid from the assign() above.
        if (chunk->null_count() != 0 && bitmap != nullptr) {
            arrow::internal::CopyBitmap(bitmap, chunk->offset(), length, dest, row);
        }
        row += length;
    }
    return validity;
}

inline void append_int_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                              std::vector<std::int64_t>& out) {
    const auto widen = [](auto value) { return static_cast<std::int64_t>(value); };
    for (const auto& chunk : chunked->chunks()) {
        switch (chunk->type_id()) {
            case arrow::Type::INT64:
                append_same_layout<arrow::Int64Array>(*chunk, out);
                break;
            case arrow::Type::INT32:
                append_converted<arrow::Int32Array>(*chunk, out, widen);
                break;
            case arrow::Type::INT16:
                append_converted<arrow::Int16Array>(*chunk, out, widen);
                break;
            case arrow::Type::INT8:
                append_converted<arrow::Int8Array>(*chunk, out, widen);
                break;
            case arrow::Type::UINT64:
                append_converted<arrow::UInt64Array>(*chunk, out, widen);
                break;
            case arrow::Type::UINT32:
                append_converted<arrow::UInt32Array>(*chunk, out, widen);
                break;
            case arrow::Type::UINT16:
                append_converted<arrow::UInt16Array>(*chunk, out, widen);
                break;
            case arrow::Type::UINT8:
                append_converted<arrow::UInt8Array>(*chunk, out, widen);
                break;
            default:
                throw std::runtime_error("read_parquet: unsupported integer column type");
        }
    }
}

inline void append_double_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 std::vector<double>& out) {
    for (const auto& chunk : chunked->chunks()) {
        switch (chunk->type_id()) {
            case arrow::Type::DOUBLE:
                append_same_layout<arrow::DoubleArray>(*chunk, out);
                break;
            case arrow::Type::FLOAT:
                append_converted<arrow::FloatArray>(*chunk, out,
                                                    [](float value) { return double{value}; });
                break;
            default:
                throw std::runtime_error("read_parquet: unsupported float column type");
        }
    }
}

inline void append_bool_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                               ibex::Column<bool>& out) {
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::BOOL) {
            throw std::runtime_error("read_parquet: unsupported boolean column type");
        }
        const auto& values = static_cast<const arrow::BooleanArray&>(*chunk);
        for (std::int64_t row = 0; row < values.length(); ++row) {
            out.push_back(!values.IsNull(row) && values.Value(row));
        }
    }
}

/// Build a dictionary-encoded ibex column from an Arrow ChunkedArray of
/// DictionaryArrays.
///
/// Arrow hands back one dictionary *per chunk* (per row group), and the same
/// string may sit at a different code in each. Ibex's `Column<Categorical>` has
/// a single dictionary, so each chunk's local codes are remapped into one
/// unified dictionary. That remap is cheap precisely because this path is only
/// taken for low-cardinality columns — the per-chunk dictionaries have a handful
/// of entries, while the code vector has millions.
inline auto build_categorical_column(const std::shared_ptr<arrow::ChunkedArray>& chunked)
    -> ibex::Column<ibex::Categorical> {
    std::vector<std::string> dict;
    std::map<std::string, std::int32_t, std::less<>> index;
    std::vector<std::int32_t> codes;
    codes.reserve(static_cast<std::size_t>(chunked->length()));

    auto intern = [&](std::string value) -> std::int32_t {
        if (auto it = index.find(value); it != index.end()) {
            return it->second;
        }
        auto code = static_cast<std::int32_t>(dict.size());
        index.emplace(value, code);
        dict.push_back(std::move(value));
        return code;
    };

    for (const auto& chunk : chunked->chunks()) {
        const auto& dict_array = static_cast<const arrow::DictionaryArray&>(*chunk);

        const auto& values = *dict_array.dictionary();
        if (values.type_id() != arrow::Type::STRING &&
            values.type_id() != arrow::Type::LARGE_STRING) {
            throw std::runtime_error("read_parquet: unsupported dictionary value type");
        }
        const auto& strings = static_cast<const arrow::StringArray&>(values);

        std::vector<std::int32_t> local_to_global;
        local_to_global.reserve(static_cast<std::size_t>(strings.length()));
        for (int64_t i = 0; i < strings.length(); ++i) {
            local_to_global.push_back(intern(strings.GetString(i)));
        }

        const auto& indices = *dict_array.indices();
        if (indices.type_id() != arrow::Type::INT32) {
            throw std::runtime_error("read_parquet: unsupported dictionary index type");
        }
        const auto& int_indices = static_cast<const arrow::Int32Array&>(indices);
        for (int64_t i = 0; i < int_indices.length(); ++i) {
            // Match the plain-string path, which reads a null as an empty string.
            codes.push_back(int_indices.IsNull(i)
                                ? intern(std::string{})
                                : local_to_global[static_cast<std::size_t>(int_indices.Value(i))]);
        }
    }

    return ibex::Column<ibex::Categorical>{std::move(dict), std::move(codes)};
}

/// Decode straight into the column's flat char+offset buffers.
///
/// Going through a `std::vector<std::string>` first costs a heap allocation and
/// a copy for every value that does not fit libstdc++'s 15-char SSO — which is
/// every one of them for a column like c_comment (~73 chars) — and then
/// `Column<std::string>`'s constructor copies all the bytes a second time and
/// frees the strings again. Customer's four string columns are only 150k rows
/// and were costing 24ms; that was ~100 cycles per value to move ~30 bytes.
/// `GetView` hands back a view into Arrow's buffer, and push_back memcpy's it
/// into the flat buffer once.
inline void append_string_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 ibex::Column<std::string>& out) {
    std::size_t rows = 0;
    std::size_t bytes = 0;
    for (const auto& chunk : chunked->chunks()) {
        rows += static_cast<std::size_t>(chunk->length());
        if (chunk->type_id() == arrow::Type::STRING) {
            bytes += static_cast<std::size_t>(
                std::static_pointer_cast<arrow::StringArray>(chunk)->total_values_length());
        } else if (chunk->type_id() == arrow::Type::LARGE_STRING) {
            bytes += static_cast<std::size_t>(
                std::static_pointer_cast<arrow::LargeStringArray>(chunk)->total_values_length());
        }
    }
    out.reserve(rows, bytes);

    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() == arrow::Type::STRING) {
            auto arr = std::static_pointer_cast<arrow::StringArray>(chunk);
            for (int64_t i = 0; i < arr->length(); ++i) {
                // A null row still gets an empty string, as before.
                out.push_back(arr->IsNull(i) ? std::string_view{} : arr->GetView(i));
            }
        } else if (chunk->type_id() == arrow::Type::LARGE_STRING) {
            auto arr = std::static_pointer_cast<arrow::LargeStringArray>(chunk);
            for (int64_t i = 0; i < arr->length(); ++i) {
                out.push_back(arr->IsNull(i) ? std::string_view{} : arr->GetView(i));
            }
        } else {
            throw std::runtime_error("read_parquet: unsupported string column type");
        }
    }
}

inline void append_date32_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 std::vector<ibex::Date>& out) {
    // Arrow DATE32 is int32 days since epoch, exactly ibex::Date's representation.
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::DATE32) {
            throw std::runtime_error("read_parquet: unsupported date32 column type");
        }
        append_same_layout<arrow::Date32Array>(*chunk, out);
    }
}

inline void append_date64_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 std::vector<ibex::Date>& out) {
    // Arrow DATE64: int64 milliseconds since epoch, always a multiple of 86_400_000.
    constexpr std::int64_t kMillisPerDay = 86'400'000;
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::DATE64) {
            throw std::runtime_error("read_parquet: unsupported date64 column type");
        }
        append_converted<arrow::Date64Array>(*chunk, out, [](std::int64_t millis) {
            return ibex::Date{static_cast<std::int32_t>(millis / kMillisPerDay)};
        });
    }
}

inline void append_timestamp_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                    const std::shared_ptr<arrow::DataType>& type,
                                    std::vector<ibex::Timestamp>& out) {
    // Scale Arrow timestamp units to nanoseconds (Ibex's storage unit).
    const auto unit = std::static_pointer_cast<arrow::TimestampType>(type)->unit();
    std::int64_t scale = 1;
    switch (unit) {
        case arrow::TimeUnit::SECOND:
            scale = 1'000'000'000;
            break;
        case arrow::TimeUnit::MILLI:
            scale = 1'000'000;
            break;
        case arrow::TimeUnit::MICRO:
            scale = 1'000;
            break;
        case arrow::TimeUnit::NANO:
            scale = 1;
            break;
    }
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::TIMESTAMP) {
            throw std::runtime_error("read_parquet: unsupported timestamp column type");
        }
        // A nanosecond column needs no rescaling, so it is already ibex::Timestamp's
        // layout and copies wholesale.
        if (scale == 1) {
            append_same_layout<arrow::TimestampArray>(*chunk, out);
        } else {
            append_converted<arrow::TimestampArray>(*chunk, out, [scale](std::int64_t value) {
                return ibex::Timestamp{value * scale};
            });
        }
    }
}

/// Populate `sink` (an `ibex::runtime::Table` or `ibex::runtime::Chunk` —
/// both expose a matching `add_column(name, ColumnValue, optional<ValidityBitmap>)`)
/// from an already-read Arrow table. Shared by the whole-file `read_parquet()`
/// path and the row-group/batch streaming `ChunkedParquetSourceOperator` so a
/// single, tested conversion path handles both.
template <typename Sink>
inline void populate_from_arrow_table(const std::shared_ptr<arrow::Table>& table, Sink& sink) {
    for (int i = 0; i < table->num_columns(); ++i) {
        const auto& field = table->field(i);
        const auto& col = table->column(i);

        auto validity = validity_from_arrow(*col);

        // Hand the column to the sink, carrying its nulls when it has any.
        auto emit = [&](auto column) {
            if (validity.has_value()) {
                sink.add_column(field->name(), std::move(column), std::move(*validity));
            } else {
                sink.add_column(field->name(), std::move(column));
            }
        };
        // Restore the zero-at-a-null-slot invariant that the bulk value copy
        // does not maintain. A no-op when the column has no nulls.
        switch (col->type()->id()) {
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64:
            case arrow::Type::UINT8:
            case arrow::Type::UINT16:
            case arrow::Type::UINT32:
            case arrow::Type::UINT64: {
                std::vector<std::int64_t> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_int_column(col, values);
                emit(ibex::Column<std::int64_t>{std::move(values)});
                break;
            }
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE: {
                std::vector<double> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_double_column(col, values);
                emit(ibex::Column<double>{std::move(values)});
                break;
            }
            case arrow::Type::BOOL: {
                ibex::Column<bool> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_bool_column(col, values);
                emit(std::move(values));
                break;
            }
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING: {
                // The string path already writes an empty string at a null row.
                ibex::Column<std::string> values;
                append_string_column(col, values);
                emit(std::move(values));
                break;
            }
            case arrow::Type::DICTIONARY: {
                emit(build_categorical_column(col));
                break;
            }
            case arrow::Type::DATE32: {
                std::vector<ibex::Date> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_date32_column(col, values);
                emit(ibex::Column<ibex::Date>{std::move(values)});
                break;
            }
            case arrow::Type::DATE64: {
                std::vector<ibex::Date> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_date64_column(col, values);
                emit(ibex::Column<ibex::Date>{std::move(values)});
                break;
            }
            case arrow::Type::TIMESTAMP: {
                std::vector<ibex::Timestamp> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_timestamp_column(col, col->type(), values);
                emit(ibex::Column<ibex::Timestamp>{std::move(values)});
                break;
            }
            default:
                throw std::runtime_error("read_parquet: unsupported column type for " +
                                         field->name());
        }
    }
}

/// Build a zero-row Table carrying only the names and types of `schema` — what
/// a lazy binding knows about the file before any column has been decoded.
/// Mirrors the type mapping in `populate_from_arrow_table`, so a column the
/// schema admits here is one that path can actually decode.
inline auto schema_table_from_arrow(const arrow::Schema& schema) -> ibex::runtime::Table {
    ibex::runtime::Table out;
    for (const auto& field : schema.fields()) {
        switch (field->type()->id()) {
            case arrow::Type::INT8:
            case arrow::Type::INT16:
            case arrow::Type::INT32:
            case arrow::Type::INT64:
            case arrow::Type::UINT8:
            case arrow::Type::UINT16:
            case arrow::Type::UINT32:
            case arrow::Type::UINT64:
                out.add_column(field->name(), ibex::Column<std::int64_t>{});
                break;
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE:
                out.add_column(field->name(), ibex::Column<double>{});
                break;
            case arrow::Type::BOOL:
                out.add_column(field->name(), ibex::Column<bool>{});
                break;
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING:
                out.add_column(field->name(), ibex::Column<std::string>{});
                break;
            case arrow::Type::DICTIONARY:
                out.add_column(field->name(), ibex::Column<ibex::Categorical>{});
                break;
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
                out.add_column(field->name(), ibex::Column<ibex::Date>{});
                break;
            case arrow::Type::TIMESTAMP:
                out.add_column(field->name(), ibex::Column<ibex::Timestamp>{});
                break;
            default:
                throw std::runtime_error("read_parquet: unsupported column type for " +
                                         field->name());
        }
    }
    return out;
}

/// Leaf indices of the string columns this file stores fully dictionary-encoded,
/// which are the ones worth reading back as `Column<Categorical>`.
///
/// The file's own writer is the oracle here. Parquet builds a dictionary per
/// column chunk and abandons it — falling back to PLAIN data pages — once the
/// dictionary outgrows its page size limit. So "every data page in every row
/// group is dictionary-encoded" is a cardinality test the writer already paid
/// for: on TPC-H it selects l_returnflag, l_linestatus, l_shipinstruct,
/// l_shipmode, p_brand, p_container … and rejects l_comment and p_name.
///
/// Getting this wrong is a performance choice, not a correctness one: a column
/// read either way holds the same values.
/// A column's dictionary size in bytes, summed over row groups, from metadata
/// alone. The dictionary page runs from its own offset to the first data page.
inline auto dictionary_bytes(const parquet::FileMetaData& metadata, int col) -> std::int64_t {
    std::int64_t bytes = 0;
    for (int group = 0; group < metadata.num_row_groups(); ++group) {
        auto chunk = metadata.RowGroup(group)->ColumnChunk(col);
        const auto dict_offset = chunk->dictionary_page_offset();
        const auto data_offset = chunk->data_page_offset();
        if (data_offset > dict_offset) {
            bytes += data_offset - dict_offset;
        }
    }
    return bytes;
}

/// The number of entries in a column's dictionary, summed over row groups, or
/// nullopt when any row group does not expose one.
///
/// Read from the dictionary page's own header. Callers must bound this with
/// `dictionary_bytes` first: a dictionary page is only bounded by the writer's
/// page-size limit, and reading `l_comment`'s -- megabytes of strings this
/// decision does not need -- to learn that it has far too many entries would
/// cost more than the decision saves.
inline auto dictionary_entry_count(parquet::ParquetFileReader& reader, int col)
    -> std::optional<std::int64_t> {
    const auto& metadata = *reader.metadata();
    std::int64_t entries = 0;
    for (int group = 0; group < metadata.num_row_groups(); ++group) {
        auto pages = reader.RowGroup(group)->GetColumnPageReader(col);
        if (pages == nullptr) {
            return std::nullopt;
        }
        const auto page = pages->NextPage();
        if (page == nullptr || page->type() != parquet::PageType::DICTIONARY_PAGE) {
            return std::nullopt;
        }
        entries += static_cast<const parquet::DictionaryPage&>(*page).num_values();
    }
    return entries;
}

inline auto dictionary_column_indices(parquet::ParquetFileReader& reader) -> std::vector<int> {
    const auto& metadata = *reader.metadata();
    std::vector<int> out;
    for (int col = 0; col < metadata.num_columns(); ++col) {
        if (metadata.schema()->Column(col)->physical_type() != parquet::Type::BYTE_ARRAY) {
            continue;
        }
        bool fully_dictionary = metadata.num_row_groups() > 0;
        for (int group = 0; group < metadata.num_row_groups() && fully_dictionary; ++group) {
            auto chunk = metadata.RowGroup(group)->ColumnChunk(col);
            if (!chunk->has_dictionary_page()) {
                fully_dictionary = false;
                break;
            }
            for (const auto& stats : chunk->encoding_stats()) {
                // Only data pages count: the dictionary page is itself PLAIN-encoded,
                // so a column's encoding list always mentions PLAIN.
                const bool is_data = stats.page_type == parquet::PageType::DATA_PAGE ||
                                     stats.page_type == parquet::PageType::DATA_PAGE_V2;
                if (is_data && stats.encoding != parquet::Encoding::RLE_DICTIONARY &&
                    stats.encoding != parquet::Encoding::PLAIN_DICTIONARY) {
                    fully_dictionary = false;
                    break;
                }
            }
        }
        // A dictionary must be small to be worth having. Reading a column as
        // `Column<Categorical>` interns every dictionary entry, which costs
        // ~400ns each and buys nothing when the dictionary is nearly as large
        // as the column: supplier's s_name has one entry per row, and paying
        // that turned a 20,000-row read into 9ms. Few entries is the condition
        // that makes Categorical worth it on BOTH sides -- cheap to intern, and
        // the representation downstream group-bys hash by code rather than by
        // string. So this is a single test on the absolute entry count, with no
        // row-count floor: `nation`'s 25 names stay categorical because 25
        // entries are 25 entries, whatever the table's size.
        if (fully_dictionary) {
            // Bracket the entry count from the dictionary's byte size before
            // reading it. A BYTE_ARRAY entry is a 4-byte length plus at least
            // one character, so `bytes / 5` is the most entries it can hold;
            // and an entry averaging more than `kBoundStringBytes` is one this
            // decision would reject anyway, since interning copies those bytes.
            // Only a dictionary whose bracket straddles the threshold has to be
            // read -- which leaves the small, genuinely borderline ones.
            namespace policy = ibex::parquet_dict;
            const std::int64_t rows = metadata.num_rows();
            switch (policy::verdict_from_bytes(dictionary_bytes(metadata, col), rows)) {
                case policy::Verdict::Keep:
                    break;
                case policy::Verdict::Dense:
                    fully_dictionary = false;
                    break;
                case policy::Verdict::NeedsCount: {
                    const auto entries = dictionary_entry_count(reader, col);
                    fully_dictionary = entries.has_value() && policy::worth_keeping(*entries, rows);
                    break;
                }
            }
        }
        if (std::getenv("IBEX_DICT_ENTRY_DEBUG") != nullptr) {
            std::fprintf(stderr, "[dict] col=%-18s dict_bytes=%9lld -> %s\n",
                         metadata.schema()->Column(col)->name().c_str(),
                         static_cast<long long>(dictionary_bytes(metadata, col)),
                         fully_dictionary ? "categorical" : "dense");
        }
        if (fully_dictionary) {
            out.push_back(col);
        }
    }
    return out;
}

/// Open `input` as an Arrow-backed Parquet reader, reading every fully
/// dictionary-encoded string column straight back as a dictionary rather than
/// materializing one `std::string` per row.
/// `dictionary_column_indices`, computed once per file.
inline auto cached_dictionary_column_indices(parquet::ParquetFileReader& reader,
                                             const std::string& path) -> std::vector<int> {
    static std::mutex mutex;
    static std::map<std::pair<std::string, std::int64_t>, std::vector<int>> cache;
    const auto key = std::pair{path, reader.metadata()->num_rows()};
    {
        const std::lock_guard lock(mutex);
        if (const auto it = cache.find(key); it != cache.end()) {
            return it->second;
        }
    }
    auto columns = dictionary_column_indices(reader);
    const std::lock_guard lock(mutex);
    cache.emplace(key, columns);
    return columns;
}

inline auto make_parquet_reader(std::shared_ptr<arrow::io::RandomAccessFile> input,
                                const std::string& path)
    -> std::unique_ptr<parquet::arrow::FileReader> {
    parquet::arrow::FileReaderBuilder builder;
    auto status = builder.Open(std::move(input));
    if (!status.ok()) {
        throw std::runtime_error("read_parquet: failed to read: " + path + " (" +
                                 status.ToString() + ")");
    }

    auto properties = parquet::default_arrow_reader_properties();
    // Memoized per file: deciding this reads the borderline dictionaries, and
    // a reader is built many times over a query's life (the pool hands out one
    // per worker). Keyed by path and row count so a file replaced underneath a
    // long-lived process is not answered from the old entry.
    for (int col : cached_dictionary_column_indices(*builder.raw_reader(), path)) {
        properties.set_read_dictionary(col, true);
    }
    builder.properties(properties);
    builder.memory_pool(arrow::default_memory_pool());

    std::unique_ptr<parquet::arrow::FileReader> reader;
    status = builder.Build(&reader);
    if (!status.ok()) {
        throw std::runtime_error("read_parquet: failed to open: " + path + " (" +
                                 status.ToString() + ")");
    }
    return reader;
}

/// Build another independent reader while reusing footer metadata that was
/// already parsed at bind time. This avoids a second footer read/deserialization
/// for every factory product without sharing any mutable decoder state.
inline auto make_parquet_reader(std::shared_ptr<arrow::io::RandomAccessFile> input,
                                const std::string& path,
                                const std::shared_ptr<parquet::FileMetaData>& metadata,
                                const std::vector<int>& dictionary_columns)
    -> std::unique_ptr<parquet::arrow::FileReader> {
    parquet::arrow::FileReaderBuilder builder;
    auto status = builder.Open(std::move(input), parquet::default_reader_properties(), metadata);
    if (!status.ok()) {
        throw std::runtime_error("read_parquet: failed to read: " + path + " (" +
                                 status.ToString() + ")");
    }

    auto properties = parquet::default_arrow_reader_properties();
    for (int col : dictionary_columns) {
        properties.set_read_dictionary(col, true);
    }
    builder.properties(properties);
    builder.memory_pool(arrow::default_memory_pool());

    std::unique_ptr<parquet::arrow::FileReader> reader;
    status = builder.Build(&reader);
    if (!status.ok()) {
        throw std::runtime_error("read_parquet: failed to open: " + path + " (" +
                                 status.ToString() + ")");
    }
    return reader;
}

constexpr std::int64_t kDirectDecodeBatchRows = 64 * 1024;

struct DirectValidity {
    explicit DirectValidity(std::size_t rows) : bits(rows, true) {}

    void append(bool valid) {
        if (!valid) {
            bits.set(position, false);
            has_null = true;
        }
        ++position;
    }

    void append_valid(std::size_t count) { position += count; }

    auto finish() && -> std::optional<ibex::runtime::ValidityBitmap> {
        if (!has_null) {
            return std::nullopt;
        }
        return std::move(bits);
    }

    ibex::runtime::ValidityBitmap bits;
    std::size_t position = 0;
    bool has_null = false;
};

/// Consecutive Parquet row groups covered by one direct decode. Source row
/// indices remain file-global so the same selection machinery works for both
/// whole-file lazy reads and row-group streaming.
struct DirectDecodeGroups {
    int begin = 0;
    int end = 0;
    std::size_t source_start = 0;
    std::size_t rows = 0;
};

inline auto all_decode_groups(const parquet::FileMetaData& metadata) -> DirectDecodeGroups {
    return DirectDecodeGroups{.begin = 0,
                              .end = metadata.num_row_groups(),
                              .source_start = 0,
                              .rows = static_cast<std::size_t>(metadata.num_rows())};
}

/// Column-chunk statistics prove the chunk holds no nulls. Writers (including
/// parquet-cpp-arrow) mark columns optional even when every value is present,
/// so this is the only way the null-free fast paths ever fire on real files.
inline auto chunk_has_no_nulls(const parquet::FileMetaData& metadata, int group, int leaf_index)
    -> bool {
    const auto chunk = metadata.RowGroup(group)->ColumnChunk(leaf_index);
    if (!chunk->is_stats_set()) {
        return false;
    }
    const auto stats = chunk->statistics();
    return stats != nullptr && stats->HasNullCount() && stats->null_count() == 0;
}

inline auto groups_have_no_nulls(const parquet::FileMetaData& metadata,
                                 const DirectDecodeGroups& groups, int leaf_index) -> bool {
    for (int group = groups.begin; group < groups.end; ++group) {
        if (!chunk_has_no_nulls(metadata, group, leaf_index)) {
            return false;
        }
    }
    return true;
}

template <typename DType, typename Emit>
inline auto decode_physical_column(parquet::arrow::FileReader& reader, int leaf_index,
                                   const ibex::runtime::Selection* selection,
                                   const DirectDecodeGroups& groups, Emit&& emit) -> std::size_t;

/// Rows a row-group range contributes: all of them, or the selected subset.
inline auto decode_output_rows(const ibex::runtime::Selection* selection,
                               const DirectDecodeGroups& groups) -> std::size_t {
    if (selection == nullptr) {
        return groups.rows;
    }
    const auto first = std::lower_bound(selection->begin(), selection->end(), groups.source_start);
    const auto last = std::lower_bound(first, selection->end(), groups.source_start + groups.rows);
    return static_cast<std::size_t>(std::distance(first, last));
}

/// Decode a row-group range into a buffer the caller already sized.
///
/// Split out from `decode_numeric_column` so that a caller holding the whole
/// column can hand each row-group range its own slice and decode the ranges
/// concurrently. It takes a raw pointer rather than the column on purpose:
/// `Column::span()` detaches copy-on-write storage, which is not safe to call
/// from several workers at once, so the destination pointer has to be taken
/// once before any of them start.
///
/// `output_data` must have room for `decode_output_rows(selection, groups)`.
template <typename DType, typename Out, bool SameLayout, typename Convert>
inline auto decode_numeric_into(parquet::arrow::FileReader& reader, int leaf_index,
                                const ibex::runtime::Selection* selection,
                                const DirectDecodeGroups& groups, Out* output_data,
                                std::size_t output_rows, DirectValidity& validity,
                                Convert&& convert) -> std::size_t {
    using Raw = typename DType::c_type;
    const auto& metadata = *reader.parquet_reader()->metadata();
    const bool optional = metadata.schema()->Column(leaf_index)->max_definition_level() != 0;
    if (selection != nullptr || (optional && !groups_have_no_nulls(metadata, groups, leaf_index))) {
        std::size_t output_pos = 0;
        const auto emitted = decode_physical_column<DType>(
            reader, leaf_index, selection, groups, [&](const Raw* value) {
                validity.append(value != nullptr);
                output_data[output_pos++] = value == nullptr ? Out{} : convert(*value);
            });
        if (output_pos != output_rows || emitted != output_rows) {
            throw std::runtime_error("read_parquet: selected decoder emitted the wrong row count");
        }
        return emitted;
    }

    std::size_t emitted = 0;
    std::unique_ptr<Raw[]> converted_values;
    if constexpr (!SameLayout) {
        converted_values.reset(new Raw[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    } else {
        static_assert(std::is_same_v<Raw, Out>);
    }
    std::unique_ptr<std::int16_t[]> definitions;
    if (optional) {
        definitions.reset(new std::int16_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    }
    for (int group = groups.begin; group < groups.end; ++group) {
        auto row_group = reader.parquet_reader()->RowGroup(group);
        auto column = row_group->Column(leaf_index);
        if (column->type() != DType::type_num) {
            throw std::runtime_error("read_parquet: physical column type does not match schema");
        }
        auto typed = std::static_pointer_cast<parquet::TypedColumnReader<DType>>(column);
        const auto group_rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
        std::size_t row = 0;
        while (row < group_rows && typed->HasNext()) {
            const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
                static_cast<std::size_t>(kDirectDecodeBatchRows), group_rows - row));
            std::int64_t values_read = 0;
            Raw* destination = nullptr;
            if constexpr (SameLayout) {
                destination = output_data + emitted;
            } else {
                destination = converted_values.get();
            }
            const std::int64_t levels_read =
                typed->ReadBatch(request, optional ? definitions.get() : nullptr, nullptr,
                                 destination, &values_read);
            if (levels_read <= 0 || levels_read != values_read) {
                throw std::runtime_error(
                    "read_parquet: dense column decoder made no progress or found a null "
                    "value that contradicts the column statistics");
            }
            const auto count = static_cast<std::size_t>(values_read);
            if constexpr (!SameLayout) {
                for (std::size_t i = 0; i < count; ++i) {
                    output_data[emitted + i] = convert(converted_values[i]);
                }
            }
            validity.append_valid(count);
            emitted += count;
            row += static_cast<std::size_t>(levels_read);
        }
        if (row != group_rows) {
            throw std::runtime_error("read_parquet: column ended before its row group");
        }
    }
    return emitted;
}

/// Decode a whole column, sizing its buffer first. The serial entry point;
/// `decode_numeric_into` is the same decode against a caller-owned buffer.
template <typename DType, typename Out, bool SameLayout, typename Convert>
inline auto decode_numeric_column(parquet::arrow::FileReader& reader, int leaf_index,
                                  const ibex::runtime::Selection* selection,
                                  const DirectDecodeGroups& groups, ibex::Column<Out>& out,
                                  DirectValidity& validity, Convert&& convert) -> std::size_t {
    const std::size_t output_start = out.size();
    const std::size_t output_rows = decode_output_rows(selection, groups);
    if constexpr (std::is_trivially_default_constructible_v<Out>) {
        out.resize_for_overwrite(output_start + output_rows);
    } else {
        out.resize(output_start + output_rows);
    }
    return decode_numeric_into<DType, Out, SameLayout>(
        reader, leaf_index, selection, groups, out.span().data() + output_start, output_rows,
        validity, std::forward<Convert>(convert));
}

/// Dense String decode — the counterpart of decode_numeric_column's bulk path,
/// which strings never had: every full read of a non-dictionary string column
/// went through the level-aware per-value loop.
///
/// Two costs disappear when the chunk statistics prove the group is null-free:
/// definition levels are not decoded at all (6M int16 of scratch traffic on
/// lineitem), and validity is bumped in bulk rather than a bit per row.
///
/// The third cost is the one that actually dominated: `Column<std::string>`'s
/// flat character buffer was never reserved (only its offsets were), so it grew
/// by doubling — re-copying l_comment's ~160MB of text about as many times as it
/// takes to reach that size. The column chunk's uncompressed byte size is a
/// close upper bound on the decoded characters (it also carries a 4-byte length
/// per value), so one reserve up front removes the whole reallocation chain.
inline auto decode_string_column(parquet::arrow::FileReader& reader, int leaf_index,
                                 const ibex::runtime::Selection* selection,
                                 const DirectDecodeGroups& groups, ibex::Column<std::string>& out,
                                 DirectValidity& validity) -> std::size_t {
    const auto& metadata = *reader.parquet_reader()->metadata();
    const bool optional = metadata.schema()->Column(leaf_index)->max_definition_level() != 0;

    if (selection != nullptr || (optional && !groups_have_no_nulls(metadata, groups, leaf_index))) {
        return decode_physical_column<parquet::ByteArrayType>(
            reader, leaf_index, selection, groups, [&](const parquet::ByteArray* value) {
                validity.append(value != nullptr);
                out.push_back(
                    value == nullptr
                        ? std::string_view{}
                        : std::string_view{
                              reinterpret_cast<const char*>(  // NOLINT(*-reinterpret-cast)
                                  value->ptr),
                              value->len});
            });
    }

    // The chunk's uncompressed byte size bounds the characters it can decode to
    // (it also carries a 4-byte length per value, so it overshoots slightly).
    // That bound is what lets the values be written straight through cursors
    // instead of one push_back — see Column<std::string>::begin_bulk_append.
    std::size_t chars_bound = 0;
    for (int group = groups.begin; group < groups.end; ++group) {
        chars_bound += static_cast<std::size_t>(
            metadata.RowGroup(group)->ColumnChunk(leaf_index)->total_uncompressed_size());
    }
    auto writer = out.begin_bulk_append(groups.rows, chars_bound);

    std::unique_ptr<parquet::ByteArray[]> values(
        new parquet::ByteArray[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::size_t emitted = 0;

    for (int group = groups.begin; group < groups.end; ++group) {
        auto row_group = reader.parquet_reader()->RowGroup(group);
        auto column = row_group->Column(leaf_index);
        if (column->type() != parquet::ByteArrayType::type_num) {
            throw std::runtime_error("read_parquet: physical column type does not match schema");
        }
        auto typed = std::static_pointer_cast<parquet::ByteArrayReader>(column);
        const auto group_rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
        std::size_t row = 0;
        while (row < group_rows && typed->HasNext()) {
            const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
                static_cast<std::size_t>(kDirectDecodeBatchRows), group_rows - row));
            std::int64_t values_read = 0;
            const std::int64_t levels_read =
                typed->ReadBatch(request, nullptr, nullptr, values.get(), &values_read);
            if (levels_read <= 0 || levels_read != values_read) {
                throw std::runtime_error(
                    "read_parquet: dense string decoder made no progress or found a null value "
                    "that contradicts the column statistics");
            }
            const auto count = static_cast<std::size_t>(values_read);
            // The ByteArray points into the page's decompression buffer, so the
            // copy has to happen before the next ReadBatch reuses it.
            for (std::size_t i = 0; i < count; ++i) {
                writer.append(std::string_view{
                    reinterpret_cast<const char*>(values[i].ptr),  // NOLINT(*-reinterpret-cast)
                    values[i].len});
            }
            validity.append_valid(count);
            emitted += count;
            row += static_cast<std::size_t>(levels_read);
        }
        if (row != group_rows) {
            throw std::runtime_error("read_parquet: column ended before its row group");
        }
    }
    out.finish_bulk_append(writer);
    return emitted;
}

inline auto decode_dictionary_column(parquet::arrow::FileReader& reader, int leaf_index,
                                     const ibex::runtime::Selection* selection,
                                     const DirectDecodeGroups& groups,
                                     std::vector<std::string>& dictionary,
                                     std::vector<std::int32_t>& codes, DirectValidity& validity)
    -> std::size_t {
    std::map<std::string, std::int32_t, std::less<>> index;
    auto intern = [&](std::string value) {
        auto [it, inserted] = index.try_emplace(value, 0);
        if (inserted) {
            it->second = static_cast<std::int32_t>(dictionary.size());
            dictionary.push_back(std::move(value));
        }
        return it->second;
    };

    std::unique_ptr<std::int32_t[]> local_codes(
        new std::int32_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::unique_ptr<std::int16_t[]> definitions(
        new std::int16_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    const auto& metadata = *reader.parquet_reader()->metadata();
    std::size_t group_start = groups.source_start;
    std::size_t selected_pos =
        selection == nullptr
            ? 0
            : static_cast<std::size_t>(
                  std::lower_bound(selection->begin(), selection->end(), group_start) -
                  selection->begin());
    std::size_t emitted = 0;

    for (int group = groups.begin; group < groups.end; ++group) {
        const auto group_rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
        std::size_t selected_end = selected_pos;
        if (selection != nullptr) {
            while (selected_end < selection->size() &&
                   (*selection)[selected_end] < group_start + group_rows) {
                ++selected_end;
            }
            if (selected_end == selected_pos) {
                group_start += group_rows;
                continue;
            }
        }

        auto row_group = reader.parquet_reader()->RowGroup(group);
        auto column =
            row_group->ColumnWithExposeEncoding(leaf_index, parquet::ExposedEncoding::DICTIONARY);
        if (column->GetExposedEncoding() != parquet::ExposedEncoding::DICTIONARY) {
            throw std::runtime_error("read_parquet: dictionary column changed encoding");
        }
        const auto* descriptor = column->descr();
        if (descriptor->max_repetition_level() != 0 || descriptor->max_definition_level() > 1) {
            throw std::runtime_error("read_parquet: nested columns are not supported");
        }
        const bool optional = descriptor->max_definition_level() != 0;
        // Chunk statistics proving null_count == 0 let this group skip
        // definition-level decoding and the per-row validity branch.
        const bool use_defs = optional && !chunk_has_no_nulls(metadata, group, leaf_index);
        auto typed = std::static_pointer_cast<parquet::ByteArrayReader>(column);
        std::vector<std::int32_t> local_to_global;

        std::size_t row = 0;
        while (row < group_rows && typed->HasNext()) {
            const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
                static_cast<std::size_t>(kDirectDecodeBatchRows), group_rows - row));
            std::int64_t codes_read = 0;
            const parquet::ByteArray* local_dictionary = nullptr;
            std::int32_t dictionary_size = 0;
            const std::int64_t levels_read = typed->ReadBatchWithDictionary(
                request, use_defs ? definitions.get() : nullptr, nullptr, local_codes.get(),
                &codes_read, &local_dictionary, &dictionary_size);
            if (levels_read <= 0) {
                throw std::runtime_error("read_parquet: dictionary decoder made no progress");
            }
            if (local_dictionary != nullptr) {
                local_to_global.clear();
                local_to_global.reserve(static_cast<std::size_t>(dictionary_size));
                for (std::int32_t i = 0; i < dictionary_size; ++i) {
                    const auto& value = local_dictionary[i];
                    local_to_global.push_back(
                        intern(std::string(reinterpret_cast<const char*>(value.ptr), value.len)));
                }
            }
            if (local_to_global.empty() && codes_read != 0) {
                throw std::runtime_error("read_parquet: dictionary page was not exposed");
            }

            if (selection == nullptr && !use_defs) {
                for (std::int64_t offset = 0; offset < codes_read; ++offset) {
                    const auto local = local_codes[static_cast<std::size_t>(offset)];
                    if (local < 0 || static_cast<std::size_t>(local) >= local_to_global.size()) {
                        throw std::runtime_error("read_parquet: invalid dictionary index");
                    }
                    codes.push_back(local_to_global[static_cast<std::size_t>(local)]);
                }
                validity.append_valid(static_cast<std::size_t>(codes_read));
                emitted += static_cast<std::size_t>(codes_read);
                row += static_cast<std::size_t>(levels_read);
                continue;
            }

            std::size_t code_pos = 0;
            for (std::int64_t offset = 0; offset < levels_read; ++offset) {
                const bool valid = !use_defs || definitions[static_cast<std::size_t>(offset)] == 1;
                const std::size_t source_row = group_start + row + static_cast<std::size_t>(offset);
                bool keep = selection == nullptr;
                if (selection != nullptr && selected_pos < selected_end &&
                    (*selection)[selected_pos] == source_row) {
                    keep = true;
                    ++selected_pos;
                }
                if (keep) {
                    validity.append(valid);
                    if (!valid) {
                        codes.push_back(intern(std::string{}));
                    } else {
                        const auto local = local_codes[code_pos];
                        if (local < 0 ||
                            static_cast<std::size_t>(local) >= local_to_global.size()) {
                            throw std::runtime_error("read_parquet: invalid dictionary index");
                        }
                        codes.push_back(local_to_global[static_cast<std::size_t>(local)]);
                    }
                    ++emitted;
                }
                code_pos += static_cast<std::size_t>(valid);
            }
            if (code_pos != static_cast<std::size_t>(codes_read)) {
                throw std::runtime_error("read_parquet: inconsistent definition levels");
            }
            row += static_cast<std::size_t>(levels_read);
        }
        if (row != group_rows) {
            throw std::runtime_error("read_parquet: column ended before its row group");
        }
        group_start += group_rows;
    }
    return emitted;
}

template <typename DType, typename Emit>
inline auto decode_physical_column(parquet::arrow::FileReader& reader, int leaf_index,
                                   const ibex::runtime::Selection* selection,
                                   const DirectDecodeGroups& groups, Emit&& emit) -> std::size_t {
    using Raw = typename DType::c_type;

    std::unique_ptr<Raw[]> values(new Raw[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::unique_ptr<std::int16_t[]> definitions(
        new std::int16_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);

    const auto& metadata = *reader.parquet_reader()->metadata();
    std::size_t group_start = groups.source_start;
    std::size_t selected_pos =
        selection == nullptr
            ? 0
            : static_cast<std::size_t>(
                  std::lower_bound(selection->begin(), selection->end(), group_start) -
                  selection->begin());
    std::size_t emitted = 0;

    for (int group = groups.begin; group < groups.end; ++group) {
        const auto group_rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
        std::size_t selected_end = selected_pos;
        if (selection != nullptr) {
            while (selected_end < selection->size() &&
                   (*selection)[selected_end] < group_start + group_rows) {
                ++selected_end;
            }
            if (selected_end == selected_pos) {
                group_start += group_rows;
                continue;
            }
        }

        auto row_group = reader.parquet_reader()->RowGroup(group);
        auto column = row_group->Column(leaf_index);
        if (column->type() != DType::type_num) {
            throw std::runtime_error("read_parquet: physical column type does not match schema");
        }
        const auto* descriptor = column->descr();
        if (descriptor->max_repetition_level() != 0 || descriptor->max_definition_level() > 1) {
            throw std::runtime_error("read_parquet: nested columns are not supported");
        }
        const bool optional = descriptor->max_definition_level() != 0;
        auto typed = std::static_pointer_cast<parquet::TypedColumnReader<DType>>(column);

        // Null-free columns (required, or optional with chunk statistics
        // proving null_count == 0) have a 1:1 mapping between logical rows
        // and physical values, which unlocks two selected fast paths.
        // Columns that may hold nulls stay on the level-aware path below
        // because TypedColumnReader::Skip counts physical values rather than
        // logical rows, and nulls have no physical value to skip.
        const bool group_null_free = !optional || chunk_has_no_nulls(metadata, group, leaf_index);
        const std::size_t group_selected = selected_end - selected_pos;
        // Skip() only avoids decode work when a gap covers the rest of a data
        // page; partial-page gaps are decoded into scratch anyway, so for
        // scattered selections the per-call overhead makes it a net loss.
        // Route needle-in-haystack selections through Skip and everything
        // else through dense batch decode + gather.
        const bool group_sparse =
            group_selected > 0 &&
            group_rows / group_selected > static_cast<std::size_t>(kDirectDecodeBatchRows);
        if (selection != nullptr && group_null_free && group_sparse) {
            std::size_t source_row = group_start;
            while (selected_pos < selected_end) {
                const std::size_t target = (*selection)[selected_pos];
                const std::size_t gap = target - source_row;
                if (gap != 0) {
                    const auto skipped = typed->Skip(static_cast<std::int64_t>(gap));
                    if (skipped != static_cast<std::int64_t>(gap)) {
                        throw std::runtime_error(
                            "read_parquet: column ended while skipping rejected rows");
                    }
                    source_row += gap;
                }

                std::size_t run = 1;
                while (selected_pos + run < selected_end &&
                       (*selection)[selected_pos + run] == target + run &&
                       run < static_cast<std::size_t>(kDirectDecodeBatchRows)) {
                    ++run;
                }
                // Skip() consumes definition levels through its internal
                // scratch ReadBatch, so our reads on an optional column must
                // consume them too or the level decoder falls out of step.
                // ReadBatch stops at page boundaries, so loop until the run
                // is complete — and emit each partial batch before the next
                // read: ByteArray values point into the current page's
                // decompression buffer, which the next page's read reuses.
                std::size_t got = 0;
                while (got < run) {
                    std::int64_t values_read = 0;
                    const auto levels_read =
                        typed->ReadBatch(static_cast<std::int64_t>(run - got),
                                         optional ? definitions.get() : nullptr, nullptr,
                                         values.get(), &values_read);
                    if (levels_read <= 0 || values_read != levels_read) {
                        throw std::runtime_error(
                            "read_parquet: column ended or held an unexpected null while "
                            "reading selected rows");
                    }
                    for (std::int64_t i = 0; i < levels_read; ++i) {
                        emit(&values[static_cast<std::size_t>(i)]);
                    }
                    got += static_cast<std::size_t>(levels_read);
                }
                emitted += run;
                selected_pos += run;
                source_row += run;
            }
            group_start += group_rows;
            continue;
        }
        if (selection != nullptr && group_null_free) {
            // Dense batch decode + gather of the selected offsets. Definition
            // levels stay unread: nothing else consumes levels in this branch
            // and the group is proven null-free. Once the selection is
            // exhausted the rest of the group is never decoded.
            std::size_t row = 0;
            while (selected_pos < selected_end) {
                if (row >= group_rows || !typed->HasNext()) {
                    throw std::runtime_error(
                        "read_parquet: column ended while reading selected rows");
                }
                const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
                    static_cast<std::size_t>(kDirectDecodeBatchRows), group_rows - row));
                std::int64_t values_read = 0;
                const std::int64_t levels_read =
                    typed->ReadBatch(request, nullptr, nullptr, values.get(), &values_read);
                if (levels_read <= 0 || values_read != levels_read) {
                    throw std::runtime_error(
                        "read_parquet: column ended or held an unexpected null while reading "
                        "selected rows");
                }
                const std::size_t batch_start = group_start + row;
                const std::size_t batch_end = batch_start + static_cast<std::size_t>(levels_read);
                while (selected_pos < selected_end && (*selection)[selected_pos] < batch_end) {
                    emit(&values[(*selection)[selected_pos] - batch_start]);
                    ++emitted;
                    ++selected_pos;
                }
                row += static_cast<std::size_t>(levels_read);
            }
            group_start += group_rows;
            continue;
        }

        std::size_t row = 0;
        while (row < group_rows && typed->HasNext()) {
            const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
                static_cast<std::size_t>(kDirectDecodeBatchRows), group_rows - row));
            std::int64_t values_read = 0;
            const std::int64_t levels_read =
                typed->ReadBatch(request, optional ? definitions.get() : nullptr, nullptr,
                                 values.get(), &values_read);
            if (levels_read <= 0) {
                throw std::runtime_error("read_parquet: column decoder made no progress");
            }

            std::size_t raw_pos = 0;
            for (std::int64_t offset = 0; offset < levels_read; ++offset) {
                const bool valid = !optional || definitions[static_cast<std::size_t>(offset)] == 1;
                const std::size_t source_row = group_start + row + static_cast<std::size_t>(offset);
                bool keep = selection == nullptr;
                if (selection != nullptr && selected_pos < selected_end &&
                    (*selection)[selected_pos] == source_row) {
                    keep = true;
                    ++selected_pos;
                }
                if (keep) {
                    emit(valid ? &values[raw_pos] : nullptr);
                    ++emitted;
                }
                raw_pos += static_cast<std::size_t>(valid);
            }
            if (raw_pos != static_cast<std::size_t>(values_read)) {
                throw std::runtime_error("read_parquet: inconsistent definition levels");
            }
            row += static_cast<std::size_t>(levels_read);
        }
        if (row != group_rows) {
            throw std::runtime_error("read_parquet: column ended before its row group");
        }
        group_start += group_rows;
    }
    return emitted;
}

/// Decodes one row-group range of a column into the slice starting at
/// `output_start`. Returns the rows emitted.
using ShardDecodeFn = std::function<std::size_t(
    parquet::arrow::FileReader&, const DirectDecodeGroups&, std::size_t, std::size_t)>;

/// Runs once, on the calling thread, after every `ShardDecodeFn` call for a
/// column has returned; finishes assembling `entry.column`. Only dictionary
/// needs one — see `ShardedColumn`'s comment.
using ShardFinalizeFn = std::function<void(ibex::runtime::ColumnEntry& entry)>;

/// A column whose row-group ranges can be decoded independently, with its
/// buffer already allocated and its decoder bound to that buffer.
///
/// What makes the ranges independent is that every range's output row count is
/// known before any decoding happens — from the footer, or from where the
/// selection crosses the range's row bounds — so each range owns a disjoint
/// slice of one flat buffer and there is nothing to merge afterwards, for
/// every type except one. The result is the same bytes the serial decode
/// would have written.
///
/// `Column<bool>` packs 64 rows into a word, so neighbouring ranges would
/// write the same word; a string's destination offset depends on the total
/// length of every preceding row; and a column with nulls shares a validity
/// bitmap, which has that same word-sharing problem — hence the null-free
/// requirement rather than a null-handling shard path, for every shardable
/// type including dictionary.
///
/// Dictionary is the one type that IS split despite needing a merge: its
/// codes are fixed-width, so writing them is exactly as parallel as any
/// numeric type — what differs is that a code only means something against
/// the per-row-group dictionary it was read with, and each shard reads its
/// own. `finalize`, run once on the calling thread after every shard's
/// `decode_range` has returned, unifies those per-shard dictionaries into one
/// and remaps the codes already written in place — the same per-chunk-to-one
/// remap `build_categorical_column` already does across Arrow chunks, done
/// here across shard outputs instead, and just as cheap for the same reason:
/// this path only exists for low-cardinality columns, so there are a handful
/// of strings to unify against millions of already-written codes.
struct ShardedColumn {
    ibex::runtime::ColumnEntry entry;
    ShardDecodeFn decode_range;
    /// Empty for every type except dictionary. See the class comment.
    ShardFinalizeFn finalize;
};

/// Size the destination buffer, hand its address to a decoder bound to it, and
/// leave the column in `entry`.
///
/// The address is taken once, here, rather than inside each range: `span()`
/// detaches copy-on-write storage, so several workers calling it at once would
/// race.
template <typename DType, typename Out, bool SameLayout, typename Convert>
inline auto sharded_numeric(ibex::runtime::ColumnEntry& entry, int leaf_index,
                            const ibex::runtime::Selection* selection, std::size_t output_rows,
                            Convert convert) -> ShardDecodeFn {
    entry.column = std::make_shared<ibex::runtime::ColumnValue>(ibex::Column<Out>{});
    auto& out = std::get<ibex::Column<Out>>(*entry.column);
    if constexpr (std::is_trivially_default_constructible_v<Out>) {
        out.resize_for_overwrite(output_rows);
    } else {
        out.resize(output_rows);
    }
    Out* const base = out.span().data();
    return [base, leaf_index, selection, convert](
               parquet::arrow::FileReader& reader, const DirectDecodeGroups& range,
               std::size_t output_start, std::size_t rows) -> std::size_t {
        DirectValidity validity(rows);
        const auto emitted = decode_numeric_into<DType, Out, SameLayout>(
            reader, leaf_index, selection, range, base + output_start, rows, validity, convert);
        if (emitted != rows || validity.position != rows || validity.has_null) {
            throw std::runtime_error("read_parquet: sharded decode produced the wrong row count");
        }
        return emitted;
    };
}

/// Plans a dictionary column's shard. Codes are fixed-width, so writing them
/// is exactly as parallel as any numeric shard — `decode_range` writes each
/// row group's codes straight into the shared flat buffer, LOCAL to that row
/// group's own dictionary (there is no way to know ahead of time what a row
/// group's dictionary holds). `finalize` then unifies the per-shard local
/// dictionaries into one global one and remaps the already-written codes in
/// place, on the calling thread, once every shard has decoded — the same
/// per-chunk-to-one remap `build_categorical_column` does across Arrow
/// chunks, done here across shard outputs instead.
inline void sharded_dictionary(ShardedColumn& planned, int leaf_index,
                               const ibex::runtime::Selection* selection,
                               const DirectDecodeGroups& groups, std::size_t output_rows) {
    const auto num_shards = static_cast<std::size_t>(groups.end - groups.begin);
    auto codes = std::make_shared<std::vector<std::int32_t>>(output_rows, 0);
    auto local_dicts = std::make_shared<std::vector<std::vector<std::string>>>(num_shards);
    // {output_start, rows} per shard, in row-group order. A shard the
    // selection rejects entirely is never decoded and keeps {0, 0}, which
    // `finalize`'s remap loop treats as nothing to do.
    auto shard_bounds =
        std::make_shared<std::vector<std::pair<std::size_t, std::size_t>>>(num_shards);
    std::int32_t* const base = codes->data();
    const int groups_begin = groups.begin;

    planned.decode_range = [base, leaf_index, selection, local_dicts, shard_bounds, groups_begin](
                               parquet::arrow::FileReader& reader, const DirectDecodeGroups& range,
                               std::size_t output_start, std::size_t rows) -> std::size_t {
        const auto shard = static_cast<std::size_t>(range.begin - groups_begin);
        DirectValidity validity(rows);
        std::vector<std::int32_t> local_codes;
        local_codes.reserve(rows);
        const auto emitted = decode_dictionary_column(reader, leaf_index, selection, range,
                                                      (*local_dicts)[shard], local_codes, validity);
        if (emitted != rows || validity.position != rows || validity.has_null) {
            throw std::runtime_error(
                "read_parquet: sharded dictionary decode produced the wrong row count");
        }
        std::copy(local_codes.begin(), local_codes.end(), base + output_start);
        (*shard_bounds)[shard] = {output_start, rows};
        return emitted;
    };

    planned.finalize = [codes, local_dicts,
                        shard_bounds](ibex::runtime::ColumnEntry& entry) mutable {
        std::vector<std::string> global_dict;
        std::map<std::string, std::int32_t, std::less<>> global_index;
        auto intern = [&](const std::string& value) {
            auto [it, inserted] = global_index.try_emplace(value, 0);
            if (inserted) {
                it->second = static_cast<std::int32_t>(global_dict.size());
                global_dict.push_back(value);
            }
            return it->second;
        };
        std::int32_t* const codes_base = codes->data();
        for (std::size_t shard = 0; shard < local_dicts->size(); ++shard) {
            const auto [output_start, rows] = (*shard_bounds)[shard];
            if (rows == 0) {
                continue;
            }
            std::vector<std::int32_t> remap;
            remap.reserve((*local_dicts)[shard].size());
            for (const auto& value : (*local_dicts)[shard]) {
                remap.push_back(intern(value));
            }
            for (std::size_t i = output_start; i < output_start + rows; ++i) {
                const auto local = codes_base[i];
                if (local < 0 || static_cast<std::size_t>(local) >= remap.size()) {
                    throw std::runtime_error("read_parquet: invalid sharded dictionary code");
                }
                codes_base[i] = remap[static_cast<std::size_t>(local)];
            }
        }
        entry.column = std::make_shared<ibex::runtime::ColumnValue>(
            ibex::Column<ibex::Categorical>{std::move(global_dict), std::move(*codes)});
    };
}

/// nullopt when this column cannot be split by row group — see `ShardedColumn`
/// for which columns those are and why.
inline auto plan_sharded_column(parquet::arrow::FileReader& reader, const arrow::Field& field,
                                int leaf_index, const ibex::runtime::Selection* selection,
                                const DirectDecodeGroups& groups, std::size_t output_rows)
    -> std::optional<ShardedColumn> {
    const auto& metadata = *reader.parquet_reader()->metadata();
    if (metadata.schema()->Column(leaf_index)->max_definition_level() != 0 &&
        !groups_have_no_nulls(metadata, groups, leaf_index)) {
        return std::nullopt;
    }

    ShardedColumn planned;
    planned.entry.name = field.name();
    const auto id = field.type()->id();
    switch (id) {
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32:
            planned.decode_range = sharded_numeric<parquet::Int32Type, std::int64_t, false>(
                planned.entry, leaf_index, selection, output_rows, [id](std::int32_t value) {
                    if (id == arrow::Type::UINT8 || id == arrow::Type::UINT16 ||
                        id == arrow::Type::UINT32) {
                        return static_cast<std::int64_t>(static_cast<std::uint32_t>(value));
                    }
                    return static_cast<std::int64_t>(value);
                });
            break;
        case arrow::Type::INT64:
        case arrow::Type::UINT64:
            planned.decode_range = sharded_numeric<parquet::Int64Type, std::int64_t, true>(
                planned.entry, leaf_index, selection, output_rows,
                [](std::int64_t value) { return value; });
            break;
        case arrow::Type::FLOAT:
            planned.decode_range = sharded_numeric<parquet::FloatType, double, false>(
                planned.entry, leaf_index, selection, output_rows,
                [](float value) { return static_cast<double>(value); });
            break;
        case arrow::Type::DOUBLE:
            planned.decode_range = sharded_numeric<parquet::DoubleType, double, true>(
                planned.entry, leaf_index, selection, output_rows,
                [](double value) { return value; });
            break;
        case arrow::Type::DATE32:
            planned.decode_range = sharded_numeric<parquet::Int32Type, ibex::Date, false>(
                planned.entry, leaf_index, selection, output_rows,
                [](std::int32_t value) { return ibex::Date{value}; });
            break;
        case arrow::Type::DATE64: {
            constexpr std::int64_t kMillisPerDay = 86'400'000;
            planned.decode_range = sharded_numeric<parquet::Int64Type, ibex::Date, false>(
                planned.entry, leaf_index, selection, output_rows, [](std::int64_t value) {
                    return ibex::Date{static_cast<std::int32_t>(value / kMillisPerDay)};
                });
            break;
        }
        case arrow::Type::TIMESTAMP: {
            // INT96 timestamps go through the per-value physical path, which
            // this does not cover.
            if (metadata.schema()->Column(leaf_index)->physical_type() == parquet::Type::INT96) {
                return std::nullopt;
            }
            const auto unit = static_cast<const arrow::TimestampType&>(*field.type()).unit();
            std::int64_t scale = 1;
            if (unit == arrow::TimeUnit::SECOND) {
                scale = 1'000'000'000;
            } else if (unit == arrow::TimeUnit::MILLI) {
                scale = 1'000'000;
            } else if (unit == arrow::TimeUnit::MICRO) {
                scale = 1'000;
            }
            planned.decode_range = sharded_numeric<parquet::Int64Type, ibex::Timestamp, false>(
                planned.entry, leaf_index, selection, output_rows,
                [scale](std::int64_t value) { return ibex::Timestamp{value * scale}; });
            break;
        }
        case arrow::Type::DICTIONARY:
            sharded_dictionary(planned, leaf_index, selection, groups, output_rows);
            break;
        default:
            return std::nullopt;
    }
    return planned;
}

inline auto direct_column(parquet::arrow::FileReader& reader, const arrow::Field& field,
                          int leaf_index, const ibex::runtime::Selection* selection,
                          const DirectDecodeGroups& groups, std::size_t output_rows)
    -> ibex::runtime::ColumnEntry {
    DirectValidity validity(output_rows);
    ibex::runtime::ColumnEntry entry;
    entry.name = field.name();

    auto verify = [&](std::size_t emitted) {
        if (emitted != output_rows || validity.position != output_rows) {
            throw std::runtime_error("read_parquet: decoded column has the wrong row count");
        }
    };

    switch (field.type()->id()) {
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::UINT8:
        case arrow::Type::UINT16:
        case arrow::Type::UINT32: {
            ibex::Column<std::int64_t> out;
            out.reserve(output_rows);
            const auto id = field.type()->id();
            auto emitted = decode_numeric_column<parquet::Int32Type, std::int64_t, false>(
                reader, leaf_index, selection, groups, out, validity, [&](std::int32_t value) {
                    if (id == arrow::Type::UINT8 || id == arrow::Type::UINT16 ||
                        id == arrow::Type::UINT32) {
                        return static_cast<std::int64_t>(static_cast<std::uint32_t>(value));
                    }
                    return static_cast<std::int64_t>(value);
                });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::INT64:
        case arrow::Type::UINT64: {
            ibex::Column<std::int64_t> out;
            out.reserve(output_rows);
            auto emitted = decode_numeric_column<parquet::Int64Type, std::int64_t, true>(
                reader, leaf_index, selection, groups, out, validity,
                [](std::int64_t value) { return value; });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::FLOAT: {
            ibex::Column<double> out;
            out.reserve(output_rows);
            auto emitted = decode_numeric_column<parquet::FloatType, double, false>(
                reader, leaf_index, selection, groups, out, validity,
                [](float value) { return static_cast<double>(value); });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::DOUBLE: {
            ibex::Column<double> out;
            out.reserve(output_rows);
            auto emitted = decode_numeric_column<parquet::DoubleType, double, true>(
                reader, leaf_index, selection, groups, out, validity,
                [](double value) { return value; });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::BOOL: {
            ibex::Column<bool> out;
            out.reserve(output_rows);
            auto emitted = decode_physical_column<parquet::BooleanType>(
                reader, leaf_index, selection, groups, [&](const bool* value) {
                    validity.append(value != nullptr);
                    out.push_back(value != nullptr && *value);
                });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING: {
            // Straight into the flat buffers — see append_string_column. The
            // ByteArray still points into the page's decompression buffer, so
            // the copy must happen before the next page read.
            ibex::Column<std::string> out;
            out.reserve(output_rows);
            auto emitted =
                decode_string_column(reader, leaf_index, selection, groups, out, validity);
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::DICTIONARY: {
            std::vector<std::string> dictionary;
            std::vector<std::int32_t> codes;
            codes.reserve(output_rows);
            auto emitted = decode_dictionary_column(reader, leaf_index, selection, groups,
                                                    dictionary, codes, validity);
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(
                ibex::Column<ibex::Categorical>{std::move(dictionary), std::move(codes)});
            break;
        }
        case arrow::Type::DATE32: {
            ibex::Column<ibex::Date> out;
            out.reserve(output_rows);
            auto emitted = decode_numeric_column<parquet::Int32Type, ibex::Date, false>(
                reader, leaf_index, selection, groups, out, validity,
                [](std::int32_t value) { return ibex::Date{value}; });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::DATE64: {
            constexpr std::int64_t kMillisPerDay = 86'400'000;
            ibex::Column<ibex::Date> out;
            out.reserve(output_rows);
            auto emitted = decode_numeric_column<parquet::Int64Type, ibex::Date, false>(
                reader, leaf_index, selection, groups, out, validity, [](std::int64_t value) {
                    return ibex::Date{static_cast<std::int32_t>(value / kMillisPerDay)};
                });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        case arrow::Type::TIMESTAMP: {
            ibex::Column<ibex::Timestamp> out;
            out.reserve(output_rows);
            const auto unit = static_cast<const arrow::TimestampType&>(*field.type()).unit();
            std::int64_t scale = 1;
            if (unit == arrow::TimeUnit::SECOND) {
                scale = 1'000'000'000;
            } else if (unit == arrow::TimeUnit::MILLI) {
                scale = 1'000'000;
            } else if (unit == arrow::TimeUnit::MICRO) {
                scale = 1'000;
            }

            const auto physical =
                reader.parquet_reader()->metadata()->schema()->Column(leaf_index)->physical_type();
            std::size_t emitted = 0;
            if (physical == parquet::Type::INT96) {
                emitted = decode_physical_column<parquet::Int96Type>(
                    reader, leaf_index, selection, groups, [&](const parquet::Int96* value) {
                        validity.append(value != nullptr);
                        out.push_back(ibex::Timestamp{
                            value == nullptr ? 0 : parquet::Int96GetNanoSeconds(*value)});
                    });
            } else {
                emitted = decode_numeric_column<parquet::Int64Type, ibex::Timestamp, false>(
                    reader, leaf_index, selection, groups, out, validity,
                    [scale](std::int64_t value) { return ibex::Timestamp{value * scale}; });
            }
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(std::move(out));
            break;
        }
        default:
            throw std::runtime_error("read_parquet: unsupported column type for " + field.name());
    }

    entry.validity = std::move(validity).finish();
    return entry;
}

/// One piece of decode work: a column, or one row-group range of one column.
struct DecodeTask {
    std::size_t column;
    DirectDecodeGroups range;
    std::size_t output_start;  ///< where this range's rows begin in the column
    std::size_t rows;
};

/// Decode `field_indices` using one reader per worker.
///
/// **The work is split by column and, where a column allows it, by row group.**
/// Columns alone are the wider axis on a PDS-H-shaped file — projection
/// pushdown asks for several of them — but they are a coarse one: a query that
/// wants a single column (q06 filters on `l_shipdate` alone) had no parallelism
/// at all, and three columns of very different sizes leave workers idle behind
/// the largest. Splitting by row group fixes both, for the columns whose output
/// layout can be worked out before decoding; `ShardedColumn` says which those
/// are. Either way a worker writes only into its own slice of a buffer that was
/// allocated up front, so nothing is merged or concatenated afterwards and the
/// result is the same bytes as the serial decode.
///
/// Single-row-group files (every PDS-H dimension table) still split by column
/// only. There is nothing else to split.
///
/// `readers` must hold one independent `FileReader` per worker. Page readers
/// and decode cursors are mutable, so workers cannot share one — see
/// `ParquetLazySourceReader`, which owns the pool these come from. `readers[0]`
/// may be the caller's own reader: the calling thread blocks in `wait()` for
/// the whole batch and so never touches it concurrently.
inline auto direct_decode_table(std::span<parquet::arrow::FileReader* const> readers,
                                const arrow::Schema& schema, const std::vector<int>& field_indices,
                                const ibex::runtime::Selection* selection, std::size_t source_rows,
                                const DirectDecodeGroups& groups) -> ibex::runtime::Table {
    if (readers.empty()) {
        throw std::runtime_error("read_parquet: no reader for decode");
    }
    auto& reader = *readers.front();
    if (selection != nullptr) {
        if (!std::is_sorted(selection->begin(), selection->end()) ||
            std::adjacent_find(selection->begin(), selection->end()) != selection->end() ||
            (!selection->empty() && selection->back() >= source_rows)) {
            throw std::runtime_error("read_parquet: invalid row selection");
        }
    }

    if (groups.begin < 0 || groups.end < groups.begin ||
        groups.end > reader.parquet_reader()->metadata()->num_row_groups() ||
        groups.source_start + groups.rows > source_rows) {
        throw std::runtime_error("read_parquet: invalid decode row-group range");
    }

    std::size_t output_rows = groups.rows;
    if (selection != nullptr) {
        const auto selection_begin =
            std::lower_bound(selection->begin(), selection->end(), groups.source_start);
        const auto selection_end =
            std::lower_bound(selection_begin, selection->end(), groups.source_start + groups.rows);
        output_rows = static_cast<std::size_t>(std::distance(selection_begin, selection_end));
    }

    // Resolve every leaf index up front. The schema checks are cheap and doing
    // them here keeps the worker body free of anything that could throw for a
    // reason unrelated to its own column.
    const auto& manifest = reader.manifest();
    std::vector<int> leaves;
    leaves.reserve(field_indices.size());
    for (int field_index : field_indices) {
        if (field_index < 0 || field_index >= schema.num_fields() ||
            field_index >= static_cast<int>(manifest.schema_fields.size()) ||
            !manifest.schema_fields[static_cast<std::size_t>(field_index)].is_leaf()) {
            throw std::runtime_error("read_parquet: nested columns are not supported");
        }
        leaves.push_back(
            manifest.schema_fields[static_cast<std::size_t>(field_index)].column_index);
    }

    std::vector<ibex::runtime::ColumnEntry> entries(field_indices.size());

    // Plan before dispatching. A column that can be split by row group gets its
    // buffer allocated here and one task per row group; anything else stays one
    // task for the whole column.
    std::vector<std::optional<ShardedColumn>> planned(field_indices.size());
    const bool may_shard = readers.size() > 1 && groups.end - groups.begin > 1;
    if (may_shard) {
        for (std::size_t i = 0; i < field_indices.size(); ++i) {
            planned[i] = plan_sharded_column(reader, *schema.field(field_indices[i]), leaves[i],
                                             selection, groups, output_rows);
        }
    }

    // Whole-column tasks are queued FIRST, ahead of every split one. They are
    // the indivisible ones — a dictionary or string column has to be decoded
    // end to end by a single worker — so whichever starts last sets the finish
    // time for the whole decode. Queued behind thirty shard tasks, q01's two
    // dictionary columns started last and cost 6.5%; started first, they run
    // under the split columns instead of after them.
    std::vector<DecodeTask> tasks;
    for (std::size_t i = 0; i < field_indices.size(); ++i) {
        if (!planned[i].has_value()) {
            tasks.push_back({.column = i, .range = groups, .output_start = 0, .rows = output_rows});
        }
    }
    for (std::size_t i = 0; i < field_indices.size(); ++i) {
        if (!planned[i].has_value()) {
            continue;
        }
        std::size_t source_start = groups.source_start;
        std::size_t output_start = 0;
        for (int group = groups.begin; group < groups.end; ++group) {
            const auto group_rows = static_cast<std::size_t>(
                reader.parquet_reader()->metadata()->RowGroup(group)->num_rows());
            const DirectDecodeGroups range{
                .begin = group, .end = group + 1, .source_start = source_start, .rows = group_rows};
            const std::size_t rows = decode_output_rows(selection, range);
            if (rows != 0) {
                tasks.push_back(
                    {.column = i, .range = range, .output_start = output_start, .rows = rows});
            }
            source_start += group_rows;
            output_start += rows;
        }
        if (output_start != output_rows) {
            throw std::runtime_error("read_parquet: row-group split lost rows");
        }
    }

    auto run_task = [&](const DecodeTask& task, parquet::arrow::FileReader& worker_reader) {
        if (planned[task.column].has_value()) {
            planned[task.column]->decode_range(worker_reader, task.range, task.output_start,
                                               task.rows);
            return;
        }
        entries[task.column] =
            direct_column(worker_reader, *schema.field(field_indices[task.column]),
                          leaves[task.column], selection, task.range, task.rows);
    };

    const std::size_t workers = std::min(readers.size(), tasks.size());
    if (workers <= 1) {
        for (const auto& task : tasks) {
            run_task(task, reader);
        }
    } else {
        // A shared cursor rather than a static split: a string column can cost
        // an order of magnitude more than an int one, so a fixed assignment
        // would leave workers idle behind whoever drew `l_comment`.
        std::atomic<std::size_t> next{0};
        auto batch = ibex::runtime::process_worker_pool().submit(workers, [&](std::size_t worker) {
            for (std::size_t i = next.fetch_add(1, std::memory_order_relaxed); i < tasks.size();
                 i = next.fetch_add(1, std::memory_order_relaxed)) {
                run_task(tasks[i], *readers[worker]);
            }
        });
        batch.wait();
    }

    // Every shard for every column has now decoded. Dictionary is the only
    // planned type that leaves work for the calling thread — see
    // `ShardedColumn`'s comment — so this is cheap for every other column.
    for (auto& column : planned) {
        if (column.has_value() && column->finalize) {
            column->finalize(column->entry);
        }
    }

    ibex::runtime::Table out;
    for (std::size_t i = 0; i < entries.size(); ++i) {
        auto& entry = planned[i].has_value() ? planned[i]->entry : entries[i];
        out.add_column_shared(std::move(entry.name), std::move(entry.column),
                              std::move(entry.validity));
    }
    out.logical_rows = output_rows;
    return out;
}

inline auto direct_decode_table(parquet::arrow::FileReader& reader, const arrow::Schema& schema,
                                const std::vector<int>& field_indices,
                                const ibex::runtime::Selection* selection, std::size_t source_rows,
                                const DirectDecodeGroups& groups) -> ibex::runtime::Table {
    parquet::arrow::FileReader* one = &reader;
    return direct_decode_table(std::span{&one, 1}, schema, field_indices, selection, source_rows,
                               groups);
}

inline auto direct_decode_table(parquet::arrow::FileReader& reader, const arrow::Schema& schema,
                                const std::vector<int>& field_indices,
                                const ibex::runtime::Selection* selection, std::size_t source_rows)
    -> ibex::runtime::Table {
    return direct_decode_table(reader, schema, field_indices, selection, source_rows,
                               all_decode_groups(*reader.parquet_reader()->metadata()));
}

inline auto direct_decode_table(std::span<parquet::arrow::FileReader* const> readers,
                                const arrow::Schema& schema, const std::vector<int>& field_indices,
                                const ibex::runtime::Selection* selection, std::size_t source_rows)
    -> ibex::runtime::Table {
    if (readers.empty()) {
        throw std::runtime_error("read_parquet: no reader for decode");
    }
    return direct_decode_table(readers, schema, field_indices, selection, source_rows,
                               all_decode_groups(*readers.front()->parquet_reader()->metadata()));
}

}  // namespace

/// Open `path` for deferred reading: take its schema and row count from the
/// footer, and hand back a handle that decodes individual columns on demand.
///
/// This is what gives `let t = read_parquet(p)` projection pushdown. Binding
/// touches metadata only; a query that references 4 of 16 columns decodes 4.
/// Merge one column's footer statistics across every row group into a whole-file
/// range. Integers only: `span = max - min + 1` is what the planner derives a
/// distinct-count estimate from, and that arithmetic means nothing for a double
/// or a string.
///
/// Any chunk that cannot answer abandons the column entirely. A range merged
/// from a subset of the row groups is not a conservative answer, it is a wrong
/// one -- the planner would read it as "no value lies outside this".
inline auto merge_column_stats(const parquet::FileMetaData& metadata, int leaf_index)
    -> ibex::runtime::ColumnStats {
    ibex::runtime::ColumnStats out;
    std::size_t nulls = 0;
    bool nulls_known = true;
    bool range_known = true;
    std::int64_t low = 0;
    std::int64_t high = 0;

    for (int group = 0; group < metadata.num_row_groups(); ++group) {
        const auto chunk = metadata.RowGroup(group)->ColumnChunk(leaf_index);
        if (!chunk->is_stats_set()) {
            return {};
        }
        const auto stats = chunk->statistics();
        if (stats == nullptr) {
            return {};
        }
        if (stats->HasNullCount()) {
            nulls += static_cast<std::size_t>(stats->null_count());
        } else {
            nulls_known = false;
        }
        if (!range_known || !stats->HasMinMax()) {
            range_known = false;
            continue;
        }
        std::int64_t chunk_low = 0;
        std::int64_t chunk_high = 0;
        if (stats->physical_type() == parquet::Type::INT64) {
            const auto& typed = static_cast<const parquet::Int64Statistics&>(*stats);
            chunk_low = typed.min();
            chunk_high = typed.max();
        } else if (stats->physical_type() == parquet::Type::INT32) {
            const auto& typed = static_cast<const parquet::Int32Statistics&>(*stats);
            chunk_low = typed.min();
            chunk_high = typed.max();
        } else {
            range_known = false;  // not an integer column
            continue;
        }
        if (group == 0 || chunk_low < low) {
            low = chunk_low;
        }
        if (group == 0 || chunk_high > high) {
            high = chunk_high;
        }
    }

    if (range_known && metadata.num_row_groups() > 0) {
        out.min = low;
        out.max = high;
    }
    if (nulls_known) {
        out.null_count = nulls;
    }
    return out;
}

/// Whole-file per-column statistics, read from the footer that is already in
/// memory. No page is decoded, so this costs a binding nothing.
inline auto read_column_stats(const parquet::FileMetaData& metadata,
                              const arrow::Schema& arrow_schema)
    -> ibex::runtime::SourceColumnStats {
    ibex::runtime::SourceColumnStats out;
    const auto* descr = metadata.schema();
    for (int leaf = 0; leaf < descr->num_columns(); ++leaf) {
        // A leaf's path is the Arrow field name only for flat schemas; nested
        // columns (a struct's fields) share a top-level name and must not be
        // conflated, so take only leaves whose path is a top-level field.
        const std::string name = descr->Column(leaf)->path()->ToDotString();
        if (arrow_schema.GetFieldIndex(name) < 0) {
            continue;
        }
        auto stats = merge_column_stats(metadata, leaf);
        if (stats.min.has_value() || stats.null_count.has_value()) {
            out.emplace(name, stats);
        }
    }
    return out;
}

/// Fused dynamic-filter key scan (dynamic filter pushdown, stage 4): decode
/// the key column batch by batch and evaluate the join-derived filter as
/// values leave the page decoder, emitting passing row indices instead of a
/// materialized column. Row groups whose footer range is disjoint from the
/// build keys' bounds are skipped without touching a page. Rows with a null
/// key fail the filter (the caller proved the scan feeds one inner join, and
/// null keys never match).
///
/// Returns nullopt when the filter stops rejecting: mirroring the runtime's
/// escape hatch, a filter that passes almost everything must not force the
/// gather-decode path, so once enough rows are scanned at a high pass rate
/// the scan gives up and the caller decodes densely.
///
/// The scan's unit of work is the row group, and row groups are independent:
/// each owns its pages and its first row index is a footer fact, so groups can
/// be scanned in any order and concatenated back in file order. That is the
/// axis this scan is parallelized on (`filtered_key_selection`); everything
/// below scans exactly one group.

/// Same policy knobs as the runtime's sampled escape hatch, applied to running
/// totals: decided per row group, so a wrong guess costs at most a couple of
/// groups' key decode.
constexpr double kAbandonPassRate = 0.75;
constexpr std::size_t kAbandonMinRows = 1 << 18;

/// One row group's contribution, appended to `selected` as absolute row
/// indices. False means the column is nested, which has no fused answer at all.
template <typename DType>
inline auto filtered_key_group_scan(parquet::arrow::FileReader& reader, int leaf_index, int group,
                                    std::size_t shard_rows, std::size_t base, std::size_t skip,
                                    const ibex::runtime::DynamicScanFilter& filter,
                                    ibex::runtime::Selection& selected) -> bool {
    using Raw = typename DType::c_type;

    auto column = reader.parquet_reader()->RowGroup(group)->Column(leaf_index);
    if (column->type() != DType::type_num) {
        throw std::runtime_error("read_parquet: physical column type does not match schema");
    }
    const auto* descriptor = column->descr();
    if (descriptor->max_repetition_level() != 0 || descriptor->max_definition_level() > 1) {
        return false;  // nested columns: no fused answer
    }
    const bool optional = descriptor->max_definition_level() != 0;
    auto typed = std::static_pointer_cast<parquet::TypedColumnReader<DType>>(column);
    if (skip > 0 &&
        typed->Skip(static_cast<std::int64_t>(skip)) != static_cast<std::int64_t>(skip)) {
        throw std::runtime_error("read_parquet: key column shard skip ended before its start");
    }

    std::unique_ptr<Raw[]> values(new Raw[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::unique_ptr<std::int16_t[]> definitions(
        new std::int16_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);

    std::size_t row = 0;
    while (row < shard_rows && typed->HasNext()) {
        const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
            static_cast<std::size_t>(kDirectDecodeBatchRows), shard_rows - row));
        std::int64_t values_read = 0;
        const std::int64_t levels_read = typed->ReadBatch(
            request, optional ? definitions.get() : nullptr, nullptr, values.get(), &values_read);
        if (levels_read <= 0) {
            throw std::runtime_error("read_parquet: key column ended before its row group");
        }
        if (!optional || values_read == levels_read) {
            for (std::int64_t i = 0; i < values_read; ++i) {
                if (filter.passes(static_cast<std::int64_t>(values[static_cast<std::size_t>(i)]))) {
                    selected.push_back(base + row + static_cast<std::size_t>(i));
                }
            }
        } else {
            // Nulls present: values are compacted, definition levels map
            // them back to rows. A null key fails the filter.
            std::size_t value_index = 0;
            for (std::int64_t i = 0; i < levels_read; ++i) {
                if (definitions[static_cast<std::size_t>(i)] == 0) {
                    continue;
                }
                if (filter.passes(static_cast<std::int64_t>(values[value_index]))) {
                    selected.push_back(base + row + static_cast<std::size_t>(i));
                }
                ++value_index;
            }
        }
        row += static_cast<std::size_t>(levels_read);
    }
    if (row != shard_rows) {
        throw std::runtime_error("read_parquet: key column ended before its row group");
    }
    return true;
}

/// A row-group SHARD this scan will actually decode: `rows` values starting
/// `skip` rows into physical row group `index`, whose file-global row index is
/// `base` (already offset by `skip`). Groups the footer proves cannot match
/// are never in here. A shard is either a whole row group (`skip == 0`, `rows
/// == the group's row count`) or one piece of one split by `split_scan_groups`
/// below — both shapes run through the same scan code.
struct KeyScanGroup {
    int index;
    std::size_t base;
    std::size_t rows;
    std::size_t skip = 0;
};

/// A consecutive run of data pages.  Its byte range begins at a real page
/// boundary, so no preceding data page is decoded just to reach this stripe.
/// Dictionary bytes, when present, are prepended once per *stripe*, never once
/// per data page.
struct StringPageStripe {
    std::size_t base;
    std::size_t rows;
    std::int64_t data_offset;
    std::int64_t data_size;
    std::int64_t dictionary_offset = 0;
    std::int64_t dictionary_size = 0;
    parquet::Compression::type compression;
    const parquet::ColumnDescriptor* descriptor;
};

/// Subdivide each scan group so at least `target` independent shards exist,
/// giving the fused filter scans a finer parallelism axis than "one task per
/// row group" — the axis alone caps a table with 1-6 row groups (every SF-1
/// PDS-H dimension table has exactly one) at 1-6x regardless of core count.
/// `parquet::ColumnReader::Skip` lets any shard start decoding partway through
/// a physical row group (cheaper than a full decode of the skipped rows, and
/// what makes this correct: each shard gets its own fresh reader/cursor from
/// `RowGroup(index)->Column(leaf)`, so concurrent shards of the same physical
/// group never share mutable decoder state).
///
/// Shards below `kMinScanShardRows` are not worth the extra `Skip()` call, so
/// a small group is left whole; this also makes the function a no-op when
/// `target <= 1`, which callers rely on to skip splitting entirely when the
/// query is not running parallel.
inline auto split_scan_groups(std::vector<KeyScanGroup> groups, std::size_t target)
    -> std::vector<KeyScanGroup> {
    constexpr std::size_t kMinScanShardRows = 1 << 16;  // 65536
    if (target <= 1) {
        return groups;
    }
    // Splitting a row group is not free the way splitting a file is: a shard
    // starts by `Skip`ping to its offset, and Parquet has no intra-row-group
    // seek, so `Skip` DECODES AND DISCARDS. Shard k therefore re-decodes every
    // preceding shard's rows and the group costs rows*(n+1)/2 instead of rows
    // -- 4.5x at eight shards, which is what q17 measured (task-clock 79ms at
    // one decode thread against 367ms at eight, rising ~41ms per thread).
    //
    // So split only when there is nothing else to spread over. With at least
    // `target` groups the file already has that axis and splitting is pure
    // redundant decode: never splitting measured q17 -32.7%, q08 -21.3%,
    // q05 -14.3%, q09 -10.9%, q18 -8.7% (all p=0.000, 14-0 pairs), suite
    // geomean -4.4%, with no query regressing and byte-identical output.
    //
    // Below `target` groups the re-decode is still the only way to reach the
    // cores -- one group over eight shards is 4.5x the work across 8 threads,
    // which is a win over deciding to stay serial -- so the old behaviour is
    // kept exactly there.
    if (groups.size() >= target) {
        return groups;
    }
    std::vector<KeyScanGroup> split;
    split.reserve(groups.size() * target);
    for (const auto& group : groups) {
        const std::size_t shards =
            std::min(target, std::max<std::size_t>(1, group.rows / kMinScanShardRows));
        if (shards <= 1) {
            split.push_back(group);
            continue;
        }
        const std::size_t shard_rows = (group.rows + shards - 1) / shards;
        std::size_t offset = 0;
        while (offset < group.rows) {
            const std::size_t take = std::min(shard_rows, group.rows - offset);
            split.push_back(KeyScanGroup{.index = group.index,
                                         .base = group.base + offset,
                                         .rows = take,
                                         .skip = group.skip + offset});
            offset += take;
        }
    }
    return split;
}

/// Row groups worth decoding, in file order. A group whose stats range is
/// disjoint from the build keys' range cannot contribute a match, so it is
/// dropped here without reading a page.
template <typename DType>
inline auto filtered_key_scan_groups(const parquet::FileMetaData& metadata, int leaf_index,
                                     const ibex::runtime::DynamicScanFilter& filter)
    -> std::vector<KeyScanGroup> {
    std::vector<KeyScanGroup> groups;
    groups.reserve(static_cast<std::size_t>(metadata.num_row_groups()));
    std::size_t base = 0;
    for (int group = 0; group < metadata.num_row_groups(); ++group) {
        const auto group_rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
        bool skip = false;
        if (filter.min.has_value() && filter.max.has_value()) {
            const auto chunk = metadata.RowGroup(group)->ColumnChunk(leaf_index);
            if (chunk->is_stats_set()) {
                const auto stats = chunk->statistics();
                if (stats != nullptr && stats->HasMinMax() &&
                    stats->physical_type() == DType::type_num) {
                    const auto& typed_stats =
                        static_cast<const parquet::TypedStatistics<DType>&>(*stats);
                    const auto group_min = static_cast<std::int64_t>(typed_stats.min());
                    const auto group_max = static_cast<std::int64_t>(typed_stats.max());
                    skip = group_max < *filter.min || group_min > *filter.max;
                }
            }
        }
        if (!skip) {
            groups.push_back({group, base, group_rows});
        }
        base += group_rows;
    }
    return groups;
}

/// The abandon rule itself, over totals accumulated in file order. Both the
/// serial and the parallel driver ask this same question at the same points,
/// so they cannot drift apart.
inline auto key_scan_abandons(std::size_t scanned, std::size_t passing) -> bool {
    return scanned >= kAbandonMinRows &&
           static_cast<double>(passing) > kAbandonPassRate * static_cast<double>(scanned);
}

/// Concatenate the per-group results in file order. Each part is ascending and
/// the groups partition the file in order, so the result is sorted ascending —
/// identical to what a single-threaded scan would have built, not merely
/// equivalent to it.
inline auto merge_key_scan_parts(const std::vector<ibex::runtime::Selection>& parts)
    -> ibex::runtime::Selection {
    std::size_t total = 0;
    for (const auto& part : parts) {
        total += part.size();
    }
    ibex::runtime::Selection selected;
    selected.reserve(total);
    for (const auto& part : parts) {
        selected.insert(selected.end(), part.begin(), part.end());
    }
    return selected;
}

/// Drives the fused key scan over `groups`, one reader per worker.
///
/// Serial when handed one reader. Otherwise workers claim groups from a shared
/// cursor; each writes only its own slot, so the parts need no locking. The
/// one piece of shared state is the abandon check: a worker that completes a
/// group tries to extend the *contiguous* prefix of finished groups and replay
/// the serial rule over it, because that rule only has meaning in file order.
/// Once it fires, the answer is already known to be nullopt, so the remaining
/// groups are abandoned unscanned — the work done varies with the schedule,
/// the answer does not.
///
/// A parallel scan that ends up abandoning therefore decodes more than the
/// serial one would: the check cannot fire until a group finishes, and by then
/// every worker holds a group. The overshoot is bounded by one wave — at most
/// `readers.size()` groups past where the serial scan would have stopped — and
/// it costs CPU rather than latency, because those groups were scanned
/// concurrently with the one that triggered the check. That is the trade for
/// not serializing a leading group on every scan, including the far more
/// common one that never abandons at all.
template <typename DType>
inline auto filtered_key_selection(std::span<parquet::arrow::FileReader* const> readers,
                                   int leaf_index, const ibex::runtime::DynamicScanFilter& filter,
                                   const std::vector<KeyScanGroup>& groups)
    -> std::optional<ibex::runtime::Selection> {
    std::vector<ibex::runtime::Selection> parts(groups.size());

    if (readers.size() <= 1 || groups.size() <= 1) {
        std::size_t scanned = 0;
        std::size_t passing = 0;
        for (std::size_t i = 0; i < groups.size(); ++i) {
            if (!filtered_key_group_scan<DType>(*readers.front(), leaf_index, groups[i].index,
                                                groups[i].rows, groups[i].base, groups[i].skip,
                                                filter, parts[i])) {
                return std::nullopt;
            }
            scanned += groups[i].rows;
            passing += parts[i].size();
            if (key_scan_abandons(scanned, passing)) {
                return std::nullopt;
            }
        }
        return merge_key_scan_parts(parts);
    }

    std::atomic<std::size_t> cursor{0};
    std::atomic<bool> stop{false};
    std::atomic<bool> nested{false};
    std::mutex prefix_mutex;
    std::vector<char> done(groups.size(), 0);
    std::size_t prefix = 0;
    std::size_t scanned = 0;
    std::size_t passing = 0;

    auto batch =
        ibex::runtime::process_worker_pool().submit(readers.size(), [&](std::size_t worker) {
            while (!stop.load(std::memory_order_relaxed)) {
                const std::size_t i = cursor.fetch_add(1, std::memory_order_relaxed);
                if (i >= groups.size()) {
                    return;
                }
                if (!filtered_key_group_scan<DType>(*readers[worker], leaf_index, groups[i].index,
                                                    groups[i].rows, groups[i].base, groups[i].skip,
                                                    filter, parts[i])) {
                    nested.store(true, std::memory_order_relaxed);
                    stop.store(true, std::memory_order_relaxed);
                    return;
                }
                std::lock_guard const lock(prefix_mutex);
                done[i] = 1;
                while (prefix < groups.size() && done[prefix] != 0) {
                    scanned += groups[prefix].rows;
                    passing += parts[prefix].size();
                    ++prefix;
                    if (key_scan_abandons(scanned, passing)) {
                        stop.store(true, std::memory_order_relaxed);
                        return;
                    }
                }
            }
        });
    batch.wait();

    if (nested.load(std::memory_order_relaxed) || stop.load(std::memory_order_relaxed)) {
        return std::nullopt;
    }
    return merge_key_scan_parts(parts);
}

/// ── Fused string filter scan ────────────────────────────────────────────────
///
/// The same trade as the key scan, for the column a query references only from
/// its filter: match the pattern against the bytes the page decoder hands back
/// and emit row indices, so no `Column<std::string>` is ever built. On TPC-H's
/// `o_comment` that is 79MB of characters copied and 1.5m offsets written, for
/// an answer that is one bit per row.
///
/// It is also what makes a string column splittable at all. Strings are
/// excluded from the row-group decode split because a shard's destination
/// offset depends on the total length of every preceding row; a predicate
/// result has no offsets, so the groups are independent and this scan is
/// parallel by row group like the key scan is.
///
/// Two differences from the key scan, both because this predicate is the
/// query's own rather than a speculative join filter: every row group is
/// scanned (footer statistics prune nothing here, and the answer is needed for
/// every row), and there is no abandon rule — a filter that passes almost
/// everything still has to be evaluated.

/// One row-group shard's contribution, appended as absolute row indices
/// (`skip` rows into the physical group, same convention as the key scan's
/// `KeyScanGroup` — see `split_scan_groups`). False means the column is
/// nested, which has no fused answer at all.
inline auto filtered_string_group_scan(parquet::arrow::FileReader& reader, int leaf_index,
                                       int group, std::size_t shard_rows, std::size_t base,
                                       std::size_t skip,
                                       const ibex::runtime::StringScanFilter& filter,
                                       ibex::runtime::Selection& selected) -> bool {
    auto column = reader.parquet_reader()->RowGroup(group)->Column(leaf_index);
    if (column->type() != parquet::ByteArrayType::type_num) {
        throw std::runtime_error("read_parquet: physical column type does not match schema");
    }
    const auto* descriptor = column->descr();
    if (descriptor->max_repetition_level() != 0 || descriptor->max_definition_level() > 1) {
        return false;  // nested columns: no fused answer
    }
    const bool optional = descriptor->max_definition_level() != 0;
    auto typed = std::static_pointer_cast<parquet::ByteArrayReader>(column);
    if (skip > 0 &&
        typed->Skip(static_cast<std::int64_t>(skip)) != static_cast<std::int64_t>(skip)) {
        throw std::runtime_error("read_parquet: string column shard skip ended before its start");
    }

    std::unique_ptr<parquet::ByteArray[]> values(
        new parquet::ByteArray[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::unique_ptr<std::int16_t[]> definitions(
        new std::int16_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);

    // The ByteArray points into the page buffer, which the next ReadBatch
    // reuses — matching within the batch is exactly why nothing needs copying.
    const auto view = [](const parquet::ByteArray& value) {
        return std::string_view{
            reinterpret_cast<const char*>(value.ptr),  // NOLINT(*-reinterpret-cast)
            value.len};
    };

    std::size_t row = 0;
    while (row < shard_rows && typed->HasNext()) {
        const auto request = static_cast<std::int64_t>(std::min<std::size_t>(
            static_cast<std::size_t>(kDirectDecodeBatchRows), shard_rows - row));
        std::int64_t values_read = 0;
        const std::int64_t levels_read = typed->ReadBatch(
            request, optional ? definitions.get() : nullptr, nullptr, values.get(), &values_read);
        if (levels_read <= 0) {
            throw std::runtime_error("read_parquet: string column ended before its row group");
        }
        if (!optional || values_read == levels_read) {
            for (std::int64_t i = 0; i < values_read; ++i) {
                if (filter.passes(view(values[static_cast<std::size_t>(i)]))) {
                    selected.push_back(base + row + static_cast<std::size_t>(i));
                }
            }
        } else {
            // Nulls present: values are compacted, definition levels map them
            // back to rows. `like(null, p)` is null and a filter keeps only
            // true, so a null row fails whichever way the predicate is signed.
            std::size_t value_index = 0;
            for (std::int64_t i = 0; i < levels_read; ++i) {
                if (definitions[static_cast<std::size_t>(i)] == 0) {
                    continue;
                }
                if (filter.passes(view(values[value_index]))) {
                    selected.push_back(base + row + static_cast<std::size_t>(i));
                }
                ++value_index;
            }
        }
        row += static_cast<std::size_t>(levels_read);
    }
    if (row != shard_rows) {
        throw std::runtime_error("read_parquet: string column ended before its row group");
    }
    return true;
}

inline auto string_page_stripes(parquet::arrow::FileReader& reader, int leaf_index,
                                const std::vector<KeyScanGroup>& groups, std::size_t target)
    -> std::optional<std::vector<StringPageStripe>> {
    auto* raw = reader.parquet_reader();
    auto indices = raw->GetPageIndexReader();
    if (indices == nullptr)
        return std::nullopt;
    struct GroupPages {
        const KeyScanGroup* group;
        std::vector<parquet::PageLocation> pages;
        const parquet::ColumnDescriptor* descriptor;
        parquet::Compression::type compression;
        std::int64_t dict_offset;
        std::int64_t dict_size;
    };
    std::vector<GroupPages> all;
    std::size_t total_pages = 0;
    for (const auto& group : groups) {
        auto index = indices->RowGroup(group.index);
        auto offset = index == nullptr ? nullptr : index->GetOffsetIndex(leaf_index);
        auto chunk = raw->metadata()->RowGroup(group.index)->ColumnChunk(leaf_index);
        auto column = raw->RowGroup(group.index)->Column(leaf_index);
        if (offset == nullptr || offset->page_locations().empty())
            return std::nullopt;
        auto pages = offset->page_locations();
        const std::int64_t dict_offset =
            chunk->has_dictionary_page() ? chunk->dictionary_page_offset() : 0;
        const std::int64_t dict_size =
            chunk->has_dictionary_page() ? pages.front().offset - dict_offset : 0;
        if (dict_offset < 0 || dict_size < 0)
            return std::nullopt;
        total_pages += pages.size();
        all.push_back({&group, std::move(pages), column->descr(), chunk->compression(), dict_offset,
                       dict_size});
    }
    if (total_pages <= groups.size())
        return std::nullopt;
    std::vector<StringPageStripe> out;
    for (const auto& entry : all) {
        const auto& pages = entry.pages;
        const std::size_t stripes = std::min(
            pages.size(),
            std::max<std::size_t>(1, (pages.size() * target + total_pages - 1) / total_pages));
        const std::size_t pages_per = (pages.size() + stripes - 1) / stripes;
        for (std::size_t first = 0; first < pages.size(); first += pages_per) {
            const std::size_t last = std::min(pages.size(), first + pages_per);
            const auto first_row = pages[first].first_row_index;
            const auto next_row = last < pages.size()
                                      ? pages[last].first_row_index
                                      : static_cast<std::int64_t>(entry.group->rows);
            const auto end = pages[last - 1].offset + pages[last - 1].compressed_page_size;
            if (first_row < 0 || next_row <= first_row ||
                next_row > static_cast<std::int64_t>(entry.group->rows) ||
                end <= pages[first].offset)
                return std::nullopt;
            out.push_back({.base = entry.group->base + static_cast<std::size_t>(first_row),
                           .rows = static_cast<std::size_t>(next_row - first_row),
                           .data_offset = pages[first].offset,
                           .data_size = end - pages[first].offset,
                           .dictionary_offset = entry.dict_offset,
                           .dictionary_size = entry.dict_size,
                           .compression = entry.compression,
                           .descriptor = entry.descriptor});
        }
    }
    return out;
}

inline auto filtered_string_page_stripe(const StringPageStripe& task,
                                        const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                                        const ibex::runtime::StringScanFilter& filter,
                                        ibex::runtime::Selection& selected) -> bool {
    std::shared_ptr<arrow::io::InputStream> stream;
    if (task.dictionary_size == 0) {
        auto result =
            arrow::io::RandomAccessFile::GetStream(input, task.data_offset, task.data_size);
        if (!result.ok())
            throw std::runtime_error("read_parquet: failed to open string page stripe (" +
                                     result.status().ToString() + ")");
        stream = std::move(*result);
    } else {
        auto dictionary = input->ReadAt(task.dictionary_offset, task.dictionary_size);
        auto data = input->ReadAt(task.data_offset, task.data_size);
        if (!dictionary.ok() || !data.ok())
            throw std::runtime_error("read_parquet: failed to read string page stripe");
        arrow::BufferBuilder joined;
        if (auto status = joined.Append((*dictionary)->data(), (*dictionary)->size()); !status.ok())
            throw std::runtime_error(status.ToString());
        if (auto status = joined.Append((*data)->data(), (*data)->size()); !status.ok())
            throw std::runtime_error(status.ToString());
        std::shared_ptr<arrow::Buffer> buffer;
        if (auto status = joined.Finish(&buffer); !status.ok())
            throw std::runtime_error(status.ToString());
        stream = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    }
    auto pages = parquet::PageReader::Open(std::move(stream), static_cast<std::int64_t>(task.rows),
                                           task.compression);
    auto column = std::static_pointer_cast<parquet::ByteArrayReader>(
        parquet::ColumnReader::Make(task.descriptor, std::move(pages)));
    const bool optional = task.descriptor->max_definition_level() != 0;
    std::unique_ptr<parquet::ByteArray[]> values(
        new parquet::ByteArray[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::unique_ptr<std::int16_t[]> definitions(
        new std::int16_t[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
    std::size_t row = 0;
    while (row < task.rows && column->HasNext()) {
        const auto request = static_cast<std::int64_t>(
            std::min<std::size_t>(kDirectDecodeBatchRows, task.rows - row));
        std::int64_t read = 0;
        const auto levels = column->ReadBatch(request, optional ? definitions.get() : nullptr,
                                              nullptr, values.get(), &read);
        if (levels <= 0)
            throw std::runtime_error("read_parquet: string page stripe made no progress");
        std::size_t value = 0;
        for (std::int64_t i = 0; i < levels; ++i) {
            if (!optional || definitions[static_cast<std::size_t>(i)] != 0) {
                const auto& v = values[value++];
                if (filter.passes(std::string_view{reinterpret_cast<const char*>(v.ptr), v.len}))
                    selected.push_back(task.base + row + static_cast<std::size_t>(i));
            }
        }
        row += static_cast<std::size_t>(levels);
    }
    if (row != task.rows)
        throw std::runtime_error("read_parquet: string page stripe ended early");
    return true;
}

inline auto filtered_string_page_stripes(const std::shared_ptr<arrow::io::RandomAccessFile>& input,
                                         const ibex::runtime::StringScanFilter& filter,
                                         const std::vector<StringPageStripe>& tasks,
                                         std::size_t workers)
    -> std::optional<ibex::runtime::Selection> {
    std::vector<ibex::runtime::Selection> parts(tasks.size());
    std::atomic<std::size_t> cursor{0};
    auto run = [&](std::size_t) {
        for (;;) {
            const auto i = cursor.fetch_add(1, std::memory_order_relaxed);
            if (i >= tasks.size())
                return;
            filtered_string_page_stripe(tasks[i], input, filter, parts[i]);
        }
    };
    if (workers <= 1)
        run(0);
    else
        ibex::runtime::process_worker_pool().submit(workers, run).wait();
    return merge_key_scan_parts(parts);
}

/// Every row group, in file order, with the file row index each starts at.
inline auto whole_file_scan_groups(const parquet::FileMetaData& metadata)
    -> std::vector<KeyScanGroup> {
    std::vector<KeyScanGroup> groups;
    groups.reserve(static_cast<std::size_t>(metadata.num_row_groups()));
    std::size_t base = 0;
    for (int group = 0; group < metadata.num_row_groups(); ++group) {
        const auto rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
        groups.push_back(KeyScanGroup{.index = group, .base = base, .rows = rows});
        base += rows;
    }
    return groups;
}

/// Drives the fused string scan over `groups`, one reader per worker. Workers
/// claim groups from a shared cursor and write only their own slot, so the
/// parts need no locking, and concatenating them in file order reproduces the
/// serial scan's answer exactly rather than merely equivalently.
inline auto filtered_string_selection(std::span<parquet::arrow::FileReader* const> readers,
                                      int leaf_index, const ibex::runtime::StringScanFilter& filter,
                                      const std::vector<KeyScanGroup>& groups)
    -> std::optional<ibex::runtime::Selection> {
    std::vector<ibex::runtime::Selection> parts(groups.size());

    if (readers.size() <= 1 || groups.size() <= 1) {
        for (std::size_t i = 0; i < groups.size(); ++i) {
            if (!filtered_string_group_scan(*readers.front(), leaf_index, groups[i].index,
                                            groups[i].rows, groups[i].base, groups[i].skip, filter,
                                            parts[i])) {
                return std::nullopt;
            }
        }
        return merge_key_scan_parts(parts);
    }

    std::atomic<std::size_t> cursor{0};
    std::atomic<bool> nested{false};

    auto batch =
        ibex::runtime::process_worker_pool().submit(readers.size(), [&](std::size_t worker) {
            while (!nested.load(std::memory_order_relaxed)) {
                const std::size_t i = cursor.fetch_add(1, std::memory_order_relaxed);
                if (i >= groups.size()) {
                    return;
                }
                if (!filtered_string_group_scan(*readers[worker], leaf_index, groups[i].index,
                                                groups[i].rows, groups[i].base, groups[i].skip,
                                                filter, parts[i])) {
                    nested.store(true, std::memory_order_relaxed);
                    return;
                }
            }
        });
    batch.wait();

    if (nested.load(std::memory_order_relaxed)) {
        return std::nullopt;
    }
    return merge_key_scan_parts(parts);
}

/// Immutable reader construction inputs plus the bind-time reader. The first
/// consumer takes that already-open reader; later (including concurrent)
/// consumers build independent readers from the shared, already-parsed footer.
class ParquetLazyReaderFactoryState {
   public:
    ParquetLazyReaderFactoryState(std::shared_ptr<arrow::io::RandomAccessFile> input,
                                  std::shared_ptr<parquet::FileMetaData> metadata,
                                  std::unique_ptr<parquet::arrow::FileReader> first_reader,
                                  std::string path)
        : input_(std::move(input)),
          metadata_(std::move(metadata)),
          first_reader_(std::move(first_reader)),
          path_(std::move(path)) {
        // Decided here rather than in `make_reader`, because deciding it needs
        // a file reader (the dictionary entry counts come from the pages
        // themselves) and `first_reader_` is moved out on the first call. One
        // probe per file, reused by every reader the factory hands out -- which
        // also keeps the choice identical across them, as it must be: two
        // readers disagreeing would give one column two representations.
        if (first_reader_ != nullptr) {
            dictionary_columns_ = dictionary_column_indices(*first_reader_->parquet_reader());
        }
    }

    auto make_reader() -> std::unique_ptr<parquet::arrow::FileReader> {
        {
            std::lock_guard lock(first_reader_mutex_);
            if (first_reader_ != nullptr) {
                return std::move(first_reader_);
            }
        }
        return make_parquet_reader(input_, path_, metadata_, dictionary_columns_);
    }

    [[nodiscard]] auto input() const -> const std::shared_ptr<arrow::io::RandomAccessFile>& {
        return input_;
    }

   private:
    std::shared_ptr<arrow::io::RandomAccessFile> input_;
    std::shared_ptr<parquet::FileMetaData> metadata_;
    std::vector<int> dictionary_columns_;
    std::unique_ptr<parquet::arrow::FileReader> first_reader_;
    std::string path_;
    std::mutex first_reader_mutex_;
};

/// One independent Parquet decoder product. The Arrow schema and name index are
/// immutable and shared by every product; the FileReader is deliberately not.
/// Its page readers and decode cursors are mutable, so sharing it would serialize
/// or race once the runtime assigns source work to multiple workers.
class ParquetLazySourceReader final : public ibex::runtime::LazySourceReader {
   public:
    ParquetLazySourceReader(std::unique_ptr<parquet::arrow::FileReader> reader,
                            std::shared_ptr<ParquetLazyReaderFactoryState> factory,
                            std::shared_ptr<const std::map<std::string, int>> indices,
                            std::shared_ptr<arrow::Schema> schema, std::size_t rows,
                            std::string path)
        : reader_(std::move(reader)),
          factory_(std::move(factory)),
          indices_(std::move(indices)),
          schema_(std::move(schema)),
          rows_(rows),
          path_(std::move(path)) {}

    /// One unit per row group: the row group is the file's own decode
    /// boundary, so a unit is exactly what the decoder can already read
    /// without touching anything else, and no unit ever straddles a
    /// dictionary.
    auto decode_units() -> std::vector<ibex::runtime::SourceUnit> override {
        const auto& metadata = *reader_->parquet_reader()->metadata();
        std::vector<ibex::runtime::SourceUnit> units;
        units.reserve(static_cast<std::size_t>(metadata.num_row_groups()));
        std::size_t start = 0;
        for (int group = 0; group < metadata.num_row_groups(); ++group) {
            const auto rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
            units.push_back(ibex::runtime::SourceUnit{.start = start, .rows = rows});
            start += rows;
        }
        return units;
    }

    auto decode(const std::vector<std::string>& names, const ibex::runtime::Selection* selection,
                const ibex::runtime::SourceUnit* unit, const ibex::runtime::ExecutionContext& exec)
        -> std::expected<ibex::runtime::Table, std::string> override {
        std::vector<int> column_indices;
        column_indices.reserve(names.size());
        for (const auto& name : names) {
            auto it = indices_->find(name);
            if (it == indices_->end()) {
                return std::unexpected("read_parquet: no column '" + name + "' in " + path_);
            }
            column_indices.push_back(it->second);
        }

        try {
            const auto& metadata = *reader_->parquet_reader()->metadata();
            const auto groups =
                unit == nullptr ? all_decode_groups(metadata) : unit_decode_groups(metadata, *unit);
            // Row groups multiply the units: a single-column decode still has
            // one task per row group to hand out (see direct_decode_table).
            // A unit IS one row group, so there the column count is all of it.
            const auto tasks = column_indices.size() *
                               static_cast<std::size_t>(std::max(1, groups.end - groups.begin));
            auto readers = parallel_readers(tasks, exec);
            return direct_decode_table(std::span{readers}, *schema_, column_indices, selection,
                                       rows_, groups);
        } catch (const std::exception& e) {
            return std::unexpected("read_parquet: failed to read columns from " + path_ + " (" +
                                   e.what() + ")");
        }
    }

    auto key_filter_scan(const std::string& key, const ibex::runtime::DynamicScanFilter& filter,
                         const ibex::runtime::SourceUnit* unit,
                         const ibex::runtime::ExecutionContext& exec)
        -> std::expected<std::optional<ibex::runtime::Selection>, std::string> override {
        auto it = indices_->find(key);
        if (it == indices_->end()) {
            return std::unexpected("read_parquet: no column '" + key + "' in " + path_);
        }
        // Only types whose ordinary decode is the identity/sign-extension
        // into int64 — the fused filter must see exactly the values the
        // materialized column would hold. (Zero-extended unsigned widths
        // would need their own conversion; join keys are never those.)
        const auto id = schema_->field(it->second)->type()->id();
        const bool fusable = id == arrow::Type::INT8 || id == arrow::Type::INT16 ||
                             id == arrow::Type::INT32 || id == arrow::Type::INT64 ||
                             id == arrow::Type::UINT64;
        const auto& manifest = reader_->manifest();
        if (!fusable || it->second >= static_cast<int>(manifest.schema_fields.size()) ||
            !manifest.schema_fields[static_cast<std::size_t>(it->second)].is_leaf()) {
            return std::optional<ibex::runtime::Selection>{};
        }
        const int leaf_index =
            manifest.schema_fields[static_cast<std::size_t>(it->second)].column_index;
        try {
            const auto physical = reader_->parquet_reader()
                                      ->metadata()
                                      ->schema()
                                      ->Column(leaf_index)
                                      ->physical_type();
            const auto& metadata = *reader_->parquet_reader()->metadata();
            const auto target = scan_shard_target(unit, exec);
            if (physical == parquet::Type::INT64) {
                const auto groups = restrict_to_unit(
                    split_scan_groups(
                        filtered_key_scan_groups<parquet::Int64Type>(metadata, leaf_index, filter),
                        target),
                    unit);
                auto readers = parallel_readers(groups.size(), exec);
                return filtered_key_selection<parquet::Int64Type>(std::span{readers}, leaf_index,
                                                                  filter, groups);
            }
            if (physical == parquet::Type::INT32) {
                const auto groups = restrict_to_unit(
                    split_scan_groups(
                        filtered_key_scan_groups<parquet::Int32Type>(metadata, leaf_index, filter),
                        target),
                    unit);
                auto readers = parallel_readers(groups.size(), exec);
                return filtered_key_selection<parquet::Int32Type>(std::span{readers}, leaf_index,
                                                                  filter, groups);
            }
            return std::optional<ibex::runtime::Selection>{};
        } catch (const std::exception& e) {
            return std::unexpected("read_parquet: fused key filter scan failed on " + path_ + " (" +
                                   e.what() + ")");
        }
    }

    auto string_filter_scan(const std::string& column,
                            const ibex::runtime::StringScanFilter& filter,
                            const ibex::runtime::SourceUnit* unit,
                            const ibex::runtime::ExecutionContext& exec)
        -> std::expected<std::optional<ibex::runtime::Selection>, std::string> override {
        auto it = indices_->find(column);
        if (it == indices_->end()) {
            return std::unexpected("read_parquet: no column '" + column + "' in " + path_);
        }
        // Plain strings only. A dictionary column arrives as DICTIONARY here
        // and decodes to codes plus one small dictionary, which is cheap to
        // materialize and would be *slower* matched value by value.
        const auto id = schema_->field(it->second)->type()->id();
        const auto& manifest = reader_->manifest();
        if ((id != arrow::Type::STRING && id != arrow::Type::LARGE_STRING) ||
            it->second >= static_cast<int>(manifest.schema_fields.size()) ||
            !manifest.schema_fields[static_cast<std::size_t>(it->second)].is_leaf()) {
            return std::optional<ibex::runtime::Selection>{};
        }
        const int leaf_index =
            manifest.schema_fields[static_cast<std::size_t>(it->second)].column_index;
        try {
            const auto& metadata = *reader_->parquet_reader()->metadata();
            if (metadata.schema()->Column(leaf_index)->physical_type() !=
                parquet::Type::BYTE_ARRAY) {
                return std::optional<ibex::runtime::Selection>{};
            }
            const auto groups = restrict_to_unit(whole_file_scan_groups(metadata), unit);
            const auto page_target = page_stripe_target(unit, exec);
            if (page_target > 1) {
                if (auto stripes = string_page_stripes(*reader_, leaf_index, groups, page_target);
                    stripes.has_value() && stripes->size() > groups.size()) {
                    return filtered_string_page_stripes(factory_->input(), filter, *stripes,
                                                        std::min(page_target, stripes->size()));
                }
            }
            const auto target = scan_shard_target(unit, exec);
            const auto fallback = split_scan_groups(groups, target);
            auto readers = parallel_readers(fallback.size(), exec);
            return filtered_string_selection(std::span{readers}, leaf_index, filter, fallback);
        } catch (const std::exception& e) {
            return std::unexpected("read_parquet: fused string filter scan failed on " + path_ +
                                   " (" + e.what() + ")");
        }
    }

   private:
    /// The row-group range covering exactly `unit`. `decode_units` builds units
    /// from row-group boundaries, so this always lands on one; a unit that did
    /// not is a caller mixing units from a different source, which is a bug
    /// rather than something to round.
    static auto unit_decode_groups(const parquet::FileMetaData& metadata,
                                   const ibex::runtime::SourceUnit& unit) -> DirectDecodeGroups {
        std::size_t start = 0;
        for (int group = 0; group < metadata.num_row_groups(); ++group) {
            const auto rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
            if (start == unit.start && rows == unit.rows) {
                return DirectDecodeGroups{
                    .begin = group, .end = group + 1, .source_start = start, .rows = rows};
            }
            start += rows;
        }
        throw std::runtime_error("read_parquet: decode unit does not match a row group");
    }

    /// The scan groups lying inside `unit`. Both fused scans plan over the
    /// whole file and index their answers file-globally, so restricting them
    /// to a unit is a filter on the group list and nothing else — which is why
    /// streaming keeps the fused scans instead of declining them.
    static auto restrict_to_unit(std::vector<KeyScanGroup> groups,
                                 const ibex::runtime::SourceUnit* unit)
        -> std::vector<KeyScanGroup> {
        if (unit == nullptr) {
            return groups;
        }
        const std::size_t end = unit->start + unit->rows;
        std::erase_if(groups, [&](const KeyScanGroup& group) {
            return group.base < unit->start || group.base >= end;
        });
        return groups;
    }

    /// How many shards `split_scan_groups` should aim for before the fused
    /// scans plan their row groups. Mirrors `parallel_readers`' own gate
    /// exactly (same fields, same thresholds) because splitting only pays when
    /// that call is actually going to hand out more than one reader — a `unit`
    /// is already exactly one row group scheduled by a level above (splitting
    /// it further would just add `Skip()` calls with no reader to run them
    /// concurrently), and the on-worker-pool-thread / row-count / factory
    /// checks are the same "would this run serial anyway" question
    /// `parallel_readers` asks. Returning 1 makes `split_scan_groups` a no-op.
    [[nodiscard]] auto scan_shard_target(const ibex::runtime::SourceUnit* unit,
                                         const ibex::runtime::ExecutionContext& exec) const
        -> std::size_t {
        if (unit != nullptr || factory_ == nullptr || !exec.can_fan_out() ||
            rows_ < std::max(exec.parallel_min_rows, kParallelDecodeMinRows) ||
            ibex::runtime::on_worker_pool_thread()) {
            return 1;
        }
        const auto& pool = ibex::runtime::process_worker_pool();
        return std::min(exec.compute_budget(), pool.size());
    }

    /// The page-index stripe path has independent compressed byte ranges, so
    /// unlike Arrow FileReader shards it is safe to submit below a streamed
    /// row-group worker. The pool's cooperative nested wait guarantees that
    /// those parent workers execute the child tasks rather than deadlocking
    /// while the fixed pool is saturated. A single process-wide pool still
    /// bounds actual concurrency to its size; each source unit merely offers
    /// enough stripes for idle capacity to find useful work.
    [[nodiscard]] auto page_stripe_target(const ibex::runtime::SourceUnit* unit,
                                          const ibex::runtime::ExecutionContext& exec) const
        -> std::size_t {
        const std::size_t work_rows = unit == nullptr ? rows_ : unit->rows;
        if (factory_ == nullptr || !exec.can_fan_out() ||
            work_rows < std::max(exec.parallel_min_rows, kParallelDecodeMinRows)) {
            return 1;
        }
        const auto& pool = ibex::runtime::process_worker_pool();
        return std::min(exec.compute_budget(), pool.size());
    }

    /// Readers to spread `units` independent pieces of decode work over: this
    /// product's own reader first, then as many extra ones as the thread budget
    /// justifies. A unit is a column for a whole-table decode and a row group
    /// for the fused key scan; what they have in common is that Arrow's
    /// FileReader carries mutable page readers and cursors, so concurrent units
    /// need one reader each.
    ///
    /// Extra readers are built once and kept, because a lazy source is decoded
    /// repeatedly (a query re-scans it per statement) and re-parsing is pure
    /// waste — the footer is already shared, so an extra reader is just page
    /// readers and cursors.
    ///
    /// Serial when the query is not parallel or there is only one unit.
    ///
    /// A call already on a pool thread (a nested decode — a dimension table
    /// scanned from inside a pipeline worker) used to bail to serial: `submit`
    /// from a worker could strand its tasks against a saturated pool. The
    /// cooperative ring waits (`plans/cooperative-pipeline-waits-plan.md`) fixed
    /// that — a parked worker now runs queued tasks — so a nested decode fans
    /// out too, bounded by the workers actually free (`pool.size() - busy`).
    /// This is the lever for the small dimension tables (customer/part, 1-2 row
    /// groups) whose 5-7 columns otherwise decode on one worker while six idle.
    ///
    /// The settings come from the query's `ExecutionContext`, never from the
    /// environment — a decoder that read `IBEX_PARALLEL` itself would be a
    /// second authority on whether the query is parallel, free to disagree
    /// with the one the rest of the engine obeys.
    auto parallel_readers(std::size_t units, const ibex::runtime::ExecutionContext& exec)
        -> std::vector<parquet::arrow::FileReader*> {
        auto& pool = ibex::runtime::process_worker_pool();
        std::size_t want = 1;
        const bool nested = ibex::runtime::on_worker_pool_thread();
        if (factory_ != nullptr && units > 1 && exec.can_fan_out() &&
            rows_ >= std::max(exec.parallel_min_rows, kParallelDecodeMinRows)) {
            std::size_t budget = exec.compute_budget();
            // A nested decode runs under a pipeline worker; cap its extra readers
            // so a full-width fan-out from every concurrent unit cannot badly
            // oversubscribe the pool. The cooperative ring waits make it safe;
            // this keeps it cheap (no per-task bookkeeping to size it exactly).
            if (nested) {
                budget = std::min<std::size_t>(budget, kNestedDecodeFanout);
            }
            want = std::min({units, budget, pool.size()});
        }

        while (extra_readers_.size() + 1 < want) {
            extra_readers_.push_back(factory_->make_reader());
        }

        std::vector<parquet::arrow::FileReader*> readers;
        readers.reserve(want);
        readers.push_back(reader_.get());
        for (std::size_t i = 0; i + 1 < want; ++i) {
            readers.push_back(extra_readers_[i].get());
        }
        return readers;
    }

    /// Below this the decode is short enough that spinning up the pool and
    /// building extra readers costs more than it saves.
    static constexpr std::size_t kParallelDecodeMinRows = 65536;

    /// Extra readers a decode already on a pool worker may fan out to.
    static constexpr std::size_t kNestedDecodeFanout = 4;

    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::vector<std::unique_ptr<parquet::arrow::FileReader>> extra_readers_;
    std::shared_ptr<ParquetLazyReaderFactoryState> factory_;
    std::shared_ptr<const std::map<std::string, int>> indices_;
    std::shared_ptr<arrow::Schema> schema_;
    std::size_t rows_;
    std::string path_;
};

inline auto read_parquet_lazy(std::string_view path) -> ibex::runtime::LazyTablePtr {
    std::string path_string{path};
    auto input = open_parquet_input(path);

    // This reader exists only long enough to bind immutable metadata. Decode
    // work receives fresh readers from the factory below.
    auto metadata_reader = make_parquet_reader(input, path_string);

    std::shared_ptr<arrow::Schema> arrow_schema;
    auto st = metadata_reader->GetSchema(&arrow_schema);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to read schema: " + path_string + " (" +
                                 st.ToString() + ")");
    }

    const auto rows =
        static_cast<std::size_t>(metadata_reader->parquet_reader()->metadata()->num_rows());
    auto parquet_metadata = metadata_reader->parquet_reader()->metadata();

    // Column name -> field index, so a demand expressed in names can be turned
    // into the indices Arrow's selective read wants.
    auto indices = std::make_shared<const std::map<std::string, int>>([&arrow_schema] {
        std::map<std::string, int> result;
        for (int i = 0; i < arrow_schema->num_fields(); ++i) {
            result.emplace(arrow_schema->field(i)->name(), i);
        }
        return result;
    }());

    auto factory_state = std::make_shared<ParquetLazyReaderFactoryState>(
        std::move(input), parquet_metadata, std::move(metadata_reader), path_string);
    ibex::runtime::LazySourceReaderFactory reader_factory =
        [factory_state, indices, arrow_schema, rows,
         path_string]() -> std::expected<ibex::runtime::LazySourceReaderPtr, std::string> {
        try {
            auto reader = factory_state->make_reader();
            return std::make_unique<ParquetLazySourceReader>(
                std::move(reader), factory_state, indices, arrow_schema, rows, path_string);
        } catch (const std::exception& e) {
            return std::unexpected("read_parquet: failed to create reader for " + path_string +
                                   " (" + e.what() + ")");
        }
    };

    return std::make_shared<ibex::runtime::LazyTable>(
        schema_table_from_arrow(*arrow_schema), rows, std::move(reader_factory),
        read_column_stats(*parquet_metadata, *arrow_schema));
}

inline auto read_parquet(std::string_view path) -> ibex::runtime::Table {
    std::string path_string{path};
    auto input = open_parquet_input(path);

    auto reader = make_parquet_reader(std::move(input), path_string);

    std::shared_ptr<arrow::Schema> schema;
    auto st = reader->GetSchema(&schema);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to read schema: " + path_string + " (" +
                                 st.ToString() + ")");
    }
    std::vector<int> fields(static_cast<std::size_t>(schema->num_fields()));
    for (int i = 0; i < schema->num_fields(); ++i) {
        fields[static_cast<std::size_t>(i)] = i;
    }
    const auto rows = static_cast<std::size_t>(reader->parquet_reader()->metadata()->num_rows());
    try {
        return direct_decode_table(*reader, *schema, fields, nullptr, rows);
    } catch (const std::exception& e) {
        throw std::runtime_error("read_parquet: failed to load table: " + path_string + " (" +
                                 e.what() + ")");
    }
}

/// Row-group streaming source for `read_parquet`, registered via
/// `ExternRegistry::register_chunked_table` alongside the whole-file
/// `read_parquet()` above. Each row group is decoded directly into an Ibex
/// chunk, without an intermediate Arrow RecordBatch or Table. Categorical
/// chunks are remapped onto shared dictionaries, preserving the streaming
/// operator contract across row-group dictionary boundaries.
class ChunkedParquetSourceOperator final : public ibex::runtime::Operator {
   public:
    static auto create(std::string path) -> std::expected<ibex::runtime::OperatorPtr, std::string> {
        try {
            auto op =
                std::unique_ptr<ChunkedParquetSourceOperator>(new ChunkedParquetSourceOperator());
            op->init(std::move(path));
            return ibex::runtime::OperatorPtr(std::move(op));
        } catch (const std::exception& e) {
            return std::unexpected(std::string(e.what()));
        }
    }

    ChunkedParquetSourceOperator(const ChunkedParquetSourceOperator&) = delete;
    ChunkedParquetSourceOperator& operator=(const ChunkedParquetSourceOperator&) = delete;
    ChunkedParquetSourceOperator(ChunkedParquetSourceOperator&&) noexcept = delete;
    ChunkedParquetSourceOperator& operator=(ChunkedParquetSourceOperator&&) noexcept = delete;
    ~ChunkedParquetSourceOperator() override = default;

    [[nodiscard]] auto next()
        -> std::expected<std::optional<ibex::runtime::Chunk>, std::string> override {
        if (next_group_ >= group_count_) {
            return std::optional<ibex::runtime::Chunk>{};
        }

        try {
            const int group = next_group_++;
            const auto group_rows = static_cast<std::size_t>(
                reader_->parquet_reader()->metadata()->RowGroup(group)->num_rows());
            const DirectDecodeGroups groups{.begin = group,
                                            .end = group + 1,
                                            .source_start = next_source_row_,
                                            .rows = group_rows};
            next_source_row_ += group_rows;
            auto table = direct_decode_table(*reader_, *schema_, field_indices_, nullptr,
                                             source_rows_, groups);

            for (std::size_t i = 0; i < table.columns.size(); ++i) {
                auto* local =
                    std::get_if<ibex::Column<ibex::Categorical>>(table.columns[i].column.get());
                if (local == nullptr) {
                    continue;
                }
                auto& state = *categorical_states_[i];
                ibex::Column<ibex::Categorical> remapped{
                    state.dictionary_ptr(), state.index_ptr(), {}};
                remapped.reserve(local->size());
                for (std::size_t row = 0; row < local->size(); ++row) {
                    remapped.push_back((*local)[row]);
                }
                table.columns[i].column =
                    std::make_shared<ibex::runtime::ColumnValue>(std::move(remapped));
            }

            ibex::runtime::Chunk chunk;
            chunk.columns = std::move(table.columns);
            if (chunk.columns.empty()) {
                chunk.logical_rows = table.logical_rows;
            }
            return std::optional<ibex::runtime::Chunk>{std::move(chunk)};
        } catch (const std::exception& e) {
            return std::unexpected("read_parquet: failed to read row group from " + path_ + " (" +
                                   e.what() + ")");
        }
    }

   private:
    ChunkedParquetSourceOperator() = default;

    void init(std::string path) {
        path_ = std::move(path);
        auto input = open_parquet_input(path_);

        reader_ = make_parquet_reader(std::move(input), path_);
        auto st = reader_->GetSchema(&schema_);
        if (!st.ok()) {
            throw std::runtime_error("read_parquet: failed to read schema: " + path_ + " (" +
                                     st.ToString() + ")");
        }

        const auto& metadata = *reader_->parquet_reader()->metadata();
        group_count_ = metadata.num_row_groups();
        source_rows_ = static_cast<std::size_t>(metadata.num_rows());
        field_indices_.resize(static_cast<std::size_t>(schema_->num_fields()));
        categorical_states_.resize(field_indices_.size());
        for (int i = 0; i < schema_->num_fields(); ++i) {
            const auto pos = static_cast<std::size_t>(i);
            field_indices_[pos] = i;
            if (schema_->field(i)->type()->id() == arrow::Type::DICTIONARY) {
                categorical_states_[pos].emplace();
            }
        }
    }

    std::string path_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<int> field_indices_;
    std::vector<std::optional<ibex::Column<ibex::Categorical>>> categorical_states_;
    std::size_t source_rows_ = 0;
    std::size_t next_source_row_ = 0;
    int next_group_ = 0;
    int group_count_ = 0;
};

namespace {

/// Build an Arrow array from an ibex ColumnEntry, preserving null values.
inline auto build_arrow_array(const ibex::runtime::ColumnEntry& entry)
    -> std::shared_ptr<arrow::Array> {
    return std::visit(
        [&](const auto& col) -> std::shared_ptr<arrow::Array> {
            using ColT = std::decay_t<decltype(col)>;
            const std::size_t n = col.size();

            if constexpr (std::is_same_v<ColT, ibex::Column<std::int64_t>>) {
                arrow::Int64Builder builder;
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok())
                    throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i]);
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append int64 failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish int64 failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                arrow::DoubleBuilder builder;
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok())
                    throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i]);
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append double failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish double failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<std::string>>) {
                arrow::StringBuilder builder;
                for (std::size_t i = 0; i < n; ++i) {
                    arrow::Status st;
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        auto sv = col[i];
                        st = builder.Append(sv.data(), static_cast<int32_t>(sv.size()));
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append string failed");
                }
                std::shared_ptr<arrow::Array> arr;
                auto st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish string failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Categorical>>) {
                arrow::StringBuilder builder;
                for (std::size_t i = 0; i < n; ++i) {
                    arrow::Status st;
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        auto sv = col[i];  // string_view from dictionary
                        st = builder.Append(sv.data(), static_cast<int32_t>(sv.size()));
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append categorical failed");
                }
                std::shared_ptr<arrow::Array> arr;
                auto st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish categorical failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                arrow::Date32Builder builder;
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok())
                    throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i].days);
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append date failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish date failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>) {
                arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO),
                                                arrow::default_memory_pool());
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok())
                    throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i].nanos);
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append timestamp failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish timestamp failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<bool>>) {
                arrow::BooleanBuilder builder;
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok())
                    throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i]);
                    }
                    if (!st.ok())
                        throw std::runtime_error("write_parquet: append bool failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok())
                    throw std::runtime_error("write_parquet: finish bool failed");
                return arr;
            } else {
                static_assert(std::is_same_v<ColT, void>, "unhandled column type in write_parquet");
            }
        },
        *entry.column);
}

/// Derive an Arrow field type from an ibex ColumnEntry.
inline auto column_to_arrow_field(const ibex::runtime::ColumnEntry& entry)
    -> std::shared_ptr<arrow::Field> {
    return std::visit(
        [&](const auto& col) -> std::shared_ptr<arrow::Field> {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, ibex::Column<std::int64_t>>) {
                return arrow::field(entry.name, arrow::int64());
            } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                return arrow::field(entry.name, arrow::float64());
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                return arrow::field(entry.name, arrow::date32());
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>) {
                return arrow::field(entry.name, arrow::timestamp(arrow::TimeUnit::NANO));
            } else if constexpr (std::is_same_v<ColT, ibex::Column<bool>>) {
                return arrow::field(entry.name, arrow::boolean());
            } else {
                // string, categorical → UTF-8
                return arrow::field(entry.name, arrow::utf8());
            }
        },
        *entry.column);
}

}  // namespace

/// Write `table` to a Parquet file at `path`.
///
/// Column type mappings:
///   Int64       → Parquet INT64
///   Double      → Parquet DOUBLE
///   String      → Parquet UTF8
///   Categorical → Parquet UTF8 (dictionary decoded)
///   Date        → Parquet DATE32
///   Timestamp   → Parquet TIMESTAMP (nanoseconds, UTC)
///   Bool        → Parquet BOOLEAN
///
/// Returns the number of rows written.
inline auto write_parquet(const ibex::runtime::Table& table, std::string_view path)
    -> std::int64_t {
    const auto& cols = table.columns;

    // Build Arrow schema
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(cols.size());
    for (const auto& entry : cols) {
        fields.push_back(column_to_arrow_field(entry));
    }
    auto schema = arrow::schema(std::move(fields));

    // Build Arrow arrays
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(cols.size());
    for (const auto& entry : cols) {
        arrays.push_back(build_arrow_array(entry));
    }

    auto arrow_table = arrow::Table::Make(schema, arrays);

    // Open output file
    auto sink_result = arrow::io::FileOutputStream::Open(std::string(path));
    if (!sink_result.ok()) {
        throw std::runtime_error("write_parquet: cannot open for writing: " + std::string(path) +
                                 " (" + sink_result.status().ToString() + ")");
    }

    // Write Parquet file
    auto st = parquet::arrow::WriteTable(*arrow_table, arrow::default_memory_pool(),
                                         sink_result.ValueOrDie(),
                                         /*chunk_size=*/static_cast<int64_t>(64) * 1024 * 1024);
    if (!st.ok()) {
        throw std::runtime_error("write_parquet: failed to write: " + std::string(path) + " (" +
                                 st.ToString() + ")");
    }

    return static_cast<std::int64_t>(table.rows());
}
