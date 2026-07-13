#pragma once
// Ibex Parquet library — provides read_parquet() and write_parquet() for use in Ibex scripts.
//
// Reading:
//   extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";
//   let df = read_parquet("data/myfile.parquet");
//   let public = read_parquet("https://data.example.com/myfile.parquet");
//   let remote = read_parquet("s3://bucket/path/myfile.parquet?region=us-east-1");
//
// Writing:
//   extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";
//   let rows = write_parquet(df, "data/out.parquet");
//
// Compile with: -I$(IBEX_ROOT)/libraries

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <algorithm>
#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/util/bitmap_ops.h>
#include <arrow/util/formatting.h>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <curl/curl.h>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/arrow/writer.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

inline auto is_s3_uri(std::string_view path) -> bool {
    return path.starts_with("s3://");
}

inline auto is_https_uri(std::string_view path) -> bool {
    return path.starts_with("https://");
}

inline void close_and_remove_temp(int fd, const std::string& path) {
    if (fd >= 0) {
        (void)::close(fd);
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

inline auto make_temp_parquet_file() -> std::pair<int, std::string> {
    std::error_code ec;
    auto temp_dir = std::filesystem::temp_directory_path(ec);
    if (ec) {
        throw std::runtime_error("read_parquet: failed to locate temp directory: " + ec.message());
    }

    auto pattern = (temp_dir / "ibex-parquet-XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');

    int fd = ::mkstemp(buffer.data());
    if (fd < 0) {
        throw std::runtime_error("read_parquet: failed to create temp file: " +
                                 std::string(std::strerror(errno)));
    }
    return {fd, std::string(buffer.data())};
}

inline auto write_http_chunk(char* ptr, std::size_t size, std::size_t nmemb, void* userdata)
    -> std::size_t {
    if (size != 0 && nmemb > static_cast<std::size_t>(-1) / size) {
        return 0;
    }

    const auto total = size * nmemb;
    const char* data = ptr;
    auto* fd = static_cast<int*>(userdata);
    std::size_t written = 0;
    while (written < total) {
        const auto n = ::write(*fd, data + written, total - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return 0;
        }
        if (n == 0) {
            return 0;
        }
        written += static_cast<std::size_t>(n);
    }
    return total;
}

inline auto download_https_to_temp(std::string_view url) -> std::string {
    static const bool curl_initialized = [] {
        return curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK;
    }();
    if (!curl_initialized) {
        throw std::runtime_error("read_parquet: failed to initialize HTTPS client");
    }

    auto [fd, temp_path] = make_temp_parquet_file();
    CURL* curl = curl_easy_init();
    if (curl == nullptr) {
        close_and_remove_temp(fd, temp_path);
        throw std::runtime_error("read_parquet: failed to create HTTPS client");
    }

    std::string url_string{url};
    char error_buffer[CURL_ERROR_SIZE] = {};
    curl_easy_setopt(curl, CURLOPT_URL, url_string.c_str());
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error_buffer);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_http_chunk);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &fd);
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

    const int close_result = ::close(fd);
    fd = -1;

    if (rc != CURLE_OK) {
        close_and_remove_temp(fd, temp_path);
        std::string detail = error_buffer[0] != '\0' ? error_buffer : curl_easy_strerror(rc);
        throw std::runtime_error("read_parquet: failed to download '" + url_string + "' (" +
                                 detail + ", HTTP " + std::to_string(response_code) + ")");
    }
    if (close_result != 0) {
        close_and_remove_temp(fd, temp_path);
        throw std::runtime_error("read_parquet: failed to finish temp download for '" + url_string +
                                 "': " + std::strerror(errno));
    }

    return temp_path;
}

inline auto open_parquet_input(std::string_view path)
    -> std::shared_ptr<arrow::io::RandomAccessFile> {
    std::string path_string{path};
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

inline void append_string_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 std::vector<std::string>& out) {
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() == arrow::Type::STRING) {
            auto arr = std::static_pointer_cast<arrow::StringArray>(chunk);
            for (int64_t i = 0; i < arr->length(); ++i) {
                out.push_back(arr->IsNull(i) ? std::string{} : arr->GetString(i));
            }
        } else if (chunk->type_id() == arrow::Type::LARGE_STRING) {
            auto arr = std::static_pointer_cast<arrow::LargeStringArray>(chunk);
            for (int64_t i = 0; i < arr->length(); ++i) {
                out.push_back(arr->IsNull(i) ? std::string{} : arr->GetString(i));
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
                std::vector<std::string> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_string_column(col, values);
                emit(ibex::Column<std::string>{std::move(values)});
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
inline auto dictionary_column_indices(const parquet::FileMetaData& metadata) -> std::vector<int> {
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
        if (fully_dictionary) {
            out.push_back(col);
        }
    }
    return out;
}

/// Open `input` as an Arrow-backed Parquet reader, reading every fully
/// dictionary-encoded string column straight back as a dictionary rather than
/// materializing one `std::string` per row.
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
    for (int col : dictionary_column_indices(*builder.raw_reader()->metadata())) {
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

template <typename DType, typename Emit>
inline auto decode_physical_column(parquet::arrow::FileReader& reader, int leaf_index,
                                   const ibex::runtime::Selection* selection,
                                   const DirectDecodeGroups& groups, Emit&& emit) -> std::size_t;

template <typename DType, typename Out, bool SameLayout, typename Convert>
inline auto decode_numeric_column(parquet::arrow::FileReader& reader, int leaf_index,
                                  const ibex::runtime::Selection* selection,
                                  const DirectDecodeGroups& groups, ibex::Column<Out>& out,
                                  DirectValidity& validity, Convert&& convert) -> std::size_t {
    using Raw = typename DType::c_type;
    if (selection != nullptr ||
        reader.parquet_reader()->metadata()->schema()->Column(leaf_index)->max_definition_level() !=
            0) {
        return decode_physical_column<DType>(
            reader, leaf_index, selection, groups, [&](const Raw* value) {
                validity.append(value != nullptr);
                out.push_back(value == nullptr ? Out{} : convert(*value));
            });
    }

    const auto& metadata = *reader.parquet_reader()->metadata();
    std::size_t emitted = 0;
    const std::size_t output_start = out.size();
    std::unique_ptr<Raw[]> converted_values;
    if constexpr (SameLayout) {
        static_assert(std::is_same_v<Raw, Out>);
        out.resize_for_overwrite(output_start + groups.rows);
    } else {
        converted_values.reset(new Raw[static_cast<std::size_t>(kDirectDecodeBatchRows)]);
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
                destination = out.span().data() + output_start + emitted;
            } else {
                destination = converted_values.get();
            }
            const std::int64_t levels_read =
                typed->ReadBatch(request, nullptr, nullptr, destination, &values_read);
            if (levels_read <= 0 || levels_read != values_read) {
                throw std::runtime_error("read_parquet: dense column decoder made no progress");
            }
            const auto count = static_cast<std::size_t>(values_read);
            if constexpr (!SameLayout) {
                for (std::size_t i = 0; i < count; ++i) {
                    out.push_back(convert(converted_values[i]));
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
                request, optional ? definitions.get() : nullptr, nullptr, local_codes.get(),
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

            std::size_t code_pos = 0;
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

        // Flat required columns can seek over rejected rows without decoding
        // them. Nullable columns stay on the level-aware path below because
        // TypedColumnReader::Skip counts physical values rather than logical
        // rows, and nulls have no physical value to skip.
        if (selection != nullptr && !optional) {
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
                std::int64_t values_read = 0;
                const auto levels_read = typed->ReadBatch(static_cast<std::int64_t>(run), nullptr,
                                                          nullptr, values.get(), &values_read);
                if (levels_read != static_cast<std::int64_t>(run) || values_read != levels_read) {
                    throw std::runtime_error(
                        "read_parquet: column ended while reading selected rows");
                }
                for (std::size_t i = 0; i < run; ++i) {
                    emit(&values[i]);
                }
                emitted += run;
                selected_pos += run;
                source_row += run;
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
            std::vector<std::string> out;
            out.reserve(output_rows);
            auto emitted = decode_physical_column<parquet::ByteArrayType>(
                reader, leaf_index, selection, groups, [&](const parquet::ByteArray* value) {
                    validity.append(value != nullptr);
                    if (value == nullptr) {
                        out.emplace_back();
                    } else {
                        out.emplace_back(reinterpret_cast<const char*>(value->ptr), value->len);
                    }
                });
            verify(emitted);
            entry.column = std::make_shared<ibex::runtime::ColumnValue>(
                ibex::Column<std::string>{std::move(out)});
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

inline auto direct_decode_table(parquet::arrow::FileReader& reader, const arrow::Schema& schema,
                                const std::vector<int>& field_indices,
                                const ibex::runtime::Selection* selection, std::size_t source_rows,
                                const DirectDecodeGroups& groups) -> ibex::runtime::Table {
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

    ibex::runtime::Table out;
    const auto& manifest = reader.manifest();
    for (int field_index : field_indices) {
        if (field_index < 0 || field_index >= schema.num_fields() ||
            field_index >= static_cast<int>(manifest.schema_fields.size()) ||
            !manifest.schema_fields[static_cast<std::size_t>(field_index)].is_leaf()) {
            throw std::runtime_error("read_parquet: nested columns are not supported");
        }
        auto entry = direct_column(
            reader, *schema.field(field_index),
            manifest.schema_fields[static_cast<std::size_t>(field_index)].column_index, selection,
            groups, output_rows);
        out.add_column_shared(std::move(entry.name), std::move(entry.column),
                              std::move(entry.validity));
    }
    out.logical_rows = output_rows;
    return out;
}

inline auto direct_decode_table(parquet::arrow::FileReader& reader, const arrow::Schema& schema,
                                const std::vector<int>& field_indices,
                                const ibex::runtime::Selection* selection, std::size_t source_rows)
    -> ibex::runtime::Table {
    return direct_decode_table(reader, schema, field_indices, selection, source_rows,
                               all_decode_groups(*reader.parquet_reader()->metadata()));
}

}  // namespace

/// Open `path` for deferred reading: take its schema and row count from the
/// footer, and hand back a handle that decodes individual columns on demand.
///
/// This is what gives `let t = read_parquet(p)` projection pushdown. Binding
/// touches metadata only; a query that references 4 of 16 columns decodes 4.
inline auto read_parquet_lazy(std::string_view path) -> ibex::runtime::LazyTablePtr {
    std::string path_string{path};
    auto input = open_parquet_input(path);

    // Shared rather than unique: the decode callback below outlives this scope
    // and each call reads from the same open file.
    std::shared_ptr<parquet::arrow::FileReader> reader =
        make_parquet_reader(std::move(input), path_string);

    std::shared_ptr<arrow::Schema> arrow_schema;
    auto st = reader->GetSchema(&arrow_schema);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to read schema: " + path_string + " (" +
                                 st.ToString() + ")");
    }

    const auto rows = static_cast<std::size_t>(reader->parquet_reader()->metadata()->num_rows());

    // Column name -> field index, so a demand expressed in names can be turned
    // into the indices Arrow's selective read wants.
    auto indices = std::make_shared<std::map<std::string, int>>();
    for (int i = 0; i < arrow_schema->num_fields(); ++i) {
        indices->emplace(arrow_schema->field(i)->name(), i);
    }

    auto decode = [reader, indices, arrow_schema, rows, path_string](
                      const std::vector<std::string>& names,
                      const ibex::runtime::Selection* selection)
        -> std::expected<ibex::runtime::Table, std::string> {
        std::vector<int> column_indices;
        column_indices.reserve(names.size());
        for (const auto& name : names) {
            auto it = indices->find(name);
            if (it == indices->end()) {
                return std::unexpected("read_parquet: no column '" + name + "' in " + path_string);
            }
            column_indices.push_back(it->second);
        }

        try {
            return direct_decode_table(*reader, *arrow_schema, column_indices, selection, rows);
        } catch (const std::exception& e) {
            return std::unexpected("read_parquet: failed to read columns from " + path_string +
                                   " (" + e.what() + ")");
        }
    };

    return std::make_shared<ibex::runtime::LazyTable>(schema_table_from_arrow(*arrow_schema), rows,
                                                      std::move(decode));
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
