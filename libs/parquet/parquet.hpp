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

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
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
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
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
    for (std::size_t i = 0; i < count; ++i) {
        dst[i] = typed.IsNull(static_cast<std::int64_t>(i)) ? Out{} : convert(src[i]);
    }
}

/// The same, for a chunk whose Arrow buffer already has the destination's exact
/// layout — `std::int64_t`, `double`, and the single-field `ibex::Date` /
/// `ibex::Timestamp` wrappers.
///
/// The all-valid case inserts straight from Arrow's buffer, which for a
/// trivially copyable element is one memmove. Note it does NOT go through
/// `resize`: resize value-initializes the new elements, so a resize-then-copy
/// writes the destination twice — enough to cancel out the gain over the
/// per-row `push_back` this replaces.
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
    const auto* src = typed.raw_values();

    if (typed.null_count() == 0) {
        const auto* values = reinterpret_cast<const Out*>(src);
        out.insert(out.end(), values, values + count);
        return;
    }

    const std::size_t base = out.size();
    out.resize(base + count);
    Out* dst = out.data() + base;
    for (std::size_t i = 0; i < count; ++i) {
        dst[i] = typed.IsNull(static_cast<std::int64_t>(i)) ? Out{} : std::bit_cast<Out>(src[i]);
    }
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
                sink.add_column(field->name(), ibex::Column<std::int64_t>{std::move(values)});
                break;
            }
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE: {
                std::vector<double> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_double_column(col, values);
                sink.add_column(field->name(), ibex::Column<double>{std::move(values)});
                break;
            }
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING: {
                std::vector<std::string> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_string_column(col, values);
                sink.add_column(field->name(), ibex::Column<std::string>{std::move(values)});
                break;
            }
            case arrow::Type::DICTIONARY: {
                sink.add_column(field->name(), build_categorical_column(col));
                break;
            }
            case arrow::Type::DATE32: {
                std::vector<ibex::Date> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_date32_column(col, values);
                sink.add_column(field->name(), ibex::Column<ibex::Date>{std::move(values)});
                break;
            }
            case arrow::Type::DATE64: {
                std::vector<ibex::Date> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_date64_column(col, values);
                sink.add_column(field->name(), ibex::Column<ibex::Date>{std::move(values)});
                break;
            }
            case arrow::Type::TIMESTAMP: {
                std::vector<ibex::Timestamp> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_timestamp_column(col, col->type(), values);
                sink.add_column(field->name(), ibex::Column<ibex::Timestamp>{std::move(values)});
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

    auto decode = [reader, indices, path_string](const std::vector<std::string>& names)
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

        std::shared_ptr<arrow::Table> table;
        auto read_status = reader->ReadTable(column_indices, &table);
        if (!read_status.ok()) {
            return std::unexpected("read_parquet: failed to read columns from " + path_string +
                                   " (" + read_status.ToString() + ")");
        }
        try {
            ibex::runtime::Table out;
            populate_from_arrow_table(table, out);
            return out;
        } catch (const std::exception& e) {
            return std::unexpected(std::string(e.what()));
        }
    };

    return std::make_shared<ibex::runtime::LazyTable>(schema_table_from_arrow(*arrow_schema), rows,
                                                      std::move(decode));
}

inline auto read_parquet(std::string_view path) -> ibex::runtime::Table {
    std::string path_string{path};
    auto input = open_parquet_input(path);

    auto reader = make_parquet_reader(std::move(input), path_string);

    std::shared_ptr<arrow::Table> table;
    auto st = reader->ReadTable(&table);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to load table: " + path_string + " (" +
                                 st.ToString() + ")");
    }

    ibex::runtime::Table out;
    populate_from_arrow_table(table, out);
    return out;
}

namespace {

/// Number of rows per streamed batch for `ChunkedParquetSourceOperator`.
/// Matches `libs/csv/csv.cpp`'s `kChunkedCsvRowsPerChunk` so chunk sizing is
/// consistent across sources. `parquet::arrow::FileReader::set_batch_size`
/// controls this independent of the file's own row-group sizing, so chunk
/// size stays bounded even against a file written with one giant row group.
constexpr std::int64_t kParquetRowsPerChunk = 65536;

}  // namespace

/// Row-group/batch streaming source for `read_parquet`, registered via
/// `ExternRegistry::register_chunked_table` alongside the whole-file
/// `read_parquet()` above. Emits one `ibex::runtime::Chunk` per
/// `kParquetRowsPerChunk`-row Arrow batch instead of materializing the whole
/// file, so peak memory during a read is bounded by chunk size rather than
/// file size.
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
        std::shared_ptr<arrow::RecordBatch> batch;
        auto st = batches_->ReadNext(&batch);
        if (!st.ok()) {
            return std::unexpected("read_parquet: failed to read batch: " + path_ + " (" +
                                   st.ToString() + ")");
        }
        if (batch == nullptr) {
            return std::optional<ibex::runtime::Chunk>{};
        }

        auto table_result = arrow::Table::FromRecordBatches(batch->schema(), {batch});
        if (!table_result.ok()) {
            return std::unexpected("read_parquet: failed to wrap batch: " + path_ + " (" +
                                   table_result.status().ToString() + ")");
        }

        try {
            ibex::runtime::Chunk chunk;
            populate_from_arrow_table(table_result.ValueOrDie(), chunk);
            return std::optional<ibex::runtime::Chunk>{std::move(chunk)};
        } catch (const std::exception& e) {
            return std::unexpected(std::string(e.what()));
        }
    }

   private:
    ChunkedParquetSourceOperator() = default;

    void init(std::string path) {
        path_ = std::move(path);
        auto input = open_parquet_input(path_);

        reader_ = make_parquet_reader(std::move(input), path_);
        reader_->set_batch_size(kParquetRowsPerChunk);

        auto batches_result = reader_->GetRecordBatchReader();
        if (!batches_result.ok()) {
            throw std::runtime_error("read_parquet: failed to open batch stream: " + path_ + " (" +
                                     batches_result.status().ToString() + ")");
        }
        batches_ = std::move(batches_result).ValueOrDie();
    }

    std::string path_;
    // reader_ must outlive batches_'s use of it (Arrow doc: "FileReaders must
    // outlive their RecordBatchReaders").
    std::unique_ptr<parquet::arrow::FileReader> reader_;
    std::unique_ptr<arrow::RecordBatchReader> batches_;
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
