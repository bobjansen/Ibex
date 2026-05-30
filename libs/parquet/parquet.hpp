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

#include <arrow/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/util/formatting.h>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <curl/curl.h>
#include <filesystem>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <stdexcept>
#include <string>
#include <string_view>
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

inline void append_int_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                              std::vector<std::int64_t>& out) {
    for (const auto& chunk : chunked->chunks()) {
        switch (chunk->type_id()) {
            case arrow::Type::INT64: {
                auto arr = std::static_pointer_cast<arrow::Int64Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : arr->Value(i));
                }
                break;
            }
            case arrow::Type::INT32: {
                auto arr = std::static_pointer_cast<arrow::Int32Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            case arrow::Type::INT16: {
                auto arr = std::static_pointer_cast<arrow::Int16Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            case arrow::Type::INT8: {
                auto arr = std::static_pointer_cast<arrow::Int8Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            case arrow::Type::UINT64: {
                auto arr = std::static_pointer_cast<arrow::UInt64Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            case arrow::Type::UINT32: {
                auto arr = std::static_pointer_cast<arrow::UInt32Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            case arrow::Type::UINT16: {
                auto arr = std::static_pointer_cast<arrow::UInt16Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            case arrow::Type::UINT8: {
                auto arr = std::static_pointer_cast<arrow::UInt8Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0 : static_cast<std::int64_t>(arr->Value(i)));
                }
                break;
            }
            default:
                throw std::runtime_error("read_parquet: unsupported integer column type");
        }
    }
}

inline void append_double_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 std::vector<double>& out) {
    for (const auto& chunk : chunked->chunks()) {
        switch (chunk->type_id()) {
            case arrow::Type::DOUBLE: {
                auto arr = std::static_pointer_cast<arrow::DoubleArray>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0.0 : arr->Value(i));
                }
                break;
            }
            case arrow::Type::FLOAT: {
                auto arr = std::static_pointer_cast<arrow::FloatArray>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    out.push_back(arr->IsNull(i) ? 0.0 : static_cast<double>(arr->Value(i)));
                }
                break;
            }
            default:
                throw std::runtime_error("read_parquet: unsupported float column type");
        }
    }
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
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::DATE32) {
            throw std::runtime_error("read_parquet: unsupported date32 column type");
        }
        auto arr = std::static_pointer_cast<arrow::Date32Array>(chunk);
        for (int64_t i = 0; i < arr->length(); ++i) {
            out.push_back(arr->IsNull(i) ? ibex::Date{0} : ibex::Date{arr->Value(i)});
        }
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
        auto arr = std::static_pointer_cast<arrow::Date64Array>(chunk);
        for (int64_t i = 0; i < arr->length(); ++i) {
            if (arr->IsNull(i)) {
                out.push_back(ibex::Date{0});
            } else {
                out.push_back(ibex::Date{static_cast<std::int32_t>(arr->Value(i) / kMillisPerDay)});
            }
        }
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
        auto arr = std::static_pointer_cast<arrow::TimestampArray>(chunk);
        for (int64_t i = 0; i < arr->length(); ++i) {
            out.push_back(arr->IsNull(i) ? ibex::Timestamp{0}
                                         : ibex::Timestamp{arr->Value(i) * scale});
        }
    }
}

}  // namespace

inline auto read_parquet(std::string_view path) -> ibex::runtime::Table {
    std::string path_string{path};
    auto input = open_parquet_input(path);

    auto reader_result = parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool());
    if (!reader_result.ok()) {
        throw std::runtime_error("read_parquet: failed to read: " + path_string + " (" +
                                 reader_result.status().ToString() + ")");
    }
    std::unique_ptr<parquet::arrow::FileReader> reader = std::move(reader_result).ValueOrDie();

    std::shared_ptr<arrow::Table> table;
    auto st = reader->ReadTable(&table);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to load table: " + path_string + " (" +
                                 st.ToString() + ")");
    }

    ibex::runtime::Table out;
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
                out.add_column(field->name(), ibex::Column<std::int64_t>{std::move(values)});
                break;
            }
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE: {
                std::vector<double> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_double_column(col, values);
                out.add_column(field->name(), ibex::Column<double>{std::move(values)});
                break;
            }
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING: {
                std::vector<std::string> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_string_column(col, values);
                out.add_column(field->name(), ibex::Column<std::string>{std::move(values)});
                break;
            }
            case arrow::Type::DATE32: {
                std::vector<ibex::Date> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_date32_column(col, values);
                out.add_column(field->name(), ibex::Column<ibex::Date>{std::move(values)});
                break;
            }
            case arrow::Type::DATE64: {
                std::vector<ibex::Date> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_date64_column(col, values);
                out.add_column(field->name(), ibex::Column<ibex::Date>{std::move(values)});
                break;
            }
            case arrow::Type::TIMESTAMP: {
                std::vector<ibex::Timestamp> values;
                values.reserve(static_cast<std::size_t>(col->length()));
                append_timestamp_column(col, col->type(), values);
                out.add_column(field->name(), ibex::Column<ibex::Timestamp>{std::move(values)});
                break;
            }
            default:
                throw std::runtime_error("read_parquet: unsupported column type for " +
                                         field->name());
        }
    }

    return out;
}

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
