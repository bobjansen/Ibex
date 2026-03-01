#pragma once
// Ibex Parquet library — provides read_parquet() and write_parquet() for use in Ibex scripts.
//
// Reading:
//   extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";
//   let df = read_parquet("data/myfile.parquet");
//
// Writing:
//   extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";
//   let rows = write_parquet(df, "data/out.parquet");
//
// Compile with: -I$(IBEX_ROOT)/libraries

#include <ibex/runtime/interpreter.hpp>
#include <ibex/core/column.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/formatting.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

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
                                 std::vector<std::string>& out) {
    arrow::internal::StringFormatter<arrow::Date32Type> formatter;
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::DATE32) {
            throw std::runtime_error("read_parquet: unsupported date32 column type");
        }
        auto arr = std::static_pointer_cast<arrow::Date32Array>(chunk);
        for (int64_t i = 0; i < arr->length(); ++i) {
            if (arr->IsNull(i)) {
                out.emplace_back();
                continue;
            }
            std::string value;
            auto append = [&value](std::string_view sv) -> arrow::Status {
                value.append(sv);
                return arrow::Status::OK();
            };
            auto st = formatter(arr->Value(i), append);
            if (!st.ok()) {
                throw std::runtime_error("read_parquet: failed to format date32 value");
            }
            out.emplace_back(std::move(value));
        }
    }
}

inline void append_date64_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                 std::vector<std::string>& out) {
    arrow::internal::StringFormatter<arrow::Date64Type> formatter;
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::DATE64) {
            throw std::runtime_error("read_parquet: unsupported date64 column type");
        }
        auto arr = std::static_pointer_cast<arrow::Date64Array>(chunk);
        for (int64_t i = 0; i < arr->length(); ++i) {
            if (arr->IsNull(i)) {
                out.emplace_back();
                continue;
            }
            std::string value;
            auto append = [&value](std::string_view sv) -> arrow::Status {
                value.append(sv);
                return arrow::Status::OK();
            };
            auto st = formatter(arr->Value(i), append);
            if (!st.ok()) {
                throw std::runtime_error("read_parquet: failed to format date64 value");
            }
            out.emplace_back(std::move(value));
        }
    }
}

inline void append_timestamp_column(const std::shared_ptr<arrow::ChunkedArray>& chunked,
                                    const std::shared_ptr<arrow::DataType>& type,
                                    std::vector<std::string>& out) {
    arrow::internal::StringFormatter<arrow::TimestampType> formatter(type.get());
    for (const auto& chunk : chunked->chunks()) {
        if (chunk->type_id() != arrow::Type::TIMESTAMP) {
            throw std::runtime_error("read_parquet: unsupported timestamp column type");
        }
        auto arr = std::static_pointer_cast<arrow::TimestampArray>(chunk);
        for (int64_t i = 0; i < arr->length(); ++i) {
            if (arr->IsNull(i)) {
                out.emplace_back();
                continue;
            }
            std::string value;
            auto append = [&value](std::string_view sv) -> arrow::Status {
                value.append(sv);
                return arrow::Status::OK();
            };
            auto st = formatter(arr->Value(i), append);
            if (!st.ok()) {
                throw std::runtime_error("read_parquet: failed to format timestamp value");
            }
            out.emplace_back(std::move(value));
        }
    }
}

}  // namespace

inline auto read_parquet(std::string_view path) -> ibex::runtime::Table {
    auto input_result = arrow::io::ReadableFile::Open(std::string(path));
    if (!input_result.ok()) {
        throw std::runtime_error("read_parquet: failed to open: " + std::string(path) + " (" +
                                 input_result.status().ToString() + ")");
    }

    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto st = parquet::arrow::OpenFile(input_result.ValueOrDie(), arrow::default_memory_pool(),
                                       &reader);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to read: " + std::string(path) + " (" +
                                 st.ToString() + ")");
    }

    std::shared_ptr<arrow::Table> table;
    st = reader->ReadTable(&table);
    if (!st.ok()) {
        throw std::runtime_error("read_parquet: failed to load table: " + std::string(path) +
                                 " (" + st.ToString() + ")");
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
                values.reserve(col->length());
                append_int_column(col, values);
                out.add_column(field->name(), ibex::Column<std::int64_t>{std::move(values)});
                break;
            }
            case arrow::Type::FLOAT:
            case arrow::Type::DOUBLE: {
                std::vector<double> values;
                values.reserve(col->length());
                append_double_column(col, values);
                out.add_column(field->name(), ibex::Column<double>{std::move(values)});
                break;
            }
            case arrow::Type::STRING:
            case arrow::Type::LARGE_STRING: {
                std::vector<std::string> values;
                values.reserve(col->length());
                append_string_column(col, values);
                out.add_column(field->name(), ibex::Column<std::string>{std::move(values)});
                break;
            }
            case arrow::Type::DATE32: {
                std::vector<std::string> values;
                values.reserve(col->length());
                append_date32_column(col, values);
                out.add_column(field->name(), ibex::Column<std::string>{std::move(values)});
                break;
            }
            case arrow::Type::DATE64: {
                std::vector<std::string> values;
                values.reserve(col->length());
                append_date64_column(col, values);
                out.add_column(field->name(), ibex::Column<std::string>{std::move(values)});
                break;
            }
            case arrow::Type::TIMESTAMP: {
                std::vector<std::string> values;
                values.reserve(col->length());
                append_timestamp_column(col, col->type(), values);
                out.add_column(field->name(), ibex::Column<std::string>{std::move(values)});
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
                if (!st.ok()) throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i]);
                    }
                    if (!st.ok()) throw std::runtime_error("write_parquet: append int64 failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok()) throw std::runtime_error("write_parquet: finish int64 failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                arrow::DoubleBuilder builder;
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok()) throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i]);
                    }
                    if (!st.ok()) throw std::runtime_error("write_parquet: append double failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok()) throw std::runtime_error("write_parquet: finish double failed");
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
                    if (!st.ok()) throw std::runtime_error("write_parquet: append string failed");
                }
                std::shared_ptr<arrow::Array> arr;
                auto st = builder.Finish(&arr);
                if (!st.ok()) throw std::runtime_error("write_parquet: finish string failed");
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
                if (!st.ok()) throw std::runtime_error("write_parquet: finish categorical failed");
                return arr;
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                arrow::Date32Builder builder;
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok()) throw std::runtime_error("write_parquet: reserve failed");
                for (std::size_t i = 0; i < n; ++i) {
                    if (ibex::runtime::is_null(entry, i)) {
                        st = builder.AppendNull();
                    } else {
                        st = builder.Append(col[i].days);
                    }
                    if (!st.ok()) throw std::runtime_error("write_parquet: append date failed");
                }
                std::shared_ptr<arrow::Array> arr;
                st = builder.Finish(&arr);
                if (!st.ok()) throw std::runtime_error("write_parquet: finish date failed");
                return arr;
            } else {
                // Column<Timestamp> — store as INT64 (nanoseconds since epoch)
                static_assert(std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>,
                              "unhandled column type in write_parquet");
                arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO),
                                                arrow::default_memory_pool());
                auto st = builder.Reserve(static_cast<int64_t>(n));
                if (!st.ok()) throw std::runtime_error("write_parquet: reserve failed");
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
                if (!st.ok()) throw std::runtime_error("write_parquet: finish timestamp failed");
                return arr;
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
