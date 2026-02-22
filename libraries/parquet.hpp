#pragma once
// Ibex Parquet library — provides read_parquet() for use in Ibex scripts.
//
// Usage in .ibex:
//   extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";
//   let df = read_parquet("data/myfile.parquet");
//
// Compile with: -I$(IBEX_ROOT)/libraries

#include <ibex/runtime/interpreter.hpp>
#include <ibex/core/column.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/util/formatting.h>
#include <parquet/arrow/reader.h>

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
