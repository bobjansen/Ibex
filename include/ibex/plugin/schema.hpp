#pragma once
// Shared schema/serialisation helpers for Ibex I/O plugins.
//
// Streaming transport plugins (kafka, websocket, udp, ...) all face the same
// problem: a wire payload arrives as a JSON object and must be materialised
// into a typed one-row Table, and Table rows must be serialised back to JSON
// on the way out.  This header centralises that machinery so every transport
// shares one schema mini-language and one JSON <-> Table conversion path.
//
// Schema spec mini-language (comma-separated name:type entries):
//
//   "ts:timestamp,symbol:cat,price:f64,volume:i64"
//
// Supported types: i64/int/int64, f64/double/float64, bool/boolean,
// str/string, cat/categorical, date, ts/timestamp.
//
// The first date/timestamp field in the schema becomes the Table time_index.
//
// Options spec mini-language (key=value entries separated by ';' or newlines):
//
//   "poll_timeout_ms=200;consumer.auto.offset.reset=earliest"
//
// Requires nlohmann/json — link nlohmann_json::nlohmann_json in the plugin.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <expected>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::plugin {

enum class FieldKind : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Categorical,
    Date,
    Timestamp,
};

struct SchemaField {
    std::string name;
    FieldKind kind = FieldKind::String;
};

inline auto trim(std::string_view text) -> std::string_view {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0) {
        text.remove_suffix(1);
    }
    return text;
}

inline auto parse_field_kind(std::string_view type_str) -> std::expected<FieldKind, std::string> {
    if (type_str == "i64" || type_str == "int" || type_str == "int64") {
        return FieldKind::Int;
    }
    if (type_str == "f64" || type_str == "double" || type_str == "float64") {
        return FieldKind::Double;
    }
    if (type_str == "bool" || type_str == "boolean") {
        return FieldKind::Bool;
    }
    if (type_str == "str" || type_str == "string") {
        return FieldKind::String;
    }
    if (type_str == "cat" || type_str == "categorical") {
        return FieldKind::Categorical;
    }
    if (type_str == "date") {
        return FieldKind::Date;
    }
    if (type_str == "ts" || type_str == "timestamp") {
        return FieldKind::Timestamp;
    }
    return std::unexpected("unsupported schema type: " + std::string(type_str));
}

inline auto parse_schema(std::string_view spec)
    -> std::expected<std::vector<SchemaField>, std::string> {
    std::vector<SchemaField> fields;
    std::vector<std::string_view> names;
    std::size_t pos = 0;
    while (pos < spec.size()) {
        const std::size_t next = spec.find(',', pos);
        std::string_view item =
            next == std::string_view::npos ? spec.substr(pos) : spec.substr(pos, next - pos);
        item = trim(item);
        if (item.empty()) {
            return std::unexpected("schema contains an empty field entry");
        }
        const std::size_t colon = item.find(':');
        if (colon == std::string_view::npos || colon == 0 || colon + 1 >= item.size()) {
            return std::unexpected(
                "schema entries must use the form name:type with explicit field names");
        }
        std::string_view name = trim(item.substr(0, colon));
        std::string_view kind_text = trim(item.substr(colon + 1));
        if (name.empty()) {
            return std::unexpected("schema field names must not be empty");
        }
        if (std::ranges::find(names, name) != names.end()) {
            return std::unexpected("schema contains duplicate field '" + std::string(name) + "'");
        }
        auto kind = parse_field_kind(kind_text);
        if (!kind) {
            return std::unexpected(kind.error());
        }
        fields.push_back({.name = std::string(name), .kind = *kind});
        names.push_back(name);
        if (next == std::string_view::npos) {
            break;
        }
        pos = next + 1;
    }
    if (fields.empty()) {
        return std::unexpected("schema must not be empty");
    }
    return fields;
}

inline auto parse_key_value_options(std::string_view spec)
    -> std::expected<std::vector<std::pair<std::string, std::string>>, std::string> {
    std::vector<std::pair<std::string, std::string>> parsed;
    if (trim(spec).empty()) {
        return parsed;
    }
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        const std::size_t next = spec.find_first_of(";\n", pos);
        std::string_view item =
            next == std::string_view::npos ? spec.substr(pos) : spec.substr(pos, next - pos);
        item = trim(item);
        if (!item.empty()) {
            const std::size_t eq = item.find('=');
            if (eq == std::string_view::npos || eq == 0 || eq + 1 >= item.size()) {
                return std::unexpected(
                    "options must be key=value entries separated by ';' or newlines");
            }
            parsed.emplace_back(std::string(trim(item.substr(0, eq))),
                                std::string(trim(item.substr(eq + 1))));
        }
        if (next == std::string_view::npos) {
            break;
        }
        pos = next + 1;
    }
    return parsed;
}

/// Parse a non-negative integer option value (e.g. a millisecond timeout).
inline auto parse_non_negative_int(std::string_view value, std::string_view option_name)
    -> std::expected<int, std::string> {
    int parsed = 0;
    const auto* begin = value.data();
    const auto* end = value.data() + value.size();
    auto [ptr, ec] = std::from_chars(begin, end, parsed);
    if (ec != std::errc{} || ptr != end || parsed < 0) {
        return std::unexpected(std::string(option_name) + " must be a non-negative integer");
    }
    return parsed;
}

inline auto value_to_int64(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<std::int64_t, std::string> {
    if (!value.is_number_integer()) {
        return std::unexpected("field '" + std::string(field_name) + "' must be an integer");
    }
    return value.get<std::int64_t>();
}

inline auto value_to_double(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<double, std::string> {
    if (!value.is_number()) {
        return std::unexpected("field '" + std::string(field_name) + "' must be numeric");
    }
    return value.get<double>();
}

inline auto value_to_bool(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<bool, std::string> {
    if (!value.is_boolean()) {
        return std::unexpected("field '" + std::string(field_name) + "' must be a boolean");
    }
    return value.get<bool>();
}

inline auto value_to_string(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<std::string, std::string> {
    if (!value.is_string()) {
        return std::unexpected("field '" + std::string(field_name) + "' must be a string");
    }
    return value.get<std::string>();
}

/// Materialise one parsed JSON object into a typed one-row Table following
/// `schema`.  Every schema field must be present and non-null.  The first
/// Date/Timestamp field becomes the Table time_index.
inline auto table_from_json_object(const nlohmann::json& object,
                                   const std::vector<SchemaField>& schema)
    -> std::expected<ibex::runtime::Table, std::string> {
    if (!object.is_object()) {
        return std::unexpected("payload must be a JSON object");
    }

    ibex::runtime::Table table;
    std::optional<std::string> time_index;
    for (const auto& field : schema) {
        auto it = object.find(field.name);
        if (it == object.end() || it->is_null()) {
            return std::unexpected("payload is missing required field '" + field.name + "'");
        }
        switch (field.kind) {
            case FieldKind::Int: {
                auto value = value_to_int64(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<std::int64_t>{*value});
                break;
            }
            case FieldKind::Double: {
                auto value = value_to_double(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<double>{*value});
                break;
            }
            case FieldKind::Bool: {
                auto value = value_to_bool(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<bool>{*value});
                break;
            }
            case FieldKind::String: {
                auto value = value_to_string(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<std::string>{std::move(*value)});
                break;
            }
            case FieldKind::Categorical: {
                auto value = value_to_string(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                ibex::Column<ibex::Categorical> column;
                column.push_back(*value);
                table.add_column(field.name, std::move(column));
                break;
            }
            case FieldKind::Date: {
                auto value = value_to_int64(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<ibex::Date>{
                                                 ibex::Date{static_cast<std::int32_t>(*value)}});
                if (!time_index.has_value()) {
                    time_index = field.name;
                }
                break;
            }
            case FieldKind::Timestamp: {
                auto value = value_to_int64(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name,
                                 ibex::Column<ibex::Timestamp>{ibex::Timestamp{*value}});
                if (!time_index.has_value()) {
                    time_index = field.name;
                }
                break;
            }
        }
    }
    if (time_index.has_value()) {
        table.time_index = std::move(*time_index);
    }
    return table;
}

/// Parse `payload` as JSON and materialise it via table_from_json_object.
inline auto table_from_json_payload(std::string_view payload,
                                    const std::vector<SchemaField>& schema)
    -> std::expected<ibex::runtime::Table, std::string> {
    nlohmann::json object;
    try {
        object = nlohmann::json::parse(payload);
    } catch (const nlohmann::json::parse_error& e) {
        return std::unexpected("payload is not valid JSON: " + std::string(e.what()));
    }
    return table_from_json_object(object, schema);
}

/// Check whether a parsed JSON object satisfies `schema` (every field present,
/// non-null, and of a convertible type).  Lets batching sources filter
/// malformed messages up front so the batch materialisation cannot fail.
inline auto json_matches_schema(const nlohmann::json& object,
                                const std::vector<SchemaField>& schema) -> bool {
    if (!object.is_object()) {
        return false;
    }
    for (const auto& field : schema) {
        auto it = object.find(field.name);
        if (it == object.end() || it->is_null()) {
            return false;
        }
        switch (field.kind) {
            case FieldKind::Int:
            case FieldKind::Date:
            case FieldKind::Timestamp:
                if (!it->is_number_integer())
                    return false;
                break;
            case FieldKind::Double:
                if (!it->is_number())
                    return false;
                break;
            case FieldKind::Bool:
                if (!it->is_boolean())
                    return false;
                break;
            case FieldKind::String:
            case FieldKind::Categorical:
                if (!it->is_string())
                    return false;
                break;
        }
    }
    return true;
}

/// Materialise a batch of parsed JSON objects into a multi-row Table following
/// `schema`, one row per object.  Every schema field must be present and
/// non-null in every object (pre-filter with json_matches_schema when inputs
/// are untrusted).  The first Date/Timestamp field becomes the time_index.
inline auto table_from_json_objects(const std::vector<nlohmann::json>& objects,
                                    const std::vector<SchemaField>& schema)
    -> std::expected<ibex::runtime::Table, std::string> {
    ibex::runtime::Table table;
    std::optional<std::string> time_index;
    const std::size_t n_rows = objects.size();

    for (const auto& field : schema) {
        // Field lookup shared by every column type below.
        auto field_at = [&](std::size_t row) -> std::expected<const nlohmann::json*, std::string> {
            const auto& object = objects[row];
            if (!object.is_object()) {
                return std::unexpected("payload " + std::to_string(row) + " must be a JSON object");
            }
            auto it = object.find(field.name);
            if (it == object.end() || it->is_null()) {
                return std::unexpected("payload " + std::to_string(row) +
                                       " is missing required field '" + field.name + "'");
            }
            return &*it;
        };

        switch (field.kind) {
            case FieldKind::Int: {
                ibex::Column<std::int64_t> column;
                column.reserve(n_rows);
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_int64(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(*value);
                }
                table.add_column(field.name, std::move(column));
                break;
            }
            case FieldKind::Double: {
                ibex::Column<double> column;
                column.reserve(n_rows);
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_double(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(*value);
                }
                table.add_column(field.name, std::move(column));
                break;
            }
            case FieldKind::Bool: {
                ibex::Column<bool> column;
                column.reserve(n_rows);
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_bool(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(*value);
                }
                table.add_column(field.name, std::move(column));
                break;
            }
            case FieldKind::String: {
                ibex::Column<std::string> column;
                column.reserve(n_rows);
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_string(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(std::move(*value));
                }
                table.add_column(field.name, std::move(column));
                break;
            }
            case FieldKind::Categorical: {
                ibex::Column<ibex::Categorical> column;
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_string(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(*value);
                }
                table.add_column(field.name, std::move(column));
                break;
            }
            case FieldKind::Date: {
                ibex::Column<ibex::Date> column;
                column.reserve(n_rows);
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_int64(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(ibex::Date{static_cast<std::int32_t>(*value)});
                }
                table.add_column(field.name, std::move(column));
                if (!time_index.has_value()) {
                    time_index = field.name;
                }
                break;
            }
            case FieldKind::Timestamp: {
                ibex::Column<ibex::Timestamp> column;
                column.reserve(n_rows);
                for (std::size_t row = 0; row < n_rows; ++row) {
                    auto cell = field_at(row);
                    if (!cell)
                        return std::unexpected(cell.error());
                    auto value = value_to_int64(**cell, field.name);
                    if (!value)
                        return std::unexpected(value.error());
                    column.push_back(ibex::Timestamp{*value});
                }
                table.add_column(field.name, std::move(column));
                if (!time_index.has_value()) {
                    time_index = field.name;
                }
                break;
            }
        }
    }
    if (time_index.has_value()) {
        table.time_index = std::move(*time_index);
    }
    return table;
}

/// Serialise one row of `table` as a compact JSON object string.
inline auto table_row_to_json(const ibex::runtime::Table& table, std::size_t row)
    -> std::expected<std::string, std::string> {
    using json = nlohmann::json;
    json object = json::object();
    for (const auto& entry : table.columns) {
        if (ibex::runtime::is_null(entry, row)) {
            object[entry.name] = nullptr;
            continue;
        }
        std::visit(
            [&](const auto& column) {
                using Col = std::decay_t<decltype(column)>;
                if constexpr (std::is_same_v<Col, ibex::Column<std::int64_t>>) {
                    object[entry.name] = column[row];
                } else if constexpr (std::is_same_v<Col, ibex::Column<double>>) {
                    object[entry.name] = column[row];
                } else if constexpr (std::is_same_v<Col, ibex::Column<bool>>) {
                    object[entry.name] = column[row];
                } else if constexpr (std::is_same_v<Col, ibex::Column<std::string>>) {
                    object[entry.name] = std::string(column[row]);
                } else if constexpr (std::is_same_v<Col, ibex::Column<ibex::Categorical>>) {
                    object[entry.name] = std::string(column[row]);
                } else if constexpr (std::is_same_v<Col, ibex::Column<ibex::Date>>) {
                    object[entry.name] = column[row].days;
                } else if constexpr (std::is_same_v<Col, ibex::Column<ibex::Timestamp>>) {
                    object[entry.name] = column[row].nanos;
                }
            },
            *entry.column);
    }
    return object.dump();
}

}  // namespace ibex::plugin
