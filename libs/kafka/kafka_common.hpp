#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

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

namespace ibex_kafka {

enum class KafkaFieldKind : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Categorical,
    Date,
    Timestamp,
};

struct KafkaSchemaField {
    std::string name;
    KafkaFieldKind kind = KafkaFieldKind::String;
};

struct KafkaConsumerOptions {
    std::vector<std::pair<std::string, std::string>> config;
    int poll_timeout_ms = 200;
};

struct KafkaProducerOptions {
    std::vector<std::pair<std::string, std::string>> config;
    int flush_timeout_ms = 5000;
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

inline auto parse_kafka_field_kind(std::string_view type_str)
    -> std::expected<KafkaFieldKind, std::string> {
    if (type_str == "i64" || type_str == "int" || type_str == "int64") {
        return KafkaFieldKind::Int;
    }
    if (type_str == "f64" || type_str == "double" || type_str == "float64") {
        return KafkaFieldKind::Double;
    }
    if (type_str == "bool" || type_str == "boolean") {
        return KafkaFieldKind::Bool;
    }
    if (type_str == "str" || type_str == "string") {
        return KafkaFieldKind::String;
    }
    if (type_str == "cat" || type_str == "categorical") {
        return KafkaFieldKind::Categorical;
    }
    if (type_str == "date") {
        return KafkaFieldKind::Date;
    }
    if (type_str == "ts" || type_str == "timestamp") {
        return KafkaFieldKind::Timestamp;
    }
    return std::unexpected("unsupported Kafka schema type: " + std::string(type_str));
}

inline auto parse_kafka_schema(std::string_view spec)
    -> std::expected<std::vector<KafkaSchemaField>, std::string> {
    std::vector<KafkaSchemaField> fields;
    std::vector<std::string_view> names;
    std::size_t pos = 0;
    while (pos < spec.size()) {
        const std::size_t next = spec.find(',', pos);
        std::string_view item =
            next == std::string_view::npos ? spec.substr(pos) : spec.substr(pos, next - pos);
        item = trim(item);
        if (item.empty()) {
            return std::unexpected("Kafka schema contains an empty field entry");
        }
        const std::size_t colon = item.find(':');
        if (colon == std::string_view::npos || colon == 0 || colon + 1 >= item.size()) {
            return std::unexpected(
                "Kafka schema entries must use the form name:type with explicit field names");
        }
        std::string_view name = trim(item.substr(0, colon));
        std::string_view kind_text = trim(item.substr(colon + 1));
        if (name.empty()) {
            return std::unexpected("Kafka schema field names must not be empty");
        }
        if (std::ranges::find(names, name) != names.end()) {
            return std::unexpected("Kafka schema contains duplicate field '" + std::string(name) +
                                   "'");
        }
        auto kind = parse_kafka_field_kind(kind_text);
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
        return std::unexpected("Kafka schema must not be empty");
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
                    "Kafka options must be key=value entries separated by ';' or newlines");
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

inline auto parse_kafka_consumer_options(std::string_view spec)
    -> std::expected<KafkaConsumerOptions, std::string> {
    KafkaConsumerOptions options;
    auto parsed = parse_key_value_options(spec);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    for (auto& [key, value] : *parsed) {
        if (key == "poll_timeout_ms") {
            int timeout = 0;
            const auto* begin = value.data();
            const auto* end = value.data() + value.size();
            auto [ptr, ec] = std::from_chars(begin, end, timeout);
            if (ec != std::errc{} || ptr != end || timeout < 0) {
                return std::unexpected("poll_timeout_ms must be a non-negative integer");
            }
            options.poll_timeout_ms = timeout;
            continue;
        }
        if (key.rfind("consumer.", 0) == 0) {
            options.config.emplace_back(key.substr(9), std::move(value));
            continue;
        }
        return std::unexpected("unsupported Kafka consumer option: " + key);
    }
    return options;
}

inline auto parse_kafka_producer_options(std::string_view spec)
    -> std::expected<KafkaProducerOptions, std::string> {
    KafkaProducerOptions options;
    auto parsed = parse_key_value_options(spec);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    for (auto& [key, value] : *parsed) {
        if (key == "flush_timeout_ms") {
            int timeout = 0;
            const auto* begin = value.data();
            const auto* end = value.data() + value.size();
            auto [ptr, ec] = std::from_chars(begin, end, timeout);
            if (ec != std::errc{} || ptr != end || timeout < 0) {
                return std::unexpected("flush_timeout_ms must be a non-negative integer");
            }
            options.flush_timeout_ms = timeout;
            continue;
        }
        if (key.rfind("producer.", 0) == 0) {
            options.config.emplace_back(key.substr(9), std::move(value));
            continue;
        }
        return std::unexpected("unsupported Kafka producer option: " + key);
    }
    return options;
}

inline auto value_to_int64(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<std::int64_t, std::string> {
    if (!value.is_number_integer()) {
        return std::unexpected("Kafka field '" + std::string(field_name) + "' must be an integer");
    }
    return value.get<std::int64_t>();
}

inline auto value_to_double(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<double, std::string> {
    if (!value.is_number()) {
        return std::unexpected("Kafka field '" + std::string(field_name) + "' must be numeric");
    }
    return value.get<double>();
}

inline auto value_to_bool(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<bool, std::string> {
    if (!value.is_boolean()) {
        return std::unexpected("Kafka field '" + std::string(field_name) + "' must be a boolean");
    }
    return value.get<bool>();
}

inline auto value_to_string(const nlohmann::json& value, std::string_view field_name)
    -> std::expected<std::string, std::string> {
    if (!value.is_string()) {
        return std::unexpected("Kafka field '" + std::string(field_name) + "' must be a string");
    }
    return value.get<std::string>();
}

inline auto table_from_json_payload(std::string_view payload,
                                    const std::vector<KafkaSchemaField>& schema)
    -> std::expected<ibex::runtime::Table, std::string> {
    using json = nlohmann::json;
    json object;
    try {
        object = json::parse(payload);
    } catch (const json::parse_error& e) {
        return std::unexpected("Kafka payload is not valid JSON: " + std::string(e.what()));
    }
    if (!object.is_object()) {
        return std::unexpected("Kafka payload must be a JSON object");
    }

    ibex::runtime::Table table;
    std::optional<std::string> time_index;
    for (const auto& field : schema) {
        auto it = object.find(field.name);
        if (it == object.end() || it->is_null()) {
            return std::unexpected("Kafka payload is missing required field '" + field.name + "'");
        }
        switch (field.kind) {
            case KafkaFieldKind::Int: {
                auto value = value_to_int64(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<std::int64_t>{*value});
                break;
            }
            case KafkaFieldKind::Double: {
                auto value = value_to_double(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<double>{*value});
                break;
            }
            case KafkaFieldKind::Bool: {
                auto value = value_to_bool(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<bool>{*value});
                break;
            }
            case KafkaFieldKind::String: {
                auto value = value_to_string(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                table.add_column(field.name, ibex::Column<std::string>{std::move(*value)});
                break;
            }
            case KafkaFieldKind::Categorical: {
                auto value = value_to_string(*it, field.name);
                if (!value) {
                    return std::unexpected(value.error());
                }
                ibex::Column<ibex::Categorical> column;
                column.push_back(*value);
                table.add_column(field.name, std::move(column));
                break;
            }
            case KafkaFieldKind::Date: {
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
            case KafkaFieldKind::Timestamp: {
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

}  // namespace ibex_kafka
