#pragma once
// Kafka plugin — shared consumer/producer option parsing.
//
// The schema mini-language and JSON <-> Table conversion live in the shared
// plugin helper header <ibex/plugin/schema.hpp>; the aliases below keep the
// historical Kafka-prefixed names used throughout this plugin.

#include <ibex/plugin/schema.hpp>

#include <expected>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex_kafka {

using KafkaFieldKind = ibex::plugin::FieldKind;
using KafkaSchemaField = ibex::plugin::SchemaField;

using ibex::plugin::parse_key_value_options;
using ibex::plugin::table_from_json_payload;
using ibex::plugin::table_row_to_json;
using ibex::plugin::trim;

inline auto parse_kafka_field_kind(std::string_view type_str)
    -> std::expected<KafkaFieldKind, std::string> {
    return ibex::plugin::parse_field_kind(type_str);
}

inline auto parse_kafka_schema(std::string_view spec)
    -> std::expected<std::vector<KafkaSchemaField>, std::string> {
    return ibex::plugin::parse_schema(spec);
}

struct KafkaConsumerOptions {
    std::vector<std::pair<std::string, std::string>> config;
    int poll_timeout_ms = 200;
};

struct KafkaProducerOptions {
    std::vector<std::pair<std::string, std::string>> config;
    int flush_timeout_ms = 5000;
};

inline auto parse_kafka_consumer_options(std::string_view spec)
    -> std::expected<KafkaConsumerOptions, std::string> {
    KafkaConsumerOptions options;
    auto parsed = parse_key_value_options(spec);
    if (!parsed) {
        return std::unexpected(parsed.error());
    }
    for (auto& [key, value] : *parsed) {
        if (key == "poll_timeout_ms") {
            auto timeout = ibex::plugin::parse_non_negative_int(value, "poll_timeout_ms");
            if (!timeout) {
                return std::unexpected(timeout.error());
            }
            options.poll_timeout_ms = *timeout;
            continue;
        }
        if (key.starts_with("consumer.")) {
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
            auto timeout = ibex::plugin::parse_non_negative_int(value, "flush_timeout_ms");
            if (!timeout) {
                return std::unexpected(timeout.error());
            }
            options.flush_timeout_ms = *timeout;
            continue;
        }
        if (key.starts_with("producer.")) {
            options.config.emplace_back(key.substr(9), std::move(value));
            continue;
        }
        return std::unexpected("unsupported Kafka producer option: " + key);
    }
    return options;
}

}  // namespace ibex_kafka
