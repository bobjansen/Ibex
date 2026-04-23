#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Stream.hh>
#include <avro/Types.hh>
#include <cstdint>
#include <expected>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kafka_common.hpp"
#include "schema_registry_client.hpp"

namespace ibex_kafka {

struct KafkaAvroConsumerOptions {
    KafkaConsumerOptions consumer;
    SchemaRegistryClientOptions registry;
};

inline auto parse_kafka_avro_consumer_options(std::string_view spec)
    -> std::expected<KafkaAvroConsumerOptions, std::string> {
    KafkaAvroConsumerOptions options;
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
            options.consumer.poll_timeout_ms = timeout;
            continue;
        }
        if (key == "registry.connect_timeout_ms") {
            long timeout = 0;
            const auto* begin = value.data();
            const auto* end = value.data() + value.size();
            auto [ptr, ec] = std::from_chars(begin, end, timeout);
            if (ec != std::errc{} || ptr != end || timeout < 0) {
                return std::unexpected(
                    "registry.connect_timeout_ms must be a non-negative integer");
            }
            options.registry.connect_timeout_ms = timeout;
            continue;
        }
        if (key == "registry.request_timeout_ms") {
            long timeout = 0;
            const auto* begin = value.data();
            const auto* end = value.data() + value.size();
            auto [ptr, ec] = std::from_chars(begin, end, timeout);
            if (ec != std::errc{} || ptr != end || timeout < 0) {
                return std::unexpected(
                    "registry.request_timeout_ms must be a non-negative integer");
            }
            options.registry.request_timeout_ms = timeout;
            continue;
        }
        if (key.rfind("consumer.", 0) == 0) {
            options.consumer.config.emplace_back(key.substr(9), std::move(value));
            continue;
        }
        return std::unexpected("unsupported Kafka Avro consumer option: " + key);
    }
    return options;
}

class AvroSchemaCache {
   public:
    explicit AvroSchemaCache(std::string registry_url, SchemaRegistryClientOptions options = {},
                             SchemaRegistryClient::Getter getter = {})
        : client_(std::move(registry_url), options, std::move(getter)) {}

    auto schema_by_id(std::int32_t schema_id)
        -> std::expected<const avro::ValidSchema*, std::string> {
        auto it = compiled_.find(schema_id);
        if (it != compiled_.end()) {
            return &it->second;
        }

        auto entry = client_.fetch_schema_by_id(schema_id);
        if (!entry) {
            return std::unexpected(entry.error());
        }
        if (entry->type != RegistrySchemaType::Avro) {
            return std::unexpected("schema registry id " + std::to_string(schema_id) +
                                   " is not an Avro schema");
        }
        if (!entry->references.empty()) {
            return std::unexpected("Avro schema references are not supported yet");
        }

        try {
            auto schema = avro::compileJsonSchemaFromString(entry->schema);
            auto [inserted, _] = compiled_.emplace(schema_id, std::move(schema));
            return &inserted->second;
        } catch (const std::exception& e) {
            return std::unexpected("failed to compile Avro schema id " + std::to_string(schema_id) +
                                   ": " + std::string(e.what()));
        }
    }

   private:
    SchemaRegistryClient client_;
    std::unordered_map<std::int32_t, avro::ValidSchema> compiled_;
};

inline auto unwrap_avro_datum(const avro::GenericDatum& datum, std::string_view field_name)
    -> std::expected<std::reference_wrapper<const avro::GenericDatum>, std::string> {
    const avro::GenericDatum* current = &datum;
    while (current->type() == avro::AVRO_UNION) {
        const auto& uni = current->value<avro::GenericUnion>();
        current = &uni.datum();
    }
    if (current->type() == avro::AVRO_NULL) {
        return std::unexpected("Avro field '" + std::string(field_name) +
                               "' resolved to null, which is not supported in Kafka Avro v1");
    }
    return std::cref(*current);
}

inline auto avro_to_int64(const avro::GenericDatum& datum, std::string_view field_name)
    -> std::expected<std::int64_t, std::string> {
    switch (datum.type()) {
        case avro::AVRO_INT:
            return datum.value<std::int32_t>();
        case avro::AVRO_LONG:
            return datum.value<std::int64_t>();
        default:
            return std::unexpected("Avro field '" + std::string(field_name) +
                                   "' must be int/long, got " + avro::toString(datum.type()));
    }
}

inline auto avro_to_double(const avro::GenericDatum& datum, std::string_view field_name)
    -> std::expected<double, std::string> {
    switch (datum.type()) {
        case avro::AVRO_INT:
            return static_cast<double>(datum.value<std::int32_t>());
        case avro::AVRO_LONG:
            return static_cast<double>(datum.value<std::int64_t>());
        case avro::AVRO_FLOAT:
            return static_cast<double>(datum.value<float>());
        case avro::AVRO_DOUBLE:
            return datum.value<double>();
        default:
            return std::unexpected("Avro field '" + std::string(field_name) +
                                   "' must be numeric, got " + avro::toString(datum.type()));
    }
}

inline auto avro_to_bool(const avro::GenericDatum& datum, std::string_view field_name)
    -> std::expected<bool, std::string> {
    if (datum.type() != avro::AVRO_BOOL) {
        return std::unexpected("Avro field '" + std::string(field_name) +
                               "' must be boolean, got " + avro::toString(datum.type()));
    }
    return datum.value<bool>();
}

inline auto avro_to_string(const avro::GenericDatum& datum, std::string_view field_name)
    -> std::expected<std::string, std::string> {
    switch (datum.type()) {
        case avro::AVRO_STRING:
            return datum.value<std::string>();
        case avro::AVRO_ENUM:
            return datum.value<avro::GenericEnum>().symbol();
        default:
            return std::unexpected("Avro field '" + std::string(field_name) +
                                   "' must be string/enum, got " + avro::toString(datum.type()));
    }
}

inline auto mul_checked(std::int64_t value, std::int64_t factor, std::string_view field_name)
    -> std::expected<std::int64_t, std::string> {
    const __int128 widened = static_cast<__int128>(value) * static_cast<__int128>(factor);
    if (widened < std::numeric_limits<std::int64_t>::min() ||
        widened > std::numeric_limits<std::int64_t>::max()) {
        return std::unexpected("Avro timestamp field '" + std::string(field_name) +
                               "' overflows int64 nanoseconds");
    }
    return static_cast<std::int64_t>(widened);
}

inline auto avro_to_timestamp_nanos(const avro::GenericDatum& datum, std::string_view field_name)
    -> std::expected<std::int64_t, std::string> {
    auto raw = avro_to_int64(datum, field_name);
    if (!raw) {
        return std::unexpected(raw.error());
    }
    switch (datum.logicalType().type()) {
        case avro::LogicalType::NONE:
        case avro::LogicalType::TIMESTAMP_NANOS:
        case avro::LogicalType::LOCAL_TIMESTAMP_NANOS:
            return *raw;
        case avro::LogicalType::TIMESTAMP_MICROS:
        case avro::LogicalType::LOCAL_TIMESTAMP_MICROS:
            return mul_checked(*raw, 1000, field_name);
        case avro::LogicalType::TIMESTAMP_MILLIS:
        case avro::LogicalType::LOCAL_TIMESTAMP_MILLIS:
            return mul_checked(*raw, 1000 * 1000, field_name);
        default:
            return std::unexpected("Avro field '" + std::string(field_name) +
                                   "' has unsupported logical type for Timestamp");
    }
}

inline auto table_from_avro_record(const avro::GenericDatum& datum,
                                   const std::vector<KafkaSchemaField>& schema)
    -> std::expected<ibex::runtime::Table, std::string> {
    if (datum.type() != avro::AVRO_RECORD) {
        return std::unexpected("Kafka Avro payload must decode to a record, got " +
                               avro::toString(datum.type()));
    }

    const auto& record = datum.value<avro::GenericRecord>();
    ibex::runtime::Table table;
    std::optional<std::string> time_index;

    for (const auto& field : schema) {
        if (!record.hasField(field.name)) {
            return std::unexpected("Avro payload is missing required field '" + field.name + "'");
        }
        auto value = unwrap_avro_datum(record.field(field.name), field.name);
        if (!value) {
            return std::unexpected(value.error());
        }
        const avro::GenericDatum& unwrapped = value->get();

        switch (field.kind) {
            case KafkaFieldKind::Int: {
                auto scalar = avro_to_int64(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                table.add_column(field.name, ibex::Column<std::int64_t>{*scalar});
                break;
            }
            case KafkaFieldKind::Double: {
                auto scalar = avro_to_double(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                table.add_column(field.name, ibex::Column<double>{*scalar});
                break;
            }
            case KafkaFieldKind::Bool: {
                auto scalar = avro_to_bool(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                table.add_column(field.name, ibex::Column<bool>{*scalar});
                break;
            }
            case KafkaFieldKind::String: {
                auto scalar = avro_to_string(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                table.add_column(field.name, ibex::Column<std::string>{std::move(*scalar)});
                break;
            }
            case KafkaFieldKind::Categorical: {
                auto scalar = avro_to_string(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                ibex::Column<ibex::Categorical> column;
                column.push_back(*scalar);
                table.add_column(field.name, std::move(column));
                break;
            }
            case KafkaFieldKind::Date: {
                auto scalar = avro_to_int64(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                table.add_column(field.name, ibex::Column<ibex::Date>{
                                                 ibex::Date{static_cast<std::int32_t>(*scalar)}});
                if (!time_index.has_value()) {
                    time_index = field.name;
                }
                break;
            }
            case KafkaFieldKind::Timestamp: {
                auto scalar = avro_to_timestamp_nanos(unwrapped, field.name);
                if (!scalar) {
                    return std::unexpected(scalar.error());
                }
                table.add_column(field.name,
                                 ibex::Column<ibex::Timestamp>{ibex::Timestamp{*scalar}});
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

inline auto table_from_avro_registry_payload(std::string_view payload,
                                             const std::vector<KafkaSchemaField>& schema,
                                             AvroSchemaCache& cache)
    -> std::expected<ibex::runtime::Table, std::string> {
    auto wire = parse_schema_registry_wire_message(payload);
    if (!wire) {
        return std::unexpected(wire.error());
    }

    auto writer_schema = cache.schema_by_id(wire->schema_id);
    if (!writer_schema) {
        return std::unexpected(writer_schema.error());
    }

    try {
        auto input = avro::memoryInputStream(reinterpret_cast<const uint8_t*>(wire->payload.data()),
                                             wire->payload.size());
        auto decoder = avro::binaryDecoder();
        decoder->init(*input);

        avro::GenericDatum datum(**writer_schema);
        avro::GenericReader reader(**writer_schema, decoder);
        reader.read(datum);
        reader.drain();
        return table_from_avro_record(datum, schema);
    } catch (const std::exception& e) {
        return std::unexpected("failed to decode Kafka Avro payload: " + std::string(e.what()));
    }
}

}  // namespace ibex_kafka
