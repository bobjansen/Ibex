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

inline auto kafka_avro_unsupported(std::string message) -> std::string {
    return "Kafka Avro v1 does not support " + std::move(message);
}

inline auto avro_node_type_name(avro::Type type) -> std::string {
    if (type == avro::AVRO_SYMBOLIC) {
        return "symbolic";
    }
    return avro::toString(type);
}

inline auto dereference_symbolic_avro_node(const avro::NodePtr& node) -> avro::NodePtr {
    if (node != nullptr && node->type() == avro::AVRO_SYMBOLIC && node->leaves() > 0) {
        return node->leafAt(0);
    }
    return node;
}

inline auto kafka_avro_validate_flat_field(const avro::NodePtr& node, std::string_view field_name)
    -> std::expected<void, std::string> {
    const avro::NodePtr resolved = dereference_symbolic_avro_node(node);
    if (resolved == nullptr) {
        return std::unexpected(kafka_avro_unsupported(std::string("empty schema node for field '") +
                                                      std::string(field_name) + "'"));
    }

    switch (resolved->type()) {
        case avro::AVRO_RECORD:
            return std::unexpected(kafka_avro_unsupported(std::string("nested record field '") +
                                                          std::string(field_name) + "'"));
        case avro::AVRO_ARRAY:
            return std::unexpected(kafka_avro_unsupported(std::string("array field '") +
                                                          std::string(field_name) + "'"));
        case avro::AVRO_MAP:
            return std::unexpected(
                kafka_avro_unsupported(std::string("map field '") + std::string(field_name) + "'"));
        case avro::AVRO_UNION:
            return std::unexpected(
                kafka_avro_unsupported(std::string("union field '") + std::string(field_name) +
                                       "'; nullable unions are not supported yet"));
        case avro::AVRO_FIXED:
            return std::unexpected(kafka_avro_unsupported(std::string("fixed field '") +
                                                          std::string(field_name) + "'"));
        case avro::AVRO_BYTES:
            return std::unexpected(kafka_avro_unsupported(std::string("bytes field '") +
                                                          std::string(field_name) + "'"));
        case avro::AVRO_NULL:
            return std::unexpected(kafka_avro_unsupported(std::string("null field '") +
                                                          std::string(field_name) + "'"));
        default:
            return {};
    }
}

inline auto kafka_avro_logical_type_name(avro::LogicalType::Type type) -> std::string {
    switch (type) {
        case avro::LogicalType::NONE:
            return "none";
        case avro::LogicalType::BIG_DECIMAL:
            return "big-decimal";
        case avro::LogicalType::DECIMAL:
            return "decimal";
        case avro::LogicalType::DATE:
            return "date";
        case avro::LogicalType::TIME_MILLIS:
            return "time-millis";
        case avro::LogicalType::TIME_MICROS:
            return "time-micros";
        case avro::LogicalType::TIMESTAMP_MILLIS:
            return "timestamp-millis";
        case avro::LogicalType::TIMESTAMP_MICROS:
            return "timestamp-micros";
        case avro::LogicalType::TIMESTAMP_NANOS:
            return "timestamp-nanos";
        case avro::LogicalType::LOCAL_TIMESTAMP_MILLIS:
            return "local-timestamp-millis";
        case avro::LogicalType::LOCAL_TIMESTAMP_MICROS:
            return "local-timestamp-micros";
        case avro::LogicalType::LOCAL_TIMESTAMP_NANOS:
            return "local-timestamp-nanos";
        case avro::LogicalType::DURATION:
            return "duration";
        case avro::LogicalType::UUID:
            return "uuid";
        case avro::LogicalType::CUSTOM:
            return "custom";
    }
    return "unknown";
}

inline auto kafka_avro_field_type_error(std::string_view field_name, std::string_view expected,
                                        const avro::NodePtr& node)
    -> std::expected<void, std::string> {
    return std::unexpected("Avro field '" + std::string(field_name) + "' must be " +
                           std::string(expected) + " for the requested Ibex schema, got " +
                           avro_node_type_name(node->type()));
}

inline auto validate_kafka_avro_field_compatibility(const KafkaSchemaField& field,
                                                    const avro::NodePtr& node)
    -> std::expected<void, std::string> {
    const avro::Type type = node->type();
    const auto logical_type = node->logicalType().type();

    switch (field.kind) {
        case KafkaFieldKind::Int:
            if (type != avro::AVRO_INT && type != avro::AVRO_LONG) {
                return kafka_avro_field_type_error(field.name, "int/long", node);
            }
            if (logical_type != avro::LogicalType::NONE) {
                return std::unexpected(
                    "Avro field '" + field.name + "' has unsupported logical type '" +
                    kafka_avro_logical_type_name(logical_type) + "' for requested Int");
            }
            return {};
        case KafkaFieldKind::Double:
            if (type != avro::AVRO_INT && type != avro::AVRO_LONG && type != avro::AVRO_FLOAT &&
                type != avro::AVRO_DOUBLE) {
                return kafka_avro_field_type_error(field.name, "numeric", node);
            }
            if (logical_type != avro::LogicalType::NONE) {
                return std::unexpected(
                    "Avro field '" + field.name + "' has unsupported logical type '" +
                    kafka_avro_logical_type_name(logical_type) + "' for requested Float64");
            }
            return {};
        case KafkaFieldKind::Bool:
            if (type != avro::AVRO_BOOL) {
                return kafka_avro_field_type_error(field.name, "boolean", node);
            }
            return {};
        case KafkaFieldKind::String:
        case KafkaFieldKind::Categorical:
            if (type != avro::AVRO_STRING && type != avro::AVRO_ENUM) {
                return kafka_avro_field_type_error(field.name, "string/enum", node);
            }
            return {};
        case KafkaFieldKind::Date:
            if (type != avro::AVRO_INT && type != avro::AVRO_LONG) {
                return kafka_avro_field_type_error(field.name, "int/long days", node);
            }
            if (logical_type != avro::LogicalType::NONE &&
                logical_type != avro::LogicalType::DATE) {
                return std::unexpected(
                    "Avro field '" + field.name + "' has unsupported logical type '" +
                    kafka_avro_logical_type_name(logical_type) + "' for requested Date");
            }
            return {};
        case KafkaFieldKind::Timestamp:
            if (type != avro::AVRO_INT && type != avro::AVRO_LONG) {
                return kafka_avro_field_type_error(field.name, "int/long timestamp", node);
            }
            switch (logical_type) {
                case avro::LogicalType::NONE:
                case avro::LogicalType::TIMESTAMP_MILLIS:
                case avro::LogicalType::TIMESTAMP_MICROS:
                case avro::LogicalType::TIMESTAMP_NANOS:
                case avro::LogicalType::LOCAL_TIMESTAMP_MILLIS:
                case avro::LogicalType::LOCAL_TIMESTAMP_MICROS:
                case avro::LogicalType::LOCAL_TIMESTAMP_NANOS:
                    return {};
                default:
                    return std::unexpected(
                        "Avro field '" + field.name + "' has unsupported logical type '" +
                        kafka_avro_logical_type_name(logical_type) + "' for requested Timestamp");
            }
    }
    return {};
}

inline auto validate_kafka_avro_v1_schema(const avro::ValidSchema& writer_schema,
                                          const std::vector<KafkaSchemaField>& requested_schema)
    -> std::expected<void, std::string> {
    avro::NodePtr root = dereference_symbolic_avro_node(writer_schema.root());
    if (root == nullptr) {
        return std::unexpected(kafka_avro_unsupported("empty writer schema"));
    }
    if (root->type() != avro::AVRO_RECORD) {
        return std::unexpected(kafka_avro_unsupported(
            std::string("top-level Avro ") + avro_node_type_name(root->type()) +
            " schemas; the Schema Registry value schema must be a record"));
    }

    for (std::size_t i = 0; i < root->leaves(); ++i) {
        auto valid = kafka_avro_validate_flat_field(root->leafAt(i), root->nameAt(i));
        if (!valid) {
            return std::unexpected(valid.error());
        }
    }

    for (const auto& field : requested_schema) {
        std::size_t index = 0;
        if (!root->nameIndex(field.name, index)) {
            return std::unexpected("Avro writer schema is missing required field '" + field.name +
                                   "'");
        }
        avro::NodePtr node = dereference_symbolic_avro_node(root->leafAt(index));
        auto compatible = validate_kafka_avro_field_compatibility(field, node);
        if (!compatible) {
            return std::unexpected(compatible.error());
        }
    }
    return {};
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
    auto valid_schema = validate_kafka_avro_v1_schema(**writer_schema, schema);
    if (!valid_schema) {
        return std::unexpected(valid_schema.error());
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
