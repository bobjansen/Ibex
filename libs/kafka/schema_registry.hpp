#pragma once

#include <cstdint>
#include <expected>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ibex_kafka {

enum class RegistrySchemaType : std::uint8_t {
    Avro,
    Protobuf,
    Json,
};

struct RegistrySchemaReference {
    std::string name;
    std::string subject;
    std::int32_t version = 0;
};

struct RegistrySchemaEntry {
    std::int32_t id = 0;
    RegistrySchemaType type = RegistrySchemaType::Avro;
    std::string schema;
    std::vector<RegistrySchemaReference> references;
};

struct SchemaRegistryWireMessage {
    std::int32_t schema_id = 0;
    std::string_view payload;
};

inline auto parse_registry_schema_type(std::string_view text)
    -> std::expected<RegistrySchemaType, std::string> {
    if (text == "AVRO") {
        return RegistrySchemaType::Avro;
    }
    if (text == "PROTOBUF") {
        return RegistrySchemaType::Protobuf;
    }
    if (text == "JSON") {
        return RegistrySchemaType::Json;
    }
    return std::unexpected("unsupported schema registry type: " + std::string(text));
}

inline auto parse_schema_registry_entry(std::string_view body)
    -> std::expected<RegistrySchemaEntry, std::string> {
    using json = nlohmann::json;
    json object;
    try {
        object = json::parse(body);
    } catch (const json::parse_error& e) {
        return std::unexpected("schema registry response is not valid JSON: " +
                               std::string(e.what()));
    }
    if (!object.is_object()) {
        return std::unexpected("schema registry response must be a JSON object");
    }

    RegistrySchemaEntry entry;

    if (const auto id_it = object.find("id"); id_it != object.end()) {
        if (!id_it->is_number_integer()) {
            return std::unexpected("schema registry response field 'id' must be an integer");
        }
        entry.id = id_it->get<std::int32_t>();
    }

    const auto schema_it = object.find("schema");
    if (schema_it == object.end() || !schema_it->is_string()) {
        return std::unexpected("schema registry response must contain string field 'schema'");
    }
    entry.schema = schema_it->get<std::string>();

    std::string_view type_text = "AVRO";
    if (const auto type_it = object.find("schemaType"); type_it != object.end()) {
        if (!type_it->is_string()) {
            return std::unexpected("schema registry response field 'schemaType' must be a string");
        }
        type_text = type_it->get_ref<const std::string&>();
    }
    auto parsed_type = parse_registry_schema_type(type_text);
    if (!parsed_type) {
        return std::unexpected(parsed_type.error());
    }
    entry.type = *parsed_type;

    if (const auto refs_it = object.find("references"); refs_it != object.end()) {
        if (!refs_it->is_array()) {
            return std::unexpected("schema registry response field 'references' must be an array");
        }
        for (const auto& ref : *refs_it) {
            if (!ref.is_object()) {
                return std::unexpected("schema registry reference entries must be objects");
            }
            const auto name_it = ref.find("name");
            const auto subject_it = ref.find("subject");
            const auto version_it = ref.find("version");
            if (name_it == ref.end() || !name_it->is_string() || subject_it == ref.end() ||
                !subject_it->is_string() || version_it == ref.end() ||
                !version_it->is_number_integer()) {
                return std::unexpected(
                    "schema registry references require name, subject, and version");
            }
            entry.references.push_back({
                .name = name_it->get<std::string>(),
                .subject = subject_it->get<std::string>(),
                .version = version_it->get<std::int32_t>(),
            });
        }
    }

    return entry;
}

inline auto parse_schema_registry_supported_types(std::string_view body)
    -> std::expected<std::vector<RegistrySchemaType>, std::string> {
    using json = nlohmann::json;
    json value;
    try {
        value = json::parse(body);
    } catch (const json::parse_error& e) {
        return std::unexpected("schema registry types response is not valid JSON: " +
                               std::string(e.what()));
    }
    if (!value.is_array()) {
        return std::unexpected("schema registry types response must be a JSON array");
    }

    std::vector<RegistrySchemaType> types;
    for (const auto& item : value) {
        if (!item.is_string()) {
            return std::unexpected("schema registry types entries must be strings");
        }
        auto parsed = parse_registry_schema_type(item.get_ref<const std::string&>());
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        types.push_back(*parsed);
    }
    return types;
}

inline auto parse_schema_registry_wire_message(std::string_view bytes)
    -> std::expected<SchemaRegistryWireMessage, std::string> {
    if (bytes.size() < 5) {
        return std::unexpected("schema registry wire payload must be at least 5 bytes");
    }
    const auto* raw = reinterpret_cast<const unsigned char*>(bytes.data());
    if (raw[0] != 0) {
        return std::unexpected("unsupported schema registry wire magic byte");
    }
    const std::int32_t id =
        (static_cast<std::int32_t>(raw[1]) << 24) | (static_cast<std::int32_t>(raw[2]) << 16) |
        (static_cast<std::int32_t>(raw[3]) << 8) | static_cast<std::int32_t>(raw[4]);
    return SchemaRegistryWireMessage{.schema_id = id, .payload = bytes.substr(5)};
}

}  // namespace ibex_kafka
