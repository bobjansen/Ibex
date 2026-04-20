#include <catch2/catch_test_macros.hpp>

#include <schema_registry.hpp>

TEST_CASE("Schema registry entry parser handles Avro response") {
    auto parsed = ibex_kafka::parse_schema_registry_entry(
        R"({"schema":"{\"type\":\"record\",\"name\":\"tick\"}","schemaType":"AVRO","id":17})");
    REQUIRE(parsed);
    REQUIRE(parsed->id == 17);
    REQUIRE(parsed->type == ibex_kafka::RegistrySchemaType::Avro);
    REQUIRE(parsed->schema == R"({"type":"record","name":"tick"})");
    REQUIRE(parsed->references.empty());
}

TEST_CASE("Schema registry entry parser handles Protobuf references") {
    auto parsed = ibex_kafka::parse_schema_registry_entry(R"({
        "schema":"syntax = \"proto3\"; message Tick { string symbol = 1; }",
        "schemaType":"PROTOBUF",
        "references":[{"name":"common.proto","subject":"common-value","version":3}]
    })");
    REQUIRE(parsed);
    REQUIRE(parsed->type == ibex_kafka::RegistrySchemaType::Protobuf);
    REQUIRE(parsed->references.size() == 1);
    REQUIRE(parsed->references[0].name == "common.proto");
    REQUIRE(parsed->references[0].subject == "common-value");
    REQUIRE(parsed->references[0].version == 3);
}

TEST_CASE("Schema registry supported types parser handles Redpanda response") {
    auto parsed =
        ibex_kafka::parse_schema_registry_supported_types(R"(["JSON","PROTOBUF","AVRO"])");
    REQUIRE(parsed);
    REQUIRE(parsed->size() == 3);
    REQUIRE((*parsed)[0] == ibex_kafka::RegistrySchemaType::Json);
    REQUIRE((*parsed)[1] == ibex_kafka::RegistrySchemaType::Protobuf);
    REQUIRE((*parsed)[2] == ibex_kafka::RegistrySchemaType::Avro);
}

TEST_CASE("Schema registry wire parser extracts schema id and payload") {
    const std::string payload = std::string("\x00\x00\x00\x00\x2A", 5) + std::string("hello", 5);
    auto parsed = ibex_kafka::parse_schema_registry_wire_message(payload);
    REQUIRE(parsed);
    REQUIRE(parsed->schema_id == 42);
    REQUIRE(parsed->payload == "hello");
}

TEST_CASE("Schema registry wire parser rejects bad payloads") {
    auto too_short = ibex_kafka::parse_schema_registry_wire_message("abcd");
    REQUIRE_FALSE(too_short);

    const std::string bad_magic = std::string("\x01\x00\x00\x00\x01", 5) + "x";
    auto wrong_magic = ibex_kafka::parse_schema_registry_wire_message(bad_magic);
    REQUIRE_FALSE(wrong_magic);
}
