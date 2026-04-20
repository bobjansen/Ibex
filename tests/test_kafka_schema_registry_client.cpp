#include <catch2/catch_test_macros.hpp>

#include "schema_registry_client.hpp"

TEST_CASE("Schema registry client normalizes base URLs") {
    auto normalized = ibex_kafka::normalize_schema_registry_url("  http://127.0.0.1:8081///  ");
    REQUIRE(normalized);
    REQUIRE(*normalized == "http://127.0.0.1:8081");
}

TEST_CASE("Schema registry client builds schema and type URLs") {
    auto schema_url = ibex_kafka::schema_registry_schema_by_id_url("http://registry:8081/", 42);
    REQUIRE(schema_url);
    REQUIRE(*schema_url == "http://registry:8081/schemas/ids/42");

    auto types_url = ibex_kafka::schema_registry_supported_types_url("http://registry:8081/");
    REQUIRE(types_url);
    REQUIRE(*types_url == "http://registry:8081/schemas/types");
}

TEST_CASE("Schema registry client caches schemas by id") {
    int calls = 0;
    ibex_kafka::SchemaRegistryClient client(
        "http://registry:8081", {},
        [&](std::string_view url) -> std::expected<std::string, std::string> {
            ++calls;
            REQUIRE(url == "http://registry:8081/schemas/ids/17");
            return R"({"schema":"{\"type\":\"record\",\"name\":\"tick\"}","schemaType":"AVRO"})";
        });

    auto first = client.fetch_schema_by_id(17);
    REQUIRE(first);
    REQUIRE(first->id == 17);
    REQUIRE(first->type == ibex_kafka::RegistrySchemaType::Avro);

    auto second = client.fetch_schema_by_id(17);
    REQUIRE(second);
    REQUIRE(second->id == 17);
    REQUIRE(calls == 1);
}

TEST_CASE("Schema registry client rejects mismatched schema ids") {
    ibex_kafka::SchemaRegistryClient client(
        "http://registry:8081", {},
        [](std::string_view) -> std::expected<std::string, std::string> {
            return R"({"id":18,"schema":"message Tick {}","schemaType":"PROTOBUF"})";
        });

    auto fetched = client.fetch_schema_by_id(17);
    REQUIRE_FALSE(fetched);
    REQUIRE(fetched.error().find("request id 17") != std::string::npos);
}

TEST_CASE("Schema registry client fetches supported types") {
    ibex_kafka::SchemaRegistryClient client(
        "http://registry:8081/", {},
        [](std::string_view url) -> std::expected<std::string, std::string> {
            REQUIRE(url == "http://registry:8081/schemas/types");
            return R"(["JSON","PROTOBUF","AVRO"])";
        });

    auto types = client.fetch_supported_types();
    REQUIRE(types);
    REQUIRE(types->size() == 3);
    REQUIRE((*types)[0] == ibex_kafka::RegistrySchemaType::Json);
    REQUIRE((*types)[1] == ibex_kafka::RegistrySchemaType::Protobuf);
    REQUIRE((*types)[2] == ibex_kafka::RegistrySchemaType::Avro);
}
