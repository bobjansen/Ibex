#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/GenericDatum.hh>
#include <avro/Stream.hh>

#include "kafka_avro.hpp"

namespace {

auto make_wire_payload(std::int32_t schema_id, std::string_view payload) -> std::string {
    std::string wire;
    wire.push_back('\0');
    wire.push_back(static_cast<char>((schema_id >> 24) & 0xff));
    wire.push_back(static_cast<char>((schema_id >> 16) & 0xff));
    wire.push_back(static_cast<char>((schema_id >> 8) & 0xff));
    wire.push_back(static_cast<char>(schema_id & 0xff));
    wire.append(payload);
    return wire;
}

auto encode_tick_record() -> std::string {
    static const std::string schema_json = R"({
      "type":"record",
      "name":"tick",
      "fields":[
        {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        {"name":"symbol","type":"string"},
        {"name":"price","type":"double"},
        {"name":"size","type":"long"},
        {"name":"active","type":"boolean"}
      ]
    })";

    auto schema = avro::compileJsonSchemaFromString(schema_json);
    avro::GenericDatum datum(schema);
    auto& record = datum.value<avro::GenericRecord>();
    record.field("ts") = avro::GenericDatum(std::int64_t{1710000000123});
    record.field("symbol") = avro::GenericDatum(std::string("AAPL"));
    record.field("price") = avro::GenericDatum(187.25);
    record.field("size") = avro::GenericDatum(std::int64_t{42});
    record.field("active") = avro::GenericDatum(true);

    auto out = avro::memoryOutputStream();
    auto encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::GenericWriter writer(schema, encoder);
    writer.write(datum);
    encoder->flush();

    auto bytes = avro::snapshot(*out);
    return std::string(reinterpret_cast<const char*>(bytes->data()), bytes->size());
}

}  // namespace

TEST_CASE("Kafka Avro consumer options parser handles registry timeouts") {
    auto parsed = ibex_kafka::parse_kafka_avro_consumer_options(
        "poll_timeout_ms=150;registry.connect_timeout_ms=250;registry.request_timeout_ms=500;"
        "consumer.auto.offset.reset=latest");
    REQUIRE(parsed);
    REQUIRE(parsed->consumer.poll_timeout_ms == 150);
    REQUIRE(parsed->registry.connect_timeout_ms == 250);
    REQUIRE(parsed->registry.request_timeout_ms == 500);
    REQUIRE(parsed->consumer.config.size() == 1);
    REQUIRE(parsed->consumer.config[0].first == "auto.offset.reset");
    REQUIRE(parsed->consumer.config[0].second == "latest");
}

TEST_CASE("Kafka Avro payload decodes flat schema-registry-framed records") {
    static const std::string registry_response = R"({
      "schema":"{\"type\":\"record\",\"name\":\"tick\",\"fields\":[{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"size\",\"type\":\"long\"},{\"name\":\"active\",\"type\":\"boolean\"}]}",
      "schemaType":"AVRO",
      "id":17
    })";

    ibex_kafka::AvroSchemaCache cache(
        "http://registry:8081", {},
        [&](std::string_view url) -> std::expected<std::string, std::string> {
            REQUIRE(url == "http://registry:8081/schemas/ids/17");
            return registry_response;
        });

    const std::string payload = make_wire_payload(17, encode_tick_record());
    auto table = ibex_kafka::table_from_avro_registry_payload(
        payload,
        {
            {.name = "ts", .kind = ibex_kafka::KafkaFieldKind::Timestamp},
            {.name = "symbol", .kind = ibex_kafka::KafkaFieldKind::String},
            {.name = "price", .kind = ibex_kafka::KafkaFieldKind::Double},
            {.name = "size", .kind = ibex_kafka::KafkaFieldKind::Int},
            {.name = "active", .kind = ibex_kafka::KafkaFieldKind::Bool},
        },
        cache);

    REQUIRE(table);
    REQUIRE(table->rows() == 1);
    REQUIRE(table->time_index == std::optional<std::string>{"ts"});

    const auto* ts = std::get_if<ibex::Column<ibex::Timestamp>>(table->find("ts"));
    const auto* symbol = std::get_if<ibex::Column<std::string>>(table->find("symbol"));
    const auto* price = std::get_if<ibex::Column<double>>(table->find("price"));
    const auto* size = std::get_if<ibex::Column<std::int64_t>>(table->find("size"));
    const auto* active = std::get_if<ibex::Column<bool>>(table->find("active"));

    REQUIRE(ts != nullptr);
    REQUIRE(symbol != nullptr);
    REQUIRE(price != nullptr);
    REQUIRE(size != nullptr);
    REQUIRE(active != nullptr);

    REQUIRE((*ts)[0].nanos == 1710000000123LL * 1000LL * 1000LL);
    REQUIRE((*symbol)[0] == "AAPL");
    REQUIRE((*price)[0] == Catch::Approx(187.25));
    REQUIRE((*size)[0] == 42);
    REQUIRE((*active)[0]);
}

TEST_CASE("Kafka Avro payload preserves transient registry failures") {
    ibex_kafka::AvroSchemaCache cache(
        "http://registry:8081", {},
        [](std::string_view) -> std::expected<std::string, std::string> {
            return std::unexpected(
                ibex_kafka::make_transient_schema_registry_error("registry unavailable"));
        });

    const std::string payload = make_wire_payload(17, "not decoded before schema lookup");
    auto table = ibex_kafka::table_from_avro_registry_payload(
        payload,
        {
            {.name = "ts", .kind = ibex_kafka::KafkaFieldKind::Timestamp},
        },
        cache);

    REQUIRE_FALSE(table);
    REQUIRE(ibex_kafka::is_transient_schema_registry_error(table.error()));
}

TEST_CASE("Kafka Avro v1 schema validator rejects unsupported shapes clearly") {
    SECTION("top-level non-record schema") {
        auto schema = avro::compileJsonSchemaFromString(R"({
          "type":"array",
          "items":"long"
        })");
        auto valid = ibex_kafka::validate_kafka_avro_v1_schema(schema, {});
        REQUIRE_FALSE(valid);
        REQUIRE(valid.error().find("top-level Avro array schemas") != std::string::npos);
    }

    SECTION("array field") {
        auto schema = avro::compileJsonSchemaFromString(R"({
          "type":"record",
          "name":"tick",
          "fields":[{"name":"tags","type":{"type":"array","items":"string"}}]
        })");
        auto valid = ibex_kafka::validate_kafka_avro_v1_schema(
            schema, {{.name = "tags", .kind = ibex_kafka::KafkaFieldKind::String}});
        REQUIRE_FALSE(valid);
        REQUIRE(valid.error().find("array field 'tags'") != std::string::npos);
    }

    SECTION("nullable union field") {
        auto schema = avro::compileJsonSchemaFromString(R"({
          "type":"record",
          "name":"tick",
          "fields":[{"name":"size","type":["null","long"]}]
        })");
        auto valid = ibex_kafka::validate_kafka_avro_v1_schema(
            schema, {{.name = "size", .kind = ibex_kafka::KafkaFieldKind::Int}});
        REQUIRE_FALSE(valid);
        REQUIRE(valid.error().find("union field 'size'") != std::string::npos);
        REQUIRE(valid.error().find("nullable unions are not supported yet") != std::string::npos);
    }

    SECTION("missing requested field") {
        auto schema = avro::compileJsonSchemaFromString(R"({
          "type":"record",
          "name":"tick",
          "fields":[{"name":"price","type":"double"}]
        })");
        auto valid = ibex_kafka::validate_kafka_avro_v1_schema(
            schema, {{.name = "size", .kind = ibex_kafka::KafkaFieldKind::Int}});
        REQUIRE_FALSE(valid);
        REQUIRE(valid.error().find("missing required field 'size'") != std::string::npos);
    }

    SECTION("field type mismatch") {
        auto schema = avro::compileJsonSchemaFromString(R"({
          "type":"record",
          "name":"tick",
          "fields":[{"name":"price","type":"string"}]
        })");
        auto valid = ibex_kafka::validate_kafka_avro_v1_schema(
            schema, {{.name = "price", .kind = ibex_kafka::KafkaFieldKind::Double}});
        REQUIRE_FALSE(valid);
        REQUIRE(valid.error().find("must be numeric") != std::string::npos);
    }

    SECTION("unsupported logical type for requested timestamp") {
        auto schema = avro::compileJsonSchemaFromString(R"({
          "type":"record",
          "name":"tick",
          "fields":[{"name":"ts","type":{"type":"int","logicalType":"time-millis"}}]
        })");
        auto valid = ibex_kafka::validate_kafka_avro_v1_schema(
            schema, {{.name = "ts", .kind = ibex_kafka::KafkaFieldKind::Timestamp}});
        REQUIRE_FALSE(valid);
        REQUIRE(valid.error().find("unsupported logical type 'time-millis'") != std::string::npos);
    }
}
