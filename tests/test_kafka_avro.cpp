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
