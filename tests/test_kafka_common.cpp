#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <kafka_common.hpp>
#include <nlohmann/json.hpp>

namespace {

auto get_i64(const ibex::runtime::Table& table, const char* name)
    -> const ibex::Column<std::int64_t>& {
    const auto* col = std::get_if<ibex::Column<std::int64_t>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

auto get_f64(const ibex::runtime::Table& table, const char* name) -> const ibex::Column<double>& {
    const auto* col = std::get_if<ibex::Column<double>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

auto get_bool(const ibex::runtime::Table& table, const char* name) -> const ibex::Column<bool>& {
    const auto* col = std::get_if<ibex::Column<bool>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

auto get_string(const ibex::runtime::Table& table, const char* name)
    -> const ibex::Column<std::string>& {
    const auto* col = std::get_if<ibex::Column<std::string>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

auto get_cat(const ibex::runtime::Table& table, const char* name)
    -> const ibex::Column<ibex::Categorical>& {
    const auto* col = std::get_if<ibex::Column<ibex::Categorical>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

auto get_date(const ibex::runtime::Table& table, const char* name)
    -> const ibex::Column<ibex::Date>& {
    const auto* col = std::get_if<ibex::Column<ibex::Date>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

auto get_ts(const ibex::runtime::Table& table, const char* name)
    -> const ibex::Column<ibex::Timestamp>& {
    const auto* col = std::get_if<ibex::Column<ibex::Timestamp>>(table.find(name));
    REQUIRE(col != nullptr);
    return *col;
}

}  // namespace

TEST_CASE("Kafka schema parser accepts explicit typed fields") {
    auto schema =
        ibex_kafka::parse_kafka_schema("ts:timestamp, price:f64, size:int, ok:bool, symbol:cat");
    REQUIRE(schema);
    REQUIRE(schema->size() == 5);
    REQUIRE((*schema)[0].name == "ts");
    REQUIRE((*schema)[0].kind == ibex_kafka::KafkaFieldKind::Timestamp);
    REQUIRE((*schema)[1].kind == ibex_kafka::KafkaFieldKind::Double);
    REQUIRE((*schema)[2].kind == ibex_kafka::KafkaFieldKind::Int);
    REQUIRE((*schema)[3].kind == ibex_kafka::KafkaFieldKind::Bool);
    REQUIRE((*schema)[4].kind == ibex_kafka::KafkaFieldKind::Categorical);
}

TEST_CASE("Kafka schema parser rejects duplicate or malformed fields") {
    auto duplicate = ibex_kafka::parse_kafka_schema("symbol:str, symbol:cat");
    REQUIRE_FALSE(duplicate);
    REQUIRE(duplicate.error() == "Kafka schema contains duplicate field 'symbol'");

    auto malformed = ibex_kafka::parse_kafka_schema("symbol");
    REQUIRE_FALSE(malformed);
    REQUIRE_THAT(malformed.error(), Catch::Matchers::ContainsSubstring("name:type"));
}

TEST_CASE("Kafka consumer options parser keeps poll timeout and consumer config") {
    auto options = ibex_kafka::parse_kafka_consumer_options(
        "poll_timeout_ms=25; consumer.auto.offset.reset=earliest\nconsumer.fetch.wait.max.ms=5");
    REQUIRE(options);
    REQUIRE(options->poll_timeout_ms == 25);
    REQUIRE(options->config.size() == 2);
    REQUIRE(options->config[0].first == "auto.offset.reset");
    REQUIRE(options->config[0].second == "earliest");
    REQUIRE(options->config[1].first == "fetch.wait.max.ms");
    REQUIRE(options->config[1].second == "5");
}

TEST_CASE("Kafka producer options parser rejects unsupported keys") {
    auto options = ibex_kafka::parse_kafka_producer_options("acks=all");
    REQUIRE_FALSE(options);
    REQUIRE_THAT(options.error(),
                 Catch::Matchers::ContainsSubstring("unsupported Kafka producer option"));
}

TEST_CASE("Kafka JSON payload converts to a typed one-row table") {
    auto schema = ibex_kafka::parse_kafka_schema(
        "price:f64,size:i64,ok:bool,symbol:str,venue:cat,day:date,ts:timestamp");
    REQUIRE(schema);

    auto table = ibex_kafka::table_from_json_payload(
        R"({"price":101.5,"size":42,"ok":true,"symbol":"AAPL","venue":"XNAS","day":20123,"ts":123456789})",
        *schema);
    REQUIRE(table);
    REQUIRE(table->rows() == 1);
    REQUIRE(get_f64(*table, "price")[0] == Catch::Approx(101.5));
    REQUIRE(get_i64(*table, "size")[0] == 42);
    REQUIRE(get_bool(*table, "ok")[0]);
    REQUIRE(get_string(*table, "symbol")[0] == "AAPL");
    REQUIRE(std::string(get_cat(*table, "venue")[0]) == "XNAS");
    REQUIRE(get_date(*table, "day")[0].days == 20123);
    REQUIRE(get_ts(*table, "ts")[0].nanos == 123456789);
    REQUIRE(table->time_index.has_value());
    REQUIRE(*table->time_index == "day");
}

TEST_CASE("Kafka JSON payload rejects missing required fields") {
    auto schema = ibex_kafka::parse_kafka_schema("symbol:str,price:f64");
    REQUIRE(schema);

    auto table = ibex_kafka::table_from_json_payload(R"({"symbol":"AAPL"})", *schema);
    REQUIRE_FALSE(table);
    REQUIRE(table.error() == "Kafka payload is missing required field 'price'");
}

TEST_CASE("Kafka JSON payload rejects wrong field types") {
    auto schema = ibex_kafka::parse_kafka_schema("size:i64");
    REQUIRE(schema);

    auto table = ibex_kafka::table_from_json_payload(R"({"size":"42"})", *schema);
    REQUIRE_FALSE(table);
    REQUIRE(table.error() == "Kafka field 'size' must be an integer");
}

TEST_CASE("Kafka table rows serialize supported columns and nulls to JSON") {
    ibex::runtime::Table table;
    table.add_column("price", ibex::Column<double>{101.5, 102.0});
    table.add_column("size", ibex::Column<std::int64_t>{42, 43});
    table.add_column("ok", ibex::Column<bool>{true, false});
    table.add_column("symbol", ibex::Column<std::string>{"AAPL", "MSFT"});
    ibex::Column<ibex::Categorical> venue;
    venue.push_back("XNAS");
    venue.push_back("BATS");
    table.add_column("venue", std::move(venue));
    table.add_column("day", ibex::Column<ibex::Date>{ibex::Date{20123}, ibex::Date{20124}});
    table.add_column("ts", ibex::Column<ibex::Timestamp>{ibex::Timestamp{123456789},
                                                         ibex::Timestamp{223456789}});
    ibex::runtime::ValidityBitmap validity(2, true);
    validity.set(1, false);
    table.add_column("note", ibex::Column<std::string>{"open", "close"}, std::move(validity));

    auto first = ibex_kafka::table_row_to_json(table, 0);
    REQUIRE(first);
    auto first_json = nlohmann::json::parse(*first);
    REQUIRE(first_json["day"] == 20123);
    REQUIRE(first_json["note"] == "open");
    REQUIRE(first_json["ok"] == true);
    REQUIRE(first_json["price"] == Catch::Approx(101.5));
    REQUIRE(first_json["size"] == 42);
    REQUIRE(first_json["symbol"] == "AAPL");
    REQUIRE(first_json["ts"] == 123456789);
    REQUIRE(first_json["venue"] == "XNAS");

    auto second = ibex_kafka::table_row_to_json(table, 1);
    REQUIRE(second);
    auto second_json = nlohmann::json::parse(*second);
    REQUIRE(second_json["day"] == 20124);
    REQUIRE(second_json["note"].is_null());
    REQUIRE(second_json["ok"] == false);
    REQUIRE(second_json["price"] == Catch::Approx(102.0));
    REQUIRE(second_json["size"] == 43);
    REQUIRE(second_json["symbol"] == "MSFT");
    REQUIRE(second_json["ts"] == 223456789);
    REQUIRE(second_json["venue"] == "BATS");
}
