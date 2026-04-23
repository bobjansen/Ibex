#pragma once

#include <ibex/runtime/extern_registry.hpp>

#include <librdkafka/rdkafka.h>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "kafka_avro.hpp"
#include "kafka_common.hpp"

namespace ibex_kafka {

struct ConsumerState {
    rd_kafka_t* consumer = nullptr;
    std::vector<KafkaSchemaField> schema;
    int poll_timeout_ms = 200;
    rd_kafka_message_t* pending_commit = nullptr;

    ConsumerState() = default;
    ConsumerState(const ConsumerState&) = delete;
    auto operator=(const ConsumerState&) -> ConsumerState& = delete;

    ~ConsumerState() {
        if (pending_commit != nullptr) {
            rd_kafka_message_destroy(pending_commit);
        }
        if (consumer != nullptr) {
            rd_kafka_consumer_close(consumer);
            rd_kafka_destroy(consumer);
        }
    }
};

struct ProducerState {
    rd_kafka_t* producer = nullptr;
    int flush_timeout_ms = 5000;

    ProducerState() = default;
    ProducerState(const ProducerState&) = delete;
    auto operator=(const ProducerState&) -> ProducerState& = delete;

    ~ProducerState() {
        if (producer != nullptr) {
            rd_kafka_flush(producer, flush_timeout_ms);
            rd_kafka_destroy(producer);
        }
    }
};

struct AvroConsumerState {
    rd_kafka_t* consumer = nullptr;
    std::vector<KafkaSchemaField> schema;
    int poll_timeout_ms = 200;
    rd_kafka_message_t* pending_commit = nullptr;
    AvroSchemaCache schema_cache;

    explicit AvroConsumerState(std::string registry_url, SchemaRegistryClientOptions options)
        : schema_cache(std::move(registry_url), options) {}

    AvroConsumerState(const AvroConsumerState&) = delete;
    auto operator=(const AvroConsumerState&) -> AvroConsumerState& = delete;

    ~AvroConsumerState() {
        if (pending_commit != nullptr) {
            rd_kafka_message_destroy(pending_commit);
        }
        if (consumer != nullptr) {
            rd_kafka_consumer_close(consumer);
            rd_kafka_destroy(consumer);
        }
    }
};

inline auto rd_kafka_error_message(std::string_view context, std::string_view message)
    -> std::string {
    std::string text(context);
    text += ": ";
    text += message;
    return text;
}

inline auto build_consumer_state(const std::string& brokers, const std::string& topic,
                                 const std::string& group, const std::string& schema_spec,
                                 const std::string& options_spec)
    -> std::expected<std::unique_ptr<ConsumerState>, std::string> {
    auto schema = parse_kafka_schema(schema_spec);
    if (!schema) {
        return std::unexpected(schema.error());
    }
    auto options = parse_kafka_consumer_options(options_spec);
    if (!options) {
        return std::unexpected(options.error());
    }

    std::unique_ptr<ConsumerState> state = std::make_unique<ConsumerState>();
    state->schema = std::move(*schema);
    state->poll_timeout_ms = options->poll_timeout_ms;

    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv bootstrap.servers", errstr));
    }
    if (rd_kafka_conf_set(conf, "group.id", group.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv group.id", errstr));
    }
    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv enable.auto.commit", errstr));
    }
    for (const auto& [key, value] : options->config) {
        if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            return std::unexpected(
                rd_kafka_error_message("kafka_recv option '" + key + "'", errstr));
        }
    }

    state->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (state->consumer == nullptr) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv consumer init", errstr));
    }
    rd_kafka_poll_set_consumer(state->consumer);

    rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic.c_str(), RD_KAFKA_PARTITION_UA);
    const rd_kafka_resp_err_t subscribe_err = rd_kafka_subscribe(state->consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);
    if (subscribe_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return std::unexpected("kafka_recv subscribe failed: " +
                               std::string(rd_kafka_err2str(subscribe_err)));
    }

    return state;
}

inline auto build_producer_state(const std::string& brokers, const std::string& options_spec)
    -> std::expected<std::unique_ptr<ProducerState>, std::string> {
    auto options = parse_kafka_producer_options(options_spec);
    if (!options) {
        return std::unexpected(options.error());
    }
    std::unique_ptr<ProducerState> state = std::make_unique<ProducerState>();
    state->flush_timeout_ms = options->flush_timeout_ms;

    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_send bootstrap.servers", errstr));
    }
    for (const auto& [key, value] : options->config) {
        if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            return std::unexpected(
                rd_kafka_error_message("kafka_send option '" + key + "'", errstr));
        }
    }

    state->producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (state->producer == nullptr) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_send producer init", errstr));
    }
    return state;
}

inline auto build_avro_consumer_state(const std::string& brokers, const std::string& topic,
                                      const std::string& group, const std::string& schema_spec,
                                      const std::string& registry_url,
                                      const std::string& options_spec)
    -> std::expected<std::unique_ptr<AvroConsumerState>, std::string> {
    auto schema = parse_kafka_schema(schema_spec);
    if (!schema) {
        return std::unexpected(schema.error());
    }
    auto options = parse_kafka_avro_consumer_options(options_spec);
    if (!options) {
        return std::unexpected(options.error());
    }

    std::unique_ptr<AvroConsumerState> state =
        std::make_unique<AvroConsumerState>(registry_url, options->registry);
    state->schema = std::move(*schema);
    state->poll_timeout_ms = options->consumer.poll_timeout_ms;

    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv_avro bootstrap.servers", errstr));
    }
    if (rd_kafka_conf_set(conf, "group.id", group.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv_avro group.id", errstr));
    }
    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(
            rd_kafka_error_message("kafka_recv_avro enable.auto.commit", errstr));
    }
    for (const auto& [key, value] : options->consumer.config) {
        if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) !=
            RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            return std::unexpected(
                rd_kafka_error_message("kafka_recv_avro option '" + key + "'", errstr));
        }
    }

    state->consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (state->consumer == nullptr) {
        rd_kafka_conf_destroy(conf);
        return std::unexpected(rd_kafka_error_message("kafka_recv_avro consumer init", errstr));
    }
    rd_kafka_poll_set_consumer(state->consumer);

    rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic.c_str(), RD_KAFKA_PARTITION_UA);
    const rd_kafka_resp_err_t subscribe_err = rd_kafka_subscribe(state->consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);
    if (subscribe_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return std::unexpected("kafka_recv_avro subscribe failed: " +
                               std::string(rd_kafka_err2str(subscribe_err)));
    }

    return state;
}

inline auto consumer_key(const std::string& brokers, const std::string& topic,
                         const std::string& group, const std::string& schema,
                         const std::string& options) -> std::string {
    return brokers + '\n' + topic + '\n' + group + '\n' + schema + '\n' + options;
}

inline auto producer_key(const std::string& brokers, const std::string& topic,
                         const std::string& options) -> std::string {
    return brokers + '\n' + topic + '\n' + options;
}

inline auto avro_consumer_key(const std::string& brokers, const std::string& topic,
                              const std::string& group, const std::string& schema,
                              const std::string& registry_url, const std::string& options)
    -> std::string {
    return brokers + '\n' + topic + '\n' + group + '\n' + schema + '\n' + registry_url + '\n' +
           options;
}

inline auto get_consumer_state(const std::string& brokers, const std::string& topic,
                               const std::string& group, const std::string& schema,
                               const std::string& options)
    -> std::expected<ConsumerState*, std::string> {
    static std::mutex mu;
    static std::unordered_map<std::string, std::unique_ptr<ConsumerState>> states;

    const std::string key = consumer_key(brokers, topic, group, schema, options);
    std::lock_guard<std::mutex> lock(mu);
    auto it = states.find(key);
    if (it == states.end()) {
        auto built = build_consumer_state(brokers, topic, group, schema, options);
        if (!built) {
            return std::unexpected(built.error());
        }
        it = states.emplace(key, std::move(*built)).first;
    }
    return it->second.get();
}

inline auto get_producer_state(const std::string& brokers, const std::string& topic,
                               const std::string& options)
    -> std::expected<ProducerState*, std::string> {
    static std::mutex mu;
    static std::unordered_map<std::string, std::unique_ptr<ProducerState>> states;

    const std::string key = producer_key(brokers, topic, options);
    std::lock_guard<std::mutex> lock(mu);
    auto it = states.find(key);
    if (it == states.end()) {
        auto built = build_producer_state(brokers, options);
        if (!built) {
            return std::unexpected(built.error());
        }
        it = states.emplace(key, std::move(*built)).first;
    }
    return it->second.get();
}

inline auto get_avro_consumer_state(const std::string& brokers, const std::string& topic,
                                    const std::string& group, const std::string& schema,
                                    const std::string& registry_url, const std::string& options)
    -> std::expected<AvroConsumerState*, std::string> {
    static std::mutex mu;
    static std::unordered_map<std::string, std::unique_ptr<AvroConsumerState>> states;

    const std::string key = avro_consumer_key(brokers, topic, group, schema, registry_url, options);
    std::lock_guard<std::mutex> lock(mu);
    auto it = states.find(key);
    if (it == states.end()) {
        auto built =
            build_avro_consumer_state(brokers, topic, group, schema, registry_url, options);
        if (!built) {
            return std::unexpected(built.error());
        }
        it = states.emplace(key, std::move(*built)).first;
    }
    return it->second.get();
}

inline auto kafka_recv(const std::string& brokers, const std::string& topic,
                       const std::string& group, const std::string& schema,
                       const std::string& options = "")
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    auto state_result = get_consumer_state(brokers, topic, group, schema, options);
    if (!state_result) {
        return std::unexpected(state_result.error());
    }
    ConsumerState* state = *state_result;

    if (state->pending_commit != nullptr) {
        const rd_kafka_resp_err_t commit_err =
            rd_kafka_commit_message(state->consumer, state->pending_commit, 0);
        rd_kafka_message_destroy(state->pending_commit);
        state->pending_commit = nullptr;
        if (commit_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return std::unexpected("kafka_recv commit failed: " +
                                   std::string(rd_kafka_err2str(commit_err)));
        }
    }

    rd_kafka_message_t* message = rd_kafka_consumer_poll(state->consumer, state->poll_timeout_ms);
    if (message == nullptr) {
        return ibex::runtime::ExternValue{ibex::runtime::StreamTimeout{}};
    }

    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        const rd_kafka_resp_err_t err = message->err;
        rd_kafka_message_destroy(message);
        if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF ||
            err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) {
            return ibex::runtime::ExternValue{ibex::runtime::StreamTimeout{}};
        }
        return std::unexpected("kafka_recv poll failed: " + std::string(rd_kafka_err2str(err)));
    }

    std::string_view payload(static_cast<const char*>(message->payload), message->len);
    auto table = table_from_json_payload(payload, state->schema);
    if (!table) {
        rd_kafka_message_destroy(message);
        return std::unexpected(table.error());
    }
    state->pending_commit = message;
    return ibex::runtime::ExternValue{std::move(*table)};
}

inline auto kafka_send(const ibex::runtime::Table& table, const std::string& brokers,
                       const std::string& topic, const std::string& options = "")
    -> std::expected<std::int64_t, std::string> {
    auto state_result = get_producer_state(brokers, topic, options);
    if (!state_result) {
        return std::unexpected(state_result.error());
    }
    ProducerState* state = *state_result;

    rd_kafka_topic_t* rkt = rd_kafka_topic_new(state->producer, topic.c_str(), nullptr);
    if (rkt == nullptr) {
        return std::unexpected("kafka_send topic init failed: " +
                               std::string(rd_kafka_err2str(rd_kafka_last_error())));
    }

    std::int64_t sent = 0;
    for (std::size_t row = 0; row < table.rows(); ++row) {
        auto payload = table_row_to_json(table, row);
        if (!payload) {
            rd_kafka_topic_destroy(rkt);
            return std::unexpected(payload.error());
        }
        if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, payload->data(),
                             payload->size(), nullptr, 0, nullptr) == -1) {
            const auto err = rd_kafka_last_error();
            rd_kafka_topic_destroy(rkt);
            return std::unexpected("kafka_send produce failed: " +
                                   std::string(rd_kafka_err2str(err)));
        }
        ++sent;
    }
    rd_kafka_topic_destroy(rkt);
    const rd_kafka_resp_err_t flush_err = rd_kafka_flush(state->producer, state->flush_timeout_ms);
    if (flush_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return std::unexpected("kafka_send flush failed: " +
                               std::string(rd_kafka_err2str(flush_err)));
    }
    return sent;
}

inline auto kafka_recv_avro(const std::string& brokers, const std::string& topic,
                            const std::string& group, const std::string& schema,
                            const std::string& registry_url, const std::string& options = "")
    -> std::expected<ibex::runtime::ExternValue, std::string> {
    auto state_result =
        get_avro_consumer_state(brokers, topic, group, schema, registry_url, options);
    if (!state_result) {
        return std::unexpected(state_result.error());
    }
    AvroConsumerState* state = *state_result;

    if (state->pending_commit != nullptr) {
        const rd_kafka_resp_err_t commit_err =
            rd_kafka_commit_message(state->consumer, state->pending_commit, 0);
        rd_kafka_message_destroy(state->pending_commit);
        state->pending_commit = nullptr;
        if (commit_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return std::unexpected("kafka_recv_avro commit failed: " +
                                   std::string(rd_kafka_err2str(commit_err)));
        }
    }

    rd_kafka_message_t* message = rd_kafka_consumer_poll(state->consumer, state->poll_timeout_ms);
    if (message == nullptr) {
        return ibex::runtime::ExternValue{ibex::runtime::StreamTimeout{}};
    }

    if (message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        const rd_kafka_resp_err_t err = message->err;
        rd_kafka_message_destroy(message);
        if (err == RD_KAFKA_RESP_ERR__PARTITION_EOF ||
            err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) {
            return ibex::runtime::ExternValue{ibex::runtime::StreamTimeout{}};
        }
        return std::unexpected("kafka_recv_avro poll failed: " +
                               std::string(rd_kafka_err2str(err)));
    }

    std::string_view payload(static_cast<const char*>(message->payload), message->len);
    auto table = table_from_avro_registry_payload(payload, state->schema, state->schema_cache);
    if (!table) {
        rd_kafka_message_destroy(message);
        return std::unexpected(table.error());
    }
    state->pending_commit = message;
    return ibex::runtime::ExternValue{std::move(*table)};
}

}  // namespace ibex_kafka
