// Ibex plugin entry point for kafka.hpp.
//
// Build as a shared library alongside kafka.ibex and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   import "kafka";

#include "kafka.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "kafka_recv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 4 && args.size() != 5) {
                return std::unexpected(
                    "kafka_recv(brokers, topic, group, schema[, options]) expects 4 or 5 string "
                    "arguments");
            }
            const auto* brokers = std::get_if<std::string>(&args[0]);
            const auto* topic = std::get_if<std::string>(&args[1]);
            const auto* group = std::get_if<std::string>(&args[2]);
            const auto* schema = std::get_if<std::string>(&args[3]);
            if (brokers == nullptr || topic == nullptr || group == nullptr || schema == nullptr) {
                return std::unexpected(
                    "kafka_recv(brokers, topic, group, schema[, options]) expects string "
                    "arguments");
            }
            std::string options;
            if (args.size() == 5) {
                const auto* option_text = std::get_if<std::string>(&args[4]);
                if (option_text == nullptr) {
                    return std::unexpected(
                        "kafka_recv(..., options) expects the options argument to be a string");
                }
                options = *option_text;
            }
            return ibex_kafka::kafka_recv(*brokers, *topic, *group, *schema, options);
        });

    registry->register_scalar_table_consumer(
        "kafka_send", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 2 && args.size() != 3) {
                return std::unexpected(
                    "kafka_send(df, brokers, topic[, options]) expects 2 or 3 string arguments");
            }
            const auto* brokers = std::get_if<std::string>(&args[0]);
            const auto* topic = std::get_if<std::string>(&args[1]);
            if (brokers == nullptr || topic == nullptr) {
                return std::unexpected(
                    "kafka_send(df, brokers, topic[, options]) expects string arguments");
            }
            std::string options;
            if (args.size() == 3) {
                const auto* option_text = std::get_if<std::string>(&args[2]);
                if (option_text == nullptr) {
                    return std::unexpected(
                        "kafka_send(..., options) expects the options argument to be a string");
                }
                options = *option_text;
            }
            auto sent = ibex_kafka::kafka_send(table, *brokers, *topic, options);
            if (!sent) {
                return std::unexpected(sent.error());
            }
            return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{*sent}};
        });
}
