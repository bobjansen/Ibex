// Ibex plugin entry point for udp.hpp.
//
// Build as a shared library alongside udp.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   import "udp";

#include "udp.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    // udp_recv(port: Int, schema: String[, options: String]) -> DataFrame
    // Blocks until at least one UDP datagram arrives, drains the ready batch,
    // and returns a multi-row Table whose columns follow the schema string,
    // e.g. "ts:timestamp,symbol:str,price:f64,volume:i64".
    // Returns an empty Table to signal end-of-stream ({"eof":true} sentinel
    // or socket error).
    registry->register_table(
        "udp_recv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 2 && args.size() != 3) {
                return std::unexpected(
                    "udp_recv(port, schema[, options]) expects 2 or 3 arguments");
            }
            const auto* port = std::get_if<std::int64_t>(&args[0]);
            const auto* schema = std::get_if<std::string>(&args[1]);
            if (port == nullptr || schema == nullptr) {
                return std::unexpected(
                    "udp_recv(port, schema[, options]): port must be an integer and schema a "
                    "string");
            }
            std::string options;
            if (args.size() == 3) {
                const auto* opts = std::get_if<std::string>(&args[2]);
                if (opts == nullptr) {
                    return std::unexpected("udp_recv: options must be a string");
                }
                options = *opts;
            }
            try {
                return ibex::runtime::ExternValue{ibex_udp::udp_recv(*port, *schema, options)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    // udp_send(df: DataFrame, host: String, port: Int) -> Int
    // Serialises each row of the DataFrame to a JSON UDP datagram.
    // Returns the number of rows sent.
    registry->register_scalar_table_consumer(
        "udp_send", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 2) {
                return std::unexpected(
                    "udp_send(df, host, port) expects 2 scalar arguments (host, port)");
            }
            const auto* host = std::get_if<std::string>(&args[0]);
            const auto* port = std::get_if<std::int64_t>(&args[1]);
            if (host == nullptr) {
                return std::unexpected("udp_send: host must be a string");
            }
            if (port == nullptr) {
                return std::unexpected("udp_send: port must be an integer");
            }
            try {
                std::int64_t sent = ibex_udp::udp_send(table, *host, *port);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{sent}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
