// Ibex plugin entry point for udp.hpp.
//
// Build as a shared library alongside udp.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   import "udp";
//
// or explicitly:
//
//   extern fn udp_recv(port: Int) -> DataFrame from "udp";
//   extern fn udp_send(df: DataFrame, host: String, port: Int) -> Int from "udp";

#include "udp.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    // udp_recv(port: Int) -> DataFrame
    // Blocks until one UDP datagram arrives, returns a one-row Table.
    // Returns an empty Table to signal end-of-stream (EOF sentinel or error).
    registry->register_table(
        "udp_recv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("udp_recv(port) expects exactly 1 argument");
            }
            const auto* port = std::get_if<std::int64_t>(&args[0]);
            if (port == nullptr) {
                return std::unexpected("udp_recv(port): port must be an integer");
            }
            try {
                return ibex::runtime::ExternValue{ibex_udp::udp_recv(*port)};
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
