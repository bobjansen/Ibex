// Ibex plugin entry point for the WebSocket plugin.
//
// Build as a shared library alongside websocket.hpp and place it (together
// with websocket.ibex) in a directory on IBEX_LIBRARY_PATH so that the Ibex
// REPL loads it automatically when a script declares:
//
//   import "websocket";

#include "websocket.hpp"

#include <ibex/runtime/extern_registry.hpp>

namespace {

using ibex::runtime::ExternArgs;
using ibex::runtime::ExternValue;

auto arg_int(const ExternArgs& args, std::size_t index) -> const std::int64_t* {
    return index < args.size() ? std::get_if<std::int64_t>(&args[index]) : nullptr;
}

auto arg_str(const ExternArgs& args, std::size_t index) -> const std::string* {
    return index < args.size() ? std::get_if<std::string>(&args[index]) : nullptr;
}

}  // namespace

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    // ws_listen(port: Int) -> Int
    //
    // Eagerly opens the WebSocket listen socket on `port` so that browser clients
    // can connect before the first bar is emitted by a Stream {} sink.
    // Calling ws_listen() before the Stream block is optional but recommended
    // for a good user experience — without it the server is created lazily on
    // the first ws_send() call (i.e. only after the first OHLC bucket closes).
    //
    // Returns 0 on success.  Throws on bind failure (e.g. port already in use).
    registry->register_scalar(
        "ws_listen", ibex::runtime::ScalarKind::Int,
        [](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("ws_listen(port) expects exactly 1 argument");
            }
            const auto* port = arg_int(args, 0);
            if (port == nullptr) {
                return std::unexpected("ws_listen(port): port must be an integer");
            }
            try {
                ibex_ws::ws_listen(*port);
                return ExternValue{ibex::runtime::ScalarValue{std::int64_t{0}}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    // ws_recv(port: Int, schema: String[, options: String]) -> DataFrame
    //
    // WebSocket server source: waits up to poll_timeout_ms for an incoming
    // JSON message from any connected WebSocket client on `port` and converts
    // it to a one-row DataFrame following the schema string, e.g.
    // "ts:timestamp,symbol:str,price:f64,volume:i64".
    //
    // Returns:
    //   - A one-row Table on success.
    //   - An empty Table (EOF) when a client sends {"eof":true} or a close frame.
    //   - StreamTimeout when no message arrives within the polling window.
    registry->register_table(
        "ws_recv", [](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 2 && args.size() != 3) {
                return std::unexpected("ws_recv(port, schema[, options]) expects 2 or 3 arguments");
            }
            const auto* port = arg_int(args, 0);
            const auto* schema = arg_str(args, 1);
            if (port == nullptr || schema == nullptr) {
                return std::unexpected(
                    "ws_recv(port, schema[, options]): port must be an integer and schema a "
                    "string");
            }
            std::string options;
            if (args.size() == 3) {
                const auto* opts = arg_str(args, 2);
                if (opts == nullptr) {
                    return std::unexpected("ws_recv: options must be a string");
                }
                options = *opts;
            }
            try {
                return ibex_ws::ws_recv(*port, *schema, options);
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    // ws_connect(url: String, schema: String[, options: String]) -> DataFrame
    //
    // WebSocket client source: connects to an external feed at `url`
    // (ws://host[:port][/path]), optionally sends a subscribe message
    // (options key "subscribe"), and converts each incoming JSON message to a
    // one-row DataFrame following the schema string.
    //
    // Returns the same Table / empty-Table / StreamTimeout protocol as ws_recv.
    registry->register_table(
        "ws_connect", [](const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 2 && args.size() != 3) {
                return std::unexpected(
                    "ws_connect(url, schema[, options]) expects 2 or 3 arguments");
            }
            const auto* url = arg_str(args, 0);
            const auto* schema = arg_str(args, 1);
            if (url == nullptr || schema == nullptr) {
                return std::unexpected(
                    "ws_connect(url, schema[, options]): url and schema must be strings");
            }
            std::string options;
            if (args.size() == 3) {
                const auto* opts = arg_str(args, 2);
                if (opts == nullptr) {
                    return std::unexpected("ws_connect: options must be a string");
                }
                options = *opts;
            }
            try {
                return ibex_ws::ws_connect(*url, *schema, options);
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    // ws_send(df: DataFrame, port: Int) -> Int
    //
    // WebSocket server sink: broadcasts each row of `df` as a JSON text frame
    // to all connected and handshaked clients on `port`.
    //
    // Creates the TCP listener on `port` on first call, so ws_send can be used
    // without a paired ws_recv.
    //
    // Returns the number of rows sent.
    registry->register_scalar_table_consumer(
        "ws_send", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table,
           const ExternArgs& args) -> std::expected<ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("ws_send(df, port) expects 1 scalar argument (port)");
            }
            const auto* port = arg_int(args, 0);
            if (port == nullptr) {
                return std::unexpected("ws_send: port must be an integer");
            }
            try {
                const std::int64_t sent = ibex_ws::ws_send(table, *port);
                return ExternValue{ibex::runtime::ScalarValue{sent}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
