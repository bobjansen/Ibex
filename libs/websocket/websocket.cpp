// Ibex plugin entry point for the WebSocket plugin.
//
// Build as a shared library alongside websocket.hpp and place it (together
// with websocket.ibex) in a directory on IBEX_LIBRARY_PATH so that the Ibex
// REPL loads it automatically when a script declares:
//
//   import "websocket";
//
// or explicitly:
//
//   extern fn ws_recv(port: Int) -> DataFrame from "websocket";
//   extern fn ws_send(df: DataFrame, port: Int) -> Int from "websocket";

#include "websocket.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
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
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("ws_listen(port) expects exactly 1 argument");
            }
            const auto* port = std::get_if<std::int64_t>(&args[0]);
            if (port == nullptr) {
                return std::unexpected("ws_listen(port): port must be an integer");
            }
            try {
                ibex_ws::get_server(static_cast<int>(*port));  // bind eagerly
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{std::int64_t{0}}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    // ws_recv(port: Int) -> DataFrame
    //
    // WebSocket server source: waits up to 200 ms for an incoming JSON tick
    // message from any connected WebSocket client on `port`.
    //
    // Returns:
    //   - A one-row Table on success.
    //   - An empty Table (EOF) when the client sends {"eof":true} or a close frame.
    //   - StreamTimeout when no message arrives within the polling window.
    registry->register_table(
        "ws_recv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("ws_recv(port) expects exactly 1 argument");
            }
            const auto* port = std::get_if<std::int64_t>(&args[0]);
            if (port == nullptr) {
                return std::unexpected("ws_recv(port): port must be an integer");
            }
            try {
                return ibex_ws::ws_recv(*port);  // already ExternValue (Table or StreamTimeout)
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
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("ws_send(df, port) expects 1 scalar argument (port)");
            }
            const auto* port = std::get_if<std::int64_t>(&args[0]);
            if (port == nullptr) {
                return std::unexpected("ws_send: port must be an integer");
            }
            try {
                const std::int64_t sent = ibex_ws::ws_send(table, *port);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{sent}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
