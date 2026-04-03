#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "websocket.hpp"

namespace {

// Receive bytes into buf until the delimited string appears.  Leaves any
// bytes past the delimiter in buf so callers can continue reading frames.
void recv_until(int fd, std::string& buf, std::string_view delim) {
    while (buf.find(delim) == std::string::npos) {
        char tmp[4096];
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0)
            break;
        buf.append(tmp, static_cast<std::size_t>(n));
    }
}

// Read exactly n bytes from fd, prepending whatever is already in buf.
auto recv_exactly(int fd, std::string& buf, std::size_t n) -> bool {
    while (buf.size() < n) {
        char tmp[4096];
        const ssize_t r = ::recv(fd, tmp, sizeof(tmp), 0);
        if (r <= 0)
            return false;
        buf.append(tmp, static_cast<std::size_t>(r));
    }
    return true;
}

}  // namespace

// ─── SHA-1 / accept-key ───────────────────────────────────────────────────────

TEST_CASE("compute_accept_key matches RFC 6455 §1.3 test vector") {
    // Known pair from the RFC: client key → expected server accept value.
    const auto accept = ibex_ws::compute_accept_key("dGhlIHNhbXBsZSBub25jZQ==");
    CHECK(accept == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

// ─── ws_send end-to-end ───────────────────────────────────────────────────────
//
// Sequence:
//   1. ws_listen() binds the TCP port.
//   2. Client thread connects and sends the HTTP upgrade request.
//   3. Main thread sleeps 50 ms (upgrade sits in the kernel recv buffer).
//   4. ws_send() accepts the connection, promotes the handshake via
//      MSG_DONTWAIT recv, sends HTTP 101, then broadcasts the WS frame.
//   5. Client reads the HTTP 101 + any bundled WS frame bytes, then parses
//      the frame, verifying the JSON payload.
//
// This test catches the regression where the first WS frame arrives bundled
// with the HTTP 101 response in the same TCP segment and is discarded.

TEST_CASE("ws_send: handshake and single-row broadcast") {
    constexpr int kPort = 17765;

    try {
        ibex_ws::ws_listen(kPort);
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        if (msg.find("Operation not permitted") != std::string::npos ||
            msg.find("Permission denied") != std::string::npos) {
            SKIP("websocket test requires socket permissions");
        }
        throw;
    }

    std::string received_json;
    bool handshake_ok = false;

    std::thread client([&]() {
        int sock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            if (errno == EPERM || errno == EACCES) {
                return;
            }
            REQUIRE(sock >= 0);
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<std::uint16_t>(kPort));
        addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");

        // Connect with brief retries in case the socket isn't ready yet.
        bool connected = false;
        for (int i = 0; i < 20 && !connected; ++i) {
            if (::connect(sock, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0)
                connected = true;
            else
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!connected) {
            ::close(sock);
            return;
        }

        const std::string key = "dGhlIHNhbXBsZSBub25jZQ==";
        const std::string request =
            "GET /ibex HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: " +
            key +
            "\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n";
        ::send(sock, request.data(), request.size(), MSG_NOSIGNAL);

        // Read until end of HTTP headers; preserve leftover bytes (may contain
        // the first WS frame if it was bundled in the same TCP segment).
        std::string buf;
        recv_until(sock, buf, "\r\n\r\n");

        const auto sep = buf.find("\r\n\r\n");
        if (sep == std::string::npos) {
            ::close(sock);
            return;
        }

        handshake_ok = buf.find("101 Switching Protocols") != std::string::npos;

        // Strip HTTP headers, keep leftover WS frame bytes.
        buf.erase(0, sep + 4);

        // Parse the incoming WS frame.
        if (!recv_exactly(sock, buf, 2)) {
            ::close(sock);
            return;
        }
        const auto b0 = static_cast<std::uint8_t>(buf[0]);
        const auto b1 = static_cast<std::uint8_t>(buf[1]);
        buf.erase(0, 2);

        std::size_t payload_len = b1 & 0x7Fu;
        if (payload_len == 126) {
            if (!recv_exactly(sock, buf, 2)) {
                ::close(sock);
                return;
            }
            payload_len =
                (static_cast<std::uint8_t>(buf[0]) << 8) | static_cast<std::uint8_t>(buf[1]);
            buf.erase(0, 2);
        } else if (payload_len == 127) {
            if (!recv_exactly(sock, buf, 8)) {
                ::close(sock);
                return;
            }
            payload_len = 0;
            for (int i = 0; i < 8; ++i)
                payload_len = (payload_len << 8) | static_cast<std::uint8_t>(buf[i]);
            buf.erase(0, 8);
        }

        if (!recv_exactly(sock, buf, payload_len)) {
            ::close(sock);
            return;
        }
        received_json = buf.substr(0, payload_len);

        ::close(sock);
    });

    // Give the client time to connect and send the HTTP upgrade request so it
    // sits in the kernel recv buffer before ws_send calls MSG_DONTWAIT recv.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ibex::runtime::Table table;
    table.add_column("value", ibex::Column<std::int64_t>{42});

    const auto sent = ibex_ws::ws_send(table, kPort);

    client.join();

    CHECK(sent == 1);
    CHECK(handshake_ok);
    REQUIRE_FALSE(received_json.empty());
    CHECK(received_json.find("\"value\"") != std::string::npos);
    CHECK(received_json.find("42") != std::string::npos);
}
