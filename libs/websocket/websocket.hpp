// Ibex WebSocket plugin — streaming tick source and data sink over WebSocket.
//
// Wire protocol: one JSON object per WebSocket text message.
//
//   Tick (source, ws_recv): {"ts":<ns>,"symbol":"<str>","price":<float>,"volume":<int>}
//   Data (sink, ws_send):   any DataFrame row serialised as a flat JSON object
//
// Usage in an Ibex script:
//   import "websocket";
//
//   // Ingest UDP ticks, resample to 1-minute OHLC bars, push to browser dashboard.
//   let ohlc_stream = Stream {
//       source    = udp_recv(9001),
//       transform = [resample 1m, select {
//           open  = first(price),
//           high  = max(price),
//           low   = min(price),
//           close = last(price)
//       }],
//       sink = ws_send(8080)
//   };
//
// Browser clients connect to ws://localhost:8080 and receive OHLC bars in
// real time.  ws_recv(port) lets WebSocket clients send tick data into Ibex
// instead of (or in addition to) UDP.
//
// Both functions share server state for the same port, so a single TCP listen
// socket serves all connected browser clients.

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ibex_ws {

// ─── Minimal JSON field extractor ────────────────────────────────────────────
//
// Same helpers as the UDP plugin — duplicated here so the websocket plugin is
// self-contained and has no inter-plugin dependency.

inline auto json_get_str(std::string_view json, std::string_view key)
    -> std::optional<std::string> {
    std::string needle = "\"";
    needle += key;
    needle += "\":\"";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos) return std::nullopt;
    pos += needle.size();
    auto end = json.find('"', pos);
    if (end == std::string_view::npos) return std::nullopt;
    return std::string(json.substr(pos, end - pos));
}

inline auto json_get_int64(std::string_view json, std::string_view key)
    -> std::optional<std::int64_t> {
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos) return std::nullopt;
    pos += needle.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    std::int64_t value = 0;
    auto [ptr, ec] = std::from_chars(json.data() + pos, json.data() + json.size(), value);
    if (ec != std::errc{}) return std::nullopt;
    return value;
}

inline auto json_get_double(std::string_view json, std::string_view key)
    -> std::optional<double> {
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos) return std::nullopt;
    pos += needle.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    char* end_ptr = nullptr;
    double value = std::strtod(json.data() + pos, &end_ptr);
    if (end_ptr == json.data() + pos) return std::nullopt;
    return value;
}

// ─── SHA-1 ───────────────────────────────────────────────────────────────────
//
// Minimal RFC 3174 implementation used only for the WebSocket opening handshake.

struct Sha1Digest {
    std::array<std::uint8_t, 20> bytes{};
};

inline auto sha1(const std::uint8_t* data, std::size_t len) -> Sha1Digest {
    std::uint32_t h0 = 0x67452301u;
    std::uint32_t h1 = 0xEFCDAB89u;
    std::uint32_t h2 = 0x98BADCFEu;
    std::uint32_t h3 = 0x10325476u;
    std::uint32_t h4 = 0xC3D2E1F0u;

    auto rol32 = [](std::uint32_t v, int n) -> std::uint32_t {
        return (v << n) | (v >> (32 - n));
    };

    // Pre-processing: append 0x80, pad to 56 mod 64 bytes, append big-endian bit length.
    std::vector<std::uint8_t> msg(data, data + len);
    msg.push_back(0x80u);
    while (msg.size() % 64 != 56) msg.push_back(0x00u);
    const std::uint64_t bit_len = static_cast<std::uint64_t>(len) * 8u;
    for (int i = 7; i >= 0; --i)
        msg.push_back(static_cast<std::uint8_t>((bit_len >> (i * 8)) & 0xFFu));

    // Process each 64-byte chunk.
    for (std::size_t chunk = 0; chunk < msg.size(); chunk += 64) {
        std::array<std::uint32_t, 80> w{};
        for (int i = 0; i < 16; ++i) {
            const std::uint8_t* p = &msg[chunk + static_cast<std::size_t>(i) * 4];
            w[static_cast<std::size_t>(i)] =
                (std::uint32_t{p[0]} << 24) | (std::uint32_t{p[1]} << 16) |
                (std::uint32_t{p[2]} <<  8) |  std::uint32_t{p[3]};
        }
        for (int i = 16; i < 80; ++i) {
            auto idx = static_cast<std::size_t>(i);
            w[idx] = rol32(w[idx-3] ^ w[idx-8] ^ w[idx-14] ^ w[idx-16], 1);
        }

        std::uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
        for (int i = 0; i < 80; ++i) {
            std::uint32_t f{};
            std::uint32_t k{};
            if (i < 20) {
                f = (b & c) | (~b & d);
                k = 0x5A827999u;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1u;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDCu;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6u;
            }
            const std::uint32_t temp = rol32(a, 5) + f + e + k + w[static_cast<std::size_t>(i)];
            e = d; d = c; c = rol32(b, 30); b = a; a = temp;
        }
        h0 += a; h1 += b; h2 += c; h3 += d; h4 += e;
    }

    Sha1Digest digest;
    auto store = [&](std::size_t i, std::uint32_t h) {
        digest.bytes[i*4+0] = static_cast<std::uint8_t>((h >> 24) & 0xFFu);
        digest.bytes[i*4+1] = static_cast<std::uint8_t>((h >> 16) & 0xFFu);
        digest.bytes[i*4+2] = static_cast<std::uint8_t>((h >>  8) & 0xFFu);
        digest.bytes[i*4+3] = static_cast<std::uint8_t>( h        & 0xFFu);
    };
    store(0, h0); store(1, h1); store(2, h2); store(3, h3); store(4, h4);
    return digest;
}

// ─── Base64 encoder ───────────────────────────────────────────────────────────

inline auto base64_encode(const std::uint8_t* data, std::size_t len) -> std::string {
    static constexpr std::string_view kChars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (std::size_t i = 0; i < len; i += 3) {
        const std::uint32_t a = data[i];
        const std::uint32_t b = (i + 1 < len) ? data[i + 1] : 0u;
        const std::uint32_t c = (i + 2 < len) ? data[i + 2] : 0u;
        const std::uint32_t triple = (a << 16) | (b << 8) | c;
        out += kChars[(triple >> 18) & 0x3Fu];
        out += kChars[(triple >> 12) & 0x3Fu];
        out += (i + 1 < len) ? kChars[(triple >> 6) & 0x3Fu] : '=';
        out += (i + 2 < len) ? kChars[ triple       & 0x3Fu] : '=';
    }
    return out;
}

// ─── WebSocket handshake helpers ──────────────────────────────────────────────

static constexpr std::string_view kWsMagicGuid =
    "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// Extract the base64 value of the Sec-WebSocket-Key header from an HTTP request.
inline auto extract_ws_key(std::string_view http) -> std::optional<std::string> {
    // Header name is case-insensitive; check both common forms.
    for (auto hdr : {"Sec-WebSocket-Key:", "sec-websocket-key:"}) {
        auto pos = http.find(hdr);
        if (pos == std::string_view::npos) continue;
        pos += std::string_view(hdr).size();
        while (pos < http.size() && http[pos] == ' ') ++pos;
        auto end = http.find('\r', pos);
        if (end == std::string_view::npos) end = http.find('\n', pos);
        if (end == std::string_view::npos) return std::nullopt;
        return std::string(http.substr(pos, end - pos));
    }
    return std::nullopt;
}

// Compute the Sec-WebSocket-Accept header value (RFC 6455 §4.2.2).
inline auto compute_accept_key(std::string_view client_key) -> std::string {
    std::string combined = std::string(client_key) + std::string(kWsMagicGuid);
    auto digest = sha1(reinterpret_cast<const std::uint8_t*>(combined.data()), combined.size());
    return base64_encode(digest.bytes.data(), digest.bytes.size());
}

// ─── WebSocket frame encoding (server → client, unmasked) ────────────────────

inline auto ws_encode_text_frame(std::string_view payload) -> std::string {
    std::string frame;
    frame.reserve(payload.size() + 10);
    frame += '\x81';  // FIN=1, opcode=0x1 (text)
    const std::size_t len = payload.size();
    if (len <= 125) {
        frame += static_cast<char>(len);
    } else if (len <= 65535) {
        frame += '\x7E';
        frame += static_cast<char>((len >> 8) & 0xFF);
        frame += static_cast<char>( len       & 0xFF);
    } else {
        frame += '\x7F';
        for (int i = 7; i >= 0; --i)
            frame += static_cast<char>((len >> (i * 8)) & 0xFF);
    }
    frame.append(payload);
    return frame;
}

// ─── WebSocket frame decoding (client → server, masked) ──────────────────────

struct WsFrame {
    std::uint8_t opcode{};
    std::string  payload;
    bool         fin{true};
};

// Attempts to decode one frame from `buf`, consuming its bytes on success.
// Returns nullopt when buf contains fewer bytes than the complete frame.
inline auto ws_decode_frame(std::string& buf) -> std::optional<WsFrame> {
    if (buf.size() < 2) return std::nullopt;

    const auto* raw = reinterpret_cast<const std::uint8_t*>(buf.data());
    const bool fin    = (raw[0] & 0x80u) != 0;
    const std::uint8_t opcode = raw[0] & 0x0Fu;
    const bool masked = (raw[1] & 0x80u) != 0;
    std::uint64_t payload_len = raw[1] & 0x7Fu;
    std::size_t   header_size = 2;

    if (payload_len == 126) {
        if (buf.size() < 4) return std::nullopt;
        payload_len = (std::uint64_t{raw[2]} << 8) | raw[3];
        header_size = 4;
    } else if (payload_len == 127) {
        if (buf.size() < 10) return std::nullopt;
        payload_len = 0;
        for (int i = 0; i < 8; ++i)
            payload_len = (payload_len << 8) | raw[2 + static_cast<std::size_t>(i)];
        header_size = 10;
    }

    if (masked) header_size += 4;
    if (buf.size() < header_size + static_cast<std::size_t>(payload_len)) return std::nullopt;

    WsFrame frame;
    frame.fin    = fin;
    frame.opcode = opcode;
    frame.payload.resize(static_cast<std::size_t>(payload_len));

    if (masked) {
        const std::uint8_t* mask_key = raw + (header_size - 4);
        for (std::size_t i = 0; i < static_cast<std::size_t>(payload_len); ++i)
            frame.payload[i] = static_cast<char>(raw[header_size + i] ^ mask_key[i % 4]);
    } else {
        frame.payload.assign(buf, header_size, static_cast<std::size_t>(payload_len));
    }

    buf.erase(0, header_size + static_cast<std::size_t>(payload_len));
    return frame;
}

// ─── Server state ─────────────────────────────────────────────────────────────

struct WsClient {
    int         fd{-1};
    bool        handshaked{false};
    std::string buf;  // partial receive buffer
};

struct WsServer {
    int                   listen_fd{-1};
    std::vector<WsClient> clients;

    WsServer() = default;
    WsServer(const WsServer&) = delete;
    auto operator=(const WsServer&) -> WsServer& = delete;

    ~WsServer() {
        for (auto& c : clients)
            if (c.fd >= 0) ::close(c.fd);
        if (listen_fd >= 0) ::close(listen_fd);
    }
};

// One WsServer per port, lazily constructed.
inline auto get_server(int port) -> WsServer& {
    static std::unordered_map<int, std::unique_ptr<WsServer>> g_servers;
    auto it = g_servers.find(port);
    if (it != g_servers.end()) return *it->second;

    auto srv = std::make_unique<WsServer>();

    srv->listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0)
        throw std::runtime_error(std::string("ws: socket: ") + std::strerror(errno));

    int reuse = 1;
    ::setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(static_cast<std::uint16_t>(port));
    addr.sin_addr.s_addr = INADDR_ANY;
    if (::bind(srv->listen_fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(srv->listen_fd);
        throw std::runtime_error(std::string("ws: bind port ") + std::to_string(port) +
                                 ": " + std::strerror(errno));
    }
    if (::listen(srv->listen_fd, 16) < 0) {
        ::close(srv->listen_fd);
        throw std::runtime_error(std::string("ws: listen: ") + std::strerror(errno));
    }

    // Non-blocking so accept() in ws_send never stalls.
    ::fcntl(srv->listen_fd, F_SETFL, O_NONBLOCK);

    it = g_servers.emplace(port, std::move(srv)).first;
    return *it->second;
}

// ─── Helpers shared between ws_recv and ws_send ───────────────────────────────

// Perform a pending WebSocket upgrade handshake for client `c`.
// Returns true when the handshake is complete (or already was).
// Returns false and closes c.fd when the upgrade request is invalid.
inline auto try_handshake(WsClient& c) -> bool {
    if (c.handshaked) return true;
    if (c.buf.find("\r\n\r\n") == std::string::npos) return false;  // incomplete request

    auto key = extract_ws_key(c.buf);
    if (!key) {
        // Not a valid WebSocket upgrade; drop connection.
        ::close(c.fd);
        c.fd = -1;
        return false;
    }

    std::string response =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " + compute_accept_key(*key) + "\r\n\r\n";
    ::send(c.fd, response.data(), response.size(), MSG_NOSIGNAL);
    c.handshaked = true;
    c.buf.clear();
    return true;
}

// Remove clients whose fd has been set to -1 (disconnected / errored).
inline void prune_clients(WsServer& srv) {
    srv.clients.erase(
        std::remove_if(srv.clients.begin(), srv.clients.end(),
                       [](const WsClient& c) { return c.fd < 0; }),
        srv.clients.end());
}

// Serialise one row of `table` as a compact JSON object string.
inline auto row_to_json(const ibex::runtime::Table& table, std::size_t row) -> std::string {
    using namespace ibex::runtime;
    using namespace ibex;

    std::string json = "{";
    bool first_field = true;
    for (const auto& entry : table.columns) {
        if (!first_field) json += ',';
        first_field = false;
        json += '"';
        json += entry.name;
        json += "\":";
        if (is_null(entry, row)) {
            json += "null";
            continue;
        }
        std::visit(
            [&](const auto& col) {
                using ColType = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                    json += std::to_string(col[row]);
                } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                    char tmp[64];
                    std::snprintf(tmp, sizeof(tmp), "%.6g", col[row]);
                    json += tmp;
                } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                    json += '"';
                    json += col[row];
                    json += '"';
                } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                    json += '"';
                    json += col[row];  // operator[] returns std::string_view
                    json += '"';
                } else if constexpr (std::is_same_v<ColType, Column<Timestamp>>) {
                    json += std::to_string(col[row].nanos);
                } else if constexpr (std::is_same_v<ColType, Column<Date>>) {
                    json += std::to_string(col[row].days);
                }
            },
            *entry.column);
    }
    json += '}';
    return json;
}

// ─── ws_recv ──────────────────────────────────────────────────────────────────
//
// Listens on `port` for WebSocket client connections, reads one JSON tick
// message from any connected client, and returns a one-row DataFrame.
//
// The function uses a 200 ms select() timeout so the stream event loop can
// fire wall-clock bucket flushes (TimeBucket mode) during idle periods.
//
// Return values:
//   ExternValue{Table}          — one tick row (normal case)
//   ExternValue{Table{}}        — empty Table = EOF ({"eof":true} or close frame)
//   ExternValue{StreamTimeout}  — no message in 200 ms; call again

inline auto ws_recv(std::int64_t port) -> ibex::runtime::ExternValue {
    using namespace ibex::runtime;
    using namespace ibex;

    WsServer& srv = get_server(static_cast<int>(port));

    // ── Build select() fd set ─────────────────────────────────────────────────
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(srv.listen_fd, &rfds);
    int maxfd = srv.listen_fd;
    for (const auto& c : srv.clients) {
        if (c.fd >= 0) {
            FD_SET(c.fd, &rfds);
            if (c.fd > maxfd) maxfd = c.fd;
        }
    }

    struct timeval tv{0, 200'000};  // 200 ms
    const int nready = ::select(maxfd + 1, &rfds, nullptr, nullptr, &tv);
    if (nready <= 0) return StreamTimeout{};

    // ── Accept new connections ────────────────────────────────────────────────
    if (FD_ISSET(srv.listen_fd, &rfds)) {
        const int cfd = ::accept(srv.listen_fd, nullptr, nullptr);
        if (cfd >= 0) srv.clients.push_back(WsClient{cfd, false, {}});
    }

    // ── Service each client ───────────────────────────────────────────────────
    for (auto& c : srv.clients) {
        if (c.fd < 0 || !FD_ISSET(c.fd, &rfds)) continue;

        char tmp[4096];
        const ssize_t n = ::recv(c.fd, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            ::close(c.fd);
            c.fd = -1;
            continue;
        }
        c.buf.append(tmp, static_cast<std::size_t>(n));

        if (!c.handshaked) {
            try_handshake(c);
            continue;  // wait for first data frame after handshake
        }

        // Decode WebSocket frames; return on the first useful text message.
        while (true) {
            auto frame_opt = ws_decode_frame(c.buf);
            if (!frame_opt) break;

            const auto& frame = *frame_opt;

            if (frame.opcode == 0x8u) {
                // Close frame: echo close and mark disconnected.
                const std::string close_resp = {'\x88', '\x00'};
                ::send(c.fd, close_resp.data(), close_resp.size(), MSG_NOSIGNAL);
                ::close(c.fd);
                c.fd = -1;
                prune_clients(srv);
                return ExternValue{Table{}};  // EOF
            }

            if (frame.opcode == 0x9u) {
                // Ping: send pong.
                std::string pong;
                pong += '\x8A';
                pong += static_cast<char>(frame.payload.size() & 0x7Fu);
                pong += frame.payload;
                ::send(c.fd, pong.data(), pong.size(), MSG_NOSIGNAL);
                continue;
            }

            if (frame.opcode == 0x1u || frame.opcode == 0x0u) {
                // Text frame (or continuation — treat as complete for simplicity).
                std::string_view json = frame.payload;

                // EOF sentinel from client.
                if (json.find("\"eof\"") != std::string_view::npos &&
                    json.find("true")   != std::string_view::npos) {
                    prune_clients(srv);
                    return ExternValue{Table{}};
                }

                // Parse tick fields.
                auto ts_opt     = json_get_int64(json, "ts");
                auto symbol_opt = json_get_str(json, "symbol");
                auto price_opt  = json_get_double(json, "price");
                auto volume_opt = json_get_int64(json, "volume");

                if (!ts_opt || !symbol_opt || !price_opt || !volume_opt) {
                    // Malformed message — skip and wait for next.
                    continue;
                }

                Table table;

                Column<Timestamp> ts_col;
                ts_col.push_back(Timestamp{.nanos = *ts_opt});
                table.add_column("ts", ColumnValue{std::move(ts_col)});
                table.time_index = "ts";

                Column<std::string> sym_col;
                sym_col.push_back(std::move(*symbol_opt));
                table.add_column("symbol", ColumnValue{std::move(sym_col)});

                Column<double> price_col;
                price_col.push_back(*price_opt);
                table.add_column("price", ColumnValue{std::move(price_col)});

                Column<std::int64_t> vol_col;
                vol_col.push_back(*volume_opt);
                table.add_column("volume", ColumnValue{std::move(vol_col)});

                prune_clients(srv);
                return ExternValue{std::move(table)};
            }
        }
    }

    prune_clients(srv);
    return StreamTimeout{};
}

// ─── ws_send ──────────────────────────────────────────────────────────────────
//
// Broadcasts each row of `table` as a JSON WebSocket text frame to all
// handshaked clients on `port`.  Creates the server if it does not exist yet,
// so ws_send can be used as a standalone sink without ws_recv.
//
// Non-handshaked clients that have sent their HTTP upgrade request are
// promoted to the handshaked state on each call so that a client connecting
// in between two sink calls is ready to receive on the very next send.
//
// Returns the number of rows sent (regardless of client count).

inline auto ws_send(const ibex::runtime::Table& table, std::int64_t port) -> std::int64_t {
    WsServer& srv = get_server(static_cast<int>(port));

    // ── Accept all pending TCP connections (non-blocking) ─────────────────────
    while (true) {
        const int cfd = ::accept(srv.listen_fd, nullptr, nullptr);
        if (cfd < 0) break;  // EAGAIN / EWOULDBLOCK — no more pending connections
        srv.clients.push_back(WsClient{cfd, false, {}});
    }

    // ── Promote pending handshakes ────────────────────────────────────────────
    for (auto& c : srv.clients) {
        if (c.fd < 0 || c.handshaked) continue;
        char tmp[4096];
        const ssize_t n = ::recv(c.fd, tmp, sizeof(tmp), MSG_DONTWAIT);
        if (n > 0) {
            c.buf.append(tmp, static_cast<std::size_t>(n));
            try_handshake(c);
        } else if (n == 0) {
            ::close(c.fd);
            c.fd = -1;
        }
        // n < 0 (EAGAIN): client has not sent the upgrade request yet — try again next time.
    }

    prune_clients(srv);

    // ── Broadcast rows ────────────────────────────────────────────────────────
    const std::size_t rows = table.rows();
    std::int64_t sent = 0;

    for (std::size_t row = 0; row < rows; ++row) {
        const std::string json  = row_to_json(table, row);
        const std::string frame = ws_encode_text_frame(json);

        for (auto& c : srv.clients) {
            if (!c.handshaked) continue;
            const ssize_t n = ::send(c.fd, frame.data(), frame.size(), MSG_NOSIGNAL);
            if (n < 0) {
                ::close(c.fd);
                c.fd = -1;
            }
        }
        ++sent;
    }

    prune_clients(srv);
    return sent;
}

}  // namespace ibex_ws
