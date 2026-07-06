// Ibex WebSocket plugin — schema-driven streaming source, sink, and client.
//
// Wire protocol: one JSON object per WebSocket text message.  The message
// shape is described by an explicit schema string using the shared plugin
// mini-language (see <ibex/plugin/schema.hpp>):
//
//   "ts:timestamp,symbol:cat,price:f64,volume:i64"
//
// Functions:
//
//   ws_recv(port, schema[, options])   — WebSocket *server* source: accepts
//       client connections on `port` and converts each incoming JSON text
//       message to a one-row DataFrame following `schema`.
//   ws_connect(url, schema[, options]) — WebSocket *client* source: connects
//       to an external feed at `url` (ws://host[:port][/path]) and converts
//       each incoming JSON text message the same way.
//   ws_send(df, port)                  — sink: broadcasts each DataFrame row
//       as a JSON text frame to all clients connected on `port`.
//   ws_listen(port)                    — eagerly binds the listen socket.
//
// Options (key=value pairs separated by ';'):
//   poll_timeout_ms=200       select() window before returning StreamTimeout
//   on_malformed=skip|error   what to do with messages that fail the schema
//                             (default: skip)
//   subscribe=<text>          ws_connect only: text frame sent right after
//                             the handshake (e.g. an exchange subscribe
//                             message; must not contain ';')
//
// End-of-stream: a message {"eof":true} or a WebSocket close frame yields an
// empty DataFrame, which terminates the Stream event loop.
//
// Usage in an Ibex script:
//   import "websocket";
//
//   // Ingest ticks from an external feed, resample, push to a dashboard.
//   let ohlc_stream = Stream {
//       source    = ws_connect("ws://feed.example.com/ticks",
//                              "ts:timestamp,symbol:str,price:f64,volume:i64"),
//       transform = [resample 1m, select {
//           open  = first(price),
//           high  = max(price),
//           low   = min(price),
//           close = last(price)
//       }],
//       sink = ws_send(8080)
//   };
//
// ws_recv and ws_send share server state for the same port, so a single TCP
// listen socket serves all connected browser clients.

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/plugin/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <random>
#include <robin_hood.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace ibex_ws {

// ─── SHA-1 ───────────────────────────────────────────────────────────────────
//
// Minimal RFC 3174 implementation used only for the WebSocket opening handshake.

struct Sha1Digest {
    std::array<std::uint8_t, 20> bytes{};
};

inline auto sha1(const std::uint8_t* data, std::size_t len) -> Sha1Digest {
    std::uint32_t h0 = 0x67452301U;
    std::uint32_t h1 = 0xEFCDAB89U;
    std::uint32_t h2 = 0x98BADCFEU;
    std::uint32_t h3 = 0x10325476U;
    std::uint32_t h4 = 0xC3D2E1F0U;

    auto rol32 = [](std::uint32_t v, int n) -> std::uint32_t {
        return (v << n) | (v >> (32 - n));
    };

    // Pre-processing: append 0x80, pad to 56 mod 64 bytes, append big-endian bit length.
    std::vector<std::uint8_t> msg(data, data + len);
    msg.push_back(0x80U);
    while (msg.size() % 64 != 56)
        msg.push_back(0x00U);
    const std::uint64_t bit_len = static_cast<std::uint64_t>(len) * 8U;
    for (int i = 7; i >= 0; --i)
        msg.push_back(static_cast<std::uint8_t>((bit_len >> (i * 8)) & 0xFFU));

    // Process each 64-byte chunk.
    for (std::size_t chunk = 0; chunk < msg.size(); chunk += 64) {
        std::array<std::uint32_t, 80> w{};
        for (int i = 0; i < 16; ++i) {
            const std::uint8_t* p = &msg[chunk + (static_cast<std::size_t>(i) * 4)];
            w[static_cast<std::size_t>(i)] = (std::uint32_t{p[0]} << 24) |
                                             (std::uint32_t{p[1]} << 16) |
                                             (std::uint32_t{p[2]} << 8) | std::uint32_t{p[3]};
        }
        for (int i = 16; i < 80; ++i) {
            auto idx = static_cast<std::size_t>(i);
            w[idx] = rol32(w[idx - 3] ^ w[idx - 8] ^ w[idx - 14] ^ w[idx - 16], 1);
        }

        std::uint32_t a = h0;
        std::uint32_t b = h1;
        std::uint32_t c = h2;
        std::uint32_t d = h3;
        std::uint32_t e = h4;
        for (int i = 0; i < 80; ++i) {
            std::uint32_t f{};
            std::uint32_t k{};
            if (i < 20) {
                f = (b & c) | (~b & d);
                k = 0x5A827999U;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1U;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDCU;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6U;
            }
            const std::uint32_t temp = rol32(a, 5) + f + e + k + w[static_cast<std::size_t>(i)];
            e = d;
            d = c;
            c = rol32(b, 30);
            b = a;
            a = temp;
        }
        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }

    Sha1Digest digest;
    auto store = [&](std::size_t i, std::uint32_t h) {
        digest.bytes[(i * 4) + 0] = static_cast<std::uint8_t>((h >> 24) & 0xFFu);
        digest.bytes[(i * 4) + 1] = static_cast<std::uint8_t>((h >> 16) & 0xFFu);
        digest.bytes[(i * 4) + 2] = static_cast<std::uint8_t>((h >> 8) & 0xFFu);
        digest.bytes[(i * 4) + 3] = static_cast<std::uint8_t>(h & 0xFFu);
    };
    store(0, h0);
    store(1, h1);
    store(2, h2);
    store(3, h3);
    store(4, h4);
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
        const std::uint32_t b = (i + 1 < len) ? data[i + 1] : 0U;
        const std::uint32_t c = (i + 2 < len) ? data[i + 2] : 0U;
        const std::uint32_t triple = (a << 16) | (b << 8) | c;
        out += kChars[(triple >> 18) & 0x3FU];
        out += kChars[(triple >> 12) & 0x3FU];
        out += (i + 1 < len) ? kChars[(triple >> 6) & 0x3FU] : '=';
        out += (i + 2 < len) ? kChars[triple & 0x3FU] : '=';
    }
    return out;
}

// ─── WebSocket handshake helpers ──────────────────────────────────────────────

static constexpr std::string_view kWsMagicGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// Extract the value of an HTTP header (case-insensitive name match).
inline auto extract_http_header(std::string_view http, std::string_view name)
    -> std::optional<std::string> {
    auto lower = [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    };
    for (std::size_t i = 0; i + name.size() + 1 < http.size(); ++i) {
        if (i != 0 && http[i - 1] != '\n')
            continue;  // headers start at the beginning of a line
        std::size_t j = 0;
        while (j < name.size() && lower(http[i + j]) == lower(name[j]))
            ++j;
        if (j != name.size() || http[i + j] != ':')
            continue;
        std::size_t pos = i + name.size() + 1;
        while (pos < http.size() && (http[pos] == ' ' || http[pos] == '\t'))
            ++pos;
        auto end = http.find('\r', pos);
        if (end == std::string_view::npos)
            end = http.find('\n', pos);
        if (end == std::string_view::npos)
            return std::nullopt;
        return std::string(http.substr(pos, end - pos));
    }
    return std::nullopt;
}

// Extract the base64 value of the Sec-WebSocket-Key header from an HTTP request.
inline auto extract_ws_key(std::string_view http) -> std::optional<std::string> {
    return extract_http_header(http, "Sec-WebSocket-Key");
}

// Compute the Sec-WebSocket-Accept header value (RFC 6455 §4.2.2).
inline auto compute_accept_key(std::string_view client_key) -> std::string {
    std::string combined = std::string(client_key) + std::string(kWsMagicGuid);
    auto digest = sha1(reinterpret_cast<const std::uint8_t*>(combined.data()), combined.size());
    return base64_encode(digest.bytes.data(), digest.bytes.size());
}

// Random bytes for client masks and Sec-WebSocket-Key nonces.
inline void random_bytes(std::uint8_t* out, std::size_t n) {
    thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(0, 255);
    for (std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::uint8_t>(dist(rng));
}

// Generate a random 16-byte base64 Sec-WebSocket-Key (RFC 6455 §4.1).
inline auto random_ws_key() -> std::string {
    std::array<std::uint8_t, 16> nonce{};
    random_bytes(nonce.data(), nonce.size());
    return base64_encode(nonce.data(), nonce.size());
}

// ─── WebSocket frame encoding ─────────────────────────────────────────────────
//
// Server→client frames are unmasked; client→server frames must be masked
// (RFC 6455 §5.3).

inline auto ws_encode_frame(std::uint8_t opcode, std::string_view payload, bool masked)
    -> std::string {
    std::string frame;
    frame.reserve(payload.size() + 14);
    frame += static_cast<char>(0x80U | (opcode & 0x0FU));  // FIN=1
    const std::size_t len = payload.size();
    const std::uint8_t mask_bit = masked ? 0x80U : 0x00U;
    if (len <= 125) {
        frame += static_cast<char>(mask_bit | len);
    } else if (len <= 65535) {
        frame += static_cast<char>(mask_bit | 0x7EU);
        frame += static_cast<char>((len >> 8) & 0xFF);
        frame += static_cast<char>(len & 0xFF);
    } else {
        frame += static_cast<char>(mask_bit | 0x7FU);
        for (int i = 7; i >= 0; --i)
            frame += static_cast<char>((len >> (i * 8)) & 0xFF);
    }
    if (masked) {
        std::array<std::uint8_t, 4> key{};
        random_bytes(key.data(), key.size());
        for (const auto byte : key)
            frame += static_cast<char>(byte);
        for (std::size_t i = 0; i < len; ++i)
            frame += static_cast<char>(static_cast<std::uint8_t>(payload[i]) ^ key[i % 4]);
    } else {
        frame.append(payload);
    }
    return frame;
}

inline auto ws_encode_text_frame(std::string_view payload) -> std::string {
    return ws_encode_frame(0x1U, payload, /*masked=*/false);
}

// ─── WebSocket frame decoding ─────────────────────────────────────────────────

struct WsFrame {
    std::uint8_t opcode{};
    std::string payload;
    bool fin{true};
};

// Attempts to decode one frame from `buf`, consuming its bytes on success.
// Returns nullopt when buf contains fewer bytes than the complete frame.
// Handles both masked (client→server) and unmasked (server→client) frames.
inline auto ws_decode_frame(std::string& buf) -> std::optional<WsFrame> {
    if (buf.size() < 2)
        return std::nullopt;

    const auto* raw = reinterpret_cast<const std::uint8_t*>(buf.data());
    const bool fin = (raw[0] & 0x80U) != 0;
    const std::uint8_t opcode = raw[0] & 0x0FU;
    const bool masked = (raw[1] & 0x80U) != 0;
    std::uint64_t payload_len = raw[1] & 0x7FU;
    std::size_t header_size = 2;

    if (payload_len == 126) {
        if (buf.size() < 4)
            return std::nullopt;
        payload_len = (std::uint64_t{raw[2]} << 8) | raw[3];
        header_size = 4;
    } else if (payload_len == 127) {
        if (buf.size() < 10)
            return std::nullopt;
        payload_len = 0;
        for (int i = 0; i < 8; ++i)
            payload_len = (payload_len << 8) | raw[2 + static_cast<std::size_t>(i)];
        header_size = 10;
    }

    if (masked)
        header_size += 4;
    if (buf.size() < header_size + static_cast<std::size_t>(payload_len))
        return std::nullopt;

    WsFrame frame;
    frame.fin = fin;
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

// ─── Options ──────────────────────────────────────────────────────────────────

struct WsOptions {
    int poll_timeout_ms = 200;
    bool malformed_error = false;  // on_malformed=skip (default) | error
    std::string subscribe;         // ws_connect only: text frame sent after handshake
};

// Parse the options spec for ws_recv / ws_connect.  Throws on unknown keys.
inline auto parse_ws_options(std::string_view spec, bool allow_subscribe) -> WsOptions {
    WsOptions options;
    auto parsed = ibex::plugin::parse_key_value_options(spec);
    if (!parsed)
        throw std::runtime_error("ws: " + parsed.error());
    for (auto& [key, value] : *parsed) {
        if (key == "poll_timeout_ms") {
            auto timeout = ibex::plugin::parse_non_negative_int(value, "poll_timeout_ms");
            if (!timeout)
                throw std::runtime_error("ws: " + timeout.error());
            options.poll_timeout_ms = *timeout;
            continue;
        }
        if (key == "on_malformed") {
            if (value == "skip") {
                options.malformed_error = false;
            } else if (value == "error") {
                options.malformed_error = true;
            } else {
                throw std::runtime_error("ws: on_malformed must be 'skip' or 'error'");
            }
            continue;
        }
        if (allow_subscribe && key == "subscribe") {
            options.subscribe = std::move(value);
            continue;
        }
        throw std::runtime_error("ws: unsupported option: " + key);
    }
    return options;
}

// Parse and validate a schema spec, throwing on error (the plugin registry
// entry points catch and convert to std::unexpected).
inline auto parse_schema_or_throw(std::string_view spec) -> std::vector<ibex::plugin::SchemaField> {
    auto schema = ibex::plugin::parse_schema(spec);
    if (!schema)
        throw std::runtime_error("ws: " + schema.error());
    return std::move(*schema);
}

// ─── Message → row conversion ─────────────────────────────────────────────────

enum class PayloadResult : std::uint8_t {
    Row,   // `out` holds a one-row Table
    Eof,   // {"eof":true} sentinel — end of stream
    Skip,  // malformed message, on_malformed=skip
};

// Convert one JSON text payload to a one-row Table following `schema`.
// Throws when the message is malformed and on_malformed=error.
inline auto payload_to_row(std::string_view payload,
                           const std::vector<ibex::plugin::SchemaField>& schema,
                           const WsOptions& options, ibex::runtime::Table& out) -> PayloadResult {
    const nlohmann::json object =
        nlohmann::json::parse(payload, nullptr, /*allow_exceptions=*/false);
    if (object.is_discarded() || !object.is_object()) {
        if (options.malformed_error)
            throw std::runtime_error("ws: payload is not a JSON object");
        return PayloadResult::Skip;
    }

    if (auto it = object.find("eof"); it != object.end() && it->is_boolean() && it->get<bool>())
        return PayloadResult::Eof;

    auto table = ibex::plugin::table_from_json_object(object, schema);
    if (!table) {
        if (options.malformed_error)
            throw std::runtime_error("ws: " + table.error());
        return PayloadResult::Skip;
    }
    out = std::move(*table);
    return PayloadResult::Row;
}

// ─── Server state ─────────────────────────────────────────────────────────────

struct WsClient {
    int fd{-1};
    bool handshaked{false};
    std::string buf;  // partial receive buffer
};

struct WsServer {
    int listen_fd{-1};
    std::vector<WsClient> clients;

    WsServer() = default;
    WsServer(WsServer&&) = delete;
    WsServer& operator=(WsServer&&) = delete;
    WsServer(const WsServer&) = delete;
    auto operator=(const WsServer&) -> WsServer& = delete;

    ~WsServer() {
        for (auto& c : clients)
            if (c.fd >= 0)
                ::close(c.fd);
        if (listen_fd >= 0)
            ::close(listen_fd);
    }
};

// One WsServer per port, lazily constructed.
inline auto get_server(int port) -> WsServer& {
    static robin_hood::unordered_map<int, std::unique_ptr<WsServer>> g_servers;
    auto it = g_servers.find(port);
    if (it != g_servers.end())
        return *it->second;

    auto srv = std::make_unique<WsServer>();

    srv->listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0)
        throw std::runtime_error(std::string("ws: socket: ") + std::strerror(errno));

    int reuse = 1;
    ::setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));
    addr.sin_addr.s_addr = INADDR_ANY;
    if (::bind(srv->listen_fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(srv->listen_fd);
        throw std::runtime_error(std::string("ws: bind port ") + std::to_string(port) + ": " +
                                 std::strerror(errno));
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
    if (c.handshaked)
        return true;
    if (c.buf.find("\r\n\r\n") == std::string::npos)
        return false;  // incomplete request

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
        "Sec-WebSocket-Accept: " +
        compute_accept_key(*key) + "\r\n\r\n";
    ::send(c.fd, response.data(), response.size(), MSG_NOSIGNAL);
    c.handshaked = true;
    c.buf.clear();
    return true;
}

// Remove clients whose fd has been set to -1 (disconnected / errored).
inline void prune_clients(WsServer& srv) {
    srv.clients.erase(std::remove_if(srv.clients.begin(), srv.clients.end(),
                                     [](const WsClient& c) { return c.fd < 0; }),
                      srv.clients.end());
}

// Decode and handle all complete frames buffered for client `c`.  Returns a
// value (one-row Table or empty Table for EOF) as soon as one message is
// useful; nullopt when the buffer holds no complete, useful frame.
inline auto service_client_frames(WsClient& c, const std::vector<ibex::plugin::SchemaField>& schema,
                                  const WsOptions& options)
    -> std::optional<ibex::runtime::ExternValue> {
    using namespace ibex::runtime;

    while (true) {
        auto frame_opt = ws_decode_frame(c.buf);
        if (!frame_opt)
            return std::nullopt;

        const auto& frame = *frame_opt;

        if (frame.opcode == 0x8U) {
            // Close frame: echo close and mark disconnected.
            const std::string close_resp = ws_encode_frame(0x8U, {}, /*masked=*/false);
            ::send(c.fd, close_resp.data(), close_resp.size(), MSG_NOSIGNAL);
            ::close(c.fd);
            c.fd = -1;
            return ExternValue{Table{}};  // EOF
        }

        if (frame.opcode == 0x9U) {
            // Ping: send pong.
            const std::string pong = ws_encode_frame(0xAU, frame.payload, /*masked=*/false);
            ::send(c.fd, pong.data(), pong.size(), MSG_NOSIGNAL);
            continue;
        }

        if (frame.opcode == 0x1U || frame.opcode == 0x0U) {
            // Text frame (or continuation — treat as complete for simplicity).
            Table row;
            switch (payload_to_row(frame.payload, schema, options, row)) {
                case PayloadResult::Eof:
                    return ExternValue{Table{}};
                case PayloadResult::Row:
                    return ExternValue{std::move(row)};
                case PayloadResult::Skip:
                    continue;  // wait for next message
            }
        }
    }
}

// ─── ws_recv ──────────────────────────────────────────────────────────────────
//
// WebSocket server source.  Listens on `port` for client connections, reads
// one JSON text message from any connected client, and returns a one-row
// DataFrame following `schema_spec`.
//
// The select() timeout (poll_timeout_ms, default 200 ms) lets the stream
// event loop fire wall-clock bucket flushes (TimeBucket mode) during idle
// periods.
//
// Return values:
//   ExternValue{Table}          — one message row (normal case)
//   ExternValue{Table{}}        — empty Table = EOF ({"eof":true} or close frame)
//   ExternValue{StreamTimeout}  — no message in the poll window; call again

inline auto ws_recv(std::int64_t port, std::string_view schema_spec,
                    std::string_view options_spec = {}) -> ibex::runtime::ExternValue {
    using namespace ibex::runtime;

    const auto schema = parse_schema_or_throw(schema_spec);
    const auto options = parse_ws_options(options_spec, /*allow_subscribe=*/false);

    WsServer& srv = get_server(static_cast<int>(port));

    // ── Serve frames already buffered from a previous call ───────────────────
    // A previous call may have returned mid-buffer; those bytes are no longer
    // visible to select(), so drain them before waiting on the sockets.
    for (auto& c : srv.clients) {
        if (c.fd < 0 || !c.handshaked || c.buf.empty())
            continue;
        if (auto value = service_client_frames(c, schema, options)) {
            prune_clients(srv);
            return std::move(*value);
        }
    }

    // ── Build select() fd set ─────────────────────────────────────────────────
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(srv.listen_fd, &rfds);
    int maxfd = srv.listen_fd;
    for (const auto& c : srv.clients) {
        if (c.fd >= 0) {
            FD_SET(c.fd, &rfds);
            maxfd = std::max(c.fd, maxfd);
        }
    }

    struct timeval tv{.tv_sec = options.poll_timeout_ms / 1000,
                      .tv_usec = (options.poll_timeout_ms % 1000) * 1000};
    const int nready = ::select(maxfd + 1, &rfds, nullptr, nullptr, &tv);
    if (nready <= 0)
        return StreamTimeout{};

    // ── Accept new connections ────────────────────────────────────────────────
    if (FD_ISSET(srv.listen_fd, &rfds)) {
        const int cfd = ::accept(srv.listen_fd, nullptr, nullptr);
        if (cfd >= 0)
            srv.clients.push_back(WsClient{.fd = cfd, .handshaked = false, .buf = {}});
    }

    // ── Service each client ───────────────────────────────────────────────────
    for (auto& c : srv.clients) {
        if (c.fd < 0 || !FD_ISSET(c.fd, &rfds))
            continue;

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

        if (auto value = service_client_frames(c, schema, options)) {
            prune_clients(srv);
            return std::move(*value);
        }
    }

    prune_clients(srv);
    return StreamTimeout{};
}

// ─── ws_connect ───────────────────────────────────────────────────────────────
//
// WebSocket client source.  Connects to `url` (ws://host[:port][/path]),
// performs the client side of the opening handshake, optionally sends a
// subscribe message, then converts each incoming JSON text message to a
// one-row DataFrame following `schema_spec`.
//
// The connection is cached per (url, schema, options) and reused across
// calls, matching the Stream event-loop contract.

struct WsUrl {
    std::string host;
    std::string port;
    std::string path;
};

inline auto parse_ws_url(std::string_view url) -> WsUrl {
    if (url.starts_with("wss://"))
        throw std::runtime_error("ws_connect: TLS (wss://) is not supported yet: " +
                                 std::string(url));
    if (!url.starts_with("ws://"))
        throw std::runtime_error("ws_connect: URL must start with ws://: " + std::string(url));
    url.remove_prefix(std::string_view("ws://").size());

    WsUrl parsed;
    const auto slash = url.find('/');
    std::string_view authority = slash == std::string_view::npos ? url : url.substr(0, slash);
    parsed.path = slash == std::string_view::npos ? "/" : std::string(url.substr(slash));

    const auto colon = authority.find(':');
    if (colon == std::string_view::npos) {
        parsed.host = std::string(authority);
        parsed.port = "80";
    } else {
        parsed.host = std::string(authority.substr(0, colon));
        parsed.port = std::string(authority.substr(colon + 1));
    }
    if (parsed.host.empty())
        throw std::runtime_error("ws_connect: URL is missing a host: " + std::string(url));
    return parsed;
}

struct WsConnection {
    int fd{-1};
    std::string buf;  // partial receive buffer (may hold frames from handshake)
    bool eof{false};

    WsConnection() = default;
    WsConnection(WsConnection&&) = delete;
    WsConnection& operator=(WsConnection&&) = delete;
    WsConnection(const WsConnection&) = delete;
    auto operator=(const WsConnection&) -> WsConnection& = delete;

    ~WsConnection() {
        if (fd >= 0)
            ::close(fd);
    }
};

// Establish the TCP connection and perform the client opening handshake.
// Throws with a descriptive message on any failure.
inline void ws_client_connect(WsConnection& conn, const WsUrl& url, const WsOptions& options) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* addrs = nullptr;
    const int rc = ::getaddrinfo(url.host.c_str(), url.port.c_str(), &hints, &addrs);
    if (rc != 0)
        throw std::runtime_error("ws_connect: resolve " + url.host + ": " + ::gai_strerror(rc));

    int fd = -1;
    for (const addrinfo* ai = addrs; ai != nullptr; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0)
            continue;
        // Bound the handshake: 5 s send/receive timeouts (also bounds connect on Linux).
        struct timeval tv{.tv_sec = 5, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(addrs);
    if (fd < 0)
        throw std::runtime_error("ws_connect: cannot connect to " + url.host + ":" + url.port +
                                 ": " + std::strerror(errno));

    // ── Opening handshake ─────────────────────────────────────────────────────
    const std::string key = random_ws_key();
    const std::string request = "GET " + url.path +
                                " HTTP/1.1\r\n"
                                "Host: " +
                                url.host + ":" + url.port +
                                "\r\n"
                                "Upgrade: websocket\r\n"
                                "Connection: Upgrade\r\n"
                                "Sec-WebSocket-Key: " +
                                key +
                                "\r\n"
                                "Sec-WebSocket-Version: 13\r\n\r\n";
    if (::send(fd, request.data(), request.size(), MSG_NOSIGNAL) < 0) {
        ::close(fd);
        throw std::runtime_error(std::string("ws_connect: handshake send: ") +
                                 std::strerror(errno));
    }

    std::string response;
    while (response.find("\r\n\r\n") == std::string::npos) {
        if (response.size() > 64 * 1024) {
            ::close(fd);
            throw std::runtime_error("ws_connect: handshake response too large");
        }
        char tmp[4096];
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            ::close(fd);
            throw std::runtime_error("ws_connect: connection closed during handshake");
        }
        response.append(tmp, static_cast<std::size_t>(n));
    }

    const auto header_end = response.find("\r\n\r\n");
    const std::string_view headers = std::string_view(response).substr(0, header_end + 4);
    if (headers.find(" 101 ") == std::string_view::npos) {
        ::close(fd);
        throw std::runtime_error("ws_connect: server did not upgrade (no HTTP 101): " +
                                 std::string(headers.substr(0, headers.find('\r'))));
    }
    const auto accept = extract_http_header(headers, "Sec-WebSocket-Accept");
    if (!accept || *accept != compute_accept_key(key)) {
        ::close(fd);
        throw std::runtime_error("ws_connect: invalid Sec-WebSocket-Accept in handshake");
    }

    conn.fd = fd;
    // Keep any frame bytes that arrived bundled with the handshake response.
    conn.buf.assign(response, header_end + 4, response.size() - header_end - 4);

    if (!options.subscribe.empty()) {
        const std::string frame = ws_encode_frame(0x1U, options.subscribe, /*masked=*/true);
        if (::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL) < 0)
            throw std::runtime_error(std::string("ws_connect: subscribe send: ") +
                                     std::strerror(errno));
    }
}

// One cached connection per (url, schema, options) triple.
inline auto get_connection(const std::string& url, std::string_view schema_spec,
                           std::string_view options_spec, const WsOptions& options)
    -> WsConnection& {
    static robin_hood::unordered_map<std::string, std::unique_ptr<WsConnection>> g_connections;
    std::string cache_key = url + '\n';
    cache_key += schema_spec;
    cache_key += '\n';
    cache_key += options_spec;
    auto it = g_connections.find(cache_key);
    if (it != g_connections.end())
        return *it->second;

    auto conn = std::make_unique<WsConnection>();
    ws_client_connect(*conn, parse_ws_url(url), options);
    it = g_connections.emplace(std::move(cache_key), std::move(conn)).first;
    return *it->second;
}

inline auto ws_connect(std::string_view url, std::string_view schema_spec,
                       std::string_view options_spec = {}) -> ibex::runtime::ExternValue {
    using namespace ibex::runtime;

    const auto schema = parse_schema_or_throw(schema_spec);
    const auto options = parse_ws_options(options_spec, /*allow_subscribe=*/true);
    WsConnection& conn = get_connection(std::string(url), schema_spec, options_spec, options);

    if (conn.eof)
        return ExternValue{Table{}};

    // Drain buffered frames first (e.g. bundled with the handshake response),
    // then poll the socket once for more.
    for (int pass = 0; pass < 2; ++pass) {
        while (true) {
            auto frame_opt = ws_decode_frame(conn.buf);
            if (!frame_opt)
                break;

            const auto& frame = *frame_opt;

            if (frame.opcode == 0x8U) {
                // Close frame: echo (masked) close and end the stream.
                const std::string close_resp = ws_encode_frame(0x8U, {}, /*masked=*/true);
                ::send(conn.fd, close_resp.data(), close_resp.size(), MSG_NOSIGNAL);
                ::close(conn.fd);
                conn.fd = -1;
                conn.eof = true;
                return ExternValue{Table{}};
            }

            if (frame.opcode == 0x9U) {
                // Ping: send (masked) pong.
                const std::string pong = ws_encode_frame(0xAU, frame.payload, /*masked=*/true);
                ::send(conn.fd, pong.data(), pong.size(), MSG_NOSIGNAL);
                continue;
            }

            if (frame.opcode == 0x1U || frame.opcode == 0x0U) {
                Table row;
                switch (payload_to_row(frame.payload, schema, options, row)) {
                    case PayloadResult::Eof:
                        conn.eof = true;
                        return ExternValue{Table{}};
                    case PayloadResult::Row:
                        return ExternValue{std::move(row)};
                    case PayloadResult::Skip:
                        continue;
                }
            }
        }

        if (pass == 1)
            break;

        // ── Poll the socket for more bytes ────────────────────────────────────
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(conn.fd, &rfds);
        struct timeval tv{.tv_sec = options.poll_timeout_ms / 1000,
                          .tv_usec = (options.poll_timeout_ms % 1000) * 1000};
        const int nready = ::select(conn.fd + 1, &rfds, nullptr, nullptr, &tv);
        if (nready <= 0)
            return StreamTimeout{};

        char tmp[4096];
        const ssize_t n = ::recv(conn.fd, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            // Peer closed without a close frame.
            ::close(conn.fd);
            conn.fd = -1;
            conn.eof = true;
            return ExternValue{Table{}};
        }
        conn.buf.append(tmp, static_cast<std::size_t>(n));
    }

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
        if (cfd < 0)
            break;  // EAGAIN / EWOULDBLOCK — no more pending connections
        srv.clients.push_back(WsClient{cfd, false, {}});
    }

    // ── Promote pending handshakes ────────────────────────────────────────────
    for (auto& c : srv.clients) {
        if (c.fd < 0 || c.handshaked)
            continue;
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
        auto json = ibex::plugin::table_row_to_json(table, row);
        if (!json)
            throw std::runtime_error("ws_send: " + json.error());
        const std::string frame = ws_encode_text_frame(*json);

        for (auto& c : srv.clients) {
            if (!c.handshaked)
                continue;
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

/// ws_listen(port) — eagerly binds the listen socket on `port`.
/// Calling this before the stream loop lets clients connect before the first
/// bar is emitted.  Returns 0 on success; throws on bind failure.
inline auto ws_listen(std::int64_t port) -> std::int64_t {
    get_server(static_cast<int>(port));
    return 0;
}

}  // namespace ibex_ws

// ─── Global aliases ───────────────────────────────────────────────────────────
// Expose plugin functions without namespace qualification so that ibex_compile-
// generated code can call them by their extern fn name directly.
using ibex_ws::ws_connect;
using ibex_ws::ws_listen;
using ibex_ws::ws_recv;
using ibex_ws::ws_send;
