// Ibex UDP plugin — schema-driven streaming source and sink over UDP.
//
// Wire protocol: one JSON object per UDP datagram, newline-terminated.
// The message shape is described by an explicit schema string using the
// shared plugin mini-language (see <ibex/plugin/schema.hpp>):
//
//   "ts:timestamp,symbol:str,price:f64,volume:i64"
//
// Usage in an Ibex script:
//   import "udp";
//
//   let ohlc = Stream {
//       source    = udp_recv(9001, "ts:ts,symbol:str,price:f64,volume:i64"),
//       transform = [resample 1m, select {
//           open  = first(price),
//           high  = max(price),
//           low   = min(price),
//           close = last(price)
//       }],
//       sink = udp_send("127.0.0.1", 9002)
//   };
//
// Options (key=value pairs separated by ';'):
//   on_malformed=skip|error   what to do with datagrams that fail the schema
//                             (default: skip)
//
// End-of-stream: a datagram {"eof":true} yields an empty DataFrame, which
// terminates the Stream event loop.

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/plugin/schema.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// recvmmsg / sendmmsg batch syscalls (Linux ≥ 2.6.33).
#ifdef __linux__
#include <sys/syscall.h>
#endif

namespace ibex_udp {

// ─── Options ──────────────────────────────────────────────────────────────────

struct UdpOptions {
    bool malformed_error = false;  // on_malformed=skip (default) | error
};

inline auto parse_udp_options(std::string_view spec) -> UdpOptions {
    UdpOptions options;
    auto parsed = ibex::plugin::parse_key_value_options(spec);
    if (!parsed)
        throw std::runtime_error("udp: " + parsed.error());
    for (const auto& [key, value] : *parsed) {
        if (key == "on_malformed") {
            if (value == "skip") {
                options.malformed_error = false;
            } else if (value == "error") {
                options.malformed_error = true;
            } else {
                throw std::runtime_error("udp: on_malformed must be 'skip' or 'error'");
            }
            continue;
        }
        throw std::runtime_error("udp: unsupported option: " + key);
    }
    return options;
}

// ─── Shared socket state ─────────────────────────────────────────────────────
//
// Keyed by port number so multiple recv streams on different ports can coexist.

struct RecvSocket {
    int fd = -1;
    // Set when an {"eof":true} sentinel arrived bundled with data rows: the
    // rows are returned first and the EOF is delivered on the next call.
    bool pending_eof = false;

    explicit RecvSocket(int port) {
        fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) {
            throw std::runtime_error(std::string("udp_recv: socket: ") + std::strerror(errno));
        }
        int reuse = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        // Request a large receive buffer to absorb bursts at high tick rates.
        // The kernel will grant up to /proc/sys/net/core/rmem_max; even if
        // capped, requesting the maximum is always worth doing.
        int rcvbuf = 16 * 1024 * 1024;  // 16 MB
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        addr.sin_addr.s_addr = INADDR_ANY;
        if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(fd);
            fd = -1;
            throw std::runtime_error(std::string("udp_recv: bind port ") + std::to_string(port) +
                                     ": " + std::strerror(errno));
        }
    }

    ~RecvSocket() {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    RecvSocket(const RecvSocket&) = delete;
    auto operator=(const RecvSocket&) -> RecvSocket& = delete;
};

inline auto get_recv_socket(int port) -> RecvSocket& {
    static std::unordered_map<int, std::unique_ptr<RecvSocket>> sockets;
    auto it = sockets.find(port);
    if (it == sockets.end()) {
        it = sockets.emplace(port, std::make_unique<RecvSocket>(port)).first;
    }
    return *it->second;
}

// ─── udp_recv ────────────────────────────────────────────────────────────────
//
// Blocks until at least one UDP datagram arrives on `port`, then drains up to
// kBatch datagrams in a single recvmmsg syscall, parses them, and returns a
// multi-row DataFrame whose columns follow `schema_spec`.
//
// Returning many rows per call amortises the per-call overhead of the stream
// runtime loop and the column-building code, giving ~100–256× more throughput
// at high tick rates vs. the single-datagram approach.
//
// Returns an empty DataFrame to signal end-of-stream when any datagram
// contains the sentinel value `{"eof":true}` (rows received before the
// sentinel are returned first; the EOF is delivered on the next call).

inline auto udp_recv(std::int64_t port, std::string_view schema_spec,
                     std::string_view options_spec = {}) -> ibex::runtime::Table {
    using namespace ibex::runtime;

    auto schema_result = ibex::plugin::parse_schema(schema_spec);
    if (!schema_result)
        throw std::runtime_error("udp_recv: " + schema_result.error());
    const auto& schema = *schema_result;
    const auto options = parse_udp_options(options_spec);

    RecvSocket& sock = get_recv_socket(static_cast<int>(port));

    if (sock.pending_eof) {
        sock.pending_eof = false;
        return Table{};
    }

    // Batch size: drain up to this many datagrams per call.
    // 256 × 1 KB = 256 KB of stack — well within the 8 MB Linux default.
    static constexpr int kBatch = 256;
    static constexpr std::size_t kMsgBuf = 1024;

    int n = 0;
#ifdef __linux__
    struct mmsghdr msgs[kBatch]{};
    struct iovec iovecs[kBatch]{};
    char bufs[kBatch][kMsgBuf];

    for (int i = 0; i < kBatch; ++i) {
        iovecs[i].iov_base = bufs[i];
        iovecs[i].iov_len = kMsgBuf - 1;  // leave room for '\0'
        msgs[i].msg_hdr.msg_iov = &iovecs[i];
        msgs[i].msg_hdr.msg_iovlen = 1;
    }

    while (true) {
        // MSG_WAITFORONE: block until ≥1 datagram is available, then
        // return however many are ready (up to kBatch) without further
        // blocking.  This avoids both busy-spinning and per-message wakeups.
        n = ::recvmmsg(sock.fd, msgs, kBatch, MSG_WAITFORONE, nullptr);
        if (n <= 0) {
            return Table{};  // socket error / closed
        }
#else
    char bufs[kBatch][kMsgBuf]{};
    std::size_t lens[kBatch]{};

    while (true) {
        const auto first_len = ::recv(sock.fd, bufs[0], kMsgBuf - 1, 0);
        if (first_len <= 0) {
            return Table{};  // socket error / closed
        }
        lens[0] = static_cast<std::size_t>(first_len);
        n = 1;

        while (n < kBatch) {
            const auto len = ::recv(sock.fd, bufs[n], kMsgBuf - 1, MSG_DONTWAIT);
            if (len <= 0) {
                break;
            }
            lens[n] = static_cast<std::size_t>(len);
            ++n;
        }
#endif
        if (n <= 0) {
            return Table{};  // socket error / closed
        }

        bool saw_eof = false;
        std::vector<nlohmann::json> objects;
        objects.reserve(static_cast<std::size_t>(n));

        for (int i = 0; i < n; ++i) {
#ifdef __linux__
            const std::size_t len = msgs[i].msg_len;
#else
            const std::size_t len = lens[i];
#endif
            bufs[i][len] = '\0';
            const std::string_view payload(bufs[i], len);

            nlohmann::json object =
                nlohmann::json::parse(payload, nullptr, /*allow_exceptions=*/false);
            if (object.is_discarded() || !object.is_object()) {
                if (options.malformed_error)
                    throw std::runtime_error("udp_recv: payload is not a JSON object");
                continue;  // malformed — skip silently
            }

            // EOF sentinel — return any rows already parsed; the EOF itself
            // is delivered on the next call via pending_eof.
            if (auto it = object.find("eof");
                it != object.end() && it->is_boolean() && it->get<bool>()) {
                saw_eof = true;
                break;
            }

            if (!options.malformed_error && !ibex::plugin::json_matches_schema(object, schema)) {
                continue;  // does not fit the schema — skip silently
            }
            objects.push_back(std::move(object));
        }

        if (objects.empty()) {
            if (saw_eof)
                return Table{};
            continue;  // every datagram in this batch was malformed — retry
        }

        auto table = ibex::plugin::table_from_json_objects(objects, schema);
        if (!table)
            throw std::runtime_error("udp_recv: " + table.error());
        sock.pending_eof = saw_eof;
        return std::move(*table);
    }
}

// ─── udp_send ────────────────────────────────────────────────────────────────
//
// Serialises each row of `table` as a JSON object and sends it as a UDP
// datagram to `host:port`.  Returns the number of rows sent.

inline auto udp_send(const ibex::runtime::Table& table, std::string_view host, std::int64_t port)
    -> std::int64_t {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        throw std::runtime_error(std::string("udp_send: socket: ") + std::strerror(errno));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, std::string(host).c_str(), &addr.sin_addr) <= 0) {
        ::close(fd);
        throw std::runtime_error("udp_send: invalid host address: " + std::string(host));
    }

    std::int64_t sent = 0;
    const std::size_t rows = table.rows();

    for (std::size_t row = 0; row < rows; ++row) {
        auto json = ibex::plugin::table_row_to_json(table, row);
        if (!json) {
            ::close(fd);
            throw std::runtime_error("udp_send: " + json.error());
        }
        json->push_back('\n');

        ::sendto(fd, json->data(), json->size(), 0, reinterpret_cast<const sockaddr*>(&addr),
                 sizeof(addr));
        ++sent;
    }

    ::close(fd);
    return sent;
}

}  // namespace ibex_udp

// ─── Global aliases ───────────────────────────────────────────────────────────
// Expose plugin functions without namespace qualification so that ibex_compile-
// generated code can call them by their extern fn name directly.
using ibex_udp::udp_recv;
using ibex_udp::udp_send;
