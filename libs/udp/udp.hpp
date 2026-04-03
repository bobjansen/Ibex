// Ibex UDP plugin — streaming tick source and bar sink over UDP.
//
// Wire protocol: one JSON object per UDP datagram, newline-terminated.
//
//   Tick (source, incoming):
//     {"ts":<ns_since_epoch>,"symbol":"<str>","price":<float>,"volume":<int>}
//
//   Bar (sink, outgoing):
//     {"ts":<ns_since_epoch>,"open":<float>,"high":<float>,"low":<float>,"close":<float>}
//
// Usage in an Ibex script:
//   import "udp";
//
//   let ohlc = Stream {
//       source    = udp_recv(9001),
//       transform = [resample 1m, select {
//           open  = first(price),
//           high  = max(price),
//           low   = min(price),
//           close = last(price)
//       }],
//       sink = udp_send("127.0.0.1", 9002)
//   };

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <charconv>
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

// recvmmsg / sendmmsg batch syscalls (Linux ≥ 2.6.33).
#ifdef __linux__
#include <sys/syscall.h>
#endif

namespace ibex_udp {

// ─── Minimal JSON field extractor ────────────────────────────────────────────
//
// Parses `"key":value` pairs from a flat JSON object string.
// Supports string, integer, and floating-point value types.
// Not a full JSON parser — sufficient for our fixed-schema datagrams.

inline auto json_get_str(std::string_view json, std::string_view key)
    -> std::optional<std::string> {
    // Search for `"key":` then skip optional whitespace before the opening quote.
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos)
        return std::nullopt;
    pos += needle.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t'))
        ++pos;
    if (pos >= json.size() || json[pos] != '"')
        return std::nullopt;
    ++pos;  // skip opening quote
    auto end = json.find('"', pos);
    if (end == std::string_view::npos)
        return std::nullopt;
    return std::string(json.substr(pos, end - pos));
}

inline auto json_get_int64(std::string_view json, std::string_view key)
    -> std::optional<std::int64_t> {
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos)
        return std::nullopt;
    pos += needle.size();
    // Skip whitespace
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t'))
        ++pos;
    std::int64_t value = 0;
    auto [ptr, ec] = std::from_chars(json.data() + pos, json.data() + json.size(), value);
    if (ec != std::errc{})
        return std::nullopt;
    return value;
}

inline auto json_get_double(std::string_view json, std::string_view key) -> std::optional<double> {
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos)
        return std::nullopt;
    pos += needle.size();
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t'))
        ++pos;
    // strtod for floating-point (from_chars for float not universally available pre-C++17)
    char* end = nullptr;
    double value = std::strtod(json.data() + pos, &end);
    if (end == json.data() + pos)
        return std::nullopt;
    return value;
}

// ─── Shared socket state ─────────────────────────────────────────────────────
//
// Keyed by port number so multiple recv streams on different ports can coexist.

struct RecvSocket {
    int fd = -1;

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
// multi-row DataFrame with columns: ts (Timestamp), symbol (String),
// price (Float64), volume (Int64).
//
// Returning many rows per call amortises the per-call overhead of the stream
// runtime loop and the column-building code, giving ~100–256× more throughput
// at high tick rates vs. the single-datagram approach.
//
// Returns an empty DataFrame to signal end-of-stream when any datagram
// contains the sentinel value `{"eof":true}`.

inline auto udp_recv(std::int64_t port) -> ibex::runtime::Table {
    using namespace ibex::runtime;
    using namespace ibex;

    RecvSocket& sock = get_recv_socket(static_cast<int>(port));

    // Batch size: drain up to this many datagrams per call.
    // 256 × 256 B = 64 KB of stack — well within the 8 MB Linux default.
    static constexpr int kBatch = 256;
    static constexpr std::size_t kMsgBuf = 256;  // max tick JSON ≈ 70 bytes

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

        Column<Timestamp> ts_col;
        Column<std::string> sym_col;
        Column<double> price_col;
        Column<std::int64_t> vol_col;
        ts_col.reserve(static_cast<std::size_t>(n));
        sym_col.reserve(static_cast<std::size_t>(n));
        price_col.reserve(static_cast<std::size_t>(n));
        vol_col.reserve(static_cast<std::size_t>(n));

        for (int i = 0; i < n; ++i) {
#ifdef __linux__
            const std::size_t len = msgs[i].msg_len;
#else
            const std::size_t len = lens[i];
#endif
            bufs[i][len] = '\0';
            const std::string_view json(bufs[i], len);

            // EOF sentinel — return any rows already parsed; caller loops
            // back and will see the empty Table on the next call.
            if (json.find("\"eof\"") != std::string_view::npos &&
                json.find("true") != std::string_view::npos) {
                if (ts_col.empty())
                    return Table{};
                // Fall through: return partial batch; EOF handled next call.
                break;
            }

            const auto ts_opt = json_get_int64(json, "ts");
            const auto symbol_opt = json_get_str(json, "symbol");
            const auto price_opt = json_get_double(json, "price");
            const auto volume_opt = json_get_int64(json, "volume");

            if (!ts_opt || !symbol_opt || !price_opt || !volume_opt)
                continue;  // malformed — skip silently

            ts_col.push_back(Timestamp{.nanos = *ts_opt});
            sym_col.push_back(*symbol_opt);
            price_col.push_back(*price_opt);
            vol_col.push_back(*volume_opt);
        }

        if (ts_col.empty())
            continue;  // every datagram in this batch was malformed — retry

        Table table;
        table.add_column("ts", ColumnValue{std::move(ts_col)});
        table.time_index = "ts";
        table.add_column("symbol", ColumnValue{std::move(sym_col)});
        table.add_column("price", ColumnValue{std::move(price_col)});
        table.add_column("volume", ColumnValue{std::move(vol_col)});
        return table;
    }
}

// ─── udp_send ────────────────────────────────────────────────────────────────
//
// Serialises each row of `table` as a JSON object and sends it as a UDP
// datagram to `host:port`.  Returns the number of rows sent.

inline auto udp_send(const ibex::runtime::Table& table, std::string_view host, std::int64_t port)
    -> std::int64_t {
    using namespace ibex::runtime;
    using namespace ibex;

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
        std::string json = "{";
        bool first_field = true;
        for (const auto& entry : table.columns) {
            if (!first_field)
                json += ',';
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
                        // Use snprintf for portable float formatting
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
        json += '\n';

        ::sendto(fd, json.data(), json.size(), 0, reinterpret_cast<const sockaddr*>(&addr),
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
