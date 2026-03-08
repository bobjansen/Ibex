// monitor.cpp — WebSocket bar monitor for the Ibex Brownian motion demo.
//
// Connects to the Ibex WebSocket stream (port 8765) and:
//   • logs every incoming bar on a single line as it arrives
//   • prints a per-symbol OHLC summary table every 10 seconds
//
// Wire format received:
//   {"ts":<ns>,"symbol":"AAPL","open":182.0,"high":183.1,"low":181.8,"close":182.5}
//
// Build (no external deps):
//   clang++ -std=c++23 -O2 -o monitor monitor.cpp

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// ─── CLI ──────────────────────────────────────────────────────────────────────

struct Config {
    const char* host = "127.0.0.1";
    int port = 8765;
    double interval = 10.0;  // seconds between summary tables
};

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        auto need = [&](const char* f) -> const char* {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s needs a value\n", f);
                std::exit(1);
            }
            return argv[++i];
        };
        if (arg == "--host")
            cfg.host = need("--host");
        else if (arg == "--port")
            cfg.port = std::atoi(need("--port"));
        else if (arg == "--interval")
            cfg.interval = std::atof(need("--interval"));
        else {
            std::fprintf(stderr, "Usage: %s [--host H] [--port P] [--interval SECS]\n", argv[0]);
            std::exit(1);
        }
    }
    return cfg;
}

// ─── SHA-1 (RFC 3174) ─────────────────────────────────────────────────────────
// Identical implementation to websocket.hpp but without ibex headers.

struct Sha1Digest {
    std::array<std::uint8_t, 20> bytes{};
};

static auto sha1(const std::uint8_t* data, std::size_t len) -> Sha1Digest {
    std::uint32_t h0 = 0x67452301u, h1 = 0xEFCDAB89u, h2 = 0x98BADCFEu, h3 = 0x10325476u,
                  h4 = 0xC3D2E1F0u;

    auto rol32 = [](std::uint32_t v, int n) -> std::uint32_t {
        return (v << n) | (v >> (32 - n));
    };

    std::vector<std::uint8_t> msg(data, data + len);
    msg.push_back(0x80u);
    while (msg.size() % 64 != 56)
        msg.push_back(0x00u);
    const std::uint64_t bit_len = static_cast<std::uint64_t>(len) * 8u;
    for (int i = 7; i >= 0; --i)
        msg.push_back(static_cast<std::uint8_t>((bit_len >> (i * 8)) & 0xFFu));

    for (std::size_t chunk = 0; chunk < msg.size(); chunk += 64) {
        std::array<std::uint32_t, 80> w{};
        for (int i = 0; i < 16; ++i) {
            const std::uint8_t* p = &msg[chunk + static_cast<std::size_t>(i) * 4];
            w[static_cast<std::size_t>(i)] = (std::uint32_t{p[0]} << 24) |
                                             (std::uint32_t{p[1]} << 16) |
                                             (std::uint32_t{p[2]} << 8) | std::uint32_t{p[3]};
        }
        for (int i = 16; i < 80; ++i) {
            auto idx = static_cast<std::size_t>(i);
            w[idx] = rol32(w[idx - 3] ^ w[idx - 8] ^ w[idx - 14] ^ w[idx - 16], 1);
        }
        std::uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
        for (int i = 0; i < 80; ++i) {
            std::uint32_t f{}, k{};
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
            const std::uint32_t tmp = rol32(a, 5) + f + e + k + w[static_cast<std::size_t>(i)];
            e = d;
            d = c;
            c = rol32(b, 30);
            b = a;
            a = tmp;
        }
        h0 += a;
        h1 += b;
        h2 += c;
        h3 += d;
        h4 += e;
    }
    Sha1Digest out;
    auto store = [&](std::size_t i, std::uint32_t h) {
        out.bytes[i * 4 + 0] = static_cast<std::uint8_t>((h >> 24) & 0xFF);
        out.bytes[i * 4 + 1] = static_cast<std::uint8_t>((h >> 16) & 0xFF);
        out.bytes[i * 4 + 2] = static_cast<std::uint8_t>((h >> 8) & 0xFF);
        out.bytes[i * 4 + 3] = static_cast<std::uint8_t>(h & 0xFF);
    };
    store(0, h0);
    store(1, h1);
    store(2, h2);
    store(3, h3);
    store(4, h4);
    return out;
}

// ─── Base64 encoder ───────────────────────────────────────────────────────────

static auto base64_encode(const std::uint8_t* data, std::size_t len) -> std::string {
    static constexpr std::string_view kC =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    for (std::size_t i = 0; i < len; i += 3) {
        const std::uint32_t a = data[i];
        const std::uint32_t b = (i + 1 < len) ? data[i + 1] : 0u;
        const std::uint32_t c = (i + 2 < len) ? data[i + 2] : 0u;
        const std::uint32_t t = (a << 16) | (b << 8) | c;
        out += kC[(t >> 18) & 0x3F];
        out += kC[(t >> 12) & 0x3F];
        out += (i + 1 < len) ? kC[(t >> 6) & 0x3F] : '=';
        out += (i + 2 < len) ? kC[t & 0x3F] : '=';
    }
    return out;
}

// ─── WebSocket handshake ──────────────────────────────────────────────────────

static auto compute_accept(std::string_view key) -> std::string {
    static constexpr std::string_view kMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    std::string combined = std::string(key) + std::string(kMagic);
    auto digest = sha1(reinterpret_cast<const std::uint8_t*>(combined.data()), combined.size());
    return base64_encode(digest.bytes.data(), digest.bytes.size());
}

// ─── TCP connect with retry ───────────────────────────────────────────────────

static auto tcp_connect(const char* host, int port, int retries = 30) -> int {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));
    ::inet_pton(AF_INET, host, &addr.sin_addr);

    for (int i = 0; i < retries; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return -1;
        if (::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) == 0)
            return fd;
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return -1;
}

// ─── Receive buffer — exact reads, leftover-safe ─────────────────────────────

struct RecvBuf {
    int fd;
    std::string data;

    // Read exactly n bytes; returns false on EOF/error.
    auto read_exact(std::size_t n) -> bool {
        while (data.size() < n) {
            char tmp[4096];
            const ssize_t r = ::recv(fd, tmp, sizeof(tmp), 0);
            if (r <= 0)
                return false;
            data.append(tmp, static_cast<std::size_t>(r));
        }
        return true;
    }

    auto consume(std::size_t n) -> std::string {
        std::string out = data.substr(0, n);
        data.erase(0, n);
        return out;
    }

    // Read until delimiter; everything past delimiter stays in data.
    auto read_until(std::string_view delim) -> bool {
        while (data.find(delim) == std::string::npos) {
            char tmp[4096];
            const ssize_t r = ::recv(fd, tmp, sizeof(tmp), 0);
            if (r <= 0)
                return false;
            data.append(tmp, static_cast<std::size_t>(r));
        }
        return true;
    }
};

// ─── WebSocket handshake (client side) ───────────────────────────────────────

static auto ws_handshake(RecvBuf& rb, const char* host, int port) -> bool {
    // Random 16-byte key
    std::array<std::uint8_t, 16> raw_key{};
    for (auto& b : raw_key)
        b = static_cast<std::uint8_t>(std::rand() & 0xFF);
    const std::string key_b64 = base64_encode(raw_key.data(), raw_key.size());
    const std::string expected = compute_accept(key_b64);

    // Send HTTP upgrade
    std::string req =
        "GET /ibex HTTP/1.1\r\n"
        "Host: " +
        std::string(host) + ":" + std::to_string(port) +
        "\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: " +
        key_b64 +
        "\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n";
    if (::send(rb.fd, req.data(), req.size(), MSG_NOSIGNAL) < 0)
        return false;

    // Read until end of HTTP headers; leftover bytes (first WS frame) stay in buf.
    if (!rb.read_until("\r\n\r\n"))
        return false;
    const auto sep = rb.data.find("\r\n\r\n");
    const std::string headers = rb.data.substr(0, sep);
    rb.data.erase(0, sep + 4);

    if (headers.find("101 Switching Protocols") == std::string::npos) {
        std::fprintf(stderr, "monitor: upgrade failed:\n%s\n", headers.c_str());
        return false;
    }
    if (headers.find(expected) == std::string::npos) {
        std::fprintf(stderr, "monitor: Sec-WebSocket-Accept mismatch\n");
        return false;
    }
    return true;
}

// ─── WebSocket frame reader (server→client = unmasked) ───────────────────────

struct WsFrame {
    std::uint8_t opcode{};
    std::string payload;
};

static auto read_ws_frame(RecvBuf& rb) -> std::optional<WsFrame> {
    if (!rb.read_exact(2))
        return std::nullopt;
    const auto b0 = static_cast<std::uint8_t>(rb.data[0]);
    const auto b1 = static_cast<std::uint8_t>(rb.data[1]);
    rb.data.erase(0, 2);

    const std::uint8_t opcode = b0 & 0x0Fu;
    std::size_t payload_len = b1 & 0x7Fu;

    if (payload_len == 126) {
        if (!rb.read_exact(2))
            return std::nullopt;
        payload_len =
            (static_cast<std::uint8_t>(rb.data[0]) << 8) | static_cast<std::uint8_t>(rb.data[1]);
        rb.data.erase(0, 2);
    } else if (payload_len == 127) {
        if (!rb.read_exact(8))
            return std::nullopt;
        payload_len = 0;
        for (int i = 0; i < 8; ++i)
            payload_len = (payload_len << 8) | static_cast<std::uint8_t>(rb.data[i]);
        rb.data.erase(0, 8);
    }

    if (!rb.read_exact(payload_len))
        return std::nullopt;
    WsFrame f;
    f.opcode = opcode;
    f.payload = rb.consume(payload_len);
    return f;
}

// ─── Minimal JSON field extraction ───────────────────────────────────────────

static auto json_str(std::string_view json, std::string_view key) -> std::optional<std::string> {
    std::string needle = "\"";
    needle += key;
    needle += "\":\"";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos)
        return std::nullopt;
    pos += needle.size();
    auto end = json.find('"', pos);
    if (end == std::string_view::npos)
        return std::nullopt;
    return std::string(json.substr(pos, end - pos));
}

static auto json_double(std::string_view json, std::string_view key) -> std::optional<double> {
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos)
        return std::nullopt;
    pos += needle.size();
    char* end = nullptr;
    double v = std::strtod(json.data() + pos, &end);
    if (end == json.data() + pos)
        return std::nullopt;
    return v;
}

static auto json_int64(std::string_view json, std::string_view key) -> std::optional<std::int64_t> {
    std::string needle = "\"";
    needle += key;
    needle += "\":";
    auto pos = json.find(needle);
    if (pos == std::string_view::npos)
        return std::nullopt;
    pos += needle.size();
    char* end = nullptr;
    long long v = std::strtoll(json.data() + pos, &end, 10);
    if (end == json.data() + pos)
        return std::nullopt;
    return static_cast<std::int64_t>(v);
}

// ─── Bar + shared state ───────────────────────────────────────────────────────

struct Bar {
    std::int64_t ts{};
    double open{}, high{}, low{}, close{};
};

static std::mutex g_mu;
static std::unordered_map<std::string, Bar> g_bars;  // latest bar per symbol

// ─── ANSI helpers ─────────────────────────────────────────────────────────────

static constexpr std::string_view kReset = "\033[0m";
static constexpr std::string_view kBold = "\033[1m";
static constexpr std::string_view kGreen = "\033[32m";
static constexpr std::string_view kRed = "\033[31m";
static constexpr std::string_view kCyan = "\033[36m";

static auto ns_to_hms(std::int64_t ns) -> std::string {
    const std::time_t sec = static_cast<std::time_t>(ns / 1'000'000'000LL);
    char buf[32];
    struct tm tm_utc{};
    ::gmtime_r(&sec, &tm_utc);
    std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec);
    return buf;
}

// ─── Summary printer ─────────────────────────────────────────────────────────

static void print_summary(const std::unordered_map<std::string, Bar>& bars) {
    char timebuf[32];
    const std::time_t now = std::time(nullptr);
    struct tm tm_utc{};
    ::gmtime_r(&now, &tm_utc);
    std::strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S UTC", &tm_utc);

    std::printf("\n%s%s┌──────────────────────────────────────────────────────────┐\n",
                kBold.data(), kCyan.data());
    std::printf("│  10-second snapshot  %-36s│\n", timebuf);
    std::printf("├──────────┬───────────┬───────────┬───────────┬───────────┤\n");
    std::printf("│ %-8s │ %9s │ %9s │ %9s │ %9s │\n%s", "Symbol", "Open", "High", "Low", "Close",
                kReset.data());
    std::printf("%s%s├──────────┼───────────┼───────────┼───────────┼───────────┤%s\n",
                kBold.data(), kCyan.data(), kReset.data());

    if (bars.empty()) {
        std::printf("│  (no bars yet)                                           │\n");
    } else {
        // Collect and sort by symbol name
        std::vector<std::pair<std::string, Bar>> sorted(bars.begin(), bars.end());
        std::sort(sorted.begin(), sorted.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });

        for (const auto& [sym, b] : sorted) {
            const double chg = (b.close - b.open) / b.open * 100.0;
            const bool up = chg >= 0.0;
            const char* color = up ? kGreen.data() : kRed.data();
            const char* arrow = up ? "▲" : "▼";
            std::printf(
                "│ %s%-8s%s │ %9.2f │ %9.2f │ %9.2f │ %s%9.2f%s │"
                "  %s%s%+.2f%%%s\n",
                kBold.data(), sym.c_str(), kReset.data(), b.open, b.high, b.low, color, b.close,
                kReset.data(), color, arrow, chg, kReset.data());
        }
    }

    std::printf("%s%s└──────────┴───────────┴───────────┴───────────┴───────────┘%s\n\n",
                kBold.data(), kCyan.data(), kReset.data());
    std::fflush(stdout);
}

// ─── Signal ───────────────────────────────────────────────────────────────────

static std::atomic<bool> g_running{true};
static void on_signal(int) {
    g_running.store(false, std::memory_order_relaxed);
}

// ─── Main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    const Config cfg = parse_args(argc, argv);
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::srand(static_cast<unsigned>(std::time(nullptr)));

    std::printf("Connecting to ws://%s:%d ...\n", cfg.host, cfg.port);
    const int fd = tcp_connect(cfg.host, cfg.port);
    if (fd < 0) {
        std::fprintf(stderr, "monitor: could not connect to %s:%d\n", cfg.host, cfg.port);
        return 1;
    }

    RecvBuf rb{fd, {}};
    if (!ws_handshake(rb, cfg.host, cfg.port))
        return 1;
    std::printf("Connected.  Summary every %.0f s.  Each bar logged as it arrives.\n\n",
                cfg.interval);
    std::printf("  %s%-10s  %-8s  %9s  %9s  %9s  %9s%s\n", kBold.data(), "Time", "Symbol", "Open",
                "High", "Low", "Close", kReset.data());
    std::printf("  %s\n", std::string(64, '-').c_str());
    std::fflush(stdout);

    // Background summary thread
    const auto interval_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double>(cfg.interval));
    std::thread summary_thread([&] {
        auto next = std::chrono::steady_clock::now() + interval_ns;
        while (g_running.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_until(next);
            next += interval_ns;
            std::unordered_map<std::string, Bar> snapshot;
            {
                std::lock_guard lk(g_mu);
                snapshot = g_bars;
            }
            print_summary(snapshot);
        }
    });

    // Receive loop
    while (g_running.load(std::memory_order_relaxed)) {
        auto frame_opt = read_ws_frame(rb);
        if (!frame_opt)
            break;  // EOF or error
        const auto& frame = *frame_opt;

        if (frame.opcode == 0x8) {  // Close
            std::printf("\nServer closed connection.\n");
            break;
        }
        if (frame.opcode == 0x9) {  // Ping → Pong
            const std::string pong = {'\x8A', static_cast<char>(frame.payload.size() & 0x7F)};
            ::send(fd, (pong + frame.payload).data(), pong.size() + frame.payload.size(),
                   MSG_NOSIGNAL);
            continue;
        }
        if (frame.opcode != 0x1 && frame.opcode != 0x0)
            continue;  // not text

        std::string_view json = frame.payload;
        const auto sym = json_str(json, "symbol");
        const auto open = json_double(json, "open");
        const auto high = json_double(json, "high");
        const auto low = json_double(json, "low");
        const auto close = json_double(json, "close");
        const auto ts = json_int64(json, "ts");

        if (!sym || !open || !high || !low || !close) {
            std::fprintf(stderr, "monitor: malformed bar: %.*s\n", static_cast<int>(json.size()),
                         json.data());
            continue;
        }

        const Bar bar{ts.value_or(0), *open, *high, *low, *close};
        {
            std::lock_guard lk(g_mu);
            g_bars[*sym] = bar;
        }

        const double chg = (*close - *open) / *open * 100.0;
        const char* color = (chg >= 0) ? kGreen.data() : kRed.data();
        std::printf("  %-10s  %s%-8s%s  %9.2f  %9.2f  %9.2f  %s%9.2f%s  %s%+.2f%%%s\n",
                    ns_to_hms(ts.value_or(0)).c_str(), kBold.data(), sym->c_str(), kReset.data(),
                    *open, *high, *low, color, *close, kReset.data(), color, chg, kReset.data());
        std::fflush(stdout);
    }

    g_running.store(false, std::memory_order_relaxed);
    summary_thread.join();

    ::send(fd, "\x88\x00", 2, MSG_NOSIGNAL);
    ::close(fd);
}
