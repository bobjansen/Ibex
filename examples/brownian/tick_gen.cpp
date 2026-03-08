// tick_gen.cpp — multi-symbol geometric Brownian motion UDP tick generator.
//
// Sends a continuous stream of synthetic tick datagrams to a UDP port.
// Each symbol follows an independent GBM price process.
//
// Wire format (matches udp_recv plugin):
//   {"ts":<ns_since_epoch>,"symbol":"AAPL","price":182.35,"volume":600}
//
// Usage:
//   tick_gen [--host H] [--port P] [--rate HZ] [--sigma S]
//   (all flags optional; see defaults below)
//
// Build:
//   clang++ -std=c++23 -O2 -o tick_gen tick_gen.cpp

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <random>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

// ─── CLI ──────────────────────────────────────────────────────────────────────

struct Config {
    const char* host = "127.0.0.1";
    int port = 9001;
    double rate = 20.0;     // total ticks/sec across all symbols
    double sigma = 0.0015;  // per-tick GBM vol  (~24 % ann. at 20 ticks/s)
};

static void usage(const char* prog) {
    std::fprintf(stderr,
                 "Usage: %s [--host H] [--port P] [--rate HZ] [--sigma S]\n"
                 "  --host   destination host  (default 127.0.0.1)\n"
                 "  --port   destination port  (default 9001)\n"
                 "  --rate   ticks/sec total   (default 20)\n"
                 "  --sigma  per-tick GBM vol  (default 0.0015)\n",
                 prog);
}

static Config parse_args(int argc, char* argv[]) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        auto need = [&](const char* flag) -> const char* {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "error: %s requires a value\n", flag);
                std::exit(1);
            }
            return argv[++i];
        };
        if (arg == "--host")
            cfg.host = need("--host");
        else if (arg == "--port")
            cfg.port = std::atoi(need("--port"));
        else if (arg == "--rate")
            cfg.rate = std::atof(need("--rate"));
        else if (arg == "--sigma")
            cfg.sigma = std::atof(need("--sigma"));
        else {
            usage(argv[0]);
            std::exit(1);
        }
    }
    return cfg;
}

// ─── Symbols ──────────────────────────────────────────────────────────────────

struct Symbol {
    const char* name;
    double price;
};

static std::array<Symbol, 4> g_symbols{{
    {"AAPL", 182.0},
    {"GOOG", 175.0},
    {"MSFT", 415.0},
    {"AMZN", 198.0},
}};

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

    // UDP socket with a large send buffer to absorb bursts.
    const int sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        std::perror("socket");
        return 1;
    }
    {
        int sndbuf = 16 * 1024 * 1024;  // 16 MB
        ::setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(static_cast<std::uint16_t>(cfg.port));
    if (::inet_pton(AF_INET, cfg.host, &dest.sin_addr) != 1) {
        std::fprintf(stderr, "error: invalid host '%s'\n", cfg.host);
        return 1;
    }

    // GBM state
    std::mt19937_64 rng{std::random_device{}()};
    std::normal_distribution<double> gauss{0.0, 1.0};
    std::uniform_int_distribution<int> vol{1, 14};  // × 100 = 100..1400

    // At rates above this threshold one tick interval < OS scheduler
    // resolution (~100 µs), so sleep_until is meaningless.  Above the
    // threshold we send in batches as fast as the socket allows.
    static constexpr double kSleepThresholdHz = 50'000.0;

    // sendmmsg batch size.  Each slot holds one pre-built datagram.
    // 128 × 192 B = 24 KB on the stack — negligible.
    static constexpr int kBatch = 128;
    struct mmsghdr batch_msgs[kBatch]{};
    struct iovec batch_iovecs[kBatch]{};
    char batch_bufs[kBatch][192];

    for (int i = 0; i < kBatch; ++i) {
        batch_msgs[i].msg_hdr.msg_name = &dest;
        batch_msgs[i].msg_hdr.msg_namelen = sizeof(dest);
        batch_msgs[i].msg_hdr.msg_iov = &batch_iovecs[i];
        batch_msgs[i].msg_hdr.msg_iovlen = 1;
        batch_iovecs[i].iov_base = batch_bufs[i];
    }

    const auto tick_interval = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double>(static_cast<double>(kBatch) / cfg.rate));

    std::printf("tick_gen  |  %.0f ticks/s → udp://%s:%d  |  σ=%.4f  |  batch=%d\n", cfg.rate,
                cfg.host, cfg.port, cfg.sigma, kBatch);
    for (const auto& s : g_symbols)
        std::printf("  %-6s  %.2f\n", s.name, s.price);
    std::printf("Ctrl+C to stop.\n\n");
    std::fflush(stdout);

    long long n = 0;
    auto report_at = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    auto next_batch = std::chrono::steady_clock::now();

    while (g_running.load(std::memory_order_relaxed)) {
        // Build one batch of kBatch datagrams.
        for (int b = 0; b < kBatch; ++b) {
            auto& sym = g_symbols[static_cast<std::size_t>(n) % g_symbols.size()];

            // GBM step: S(t+1) = S(t) × exp(σ × Z)
            sym.price *= std::exp(cfg.sigma * gauss(rng));
            const int volume = vol(rng) * 100;

            const auto ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count();

            const int len =
                std::snprintf(batch_bufs[b], sizeof(batch_bufs[b]),
                              "{\"ts\":%lld,\"symbol\":\"%s\",\"price\":%.2f,\"volume\":%d}",
                              static_cast<long long>(ts_ns), sym.name, sym.price, volume);
            batch_iovecs[b].iov_len = static_cast<std::size_t>(len);
            ++n;
        }

        // Ship the whole batch in one syscall.
        ::sendmmsg(sock, batch_msgs, kBatch, MSG_NOSIGNAL);

        // Status report every 10 s
        const auto now = std::chrono::steady_clock::now();
        if (now >= report_at) {
            std::printf("%lld ticks  |", n);
            for (const auto& s : g_symbols)
                std::printf("  %s=%.2f", s.name, s.price);
            std::printf("\n");
            std::fflush(stdout);
            report_at += std::chrono::seconds(10);
        }

        // Only sleep when the batch interval is long enough for the OS to
        // honour it.  At high rates we let the socket back-pressure us.
        if (cfg.rate <= kSleepThresholdHz) {
            next_batch += tick_interval;
            std::this_thread::sleep_until(next_batch);
        }
    }

    std::printf("Stopped after %lld ticks.\n", n);
    ::close(sock);
}
