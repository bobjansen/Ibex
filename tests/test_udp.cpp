#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <arpa/inet.h>
#include <cstdint>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "udp.hpp"

namespace {

// Send one raw datagram to 127.0.0.1:port.
void send_datagram(int port, std::string_view payload) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    REQUIRE(fd >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));
    addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
    ::sendto(fd, payload.data(), payload.size(), 0, reinterpret_cast<const sockaddr*>(&addr),
             sizeof(addr));
    ::close(fd);
}

}  // namespace

TEST_CASE("parse_udp_options understands the malformed policy") {
    CHECK_FALSE(ibex_udp::parse_udp_options("").malformed_error);
    CHECK_FALSE(ibex_udp::parse_udp_options("on_malformed=skip").malformed_error);
    CHECK(ibex_udp::parse_udp_options("on_malformed=error").malformed_error);
    CHECK_THROWS(ibex_udp::parse_udp_options("on_malformed=maybe"));
    CHECK_THROWS(ibex_udp::parse_udp_options("bogus=1"));
}

// Loopback: udp_send serialises rows to JSON datagrams, udp_recv materialises
// them back through the schema string.  Malformed datagrams are skipped and
// the {"eof":true} sentinel ends the stream after any batched rows.

TEST_CASE("udp_recv: schema-driven batch receive skips malformed and honours eof") {
    constexpr int kPort = 17968;
    const std::string schema = "ts:timestamp,symbol:str,price:f64,volume:i64";

    // Bind the receive socket before sending so the datagrams are buffered.
    try {
        ibex_udp::get_recv_socket(kPort);
    } catch (const std::runtime_error& e) {
        const std::string msg = e.what();
        if (msg.find("Operation not permitted") != std::string::npos ||
            msg.find("Permission denied") != std::string::npos) {
            SKIP("udp test requires socket permissions");
        }
        throw;
    }

    ibex::runtime::Table ticks;
    ticks.add_column("ts", ibex::Column<ibex::Timestamp>{ibex::Timestamp{123456789},
                                                         ibex::Timestamp{123456790}});
    ticks.add_column("symbol", ibex::Column<std::string>{"AAPL", "MSFT"});
    ticks.add_column("price", ibex::Column<double>{101.5, 330.25});
    ticks.add_column("volume", ibex::Column<std::int64_t>{42, 7});

    CHECK(ibex_udp::udp_send(ticks, "127.0.0.1", kPort) == 2);
    send_datagram(kPort, "not json");
    send_datagram(kPort, R"({"eof":true})");

    // Drain: rows first (possibly split across batches), then the empty EOF
    // table.  Concatenate values across batches so a kernel-side split of the
    // datagram burst cannot flake the test.
    std::vector<std::int64_t> ts_vals;
    std::vector<std::string> symbols;
    std::vector<double> prices;
    std::vector<std::int64_t> volumes;
    bool got_eof = false;
    for (int i = 0; i < 10 && !got_eof; ++i) {
        auto table = ibex_udp::udp_recv(kPort, schema);
        if (table.rows() == 0) {
            got_eof = true;
            break;
        }
        REQUIRE(table.time_index.has_value());
        CHECK(*table.time_index == "ts");
        const auto* ts = std::get_if<ibex::Column<ibex::Timestamp>>(table.find("ts"));
        const auto* symbol = std::get_if<ibex::Column<std::string>>(table.find("symbol"));
        const auto* price = std::get_if<ibex::Column<double>>(table.find("price"));
        const auto* volume = std::get_if<ibex::Column<std::int64_t>>(table.find("volume"));
        REQUIRE(ts != nullptr);
        REQUIRE(symbol != nullptr);
        REQUIRE(price != nullptr);
        REQUIRE(volume != nullptr);
        for (std::size_t r = 0; r < table.rows(); ++r) {
            ts_vals.push_back((*ts)[r].nanos);
            symbols.emplace_back((*symbol)[r]);
            prices.push_back((*price)[r]);
            volumes.push_back((*volume)[r]);
        }
    }

    CHECK(got_eof);
    REQUIRE(ts_vals.size() == 2);
    CHECK(ts_vals[0] == 123456789);
    CHECK(ts_vals[1] == 123456790);
    CHECK(symbols[0] == "AAPL");
    CHECK(symbols[1] == "MSFT");
    CHECK(prices[0] == Catch::Approx(101.5));
    CHECK(prices[1] == Catch::Approx(330.25));
    CHECK(volumes[0] == 42);
    CHECK(volumes[1] == 7);
}
