// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "../libs/parquet/dictionary_policy.hpp"

namespace policy = ibex::parquet_dict;

TEST_CASE("dictionary policy keeps a small dictionary whatever it compresses",
          "[parquet][dictionary]") {
    // nation: 25 names for 25 rows. It compresses nothing, and it must still be
    // kept -- 25 entries cost nothing to intern, and the queries that group by
    // n_name hash by code instead of by string. Gating this on compression
    // alone regressed q09 by 14%.
    CHECK(policy::worth_keeping(25, 25));
    CHECK(policy::worth_keeping(5, 5));           // region
    CHECK(policy::worth_keeping(6000, 3000000));  // o_clerk
}

TEST_CASE("dictionary policy keeps a large dictionary that earns its keep",
          "[parquet][dictionary]") {
    // c_phone: 55k entries standing in for 300k rows. Too many to call small,
    // but each entry covers five rows, so every row-wise string operation above
    // runs once per entry instead of once per row.
    CHECK(policy::worth_keeping(55296, 300000));
    CHECK(policy::worth_keeping(410665, 12000000));  // l_comment
}

TEST_CASE("dictionary policy drops a dictionary with an entry per row", "[parquet][dictionary]") {
    // supplier's s_name/s_address/s_phone: 20000 entries for 20000 rows. Large
    // enough to cost real interning and compressing nothing in return. This is
    // the case that made a 20,000-row read take 9ms per column.
    CHECK_FALSE(policy::worth_keeping(20000, 20000));
    CHECK_FALSE(policy::worth_keeping(16384, 20000));  // s_comment
}

TEST_CASE("dictionary policy uses actual counts at the retention boundaries",
          "[parquet][dictionary]") {
    CHECK(policy::worth_keeping(8192, 8192));
    CHECK_FALSE(policy::worth_keeping(8193, 8193));
    CHECK(policy::worth_keeping(10000, 20000));
    CHECK_FALSE(policy::worth_keeping(10001, 20000));
    CHECK_FALSE(policy::worth_keeping(10001, 20001));
    CHECK(policy::worth_keeping(100, 10000));  // also true for arbitrarily long labels
}

namespace {

// A Thrift compact-protocol PageHeader as parquet-mr and Arrow write it: an
// i32 field is header byte 0x15 (delta 1, type 5) and a zigzag varint.
auto zigzag_varint(std::int64_t value) -> std::vector<std::uint8_t> {
    auto v = (static_cast<std::uint64_t>(value) << 1) ^ static_cast<std::uint64_t>(value >> 63);
    std::vector<std::uint8_t> out;
    while (v >= 0x80) {
        out.push_back(static_cast<std::uint8_t>(v | 0x80));
        v >>= 7;
    }
    out.push_back(static_cast<std::uint8_t>(v));
    return out;
}

auto page_header(std::int64_t page_type, std::int64_t entries, bool with_crc)
    -> std::vector<std::uint8_t> {
    std::vector<std::uint8_t> out;
    auto field = [&](std::uint8_t header, std::int64_t value) {
        out.push_back(header);
        const auto v = zigzag_varint(value);
        out.insert(out.end(), v.begin(), v.end());
    };
    field(0x15, page_type);  // 1: type
    field(0x15, 1 << 20);    // 2: uncompressed_page_size
    field(0x15, 1 << 19);    // 3: compressed_page_size
    if (with_crc) {
        field(0x15, -12345);  // 4: crc
    }
    out.push_back(with_crc ? 0x3C : 0x4C);  // 7: dictionary_page_header (struct)
    field(0x15, entries);                   //   1: num_values
    field(0x15, 0);                         //   2: encoding PLAIN
    out.push_back(0x11);                    //   3: is_sorted = true
    out.push_back(0x00);                    //   stop
    out.push_back(0x00);                    // stop
    return out;
}

auto entries_of(const std::vector<std::uint8_t>& bytes) {
    return policy::dictionary_header_entries(bytes.data(), bytes.size());
}

}  // namespace

TEST_CASE("dictionary header count reads num_values from the page header",
          "[parquet][dictionary]") {
    CHECK(entries_of(page_header(2, 25, false)) == 25);
    CHECK(entries_of(page_header(2, 410665, true)) == 410665);
    CHECK(entries_of(page_header(2, 0, false)) == 0);

    // Trailing page bytes after the header are not the parser's business.
    auto with_body = page_header(2, 6000, true);
    with_body.insert(with_body.end(), 64, 0xAB);
    CHECK(entries_of(with_body) == 6000);
}

TEST_CASE("dictionary header count follows long-form field ids and skips unknown fields",
          "[parquet][dictionary]") {
    // Field 1 written long-form (delta 0, explicit zigzag id), then an unknown
    // binary field 5 and an unknown list field 6 before the dictionary header.
    std::vector<std::uint8_t> bytes{0x05, 0x02, 0x04,             // 1: type = 2
                                    0x48, 0x03, 'a',  'b',  'c',  // 5: binary "abc"
                                    0x19, 0x25, 0x02, 0x04,       // 6: list<i32>{1, 2}
                                    0x1C,                         // 7: struct
                                    0x15, 0x32,                   //   1: num_values = 25
                                    0x00, 0x00};
    CHECK(entries_of(bytes) == 25);
}

TEST_CASE("dictionary header count declines anything but a whole dictionary page header",
          "[parquet][dictionary]") {
    CHECK_FALSE(entries_of(page_header(0, 25, false)).has_value());  // DATA_PAGE
    CHECK_FALSE(entries_of(page_header(2, -1, false)).has_value());  // negative count

    const auto whole = page_header(2, 410665, true);
    for (std::size_t cut = 0; cut < whole.size(); ++cut) {
        const std::vector<std::uint8_t> truncated(whole.begin(),
                                                  whole.begin() + static_cast<std::ptrdiff_t>(cut));
        CHECK_FALSE(entries_of(truncated).has_value());
    }

    // A header with no dictionary_page_header at all.
    const std::vector<std::uint8_t> bare{0x15, 0x04, 0x00};
    CHECK_FALSE(entries_of(bare).has_value());
}
