// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "../src/runtime/packed_key_encoder_internal.hpp"

using namespace ibex;
using runtime::PackedKeyEncoder;

namespace {

auto int_entry(std::vector<std::int64_t> values) -> runtime::ColumnEntry {
    runtime::ColumnEntry entry;
    entry.name = "k";
    Column<std::int64_t> col;
    for (auto v : values) {
        col.push_back(v);
    }
    entry.column = std::make_shared<runtime::ColumnValue>(std::move(col));
    return entry;
}

auto date_entry(std::vector<std::int32_t> days) -> runtime::ColumnEntry {
    runtime::ColumnEntry entry;
    entry.name = "d";
    Column<Date> col;
    for (auto v : days) {
        col.push_back(Date{v});
    }
    entry.column = std::make_shared<runtime::ColumnValue>(std::move(col));
    return entry;
}

auto cat_entry(std::vector<std::string> dict, std::vector<std::int32_t> codes)
    -> runtime::ColumnEntry {
    runtime::ColumnEntry entry;
    entry.name = "c";
    Column<Categorical> col{std::make_shared<std::vector<std::string>>(std::move(dict)),
                            std::make_shared<Column<Categorical>::index_map>()};
    for (auto code : codes) {
        col.push_code(code);
    }
    entry.column = std::make_shared<runtime::ColumnValue>(std::move(col));
    return entry;
}

auto ptrs(const std::vector<runtime::ColumnEntry>& entries)
    -> std::vector<const runtime::ColumnEntry*> {
    std::vector<const runtime::ColumnEntry*> out;
    out.reserve(entries.size());
    for (const auto& entry : entries) {
        out.push_back(&entry);
    }
    return out;
}

auto ranged_encoder() -> PackedKeyEncoder {
    PackedKeyEncoder encoder;
    encoder.size_cells_from_data = true;
    return encoder;
}

}  // namespace

TEST_CASE("data-sized packed key fits three Int64 and three Categorical keys in one word",
          "[packed_key]") {
    // The h2o q10 key: 288 bits at type widths, which misses Packed256.
    std::vector<runtime::ColumnEntry> cols;
    cols.push_back(cat_entry({"id001", "id002", "id003"}, {0, 1, 2}));
    cols.push_back(cat_entry({"id001", "id002"}, {0, 1, 1}));
    cols.push_back(cat_entry({"a", "b", "c", "d"}, {3, 2, 1}));
    cols.push_back(int_entry({1, 100, 50}));
    cols.push_back(int_entry({-7, 3, 90}));
    cols.push_back(int_entry({1, 100'000, 77}));

    PackedKeyEncoder fixed;
    CHECK_FALSE(fixed.build_packed_key(ptrs(cols)).has_value());

    auto encoder = ranged_encoder();
    auto plan = encoder.build_packed_key(ptrs(cols));
    REQUIRE(plan.has_value());
    CHECK(plan->width == sizeof(std::uint64_t));
    std::set<std::uint64_t> keys;
    for (std::size_t row = 0; row < 3; ++row) {
        keys.insert(PackedKeyEncoder::pack_row<std::uint64_t>(plan->cols, row));
    }
    CHECK(keys.size() == 3);
}

TEST_CASE("data-sized packed key keeps distinct tuples distinct across the window",
          "[packed_key]") {
    // Values at both ends of each column's window, negatives included, must
    // never share a packed word.
    std::vector<std::int64_t> a;
    std::vector<std::int32_t> d;
    std::vector<std::int64_t> c;
    for (std::int64_t i = -3; i <= 3; ++i) {
        for (std::int64_t j = -2; j <= 2; ++j) {
            for (std::int64_t k : {std::numeric_limits<std::int64_t>::min() / 2, std::int64_t{0}}) {
                a.push_back(i);
                d.push_back(static_cast<std::int32_t>(j * 1000));
                c.push_back(k);
            }
        }
    }
    std::vector<runtime::ColumnEntry> cols;
    cols.push_back(int_entry(a));
    cols.push_back(date_entry(d));
    cols.push_back(int_entry(c));
    auto encoder = ranged_encoder();
    auto plan = encoder.build_packed_key(ptrs(cols));
    REQUIRE(plan.has_value());
    REQUIRE(plan->width == sizeof(PackedKeyEncoder::Packed128));
    std::set<std::pair<std::uint64_t, std::uint64_t>> keys;
    for (std::size_t row = 0; row < a.size(); ++row) {
        const auto key = PackedKeyEncoder::pack_row<PackedKeyEncoder::Packed128>(plan->cols, row);
        keys.emplace(key.w[0], key.w[1]);
    }
    CHECK(keys.size() == a.size());
}

TEST_CASE("data-sized packed key reports a miss when a later chunk leaves the window",
          "[packed_key]") {
    auto encoder = ranged_encoder();
    std::vector<runtime::ColumnEntry> first;
    first.push_back(int_entry({0, 1}));
    first.push_back(int_entry({0, std::int64_t{1} << 40}));
    first.push_back(int_entry({0, std::int64_t{1} << 20}));
    REQUIRE(encoder.build_packed_key(ptrs(first)).has_value());

    SECTION("values inside the window still pack") {
        std::vector<runtime::ColumnEntry> later;
        later.push_back(int_entry({1}));
        later.push_back(int_entry({5}));
        later.push_back(int_entry({7}));
        CHECK(encoder.build_packed_key(ptrs(later)).has_value());
        CHECK_FALSE(encoder.layout_miss());
    }
    SECTION("a value past the window is a miss, not a packable key") {
        std::vector<runtime::ColumnEntry> later;
        later.push_back(int_entry({1}));
        later.push_back(int_entry({std::numeric_limits<std::int64_t>::max()}));
        later.push_back(int_entry({7}));
        CHECK_FALSE(encoder.build_packed_key(ptrs(later)).has_value());
        CHECK(encoder.layout_miss());
    }
    SECTION("a value below the window is a miss") {
        std::vector<runtime::ColumnEntry> later;
        later.push_back(int_entry({1}));
        later.push_back(int_entry({5}));
        later.push_back(int_entry({std::numeric_limits<std::int64_t>::min()}));
        CHECK_FALSE(encoder.build_packed_key(ptrs(later)).has_value());
        CHECK(encoder.layout_miss());
    }
}

TEST_CASE("data-sized packed key reports a miss when a Categorical outgrows its ids",
          "[packed_key]") {
    // 41 + 21 bits of Int64 span leave the Categorical one spare bit: ids 0..3.
    auto encoder = ranged_encoder();
    std::vector<runtime::ColumnEntry> first;
    first.push_back(cat_entry({"a", "b"}, {0, 1}));
    first.push_back(int_entry({0, std::int64_t{1} << 40}));
    first.push_back(int_entry({0, std::int64_t{1} << 20}));
    REQUIRE(encoder.build_packed_key(ptrs(first)).has_value());

    std::vector<runtime::ColumnEntry> later;
    later.push_back(cat_entry({"c", "d", "e", "f"}, {0, 3}));
    later.push_back(int_entry({0, 0}));
    later.push_back(int_entry({0, 0}));
    CHECK_FALSE(encoder.build_packed_key(ptrs(later)).has_value());
    CHECK(encoder.layout_miss());
}

TEST_CASE("data-sized packed key misses when even the data needs more than 256 bits",
          "[packed_key]") {
    constexpr auto lo = std::numeric_limits<std::int64_t>::min();
    constexpr auto hi = std::numeric_limits<std::int64_t>::max();
    std::vector<runtime::ColumnEntry> cols;
    for (int i = 0; i < 5; ++i) {
        cols.push_back(int_entry({lo, hi}));
    }
    auto encoder = ranged_encoder();
    CHECK_FALSE(encoder.build_packed_key(ptrs(cols)).has_value());
    CHECK(encoder.layout_miss());
}
