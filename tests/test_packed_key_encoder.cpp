// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <map>
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

auto shared_cat_entry(const std::shared_ptr<std::vector<std::string>>& dict,
                      std::vector<std::int32_t> codes) -> runtime::ColumnEntry {
    runtime::ColumnEntry entry;
    entry.name = "c";
    Column<Categorical> col{dict, std::make_shared<Column<Categorical>::index_map>()};
    for (auto code : codes) {
        col.push_code(code);
    }
    entry.column = std::make_shared<runtime::ColumnValue>(std::move(col));
    return entry;
}

auto str_entry(const std::vector<std::string>& values) -> runtime::ColumnEntry {
    runtime::ColumnEntry entry;
    entry.name = "s";
    Column<std::string> col;
    for (const auto& v : values) {
        col.push_back(v);
    }
    entry.column = std::make_shared<runtime::ColumnValue>(std::move(col));
    return entry;
}

/// Runs every task inline, in order: the partitioned passes without threads.
const auto inline_run = [](std::size_t tasks, const auto& body) {
    for (std::size_t t = 0; t < tasks; ++t) {
        body(t);
    }
};

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

TEST_CASE("data-sized packed key interns String keys, within and across chunks", "[packed_key]") {
    // Interned serially (one partition) and partitioned (16, over enough rows
    // to take the partitioned passes): equal strings must pack equal and
    // distinct ones distinct, in one chunk and in the next, which must find
    // the first chunk's strings again rather than issue new ids.
    for (const std::size_t parts : {std::size_t{1}, std::size_t{16}}) {
        INFO("parts " << parts);
        constexpr std::size_t kRows = PackedKeyEncoder::kParallelInternRows + 1'000;
        const auto label = [](std::size_t i) {
            const std::size_t k = (i * 7919) % 5'003;
            return k == 0 ? std::string{} : "value-" + std::to_string(k) + "-past-the-sso";
        };
        std::vector<std::string> first_values;
        std::vector<std::int64_t> zeros(kRows, 0);
        for (std::size_t i = 0; i < kRows; ++i) {
            first_values.push_back(label(i));
        }
        auto encoder = ranged_encoder();
        std::vector<runtime::ColumnEntry> first;
        first.push_back(int_entry(zeros));
        first.push_back(str_entry(first_values));
        first.push_back(int_entry(zeros));
        encoder.intern_strings(ptrs(first), parts, 4, inline_run);
        auto plan = encoder.build_packed_key(ptrs(first));
        REQUIRE(plan.has_value());
        std::map<std::string, std::uint64_t> key_of;
        std::set<std::uint64_t> keys;
        for (std::size_t row = 0; row < kRows; ++row) {
            const auto key = PackedKeyEncoder::pack_row<std::uint64_t>(plan->cols, row);
            const auto [it, inserted] = key_of.emplace(first_values[row], key);
            REQUIRE(it->second == key);
            if (inserted) {
                REQUIRE(keys.insert(key).second);
            }
        }
        CHECK(key_of.size() == 5'003);

        // Two strings seen before, in a new order, and one that was not.
        std::vector<runtime::ColumnEntry> later;
        later.push_back(int_entry({0, 0, 0}));
        later.push_back(str_entry({label(2), "never-seen", label(1)}));
        later.push_back(int_entry({0, 0, 0}));
        encoder.intern_strings(ptrs(later), parts, 4, inline_run);
        auto later_plan = encoder.build_packed_key(ptrs(later));
        REQUIRE(later_plan.has_value());
        CHECK(PackedKeyEncoder::pack_row<std::uint64_t>(later_plan->cols, 0) == key_of[label(2)]);
        CHECK(PackedKeyEncoder::pack_row<std::uint64_t>(later_plan->cols, 2) == key_of[label(1)]);
        CHECK_FALSE(keys.contains(PackedKeyEncoder::pack_row<std::uint64_t>(later_plan->cols, 1)));
    }
}

TEST_CASE("data-sized packed key misses on a String column nobody interned", "[packed_key]") {
    auto encoder = ranged_encoder();
    std::vector<runtime::ColumnEntry> cols;
    cols.push_back(int_entry({1, 2}));
    cols.push_back(str_entry({"a", "b"}));
    cols.push_back(int_entry({1, 2}));
    CHECK_FALSE(encoder.build_packed_key(ptrs(cols)).has_value());
    CHECK(encoder.layout_miss());

    // Interning a different chunk does not vouch for this one.
    std::vector<runtime::ColumnEntry> other;
    other.push_back(int_entry({1, 2}));
    other.push_back(str_entry({"a", "b"}));
    other.push_back(int_entry({1, 2}));
    encoder.intern_strings(ptrs(other), 1, 1, inline_run);
    CHECK_FALSE(encoder.build_packed_key(ptrs(cols)).has_value());
    CHECK(encoder.layout_miss());
}

TEST_CASE("data-sized packed key reads a shared Categorical dictionary's codes as ids",
          "[packed_key]") {
    // Chunks that share one dictionary skip interning; the dictionary may
    // grow in place between them, and a chunk with ANOTHER dictionary must
    // still map each string to the id it had before.
    auto dict = std::make_shared<std::vector<std::string>>(std::vector<std::string>{"a", "b"});
    auto encoder = ranged_encoder();
    const auto key_of = [&](std::vector<runtime::ColumnEntry>& cols, std::size_t row) {
        auto plan = encoder.build_packed_key(ptrs(cols));
        REQUIRE(plan.has_value());
        return PackedKeyEncoder::pack_row<std::uint64_t>(plan->cols, row);
    };
    std::vector<runtime::ColumnEntry> first;
    first.push_back(shared_cat_entry(dict, {0, 1}));
    first.push_back(int_entry({0, 0}));
    first.push_back(int_entry({0, 0}));
    const auto key_a = key_of(first, 0);
    const auto key_b = key_of(first, 1);
    CHECK(key_a != key_b);

    dict->push_back("c");
    std::vector<runtime::ColumnEntry> grown;
    grown.push_back(shared_cat_entry(dict, {2, 1}));
    grown.push_back(int_entry({0, 0}));
    grown.push_back(int_entry({0, 0}));
    const auto key_c = key_of(grown, 0);
    CHECK(key_of(grown, 1) == key_b);
    CHECK(key_c != key_a);
    CHECK(key_c != key_b);

    std::vector<runtime::ColumnEntry> other;
    other.push_back(cat_entry({"c", "b", "a", "d"}, {0, 1, 2, 3}));
    other.push_back(int_entry({0, 0, 0, 0}));
    other.push_back(int_entry({0, 0, 0, 0}));
    CHECK(key_of(other, 0) == key_c);
    CHECK(key_of(other, 1) == key_b);
    CHECK(key_of(other, 2) == key_a);
    const auto key_d = key_of(other, 3);
    CHECK(key_d != key_a);
    CHECK(key_d != key_b);
    CHECK(key_d != key_c);
}
