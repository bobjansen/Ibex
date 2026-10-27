// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// An adopted categorical dictionary stays in the producer's buffers until
// something needs its strings. A column that uses a few codes of a huge shared
// dictionary (a Polars Categorical shares one pool across every frame) must
// not pay for the whole dictionary just to be scanned, grouped or compared.

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

namespace {

constexpr std::size_t kDictionary = 200'000;
constexpr std::int32_t kFirstUsed = static_cast<std::int32_t>(kDictionary) - 3;

// Owns the buffers an adopted column points into.
struct Buffers {
    std::vector<std::uint32_t> offsets;
    std::string chars;
    std::vector<std::int32_t> codes;
};

// Keys draw on the last three entries of a kDictionary-entry dictionary.
auto adopted_keys(std::size_t rows) -> Column<Categorical> {
    auto buffers = std::make_shared<Buffers>();
    buffers->offsets.reserve(kDictionary + 1);
    buffers->offsets.push_back(0);
    for (std::size_t i = 0; i < kDictionary; ++i) {
        buffers->chars += "k" + std::to_string(i);
        buffers->offsets.push_back(static_cast<std::uint32_t>(buffers->chars.size()));
    }
    for (std::size_t i = 0; i < rows; ++i) {
        buffers->codes.push_back(kFirstUsed + static_cast<std::int32_t>(i % 3));
    }
    return Column<Categorical>::from_external(
        buffers, buffers->codes.data(), 0, rows, buffers->offsets.data(), buffers->chars.data(), 0,
        kDictionary);
}

struct Fixture {
    runtime::TableRegistry tables;
    const Column<Categorical>* keys = nullptr;
};

auto fixture(std::size_t rows) -> Fixture {
    Fixture out;
    runtime::Table table;
    table.add_column("g", adopted_keys(rows));
    Column<std::int64_t> values;
    for (std::size_t i = 0; i < rows; ++i) {
        values.push_back(static_cast<std::int64_t>(i));
    }
    table.add_column("v", std::move(values));
    out.tables.insert_or_assign("t", std::move(table));
    out.keys = std::get_if<Column<Categorical>>(out.tables.at("t").find("g"));
    REQUIRE(out.keys != nullptr);
    REQUIRE_FALSE(out.keys->dictionary_materialized());
    return out;
}

auto run(std::string_view src, const runtime::TableRegistry& tables) -> runtime::Table {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE(result.has_value());
    return std::move(*result);
}

}  // namespace

TEST_CASE("grouping an adopted categorical does not build its dictionary",
          "[runtime][categorical][dictionary]") {
    auto fx = fixture(30'000);
    const auto result = run("t[select { s = sum(v) }, by g];", fx.tables);
    CHECK(result.rows() == 3);
    CHECK_FALSE(fx.keys->dictionary_materialized());
}

TEST_CASE("distinct, count and filters leave an adopted dictionary alone",
          "[runtime][categorical][dictionary]") {
    auto fx = fixture(30'000);
    CHECK(run("t[select { n = count() }, by g];", fx.tables).rows() == 3);
    CHECK_FALSE(fx.keys->dictionary_materialized());
    CHECK(run("t[distinct g];", fx.tables).rows() == 3);
    CHECK_FALSE(fx.keys->dictionary_materialized());
    CHECK(run("t[filter v > 10];", fx.tables).rows() == 29'989);
    CHECK_FALSE(fx.keys->dictionary_materialized());
}

TEST_CASE("copies of an adopted dictionary fill it once, from any thread",
          "[runtime][categorical][dictionary]") {
    const auto column = adopted_keys(10);
    std::vector<Column<Categorical>> copies(8, column);
    std::vector<std::thread> threads;
    std::vector<std::size_t> sizes(copies.size());
    for (std::size_t i = 0; i < copies.size(); ++i) {
        threads.emplace_back([&, i] { sizes[i] = copies[i].dictionary().size(); });
    }
    for (auto& t : threads) {
        t.join();
    }
    for (const auto size : sizes) {
        CHECK(size == kDictionary);
    }
    CHECK(column.dictionary_materialized());
    CHECK(copies.front().dictionary()[kDictionary - 1] == "k" + std::to_string(kDictionary - 1));
}
