// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <vector>

namespace {
auto evaluate(const char* query, std::initializer_list<std::int32_t> values, bool nullable = false)
    -> ibex::runtime::Table {
    ibex::runtime::Table input;
    auto column = ibex::Column<std::int64_t>::from_compact_int32(values);
    if (nullable) {
        ibex::runtime::ValidityBitmap validity(values.size(), true);
        validity.set(1, false);
        input.add_column("v", std::move(column), std::move(validity));
    } else
        input.add_column("v", std::move(column));
    input.add_column("g", ibex::Column<std::int64_t>(std::vector<std::int64_t>(values.size(), 0)));
    input.add_column("w",
                     ibex::Column<std::int64_t>(std::vector<std::int64_t>(
                         values.size(), std::numeric_limits<std::int64_t>::max() - INT32_MAX)));
    ibex::runtime::TableRegistry registry;
    registry.emplace("t", std::move(input));
    auto parsed = ibex::parser::parse(query);
    REQUIRE(parsed.has_value());
    auto plan = ibex::parser::lower(*parsed);
    REQUIRE(plan.has_value());
    auto result = ibex::runtime::interpret(**plan, registry);
    REQUIRE(result.has_value());
    return std::move(*result);
}
auto integer(const ibex::runtime::Table& table, const char* name, std::size_t row = 0)
    -> std::int64_t {
    return std::get<ibex::Column<std::int64_t>>(*table.find_entry(name)->column)[row];
}
}  // namespace
TEST_CASE("compact Int32 arithmetic promotes before operating", "[compact-int32][boundary]") {
    auto negative =
        evaluate("t[select { n = -v, a = abs(v), d = v - 1, twice = v + v }];", {INT32_MIN});
    REQUIRE(integer(negative, "n") == INT64_C(2147483648));
    REQUIRE(integer(negative, "a") == INT64_C(2147483648));
    REQUIRE(integer(negative, "d") == INT64_C(-2147483649));
    REQUIRE(integer(negative, "twice") == INT64_C(-4294967296));
    auto positive = evaluate("t[select { x = v + w, p = v * 2147483648, q = v*v }];", {INT32_MAX});
    REQUIRE(integer(positive, "x") == std::numeric_limits<std::int64_t>::max());
    REQUIRE(integer(positive, "p") == INT64_C(4611686016279904256));
    REQUIRE(integer(positive, "q") == INT64_C(4611686014132420609));
}
TEST_CASE("compact Int32 nullable sums and computed sums stay wide", "[compact-int32][boundary]") {
    for (const auto* query : {"t[select { s = sum(v), p = sum(v*v), n = count(v) }];",
                              "t[select { s = sum(v), p = sum(v*v), n = count(v) }, by { g }];"}) {
        auto result = evaluate(query, {2000000000, INT32_MAX, -2000000000}, true);
        REQUIRE(integer(result, "s") == 0);
        REQUIRE(integer(result, "p") == INT64_C(8000000000000000000));
        REQUIRE(integer(result, "n") == 2);
    }
    auto result =
        evaluate("t[select { s = sum(v), lo = min(v), hi = max(v) }];", {INT32_MAX, INT32_MAX});
    REQUIRE(integer(result, "s") == INT64_C(4294967294));
    REQUIRE(integer(result, "lo") == INT32_MAX);
    REQUIRE(integer(result, "hi") == INT32_MAX);
}
TEST_CASE("compact Int32 computed updates retain values beyond 32 bits",
          "[compact-int32][boundary]") {
    auto result =
        evaluate("t[update { v = v + 3000000000 }][order { v }];", {2000000000, -2000000000});
    REQUIRE(integer(result, "v", 0) == INT64_C(1000000000));
    REQUIRE(integer(result, "v", 1) == INT64_C(5000000000));
}

TEST_CASE("integer storage visits physical spans without widening", "[compact-int32][ingestion]") {
    using Integer = ibex::Column<std::int64_t>;
    auto compact = Integer::from_compact_int32({INT32_MAX, INT32_MAX});
    const auto sum = [](auto values) -> std::int64_t {
        std::int64_t result = 0;
        for (auto value : values)
            result += static_cast<std::int64_t>(value);
        return result;
    };
    REQUIRE(compact.visit_integer_storage(sum) == INT64_C(4294967294));
    REQUIRE(compact.visit_integer_storage([](auto values) { return sizeof(values[0]); }) == 4);
    // A cached wide view would forbid fresh-output access. The visitor must
    // leave the backing untouched, even after calculating a wide result.
    REQUIRE_NOTHROW(compact.compact_int32_output());
    const Integer wide{INT64_MAX, -1};
    REQUIRE(wide.visit_integer_storage(sum) == INT64_MAX - 1);
    REQUIRE(wide.visit_integer_storage([](auto values) { return sizeof(values[0]); }) == 8);
}

TEST_CASE("adopted Int32 storage keeps its owner and detaches on mutation",
          "[compact-int32][ingestion]") {
    using Integer = ibex::Column<std::int64_t>;
    auto source = std::make_shared<std::vector<std::int32_t>>(
        std::initializer_list<std::int32_t>{99, INT32_MIN, INT32_MAX});
    std::weak_ptr<const void> lifetime = source;
    auto column = Integer::from_external_int32(source, source->data() + 1, 2);
    const auto* original = source->data() + 1;
    REQUIRE(column.compact_int32_data() == original);
    REQUIRE(column.capacity() == 2);
    REQUIRE_THROWS_AS(column.compact_int32_output(), std::logic_error);
    source.reset();
    REQUIRE_FALSE(lifetime.expired());
    auto copy = column;
    column.reserve(16);
    REQUIRE(column.is_compact_int32());
    REQUIRE(column.capacity() >= 16);
    REQUIRE(column.compact_int32_data() != original);
    REQUIRE(copy.compact_int32_data() == original);
    column.append_integer_values(copy);
    REQUIRE(column.size() == 4);
    REQUIRE(column.compact_int32_data()[3] == INT32_MAX);
    copy[0] = INT64_C(3000000000);
    REQUIRE_FALSE(copy.is_compact_int32());
    REQUIRE(lifetime.expired());
    REQUIRE(std::as_const(copy)[0] == INT64_C(3000000000));
    REQUIRE(column.compact_int32_data()[0] == INT32_MIN);
}

TEST_CASE("empty and invalid external Int32 storage are handled", "[compact-int32][ingestion]") {
    using Integer = ibex::Column<std::int64_t>;
    auto owner = std::make_shared<int>(0);
    REQUIRE_THROWS_AS(Integer::from_external_int32({}, nullptr, 0), std::invalid_argument);
    REQUIRE_THROWS_AS(Integer::from_external_int32(owner, nullptr, 1), std::invalid_argument);
    auto empty = Integer::from_external_int32(owner, nullptr, 0);
    REQUIRE(empty.empty());
    REQUIRE(empty.visit_integer_storage([](auto values) { return values.size(); }) == 0);
    REQUIRE(std::as_const(empty).data() == nullptr);
    empty.append_integer_values(Integer::from_compact_int32({42}));
    REQUIRE(empty.compact_int32_data()[0] == 42);
}
