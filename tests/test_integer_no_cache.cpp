// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

TEST_CASE("compact integer reads never attach a widened representation",
          "[compact-int32][no-cache]") {
    using Col = ibex::Column<std::int64_t>;
    auto source = Col::from_compact_int32({INT32_MIN, INT32_MAX, INT32_MAX});
    const auto& input = source;
    const auto* original = source.compact_int32_data();
    REQUIRE(input[0] == INT64_C(-2147483648));
    REQUIRE(input.at(1) == INT32_MAX);
    REQUIRE(input.front() == INT32_MIN);
    REQUIRE(input.back() == INT32_MAX);
    REQUIRE(std::accumulate(input.begin(), input.end(), INT64_C(0)) == INT64_C(2147483646));
    static_assert(std::random_access_iterator<ibex::IntegerView>);
    static_assert(!std::contiguous_iterator<ibex::IntegerView>);
    REQUIRE(std::distance(input.begin(), input.end()) == 3);
    REQUIRE(input.rbegin()[0] == INT32_MAX);
    REQUIRE_THROWS_AS(input.data(), std::logic_error);
    REQUIRE_THROWS_AS(input.span(), std::logic_error);
    REQUIRE_THROWS_AS(input.buffer_data(), std::logic_error);
    auto wide = input.widened_int64();
    REQUIRE_FALSE(wide.is_compact_int32());
    REQUIRE(std::as_const(wide).data()[0] == INT32_MIN);
    wide[0] = INT64_C(4000000000);
    REQUIRE(input[0] == INT32_MIN);
    REQUIRE(input.compact_int32_data() == original);
    REQUIRE_NOTHROW(source.compact_int32_output());
    auto filtered = input.filter([](std::int64_t value) { return value > 0; });
    REQUIRE(filtered.size() == 2);
    auto transformed = input.transform([](std::int64_t value) { return value * 2; });
    REQUIRE(std::as_const(transformed)[0] == INT64_C(-4294967296));
}

TEST_CASE("logical integer iterators preserve raw mutable positions",
          "[compact-int32][no-cache][ingestion]") {
    ibex::Column<std::int64_t> values{1, 2};
    const auto& input = values;
    REQUIRE(ibex::IntegerView(input.data() + 2) == input.end());
    REQUIRE(ibex::IntegerView(input.data() + 2) - input.begin() == 2);
    (void)values.insert(values.end(), 3);
    REQUIRE(input[0] == 1);
    REQUIRE(input[1] == 2);
    REQUIRE(input[2] == 3);
    (void)values.erase(values.begin() + 1);
    REQUIRE(input[0] == 1);
    REQUIRE(input[1] == 3);
}

namespace {
auto inputs(bool compact, bool nulls) -> ibex::runtime::TableRegistry {
    auto column = [compact](std::initializer_list<std::int32_t> values) {
        if (compact)
            return ibex::Column<std::int64_t>::from_compact_int32(values);
        ibex::Column<std::int64_t> out;
        for (auto value : values)
            out.push_back(value);
        return out;
    };
    ibex::runtime::Table t, r;
    t.add_column("g", column({1, 2, 1, 3, 2, 3}));
    t.add_column("h", column({0, 0, 1, 1, 0, 1}));
    auto values = column({-7, 3, 2, 0, 5, -1});
    if (nulls) {
        ibex::runtime::ValidityBitmap validity(6, true);
        validity.set(1, false);
        t.add_column("v", std::move(values), std::move(validity));
    } else
        t.add_column("v", std::move(values));
    r.add_column("g", column({1, 2, 4}));
    r.add_column("w", column({INT32_MIN, INT32_MAX, 0}));
    ibex::runtime::TableRegistry result;
    result.emplace("t", std::move(t));
    result.emplace("r", std::move(r));
    return result;
}
}  // namespace

TEST_CASE("integer membership joins retain signed boundaries in offset storage",
          "[compact-int32][no-cache][join]") {
    using Col = ibex::Column<std::int64_t>;
    for (const bool compact : {false, true}) {
        for (const bool nullable : {false, true}) {
            auto narrow = std::make_shared<std::vector<std::int32_t>>(
                std::initializer_list<std::int32_t>{9, 9, INT32_MIN, -1, 0, 1, INT32_MAX, -1});
            auto wide = std::make_shared<std::vector<std::int64_t>>(narrow->begin(), narrow->end());
            const Col input = compact ? Col::from_external_int32(narrow, narrow->data() + 2, 6)
                                      : Col::from_external(wide, wide->data(), 2, 6);
            ibex::runtime::Table left, right;
            if (nullable)
                left.add_column("k", input, std::vector<bool>{false, true, true, true, true, true});
            else
                left.add_column("k", input);
            left.add_column("id", Col{0, 1, 2, 3, 4, 5});
            right.add_column("k", compact ? Col::from_compact_int32({INT32_MIN, INT32_MAX})
                                          : Col{INT32_MIN, INT32_MAX});
            ibex::runtime::TableRegistry tables{{"l", std::move(left)}, {"r", std::move(right)}};
            for (const char* kind : {"semi", "anti"}) {
                CAPTURE(compact, nullable, kind);
                auto parsed = ibex::parser::parse(std::string("l ") + kind + " join r on k;");
                REQUIRE(parsed.has_value());
                auto plan = ibex::parser::lower(*parsed);
                REQUIRE(plan.has_value());
                auto result = ibex::runtime::interpret(**plan, tables);
                REQUIRE(result.has_value());
                const auto& ids = std::get<Col>(*result->find("id"));
                const std::vector<std::int64_t> expected =
                    std::string_view(kind) == "semi"
                        ? (nullable ? std::vector<std::int64_t>{4}
                                    : std::vector<std::int64_t>{0, 4})
                        : (nullable ? std::vector<std::int64_t>{0, 1, 2, 3, 5}
                                    : std::vector<std::int64_t>{1, 2, 3, 5});
                CHECK(std::vector<std::int64_t>(ids.begin(), ids.end()) == expected);
                if (nullable && std::string_view(kind) == "anti")
                    CHECK(ibex::runtime::is_null(*result->find_entry("k"), 0));
            }
            CHECK(input.is_compact_int32() == compact);
        }
    }
}

TEST_CASE("native integer operators agree without a cached wide input",
          "[compact-int32][no-cache]") {
    for (bool nulls : {false, true}) {
        auto wide = inputs(false, nulls);
        auto compact = inputs(true, nulls);
        for (const char* query : {"t[filter v>=0];",
                                  "t[filter v>g];",
                                  "t[select {x=Float64(v)}];",
                                  "t[filter v*2+g>=0];",
                                  "t[select {s=sum(v), lo=min(v), hi=max(v), n=count(v)}];",
                                  "t[select {s=sum(v), m=mean(v), d=std(v)}, by {g}];",
                                  "t[select {s=sum(v)}, by {g,h}];",
                                  "t[select {m=median(v), q=quantile(v,0.5)}, by {g}];",
                                  "t[order v];",
                                  "t[order v desc, head 3];",
                                  "t[distinct {g,h}];",
                                  "t[update {x=v+3000000000, y=v*v, z=abs(v)}];",
                                  "t[update {x=lag(v,1), y=lead(v,1), z=cumsum(v)}];",
                                  "t[update {x=coalesce(v,0)}];",
                                  "t[select {x=coalesce(v,0)*coalesce(v,0)}];",
                                  "rbind(t,t);",
                                  "t[where v>0 update {v=42}];",
                                  "t[where g==1 update {v=v+1}];",
                                  "t[where is_null(v) update {v=42}];",
                                  "t[update {x=lag(v,1)}, by {g}];",
                                  "t[update {x=fill_forward(v), y=fill_backward(v)}];",
                                  "t join r on g;",
                                  "t left join r on g;",
                                  "t outer join r on g;",
                                  "t semi join r on g;",
                                  "t anti join r on g;"}) {
            DYNAMIC_SECTION(query << " nulls=" << nulls) {
                CAPTURE(query, nulls);
                auto parsed = ibex::parser::parse(query);
                REQUIRE(parsed.has_value());
                auto plan = ibex::parser::lower(*parsed);
                REQUIRE(plan.has_value());
                auto expected = ibex::runtime::interpret(**plan, wide);
                REQUIRE(expected.has_value());
                auto actual = ibex::runtime::interpret(**plan, compact);
                REQUIRE(actual.has_value());
                auto mismatch = ibex::runtime::compare_tables(*expected, *actual);
                if (mismatch)
                    FAIL(mismatch->message());
                for (const auto& entry : compact.at("t").columns) {
                    const auto& value = std::get<ibex::Column<std::int64_t>>(*entry.column);
                    REQUIRE(value.is_compact_int32());
                }
            }
        }
    }
}

TEST_CASE("compact integer keys and aggregates agree at one million rows",
          "[compact-int32][no-cache][dense]") {
    constexpr std::size_t rows = 1000000;
    const auto registry = [](bool compact) {
        ibex::runtime::TableRegistry result;
        ibex::runtime::Table table, lookup;
        auto add = [compact](ibex::runtime::Table& target, const char* name, auto fn,
                             std::size_t size) {
            ibex::detail::NoInitVector<std::int32_t> values(size);
            for (std::size_t i = 0; i < size; ++i)
                values[i] = fn(i);
            if (compact) {
                target.add_column(
                    name, ibex::Column<std::int64_t>::from_compact_int32(std::move(values)));
            } else {
                ibex::Column<std::int64_t> wide;
                wide.reserve(size);
                for (auto value : values)
                    wide.push_back(value);
                target.add_column(name, std::move(wide));
            }
        };
        add(table, "g", [](std::size_t i) { return static_cast<std::int32_t>(i % 997); }, rows);
        add(
            table, "v", [](std::size_t i) { return static_cast<std::int32_t>(i % 101) - 50; },
            rows);
        add(lookup, "g", [](std::size_t i) { return static_cast<std::int32_t>(i); }, 997);
        add(lookup, "w", [](std::size_t i) { return static_cast<std::int32_t>(i * 2); }, 997);
        result.emplace("t", std::move(table));
        result.emplace("r", std::move(lookup));
        return result;
    };
    auto wide = registry(false), compact = registry(true);
    for (const char* query :
         {"t[select {s=sum(v)}, by {g}];", "t[select {s=sum(v), m=mean(v), d=std(v)}, by {g}];",
          "t[filter v>=0][select {s=sum(v)}, by {g}];", "t[update {x=v*v+g}][select {s=sum(x)}];",
          "(t join r on g)[select {s=sum(v), n=count()}];",
          "(t left join r on g)[select {n=count(w)}, by {g}];"}) {
        DYNAMIC_SECTION(query) {
            auto parsed = ibex::parser::parse(query);
            REQUIRE(parsed.has_value());
            auto plan = ibex::parser::lower(*parsed);
            REQUIRE(plan.has_value());
            auto expected = ibex::runtime::interpret(**plan, wide);
            REQUIRE(expected.has_value());
            auto actual = ibex::runtime::interpret(**plan, compact);
            REQUIRE(actual.has_value());
            if (auto mismatch = ibex::runtime::compare_tables(*expected, *actual))
                FAIL(mismatch->message());
        }
    }
}

TEST_CASE("left join keeps an unmatched compact payload compact",
          "[compact-int32][no-cache][join]") {
    // Rows 4 and 6 of t (g = 3) have no match in r, so the gather of r's `w`
    // takes the sentinel path; at six rows it runs serially.
    auto parsed = ibex::parser::parse("t left join r on g;");
    REQUIRE(parsed.has_value());
    auto plan = ibex::parser::lower(*parsed);
    REQUIRE(plan.has_value());
    auto wide = ibex::runtime::interpret(**plan, inputs(false, false));
    REQUIRE(wide.has_value());
    auto actual = ibex::runtime::interpret(**plan, inputs(true, false));
    REQUIRE(actual.has_value());
    auto mismatch = ibex::runtime::compare_tables(*wide, *actual);
    if (mismatch)
        FAIL(mismatch->message());
    const auto* entry = actual->find_entry("w");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    REQUIRE(std::get<ibex::Column<std::int64_t>>(*entry->column).is_compact_int32());
}
