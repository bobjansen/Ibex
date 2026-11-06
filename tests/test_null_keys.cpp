// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Does this operator's key or comparison consult the validity bitmap?
//
// A null cell holds its type's zero, so any code that compares VALUES alone
// treats a null as a genuine zero: group-by merges the two, a join matches
// them, a sortedness check calls an unsorted column sorted. The bugs are
// invisible to any test whose data avoids the zero value, which is why every
// case here pits a null against a GENUINE zero (or empty string) in the same
// column.
//
// `tests/data/null_keys_check.ibex` covers the same question end-to-end for
// group-by, distinct, order, the preserving joins, dcast and asof. This file
// covers the operators that sweep did not reach.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "../src/runtime/interpreter_internal.hpp"

using namespace ibex;

namespace {

auto run(std::string_view src, const runtime::TableRegistry& tables) -> runtime::Table {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE(result.has_value());
    return std::move(*result);
}

auto ints(const runtime::Table& t, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

// Which rows of `name` are null, as a bool per row.
auto nulls(const runtime::Table& t, const std::string& name) -> std::vector<bool> {
    const auto* entry = t.find_entry(name);
    REQUIRE(entry != nullptr);
    std::vector<bool> out(t.rows(), false);
    if (entry->validity.has_value()) {
        for (std::size_t i = 0; i < t.rows(); ++i) {
            out[i] = !(*entry->validity)[i];
        }
    }
    return out;
}

/// Key column `k` = 0, null, 0, 5, null — two nulls against two genuine zeros.
auto null_vs_zero() -> runtime::TableRegistry {
    runtime::Table t;
    runtime::ValidityBitmap kv(5, true);
    kv.set(1, false);
    kv.set(4, false);
    t.add_column("k", Column<std::int64_t>{0, 0, 0, 5, 0}, kv);
    t.add_column("v", Column<std::int64_t>{1, 10, 2, 100, 20});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    return tables;
}

}  // namespace

TEST_CASE("null keys: head N by keeps the null group separate", "[nulls][keys]") {
    auto tables = null_vs_zero();
    // Three groups: the genuine zeros, the nulls, and 5.
    CHECK(ints(run("t[head 1, by { k }];", tables), "v") == std::vector<std::int64_t>{1, 10, 100});
    CHECK(run("t[head 2, by { k }];", tables).rows() == 5);
}

TEST_CASE("null keys: tail N by keeps the null group separate", "[nulls][keys]") {
    auto tables = null_vs_zero();
    // The last row of each of the three groups, the groups in first-encounter
    // order. Merging null into the zero group loses one group entirely, which
    // is how this was found.
    CHECK(ints(run("t[tail 1, by { k }];", tables), "v") == std::vector<std::int64_t>{2, 20, 100});
    CHECK(run("t[tail 2, by { k }];", tables).rows() == 5);
}

TEST_CASE("null keys: order puts nulls last even when the values look sorted", "[nulls][keys]") {
    // The null is row 0 and holds the zero its type carries, so the VALUE
    // sequence 0, 1, 2 is ascending and a check that reads values alone calls
    // the column sorted — leaving the null exactly where it must not be.
    runtime::Table t;
    runtime::ValidityBitmap kv(3, true);
    kv.set(0, false);
    t.add_column("k", Column<std::int64_t>{0, 1, 2}, kv);
    t.add_column("v", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[order { k }];", tables);
    CHECK(ints(out, "v") == std::vector<std::int64_t>{20, 30, 10});
    CHECK(nulls(out, "k") == std::vector<bool>{false, false, true});
}

TEST_CASE("null keys: order is unaffected when the key has no nulls", "[nulls][keys]") {
    // The guard above declines a shortcut for nullable keys; this pins that a
    // column without nulls still takes it and still sorts.
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{3, 1, 2});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    CHECK(ints(run("t[order { k }];", tables), "k") == std::vector<std::int64_t>{1, 2, 3});
}

TEST_CASE("null keys: rank over a nullable key ranks by value", "[nulls][keys]") {
    // Values 7, null, 3, 7. Ranking the non-null rows: 3 takes rank 1 and the
    // two 7s tie over ranks 2 and 3, averaging 2.5. The null keeps a null rank.
    //
    // Ordering nulls by row index instead made the comparator intransitive —
    // 7 < null, null < 3, 3 < 7 — and a sort handed a comparator like that
    // returns an arbitrary permutation, so every rank came out in row order.
    runtime::Table t;
    runtime::ValidityBitmap kv(4, true);
    kv.set(1, false);
    t.add_column("x", Column<std::int64_t>{7, 0, 3, 7}, kv);
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[update { r = rank(x) }];", tables);
    const auto* col = out.find("r");
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<double>>(col);
    REQUIRE(values != nullptr);
    CHECK((*values)[0] == 2.5);
    CHECK((*values)[2] == 1.0);
    CHECK((*values)[3] == 2.5);
    CHECK(nulls(out, "r") == std::vector<bool>{false, true, false, false});
}

TEST_CASE("null keys: rank na_option top and bottom place the null", "[nulls][keys]") {
    runtime::Table t;
    runtime::ValidityBitmap kv(4, true);
    kv.set(1, false);
    t.add_column("x", Column<std::int64_t>{7, 0, 3, 7}, kv);
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto top = run("t[update { r = rank(x, na_option = top) }];", tables);
    const auto* top_values = std::get_if<Column<double>>(top.find("r"));
    REQUIRE(top_values != nullptr);
    CHECK((*top_values)[1] == 1.0);  // the null sorts first and takes rank 1

    auto bottom = run("t[update { r = rank(x, na_option = bottom) }];", tables);
    const auto* bottom_values = std::get_if<Column<double>>(bottom.find("r"));
    REQUIRE(bottom_values != nullptr);
    CHECK((*bottom_values)[1] == 4.0);  // and last, taking rank 4
}

TEST_CASE("null keys: dcast keeps distinct Float64 row keys apart", "[nulls][keys]") {
    // The row key was encoded into an int64 by truncation, which is not
    // injective: 1.5 and 1.9 both became 1, so the two rows merged and only
    // the later one's cells survived. Nothing about this needed a null to go
    // wrong — it is the same "compare an encoding, not the value" mistake.
    runtime::Table t;
    t.add_column("rk", Column<double>{1.5, 1.5, 1.9, 1.9});
    t.add_column("pk", Column<std::string>{"a", "b", "a", "b"});
    t.add_column("val", Column<std::int64_t>{10, 20, 30, 40});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[dcast pk, select { val }, by { rk }];", tables);
    REQUIRE(out.rows() == 2);
    CHECK(ints(out, "a") == std::vector<std::int64_t>{10, 30});
    CHECK(ints(out, "b") == std::vector<std::int64_t>{20, 40});
}

TEST_CASE("null keys: dcast separates a null row key from every real value", "[nulls][keys]") {
    // The null used to be encoded as INT64_MIN, a value a real key can hold —
    // so a row keyed INT64_MIN merged with the nulls. Null-ness now travels in
    // the key's mask, where no value can reach it.
    runtime::Table t;
    runtime::ValidityBitmap rv(3, true);
    rv.set(1, false);
    t.add_column("rk", Column<std::int64_t>{std::numeric_limits<std::int64_t>::min(), 0, 5}, rv);
    t.add_column("pk", Column<std::string>{"a", "a", "a"});
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[dcast pk, select { val }, by { rk }];", tables);
    CHECK(out.rows() == 3);
    CHECK(ints(out, "a") == std::vector<std::int64_t>{1, 2, 3});
}

TEST_CASE("null keys: a TimeFrame index may not contain nulls", "[nulls][keys]") {
    // A null has no position in time, so it cannot be an index. The refusal is
    // at as_timeframe, where the TimeFrame is established, rather than at each
    // operator that would later ask where the row sits.
    runtime::Table t;
    runtime::ValidityBitmap tv(3, true);
    tv.set(1, false);
    t.add_column("ts", Column<std::int64_t>{1, 0, 3}, tv);
    t.add_column("v", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto parsed = parser::parse("as_timeframe(t, \"ts\");");
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().find("null at row 1") != std::string::npos);
}

TEST_CASE("null keys: a fully valid TimeFrame index is still accepted", "[nulls][keys]") {
    runtime::Table t;
    t.add_column("ts", Column<std::int64_t>{3, 1, 2});
    t.add_column("v", Column<std::int64_t>{30, 10, 20});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    // Sorted by the index, which is what as_timeframe establishes.
    CHECK(ints(run("as_timeframe(t, \"ts\");", tables), "v") ==
          std::vector<std::int64_t>{10, 20, 30});
}

TEST_CASE("null keys: an empty string key does not take the run-length sentinel", "[nulls][keys]") {
    // The group-by run-length cache compared `key == prev_key` with `prev_key`
    // default-built — and a default string_view IS the empty string. So a first
    // row holding a GENUINE "" matched the empty cache and took the cache's
    // sentinel code, which then indexed the Cartesian cell array out of bounds:
    // a wild write, and a segfault in `resample ... by` on real data.
    //
    // Two keys, because the out-of-range code only reaches the cell array on
    // the multi-key path.
    runtime::Table t;
    runtime::ValidityBitmap sv(4, true);
    sv.set(1, false);
    sv.set(3, false);
    t.add_column("g", Column<std::int64_t>{1, 1, 1, 1});
    t.add_column("s", Column<std::string>{"", "", "", ""}, sv);
    t.add_column("v", Column<std::int64_t>{1, 10, 2, 20});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    // The genuine empty strings are one group and the nulls another.
    auto out = run("t[select { n = count(), total = sum(v) }, by { g, s }];", tables);
    REQUIRE(out.rows() == 2);
    CHECK(ints(out, "n") == std::vector<std::int64_t>{2, 2});
    CHECK(ints(out, "total") == std::vector<std::int64_t>{3, 30});
}

namespace {

/// Seven rows; rows 1 and 4 are null. `rank` is each non-null row's position
/// in ascending order, and every typed key below encodes it monotonically --
/// including a genuine zero (rank 2). The null slots hold leftover values that
/// would order row 4 before row 1 if a sort read them; nulls must keep input
/// order instead.
constexpr std::array<int, 7> kRank{3, -1, 1, 5, -1, 2, 4};
constexpr std::array<int, 7> kNullPayload{0, 6, 0, 0, 0, 0, 0};

auto null_ordering_table() -> runtime::TableRegistry {
    runtime::ValidityBitmap valid(kRank.size(), true);
    std::vector<std::int64_t> i64;
    std::vector<double> f64;
    std::vector<std::string> str;
    Column<Categorical>::codes_storage codes;
    std::vector<Date> dates;
    std::vector<Timestamp> stamps;
    std::vector<std::int64_t> row;
    for (std::size_t r = 0; r < kRank.size(); ++r) {
        const bool is_null = kRank[r] < 0;
        const int k = is_null ? kNullPayload[r] : kRank[r];
        if (is_null) {
            valid.set(r, false);
        }
        i64.push_back(k - 2);
        f64.push_back((k * 1.5) - 3.0);
        str.emplace_back(1, static_cast<char>('a' + k));
        // Dictionary deliberately not in value order: e, a, d, b, c, f, g.
        constexpr std::array<int, 7> kCodeOf{1, 3, 4, 2, 0, 5, 6};
        codes.push_back(kCodeOf[static_cast<std::size_t>(k)]);
        dates.push_back(Date{.days = k - 2});
        stamps.push_back(Timestamp{.nanos = k - 2});
        row.push_back(static_cast<std::int64_t>(r));
    }
    runtime::Table t;
    t.add_column("i", Column<std::int64_t>{i64}, valid);
    t.add_column("f", Column<double>{f64}, valid);
    t.add_column("s", Column<std::string>{str}, valid);
    t.add_column("c",
                 Column<Categorical>{std::vector<std::string>{"e", "a", "d", "b", "c", "f", "g"},
                                     std::move(codes)},
                 valid);
    t.add_column("dt", Column<Date>{dates}, valid);
    t.add_column("ts", Column<Timestamp>{stamps}, valid);
    // Two groups for the multi-key case: rows 0-3 and 4-6.
    t.add_column("g", Column<std::int64_t>{0, 0, 0, 0, 1, 1, 1});
    t.add_column("row", Column<std::int64_t>{row});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    return tables;
}

}  // namespace

TEST_CASE("null keys: order puts nulls last for every key type and direction", "[nulls][keys]") {
    // Non-null rows by rank, ascending: 2, 5, 0, 6, 3; descending the reverse.
    // The nulls (rows 1 and 4) follow in input order either way.
    const std::vector<std::int64_t> ascending{2, 5, 0, 6, 3, 1, 4};
    const std::vector<std::int64_t> descending{3, 6, 0, 5, 2, 1, 4};
    const auto tables = null_ordering_table();
    for (const std::string key : {"i", "f", "s", "c", "dt", "ts"}) {
        INFO("key " << key);
        CHECK(ints(run("t[order { " + key + " }];", tables), "row") == ascending);
        CHECK(ints(run("t[order { " + key + " desc }];", tables), "row") == descending);
    }
}

TEST_CASE("null keys: a nullable second key puts nulls last within each group", "[nulls][keys]") {
    // Group 0 holds rows 0-3 (ranks 3, null, 1, 5); group 1 rows 4-6
    // (null, 2, 4).
    const auto tables = null_ordering_table();
    for (const std::string key : {"i", "f", "s", "c", "dt", "ts"}) {
        INFO("key " << key);
        CHECK(ints(run("t[order { g, " + key + " }];", tables), "row") ==
              std::vector<std::int64_t>{2, 0, 3, 1, 5, 6, 4});
        CHECK(ints(run("t[order { g desc, " + key + " desc }];", tables), "row") ==
              std::vector<std::int64_t>{6, 5, 4, 3, 0, 2, 1});
    }
}

namespace {

/// Float64 key `k` with three NaNs of different payloads and signs, -0.0 next
/// to 0.0, two 1.0s and a null. Values are powers of two so every group's sum
/// names exactly which rows it holds.
///
///   row:  0     1    2      3     4    5      6     7
///   k:    NaN   1.0  -NaN   -0.0  0.0  NaN'   null  1.0
///   v:    1     2    4      8     16   32     64    128
auto nan_keys() -> runtime::TableRegistry {
    const double nan_a = std::numeric_limits<double>::quiet_NaN();
    const double nan_b = -std::numeric_limits<double>::quiet_NaN();
    // A different payload, not just a different sign.
    const auto nan_c = std::bit_cast<double>(std::uint64_t{0x7ff8000000000123ULL});
    runtime::Table t;
    runtime::ValidityBitmap kv(8, true);
    kv.set(6, false);
    t.add_column("k", Column<double>{nan_a, 1.0, nan_b, -0.0, 0.0, nan_c, 0.0, 1.0}, kv);
    t.add_column("g", Column<std::int64_t>{7, 7, 7, 7, 7, 7, 7, 7});
    t.add_column("v", Column<std::int64_t>{1, 2, 4, 8, 16, 32, 64, 128});
    t.add_column("pk", Column<std::string>{"a", "a", "b", "a", "b", "c", "a", "b"});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    return tables;
}

auto sorted(std::vector<std::int64_t> values) -> std::vector<std::int64_t> {
    std::ranges::sort(values);
    return values;
}

}  // namespace

// NaN keys form ONE group whatever their payload or sign, as in Polars, DuckDB
// and PostgreSQL; -0.0 and 0.0 form one group; null stays its own group.
// Groups: NaN rows {0,2,5} = 37, 1.0 rows {1,7} = 130, zero rows {3,4} = 24,
// null row {6} = 64.
TEST_CASE("NaN keys: by groups every NaN together", "[nulls][keys][nan]") {
    const auto tables = nan_keys();
    const std::vector<std::int64_t> sums{24, 37, 64, 130};
    CHECK(sorted(ints(run("t[select { s = sum(v) }, by { k }];", tables), "s")) == sums);
    CHECK(sorted(ints(run("t[select { s = sum(v) }, by { k, g }];", tables), "s")) == sums);
    CHECK(sorted(ints(run("t[select { s = sum(v) }, by { g, k }];", tables), "s")) == sums);
    CHECK(ints(run("t[update { s = sum(v) }, by { k }];", tables), "s") ==
          std::vector<std::int64_t>{37, 130, 37, 24, 24, 37, 64, 130});
    CHECK(ints(run("t[update { s = sum(v) }, by { k, g }];", tables), "s") ==
          std::vector<std::int64_t>{37, 130, 37, 24, 24, 37, 64, 130});
    // The first row of each group.
    CHECK(sorted(ints(run("t[head 1, by { k }];", tables), "v")) ==
          std::vector<std::int64_t>{1, 2, 8, 64});
}

TEST_CASE("NaN keys: distinct and count_distinct see one NaN", "[nulls][keys][nan]") {
    const auto tables = nan_keys();
    CHECK(run("t[distinct { k }];", tables).rows() == 4);
    CHECK(run("t[distinct { k, g }];", tables).rows() == 4);
    // Non-null distinct values: NaN, 1.0 and zero.
    CHECK(ints(run("t[select { n = count_distinct(k) }];", tables), "n") ==
          std::vector<std::int64_t>{3});
    CHECK(sorted(ints(run("t[select { n = count_distinct(k) }, by { g }];", tables), "n")) ==
          std::vector<std::int64_t>{3});
}

TEST_CASE("NaN keys: dcast puts every NaN row key in one row", "[nulls][keys][nan]") {
    const auto tables = nan_keys();
    // Row keys NaN, 1.0, zero and null: four rows.
    CHECK(run("t[dcast pk, select { v }, by { k }];", tables).rows() == 4);
}

namespace {

/// The same rows as `nan_keys` without the null row, so key paths that only
/// take null-free columns (the typed single-key maps and distinct sets) run.
/// Groups: NaN rows {0,2,5} = 37, 1.0 rows {1,6} = 66, zero rows {3,4} = 24.
auto nan_keys_no_nulls() -> runtime::TableRegistry {
    const auto nan_c = std::bit_cast<double>(std::uint64_t{0x7ff8000000000123ULL});
    runtime::Table t;
    t.add_column("k",
                 Column<double>{std::numeric_limits<double>::quiet_NaN(), 1.0,
                                -std::numeric_limits<double>::quiet_NaN(), -0.0, 0.0, nan_c, 1.0});
    t.add_column("g", Column<std::int64_t>{7, 7, 7, 7, 7, 7, 7});
    t.add_column("v", Column<std::int64_t>{1, 2, 4, 8, 16, 32, 64});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    return tables;
}

/// Many distinct keys, so group tables grow and rehash, with a NaN of a
/// different payload every 100 rows: 2,000 real keys plus one NaN group.
auto many_keys_with_nans() -> runtime::TableRegistry {
    constexpr std::size_t kRows = 4000;
    std::vector<double> k(kRows);
    std::vector<std::int64_t> v(kRows, 1);
    for (std::size_t i = 0; i < kRows; ++i) {
        k[i] = (i % 100 == 0) ? std::bit_cast<double>(std::uint64_t{0x7ff8000000000000ULL} | i)
                              : static_cast<double>(i % 2000) + 0.5;
    }
    runtime::Table t;
    t.add_column("k", Column<double>{std::move(k)});
    t.add_column("v", Column<std::int64_t>{std::move(v)});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    return tables;
}

}  // namespace

TEST_CASE("NaN keys: null-free typed key paths group every NaN together", "[nulls][keys][nan]") {
    const auto tables = nan_keys_no_nulls();
    CHECK(sorted(ints(run("t[select { s = sum(v) }, by { k }];", tables), "s")) ==
          std::vector<std::int64_t>{24, 37, 66});
    CHECK(ints(run("t[update { s = sum(v) }, by { k }];", tables), "s") ==
          std::vector<std::int64_t>{37, 66, 37, 24, 24, 37, 66});
    CHECK(run("t[distinct { k }];", tables).rows() == 3);
    CHECK(ints(run("t[select { n = count_distinct(k) }];", tables), "n") ==
          std::vector<std::int64_t>{3});
    CHECK(ints(run("t[select { n = count_distinct(k) }, by { g }];", tables), "n") ==
          std::vector<std::int64_t>{3});
}

TEST_CASE("NaN keys: one NaN group survives a growing group table", "[nulls][keys][nan]") {
    const auto tables = many_keys_with_nans();
    // Rows with i % 100 == 0 are NaN (40 of them); the other 3,960 rows hold
    // 1,980 distinct values (the ones whose i % 2000 is a multiple of 100 only
    // ever appear as NaN rows).
    CHECK(run("t[select { n = count() }, by { k }];", tables).rows() == 1981);
    CHECK(run("t[distinct { k }];", tables).rows() == 1981);
    CHECK(ints(run("t[select { n = count_distinct(k) }];", tables), "n") ==
          std::vector<std::int64_t>{1981});
}

TEST_CASE("NaN keys: the materialized aggregate groups every NaN together", "[nulls][keys][nan]") {
    // `aggregate_table` is what the physical fallback and the decimal path run;
    // a script never reaches it, so it is called directly. Both fixtures: the
    // null-free one takes the typed single-key map, the null one the encoded
    // key path.
    const std::vector<ir::AggSpec> aggs{
        ir::AggSpec{.func = ir::AggFunc::Sum, .column = {.name = "v"}, .alias = "s"},
        ir::AggSpec{.func = ir::AggFunc::CountDistinct, .column = {.name = "k"}, .alias = "n"}};
    // `sum` alone for the keyed cases: `count_distinct` routes the whole
    // aggregate down its own path, past the typed key maps under test.
    const std::vector<ir::AggSpec> sum_only{
        ir::AggSpec{.func = ir::AggFunc::Sum, .column = {.name = "v"}, .alias = "s"}};
    const std::vector<ir::ColumnRef> by_k{{.name = "k"}};
    const std::vector<ir::ColumnRef> by_g{{.name = "g"}};
    const std::vector<ir::ColumnRef> by_k_g{{.name = "k"}, {.name = "g"}};
    {
        const auto tables = nan_keys_no_nulls();
        const auto& t = tables.at("t");
        auto out = runtime::aggregate_table(t, by_k, sum_only);
        REQUIRE(out.has_value());
        CHECK(sorted(ints(*out, "s")) == std::vector<std::int64_t>{24, 37, 66});
        auto two = runtime::aggregate_table(t, by_k_g, sum_only);
        REQUIRE(two.has_value());
        CHECK(sorted(ints(*two, "s")) == std::vector<std::int64_t>{24, 37, 66});
        auto distinct = runtime::aggregate_table(t, by_g, aggs);
        REQUIRE(distinct.has_value());
        CHECK(ints(*distinct, "n") == std::vector<std::int64_t>{3});
    }
    {
        const auto tables = nan_keys();
        const auto& t = tables.at("t");
        auto out = runtime::aggregate_table(t, by_k, sum_only);
        REQUIRE(out.has_value());
        CHECK(sorted(ints(*out, "s")) == std::vector<std::int64_t>{24, 37, 64, 130});
        auto two = runtime::aggregate_table(t, by_k_g, sum_only);
        REQUIRE(two.has_value());
        CHECK(sorted(ints(*two, "s")) == std::vector<std::int64_t>{24, 37, 64, 130});
        auto distinct = runtime::aggregate_table(t, by_g, aggs);
        REQUIRE(distinct.has_value());
        CHECK(ints(*distinct, "n") == std::vector<std::int64_t>{3});
    }
}

TEST_CASE("NaN keys: a NaN join key still matches nothing", "[nulls][keys][nan]") {
    // Grouping puts every NaN in one group; joins keep IEEE `==` (SPEC 5.6).
    // They share the key hash, which now sends every NaN to one bucket -- that
    // must change where a NaN probes, never what it matches.
    const double nan = std::numeric_limits<double>::quiet_NaN();
    runtime::Table l;
    l.add_column("k", Column<double>{nan, 1.0, -0.0});
    l.add_column("lv", Column<std::int64_t>{1, 2, 3});
    runtime::Table r;
    r.add_column("k", Column<double>{nan, 1.0, 0.0});
    r.add_column("rv", Column<std::int64_t>{10, 20, 30});
    runtime::TableRegistry tables;
    tables.emplace("l", std::move(l));
    tables.emplace("r", std::move(r));
    // 1.0 and the zeros match; the NaNs do not.
    CHECK(sorted(ints(run("l join r on k;", tables), "lv")) == std::vector<std::int64_t>{2, 3});
    CHECK(sorted(ints(run("l join r on { k, k };", tables), "lv")) ==
          std::vector<std::int64_t>{2, 3});
    CHECK(ints(run("l semi join r on k;", tables), "lv") == std::vector<std::int64_t>{2, 3});
    CHECK(ints(run("l anti join r on k;", tables), "lv") == std::vector<std::int64_t>{1});
}

TEST_CASE("NaN keys: an as-of join's NaN equality key matches nothing", "[nulls][keys][nan]") {
    // The as-of join matches equality keys with IEEE `==`: a NaN right row is
    // never a candidate, and a NaN left row stays unmatched, even against a
    // NaN of the same bits. One equality key takes a typed path; a second key
    // takes the `Key` map.
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (const bool second_key : {false, true}) {
        INFO("second key " << second_key);
        runtime::Table lhs;
        lhs.add_column("ts", Column<Timestamp>{Timestamp{10}, Timestamp{20}, Timestamp{30}});
        lhs.add_column("k", Column<double>{nan, 1.0, nan});
        runtime::Table rhs;
        rhs.add_column("ts", Column<Timestamp>{Timestamp{5}, Timestamp{6}, Timestamp{7}});
        rhs.add_column("k", Column<double>{nan, 1.0, nan});
        if (second_key) {
            lhs.add_column("g", Column<std::int64_t>{4, 4, 4});
            rhs.add_column("g", Column<std::int64_t>{4, 4, 4});
        }
        lhs.add_column("lv", Column<std::int64_t>{1, 2, 3});
        rhs.add_column("rv", Column<std::int64_t>{10, 20, 30});
        lhs.set_properties(runtime::TableProperties::time_frame("ts"));
        rhs.set_properties(runtime::TableProperties::time_frame("ts"));
        runtime::TableRegistry tables;
        tables.emplace("lhs", std::move(lhs));
        tables.emplace("rhs", std::move(rhs));
        const auto out = run(
            second_key ? "lhs asof join rhs on { ts, k, g };" : "lhs asof join rhs on { ts, k };",
            tables);
        REQUIRE(out.rows() == 3);
        CHECK(nulls(out, "rv") == std::vector<bool>{true, false, true});
    }
}

// Float64 ORDER: every NaN, whatever its sign or payload, ties with every
// other NaN and sorts above +inf, so it lands last under `asc` and first under
// `desc`; -0.0 ties with 0.0; null stays last under both (Polars, DuckDB and
// PostgreSQL all put NaN above every number). Rows of `nan_keys_no_nulls`:
//   k: NaN 1.0 -NaN -0.0 0.0 NaN' 1.0   v: 1 2 4 8 16 32 64
TEST_CASE("NaN order: every sort path puts NaN above every number", "[order][nan]") {
    const auto plain = nan_keys_no_nulls();
    // Single ascending Float64 key: the double radix fast path.
    CHECK(ints(run("t[order { k asc }];", plain), "v") ==
          std::vector<std::int64_t>{8, 16, 2, 64, 1, 4, 32});
    // Single descending numeric key: the multi-key radix path.
    CHECK(ints(run("t[order { k desc }];", plain), "v") ==
          std::vector<std::int64_t>{1, 4, 32, 2, 64, 8, 16});
    CHECK(ints(run("t[order { g asc, k desc }];", plain), "v") ==
          std::vector<std::int64_t>{1, 4, 32, 2, 64, 8, 16});

    // A null-bearing key takes the comparison sort. Null row v = 64.
    const auto with_null = nan_keys();
    CHECK(ints(run("t[order { k asc }];", with_null), "v") ==
          std::vector<std::int64_t>{8, 16, 2, 128, 1, 4, 32, 64});
    CHECK(ints(run("t[order { k desc }];", with_null), "v") ==
          std::vector<std::int64_t>{1, 4, 32, 2, 128, 8, 16, 64});
}

TEST_CASE("NaN order: top-k keeps NaN above every number", "[order][topk][nan]") {
    const auto plain = nan_keys_no_nulls();
    CHECK(ints(run("t[order { k desc }, head 4];", plain), "v") ==
          std::vector<std::int64_t>{1, 4, 32, 2});
    CHECK(ints(run("t[order { k asc }, head 3];", plain), "v") ==
          std::vector<std::int64_t>{8, 16, 2});
    const auto with_null = nan_keys();
    CHECK(ints(run("t[order { k desc }, head 3];", with_null), "v") ==
          std::vector<std::int64_t>{1, 4, 32});
}

TEST_CASE("NaN order: rank ties every NaN and ranks it last ascending", "[order][rank][nan]") {
    const auto plain = nan_keys_no_nulls();
    // Zeros rank 1, the 1.0s 2, every NaN 3.
    CHECK(ints(run("t[update { r = rank(k, method = dense) }];", plain), "r") ==
          std::vector<std::int64_t>{3, 2, 3, 1, 1, 3, 2});
}

TEST_CASE("NaN order: a NaN in the middle is not already sorted", "[order][nan]") {
    // `<` against a NaN is false both ways, so a scan for a decreasing step
    // with `<` passes [1, NaN, 0].
    runtime::Table t;
    t.add_column("k", Column<double>{1.0, std::numeric_limits<double>::quiet_NaN(), 0.0});
    t.add_column("v", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));
    CHECK(ints(run("t[order { k asc }];", tables), "v") == std::vector<std::int64_t>{3, 1, 2});
}

TEST_CASE("NaN order: the shared Float64 order helpers", "[order][nan]") {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    const auto payload = std::bit_cast<double>(std::uint64_t{0x7ff8000000000123ULL});
    CHECK(runtime::order_compare_double(nan, -nan) == 0);
    CHECK(runtime::order_compare_double(nan, payload) == 0);
    CHECK(runtime::order_compare_double(nan, inf) == 1);
    CHECK(runtime::order_compare_double(-inf, -nan) == -1);
    CHECK(runtime::order_compare_double(-0.0, 0.0) == 0);
    CHECK(runtime::order_less_double(inf, nan));
    CHECK_FALSE(runtime::order_less_double(nan, inf));
    CHECK_FALSE(runtime::order_less_double(nan, nan));
    CHECK(runtime::double_to_sortable_u64(-nan) == runtime::double_to_sortable_u64(payload));
    CHECK(runtime::double_to_sortable_u64(-0.0) == runtime::double_to_sortable_u64(0.0));
    CHECK(runtime::double_to_sortable_u64(inf) < runtime::double_to_sortable_u64(-nan));
    CHECK(runtime::double_to_sortable_u64(-inf) < runtime::double_to_sortable_u64(-1.0));
}

TEST_CASE("top-k sorts a null key last, as order does", "[order][topk][nulls]") {
    // `order` puts nulls last under asc and desc; a top-k over the same key
    // must keep exactly the rows `order` then `head` would. Null row v = 64.
    const auto with_null = nan_keys();
    CHECK(ints(run("t[order { k asc }, head 7];", with_null), "v") ==
          std::vector<std::int64_t>{8, 16, 2, 128, 1, 4, 32});
    CHECK(ints(run("t[order { k asc }, head 8];", with_null), "v") ==
          std::vector<std::int64_t>{8, 16, 2, 128, 1, 4, 32, 64});
    CHECK(ints(run("t[order { k desc }, head 7];", with_null), "v") ==
          std::vector<std::int64_t>{1, 4, 32, 2, 128, 8, 16});
}
