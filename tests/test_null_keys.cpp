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

#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

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
    CHECK(ints(run("t[head 1, by { k }];", tables), "v") ==
          std::vector<std::int64_t>{1, 10, 100});
    CHECK(run("t[head 2, by { k }];", tables).rows() == 5);
}

TEST_CASE("null keys: tail N by keeps the null group separate", "[nulls][keys]") {
    auto tables = null_vs_zero();
    // The last row of each of the three groups, the groups in first-encounter
    // order. Merging null into the zero group loses one group entirely, which
    // is how this was found.
    CHECK(ints(run("t[tail 1, by { k }];", tables), "v") ==
          std::vector<std::int64_t>{2, 20, 100});
    CHECK(run("t[tail 2, by { k }];", tables).rows() == 5);
}

TEST_CASE("null keys: order puts nulls last even when the values look sorted",
          "[nulls][keys]") {
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

TEST_CASE("null keys: dcast separates a null row key from every real value",
          "[nulls][keys]") {
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
