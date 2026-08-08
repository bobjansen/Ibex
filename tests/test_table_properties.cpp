// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Rules for `derive_table_properties`, the one place an operator's output
// metadata is derived from its input. Every operator is meant to route through
// it, so the rules are pinned here rather than re-verified through whichever
// operators happen to exercise them.

#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <runtime_internal.hpp>
#include <string>
#include <vector>

namespace {

using ibex::runtime::KeyFate;
using ibex::runtime::RowTransform;
using ibex::runtime::TableProperties;

/// Every column survives under its own name.
auto keep_all() {
    return [](const std::string& name) -> KeyFate { return KeyFate::kept(name); };
}

/// Every column survives except `dropped`, which is gone from the output.
auto drop(std::string dropped) {
    return [dropped = std::move(dropped)](const std::string& name) -> KeyFate {
        return name == dropped ? KeyFate::dropped() : KeyFate::kept(name);
    };
}

/// `overwritten` is still a column, but an update rewrote its values.
auto overwrite(std::string overwritten) {
    return [overwritten = std::move(overwritten)](const std::string& name) -> KeyFate {
        return name == overwritten ? KeyFate::overwritten() : KeyFate::kept(name);
    };
}

auto ordering_of(std::initializer_list<std::string> names) -> std::vector<ibex::ir::OrderKey> {
    std::vector<ibex::ir::OrderKey> keys;
    keys.reserve(names.size());
    for (const auto& name : names) {
        keys.push_back({.name = name, .ascending = true});
    }
    return keys;
}

/// A TimeFrame indexed by `ts`, laid out group-major by `symbol` -- what
/// `resample 1d, by symbol` leaves behind.
auto sample() -> TableProperties {
    return TableProperties::time_frame("ts").with_grouping({"symbol"});
}

}  // namespace

TEST_CASE("Preserve and Subset carry every property through", "[runtime][properties]") {
    for (const auto transform : {RowTransform::Preserve, RowTransform::Subset}) {
        const auto out = TableProperties::derive(sample(), keep_all(), transform);
        REQUIRE(out.ordering().has_value());
        CHECK(out.ordering()->size() == 1);
        CHECK(out.ordering()->front().name == "ts");
        CHECK(out.time_index() == std::optional<std::string>{"ts"});
        CHECK(out.grouped_by() == std::vector<std::string>{"symbol"});
    }
}

TEST_CASE("Losing the time index voids the ordering with it", "[runtime][properties]") {
    const auto out = TableProperties::derive(sample(), drop("ts"), RowTransform::Preserve);
    CHECK_FALSE(out.time_index().has_value());
    CHECK_FALSE(out.ordering().has_value());
    // The grouping is independent of the time index and survives on its own.
    CHECK(out.grouped_by() == std::vector<std::string>{"symbol"});
}

TEST_CASE("Ordering is all-or-nothing across its keys", "[runtime][properties]") {
    const auto input = TableProperties::sorted_by(ordering_of({"a", "b"}));

    CHECK(
        TableProperties::derive(input, keep_all(), RowTransform::Preserve).ordering().has_value());
    CHECK_FALSE(
        TableProperties::derive(input, drop("b"), RowTransform::Preserve).ordering().has_value());
}

TEST_CASE("A dropped grouping key voids the whole grouping claim", "[runtime][properties]") {
    const auto input = TableProperties::grouped({"symbol", "venue"});

    const auto out = TableProperties::derive(input, drop("venue"), RowTransform::Preserve);
    // Naming a stale column in the row-order diagnostic would be worse than
    // dropping the claim, so the claim goes.
    CHECK(out.grouped_by().empty());
}

TEST_CASE("A rename follows every key to its new name", "[runtime][properties]") {
    const auto rename = [](const std::string& name) -> KeyFate {
        return KeyFate::kept(name == "ts" ? "stamp" : name);
    };

    const auto out = TableProperties::derive(sample(), rename, RowTransform::Preserve);
    REQUIRE(out.ordering().has_value());
    CHECK(out.ordering()->front().name == "stamp");
    CHECK(out.time_index() == std::optional<std::string>{"stamp"});
}

TEST_CASE("Reorder voids the ordering but keeps the rest", "[runtime][properties]") {
    // Every column is still present, so `fate` alone would happily map the
    // ordering keys -- but the rows they described have moved. The sorting
    // operator states the new ordering itself.
    const auto out = TableProperties::derive(sample(), keep_all(), RowTransform::Reorder);
    CHECK_FALSE(out.ordering().has_value());
    CHECK(out.time_index() == std::optional<std::string>{"ts"});
}

TEST_CASE("Reorder keeps the grouping hazard flag armed", "[runtime][properties]") {
    // `grouped_by` is a hazard flag, not a capability: it records that adjacent
    // rows may cross a partition. Reshuffling the rows does not make an
    // order-dependent call over them safe, so clearing it here would disarm the
    // guard in check_row_order.
    const auto out = TableProperties::derive(sample(), keep_all(), RowTransform::Reorder);
    CHECK(out.grouped_by() == std::vector<std::string>{"symbol"});
}

TEST_CASE("Recombine clears everything", "[runtime][properties]") {
    const auto out = TableProperties::derive(sample(), keep_all(), RowTransform::Recombine);
    CHECK_FALSE(out.ordering().has_value());
    CHECK_FALSE(out.time_index().has_value());
    CHECK(out.grouped_by().empty());
}

TEST_CASE("The named constructors state what an operator established", "[runtime][properties]") {
    // Each is the answer to "how did this operator come by its metadata?", which
    // is the question assembling the fields one at a time lets an author skip.
    SECTION("none claims nothing") {
        const auto props = TableProperties::none();
        CHECK_FALSE(props.ordering().has_value());
        CHECK_FALSE(props.time_index().has_value());
        CHECK(props.grouped_by().empty());
    }

    SECTION("sorted_by records the keys the operator sorted on") {
        const auto props = TableProperties::sorted_by(ordering_of({"symbol", "ts"}));
        REQUIRE(props.ordering().has_value());
        CHECK(props.ordering()->size() == 2);
        CHECK_FALSE(props.time_index().has_value());
    }

    SECTION("time_frame implies the time-ascending ordering") {
        // A TimeFrame is time-sorted by construction, so establishing the index
        // establishes the ordering with it -- callers cannot set one and forget
        // the other.
        const auto props = TableProperties::time_frame("ts");
        REQUIRE(props.ordering().has_value());
        REQUIRE(props.ordering()->size() == 1);
        CHECK(props.ordering()->front().name == "ts");
        CHECK(props.ordering()->front().ascending);
        CHECK(props.time_index() == std::optional<std::string>{"ts"});
    }

    SECTION("grouped records only the grouping") {
        const auto props = TableProperties::grouped({"symbol"});
        CHECK(props.grouped_by() == std::vector<std::string>{"symbol"});
        CHECK_FALSE(props.ordering().has_value());
    }
}

// ── normalize_time_index ─────────────────────────────────────────────────────
//
// "Has a time index" implies "is time-ascending" only while the rows sit in one
// time-ordered run. A `by` upstream breaks that: `window`/`update` + `by` lay
// the rows out group-major, `resample` + `by` interleaves the groups. Rewriting
// either to "time index ascending" asserts something false, and that assertion
// escapes the process -- it is exported as Arrow `ibex.ordering()` metadata
// (arrow_c_data.cpp) and compared as part of table equality (table_compare.cpp).

namespace {

using ibex::runtime::normalize_time_index;
using ibex::runtime::Table;

}  // namespace

TEST_CASE("A TimeFrame with no stated ordering gets the time-ascending default",
          "[runtime][properties][normalize]") {
    Table table;
    table.add_column("ts", ibex::Column<ibex::Timestamp>{ibex::Timestamp{1}, ibex::Timestamp{2}});
    table.add_column("symbol", ibex::Column<std::int64_t>{1, 2});

    table.set_properties(TableProperties::recovered(std::nullopt, "ts", {}));

    REQUIRE(table.ordering().has_value());
    REQUIRE(table.ordering()->size() == 1);
    CHECK(table.ordering()->front().name == "ts");
    CHECK(table.ordering()->front().ascending);
}

TEST_CASE("A stated ordering is never overwritten", "[runtime][properties][normalize]") {
    // The invariant fills a gap; it does not correct an operator. Three real
    // orderings over a TimeFrame are not "time index ascending", and rewriting
    // any of them states something false about where the rows actually are.
    Table table;
    table.add_column("ts", ibex::Column<ibex::Timestamp>{ibex::Timestamp{1}, ibex::Timestamp{2}});
    table.add_column("symbol", ibex::Column<std::int64_t>{1, 2});

    SECTION("group-major: (group keys..., time)") {
        table.set_properties(
            TableProperties::recovered(ordering_of({"symbol", "ts"}), "ts", {"symbol"}));
        REQUIRE(table.ordering()->size() == 2);
        CHECK(table.ordering()->front().name == "symbol");
    }

    SECTION("`order { symbol asc }` over a TimeFrame: (symbol, ts)") {
        // Sorting a TimeFrame by another key is permitted precisely because the
        // time index is appended as a tiebreaker, so the result really is
        // (symbol, ts) -- and a downstream window relies on the metadata saying
        // so.
        table.set_properties(TableProperties::recovered(ordering_of({"symbol", "ts"}), "ts", {}));
        REQUIRE(table.ordering()->size() == 2);
        CHECK(table.ordering()->front().name == "symbol");
    }

    SECTION("an ordering that does not mention the time index at all") {
        table.set_properties(TableProperties::recovered(ordering_of({"symbol"}), "ts", {}));
        REQUIRE(table.ordering()->size() == 1);
        CHECK(table.ordering()->front().name == "symbol");
    }
}

TEST_CASE("A table with no time index gets no ordering invented",
          "[runtime][properties][normalize]") {
    Table table;
    table.add_column("symbol", ibex::Column<std::int64_t>{1, 2});
    table.set_properties(TableProperties::none());
    CHECK_FALSE(table.ordering().has_value());
}

// ── Dropped vs Overwritten ───────────────────────────────────────────────────
//
// The two are identical for an ordering key and opposite for a grouping key,
// which is the whole reason `KeyFate` has three cases instead of an optional.

TEST_CASE("An overwritten key voids an ordering, exactly as a dropped one does",
          "[runtime][properties][fate]") {
    const auto input = TableProperties::sorted_by(ordering_of({"a", "b"}));

    CHECK_FALSE(
        TableProperties::derive(input, drop("b"), RowTransform::Preserve).ordering().has_value());
    // Rewritten values describe the row order no better than a missing column.
    CHECK_FALSE(TableProperties::derive(input, overwrite("b"), RowTransform::Preserve)
                    .ordering()
                    .has_value());
}

TEST_CASE("An overwritten grouping key keeps the hazard flag armed",
          "[runtime][properties][fate]") {
    const auto input = TableProperties::grouped({"symbol", "venue"});

    SECTION("dropped voids the claim -- it cannot be named") {
        CHECK(TableProperties::derive(input, drop("venue"), RowTransform::Preserve)
                  .grouped_by()
                  .empty());
    }

    SECTION("overwritten keeps it -- the rows have not moved") {
        // `update { venue = ... } by { symbol, venue }` rewrites the values but
        // leaves every row where it was, so the run boundaries an unpartitioned
        // lag would read across are still there. Voiding the claim here would
        // disarm check_row_order over a live hazard.
        const auto out = TableProperties::derive(input, overwrite("venue"), RowTransform::Preserve);
        CHECK(out.grouped_by() == std::vector<std::string>{"symbol", "venue"});
    }
}

TEST_CASE("An overwritten time index is no longer an index", "[runtime][properties][fate]") {
    // The column is still present, but its timestamps were rewritten, so it
    // indexes nothing -- and losing the time index takes the ordering with it.
    const auto out = TableProperties::derive(sample(), overwrite("ts"), RowTransform::Preserve);
    CHECK_FALSE(out.time_index().has_value());
    CHECK_FALSE(out.ordering().has_value());
    // The grouping is independent and survives the overwrite of a different key.
    CHECK(out.grouped_by() == std::vector<std::string>{"symbol"});
}

// `satisfies` decides whether a sort can be skipped outright, so its two
// directions are not symmetric in cost: a false negative pays for a sort that
// was not needed, a false positive returns unsorted rows as sorted.
TEST_CASE("An ordering satisfies each of its own prefixes", "[runtime][properties][ordering]") {
    const auto props = TableProperties::sorted_by(
        {{.name = "a", .ascending = true}, {.name = "b", .ascending = false}});

    CHECK(props.satisfies({}));
    CHECK(props.satisfies({{.name = "a", .ascending = true}}));
    CHECK(props.satisfies({{.name = "a", .ascending = true}, {.name = "b", .ascending = false}}));
}

TEST_CASE("An ordering does not satisfy an extension of itself",
          "[runtime][properties][ordering]") {
    // Rows ordered by (a) may be in any order within a run of equal `a`, so
    // they say nothing about (a, b).
    const auto props = TableProperties::sorted_by({{.name = "a", .ascending = true}});
    CHECK_FALSE(
        props.satisfies({{.name = "a", .ascending = true}, {.name = "b", .ascending = true}}));
}

TEST_CASE("A direction or a name must match key for key", "[runtime][properties][ordering]") {
    const auto props = TableProperties::sorted_by(
        {{.name = "a", .ascending = true}, {.name = "b", .ascending = true}});
    CHECK_FALSE(props.satisfies({{.name = "a", .ascending = false}}));
    CHECK_FALSE(props.satisfies({{.name = "b", .ascending = true}}));
    // Right names, wrong order of them.
    CHECK_FALSE(
        props.satisfies({{.name = "b", .ascending = true}, {.name = "a", .ascending = true}}));
}

TEST_CASE("No claim satisfies nothing", "[runtime][properties][ordering]") {
    CHECK_FALSE(TableProperties::none().satisfies({{.name = "a", .ascending = true}}));
    // Not even the empty request: with no claim, the rows may be in any order,
    // and an `order` with no keys sorts by the whole schema rather than by none.
    CHECK_FALSE(TableProperties::none().satisfies({}));
}
