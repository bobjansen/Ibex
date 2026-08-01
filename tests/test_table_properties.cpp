// Rules for `derive_table_properties`, the one place an operator's output
// metadata is derived from its input. Every operator is meant to route through
// it, so the rules are pinned here rather than re-verified through whichever
// operators happen to exercise them.

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <runtime_internal.hpp>
#include <string>
#include <vector>

namespace {

using ibex::runtime::derive_table_properties;
using ibex::runtime::RowTransform;
using ibex::runtime::TableProperties;

/// Every column survives under its own name.
auto keep_all() {
    return [](const std::string& name) -> std::optional<std::string> { return name; };
}

/// Every column survives except `dropped`.
auto drop(std::string dropped) {
    return [dropped = std::move(dropped)](const std::string& name) -> std::optional<std::string> {
        return name == dropped ? std::nullopt : std::optional<std::string>{name};
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

auto sample() -> TableProperties {
    return TableProperties{
        .ordering = ordering_of({"ts"}), .time_index = "ts", .grouped_by = {"symbol"}};
}

}  // namespace

TEST_CASE("Preserve and Subset carry every property through", "[runtime][properties]") {
    for (const auto transform : {RowTransform::Preserve, RowTransform::Subset}) {
        const auto out = derive_table_properties(sample(), keep_all(), transform);
        REQUIRE(out.ordering.has_value());
        CHECK(out.ordering->size() == 1);
        CHECK(out.ordering->front().name == "ts");
        CHECK(out.time_index == std::optional<std::string>{"ts"});
        CHECK(out.grouped_by == std::vector<std::string>{"symbol"});
    }
}

TEST_CASE("Losing the time index voids the ordering with it", "[runtime][properties]") {
    const auto out = derive_table_properties(sample(), drop("ts"), RowTransform::Preserve);
    CHECK_FALSE(out.time_index.has_value());
    CHECK_FALSE(out.ordering.has_value());
    // The grouping is independent of the time index and survives on its own.
    CHECK(out.grouped_by == std::vector<std::string>{"symbol"});
}

TEST_CASE("Ordering is all-or-nothing across its keys", "[runtime][properties]") {
    TableProperties input;
    input.ordering = ordering_of({"a", "b"});

    CHECK(derive_table_properties(input, keep_all(), RowTransform::Preserve).ordering.has_value());
    CHECK_FALSE(
        derive_table_properties(input, drop("b"), RowTransform::Preserve).ordering.has_value());
}

TEST_CASE("A dropped grouping key voids the whole grouping claim", "[runtime][properties]") {
    TableProperties input;
    input.grouped_by = {"symbol", "venue"};

    const auto out = derive_table_properties(input, drop("venue"), RowTransform::Preserve);
    // Naming a stale column in the row-order diagnostic would be worse than
    // dropping the claim, so the claim goes.
    CHECK(out.grouped_by.empty());
}

TEST_CASE("A rename follows every key to its new name", "[runtime][properties]") {
    const auto rename = [](const std::string& name) -> std::optional<std::string> {
        return name == "ts" ? std::optional<std::string>{"stamp"}
                            : std::optional<std::string>{name};
    };

    const auto out = derive_table_properties(sample(), rename, RowTransform::Preserve);
    REQUIRE(out.ordering.has_value());
    CHECK(out.ordering->front().name == "stamp");
    CHECK(out.time_index == std::optional<std::string>{"stamp"});
}

TEST_CASE("Reorder voids the ordering but keeps the rest", "[runtime][properties]") {
    // Every column is still present, so `fate` alone would happily map the
    // ordering keys -- but the rows they described have moved. The sorting
    // operator states the new ordering itself.
    const auto out = derive_table_properties(sample(), keep_all(), RowTransform::Reorder);
    CHECK_FALSE(out.ordering.has_value());
    CHECK(out.time_index == std::optional<std::string>{"ts"});
}

TEST_CASE("Reorder keeps the grouping hazard flag armed", "[runtime][properties]") {
    // `grouped_by` is a hazard flag, not a capability: it records that adjacent
    // rows may cross a partition. Reshuffling the rows does not make an
    // order-dependent call over them safe, so clearing it here would disarm the
    // guard in check_row_order.
    const auto out = derive_table_properties(sample(), keep_all(), RowTransform::Reorder);
    CHECK(out.grouped_by == std::vector<std::string>{"symbol"});
}

TEST_CASE("Recombine clears everything", "[runtime][properties]") {
    const auto out = derive_table_properties(sample(), keep_all(), RowTransform::Recombine);
    CHECK_FALSE(out.ordering.has_value());
    CHECK_FALSE(out.time_index.has_value());
    CHECK(out.grouped_by.empty());
}
