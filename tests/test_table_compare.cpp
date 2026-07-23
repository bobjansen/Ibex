#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

TEST_CASE("structured table comparator accepts identical nullable categorical tables", "[parity]") {
    runtime::Table expected;
    Column<Categorical> cat{{"a", "b"}, {0, 1, 0}};
    expected.add_column("kind", cat, runtime::ValidityBitmap{true, false, true});
    expected.ordering = std::vector<ir::OrderKey>{{.name = "kind", .ascending = true}};
    expected.time_index = "kind";
    auto actual = expected;
    CHECK_FALSE(runtime::compare_tables(expected, actual).has_value());
}

TEST_CASE("structured table comparator reports validity and categorical backing", "[parity]") {
    runtime::Table expected;
    expected.add_column("kind", Column<Categorical>{{"a", "b"}, {0, 1}},
                        runtime::ValidityBitmap{true, false});
    auto validity_changed = expected;
    validity_changed.columns[0].validity = runtime::ValidityBitmap{true, true};
    REQUIRE(runtime::compare_tables(expected, validity_changed).has_value());
    CHECK(runtime::compare_tables(expected, validity_changed)->location ==
          "column 0 row 1 validity");

    runtime::Table code_changed;
    code_changed.add_column("kind", Column<Categorical>{{"b", "a"}, {0, 1}},
                            runtime::ValidityBitmap{true, false});
    REQUIRE(runtime::compare_tables(expected, code_changed).has_value());
    CHECK(runtime::compare_tables(expected, code_changed)->location ==
          "column 0 categorical dictionary");
}

TEST_CASE("structured table comparator reports metadata and zero-column row counts", "[parity]") {
    runtime::Table expected;
    expected.logical_rows = 3;
    runtime::Table actual;
    actual.logical_rows = 4;
    REQUIRE(runtime::compare_tables(expected, actual).has_value());
    CHECK(runtime::compare_tables(expected, actual)->location == "logical_rows");
}
