#include <ibex/core/column.hpp>

#include <catch2/catch_test_macros.hpp>

TEST_CASE("Column<int> basic operations", "[core][column]") {
    ibex::Column<int> col{1, 2, 3, 4, 5};

    SECTION("size and element access") {
        REQUIRE(col.size() == 5);
        REQUIRE_FALSE(col.empty());
        REQUIRE(col.at(0) == 1);
        REQUIRE(col[4] == 5);
    }

    SECTION("push_back grows the column") {
        col.push_back(6);
        REQUIRE(col.size() == 6);
        REQUIRE(col.at(5) == 6);
    }

    SECTION("span provides zero-copy view") {
        auto view = col.span();
        REQUIRE(view.size() == 5);
        REQUIRE(view[2] == 3);
    }

    SECTION("at() throws on out-of-bounds") {
        REQUIRE_THROWS_AS(col.at(100), std::out_of_range);
    }
}

TEST_CASE("Column<int> filter", "[core][column]") {
    ibex::Column<int> col{1, 2, 3, 4, 5, 6};

    auto evens = col.filter([](int x) { return x % 2 == 0; });

    REQUIRE(evens.size() == 3);
    REQUIRE(evens[0] == 2);
    REQUIRE(evens[1] == 4);
    REQUIRE(evens[2] == 6);
}

TEST_CASE("Column<int> transform", "[core][column]") {
    ibex::Column<int> col{1, 2, 3};

    auto doubled = col.transform([](int x) { return x * 2; });

    REQUIRE(doubled.size() == 3);
    REQUIRE(doubled[0] == 2);
    REQUIRE(doubled[1] == 4);
    REQUIRE(doubled[2] == 6);
}

TEST_CASE("Column<double> works with floating-point", "[core][column]") {
    ibex::Column<double> col{1.5, 2.5, 3.5};

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == 1.5);
}

TEST_CASE("Column default-constructs empty", "[core][column]") {
    ibex::Column<int> col;

    REQUIRE(col.empty());
    REQUIRE(col.size() == 0);
}

TEST_CASE("Column range-for iteration", "[core][column]") {
    ibex::Column<int> col{10, 20, 30};

    int sum = 0;
    for (auto val : col) {
        sum += val;
    }
    REQUIRE(sum == 60);
}
