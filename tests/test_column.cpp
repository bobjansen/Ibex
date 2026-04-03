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

TEST_CASE("Column<bool> packs bits across word boundaries", "[core][column][bool]") {
    ibex::Column<bool> col;
    col.reserve(130);
    for (std::size_t i = 0; i < 130; ++i) {
        col.push_back((i % 3) == 0);
    }

    REQUIRE(col.size() == 130);
    REQUIRE(col.word_count() == 3);
    REQUIRE(col[0] == true);
    REQUIRE(col[1] == false);
    REQUIRE(col[63] == true);
    REQUIRE(col[64] == false);
    REQUIRE(col[129] == true);
}

TEST_CASE("Column<bool> supports mutation and resize", "[core][column][bool]") {
    ibex::Column<bool> col{true, false, true};

    col[1] = true;
    col.resize(6, true);
    col[4] = false;

    REQUIRE(col.size() == 6);
    REQUIRE(col[0] == true);
    REQUIRE(col[1] == true);
    REQUIRE(col[2] == true);
    REQUIRE(col[3] == true);
    REQUIRE(col[4] == false);
    REQUIRE(col[5] == true);

    col.assign(70, false);
    REQUIRE(col.size() == 70);
    REQUIRE(col.word_count() == 2);
    REQUIRE(col[0] == false);
    REQUIRE(col[69] == false);
}

// --- Column<std::string> flat-buffer specialization -------------------------

TEST_CASE("Column<string> default-constructs empty", "[core][column][string]") {
    ibex::Column<std::string> col;

    REQUIRE(col.empty());
    REQUIRE(col.size() == 0);
}

TEST_CASE("Column<string> push_back and access", "[core][column][string]") {
    ibex::Column<std::string> col;
    col.push_back("hello");
    col.push_back("world");
    col.push_back("!");

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "hello");
    REQUIRE(col[1] == "world");
    REQUIRE(col[2] == "!");
}

TEST_CASE("Column<string> from vector", "[core][column][string]") {
    std::vector<std::string> data = {"alpha", "beta", "gamma"};
    ibex::Column<std::string> col(data);

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "alpha");
    REQUIRE(col[1] == "beta");
    REQUIRE(col[2] == "gamma");
}

TEST_CASE("Column<string> initializer list", "[core][column][string]") {
    ibex::Column<std::string> col{"foo", "bar", "baz"};

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "foo");
    REQUIRE(col[1] == "bar");
    REQUIRE(col[2] == "baz");
}

TEST_CASE("Column<string> at() throws on out-of-bounds", "[core][column][string]") {
    ibex::Column<std::string> col{"one"};
    REQUIRE_THROWS_AS(col.at(5), std::out_of_range);
}

TEST_CASE("Column<string> range-for iteration", "[core][column][string]") {
    ibex::Column<std::string> col{"a", "bb", "ccc"};

    std::string joined;
    for (auto sv : col) {
        joined += sv;
    }
    REQUIRE(joined == "abbccc");
}

TEST_CASE("Column<string> handles empty strings", "[core][column][string]") {
    ibex::Column<std::string> col{"", "x", ""};

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "");
    REQUIRE(col[1] == "x");
    REQUIRE(col[2] == "");
}

TEST_CASE("Column<string> clear resets state", "[core][column][string]") {
    ibex::Column<std::string> col{"hello", "world"};
    col.clear();

    REQUIRE(col.empty());
    REQUIRE(col.size() == 0);
}

TEST_CASE("Column<string> resize fills with value", "[core][column][string]") {
    ibex::Column<std::string> col;
    col.resize(3, "x");

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "x");
    REQUIRE(col[1] == "x");
    REQUIRE(col[2] == "x");
}

// --- Column<Categorical> dictionary-encoded strings -------------------------

TEST_CASE("Categorical default-constructs empty", "[core][column][categorical]") {
    ibex::Column<ibex::Categorical> col;
    REQUIRE(col.empty());
    REQUIRE(col.size() == 0);
}

TEST_CASE("Categorical push_back and access", "[core][column][categorical]") {
    ibex::Column<ibex::Categorical> col;
    col.push_back("AAPL");
    col.push_back("GOOG");
    col.push_back("AAPL");

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "AAPL");
    REQUIRE(col[1] == "GOOG");
    REQUIRE(col[2] == "AAPL");
}

TEST_CASE("Categorical shares dictionary codes for repeated values",
          "[core][column][categorical]") {
    ibex::Column<ibex::Categorical> col;
    col.push_back("A");
    col.push_back("B");
    col.push_back("A");
    col.push_back("B");

    REQUIRE(col.code_at(0) == col.code_at(2));
    REQUIRE(col.code_at(1) == col.code_at(3));
    REQUIRE(col.code_at(0) != col.code_at(1));
}

TEST_CASE("Categorical from dictionary", "[core][column][categorical]") {
    std::vector<std::string> dict = {"X", "Y", "Z"};
    ibex::Column<ibex::Categorical> col(dict);

    col.push_code(0);
    col.push_code(2);
    col.push_code(1);

    REQUIRE(col.size() == 3);
    REQUIRE(col[0] == "X");
    REQUIRE(col[1] == "Z");
    REQUIRE(col[2] == "Y");
}

TEST_CASE("Categorical find_code returns existing code", "[core][column][categorical]") {
    ibex::Column<ibex::Categorical> col;
    col.push_back("hello");
    col.push_back("world");

    auto code = col.find_code("hello");
    REQUIRE(code.has_value());
    REQUIRE(*code == col.code_at(0));

    auto missing = col.find_code("unknown");
    REQUIRE_FALSE(missing.has_value());
}

TEST_CASE("Categorical dictionary stores unique values", "[core][column][categorical]") {
    ibex::Column<ibex::Categorical> col;
    col.push_back("A");
    col.push_back("B");
    col.push_back("A");
    col.push_back("C");
    col.push_back("B");

    REQUIRE(col.dictionary().size() == 3);
    REQUIRE(col.size() == 5);
}

// --- Column<int> additional operations --------------------------------------

TEST_CASE("Column<int> front and back", "[core][column]") {
    ibex::Column<int> col{10, 20, 30};

    REQUIRE(col.front() == 10);
    REQUIRE(col.back() == 30);
}

TEST_CASE("Column<int> reserve and capacity", "[core][column]") {
    ibex::Column<int> col;
    col.reserve(100);

    REQUIRE(col.capacity() >= 100);
    REQUIRE(col.size() == 0);
}

TEST_CASE("Column<int> resize", "[core][column]") {
    ibex::Column<int> col{1, 2, 3};
    col.resize(5);

    REQUIRE(col.size() == 5);
    REQUIRE(col[0] == 1);
    REQUIRE(col[4] == 0);  // value-initialized
}

TEST_CASE("Column<int> clear and pop_back", "[core][column]") {
    ibex::Column<int> col{1, 2, 3};

    col.pop_back();
    REQUIRE(col.size() == 2);
    REQUIRE(col.back() == 2);

    col.clear();
    REQUIRE(col.empty());
}

TEST_CASE("Column<int> data pointer", "[core][column]") {
    ibex::Column<int> col{10, 20, 30};

    const int* ptr = col.data();
    REQUIRE(ptr[0] == 10);
    REQUIRE(ptr[1] == 20);
    REQUIRE(ptr[2] == 30);
}

TEST_CASE("Column<int> reverse iteration", "[core][column]") {
    ibex::Column<int> col{1, 2, 3};

    std::vector<int> reversed;
    for (auto it = col.rbegin(); it != col.rend(); ++it) {
        reversed.push_back(*it);
    }
    REQUIRE(reversed == std::vector<int>{3, 2, 1});
}

TEST_CASE("Column<int> assign from initializer list", "[core][column]") {
    ibex::Column<int> col{1, 2, 3};
    col.assign({10, 20});

    REQUIRE(col.size() == 2);
    REQUIRE(col[0] == 10);
    REQUIRE(col[1] == 20);
}

TEST_CASE("Column<int> emplace_back", "[core][column]") {
    ibex::Column<int> col;
    col.emplace_back(42);

    REQUIRE(col.size() == 1);
    REQUIRE(col[0] == 42);
}
