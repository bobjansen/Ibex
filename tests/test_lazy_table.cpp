#include <ibex/runtime/lazy_table.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <set>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

/// A stand-in for a columnar file: records which columns each decode asked for,
/// so a test can assert on what was read rather than only on what came back.
struct FakeSource {
    std::vector<std::vector<std::string>> decode_calls;

    auto schema() const -> runtime::Table {
        runtime::Table t;
        t.add_column("a", Column<std::int64_t>{});
        t.add_column("b", Column<double>{});
        t.add_column("c", Column<std::string>{});
        return t;
    }

    auto decode(const std::vector<std::string>& names)
        -> std::expected<runtime::Table, std::string> {
        decode_calls.push_back(names);
        runtime::Table out;
        for (const auto& name : names) {
            if (name == "a") {
                out.add_column("a", Column<std::int64_t>{std::vector<std::int64_t>{1, 2, 3}});
            } else if (name == "b") {
                out.add_column("b", Column<double>{std::vector<double>{1.5, 2.5, 3.5}});
            } else if (name == "c") {
                out.add_column("c", Column<std::string>{std::vector<std::string>{"x", "y", "z"}});
            }
        }
        return out;
    }
};

auto make_lazy(FakeSource& source) -> runtime::LazyTable {
    return runtime::LazyTable{source.schema(), 3, [&source](const std::vector<std::string>& names) {
                                  return source.decode(names);
                              }};
}

auto names_of(const runtime::Table& table) -> std::vector<std::string> {
    std::vector<std::string> out;
    out.reserve(table.columns.size());
    for (const auto& entry : table.columns) {
        out.push_back(entry.name);
    }
    return out;
}

}  // namespace

TEST_CASE("LazyTable: the schema is known without decoding anything", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    CHECK(names_of(lazy.schema()) == std::vector<std::string>{"a", "b", "c"});
    CHECK(lazy.rows() == 3);
    CHECK(source.decode_calls.empty());
}

TEST_CASE("LazyTable: project decodes only the columns asked for", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.project({"b"});
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"b"});
    CHECK(table->rows() == 3);
    REQUIRE(source.decode_calls.size() == 1);
    CHECK(source.decode_calls[0] == std::vector<std::string>{"b"});
}

TEST_CASE("LazyTable: a decoded column is not decoded twice", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    REQUIRE(lazy.project({"a", "b"}));
    REQUIRE(source.decode_calls.size() == 1);

    // Second query overlaps the first: only the genuinely new column is read.
    auto table = lazy.project({"b", "c"});
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"b", "c"});
    REQUIRE(source.decode_calls.size() == 2);
    CHECK(source.decode_calls[1] == std::vector<std::string>{"c"});

    // Fully cached: no decode at all.
    REQUIRE(lazy.project({"a", "b"}));
    CHECK(source.decode_calls.size() == 2);
}

TEST_CASE("LazyTable: projected columns come back in schema order", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // Decode `c` first, so cache insertion order differs from schema order.
    REQUIRE(lazy.project({"c"}));
    auto table = lazy.project({"a", "c"});
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"a", "c"});
}

TEST_CASE("LazyTable: names outside the schema are ignored", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // A join's demand is the union across both sides, so a source is routinely
    // asked for names it does not have.
    auto table = lazy.project({"a", "not_here"});
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"a"});
    CHECK(source.decode_calls[0] == std::vector<std::string>{"a"});
}

TEST_CASE("LazyTable: an empty projection still carries the row count", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // `count()` over an unfiltered scan needs the row count and no column.
    auto table = lazy.project({});
    REQUIRE(table);
    CHECK(table->columns.empty());
    CHECK(table->rows() == 3);
    CHECK(source.decode_calls.empty());
}

TEST_CASE("LazyTable: materialize decodes every column", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.materialize();
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"a", "b", "c"});
    CHECK(table->rows() == 3);
}

TEST_CASE("LazyTable: a decode failure surfaces as an error", "[runtime][lazy_table]") {
    runtime::Table schema;
    schema.add_column("a", Column<std::int64_t>{});
    runtime::LazyTable lazy{
        std::move(schema), 3,
        [](const std::vector<std::string>&) -> std::expected<runtime::Table, std::string> {
            return std::unexpected("boom");
        }};

    auto table = lazy.project({"a"});
    REQUIRE_FALSE(table);
    CHECK(table.error() == "boom");
}
