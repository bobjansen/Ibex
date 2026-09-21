// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "../src/runtime/aggregate_chunked_internal.hpp"
#include "../src/runtime/interpreter_internal.hpp"

using namespace ibex;

namespace {
class AggregateChunks final : public runtime::Operator {
   public:
    explicit AggregateChunks(std::vector<runtime::Chunk> chunks, bool require_release = false)
        : chunks_(std::move(chunks)), require_release_(require_release) {}
    auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
        if (require_release_ && !previous_.expired()) {
            return std::unexpected("aggregate retained an input chunk instead of reducing it");
        }
        if (pos_ == chunks_.size())
            return std::optional<runtime::Chunk>{};
        if (require_release_)
            previous_ = chunks_[pos_].columns[1].column;
        return std::optional<runtime::Chunk>{std::move(chunks_[pos_++])};
    }

   private:
    std::vector<runtime::Chunk> chunks_;
    std::size_t pos_ = 0;
    bool require_release_ = false;
    std::weak_ptr<runtime::ColumnValue> previous_;
};
auto aggregate_chunks(std::vector<runtime::Chunk> chunks, const std::vector<ir::ColumnRef>& keys,
                      const std::vector<ir::AggSpec>& aggs, bool require_release = false) {
    runtime::ExecutionContext exec;
    exec.parallel_threads = 1;
    return runtime::MaterializeOperator(
               runtime::make_chunked_aggregate_operator(
                   std::make_unique<AggregateChunks>(std::move(chunks), require_release), &keys,
                   &aggs, exec, {}, std::nullopt))
        .run();
}
void sorted(runtime::Chunk& c, std::vector<ir::OrderKey> keys = {{.name = "k"}}) {
    c.set_properties(runtime::TableProperties::sorted_by(std::move(keys)));
}
auto ints(const runtime::Table& t, const char* name) -> const Column<std::int64_t>& {
    return std::get<Column<std::int64_t>>(*t.find(name));
}
}  // namespace

TEST_CASE("duplicate ordering keys do not imply grouping contiguity", "[aggregate][audit]") {
    runtime::Chunk c;
    c.add_column("k", Column<std::int64_t>{1, 1, 1});
    c.add_column("v", Column<std::int64_t>{1, 2, 1});
    sorted(c, {{.name = "k"}, {.name = "k"}});
    auto out = aggregate_chunks({c}, {{.name = "k"}, {.name = "v"}},
                                {{.func = ir::AggFunc::Count, .column = {}, .alias = "n"}});
    REQUIRE(out.has_value());
    REQUIRE(out->rows() == 2);
    REQUIRE(ints(*out, "n")[0] == 2);
}

TEST_CASE("sorted aggregate preserves null keys introduced in later chunks", "[aggregate][audit]") {
    runtime::Chunk first;
    first.add_column("k", Column<std::int64_t>{0});
    first.add_column("v", Column<std::int64_t>{10});
    sorted(first);
    runtime::Chunk later;
    later.add_column("k", Column<std::int64_t>{0, 0});
    later.columns[0].validity = runtime::ValidityBitmap(2, false);
    later.add_column("v", Column<std::int64_t>{20, 30});
    sorted(later);
    std::size_t preceding_groups = 0;
    SECTION("null begins at chunk boundary") {}
    SECTION("null begins after an output batch") {
        Column<std::int64_t> keys;
        Column<std::int64_t> values;
        preceding_groups = 8192;
        for (std::int64_t k = -8192; k <= 0; ++k) {
            keys.push_back(k);
            values.push_back(10);
        }
        first.columns.clear();
        first.add_column("k", std::move(keys));
        first.add_column("v", std::move(values));
    }
    SECTION("null begins inside later chunk") {
        later.columns[0].validity->set(0, true);
    }
    runtime::Chunk tail;
    tail.add_column("k", Column<std::int64_t>{0});
    tail.columns[0].validity = runtime::ValidityBitmap(1, false);
    tail.add_column("v", Column<std::int64_t>{50});
    sorted(tail);
    const bool first_later_valid = (*later.columns[0].validity)[0];
    auto out =
        aggregate_chunks({first, later, tail}, {{.name = "k"}},
                         {{.func = ir::AggFunc::Sum, .column = {.name = "v"}, .alias = "s"}});
    REQUIRE(out.has_value());
    REQUIRE(out->rows() == preceding_groups + 2);
    REQUIRE_FALSE(runtime::is_null(*out->find_entry("k"), preceding_groups));
    REQUIRE(runtime::is_null(*out->find_entry("k"), preceding_groups + 1));
    REQUIRE(ints(*out, "s")[preceding_groups] == (first_later_valid ? 30 : 10));
    REQUIRE(ints(*out, "s")[preceding_groups + 1] == (first_later_valid ? 80 : 100));
}

TEST_CASE("sorted string extrema use the supported aggregate path", "[aggregate][audit]") {
    runtime::Chunk c;
    c.add_column("k", Column<std::int64_t>{1, 1, 2});
    c.add_column("v", Column<std::string>{"b", "a", "c"});
    sorted(c);
    auto out =
        aggregate_chunks({c}, {{.name = "k"}},
                         {{.func = ir::AggFunc::Min, .column = {.name = "v"}, .alias = "lo"},
                          {.func = ir::AggFunc::Max, .column = {.name = "v"}, .alias = "hi"}});
    REQUIRE(out.has_value());
    REQUIRE(std::get<Column<std::string>>(*out->find("lo"))[0] == "a");
    REQUIRE(std::get<Column<std::string>>(*out->find("hi"))[0] == "b");
}

TEST_CASE("boolean first and last work without another materializing aggregate",
          "[aggregate][audit]") {
    runtime::Chunk c;
    c.add_column("k", Column<std::int64_t>{1, 1, 2});
    c.add_column("v", Column<bool>{true, false, true});
    SECTION("hash") {}
    SECTION("sorted") {
        sorted(c);
    }
    auto out =
        aggregate_chunks({c}, {{.name = "k"}},
                         {{.func = ir::AggFunc::First, .column = {.name = "v"}, .alias = "f"},
                          {.func = ir::AggFunc::Last, .column = {.name = "v"}, .alias = "l"}});
    REQUIRE(out.has_value());
    REQUIRE(std::get<Column<bool>>(*out->find("f"))[0]);
    REQUIRE_FALSE(std::get<Column<bool>>(*out->find("l"))[0]);
}

TEST_CASE("count distinct treats signed zeros as equal", "[aggregate][audit]") {
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 1});
    t.add_column("v", Column<double>{0.0, -0.0, 1.0});
    std::vector<ir::ColumnRef> keys{{.name = "k"}};
    std::vector<ir::AggSpec> aggs{
        {.func = ir::AggFunc::CountDistinct, .column = {.name = "v"}, .alias = "n"}};
    SECTION("streaming") {
        runtime::Chunk c;
        c.columns = std::move(t.columns);
        auto out = aggregate_chunks({c}, keys, aggs);
        REQUIRE(out.has_value());
        REQUIRE(ints(*out, "n")[0] == 2);
    }
    SECTION("materialized") {
        auto out = runtime::aggregate_table(t, keys, aggs);
        REQUIRE(out.has_value());
        REQUIRE(ints(*out, "n")[0] == 2);
    }
}

TEST_CASE("global count keeps an empty columnless input", "[aggregate][audit]") {
    auto parsed = parser::parse("Table(0)[select { n=count() }];");
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto out = runtime::interpret(**lowered, {});
    REQUIRE(out.has_value());
    REQUIRE(out->rows() == 1);
    REQUIRE(ints(*out, "n")[0] == 0);
}

TEST_CASE("Boolean first last skip nulls and preserve all-null groups",
          "[aggregate][audit][bool]") {
    const bool global = GENERATE(false, true);
    const bool all_null = GENERATE(false, true);
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 1, 1, 2, 2});
    t.add_column("v", Column<bool>{false, true, false, true, true, false});
    t.columns[1].validity = runtime::ValidityBitmap(6, false);
    if (!all_null) {
        t.columns[1].validity->set(1, true);
        t.columns[1].validity->set(2, true);
    }
    std::vector<ir::ColumnRef> keys;
    if (!global)
        keys.push_back({.name = "k"});
    std::vector<ir::AggSpec> aggs{
        {.func = ir::AggFunc::First, .column = {.name = "v"}, .alias = "f"},
        {.func = ir::AggFunc::Last, .column = {.name = "v"}, .alias = "l"}};
    std::expected<runtime::Table, std::string> out;
    SECTION("streaming across chunks") {
        const bool ordered = GENERATE(false, true);
        std::vector<runtime::Chunk> chunks;
        for (std::size_t begin = 0; begin < 6; begin += 2) {
            runtime::Chunk c;
            Column<std::int64_t> k;
            Column<bool> v;
            for (std::size_t row = begin; row < begin + 2; ++row) {
                k.push_back(ints(t, "k")[row]);
                v.push_back(std::get<Column<bool>>(*t.find("v"))[row]);
            }
            c.add_column("k", std::move(k));
            c.add_column("v", std::move(v));
            c.columns[1].validity = runtime::ValidityBitmap(2, false);
            for (std::size_t row = 0; row < 2; ++row) {
                c.columns[1].validity->set(row, (*t.columns[1].validity)[begin + row]);
            }
            if (ordered)
                sorted(c);
            chunks.push_back(std::move(c));
        }
        // A materializing fallback keeps the first column alive while fetching
        // the next chunk; streaming must release it after reducing its rows.
        out = aggregate_chunks(std::move(chunks), keys, aggs, true);
    }
    SECTION("materialized") {
        out = runtime::aggregate_table(t, keys, aggs);
    }
    REQUIRE(out.has_value());
    REQUIRE(out->rows() == (global ? 1 : 2));
    REQUIRE(runtime::is_null(*out->find_entry("f"), 0) == all_null);
    REQUIRE(runtime::is_null(*out->find_entry("l"), 0) == all_null);
    if (!all_null) {
        REQUIRE(std::get<Column<bool>>(*out->find("f"))[0]);
        REQUIRE_FALSE(std::get<Column<bool>>(*out->find("l"))[0]);
    }
    if (!global) {
        REQUIRE(runtime::is_null(*out->find_entry("f"), 1));
        REQUIRE(runtime::is_null(*out->find_entry("l"), 1));
    }
}
