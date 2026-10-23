// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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

// The serial Int64 path numbers groups without a hash map while their keys
// arrive in strictly increasing order, and builds the map only when a key goes
// backwards. Whatever the shape, the result must be the hash answer: groups in
// first-appearance order, every row counted once in its own group.
TEST_CASE("serial int64 aggregate defers its index until a key goes backwards",
          "[aggregate][audit]") {
    using Keys = std::vector<std::vector<std::int64_t>>;  // one inner vector per chunk
    const auto runs = [](std::int64_t from, std::int64_t to, std::int64_t length) {
        std::vector<std::int64_t> keys;
        for (std::int64_t k = from; k < to; ++k) {
            for (std::int64_t r = 0; r < 1 + (k % length); ++r) {
                keys.push_back(k);
            }
        }
        return keys;
    };
    std::vector<std::int64_t> backwards = runs(0, 3000, 4);
    for (const std::int64_t k : {17, 2999, 4000, 4000, 3, 5000, 0}) {
        backwards.push_back(k);
    }
    std::vector<std::int64_t> later = runs(100, 200, 3);  // all below chunk 1's max
    later.push_back(9000);
    const Keys shape =
        GENERATE_COPY(Keys{runs(0, 5000, 4)},                       // sorted, never backwards
                      Keys{backwards},                              // backwards mid-chunk
                      Keys{runs(0, 3000, 4), later},                // backwards in a later chunk
                      Keys{runs(0, 2000, 3), runs(2000, 4000, 5)},  // sorted across chunks
                      Keys{{std::numeric_limits<std::int64_t>::min(), -1, -1, 7, -1}});
    std::vector<runtime::Chunk> chunks;
    std::vector<std::int64_t> order;
    std::map<std::int64_t, std::pair<std::int64_t, std::int64_t>> expected;  // count, sum
    std::int64_t value = 0;
    for (const auto& chunk_keys : shape) {
        Column<std::int64_t> keys;
        Column<std::int64_t> values;
        for (const std::int64_t k : chunk_keys) {
            keys.push_back(k);
            values.push_back(++value);
            if (!expected.contains(k)) {
                order.push_back(k);
            }
            expected[k].first += 1;
            expected[k].second += value;
        }
        runtime::Chunk c;
        c.add_column("k", std::move(keys));
        c.add_column("v", std::move(values));
        chunks.push_back(std::move(c));
    }
    auto out =
        aggregate_chunks(std::move(chunks), {{.name = "k"}},
                         {{.func = ir::AggFunc::Count, .column = {}, .alias = "n"},
                          {.func = ir::AggFunc::Sum, .column = {.name = "v"}, .alias = "s"}});
    REQUIRE(out.has_value());
    REQUIRE(out->rows() == order.size());
    const auto& k = ints(*out, "k");
    const auto& n = ints(*out, "n");
    const auto& s = ints(*out, "s");
    std::size_t wrong = 0;
    for (std::size_t g = 0; g < order.size(); ++g) {
        wrong += k[g] == order[g] && n[g] == expected[order[g]].first &&
                         s[g] == expected[order[g]].second
                     ? 0U
                     : 1U;
    }
    CHECK(wrong == 0);
}

// A grouped First/Last over a Categorical keeps the input's codes in its slots
// and emits them against the input's dictionary, as long as every chunk shares
// that dictionary. A chunk over another dictionary sends the aggregate back to
// boxed strings, decoding what it already held -- the answer must not move.
TEST_CASE("categorical first and last keep codes, and survive a dictionary change",
          "[aggregate][audit][categorical]") {
    const bool with_nulls = GENERATE(false, true);
    const bool second_dictionary = GENERATE(false, true);
    using Cat = Column<Categorical>;
    const auto shared_dict =
        std::make_shared<std::vector<std::string>>(std::vector<std::string>{"a", "b", "c", "d", "e"});
    const auto shared_index = std::make_shared<Cat::index_map>();
    for (std::size_t i = 0; i < shared_dict->size(); ++i) {
        shared_index->emplace((*shared_dict)[i], static_cast<Cat::code_type>(i));
    }
    // Rows: k = 1 2 1 3 | 2 1 3 4, v = a b x x | c d x e, where x is null when
    // `with_nulls` and otherwise the value named in `filler`.
    const std::vector<std::int64_t> k1{1, 2, 1, 3};
    const std::vector<std::int64_t> k2{2, 1, 3, 4};
    const std::vector<std::string> v1{"a", "b", "c", "e"};
    const std::vector<std::string> v2{"c", "d", "b", "e"};
    const auto make_chunk = [&](const std::vector<std::int64_t>& keys,
                                const std::vector<std::string>& values, bool own_dictionary,
                                std::vector<bool> valid) {
        runtime::Chunk c;
        Column<std::int64_t> k;
        for (const std::int64_t key : keys) {
            k.push_back(key);
        }
        c.add_column("k", std::move(k));
        Cat v = own_dictionary
                    ? Cat{}
                    : Cat{shared_dict, shared_index, Cat::codes_storage{}};
        for (const auto& value : values) {
            v.push_back(value);
        }
        c.add_column("v", std::move(v));
        if (with_nulls) {
            c.columns[1].validity = runtime::ValidityBitmap(valid.size(), true);
            for (std::size_t row = 0; row < valid.size(); ++row) {
                c.columns[1].validity->set(row, valid[row]);
            }
        }
        return c;
    };
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(make_chunk(k1, v1, false, {true, true, false, false}));
    chunks.push_back(make_chunk(k2, v2, second_dictionary, {true, true, false, true}));
    const std::vector<ir::ColumnRef> keys{{.name = "k"}};
    const std::vector<ir::AggSpec> aggs{
        {.func = ir::AggFunc::First, .column = {.name = "v"}, .alias = "f"},
        {.func = ir::AggFunc::Last, .column = {.name = "v"}, .alias = "l"}};
    auto out = aggregate_chunks(std::move(chunks), keys, aggs);
    REQUIRE(out.has_value());
    REQUIRE(out->rows() == 4);
    for (std::size_t g = 0; g < 4; ++g) {
        CHECK(ints(*out, "k")[g] == static_cast<std::int64_t>(g + 1));
    }
    const auto& f = std::get<Cat>(*out->find("f"));
    const auto& l = std::get<Cat>(*out->find("l"));
    if (with_nulls) {
        const std::vector<std::string> want_f{"a", "b", "", "e"};
        const std::vector<std::string> want_l{"d", "c", "", "e"};
        for (std::size_t g = 0; g < 4; ++g) {
            INFO("group " << g);
            CHECK(runtime::is_null(*out->find_entry("f"), g) == (g == 2));
            CHECK(runtime::is_null(*out->find_entry("l"), g) == (g == 2));
            if (g != 2) {
                CHECK(std::string(f[g]) == want_f[g]);
                CHECK(std::string(l[g]) == want_l[g]);
            }
        }
    } else {
        const std::vector<std::string> want_f{"a", "b", "e", "e"};
        const std::vector<std::string> want_l{"d", "c", "b", "e"};
        for (std::size_t g = 0; g < 4; ++g) {
            INFO("group " << g);
            CHECK(std::string(f[g]) == want_f[g]);
            CHECK(std::string(l[g]) == want_l[g]);
        }
    }
    // Over one dictionary the output shares it: the codes were kept, not the
    // strings re-hashed into a new dictionary.
    if (!second_dictionary) {
        CHECK(f.dictionary_ptr() == shared_dict);
        CHECK(l.dictionary_ptr() == shared_dict);
    }
}
