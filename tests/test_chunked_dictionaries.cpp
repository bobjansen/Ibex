// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Categorical dictionaries across the chunks of one extern chunked source.
//
// The chunk contract (src/runtime/CONTRACTS.md §1) says every chunk of a stream
// shares one Categorical dictionary, and downstream operators copy codes
// verbatim. An extern source is free to build each chunk over its own
// dictionary (an Arrow stream does, batch by batch), so the runtime must not
// read those codes as if they shared one.

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

namespace {

using Cat = Column<Categorical>;

class VectorSource final : public runtime::Operator {
   public:
    explicit VectorSource(std::vector<runtime::Chunk> chunks) : chunks_(std::move(chunks)) {}

    auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
        if (pos_ >= chunks_.size()) {
            return std::optional<runtime::Chunk>{};
        }
        return std::optional<runtime::Chunk>{std::move(chunks_[pos_++])};
    }

   private:
    std::vector<runtime::Chunk> chunks_;
    std::size_t pos_ = 0;
};

/// One chunk whose column `k` is built over its OWN dictionary `dict`.
auto chunk_over(std::vector<std::string> dict, Cat::codes_storage codes,
                const std::vector<std::int64_t>& values) -> runtime::Chunk {
    runtime::Table table;
    table.add_column("k", Cat(std::move(dict), std::move(codes)));
    Column<std::int64_t> v;
    for (const auto x : values) {
        v.push_back(x);
    }
    table.add_column("v", std::move(v));
    runtime::Chunk chunk;
    chunk.columns = std::move(table.columns);
    return chunk;
}

/// Rows, in order: a, b | b, a, c. The second chunk's dictionary lists the
/// values in a different order and adds one, so verbatim codes would read
/// chunk two as a, b, c... -- or out of range.
auto split_dictionary_chunks() -> std::vector<runtime::Chunk> {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(chunk_over({"a", "b"}, {0, 1}, {1, 2}));
    chunks.push_back(chunk_over({"c", "b", "a"}, {1, 2, 0}, {3, 4, 5}));
    return chunks;
}

auto run(const std::string& query, std::size_t cores)
    -> std::expected<runtime::Table, std::string> {
    runtime::ExternRegistry externs;
    externs.register_chunked_table("split_dict", [](const runtime::ExternArgs&) {
        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(split_dictionary_chunks())};
    });
    auto parsed = parser::parse("extern fn split_dict() -> DataFrame from \"x.hpp\"; " + query);
    REQUIRE(parsed.has_value());
    auto ir = parser::lower(*parsed);
    REQUIRE(ir.has_value());
    runtime::ExecutionContext exec;
    exec.parallel_threads = cores;
    const runtime::TableRegistry registry;
    return runtime::interpret(**ir, registry, nullptr, &externs, nullptr, exec);
}

auto labels(const runtime::Table& table, const std::string& name) -> std::vector<std::string> {
    const auto* col = std::get_if<Cat>(table.find(name));
    REQUIRE(col != nullptr);
    std::vector<std::string> out;
    out.reserve(col->size());
    for (std::size_t i = 0; i < col->size(); ++i) {
        out.emplace_back((*col)[i]);
    }
    return out;
}

auto ints(const runtime::Table& table, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = std::get_if<Column<std::int64_t>>(table.find(name));
    REQUIRE(col != nullptr);
    return {col->begin(), col->end()};
}

}  // namespace

TEST_CASE("MaterializeOperator moves rows onto one dictionary when chunks disagree",
          "[runtime][operator][categorical][dictionary]") {
    // A plugin's own sink (adbc::query materializes its batches itself) never
    // passes through the runtime's source wrapper.
    auto chunks = split_dictionary_chunks();
    // A third chunk back on the FIRST chunk's dictionary, after it has grown:
    // the sink must translate it too, not copy it verbatim.
    Cat grown = std::get<Cat>(*chunks[0].columns[0].column).with_codes();
    grown.push_back("d");  // interns "d" into the first chunk's dictionary
    runtime::Chunk third;
    third.columns.emplace_back("k", std::make_shared<runtime::ColumnValue>(grown), std::nullopt);
    third.columns.emplace_back("v", std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{6}),
                               std::nullopt);
    chunks.push_back(std::move(third));

    auto out =
        runtime::MaterializeOperator(std::make_unique<VectorSource>(std::move(chunks))).run();
    REQUIRE(out.has_value());
    CHECK(labels(*out, "k") == std::vector<std::string>{"a", "b", "b", "a", "c", "d"});
}

TEST_CASE("dictionary unification never reads a null row's code",
          "[runtime][operator][categorical][dictionary]") {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(chunk_over({"a"}, {0}, {1}));
    // Row 0 is null and its payload is garbage; row 1 is "b".
    runtime::Chunk second = chunk_over({"b"}, {123456789, 0}, {2, 3});
    runtime::ValidityBitmap valid;
    valid.push_back(false);
    valid.push_back(true);
    second.columns[0].validity = std::move(valid);
    chunks.push_back(std::move(second));

    auto out =
        runtime::MaterializeOperator(std::make_unique<VectorSource>(std::move(chunks))).run();
    REQUIRE(out.has_value());
    const auto* entry = out->find_entry("k");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    const runtime::ValidityBitmap& validity =
        *entry->validity;  // NOLINT(bugprone-unchecked-optional-access)
    CHECK(validity[0]);
    CHECK_FALSE(validity[1]);
    CHECK(validity[2]);
    CHECK(labels(*out, "k")[2] == "b");
}

TEST_CASE("dictionary unification rejects a code outside its dictionary",
          "[runtime][operator][categorical][dictionary]") {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(chunk_over({"a"}, {0}, {1}));
    chunks.push_back(chunk_over({"b"}, {5}, {2}));
    auto out =
        runtime::MaterializeOperator(std::make_unique<VectorSource>(std::move(chunks))).run();
    REQUIRE_FALSE(out.has_value());
    CHECK(out.error().contains("outside its dictionary"));
}

TEST_CASE("chunked source with per-chunk dictionaries keeps its labels",
          "[runtime][chunked][categorical][dictionary]") {
    for (const std::size_t cores : {1U, 8U}) {
        CAPTURE(cores);
        SECTION("materialized") {
            auto out = run("split_dict();", cores);
            REQUIRE(out.has_value());
            CHECK(labels(*out, "k") == std::vector<std::string>{"a", "b", "b", "a", "c"});
            CHECK(ints(*out, "v") == std::vector<std::int64_t>{1, 2, 3, 4, 5});
        }
        SECTION("filtered on a label") {
            auto out = run("split_dict()[filter k == \"a\"];", cores);
            REQUIRE(out.has_value());
            CHECK(ints(*out, "v") == std::vector<std::int64_t>{1, 4});
        }
        SECTION("grouped") {
            auto out = run("split_dict()[select { s = sum(v) }, by k];", cores);
            REQUIRE(out.has_value());
            CHECK(labels(*out, "k") == std::vector<std::string>{"a", "b", "c"});
            CHECK(ints(*out, "s") == std::vector<std::int64_t>{5, 5, 5});
        }
        SECTION("distinct") {
            auto out = run("split_dict()[distinct k];", cores);
            REQUIRE(out.has_value());
            CHECK(labels(*out, "k") == std::vector<std::string>{"a", "b", "c"});
        }
    }
}
