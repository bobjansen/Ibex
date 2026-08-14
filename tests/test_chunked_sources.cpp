// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Multi-chunk execution: Phase 0 of plans/pipelined-execution-plan.md.
//
// Production has never emitted more than one chunk from any source, so every
// cross-chunk path in every operator is code that has never run. `IBEX_CHUNK_ROWS`
// makes a materialized source emit successive row ranges so those paths execute,
// and these tests pin the two defects that switch immediately exposed:
//
//   * a second Categorical key column read freed memory, because interning the
//     first one handed out a pointer that interning the second invalidated;
//   * concatenating buffered chunks appended column values but not validity
//     bitmaps, so every row past the first chunk read as null.
//
// Both were invisible at one chunk and wrong at two.

#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/env.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

/// Sets `IBEX_CHUNK_ROWS` for a scope and restores whatever was there.
class ChunkGrainGuard {
   public:
    explicit ChunkGrainGuard(const std::string& grain) {
        if (const char* value = std::getenv("IBEX_CHUNK_ROWS"); value != nullptr) {
            saved_ = value;
        }
        runtime::set_env("IBEX_CHUNK_ROWS", grain);
    }
    ~ChunkGrainGuard() {
        if (saved_.has_value()) {
            runtime::set_env("IBEX_CHUNK_ROWS", *saved_);
        } else {
            runtime::unset_env("IBEX_CHUNK_ROWS");
        }
    }
    ChunkGrainGuard(const ChunkGrainGuard&) = delete;
    auto operator=(const ChunkGrainGuard&) -> ChunkGrainGuard& = delete;
    ChunkGrainGuard(ChunkGrainGuard&&) = delete;
    auto operator=(ChunkGrainGuard&&) -> ChunkGrainGuard& = delete;

   private:
    std::optional<std::string> saved_;
};

auto run(const std::string& source, const runtime::TableRegistry& registry) -> runtime::Table {
    auto program = parser::parse(source);
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());
    runtime::ExecutionContext exec;
    auto result = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, exec);
    REQUIRE(result.has_value());
    return std::move(*result);
}

}  // namespace

TEST_CASE("chunked source groups two categorical keys identically to one chunk",
          "[runtime][chunked][categorical]") {
    // Two Categorical key columns is the shape that broke: interning column 0
    // returned a pointer into the interning state, and interning column 1 grew
    // that state and invalidated it. One chunk never noticed, because nothing
    // read the stale pointer before it was rebuilt.
    // Three categorical key columns with sizeable dictionaries, which is the
    // shape that broke: interning column 0 returned a pointer into the encoder's
    // interning state, and interning column 1 grew that state and invalidated it.
    //
    // The defect was a use-after-free, so where it is caught depends on the
    // build. A local Release run proves nothing — whether the freed buffer still
    // holds its old contents is up to the allocator, and this test passes
    // against the unfixed code there. CI is where it bites: the Debug half of
    // the matrix configures `IBEX_ENABLE_SANITIZERS=ON` on both Linux and macOS,
    // and ASan reports the read deterministically. What found it originally was
    // the PDS-H answer check under IBEX_CHUNK_ROWS, where q7 emitted 8 groups
    // instead of 4.
    constexpr std::size_t kRows = 8000;
    constexpr std::int32_t kDict = 64;
    std::vector<std::string> names;
    names.reserve(static_cast<std::size_t>(kDict));
    for (std::int32_t i = 0; i < kDict; ++i) {
        names.push_back("value-" + std::to_string(i));
    }
    Column<Categorical> a{names, std::vector<std::int32_t>{}};
    Column<Categorical> b{names, std::vector<std::int32_t>{}};
    Column<Categorical> c{names, std::vector<std::int32_t>{}};
    Column<std::int64_t> amount;
    for (std::size_t i = 0; i < kRows; ++i) {
        a.push_code(static_cast<std::int32_t>(i % 5));
        b.push_code(static_cast<std::int32_t>((i / 5) % 7));
        c.push_code(static_cast<std::int32_t>((i / 35) % 11));
        amount.push_back(1);
    }
    runtime::Table table;
    table.add_column("a", std::move(a));
    table.add_column("b", std::move(b));
    table.add_column("c", std::move(c));
    table.add_column("amount", std::move(amount));

    runtime::TableRegistry registry;
    registry.emplace("t", table);
    const std::string query = "t[select { n = sum(amount) }, by { a, b, c }];";

    const auto whole = run(query, registry);
    for (const char* grain : {"1000", "137", "17", "1"}) {
        const ChunkGrainGuard guard{grain};
        const auto chunked = run(query, registry);
        INFO("grain " << grain);
        // Same groups, in the same order, with the same sums: a lost group
        // shows up as extra rows, a mis-hashed key as a different count.
        REQUIRE(chunked.rows() == whole.rows());  // NOLINT
        auto mismatch = runtime::compare_tables(whole, chunked);
        if (mismatch.has_value()) {
            FAIL(mismatch->message());
        }
    }
}

TEST_CASE("chunked source preserves validity across a concatenating operator",
          "[runtime][chunked][validity]") {
    // `order` buffers every chunk and concatenates. The concat appended column
    // values but not validity, so the result kept only the FIRST chunk's bitmap
    // and every row past it read as null.
    constexpr std::size_t kRows = 3000;
    Column<std::int64_t> key;
    Column<double> value;
    runtime::ValidityBitmap valid;
    for (std::size_t i = 0; i < kRows; ++i) {
        key.push_back(static_cast<std::int64_t>(kRows - i));  // descending: every row moves
        value.push_back(static_cast<double>(i));
        valid.push_back(i % 7 != 2);
    }
    runtime::Table table;
    table.add_column("key", std::move(key));
    table.add_column("value", std::move(value));
    table.columns[1].validity = std::move(valid);

    runtime::TableRegistry registry;
    registry.emplace("t", table);
    const std::string query = "t[order { key asc }];";

    const auto whole = run(query, registry);
    for (const char* grain : {"1000", "137", "64", "63"}) {
        const ChunkGrainGuard guard{grain};
        const auto chunked = run(query, registry);
        INFO("grain " << grain);
        auto mismatch = runtime::compare_tables(whole, chunked);
        if (mismatch.has_value()) {
            FAIL(mismatch->message());
        }
    }
}

TEST_CASE("chunked source preserves validity that appears only in a later chunk",
          "[runtime][chunked][validity]") {
    // The backfill direction: the first chunk has no bitmap at all, so one must
    // be built for the rows already appended before the later chunk's bits can
    // be added. Getting the row count wrong here silently nulls the head of the
    // column, which the test above cannot see.
    constexpr std::size_t kRows = 600;
    Column<std::int64_t> key;
    Column<double> value;
    runtime::ValidityBitmap valid;
    for (std::size_t i = 0; i < kRows; ++i) {
        key.push_back(static_cast<std::int64_t>(kRows - i));
        value.push_back(static_cast<double>(i));
        valid.push_back(i < 500);  // nulls only in the tail
    }
    runtime::Table table;
    table.add_column("key", std::move(key));
    table.add_column("value", std::move(value));
    table.columns[1].validity = std::move(valid);

    runtime::TableRegistry registry;
    registry.emplace("t", table);
    const std::string query = "t[order { key asc }];";

    const auto whole = run(query, registry);
    for (const char* grain : {"250", "100", "64"}) {
        const ChunkGrainGuard guard{grain};
        const auto chunked = run(query, registry);
        INFO("grain " << grain);
        auto mismatch = runtime::compare_tables(whole, chunked);
        if (mismatch.has_value()) {
            FAIL(mismatch->message());
        }
    }
}
