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

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/env.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
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

/// Yields prebuilt chunks in order, so a test can vary what each one carries --
/// here, whether it has a validity bitmap at all.
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

auto run_extern(const std::string& source, const runtime::ExternRegistry& externs)
    -> runtime::Table {
    auto program = parser::parse(source);
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());
    const runtime::TableRegistry registry;
    const runtime::ExecutionContext exec;
    auto result = runtime::interpret(*ir.value(), registry, nullptr, &externs, nullptr, exec);
    REQUIRE(result.has_value());
    return std::move(*result);
}

auto run(const std::string& source, const runtime::TableRegistry& registry) -> runtime::Table {
    auto program = parser::parse(source);
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());
    const runtime::ExecutionContext exec;
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

TEST_CASE("chunked distinct dedups across a serial-then-parallel transition",
          "[runtime][chunked][distinct]") {
    // Distinct's packed path keeps two dedup stores that cannot see each other:
    // `seen` for the serial branch, one set per partition for the parallel one.
    // Its row gate is evaluated per chunk until the parallel path first runs,
    // so a chunk UNDER the gate dedups into `seen` and leaves the operator
    // still unactivated -- and the next chunk over the gate is then the first
    // parallel use, against empty partitions. Every value the serial chunk
    // already emitted is inserted afresh and emitted a second time.
    //
    // The shape is a small chunk ahead of a large one, which any filter with
    // uneven selectivity produces: here 5000 rows survive chunk 0 (under the
    // 32768 gate) and 40000 survive chunk 1 (over it). Deliberately NOT a
    // uniform grain -- equal chunks are all on one side of the gate and the
    // transition never happens.
    constexpr std::size_t kRows = 120000;
    constexpr std::size_t kFirstChunk = 40000;
    Column<std::int64_t> a;
    Column<std::int64_t> b;
    Column<std::int64_t> keep;
    for (std::size_t i = 0; i < kRows; ++i) {
        a.push_back(static_cast<std::int64_t>(i % 100));
        b.push_back(7);  // a second column, so the key packs rather than taking
                         // the single-column path -- which has a parallel branch
                         // of its own, covered by the next two cases
        keep.push_back(i < kFirstChunk ? (i % 8 == 0 ? 1 : 0) : 1);
    }
    runtime::Table table;
    table.add_column("a", std::move(a));
    table.add_column("b", std::move(b));
    table.add_column("keep", std::move(keep));

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));
    const std::string query = "t[filter keep == 1][distinct { a, b }];";

    const auto whole = run(query, registry);
    REQUIRE(whole.rows() == 100);  // a is i % 100, b constant

    const ChunkGrainGuard guard{"40000"};
    const auto chunked = run(query, registry);
    // Against the unfixed operator this is 125: the 25 values chunk 0 emitted
    // serially come back a second time from the first parallel chunk.
    REQUIRE(chunked.rows() == whole.rows());
    auto mismatch = runtime::compare_tables(whole, chunked);
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}

TEST_CASE("chunked single-column distinct dedups across a serial-then-parallel transition",
          "[runtime][chunked][distinct]") {
    // The twin of the packed case above, for the one-column typed store. It has
    // the same two invisible-to-each-other halves -- `seen` for the serial
    // branch, one set per partition for the parallel one -- so it has the same
    // hazard: a chunk under the row gate dedups into `seen` while leaving the
    // operator unactivated, and the first chunk over the gate then partitions
    // against empty sets and re-emits everything the serial chunk accepted.
    //
    // Same shape as the packed test: 5000 rows survive chunk 0 (under the 32768
    // gate) and 40000 survive chunk 1 (over it).
    constexpr std::size_t kRows = 120000;
    constexpr std::size_t kFirstChunk = 40000;
    Column<std::int64_t> a;
    Column<std::int64_t> keep;
    for (std::size_t i = 0; i < kRows; ++i) {
        a.push_back(static_cast<std::int64_t>(i % 100));
        keep.push_back(i < kFirstChunk ? (i % 8 == 0 ? 1 : 0) : 1);
    }
    runtime::Table table;
    table.add_column("a", std::move(a));
    table.add_column("keep", std::move(keep));

    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));
    const std::string query = "t[filter keep == 1][select { a }][distinct { a }];";

    const auto whole = run(query, registry);
    REQUIRE(whole.rows() == 100);

    const ChunkGrainGuard guard{"40000"};
    const auto chunked = run(query, registry);
    REQUIRE(chunked.rows() == whole.rows());
    auto mismatch = runtime::compare_tables(whole, chunked);
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}

TEST_CASE("chunked single-column distinct migrates partitioned values on a null chunk",
          "[runtime][chunked][distinct][validity]") {
    // The typed stores hold raw values and cannot express null, so the first
    // null-bearing chunk migrates whatever they hold into the validity-aware
    // generic index. Once the parallel path has run those values live in
    // `parts`, NOT in `seen` -- a migration that reads only `seen` seeds
    // nothing, and every value the threaded chunks emitted comes back a second
    // time.
    //
    // This needs chunks that DIFFER in whether they carry a bitmap at all,
    // which slicing one column cannot produce: an all-valid slice of a
    // null-bearing column still carries its bitmap, so every chunk would take
    // the generic path and the typed store would never run. A chunked extern
    // source can, and it is also the real shape -- a Parquet row group with
    // null-free stats yields a column with no validity, and a later row group
    // is then the first to carry one.
    constexpr std::size_t kFirstChunk = 40000;  // over the parallel path's row gate
    constexpr std::int64_t kValues = 100;

    runtime::ExternRegistry externs;
    externs.register_chunked_table("null_tail_keys", [&](const runtime::ExternArgs&) {
        std::vector<runtime::Chunk> chunks;

        runtime::Chunk dense;
        runtime::ColumnEntry dense_entry;
        dense_entry.name = "a";
        dense_entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
        auto& dense_col = std::get<Column<std::int64_t>>(*dense_entry.column);
        dense_col.reserve(kFirstChunk);
        for (std::size_t i = 0; i < kFirstChunk; ++i) {
            dense_col.push_back(static_cast<std::int64_t>(i) % kValues);
        }
        dense.columns.push_back(std::move(dense_entry));
        chunks.push_back(std::move(dense));

        // The first chunk to carry a bitmap: one repeat, one null, one repeat.
        runtime::Chunk nullable;
        runtime::ColumnEntry nullable_entry;
        nullable_entry.name = "a";
        nullable_entry.column =
            std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{5, 7, 0});
        runtime::ValidityBitmap valid;
        valid.push_back(true);
        valid.push_back(false);
        valid.push_back(true);
        nullable_entry.validity = std::move(valid);
        nullable.columns.push_back(std::move(nullable_entry));
        chunks.push_back(std::move(nullable));

        return std::expected<runtime::OperatorPtr, std::string>{
            std::make_unique<VectorSource>(std::move(chunks))};
    });

    const auto result = run_extern(
        "extern fn null_tail_keys() -> DataFrame from \"x.hpp\"; "
        "null_tail_keys()[distinct { a }];",
        externs);

    // The 100 values of the dense chunk, plus the null. The nullable chunk's 5
    // and 0 are repeats, and a migration that seeded only `seen` emits both
    // again for 103 rows -- which is what this asserted against before the
    // partitions were seeded too.
    REQUIRE(result.rows() == static_cast<std::size_t>(kValues) + 1);
}

TEST_CASE("chunked aggregate: moment aggregates agree serially and in parallel",
          "[runtime][chunked][aggregate]") {
    // The Welford/Pébay accumulators live in a per-group SCRATCH region, not in
    // AggSlotCore, so the parallel accumulate -- which works by giving each
    // morsel a private slot array and merging -- needs a private scratch to
    // match. Without one, two morsels accumulate the same group's variance into
    // the same doubles and the answer is silently wrong.
    //
    // Every other test of these aggregates is a handful of rows, which cannot
    // reach that path: it needs >= 65536 rows per morsel and >= 2 morsels. This
    // one is sized to cross it, with few enough groups that the merge-to-scan
    // gate lets it through.
    constexpr std::size_t kRows = 400000;
    constexpr std::int64_t kGroups = 4;
    Column<std::int64_t> key;
    Column<double> value;
    for (std::size_t i = 0; i < kRows; ++i) {
        key.push_back(static_cast<std::int64_t>(i) % kGroups);
        // Spread and asymmetric, so skew and kurtosis are not trivially zero.
        const auto x = static_cast<double>(i % 1000);
        value.push_back((x * x) / 100.0);
    }
    runtime::Table table;
    table.add_column("k", std::move(key));
    table.add_column("v", std::move(value));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    // TWO queries on purpose. `agg_is_combinable` excludes skew and kurtosis, so
    // a query containing either declines the parallel accumulate for ALL of its
    // aggregates -- putting them together would test the serial path twice and
    // pass against a broken parallel one. Only the first query below reaches the
    // private-scratch merge; the second covers the wider scratch stride.
    const std::string parallel_query = "t[select { s = std(v), n = count(v) }, by { k }];";
    const std::string serial_query = "t[select { sk = skew(v), ku = kurtosis(v) }, by { k }];";

    const auto run_with = [&](const std::string& query, bool parallel) {
        auto program = parser::parse(query);
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext exec;
        exec.parallel_threads = (parallel) ? 0 : 1;
        auto result = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        return std::move(*result);
    };

    const auto serial = run_with(parallel_query, false);
    const auto parallel = run_with(parallel_query, true);
    REQUIRE(serial.rows() == static_cast<std::size_t>(kGroups));
    REQUIRE(parallel.rows() == serial.rows());

    // Not bit-equality: a parallel reduction sums in a different order, and the
    // operator's own contract only promises the same answer to within the last
    // ulps (run-to-run stable, which is the property that matters).
    for (const char* name : {"s"}) {
        const auto* a = std::get_if<Column<double>>(serial.find(name));
        const auto* b = std::get_if<Column<double>>(parallel.find(name));
        REQUIRE(a != nullptr);
        REQUIRE(b != nullptr);
        for (std::size_t i = 0; i < a->size(); ++i) {
            INFO("column " << name << " row " << i);
            REQUIRE((*a)[i] != 0.0);  // a zero here would pass vacuously
            REQUIRE(std::abs((*a)[i] - (*b)[i]) <= 1e-9 * std::abs((*a)[i]));
        }
    }

    // Skew and kurtosis stay serial, but they use the same scratch region at a
    // wider stride, so a mis-sized or mis-offset layout shows up here.
    const auto moments = run_with(serial_query, true);
    REQUIRE(moments.rows() == static_cast<std::size_t>(kGroups));
    for (const char* name : {"sk", "ku"}) {
        const auto* col = std::get_if<Column<double>>(moments.find(name));
        REQUIRE(col != nullptr);
        for (std::size_t i = 0; i < col->size(); ++i) {
            INFO("column " << name << " row " << i);
            REQUIRE(std::isfinite((*col)[i]));
            REQUIRE((*col)[i] != 0.0);
        }
    }
}

TEST_CASE("chunked aggregate: output emission agrees serially and in parallel",
          "[runtime][chunked][aggregate]") {
    // `build_output_chunk` emits as `(output column x group range)` tasks.
    // Each task owns a disjoint slice of one column's buffer, so tasks cannot
    // race -- but a task that reads the wrong index (its own `ci` against
    // another column's source array, or its `g` against another range)
    // produces a plausible-looking chunk with the values of a neighbour. Only
    // a run with enough groups to cross the gate can see it: the emit stays
    // serial below `parallel_min_rows` groups, and every other aggregate test
    // has a handful of groups. `kGroups` is deliberately not a multiple of 64,
    // so the final partial validity word is covered too.
    constexpr std::size_t kRows = 300000;
    constexpr std::int64_t kGroups = 100000;
    Column<std::int64_t> k1;
    Column<std::int64_t> k2;
    Column<std::int64_t> v;
    for (std::size_t i = 0; i < kRows; ++i) {
        const auto g = static_cast<std::int64_t>(i) % kGroups;
        // Two DIFFERENT key columns: a column that emitted the other key's
        // values would still look well-formed.
        k1.push_back(g);
        k2.push_back(-g - 1);
        v.push_back(g * 3);
    }
    runtime::Table table;
    table.add_column("k1", std::move(k1));
    table.add_column("k2", std::move(k2));
    table.add_column("v", std::move(v));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    const std::string query =
        "t[select { n = count(v), s = sum(v), lo = min(v), hi = max(v) }, by { k1, k2 }];";

    const auto run_with = [&](bool parallel) {
        auto program = parser::parse(query);
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext exec;
        exec.parallel_threads = (parallel) ? 0 : 1;
        auto result = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        return std::move(*result);
    };

    const auto serial = run_with(false);
    const auto parallel = run_with(true);
    REQUIRE(serial.rows() == static_cast<std::size_t>(kGroups));
    REQUIRE(parallel.rows() == serial.rows());

    // Integer aggregates, so this is exact equality -- and row-for-row, because
    // the emit order is the group order whichever worker gets the column.
    for (const char* name : {"k1", "k2", "n", "s", "lo", "hi"}) {
        const auto* a = std::get_if<Column<std::int64_t>>(serial.find(name));
        const auto* b = std::get_if<Column<std::int64_t>>(parallel.find(name));
        REQUIRE(a != nullptr);
        REQUIRE(b != nullptr);
        REQUIRE(a->size() == static_cast<std::size_t>(kGroups));
        for (std::size_t i = 0; i < a->size(); ++i) {
            INFO("column " << name << " row " << i);
            REQUIRE((*a)[i] == (*b)[i]);
        }
    }
}

TEST_CASE("chunked aggregate: a narrow output still emits over the whole pool",
          "[runtime][chunked][aggregate]") {
    // The shape the range split exists for. One key plus one aggregate is two
    // output columns, so a column-per-task emit could use at most two workers
    // however many groups there are -- and this is the common wide-output
    // shape in PDS-H (q18's `sum(l_quantity) by l_orderkey`, q21's
    // `count() by l_orderkey`), at millions of groups.
    //
    // Correctness here is the same claim as the sibling test above, but the
    // arrangement is the interesting one: with two columns and ~70k groups
    // there are far MORE tasks than columns, so a task's `lo`/`hi` is what
    // separates it from its neighbours rather than its column index.
    constexpr std::size_t kGroups = 70'003;  // > parallel_min_rows, and prime
    constexpr std::size_t kRows = 3 * kGroups;
    Column<std::int64_t> key;
    Column<double> value;
    runtime::ValidityBitmap valid;
    key.reserve(kRows);
    value.reserve(kRows);
    for (std::size_t i = 0; i < kRows; ++i) {
        key.push_back(static_cast<std::int64_t>(i % kGroups));
        value.push_back(static_cast<double>(i) * 0.5);
        // Every value of one whole group in every 97 is null, so `min` reports
        // an invalid group -- the branch the emit's word-wise "did any range
        // clear a bit" scan exists for, and one that a all-valid input leaves
        // untested.
        valid.push_back((i % kGroups) % 97 != 5);
    }
    runtime::Table table;
    table.add_column("k", std::move(key));
    table.add_column("v", std::move(value));
    table.columns[1].validity = std::move(valid);
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    const std::string query = "t[select { lo = min(v) }, by { k }];";

    const auto run_with = [&](bool parallel) {
        auto program = parser::parse(query);
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext exec;
        exec.parallel_threads = (parallel) ? 0 : 1;
        auto result = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        return std::move(*result);
    };

    const auto serial = run_with(false);
    const auto parallel = run_with(true);
    REQUIRE(serial.rows() == kGroups);
    REQUIRE(parallel.rows() == kGroups);

    const auto* keys_s = std::get_if<Column<std::int64_t>>(serial.find("k"));
    const auto* keys_p = std::get_if<Column<std::int64_t>>(parallel.find("k"));
    const auto* lo_s = std::get_if<Column<double>>(serial.find("lo"));
    const auto* lo_p = std::get_if<Column<double>>(parallel.find("lo"));
    REQUIRE(keys_s != nullptr);
    REQUIRE(keys_p != nullptr);
    REQUIRE(lo_s != nullptr);
    REQUIRE(lo_p != nullptr);

    // The null groups must survive as nulls, in both runs, at the same rows --
    // a validity word written by the wrong range would move them.
    // A null group means the emit attached a validity bitmap; both runs must
    // agree that it did.
    REQUIRE(serial.columns[1].validity.has_value());
    REQUIRE(parallel.columns[1].validity.has_value());
    const auto& valid_s = *serial.columns[1].validity;
    const auto& valid_p = *parallel.columns[1].validity;
    REQUIRE(valid_s.size() == kGroups);
    REQUIRE(valid_p.size() == kGroups);
    // Asserted against a HAND-COMPUTED answer, not just against each other:
    // serial and parallel now share one task grid, so an index that is wrong
    // in the same way in both would agree with itself. Group g is first seen
    // at row g, so the group order is ascending; its rows are g, g + kGroups,
    // g + 2*kGroups, whose values are (row * 0.5) -- so the minimum is g/2.
    std::size_t nulls = 0;
    for (std::size_t i = 0; i < kGroups; ++i) {
        INFO("row " << i);
        REQUIRE((*keys_s)[i] == static_cast<std::int64_t>(i));
        REQUIRE((*keys_p)[i] == static_cast<std::int64_t>(i));
        REQUIRE(valid_s[i] == valid_p[i]);
        REQUIRE(valid_s[i] == (i % 97 != 5));
        if (!valid_s[i]) {
            ++nulls;
            continue;
        }
        REQUIRE((*lo_s)[i] == static_cast<double>(i) * 0.5);
        REQUIRE((*lo_p)[i] == static_cast<double>(i) * 0.5);
    }
    // The null groups are the point of the validity assertions above, so fail
    // loudly rather than silently if the input stopped producing any.
    REQUIRE(nulls > 0);
    REQUIRE(nulls == (kGroups + 91) / 97);
}

TEST_CASE("chunked aggregate: clustered integer counts merge runs across chunks",
          "[runtime][chunked][aggregate]") {
    constexpr std::size_t kRows = 300'000;
    Column<std::int64_t> keys;
    keys.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        keys.push_back(static_cast<std::int64_t>(row / 4));
    }
    runtime::Table table;
    table.add_column("k", std::move(keys));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));
    const std::string query = "t[select { n = count() }, by { k }];";

    const auto whole = run(query, registry);
    // Deliberately not divisible by the four-row run length: every boundary
    // bisects a group, so the per-chunk summaries must coalesce it exactly.
    const ChunkGrainGuard guard{"70001"};
    const auto chunked = run(query, registry);
    REQUIRE(chunked.rows() == 75'000);
    auto mismatch = runtime::compare_tables(whole, chunked);
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}
