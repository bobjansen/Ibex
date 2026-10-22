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
#include <ibex/core/decimal.hpp>
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

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "aggregate_chunked_internal.hpp"
#include "execution_profile_internal.hpp"
#include "join_chunked_internal.hpp"

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

TEST_CASE("multi-key categorical aggregate: the fused parallel path matches a reference",
          "[runtime][chunked][aggregate][categorical]") {
    // A multi-key Categorical group-by no longer runs a serial discovery pass
    // that writes a gid per row for a second pass to read back: the Cartesian
    // cell of the key codes IS a dense index, so each morsel accumulates into
    // its own `cells x aggs` slot array and the merge assigns gids on first
    // occurrence. Only a run past the morsel gate (>= 131072 rows in a chunk)
    // takes it, and every other multi-key Categorical test is a few thousand
    // rows -- so without this size the path never executes.
    constexpr std::size_t kRows = 450'000;
    Column<Categorical> a;
    Column<Categorical> b;
    Column<Categorical> c;
    Column<double> value;
    value.reserve(kRows);
    for (std::size_t i = 0; i < kRows; ++i) {
        // Three keys whose combinations do NOT fill their Cartesian product
        // (i % 2 is implied by i % 4), so the cell space is 24 but only 12
        // groups exist -- an empty cell that got a group would show up.
        a.push_back(std::string("a") + static_cast<char>('0' + (i % 4)));
        b.push_back(std::string("b") + static_cast<char>('0' + (i % 3)));
        c.push_back(std::string("c") + static_cast<char>('0' + (i % 2)));
        value.push_back(static_cast<double>(i % 1000) * 0.5);
    }
    runtime::Table table;
    table.add_column("a", std::move(a));
    table.add_column("b", std::move(b));
    table.add_column("c", std::move(c));
    table.add_column("v", std::move(value));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    const std::string query =
        "t[select { n = count(), s = sum(v), lo = min(v), hi = max(v) }, by { a, b, c }];";

    // An independent reference: a plain map keyed by the key TEXT, walked in
    // row order. It shares no machinery with the engine's cells, morsels or
    // slot arrays, which is the point -- the engine agreeing with itself at one
    // core and at eight would not catch a cell computed wrongly in both.
    struct Ref {
        std::size_t first_row = 0;
        std::int64_t n = 0;
        double s = 0.0;
        double lo = 0.0;
        double hi = 0.0;
    };
    std::map<std::string, Ref> reference;
    std::vector<std::string> order;
    for (std::size_t i = 0; i < kRows; ++i) {
        std::string key = std::string("a") + static_cast<char>('0' + (i % 4)) + "|b" +
                          static_cast<char>('0' + (i % 3)) + "|c" +
                          static_cast<char>('0' + (i % 2));
        const double v = static_cast<double>(i % 1000) * 0.5;
        auto [it, inserted] = reference.try_emplace(key);
        if (inserted) {
            it->second.first_row = i;
            it->second.lo = v;
            it->second.hi = v;
            order.push_back(key);
        }
        it->second.n += 1;
        it->second.s += v;
        it->second.lo = std::min(it->second.lo, v);
        it->second.hi = std::max(it->second.hi, v);
    }
    REQUIRE(reference.size() == 12);  // not 24: the cell space is not filled

    const auto check = [&](const runtime::Table& out) {
        REQUIRE(out.rows() == reference.size());
        const auto* ca = std::get_if<Column<Categorical>>(out.find("a"));
        const auto* cb = std::get_if<Column<Categorical>>(out.find("b"));
        const auto* cc = std::get_if<Column<Categorical>>(out.find("c"));
        const auto* cn = std::get_if<Column<std::int64_t>>(out.find("n"));
        const auto* cs = std::get_if<Column<double>>(out.find("s"));
        const auto* clo = std::get_if<Column<double>>(out.find("lo"));
        const auto* chi = std::get_if<Column<double>>(out.find("hi"));
        REQUIRE(ca != nullptr);
        REQUIRE(cb != nullptr);
        REQUIRE(cc != nullptr);
        REQUIRE(cn != nullptr);
        REQUIRE(cs != nullptr);
        REQUIRE(clo != nullptr);
        REQUIRE(chi != nullptr);
        for (std::size_t row = 0; row < out.rows(); ++row) {
            const std::string key = std::string((*ca)[row]) + "|" + std::string((*cb)[row]) + "|" +
                                    std::string((*cc)[row]);
            INFO("row " << row << " key " << key);
            // Group order is first-occurrence order, whichever morsel found it.
            REQUIRE(key == order[row]);
            const Ref& want = reference.at(key);
            REQUIRE((*cn)[row] == want.n);
            REQUIRE((*cs)[row] == want.s);
            REQUIRE((*clo)[row] == want.lo);
            REQUIRE((*chi)[row] == want.hi);
        }
    };

    runtime::Table parallel;
    runtime::Table serial;
    {
        auto program = parser::parse(query);
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        for (const bool fan_out : {true, false}) {
            runtime::ExecutionContext exec;
            exec.parallel_threads = fan_out ? 0 : 1;
            auto result =
                runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, exec);
            REQUIRE(result.has_value());
            (fan_out ? parallel : serial) = std::move(*result);
        }
    }
    check(parallel);
    check(serial);
    // The morsel cut is derived from the row count, not the thread count, so
    // the float reduction order -- and therefore every bit of the output -- is
    // the same however many workers ran it.
    auto mismatch = runtime::compare_tables(serial, parallel);
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }

    // Across chunks the cell -> gid array has to carry groups forward, and each
    // chunk is still large enough to take the fused path on its own.
    {
        const ChunkGrainGuard guard("150001");
        const auto chunked = run(query, registry);
        check(chunked);
    }
}

TEST_CASE("inner join: a small dense Int build side probes by direct address",
          "[runtime][chunked][join]") {
    // A build side whose keys cover a small range (h2o's id1/id2 joins) is
    // probed through a direct-address head table instead of its hash map.
    // Probe keys below, above and far outside that range must miss -- the
    // offset from the range's bottom wraps for a key below it -- and nulls
    // on either side must match nothing, whatever value they hold. Checked
    // against the pairs computed here, serially and fanned out, for a unique
    // build side, one with repeated keys, and one too sparse for the table.
    constexpr std::size_t kLeftRows = 300'000;
    constexpr auto kMin = std::numeric_limits<std::int64_t>::min();
    constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
    const auto run_case = [&](const std::vector<std::int64_t>& right_keys,
                              const std::vector<bool>& right_valid, bool expect_dense) {
        Column<std::int64_t> rk;
        Column<std::int64_t> rv;
        runtime::ValidityBitmap rk_valid;
        for (std::size_t r = 0; r < right_keys.size(); ++r) {
            rk.push_back(right_keys[r]);
            rv.push_back(static_cast<std::int64_t>(r) * 100);
            rk_valid.push_back(right_valid[r]);
        }
        Column<std::int64_t> lk;
        Column<std::int64_t> lv;
        runtime::ValidityBitmap lk_valid;
        std::vector<std::pair<std::int64_t, std::int64_t>> want;
        for (std::size_t i = 0; i < kLeftRows; ++i) {
            std::int64_t key = static_cast<std::int64_t>((i * 7919) % 23) - 8;
            if (i % 101 == 7) {
                key = i % 2 == 0 ? kMin : kMax;
            }
            const bool valid = i % 37 != 3;
            lk.push_back(valid ? key : right_keys.front());
            lv.push_back(static_cast<std::int64_t>(i));
            lk_valid.push_back(valid);
            if (!valid) {
                continue;
            }
            for (std::size_t r = 0; r < right_keys.size(); ++r) {
                if (right_valid[r] && right_keys[r] == key) {
                    want.emplace_back(static_cast<std::int64_t>(i),
                                      static_cast<std::int64_t>(r) * 100);
                }
            }
        }
        std::ranges::sort(want);
        runtime::Table left;
        left.add_column("k", std::move(lk));
        left.columns.back().validity = std::move(lk_valid);
        left.add_column("lv", std::move(lv));
        runtime::Table right;
        right.add_column("k", std::move(rk));
        right.columns.back().validity = std::move(rk_valid);
        right.add_column("rv", std::move(rv));
        runtime::TableRegistry registry;
        registry.emplace("lhs", std::move(left));
        registry.emplace("rhs", std::move(right));
        auto program = parser::parse("lhs join rhs on k;");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        for (const std::size_t threads : {std::size_t{1}, std::size_t{8}}) {
            INFO("threads " << threads);
            runtime::ExecutionContext exec;
            exec.parallel_threads = threads;
            exec.parallel_min_rows = 0;
            const auto before = runtime::dense_int_join_probes().load();
            const auto out =
                runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, exec);
            INFO((out.has_value() ? std::string{} : out.error()));
            REQUIRE(out.has_value());
            CHECK((runtime::dense_int_join_probes().load() > before) == expect_dense);
            const auto& olv = std::get<Column<std::int64_t>>(*out->find("lv"));
            const auto& orv = std::get<Column<std::int64_t>>(*out->find("rv"));
            std::vector<std::pair<std::int64_t, std::int64_t>> got;
            got.reserve(out->rows());
            for (std::size_t row = 0; row < out->rows(); ++row) {
                got.emplace_back(olv[row], orv[row]);
            }
            std::ranges::sort(got);
            REQUIRE(got == want);
        }
    };
    SECTION("unique keys, one null with an in-range payload") {
        run_case({-3, -2, -1, 0, 1, 2, 3, 4, 5, 0},
                 {true, true, true, true, true, true, true, true, true, false}, true);
    }
    SECTION("repeated keys chain") {
        run_case({2, -3, 2, 7, -3, 2, 0}, {true, true, true, true, true, true, true}, true);
    }
    SECTION("keys too sparse for the table") {
        run_case({0, 1, std::int64_t{1} << 40}, {true, true, true}, false);
    }
}

TEST_CASE("inner join: the left is drained only until the orientation is decided",
          "[runtime][chunked][join]") {
    // The join needs one fact about its left side -- whether it has more rows
    // than the right -- and it used to learn that by CONCATENATING the whole
    // side into a Table. It now stops pulling as soon as the comparison is
    // settled and streams the rest, so a left that is bigger than the right is
    // never copied. Both halves of that need covering:
    //
    //   * the left outruns the right, so the drain stops part-way and the
    //     probe continues from the partially-consumed operator -- the buffered
    //     chunks first, then the rest;
    //   * the left runs out first, so everything it had is buffered and the
    //     swap indexes it, which is the path that still concatenates.
    //
    // The right must exceed `kStreamRightThreshold` (65536) or the join takes
    // the small-right fast path and never measures the left at all.
    constexpr std::int64_t kRightRows = 70'000;

    Column<std::int64_t> right_id;
    Column<std::int64_t> right_val;
    right_id.reserve(kRightRows);
    for (std::int64_t i = 0; i < kRightRows; ++i) {
        right_id.push_back(i);
        right_val.push_back(i * 3);
    }
    runtime::Table right;
    right.add_column("id", std::move(right_id));
    right.add_column("rv", std::move(right_val));

    for (const std::int64_t left_rows : {200'000, 50'000}) {
        INFO("left_rows " << left_rows);
        Column<std::int64_t> left_id;
        Column<std::int64_t> left_val;
        left_id.reserve(static_cast<std::size_t>(left_rows));
        for (std::int64_t i = 0; i < left_rows; ++i) {
            left_id.push_back(i % kRightRows);
            left_val.push_back(i);
        }
        runtime::Table left;
        left.add_column("id", std::move(left_id));
        left.add_column("lv", std::move(left_val));

        runtime::TableRegistry registry;
        registry.emplace("lhs", std::move(left));
        registry.emplace("rhs", right);

        const std::string query = "lhs join rhs on id;";
        const auto one_chunk = run(query, registry);
        runtime::Table many_chunks;
        {
            // A grain that divides neither side, so the drain stops mid-chunk
            // relative to the right's row count rather than on a boundary.
            const ChunkGrainGuard guard("30001");
            many_chunks = run(query, registry);
        }
        auto mismatch = runtime::compare_tables(one_chunk, many_chunks);
        if (mismatch.has_value()) {
            FAIL(mismatch->message());
        }

        // Every right key is unique and every left key exists in it, so the
        // join is one output row per left row -- and `rv` is a function of the
        // key, which is what catches a probe that carried the wrong right row
        // across the buffered/streamed boundary.
        REQUIRE(many_chunks.rows() == static_cast<std::size_t>(left_rows));
        const auto* out_lv = std::get_if<Column<std::int64_t>>(many_chunks.find("lv"));
        const auto* out_rv = std::get_if<Column<std::int64_t>>(many_chunks.find("rv"));
        REQUIRE(out_lv != nullptr);
        REQUIRE(out_rv != nullptr);
        for (std::size_t row = 0; row < many_chunks.rows(); ++row) {
            INFO("row " << row);
            REQUIRE((*out_rv)[row] == ((*out_lv)[row] % kRightRows) * 3);
        }
    }
}

TEST_CASE("chunked semi/anti join: a multi-chunk right side matches one chunk",
          "[runtime][chunked][join]") {
    // The semi/anti join no longer materializes its right side into one Table;
    // it drains the producer and keeps the key column of each chunk. So every
    // right-side scan it runs -- the swapped build's key-space bitmap, the
    // intersection scan, the plain set build -- reads a CONCATENATION now, and
    // a global row range it splits for the pool can begin in one chunk and end
    // in another. At one chunk none of that is exercised.
    //
    // Getting this test to bite took two tries, and the reason is worth
    // recording: a left side that does not densely cover the right's key space
    // is blind to chunk boundaries. Dropping the last row of every right chunk
    // (the mutation this is written against) changes nothing if no left row
    // ever asks for those particular keys. Here the left covers every right
    // row below 20000 and the grain is small, so ~10 boundaries fall inside
    // the covered range.
    constexpr std::int64_t kRightRows = 70'000;  // > kSemiSwapThreshold (65536)
    constexpr std::int64_t kLeftRows = 40'000;
    constexpr const char* kGrain = "2003";  // 35 right chunks, and divides neither side

    Column<std::int64_t> right_id;
    runtime::ValidityBitmap right_valid;
    right_id.reserve(kRightRows);
    for (std::int64_t r = 0; r < kRightRows; ++r) {
        // Right row r holds key 2r, and every 1000th is null. A null build key
        // matches nothing -- including a left row holding that very value,
        // which is what `nulls never` means on the build side.
        right_id.push_back(r * 2);
        right_valid.push_back(r % 1000 != 0);
    }
    runtime::Table right;
    right.add_column("id", std::move(right_id));
    right.columns[0].validity = std::move(right_valid);

    // One definition of the answer, hand-computed from the construction above.
    const auto left_matches = [](std::int64_t id) {
        return id % 2 == 0 && id / 2 < kRightRows && (id / 2) % 1000 != 0;
    };

    // Two lefts, to reach both swapped builds. Both are dense over 0..39998 so
    // both see the chunk boundaries; the second adds one far-away key, which
    // widens the left key SPAN past the bitmap's density gate and sends the
    // build down the hash-map intersection instead of the dense one. That is
    // q04's path and q22's path respectively, and they scan the right
    // differently.
    for (const bool sparse_outlier : {false, true}) {
        Column<std::int64_t> left_id;
        Column<std::int64_t> payload;
        left_id.reserve(kLeftRows);
        for (std::int64_t i = 0; i < kLeftRows; ++i) {
            // Even ids can match; odd ids never can, since every right key is
            // even.
            left_id.push_back(sparse_outlier && i == kLeftRows - 1 ? 100'000'000 : i);
            payload.push_back(i * 10);
        }
        runtime::Table left;
        left.add_column("id", std::move(left_id));
        left.add_column("payload", std::move(payload));

        runtime::TableRegistry registry;
        registry.emplace("lhs", std::move(left));
        registry.emplace("rhs", right);

        for (const char* query : {"lhs semi join rhs on id;", "lhs anti join rhs on id;"}) {
            INFO(query << (sparse_outlier ? " (hash intersection)" : " (dense intersection)"));
            const auto one_chunk = run(query, registry);
            runtime::Table many_chunks;
            {
                const ChunkGrainGuard guard(kGrain);
                many_chunks = run(query, registry);
            }
            auto mismatch = runtime::compare_tables(one_chunk, many_chunks);
            if (mismatch.has_value()) {
                FAIL(mismatch->message());
            }

            // And against the hand-computed answer, so a scan that is wrong the
            // same way at one chunk and at thirty-five cannot agree with itself.
            const auto* ids = std::get_if<Column<std::int64_t>>(many_chunks.find("id"));
            REQUIRE(ids != nullptr);
            const bool semi = std::string_view{query}.find("semi") != std::string_view::npos;
            std::size_t expected = 0;
            for (std::int64_t i = 0; i < kLeftRows; ++i) {
                const std::int64_t id = sparse_outlier && i == kLeftRows - 1 ? 100'000'000 : i;
                if (left_matches(id) == semi) {
                    ++expected;
                }
            }
            REQUIRE(ids->size() == expected);
            for (std::size_t row = 0; row < ids->size(); ++row) {
                INFO("row " << row << " id " << (*ids)[row]);
                REQUIRE(left_matches((*ids)[row]) == semi);
            }
        }
    }
}

// The swapped hash build probes its buffered left by slot: each left row's
// slot in the left-key map is recorded as the map is built, and a row matches
// iff its slot was hit by the right scan. Duplicate left keys share a slot --
// here every key appears twice, half the table apart, so the two copies land
// in different buffered chunks -- and a null left key must match nothing even
// though its zero-valued cell has a slot like any other.
TEST_CASE("chunked semi/anti join: the swapped hash build probes duplicate and null left keys",
          "[runtime][chunked][join]") {
    constexpr std::int64_t kRightRows = 70'000;  // > kSemiSwapThreshold (65536)
    constexpr std::int64_t kLeftRows = 40'000;
    constexpr std::int64_t kDistinct = kLeftRows / 2;

    Column<std::int64_t> right_id;
    right_id.reserve(kRightRows);
    for (std::int64_t r = 0; r < kRightRows; ++r) {
        right_id.push_back(r * 2);  // even keys 0 .. 139998, zero included
    }
    runtime::Table right;
    right.add_column("id", std::move(right_id));

    // Left row i holds key i % kDistinct, except a far outlier that widens the
    // key span past the dense bitmap's gate (the hash path, q04's), and every
    // 777th row is null.
    const auto left_key = [](std::int64_t i) -> std::int64_t {
        return i == kLeftRows - 1 ? 100'000'000 : i % kDistinct;
    };
    const auto left_null = [](std::int64_t i) { return i % 777 == 0; };
    Column<std::int64_t> left_id;
    Column<std::int64_t> row_no;
    runtime::ValidityBitmap left_valid;
    for (std::int64_t i = 0; i < kLeftRows; ++i) {
        left_id.push_back(left_null(i) ? 0 : left_key(i));
        left_valid.push_back(!left_null(i));
        row_no.push_back(i);
    }
    runtime::Table left;
    left.add_column("id", std::move(left_id));
    left.columns[0].validity = std::move(left_valid);
    left.add_column("row_no", std::move(row_no));

    runtime::TableRegistry registry;
    registry.emplace("lhs", std::move(left));
    registry.emplace("rhs", std::move(right));

    const auto matches = [&](std::int64_t i) {
        const std::int64_t id = left_key(i);
        return !left_null(i) && id % 2 == 0 && id / 2 < kRightRows;
    };
    for (const char* query : {"lhs semi join rhs on id;", "lhs anti join rhs on id;"}) {
        INFO(query);
        const bool semi = std::string_view{query}.find("semi") != std::string_view::npos;
        runtime::Table out;
        {
            const ChunkGrainGuard guard("2003");
            out = run(query, registry);
        }
        const auto* rows = std::get_if<Column<std::int64_t>>(out.find("row_no"));
        REQUIRE(rows != nullptr);
        std::vector<std::int64_t> expected;
        for (std::int64_t i = 0; i < kLeftRows; ++i) {
            if (matches(i) == semi) {
                expected.push_back(i);
            }
        }
        REQUIRE(std::vector<std::int64_t>(rows->begin(), rows->end()) == expected);
    }
}

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
    Column<Categorical> a{names, Column<Categorical>::codes_storage{}};
    Column<Categorical> b{names, Column<Categorical>::codes_storage{}};
    Column<Categorical> c{names, Column<Categorical>::codes_storage{}};
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

TEST_CASE("chunked Decimal sum and mean stream without materializing input",
          "[runtime][chunked][aggregate][decimal]") {
    constexpr std::size_t kRows = 200'000;
    const DecimalType type{.precision = 8, .scale = 2};
    auto value = runtime::make_decimal_column(type);
    value.resize(kRows, Decimal{1});  // 0.01 per row
    runtime::Table table;
    table.add_column("x", std::move(value));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));

    const ChunkGrainGuard guard{"1024"};
    const auto sum = run("t[select { total = sum(x) }];", registry);
    const auto* total = std::get_if<Column<Decimal>>(sum.find("total"));
    REQUIRE(total != nullptr);
    CHECK(runtime::decimal_type_of(*total) == (DecimalType{.precision = 38, .scale = 2}));
    CHECK((*total)[0].units == 200'000);

    const auto mean = run("t[select { average = mean(x) }];", registry);
    const auto* average = std::get_if<Column<double>>(mean.find("average"));
    REQUIRE(average != nullptr);
    CHECK((*average)[0] == 0.01);
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

TEST_CASE("chunked aggregate: a clustered sum's map-free merge equals serial, in order or not",
          "[runtime][chunked][aggregate]") {
    // q18's shape: sum(Double) by one Int64 key, streamed in chunks, which
    // takes the async hot path. Its cold merge skips the hash map while a
    // partition's keys never decrease, and indexes what it has the moment one
    // does. Chunks of 70001 rows cut three-row key runs, so a group's rows
    // arrive in two jobs. The second shape goes backwards part way through,
    // into groups that already exist, then on to new ones: without the switch
    // to the map those rows would become duplicate groups. The sums are
    // order-sensitive, so bit equality also pins the order records are added.
    constexpr std::int64_t kRows = 400'000;
    const auto clustered = [](std::int64_t row) { return row / 3; };
    const auto backwards = [](std::int64_t row) {
        if (row >= 240'000 && row < 300'000) {
            return (row / 3) % 5'000;
        }
        return row / 3;
    };
    for (const bool goes_backwards : {false, true}) {
        INFO("goes backwards: " << goes_backwards);
        runtime::TableRegistry registry;
        {
            Column<std::int64_t> g;
            Column<double> v;
            for (std::int64_t row = 0; row < kRows; ++row) {
                g.push_back(goes_backwards ? backwards(row) : clustered(row));
                v.push_back(1e9 + (static_cast<double>(row % 991) * 0.125));
            }
            runtime::Table t;
            t.add_column("g", std::move(g));
            t.add_column("v", std::move(v));
            registry.emplace("t", std::move(t));
        }
        auto program = parser::parse("t[select { s = sum(v) }, by { g }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());

        const ChunkGrainGuard guard{"70001"};
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        runtime::ParallelPipelineStats stats;
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        parallel.parallel_stats = &stats;
        const auto s = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, serial);
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(s.has_value());
        REQUIRE(p.has_value());
        REQUIRE(stats.parallel_aggregate_partitions.load() > 0);
        // The backwards stretch revisits keys 0..4999, so keys 80000..99999
        // never occur.
        REQUIRE(s->rows() == (goes_backwards ? std::size_t{113'334} : std::size_t{133'334}));
        REQUIRE(p->rows() == s->rows());
        const auto& sg = std::get<Column<std::int64_t>>(*s->find("g"));
        const auto& pg = std::get<Column<std::int64_t>>(*p->find("g"));
        const auto& sv = std::get<Column<double>>(*s->find("s"));
        const auto& pv = std::get<Column<double>>(*p->find("s"));
        for (std::size_t i = 0; i < s->rows(); ++i) {
            INFO("group " << i);
            REQUIRE(sg[i] == pg[i]);
            REQUIRE(sv[i] == pv[i]);
        }
    }
}

TEST_CASE("chunked aggregate: an Int64 sum by one Int64 key streams through the hot table",
          "[runtime][chunked][aggregate]") {
    // q18 since l_quantity became Int64: sum(Int64) by one Int64 key. The
    // async hot table used to admit only Double sums, so this shape fell back
    // to three pool barriers per chunk -- 1,556 of them at SF-8 on Polars'
    // 122,880-row row groups, and 4.8x the Double query's time. The values
    // sit above 2^53, so a sum that went through `double_value` anywhere
    // (they share storage in AggSlotCore) cannot come out exact. Expected
    // sums are computed here, in first-seen order, not by another engine run.
    constexpr std::int64_t kRows = 400'000;
    constexpr std::int64_t kBase = std::int64_t{3'000'000'000'000'000};
    const auto clustered = [](std::int64_t row) { return row / 3; };
    const auto backwards = [](std::int64_t row) {
        if (row >= 240'000 && row < 300'000) {
            return (row / 3) % 5'000;
        }
        return row / 3;
    };
    for (const bool goes_backwards : {false, true}) {
        INFO("goes backwards: " << goes_backwards);
        runtime::TableRegistry registry;
        std::vector<std::int64_t> want_keys;
        std::map<std::int64_t, std::int64_t> want_sums;
        {
            Column<std::int64_t> g;
            Column<std::int64_t> v;
            for (std::int64_t row = 0; row < kRows; ++row) {
                const std::int64_t key = goes_backwards ? backwards(row) : clustered(row);
                const std::int64_t value = kBase + (row % 991);
                g.push_back(key);
                v.push_back(value);
                if (!want_sums.contains(key)) {
                    want_keys.push_back(key);
                }
                want_sums[key] += value;
            }
            runtime::Table t;
            t.add_column("g", std::move(g));
            t.add_column("v", std::move(v));
            registry.emplace("t", std::move(t));
        }
        auto program = parser::parse("t[select { s = sum(v) }, by { g }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());

        const ChunkGrainGuard guard{"70001"};
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/8,
                                                                        /*report=*/false);
        parallel.execution_profile = profile;
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(p.has_value());

        // The hot table submits one task per chunk and joins once at end of
        // stream (the Double path counts the same single barrier). The
        // per-chunk fallback parks at two barriers per chunk: 12 here.
        const auto rows = profile->snapshot();
        const auto discovery = std::ranges::find_if(
            rows, [](const auto& r) { return r.label == "Aggregate.Discovery"; });
        const std::uint64_t discovery_barriers = discovery == rows.end() ? 0 : discovery->barriers;
        CHECK(discovery_barriers <= 1);

        REQUIRE(p->rows() == want_keys.size());
        const auto& pg = std::get<Column<std::int64_t>>(*p->find("g"));
        const auto& pv = std::get<Column<std::int64_t>>(*p->find("s"));
        for (std::size_t i = 0; i < want_keys.size(); ++i) {
            INFO("group " << i);
            REQUIRE(pg[i] == want_keys[i]);
            REQUIRE(pv[i] == want_sums.at(want_keys[i]));
        }
    }
}

TEST_CASE("chunked aggregate: an Int64 sum by two Int64 keys streams through the pair path",
          "[runtime][chunked][aggregate]") {
    // q20's shape since l_quantity became Int64: sum(Int64) by (l_partkey,
    // l_suppkey). The async pair path (one task per chunk routing records to
    // their owner, one join at end of stream) admitted only Double sums, like
    // the hot table above did. Keys repeat far apart, so every group's rows
    // span several chunk jobs and the owners' merge adds them. Values sit
    // above 2^53, so a sum that went through `double_value` cannot come out
    // exact. Expected sums are computed here, in first-seen order.
    constexpr std::int64_t kRows = 400'000;
    constexpr std::int64_t kBase = std::int64_t{3'000'000'000'000'000};
    runtime::TableRegistry registry;
    std::vector<std::pair<std::int64_t, std::int64_t>> want_keys;
    std::map<std::pair<std::int64_t, std::int64_t>, std::int64_t> want_sums;
    {
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        Column<std::int64_t> v;
        for (std::int64_t row = 0; row < kRows; ++row) {
            const std::pair<std::int64_t, std::int64_t> key{(row * 7) % 20'011, row % 4};
            const std::int64_t value = kBase + (row % 991);
            a.push_back(key.first);
            b.push_back(key.second);
            v.push_back(value);
            if (!want_sums.contains(key)) {
                want_keys.push_back(key);
            }
            want_sums[key] += value;
        }
        runtime::Table t;
        t.add_column("a", std::move(a));
        t.add_column("b", std::move(b));
        t.add_column("v", std::move(v));
        registry.emplace("t", std::move(t));
    }
    auto program = parser::parse("t[select { s = sum(v) }, by { a, b }];");
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());

    const ChunkGrainGuard guard{"70001"};
    runtime::ExecutionContext parallel;
    parallel.parallel_threads = 8;
    parallel.parallel_min_rows = 0;
    auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/8,
                                                                    /*report=*/false);
    parallel.execution_profile = profile;
    const auto p = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
    REQUIRE(p.has_value());

    // One end-of-stream join, however many chunks; the per-chunk fallback
    // parks at pool barriers for every chunk.
    const auto rows = profile->snapshot();
    const auto discovery =
        std::ranges::find_if(rows, [](const auto& r) { return r.label == "Aggregate.Discovery"; });
    const std::uint64_t discovery_barriers = discovery == rows.end() ? 0 : discovery->barriers;
    CHECK(discovery_barriers <= 1);

    REQUIRE(p->rows() == want_keys.size());
    const auto& pa = std::get<Column<std::int64_t>>(*p->find("a"));
    const auto& pb = std::get<Column<std::int64_t>>(*p->find("b"));
    const auto& pv = std::get<Column<std::int64_t>>(*p->find("s"));
    for (std::size_t i = 0; i < want_keys.size(); ++i) {
        INFO("group " << i);
        REQUIRE(pa[i] == want_keys[i].first);
        REQUIRE(pb[i] == want_keys[i].second);
        REQUIRE(pv[i] == want_sums.at(want_keys[i]));
    }
}

TEST_CASE("zero fused tail retains typed columns", "[schema][tail]") {
    for (const char* grain : {"1", "16"}) {
        ChunkGrainGuard guard(grain);
        for (const char* predicate : {"v > 0", "v > 100"}) {
            INFO(grain << ": " << predicate);
            runtime::Table table;
            table.add_column("v", Column<std::int64_t>{10, 20});
            auto out = run(std::string("t[filter ") + predicate + ", tail 0];", {{"t", table}});
            CHECK(out.rows() == 0);
            REQUIRE(out.columns.size() == 1);
            CHECK(out.columns[0].name == "v");
            CHECK(std::holds_alternative<Column<std::int64_t>>(*out.find("v")));
            auto total = run("t[select { n = count(), s = sum(v) }];", {{"t", out}});
            REQUIRE(total.rows() == 1);
            CHECK(std::get<Column<std::int64_t>>(*total.find("n"))[0] == 0);
            CHECK(runtime::is_null(*total.find_entry("s"), 0));
        }
    }
}

TEST_CASE("empty order retains typed columns", "[schema][order]") {
    runtime::Table table;
    table.add_column("v", Column<std::int64_t>{});
    table.add_column("s", Column<std::string>{});
    for (const char* query : {"t[order v];", "t[order s desc];", "t[order];"}) {
        INFO(query);
        auto out = run(query, {{"t", table}});
        CHECK(out.rows() == 0);
        REQUIRE(out.columns.size() == 2);
        CHECK(out.columns[0].name == "v");
        CHECK(out.columns[1].name == "s");
        CHECK(std::holds_alternative<Column<std::int64_t>>(*out.find("v")));
        CHECK(std::holds_alternative<Column<std::string>>(*out.find("s")));
        auto total = run("t[select { n = count(), s = sum(v) }];", {{"t", out}});
        REQUIRE(total.rows() == 1);
        CHECK(std::get<Column<std::int64_t>>(*total.find("n"))[0] == 0);
        CHECK(runtime::is_null(*total.find_entry("s"), 0));
    }
}

TEST_CASE("empty distinct retains typed columns", "[schema][distinct]") {
    runtime::Table table;
    table.add_column("k", Column<std::int64_t>{});
    table.add_column("v", Column<std::int64_t>{});
    table.add_column("s", Column<std::string>{});
    for (const char* query : {"t[distinct k];", "t[distinct { k, v }];", "t[distinct { k, s }];"}) {
        INFO(query);
        auto out = run(query, {{"t", table}});
        CHECK(out.rows() == 0);
        REQUIRE(out.find("k") != nullptr);
        CHECK(std::holds_alternative<Column<std::int64_t>>(*out.find("k")));
        auto total = run("t[select { n = count(), s = sum(k) }];", {{"t", out}});
        REQUIRE(total.rows() == 1);
        CHECK(std::get<Column<std::int64_t>>(*total.find("n"))[0] == 0);
        CHECK(runtime::is_null(*total.find_entry("s"), 0));
    }
}

TEST_CASE("empty swapped inner join retains typed columns", "[schema][join]") {
    runtime::Table left;
    left.add_column("k", Column<std::int64_t>{1, 2});
    left.add_column("v", Column<double>{10.0, 20.0});
    runtime::Table right;
    right.add_column("k", Column<std::int64_t>{std::vector<std::int64_t>(70'000, 7)});
    right.add_column("s", Column<std::string>{std::vector<std::string>(70'000, "x")});
    for (const char* grain : {"0", "1024"}) {
        ChunkGrainGuard guard(grain);
        auto out = run("a join b on k;", {{"a", left}, {"b", right}});
        CHECK(out.rows() == 0);
        REQUIRE(out.columns.size() == 3);
        REQUIRE(out.find("k") != nullptr);
        REQUIRE(out.find("v") != nullptr);
        REQUIRE(out.find("s") != nullptr);
        CHECK(std::holds_alternative<Column<double>>(*out.find("v")));
        CHECK(std::holds_alternative<Column<std::string>>(*out.find("s")));
        auto total = run("t[select { n = count(), s = sum(v) }];", {{"t", out}});
        REQUIRE(total.rows() == 1);
        CHECK(std::get<Column<std::int64_t>>(*total.find("n"))[0] == 0);
        CHECK(runtime::is_null(*total.find_entry("s"), 0));
    }
}

TEST_CASE("empty join probe retains the first chunk identity", "[schema][join][identity]") {
    runtime::Table right;
    right.add_column("k", Column<std::int64_t>{7});
    std::vector<runtime::Chunk> inputs;
    for (std::size_t i = 0; i < 2; ++i) {
        runtime::Chunk chunk;
        chunk.add_column("k", Column<std::int64_t>{1});
        chunk.add_column("v", Column<double>{10.0});
        chunk.sequence = 42 + i;
        chunk.row_offset = 100 + i;
        inputs.push_back(std::move(chunk));
    }
    const std::vector<ir::JoinKey> keys{"k"};
    runtime::ExecutionContext exec;
    exec.parallel_threads = 1;
    auto op = runtime::make_chunked_inner_join_operator(
        std::make_unique<VectorSource>(std::move(inputs)), std::move(right), &keys, exec);
    auto first = op->next();
    REQUIRE(first.has_value());
    REQUIRE(first->has_value());
    CHECK((**first).rows() == 0);
    CHECK((**first).columns.size() == 2);
    CHECK((**first).sequence == 42);
    CHECK((**first).row_offset == 100);
    for (int i = 0; i < 2; ++i) {
        auto end = op->next();
        REQUIRE(end.has_value());
        CHECK_FALSE(end->has_value());
    }
}

TEST_CASE("generic group-by keys: partitioned discovery equals a hand-computed grouping",
          "[runtime][chunked][aggregate][parallel]") {
    // A key no fast path takes -- a long string, a double and a nullable int --
    // lands on the generic path, which keeps its group keys column by column
    // (`GroupKeyStore`) and, on enough rows, discovers groups by hash partition
    // across workers. Both are checked against a grouping computed here by
    // hand, in first-occurrence order: the serial path was rewritten too, so
    // agreeing with it proves nothing on its own.
    //
    // The shapes that break a key store: a string past the small-string
    // buffer (its bytes live in the store's buffer, not in a std::string),
    // -0.0 and 0.0 (one group, holding the first row's sign), and a null key
    // (one group, whatever payload the column holds under it). Streamed in
    // 20000-row chunks, the first chunk is under the partitioned path's row
    // floor and runs serially; the second starts partitioning with groups
    // already found, which must be handed over rather than issued again.
    constexpr std::size_t kRows = 300'000;
    Column<std::string> s;
    Column<double> d;
    Column<std::int64_t> n;
    Column<std::int64_t> q;
    runtime::ValidityBitmap n_valid;
    struct Group {
        std::string s;
        double d = 0.0;
        std::optional<std::int64_t> n;
        std::int64_t count = 0;
        std::int64_t total = 0;
    };
    std::vector<Group> groups;
    std::map<std::tuple<std::string, double, std::int64_t, bool>, std::size_t> index;
    for (std::size_t i = 0; i < kRows; ++i) {
        const std::string sv =
            "customer-" + std::to_string((i * 7919) % 2'003) + "-padding-past-the-sso";
        const double dv = std::array<double, 3>{0.0, -0.0, 1.5}[i % 3];
        const bool is_null = i % 13 == 0;
        const auto nv = static_cast<std::int64_t>(i % 11);
        const auto qv = static_cast<std::int64_t>(i % 97);
        s.push_back(sv);
        d.push_back(dv);
        n.push_back(nv);
        n_valid.push_back(!is_null);
        q.push_back(qv);
        // -0.0 == 0.0 as a map key too, so they share a group, as they must.
        const auto key = std::make_tuple(sv, dv, is_null ? 0 : nv, is_null);
        auto [it, inserted] = index.emplace(key, groups.size());
        if (inserted) {
            groups.push_back(Group{.s = sv,
                                   .d = dv,
                                   .n = is_null ? std::nullopt : std::optional{nv},
                                   .count = 0,
                                   .total = 0});
        }
        ++groups[it->second].count;
        groups[it->second].total += qv;
    }
    runtime::Table t;
    t.add_column("s", std::move(s));
    t.add_column("d", std::move(d));
    t.add_column("n", std::move(n));
    t.columns.back().validity = std::move(n_valid);
    t.add_column("q", std::move(q));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto program = parser::parse("t[select { total = sum(q), c = count() }, by { s, d, n }];");
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());

    const auto check = [&](const runtime::Table& out) {
        REQUIRE(out.rows() == groups.size());
        const auto& os = std::get<Column<std::string>>(*out.find("s"));
        const auto& od = std::get<Column<double>>(*out.find("d"));
        const auto& on = std::get<Column<std::int64_t>>(*out.find("n"));
        const auto* on_valid = out.find_entry("n");
        REQUIRE(on_valid != nullptr);
        const auto& ot = std::get<Column<std::int64_t>>(*out.find("total"));
        const auto& oc = std::get<Column<std::int64_t>>(*out.find("c"));
        for (std::size_t g = 0; g < groups.size(); ++g) {
            INFO("group " << g);
            const Group& want = groups[g];
            REQUIRE(std::string(os[g]) == want.s);
            REQUIRE(od[g] == want.d);
            REQUIRE(std::signbit(od[g]) == std::signbit(want.d));
            REQUIRE(runtime::is_null(*on_valid, g) == !want.n.has_value());
            if (want.n.has_value()) {
                REQUIRE(on[g] == *want.n);
            }
            REQUIRE(oc[g] == want.count);
            REQUIRE(ot[g] == want.total);
        }
    };

    for (const bool chunked : {false, true}) {
        INFO("chunked: " << chunked);
        std::optional<ChunkGrainGuard> guard;
        if (chunked) {
            guard.emplace("20000");
        }
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        runtime::ParallelPipelineStats stats;
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        parallel.parallel_stats = &stats;
        const auto sr =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, serial);
        const auto pr =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        INFO((sr.has_value() ? std::string{} : sr.error()));
        REQUIRE(sr.has_value());
        INFO((pr.has_value() ? std::string{} : pr.error()));
        REQUIRE(pr.has_value());
        // Guards the test: the parallel run really partitioned its discovery.
        REQUIRE(stats.parallel_aggregate_partitions.load() > 0);
        check(*sr);
        check(*pr);
    }
}

TEST_CASE("packed group-by keys: a String key column interns into the packed key",
          "[runtime][chunked][aggregate][parallel]") {
    // A String among three or more keys used to send the whole key to the
    // generic path (h2o q10 at K=2e0: id3 a 5M-level String). It now interns
    // into ids per row and packs like a Categorical. Checked against a
    // grouping computed here, in first-occurrence order, serially and with
    // the interning partitioned across workers, whole and in 20000-row
    // chunks -- the first chunk then interns serially and the rest must find
    // its strings again. Values past the small-string buffer and the empty
    // string are the shapes an interning arena gets wrong.
    constexpr std::size_t kRows = 300'000;
    Column<std::string> s;
    Column<std::int64_t> a;
    Column<std::int64_t> b;
    Column<std::int64_t> q;
    struct Group {
        std::string s;
        std::int64_t a = 0;
        std::int64_t b = 0;
        std::int64_t count = 0;
        std::int64_t total = 0;
    };
    std::vector<Group> groups;
    std::map<std::tuple<std::string, std::int64_t, std::int64_t>, std::size_t> index;
    for (std::size_t i = 0; i < kRows; ++i) {
        const std::size_t k = (i * 7919) % 2'003;
        const std::string sv = k == 0  ? std::string{}
                               : k % 2 ? "c" + std::to_string(k)
                                       : "customer-" + std::to_string(k) + "-padding-past-the-sso";
        const auto av = static_cast<std::int64_t>(i % 3) - 1;
        const auto bv = static_cast<std::int64_t>((i * 31) % 5) * 1'000'000;
        const auto qv = static_cast<std::int64_t>(i % 97);
        s.push_back(sv);
        a.push_back(av);
        b.push_back(bv);
        q.push_back(qv);
        auto [it, inserted] = index.emplace(std::make_tuple(sv, av, bv), groups.size());
        if (inserted) {
            groups.push_back(Group{.s = sv, .a = av, .b = bv, .count = 0, .total = 0});
        }
        ++groups[it->second].count;
        groups[it->second].total += qv;
    }
    runtime::Table t;
    t.add_column("s", std::move(s));
    t.add_column("a", std::move(a));
    t.add_column("b", std::move(b));
    t.add_column("q", std::move(q));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto program = parser::parse("t[select { total = sum(q), c = count() }, by { a, s, b }];");
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());

    const auto check = [&](const runtime::Table& out) {
        REQUIRE(out.rows() == groups.size());
        const auto& os = std::get<Column<std::string>>(*out.find("s"));
        const auto& oa = std::get<Column<std::int64_t>>(*out.find("a"));
        const auto& ob = std::get<Column<std::int64_t>>(*out.find("b"));
        const auto& ot = std::get<Column<std::int64_t>>(*out.find("total"));
        const auto& oc = std::get<Column<std::int64_t>>(*out.find("c"));
        for (std::size_t g = 0; g < groups.size(); ++g) {
            INFO("group " << g);
            const Group& want = groups[g];
            REQUIRE(std::string(os[g]) == want.s);
            REQUIRE(oa[g] == want.a);
            REQUIRE(ob[g] == want.b);
            REQUIRE(oc[g] == want.count);
            REQUIRE(ot[g] == want.total);
        }
    };

    for (const bool chunked : {false, true}) {
        INFO("chunked: " << chunked);
        std::optional<ChunkGrainGuard> guard;
        if (chunked) {
            guard.emplace("20000");
        }
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto sr =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, serial);
        const auto pr =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        INFO((sr.has_value() ? std::string{} : sr.error()));
        REQUIRE(sr.has_value());
        INFO((pr.has_value() ? std::string{} : pr.error()));
        REQUIRE(pr.has_value());
        check(*sr);
        check(*pr);
    }
}

TEST_CASE("one-core aggregate: partitions discovery only for a mostly distinct key",
          "[runtime][chunked][aggregate]") {
    // On one core, partitioning is a cache-locality trade, not parallelism:
    // it wins when one table of the groups would miss cache (h2o q10) and
    // loses when the groups are few. A strided sample decides. Both shapes
    // are checked against a grouping computed here, in first-occurrence
    // order, and a counter shows which way each went.
    constexpr std::size_t kRows = 300'000;
    const auto run = [&](const auto& key_of, bool expect_partitioned) {
        Column<std::int64_t> a;
        Column<std::int64_t> b;
        Column<std::int64_t> c;
        Column<std::int64_t> q;
        std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>> order;
        std::map<std::tuple<std::int64_t, std::int64_t, std::int64_t>,
                 std::pair<std::int64_t, std::int64_t>>
            sums;  // count, total
        for (std::size_t i = 0; i < kRows; ++i) {
            const auto key = key_of(i);
            const auto qv = static_cast<std::int64_t>(i % 97);
            a.push_back(std::get<0>(key));
            b.push_back(std::get<1>(key));
            c.push_back(std::get<2>(key));
            q.push_back(qv);
            auto [it, inserted] = sums.emplace(key, std::pair<std::int64_t, std::int64_t>{0, 0});
            if (inserted) {
                order.push_back(key);
            }
            ++it->second.first;
            it->second.second += qv;
        }
        runtime::Table t;
        t.add_column("a", std::move(a));
        t.add_column("b", std::move(b));
        t.add_column("c", std::move(c));
        t.add_column("q", std::move(q));
        runtime::TableRegistry registry;
        registry.emplace("t", std::move(t));
        auto program = parser::parse("t[select { total = sum(q), n = count() }, by { a, b, c }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ParallelPipelineStats stats;
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        serial.parallel_stats = &stats;
        const auto before = runtime::single_core_partitioned_discoveries().load();
        const auto out =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, serial);
        INFO((out.has_value() ? std::string{} : out.error()));
        REQUIRE(out.has_value());
        // Partitioned or not, one core never fans out.
        CHECK(stats.parallel_aggregate_partitions.load() == 0);
        CHECK((runtime::single_core_partitioned_discoveries().load() > before) ==
              expect_partitioned);
        REQUIRE(out->rows() == order.size());
        const auto& oa = std::get<Column<std::int64_t>>(*out->find("a"));
        const auto& ob = std::get<Column<std::int64_t>>(*out->find("b"));
        const auto& oc = std::get<Column<std::int64_t>>(*out->find("c"));
        const auto& ot = std::get<Column<std::int64_t>>(*out->find("total"));
        const auto& on = std::get<Column<std::int64_t>>(*out->find("n"));
        for (std::size_t g = 0; g < order.size(); ++g) {
            INFO("group " << g);
            const auto& key = order[g];
            REQUIRE(oa[g] == std::get<0>(key));
            REQUIRE(ob[g] == std::get<1>(key));
            REQUIRE(oc[g] == std::get<2>(key));
            REQUIRE(on[g] == sums[key].first);
            REQUIRE(ot[g] == sums[key].second);
        }
    };
    SECTION("nearly every row a new group: partitioned") {
        run(
            [](std::size_t i) {
                const auto k = static_cast<std::int64_t>((i * 7919) % 250'000);
                return std::tuple{k % 1000, k / 1000, -k};
            },
            true);
    }
    SECTION("distinct but clustered, four rows a key: the serial loop's run shortcut") {
        run(
            [](std::size_t i) {
                const auto k = static_cast<std::int64_t>(i / 4);
                return std::tuple{k % 1000, k / 1000, -k};
            },
            false);
    }
    SECTION("210 groups: the one serial table") {
        run(
            [](std::size_t i) {
                return std::tuple{static_cast<std::int64_t>(i % 10),
                                  static_cast<std::int64_t>(i % 7),
                                  static_cast<std::int64_t>(i % 3)};
            },
            false);
    }
}

TEST_CASE("packed group-by keys: nullable key columns keep the packed path",
          "[runtime][chunked][aggregate][parallel]") {
    // h2o q10 on its 5%-NA data: every key column has a validity bitmap, which
    // used to send the whole key to the generic path. Each nullable column
    // now keeps a null cell in the packed key. A Categorical, a String and
    // two Int64 keys, each null on its own rows with a payload that is a real
    // value elsewhere -- so a packer that read the payload would merge a null
    // group into a value's. Checked against a grouping computed here, in
    // first-occurrence order, serially and fanned out, whole and in chunks.
    constexpr std::size_t kRows = 300'000;
    const std::vector<std::string> labels{"p", "q", "r", "s", "t"};
    Column<Categorical>::codes_storage codes;
    Column<std::string> s;
    Column<std::int64_t> a;
    Column<std::int64_t> b;
    Column<std::int64_t> q;
    runtime::ValidityBitmap c_valid;
    runtime::ValidityBitmap s_valid;
    runtime::ValidityBitmap a_valid;
    runtime::ValidityBitmap b_valid;
    using Cell = std::optional<std::int64_t>;
    using Key = std::tuple<Cell, std::optional<std::string>, Cell, Cell>;
    std::vector<Key> order;
    std::map<Key, std::pair<std::int64_t, std::int64_t>> sums;  // count, total
    for (std::size_t i = 0; i < kRows; ++i) {
        const std::size_t k = (i * 7919) % 60'001;
        const auto cv = static_cast<std::int64_t>(k % 5);
        const std::string sv = "s" + std::to_string(k % 101);
        const auto av = static_cast<std::int64_t>(k % 7) - 3;
        const auto bv = static_cast<std::int64_t>(k / 5) * 1'000;
        const bool c_ok = i % 19 != 3;
        const bool s_ok = i % 23 != 5;
        const bool a_ok = i % 29 != 7;
        const bool b_ok = i % 31 != 11;
        const auto qv = static_cast<std::int64_t>(i % 97);
        codes.push_back(static_cast<Column<Categorical>::code_type>(cv));
        s.push_back(sv);
        a.push_back(av);
        b.push_back(bv);
        q.push_back(qv);
        c_valid.push_back(c_ok);
        s_valid.push_back(s_ok);
        a_valid.push_back(a_ok);
        b_valid.push_back(b_ok);
        const Key key{c_ok ? Cell{cv} : std::nullopt,
                      s_ok ? std::optional<std::string>{sv} : std::nullopt,
                      a_ok ? Cell{av} : std::nullopt, b_ok ? Cell{bv} : std::nullopt};
        auto [it, inserted] = sums.emplace(key, std::pair<std::int64_t, std::int64_t>{0, 0});
        if (inserted) {
            order.push_back(key);
        }
        ++it->second.first;
        it->second.second += qv;
    }
    runtime::Table t;
    t.add_column("c", Column<Categorical>(labels, std::move(codes)));
    t.columns.back().validity = std::move(c_valid);
    t.add_column("s", std::move(s));
    t.columns.back().validity = std::move(s_valid);
    t.add_column("a", std::move(a));
    t.columns.back().validity = std::move(a_valid);
    t.add_column("b", std::move(b));
    t.columns.back().validity = std::move(b_valid);
    t.add_column("q", std::move(q));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    auto program = parser::parse("t[select { total = sum(q), n = count() }, by { c, s, a, b }];");
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());

    const auto check = [&](const runtime::Table& out) {
        REQUIRE(out.rows() == order.size());
        const auto* oc = out.find_entry("c");
        const auto* os = out.find_entry("s");
        const auto* oa = out.find_entry("a");
        const auto* ob = out.find_entry("b");
        REQUIRE(oc != nullptr);
        REQUIRE(os != nullptr);
        REQUIRE(oa != nullptr);
        REQUIRE(ob != nullptr);
        const auto& cc = std::get<Column<Categorical>>(*oc->column);
        const auto& sc = std::get<Column<std::string>>(*os->column);
        const auto& ac = std::get<Column<std::int64_t>>(*oa->column);
        const auto& bc = std::get<Column<std::int64_t>>(*ob->column);
        const auto& ot = std::get<Column<std::int64_t>>(*out.find("total"));
        const auto& on = std::get<Column<std::int64_t>>(*out.find("n"));
        for (std::size_t g = 0; g < order.size(); ++g) {
            INFO("group " << g);
            const auto& [kc, ks, ka, kb] = order[g];
            REQUIRE(runtime::is_null(*oc, g) == !kc.has_value());
            if (kc) {
                REQUIRE(std::string(cc[g]) == labels[static_cast<std::size_t>(*kc)]);
            }
            REQUIRE(runtime::is_null(*os, g) == !ks.has_value());
            if (ks) {
                REQUIRE(std::string(sc[g]) == *ks);
            }
            REQUIRE(runtime::is_null(*oa, g) == !ka.has_value());
            if (ka) {
                REQUIRE(ac[g] == *ka);
            }
            REQUIRE(runtime::is_null(*ob, g) == !kb.has_value());
            if (kb) {
                REQUIRE(bc[g] == *kb);
            }
            REQUIRE(on[g] == sums.at(order[g]).first);
            REQUIRE(ot[g] == sums.at(order[g]).second);
        }
    };

    for (const bool chunked : {false, true}) {
        INFO("chunked: " << chunked);
        std::optional<ChunkGrainGuard> guard;
        if (chunked) {
            guard.emplace("20000");
        }
        runtime::ExecutionContext serial;
        serial.parallel_threads = 1;
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto packed_before = runtime::packed_nullable_key_chunks().load();
        const auto sr =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, serial);
        const auto pr =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        INFO((sr.has_value() ? std::string{} : sr.error()));
        REQUIRE(sr.has_value());
        INFO((pr.has_value() ? std::string{} : pr.error()));
        REQUIRE(pr.has_value());
        // Guards the test: the nullable key really stayed on the packed path.
        CHECK(runtime::packed_nullable_key_chunks().load() > packed_before);
        check(*sr);
        check(*pr);
    }
}

TEST_CASE("chunked aggregate: a narrow-span Int64 key groups through a dense array",
          "[runtime][chunked][aggregate]") {
    // h2o q4's shape: one Int64 key over ~100 distinct values. Its span is a
    // small fraction of the rows, so `key - min` indexes a gid array and the
    // morsels fan out even over one chunk. The chunked runs move the bounds:
    // a later chunk reaches below the first chunk's minimum (rebuild, gids
    // unchanged), and in the `wide` run one chunk spans past the dense limit,
    // so the path retires and the hash paths must carry on with the groups it
    // made. Expected values are computed here, in first-seen order.
    constexpr std::int64_t kRows = 600'000;
    const auto key_of = [](std::int64_t row, bool wide) -> std::int64_t {
        if (wide && row >= 450'000 && row < 460'000) {
            return (row % 7) * 1'000'000'000;  // retires the dense path
        }
        const std::int64_t base = (row * 7919) % 97;
        return row < 300'000 ? base : base - 40;  // later rows reach below
    };
    for (const auto& [grain, wide] :
         {std::pair<const char*, bool>{"", false}, std::pair<const char*, bool>{"70001", false},
          std::pair<const char*, bool>{"70001", true}}) {
        INFO("grain '" << grain << "' wide " << wide);
        runtime::TableRegistry registry;
        std::vector<std::int64_t> want_keys;
        struct Want {
            std::int64_t n = 0;
            std::int64_t sum = 0;
            std::int64_t lo = 0;
            std::int64_t hi = 0;
            std::int64_t first = 0;
        };
        std::map<std::int64_t, Want> want;
        {
            Column<std::int64_t> g;
            Column<std::int64_t> v;
            for (std::int64_t row = 0; row < kRows; ++row) {
                const std::int64_t key = key_of(row, wide);
                const std::int64_t value = ((row * 104729) % 1'000'003) - 500'000;
                g.push_back(key);
                v.push_back(value);
                auto [it, fresh] = want.try_emplace(key);
                Want& w = it->second;
                if (fresh) {
                    want_keys.push_back(key);
                    w.lo = value;
                    w.hi = value;
                    w.first = value;
                }
                ++w.n;
                w.sum += value;
                w.lo = std::min(w.lo, value);
                w.hi = std::max(w.hi, value);
            }
            runtime::Table t;
            t.add_column("g", std::move(g));
            t.add_column("v", std::move(v));
            registry.emplace("t", std::move(t));
        }
        std::optional<ChunkGrainGuard> guard;
        if (*grain != '\0') {
            guard.emplace(grain);
        }
        auto program = parser::parse(
            "t[select { n = count(), s = sum(v), lo = min(v), hi = max(v), f = first(v) }, "
            "by { g }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(p.has_value());

        REQUIRE(p->rows() == want_keys.size());
        const auto& pg = std::get<Column<std::int64_t>>(*p->find("g"));
        const auto& pn = std::get<Column<std::int64_t>>(*p->find("n"));
        const auto& ps = std::get<Column<std::int64_t>>(*p->find("s"));
        const auto& plo = std::get<Column<std::int64_t>>(*p->find("lo"));
        const auto& phi = std::get<Column<std::int64_t>>(*p->find("hi"));
        const auto& pf = std::get<Column<std::int64_t>>(*p->find("f"));
        for (std::size_t i = 0; i < want_keys.size(); ++i) {
            INFO("group " << i);
            const Want& w = want.at(want_keys[i]);
            REQUIRE(pg[i] == want_keys[i]);
            REQUIRE(pn[i] == w.n);
            REQUIRE(ps[i] == w.sum);
            REQUIRE(plo[i] == w.lo);
            REQUIRE(phi[i] == w.hi);
            REQUIRE(pf[i] == w.first);
        }
    }
}

TEST_CASE("median by integer and Categorical keys: parallel cell grouping matches a reference",
          "[runtime][aggregate][median]") {
    // h2o q6: `median(v), std(v) by { id4, id5 }`. Median materializes and
    // groups in `aggregate_table`, which numbers groups in parallel when every
    // key is an integer or a code and their Cartesian span is small. Keys here
    // straddle zero, mix Int64, Date and Categorical, and one shape spans too
    // wide for the cells so the per-row index must give the same answer.
    // Expected values are computed here, in first-seen order.
    constexpr std::size_t kRows = 200'000;
    const std::vector<std::string> labels{"x", "y", "z"};
    Column<std::int64_t> a;
    Column<Date> d;
    Column<std::int64_t> wide;
    std::vector<std::string> c_values;
    Column<double> v;
    for (std::size_t row = 0; row < kRows; ++row) {
        a.push_back(static_cast<std::int64_t>((row * 7919) % 23) - 11);
        d.push_back(Date{static_cast<std::int32_t>(19'000 + ((row * 104'729) % 17))});
        wide.push_back(static_cast<std::int64_t>((row * 31) % 5) * 1'000'000'007);
        c_values.push_back(labels[(row * 13) % labels.size()]);
        v.push_back(static_cast<double>((row * 2'654'435'761ULL) % 100'003) / 7.0);
    }
    runtime::Table t;
    t.add_column("a", a);
    t.add_column("d", d);
    t.add_column("w", wide);
    {
        Column<Categorical>::codes_storage codes;
        for (const auto& value : c_values) {
            codes.push_back(static_cast<Column<Categorical>::code_type>(
                std::ranges::find(labels, value) - labels.begin()));
        }
        t.add_column("c", Column<Categorical>(labels, std::move(codes)));
    }
    t.add_column("v", v);
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    // Group keys, as one string per row for the reference.
    const auto key_text = [&](std::string_view by, std::size_t row) {
        std::string out;
        for (const char k : by) {
            switch (k) {
                case 'a':
                    out += std::to_string(a[row]) + "|";
                    break;
                case 'd':
                    out += std::to_string(d[row].days) + "|";
                    break;
                case 'w':
                    out += std::to_string(wide[row]) + "|";
                    break;
                default:
                    out += c_values[row] + "|";
                    break;
            }
        }
        return out;
    };
    struct Case {
        std::string by;     // key letters, in `by` order
        std::string query;  // aggregates
        bool with_std;
    };
    for (const Case& test :
         {Case{"ad", "m = median(v), s = std(v)", true}, Case{"a", "m = median(v)", false},
          Case{"ca", "m = median(v), s = std(v)", true},
          Case{"w", "m = median(v), s = std(v)", true}}) {
        INFO("by " << test.by << ": " << test.query);
        std::vector<std::string> order;
        std::map<std::string, std::vector<double>> groups;
        for (std::size_t row = 0; row < kRows; ++row) {
            auto key = key_text(test.by, row);
            auto [it, fresh] = groups.try_emplace(key);
            if (fresh) {
                order.push_back(key);
            }
            it->second.push_back(v[row]);
        }
        std::string by_list;
        for (const char k : test.by) {
            by_list += std::string(by_list.empty() ? "" : ", ") + k;
        }
        auto program = parser::parse("t[select { " + test.query + " }, by { " + by_list + " }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(p.has_value());
        REQUIRE(p->rows() == order.size());
        const auto& pm = std::get<Column<double>>(*p->find("m"));
        for (std::size_t g = 0; g < order.size(); ++g) {
            INFO("group " << g << " key " << order[g]);
            // First-seen order, checked through the keys' text.
            std::string got;
            for (const char k : test.by) {
                const auto& col = *p->find(std::string(1, k));
                if (const auto* ints = std::get_if<Column<std::int64_t>>(&col)) {
                    got += std::to_string((*ints)[g]) + "|";
                } else if (const auto* dates = std::get_if<Column<Date>>(&col)) {
                    got += std::to_string((*dates)[g].days) + "|";
                } else {
                    const auto& cat = std::get<Column<Categorical>>(col);
                    got += std::string(cat[g]) + "|";
                }
            }
            REQUIRE(got == order[g]);
            auto values = groups.at(order[g]);
            std::ranges::sort(values);
            const std::size_t n = values.size();
            const double median =
                n % 2 == 1 ? values[n / 2] : (values[(n / 2) - 1] + values[n / 2]) / 2.0;
            REQUIRE(pm[g] == median);
            if (test.with_std) {
                double mean = 0.0;
                for (const double x : values) {
                    mean += x;
                }
                mean /= static_cast<double>(n);
                double m2 = 0.0;
                for (const double x : values) {
                    m2 += (x - mean) * (x - mean);
                }
                const double sd = std::sqrt(m2 / static_cast<double>(n - 1));
                const auto& ps = std::get<Column<double>>(*p->find("s"));
                REQUIRE(std::abs(ps[g] - sd) <= 1e-9 * sd);
            }
        }
    }
}

TEST_CASE("grouped top-k: partitioned flat heaps take each group's stable-sort rows",
          "[runtime][chunked][topk]") {
    // h2o q8: `order { v desc }, head 2, by g` over many groups. The groups
    // are hash partitioned across workers and keep flat (key, sequence) heaps;
    // only rows still in a heap are copied out, into pieces that are compacted
    // once mostly displaced. Expected rows come from a stable sort per group,
    // computed here: ties go to the earlier row for head and to the later row
    // for tail, and the output is every winner in (key, arrival) order.
    struct Case {
        std::string query;
        bool desc;
        bool tail;
        std::size_t k;
        bool rising;  // every row beats the last: the store must compact
        std::string grain;
        std::int64_t groups = 50'021;  // past rows / 2, a chunk is kept whole
    };
    for (const Case& test :
         {Case{"t[order { v desc }, head 2, by g]", true, false, 2, false, ""},
          Case{"t[order { v desc }, head 2, by g]", true, false, 2, false, "70001"},
          Case{"t[order { v }, head 3, by g]", false, false, 3, false, "70001"},
          Case{"t[order { v }, tail 2, by g]", false, true, 2, false, "70001"},
          Case{"t[order { v desc }, head 1, by g]", true, false, 1, true, "300000"},
          Case{"t[order { v desc }, head 2, by g]", true, false, 2, false, "70001", 300'007}}) {
        INFO(test.query << " grain '" << test.grain << "' rising " << test.rising);
        const std::size_t rows = test.rising ? 1'500'000 : 400'000;
        Column<std::int64_t> g;
        Column<double> v;
        Column<std::int64_t> id;
        runtime::ValidityBitmap g_valid;
        g_valid.assign(rows, true);
        for (std::size_t row = 0; row < rows; ++row) {
            const auto key = static_cast<std::int64_t>(row * 7919) % test.groups;
            g.push_back(key);
            if (!test.rising && row % 997 == 0) {
                g_valid.set(row, false);  // one null group
            }
            // Few distinct values, so ties decide most winners.
            // Rising, until a last chunk that displaces nothing: the winners
            // are then read through the refs compaction rewrote.
            const double rising = row < 1'200'000 ? static_cast<double>(row) : -1.0;
            v.push_back(test.rising ? rising : static_cast<double>((row * 104'729) % 13) - 6.0);
            id.push_back(static_cast<std::int64_t>(row));
        }
        // Reference: rows per group, stably sorted, then the head or tail.
        std::map<std::int64_t, std::vector<std::size_t>> groups;  // null -> min
        constexpr std::int64_t kNullGroup = std::numeric_limits<std::int64_t>::min();
        for (std::size_t row = 0; row < rows; ++row) {
            groups[g_valid[row] ? g[row] : kNullGroup].push_back(row);
        }
        const auto before = [&](std::size_t a, std::size_t b) {
            return test.desc ? v[a] > v[b] : v[a] < v[b];
        };
        std::vector<std::size_t> want;
        for (auto& [key, members] : groups) {
            std::ranges::stable_sort(members, before);
            const std::size_t take = std::min(test.k, members.size());
            if (test.tail) {
                want.insert(want.end(), members.end() - static_cast<std::ptrdiff_t>(take),
                            members.end());
            } else {
                want.insert(want.end(), members.begin(),
                            members.begin() + static_cast<std::ptrdiff_t>(take));
            }
        }
        std::ranges::sort(want, [&](std::size_t a, std::size_t b) {
            if (v[a] != v[b]) {
                return before(a, b);
            }
            return a < b;
        });

        runtime::Table t;
        t.add_column("g", std::move(g), std::move(g_valid));
        t.add_column("v", std::move(v));
        t.add_column("id", std::move(id));
        runtime::TableRegistry registry;
        registry.emplace("t", std::move(t));
        std::optional<ChunkGrainGuard> guard;
        if (!test.grain.empty()) {
            guard.emplace(test.grain);
        }
        auto program = parser::parse(test.query + ";");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(p.has_value());
        REQUIRE(p->rows() == want.size());
        const auto& pid = std::get<Column<std::int64_t>>(*p->find("id"));
        for (std::size_t i = 0; i < want.size(); ++i) {
            INFO("output row " << i);
            REQUIRE(static_cast<std::size_t>(pid[i]) == want[i]);
        }
    }
}

TEST_CASE("chunked aggregate: a nullable narrow-span Int64 key keeps the dense array",
          "[runtime][chunked][aggregate][validity]") {
    // h2o q4 with NAs: `by id4` where id4 has nulls. The null key is one more
    // dense cell, so the path no longer migrates to the generic one. Row 0 is
    // null (the null group is gid 0); the payload under a null is far outside
    // the span, so a null read as a value would index past the array. The
    // chunked runs widen the bounds below the first chunk (rebuild, the null
    // cell moves; a later chunk reaches above, so the old null cell becomes a
    // value cell), and in the `wide` run the path retires with a null group
    // already made, so the generic path must adopt it. Expected values are
    // computed here, in first-seen order; the null group is key nullopt.
    constexpr std::int64_t kRows = 600'000;
    const auto is_null_row = [](std::int64_t row) { return row % 13 == 0; };
    const auto key_of = [](std::int64_t row, bool wide) -> std::int64_t {
        if (wide && row >= 450'000 && row < 460'000) {
            return (row % 7) * 1'000'000'000;
        }
        const std::int64_t base = (row * 7919) % 97;
        if (row < 300'000) {
            return base;
        }
        return row < 400'000 ? base - 40 : base + 50;
    };
    for (const auto& [grain, wide] :
         {std::pair<const char*, bool>{"", false}, std::pair<const char*, bool>{"70001", false},
          std::pair<const char*, bool>{"70001", true}}) {
        INFO("grain '" << grain << "' wide " << wide);
        runtime::TableRegistry registry;
        struct Want {
            std::int64_t n = 0;
            std::int64_t sum = 0;
        };
        std::vector<std::optional<std::int64_t>> want_keys;
        std::map<std::optional<std::int64_t>, Want> want;
        {
            Column<std::int64_t> g;
            Column<std::int64_t> v;
            runtime::ValidityBitmap valid;
            for (std::int64_t row = 0; row < kRows; ++row) {
                const bool null = is_null_row(row);
                const std::int64_t value = ((row * 104729) % 1'000'003) - 500'000;
                g.push_back(null ? 5'000'000'000'000 + row : key_of(row, wide));
                valid.push_back(!null);
                v.push_back(value);
                const std::optional<std::int64_t> key =
                    null ? std::nullopt : std::optional{key_of(row, wide)};
                auto [it, fresh] = want.try_emplace(key);
                if (fresh) {
                    want_keys.push_back(key);
                }
                ++it->second.n;
                it->second.sum += value;
            }
            runtime::Table t;
            t.add_column("g", std::move(g), std::move(valid));
            t.add_column("v", std::move(v));
            registry.emplace("t", std::move(t));
        }
        std::optional<ChunkGrainGuard> guard;
        if (*grain != '\0') {
            guard.emplace(grain);
        }
        auto program = parser::parse("t[select { n = count(), s = sum(v) }, by g];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(p.has_value());

        REQUIRE(p->rows() == want_keys.size());
        const auto* g_entry = p->find_entry("g");
        REQUIRE(g_entry != nullptr);
        const auto& pg = std::get<Column<std::int64_t>>(*g_entry->column);
        const auto& pn = std::get<Column<std::int64_t>>(*p->find("n"));
        const auto& ps = std::get<Column<std::int64_t>>(*p->find("s"));
        for (std::size_t i = 0; i < want_keys.size(); ++i) {
            INFO("group " << i);
            const bool null = runtime::is_null(*g_entry, i);
            REQUIRE(null == !want_keys[i].has_value());
            if (!null) {
                REQUIRE(pg[i] == *want_keys[i]);
            }
            const Want& w = want.at(want_keys[i]);
            REQUIRE(pn[i] == w.n);
            REQUIRE(ps[i] == w.sum);
        }
    }
}

TEST_CASE("median by nullable integer and Categorical keys: parallel cell grouping keeps nulls",
          "[runtime][aggregate][median][validity]") {
    // h2o q6 with NAs: the keys of `median(v) by { id4, id5 }` have nulls. A
    // nullable key's null takes one offset past its span, so the parallel cell
    // grouping still runs. The payload under an Int64 null is far outside the
    // span, so a null read as a value would index past the cells; `z` is null
    // on every row. Expected values are computed here, in first-seen order.
    constexpr std::size_t kRows = 200'000;
    const std::vector<std::string> labels{"x", "y", "z"};
    Column<std::int64_t> a;
    runtime::ValidityBitmap a_valid;
    Column<Date> d;
    Column<std::int64_t> z;
    runtime::ValidityBitmap z_valid;
    Column<Categorical>::codes_storage c_codes;
    runtime::ValidityBitmap c_valid;
    Column<double> v;
    const auto a_null = [](std::size_t row) { return row % 11 == 0; };
    const auto c_null = [](std::size_t row) { return row % 7 == 3; };
    for (std::size_t row = 0; row < kRows; ++row) {
        a.push_back(a_null(row) ? 9'000'000'000'000 + static_cast<std::int64_t>(row)
                                : static_cast<std::int64_t>((row * 7919) % 23) - 11);
        a_valid.push_back(!a_null(row));
        d.push_back(Date{static_cast<std::int32_t>(19'000 + ((row * 104'729) % 17))});
        z.push_back(static_cast<std::int64_t>(row));
        z_valid.push_back(false);
        c_codes.push_back(static_cast<Column<Categorical>::code_type>((row * 13) % labels.size()));
        c_valid.push_back(!c_null(row));
        v.push_back(static_cast<double>((row * 2'654'435'761ULL) % 100'003) / 7.0);
    }
    runtime::Table t;
    t.add_column("a", a, a_valid);
    t.add_column("d", d);
    t.add_column("z", z, z_valid);
    t.add_column("c", Column<Categorical>(labels, std::move(c_codes)), c_valid);
    t.add_column("v", v);
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    const auto key_text = [&](std::string_view by, std::size_t row) {
        std::string out;
        for (const char k : by) {
            switch (k) {
                case 'a':
                    out += a_null(row) ? "null|" : std::to_string(a[row]) + "|";
                    break;
                case 'd':
                    out += std::to_string(d[row].days) + "|";
                    break;
                case 'z':
                    out += "null|";
                    break;
                default:
                    out += c_null(row) ? "null|" : labels[(row * 13) % labels.size()] + "|";
                    break;
            }
        }
        return out;
    };
    for (const std::string by : {"a", "ad", "ca", "za"}) {
        INFO("by " << by);
        std::vector<std::string> order;
        std::map<std::string, std::vector<double>> groups;
        for (std::size_t row = 0; row < kRows; ++row) {
            auto key = key_text(by, row);
            auto [it, fresh] = groups.try_emplace(key);
            if (fresh) {
                order.push_back(key);
            }
            it->second.push_back(v[row]);
        }
        std::string by_list;
        for (const char k : by) {
            by_list += std::string(by_list.empty() ? "" : ", ") + k;
        }
        auto program =
            parser::parse("t[select { m = median(v), n = count() }, by { " + by_list + " }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        REQUIRE(p.has_value());
        REQUIRE(p->rows() == order.size());
        const auto& pm = std::get<Column<double>>(*p->find("m"));
        const auto& pn = std::get<Column<std::int64_t>>(*p->find("n"));
        for (std::size_t g = 0; g < order.size(); ++g) {
            INFO("group " << g << " key " << order[g]);
            std::string got;
            for (const char k : by) {
                const auto* entry = p->find_entry(std::string(1, k));
                REQUIRE(entry != nullptr);
                if (runtime::is_null(*entry, g)) {
                    got += "null|";
                } else if (const auto* ints = std::get_if<Column<std::int64_t>>(&*entry->column)) {
                    got += std::to_string((*ints)[g]) + "|";
                } else if (const auto* dates = std::get_if<Column<Date>>(&*entry->column)) {
                    got += std::to_string((*dates)[g].days) + "|";
                } else {
                    got += std::string(std::get<Column<Categorical>>(*entry->column)[g]) + "|";
                }
            }
            REQUIRE(got == order[g]);
            auto values = groups.at(order[g]);
            std::ranges::sort(values);
            const std::size_t n = values.size();
            REQUIRE(static_cast<std::size_t>(pn[g]) == n);
            const double median =
                n % 2 == 1 ? values[n / 2] : (values[(n / 2) - 1] + values[n / 2]) / 2.0;
            REQUIRE(pm[g] == median);
        }
    }
}

TEST_CASE("chunked aggregate: owned gid ranges accumulate many groups exactly as serially",
          "[runtime][chunked][aggregate]") {
    // h2o q5 at K=1e1: ~1e6 groups, so no per-morsel partial fits and the
    // rows were accumulated serially. Owned gid ranges run them across
    // workers, each group's rows still in row order on one thread, so the
    // answer must equal the serial one bit for bit -- the Double sums, First
    // and Last included. A null-bearing value column rides along. The serial
    // answer is the same query with the path switched off; a hand-computed
    // reference pins the Int64 sums and counts too.
    constexpr std::int64_t kRows = (std::int64_t{1} << 20) + 12'345;
    constexpr std::int64_t kGroups = 200'003;
    runtime::TableRegistry registry;
    std::map<std::int64_t, std::pair<std::int64_t, std::int64_t>> want;  // key -> (n, sum)
    {
        Column<std::int64_t> g;
        Column<std::int64_t> iv;
        Column<double> dv;
        runtime::ValidityBitmap dv_valid;
        for (std::int64_t row = 0; row < kRows; ++row) {
            const std::int64_t key = (row * 7919) % kGroups;
            const std::int64_t value = ((row * 104'729) % 1'000'003) - 500'000;
            g.push_back(key);
            iv.push_back(value);
            dv.push_back(static_cast<double>(value) / 7.0);
            dv_valid.push_back(row % 5 != 0);
            auto& [n, sum] = want[key];
            ++n;
            sum += value;
        }
        runtime::Table t;
        t.add_column("g", std::move(g));
        t.add_column("iv", std::move(iv));
        t.add_column("dv", std::move(dv), std::move(dv_valid));
        registry.emplace("t", std::move(t));
    }
    auto program = parser::parse(
        "t[select { n = count(), s = sum(iv), ds = sum(dv), m = mean(dv), lo = min(dv), "
        "hi = max(iv), f = first(dv), l = last(iv), sd = std(dv) }, by g];");
    REQUIRE(program.has_value());
    auto ir = parser::lower(program.value());
    REQUIRE(ir.has_value());
    const auto run = [&](bool owned) {
        if (!owned) {
            runtime::set_env("IBEX_DISABLE_OWNED_RANGE_ACCUMULATION", "1");
        }
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        auto out = runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
        runtime::unset_env("IBEX_DISABLE_OWNED_RANGE_ACCUMULATION");
        return out;
    };
    const auto serial = run(false);
    const auto owned = run(true);
    REQUIRE(serial.has_value());
    REQUIRE(owned.has_value());
    const auto mismatch = runtime::compare_tables(*serial, *owned);
    INFO((mismatch.has_value() ? mismatch->message() : std::string{}));
    REQUIRE_FALSE(mismatch.has_value());

    REQUIRE(owned->rows() == want.size());
    const auto& pg = std::get<Column<std::int64_t>>(*owned->find("g"));
    const auto& pn = std::get<Column<std::int64_t>>(*owned->find("n"));
    const auto& ps = std::get<Column<std::int64_t>>(*owned->find("s"));
    for (std::size_t i = 0; i < owned->rows(); ++i) {
        const auto& [n, sum] = want.at(pg[i]);
        INFO("group " << i << " key " << pg[i]);
        REQUIRE(pn[i] == n);
        REQUIRE(ps[i] == sum);
    }
}

TEST_CASE("chunked aggregate: parallel dense gid numbering keeps first-seen order",
          "[runtime][chunked][aggregate]") {
    // h2o q5/q3 at K=1e1: ~1e6 int ids or Categorical codes, too many cells
    // for the fused partials, so groups were numbered by a serial probe. They
    // are now numbered across workers; the groups must still come out in
    // first-seen order, which is checked against a reference here, and equal
    // the serial numbering, for an Int64 key with and without nulls (the null
    // cell) and a Categorical key.
    constexpr std::size_t kRows = (std::size_t{1} << 20) + 4'321;
    constexpr std::int64_t kGroups = 150'001;
    std::vector<std::string> labels;
    labels.reserve(static_cast<std::size_t>(kGroups));
    for (std::int64_t i = 0; i < kGroups; ++i) {
        labels.push_back("k" + std::to_string(i));
    }
    Column<std::int64_t> g;
    runtime::ValidityBitmap g_valid;
    Column<Categorical>::codes_storage codes;
    Column<std::int64_t> v;
    std::vector<std::int64_t> key_of_row(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        const auto key = static_cast<std::int64_t>((row * 2'654'435'761ULL) % kGroups);
        key_of_row[row] = key;
        g.push_back(key);
        g_valid.push_back(row % 17 != 5);
        codes.push_back(static_cast<Column<Categorical>::code_type>(key));
        v.push_back(static_cast<std::int64_t>(row % 1'000));
    }
    runtime::Table t;
    t.add_column("g", g);
    t.add_column("gn", g, g_valid);
    t.add_column("c", Column<Categorical>(labels, std::move(codes)));
    t.add_column("v", std::move(v));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    for (const std::string key : {"g", "gn", "c"}) {
        INFO("by " << key);
        // First-seen order of the keys; -1 stands for the null group.
        std::vector<std::int64_t> order;
        {
            std::vector<std::uint8_t> seen(static_cast<std::size_t>(kGroups) + 1, 0);
            for (std::size_t row = 0; row < kRows; ++row) {
                const bool null = key == "gn" && row % 17 == 5;
                const std::size_t slot = null ? static_cast<std::size_t>(kGroups)
                                              : static_cast<std::size_t>(key_of_row[row]);
                if (seen[slot] == 0U) {
                    seen[slot] = 1U;
                    order.push_back(null ? -1 : key_of_row[row]);
                }
            }
        }
        auto program = parser::parse("t[select { n = count(), s = sum(v) }, by " + key + "];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        const auto run = [&](bool parallel_gids) {
            if (!parallel_gids) {
                runtime::set_env("IBEX_DISABLE_PARALLEL_DENSE_GIDS", "1");
            }
            runtime::ExecutionContext parallel;
            parallel.parallel_threads = 8;
            parallel.parallel_min_rows = 0;
            auto out =
                runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
            runtime::unset_env("IBEX_DISABLE_PARALLEL_DENSE_GIDS");
            return out;
        };
        const auto serial = run(false);
        const auto fanned = run(true);
        REQUIRE(serial.has_value());
        REQUIRE(fanned.has_value());
        const auto mismatch = runtime::compare_tables(*serial, *fanned);
        INFO((mismatch.has_value() ? mismatch->message() : std::string{}));
        REQUIRE_FALSE(mismatch.has_value());

        REQUIRE(fanned->rows() == order.size());
        const auto* entry = fanned->find_entry(key);
        REQUIRE(entry != nullptr);
        for (std::size_t i = 0; i < order.size(); ++i) {
            INFO("group " << i);
            if (order[i] < 0) {
                REQUIRE(runtime::is_null(*entry, i));
            } else if (key == "c") {
                REQUIRE(std::string(std::get<Column<Categorical>>(*entry->column)[i]) ==
                        labels[static_cast<std::size_t>(order[i])]);
            } else {
                REQUIRE_FALSE(runtime::is_null(*entry, i));
                REQUIRE(std::get<Column<std::int64_t>>(*entry->column)[i] == order[i]);
            }
        }
    }
}

TEST_CASE("chunked aggregate: many string groups come out in first-seen order, bytes intact",
          "[runtime][chunked][aggregate]") {
    // h2o q3 at K=2e0: ~5M string groups. Partitioned discovery now numbers
    // groups by scanning rows rather than merging partitions, writes keys
    // back in parallel, and Emission lays the key column out from per-block
    // lengths so spans copy bytes in parallel. Keys vary in length (one is
    // empty) so a wrong offset shows up as a wrong key. Expected values are
    // computed here, in first-seen order, over one chunk and several.
    constexpr std::size_t kRows = 700'001;
    constexpr std::size_t kGroups = 300'007;
    const auto key_of = [](std::size_t row) -> std::string {
        const std::size_t id = (row * 2'654'435'761ULL) % kGroups;
        if (id == 17) {
            return "";
        }
        return "k" + std::to_string(id) + std::string(id % 23, 'x');
    };
    runtime::TableRegistry registry;
    std::vector<std::string> order;
    std::map<std::string, std::pair<std::int64_t, std::int64_t>> want;  // key -> (n, sum)
    {
        std::vector<std::string> keys(kRows);
        Column<std::int64_t> v;
        for (std::size_t row = 0; row < kRows; ++row) {
            keys[row] = key_of(row);
            const auto value = static_cast<std::int64_t>(row % 1'009);
            v.push_back(value);
            auto [it, fresh] = want.try_emplace(keys[row]);
            if (fresh) {
                order.push_back(keys[row]);
            }
            ++it->second.first;
            it->second.second += value;
        }
        runtime::Table t;
        t.add_column("k", Column<std::string>{keys});
        t.add_column("v", std::move(v));
        registry.emplace("t", std::move(t));
    }
    // `k2` is built per chunk, so each chunk's key bytes are freed once the
    // aggregate has read them: a group key that kept a view of its row
    // instead of its own copy reads another chunk's bytes.
    const std::vector<std::pair<std::string, std::string>> queries{
        {"t[select { n = count(), s = sum(v) }, by k];", "k"},
        {"t[update { k2 = `${k}` }][select { n = count(), s = sum(v) }, by k2];", "k2"}};
    for (const auto& [query, key_name] : queries) {
        for (const char* grain : {"", "100003"}) {
            INFO("grain '" << grain << "' query " << query);
            std::optional<ChunkGrainGuard> guard;
            if (*grain != '\0') {
                guard.emplace(grain);
            }
            auto program = parser::parse(query);
            REQUIRE(program.has_value());
            auto ir = parser::lower(program.value());
            REQUIRE(ir.has_value());
            runtime::ExecutionContext parallel;
            parallel.parallel_threads = 8;
            parallel.parallel_min_rows = 0;
            const auto p =
                runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
            REQUIRE(p.has_value());
            REQUIRE(p->rows() == order.size());
            const auto& pk = std::get<Column<std::string>>(*p->find(key_name));
            const auto& pn = std::get<Column<std::int64_t>>(*p->find("n"));
            const auto& ps = std::get<Column<std::int64_t>>(*p->find("s"));
            for (std::size_t i = 0; i < order.size(); ++i) {
                INFO("group " << i);
                REQUIRE(pk[i] == order[i]);
                const auto& [n, sum] = want.at(order[i]);
                REQUIRE(pn[i] == n);
                REQUIRE(ps[i] == sum);
            }
        }
    }
}

TEST_CASE("chunked aggregate: six-column keys at ~1 group per row match a reference",
          "[runtime][chunked][aggregate]") {
    // h2o q10: `by { id1, ..., id6 }` with nearly every row its own group.
    // The packed key (Categoricals and Int64s) now travels with its row
    // through the scatter; the generic key (a String among them, and a
    // nullable Int64) prefetches its columns; both fill their new groups'
    // keys by row range, Categorical text and codes included. Expected values
    // are computed here, in first-seen order, over one chunk and several.
    constexpr std::size_t kRows = 300'007;
    const std::vector<std::string> labels{"a", "bb", "ccc", "dddd", "eeeee"};
    Column<Categorical>::codes_storage c1;
    Column<Categorical>::codes_storage c2;
    std::vector<std::string> s3(kRows);
    runtime::ValidityBitmap s3_valid;
    Column<std::int64_t> i4;
    Column<std::int64_t> i5;
    runtime::ValidityBitmap i5_valid;
    Column<std::int64_t> i6;
    Column<std::int64_t> v;
    std::vector<std::array<std::int64_t, 6>> raw(kRows);
    const auto s3_null = [&](std::size_t row) { return raw[row][2] % 13 == 0; };
    for (std::size_t row = 0; row < kRows; ++row) {
        const std::uint64_t h = (row * 2'654'435'761ULL) % 250'013;
        raw[row] = {static_cast<std::int64_t>(h % 5),
                    static_cast<std::int64_t>((h / 5) % 5),
                    static_cast<std::int64_t>((h / 25) % 97),
                    static_cast<std::int64_t>((h / 2425) % 11) - 5,
                    static_cast<std::int64_t>(h % 7),
                    static_cast<std::int64_t>(h % 3)};
        c1.push_back(static_cast<Column<Categorical>::code_type>(raw[row][0]));
        c2.push_back(static_cast<Column<Categorical>::code_type>(raw[row][1]));
        s3[row] = "s" + std::to_string(raw[row][2]) +
                  std::string(static_cast<std::size_t>(raw[row][2] % 9), 'z');
        s3_valid.push_back(!s3_null(row));
        i4.push_back(raw[row][3]);
        i5.push_back(raw[row][4]);
        i5_valid.push_back(raw[row][4] != 6);  // ~1/7 null
        i6.push_back(raw[row][5]);
        v.push_back(static_cast<std::int64_t>(row % 1'013));
    }
    runtime::Table t;
    t.add_column("c1", Column<Categorical>(labels, std::move(c1)));
    t.add_column("c2", Column<Categorical>(labels, std::move(c2)));
    t.add_column("s3", Column<std::string>{s3}, std::move(s3_valid));
    t.add_column("i4", std::move(i4));
    t.add_column("i5", std::move(i5), std::move(i5_valid));
    t.add_column("i6", std::move(i6));
    t.add_column("v", std::move(v));
    runtime::TableRegistry registry;
    registry.emplace("t", std::move(t));

    // packed: c1, c2, i4, i6 (+ i5 when null-free is not required: the
    // generic case covers the nullable one); generic: all six.
    struct Shape {
        std::string by;
        std::vector<int> cols;  // indices into raw
    };
    for (const Shape& shape : {Shape{"c1, c2, i4, i6", {0, 1, 3, 5}},
                               Shape{"c1, c2, s3, i4, i5, i6", {0, 1, 2, 3, 4, 5}}}) {
        std::vector<std::string> order;
        std::map<std::string, std::pair<std::int64_t, std::int64_t>> want;
        for (std::size_t row = 0; row < kRows; ++row) {
            std::string key;
            for (const int c : shape.cols) {
                if (c == 4 && raw[row][4] == 6) {
                    key += "null|";
                } else if (c == 0 || c == 1) {
                    key += labels[static_cast<std::size_t>(raw[row][static_cast<std::size_t>(c)])] +
                           "|";
                } else if (c == 2) {
                    key += s3_null(row) ? std::string("null|") : s3[row] + "|";
                } else {
                    key += std::to_string(raw[row][static_cast<std::size_t>(c)]) + "|";
                }
            }
            auto [it, fresh] = want.try_emplace(key);
            if (fresh) {
                order.push_back(key);
            }
            ++it->second.first;
            it->second.second += static_cast<std::int64_t>(row % 1'013);
        }
        for (const char* grain : {"", "70001"}) {
            INFO("by " << shape.by << " grain '" << grain << "'");
            std::optional<ChunkGrainGuard> guard;
            if (*grain != '\0') {
                guard.emplace(grain);
            }
            auto program =
                parser::parse("t[select { n = count(), s = sum(v) }, by { " + shape.by + " }];");
            REQUIRE(program.has_value());
            auto ir = parser::lower(program.value());
            REQUIRE(ir.has_value());
            runtime::ExecutionContext parallel;
            parallel.parallel_threads = 8;
            parallel.parallel_min_rows = 0;
            const auto p =
                runtime::interpret(*ir.value(), registry, nullptr, nullptr, nullptr, parallel);
            REQUIRE(p.has_value());
            REQUIRE(p->rows() == order.size());
            const auto& pn = std::get<Column<std::int64_t>>(*p->find("n"));
            const auto& ps = std::get<Column<std::int64_t>>(*p->find("s"));
            const std::vector<std::string> names{"c1", "c2", "s3", "i4", "i5", "i6"};
            for (std::size_t g = 0; g < order.size(); ++g) {
                INFO("group " << g);
                std::string got;
                for (const int c : shape.cols) {
                    const auto* entry = p->find_entry(names[static_cast<std::size_t>(c)]);
                    REQUIRE(entry != nullptr);
                    if (runtime::is_null(*entry, g)) {
                        got += "null|";
                    } else if (const auto* cat =
                                   std::get_if<Column<Categorical>>(&*entry->column)) {
                        got += std::string((*cat)[g]) + "|";
                    } else if (const auto* str =
                                   std::get_if<Column<std::string>>(&*entry->column)) {
                        got += std::string((*str)[g]) + "|";
                    } else {
                        got +=
                            std::to_string(std::get<Column<std::int64_t>>(*entry->column)[g]) + "|";
                    }
                }
                REQUIRE(got == order[g]);
                const auto& [n, sum] = want.at(order[g]);
                REQUIRE(pn[g] == n);
                REQUIRE(ps[g] == sum);
            }
        }
    }
}

TEST_CASE("chunked aggregate: packed keys keep Categorical columns as codes across chunks",
          "[runtime][chunked][aggregate]") {
    // The packed path's key store holds a Categorical key as codes alone,
    // reading its text through the dictionary. A later chunk over the same
    // dictionary extends the codes; one over another dictionary (reordered,
    // with a new label) has the text stored first; one whose key turns
    // nullable migrates the aggregate to the generic path, which then hashes
    // and compares the stored keys by text. Expected values are computed
    // here, in first-seen order.
    constexpr std::size_t kRows = 40'000;  // over the partitioned path's row gate
    enum class Later : std::uint8_t { SameDictionary, OtherDictionary, Nulls };

    for (const Later later : {Later::SameDictionary, Later::OtherDictionary, Later::Nulls}) {
        INFO("later chunk " << static_cast<int>(later));
        // Fresh per run, each with an index that matches it: a dictionary is
        // shared append-only, so the output may grow it (here with `t`).
        const auto dictionary = [](std::vector<std::string> labels) {
            auto index = std::make_shared<Column<Categorical>::index_map>();
            for (std::size_t i = 0; i < labels.size(); ++i) {
                index->emplace(labels[i], static_cast<Column<Categorical>::code_type>(i));
            }
            return std::pair{std::make_shared<std::vector<std::string>>(std::move(labels)),
                             std::move(index)};
        };
        const auto [dict_a, index_a] = dictionary({"p", "q", "r", "s"});
        const auto [dict_a2, index_a2] = dictionary({"s", "t", "q", "p", "r"});
        const auto [dict_b, index_b] = dictionary({"x", "y", "z"});
        std::vector<std::string> order;
        std::map<std::string, std::pair<std::int64_t, std::int64_t>> want;
        std::vector<runtime::Chunk> chunks;
        for (std::size_t k = 0; k < 2; ++k) {
            const bool other = k == 1 && later == Later::OtherDictionary;
            const auto& a_dict = other ? dict_a2 : dict_a;
            const bool nulls = k == 1 && later == Later::Nulls;
            Column<Categorical> a{a_dict, other ? index_a2 : index_a};
            Column<Categorical> b{dict_b, index_b};
            Column<std::int64_t> c;
            Column<std::int64_t> v;
            runtime::ValidityBitmap a_valid;
            for (std::size_t i = 0; i < kRows; ++i) {
                const auto a_code =
                    static_cast<Column<Categorical>::code_type>(((i * 7) + k) % a_dict->size());
                const auto b_code = static_cast<Column<Categorical>::code_type>((i / 3) % 3);
                const auto c_value = static_cast<std::int64_t>((i * 13) % 997) - 400;
                const auto value = static_cast<std::int64_t>(i % 101);
                const bool a_ok = !nulls || i % 11 != 4;
                a.push_code(a_code);
                b.push_code(b_code);
                c.push_back(c_value);
                v.push_back(value);
                a_valid.push_back(a_ok);
                const std::string key =
                    (a_ok ? (*a_dict)[static_cast<std::size_t>(a_code)] : std::string("null")) +
                    "|" + (*dict_b)[static_cast<std::size_t>(b_code)] + "|" +
                    std::to_string(c_value);
                auto [it, fresh] = want.try_emplace(key);
                if (fresh) {
                    order.push_back(key);
                }
                ++it->second.first;
                it->second.second += value;
            }
            runtime::Chunk chunk;
            const auto add = [&](const std::string& name, runtime::ColumnValue column) {
                runtime::ColumnEntry entry;
                entry.name = name;
                entry.column = std::make_shared<runtime::ColumnValue>(std::move(column));
                chunk.columns.push_back(std::move(entry));
            };
            add("a", std::move(a));
            if (nulls) {
                chunk.columns.back().validity = std::move(a_valid);
            }
            add("b", std::move(b));
            add("c", std::move(c));
            add("v", std::move(v));
            chunks.push_back(std::move(chunk));
        }
        runtime::ExternRegistry externs;
        externs.register_chunked_table("cat_code_keys", [&](const runtime::ExternArgs&) {
            return std::expected<runtime::OperatorPtr, std::string>{
                std::make_unique<VectorSource>(chunks)};
        });
        auto program = parser::parse(
            "extern fn cat_code_keys() -> DataFrame from \"x.hpp\"; "
            "cat_code_keys()[select { n = count(), s = sum(v) }, by { a, b, c }];");
        REQUIRE(program.has_value());
        auto ir = parser::lower(program.value());
        REQUIRE(ir.has_value());
        runtime::ExecutionContext parallel;
        parallel.parallel_threads = 8;
        parallel.parallel_min_rows = 0;
        const runtime::TableRegistry registry;
        const auto code_only_before = runtime::code_only_key_columns().load();
        const auto p =
            runtime::interpret(*ir.value(), registry, nullptr, &externs, nullptr, parallel);
        INFO((p.has_value() ? std::string{} : p.error()));
        REQUIRE(p.has_value());
        // Guards the test: the key store really held codes alone (a and b).
        CHECK(runtime::code_only_key_columns().load() >= code_only_before + 2);
        const auto* pa = p->find_entry("a");
        REQUIRE(pa != nullptr);
        const auto& a_out = std::get<Column<Categorical>>(*pa->column);
        const auto& b_out = std::get<Column<Categorical>>(*p->find("b"));
        const auto& c_out = std::get<Column<std::int64_t>>(*p->find("c"));
        const auto& pn = std::get<Column<std::int64_t>>(*p->find("n"));
        REQUIRE(p->rows() == order.size());
        const auto& ps = std::get<Column<std::int64_t>>(*p->find("s"));
        for (std::size_t g = 0; g < order.size(); ++g) {
            INFO("group " << g);
            const std::string got =
                (runtime::is_null(*pa, g) ? std::string("null") : std::string(a_out[g])) + "|" +
                std::string(b_out[g]) + "|" + std::to_string(c_out[g]);
            REQUIRE(got == order[g]);
            const auto& [n, sum] = want.at(order[g]);
            REQUIRE(pn[g] == n);
            REQUIRE(ps[g] == sum);
        }
    }
}
