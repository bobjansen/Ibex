// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

TEST_CASE("TableSourceOperator emits one chunk then EOF") {
    runtime::Table table;
    table.add_column("price", Column<std::int64_t>{10, 20, 30});
    table.add_column("symbol", Column<std::string>{"A", "B", "A"});

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(table));

    auto first = source->next();
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());
    REQUIRE(first.value()->rows() == 3);
    REQUIRE(first.value()->columns.size() == 2);

    auto second = source->next();
    REQUIRE(second.has_value());
    REQUIRE_FALSE(second.value().has_value());
}

TEST_CASE("MaterializeOperator round-trips a Table through the operator scaffold") {
    runtime::Table input;
    input.add_column("price", Column<std::int64_t>{10, 20, 30});
    input.add_column("symbol", Column<std::string>{"A", "B", "A"});

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(input));
    runtime::MaterializeOperator sink{std::move(source)};

    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.rows() == 3);
    REQUIRE(out.columns.size() == 2);

    const auto* price = out.find("price");
    REQUIRE(price != nullptr);
    const auto* price_col = std::get_if<Column<std::int64_t>>(price);
    REQUIRE(price_col != nullptr);
    REQUIRE(price_col->size() == 3);
    REQUIRE((*price_col)[0] == 10);
    REQUIRE((*price_col)[1] == 20);
    REQUIRE((*price_col)[2] == 30);

    const auto* symbol = out.find("symbol");
    REQUIRE(symbol != nullptr);
    const auto* symbol_col = std::get_if<Column<std::string>>(symbol);
    REQUIRE(symbol_col != nullptr);
    REQUIRE((*symbol_col)[0] == "A");
    REQUIRE((*symbol_col)[1] == "B");
    REQUIRE((*symbol_col)[2] == "A");
}

TEST_CASE("MaterializeOperator preserves Table ordering and time_index") {
    runtime::Table input;
    input.add_column("ts", Column<std::int64_t>{1, 2, 3});
    input.add_column("value", Column<double>{1.5, 2.5, 3.5});
    input.set_properties(ibex::runtime::TableProperties::recovered(
        std::vector<ir::OrderKey>{ir::OrderKey{.name = "ts", .ascending = true}}, std::string{"ts"},
        {}));

    auto source = std::make_unique<runtime::TableSourceOperator>(std::move(input));
    runtime::MaterializeOperator sink{std::move(source)};

    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.ordering().has_value());
    REQUIRE(out.ordering()->size() == 1);
    REQUIRE((*out.ordering())[0].name == "ts");
    REQUIRE(out.time_index().has_value());
    REQUIRE(*out.time_index() == "ts");
}

namespace {

auto make_int_chunk(const std::string& name, std::vector<std::int64_t> values) -> runtime::Chunk {
    runtime::Chunk chunk;
    runtime::ColumnEntry entry;
    entry.name = name;
    entry.column = std::make_shared<runtime::ColumnValue>(Column<std::int64_t>{});
    auto& col = std::get<Column<std::int64_t>>(*entry.column);
    col.reserve(values.size());
    for (auto v : values) {
        col.push_back(v);
    }
    chunk.columns.push_back(std::move(entry));
    return chunk;
}

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

auto make_int_chunk_valid(const std::string& name, std::vector<std::int64_t> values,
                          std::optional<std::vector<bool>> validity) -> runtime::Chunk {
    auto chunk = make_int_chunk(name, std::move(values));
    if (validity.has_value()) {
        chunk.columns[0].validity = runtime::ValidityBitmap{*validity};
    }
    return chunk;
}

}  // namespace

TEST_CASE("MaterializeOperator concatenates multi-chunk int streams") {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(make_int_chunk("x", {1, 2, 3}));
    chunks.push_back(make_int_chunk("x", {4, 5}));
    chunks.push_back(make_int_chunk("x", {6, 7, 8, 9}));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE(result.has_value());
    REQUIRE(result.value().rows() == 9);

    const auto* col = std::get_if<Column<std::int64_t>>(result.value().find("x"));
    REQUIRE(col != nullptr);
    for (std::int64_t i = 1; i <= 9; ++i) {
        REQUIRE((*col)[static_cast<std::size_t>(i - 1)] == i);
    }
}

TEST_CASE("MaterializeOperator rejects chunk schema mismatches") {
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(make_int_chunk("x", {1, 2}));

    // Second chunk has a different column name.
    chunks.push_back(make_int_chunk("y", {3, 4}));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().find("schema mismatch") != std::string::npos);
}

TEST_CASE("MaterializeOperator concatenates validity across multi-chunk streams") {
    std::vector<runtime::Chunk> chunks;
    // Chunk 0: all valid, no bitmap. Chunk 1: carries nulls. Chunk 2: no bitmap.
    chunks.push_back(make_int_chunk_valid("x", {1, 2}, std::nullopt));
    chunks.push_back(make_int_chunk_valid("x", {3, 4, 5}, std::vector<bool>{true, false, true}));
    chunks.push_back(make_int_chunk_valid("x", {6, 7}, std::nullopt));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.rows() == 7);
    const auto* entry = out.find_entry("x");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    const auto& v = *entry->validity;
    REQUIRE(v.size() == 7);
    // Chunk 0 backfilled valid, chunk 1 nulls the middle, chunk 2 filled valid.
    const std::vector<bool> expected{true, true, true, false, true, true, true};
    for (std::size_t i = 0; i < expected.size(); ++i) {
        REQUIRE(v[i] == expected[i]);
    }
}

TEST_CASE("MaterializeOperator keeps bitmap-free multi-chunk streams bitmap-free") {
    // No chunk carries a validity bitmap, so the concatenated column must stay
    // bitmap-free (the zero-overhead common case): the helper never
    // materializes a bitmap unless some chunk actually has one.
    std::vector<runtime::Chunk> chunks;
    chunks.push_back(make_int_chunk_valid("x", {1, 2}, std::nullopt));
    chunks.push_back(make_int_chunk_valid("x", {3, 4}, std::nullopt));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.rows() == 4);
    const auto* entry = out.find_entry("x");
    REQUIRE(entry != nullptr);
    REQUIRE_FALSE(entry->validity.has_value());
}

TEST_CASE("MaterializeOperator accumulates logical rows across zero-column chunks") {
    // Column-less frames (e.g. Table(n) scaffolds) carry their row count in
    // `logical_rows`; a multi-chunk zero-column stream must report the total.
    std::vector<runtime::Chunk> chunks;
    runtime::Chunk a;
    a.logical_rows = 3;
    runtime::Chunk b;
    b.logical_rows = 5;
    runtime::Chunk c;
    c.logical_rows = 2;
    chunks.push_back(std::move(a));
    chunks.push_back(std::move(b));
    chunks.push_back(std::move(c));

    runtime::MaterializeOperator sink{std::make_unique<VectorSource>(std::move(chunks))};
    auto result = sink.run();
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.columns.empty());
    REQUIRE(out.rows() == 10);
}

TEST_CASE("MaterializeOperator returns an empty Table when the source is empty") {
    class EmptySource final : public runtime::Operator {
       public:
        auto next() -> std::expected<std::optional<runtime::Chunk>, std::string> override {
            return std::optional<runtime::Chunk>{};
        }
    };

    runtime::MaterializeOperator sink{std::make_unique<EmptySource>()};
    auto result = sink.run();
    REQUIRE(result.has_value());
    REQUIRE(result.value().columns.empty());
    REQUIRE(result.value().rows() == 0);
}

namespace {

// Byte-for-byte column comparison for the round-trip tests: same alternative,
// same length, same elements (categorical compared by resolved string).
auto columns_equal(const runtime::ColumnValue& a, const runtime::ColumnValue& b) -> bool {
    if (a.index() != b.index()) {
        return false;
    }
    return std::visit(
        [&](const auto& lhs) -> bool {
            using Col = std::decay_t<decltype(lhs)>;
            const auto& rhs = std::get<Col>(b);
            if (lhs.size() != rhs.size()) {
                return false;
            }
            for (std::size_t i = 0; i < lhs.size(); ++i) {
                if (lhs[i] != rhs[i]) {
                    return false;
                }
            }
            return true;
        },
        a);
}

// Assert MaterializeOperator(PartitionedTableSource(input, grain)) reproduces
// `input` byte-for-byte: schema, column data, validity, and table metadata.
void require_roundtrip(const runtime::Table& input, std::size_t grain) {
    runtime::MaterializeOperator sink{
        std::make_unique<runtime::PartitionedTableSource>(input, grain)};
    auto result = sink.run();
    REQUIRE(result.has_value());
    const auto& out = result.value();

    REQUIRE(out.columns.size() == input.columns.size());
    REQUIRE(out.rows() == input.rows());
    for (std::size_t c = 0; c < input.columns.size(); ++c) {
        REQUIRE(out.columns[c].name == input.columns[c].name);
        REQUIRE(columns_equal(*out.columns[c].column, *input.columns[c].column));
        REQUIRE(out.columns[c].validity.has_value() == input.columns[c].validity.has_value());
        if (input.columns[c].validity.has_value()) {
            const auto& want = *input.columns[c].validity;
            const auto& got = *out.columns[c].validity;
            REQUIRE(got.size() == want.size());
            for (std::size_t r = 0; r < want.size(); ++r) {
                REQUIRE(got[r] == want[r]);
            }
        }
    }
    REQUIRE(out.ordering().has_value() == input.ordering().has_value());
    REQUIRE(out.time_index().has_value() == input.time_index().has_value());
}

}  // namespace

TEST_CASE("PartitionedTableSource round-trips a multi-type table at every grain") {
    runtime::Table input;
    input.add_column("i", Column<std::int64_t>{1, 2, 3, 4, 5, 6, 7});
    input.add_column("d", Column<double>{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5});
    input.add_column("s", Column<std::string>{"a", "bb", "ccc", "d", "ee", "f", "ggg"});
    input.add_column("c", Column<Categorical>{std::vector<std::string>{"x", "y"},
                                              std::vector<std::int32_t>{0, 1, 0, 1, 1, 0, 1}});

    // Grains that divide evenly, don't divide evenly, equal the size, and exceed it.
    for (const std::size_t grain :
         {std::size_t{1}, std::size_t{2}, std::size_t{3}, std::size_t{7}, std::size_t{100}}) {
        require_roundtrip(input, grain);
    }
}

TEST_CASE("PartitionedTableSource treats grain 0 as 1") {
    runtime::Table input;
    input.add_column("i", Column<std::int64_t>{10, 20, 30});
    require_roundtrip(input, 0);
}

TEST_CASE("PartitionedTableSource preserves nulls across range boundaries") {
    runtime::Table input;
    input.add_column("i", Column<std::int64_t>{1, 2, 3, 4, 5},
                     runtime::ValidityBitmap{true, false, true, false, true});
    // A grain of 2 splits the nulls across chunk boundaries.
    require_roundtrip(input, 2);
}

TEST_CASE("PartitionedTableSource preserves ordering and time_index") {
    runtime::Table input;
    input.add_column("ts", Column<std::int64_t>{1, 2, 3, 4});
    input.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0});
    input.set_properties(ibex::runtime::TableProperties::recovered(
        std::vector<ir::OrderKey>{ir::OrderKey{.name = "ts", .ascending = true}}, std::string{"ts"},
        {}));

    runtime::MaterializeOperator sink{std::make_unique<runtime::PartitionedTableSource>(input, 2)};
    auto result = sink.run();
    REQUIRE(result.has_value());
    const auto& out = result.value();
    REQUIRE(out.ordering().has_value());
    REQUIRE((*out.ordering())[0].name == "ts");
    REQUIRE(out.time_index().has_value());
    REQUIRE(*out.time_index() == "ts");
}

TEST_CASE("PartitionedTableSource emits one empty schema-carrier chunk for a zero-row table") {
    runtime::Table input;
    input.add_column("i", Column<std::int64_t>{});
    input.add_column("s", Column<std::string>{});

    runtime::PartitionedTableSource source{input, 4};
    auto first = source.next();
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());  // one empty schema carrier
    REQUIRE(first.value()->columns.size() == 2);
    REQUIRE(first.value()->rows() == 0);
    REQUIRE(first.value()->sequence == 0);
    REQUIRE(first.value()->row_offset == 0);

    auto second = source.next();
    REQUIRE(second.has_value());
    REQUIRE_FALSE(second.value().has_value());

    // And it round-trips the (empty) schema through Materialize.
    require_roundtrip(input, 4);
}

TEST_CASE("PartitionedTableSource partitions a column-less frame by logical rows") {
    runtime::Table input;
    input.logical_rows = 42;  // e.g. a Table(n) scaffold

    runtime::PartitionedTableSource source{input, 8};
    const std::size_t expected_offsets[] = {0, 8, 16, 24, 32, 40};
    const std::size_t expected_rows[] = {8, 8, 8, 8, 8, 2};
    for (std::uint64_t sequence = 0; sequence < std::size(expected_rows); ++sequence) {
        auto chunk = source.next();
        REQUIRE(chunk.has_value());
        REQUIRE(chunk.value().has_value());
        REQUIRE(chunk.value()->columns.empty());
        REQUIRE(chunk.value()->rows() == expected_rows[sequence]);
        REQUIRE(chunk.value()->sequence == sequence);
        REQUIRE(chunk.value()->row_offset == expected_offsets[sequence]);
    }
    auto done = source.next();
    REQUIRE(done.has_value());
    REQUIRE_FALSE(done.value().has_value());

    runtime::MaterializeOperator sink{std::make_unique<runtime::PartitionedTableSource>(input, 8)};
    auto result = sink.run();
    REQUIRE(result.has_value());
    REQUIRE(result.value().columns.empty());
    REQUIRE(result.value().rows() == 42);
}

TEST_CASE("PartitionedTableSource emits one zero-row carrier for a column-less frame") {
    runtime::Table input;
    input.logical_rows = 0;

    runtime::PartitionedTableSource source{input, 8};
    auto first = source.next();
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());
    REQUIRE(first.value()->columns.empty());
    REQUIRE(first.value()->rows() == 0);
    REQUIRE(first.value()->sequence == 0);
    REQUIRE(first.value()->row_offset == 0);

    auto done = source.next();
    REQUIRE(done.has_value());
    REQUIRE_FALSE(done.value().has_value());
    require_roundtrip(input, 8);
}

TEST_CASE("PartitionedTableSource stamps ascending sequence and absolute row_offset") {
    runtime::Table input;
    input.add_column("i", Column<std::int64_t>{0, 1, 2, 3, 4, 5, 6});  // 7 rows

    runtime::PartitionedTableSource source{input, 3};
    // Expect ranges [0,3), [3,6), [6,7): sequence 0,1,2 and row_offset 0,3,6.
    const std::size_t expected_offsets[] = {0, 3, 6};
    const std::size_t expected_rows[] = {3, 3, 1};
    for (std::uint64_t seq = 0; seq < 3; ++seq) {
        auto chunk = source.next();
        REQUIRE(chunk.has_value());
        REQUIRE(chunk.value().has_value());
        REQUIRE(chunk.value()->sequence == seq);
        REQUIRE(chunk.value()->row_offset == expected_offsets[seq]);
        REQUIRE(chunk.value()->rows() == expected_rows[seq]);
    }
    auto done = source.next();
    REQUIRE(done.has_value());
    REQUIRE_FALSE(done.value().has_value());
}
