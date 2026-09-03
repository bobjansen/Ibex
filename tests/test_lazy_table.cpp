// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

namespace {

/// Default execution settings for these tests. The tables here are far below
/// the parallel row threshold, so this selects the serial path either way; it
/// exists so the call sites name the context explicitly rather than defaulting.
const runtime::ExecutionContext kExec{};

/// A stand-in for a columnar file: records which columns each decode asked for,
/// so a test can assert on what was read rather than only on what came back.
struct FakeSource {
    std::vector<std::vector<std::string>> decode_calls;
    std::vector<std::optional<runtime::Selection>> selections;

    auto schema() const -> runtime::Table {
        runtime::Table t;
        t.add_column("a", Column<std::int64_t>{});
        t.add_column("b", Column<double>{});
        t.add_column("c", Column<std::string>{});
        return t;
    }

    auto decode(const std::vector<std::string>& names, const runtime::Selection* selection)
        -> std::expected<runtime::Table, std::string> {
        decode_calls.push_back(names);
        selections.push_back(selection == nullptr ? std::nullopt : std::optional{*selection});
        const runtime::Selection all{0, 1, 2};
        const auto& rows = selection == nullptr ? all : *selection;
        runtime::Table out;
        for (const auto& name : names) {
            if (name == "a") {
                std::vector<std::int64_t> values;
                for (auto row : rows) {
                    values.push_back(static_cast<std::int64_t>(row + 1));
                }
                out.add_column("a", Column<std::int64_t>{std::move(values)});
            } else if (name == "b") {
                std::vector<double> values;
                for (auto row : rows) {
                    values.push_back(static_cast<double>(row) + 1.5);
                }
                out.add_column("b", Column<double>{std::move(values)});
            } else if (name == "c") {
                const std::vector<std::string> source{"x", "y", "z"};
                std::vector<std::string> values;
                for (auto row : rows) {
                    values.push_back(source[row]);
                }
                out.add_column("c", Column<std::string>{std::move(values)});
            }
        }
        out.logical_rows = rows.size();
        return out;
    }
};

struct ReaderFactoryState {
    std::size_t products = 0;
    std::vector<std::size_t> decode_products;
};

class TrackingReader final : public runtime::LazySourceReader {
   public:
    TrackingReader(std::shared_ptr<ReaderFactoryState> state, std::size_t product)
        : state_(std::move(state)), product_(product) {}

    auto decode(const std::vector<std::string>& names, const runtime::Selection* selection,
                const runtime::SourceUnit* /*unit*/, const runtime::ExecutionContext& /*exec*/)
        -> std::expected<runtime::Table, std::string> override {
        state_->decode_products.push_back(product_);
        const runtime::Selection all{0, 1, 2};
        const auto& rows = selection == nullptr ? all : *selection;
        runtime::Table out;
        for (const auto& name : names) {
            std::vector<std::int64_t> values;
            values.reserve(rows.size());
            for (auto row : rows) {
                values.push_back(static_cast<std::int64_t>(row + 1));
            }
            out.add_column(name, Column<std::int64_t>{std::move(values)});
        }
        out.logical_rows = rows.size();
        return out;
    }

   private:
    std::shared_ptr<ReaderFactoryState> state_;
    std::size_t product_;
};

auto tracking_reader_factory(const std::shared_ptr<ReaderFactoryState>& state)
    -> runtime::LazySourceReaderFactory {
    return [state]() -> std::expected<runtime::LazySourceReaderPtr, std::string> {
        const auto product = state->products++;
        return runtime::LazySourceReaderPtr{std::make_unique<TrackingReader>(state, product)};
    };
}

auto make_lazy(FakeSource& source) -> runtime::LazyTable {
    return runtime::LazyTable{
        source.schema(), 3,
        [&source](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        }};
}

auto greater_than(std::string name, std::int64_t value) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{
                        .op = ir::CompareOp::Gt,
                        .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = name}}),
                        .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = value}})}};
}

auto less_than(std::string name, double value) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{
                        .op = ir::CompareOp::Lt,
                        .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = name}}),
                        .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = value}})}};
}

auto like_call(std::string name, std::string pattern) -> ir::Expr {
    ir::CallExpr call;
    call.callee = "like";
    call.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}}));
    call.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::move(pattern)}}));
    return ir::Expr{.node = std::move(call)};
}

auto not_like(std::string name, std::string pattern) -> ir::Expr {
    ir::LogicalExpr negation;
    negation.op = ir::LogicalOp::Not;
    negation.left = ir::make_expr_ptr(like_call(std::move(name), std::move(pattern)));
    return ir::Expr{.node = std::move(negation)};
}

/// A source with one text column, standing in for the shape this optimization
/// exists for: `s` is wide, the query only filters on it, and materializing it
/// is the expensive part.
///
/// Records every decode and every fused scan, so a test can assert that the
/// text column was *not* read rather than only that the answer came out right.
struct TextSourceState {
    std::vector<std::vector<std::string>> decode_calls;
    std::vector<std::string> scan_calls;
    std::vector<std::string> key_scan_calls;
    /// When false the reader declines the fused scan, exercising the fallback.
    bool fused = true;
    /// When false the reader reports no decomposition, so `scan_units` is
    /// empty and nothing streams.
    bool units = true;
};

constexpr std::array<std::string_view, 6> kText{"alpha",    "bad apple", "cherry",
                                                "very bad", "date",      "elder"};

/// Units this source decomposes into: three ranges of two rows. Deliberately
/// uneven with nothing else in the file — a unit boundary that lines up with a
/// filter boundary would hide a rebasing bug.
constexpr std::array<runtime::SourceUnit, 3> kUnits{runtime::SourceUnit{.start = 0, .rows = 2},
                                                    runtime::SourceUnit{.start = 2, .rows = 2},
                                                    runtime::SourceUnit{.start = 4, .rows = 2}};

class TextReader final : public runtime::LazySourceReader {
   public:
    explicit TextReader(std::shared_ptr<TextSourceState> state) : state_(std::move(state)) {}

    auto decode_units() -> std::vector<runtime::SourceUnit> override {
        return state_->units ? std::vector<runtime::SourceUnit>{kUnits.begin(), kUnits.end()}
                             : std::vector<runtime::SourceUnit>{};
    }

    auto decode(const std::vector<std::string>& names, const runtime::Selection* selection,
                const runtime::SourceUnit* unit, const runtime::ExecutionContext& /*exec*/)
        -> std::expected<runtime::Table, std::string> override {
        state_->decode_calls.push_back(names);
        // A unit restricts which source rows this decode covers; the selection
        // stays source-global and is intersected with it, never rebased.
        const std::size_t begin = unit == nullptr ? 0 : unit->start;
        const std::size_t end = unit == nullptr ? kText.size() : unit->start + unit->rows;
        runtime::Selection all;
        for (std::size_t row = begin; row < end; ++row) {
            all.push_back(row);
        }
        runtime::Selection within;
        if (selection != nullptr) {
            for (const auto row : *selection) {
                if (row >= begin && row < end) {
                    within.push_back(row);
                }
            }
        }
        const auto& rows = selection == nullptr ? all : within;
        runtime::Table out;
        for (const auto& name : names) {
            if (name == "s") {
                std::vector<std::string> values;
                values.reserve(rows.size());
                for (auto row : rows) {
                    values.emplace_back(kText[row]);
                }
                out.add_column("s", Column<std::string>{std::move(values)});
            } else {
                std::vector<std::int64_t> values;
                values.reserve(rows.size());
                for (auto row : rows) {
                    values.push_back(static_cast<std::int64_t>(row));
                }
                out.add_column(name, Column<std::int64_t>{std::move(values)});
            }
        }
        out.logical_rows = rows.size();
        return out;
    }

    auto string_filter_scan(const std::string& column, const runtime::StringScanFilter& filter,
                            const runtime::SourceUnit* unit,
                            const runtime::ExecutionContext& /*exec*/)
        -> std::expected<std::optional<runtime::Selection>, std::string> override {
        state_->scan_calls.push_back(column);
        if (!state_->fused) {
            return std::optional<runtime::Selection>{};
        }
        const std::size_t begin = unit == nullptr ? 0 : unit->start;
        const std::size_t end = unit == nullptr ? kText.size() : unit->start + unit->rows;
        runtime::Selection selected;
        for (std::size_t row = begin; row < end; ++row) {
            if (filter.passes(kText[row])) {
                selected.push_back(row);
            }
        }
        return std::optional{std::move(selected)};
    }

    auto key_filter_scan(const std::string& column, const runtime::DynamicScanFilter& filter,
                         const runtime::SourceUnit* unit, const runtime::ExecutionContext& /*exec*/)
        -> std::expected<std::optional<runtime::Selection>, std::string> override {
        state_->key_scan_calls.push_back(column);
        if (column != "n")
            return std::optional<runtime::Selection>{};
        const std::size_t begin = unit == nullptr ? 0 : unit->start;
        const std::size_t end = unit == nullptr ? kText.size() : unit->start + unit->rows;
        runtime::Selection selected;
        for (std::size_t row = begin; row < end; ++row)
            if (filter.passes(static_cast<std::int64_t>(row)))
                selected.push_back(row);
        return std::optional{std::move(selected)};
    }

   private:
    std::shared_ptr<TextSourceState> state_;
};

auto make_text_lazy(const std::shared_ptr<TextSourceState>& state) -> runtime::LazyTable {
    runtime::Table schema;
    schema.add_column("n", Column<std::int64_t>{});
    schema.add_column("s", Column<std::string>{});
    return runtime::LazyTable{
        std::move(schema), kText.size(),
        [state]() -> std::expected<runtime::LazySourceReaderPtr, std::string> {
            return runtime::LazySourceReaderPtr{std::make_unique<TextReader>(state)};
        }};
}

auto int_column(const runtime::Table& table, const std::string& name) -> std::vector<std::int64_t> {
    const auto* entry = table.find_entry(name);
    REQUIRE(entry != nullptr);
    const auto* column = std::get_if<Column<std::int64_t>>(&*entry->column);
    REQUIRE(column != nullptr);
    return std::vector<std::int64_t>{column->begin(), column->end()};
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

TEST_CASE("LazyTable: reader factory products own independent decoder state",
          "[runtime][lazy_table]") {
    auto state = std::make_shared<ReaderFactoryState>();
    auto factory = tracking_reader_factory(state);

    // A scheduler may keep one product per worker. Products must remain
    // distinct while alive, rather than aliasing one mutable decoder.
    auto first = factory();
    auto second = factory();
    REQUIRE(first);
    REQUIRE(second);
    CHECK(first->get() != second->get());

    runtime::Table schema;
    schema.add_column("a", Column<std::int64_t>{});
    schema.add_column("b", Column<std::int64_t>{});
    runtime::LazyTable lazy{std::move(schema), 3, factory};

    REQUIRE(lazy.project({"a"}, kExec));
    REQUIRE(lazy.project({"b"}, kExec));
    CHECK(state->decode_products == std::vector<std::size_t>{2, 2});
    CHECK(state->products == 3);
}

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

    auto table = lazy.project({"b"}, kExec);
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"b"});
    CHECK(table->rows() == 3);
    REQUIRE(source.decode_calls.size() == 1);
    CHECK(source.decode_calls[0] == std::vector<std::string>{"b"});
}

TEST_CASE("LazyTable: a decoded column is not decoded twice", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    REQUIRE(lazy.project({"a", "b"}, kExec));
    REQUIRE(source.decode_calls.size() == 1);

    // Second query overlaps the first: only the genuinely new column is read.
    auto table = lazy.project({"b", "c"}, kExec);
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"b", "c"});
    REQUIRE(source.decode_calls.size() == 2);
    CHECK(source.decode_calls[1] == std::vector<std::string>{"c"});

    // Fully cached: no decode at all.
    REQUIRE(lazy.project({"a", "b"}, kExec));
    CHECK(source.decode_calls.size() == 2);
}

TEST_CASE("LazyTable: projected columns come back in schema order", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // Decode `c` first, so cache insertion order differs from schema order.
    REQUIRE(lazy.project({"c"}, kExec));
    auto table = lazy.project({"a", "c"}, kExec);
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"a", "c"});
}

TEST_CASE("LazyTable: names outside the schema are ignored", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // A join's demand is the union across both sides, so a source is routinely
    // asked for names it does not have.
    auto table = lazy.project({"a", "not_here"}, kExec);
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"a"});
    CHECK(source.decode_calls[0] == std::vector<std::string>{"a"});
}

TEST_CASE("LazyTable: an empty projection still carries the row count", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // `count()` over an unfiltered scan needs the row count and no column.
    auto table = lazy.project({}, kExec);
    REQUIRE(table);
    CHECK(table->columns.empty());
    CHECK(table->rows() == 3);
    CHECK(source.decode_calls.empty());
}

TEST_CASE("LazyTable: materialize decodes every column", "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.materialize(kExec);
    REQUIRE(table);
    CHECK(names_of(table.value()) == std::vector<std::string>{"a", "b", "c"});
    CHECK(table->rows() == 3);
}

TEST_CASE("LazyTable: project_where decodes predicates before selected payload columns",
          "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.project_where({"a", "b"}, {greater_than("a", 1)}, kExec);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    REQUIRE(source.decode_calls.size() == 2);
    CHECK(source.decode_calls[0] == std::vector<std::string>{"a"});
    CHECK_FALSE(source.selections[0].has_value());
    CHECK(source.decode_calls[1] == std::vector<std::string>{"b"});
    REQUIRE(source.selections[1].has_value());
    CHECK(*source.selections[1] == runtime::Selection{1, 2});

    const auto* a = std::get_if<Column<std::int64_t>>(table->find("a"));
    const auto* b = std::get_if<Column<double>>(table->find("b"));
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    CHECK((*a)[0] == 2);
    CHECK((*a)[1] == 3);
    CHECK((*b)[0] == 2.5);
    CHECK((*b)[1] == 3.5);
}

TEST_CASE("LazyTable: selection_for returns the rows project_where keeps",
          "[runtime][lazy_table]") {
    // The equivalence Phase 2 of plans/per-occurrence-scan-selections-plan.md
    // rests on: the selection alone must pick out exactly the rows
    // project_where would have produced, so several occurrences of one source
    // can share a decode and gather from it instead of each re-reading.
    FakeSource selection_source;
    auto selection_lazy = make_lazy(selection_source);
    auto selection = selection_lazy.selection_for({"a", "b"}, {greater_than("a", 1)}, kExec);
    REQUIRE(selection);
    REQUIRE(selection->has_value());
    CHECK(**selection == runtime::Selection{1, 2});

    FakeSource filtered_source;
    auto filtered_lazy = make_lazy(filtered_source);
    auto table = filtered_lazy.project_where({"a", "b"}, {greater_than("a", 1)}, kExec);
    REQUIRE(table);
    CHECK(table->rows() == (*selection)->size());

    // No payload column was materialized to answer it: only the predicate
    // column is read, and read whole.
    REQUIRE(selection_source.decode_calls.size() == 1);
    CHECK(selection_source.decode_calls[0] == std::vector<std::string>{"a"});
    CHECK_FALSE(selection_source.selections[0].has_value());
}

TEST_CASE("LazyTable: selection_for reports every row as nullopt, not a full selection",
          "[runtime][lazy_table]") {
    // `nullopt` (nothing rejected) and an empty Selection (nothing survived)
    // are opposite answers, so the caller must be able to tell them apart.
    FakeSource source;
    auto lazy = make_lazy(source);

    auto none = lazy.selection_for({"a", "b"}, {}, kExec);
    REQUIRE(none);
    CHECK_FALSE(none->has_value());
    CHECK(source.decode_calls.empty());

    auto empty = lazy.selection_for({"a", "b"}, {greater_than("a", 99)}, kExec);
    REQUIRE(empty);
    REQUIRE(empty->has_value());
    CHECK((*empty)->empty());
}

TEST_CASE("LazyTable: project_where evaluates staged predicates before selected payload",
          "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.project_where(
        {"a", "b", "c"}, {greater_than("a", 0), greater_than("a", 1), less_than("b", 3.0)}, kExec);
    REQUIRE(table);
    CHECK(table->rows() == 1);
    REQUIRE(source.decode_calls.size() == 2);
    CHECK(source.decode_calls[0] == std::vector<std::string>{"a", "b"});
    CHECK_FALSE(source.selections[0].has_value());
    CHECK(source.decode_calls[1] == std::vector<std::string>{"c"});
    REQUIRE(source.selections[1].has_value());
    CHECK(*source.selections[1] == runtime::Selection{1});

    const auto* a = std::get_if<Column<std::int64_t>>(table->find("a"));
    const auto* b = std::get_if<Column<double>>(table->find("b"));
    const auto* c = std::get_if<Column<std::string>>(table->find("c"));
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    REQUIRE(c != nullptr);
    CHECK((*a)[0] == 2);
    CHECK((*b)[0] == 2.5);
    CHECK((*c)[0] == "y");
}

TEST_CASE("LazyTable: project_where compacts later predicate evaluation but keeps decoding dense",
          "[runtime][lazy_table]") {
    runtime::Table schema;
    schema.add_column("a", Column<std::int64_t>{});
    schema.add_column("b", Column<double>{});
    schema.add_column("c", Column<std::string>{});
    std::vector<std::vector<std::string>> calls;
    std::vector<std::optional<runtime::Selection>> selections;
    runtime::LazyTable lazy{
        std::move(schema), 64,
        [&](const std::vector<std::string>& names,
            const runtime::Selection* selection) -> std::expected<runtime::Table, std::string> {
            calls.push_back(names);
            selections.push_back(selection == nullptr ? std::nullopt : std::optional{*selection});
            runtime::Selection all(64);
            std::iota(all.begin(), all.end(), std::size_t{0});
            const auto& rows = selection == nullptr ? all : *selection;
            runtime::Table out;
            for (const auto& name : names) {
                if (name == "a") {
                    std::vector<std::int64_t> values;
                    for (auto row : rows) {
                        values.push_back(static_cast<std::int64_t>(row % 8));
                    }
                    out.add_column("a", Column<std::int64_t>{std::move(values)});
                } else if (name == "b") {
                    std::vector<double> values;
                    runtime::ValidityBitmap validity(rows.size(), true);
                    std::size_t position = 0;
                    for (auto row : rows) {
                        values.push_back(static_cast<double>(row));
                        if (row == 15) {
                            validity.set(position, false);
                        }
                        ++position;
                    }
                    out.add_column("b", Column<double>{std::move(values)}, std::move(validity));
                } else if (name == "c") {
                    std::vector<std::string> values;
                    for (auto row : rows) {
                        values.push_back(std::to_string(row));
                    }
                    out.add_column("c", Column<std::string>{std::move(values)});
                }
            }
            out.logical_rows = rows.size();
            return out;
        }};

    auto table = lazy.project_where({"c"}, {greater_than("a", 6), less_than("b", 20.0)}, kExec);
    REQUIRE(table);
    CHECK(table->rows() == 1);
    REQUIRE(calls.size() == 2);
    CHECK(calls[0] == std::vector<std::string>{"a", "b"});
    CHECK_FALSE(selections[0].has_value());
    CHECK(calls[1] == std::vector<std::string>{"c"});
    REQUIRE(selections[1].has_value());
    CHECK(*selections[1] == runtime::Selection{7});
}

TEST_CASE("LazyTable: project_where never poisons the whole-column cache",
          "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    REQUIRE(lazy.project_where({"a", "b"}, {greater_than("a", 1)}, kExec));
    REQUIRE(source.decode_calls.size() == 2);

    // The predicate column was decoded whole-file for the selection, so it is
    // a legitimate cache entry: projecting it later costs no decode and comes
    // back full-length.
    auto whole_a = lazy.project({"a"}, kExec);
    REQUIRE(whole_a);
    CHECK(whole_a->rows() == 3);
    CHECK(source.decode_calls.size() == 2);
    const auto* a = std::get_if<Column<std::int64_t>>(whole_a->find("a"));
    REQUIRE(a != nullptr);
    CHECK((*a)[0] == 1);
    CHECK((*a)[2] == 3);

    // The payload column was decoded under a selection, so it must NOT be
    // cached: projecting it whole decodes it again, densely.
    auto whole_b = lazy.project({"b"}, kExec);
    REQUIRE(whole_b);
    CHECK(whole_b->rows() == 3);
    REQUIRE(source.decode_calls.size() == 3);
    CHECK(source.decode_calls.back() == std::vector<std::string>{"b"});
    CHECK_FALSE(source.selections.back().has_value());
}

TEST_CASE("LazyTable: project_where reuses cached whole columns for predicates",
          "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    REQUIRE(lazy.project({"a"}, kExec));
    REQUIRE(source.decode_calls.size() == 1);

    // `a` is already cached whole-file, so only the payload column is decoded.
    auto table = lazy.project_where({"b"}, {greater_than("a", 1)}, kExec);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    REQUIRE(source.decode_calls.size() == 2);
    CHECK(source.decode_calls.back() == std::vector<std::string>{"b"});
    REQUIRE(source.selections.back().has_value());
    CHECK(*source.selections.back() == runtime::Selection{1, 2});
}

TEST_CASE("LazyTable: a decode failure surfaces as an error", "[runtime][lazy_table]") {
    runtime::Table schema;
    schema.add_column("a", Column<std::int64_t>{});
    runtime::LazyTable lazy{
        std::move(schema), 3,
        [](const std::vector<std::string>&, const runtime::Selection*)
            -> std::expected<runtime::Table, std::string> { return std::unexpected("boom"); }};

    auto table = lazy.project({"a"}, kExec);
    REQUIRE_FALSE(table);
    CHECK(table.error() == "boom");
}

TEST_CASE("lazy table carries source column stats without decoding", "[lazy_table]") {
    // A source's metadata is what the planner gets to reason from before any
    // page is read. Reaching for it must not pull data in behind it.
    FakeSource source;
    runtime::SourceColumnStats stats;
    stats.emplace("a", runtime::ColumnStats{.min = 1, .max = 3, .null_count = 0});
    const runtime::LazyTable lazy(
        source.schema(), 3,
        [&](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        },
        std::move(stats));

    const auto& out = lazy.column_stats();
    auto it = out.find("a");
    REQUIRE(it != out.end());
    CHECK(it->second.min == 1);
    CHECK(it->second.max == 3);
    CHECK(it->second.null_count == 0);
    // A column the source said nothing about is absent -- "nothing known", never
    // "nothing there".
    CHECK(out.find("b") == out.end());
    CHECK(source.decode_calls.empty());
}

TEST_CASE("lazy table defaults to knowing no column stats", "[lazy_table]") {
    FakeSource source;
    const runtime::LazyTable lazy(
        source.schema(), 3,
        [&](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        });
    CHECK(lazy.column_stats().empty());
}

TEST_CASE("JoinBloomFilter: no false negatives, few false positives",
          "[runtime][lazy_table][deferred_scan]") {
    runtime::JoinBloomFilter bloom(1000);
    for (std::int64_t key = 0; key < 1000; ++key) {
        bloom.insert(key * 7);
    }
    for (std::int64_t key = 0; key < 1000; ++key) {
        CHECK(bloom.contains(key * 7));
    }
    // Probing 10K keys that were never inserted must reject the vast
    // majority; the exact rate is probabilistic but ~1-2% at this sizing.
    std::size_t false_positives = 0;
    for (std::int64_t key = 0; key < 10000; ++key) {
        false_positives += bloom.contains(1000 * 7 + key * 7 + 3) ? 1 : 0;
    }
    CHECK(false_positives < 1000);
}

TEST_CASE("DynamicScanFilter: the exact list cancels Bloom false positives",
          "[runtime][lazy_table][deferred_scan]") {
    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(4);
    for (std::int64_t key : {10, 20, 30}) {
        filter.bloom->insert(key);
    }
    filter.in_list = {10, 20, 30};
    REQUIRE(filter.has_membership());
    CHECK(filter.passes(10));
    CHECK(filter.passes(30));
    // With an exact list every non-member fails, Bloom collisions included.
    for (std::int64_t key = 0; key < 1000; ++key) {
        if (key != 10 && key != 20 && key != 30) {
            CHECK_FALSE(filter.passes(key));
        }
    }
}

TEST_CASE("LazyTable: project_where applies a membership filter without conjuncts",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    // Keys are a = {1, 2, 3}; the join's build side saw only {2, 3}.
    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);
    filter.in_list = {2, 3};

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    const auto* a = std::get_if<Column<std::int64_t>>(&*table->find("a"));
    REQUIRE(a != nullptr);
    REQUIRE(a->size() == 2);
    CHECK((*a)[0] == 2);
    CHECK((*a)[1] == 3);
    // The key column decodes whole-file (the selection needs every row); the
    // payload column decodes through the selection.
    REQUIRE(source.selections.size() == 2);
    CHECK_FALSE(source.selections.front().has_value());
    REQUIRE(source.selections.back().has_value());
    CHECK(*source.selections.back() == runtime::Selection{1, 2});
}

TEST_CASE("LazyTable: membership composes with static conjuncts",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(1);
    filter.bloom->insert(2);
    filter.in_list = {1, 2};

    // Static conjunct keeps a > 1 -> {2, 3}; membership keeps {1, 2}. The
    // intersection is exactly {2}.
    const std::string key = "a";
    auto table =
        lazy.project_where({"a", "b"}, {greater_than("a", 1)}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 1);
    const auto* a = std::get_if<Column<std::int64_t>>(&*table->find("a"));
    REQUIRE(a != nullptr);
    REQUIRE(a->size() == 1);
    CHECK((*a)[0] == 2);
}

TEST_CASE("LazyTable: a non-integer membership key is soundly ignored",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(42);

    // "b" is a double column: no filter applies, the projection is dense.
    const std::string key = "b";
    auto table = lazy.project_where({"a", "b"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 3);
}

TEST_CASE("LazyTable: membership drops rows whose key is null",
          "[runtime][lazy_table][deferred_scan]") {
    // A deferred scan feeds exactly one inner join, and null keys never
    // match, so a null-keyed row is as dead as an out-of-set one.
    runtime::Table schema;
    schema.add_column("k", Column<std::int64_t>{});
    runtime::LazyTable lazy{
        std::move(schema), 3,
        [](const std::vector<std::string>&,
           const runtime::Selection* selection) -> std::expected<runtime::Table, std::string> {
            REQUIRE(selection == nullptr);
            runtime::Table out;
            runtime::ValidityBitmap validity(3, true);
            validity.set(1, false);
            out.add_column("k", Column<std::int64_t>{{7, 0, 7}}, std::move(validity));
            out.logical_rows = 3;
            return out;
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(7);
    filter.in_list = {7};

    const std::string key = "k";
    auto table = lazy.project_where({"k"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 2);
}

TEST_CASE("LazyTable: a barely-rejecting membership filter is abandoned",
          "[runtime][lazy_table][deferred_scan]") {
    // The escape hatch: past the sample threshold, a filter that passes
    // (nearly) everything must not push the payload onto the gather path.
    static constexpr std::size_t kRows = 200000;
    std::vector<std::optional<runtime::Selection>> selections;
    runtime::Table schema;
    schema.add_column("k", Column<std::int64_t>{});
    schema.add_column("v", Column<double>{});
    runtime::LazyTable lazy{
        std::move(schema), kRows,
        [&selections](const std::vector<std::string>& names, const runtime::Selection* selection)
            -> std::expected<runtime::Table, std::string> {
            selections.push_back(selection == nullptr ? std::nullopt : std::optional{*selection});
            runtime::Table out;
            for (const auto& name : names) {
                if (name == "k") {
                    std::vector<std::int64_t> values(kRows);
                    for (std::size_t row = 0; row < kRows; ++row) {
                        values[row] = static_cast<std::int64_t>(row % 100);
                    }
                    out.add_column("k", Column<std::int64_t>{std::move(values)});
                } else {
                    out.add_column("v", Column<double>{std::vector<double>(kRows, 1.0)});
                }
            }
            out.logical_rows = kRows;
            return out;
        }};

    // Build side saw every key value: pass rate 1.0 -> abandon.
    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(100);
    for (std::int64_t key = 0; key < 100; ++key) {
        filter.bloom->insert(key);
    }

    const std::string key = "k";
    auto table = lazy.project_where({"k", "v"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == kRows);
    for (const auto& selection : selections) {
        CHECK_FALSE(selection.has_value());
    }
}

TEST_CASE("LazyTable: a selective membership filter prunes past the sample threshold",
          "[runtime][lazy_table][deferred_scan]") {
    static constexpr std::size_t kRows = 200000;
    runtime::Table schema;
    schema.add_column("k", Column<std::int64_t>{});
    runtime::LazyTable lazy{
        std::move(schema), kRows,
        [](const std::vector<std::string>& names,
           const runtime::Selection* selection) -> std::expected<runtime::Table, std::string> {
            REQUIRE(selection == nullptr);
            runtime::Table out;
            for (const auto& name : names) {
                if (name == "k") {
                    std::vector<std::int64_t> values(kRows);
                    for (std::size_t row = 0; row < kRows; ++row) {
                        values[row] = static_cast<std::int64_t>(row % 100);
                    }
                    out.add_column("k", Column<std::int64_t>{std::move(values)});
                }
            }
            out.logical_rows = kRows;
            return out;
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(42);
    filter.in_list = {42};

    const std::string key = "k";
    auto table = lazy.project_where({"k"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == kRows / 100);
}

TEST_CASE("LazyTable: a fused key filter scan replaces the decode-then-filter path",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    std::vector<std::string> scanned_keys;
    runtime::LazyTable lazy{
        source.schema(),
        3,
        [&source](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        },
        {},
        [&scanned_keys](const std::string& key, const runtime::DynamicScanFilter&,
                        const runtime::ExecutionContext&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            scanned_keys.push_back(key);
            return std::optional{runtime::Selection{1, 2}};
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    CHECK(scanned_keys == std::vector<std::string>{"a"});
    // One decode call, already selected — the key column was never
    // materialized whole-file.
    REQUIRE(source.selections.size() == 1);
    REQUIRE(source.selections.front().has_value());
    CHECK(*source.selections.front() == runtime::Selection{1, 2});
    CHECK(source.decode_calls.front() == std::vector<std::string>{"a", "b"});
}

TEST_CASE("LazyTable: a fused scan with no answer falls back to decode-then-filter",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    runtime::LazyTable lazy{
        source.schema(),
        3,
        [&source](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        },
        {},
        [](const std::string&, const runtime::DynamicScanFilter&, const runtime::ExecutionContext&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            return std::optional<runtime::Selection>{};  // no fused answer
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);
    filter.in_list = {2, 3};

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    // Ordinary path: key decoded whole-file first, payload through the
    // filtered selection.
    REQUIRE(source.selections.size() == 2);
    CHECK_FALSE(source.selections.front().has_value());
    REQUIRE(source.selections.back().has_value());
    CHECK(*source.selections.back() == runtime::Selection{1, 2});
}

TEST_CASE("LazyTable: a fused scan error surfaces", "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    runtime::LazyTable lazy{
        source.schema(),
        3,
        [&source](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        },
        {},
        [](const std::string&, const runtime::DynamicScanFilter&, const runtime::ExecutionContext&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            return std::unexpected("fused scan boom");
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(2);

    const std::string key = "a";
    auto table = lazy.project_where({"a"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE_FALSE(table);
    CHECK(table.error() == "fused scan boom");
}

TEST_CASE("LazyTable: a cached key column bypasses the fused scan",
          "[runtime][lazy_table][deferred_scan]") {
    // Another consumer already decoded the key whole-file (a second scan
    // instance, q17-style); filtering the cached values in memory beats
    // re-reading pages, so the fused scan must not run.
    FakeSource source;
    bool fused_called = false;
    runtime::LazyTable lazy{
        source.schema(),
        3,
        [&source](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        },
        {},
        [&fused_called](const std::string&, const runtime::DynamicScanFilter&,
                        const runtime::ExecutionContext&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            fused_called = true;
            return std::optional{runtime::Selection{}};
        }};

    REQUIRE(lazy.project({"a"}, kExec));

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);
    filter.in_list = {2, 3};

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, kExec, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    CHECK_FALSE(fused_called);
}

TEST_CASE("LazyTable: project_rows decodes only the selected rows",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);
    auto table = lazy.project_rows({"a", "c"}, runtime::Selection{0, 2}, kExec);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    REQUIRE(source.selections.size() == 1);
    REQUIRE(source.selections.front().has_value());
    CHECK(*source.selections.front() == runtime::Selection{0, 2});
    const auto* c = std::get_if<Column<std::string>>(&*table->find("c"));
    REQUIRE(c != nullptr);
    CHECK((*c)[0] == "x");
    CHECK((*c)[1] == "z");
}

TEST_CASE("LazyTable: join_key_selection returns the selection and its key values",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);
    filter.in_list = {2, 3};

    auto phase = lazy.join_key_selection({}, kExec, nullptr, filter, "a");
    REQUIRE(phase);
    REQUIRE(phase->has_value());
    CHECK((*phase)->selected == runtime::Selection{1, 2});
    const auto* keys = std::get_if<Column<std::int64_t>>(&*(*phase)->keys.column);
    REQUIRE(keys != nullptr);
    REQUIRE(keys->size() == 2);
    CHECK((*keys)[0] == 2);
    CHECK((*keys)[1] == 3);
    // Only the key column was decoded (whole-file, cached).
    REQUIRE(source.decode_calls.size() == 1);
    CHECK(source.decode_calls.front() == std::vector<std::string>{"a"});
}

TEST_CASE("LazyTable: join_key_selection composes static conjuncts with membership",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(1);
    filter.bloom->insert(2);
    filter.in_list = {1, 2};

    // Conjunct keeps a > 1 -> {2, 3}; membership keeps {1, 2} -> exactly {2}.
    auto phase = lazy.join_key_selection({greater_than("a", 1)}, kExec, nullptr, filter, "a");
    REQUIRE(phase);
    REQUIRE(phase->has_value());
    CHECK((*phase)->selected == runtime::Selection{1});
    const auto* keys = std::get_if<Column<std::int64_t>>(&*(*phase)->keys.column);
    REQUIRE(keys != nullptr);
    REQUIRE(keys->size() == 1);
    CHECK((*keys)[0] == 2);
}

TEST_CASE("LazyTable: join_key_selection declines without membership or on a non-int key",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    const runtime::DynamicScanFilter empty_filter;
    auto no_membership = lazy.join_key_selection({}, kExec, nullptr, empty_filter, "a");
    REQUIRE(no_membership);
    CHECK_FALSE(no_membership->has_value());

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(2);
    auto non_int = lazy.join_key_selection({}, kExec, nullptr, filter, "b");
    REQUIRE(non_int);
    CHECK_FALSE(non_int->has_value());
}

TEST_CASE("LazyTable: join_key_selection uses the fused scan when available",
          "[runtime][lazy_table][deferred_scan]") {
    FakeSource source;
    runtime::LazyTable lazy{
        source.schema(),
        3,
        [&source](const std::vector<std::string>& names, const runtime::Selection* selection) {
            return source.decode(names, selection);
        },
        {},
        [](const std::string&, const runtime::DynamicScanFilter&, const runtime::ExecutionContext&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            return std::optional{runtime::Selection{2}};
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(3);

    auto phase = lazy.join_key_selection({}, kExec, nullptr, filter, "a");
    REQUIRE(phase);
    REQUIRE(phase->has_value());
    CHECK((*phase)->selected == runtime::Selection{2});
    // The key was decoded through the fused selection, never whole-file.
    REQUIRE(source.selections.size() == 1);
    REQUIRE(source.selections.front().has_value());
    CHECK(*source.selections.front() == runtime::Selection{2});
    const auto* keys = std::get_if<Column<std::int64_t>>(&*(*phase)->keys.column);
    REQUIRE(keys != nullptr);
    REQUIRE(keys->size() == 1);
    CHECK((*keys)[0] == 3);
}

TEST_CASE("LazyTable: predicate-only literal range uses the source key scan",
          "[runtime][lazy_table][range_filter]") {
    auto state = std::make_shared<TextSourceState>();
    auto lazy = make_text_lazy(state);

    auto table = lazy.project_where({"s"}, {greater_than("n", 1)}, kExec);
    REQUIRE(table);
    CHECK(state->key_scan_calls == std::vector<std::string>{"n"});
    for (const auto& call : state->decode_calls)
        CHECK(std::ranges::find(call, "n") == call.end());
}

TEST_CASE("LazyTable: a filter-only string column is never materialized",
          "[runtime][lazy_table][string_filter]") {
    auto state = std::make_shared<TextSourceState>();
    auto lazy = make_text_lazy(state);

    // `s` is read by the filter and by nothing else, so the source evaluates
    // the pattern during its own decode and hands back rows.
    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(not_like("s", "%bad%"));
    auto table = lazy.project_where({"n"}, conjuncts, kExec);
    REQUIRE(table);
    CHECK(int_column(*table, "n") == std::vector<std::int64_t>{0, 2, 4, 5});

    CHECK(state->scan_calls == std::vector<std::string>{"s"});
    for (const auto& call : state->decode_calls) {
        CHECK(std::ranges::find(call, "s") == call.end());
    }
}

TEST_CASE("LazyTable: a string column the projection wants is still materialized",
          "[runtime][lazy_table][string_filter]") {
    auto state = std::make_shared<TextSourceState>();
    auto lazy = make_text_lazy(state);

    // Same predicate, but now `s` is in the output. The fused scan would
    // answer the filter and leave the column undecodable in one pass, so the
    // ordinary decode-then-filter path has to stand.
    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(not_like("s", "%bad%"));
    auto table = lazy.project_where({"n", "s"}, conjuncts, kExec);
    REQUIRE(table);
    CHECK(int_column(*table, "n") == std::vector<std::int64_t>{0, 2, 4, 5});
    CHECK(state->scan_calls.empty());
}

TEST_CASE("LazyTable: a fused string filter ANDs with the conjuncts left behind",
          "[runtime][lazy_table][string_filter]") {
    auto state = std::make_shared<TextSourceState>();
    auto lazy = make_text_lazy(state);

    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(not_like("s", "%bad%"));
    conjuncts.push_back(greater_than("n", 1));
    auto table = lazy.project_where({"n"}, conjuncts, kExec);
    REQUIRE(table);
    // rows 0,2,4,5 pass the pattern; n > 1 keeps 2,4,5.
    CHECK(int_column(*table, "n") == std::vector<std::int64_t>{2, 4, 5});
    CHECK(state->scan_calls == std::vector<std::string>{"s"});
    for (const auto& call : state->decode_calls) {
        CHECK(std::ranges::find(call, "s") == call.end());
    }
}

TEST_CASE("LazyTable: a source that declines the fused scan still filters correctly",
          "[runtime][lazy_table][string_filter]") {
    auto state = std::make_shared<TextSourceState>();
    state->fused = false;
    auto lazy = make_text_lazy(state);

    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(not_like("s", "%bad%"));
    auto table = lazy.project_where({"n"}, conjuncts, kExec);
    REQUIRE(table);
    CHECK(int_column(*table, "n") == std::vector<std::int64_t>{0, 2, 4, 5});

    // Asked, declined, fell back to reading the column whole.
    CHECK(state->scan_calls == std::vector<std::string>{"s"});
    bool decoded_text = false;
    for (const auto& call : state->decode_calls) {
        decoded_text = decoded_text || std::ranges::find(call, "s") != call.end();
    }
    CHECK(decoded_text);
}

TEST_CASE("LazyTable: an unnegated like fuses too", "[runtime][lazy_table][string_filter]") {
    auto state = std::make_shared<TextSourceState>();
    auto lazy = make_text_lazy(state);

    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(like_call("s", "%bad%"));
    auto table = lazy.project_where({"n"}, conjuncts, kExec);
    REQUIRE(table);
    CHECK(int_column(*table, "n") == std::vector<std::int64_t>{1, 3});
    CHECK(state->scan_calls == std::vector<std::string>{"s"});
}

// --- Streaming a scan in units -------------------------------------------
//
// Phase 1 of plans/pipelined-execution-plan.md. The whole point of the design
// is that a unit-at-a-time scan applies the same pushdowns as the whole-source
// one, so the tests that matter are equality tests against `project_where`
// rather than tests of the streaming path in isolation: two code paths that are
// supposed to agree will not stay in step unless something checks.

namespace {

/// `project_where` run unit by unit and glued back together — what a streaming
/// scan operator does with `project_where_unit`.
struct Streamed {
    std::vector<std::int64_t> n;
    std::vector<std::string> s;
    std::size_t units = 0;
};

auto stream_units(runtime::LazyTable& lazy, const std::set<std::string>& names,
                  const std::vector<ir::Expr>& conjuncts) -> Streamed {
    Streamed out;
    for (const auto& unit : lazy.scan_units()) {
        auto part = lazy.project_where_unit(names, conjuncts, unit, kExec);
        REQUIRE(part.has_value());
        ++out.units;
        if (const auto* entry = part->find_entry("n"); entry != nullptr) {
            const auto* column = std::get_if<Column<std::int64_t>>(&*entry->column);
            REQUIRE(column != nullptr);
            out.n.insert(out.n.end(), column->begin(), column->end());
        }
        if (const auto* entry = part->find_entry("s"); entry != nullptr) {
            const auto* column = std::get_if<Column<std::string>>(&*entry->column);
            REQUIRE(column != nullptr);
            for (std::size_t row = 0; row < column->size(); ++row) {
                out.s.emplace_back((*column)[row]);
            }
        }
    }
    return out;
}

auto string_column(const runtime::Table& table, const std::string& name)
    -> std::vector<std::string> {
    const auto* entry = table.find_entry(name);
    REQUIRE(entry != nullptr);
    const auto* column = std::get_if<Column<std::string>>(&*entry->column);
    REQUIRE(column != nullptr);
    std::vector<std::string> out;
    out.reserve(column->size());
    for (std::size_t row = 0; row < column->size(); ++row) {
        out.emplace_back((*column)[row]);
    }
    return out;
}

}  // namespace

TEST_CASE("LazyTable: units concatenate to the whole-source projection",
          "[runtime][lazy_table][stream]") {
    auto whole_state = std::make_shared<TextSourceState>();
    auto whole_lazy = make_text_lazy(whole_state);
    auto stream_state = std::make_shared<TextSourceState>();
    auto stream_lazy = make_text_lazy(stream_state);

    SECTION("no predicate at all") {
        auto whole = whole_lazy.project_where({"n", "s"}, {}, kExec);
        REQUIRE(whole);
        const auto streamed = stream_units(stream_lazy, {"n", "s"}, {});
        CHECK(streamed.units == 3);
        CHECK(streamed.n == int_column(*whole, "n"));
        CHECK(streamed.s == string_column(*whole, "s"));
    }

    SECTION("a conjunct the source cannot fuse") {
        // `n > 1` is evaluated in memory over the decoded column, so this is
        // the path where the selection is computed per unit and has to be
        // rebased before it indexes that unit's rows.
        std::vector<ir::Expr> conjuncts;
        conjuncts.push_back(greater_than("n", 1));
        auto whole = whole_lazy.project_where({"n", "s"}, conjuncts, kExec);
        REQUIRE(whole);
        const auto streamed = stream_units(stream_lazy, {"n", "s"}, conjuncts);
        CHECK(streamed.n == int_column(*whole, "n"));
        CHECK(streamed.n == std::vector<std::int64_t>{2, 3, 4, 5});
        CHECK(streamed.s == string_column(*whole, "s"));
    }

    SECTION("a fused string scan, restricted to each unit") {
        // The fused scan answers in source-global indices for the whole file;
        // restricted to a unit it must answer for that unit alone, or the rows
        // it names will not exist in the unit's decoded columns.
        std::vector<ir::Expr> conjuncts;
        conjuncts.push_back(like_call("s", "%bad%"));
        auto whole = whole_lazy.project_where({"n"}, conjuncts, kExec);
        REQUIRE(whole);
        const auto streamed = stream_units(stream_lazy, {"n"}, conjuncts);
        CHECK(streamed.n == int_column(*whole, "n"));
        CHECK(streamed.n == std::vector<std::int64_t>{1, 3});
        // Asked once per unit, and the text column was never materialized.
        CHECK(stream_state->scan_calls == std::vector<std::string>{"s", "s", "s"});
        for (const auto& call : stream_state->decode_calls) {
            CHECK(std::ranges::find(call, "s") == call.end());
        }
    }

    SECTION("a fused scan the source declines") {
        stream_state->fused = false;
        whole_state->fused = false;
        std::vector<ir::Expr> conjuncts;
        conjuncts.push_back(not_like("s", "%bad%"));
        auto whole = whole_lazy.project_where({"n"}, conjuncts, kExec);
        REQUIRE(whole);
        const auto streamed = stream_units(stream_lazy, {"n"}, conjuncts);
        CHECK(streamed.n == int_column(*whole, "n"));
    }
}

TEST_CASE("LazyTable: a unit decode never enters the whole-column cache",
          "[runtime][lazy_table][stream]") {
    // A unit holds a fragment of a column. If one reached `cache_`, every later
    // reader would treat those few rows as the whole column — and the fused
    // scans, which decline when their key column is already cached, would
    // decline on the strength of a fragment.
    auto state = std::make_shared<TextSourceState>();
    auto lazy = make_text_lazy(state);

    std::vector<ir::Expr> conjuncts;
    conjuncts.push_back(greater_than("n", 1));
    const auto streamed = stream_units(lazy, {"n", "s"}, conjuncts);
    CHECK(streamed.n.size() == 4);

    // A whole-source call afterwards still sees every row, and still fuses.
    state->scan_calls.clear();
    std::vector<ir::Expr> fusable;
    fusable.push_back(like_call("s", "%bad%"));
    auto whole = lazy.project_where({"n"}, fusable, kExec);
    REQUIRE(whole);
    CHECK(int_column(*whole, "n") == std::vector<std::int64_t>{1, 3});
    CHECK(state->scan_calls == std::vector<std::string>{"s"});
}

TEST_CASE("LazyTable: a source with no decomposition reports no units",
          "[runtime][lazy_table][stream]") {
    // The decline path. `scan_units` empty is how a caller learns to keep
    // materializing whole, and it must not be an error or an empty scan.
    auto state = std::make_shared<TextSourceState>();
    state->units = false;
    auto lazy = make_text_lazy(state);
    CHECK(lazy.scan_units().empty());
    auto whole = lazy.project_where({"n", "s"}, {}, kExec);
    REQUIRE(whole);
    CHECK(int_column(*whole, "n").size() == kText.size());
}
