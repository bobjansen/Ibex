#include <ibex/runtime/lazy_table.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <numeric>
#include <optional>
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

TEST_CASE("LazyTable: project_where decodes predicates before selected payload columns",
          "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.project_where({"a", "b"}, {greater_than("a", 1)});
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

TEST_CASE("LazyTable: project_where evaluates staged predicates before selected payload",
          "[runtime][lazy_table]") {
    FakeSource source;
    auto lazy = make_lazy(source);

    auto table = lazy.project_where(
        {"a", "b", "c"}, {greater_than("a", 0), greater_than("a", 1), less_than("b", 3.0)});
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

    auto table = lazy.project_where({"c"}, {greater_than("a", 6), less_than("b", 20.0)});
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

    REQUIRE(lazy.project_where({"a", "b"}, {greater_than("a", 1)}));
    REQUIRE(source.decode_calls.size() == 2);

    // The predicate column was decoded whole-file for the selection, so it is
    // a legitimate cache entry: projecting it later costs no decode and comes
    // back full-length.
    auto whole_a = lazy.project({"a"});
    REQUIRE(whole_a);
    CHECK(whole_a->rows() == 3);
    CHECK(source.decode_calls.size() == 2);
    const auto* a = std::get_if<Column<std::int64_t>>(whole_a->find("a"));
    REQUIRE(a != nullptr);
    CHECK((*a)[0] == 1);
    CHECK((*a)[2] == 3);

    // The payload column was decoded under a selection, so it must NOT be
    // cached: projecting it whole decodes it again, densely.
    auto whole_b = lazy.project({"b"});
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

    REQUIRE(lazy.project({"a"}));
    REQUIRE(source.decode_calls.size() == 1);

    // `a` is already cached whole-file, so only the payload column is decoded.
    auto table = lazy.project_where({"b"}, {greater_than("a", 1)});
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

    auto table = lazy.project({"a"});
    REQUIRE_FALSE(table);
    CHECK(table.error() == "boom");
}

TEST_CASE("lazy table carries source column stats without decoding", "[lazy_table]") {
    // A source's metadata is what the planner gets to reason from before any
    // page is read. Reaching for it must not pull data in behind it.
    FakeSource source;
    runtime::SourceColumnStats stats;
    stats.emplace("a", runtime::ColumnStats{.min = 1, .max = 3, .null_count = 0});
    runtime::LazyTable lazy(
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
    runtime::LazyTable lazy(
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
    auto table = lazy.project_where({"a", "b"}, {}, nullptr, &filter, &key);
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
    auto table = lazy.project_where({"a", "b"}, {greater_than("a", 1)}, nullptr, &filter, &key);
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
    auto table = lazy.project_where({"a", "b"}, {}, nullptr, &filter, &key);
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
    auto table = lazy.project_where({"k"}, {}, nullptr, &filter, &key);
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
    auto table = lazy.project_where({"k", "v"}, {}, nullptr, &filter, &key);
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
    auto table = lazy.project_where({"k"}, {}, nullptr, &filter, &key);
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
        [&scanned_keys](const std::string& key, const runtime::DynamicScanFilter&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            scanned_keys.push_back(key);
            return std::optional{runtime::Selection{1, 2}};
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, nullptr, &filter, &key);
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
        [](const std::string&, const runtime::DynamicScanFilter&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            return std::optional<runtime::Selection>{};  // no fused answer
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);
    filter.in_list = {2, 3};

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, nullptr, &filter, &key);
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
        [](const std::string&, const runtime::DynamicScanFilter&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            return std::unexpected("fused scan boom");
        }};

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(1);
    filter.bloom->insert(2);

    const std::string key = "a";
    auto table = lazy.project_where({"a"}, {}, nullptr, &filter, &key);
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
        [&fused_called](const std::string&, const runtime::DynamicScanFilter&)
            -> std::expected<std::optional<runtime::Selection>, std::string> {
            fused_called = true;
            return std::optional{runtime::Selection{}};
        }};

    REQUIRE(lazy.project({"a"}));

    runtime::DynamicScanFilter filter;
    filter.bloom.emplace(2);
    filter.bloom->insert(2);
    filter.bloom->insert(3);
    filter.in_list = {2, 3};

    const std::string key = "a";
    auto table = lazy.project_where({"a", "b"}, {}, nullptr, &filter, &key);
    REQUIRE(table);
    CHECK(table->rows() == 2);
    CHECK_FALSE(fused_called);
}
