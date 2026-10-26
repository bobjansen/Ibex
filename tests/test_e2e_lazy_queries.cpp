// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// End-to-end query correctness through the whole-script planner with LAZY
// sources. `test_e2e.cpp` runs `parser::lower` + `runtime::interpret` directly
// and so never sees the batch planner's join reordering, functional-dependency
// group-key reduction, deferred probes or streaming scans -- the passes that
// only fire in `try_execute_whole_script`. These shapes (multi-join, self-join,
// deferred-probe, reused filtered scan) are where those passes have shipped
// silent wrong-answer regressions that compiled and passed the fast unit
// suite. Each case asserts the exact output.

#include <ibex/core/column.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {

using ibex::Column;
using ibex::runtime::ExternRegistry;
using ibex::runtime::Selection;
using ibex::runtime::Table;

/// One row-slice of a `ColumnValue`, preserving element type and value metadata.
auto slice_column(const ibex::runtime::ColumnValue& src, const Selection& rows)
    -> ibex::runtime::ColumnValue {
    return std::visit(
        [&](const auto& col) -> ibex::runtime::ColumnValue {
            using C = std::decay_t<decltype(col)>;
            C out;
            out.reserve(rows.size());
            for (const auto row : rows) {
                out.push_back(col[row]);
            }
            return ibex::runtime::with_meta_of(ibex::runtime::ColumnValue{std::move(out)}, src);
        },
        src);
}

/// Zero-row copy of `src` -- a schema column of the right type.
auto empty_like(const ibex::runtime::ColumnValue& src) -> ibex::runtime::ColumnValue {
    return std::visit(
        [&](const auto& col) -> ibex::runtime::ColumnValue {
            using C = std::decay_t<decltype(col)>;
            return ibex::runtime::with_meta_of(ibex::runtime::ColumnValue{C{}}, src);
        },
        src);
}

/// Register `data` as a lazy source named `name`. The decode honours the
/// selection (so `project_where` and deferred-probe filters exercise the real
/// gather path) and returns exactly the requested columns.
///
/// `live` is read when a scan decodes, not when the source is opened, so a sink
/// that replaces `*live` is seen by every scan that runs after it -- the way a
/// file rewritten between two reads behaves.
///
/// `decodes`, when given, records how many rows each decode served
/// (`kWholeSource` for a decode with no selection).
constexpr std::size_t kWholeSource = std::numeric_limits<std::size_t>::max();

void register_live_source(ExternRegistry& registry, std::string name, std::shared_ptr<Table> live,
                          std::shared_ptr<std::vector<std::size_t>> decodes = nullptr) {
    registry.register_lazy_table(
        std::move(name),
        [live = std::move(live), decodes](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            Table schema;
            for (const auto& col : live->columns) {
                schema.add_column(col.name, empty_like(*col.column));
            }
            const std::size_t rows = live->rows();
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), rows,
                [live, decodes](const std::vector<std::string>& names,
                                const Selection* selection) -> std::expected<Table, std::string> {
                    if (decodes != nullptr) {
                        decodes->push_back(selection == nullptr ? kWholeSource : selection->size());
                    }
                    const Table& data = *live;
                    Selection all;
                    all.reserve(data.rows());
                    for (std::size_t i = 0; i < data.rows(); ++i) {
                        all.push_back(i);
                    }
                    const Selection& want = selection == nullptr ? all : *selection;
                    Table out;
                    for (const auto& col_name : names) {
                        const auto* entry = data.find_entry(col_name);
                        if (entry == nullptr) {
                            return std::unexpected("lazy source: unknown column " + col_name);
                        }
                        if (entry->validity.has_value()) {
                            ibex::runtime::ValidityBitmap validity(want.size(), true);
                            for (std::size_t i = 0; i < want.size(); ++i) {
                                validity.set(i, (*entry->validity)[want[i]]);
                            }
                            out.add_column(col_name, slice_column(*entry->column, want),
                                           std::move(validity));
                        } else {
                            out.add_column(col_name, slice_column(*entry->column, want));
                        }
                    }
                    out.logical_rows = want.size();
                    return out;
                });
        });
}

void register_lazy_source(ExternRegistry& registry, std::string name, Table data) {
    register_live_source(registry, std::move(name), std::make_shared<Table>(std::move(data)));
}

/// Run `script` through the whole-script planner and return the table handed to
/// its `capture(df)` sink. Fails the test if the script does not execute.
auto run_lazy_script(const char* script, std::vector<std::pair<std::string, Table>> sources)
    -> Table {
    ExternRegistry registry;
    for (auto& [name, data] : sources) {
        register_lazy_source(registry, std::move(name), std::move(data));
    }
    Table captured;
    bool did_capture = false;
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&](const Table& t, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            captured = t;
            did_capture = true;
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });
    REQUIRE(ibex::repl::execute_script(script, registry));
    REQUIRE(did_capture);
    return captured;
}

auto i64(Table& t, const char* name) -> std::vector<std::int64_t> {
    const auto* col = std::get_if<Column<std::int64_t>>(t.find(name));
    REQUIRE(col != nullptr);
    return {col->begin(), col->end()};
}

auto rows_of(Table& t, const std::vector<const char*>& cols)
    -> std::set<std::vector<std::int64_t>> {
    std::vector<std::vector<std::int64_t>> columns;
    columns.reserve(cols.size());
    for (const auto* name : cols) {
        columns.push_back(i64(t, name));
    }
    std::set<std::vector<std::int64_t>> out;
    for (std::size_t r = 0; r < columns.front().size(); ++r) {
        std::vector<std::int64_t> row;
        row.reserve(columns.size());
        for (const auto& c : columns) {
            row.push_back(c[r]);
        }
        out.insert(std::move(row));
    }
    return out;
}

Table make_table(std::vector<std::pair<std::string, Column<std::int64_t>>> cols) {
    Table t;
    for (auto& [name, col] : cols) {
        t.add_column(name, std::move(col));
    }
    return t;
}

}  // namespace

TEST_CASE("e2e lazy: filtered reused scan feeds an inner join", "[e2e][lazy]") {
    // `lineitem` is scanned twice with different filters, then joined. The
    // scan-instance work must not let one occurrence's selection leak into the
    // shared decode, and the join must pair the right rows.
    Table orders = make_table({{"o_orderkey", {1, 2, 3, 4}}, {"o_flag", {1, 0, 1, 1}}});
    Table lineitem =
        make_table({{"l_orderkey", {1, 1, 2, 3, 4, 4}}, {"l_qty", {5, 20, 30, 7, 40, 50}}});

    const char* src = R"(
extern fn read_orders() -> DataFrame from "x.hpp";
extern fn read_lineitem() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let orders = read_orders();
let lineitem = read_lineitem();
let big = lineitem[filter l_qty > 10, select { l_orderkey, l_qty }];
let flagged = orders[filter o_flag > 0, select { o_orderkey }];
let joined = big join flagged on l_orderkey == o_orderkey;
capture(joined);
joined;
)";
    Table out = run_lazy_script(
        src, {{"read_orders", std::move(orders)}, {"read_lineitem", std::move(lineitem)}});

    // big: (1,20),(2,30),(4,40),(4,50).  flagged orderkeys: 1,3,4.
    // join -> (1,20),(4,40),(4,50)
    REQUIRE(rows_of(out, {"l_orderkey", "l_qty"}) ==
            std::set<std::vector<std::int64_t>>{{1, 20}, {4, 40}, {4, 50}});
}

TEST_CASE("e2e lazy: self-join keeps both occurrences distinct", "[e2e][lazy]") {
    // q21 / group-key-reduction shape: a source self-joined on a non-unique
    // column. A unique-key proof on one occurrence must not collapse rows of
    // the other.
    Table t = make_table({{"pk", {1, 2, 3, 4}}, {"grp", {10, 10, 20, 20}}});

    const char* src = R"(
extern fn read_t() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let a = read_t();
let b = read_t();
let bside = b[select { grp }];
let paired = a join bside on grp;
let result = paired[select { n = count() }, by { pk }];
capture(result);
result;
)";
    Table out = run_lazy_script(src, {{"read_t", std::move(t)}});

    // each pk joins to 2 rows sharing its grp -> count 2 for every pk
    REQUIRE(rows_of(out, {"pk", "n"}) ==
            std::set<std::vector<std::int64_t>>{{1, 2}, {2, 2}, {3, 2}, {4, 2}});
}

TEST_CASE("e2e lazy: three-way join chain", "[e2e][lazy]") {
    // q03/q10 shape -- customer -> orders -> lineitem, filtered on the ends,
    // aggregated. Exercises join reordering across three inputs.
    Table customer = make_table({{"c_custkey", {1, 2, 3}}, {"c_seg", {1, 2, 1}}});
    Table orders = make_table({{"o_orderkey", {10, 11, 12, 13}}, {"o_custkey", {1, 1, 2, 3}}});
    Table lineitem =
        make_table({{"l_orderkey", {10, 10, 11, 12, 13}}, {"l_rev", {100, 50, 200, 300, 400}}});

    const char* src = R"(
extern fn read_customer() -> DataFrame from "x.hpp";
extern fn read_orders() -> DataFrame from "x.hpp";
extern fn read_lineitem() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let customer = read_customer();
let orders = read_orders();
let lineitem = read_lineitem();
let seg = customer[filter c_seg == 1, select { c_custkey }];
let co = seg join orders on c_custkey == o_custkey;
let col = co join lineitem on o_orderkey == l_orderkey;
let result = col[select { rev = sum(l_rev) }, by { c_custkey }];
capture(result);
result;
)";
    Table out = run_lazy_script(src, {{"read_customer", std::move(customer)},
                                      {"read_orders", std::move(orders)},
                                      {"read_lineitem", std::move(lineitem)}});

    // seg custkeys: 1,3.  cust 1 -> orders 10,11 -> lines 100,50,200 = 350.
    // cust 3 -> order 13 -> line 400.
    REQUIRE(rows_of(out, {"c_custkey", "rev"}) ==
            std::set<std::vector<std::int64_t>>{{1, 350}, {3, 400}});
}

TEST_CASE("e2e lazy: `nulls equal` join over a deferrable probe keeps null-keyed rows",
          "[e2e][lazy][join][regression]") {
    // A selective build side over a much larger probe source is the shape the
    // deferred-probe pass targets. Its key filter skips null keys, which is
    // right for `nulls never` and would lose every null-to-null pair here, so
    // a `nulls equal` join must not be narrowed by it.
    constexpr std::size_t probe_rows = 4000;
    constexpr std::size_t build_rows = 400;
    Table probe;
    Table build;
    {
        Column<std::int64_t> id;
        Column<std::int64_t> v;
        ibex::runtime::ValidityBitmap valid(probe_rows, true);
        for (std::size_t i = 0; i < probe_rows; ++i) {
            id.push_back(static_cast<std::int64_t>(i % 100));
            v.push_back(static_cast<std::int64_t>(i));
            valid.set(i, i % 10 != 0);
        }
        probe.add_column("id", std::move(id), valid);
        probe.add_column("v", std::move(v));
    }
    {
        Column<std::int64_t> id;
        Column<std::int64_t> flag;
        ibex::runtime::ValidityBitmap valid(build_rows, true);
        for (std::size_t i = 0; i < build_rows; ++i) {
            id.push_back(static_cast<std::int64_t>(i % 5));
            flag.push_back(i < 8 ? 1 : 0);
            valid.set(i, i % 4 != 0);
        }
        build.add_column("id", std::move(id), valid);
        build.add_column("flag", std::move(flag));
    }

    // The 8 flagged build rows carry keys null,1,2,3,null,0,1,2. Probe keys
    // are null on every tenth row and i % 100 otherwise.
    std::int64_t expected_equal = 0;
    std::int64_t expected_never = 0;
    for (std::size_t i = 0; i < probe_rows; ++i) {
        const bool probe_null = i % 10 == 0;
        const auto key = static_cast<std::int64_t>(i % 100);
        for (std::size_t b = 0; b < 8; ++b) {
            const bool build_null = b % 4 == 0;
            const auto build_key = static_cast<std::int64_t>(b % 5);
            if (probe_null && build_null) {
                ++expected_equal;
            } else if (!probe_null && !build_null && key == build_key) {
                ++expected_equal;
                ++expected_never;
            }
        }
    }
    REQUIRE(expected_equal > expected_never);

    for (const bool nulls_equal : {true, false}) {
        CAPTURE(nulls_equal);
        const std::string src = std::string(R"(
extern fn read_build() -> DataFrame from "x.hpp";
extern fn read_probe() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let build = read_build();
let probe = read_probe();
let small = build[filter flag == 1, select { id }];
let joined = small join probe on id)") +
                                (nulls_equal ? " nulls equal" : "") +
                                R"(;
let result = joined[select { n = count() }];
capture(result);
result;
)";
        Table out = run_lazy_script(src.c_str(), {{"read_build", build}, {"read_probe", probe}});
        REQUIRE(i64(out, "n") ==
                std::vector<std::int64_t>{nulls_equal ? expected_equal : expected_never});
    }
}

TEST_CASE("e2e lazy: deferred probe against a self-referenced source", "[e2e][lazy]") {
    // q18 shape: lineitem drives an aggregate AND is the probe side of the main
    // join. `isolate_deferrable_probe_scans` renames only the probe occurrence;
    // the answer must be unaffected.
    Table orders = make_table({{"o_orderkey", {1, 2, 3}}});
    Table lineitem =
        make_table({{"l_orderkey", {1, 1, 1, 2, 3, 3}}, {"l_qty", {10, 20, 30, 5, 40, 50}}});

    const char* src = R"(
extern fn read_orders() -> DataFrame from "x.hpp";
extern fn read_lineitem() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let orders = read_orders();
let lineitem = read_lineitem();
let bykey = lineitem[select { total = sum(l_qty) }, by { l_orderkey }];
let big = bykey[filter total > 50, select { k = l_orderkey }];
let chosen = orders join big on o_orderkey == k;
let detail = chosen join lineitem on o_orderkey == l_orderkey;
let result = detail[select { s = sum(l_qty) }, by { o_orderkey }];
capture(result);
result;
)";
    Table out = run_lazy_script(
        src, {{"read_orders", std::move(orders)}, {"read_lineitem", std::move(lineitem)}});

    // totals: key1=60, key2=5, key3=90.  big (>50): key1, key3.
    // detail sums re-aggregate lineitem for those: key1=60, key3=90.
    REQUIRE(rows_of(out, {"o_orderkey", "s"}) ==
            std::set<std::vector<std::int64_t>>{{1, 60}, {3, 90}});
}

TEST_CASE("e2e lazy: grouped then joined", "[e2e][lazy]") {
    Table sales = make_table({{"region", {1, 1, 2, 2, 3}}, {"amount", {10, 15, 20, 5, 30}}});
    Table region_names = make_table({{"region", {1, 2, 3}}, {"quota", {20, 20, 20}}});

    const char* src = R"(
extern fn read_sales() -> DataFrame from "x.hpp";
extern fn read_regions() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let sales = read_sales();
let regions = read_regions();
let totals = sales[select { total = sum(amount) }, by { region }];
let joined = totals join regions on region;
let result = joined[filter total >= quota, select { region, total }];
capture(result);
result;
)";
    Table out = run_lazy_script(
        src, {{"read_sales", std::move(sales)}, {"read_regions", std::move(region_names)}});

    // totals: r1=25, r2=25, r3=30.  all >= 20.
    REQUIRE(rows_of(out, {"region", "total"}) ==
            std::set<std::vector<std::int64_t>>{{1, 25}, {2, 25}, {3, 30}});
}

TEST_CASE("e2e lazy: streaming count_distinct, grouped and global", "[e2e][lazy]") {
    // Exercises the streaming hash-aggregate's count_distinct path (the batch
    // planner routes an aggregate through it; `test_e2e.cpp` only reaches
    // `aggregate_table`). g: 1 1 1 2 2 3 3 ; v per g -> 1{7,7,8}=2  2{9,9}=1
    //  3{4,5}=2 ; global distinct v -> {7,8,9,4,5} = 5.
    Table hits = make_table({{"g", {1, 1, 1, 2, 2, 3, 3}}, {"v", {7, 7, 8, 9, 9, 4, 5}}});

    const char* src = R"(
extern fn read_hits() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let hits = read_hits();
let per_g = hits[select { nd = count_distinct(v) }, by { g }];
capture(per_g);
per_g;
)";
    Table grouped = run_lazy_script(src, {{"read_hits", hits}});
    REQUIRE(rows_of(grouped, {"g", "nd"}) ==
            std::set<std::vector<std::int64_t>>{{1, 2}, {2, 1}, {3, 2}});

    const char* global_src = R"(
extern fn read_hits() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let hits = read_hits();
let total = hits[select { nd = count_distinct(v) }];
capture(total);
total;
)";
    Table global = run_lazy_script(global_src, {{"read_hits", std::move(hits)}});
    REQUIRE(i64(global, "nd") == std::vector<std::int64_t>{5});
}

namespace {

/// A script over a "disk": `read_disk()` reads the current table, `store(df)`
/// replaces it, `capture(df)` records what it is handed.
struct DiskScript {
    ExternRegistry registry;
    std::shared_ptr<Table> disk;
    Table captured;

    explicit DiskScript(Table initial, Table fresh) : disk(std::make_shared<Table>(initial)) {
        register_live_source(registry, "read_disk", disk);
        register_lazy_source(registry, "read_fresh", std::move(fresh));
        registry.register_scalar_table_consumer(
            "store", ibex::runtime::ScalarKind::Int,
            [this](const Table& t, const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                *disk = t;
                return ibex::runtime::ExternValue{std::int64_t{0}};
            });
        registry.register_scalar_table_consumer(
            "capture", ibex::runtime::ScalarKind::Int,
            [this](const Table& t, const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                captured = t;
                return ibex::runtime::ExternValue{std::int64_t{0}};
            });
    }
};

}  // namespace

TEST_CASE("e2e lazy: a shared binding is read after the sink that rewrites its source",
          "[e2e][lazy][effects]") {
    // `total` is read twice, so the lowerer shares it: materialized once by the
    // executor. That used to happen before every sink, so it read the OLD disk
    // (sum 3) although `store` rewrote it first in the source.
    DiskScript script(make_table({{"x", {1, 2}}}), make_table({{"x", {10, 20}}}));
    const char* src = R"(
extern fn read_disk() -> DataFrame from "x.hpp";
extern fn read_fresh() -> DataFrame from "x.hpp";
extern fn store(df: DataFrame) -> Int from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let fresh = read_fresh();
store(fresh);
let total = read_disk()[select { s = sum(x) }];
let lo = total[filter s > 0];
let hi = total[filter s > 1];
let both = lo join hi on s;
capture(both);
both;
)";
    REQUIRE(ibex::repl::execute_script(src, script.registry));
    REQUIRE(i64(script.captured, "s") == std::vector<std::int64_t>{30});
}

TEST_CASE("e2e lazy: a binding is read before the sink declared after it rewrites its source",
          "[e2e][lazy][effects]") {
    // `r` is used once, so it used to be inlined into the plan of `capture(r)`
    // and read AFTER `store` had rewritten the disk (sum 30). In source order
    // `r` reads the old disk (sum 3).
    DiskScript script(make_table({{"x", {1, 2}}}), make_table({{"x", {10, 20}}}));
    const char* src = R"(
extern fn read_disk() -> DataFrame from "x.hpp";
extern fn read_fresh() -> DataFrame from "x.hpp";
extern fn store(df: DataFrame) -> Int from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let r = read_disk()[select { s = sum(x) }];
let fresh = read_fresh();
store(fresh);
capture(r);
r;
)";
    REQUIRE(ibex::repl::execute_script(src, script.registry));
    REQUIRE(i64(script.captured, "s") == std::vector<std::int64_t>{3});
}

namespace {

/// Six rows with a repeated leading key: `a` = 1, 1, 2, 1, 3, 4.
auto head_source() -> Table {
    return make_table({{"a", {1, 1, 2, 1, 3, 4}}, {"b", {10, 20, 30, 40, 50, 60}}});
}

auto run_recorded(const char* script, std::vector<std::size_t>& decodes_out) -> Table {
    ExternRegistry registry;
    auto decodes = std::make_shared<std::vector<std::size_t>>();
    register_live_source(registry, "read_t", std::make_shared<Table>(head_source()), decodes);
    Table captured;
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&](const Table& t, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            captured = t;
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });
    REQUIRE(ibex::repl::execute_script(script, registry));
    decodes_out = *decodes;
    return captured;
}

}  // namespace

// `t[head n]` over a lazy source reads only the source's first n rows. It
// decoded the whole source first: `head 1` over PDS-H lineitem took 400 ms.
TEST_CASE("e2e lazy: head decodes only the leading rows", "[e2e][lazy][head]") {
    for (const char* query : {"t[head 2]", "t[select { a, b }, head 2]"}) {
        INFO(query);
        const std::string src = std::string(R"(
extern fn read_t() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";
let t = read_t();
let h = )") + query + R"(;
capture(h);
h;
)";
        std::vector<std::size_t> decodes;
        Table out = run_recorded(src.c_str(), decodes);
        CHECK(i64(out, "a") == std::vector<std::int64_t>{1, 1});
        CHECK(i64(out, "b") == std::vector<std::int64_t>{10, 20});
        REQUIRE_FALSE(decodes.empty());
        for (const auto rows : decodes) {
            CHECK(rows <= 2);
        }
    }
}

TEST_CASE("e2e lazy: head 0 keeps the columns and reads no rows", "[e2e][lazy][head]") {
    std::vector<std::size_t> decodes;
    Table out = run_recorded(R"(
extern fn read_t() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";
let t = read_t();
let h = t[head 0];
capture(h);
h;
)",
                             decodes);
    CHECK(out.rows() == 0);
    CHECK(out.find("a") != nullptr);
    CHECK(out.find("b") != nullptr);
    for (const auto rows : decodes) {
        CHECK(rows == 0);
    }
}

// A filter absorbed into the decode leaves `Head(Scan)` behind; the first rows
// of the source are not the first rows that pass.
TEST_CASE("e2e lazy: a filtered head still reads past the leading rows", "[e2e][lazy][head]") {
    std::vector<std::size_t> decodes;
    Table out = run_recorded(R"(
extern fn read_t() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";
let t = read_t();
let h = t[filter a > 1, head 2];
capture(h);
h;
)",
                             decodes);
    CHECK(i64(out, "a") == std::vector<std::int64_t>{2, 3});
    CHECK(i64(out, "b") == std::vector<std::int64_t>{30, 50});
}

// A source read through `head` in one place and in full in another is not cut
// short: the limit holds only when every occurrence is under a head.
TEST_CASE("e2e lazy: a source also read in full is not limited", "[e2e][lazy][head]") {
    std::vector<std::size_t> decodes;
    Table out = run_recorded(R"(
extern fn read_t() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";
let t = read_t();
let first = t[head 2, select { a }];
let all = t[select { a2 = a, b }];
let j = first join all on a == a2;
capture(j);
j;
)",
                             decodes);
    // The two leading rows have a = 1, which matches three rows of the full
    // source each.
    CHECK(out.rows() == 6);
}

// The statement path (one interactive statement at a time) decodes up front
// too, through the same helper.
TEST_CASE("e2e lazy: head on the statement path decodes only the leading rows",
          "[e2e][lazy][head]") {
    ExternRegistry registry;
    auto decodes = std::make_shared<std::vector<std::size_t>>();
    register_live_source(registry, "read_t", std::make_shared<Table>(head_source()), decodes);
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session
                .execute("extern fn read_t() -> DataFrame from \"x.hpp\";\n"
                         "let t = read_t();")
                .ok);
    decodes->clear();
    const auto result = session.execute("t[head 2];");
    REQUIRE(result.ok);
    REQUIRE(result.table.has_value());
    CHECK(result.table->rows() == 2);
    REQUIRE_FALSE(decodes->empty());
    for (const auto rows : *decodes) {
        CHECK(rows <= 2);
    }
}

// q19's shape: a qualification OR that names both tables in every branch. The
// planner derives a filter for each table from it (the OR of each branch's
// one-sided conjuncts) and keeps the OR above the join, so the answer must be
// exactly the unfiltered one -- including where a null makes one branch
// unknown but the other branch is true.
namespace {

/// q19's shape in miniature: parts {pk, brand, size}, lines {lk, qty, price},
/// with a null brand on part 3 and a null quantity on part 4's second line.
auto implied_part() -> Table {
    Table part;
    part.add_column("pk", Column<std::int64_t>{1, 2, 3, 4});
    ibex::runtime::ValidityBitmap brand_valid(4, true);
    brand_valid.set(2, false);  // part 3 has no brand
    part.add_column("brand", Column<std::int64_t>{1, 2, 0, 1}, std::move(brand_valid));
    part.add_column("size", Column<std::int64_t>{3, 12, 12, 20});
    return part;
}

auto implied_line() -> Table {
    Table line;
    line.add_column("lk", Column<std::int64_t>{1, 1, 2, 3, 4, 4});
    ibex::runtime::ValidityBitmap qty_valid(6, true);
    qty_valid.set(5, false);  // the second line of part 4 has no quantity
    line.add_column("qty", Column<std::int64_t>{3, 25, 25, 25, 4, 0}, std::move(qty_valid));
    line.add_column("price", Column<std::int64_t>{10, 20, 30, 40, 50, 60});
    return line;
}

constexpr const char* kImpliedQuery =
    "(part join line on { pk = lk })[\n"
    "    filter (brand == 1 && size <= 5 && qty <= 5) || (size >= 10 && qty >= 20),\n"
    "    select { pk, price }\n"
    "]";

// Part 1 line 1: first branch. Parts 2 and 3: second branch (part 3's null
// brand leaves the first branch unknown, the second is true). Part 4 fails
// both; its null-quantity line is unknown in the second branch.
const std::set<std::vector<std::int64_t>> kImpliedRows{{1, 10}, {2, 30}, {3, 40}};

}  // namespace

// q19's shape: a qualification OR that names both tables in every branch. The
// planner derives a filter for each table from it (the OR of each branch's
// one-sided conjuncts) and keeps the OR above the join, so the answer must be
// exactly the unfiltered one -- including where a null makes one branch
// unknown but the other branch is true.
TEST_CASE("e2e lazy: a disjunction across join sides keeps exactly the qualifying rows",
          "[e2e][lazy][implied]") {
    const std::string src = std::string(R"(
extern fn read_part() -> DataFrame from "x.hpp";
extern fn read_line() -> DataFrame from "x.hpp";
extern fn capture(df: DataFrame) -> Int from "x.hpp";

let part = read_part();
let line = read_line();
let result = )") + kImpliedQuery +
                            R"(;
capture(result);
result;
)";
    Table out = run_lazy_script(src.c_str(),
                                {{"read_part", implied_part()}, {"read_line", implied_line()}});
    REQUIRE(rows_of(out, {"pk", "price"}) == kImpliedRows);
}

// The same through the statement path: one interactive statement at a time.
TEST_CASE("e2e lazy: a disjunction across join sides, statement at a time",
          "[e2e][lazy][implied]") {
    ExternRegistry registry;
    register_lazy_source(registry, "read_part", implied_part());
    register_lazy_source(registry, "read_line", implied_line());
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session
                .execute("extern fn read_part() -> DataFrame from \"x.hpp\";\n"
                         "extern fn read_line() -> DataFrame from \"x.hpp\";\n"
                         "let part = read_part();\nlet line = read_line();")
                .ok);
    auto result = session.execute(std::string(kImpliedQuery) + ";");
    REQUIRE(result.ok);
    REQUIRE(result.table.has_value());
    REQUIRE(rows_of(*result.table, {"pk", "price"}) == kImpliedRows);
}

TEST_CASE("e2e lazy: alias-wrapped join filters reach source payload decoding",
          "[e2e][lazy][projection_filter]") {
    ExternRegistry registry;
    for (const auto& name : {std::string("read_a"), std::string("read_b")}) {
        registry.register_lazy_table(
            name,
            [name](const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
                Table data;
                data.add_column("k", Column<std::int64_t>{1, 2, 3, 4});
                data.add_column("v", name == "read_a" ? Column<std::int64_t>{1, 3, 8, 9}
                                                      : Column<std::int64_t>{2, 4, 6, 8});
                data.add_column("payload", Column<std::int64_t>{10, 20, 30, 40});
                Table schema;
                for (const auto& column : data.columns) {
                    schema.add_column(column.name, empty_like(*column.column));
                }
                return std::make_shared<ibex::runtime::LazyTable>(
                    std::move(schema), 4,
                    [data = std::move(data)](
                        const std::vector<std::string>& names,
                        const Selection* rows) -> std::expected<Table, std::string> {
                        if (rows == nullptr && std::ranges::find(names, "payload") != names.end()) {
                            return std::unexpected(
                                "payload must be decoded after source filtering");
                        }
                        const Selection all{0, 1, 2, 3};
                        const auto& selected = rows == nullptr ? all : *rows;
                        Table result;
                        for (const auto& name : names) {
                            result.add_column(name, slice_column(*data.find(name), selected));
                        }
                        result.logical_rows = selected.size();
                        return result;
                    });
            });
    }
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    ibex::repl::ReplSession::ExecuteOptions options;
    options.plan_table_expressions = true;
    options.render = false;
    const auto result = session.execute(R"(
extern fn read_a() -> DataFrame from "x.hpp";
extern fn read_b() -> DataFrame from "x.hpp";
((read_a()[select {ak = k, av = v, ap = payload}]) join
 (read_b()[select {bk = k, bv = v, bp = payload}]) on {ak = bk})
[select {category = av, quantity = bv, left_value = ap, right_value = bp}]
[select {category, quantity, left_value, right_value}]
[filter (category == 1 && quantity == 2) || (category == 3 && quantity == 4)]
[select {total = sum(left_value + right_value)}];
)",
                                        std::move(options));
    CAPTURE(result.error);
    REQUIRE(result.ok);
    REQUIRE(result.table.has_value());
    const auto* total = std::get_if<Column<std::int64_t>>(result.table->find("total"));
    REQUIRE(total != nullptr);
    CHECK((*total)[0] == 60);
}
