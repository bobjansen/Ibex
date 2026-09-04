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

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <expected>
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
void register_lazy_source(ExternRegistry& registry, std::string name, Table data) {
    registry.register_lazy_table(
        std::move(name),
        [data = std::move(data)](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            Table schema;
            for (const auto& col : data.columns) {
                schema.add_column(col.name, empty_like(*col.column));
            }
            const std::size_t rows = data.rows();
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), rows,
                [data](const std::vector<std::string>& names,
                       const Selection* selection) -> std::expected<Table, std::string> {
                    Selection all;
                    all.reserve(data.rows());
                    for (std::size_t i = 0; i < data.rows(); ++i) {
                        all.push_back(i);
                    }
                    const Selection& want = selection == nullptr ? all : *selection;
                    Table out;
                    for (const auto& col_name : names) {
                        const auto* col = data.find(col_name);
                        if (col == nullptr) {
                            return std::unexpected("lazy source: unknown column " + col_name);
                        }
                        out.add_column(col_name, slice_column(*col, want));
                    }
                    out.logical_rows = want.size();
                    return out;
                });
        });
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
