// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

/// Comprehensive end-to-end tests: source text -> parse -> lower -> interpret.
///
/// These tests exercise the full pipeline in a single step, ensuring that the
/// parser, lowerer, and interpreter all agree on the semantics of each query.
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "physical_plan.hpp"

namespace {

using namespace ibex;

// --- Helpers -----------------------------------------------------------------

auto col_i64(const runtime::Table& t, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<std::int64_t>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

auto col_dbl(const runtime::Table& t, const std::string& name) -> std::vector<double> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    const auto* values = std::get_if<Column<double>>(col);
    REQUIRE(values != nullptr);
    return {values->begin(), values->end()};
}

auto col_str(const runtime::Table& t, const std::string& name) -> std::vector<std::string> {
    const auto* col = t.find(name);
    REQUIRE(col != nullptr);
    std::vector<std::string> out;
    if (const auto* values = std::get_if<Column<std::string>>(col)) {
        for (const auto& v : *values) {
            out.emplace_back(v);
        }
    } else if (const auto* cat = std::get_if<Column<Categorical>>(col)) {
        for (std::size_t i = 0; i < cat->size(); ++i) {
            out.emplace_back((*cat)[i]);
        }
    } else {
        FAIL("Column '" << name << "' is neither string nor categorical");
    }
    return out;
}

auto run(std::string_view src, const runtime::TableRegistry& tables,
         const runtime::ScalarRegistry* scalars = nullptr,
         const runtime::ExternRegistry* externs = nullptr) -> runtime::Table {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, scalars, externs);
    REQUIRE(result.has_value());
    return std::move(*result);
}

auto run_error(std::string_view src, const runtime::TableRegistry& tables) -> std::string {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr);
    REQUIRE_FALSE(result.has_value());
    return result.error();
}

auto make_trades() -> runtime::TableRegistry {
    runtime::Table t;
    t.add_column("price", Column<std::int64_t>{10, 20, 30, 40, 50});
    t.add_column("qty", Column<std::int64_t>{5, 3, 8, 2, 1});
    t.add_column("symbol", Column<std::string>{"AAPL", "GOOG", "AAPL", "GOOG", "AAPL"});
    runtime::TableRegistry reg;
    reg.emplace("trades", std::move(t));
    return reg;
}

// Run a program with the Phase 1 pipeline path enabled at a small morsel grain,
// so an eligible chain is partitioned into several ranges.
//
// `threads == 1` keeps the pipeline on its serial morsel chain; anything larger
// drops the grain-size threshold to 0 so even a tiny test table fans out across
// worker threads and comes back through the ordered merger.
auto run_parallel(std::string_view src, const runtime::TableRegistry& tables, std::size_t grain,
                  std::size_t threads = 1, runtime::ParallelPipelineStats* stats = nullptr)
    -> runtime::Table {
    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    runtime::ExecutionContext exec;
    exec.parallel_grain = grain;
    exec.parallel_threads = threads;
    exec.parallel_min_rows = threads > 1 ? 0 : exec.parallel_min_rows;
    // The pipeline's size gates are two: a row floor and a cell floor. A test
    // table clears neither, so both have to be lifted or the worker path is
    // silently never taken.
    exec.parallel_min_cells = threads > 1 ? 0 : exec.parallel_min_cells;
    exec.parallel_stats = stats;
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr, nullptr, exec);
    INFO((result.has_value() ? std::string{} : result.error()));
    REQUIRE(result.has_value());
    return std::move(*result);
}

// Run on worker threads and fail if the pipeline quietly fell back to serial.
// Without this the whole worker-path suite could pass while testing nothing.
auto run_on_workers(std::string_view src, const runtime::TableRegistry& tables, std::size_t grain,
                    std::size_t threads) -> runtime::Table {
    runtime::ParallelPipelineStats stats;
    auto table = run_parallel(src, tables, grain, threads, &stats);
    REQUIRE(stats.parallel_pipelines.load() == 1);
    REQUIRE(stats.serial_pipelines.load() == 0);
    return table;
}

// Byte-for-byte table equality: schema, row count, values, validity, and
// order-sensitive metadata. This is the serial-morsel parity contract: the
// partitioned path must not let per-morsel handling change table properties.
void require_tables_equal(const runtime::Table& a, const runtime::Table& b) {
    REQUIRE(a.columns.size() == b.columns.size());
    REQUIRE(a.rows() == b.rows());
    REQUIRE(a.logical_rows == b.logical_rows);
    REQUIRE(a.time_index() == b.time_index());
    REQUIRE(a.ordering().has_value() == b.ordering().has_value());
    if (a.ordering().has_value()) {
        REQUIRE(a.ordering()->size() == b.ordering()->size());
        for (std::size_t key = 0; key < a.ordering()->size(); ++key) {
            REQUIRE((*a.ordering())[key].name == (*b.ordering())[key].name);
            REQUIRE((*a.ordering())[key].ascending == (*b.ordering())[key].ascending);
        }
    }
    for (std::size_t c = 0; c < a.columns.size(); ++c) {
        REQUIRE(a.columns[c].name == b.columns[c].name);
        const auto& av = *a.columns[c].column;
        const auto& bv = *b.columns[c].column;
        REQUIRE(av.index() == bv.index());
        std::visit(
            [&](const auto& lhs) {
                using Col = std::decay_t<decltype(lhs)>;
                const auto& rhs = std::get<Col>(bv);
                REQUIRE(lhs.size() == rhs.size());
                for (std::size_t i = 0; i < lhs.size(); ++i) {
                    REQUIRE(lhs[i] == rhs[i]);
                }
            },
            av);
        REQUIRE(a.columns[c].validity.has_value() == b.columns[c].validity.has_value());
        if (a.columns[c].validity.has_value()) {
            const auto& av2 = *a.columns[c].validity;
            const auto& bv2 = *b.columns[c].validity;
            REQUIRE(av2.size() == bv2.size());
            for (std::size_t i = 0; i < av2.size(); ++i) {
                REQUIRE(av2[i] == bv2[i]);
            }
        }
    }
}

}  // namespace

// --- Grouping ----------------------------------------------------------------

TEST_CASE("E2E: group-by on two integer keys packs into one composite key", "[e2e][aggregate]") {
    // Two int keys route to the packed-u128 fast path (process_rows_int_pair)
    // instead of the generic boxed-Key path. The grouping and the two-column key
    // reconstruction on output must match exactly. Keys (a, b) with (1,10),
    // (2,20) and (1,20) present -- note (1,10) and (1,20) share `a` and (2,20)
    // and (1,20) share `b`, so a per-key collision would merge groups wrongly.
    runtime::Table t;
    t.add_column("a", Column<std::int64_t>{1, 1, 2, 2, 1});
    t.add_column("b", Column<std::int64_t>{10, 10, 20, 20, 20});
    t.add_column("v", Column<double>{1.0, 2.0, 3.0, 4.0, 5.0});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[select { s = sum(v) }, by { a, b }];", tables);
    REQUIRE(out.rows() == 3);
    // First-seen group order: (1,10), (2,20), (1,20).
    CHECK(col_i64(out, "a") == std::vector<std::int64_t>{1, 2, 1});
    CHECK(col_i64(out, "b") == std::vector<std::int64_t>{10, 20, 20});
    CHECK(col_dbl(out, "s") == std::vector<double>{3.0, 7.0, 5.0});
}

// --- Basic filter ------------------------------------------------------------

TEST_CASE("E2E: filter with greater-than", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 25];", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30, 40, 50});
}

TEST_CASE("E2E: schema ascription passes a conforming table through", "[e2e][ascribe]") {
    auto out =
        run("Table { a = [1, 2], b = [1.5, 2.5] } as DataFrame<{ a: Int64, b: Float64 }>;", {});
    CHECK(col_i64(out, "a") == std::vector<std::int64_t>{1, 2});
    CHECK(col_dbl(out, "b") == std::vector<double>{1.5, 2.5});
}

TEST_CASE("E2E: schema ascription preserves unlisted physical columns", "[e2e][ascribe]") {
    auto tables = make_trades();
    auto out = run("trades as DataFrame<{ price: Int64 }>;", tables);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
    CHECK(out.find("symbol") != nullptr);
}

// Over a source with an unknown static schema (a registry table), the
// ascription check is deferred to the runtime validation.
TEST_CASE("E2E: schema ascription rejects a missing column at run time", "[e2e][ascribe]") {
    auto tables = make_trades();
    auto err = run_error("trades as DataFrame<{ salary: Int64 }>;", tables);
    CHECK(err.find("missing column 'salary'") != std::string::npos);
}

TEST_CASE("E2E: schema ascription rejects a wrong-type column at run time", "[e2e][ascribe]") {
    auto tables = make_trades();
    auto err = run_error("trades as DataFrame<{ price: Float64 }>;", tables);
    CHECK(err.find("wrong type") != std::string::npos);
}

TEST_CASE("E2E: scalar UDF is inlined in a select expression", "[e2e][udf]") {
    auto out =
        run("fn adjust(p: Float64) -> Float64 { p * 1.01; }\n"
            "Table { price = [100.0, 200.0] }[select { adj = adjust(price) }];",
            {});
    CHECK(col_dbl(out, "adj") == std::vector<double>{101.0, 202.0});
}

TEST_CASE("E2E: scalar UDF inside an aggregate argument", "[e2e][udf]") {
    auto out =
        run("fn adjust(p: Float64) -> Float64 { p * 1.01; }\n"
            "Table { sym = [\"A\", \"A\", \"B\"], price = [100.0, 200.0, 50.0] }"
            "[select { sym, avg_adj = mean(adjust(price)) }, by sym, order { sym asc }];",
            {});
    // group A: mean(101, 202) = 151.5; group B: 50.5
    CHECK(col_dbl(out, "avg_adj") == std::vector<double>{151.5, 50.5});
}

TEST_CASE("E2E: aggregate UDF inferred from Series<T> params", "[e2e][udf]") {
    auto out =
        run("fn weighted_mean(p: Series<Float64>, w: Series<Float64>) -> Float64 {\n"
            "  sum(p * w) / sum(w);\n"
            "}\n"
            "Table { price = [1.0, 2.0, 3.0], qty = [0.1, 0.2, 0.7] }"
            "[select { wavg = weighted_mean(price, qty) }];",
            {});
    const auto wavg = col_dbl(out, "wavg");
    REQUIRE(wavg.size() == 1);
    CHECK(wavg[0] == Catch::Approx(2.6));
}

TEST_CASE("E2E: aggregate UDF with by groups", "[e2e][udf]") {
    auto out =
        run("fn weighted_mean(p: Series<Float64>, w: Series<Float64>) -> Float64 {\n"
            "  sum(p * w) / sum(w);\n"
            "}\n"
            "Table { sym = [\"A\", \"A\", \"B\", \"B\"],\n"
            "        price = [1.0, 2.0, 3.0, 4.0],\n"
            "        qty = [1.0, 3.0, 2.0, 8.0] }"
            "[select { wavg = weighted_mean(price, qty) }, by sym, order { sym asc }];",
            {});
    const auto wavg = col_dbl(out, "wavg");
    REQUIRE(wavg.size() == 2);
    CHECK(wavg[0] == Catch::Approx(1.75));
    CHECK(wavg[1] == Catch::Approx(3.8));
}

TEST_CASE("E2E: scalar UDF with let bindings inlines", "[e2e][udf]") {
    auto out =
        run("fn norm_price(p: Float64, base: Float64) -> Float64 {\n"
            "  let diff = p - base;\n"
            "  let scale = 100.0;\n"
            "  diff * scale;\n"
            "}\n"
            "Table { price = [101.0, 102.0, 99.0] }"
            "[select { delta = norm_price(price, 100.0) }];",
            {});
    const auto delta = col_dbl(out, "delta");
    REQUIRE(delta.size() == 3);
    CHECK(delta[0] == Catch::Approx(100.0));
    CHECK(delta[1] == Catch::Approx(200.0));
    CHECK(delta[2] == Catch::Approx(-100.0));
}

TEST_CASE("E2E: aggregate UDF with let-folded body computes per group", "[e2e][udf]") {
    auto out =
        run("fn wm_pct(p: Series<Float64>, w: Series<Float64>) -> Float64 {\n"
            "  let num = sum(p * w);\n"
            "  let den = sum(w);\n"
            "  100.0 * num / den;\n"
            "}\n"
            "Table { sym = [\"A\", \"A\", \"B\", \"B\"],\n"
            "        price = [1.0, 2.0, 3.0, 4.0],\n"
            "        qty = [1.0, 3.0, 2.0, 8.0] }"
            "[select { wp = wm_pct(price, qty) }, by sym, order { sym asc }];",
            {});
    const auto wp = col_dbl(out, "wp");
    REQUIRE(wp.size() == 2);
    CHECK(wp[0] == Catch::Approx(175.0));  // A: 100 * 7 / 4
    CHECK(wp[1] == Catch::Approx(380.0));  // B: 100 * 38 / 10
}

TEST_CASE("E2E: timestamp accessors year/month/day/hour/minute/second", "[e2e][time]") {
    auto out =
        run("Table { ts = [ts\"2024-01-15T03:04:05\", ts\"2024-07-04T12:30:45\"] }"
            "[select { y = year(ts), mo = month(ts), d = day(ts),"
            "          h = hour(ts), mi = minute(ts), s = second(ts) }];",
            {});
    const auto y = col_i64(out, "y");
    const auto mo = col_i64(out, "mo");
    const auto d = col_i64(out, "d");
    const auto h = col_i64(out, "h");
    const auto mi = col_i64(out, "mi");
    const auto s = col_i64(out, "s");
    REQUIRE(y.size() == 2);
    CHECK(y == std::vector<std::int64_t>{2024, 2024});
    CHECK(mo == std::vector<std::int64_t>{1, 7});
    CHECK(d == std::vector<std::int64_t>{15, 4});
    CHECK(h == std::vector<std::int64_t>{3, 12});
    CHECK(mi == std::vector<std::int64_t>{4, 30});
    CHECK(s == std::vector<std::int64_t>{5, 45});
}

TEST_CASE("E2E: date/timestamp literal broadcast into a field", "[e2e][time]") {
    // A literal field is typed by `infer_expr_type`, which allocates the output
    // column before the per-row loop fills it. Its Literal arm used to fall
    // through to String for anything that was not Int/Double/Bool, so a
    // `date"..."`/`ts"..."` broadcast allocated a String column and then tripped
    // eval_expr_column's invariant on the Date/Timestamp value it was handed.
    runtime::Table t;
    t.add_column("v", Column<std::int64_t>{1, 2});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    auto out =
        run("data[update { d = date\"2024-01-15\", ts = ts\"2024-01-15T03:04:05\" }]"
            "[select { v, y = year(d), mo = month(d), h = hour(ts) }];",
            tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "y") == std::vector<std::int64_t>{2024, 2024});
    CHECK(col_i64(out, "mo") == std::vector<std::int64_t>{1, 1});
    CHECK(col_i64(out, "h") == std::vector<std::int64_t>{3, 3});

    // The broadcast column keeps its type, rather than becoming text.
    auto typed = run("data[update { d = date\"2024-01-15\" }];", tables);
    const auto* day = typed.find("d");
    REQUIRE(day != nullptr);
    CHECK(std::get_if<Column<Date>>(day) != nullptr);
}

TEST_CASE("E2E: Date() truncates a Timestamp to its UTC day", "[e2e][time][cast]") {
    auto out =
        run("Table { ts = [ts\"2024-01-15T00:00:00\", ts\"2024-01-15T23:59:59\","
            "              ts\"2024-01-16T00:00:00\", ts\"1969-12-31T23:00:00\"] }"
            "[select { d = Date(ts), y = year(Date(ts)), mo = month(Date(ts)),"
            "          dy = day(Date(ts)) }];",
            {});
    REQUIRE(out.rows() == 4);
    const auto* col = out.find("d");
    REQUIRE(col != nullptr);
    const auto* days = std::get_if<Column<Date>>(col);
    REQUIRE(days != nullptr);
    // The first two instants are the same day; the third is the next day. The
    // last is before the epoch, where a truncating division would round *up*
    // to 1970-01-01 -- the cast floors, so it stays on 1969-12-31.
    CHECK((*days)[0] == (*days)[1]);
    CHECK((*days)[2].days == (*days)[0].days + 1);
    CHECK((*days)[3].days == -1);
    CHECK(col_i64(out, "y") == std::vector<std::int64_t>{2024, 2024, 2024, 1969});
    CHECK(col_i64(out, "mo") == std::vector<std::int64_t>{1, 1, 1, 12});
    CHECK(col_i64(out, "dy") == std::vector<std::int64_t>{15, 15, 16, 31});
}

TEST_CASE("E2E: Date() groups an instant column by day", "[e2e][time][cast][groupby]") {
    auto out =
        run("Table { ts = [ts\"2024-01-15T01:00:00\", ts\"2024-01-15T22:00:00\","
            "              ts\"2024-01-16T05:00:00\"],"
            "        v = [1.0, 3.0, 5.0] }"
            "[select { n = count(), total = sum(v) }, by { d = Date(ts) }, order { d asc }];",
            {});
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "n") == std::vector<std::int64_t>{2, 1});
    CHECK(col_dbl(out, "total")[0] == Catch::Approx(4.0));
    CHECK(col_dbl(out, "total")[1] == Catch::Approx(5.0));
}

TEST_CASE("E2E: Date() is identity on a Date and rejects other types", "[e2e][time][cast]") {
    auto out = run("Table { d = [date\"2024-03-01\"] }[select { same = Date(d) }];", {});
    REQUIRE(out.rows() == 1);
    const auto* col = out.find("same");
    REQUIRE(col != nullptr);
    const auto* days = std::get_if<Column<Date>>(col);
    REQUIRE(days != nullptr);
    CHECK((*days)[0] == Date{static_cast<std::int32_t>(std::chrono::sys_days{
                            std::chrono::year{2024} / std::chrono::month{3} / std::chrono::day{1}}
                                                           .time_since_epoch()
                                                           .count())});

    // An Int64 is days since the epoch; a Float64 or String has no meaning here.
    auto from_days = run("Table { n = [19783] }[select { d = Date(n) }];", {});
    REQUIRE(from_days.rows() == 1);
    CHECK(std::get_if<Column<Date>>(from_days.find("d")) != nullptr);

    CHECK(run_error("Table { s = [\"2024-01-15\"] }[select { d = Date(s) }];", {})
              .find("cannot cast to Date") != std::string::npos);
    CHECK(
        run_error("Table { f = [1.5] }[select { d = Date(f) }];", {}).find("cannot cast to Date") !=
        std::string::npos);
}

TEST_CASE("E2E: hour() inside computed group key", "[e2e][time][groupby]") {
    auto out =
        run("Table { ts = [ts\"2024-01-01T09:15:00\", ts\"2024-01-01T09:45:00\","
            "              ts\"2024-01-01T10:05:00\"],"
            "        v = [1.0, 3.0, 5.0] }"
            "[select { n = count(), avg_v = mean(v) },"
            " by { h = hour(ts) }, order { h asc }];",
            {});
    REQUIRE(col_i64(out, "h").size() == 2);
    CHECK(col_i64(out, "h") == std::vector<std::int64_t>{9, 10});
    CHECK(col_dbl(out, "avg_v")[0] == Catch::Approx(2.0));
    CHECK(col_dbl(out, "avg_v")[1] == Catch::Approx(5.0));
}

TEST_CASE("E2E: computed group key from arithmetic expression", "[e2e][groupby]") {
    auto out =
        run("Table { x = [1, 1, 2, 2, 3, 3], y = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0] }"
            "[select { avg_y = mean(y) }, by { bucket = x * 10 }, order { bucket asc }];",
            {});
    const auto avg = col_dbl(out, "avg_y");
    REQUIRE(avg.size() == 3);
    CHECK(avg[0] == Catch::Approx(15.0));
    CHECK(avg[1] == Catch::Approx(35.0));
    CHECK(avg[2] == Catch::Approx(55.0));
}

TEST_CASE("E2E: scalar fn wrapping aggregate result in select", "[e2e][udf]") {
    // Inline use: pmax(sum(p*w)/sum(w), 0.0) — the trailing scalar call wraps
    // the aggregate expression and must lower as a post-aggregate update.
    auto out =
        run("Table { sym = [\"A\", \"A\", \"B\", \"B\"],\n"
            "        price = [1.0, 2.0, 3.0, 4.0],\n"
            "        qty = [1.0, 3.0, 2.0, 8.0] }"
            "[select { wavg = pmax(sum(price * qty) / sum(qty), 2.0) },"
            " by sym, order { sym asc }];",
            {});
    const auto wavg = col_dbl(out, "wavg");
    REQUIRE(wavg.size() == 2);
    CHECK(wavg[0] == Catch::Approx(2.0));  // A: max(1.75, 2.0) = 2.0
    CHECK(wavg[1] == Catch::Approx(3.8));  // B: max(3.8, 2.0)  = 3.8
}

TEST_CASE("E2E: aggregate UDF with mixed Series and scalar params", "[e2e][udf]") {
    auto out =
        run("fn wm_floored(p: Series<Float64>, w: Series<Float64>, floor: Float64)"
            " -> Float64 {\n"
            "  pmax(sum(p * w) / sum(w), floor);\n"
            "}\n"
            "Table { sym = [\"A\", \"A\", \"B\", \"B\"],\n"
            "        price = [1.0, 2.0, 3.0, 4.0],\n"
            "        qty = [1.0, 3.0, 2.0, 8.0] }"
            "[select { wf = wm_floored(price, qty, 2.0) },"
            " by sym, order { sym asc }];",
            {});
    const auto wf = col_dbl(out, "wf");
    REQUIRE(wf.size() == 2);
    CHECK(wf[0] == Catch::Approx(2.0));
    CHECK(wf[1] == Catch::Approx(3.8));
}

TEST_CASE("E2E: update + by broadcasts a compound aggregate expression", "[e2e][update_by]") {
    auto out =
        run("Table { sym = [\"A\", \"A\", \"B\", \"B\"],\n"
            "        price = [1.0, 2.0, 3.0, 4.0],\n"
            "        qty = [1.0, 3.0, 2.0, 8.0] }"
            "[update { wavg = sum(price * qty) / sum(qty) }, by sym, order { sym asc }];",
            {});
    const auto wavg = col_dbl(out, "wavg");
    REQUIRE(wavg.size() == 4);
    // A: (1*1 + 2*3) / (1+3) = 7/4 = 1.75 (broadcast to both A rows)
    CHECK(wavg[0] == Catch::Approx(1.75));
    CHECK(wavg[1] == Catch::Approx(1.75));
    // B: (3*2 + 4*8) / (2+8) = 38/10 = 3.8 (broadcast to both B rows)
    CHECK(wavg[2] == Catch::Approx(3.8));
    CHECK(wavg[3] == Catch::Approx(3.8));
}

TEST_CASE("E2E: aggregate UDF in update + by broadcasts per group", "[e2e][udf][update_by]") {
    auto out =
        run("fn weighted_mean(p: Series<Float64>, w: Series<Float64>) -> Float64 {\n"
            "  sum(p * w) / sum(w);\n"
            "}\n"
            "Table { sym = [\"A\", \"A\", \"B\", \"B\"],\n"
            "        price = [1.0, 2.0, 3.0, 4.0],\n"
            "        qty = [1.0, 3.0, 2.0, 8.0] }"
            "[update { wavg = weighted_mean(price, qty) }, by sym, order { sym asc }];",
            {});
    const auto wavg = col_dbl(out, "wavg");
    REQUIRE(wavg.size() == 4);
    CHECK(wavg[0] == Catch::Approx(1.75));
    CHECK(wavg[1] == Catch::Approx(1.75));
    CHECK(wavg[2] == Catch::Approx(3.8));
    CHECK(wavg[3] == Catch::Approx(3.8));
}

TEST_CASE("E2E: nested scalar UDF calls inline", "[e2e][udf]") {
    auto out =
        run("fn inc(x: Int) -> Int { x + 1; }\n"
            "fn dbl(x: Int) -> Int { x * 2; }\n"
            "Table { a = [1, 2] }[select { y = dbl(inc(a)) }];",
            {});
    CHECK(col_i64(out, "y") == std::vector<std::int64_t>{4, 6});
}

TEST_CASE("E2E: filter with equality", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price == 20];", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{20});
    CHECK(col_str(out, "symbol") == std::vector<std::string>{"GOOG"});
}

TEST_CASE("E2E: filter with less-than-or-equal", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price <= 20];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20});
}

TEST_CASE("E2E: filter with string equality", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter symbol == \"AAPL\"];", tables);

    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 30, 50});
}

TEST_CASE("E2E: filter with AND predicate", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 15 && symbol == \"GOOG\"];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{20, 40});
}

TEST_CASE("E2E: filter with OR predicate", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price == 10 || price == 50];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 50});
}

// --- Select ------------------------------------------------------------------

TEST_CASE("E2E: select single column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select price];", tables);

    REQUIRE(out.columns.size() == 1);
    REQUIRE(out.rows() == 5);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: implicit select single column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[price];", tables);

    REQUIRE(out.columns.size() == 1);
    REQUIRE(out.rows() == 5);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: select multiple columns", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { price, symbol }];", tables);

    REQUIRE(out.columns.size() == 2);
    CHECK(out.find("price") != nullptr);
    CHECK(out.find("symbol") != nullptr);
    CHECK(out.find("qty") == nullptr);
}

TEST_CASE("E2E: implicit select multiple columns", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[{ price, symbol }];", tables);

    REQUIRE(out.columns.size() == 2);
    CHECK(out.find("price") != nullptr);
    CHECK(out.find("symbol") != nullptr);
    CHECK(out.find("qty") == nullptr);
}

TEST_CASE("E2E: select with computed field", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { total = price * qty }];", tables);

    REQUIRE(out.columns.size() == 1);
    CHECK(col_i64(out, "total") == std::vector<std::int64_t>{50, 60, 240, 80, 50});
}

// --- Filter + Select combined ------------------------------------------------

TEST_CASE("E2E: filter then select", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 15, select { symbol, price }];", tables);

    REQUIRE(out.rows() == 4);
    REQUIRE(out.columns.size() == 2);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{20, 30, 40, 50});
}

// --- Update ------------------------------------------------------------------

TEST_CASE("E2E: update adds new column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { total = price * qty }];", tables);

    REQUIRE(out.find("total") != nullptr);
    REQUIRE(out.find("price") != nullptr);
    CHECK(col_i64(out, "total") == std::vector<std::int64_t>{50, 60, 240, 80, 50});
}

TEST_CASE("E2E: update replaces existing column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { price = price + 1 }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{11, 21, 31, 41, 51});
}

TEST_CASE("E2E: update with subtraction", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { price = price - 5 }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{5, 15, 25, 35, 45});
}

TEST_CASE("E2E: update with multiplication by 2", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { doubled = price * 2 }];", tables);

    CHECK(col_i64(out, "doubled") == std::vector<std::int64_t>{20, 40, 60, 80, 100});
}

TEST_CASE("E2E: update with modulo", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[update { rem = price % 15 }];", tables);

    CHECK(col_i64(out, "rem") == std::vector<std::int64_t>{10, 5, 0, 10, 5});
}

// --- Distinct ----------------------------------------------------------------

TEST_CASE("E2E: distinct on single column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[distinct symbol];", tables);

    CHECK(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    // Order depends on first-seen - AAPL first, then GOOG.
    CHECK(symbols[0] == "AAPL");
    CHECK(symbols[1] == "GOOG");
}

TEST_CASE("E2E: distinct on multiple columns", "[e2e]") {
    runtime::Table t;
    t.add_column("a", Column<std::int64_t>{1, 1, 2, 2, 1});
    t.add_column("b", Column<std::string>{"X", "X", "Y", "Y", "Y"});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    auto out = run("data[distinct { a, b }];", tables);
    CHECK(out.rows() == 3);  // (1,X), (2,Y), (1,Y)
}

// --- Order -------------------------------------------------------------------

TEST_CASE("E2E: order ascending", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[order { price asc }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: order descending", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[order { price desc }];", tables);

    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{50, 40, 30, 20, 10});
}

TEST_CASE("E2E: order by string column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[order { symbol asc }];", tables);

    auto symbols = col_str(out, "symbol");
    CHECK(symbols[0] == "AAPL");
    CHECK(symbols[1] == "AAPL");
    CHECK(symbols[2] == "AAPL");
    CHECK(symbols[3] == "GOOG");
    CHECK(symbols[4] == "GOOG");
}

// --- Aggregation -------------------------------------------------------------

TEST_CASE("E2E: sum aggregation with group-by", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, total = sum(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto totals = col_i64(out, "total");
    // AAPL: 10+30+50=90, GOOG: 20+40=60
    if (symbols[0] == "AAPL") {
        CHECK(totals[0] == 90);
        CHECK(totals[1] == 60);
    } else {
        CHECK(totals[0] == 60);
        CHECK(totals[1] == 90);
    }
}

TEST_CASE("E2E: count aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, n = count() }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto counts = col_i64(out, "n");
    if (symbols[0] == "AAPL") {
        CHECK(counts[0] == 3);
        CHECK(counts[1] == 2);
    } else {
        CHECK(counts[0] == 2);
        CHECK(counts[1] == 3);
    }
}

TEST_CASE("E2E: mean aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, avg = mean(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto avgs = col_dbl(out, "avg");
    if (symbols[0] == "AAPL") {
        CHECK(avgs[0] == Catch::Approx(30.0));  // (10+30+50)/3
        CHECK(avgs[1] == Catch::Approx(30.0));  // (20+40)/2
    } else {
        CHECK(avgs[0] == Catch::Approx(30.0));
        CHECK(avgs[1] == Catch::Approx(30.0));
    }
}

TEST_CASE("E2E: min and max aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out =
        run("trades[select { symbol, lo = min(price), hi = max(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto lo = col_i64(out, "lo");
    auto hi = col_i64(out, "hi");
    if (symbols[0] == "AAPL") {
        CHECK(lo[0] == 10);
        CHECK(hi[0] == 50);
        CHECK(lo[1] == 20);
        CHECK(hi[1] == 40);
    } else {
        CHECK(lo[0] == 20);
        CHECK(hi[0] == 40);
        CHECK(lo[1] == 10);
        CHECK(hi[1] == 50);
    }
}

TEST_CASE("E2E: first and last aggregation", "[e2e]") {
    auto tables = make_trades();
    auto out =
        run("trades[select { symbol, f = first(price), l = last(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto firsts = col_i64(out, "f");
    auto lasts = col_i64(out, "l");
    if (symbols[0] == "AAPL") {
        CHECK(firsts[0] == 10);
        CHECK(lasts[0] == 50);
        CHECK(firsts[1] == 20);
        CHECK(lasts[1] == 40);
    } else {
        CHECK(firsts[0] == 20);
        CHECK(lasts[0] == 40);
        CHECK(firsts[1] == 10);
        CHECK(lasts[1] == 50);
    }
}

TEST_CASE("E2E: global aggregation without group-by", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { total = sum(price) }];", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "total") == std::vector<std::int64_t>{150});
}

TEST_CASE("E2E: count without group-by", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { n = count() }];", tables);

    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "n") == std::vector<std::int64_t>{5});
}

// --- Rename ------------------------------------------------------------------

TEST_CASE("E2E: rename column", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[rename { cost = price }];", tables);

    CHECK(out.find("cost") != nullptr);
    CHECK(out.find("price") == nullptr);
    CHECK(col_i64(out, "cost") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
}

TEST_CASE("E2E: rename multiple columns", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[rename { cost = price, amount = qty }];", tables);

    CHECK(out.find("cost") != nullptr);
    CHECK(out.find("amount") != nullptr);
    CHECK(out.find("price") == nullptr);
    CHECK(out.find("qty") == nullptr);
    CHECK(col_i64(out, "cost") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
    CHECK(col_i64(out, "amount") == std::vector<std::int64_t>{5, 3, 8, 2, 1});
}

// --- Multi-step pipelines ----------------------------------------------------

TEST_CASE("E2E: filter + aggregate + order pipeline", "[e2e]") {
    auto tables = make_trades();
    auto out =
        run("trades[filter price > 15, select { symbol, total = sum(price) }, by symbol, "
            "order { total desc }];",
            tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto totals = col_i64(out, "total");
    // AAPL: 30+50=80, GOOG: 20+40=60 -> desc: AAPL, GOOG
    CHECK(symbols[0] == "AAPL");
    CHECK(totals[0] == 80);
    CHECK(symbols[1] == "GOOG");
    CHECK(totals[1] == 60);
}

TEST_CASE("E2E: update then filter via chained let", "[e2e]") {
    runtime::Table t;
    t.add_column("price", Column<std::int64_t>{10, 20, 30, 40, 50});
    t.add_column("symbol", Column<std::string>{"AAPL", "GOOG", "AAPL", "GOOG", "AAPL"});
    runtime::TableRegistry tables;
    tables.emplace("trades", std::move(t));

    runtime::ExternRegistry registry;
    registry.register_table(
        "get_trades",
        [&tables](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{tables.at("trades")};
        });

    const char* src = R"(
extern fn get_trades() -> DataFrame from "fake.hpp";
let t = get_trades();
let updated = t[update { doubled = price * 2 }];
updated[filter doubled > 50];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("E2E: filter then distinct", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 15, distinct symbol];", tables);

    CHECK(out.rows() == 2);
}

// --- Chained let bindings (via REPL) -----------------------------------------

TEST_CASE("E2E: chained let bindings via execute_script", "[e2e]") {
    runtime::Table t;
    t.add_column("val", Column<std::int64_t>{1, 2, 3, 4, 5});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    runtime::ExternRegistry registry;
    registry.register_table(
        "get_data",
        [&tables](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{tables.at("data")};
        });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let d = get_data();
let big = d[filter val > 3];
big[select { val }];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

// --- Joins -------------------------------------------------------------------

TEST_CASE("E2E: inner join and select", "[e2e]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("name", Column<std::string>{"Alice", "Bob", "Charlie"});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 3, 4});
    rhs.add_column("score", Column<std::int64_t>{85, 92, 78});

    runtime::TableRegistry tables;
    tables.emplace("people", std::move(lhs));
    tables.emplace("scores", std::move(rhs));

    auto out = run("people join scores on id;", tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_i64(out, "score") == std::vector<std::int64_t>{85, 92});
}

TEST_CASE("E2E: left join preserves all left rows", "[e2e]") {
    runtime::Table lhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2, 3});
    lhs.add_column("name", Column<std::string>{"Alice", "Bob", "Charlie"});

    runtime::Table rhs;
    rhs.add_column("id", Column<std::int64_t>{2, 4});
    rhs.add_column("score", Column<std::int64_t>{85, 78});

    runtime::TableRegistry tables;
    tables.emplace("people", std::move(lhs));
    tables.emplace("scores", std::move(rhs));

    auto out = run("people left join scores on id;", tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    // Unmatched rows get default value 0 for int
    CHECK(col_i64(out, "score") == std::vector<std::int64_t>{0, 85, 0});
}

TEST_CASE("E2E: right join preserves all right rows", "[e2e]") {
    runtime::Table lhs;
    runtime::Table rhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});
    lhs.add_column("name", Column<std::string>{"alice", "bob"});

    rhs.add_column("id", Column<std::int64_t>{2, 3});
    rhs.add_column("score", Column<std::int64_t>{85, 78});

    runtime::TableRegistry tables;
    tables.emplace("people", std::move(lhs));
    tables.emplace("scores", std::move(rhs));

    auto out = run("people right join scores on id;", tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{2, 3});
    CHECK(col_str(out, "name") == std::vector<std::string>{"bob", ""});
    CHECK(col_i64(out, "score") == std::vector<std::int64_t>{85, 78});
}

TEST_CASE("E2E: outer join preserves rows from both sides", "[e2e]") {
    runtime::Table lhs;
    runtime::Table rhs;
    lhs.add_column("id", Column<std::int64_t>{1, 2});
    lhs.add_column("name", Column<std::string>{"alice", "bob"});

    rhs.add_column("id", Column<std::int64_t>{2, 3});
    rhs.add_column("score", Column<std::int64_t>{85, 78});

    runtime::TableRegistry tables;
    tables.emplace("people", std::move(lhs));
    tables.emplace("scores", std::move(rhs));

    auto out = run("people outer join scores on id;", tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "id") == std::vector<std::int64_t>{1, 2, 3});
    CHECK(col_str(out, "name") == std::vector<std::string>{"alice", "bob", ""});
    CHECK(col_i64(out, "score") == std::vector<std::int64_t>{0, 85, 78});
}
// --- Scalar predicate in filter ----------------------------------------------

TEST_CASE("E2E: filter with scalar variable", "[e2e]") {
    auto tables = make_trades();
    runtime::ScalarRegistry scalars;
    scalars.emplace("threshold", static_cast<std::int64_t>(30));

    auto out = run("trades[filter price >= threshold];", tables, &scalars);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30, 40, 50});
}

// --- Scope escape (SPEC.md Section 6.2) --------------------------------------

TEST_CASE("E2E: scope escape reads the shadowed lexical binding", "[e2e]") {
    auto tables = make_trades();
    runtime::ScalarRegistry scalars;
    scalars.emplace("price", static_cast<std::int64_t>(30));

    // `price` is the column; `^price` is the scalar it shadows.
    auto out = run("trades[filter price >= ^price];", tables, &scalars);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30, 40, 50});

    // A predicate over the lexical binding alone is constant: 30 >= 30.
    auto all = run("trades[filter ^price >= 30];", tables, &scalars);
    CHECK(all.rows() == 5);
    auto none = run("trades[filter ^price > 30];", tables, &scalars);
    CHECK(none.rows() == 0);
}

TEST_CASE("E2E: scope escape in computed fields and aggregates", "[e2e]") {
    auto tables = make_trades();
    runtime::ScalarRegistry scalars;
    scalars.emplace("price", static_cast<std::int64_t>(2));

    auto out =
        run("trades[select { price, scaled = price * ^price, lex = ^price }];", tables, &scalars);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{10, 20, 30, 40, 50});
    CHECK(col_i64(out, "scaled") == std::vector<std::int64_t>{20, 40, 60, 80, 100});
    CHECK(col_i64(out, "lex") == std::vector<std::int64_t>{2, 2, 2, 2, 2});

    // Inside an aggregate the escaped name is a per-row constant, so summing it
    // counts the group's rows.
    auto agg = run("trades[by symbol, select { symbol, s = sum(^price) }];", tables, &scalars);
    CHECK(col_i64(agg, "s") == std::vector<std::int64_t>{6, 4});
}

TEST_CASE("E2E: scope escape without a lexical binding is an error", "[e2e]") {
    auto tables = make_trades();
    // `price` is a column of the input, but `^price` never consults column
    // scope, so with no scalar bound it fails to resolve.
    const auto error = run_error("trades[filter ^price > 10];", tables);
    CHECK(error.find("^price") != std::string::npos);
}

// --- Arithmetic in filter predicates -----------------------------------------

TEST_CASE("E2E: filter with arithmetic expression", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price * qty > 100];", tables);

    // price*qty: 50, 60, 240, 80, 50 -> only 240 > 100
    REQUIRE(out.rows() == 1);
    CHECK(col_i64(out, "price") == std::vector<std::int64_t>{30});
}

// --- Double columns ----------------------------------------------------------

TEST_CASE("E2E: filter and select with double column", "[e2e]") {
    runtime::Table t;
    t.add_column("price", Column<double>{1.5, 2.5, 3.5, 4.5});
    t.add_column("symbol", Column<std::string>{"A", "B", "A", "B"});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    auto out = run("data[filter price > 2.0, select { price }];", tables);
    REQUIRE(out.rows() == 3);
    auto prices = col_dbl(out, "price");
    CHECK(prices[0] == Catch::Approx(2.5));
    CHECK(prices[1] == Catch::Approx(3.5));
    CHECK(prices[2] == Catch::Approx(4.5));
}

// --- Empty results -----------------------------------------------------------

TEST_CASE("E2E: filter produces empty result", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[filter price > 999];", tables);

    CHECK(out.rows() == 0);
}

// --- Error handling ----------------------------------------------------------

TEST_CASE("E2E: unknown table produces error", "[e2e]") {
    runtime::TableRegistry empty;
    auto error = run_error("no_such_table[select { x }];", empty);
    CHECK(!error.empty());
}

TEST_CASE("E2E: unknown column in filter produces error", "[e2e]") {
    auto tables = make_trades();
    auto error = run_error("trades[filter nonexistent > 10];", tables);
    CHECK(!error.empty());
}

TEST_CASE("E2E: unknown column in select produces error", "[e2e]") {
    auto tables = make_trades();
    auto error = run_error("trades[select nonexistent];", tables);
    CHECK(!error.empty());
}

// --- Parse errors ------------------------------------------------------------

TEST_CASE("E2E: parse error for incomplete expression", "[e2e]") {
    auto result = parser::parse("trades[filter];");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("E2E: parse error for missing closing bracket", "[e2e]") {
    auto result = parser::parse("trades[filter price > 10");
    REQUIRE_FALSE(result.has_value());
}

// --- Multiple aggs in one query ----------------------------------------------

TEST_CASE("E2E: multiple aggregations in a single select", "[e2e]") {
    auto tables = make_trades();
    auto out = run(
        "trades[select { symbol, s = sum(price), n = count(), hi = max(price), lo = min(price) }, "
        "by symbol];",
        tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto sums = col_i64(out, "s");
    auto counts = col_i64(out, "n");
    auto highs = col_i64(out, "hi");
    auto lows = col_i64(out, "lo");

    // Find AAPL index
    const std::size_t aapl = (symbols[0] == "AAPL") ? 0U : 1U;
    const std::size_t goog = 1U - aapl;
    CHECK(sums[aapl] == 90);
    CHECK(counts[aapl] == 3);
    CHECK(highs[aapl] == 50);
    CHECK(lows[aapl] == 10);
    CHECK(sums[goog] == 60);
    CHECK(counts[goog] == 2);
    CHECK(highs[goog] == 40);
    CHECK(lows[goog] == 20);
}

// --- Update with group-by ----------------------------------------------------

TEST_CASE("E2E: aggregate mean per group", "[e2e]") {
    auto tables = make_trades();
    auto out = run("trades[select { symbol, avg = mean(price) }, by symbol];", tables);

    REQUIRE(out.rows() == 2);
    auto symbols = col_str(out, "symbol");
    auto avgs = col_dbl(out, "avg");
    // AAPL mean: (10+30+50)/3 = 30, GOOG mean: (20+40)/2 = 30
    const std::size_t aapl = (symbols[0] == "AAPL") ? 0U : 1U;
    const std::size_t goog = 1U - aapl;
    CHECK(avgs[aapl] == Catch::Approx(30.0));
    CHECK(avgs[goog] == Catch::Approx(30.0));
}

// --- Date literals in filter -------------------------------------------------

TEST_CASE("E2E: filter with date literal", "[e2e]") {
    using namespace std::chrono;
    auto d1 = Date{static_cast<std::int32_t>(
        sys_days{year{2024} / month{1} / std::chrono::day{1}}.time_since_epoch().count())};
    auto d2 = Date{static_cast<std::int32_t>(
        sys_days{year{2024} / month{1} / std::chrono::day{15}}.time_since_epoch().count())};
    auto d3 = Date{static_cast<std::int32_t>(
        sys_days{year{2024} / month{2} / std::chrono::day{1}}.time_since_epoch().count())};

    runtime::Table t;
    t.add_column("day", Column<Date>{d1, d2, d3});
    t.add_column("val", Column<std::int64_t>{1, 2, 3});
    runtime::TableRegistry tables;
    tables.emplace("cal", std::move(t));

    auto out = run("cal[filter day > date\"2024-01-10\"];", tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "val") == std::vector<std::int64_t>{2, 3});
}

// --- Extern function calls --------------------------------------------------

TEST_CASE("E2E: extern scalar function in select", "[e2e]") {
    runtime::Table t;
    t.add_column("x", Column<std::int64_t>{2, 3, 4});
    runtime::TableRegistry tables;
    tables.emplace("data", std::move(t));

    runtime::ExternRegistry externs;
    externs.register_scalar(
        "double_it", runtime::ScalarKind::Int,
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            const auto* v = std::get_if<std::int64_t>(args.data());
            if (v == nullptr) {
                return std::unexpected("expected int");
            }
            return runtime::ExternValue{(*v) * 2};
        });

    auto out = run("data[select { result = double_it(x) }];", tables, nullptr, &externs);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "result") == std::vector<std::int64_t>{4, 6, 8});
}

// --- Large dataset basic smoke -----------------------------------------------

TEST_CASE("E2E: handles 1000-row table", "[e2e]") {
    runtime::Table t;
    std::vector<std::int64_t> prices;
    std::vector<std::string> symbols;
    prices.reserve(1000);
    symbols.reserve(1000);
    for (int i = 0; i < 1000; ++i) {
        prices.push_back(i);
        symbols.push_back(i % 2 == 0 ? "A" : "B");
    }
    t.add_column("price", Column<std::int64_t>(std::move(prices)));
    t.add_column("symbol", Column<std::string>(symbols));
    runtime::TableRegistry tables;
    tables.emplace("big", std::move(t));

    auto out =
        run("big[filter price >= 500, select { symbol, total = sum(price) }, by symbol];", tables);
    REQUIRE(out.rows() == 2);
    // A: even numbers 500..998 -> sum = 250*749 = 187250
    // B: odd numbers 501..999 -> sum = 250*750 = 187500
    auto symbols_out = col_str(out, "symbol");
    auto totals = col_i64(out, "total");
    const std::size_t a_idx = (symbols_out[0] == "A") ? 0U : 1U;
    const std::size_t b_idx = 1U - a_idx;
    CHECK(totals[a_idx] == 187250);
    CHECK(totals[b_idx] == 187500);
}

// --- Stream wall-clock bucket flush ------------------------------------------

// Verify that a TimeBucket stream emits a completed bucket at wall-clock bucket
// end, not only when a message with a later data timestamp arrives.
//
// Scenario: both ticks have data timestamps in bucket 0 (by data time), but the
// second tick arrives after the bucket duration has elapsed on the wall clock.
// The expected behaviour is two separate sink calls - one for each tick - rather
// than a single call with both ticks merged into bucket 0.
TEST_CASE("Stream TimeBucket flushes at wall-clock bucket end", "[e2e][stream]") {
    std::vector<runtime::Table> emitted;
    int call_count = 0;

    runtime::ExternRegistry registry;

    // Source: returns tick 1 immediately, then sleeps past the 20 ms bucket
    // boundary before returning tick 2 (which still carries a data timestamp
    // inside bucket 0).  After that it signals EOF.
    registry.register_table(
        "tick_src",
        [&](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            ++call_count;
            if (call_count == 1) {
                runtime::Table t;
                t.add_column("ts", Column<Timestamp>{Timestamp{0}});
                t.add_column("price", Column<double>{100.0});
                t.set_properties(ibex::runtime::TableProperties::time_frame("ts"));
                return runtime::ExternValue{t};
            }
            if (call_count == 2) {
                // Sleep longer than the 20 ms bucket so the wall-clock check fires.
                std::this_thread::sleep_for(std::chrono::milliseconds(30));
                runtime::Table t;
                // Data timestamp 5 ms - still inside bucket 0 by data time.
                t.add_column("ts", Column<Timestamp>{Timestamp{5'000'000LL}});
                t.add_column("price", Column<double>{110.0});
                t.set_properties(ibex::runtime::TableProperties::time_frame("ts"));
                return runtime::ExternValue{t};
            }
            return runtime::ExternValue{runtime::Table{}};  // EOF
        });

    // Sink: capture each emitted (post-resample) table.
    registry.register_scalar_table_consumer(
        "tick_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            emitted.push_back(t);
            return runtime::ExternValue{std::int64_t{0}};
        });

    // Use lower() (not lower_expr) so the extern declarations are registered in
    // the lowerer's table_externs_ / sink_externs_ sets before the Stream node
    // is lowered.
    const char* src = R"(
extern fn tick_src() -> TimeFrame from "fake.hpp";
extern fn tick_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = tick_src(),
    transform = [resample 20ms, select { close = last(price) }],
    sink      = tick_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    REQUIRE(result.has_value());

    // Wall-clock flush fires between the two source calls: bucket containing
    // tick 1 is emitted before tick 2 is processed, giving two sink calls.
    REQUIRE(emitted.size() == 2);

    auto* close0 = std::get_if<Column<double>>(emitted[0].find("close"));
    REQUIRE(close0 != nullptr);
    CHECK((*close0)[0] == Catch::Approx(100.0));

    auto* close1 = std::get_if<Column<double>>(emitted[1].find("close"));
    REQUIRE(close1 != nullptr);
    CHECK((*close1)[0] == Catch::Approx(110.0));
}

// Verify that a source returning StreamTimeout (receive timeout, no data) keeps
// the stream alive and still triggers the wall-clock flush.
//
// Scenario: one tick arrives in bucket 0, then the source returns StreamTimeout
// repeatedly while 30 ms elapses (> 20 ms bucket), then signals EOF.
// Expected: the bucket is flushed via the wall-clock check during the timeout
// loop - not delayed until EOF.
TEST_CASE("Stream TimeBucket flushes via StreamTimeout during idle period", "[e2e][stream]") {
    std::vector<runtime::Table> emitted;
    int call_count = 0;
    auto stream_start = std::chrono::steady_clock::now();

    runtime::ExternRegistry registry;

    registry.register_table(
        "tick_src",
        [&](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            ++call_count;
            if (call_count == 1) {
                // First call: one tick in bucket 0.
                runtime::Table t;
                t.add_column("ts", Column<Timestamp>{Timestamp{0}});
                t.add_column("price", Column<double>{99.0});
                t.set_properties(ibex::runtime::TableProperties::time_frame("ts"));
                return runtime::ExternValue{t};
            }
            // Subsequent calls: return StreamTimeout until 30 ms have elapsed,
            // then signal EOF.  No second tick is ever sent.
            auto elapsed = std::chrono::steady_clock::now() - stream_start;
            if (elapsed < std::chrono::milliseconds(30)) {
                return runtime::ExternValue{runtime::StreamTimeout{}};
            }
            return runtime::ExternValue{runtime::Table{}};  // EOF
        });

    registry.register_scalar_table_consumer(
        "tick_sink", runtime::ScalarKind::Int,
        [&](const runtime::Table& t,
            const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            emitted.push_back(t);
            return runtime::ExternValue{std::int64_t{0}};
        });

    const char* src = R"(
extern fn tick_src() -> TimeFrame from "fake.hpp";
extern fn tick_sink(df: DataFrame) -> Int from "fake.hpp";
Stream {
    source    = tick_src(),
    transform = [resample 20ms, select { close = last(price) }],
    sink      = tick_sink()
};
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());
    auto result = runtime::interpret(*lowered.value(), {}, nullptr, &registry);
    REQUIRE(result.has_value());

    // The bucket must have been flushed during the StreamTimeout loop, before EOF.
    // Without StreamTimeout support the bucket would only flush at EOF - which
    // would still yield one emission, but from the wrong trigger.
    // Here we verify the flush happened AND carried the correct value.
    REQUIRE(emitted.size() == 1);

    auto* close0 = std::get_if<Column<double>>(emitted[0].find("close"));
    REQUIRE(close0 != nullptr);
    CHECK((*close0)[0] == Catch::Approx(99.0));
}

// --- Melt --------------------------------------------------------------------

TEST_CASE("E2E: melt basic - all non-id columns melted", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("math", Column<std::int64_t>{90, 80});
    t.add_column("science", Column<std::int64_t>{85, 95});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[melt { name }];", tables);

    REQUIRE(out.rows() == 4);
    auto names = col_str(out, "name");
    auto vars = col_str(out, "variable");
    auto vals = col_i64(out, "value");

    // Row order: Alicexmath, Alicexscience, Bobxmath, Bobxscience
    CHECK(names == std::vector<std::string>{"Alice", "Alice", "Bob", "Bob"});
    CHECK(vars == std::vector<std::string>{"math", "science", "math", "science"});
    CHECK(vals == std::vector<std::int64_t>{90, 85, 80, 95});
}

TEST_CASE("E2E: melt with explicit measure columns via select", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("math", Column<std::int64_t>{90, 80});
    t.add_column("science", Column<std::int64_t>{85, 95});
    t.add_column("english", Column<std::int64_t>{88, 92});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[melt { name }, select { math, science }];", tables);

    REQUIRE(out.rows() == 4);
    auto names = col_str(out, "name");
    auto vars = col_str(out, "variable");
    auto vals = col_i64(out, "value");

    CHECK(names == std::vector<std::string>{"Alice", "Alice", "Bob", "Bob"});
    CHECK(vars == std::vector<std::string>{"math", "science", "math", "science"});
    CHECK(vals == std::vector<std::int64_t>{90, 85, 80, 95});
    // english column should not appear
    CHECK(out.find("english") == nullptr);
}

TEST_CASE("E2E: melt with multiple id columns", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("first", Column<std::string>{"A", "B"});
    t.add_column("last", Column<std::string>{"X", "Y"});
    t.add_column("score", Column<std::int64_t>{100, 200});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[melt { first, last }];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_str(out, "first") == std::vector<std::string>{"A", "B"});
    CHECK(col_str(out, "last") == std::vector<std::string>{"X", "Y"});
    CHECK(col_str(out, "variable") == std::vector<std::string>{"score", "score"});
    CHECK(col_i64(out, "value") == std::vector<std::int64_t>{100, 200});
}

TEST_CASE("E2E: melt with double measure columns", "[e2e][melt]") {
    runtime::Table t;
    t.add_column("id", Column<std::string>{"a", "b"});
    t.add_column("x", Column<double>{1.5, 2.5});
    t.add_column("y", Column<double>{3.5, 4.5});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[melt { id }];", tables);

    REQUIRE(out.rows() == 4);
    CHECK(col_dbl(out, "value") == std::vector<double>{1.5, 3.5, 2.5, 4.5});
}

// --- Dcast -------------------------------------------------------------------

TEST_CASE("E2E: dcast basic - long to wide", "[e2e][dcast]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Alice", "Bob", "Bob"});
    t.add_column("subject", Column<std::string>{"math", "science", "math", "science"});
    t.add_column("score", Column<std::int64_t>{90, 85, 80, 95});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[dcast subject, select score, by name];", tables);

    REQUIRE(out.rows() == 2);
    auto names = col_str(out, "name");
    CHECK(names == std::vector<std::string>{"Alice", "Bob"});

    auto math = col_i64(out, "math");
    auto science = col_i64(out, "science");
    CHECK(math == std::vector<std::int64_t>{90, 80});
    CHECK(science == std::vector<std::int64_t>{85, 95});
}

TEST_CASE("E2E: dcast with missing cells produces nulls", "[e2e][dcast]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("subject", Column<std::string>{"math", "science"});
    t.add_column("score", Column<std::int64_t>{90, 95});
    runtime::TableRegistry tables;
    tables.emplace("scores", std::move(t));

    auto out = run("scores[dcast subject, select score, by name];", tables);

    REQUIRE(out.rows() == 2);

    // Alice has math=90, science=null; Bob has math=null, science=95
    const auto* math_entry = out.find_entry("math");
    REQUIRE(math_entry != nullptr);
    REQUIRE(math_entry->validity.has_value());
    CHECK((*math_entry->validity)[0] == true);   // Alice has math
    CHECK((*math_entry->validity)[1] == false);  // Bob missing math

    const auto* sci_entry = out.find_entry("science");
    REQUIRE(sci_entry != nullptr);
    REQUIRE(sci_entry->validity.has_value());
    CHECK((*sci_entry->validity)[0] == false);  // Alice missing science
    CHECK((*sci_entry->validity)[1] == true);   // Bob has science
}

TEST_CASE("E2E: dcast with multiple row keys", "[e2e][dcast]") {
    runtime::Table t;
    t.add_column("first", Column<std::string>{"A", "A", "B", "B"});
    t.add_column("last", Column<std::string>{"X", "X", "Y", "Y"});
    t.add_column("metric", Column<std::string>{"height", "weight", "height", "weight"});
    t.add_column("value", Column<std::int64_t>{170, 65, 180, 75});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    auto out = run("t[dcast metric, select value, by { first, last }];", tables);

    REQUIRE(out.rows() == 2);
    CHECK(col_str(out, "first") == std::vector<std::string>{"A", "B"});
    CHECK(col_str(out, "last") == std::vector<std::string>{"X", "Y"});
    CHECK(col_i64(out, "height") == std::vector<std::int64_t>{170, 180});
    CHECK(col_i64(out, "weight") == std::vector<std::int64_t>{65, 75});
}

TEST_CASE("E2E: melt then dcast roundtrip", "[e2e][melt][dcast]") {
    runtime::Table t;
    t.add_column("name", Column<std::string>{"Alice", "Bob"});
    t.add_column("math", Column<std::int64_t>{90, 80});
    t.add_column("science", Column<std::int64_t>{85, 95});
    runtime::TableRegistry tables;
    tables.emplace("wide", std::move(t));

    // Melt then dcast should recover the original shape.
    auto melted = run("wide[melt { name }];", tables);
    runtime::TableRegistry tables2;
    tables2.emplace("long", std::move(melted));

    auto out = run("long[dcast variable, select value, by name];", tables2);

    REQUIRE(out.rows() == 2);
    auto names = col_str(out, "name");
    CHECK(names == std::vector<std::string>{"Alice", "Bob"});
    CHECK(col_i64(out, "math") == std::vector<std::int64_t>{90, 80});
    CHECK(col_i64(out, "science") == std::vector<std::int64_t>{85, 95});
}

// --- Tuple-LHS column assignment ---------------------------------------------

TEST_CASE("E2E: tuple-LHS update unpacks two columns from extern fn", "[e2e]") {
    // The extern function returns a two-column DataFrame (delta, gamma).
    runtime::Table greeks;
    greeks.add_column("delta", Column<double>{0.1, 0.2, 0.3});
    greeks.add_column("gamma", Column<double>{0.01, 0.02, 0.03});

    runtime::Table base_tbl;
    base_tbl.add_column("price", Column<std::int64_t>{100, 200, 300});

    runtime::ExternRegistry registry;
    registry.register_table("get_base",
                            [&base_tbl](const runtime::ExternArgs&)
                                -> std::expected<runtime::ExternValue, std::string> {
                                return runtime::ExternValue{base_tbl};
                            });
    registry.register_table(
        "compute_greeks",
        [&greeks](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{greeks};
        });

    const char* src = R"(
extern fn get_base() -> DataFrame from "fake.hpp";
extern fn compute_greeks() -> DataFrame from "fake.hpp";
let df = get_base();
df[update { (delta, gamma) = compute_greeks() }];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("E2E: tuple-LHS update via execute_script verifies column count error", "[e2e]") {
    // The extern function returns ONE column but we expect TWO - should fail.
    runtime::Table one_col;
    one_col.add_column("delta", Column<double>{0.1, 0.2});

    runtime::Table base;
    base.add_column("price", Column<std::int64_t>{100, 200});

    runtime::TableRegistry tables;
    tables.emplace("base", std::move(base));

    runtime::ExternRegistry registry;
    registry.register_table(
        "get_base",
        [&tables](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{tables.at("base")};
        });
    registry.register_table(
        "one_col_fn",
        [&one_col](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{one_col};
        });

    const char* src = R"(
extern fn get_base() -> DataFrame from "fake.hpp";
extern fn one_col_fn() -> DataFrame from "fake.hpp";
let df = get_base();
df[update { (a, b) = one_col_fn() }];
)";
    // Should return false (error printed, not crashing)
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("E2E: update = expr merges all columns from extern fn", "[e2e]") {
    runtime::Table extra;
    extra.add_column("delta", Column<double>{0.1, 0.2, 0.3});
    extra.add_column("gamma", Column<double>{0.01, 0.02, 0.03});

    runtime::Table base_tbl;
    base_tbl.add_column("price", Column<std::int64_t>{100, 200, 300});

    runtime::ExternRegistry registry;
    registry.register_table("get_base",
                            [&base_tbl](const runtime::ExternArgs&)
                                -> std::expected<runtime::ExternValue, std::string> {
                                return runtime::ExternValue{base_tbl};
                            });
    registry.register_table(
        "compute_extras",
        [&extra](const runtime::ExternArgs&) -> std::expected<runtime::ExternValue, std::string> {
            return runtime::ExternValue{extra};
        });

    const char* src = R"(
extern fn get_base() -> DataFrame from "fake.hpp";
extern fn compute_extras() -> DataFrame from "fake.hpp";
let df = get_base();
df[update = compute_extras()];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

// --- Proof: read symbols from a file, generate 250-day correlated returns ----
//
// Mirrors the real-world pattern:
//   let data    = read_csv("symbols.csv");
//   let symbols = scalar(data, "symbol");   // e.g. "AAPL,MSFT,GOOG"
//   days[update = gen_correlated_returns(symbols)]
//
// The test pre-loads `symbols` into the scalar registry (simulating what
// scalar(data,"symbol") would produce in the REPL) and registers an extern
// function that:
//   1. Parses the comma-separated symbol list from the scalar argument.
//   2. Applies a Cholesky factor to draw 250-day correlated normal returns.
//   3. Returns a DataFrame with one column per symbol (250 rows each).
// `update = expr` then merges all those columns into the 250-day base frame.

TEST_CASE("Proof: 250-day correlated returns via update = expr", "[e2e]") {
    // -- 1. "symbols.csv" content ---------------------------------------------
    // In production: let data = read_csv("symbols.csv");
    //                let symbols = scalar(data, "symbol");
    // Here we put the scalar directly into the registry.
    runtime::ScalarRegistry scalars;
    scalars.emplace("symbols", std::string("AAPL,MSFT,GOOG"));

    // -- 2. Base frame: 250 trading-day skeleton -------------------------------
    constexpr int n_days = 250;
    runtime::Table days_tbl;
    {
        Column<std::int64_t> idx;
        for (int i = 1; i <= n_days; ++i)
            idx.push_back(i);
        days_tbl.add_column("day", std::move(idx));
    }
    runtime::TableRegistry tables;
    tables.emplace("days", std::move(days_tbl));

    // -- 3. Extern: gen_correlated_returns(syms: String) -> DataFrame ----------
    // Cholesky factor L for 3-asset correlation matrix:
    //   rho(AAPL,MSFT) = 0.70,  rho(AAPL,GOOG) = 0.50,  rho(MSFT,GOOG) = 0.60
    //
    //   C = [[1.00, 0.70, 0.50],    L = [[1.0000,  0,      0     ],
    //        [0.70, 1.00, 0.60],         [0.7000,  0.7141, 0     ],
    //        [0.50, 0.60, 1.00]]         [0.5000,  0.3499, 0.7929]]
    //
    // Correlated draw: r = L * z  (z ~ N(0,I)),  scaled by daily_vol.
    runtime::ExternRegistry externs;
    externs.register_table(
        "gen_correlated_returns",
        [](const runtime::ExternArgs& args) -> std::expected<runtime::ExternValue, std::string> {
            if (args.empty())
                return std::unexpected("gen_correlated_returns: missing symbol-list argument");
            const auto* sym_str = std::get_if<std::string>(&args[0]);
            if (!sym_str)
                return std::unexpected("gen_correlated_returns: argument must be a String");

            std::vector<std::string> syms;
            {
                std::stringstream ss(*sym_str);
                std::string tok;
                while (std::getline(ss, tok, ','))
                    syms.push_back(tok);
            }
            if (syms.empty())
                return std::unexpected("gen_correlated_returns: empty symbol list");

            const std::size_t n = syms.size();
            // Cholesky (hardcoded for 3-asset; clamp at 3 for safety)
            const double L[3][3] = {
                {1.0000, 0.0000, 0.0000}, {0.7000, 0.7141, 0.0000}, {0.5000, 0.3499, 0.7929}};
            constexpr double daily_vol = 0.01;

            // NOLINTNEXTLINE(cert-msc51-cpp, cert-msc32-c)
            std::mt19937 rng{42};
            std::normal_distribution<double> std_norm;

            // n_days x n independent standard normals
            std::vector<std::vector<double>> z(n_days, std::vector<double>(n));
            for (auto& row : z)
                for (auto& v : row)
                    v = std_norm(rng);

            // Apply Cholesky and scale
            runtime::Table result;
            for (std::size_t s = 0; s < n && s < 3; ++s) {
                Column<double> col;
                col.reserve(n_days);
                for (std::size_t d = 0; d < n_days; ++d) {
                    double r = 0.0;
                    for (std::size_t k = 0; k <= s; ++k)
                        r += L[s][k] * z[d][k];
                    col.push_back(r * daily_vol);
                }
                result.add_column(syms[s], std::move(col));
            }
            return runtime::ExternValue{std::move(result)};
        });

    // -- 4. Ibex script --------------------------------------------------------
    // `symbols` is resolved from the scalar registry at runtime.
    const char* src = R"(
extern fn gen_correlated_returns(syms: String) -> DataFrame from "quant.so";
days[update = gen_correlated_returns(symbols)];
)";

    auto out = run(src, tables, &scalars, &externs);

    // -- 5. Shape checks -------------------------------------------------------
    REQUIRE(out.rows() == n_days);
    REQUIRE(out.columns.size() == 4);  // day + AAPL + MSFT + GOOG
    REQUIRE(out.find("day") != nullptr);
    REQUIRE(out.find("AAPL") != nullptr);
    REQUIRE(out.find("MSFT") != nullptr);
    REQUIRE(out.find("GOOG") != nullptr);

    // -- 6. Correlation checks -------------------------------------------------
    // Verify that the Cholesky structure produced the intended correlations.
    auto pearson = [&](const std::string& a, const std::string& b) {
        const auto& ca = std::get<Column<double>>(*out.find(a));
        const auto& cb = std::get<Column<double>>(*out.find(b));
        double ma = 0;
        double mb = 0;
        for (std::size_t i = 0; i < n_days; ++i) {
            ma += ca[i];
            mb += cb[i];
        }
        ma /= n_days;
        mb /= n_days;
        double cov = 0;
        double va = 0;
        double vb = 0;
        for (std::size_t i = 0; i < n_days; ++i) {
            double da = ca[i] - ma;
            double db = cb[i] - mb;
            cov += da * db;
            va += da * da;
            vb += db * db;
        }
        return cov / std::sqrt(va * vb);
    };

    CHECK(pearson("AAPL", "MSFT") == Catch::Approx(0.70).epsilon(0.12));
    CHECK(pearson("AAPL", "GOOG") == Catch::Approx(0.50).epsilon(0.12));
    CHECK(pearson("MSFT", "GOOG") == Catch::Approx(0.60).epsilon(0.12));
}

// --- Phase 1 serial-morsel equivalence ---------------------------------------
//
// With a compute budget of two or more, an eligible row-local chain is executed
// over a PartitionedTableSource in morsel grains. This slice runs the morsels
// serially, so the result must be byte-identical to the plain serial path. A
// small grain (relative to the row count) forces several morsels and a range
// boundary that a null straddles.

namespace {

// 11-row table so grain=3 yields 4 morsels, with a partial last morsel and a
// filter that keeps rows straddling every range boundary.
auto make_pipeline_table() -> runtime::TableRegistry {
    runtime::Table t;
    t.add_column("price", Column<std::int64_t>{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110});
    t.add_column("qty", Column<std::int64_t>{5, 3, 8, 2, 1, 7, 4, 6, 9, 2, 3});
    t.add_column("symbol", Column<std::string>{"AAPL", "GOOG", "AAPL", "GOOG", "AAPL", "MSFT",
                                               "AAPL", "GOOG", "MSFT", "AAPL", "GOOG"});
    runtime::TableRegistry reg;
    reg.emplace("t", std::move(t));
    return reg;
}

}  // namespace

TEST_CASE("E2E: parallel serial-morsel matches serial output", "[e2e][parallel]") {
    auto tables = make_pipeline_table();
    const std::size_t grain = 3;  // 11 rows -> 4 morsels

    // Each case is an eligible parallel-map shape: bare filter, fused
    // filter+project, fused filter+update+project, project, and rename.
    const char* cases[] = {
        "t[filter price > 35];",
        "t[filter price > 35, select { price, qty }];",
        "t[filter qty > 2, select { price, notional = price * qty }];",
        "t[select { price, qty }];",
        "t[filter price > 15][rename px = price];",
    };
    for (const auto* src : cases) {
        INFO("query: " << src);
        auto serial = run(src, tables);
        auto parallel = run_parallel(src, tables, grain);
        require_tables_equal(serial, parallel);
    }
}

TEST_CASE("E2E: parallel serial-morsel falls back to serial for ineligible shapes",
          "[e2e][parallel]") {
    auto tables = make_pipeline_table();
    // lag() is not row-local: the pipeline is serial-only, so the seam
    // must leave the query on the untouched serial chain and still succeed.
    auto serial = run("t[select { price, prev = lag(price, 1) }];", tables);
    auto parallel = run_parallel("t[select { price, prev = lag(price, 1) }];", tables, 3);
    require_tables_equal(serial, parallel);
}

TEST_CASE("E2E: parallel serial-morsel preserves metadata and an all-filtered schema",
          "[e2e][parallel]") {
    runtime::Table t;
    t.add_column("ts", Column<std::int64_t>{400, 100, 300, 200, 500});
    t.add_column("value", Column<std::int64_t>{4, 1, 3, 2, 5});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    // The Ascribe node is a serial boundary; filter + rename after it is the
    // partitioned pipeline. Renaming the time-index column exercises metadata
    // propagation through every morsel and the final materialization.
    constexpr auto metadata_query = R"(as_timeframe(t, "ts")[filter value > 1][rename time = ts];)";
    auto serial_metadata = run(metadata_query, tables);
    auto pipeline_metadata = run_parallel(metadata_query, tables, 2);
    require_tables_equal(serial_metadata, pipeline_metadata);
    REQUIRE(pipeline_metadata.time_index() == "time");
    REQUIRE(pipeline_metadata.ordering().has_value());
    REQUIRE((*pipeline_metadata.ordering())[0].name == "time");

    // Grain 2 produces three input morsels. Every one is rejected. The
    // serial-morsel validator requires one identified output morsel per input,
    // while final materialization must retain the serial schema and zero rows.
    const char* all_filtered_cases[] = {
        "t[filter value > 99];",
        "t[filter value > 99, select { ts, value }];",
        "t[filter value > 99, select { ts, doubled = value * 2 }];",
    };
    for (const auto* query : all_filtered_cases) {
        INFO("all-filtered query: " << query);
        auto serial_empty = run(query, tables);
        auto pipeline_empty = run_parallel(query, tables, 2);
        require_tables_equal(serial_empty, pipeline_empty);
        REQUIRE(pipeline_empty.columns.size() == 2);
        REQUIRE(pipeline_empty.rows() == 0);
    }
}

TEST_CASE("E2E: parallel serial-morsel handles a zero-row input", "[e2e][parallel]") {
    // A zero-row input still yields exactly one empty schema-carrier morsel
    // (partitioned_morsel_count -> 1), which the SerialMorselOrderValidator must
    // accept, and the result must keep the schema with zero rows.
    runtime::Table t;
    t.add_column("x", Column<std::int64_t>{});
    t.add_column("y", Column<double>{});
    runtime::TableRegistry tables;
    tables.emplace("e", std::move(t));

    const char* cases[] = {
        "e[filter x > 0];",
        "e[filter x > 0, select { x, y }];",
        "e[filter x > 0, select { x, z = y * 2 }];",
        "e[select { x, y }];",
    };
    for (const auto* query : cases) {
        INFO("zero-row query: " << query);
        auto serial = run(query, tables);
        auto pipeline = run_parallel(query, tables, 4);
        require_tables_equal(serial, pipeline);
        REQUIRE(pipeline.rows() == 0);
    }
}

// A left join whose aggregate is a count or sum on the join key takes a fused
// path that aggregates the join output directly, walking past the map nodes
// between the two. It used to walk past FilterProject as well, which dropped
// that node's predicate: the answer came back with a group per join key
// instead of a group per surviving row, silently. The expected values here are
// computed by hand rather than from a second query, because a second query
// canonicalizes to the same shape and would take the same path.
TEST_CASE("E2E: a filter between a left join and its aggregate is not dropped",
          "[e2e][join][aggregate]") {
    runtime::Table left;
    left.add_column("k", Column<std::int64_t>{1, 2, 3});
    runtime::Table right;
    right.add_column("k", Column<std::int64_t>{1, 1, 2});
    right.add_column("v", Column<std::int64_t>{100, 1, 100});
    runtime::TableRegistry tables;
    tables.emplace("l", std::move(left));
    tables.emplace("r", std::move(right));

    // Join rows: (1,100) (1,1) (2,100) (3,null). `v > k` keeps (1,100) and
    // (2,100) — it reads both sides, so no pushdown can move it below the
    // join. Grouping the survivors by k: k=1 sums to 1, k=2 sums to 2, and k=3
    // is gone entirely.
    const auto* src =
        "(l left join r on k)[filter v > k][select { k, v }][select { n = sum(k) }, by k];";
    auto out = run(src, tables);
    REQUIRE(out.rows() == 2);
    CHECK(col_i64(out, "k") == std::vector<std::int64_t>{1, 2});
    CHECK(col_i64(out, "n") == std::vector<std::int64_t>{1, 2});

    // The same, in parallel: the rewrite lives on both the chunked and the
    // interpreter side and both walked the same way.
    auto parallel = run_parallel(src, tables, 2);
    require_tables_equal(out, parallel);
}

// The same rewrite, and the other half of what it used to assume: an update
// between the join and the aggregate computes a column the join output does not
// have. It used to walk past that update and remap the aggregate onto whatever
// single column the field referenced, which is right for exactly one shape --
// the `Int64(col is not null)` flag `count(col)` lowers to -- and wrong for any
// field that computes a value. `w = v * 2` failed outright ("aggregate column
// not found: w"); summing `v` in its place would have been worse, since it
// would have answered.
TEST_CASE("E2E: an update between a left join and its aggregate is not bypassed",
          "[e2e][join][aggregate]") {
    runtime::Table left;
    left.add_column("k", Column<std::int64_t>{1, 2, 3});
    runtime::Table right;
    right.add_column("k", Column<std::int64_t>{1, 1, 2});
    right.add_column("v", Column<std::int64_t>{100, 1, 100});
    runtime::TableRegistry tables;
    tables.emplace("l", std::move(left));
    tables.emplace("r", std::move(right));

    // Join rows: (1,100) (1,1) (2,100) (3,null). Doubling v and summing by k:
    // k=1 -> 200+2, k=2 -> 200, k=3 -> null doubled is null, so the sum of an
    // all-null group.
    const auto* src = "(l left join r on k)[update { w = v * 2 }][select { n = sum(w) }, by k];";
    auto out = run(src, tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "k") == std::vector<std::int64_t>{1, 2, 3});
    const auto* sums = std::get_if<Column<std::int64_t>>(out.find("n"));
    REQUIRE(sums != nullptr);
    CHECK((*sums)[0] == 202);
    CHECK((*sums)[1] == 200);

    require_tables_equal(out, run_parallel(src, tables, 2));
}

// And the shape the fast path exists for still takes it: `count(col)` over a
// left join, which lowers to that flag update plus a sum. This is the case the
// vetting must keep admitting -- a rule that declined everything would pass the
// test above and quietly cost the queries this rewrite was written for.
TEST_CASE("E2E: count over a left join still uses the fused rewrite", "[e2e][join][aggregate]") {
    runtime::Table left;
    left.add_column("k", Column<std::int64_t>{1, 2, 3});
    runtime::Table right;
    right.add_column("k", Column<std::int64_t>{1, 1, 2});
    right.add_column("v", Column<std::int64_t>{100, 1, 100});
    runtime::TableRegistry tables;
    tables.emplace("l", std::move(left));
    tables.emplace("r", std::move(right));

    // k=1 matches twice, k=2 once, k=3 not at all — and an unmatched left row
    // counts zero, which is the whole point of counting the right column.
    const auto* src = "(l left join r on k)[select { n = count(v) }, by k];";
    auto out = run(src, tables);
    REQUIRE(out.rows() == 3);
    CHECK(col_i64(out, "k") == std::vector<std::int64_t>{1, 2, 3});
    CHECK(col_i64(out, "n") == std::vector<std::int64_t>{2, 1, 0});
    require_tables_equal(out, run_parallel(src, tables, 2));
}

TEST_CASE("E2E: a column-less row scaffold survives the parallel path", "[e2e][parallel]") {
    // A `Table(n)` scaffold carries its row count in `logical_rows` with no
    // columns, which is the shape most likely to be dropped by a path that
    // reasons about columns.
    //
    // It no longer runs over morsels. The plan is Project(Update(Scan)): the
    // update bounds the parallel prefix now, leaving a prefix of just the
    // project, and a metadata-only chain has no per-row work to spread
    // (NoRowWork). Both of those are deliberate, so this asserts the reason
    // — a different reason would mean something else changed. The morsel
    // source's own column-less handling is covered in test_operator.cpp.
    {
        auto parsed = parser::parse("Table(3)[select { c = 1 }];");
        REQUIRE(parsed.has_value());
        auto lowered = parser::lower(*parsed);
        REQUIRE(lowered.has_value());
        const auto plan =
            runtime::physical::plan_physical(**lowered, runtime::TableRegistry{}, nullptr);
        CHECK(plan.mode == runtime::physical::PipelineMode::Serial);
        CHECK(plan.serial_reason == runtime::physical::SerialOnlyReason::NoRowWork);
    }
    auto serial = run("Table(3)[select { c = 1 }];", {});
    auto parallel = run_parallel("Table(3)[select { c = 1 }];", {}, 2);
    require_tables_equal(serial, parallel);
    REQUIRE(parallel.rows() == 3);
    CHECK(col_i64(parallel, "c") == std::vector<std::int64_t>{1, 1, 1});
}

// --- Phase 1 worker-pool pipeline equivalence ----------------------------------
//
// Same pipeline, now fanned out across worker threads and reassembled by the
// ordered merger. The contract is unchanged and absolute: byte-identical to the
// serial result, for every worker count and every morsel grain. Anything else
// would mean the merger, not the map, decides the answer.

namespace {

// Wide enough that a small grain produces far more morsels than the in-flight
// ring holds, so the merger's backpressure and slot reuse are actually
// exercised rather than skipped by a one-pass fit. Nulls land at irregular
// positions so they straddle range boundaries at every grain.
auto make_wide_pipeline_table(std::size_t rows) -> runtime::TableRegistry {
    Column<std::int64_t> price;
    Column<std::int64_t> qty;
    Column<std::string> symbol;
    price.reserve(rows);
    qty.reserve(rows);
    symbol.reserve(rows);
    runtime::ValidityBitmap qty_valid;
    qty_valid.reserve(rows);
    const char* symbols[] = {"AAPL", "GOOG", "MSFT"};
    for (std::size_t i = 0; i < rows; ++i) {
        price.push_back(static_cast<std::int64_t>((i * 37) % 1000));
        qty.push_back(static_cast<std::int64_t>(i % 13));
        symbol.push_back(symbols[i % 3]);
        qty_valid.push_back(i % 7 != 3);
    }
    runtime::Table t;
    t.add_column("price", std::move(price));
    t.add_column("qty", std::move(qty));
    t.add_column("symbol", std::move(symbol));
    t.columns[1].validity = std::move(qty_valid);

    runtime::TableRegistry reg;
    reg.emplace("t", std::move(t));
    return reg;
}

}  // namespace

TEST_CASE("E2E: morsel pipeline on worker threads matches serial output", "[e2e][parallel]") {
    auto tables = make_wide_pipeline_table(1000);

    // A select/rename above a filter rides in the filter's pipeline, which is why
    // Project and Rename stay ParallelMap even though a chain of only those is
    // refused.
    const char* cases[] = {
        "t[filter price > 350];",
        "t[filter price > 350, select { price, qty }];",
        "t[filter qty > 2, select { price, notional = price * qty }];",
        "t[filter price > 150][rename px = price];",
        "t[filter price > 150][select { price, qty }];",
        "t[filter price > 995];",  // very selective: most morsels are empty
    };
    for (const auto* src : cases) {
        INFO("query: " << src);
        auto serial = run(src, tables);
        // Grains chosen so morsel counts (143, 34, 8) bracket the ring window,
        // and worker counts that both divide and do not divide them.
        for (const std::size_t grain : {7U, 30U, 128U}) {
            for (const std::size_t threads : {2U, 3U, 8U}) {
                INFO("grain " << grain << ", threads " << threads);
                require_tables_equal(serial, run_on_workers(src, tables, grain, threads));
            }
        }
    }
}

TEST_CASE("E2E: a row-local update is parallelized inside the operator", "[e2e][parallel]") {
    auto tables = make_wide_pipeline_table(1000);

    // An update is no longer a pipeline (see execution_capability(const Node&)):
    // `update_table` splits the field computation across threads itself, which
    // costs no copies where a morsel pipeline costs two whole-table ones. What
    // has to hold either way is that the answer does not change — including for
    // the shapes that stress it: a field consumed by a later operator, an
    // overwrite of an existing nullable column, and a rename of a computed one.
    // Shapes whose update reaches `update_table`, so the field split is what
    // provides the parallelism.
    const char* split_cases[] = {
        "t[update { n = price * 2 }];",
        "t[update { n = price * qty + 1 }][filter n > 100];",
        "t[update { n = price * 2 }][rename m = n];",
        "t[update { qty = qty + 1 }];",                   // overwrites an existing nullable column
        "t[update { a = price * 2, b = a + 1 }];",        // a later field reads an earlier one
        "t[update { n = pmin(price * qty + 1, 200) }];",  // compiled min + nested arithmetic
        "t[update { flag = price > 100 }];",     // packed Bool windows meet at word boundaries
        "t[update { tag = `sym=${symbol}` }];",  // String windows own byte-prefix slabs
    };
    for (const auto* src : split_cases) {
        INFO("query: " << src);
        auto serial = run(src, tables);
        for (const std::size_t threads : {2U, 8U}) {
            INFO("threads " << threads);
            // grain 7 over 1000 rows is ~143 morsels, so the field evaluation
            // really does fan out — asserted, because a silent fall back to one
            // whole-table evaluation would leave the equality check green.
            runtime::ParallelPipelineStats stats;
            require_tables_equal(serial, run_parallel(src, tables, 7, threads, &stats));
            CHECK(stats.parallel_fields.load() > 0);
            if (std::string_view(src) != "t[update { flag = price > 100 }];" &&
                std::string_view(src) != "t[update { tag = `sym=${symbol}` }];") {
                CHECK(stats.parallel_direct_numeric_fields.load() > 0);
            }
        }
    }

    // `filter … update … select` canonicalizes to the fused FilterUpdateProject,
    // which is filter-shaped — its cardinality is data-dependent, so it stays an
    // pipeline and its update never reaches `update_table`. Equality is all that
    // is claimed here.
    {
        const auto* src = "t[filter price > 100][update { n = price - qty }][select { price, n }];";
        INFO("query: " << src);
        auto serial = run(src, tables);
        for (const std::size_t threads : {2U, 8U}) {
            INFO("threads " << threads);
            require_tables_equal(serial, run_on_workers(src, tables, 7, threads));
        }
    }
}

TEST_CASE("E2E: categorical interpolation writes parallel string windows", "[e2e][parallel]") {
    constexpr std::size_t kRows = 1000;
    std::vector<Column<Categorical>::code_type> codes;
    codes.reserve(kRows);
    runtime::ValidityBitmap validity;
    validity.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        codes.push_back(static_cast<Column<Categorical>::code_type>(row % 3));
        validity.push_back(row % 11 != 5);
    }
    runtime::Table table;
    table.add_column(
        "symbol",
        Column<Categorical>{std::vector<std::string>{"AAPL", "GOOG", "MSFT"}, std::move(codes)},
        std::move(validity));
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(table));

    const auto* source = "t[update { label = `sym=${symbol}` }];";
    const auto serial = run(source, tables);
    for (const std::size_t threads : {2U, 8U}) {
        runtime::ParallelPipelineStats stats;
        require_tables_equal(serial, run_parallel(source, tables, 7, threads, &stats));
        CHECK(stats.parallel_fields.load() > 0);
    }
}

TEST_CASE("E2E: temporal interpolation writes parallel string windows", "[e2e][parallel]") {
    constexpr std::size_t kRows = 1000;
    Column<Date> days;
    Column<Timestamp> times;
    runtime::ValidityBitmap day_validity;
    days.reserve(kRows);
    times.reserve(kRows);
    day_validity.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        days.push_back(Date{static_cast<std::int32_t>(row) - 500});
        times.push_back(Timestamp{static_cast<std::int64_t>(row) * 1'000'000'000});
        day_validity.push_back(row % 13 != 4);
    }
    runtime::Table table;
    table.add_column("day", std::move(days), std::move(day_validity));
    table.add_column("time", std::move(times));
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(table));

    const auto* source = "t[update { label = `d=${day} t=${time}` }];";
    const auto serial = run(source, tables);
    for (const std::size_t threads : {2U, 8U}) {
        runtime::ParallelPipelineStats stats;
        require_tables_equal(serial, run_parallel(source, tables, 7, threads, &stats));
        CHECK(stats.parallel_fields.load() > 0);
    }
}

TEST_CASE("E2E: morsel pipeline leaves a non-row-local update serial", "[e2e][parallel]") {
    auto tables = make_wide_pipeline_table(1000);
    // Each of these would be silently wrong per morsel: the transform reads
    // neighbouring rows, and the grouped/guarded forms are not row-local at
    // all. The result must match the serial answer, which means the pipeline
    // declined them. (An ungrouped aggregate in an update is rejected by
    // evaluation itself, so it is covered in the lowerer test instead.)
    const char* cases[] = {
        "t[update { c = cumsum(price) }];",
        "t[update { m = price }, by symbol];",
        "t[where price > 500 update { price = 0 }];",
    };
    for (const auto* src : cases) {
        INFO("query: " << src);
        auto serial = run(src, tables);
        runtime::ParallelPipelineStats stats;
        auto parallel = run_parallel(src, tables, 7, 8, &stats);
        require_tables_equal(serial, parallel);
    }
}

TEST_CASE("E2E: morsel pipeline on worker threads preserves metadata and an empty result",
          "[e2e][parallel]") {
    runtime::Table t;
    Column<std::int64_t> ts;
    Column<std::int64_t> value;
    ts.reserve(200);
    value.reserve(200);
    for (std::size_t i = 0; i < 200; ++i) {
        ts.push_back(static_cast<std::int64_t>(i) * 10);
        value.push_back(static_cast<std::int64_t>(i));
    }
    t.add_column("ts", std::move(ts));
    t.add_column("value", std::move(value));
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(t));

    // The pipeline runs after the Ascribe barrier; time_index and ordering must
    // survive every morsel and the merge.
    constexpr auto metadata_query = R"(as_timeframe(t, "ts")[filter value > 1][rename time = ts];)";
    auto serial_metadata = run(metadata_query, tables);
    auto pipeline_metadata = run_on_workers(metadata_query, tables, 8, 4);
    require_tables_equal(serial_metadata, pipeline_metadata);
    REQUIRE(pipeline_metadata.time_index() == "time");
    REQUIRE(pipeline_metadata.ordering().has_value());

    // Every morsel is empty: the merger must still see one output morsel per
    // input morsel and the result must keep the serial schema.
    auto serial_empty = run("t[filter value > 9999, select { ts, value }];", tables);
    auto pipeline_empty =
        run_on_workers("t[filter value > 9999, select { ts, value }];", tables, 8, 4);
    require_tables_equal(serial_empty, pipeline_empty);
    REQUIRE(pipeline_empty.columns.size() == 2);
    REQUIRE(pipeline_empty.rows() == 0);
}

namespace {

// Run a pipeline query that fails inside every morsel, optionally with an
// interrupt pending or arriving mid-flight. Returns the reported error.
auto run_failing_pipeline(bool interrupt_before, bool interrupt_during) -> std::string {
    auto tables = make_wide_pipeline_table(200000);
    // A string/number comparison is rejected by the filter at evaluation time,
    // so it is a genuine per-morsel worker failure rather than a build error.
    auto parsed = parser::parse(R"(t[filter price > "a"];)");
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());

    runtime::ExecutionContext exec;
    exec.parallel_grain = 64;
    exec.parallel_threads = 4;
    exec.parallel_min_rows = 0;
    exec.parallel_min_cells = 0;

    if (interrupt_before) {
        runtime::request_interrupt();
    }
    std::thread interrupter;
    if (interrupt_during) {
        interrupter = std::thread([] { runtime::request_interrupt(); });
    }
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr, nullptr, exec);
    if (interrupter.joinable()) {
        interrupter.join();
    }
    runtime::clear_interrupt();

    REQUIRE_FALSE(result.has_value());
    return result.error();
}

}  // namespace

TEST_CASE("E2E: morsel pipeline reports a worker failure", "[e2e][parallel]") {
    // Every morsel fails. The pipeline must surface the failure (not hang waiting
    // for a morsel that will never arrive) and must report the same message the
    // serial path does, rather than a thread-timing-dependent one.
    CHECK(run_failing_pipeline(false, false) == "filter: cannot compare string and numeric");
}

TEST_CASE("E2E: morsel pipeline reports interruption over a concurrent worker failure",
          "[e2e][parallel]") {
    // Both conditions hold at once: a pending interrupt and a worker error
    // recorded by every morsel. Cancellation outranks the data error, or Ctrl+C
    // would surface as an arbitrary query error depending on which thread won.
    CHECK(run_failing_pipeline(true, false) == runtime::interrupt_message());

    // The same collision, now genuinely racing: whichever side wins, the query
    // must report one of the two defined answers and must terminate cleanly.
    const std::string raced = run_failing_pipeline(false, true);
    CHECK((raced == runtime::interrupt_message() ||
           raced == "filter: cannot compare string and numeric"));
}

TEST_CASE("E2E: morsel pipeline cancels cleanly when interrupted", "[e2e][parallel]") {
    // A pending interrupt must unwind the pipeline through the usual error
    // channel — cancelling in-flight workers and joining them before the
    // operator (which owns the table they read) is destroyed. The value of the
    // test is that it terminates and does not use freed memory.
    auto tables = make_wide_pipeline_table(1000);
    auto parsed = parser::parse("t[filter price > 350, select { price, qty }];");
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());

    runtime::ExecutionContext exec;
    exec.parallel_grain = 7;
    exec.parallel_threads = 4;
    exec.parallel_min_rows = 0;
    exec.parallel_min_cells = 0;

    runtime::request_interrupt();
    auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr, nullptr, exec);
    runtime::clear_interrupt();

    REQUIRE_FALSE(result.has_value());
    CHECK(result.error() == runtime::interrupt_message());
}

TEST_CASE("E2E: a filter pipeline absorbs its head into a range-evaluating source",
          "[e2e][parallel]") {
    // The zero-copy path is a silent optimization: if `range_filter_head` ever
    // stopped matching, every pipeline would go back to gathering each morsel and
    // the whole suite would still pass. These assertions are the only thing
    // that would notice.
    auto tables = make_wide_pipeline_table(1000);

    SECTION("a filter head is absorbed") {
        runtime::ParallelPipelineStats stats;
        auto pipeline = run_parallel("t[filter price > 350];", tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.range_heads.load() == 1);
        // Absorbing the head must not change the answer.
        require_tables_equal(run("t[filter price > 350];", tables), pipeline);
    }

    SECTION("a filter head still absorbed when operators follow it") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350, select { price, qty }];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.range_heads.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a scalar-call predicate is absorbed once evaluate_field is range-native") {
        // `abs` routes through evaluate_field, whose fused numeric tree and
        // per-row loop are both range-aware. This was deliberately excluded
        // while only the per-row loop was threaded, because declining the fused
        // tree per morsel cost more than gathering.
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter abs(price) > 350];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.range_heads.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a scalar call in one arm of a conjunction is still absorbed") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350 && abs(qty) < 5];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.range_heads.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a whole-column builtin keeps the gathering source") {
        // rolling/cum/lag read neighbouring rows, so a range would change the
        // answer rather than just the cost. Permanently non-range-native — and
        // evaluate_field aborts rather than compute one, so if this ever
        // regressed the abort would fire instead of a wrong answer.
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter lag(price, 1) > 350];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        CHECK(stats.range_heads.load() == 0);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a project head keeps the gathering source") {
        // Project is still a ParallelMap, so it forms a pipeline — but it is not
        // a shape `range_filter_head` absorbs, so it gathers.
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 100][select { price, qty }];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() >= 1);
        require_tables_equal(run(src, tables), pipeline);
    }
}

namespace {

// A table whose every column can be gathered into concurrently: no validity
// bitmap and no `Column<bool>`, both of which pack 64 rows to a word. The
// string column is deliberately variable-width, because that is what forces the
// two-phase pass to prefix-sum *bytes* as well as rows.
auto make_two_phase_table(std::size_t rows) -> runtime::TableRegistry {
    Column<std::int64_t> price;
    Column<double> weight;
    Column<std::string> symbol;
    price.reserve(rows);
    weight.reserve(rows);
    symbol.reserve(rows);
    const char* symbols[] = {"A", "BB", "CCCC", "DDDDDDDD", ""};
    for (std::size_t i = 0; i < rows; ++i) {
        price.push_back(static_cast<std::int64_t>((i * 37) % 1000));
        weight.push_back(static_cast<double>(i) * 0.5);
        symbol.push_back(symbols[i % 5]);
    }
    runtime::Table t;
    t.add_column("price", std::move(price));
    t.add_column("weight", std::move(weight));
    t.add_column("symbol", std::move(symbol));

    runtime::TableRegistry reg;
    reg.emplace("t", std::move(t));
    return reg;
}

}  // namespace

TEST_CASE("E2E: a lone filter pipeline presizes its output instead of merging", "[e2e][parallel]") {
    // The two-phase filter is a silent optimization in the strongest sense: it
    // produces byte-identical output to the ordered merger, so nothing but a
    // counter can tell which one ran. Every section therefore asserts BOTH the
    // path taken and the answer — a `two_phase_filters` assertion alone would
    // pass against a broken gather, and an equality assertion alone would pass
    // against a silent fall back to the merger.
    auto tables = make_two_phase_table(1000);

    SECTION("a bare filter takes the two-phase path") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a fused projection takes it too") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350, select { symbol, weight }];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("an empty result presizes to zero rows") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 100000];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("keeping every row still lands each morsel at its own offset") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price >= 0];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a grain that does not divide the input") {
        // The last morsel is short, so its prefix-sum offset is the one that
        // exposes an off-by-one in the row or byte accounting.
        for (const std::size_t grain : {std::size_t{1}, std::size_t{3}, std::size_t{64},
                                        std::size_t{65}, std::size_t{333}, std::size_t{999}}) {
            runtime::ParallelPipelineStats stats;
            const auto* src = "t[filter price > 200];";
            auto pipeline = run_parallel(src, tables, grain, 4, &stats);
            CAPTURE(grain);
            require_tables_equal(run(src, tables), pipeline);
        }
    }

    SECTION("a metadata-only operator above the filter keeps the fast path") {
        // Rename copies no rows, so it runs once over the finished output
        // rather than forcing the chain back onto the merger.
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350][rename px = price];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a chain of metadata-only operators above the filter") {
        runtime::ParallelPipelineStats stats;
        const auto* src =
            "t[filter price > 350][rename px = price][select { px, symbol }][rename s = symbol];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.two_phase_filters.load() == 1);
        // Ordering and time-index metadata has to survive the tail identically
        // to the serial path — which it does by construction, since these are
        // the same two functions the serial path calls.
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a row-touching operator above the filter keeps the ordered merger") {
        // An update computes per row, so it needs the per-morsel chunks the
        // two-phase form does not produce.
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350][select { doubled = price * 2 }];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        CHECK(stats.two_phase_filters.load() == 0);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a non-range-native predicate keeps the ordered merger") {
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter lag(price, 1) > 350];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        CHECK(stats.two_phase_filters.load() == 0);
        require_tables_equal(run(src, tables), pipeline);
    }
}

namespace {

// A table whose bit-packed columns are the point: `flag` is a `Column<bool>`
// and `qty` carries a validity bitmap. Both store 64 rows per word, so two
// morsels meeting mid-word write the same word.
auto make_bit_packed_table(std::size_t rows) -> runtime::TableRegistry {
    Column<std::int64_t> price;
    Column<std::int64_t> qty;
    Column<bool> flag;
    price.reserve(rows);
    qty.reserve(rows);
    flag.reserve(rows);
    runtime::ValidityBitmap qty_valid;
    qty_valid.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        price.push_back(static_cast<std::int64_t>((i * 37) % 1000));
        qty.push_back(static_cast<std::int64_t>(i % 13));
        // Deliberately irregular against 64 and against any grain below, so a
        // lost or misplaced bit changes the answer rather than cancelling out.
        flag.push_back(i % 3 == 0);
        qty_valid.push_back(i % 7 != 3);
    }
    runtime::Table t;
    t.add_column("price", std::move(price));
    t.add_column("qty", std::move(qty));
    t.add_column("flag", std::move(flag));
    t.columns[1].validity = std::move(qty_valid);

    runtime::TableRegistry reg;
    reg.emplace("t", std::move(t));
    return reg;
}

}  // namespace

namespace {

// A TimeFrame with `groups` symbols interleaved in time order, which is the
// precondition the per-group rolling buffer relies on.
auto make_grouped_window_table(std::size_t rows, std::size_t groups) -> runtime::TableRegistry {
    Column<Timestamp> ts;
    Column<std::string> symbol;
    Column<double> price;
    ts.reserve(rows);
    symbol.reserve(rows);
    price.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        ts.push_back(Timestamp{static_cast<std::int64_t>(i) * 1'000'000LL});
        symbol.push_back("S" + std::to_string(i % groups));
        price.push_back(static_cast<double>((i * 37) % 100) + 0.5);
    }
    runtime::Table t;
    t.add_column("ts", std::move(ts));
    t.add_column("symbol", std::move(symbol));
    t.add_column("price", std::move(price));
    t.set_properties(ibex::runtime::TableProperties::time_frame("ts"));

    runtime::TableRegistry reg;
    reg.emplace("t", std::move(t));
    return reg;
}

}  // namespace

TEST_CASE("E2E: a grouped windowed update spreads its groups across threads", "[e2e][parallel]") {
    // Parallelism here is across GROUPS, because a group's rolling buffer must
    // not cross a group boundary. Both directions are asserted via the counter:
    // the output is identical either way, so a gate that silently stopped
    // matching would cost the parallelism with every value test still green.
    const auto* src =
        "t[ select { price = price, open = first(price), close = last(price) },"
        "   by symbol, window 10s ];";

    auto run_grouped = [&](const runtime::TableRegistry& tables, std::size_t threads,
                           runtime::ParallelPipelineStats& stats) {
        auto parsed = parser::parse(src);
        REQUIRE(parsed.has_value());
        auto lowered = parser::lower(*parsed);
        REQUIRE(lowered.has_value());
        runtime::ExecutionContext exec;
        exec.parallel_threads = threads;
        exec.parallel_min_rows = 0;
        exec.parallel_min_cells = 0;
        exec.parallel_stats = &stats;
        auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        return std::move(*result);
    };

    SECTION("several groups fan out and match the serial answer") {
        auto tables = make_grouped_window_table(4000, 8);
        runtime::ParallelPipelineStats stats;
        auto parallel = run_grouped(tables, 4, stats);
        CHECK(stats.parallel_group_windows.load() == 1);
        require_tables_equal(run(src, tables), parallel);
    }

    SECTION("a single group has nothing to spread") {
        // The cap is the group count, not the thread count -- worth asserting,
        // because it is the reason this optimization does nothing for a
        // low-cardinality key however many cores are free.
        auto tables = make_grouped_window_table(4000, 1);
        runtime::ParallelPipelineStats stats;
        auto parallel = run_grouped(tables, 4, stats);
        CHECK(stats.parallel_group_windows.load() == 0);
        require_tables_equal(run(src, tables), parallel);
    }

    SECTION("one thread stays serial") {
        auto tables = make_grouped_window_table(4000, 8);
        runtime::ParallelPipelineStats stats;
        auto parallel = run_grouped(tables, 1, stats);
        CHECK(stats.parallel_group_windows.load() == 0);
        require_tables_equal(run(src, tables), parallel);
    }

    SECTION("a null group key forms its own group") {
        // Asserted on VALUES, not by comparing the parallel path against the
        // serial one: both now share the in-place key path, so a
        // parallel-vs-serial check could not see the grouping itself changing.
        // Six rows inside one window, alternating "A" and a null symbol:
        //   group A    -> rows 0,2,4, prices 1,3,5 -> first = 1
        //   group null -> rows 1,3,5, prices 2,4,6 -> first = 2
        // A null key merging into the empty-string group would give 1 for every
        // row; a null key merging into no group at all would give each row its
        // own price.
        Column<Timestamp> ts;
        Column<std::string> symbol;
        Column<double> price;
        runtime::ValidityBitmap symbol_valid;
        for (std::size_t i = 0; i < 6; ++i) {
            ts.push_back(Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL});
            symbol.push_back(i % 2 == 0 ? "A" : "");
            price.push_back(static_cast<double>(i + 1));
            symbol_valid.push_back(i % 2 == 0);
        }
        runtime::Table t;
        t.add_column("ts", std::move(ts));
        t.add_column("symbol", std::move(symbol));
        t.add_column("price", std::move(price));
        t.columns[1].validity = std::move(symbol_valid);
        t.set_properties(ibex::runtime::TableProperties::time_frame("ts"));
        runtime::TableRegistry tables;
        tables.emplace("t", std::move(t));

        auto result = run("t[ select { open = first(price) }, by symbol, window 10s ];", tables);
        const auto* open = result.find("open");
        REQUIRE(open != nullptr);
        const auto* col = std::get_if<Column<double>>(open);
        REQUIRE(col != nullptr);
        REQUIRE(col->size() == 6);
        // Group-major output: the three "A" rows (input 0, 2, 4), then the
        // three null-symbol rows (input 1, 3, 5). Every row's `open` is its own
        // group's first price, so 1.0 for A and 2.0 for the null group.
        CHECK((*col)[0] == 1.0);
        CHECK((*col)[1] == 1.0);
        CHECK((*col)[2] == 1.0);
        CHECK((*col)[3] == 2.0);
        CHECK((*col)[4] == 2.0);
        CHECK((*col)[5] == 2.0);
    }

    SECTION("a generator field refuses to run groups concurrently") {
        // `rand_*` draws from one shared stream, so running groups out of order
        // would change the ANSWER, not just the timing. This is the section
        // that would catch someone widening the gate to "any builtin".
        auto tables = make_grouped_window_table(4000, 8);
        const auto* gen_src =
            "t[ select { price = price, noise = rand_normal(0, 1), open = first(price) },"
            "   by symbol, window 10s ];";
        auto parsed = parser::parse(gen_src);
        REQUIRE(parsed.has_value());
        auto lowered = parser::lower(*parsed);
        REQUIRE(lowered.has_value());
        runtime::ExecutionContext exec;
        exec.parallel_threads = 4;
        exec.parallel_min_rows = 0;
        exec.parallel_min_cells = 0;
        runtime::ParallelPipelineStats stats;
        exec.parallel_stats = &stats;
        auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        CHECK(stats.parallel_group_windows.load() == 0);
    }
}

TEST_CASE("E2E: the pipeline size gate counts cells, not rows", "[e2e][parallel]") {
    // An pipeline copies rows out, so its cost scales with table WIDTH. Measured:
    // 131,072 rows won at 6 columns and lost at 2, on the same predicate --
    // both clear any sane row threshold, and only the cell count separates
    // them. A row-only gate cannot express that, which is why the narrow case
    // was the one shape still regressing.
    //
    // Asserted in both directions on the SAME table, varying only the
    // threshold: a gate that never fired and a gate that always fired would
    // each pass a one-sided test.
    auto tables = make_wide_pipeline_table(1000);  // 1000 rows x 3 columns
    const auto* src = "t[filter price > 350];";

    auto run_with_cell_gate = [&](std::size_t min_cells, runtime::ParallelPipelineStats& stats) {
        auto parsed = parser::parse(src);
        REQUIRE(parsed.has_value());
        auto lowered = parser::lower(*parsed);
        REQUIRE(lowered.has_value());
        runtime::ExecutionContext exec;
        exec.parallel_grain = 7;
        exec.parallel_threads = 4;
        exec.parallel_min_rows = 0;
        exec.parallel_min_cells = min_cells;
        exec.parallel_stats = &stats;
        auto result = runtime::interpret(*lowered.value(), tables, nullptr, nullptr, nullptr, exec);
        REQUIRE(result.has_value());
        return std::move(*result);
    };

    SECTION("enough cells fans out") {
        runtime::ParallelPipelineStats stats;
        auto pipeline = run_with_cell_gate(1000, stats);  // 3000 cells >= 1000
        CHECK(stats.parallel_pipelines.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("too few cells stays serial, and still answers identically") {
        runtime::ParallelPipelineStats stats;
        auto pipeline = run_with_cell_gate(10000, stats);  // 3000 cells < 10000
        CHECK(stats.parallel_pipelines.load() == 0);
        CHECK(stats.serial_pipelines.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("zero disables the gate") {
        runtime::ParallelPipelineStats stats;
        auto pipeline = run_with_cell_gate(0, stats);
        CHECK(stats.parallel_pipelines.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }
}

TEST_CASE("E2E: a bit-packed output column is gathered into concurrently", "[e2e][parallel]") {
    // Disjoint output ROWS are disjoint MEMORY only for columns storing at
    // least one addressable unit per row. A validity bitmap and a
    // `Column<bool>` pack 64 rows into a word, so two morsels meeting mid-word
    // read-modify-write the same word. Rather than excluding those shapes, the
    // gather zero-fills the destination, only ever sets bits, and OR-s the (at
    // most two) words it can share with a neighbour in atomically.
    SECTION("a nullable column takes the two-phase path") {
        auto tables = make_wide_pipeline_table(1000);
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350];";  // keeps nullable `qty`
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("a bool column takes the two-phase path") {
        auto tables = make_bit_packed_table(1000);
        runtime::ParallelPipelineStats stats;
        const auto* src = "t[filter price > 350];";
        auto pipeline = run_parallel(src, tables, 7, 4, &stats);
        REQUIRE(stats.parallel_pipelines.load() == 1);
        CHECK(stats.two_phase_filters.load() == 1);
        require_tables_equal(run(src, tables), pipeline);
    }

    SECTION("thousands of mid-word morsel boundaries under contention") {
        // The sizing is the test. A 1000-row table has too few boundaries and
        // too short a window for two threads to collide, so it passes whether
        // or not the shared words are written atomically — verified, by
        // mutation, when the gate refused these columns outright. This uses a
        // grain that is coprime with 64 so essentially every boundary lands
        // mid-word, and enough morsels that adjacent ones are in flight
        // together on 8 threads.
        auto tables = make_bit_packed_table(50000);
        const auto* src = "t[filter qty > 2];";
        auto serial = run(src, tables);
        for (int attempt = 0; attempt < 3; ++attempt) {
            CAPTURE(attempt);
            runtime::ParallelPipelineStats stats;
            auto pipeline = run_parallel(src, tables, 37, 8, &stats);
            REQUIRE(stats.two_phase_filters.load() == 1);
            require_tables_equal(serial, pipeline);
        }
    }
}

// A grouped update used to collect only the columns its fields APPENDED, so a
// field overwriting an existing column was evaluated -- and visible to later
// fields in the same block -- but never written back to the result. The
// ungrouped paths applied it, so the three disagreed on the same source text.
TEST_CASE("E2E: a grouped update writes back overwritten columns", "[e2e][update][by]") {
    runtime::Table t;
    t.add_column("sym", Column<std::string>{"a", "a", "b", "b"});
    t.add_column("p", Column<double>{1.0, 2.0, 3.0, 4.0});
    runtime::TableRegistry tables{{"t", t}};

    SECTION("overwrite alongside a new column") {
        auto out = run("t[update { p = 99.0, r = cumsum(p) }, by sym];", tables);
        CHECK(col_dbl(out, "p") == std::vector<double>{99.0, 99.0, 99.0, 99.0});
        // The new column saw the overwritten value, which is what made the
        // dropped write so easy to miss: `r` looked right while `p` did not.
        CHECK(col_dbl(out, "r") == std::vector<double>{99.0, 198.0, 99.0, 198.0});
    }

    SECTION("an update of only existing columns is legal") {
        // This used to fail outright with "produced no new columns".
        auto out = run("t[update { p = p * 10.0 }, by sym];", tables);
        CHECK(col_dbl(out, "p") == std::vector<double>{10.0, 20.0, 30.0, 40.0});
    }

    SECTION("the ungrouped path agrees") {
        auto grouped = run("t[update { p = p * 10.0 }, by sym];", tables);
        auto ungrouped = run("t[update { p = p * 10.0 }];", tables);
        CHECK(col_dbl(grouped, "p") == col_dbl(ungrouped, "p"));
    }
}

TEST_CASE("E2E: a grouped windowed update writes back overwritten columns",
          "[e2e][update][by][window]") {
    runtime::Table t;
    t.add_column("sym", Column<std::string>{"a", "a", "b", "b"});
    t.add_column("ts", Column<Timestamp>{Timestamp{0}, Timestamp{60'000'000'000}, Timestamp{0},
                                         Timestamp{60'000'000'000}});
    t.add_column("p", Column<double>{1.0, 2.0, 3.0, 4.0});
    runtime::TableRegistry tables{{"t", t}};

    SECTION("overwrite alongside a new column") {
        auto out =
            run("let tf = as_timeframe(t, \"ts\");\n"
                "tf[window 5m, by sym, update { p = 99.0, r = rolling_mean(p) }];",
                tables);
        CHECK(col_dbl(out, "p") == std::vector<double>{99.0, 99.0, 99.0, 99.0});
        CHECK(col_dbl(out, "r") == std::vector<double>{99.0, 99.0, 99.0, 99.0});
    }

    SECTION("a rolling result may overwrite its own source column") {
        auto out =
            run("let tf = as_timeframe(t, \"ts\");\n"
                "tf[window 5m, by sym, update { p = rolling_mean(p) }];",
                tables);
        // Per group: first row is its own mean, second is the mean of both.
        CHECK(col_dbl(out, "p") == std::vector<double>{1.0, 1.5, 3.0, 3.5});
    }
}

// `grouped_by` is a hazard flag: it records that adjacent rows may belong to
// different groups, so an unpartitioned order-dependent call would read across a
// boundary. Overwriting a grouping key's VALUES does not move any row, so the
// boundaries are still there and the flag must stay armed. Treating an overwrite
// like a dropped column (which cannot be named, so genuinely voids the claim)
// silently re-admits the unsafe query.
TEST_CASE("E2E: an overwritten grouping key keeps the row-order guard armed",
          "[e2e][update][by][guard]") {
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 2, 2});
    t.add_column("ts", Column<Timestamp>{Timestamp{0}, Timestamp{60'000'000'000}, Timestamp{0},
                                         Timestamp{60'000'000'000}});
    t.add_column("p", Column<double>{1.0, 2.0, 3.0, 4.0});
    runtime::TableRegistry tables{{"t", t}};

    const std::string grouped =
        "let tf = as_timeframe(t, \"ts\");\n"
        "let g = tf[window 5m, by k, update { r = rolling_mean(p) }];\n";

    SECTION("baseline: the guard refuses an unpartitioned lag") {
        auto err = run_error(grouped + "g[window 5m, update { lagged = lag(p, 1) }];", tables);
        CHECK(err.find("depends on the row order") != std::string::npos);
    }

    SECTION("after the grouping key is overwritten it still refuses") {
        auto err = run_error(grouped +
                                 "let o = g[window 5m, by k, update { k = 9, q = p * 2.0 }];\n"
                                 "o[window 5m, update { lagged = lag(p, 1) }];",
                             tables);
        CHECK(err.find("depends on the row order") != std::string::npos);
    }
}

// `distinct` keeps the first occurrence of each row in input order, so it
// removes rows without moving the survivors. Every claim the input made
// therefore still holds: a subset of a sorted sequence is sorted, the time index
// still indexes what is left, and dropping rows cannot merge two group-major
// runs. Three code paths used to disagree about this -- the serial main branch
// dropped all three, its empty-columns branch kept the grouping, and the chunked
// operator kept the grouping while dropping the rest.
TEST_CASE("E2E: distinct preserves every claim its input made", "[e2e][distinct]") {
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{2, 1, 1, 3});
    t.add_column("p", Column<double>{1.0, 2.0, 2.0, 4.0});
    runtime::TableRegistry tables{{"t", t}};

    SECTION("an ordering survives") {
        auto out = run("t[order k][distinct { k }];", tables);
        CHECK(col_i64(out, "k") == std::vector<std::int64_t>{1, 2, 3});
        REQUIRE(out.ordering().has_value());
        REQUIRE(out.ordering()->size() == 1);
        CHECK(out.ordering()->front().name == "k");
    }

    SECTION("a time index survives") {
        runtime::Table ts_table;
        ts_table.add_column(
            "ts", Column<Timestamp>{Timestamp{0}, Timestamp{0}, Timestamp{60'000'000'000}});
        ts_table.add_column("p", Column<double>{1.0, 1.0, 2.0});
        runtime::TableRegistry ts_tables{{"t", ts_table}};

        auto out = run("let tf = as_timeframe(t, \"ts\");\ntf[distinct { ts, p }];", ts_tables);
        CHECK(out.rows() == 2);
        CHECK(out.time_index() == std::optional<std::string>{"ts"});
    }
}

TEST_CASE("E2E: distinct keeps the row-order guard armed", "[e2e][distinct][guard]") {
    // The grouping claim is a hazard flag about the row layout. Removing
    // duplicate rows leaves the group-major runs exactly where they were, so an
    // unpartitioned lag downstream is no safer than it was before.
    runtime::Table t;
    t.add_column("k", Column<std::int64_t>{1, 1, 2, 2});
    t.add_column("ts", Column<Timestamp>{Timestamp{0}, Timestamp{60'000'000'000}, Timestamp{0},
                                         Timestamp{60'000'000'000}});
    t.add_column("p", Column<double>{1.0, 2.0, 3.0, 4.0});
    runtime::TableRegistry tables{{"t", t}};

    auto err = run_error(
        "let tf = as_timeframe(t, \"ts\");\n"
        "let g = tf[window 5m, by k, update { r = rolling_mean(p) }];\n"
        "g[distinct { k, ts, p }][window 5m, update { lagged = lag(p, 1) }];",
        tables);
    CHECK(err.find("depends on the row order") != std::string::npos);
}

// `with_timezone(ts, "Zone")` reinterprets a Timestamp column's values as wall
// clocks in `Zone` and converts them to the instants they denote. It is the cast
// for "these timestamps were never UTC" -- the numbers are right, the reading
// was wrong -- and it shifts by the offset AT THAT LOCAL TIME, which is what a
// fixed offset cannot do across a DST boundary.
TEST_CASE("E2E: with_timezone converts wall clocks to instants", "[e2e][timezone]") {
    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{
                           Timestamp{1'357'083'000'000'000'000},  // 2013-01-01 23:30 (EST)
                           Timestamp{1'372'929'300'000'000'000},  // 2013-07-04 09:15 (EDT)
                       });
    runtime::TableRegistry tables{{"t", t}};

    auto out = run("t[update { utc = with_timezone(ts, \"America/New_York\") }];", tables);
    const auto* utc = std::get_if<Column<Timestamp>>(out.find("utc"));
    REQUIRE(utc != nullptr);
    // Winter is UTC-5, summer UTC-4: a fixed offset would get one of them wrong.
    CHECK((*utc)[0].nanos == 1'357'083'000'000'000'000 + 5LL * 3600 * 1'000'000'000);
    CHECK((*utc)[1].nanos == 1'372'929'300'000'000'000 + 4LL * 3600 * 1'000'000'000);
}

TEST_CASE("E2E: with_timezone handles the two irregular local times", "[e2e][timezone]") {
    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{
                           Timestamp{1'362'882'600'000'000'000},  // 2013-03-10 02:30, skipped
                           Timestamp{1'383'442'200'000'000'000},  // 2013-11-03 01:30, repeated
                       });
    runtime::TableRegistry tables{{"t", t}};

    auto out = run("t[update { utc = with_timezone(ts, \"America/New_York\") }];", tables);
    const auto* entry = out.find_entry("utc");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());

    // 02:30 never happened that day, so there is no instant it names.
    CHECK_FALSE((*entry->validity)[0]);
    // 01:30 happened twice; a bare wall clock conventionally means the first.
    CHECK((*entry->validity)[1]);
    const auto* utc = std::get_if<Column<Timestamp>>(entry->column.get());
    CHECK((*utc)[1].nanos == 1'383'442'200'000'000'000 + 4LL * 3600 * 1'000'000'000);
}

TEST_CASE("E2E: with_timezone rejects an unknown zone", "[e2e][timezone]") {
    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{Timestamp{0}});
    runtime::TableRegistry tables{{"t", t}};

    auto err = run_error("t[update { x = with_timezone(ts, \"Mars/Olympus\") }];", tables);
    CHECK(err.find("unknown time zone") != std::string::npos);
}

// `in_timezone(ts, "Zone")` is the other half of the pair: it tags a column with
// a zone and leaves every instant alone. The two are opposites, and the round
// trip is the clearest statement of that -- `with_timezone` moves the values,
// `in_timezone` moves nothing.
TEST_CASE("E2E: in_timezone relabels without shifting", "[e2e][timezone]") {
    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{
                           Timestamp{1'357'083'000'000'000'000},  // January, zone is UTC-5
                           Timestamp{1'372'929'300'000'000'000},  // July, zone is UTC-4
                       });
    runtime::TableRegistry tables{{"t", t}};

    auto out =
        run("t[update { tagged = in_timezone(ts, \"America/New_York\"),\n"
            "           shifted = with_timezone(ts, \"America/New_York\") }];",
            tables);

    const auto* tagged = std::get_if<Column<Timestamp>>(out.find("tagged"));
    REQUIRE(tagged != nullptr);
    CHECK((*tagged)[0].nanos == 1'357'083'000'000'000'000);
    CHECK((*tagged)[1].nanos == 1'372'929'300'000'000'000);
    REQUIRE(tagged->meta().zone.has_value());
    CHECK(zone_name(*tagged->meta().zone) == "America/New_York");

    // Same zone, same input, different answer: that is the whole distinction.
    const auto* shifted = std::get_if<Column<Timestamp>>(out.find("shifted"));
    REQUIRE(shifted != nullptr);
    CHECK((*shifted)[0].nanos != (*tagged)[0].nanos);
}

// Nothing about a DST transition is irregular for a relabel: the values are
// already instants, and every instant exists in every zone. The wall clocks that
// `with_timezone` has to reject or disambiguate are simply not consulted here.
TEST_CASE("E2E: in_timezone is total across DST transitions", "[e2e][timezone]") {
    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{
                           Timestamp{1'362'882'600'000'000'000},  // 02:30 local, skipped that day
                           Timestamp{1'383'442'200'000'000'000},  // 01:30 local, repeated that day
                       });
    runtime::TableRegistry tables{{"t", t}};

    auto out = run("t[update { x = in_timezone(ts, \"America/New_York\") }];", tables);
    const auto* entry = out.find_entry("x");
    REQUIRE(entry != nullptr);
    CHECK_FALSE(entry->validity.has_value());
    const auto* x = std::get_if<Column<Timestamp>>(entry->column.get());
    REQUIRE(x != nullptr);
    CHECK((*x)[0].nanos == 1'362'882'600'000'000'000);
    CHECK((*x)[1].nanos == 1'383'442'200'000'000'000);
}

TEST_CASE("E2E: in_timezone preserves nulls and rejects bad arguments", "[e2e][timezone]") {
    runtime::ValidityBitmap ts_valid;
    ts_valid.push_back(true);
    ts_valid.push_back(false);

    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{Timestamp{0}, Timestamp{0}}, ts_valid);
    t.add_column("n", Column<std::int64_t>{1, 2});
    runtime::TableRegistry tables{{"t", t}};

    auto out = run("t[update { x = in_timezone(ts, \"Europe/Amsterdam\") }];", tables);
    const auto* entry = out.find_entry("x");
    REQUIRE(entry != nullptr);
    REQUIRE(entry->validity.has_value());
    CHECK((*entry->validity)[0]);
    CHECK_FALSE((*entry->validity)[1]);

    CHECK(run_error("t[update { x = in_timezone(ts, \"Mars/Olympus\") }];", tables)
              .find("unknown time zone") != std::string::npos);
    CHECK(run_error("t[update { x = in_timezone(n, \"UTC\") }];", tables)
              .find("not a Timestamp column") != std::string::npos);
}

// A day is not 86400 seconds everywhere. Bucketing zoned data on the UTC grid
// answers a different question than the one asked -- over nycflights13 it moves
// 11% of departures into a different calendar day.
TEST_CASE("E2E: resample cuts on local boundaries for a zoned index", "[e2e][timezone][resample]") {
    // Four instants either side of New York midnight on 2013-01-02 (05:00 UTC).
    runtime::Table t;
    t.add_column("ts", Column<Timestamp>{
                           Timestamp{1'357'041'600'000'000'000},  // 01-01 12:00 UTC / 07:00 local
                           Timestamp{1'357'088'400'000'000'000},  // 01-02 01:00 UTC / 01-01 20:00
                           Timestamp{1'357'106'400'000'000'000},  // 01-02 06:00 UTC / 01:00 local
                           Timestamp{1'357'128'000'000'000'000},  // 01-02 12:00 UTC / 07:00 local
                       });
    runtime::TableRegistry tables{{"t", t}};

    SECTION("an unzoned index keeps the UTC grid") {
        auto out = run(
            "let tf = as_timeframe(t, \"ts\");\ntf[resample 1d, select { n = count() }];", tables);
        // Split at 00:00 UTC: one row on the 1st, three on the 2nd.
        REQUIRE(out.rows() == 2);
        CHECK(col_i64(out, "n") == std::vector<std::int64_t>{1, 3});
    }

    SECTION("a zoned index cuts at local midnight") {
        // These instants are already correct, so the fixture only needs to say
        // which calendar to cut them on -- that is `in_timezone`, not
        // `with_timezone`, which would shift them.
        auto out =
            run("let tagged = t[update { ts = in_timezone(ts, \"America/New_York\") }];\n"
                "let tf = as_timeframe(tagged, \"ts\");\n"
                "tf[resample 1d, select { n = count() }];",
                tables);
        // Locally the four readings are 01-01 07:00, 01-01 20:00, 01-02 01:00
        // and 01-02 07:00, so local midnight splits them 2/2 where UTC split
        // them 1/3.
        REQUIRE(out.rows() == 2);
        CHECK(col_i64(out, "n") == std::vector<std::int64_t>{2, 2});
        // Every bucket boundary is a local midnight, i.e. 05:00 UTC in January.
        const auto* ts = std::get_if<Column<Timestamp>>(out.find("ts"));
        REQUIRE(ts != nullptr);
        for (std::size_t i = 0; i < ts->size(); ++i) {
            CHECK(((*ts)[i].nanos / 1'000'000'000) % 86400 == 5 * 3600);
        }
    }
}

TEST_CASE("E2E: a threaded gather returns exactly the serial rows", "[e2e][parallel][gather]") {
    // `gather_column` fills disjoint row ranges of an already-sized output, so
    // unlike a float reduction its result must be EXACTLY the serial one at any
    // thread count. A range that mislaid its offset, or an output sized without
    // being fully written (the resize_for_overwrite obligation), shows up here
    // as wrong or uninitialized values rather than as a rounding difference.
    //
    // A join and a filter are both exercised: they gather through the same
    // helper but supply very different index arrays — the join's repeats and
    // reorders rows, the filter's is strictly ascending.
    constexpr std::size_t kRows = 4000;
    std::vector<std::int64_t> keys;
    std::vector<std::int64_t> vals;
    std::vector<double> weights;
    keys.reserve(kRows);
    vals.reserve(kRows);
    weights.reserve(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        keys.push_back(static_cast<std::int64_t>(row % 97));
        vals.push_back(static_cast<std::int64_t>(row));
        weights.push_back(static_cast<double>(row) * 0.25);
    }
    runtime::Table probe;  // not `left`: that is a join keyword
    probe.add_column("k", Column<std::int64_t>{keys});
    probe.add_column("v", Column<std::int64_t>{std::move(vals)});
    probe.add_column("w", Column<double>{std::move(weights)});

    std::vector<std::int64_t> rkeys;
    std::vector<std::int64_t> tags;
    for (std::int64_t k = 0; k < 97; ++k) {
        rkeys.push_back(k);
        tags.push_back(k * 1000);
    }
    runtime::Table build;
    build.add_column("k", Column<std::int64_t>{std::move(rkeys)});
    build.add_column("tag", Column<std::int64_t>{std::move(tags)});

    runtime::TableRegistry tables;
    tables.emplace("probe", std::move(probe));
    tables.emplace("build", std::move(build));

    const std::string_view src =
        "let j = probe join build on k;\n"
        "j[filter v > 500, select { v, w, tag }, order { v }];";

    // grain/min_rows are lifted so a 4000-row table actually fans out; without
    // that the parallel runs would silently be the serial one and prove nothing.
    const auto want = run_parallel(src, tables, 0, 1);
    REQUIRE(want.rows() > 0);
    for (const std::size_t threads : {std::size_t{2}, std::size_t{3}, std::size_t{8}}) {
        const auto got = run_parallel(src, tables, 0, threads);
        REQUIRE(got.rows() == want.rows());
        CHECK(col_i64(got, "v") == col_i64(want, "v"));
        CHECK(col_i64(got, "tag") == col_i64(want, "tag"));
        CHECK(col_dbl(got, "w") == col_dbl(want, "w"));
    }
}

TEST_CASE("partitioned group discovery matches the serial groups exactly",
          "[runtime][parallel][aggregate]") {
    // High-cardinality two-integer-key grouping: the shape that used to run
    // wholly serially because the parallel-accumulate gate declines once groups
    // are numerous. Discovery now hash-partitions the rows across workers, which
    // hands out group ids from an atomic — so gid order is a race, and the
    // first-occurrence order the language promises has to be rebuilt from the
    // row each group was first seen at.
    //
    // Comparing against the serial run byte for byte is what tests that: the
    // group ORDER, the group membership, and each group's accumulated values.
    constexpr std::size_t kRows = 300'000;  // over the 2^18 row floor for the path
    std::vector<std::int64_t> parts(kRows);
    std::vector<std::int64_t> supps(kRows);
    std::vector<std::int64_t> qty(kRows);
    for (std::size_t i = 0; i < kRows; ++i) {
        // Coprime strides: every row is its own group, and the keys arrive in
        // an order unrelated to their sort order — so a merge that reordered
        // the groups, or fell back to gid order, shows up immediately.
        parts[i] = static_cast<std::int64_t>((i * 7919) % 40'009);
        supps[i] = static_cast<std::int64_t>((i * 104'729) % 13);
        qty[i] = static_cast<std::int64_t>(i % 97);
    }
    runtime::Table table;
    table.add_column("p", Column<std::int64_t>{std::move(parts)});
    table.add_column("s", Column<std::int64_t>{std::move(supps)});
    table.add_column("q", Column<std::int64_t>{std::move(qty)});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(table));

    constexpr std::string_view src = "t[select { total = sum(q), n = count() }, by { p, s }];";
    const auto serial = run_parallel(src, tables, 0, 1);
    const auto parallel = run_parallel(src, tables, 0, 4);
    // Guards the test itself: with these strides every row is a distinct group,
    // so a run that quietly grouped differently is not silently compared.
    REQUIRE(serial.rows() == kRows);
    require_tables_equal(serial, parallel);
}

TEST_CASE("owned integer counts collapse clustered runs without changing group order",
          "[runtime][parallel][aggregate]") {
    constexpr std::size_t kRows = 300'000;
    const auto check = [](std::vector<std::int64_t> keys) {
        runtime::Table table;
        table.add_column("k", Column<std::int64_t>{std::move(keys)});
        runtime::TableRegistry tables;
        tables.emplace("t", std::move(table));
        constexpr std::string_view src = "t[select { n = count() }, by { k }];";
        const auto serial = run_parallel(src, tables, 0, 1);
        const auto parallel = run_parallel(src, tables, 0, 4);
        require_tables_equal(serial, parallel);
    };

    SECTION("nondecreasing runs cross worker ranges") {
        std::vector<std::int64_t> keys(kRows);
        for (std::size_t row = 0; row < kRows; ++row) {
            keys[row] = static_cast<std::int64_t>(row / 4);
        }
        check(std::move(keys));
    }

    SECTION("clustered but unordered runs take the exact hash fallback") {
        std::vector<std::int64_t> keys(kRows);
        for (std::size_t row = 0; row < kRows; ++row) {
            const std::size_t run = row / 4;
            keys[row] = static_cast<std::int64_t>((run * 7919) % 50'003);
        }
        check(std::move(keys));
    }
}

TEST_CASE("partitioned group discovery matches the serial groups on string keys",
          "[runtime][parallel][aggregate]") {
    // The string counterpart of the case above, and the one that matters most:
    // a high-cardinality STRING key is where discovery has nothing else to hide
    // behind, and it ran wholly serially until the partitioned path learned to
    // probe with a `std::string_view` while storing an owning key per group.
    //
    // That split is exactly what this guards. Probing with a view means the
    // key handed to the hash map does not own its bytes, so a partition that
    // kept the view instead of copying would compare against freed or reused
    // storage and merge unrelated groups — which shows up here as a differing
    // group count or a differing total, not as a crash.
    constexpr std::size_t kRows = 300'000;  // over the 2^18 row floor for the path
    std::vector<std::string> keys(kRows);
    std::vector<std::int64_t> qty(kRows);
    for (std::size_t i = 0; i < kRows; ++i) {
        // Coprime stride again, and a key long enough to defeat libstdc++'s
        // 15-char SSO: a heap-allocating key is the one whose ownership bugs
        // are visible, since an inline one survives being copied wrongly.
        keys[i] = "symbol-" + std::to_string((i * 7919) % 40'009) + "-padding-to-defeat-sso";
        qty[i] = static_cast<std::int64_t>(i % 97);
    }
    runtime::Table table;
    table.add_column("k", Column<std::string>{std::move(keys)});
    table.add_column("q", Column<std::int64_t>{std::move(qty)});
    runtime::TableRegistry tables;
    tables.emplace("t", std::move(table));

    constexpr std::string_view src = "t[select { total = sum(q), n = count() }, by { k }];";
    const auto serial = run_parallel(src, tables, 0, 1);
    const auto parallel = run_parallel(src, tables, 0, 4);
    // 40'009 is prime and the stride is coprime to it, so the keys cycle through
    // every residue: exactly 40'009 groups, and each seen many times.
    REQUIRE(serial.rows() == 40'009);
    require_tables_equal(serial, parallel);
}
