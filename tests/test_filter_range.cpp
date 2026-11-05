// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Range-aware filtering: `filter_table_range` must be indistinguishable from
// gathering the range into a table of its own and filtering that.
//
// This is the correctness proof for the zero-copy morsel path (runtime
// multithreading plan, Phase 2). The rest of the suite only ever evaluates
// whole ranges, so it cannot see an offset that is applied to some column reads
// but not others — the exact defect class this file targets. The oracle is a
// deliberately naive, test-local gather: reusing the runtime's own slicing
// helper would make the comparison circular.

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "interpreter_internal.hpp"
#include "runtime_internal.hpp"

using namespace ibex;
using runtime::ColumnValue;
using runtime::RowRange;

namespace {

auto col_ref(const char* name) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = name}});
}
auto ilit(std::int64_t v) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = v}});
}
auto dlit(double v) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = v}});
}
auto slit(const char* v) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{v}}});
}
auto cmp(ir::CompareOp op, ir::ExprPtr l, ir::ExprPtr r) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{.op = op, .left = std::move(l), .right = std::move(r)}};
}
auto logical(ir::LogicalOp op, ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = op,
                                            .left = ir::make_expr_ptr(std::move(l)),
                                            .right = ir::make_expr_ptr(std::move(r))}};
}
auto arith(ir::ArithmeticOp op, ir::ExprPtr l, ir::ExprPtr r) -> ir::ExprPtr {
    return ir::make_expr_ptr(
        ir::Expr{.node = ir::BinaryExpr{.op = op, .left = std::move(l), .right = std::move(r)}});
}
auto call(const char* callee, std::vector<ir::ExprPtr> args) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{
        .node = ir::CallExpr{.callee = callee, .args = std::move(args), .named_args = {}}});
}

/// A table wide enough that every column kind the gather special-cases is
/// covered: fixed-width, bool (bit-packed), string (flat buffer), categorical
/// (dictionary codes), and a nullable column.
auto make_table(std::size_t rows) -> runtime::Table {
    runtime::Table t;
    Column<std::int64_t> id;
    Column<double> price;
    Column<bool> flag;
    Column<std::string> name;
    Column<std::int64_t> nullable;
    runtime::ValidityBitmap valid(rows, true);
    std::vector<std::string> dict{"alpha", "beta", "gamma"};
    Column<Categorical>::codes_storage codes;
    codes.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        id.push_back(static_cast<std::int64_t>(i));
        price.push_back(static_cast<double>(i % 37) * 1.5);
        flag.push_back((i % 3) == 0);
        name.push_back("row-" + std::to_string(i % 11));
        nullable.push_back(static_cast<std::int64_t>(i * 2));
        if ((i % 5) == 0) {
            valid.set(i, false);
        }
        codes.push_back(static_cast<std::int32_t>(i % 3));
    }

    t.add_column("id", ColumnValue{std::move(id)});
    t.add_column("price", ColumnValue{std::move(price)});
    t.add_column("flag", ColumnValue{std::move(flag)});
    t.add_column("name", ColumnValue{std::move(name)});
    t.add_column("nullable", ColumnValue{std::move(nullable)}, std::move(valid));
    t.add_column("kind", ColumnValue{Column<Categorical>{std::move(dict), std::move(codes)}});
    return t;
}

/// Independent oracle: copy rows [begin, begin+count) one at a time.
auto gather_rows(const runtime::Table& src, RowRange rows) -> runtime::Table {
    runtime::Table out;
    for (const auto& entry : src.columns) {
        ColumnValue col = std::visit(
            [&](const auto& c) -> ColumnValue {
                using ColT = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    Column<Categorical> dst{std::vector<std::string>(c.dictionary())};
                    for (std::size_t i = 0; i < rows.count; ++i) {
                        dst.push_code(c.code_at(rows.begin + i));
                    }
                    return ColumnValue{std::move(dst)};
                } else {
                    ColT dst;
                    for (std::size_t i = 0; i < rows.count; ++i) {
                        dst.push_back(c[rows.begin + i]);
                    }
                    return ColumnValue{std::move(dst)};
                }
            },
            *entry.column);
        if (entry.validity.has_value()) {
            runtime::ValidityBitmap bm(rows.count, true);
            for (std::size_t i = 0; i < rows.count; ++i) {
                bm.set(i, (*entry.validity)[rows.begin + i]);
            }
            out.add_column(entry.name, std::move(col), std::move(bm));
        } else {
            out.add_column(entry.name, std::move(col));
        }
    }
    return out;
}

/// The contract, asserted for one predicate over one range.
void check_range(const runtime::Table& table, const ir::Expr& predicate, RowRange rows,
                 const char* label) {
    INFO(label << " over [" << rows.begin << ", " << rows.end() << ")");

    auto ranged = runtime::filter_table_range(table, predicate, rows, nullptr);
    auto gathered = gather_rows(table, rows);
    auto expected = runtime::filter_table(gathered, predicate, nullptr);

    REQUIRE(expected.has_value());
    REQUIRE(ranged.has_value());
    auto mismatch = runtime::compare_tables(*expected, *ranged);
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}

struct NamedPredicate {
    const char* label;
    ir::Expr expr;
};

auto predicates() -> std::vector<NamedPredicate> {
    std::vector<NamedPredicate> out;
    // Each entry targets a distinct evaluator path, all of which read column
    // data at a different point and so can drop an offset independently.
    out.push_back({.label = "fused numeric cmp spec",
                   .expr = cmp(ir::CompareOp::Gt, col_ref("id"), ilit(20))});
    out.push_back({.label = "fused arith cmp spec",
                   .expr = cmp(ir::CompareOp::Lt,
                               arith(ir::ArithmeticOp::Mul, col_ref("id"), ilit(2)), ilit(90))});
    out.push_back(
        {.label = "fused numeric cmp pair",
         .expr = logical(ir::LogicalOp::And, cmp(ir::CompareOp::Ge, col_ref("id"), ilit(10)),
                         cmp(ir::CompareOp::Le, col_ref("id"), ilit(60)))});
    out.push_back({.label = "double column vs literal",
                   .expr = cmp(ir::CompareOp::Gt, col_ref("price"), dlit(20.0))});
    out.push_back({.label = "string column vs literal",
                   .expr = cmp(ir::CompareOp::Eq, col_ref("name"), slit("row-3"))});
    out.push_back({.label = "categorical vs literal",
                   .expr = cmp(ir::CompareOp::Eq, col_ref("kind"), slit("beta"))});
    out.push_back(
        {.label = "categorical IN-list",
         .expr = logical(ir::LogicalOp::Or, cmp(ir::CompareOp::Eq, col_ref("kind"), slit("beta")),
                         cmp(ir::CompareOp::Eq, col_ref("kind"), slit("gamma")))});
    out.push_back(
        {.label = "bare boolean column", .expr = ir::Expr{.node = ir::ColumnRef{.name = "flag"}}});
    out.push_back({.label = "3VL over a nullable column",
                   .expr = cmp(ir::CompareOp::Gt, col_ref("nullable"), ilit(40))});
    out.push_back({.label = "is null",
                   .expr = ir::Expr{
                       .node = ir::IsNullExpr{.operand = col_ref("nullable"), .negated = false}}});
    out.push_back({.label = "is not null",
                   .expr = ir::Expr{
                       .node = ir::IsNullExpr{.operand = col_ref("nullable"), .negated = true}}});
    out.push_back({.label = "column vs column",
                   .expr = cmp(ir::CompareOp::Lt, col_ref("id"), col_ref("nullable"))});
    out.push_back(
        {.label = "negated predicate",
         .expr = ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::Not,
                                                  .left = ir::make_expr_ptr(cmp(
                                                      ir::CompareOp::Gt, col_ref("id"), ilit(20))),
                                                  .right = nullptr}}});
    return out;
}

}  // namespace

TEST_CASE("filter_table_range matches filtering a gathered range", "[filter][range]") {
    constexpr std::size_t kRows = 200;
    const auto table = make_table(kRows);

    for (const auto& p : predicates()) {
        // Grains chosen so ranges start both on and off a 64-row word boundary
        // — the bit-packed bool gather gets its source bits from one word only
        // when the range is word-aligned.
        for (const std::size_t grain : {1U, 7U, 63U, 64U, 65U, 128U, 200U}) {
            for (std::size_t begin = 0; begin < kRows; begin += grain) {
                const std::size_t count = std::min(grain, kRows - begin);
                check_range(table, p.expr, RowRange{.begin = begin, .count = count}, p.label);
            }
        }
    }
}

TEST_CASE("filter_table_range handles degenerate ranges", "[filter][range]") {
    const auto table = make_table(64);
    const auto predicate = cmp(ir::CompareOp::Gt, col_ref("id"), ilit(10));

    SECTION("empty range at the start") {
        check_range(table, predicate, RowRange{.begin = 0, .count = 0}, "empty");
    }
    SECTION("empty range at the end") {
        check_range(table, predicate, RowRange{.begin = 64, .count = 0}, "empty");
    }
    SECTION("single row that fails the predicate") {
        check_range(table, predicate, RowRange{.begin = 0, .count = 1}, "single");
    }
    SECTION("single row that passes the predicate") {
        check_range(table, predicate, RowRange{.begin = 63, .count = 1}, "single");
    }
    SECTION("whole range takes the offset-free fast paths") {
        check_range(table, predicate, RowRange::whole(64), "whole");
    }
}

TEST_CASE("filter_table_range partitions reassemble the whole-table result", "[filter][range]") {
    constexpr std::size_t kRows = 137;
    const auto table = make_table(kRows);
    const auto predicate =
        logical(ir::LogicalOp::And, cmp(ir::CompareOp::Ge, col_ref("id"), ilit(5)),
                cmp(ir::CompareOp::Eq, col_ref("kind"), slit("alpha")));

    auto whole = runtime::filter_table(table, predicate, nullptr);
    REQUIRE(whole.has_value());

    for (const std::size_t grain : {1U, 13U, 64U, 100U}) {
        std::size_t total = 0;
        for (std::size_t begin = 0; begin < kRows; begin += grain) {
            const std::size_t count = std::min(grain, kRows - begin);
            auto part = runtime::filter_table_range(
                table, predicate, RowRange{.begin = begin, .count = count}, nullptr);
            REQUIRE(part.has_value());
            total += part->rows();
        }
        INFO("grain " << grain);
        // Every input row belongs to exactly one range, so the surviving rows
        // partition too — no row is dropped at a boundary or counted twice.
        CHECK(total == whole->rows());
    }
}

// `evaluate_field` under a partial range. Nothing in production passes it one
// yet — `is_range_native_expr` still excludes calls on cost grounds — so
// without these the range-threading in it would be untested code, exercised
// for the first time by whoever opens that gate.
namespace {

/// Wrap a computed column for comparison, blanking the payload of every null
/// cell first.
///
/// A null cell's payload is undefined in Ibex — the grouping key deliberately
/// ignores it for exactly this reason — and the two sub-paths of
/// `evaluate_field` genuinely disagree about it. The fused numeric tree
/// computes on the undefined payload and masks afterwards (`0 + 1` → 1,
/// null), while the per-row loop short-circuits on Null and pushes a default
/// (0, null). Since a partial range declines the fused tree, comparing raw
/// payloads would flag that pre-existing difference rather than anything about
/// row ranges. Validity itself is still compared exactly.
auto computed_to_table(runtime::ComputedColumn col) -> runtime::Table {
    if (col.validity.has_value()) {
        const auto& valid = *col.validity;
        std::visit(
            [&](auto& c) {
                using ColT = std::decay_t<decltype(c)>;
                for (std::size_t i = 0; i < c.size(); ++i) {
                    if (!valid[i]) {
                        if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                            // codes are already uniform for null cells
                        } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                            c.set(i, false);
                        } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                            // flat buffer: leave as produced
                        } else {
                            c[i] = typename ColT::value_type{};
                        }
                    }
                }
            },
            col.column);
    }
    // An all-true bitmap and no bitmap both mean "every row valid" — the
    // concat in MaterializeOperator relies on exactly that equivalence — but
    // the two paths pick different representations when a range happens to
    // contain no nulls. Normalize so the comparison is about validity, not
    // about how it is stored.
    if (col.validity.has_value()) {
        bool all_valid = true;
        for (std::size_t i = 0; i < col.validity->size() && all_valid; ++i) {
            all_valid = (*col.validity)[i];
        }
        if (all_valid) {
            col.validity.reset();
        }
    }
    runtime::Table t;
    if (col.validity.has_value()) {
        t.add_column("v", std::move(col.column), std::move(*col.validity));
    } else {
        t.add_column("v", std::move(col.column));
    }
    return t;
}

void check_field_range(const runtime::Table& table, const ir::Expr& expr, RowRange rows,
                       const char* label) {
    INFO(label << " over [" << rows.begin << ", " << rows.end() << ")");
    const runtime::ColumnEvalCtx ctx{
        .scalars = nullptr, .externs = nullptr, .window = std::nullopt};

    auto ranged = runtime::evaluate_field(expr, table, rows, ctx);
    auto gathered = gather_rows(table, rows);
    auto expected = runtime::evaluate_field(expr, gathered, RowRange::whole(gathered.rows()), ctx);

    REQUIRE(expected.has_value());
    REQUIRE(ranged.has_value());
    auto mismatch = runtime::compare_tables(computed_to_table(std::move(*expected)),
                                            computed_to_table(std::move(*ranged)));
    if (mismatch.has_value()) {
        FAIL(mismatch->message());
    }
}

}  // namespace

TEST_CASE("evaluate_field over a partial range matches evaluating a gathered range",
          "[filter][range]") {
    constexpr std::size_t kRows = 200;
    const auto table = make_table(kRows);

    std::vector<NamedPredicate> fields;
    // Scalar calls are range-native through the fused numeric tree.
    fields.push_back(
        {.label = "scalar call",
         .expr = ir::Expr{
             .node = ir::CallExpr{.callee = "abs", .args = {col_ref("id")}, .named_args = {}}}});
    fields.push_back(
        {.label = "nested arithmetic in a call",
         .expr = ir::Expr{
             .node = ir::CallExpr{.callee = "abs",
                                  .args = {arith(ir::ArithmeticOp::Sub, col_ref("id"), ilit(100))},
                                  .named_args = {}}}});
    // Coverage for the per-row fallback that a declined splice lands in — the
    // decline itself is not observable here (see the splice-counter test
    // below), because a whole-table `like` and the range agree on every value.
    fields.push_back(
        {.label = "spliced like leaf declines for a partial range",
         .expr =
             ir::Expr{.node = ir::BinaryExpr{
                          .op = ir::ArithmeticOp::Add,
                          .left = call("Int64", {call("like", {col_ref("name"), slit("row-1")})}),
                          .right = col_ref("id")}}});
    // Vectorized path: a boolean node in value position.
    fields.push_back(
        {.label = "boolean node", .expr = cmp(ir::CompareOp::Gt, col_ref("id"), ilit(50))});
    // Nulls must be carried at the right offset, not merely the right width.
    fields.push_back({.label = "nullable column arithmetic",
                      .expr = ir::Expr{.node = ir::BinaryExpr{.op = ir::ArithmeticOp::Add,
                                                              .left = col_ref("nullable"),
                                                              .right = ilit(1)}}});
    fields.push_back({.label = "boolean over a nullable column",
                      .expr = cmp(ir::CompareOp::Gt, col_ref("nullable"), ilit(40))});
    // A case evaluates each arm over the rows that take it; under a range those
    // are the range's rows, at the range's offset.
    fields.push_back(
        {.label = "case over a nullable column",
         .expr = std::move(
             *call("__case", {ir::make_expr_ptr(cmp(ir::CompareOp::Gt, col_ref("id"), ilit(50))),
                              col_ref("nullable"), col_ref("flag"),
                              arith(ir::ArithmeticOp::Mul, col_ref("id"), ilit(2)), ilit(-1)}))});
    // A case nested in another call: classified row-local, it is evaluated
    // over the range rather than whole, the way the Polars adapter's rounded
    // aggregates (`Float64(round(case { ... }))`) now are.
    fields.push_back(
        {.label = "case inside a call",
         .expr = std::move(*call(
             "abs",
             {call("__case", {ir::make_expr_ptr(cmp(ir::CompareOp::Gt, col_ref("id"), ilit(50))),
                              arith(ir::ArithmeticOp::Sub, col_ref("nullable"), ilit(300)),
                              col_ref("id")})}))});
    fields.push_back({.label = "row-local kernel builtin over a range",
                      .expr = std::move(*call("length", {col_ref("name")}))});
    fields.push_back({.label = "unary math over a computed nullable argument",
                      .expr = std::move(*call(
                          "sqrt", {arith(ir::ArithmeticOp::Add, col_ref("nullable"), ilit(1))}))});
    fields.push_back(
        {.label = "abs of an Int column", .expr = std::move(*call("abs", {col_ref("id")}))});

    for (const auto& f : fields) {
        for (const std::size_t grain : {1U, 7U, 63U, 64U, 65U, 200U}) {
            for (std::size_t begin = 0; begin < kRows; begin += grain) {
                const std::size_t count = std::min(grain, kRows - begin);
                check_field_range(table, f.expr, RowRange{.begin = begin, .count = count}, f.label);
            }
        }
    }
}

TEST_CASE("a ranged string kernel reads a borrowed view of an offset source", "[filter][range]") {
    // A partial range borrows String columns instead of copying them. When the
    // source is itself a view starting mid-buffer, the borrowed window must
    // start at the source's offset plus the range's, not at the range alone.
    constexpr std::size_t kRows = 200;
    constexpr std::size_t kSkip = 37;
    auto owned = std::make_shared<Column<std::string>>();
    for (std::size_t i = 0; i < kRows + kSkip; ++i) {
        owned->push_back(i % 3 == 0 ? "13-" + std::to_string(i) : "31-" + std::to_string(i));
    }
    const auto& base = *owned;
    runtime::Table table;
    table.add_column("phone",
                     Column<std::string>::from_external(owned, base.offsets_buffer_data(),
                                                        base.chars_buffer_data(), kSkip, kRows));
    const auto expr = call("like", {col_ref("phone"), slit("13%")});
    for (const RowRange rows : {RowRange{.begin = 0, .count = 64}, RowRange{.begin = 5, .count = 7},
                                RowRange{.begin = 130, .count = 70}}) {
        check_field_range(table, *expr, rows, "like over an offset view");
    }
}

TEST_CASE("a column-kernel leaf is never spliced under a partial range", "[filter][range]") {
    // `try_splice_column_leaf` evaluates its kernel over the WHOLE table, so it
    // must decline for every partial range — including one that begins at zero,
    // which is morsel 0 of every morsel pipeline. That decline cannot be tested through
    // output: with begin == 0 the spliced whole-table column and the range
    // agree on every value, so a value oracle passes either way (this was
    // confirmed by mutation — restoring the `begin != 0` guard left the
    // equivalence tests entirely green). The counter is the only witness.
    constexpr std::size_t kRows = 200;
    const auto table = make_table(kRows);
    // `Int64(like(...)) + id` is the shape the splice exists for: a column
    // kernel wrapped in a numeric cast, as a leaf of a numeric tree.
    const ir::Expr expr{.node = ir::BinaryExpr{
                            .op = ir::ArithmeticOp::Add,
                            .left = call("Int64", {call("like", {col_ref("name"), slit("row-1")})}),
                            .right = col_ref("id")}};
    const runtime::ColumnEvalCtx ctx{
        .scalars = nullptr, .externs = nullptr, .window = std::nullopt};

    SECTION("a whole range does splice") {
        // Without this the zero below would be satisfied just as well by an
        // expression that never reaches the splice at all.
        runtime::reset_column_kernel_splice_count();
        auto whole = runtime::evaluate_field(expr, table, RowRange::whole(kRows), ctx);
        REQUIRE(whole.has_value());
        CHECK(runtime::column_kernel_splice_count() > 0);
    }

    SECTION("every partial range declines, including one beginning at zero") {
        for (const std::size_t grain : {1U, 7U, 64U, 199U}) {
            for (std::size_t begin = 0; begin < kRows; begin += grain) {
                const std::size_t count = std::min(grain, kRows - begin);
                if (count == kRows) {
                    continue;  // that is the whole range, covered above
                }
                INFO("range [" << begin << ", " << (begin + count) << ")");
                runtime::reset_column_kernel_splice_count();
                auto ranged = runtime::evaluate_field(
                    expr, table, RowRange{.begin = begin, .count = count}, ctx);
                REQUIRE(ranged.has_value());
                CHECK(runtime::column_kernel_splice_count() == 0);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Morsel grain derivation.
//
// A 96-config sweep found no grain in a 4k..4M band that loses to serial, so
// this is deliberately a formula rather than a knob. What the formula must
// guarantee is only this: enough morsels per worker that one slow morsel cannot
// strand the rest, without ever exceeding the measured plateau.
// ---------------------------------------------------------------------------
TEST_CASE("morsel_grain derives a grain instead of asking for one", "[runtime][parallel]") {
    ibex::runtime::ExecutionContext exec;
    exec.parallel_threads = 8;

    SECTION("an explicit grain is honoured exactly") {
        exec.parallel_grain = 12345;
        CHECK(ibex::runtime::morsel_grain(exec, 20'000'000) == 12345);
        // Including one that the derivation would never choose itself.
        exec.parallel_grain = 8'000'000;
        CHECK(ibex::runtime::morsel_grain(exec, 20'000'000) == 8'000'000);
    }

    SECTION("a large input clamps to the plateau, not to rows/threads") {
        // The upper clamp is the load-bearing half of the formula: an uncapped
        // rows/(threads*4) would give 625k here, which the sweep measured as
        // clearly worse than 64k. If this ever returns something larger, the
        // clamp has been dropped and large inputs have silently regressed.
        CHECK(ibex::runtime::morsel_grain(exec, 20'000'000) == 65536);
    }

    SECTION("a small input shrinks the grain to keep every worker fed") {
        // 200k rows at the old fixed 64k grain is 4 morsels for 8 threads --
        // half the pool idle. The derived grain gives each worker several.
        //
        // The budget is clamped to the real pool, so the expectation has to be
        // computed from the pool too: hard-coding 8 here would fail on any
        // machine with fewer cores, which is exactly where CI runs.
        const std::size_t threads =
            std::min<std::size_t>(8, ibex::runtime::process_worker_pool().size());
        const std::size_t grain = ibex::runtime::morsel_grain(exec, 200'000);
        CHECK(grain < 65536);
        const std::size_t morsels = (200'000 + grain - 1) / grain;
        CHECK(morsels >= threads * 2);
    }

    SECTION("a tiny input still floors at a usable grain") {
        // Never one row per morsel: that would be more tasks than work.
        CHECK(ibex::runtime::morsel_grain(exec, 10) == 4096);
        CHECK(ibex::runtime::morsel_grain(exec, 0) == 4096);
    }

    SECTION("more threads means smaller morsels at the same input size") {
        // Budgets of 1 and 2 rather than 2 and 8, so the comparison survives a
        // two-core machine; a single-core pool clamps both and has nothing to
        // compare.
        if (ibex::runtime::process_worker_pool().size() >= 2) {
            exec.parallel_threads = 1;
            const std::size_t few = ibex::runtime::morsel_grain(exec, 200'000);
            exec.parallel_threads = 2;
            const std::size_t many = ibex::runtime::morsel_grain(exec, 200'000);
            CHECK(many < few);
        }
    }
}

TEST_CASE("NaN-high comparisons preserve nulls and sliced operands",
          "[runtime][filter][nan-high]") {
    const double inf = std::numeric_limits<double>::infinity();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const std::vector<double> values{-inf, -1.0, -0.0, 0.0, 1.0, inf, nan, 0.0};
    const std::vector<int> rank{0, 1, 2, 2, 3, 4, 5, 0};
    runtime::Table table;
    Column<std::int64_t> ids;
    Column<double> left, right;
    runtime::ValidityBitmap lv(64, true), rv(64, true);
    for (std::size_t a = 0; a < 8; ++a) {
        for (std::size_t b = 0; b < 8; ++b) {
            ids.push_back(static_cast<std::int64_t>(a * 8 + b));
            left.push_back(values[a]);
            right.push_back(values[b]);
            lv.set(a * 8 + b, a != 7);
            rv.set(a * 8 + b, b != 7);
        }
    }
    table.add_column("id", ColumnValue{std::move(ids)});
    table.add_column("a", ColumnValue{std::move(left)}, std::move(lv));
    table.add_column("b", ColumnValue{std::move(right)}, std::move(rv));
    for (auto op : {ir::CompareOp::Eq, ir::CompareOp::Ne, ir::CompareOp::Lt, ir::CompareOp::Le,
                    ir::CompareOp::Gt, ir::CompareOp::Ge}) {
        for (bool scalar : {false, true}) {
            auto expression = cmp(op, col_ref("a"), scalar ? dlit(nan) : col_ref("b"));
            std::get<ir::CompareExpr>(expression.node).float_comparison =
                ir::FloatComparison::NaNsEqualAndGreatest;
            auto result = runtime::filter_table_range(table, expression, RowRange{3, 58}, nullptr);
            REQUIRE(result.has_value());
            std::vector<std::int64_t> expected;
            for (std::size_t i = 3; i < 61; ++i) {
                const auto a = i / 8, b = scalar ? std::size_t{6} : i % 8;
                if (a == 7 || b == 7)
                    continue;
                const int x = rank[a], y = rank[b];
                bool keep = false;
                switch (op) {
                    case ir::CompareOp::Eq:
                        keep = x == y;
                        break;
                    case ir::CompareOp::Ne:
                        keep = x != y;
                        break;
                    case ir::CompareOp::Lt:
                        keep = x < y;
                        break;
                    case ir::CompareOp::Le:
                        keep = x <= y;
                        break;
                    case ir::CompareOp::Gt:
                        keep = x > y;
                        break;
                    case ir::CompareOp::Ge:
                        keep = x >= y;
                        break;
                }
                if (keep)
                    expected.push_back(static_cast<std::int64_t>(i));
            }
            const auto& actual = std::get<Column<std::int64_t>>(*result->columns[0].column);
            REQUIRE(actual.size() == expected.size());
            for (std::size_t i = 0; i < expected.size(); ++i)
                CHECK(actual[i] == expected[i]);
        }
    }
    auto ordinary =
        runtime::filter_table(table, cmp(ir::CompareOp::Eq, col_ref("a"), dlit(nan)), nullptr);
    REQUIRE(ordinary.has_value());
    CHECK(ordinary->rows() == 0);
}

namespace {

/// A bitmap of `bits` rows with a pattern no word-wise shortcut reproduces by
/// accident: runs, isolated bits and word-straddling stretches.
auto patterned_bits(std::size_t bits, std::uint64_t seed) -> std::vector<bool> {
    std::vector<bool> out(bits);
    std::uint64_t x = seed;
    for (std::size_t i = 0; i < bits; ++i) {
        x = (x * 6364136223846793005ULL) + 1442695040888963407ULL;
        out[i] = ((x >> 33U) % 5U) != 0U;
    }
    return out;
}

/// The same rows as an owned bitmap and as an external (Arrow) slice whose
/// buffer is exactly as long as its bits need, starting `ext_off` bits in.
struct BitmapPair {
    runtime::ValidityBitmap owned;
    runtime::ValidityBitmap external;
};

auto make_bitmap_pair(const std::vector<bool>& rows, std::size_t ext_off) -> BitmapPair {
    BitmapPair pair{.owned = runtime::ValidityBitmap(rows), .external = {}};
    auto buffer = std::make_shared<std::vector<std::uint8_t>>((ext_off + rows.size() + 7) / 8,
                                                              std::uint8_t{0xA5});
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const std::size_t bit = ext_off + i;
        auto& byte = (*buffer)[bit / 8];
        const auto mask = static_cast<std::uint8_t>(1U << (bit % 8));
        byte = rows[i] ? static_cast<std::uint8_t>(byte | mask)
                       : static_cast<std::uint8_t>(byte & ~mask);
    }
    pair.external =
        runtime::ValidityBitmap::from_external(buffer, buffer->data(), ext_off, rows.size());
    return pair;
}

}  // namespace

TEST_CASE("word-wise validity reads match per-bit reads at any offset",
          "[runtime][filter][validity]") {
    constexpr std::size_t kRows = 300;
    const auto rows = patterned_bits(kRows, 7);
    for (const std::size_t ext_off : {std::size_t{0}, std::size_t{3}, std::size_t{13}}) {
        const auto pair = make_bitmap_pair(rows, ext_off);
        for (const auto* source : {&pair.owned, &pair.external}) {
            for (std::size_t off = 0; off < 140; off += (off < 70 ? 1 : 7)) {
                for (std::size_t n = 0; off + n <= kRows; n += (n < 70 ? 1 : 13)) {
                    INFO("ext_off=" << ext_off << " external=" << (source == &pair.external)
                                    << " off=" << off << " n=" << n);
                    std::vector<std::uint8_t> bytes(n + 1, std::uint8_t{0xEE});
                    runtime::unpack_validity_bytes(*source, off, n, bytes.data());
                    const auto sliced = runtime::slice_validity_bits(*source, off, n);
                    REQUIRE(sliced.size() == n);
                    REQUIRE_FALSE(sliced.is_external());
                    auto anded = runtime::ValidityBitmap(n, true);
                    runtime::and_validity_bits(anded, *source, off, n);
                    for (std::size_t i = 0; i < n; ++i) {
                        REQUIRE(bytes[i] == static_cast<std::uint8_t>(rows[off + i]));
                        REQUIRE(sliced[i] == rows[off + i]);
                        REQUIRE(anded[i] == rows[off + i]);
                    }
                    REQUIRE(bytes[n] == 0xEE);  // nothing written past n
                    // ANDing into a longer bitmap leaves its rows past n alone.
                    auto wider = runtime::ValidityBitmap(n + 70, true);
                    runtime::and_validity_bits(wider, *source, off, n);
                    for (std::size_t i = n; i < n + 70; ++i) {
                        REQUIRE(wider[i]);
                    }
                    // Tail bits past n stay clear, so word-wise consumers of
                    // the result (merge, popcount) never see stale rows.
                    if (n % 64 != 0) {
                        const auto last = sliced.words_data()[n / 64];
                        REQUIRE((last >> (n % 64)) == 0U);
                    }
                }
            }
        }
    }
}

TEST_CASE("merge_validity and Mask::apply_validity read sliced operands row by row",
          "[runtime][filter][validity]") {
    constexpr std::size_t kRows = 260;
    const auto a_rows = patterned_bits(kRows, 11);
    const auto b_rows = patterned_bits(kRows, 29);
    const auto a = make_bitmap_pair(a_rows, 5);
    const auto b = make_bitmap_pair(b_rows, 0);
    for (const auto* av : {&a.owned, &a.external}) {
        for (const auto* bv : {&b.owned, &b.external}) {
            for (const auto [a_off, b_off, n] :
                 {std::array<std::size_t, 3>{0, 0, kRows}, {0, 0, 200}, {1, 0, 190},
                  {0, 63, 197}, {64, 64, 130}, {77, 3, 129}, {5, 5, 255}}) {
                INFO("a_off=" << a_off << " b_off=" << b_off << " n=" << n);
                const auto merged_opt = runtime::merge_validity(av, a_off, bv, b_off, n);
                REQUIRE(merged_opt.has_value());
                const auto merged = merged_opt.value_or(runtime::ValidityBitmap{});
                REQUIRE(merged.size() == n);
                const auto a_only_opt = runtime::merge_validity(av, a_off, nullptr, 0, n);
                REQUIRE(a_only_opt.has_value());
                const auto a_only = a_only_opt.value_or(runtime::ValidityBitmap{});
                REQUIRE(a_only.size() == n);
                runtime::Mask mask;
                mask.apply_validity(av, a_off, n);
                REQUIRE(mask.valid.has_value());
                const auto valid = mask.valid.value_or(std::vector<std::uint8_t>{});
                REQUIRE(valid.size() == n);
                for (std::size_t i = 0; i < n; ++i) {
                    REQUIRE(merged[i] == (a_rows[a_off + i] && b_rows[b_off + i]));
                    REQUIRE(a_only[i] == a_rows[a_off + i]);
                    REQUIRE(valid[i] == static_cast<std::uint8_t>(a_rows[a_off + i]));
                }
            }
        }
    }
}

TEST_CASE("categorical columns over one dictionary compare by code under a partial range",
          "[runtime][filter][categorical]") {
    using Cat = Column<Categorical>;
    // Sorted, so code order is string order and the expected rows can be
    // derived from the strings themselves.
    const std::vector<std::string> words{"a", "b", "c", "d", "e"};
    const auto dict = std::make_shared<std::vector<std::string>>(words);
    const auto index = std::make_shared<Cat::index_map>();
    for (std::size_t i = 0; i < words.size(); ++i) {
        index->emplace(words[i], static_cast<Cat::code_type>(i));
    }
    constexpr std::size_t kRows = 157;
    Column<std::int64_t> ids;
    Cat left{dict, index, Cat::codes_storage{}};
    Cat right{dict, index, Cat::codes_storage{}};
    std::vector<std::size_t> lw(kRows);
    std::vector<std::size_t> rw(kRows);
    const auto right_valid = patterned_bits(kRows, 3);
    for (std::size_t i = 0; i < kRows; ++i) {
        lw[i] = (i * 7U) % words.size();
        rw[i] = ((i * 3U) + 1U) % words.size();
        ids.push_back(static_cast<std::int64_t>(i));
        left.push_back(words[lw[i]]);
        right.push_back(words[rw[i]]);
    }
    REQUIRE(left.dictionary_identity() == right.dictionary_identity());
    runtime::Table table;
    table.add_column("id", ColumnValue{std::move(ids)});
    table.add_column("l", ColumnValue{std::move(left)});
    table.add_column("r", ColumnValue{std::move(right)}, runtime::ValidityBitmap(right_valid));
    const RowRange range{.begin = 13, .count = 130};
    for (auto op : {ir::CompareOp::Eq, ir::CompareOp::Ne, ir::CompareOp::Lt, ir::CompareOp::Le,
                    ir::CompareOp::Gt, ir::CompareOp::Ge}) {
        for (const bool literal : {false, true}) {
            INFO("op=" << static_cast<int>(op) << " literal=" << literal);
            auto result = runtime::filter_table_range(
                table, cmp(op, col_ref("l"), literal ? slit("c") : col_ref("r")), range, nullptr);
            REQUIRE(result.has_value());
            std::vector<std::int64_t> expected;
            for (std::size_t i = range.begin; i < range.begin + range.count; ++i) {
                if (!literal && !right_valid[i]) {
                    continue;
                }
                const auto x = lw[i];
                const auto y = literal ? std::size_t{2} : rw[i];
                bool keep = false;
                switch (op) {
                    case ir::CompareOp::Eq:
                        keep = x == y;
                        break;
                    case ir::CompareOp::Ne:
                        keep = x != y;
                        break;
                    case ir::CompareOp::Lt:
                        keep = x < y;
                        break;
                    case ir::CompareOp::Le:
                        keep = x <= y;
                        break;
                    case ir::CompareOp::Gt:
                        keep = x > y;
                        break;
                    case ir::CompareOp::Ge:
                        keep = x >= y;
                        break;
                }
                if (keep) {
                    expected.push_back(static_cast<std::int64_t>(i));
                }
            }
            const auto& actual = std::get<Column<std::int64_t>>(*result->columns[0].column);
            REQUIRE(actual.size() == expected.size());
            for (std::size_t i = 0; i < expected.size(); ++i) {
                CHECK(actual[i] == expected[i]);
            }
        }
    }
}
