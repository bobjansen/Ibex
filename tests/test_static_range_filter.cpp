// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "static_range_filter.hpp"

using namespace ibex;

namespace {

auto col(const std::string& name) -> ir::Expr {
    return ir::Expr{ir::ColumnRef{.name = name}};
}

auto lit(std::int64_t value) -> ir::Expr {
    return ir::Expr{ir::Literal{.value = value}};
}

auto date_lit(std::int32_t days) -> ir::Expr {
    return ir::Expr{ir::Literal{.value = Date{.days = days}}};
}

auto compare(ir::CompareOp op, ir::Expr left, ir::Expr right) -> ir::Expr {
    return ir::Expr{ir::CompareExpr{.op = op,
                                    .left = ir::make_expr_ptr(std::move(left)),
                                    .right = ir::make_expr_ptr(std::move(right))}};
}

auto schema() -> runtime::Table {
    runtime::Table t;
    t.add_column("i", Column<std::int64_t>{});
    t.add_column("d", Column<Date>{});
    t.add_column("x", Column<double>{});
    t.add_column("s", Column<std::string>{});
    return t;
}

auto split(const std::vector<ir::Expr>& conjuncts) {
    return runtime::split_static_range(conjuncts, schema());
}

}  // namespace

TEST_CASE("static range: literal bounds on an integer column become an interval",
          "[runtime][static_range]") {
    const auto range = split({compare(ir::CompareOp::Ge, col("i"), lit(5)),
                              compare(ir::CompareOp::Lt, col("i"), lit(10))});
    REQUIRE(range.has_value());
    CHECK(range->column == "i");
    CHECK(range->filter.min == 5);
    CHECK(range->filter.max == 9);
    CHECK(range->rest.empty());
}

TEST_CASE("static range: a literal on the left flips the comparison", "[runtime][static_range]") {
    const auto range = split({compare(ir::CompareOp::Lt, lit(5), col("i"))});
    REQUIRE(range.has_value());
    CHECK(range->filter.min == 6);
    CHECK_FALSE(range->filter.max.has_value());
}

TEST_CASE("static range: a Date column takes Date and Int literals", "[runtime][static_range]") {
    const auto with_date = split({compare(ir::CompareOp::Ge, col("d"), date_lit(18000)),
                                  compare(ir::CompareOp::Le, col("d"), date_lit(18010))});
    REQUIRE(with_date.has_value());
    CHECK(with_date->filter.min == 18000);
    CHECK(with_date->filter.max == 18010);
    // The ordinary filter compares a date with an integer day count too.
    CHECK(split({compare(ir::CompareOp::Gt, col("d"), lit(18500))}).has_value());
}

TEST_CASE("static range: a Date literal against a non-Date column is left to the filter",
          "[runtime][static_range][regression]") {
    // The ordinary filter rejects this with "cannot compare date and non-date".
    // Answering it from raw int64 bits would return an empty result instead of
    // the error.
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("i"), date_lit(18000)),
                       compare(ir::CompareOp::Lt, col("i"), date_lit(19000))})
                    .has_value());
    // Even when another comparison on the same column would be fine by itself,
    // the split declines whole so the error is not hidden by the rows it removed.
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("i"), lit(5)),
                       compare(ir::CompareOp::Lt, col("i"), date_lit(19000))})
                    .has_value());
}

TEST_CASE("static range: only integer-like columns qualify", "[runtime][static_range]") {
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("x"), lit(5))}).has_value());
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("s"), lit(5))}).has_value());
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("missing"), lit(5))}).has_value());
    CHECK_FALSE(split({}).has_value());
}

TEST_CASE("static range: what the interval cannot express is handed back in order",
          "[runtime][static_range]") {
    const auto other = compare(ir::CompareOp::Gt, col("x"), ir::Expr{ir::Literal{.value = 0.5}});
    const auto range = split({compare(ir::CompareOp::Ge, col("i"), lit(5)), other,
                              compare(ir::CompareOp::Ne, col("i"), lit(7)),
                              compare(ir::CompareOp::Lt, col("i"), lit(10))});
    REQUIRE(range.has_value());
    CHECK(range->column == "i");
    CHECK(range->filter.min == 5);
    CHECK(range->filter.max == 9);
    CHECK(range->rest.size() == 2);  // the double comparison, then `i != 7`
}

TEST_CASE("static range: the first answerable column wins and the others stay conjuncts",
          "[runtime][static_range]") {
    const auto range = split({compare(ir::CompareOp::Gt, col("x"), lit(1)),
                              compare(ir::CompareOp::Ge, col("d"), lit(100)),
                              compare(ir::CompareOp::Gt, col("i"), lit(5))});
    REQUIRE(range.has_value());
    CHECK(range->column == "d");
    CHECK(range->rest.size() == 2);
}

TEST_CASE("static range: nothing absorbed means no split", "[runtime][static_range]") {
    CHECK_FALSE(split({compare(ir::CompareOp::Ne, col("i"), lit(5))}).has_value());
    CHECK_FALSE(split({compare(ir::CompareOp::Lt, col("i"), lit(INT64_MIN))}).has_value());
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("i"), lit(INT64_MAX))}).has_value());
}

TEST_CASE("static range: a zero-row schema table still reports a type error in a conjunct",
          "[runtime][static_range][regression]") {
    // `static_range_selection` evaluates the conjuncts the interval leaves over
    // this typed, empty table when the interval selected no rows at all, so a
    // type error a range removed every row for is still reported.
    const auto empty = schema();
    const runtime::ExecutionContext exec;
    const auto bad = runtime::filter_selection(
        empty, {compare(ir::CompareOp::Gt, col("i"), date_lit(18000))}, exec, nullptr);
    CHECK_FALSE(bad.has_value());
    const auto fine = runtime::filter_selection(
        empty, {compare(ir::CompareOp::Gt, col("i"), lit(5))}, exec, nullptr);
    REQUIRE(fine.has_value());
    CHECK(fine->empty());
}

namespace {

auto real(double x) -> ir::Expr {
    return ir::Expr{ir::Literal{.value = x}};
}

auto bounds_of(ir::CompareOp op, double x) {
    return split({compare(op, col("i"), real(x))});
}

}  // namespace

TEST_CASE("static range: a double literal against an int column becomes integer bounds",
          "[runtime][static_range]") {
    const auto gt = bounds_of(ir::CompareOp::Gt, 5.5);
    REQUIRE(gt.has_value());
    CHECK(gt->filter.min == 6);
    CHECK(bounds_of(ir::CompareOp::Ge, 5.5)->filter.min == 6);
    CHECK(bounds_of(ir::CompareOp::Lt, 5.5)->filter.max == 5);
    CHECK(bounds_of(ir::CompareOp::Le, 5.5)->filter.max == 5);
    // An integral double is the integer itself.
    CHECK(bounds_of(ir::CompareOp::Gt, 5.0)->filter.min == 6);
    CHECK(bounds_of(ir::CompareOp::Ge, 5.0)->filter.min == 5);
    CHECK(bounds_of(ir::CompareOp::Lt, 5.0)->filter.max == 4);
    CHECK(bounds_of(ir::CompareOp::Le, 5.0)->filter.max == 5);
    const auto eq = bounds_of(ir::CompareOp::Eq, 5.0);
    CHECK(eq->filter.min == 5);
    CHECK(eq->filter.max == 5);
    // Negative literals round toward the correct side.
    CHECK(bounds_of(ir::CompareOp::Gt, -5.5)->filter.min == -5);
    CHECK(bounds_of(ir::CompareOp::Ge, -5.5)->filter.min == -5);
    CHECK(bounds_of(ir::CompareOp::Lt, -5.5)->filter.max == -6);
    CHECK(bounds_of(ir::CompareOp::Le, -5.5)->filter.max == -6);
}

TEST_CASE("static range: a fractional equality is an empty interval", "[runtime][static_range]") {
    const auto eq = bounds_of(ir::CompareOp::Eq, 5.5);
    REQUIRE(eq.has_value());
    CHECK(*eq->filter.min > *eq->filter.max);
    // Later bounds cannot reopen it.
    const auto later = split({compare(ir::CompareOp::Eq, col("i"), real(5.5)),
                              compare(ir::CompareOp::Ge, col("i"), lit(0)),
                              compare(ir::CompareOp::Le, col("i"), lit(10))});
    REQUIRE(later.has_value());
    CHECK(*later->filter.min > *later->filter.max);
}

TEST_CASE("static range: a double literal combines with integer bounds and flips on the left",
          "[runtime][static_range]") {
    const auto range = split({compare(ir::CompareOp::Lt, real(4.5), col("i")),
                              compare(ir::CompareOp::Lt, col("i"), lit(10))});
    REQUIRE(range.has_value());
    CHECK(range->filter.min == 5);
    CHECK(range->filter.max == 9);
    CHECK(range->rest.empty());
}

TEST_CASE("static range: double literals that cannot stay exact are conjuncts",
          "[runtime][static_range]") {
    // 2^53 - 1 is the last magnitude below the exactness limit.
    CHECK(bounds_of(ir::CompareOp::Gt, 9007199254740991.0).has_value());
    CHECK_FALSE(bounds_of(ir::CompareOp::Gt, 9007199254740992.0).has_value());
    CHECK_FALSE(bounds_of(ir::CompareOp::Gt, 1e30).has_value());
    CHECK_FALSE(bounds_of(ir::CompareOp::Lt, -1e30).has_value());
    CHECK_FALSE(bounds_of(ir::CompareOp::Gt, std::numeric_limits<double>::infinity()).has_value());
    CHECK_FALSE(bounds_of(ir::CompareOp::Gt, std::numeric_limits<double>::quiet_NaN()).has_value());
    // A double literal is never compared with a Date column by the interval.
    CHECK_FALSE(split({compare(ir::CompareOp::Gt, col("d"), real(5.5))}).has_value());
}
