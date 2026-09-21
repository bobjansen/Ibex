// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
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

auto range_of(const std::vector<ir::Expr>& conjuncts) {
    return runtime::static_range_filter(conjuncts, schema());
}

}  // namespace

TEST_CASE("static range: literal bounds on an integer column become an interval",
          "[runtime][static_range]") {
    const auto range = range_of({compare(ir::CompareOp::Ge, col("i"), lit(5)),
                                 compare(ir::CompareOp::Lt, col("i"), lit(10))});
    REQUIRE(range.has_value());
    CHECK(range->first == "i");
    CHECK(range->second.min == 5);
    CHECK(range->second.max == 9);
}

TEST_CASE("static range: a literal on the left flips the comparison", "[runtime][static_range]") {
    const auto range = range_of({compare(ir::CompareOp::Lt, lit(5), col("i"))});
    REQUIRE(range.has_value());
    CHECK(range->second.min == 6);
    CHECK_FALSE(range->second.max.has_value());
}

TEST_CASE("static range: a Date column takes Date and Int literals", "[runtime][static_range]") {
    const auto with_date = range_of({compare(ir::CompareOp::Ge, col("d"), date_lit(18000)),
                                     compare(ir::CompareOp::Le, col("d"), date_lit(18010))});
    REQUIRE(with_date.has_value());
    CHECK(with_date->second.min == 18000);
    CHECK(with_date->second.max == 18010);
    // The ordinary filter compares a date with an integer day count too.
    CHECK(range_of({compare(ir::CompareOp::Gt, col("d"), lit(18500))}).has_value());
}

TEST_CASE("static range: a Date literal against a non-Date column is left to the filter",
          "[runtime][static_range][regression]") {
    // The ordinary filter rejects this with "cannot compare date and non-date".
    // Answering it from raw int64 bits would return an empty result instead of
    // the error.
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("i"), date_lit(18000)),
                          compare(ir::CompareOp::Lt, col("i"), date_lit(19000))})
                    .has_value());
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("i"), lit(5)),
                          compare(ir::CompareOp::Lt, col("i"), date_lit(19000))})
                    .has_value());
}

TEST_CASE("static range: only integer-like columns qualify", "[runtime][static_range]") {
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("x"), lit(5))}).has_value());
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("s"), lit(5))}).has_value());
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("missing"), lit(5))}).has_value());
}

TEST_CASE("static range: declines shapes the interval cannot express", "[runtime][static_range]") {
    CHECK_FALSE(range_of({compare(ir::CompareOp::Ne, col("i"), lit(5))}).has_value());
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("i"), lit(5)),
                          compare(ir::CompareOp::Gt, col("d"), lit(5))})
                    .has_value());
    CHECK_FALSE(range_of({compare(ir::CompareOp::Lt, col("i"), lit(INT64_MIN))}).has_value());
    CHECK_FALSE(range_of({compare(ir::CompareOp::Gt, col("i"), lit(INT64_MAX))}).has_value());
    CHECK_FALSE(range_of({}).has_value());
}
