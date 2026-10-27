// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "../src/runtime/interpreter_internal.hpp"

using namespace ibex;

namespace {
const std::vector<ir::AggSpec> kPaired{
    {.func = ir::AggFunc::Corr, .column = {.name = "x"}, .alias = "r", .paired = {.name = "y"}},
    {.func = ir::AggFunc::Cov, .column = {.name = "x"}, .alias = "c", .paired = {.name = "y"}}};
}  // namespace

TEST_CASE("global paired moments are stable and independent of compute budget",
          "[aggregate][corr]") {
    const auto rows = GENERATE(2U, 4095U, 4096U, 4097U, 16421U);
    const bool int_x = GENERATE(false, true);
    const bool int_y = GENERATE(false, true);
    const auto nullable = GENERATE(0, 1, 2, 3);
    CAPTURE(rows, int_x, int_y, nullable);
    Column<double> xd;
    Column<double> yd;
    Column<std::int64_t> xi;
    Column<std::int64_t> yi;
    runtime::ValidityBitmap xv;
    runtime::ValidityBitmap yv;
    std::vector<std::pair<long double, long double>> reference;
    for (std::size_t row = 0; row < rows; ++row) {
        xi.push_back(1'000'000'000 + static_cast<std::int64_t>((row * 17) % 101));
        yi.push_back(-1'000'000'000 + static_cast<std::int64_t>((row * 29) % 103));
        xd.push_back(1e12 + static_cast<double>((row * 17) % 101) * 0.125);
        yd.push_back(-1e12 + static_cast<double>((row * 29) % 103) * 0.0625);
        // Exercise a wholly null block, and distinct validity on the two sides.
        xv.push_back(!(nullable & 1) || (row >= 4096 && row % 7 != 3));
        yv.push_back(!(nullable & 2) || (row >= 4096 && row % 11 != 4));
        if (xv[row] && yv[row]) {
            reference.emplace_back(int_x ? static_cast<long double>(xi[row]) : xd[row],
                                   int_y ? static_cast<long double>(yi[row]) : yd[row]);
        }
    }
    runtime::Table t;
    if (int_x) {
        t.add_column_shared("x", std::make_shared<runtime::ColumnValue>(std::move(xi)),
                            nullable & 1 ? std::optional{xv} : std::nullopt);
    } else {
        t.add_column_shared("x", std::make_shared<runtime::ColumnValue>(std::move(xd)),
                            nullable & 1 ? std::optional{xv} : std::nullopt);
    }
    if (int_y) {
        t.add_column_shared("y", std::make_shared<runtime::ColumnValue>(std::move(yi)),
                            nullable & 2 ? std::optional{yv} : std::nullopt);
    } else {
        t.add_column_shared("y", std::make_shared<runtime::ColumnValue>(std::move(yd)),
                            nullable & 2 ? std::optional{yv} : std::nullopt);
    }
    const auto serial = runtime::aggregate_table(t, {}, kPaired);
    REQUIRE(serial.has_value());
    REQUIRE(serial->rows() == 1);
    const auto check = [&](const char* name, std::optional<double> expected) {
        const auto* entry = serial->find_entry(name);
        REQUIRE(entry != nullptr);
        REQUIRE(runtime::is_null(*entry, 0) == !expected.has_value());
        if (expected) {
            CHECK(std::get<Column<double>>(*entry->column)[0] ==
                  Catch::Approx(*expected).epsilon(1e-10).margin(1e-10));
        }
    };
    if (reference.size() < 2) {
        check("c", std::nullopt);
        check("r", std::nullopt);
    } else {
        // Independent two-pass oracle. Translate before summing so it stays
        // accurate when long double has only double precision (Apple Silicon).
        // Exercise double explicitly on every platform to keep that regression
        // visible even on hosts with extended-precision long double.
        const auto check_reference = [&]<typename Float>() {
            const auto origin_x = static_cast<Float>(reference.front().first);
            const auto origin_y = static_cast<Float>(reference.front().second);
            const auto count = static_cast<Float>(reference.size());
            Float mx = 0;
            Float my = 0;
            for (const auto& [x, y] : reference) {
                mx += static_cast<Float>(x) - origin_x;
                my += static_cast<Float>(y) - origin_y;
            }
            mx /= count;
            my /= count;
            Float xx = 0;
            Float yy = 0;
            Float xy = 0;
            for (const auto& [x, y] : reference) {
                const auto dx = (static_cast<Float>(x) - origin_x) - mx;
                const auto dy = (static_cast<Float>(y) - origin_y) - my;
                xx += dx * dx;
                yy += dy * dy;
                xy += dx * dy;
            }
            check("c", static_cast<double>(xy / (count - 1)));
            check("r", static_cast<double>(xy / std::sqrt(xx * yy)));
        };
        check_reference.template operator()<double>();
        check_reference.template operator()<long double>();
    }
    for (const auto threads : {1U, 2U, 3U, 8U}) {
        runtime::ExecutionContext exec;
        exec.parallel_threads = threads;
        exec.parallel_min_rows = 0;
        const auto result = runtime::aggregate_table(t, {}, kPaired, &exec);
        REQUIRE(result.has_value());
        for (const auto* name : {"c", "r"}) {
            const auto* a = serial->find_entry(name);
            const auto* b = result->find_entry(name);
            REQUIRE(runtime::is_null(*a, 0) == runtime::is_null(*b, 0));
            CHECK(std::bit_cast<std::uint64_t>(std::get<Column<double>>(*a->column)[0]) ==
                  std::bit_cast<std::uint64_t>(std::get<Column<double>>(*b->column)[0]));
        }
    }
}

TEST_CASE("global paired moments preserve undefined and non-finite results", "[aggregate][corr]") {
    const auto kind = GENERATE(0, 1, 2, 3, 4, 5);
    runtime::Table t;
    Column<double> x;
    Column<double> y;
    if (kind != 0) {
        x.push_back(2.0);
        y.push_back(4.0);
    }
    if (kind >= 2) {
        x.push_back(kind == 3   ? std::numeric_limits<double>::quiet_NaN()
                    : kind == 4 ? std::numeric_limits<double>::infinity()
                                : 2.0);
        y.push_back(kind == 5 ? 8.0 : 4.0);
    }
    t.add_column("x", std::move(x));
    t.add_column("y", std::move(y));
    const auto out = runtime::aggregate_table(t, {}, kPaired);
    REQUIRE(out.has_value());
    // The node-level executor supplies the null global row for an empty table.
    REQUIRE(out->rows() == (kind == 0 ? 0 : 1));
    if (kind == 0) {
        return;
    }
    CHECK(runtime::is_null(*out->find_entry("r"), 0));
    const auto* cov = out->find_entry("c");
    CHECK(runtime::is_null(*cov, 0) == (kind == 1));
    if (kind >= 2) {
        const double value = std::get<Column<double>>(*cov->column)[0];
        if (kind == 3 || kind == 4) {
            CHECK(std::isnan(value));
        } else {
            CHECK(value == 0.0);
        }
    }
}
