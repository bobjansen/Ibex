// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <variant>
#include <vector>

#include "interpreter_internal.hpp"

// A conjunction of numeric column-vs-literal compares is evaluated block by
// block in filter.cpp (`try_fused_conjunction`), compact Int32 and NaN-ordered
// Double leaves included. These check it row for row against a direct
// evaluation of the same compares, across blocks, ranges and row limits.

namespace {
using namespace ibex;
using namespace ibex::runtime;

// More than one fused block (2048 rows), and not a multiple of 64.
constexpr std::size_t kRows = 5003;
constexpr std::array kOps = {ir::CompareOp::Eq, ir::CompareOp::Ne, ir::CompareOp::Lt,
                             ir::CompareOp::Le, ir::CompareOp::Gt, ir::CompareOp::Ge};

auto source() -> Table {
    ibex::detail::NoInitVector<std::int32_t> days(kRows);
    std::vector<double> prices(kRows);
    std::vector<std::int64_t> qtys(kRows);
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (std::size_t i = 0; i < kRows; ++i) {
        days[i] = (i % 97 == 0)   ? std::numeric_limits<std::int32_t>::max()
                  : (i % 89 == 0) ? std::numeric_limits<std::int32_t>::min()
                                  : static_cast<std::int32_t>(i % 7) - 3;
        prices[i] = (i % 11 == 0) ? nan : (static_cast<double>(i % 5) * 0.5) - 0.5;
        qtys[i] = static_cast<std::int64_t>(i % 6);
    }
    Table table;
    table.add_column("d", Column<std::int64_t>::from_compact_int32(std::move(days)));
    table.add_column("p", Column<double>{prices});
    table.add_column("q", Column<std::int64_t>{qtys});
    return table;
}

using Literal = std::variant<std::int64_t, double>;

auto leaf(const char* column, ir::CompareOp op, Literal literal,
          ir::FloatComparison policy = ir::FloatComparison::IEEE) -> ir::Expr {
    ir::Expr value;
    std::visit([&](auto v) { value.node = ir::Literal{.value = v}; }, literal);
    return ir::Expr{.node = ir::CompareExpr{
                        .op = op,
                        .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = column}}),
                        .right = ir::make_expr_ptr(std::move(value)),
                        .float_comparison = policy}};
}

auto both(ir::Expr left, ir::Expr right) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::And,
                                            .left = ir::make_expr_ptr(std::move(left)),
                                            .right = ir::make_expr_ptr(std::move(right))}};
}

template <typename A, typename B>
auto holds(ir::CompareOp op, A a, B b) -> bool {
    switch (op) {
        case ir::CompareOp::Eq:
            return a == b;
        case ir::CompareOp::Ne:
            return a != b;
        case ir::CompareOp::Lt:
            return a < b;
        case ir::CompareOp::Le:
            return a <= b;
        case ir::CompareOp::Gt:
            return a > b;
        case ir::CompareOp::Ge:
            return a >= b;
    }
    return false;
}

auto holds_literal(ir::CompareOp op, std::int64_t a, Literal b) -> bool {
    if (const auto* i = std::get_if<std::int64_t>(&b)) {
        return holds(op, a, *i);
    }
    return holds(op, static_cast<double>(a), std::get<double>(b));
}

/// NaN ranks above every number and equals itself.
auto holds_nan_high(ir::CompareOp op, double a, double b) -> bool {
    if (std::isnan(a) || std::isnan(b)) {
        const int ra = std::isnan(a) ? 1 : 0;
        const int rb = std::isnan(b) ? 1 : 0;
        return holds(op, ra, rb);
    }
    return holds(op, a, b);
}

}  // namespace

TEST_CASE("a fused conjunction selects exactly the rows its compares keep", "[filter][fused]") {
    const auto table = source();
    const auto& days = std::get<Column<std::int64_t>>(*table.find_entry("d")->column);
    REQUIRE(days.is_compact_int32());
    const auto& prices = std::get<Column<double>>(*table.find_entry("p")->column);
    const auto& qtys = std::get<Column<std::int64_t>>(*table.find_entry("q")->column);

    // In the Int32 range, past it, and a Double against compact storage.
    const std::array<Literal, 3> day_literals = {std::int64_t{0}, std::int64_t{1} << 40, 0.5};
    const std::array<Literal, 2> qty_literals = {std::int64_t{3}, 2.5};
    const std::array policies = {ir::FloatComparison::IEEE,
                                 ir::FloatComparison::NaNsEqualAndGreatest};
    const std::array ranges = {RowRange::whole(kRows), RowRange{.begin = 7, .count = 4500},
                               RowRange{.begin = 2049, .count = 63}};

    for (std::size_t i = 0; i < kOps.size(); ++i) {
        for (std::size_t j = 0; j < kOps.size(); ++j) {
            const auto day_op = kOps[i];
            const auto price_op = kOps[j];
            const auto qty_op = kOps[(i + j) % kOps.size()];
            for (const auto& day_literal : day_literals) {
                for (const auto& qty_literal : qty_literals) {
                    for (const auto policy : policies) {
                        // ((d op x && p op 0.5) && q op y): a tree, not a list.
                        const auto predicate = both(both(leaf("d", day_op, day_literal),
                                                         leaf("p", price_op, Literal{0.5}, policy)),
                                                    leaf("q", qty_op, qty_literal));
                        const auto keep = [&](std::size_t row) {
                            const bool price = policy == ir::FloatComparison::IEEE
                                                   ? holds(price_op, prices[row], 0.5)
                                                   : holds_nan_high(price_op, prices[row], 0.5);
                            return holds_literal(day_op, days[row], day_literal) && price &&
                                   holds_literal(qty_op, qtys[row], qty_literal);
                        };
                        for (const auto& range : ranges) {
                            CAPTURE(i, j, day_literal.index(), qty_literal.index(),
                                    static_cast<int>(policy), range.begin, range.count);
                            auto mask = compute_mask(predicate, table, nullptr, range);
                            REQUIRE(mask.has_value());
                            REQUIRE_FALSE(mask->valid.has_value());
                            std::vector<std::size_t> expected;
                            for (std::size_t r = 0; r < range.count; ++r) {
                                const bool kept = keep(range.begin + r);
                                REQUIRE((mask->value[r] != 0) == kept);
                                if (kept) {
                                    expected.push_back(r);
                                }
                            }
                            for (const std::size_t limit : {std::size_t{0}, std::size_t{17}}) {
                                CAPTURE(limit);
                                auto selection = compute_filter_selection(table, predicate, nullptr,
                                                                          range, limit);
                                REQUIRE(selection.has_value());
                                const std::size_t want =
                                    limit == 0 ? expected.size() : std::min(limit, expected.size());
                                REQUIRE(selection->kept == want);
                                REQUIRE(selection->keep_words.size() == (range.count + 63) / 64);
                                std::vector<std::size_t> got;
                                for (std::size_t w = 0; w < selection->keep_words.size(); ++w) {
                                    for (std::size_t bit = 0; bit < 64; ++bit) {
                                        if (((selection->keep_words[w] >> bit) & 1U) != 0) {
                                            got.push_back((w * 64) + bit);
                                        }
                                    }
                                }
                                REQUIRE(got ==
                                        std::vector<std::size_t>(
                                            expected.begin(),
                                            expected.begin() + static_cast<std::ptrdiff_t>(want)));
                            }
                        }
                    }
                }
            }
        }
    }
}

TEST_CASE("a conjunction with a nullable leaf keeps three-valued logic", "[filter][fused]") {
    // A nullable column does not fuse: `null && false` is false, `null && true`
    // is null and so not kept.
    Table table;
    ValidityBitmap valid(4, true);
    valid.set(1, false);
    valid.set(3, false);
    table.add_column("a", Column<std::int64_t>{std::vector<std::int64_t>{1, 2, 3, 4}},
                     std::move(valid));
    table.add_column("b", Column<std::int64_t>{std::vector<std::int64_t>{1, 1, 0, 0}});
    const auto predicate = both(leaf("a", ir::CompareOp::Gt, std::int64_t{0}),
                                leaf("b", ir::CompareOp::Eq, std::int64_t{1}));
    auto selection =
        compute_filter_selection(table, predicate, nullptr, RowRange::whole(table.rows()), 0);
    REQUIRE(selection.has_value());
    CHECK(selection->kept == 1);
    CHECK(selection->keep_words.at(0) == 0b0001U);
}
