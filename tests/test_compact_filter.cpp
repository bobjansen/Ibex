// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <thread>
#include <vector>

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_filter.hpp"

namespace {
using namespace ibex;
using namespace ibex::runtime;
using Integer = Column<std::int64_t>;
constexpr std::size_t kRows = 193;

auto input(bool compact, bool borrowed) -> Table {
    auto values = std::make_shared<std::vector<std::int32_t>>(kRows + 1);
    Integer id, wide;
    ValidityBitmap valid(kRows, true);
    for (std::size_t i = 0; i < kRows; ++i) {
        (*values)[i + 1] = i % 2 ? INT32_MIN : INT32_MAX;
        id.push_back(static_cast<std::int64_t>(i));
        wide.push_back(INT64_C(1099511627776) + static_cast<std::int64_t>(i));
        valid.set(i, i % 5 != 0);
    }
    Integer payload;
    if (!compact) {
        payload.assign(values->begin() + 1, values->end());
    } else if (borrowed) {
        payload = Integer::from_external_int32(values, values->data() + 1, kRows);
    } else {
        payload = Integer::from_compact_int32(
            ibex::detail::NoInitVector<std::int32_t>(values->begin() + 1, values->end()));
    }
    Table result;
    result.add_column("id", std::move(id));
    result.add_column("v", std::move(payload), std::move(valid));
    result.add_column("w", std::move(wide));
    return result;
}
auto predicate() -> ir::Expr {
    const auto bound = [](ir::CompareOp op, std::int64_t value) {
        return ir::Expr{
            .node = ir::CompareExpr{
                .op = op,
                .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "id"}}),
                .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = value}})}};
    };
    return ir::Expr{.node =
                        ir::LogicalExpr{.op = ir::LogicalOp::Or,
                                        .left = ir::make_expr_ptr(bound(ir::CompareOp::Lt, 3)),
                                        .right = ir::make_expr_ptr(bound(ir::CompareOp::Ge, 130))}};
}
auto rows(std::size_t begin, std::size_t end, std::size_t limit = kRows)
    -> std::vector<std::size_t> {
    std::vector<std::size_t> selected;
    for (std::size_t i = begin; i < end && selected.size() < limit; ++i) {
        if (i < 3 || i >= 130)
            selected.push_back(i);
    }
    return selected;
}
void check(const Table& result, const std::vector<std::size_t>& selected, bool compact) {
    REQUIRE(result.rows() == selected.size());
    const auto* v = result.find_entry("v");
    REQUIRE(v != nullptr);
    const auto& payload = std::get<Integer>(*v->column);
    REQUIRE(payload.is_compact_int32() == compact);
    const auto& wide = std::get<Integer>(*result.find_entry("w")->column);
    REQUIRE_FALSE(wide.is_compact_int32());
    const auto& id = std::get<Integer>(*result.find_entry("id")->column);
    REQUIRE_FALSE(id.is_compact_int32());  // Small Int64 values must not be narrowed.
    for (std::size_t i = 0; i < selected.size(); ++i) {
        const auto source = selected[i];
        REQUIRE(id[i] == static_cast<std::int64_t>(source));
        REQUIRE(payload[i] == (source % 2 ? INT32_MIN : INT32_MAX));
        REQUIRE(wide[i] == INT64_C(1099511627776) + static_cast<std::int64_t>(source));
        REQUIRE(v->validity.has_value());
        REQUIRE((*v->validity)[i] == (source % 5 != 0));
    }
}
}  // namespace

TEST_CASE("materialized filters preserve physical integer widths", "[compact-int32][filter]") {
    for (bool compact : {false, true}) {
        for (bool borrowed : {false, true}) {
            CAPTURE(compact, borrowed);
            const auto source = input(compact, borrowed);
            auto result = filter_table_range(source, predicate(),
                                             RowRange{.begin = 1, .count = 189}, nullptr);
            REQUIRE(result.has_value());
            check(*result, rows(1, 190), compact);
            const std::vector<ir::ColumnRef> project{{.name = "v"}, {.name = "w"}, {.name = "id"}};
            result = filter_project_table_range(source, predicate(), project,
                                                RowRange{.begin = 1, .count = 189}, nullptr);
            REQUIRE(result.has_value());
            REQUIRE(result->columns[0].name == "v");
            check(*result, rows(1, 190), compact);
            result = filter_table_limit(source, predicate(), 5, nullptr);
            REQUIRE(result.has_value());
            check(*result, rows(0, kRows, 5), compact);
            result = filter_table(source, ir::Expr{.node = ir::Literal{.value = false}}, nullptr);
            REQUIRE(result.has_value());
            check(*result, {}, compact);
        }
    }
}

TEST_CASE("native chunk filters preserve compact payloads and transport identity",
          "[compact-int32][filter][kernel]") {
    const auto source = input(true, true);
    const std::vector<ir::ColumnRef> project{{.name = "v"}, {.name = "w"}, {.name = "id"}};
    for (int mode = 0; mode < 3; ++mode) {
        CAPTURE(mode);
        auto chunk = table_to_chunk(source);
        chunk.sequence = 7;
        chunk.row_offset = 13;
        auto result =
            mode == 0 ? kernel::filter_chunk(std::move(chunk), predicate(), nullptr)
            : mode == 1
                ? kernel::filter_project_chunk(std::move(chunk), predicate(), project, nullptr)
                : kernel::filter_limit_chunk(std::move(chunk), predicate(), 5, nullptr);
        REQUIRE(result.has_value());
        REQUIRE(result->sequence == 7);
        REQUIRE(result->row_offset == 13);
        check(chunk_to_table(std::move(*result)), rows(0, kRows, mode == 2 ? 5 : kRows), true);
    }
}

TEST_CASE("parallel filter windows write compact output without widening",
          "[compact-int32][filter][parallel]") {
    const auto source = input(true, false);
    const RowRange left{.begin = 0, .count = 65}, right{.begin = 65, .count = kRows - 65};
    const auto first = compute_filter_selection(source, predicate(), nullptr, left, 0);
    const auto second = compute_filter_selection(source, predicate(), nullptr, right, 0);
    REQUIRE(first.has_value());
    REQUIRE(second.has_value());
    auto layout = build_filter_output_layout(source, nullptr);
    REQUIRE(layout.has_value());
    const std::vector<std::size_t> chars(layout->output.columns.size(), 0);
    presize_filter_output(layout->output, source, layout->src_of_dst, first->kept + second->kept,
                          chars);
    std::thread a([&] {
        gather_selection_into(layout->output, source, layout->src_of_dst, *first, left,
                              GatherDest{0});
    });
    std::thread b([&] {
        gather_selection_into(layout->output, source, layout->src_of_dst, *second, right,
                              GatherDest{first->kept});
    });
    a.join();
    b.join();
    check(layout->output, rows(0, kRows), true);
}
