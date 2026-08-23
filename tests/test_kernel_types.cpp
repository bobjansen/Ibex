// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "kernel_filter.hpp"
#include "kernel_gather.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"

namespace {

using namespace ibex;
using ibex::runtime::kernel::ChunkView;
using ibex::runtime::kernel::ColumnView;
using ibex::runtime::kernel::RowBitmap;
using ibex::runtime::kernel::RowIndices;
using ibex::runtime::kernel::RowRange;
using ibex::runtime::kernel::Selection;
using ibex::runtime::kernel::selection_rows;

}  // namespace

TEST_CASE("ColumnView aliases the column without copying", "[kernel][view]") {
    Column<std::int64_t> col{1, 2, 3, 4};
    ColumnView<std::int64_t> view(col, nullptr);
    REQUIRE(view.rows() == 4);
    REQUIRE(view.data() == col.data());
    REQUIRE(view.value(2) == 3);
    // Mutating the source is visible through the view: it is a view.
    col.push_back(5);
    REQUIRE(view.rows() == 4);  // the view's length is fixed at construction
}

TEST_CASE("ColumnView validity: absent means all valid", "[kernel][view]") {
    Column<std::int64_t> col{10, 20, 30};
    ColumnView<std::int64_t> all_valid(col, nullptr);
    REQUIRE(all_valid.is_valid(0));
    REQUIRE(all_valid.is_valid(2));

    ibex::runtime::ValidityBitmap bits;
    bits.push_back(true);
    bits.push_back(false);
    bits.push_back(true);
    ColumnView<std::int64_t> with_nulls(col, &bits);
    REQUIRE(with_nulls.is_valid(0));
    REQUIRE_FALSE(with_nulls.is_valid(1));
    REQUIRE(with_nulls.is_valid(2));
}

TEST_CASE("ChunkView exposes position, index space, and typed views", "[kernel][view]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{1, 2, 3});
    chunk.add_column("vol", Column<double>{1.5, 2.5, 3.5});
    chunk.row_offset = 96;
    chunk.sequence = 7;

    ChunkView view(chunk);
    REQUIRE(view.rows() == 3);
    REQUIRE(view.columns() == 2);
    REQUIRE(view.row_offset() == 96);
    REQUIRE(view.sequence() == 7);

    const auto price = view.view<std::int64_t>(0);
    REQUIRE(price.rows() == 3);
    REQUIRE(price.value(1) == 2);
    const auto vol = view.view<double>(1);
    REQUIRE(vol.value(2) == 3.5);
    // The view observes the chunk's storage, not a copy.
    REQUIRE(price.data() == std::get<Column<std::int64_t>>(view.column(0)).data());
}

TEST_CASE("ChunkView metadata map shares columns and preserves morsel identity", "[kernel][map]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.add_column("vol", Column<double>{1.5, 2.5}, runtime::ValidityBitmap{true, false});
    chunk.row_offset = 64;
    chunk.sequence = 3;
    const auto input_column = chunk.columns[1].column;

    const ChunkView view(chunk);
    const std::vector<runtime::kernel::MappedChunkColumn> map{
        {.source_position = 1, .name = "volume"}, {.source_position = 0, .name = "price"}};
    const auto props = runtime::TableProperties::derive(
        view.properties(),
        [](const std::string& name) -> runtime::KeyFate { return runtime::KeyFate::kept(name); },
        runtime::RowTransform::Preserve);
    const auto output = runtime::kernel::map_chunk(view, map, props);

    REQUIRE(output.columns.size() == 2);
    REQUIRE(output.columns[0].name == "volume");
    REQUIRE(output.columns[1].name == "price");
    REQUIRE(output.columns[0].column == input_column);
    REQUIRE(output.columns[0].validity.has_value());
    REQUIRE_FALSE((*output.columns[0].validity)[1]);
    REQUIRE(output.row_offset == 64);
    REQUIRE(output.sequence == 3);
}

TEST_CASE("Row-local update kernel preserves chunk identity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.sequence = 9;
    chunk.row_offset = 128;
    const auto input_column = chunk.columns.front().column;
    const std::vector<ir::FieldSpec> fields{
        {.alias = "alias", .expr = ir::Expr{.node = ir::ColumnRef{.name = "price"}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->sequence == 9);
    REQUIRE(updated->row_offset == 128);
    REQUIRE(updated->columns.size() == 2);
    REQUIRE(updated->columns[1].name == "alias");
    REQUIRE(updated->columns[1].column == input_column);
}

TEST_CASE("Row-local multi-field update reads earlier direct fields", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.sequence = 9;
    chunk.row_offset = 128;
    const std::vector<ir::FieldSpec> fields{
        {.alias = "doubled",
         .expr =
             ir::Expr{.node = ir::BinaryExpr{.op = ir::ArithmeticOp::Mul,
                                             .left = ir::make_expr_ptr(
                                                 ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                                             .right = ir::make_expr_ptr(ir::Expr{
                                                 .node = ir::Literal{.value = std::int64_t{2}}})}}},
        {.alias = "next",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Add,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "doubled"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{1}}})}}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    REQUIRE(updated->sequence == 9);
    REQUIRE(updated->row_offset == 128);
    const auto& doubled = std::get<Column<std::int64_t>>(*updated->columns[1].column);
    const auto& next = std::get<Column<std::int64_t>>(*updated->columns[2].column);
    CHECK(doubled[0] == 20);
    CHECK(doubled[1] == 40);
    CHECK(next[0] == 21);
    CHECK(next[1] == 41);
}

TEST_CASE("Row-local update shares the fixed-width numeric writer with scalar bindings",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20, 30},
                     runtime::ValidityBitmap{true, false, true});
    runtime::ScalarRegistry scalars;
    scalars.emplace("delta", std::int64_t{7});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "adjusted",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Add,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right = ir::make_expr_ptr(
                     ir::Expr{.node = ir::ColumnRef{.name = "delta", .lexical = true}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, &scalars, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& adjusted = std::get<Column<std::int64_t>>(*updated->columns[1].column);
    REQUIRE(adjusted[0] == 17);
    REQUIRE(adjusted[1] == 27);
    REQUIRE(adjusted[2] == 37);
    REQUIRE(updated->columns[1].validity.has_value());
    REQUIRE_FALSE((*updated->columns[1].validity)[1]);
}

TEST_CASE("Row-local update writes string interpolation into a presized slab", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("name", Column<std::string>{"a", "", "c"},
                     runtime::ValidityBitmap{true, false, true});
    runtime::ScalarRegistry scalars;
    scalars.emplace("suffix", std::string{"!"});

    ir::CallExpr interpolation{.callee = "__interp", .args = {}, .named_args = {}};
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"row="}}}));
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "name"}}));
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "suffix", .lexical = true}}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "label", .expr = ir::Expr{.node = std::move(interpolation)}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, &scalars, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& label = std::get<Column<std::string>>(*updated->columns[1].column);
    REQUIRE(label.size() == 3);
    CHECK(label[0] == "row=a!");
    CHECK(label[1].empty());
    CHECK(label[2] == "row=c!");
    REQUIRE(updated->columns[1].validity.has_value());
    CHECK((*updated->columns[1].validity)[0]);
    CHECK_FALSE((*updated->columns[1].validity)[1]);
    CHECK((*updated->columns[1].validity)[2]);
}

TEST_CASE("Row-local interpolation reads categorical dictionary codes", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("symbol",
                     Column<Categorical>{std::vector<std::string>{"AAPL", "GOOG"},
                                         std::vector<Column<Categorical>::code_type>{1, 0, 1}},
                     runtime::ValidityBitmap{true, false, true});
    ir::CallExpr interpolation{.callee = "__interp", .args = {}, .named_args = {}};
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"sym="}}}));
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "symbol"}}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "label", .expr = ir::Expr{.node = std::move(interpolation)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& label = std::get<Column<std::string>>(*updated->columns[1].column);
    CHECK(label[0] == "sym=GOOG");
    CHECK(label[1].empty());
    CHECK(label[2] == "sym=GOOG");
    REQUIRE(updated->columns[1].validity.has_value());
    CHECK_FALSE((*updated->columns[1].validity)[1]);
}

TEST_CASE("Row-local interpolation formats date and timestamp columns", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("day", Column<Date>{Date{0}, Date{1}}, runtime::ValidityBitmap{true, false});
    chunk.add_column("time", Column<Timestamp>{Timestamp{0}, Timestamp{1'000'000'000}});
    ir::CallExpr interpolation{.callee = "__interp", .args = {}, .named_args = {}};
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"d="}}}));
    interpolation.args.push_back(ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "day"}}));
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{" t="}}}));
    interpolation.args.push_back(
        ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "time"}}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "label", .expr = ir::Expr{.node = std::move(interpolation)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& label = std::get<Column<std::string>>(*updated->columns[2].column);
    CHECK(label[0] == "d=1970-01-01 t=1970-01-01 00:00:00.000000000");
    CHECK(label[1].empty());
    REQUIRE(updated->columns[2].validity.has_value());
    CHECK_FALSE((*updated->columns[2].validity)[1]);
}

TEST_CASE("Filter chunk kernel preserves morsel identity", "[kernel][filter]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20, 30});
    chunk.sequence = 7;
    chunk.row_offset = 128;
    const ir::Expr predicate{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Gt,
            .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{10}}})}};

    auto filtered = runtime::kernel::filter_chunk(std::move(chunk), predicate, nullptr);
    REQUIRE(filtered.has_value());
    REQUIRE(filtered->sequence == 7);
    REQUIRE(filtered->row_offset == 128);
    const auto& price = std::get<Column<std::int64_t>>(*filtered->columns[0].column);
    REQUIRE(price.size() == 2);
    REQUIRE(price[0] == 20);
    REQUIRE(price[1] == 30);
}

TEST_CASE("Filter chunk project and limit share direct predicate selection", "[kernel][filter]") {
    const ir::Expr predicate{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Ge,
            .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{20}}})}};

    runtime::Chunk projected_input;
    projected_input.add_column("price", Column<std::int64_t>{10, 20, 30, 40});
    projected_input.add_column("qty", Column<std::int64_t>{1, 2, 3, 4});
    const std::vector<ir::ColumnRef> project{{.name = "qty"}};
    auto projected = runtime::kernel::filter_project_chunk(std::move(projected_input), predicate,
                                                           project, nullptr);
    REQUIRE(projected.has_value());
    REQUIRE(projected->columns.size() == 1);
    REQUIRE(projected->columns[0].name == "qty");
    const auto& qty = std::get<Column<std::int64_t>>(*projected->columns[0].column);
    REQUIRE(qty.size() == 3);
    REQUIRE(qty[0] == 2);
    REQUIRE(qty[1] == 3);
    REQUIRE(qty[2] == 4);

    runtime::Chunk limited_input;
    limited_input.add_column("price", Column<std::int64_t>{10, 20, 30, 40});
    auto limited =
        runtime::kernel::filter_limit_chunk(std::move(limited_input), predicate, 2, nullptr);
    REQUIRE(limited.has_value());
    const auto& price = std::get<Column<std::int64_t>>(*limited->columns[0].column);
    REQUIRE(price.size() == 2);
    REQUIRE(price[0] == 20);
    REQUIRE(price[1] == 30);
}

TEST_CASE("Native filter chunk gather preserves packed, variable, and nullable columns",
          "[kernel][filter]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20, 30, 40});
    chunk.add_column("enabled", Column<bool>{true, false, true, true});
    chunk.add_column("symbol", Column<std::string>{"a", "bb", "ccc", "dddd"});
    chunk.add_column("qty", Column<std::int64_t>{1, 2, 3, 4},
                     runtime::ValidityBitmap{true, false, true, false});
    const ir::Expr predicate{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Ge,
            .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{20}}})}};

    auto filtered = runtime::kernel::filter_chunk(std::move(chunk), predicate, nullptr);
    REQUIRE(filtered.has_value());
    REQUIRE(filtered->rows() == 3);
    const auto& enabled = std::get<Column<bool>>(*filtered->columns[1].column);
    REQUIRE_FALSE(enabled[0]);
    REQUIRE(enabled[1]);
    REQUIRE(enabled[2]);
    const auto& symbol = std::get<Column<std::string>>(*filtered->columns[2].column);
    REQUIRE(symbol[0] == "bb");
    REQUIRE(symbol[1] == "ccc");
    REQUIRE(symbol[2] == "dddd");
    REQUIRE_FALSE((*filtered->columns[3].validity)[0]);
    REQUIRE((*filtered->columns[3].validity)[1]);
    REQUIRE_FALSE((*filtered->columns[3].validity)[2]);
}

TEST_CASE("Native filter chunk preserves logical rows without columns", "[kernel][filter]") {
    runtime::Chunk chunk;
    chunk.logical_rows = 3;
    const ir::Expr predicate{.node = ir::Literal{.value = true}};

    auto filtered = runtime::kernel::filter_chunk(std::move(chunk), predicate, nullptr);
    REQUIRE(filtered.has_value());
    REQUIRE(filtered->columns.empty());
    REQUIRE(filtered->logical_rows == 3);
    REQUIRE(filtered->rows() == 3);
}

TEST_CASE("Row-local alias update overwrites keys without copying its source", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.add_column("replacement", Column<std::int64_t>{30, 40},
                     runtime::ValidityBitmap{true, false});
    chunk.set_properties(runtime::TableProperties::sorted_by({{.name = "price"}}));
    const auto replacement = chunk.columns[1].column;
    const std::vector<ir::FieldSpec> fields{
        {.alias = "price", .expr = ir::Expr{.node = ir::ColumnRef{.name = "replacement"}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->columns.size() == 2);
    REQUIRE(updated->columns[0].name == "price");
    REQUIRE(updated->columns[0].column == replacement);
    REQUIRE(updated->columns[0].validity.has_value());
    REQUIRE_FALSE((*updated->columns[0].validity)[1]);
    REQUIRE_FALSE(updated->ordering().has_value());
}

TEST_CASE("Row-local alias update keeps the time-index write guard", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("time", Column<std::int64_t>{10, 20});
    chunk.add_column("replacement", Column<std::int64_t>{30, 40});
    chunk.set_properties(runtime::TableProperties::time_frame("time"));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "time", .expr = ir::Expr{.node = ir::ColumnRef{.name = "replacement"}}}};
    const runtime::ExecutionContext exec{};

    const auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE_FALSE(updated.has_value());
    REQUIRE(updated.error() == "cannot update time index column: time");
}

TEST_CASE("Row-local fill_null writes filled payloads and clears validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("value", Column<std::int64_t>{10, 0, 30},
                     runtime::ValidityBitmap{true, false, true});
    ir::CallExpr call{
        .callee = "fill_null",
        .args = {ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "value"}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{99}}})},
        .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "value", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& value = std::get<Column<std::int64_t>>(*updated->columns[0].column);
    CHECK(value[0] == 10);
    CHECK(value[1] == 99);
    CHECK(value[2] == 30);
    CHECK_FALSE(updated->columns[0].validity.has_value());
}

TEST_CASE("Row-local fill_null validates its literal even when the source is all-valid",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("value", Column<std::int64_t>{10, 20});
    ir::CallExpr call{
        .callee = "fill_null",
        .args = {ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "value"}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"bad"}}})},
        .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "filled", .expr = ir::Expr{.node = std::move(call)}}};

    const auto updated = runtime::kernel::update_row_local_chunk(
        std::move(chunk), fields, nullptr, nullptr, runtime::ExecutionContext{});
    REQUIRE_FALSE(updated.has_value());
    CHECK(updated.error() == "fill_null: fill value type does not match column type for 'value'");
}

TEST_CASE("Row-local coalesce chooses the first valid source and retains all-null validity",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("left", Column<std::int64_t>{10, 0, 0, 0},
                     runtime::ValidityBitmap{true, false, false, false});
    chunk.add_column("right", Column<std::int64_t>{0, 20, 0, 0},
                     runtime::ValidityBitmap{false, true, false, false});
    ir::CallExpr call{.callee = "coalesce",
                      .args = {ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "left"}}),
                               ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "right"}})},
                      .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "result", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& result = std::get<Column<std::int64_t>>(*updated->columns[2].column);
    CHECK(result[0] == 10);
    CHECK(result[1] == 20);
    CHECK(result[2] == 0);
    CHECK(result[3] == 0);
    REQUIRE(updated->columns[2].validity.has_value());
    CHECK_FALSE((*updated->columns[2].validity)[2]);
    CHECK_FALSE((*updated->columns[2].validity)[3]);
}

TEST_CASE("Row-local coalesce literal fallback makes a nullable result all-valid",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("value", Column<double>{1.5, 0.0}, runtime::ValidityBitmap{true, false});
    ir::CallExpr call{.callee = "coalesce",
                      .args = {ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "value"}}),
                               ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 7.0}})},
                      .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "filled", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& filled = std::get<Column<double>>(*updated->columns[1].column);
    CHECK(filled[0] == 1.5);
    CHECK(filled[1] == 7.0);
    CHECK_FALSE(updated->columns[1].validity.has_value());
}

TEST_CASE("Row-local CASE selects fixed-width literal arms with SQL null conditions",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{5, 0, -2},
                     runtime::ValidityBitmap{true, false, true});
    auto condition = ir::Expr{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Gt,
            .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})}};
    ir::CallExpr call{
        .callee = "__case",
        .args = {ir::make_expr_ptr(std::move(condition)),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{1}}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})},
        .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "sign", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& sign = std::get<Column<std::int64_t>>(*updated->columns[1].column);
    CHECK(sign[0] == 1);
    CHECK(sign[1] == 0);  // null condition does not match; ELSE wins.
    CHECK(sign[2] == 0);
    CHECK_FALSE(updated->columns[1].validity.has_value());
}

TEST_CASE("Row-local CASE writes string slabs from literal arms", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{5, -2, 1});
    auto condition = ir::Expr{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Gt,
            .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})}};
    ir::CallExpr call{
        .callee = "__case",
        .args = {ir::make_expr_ptr(std::move(condition)),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"up"}}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"down"}}})},
        .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "label", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& label = std::get<Column<std::string>>(*updated->columns[1].column);
    CHECK(label[0] == "up");
    CHECK(label[1] == "down");
    CHECK(label[2] == "up");
    CHECK_FALSE(updated->columns[1].validity.has_value());
}

TEST_CASE("Row-local CASE carries selected string-arm validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{1, 2});
    chunk.add_column("label", Column<std::string>{"yes", ""}, runtime::ValidityBitmap{true, false});
    auto condition = ir::Expr{
        .node = ir::CompareExpr{
            .op = ir::CompareOp::Gt,
            .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
            .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})}};
    ir::CallExpr call{
        .callee = "__case",
        .args = {ir::make_expr_ptr(std::move(condition)),
                 ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "label"}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::string{"other"}}})},
        .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "result", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& result = std::get<Column<std::string>>(*updated->columns[2].column);
    CHECK(result[0] == "yes");
    CHECK(result[1] == "");
    REQUIRE(updated->columns[2].validity.has_value());
    CHECK_FALSE((*updated->columns[2].validity)[1]);
}

TEST_CASE("Row-local temporal parts preserve source validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column(
        "time",
        Column<Timestamp>{Timestamp{0}, Timestamp{3LL * 60 * 60 * 1'000'000'000}, Timestamp{0}},
        runtime::ValidityBitmap{true, true, false});
    ir::CallExpr call{.callee = "hour",
                      .args = {ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "time"}})},
                      .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "hour", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& hour = std::get<Column<std::int64_t>>(*updated->columns[1].column);
    CHECK(hour[0] == 0);
    CHECK(hour[1] == 3);
    REQUIRE(updated->columns[1].validity.has_value());
    CHECK_FALSE((*updated->columns[1].validity)[2]);
}

TEST_CASE("Row-local CASE retains selected categorical labels", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("pick_left", Column<bool>{true, false, true});
    chunk.add_column("left",
                     Column<Categorical>{std::vector<std::string>{"A", "B"},
                                         std::vector<Column<Categorical>::code_type>{0, 1, 0}});
    chunk.add_column("right",
                     Column<Categorical>{std::vector<std::string>{"B", "C"},
                                         std::vector<Column<Categorical>::code_type>{1, 0, 1}});
    ir::CallExpr call{
        .callee = "__case",
        .args = {ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "pick_left"}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "left"}}),
                 ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "right"}})},
        .named_args = {}};
    const std::vector<ir::FieldSpec> fields{
        {.alias = "result", .expr = ir::Expr{.node = std::move(call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(
        std::move(chunk), fields, nullptr, nullptr, runtime::ExecutionContext{.parallel = false});
    REQUIRE(updated.has_value());
    const auto& result = std::get<Column<Categorical>>(*updated->columns[3].column);
    CHECK(result[0] == "A");
    CHECK(result[1] == "B");
    CHECK(result[2] == "A");
}

TEST_CASE("Row-local fixed-width binary update uses all-valid column views", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("left", Column<std::int64_t>{2, 3, 4});
    chunk.add_column("right", Column<std::int64_t>{5, 7, 11});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "product",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "left"}}),
                 .right = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "right"}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->columns.size() == 3);
    const auto& product = std::get<Column<std::int64_t>>(*updated->columns[2].column);
    REQUIRE(product[0] == 10);
    REQUIRE(product[1] == 21);
    REQUIRE(product[2] == 44);
}

TEST_CASE("Row-local fixed-width binary update ANDs nullable input validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("left", Column<std::int64_t>{2, 3, 4},
                     runtime::ValidityBitmap{true, false, true});
    chunk.add_column("right", Column<std::int64_t>{5, 7, 11},
                     runtime::ValidityBitmap{true, true, false});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "sum",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Add,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "left"}}),
                 .right = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "right"}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->columns[2].validity.has_value());
    REQUIRE((*updated->columns[2].validity)[0]);
    REQUIRE_FALSE((*updated->columns[2].validity)[1]);
    REQUIRE_FALSE((*updated->columns[2].validity)[2]);
}

TEST_CASE("Row-local double binary update preserves nullable division", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("numerator", Column<double>{6.0, 9.0}, runtime::ValidityBitmap{true, false});
    chunk.add_column("denominator", Column<double>{2.0, 3.0});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "quotient",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Div,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "numerator"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "denominator"}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& quotient = std::get<Column<double>>(*updated->columns[2].column);
    REQUIRE(quotient[0] == 3.0);
    REQUIRE(updated->columns[2].validity.has_value());
    REQUIRE_FALSE((*updated->columns[2].validity)[1]);
}

TEST_CASE("Row-local unary Double update writes a nullable whole column", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<double>{1.0, 4.0, 9.0},
                     runtime::ValidityBitmap{true, false, true});
    ir::CallExpr sqrt_call{.callee = "sqrt", .args = {}, .named_args = {}};
    sqrt_call.args.push_back(ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "root", .expr = ir::Expr{.node = std::move(sqrt_call)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& root = std::get<Column<double>>(*updated->columns[1].column);
    REQUIRE(root[0] == 1.0);
    REQUIRE(root[2] == 3.0);
    REQUIRE(updated->columns[1].validity.has_value());
    REQUIRE((*updated->columns[1].validity)[0]);
    REQUIRE_FALSE((*updated->columns[1].validity)[1]);
    REQUIRE((*updated->columns[1].validity)[2]);
}

TEST_CASE("Row-local numeric tree composes arithmetic clips and unary transforms",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<double>{4.0, 25.0, 16.0},
                     runtime::ValidityBitmap{true, true, false});
    chunk.add_column("cap", Column<double>{9.0, 9.0, 9.0});
    const auto price = [] {
        return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}});
    };
    const auto cap = [] {
        return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "cap"}});
    };
    auto twice =
        ir::Expr{.node = ir::BinaryExpr{
                     .op = ir::ArithmeticOp::Mul,
                     .left = price(),
                     .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 2.0}})}};
    ir::CallExpr clipped{.callee = "pmin", .args = {}, .named_args = {}};
    clipped.args.push_back(ir::make_expr_ptr(std::move(twice)));
    clipped.args.push_back(cap());
    ir::CallExpr root{.callee = "sqrt", .args = {}, .named_args = {}};
    root.args.push_back(ir::make_expr_ptr(ir::Expr{.node = std::move(clipped)}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "root", .expr = ir::Expr{.node = std::move(root)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& values = std::get<Column<double>>(*updated->columns[2].column);
    CHECK(values[0] == std::sqrt(8.0));
    CHECK(values[1] == 3.0);
    REQUIRE(updated->columns[2].validity.has_value());
    CHECK_FALSE((*updated->columns[2].validity)[2]);
}

TEST_CASE("Row-local string length update reads slabs and categorical codes", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("text", Column<std::string>{"café", "日本語"},
                     runtime::ValidityBitmap{true, false});
    Column<Categorical> category(std::vector<std::string>{"é", "plain"});
    category.push_code(0);
    category.push_code(1);
    chunk.add_column("category", std::move(category));

    ir::CallExpr characters{.callee = "length", .args = {}, .named_args = {}};
    characters.args.push_back(ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "text"}}));
    ir::CallExpr bytes{.callee = "byte_length", .args = {}, .named_args = {}};
    bytes.args.push_back(ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "category"}}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "characters", .expr = ir::Expr{.node = std::move(characters)}},
        {.alias = "bytes", .expr = ir::Expr{.node = std::move(bytes)}}};

    auto updated = runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr,
                                                           nullptr, runtime::ExecutionContext{});
    REQUIRE(updated.has_value());
    const auto& character_counts = std::get<Column<std::int64_t>>(*updated->columns[2].column);
    const auto& byte_counts = std::get<Column<std::int64_t>>(*updated->columns[3].column);
    CHECK(character_counts[0] == 4);
    CHECK(character_counts[1] == 3);
    CHECK(byte_counts[0] == 2);
    CHECK(byte_counts[1] == 5);
    REQUIRE(updated->columns[2].validity.has_value());
    CHECK_FALSE((*updated->columns[2].validity)[1]);
}

TEST_CASE("Row-local Int64 division writes Double and preserves IEEE/null semantics",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("numerator", Column<std::int64_t>{9, 1, 8},
                     runtime::ValidityBitmap{true, false, true});
    chunk.add_column("denominator", Column<std::int64_t>{2, 0, 0});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "quotient",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Div,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "numerator"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "denominator"}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& quotient = std::get<Column<double>>(*updated->columns[2].column);
    REQUIRE(quotient[0] == 4.5);
    REQUIRE(std::isinf(quotient[2]));
    REQUIRE(updated->columns[2].validity.has_value());
    REQUIRE((*updated->columns[2].validity)[0]);
    REQUIRE_FALSE((*updated->columns[2].validity)[1]);
    REQUIRE((*updated->columns[2].validity)[2]);
}

TEST_CASE("Row-local Int64 literal division writes Double", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{9, 10});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "half",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Div,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{2}}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& half = std::get<Column<double>>(*updated->columns[1].column);
    REQUIRE(half[0] == 4.5);
    REQUIRE(half[1] == 5.0);
}

TEST_CASE("Row-local mixed numeric update writes Double and ANDs validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("whole", Column<std::int64_t>{7, 9}, runtime::ValidityBitmap{true, false});
    chunk.add_column("fraction", Column<double>{2.5, 4.0});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "remainder",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mod,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "whole"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "fraction"}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& remainder = std::get<Column<double>>(*updated->columns[2].column);
    REQUIRE(remainder[0] == 2.0);
    REQUIRE(updated->columns[2].validity.has_value());
    REQUIRE_FALSE((*updated->columns[2].validity)[1]);
}

TEST_CASE("Row-local Int64 and Double literal update writes Double", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{7, 9});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "remainder",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mod,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 2.5}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& remainder = std::get<Column<double>>(*updated->columns[1].column);
    REQUIRE(remainder[0] == 2.0);
    REQUIRE(remainder[1] == 1.5);
}

TEST_CASE("Row-local comparison update packs Bool and carries 3VL validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20, 30},
                     runtime::ValidityBitmap{true, false, true});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "expensive",
         .expr = ir::Expr{
             .node = ir::CompareExpr{
                 .op = ir::CompareOp::Gt,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right = ir::make_expr_ptr(
                     ir::Expr{.node = ir::Literal{.value = std::int64_t{15}}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& expensive = std::get<Column<bool>>(*updated->columns[1].column);
    REQUIRE_FALSE(expensive[0]);
    REQUIRE(expensive[2]);
    REQUIRE(updated->columns[1].validity.has_value());
    REQUIRE((*updated->columns[1].validity)[0]);
    REQUIRE_FALSE((*updated->columns[1].validity)[1]);
    REQUIRE((*updated->columns[1].validity)[2]);
}

TEST_CASE("Row-local Int64 literal update preserves nullable source validity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20}, runtime::ValidityBitmap{true, false});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "doubled",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{2}}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& doubled = std::get<Column<std::int64_t>>(*updated->columns[1].column);
    REQUIRE(doubled[0] == 20);
    REQUIRE(updated->columns[1].validity.has_value());
    REQUIRE_FALSE((*updated->columns[1].validity)[1]);
}

TEST_CASE("Row-local Int64 literal modulo keeps zero-divisor semantics", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "remainder",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mod,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& remainder = std::get<Column<std::int64_t>>(*updated->columns[1].column);
    REQUIRE(remainder[0] == 0);
    REQUIRE(remainder[1] == 0);
}

TEST_CASE("Row-local literal update replaces a nullable column with valid values",
          "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20}, runtime::ValidityBitmap{true, false});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "price", .expr = ir::Expr{.node = ir::Literal{.value = std::int64_t{7}}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& price = std::get<Column<std::int64_t>>(*updated->columns[0].column);
    REQUIRE(price[0] == 7);
    REQUIRE(price[1] == 7);
    REQUIRE_FALSE(updated->columns[0].validity.has_value());
}

TEST_CASE("Row-local Double literal update accepts an Int literal", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<double>{1.5, 2.5});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "doubled",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right =
                     ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{2}}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& doubled = std::get<Column<double>>(*updated->columns[1].column);
    REQUIRE(doubled[0] == 3.0);
    REQUIRE(doubled[1] == 5.0);
}

TEST_CASE("Selection shapes answer their survivor counts", "[kernel][view]") {
    const RowRange range{.begin = 4, .end = 9};
    REQUIRE(selection_rows(Selection{range}, 100) == 5);

    const std::vector<std::size_t> indices{2, 5, 6, 8};
    const RowIndices picked{.data = indices.data(), .count = indices.size()};
    REQUIRE(selection_rows(Selection{picked}, 100) == 4);
    REQUIRE(picked[2] == 6);

    ibex::runtime::ValidityBitmap bits;
    for (int i = 0; i < 6; ++i) {
        bits.push_back(i == 1 || i == 4);
    }
    const RowBitmap masked{.bits = &bits};
    REQUIRE(selection_rows(Selection{masked}, 6) == 2);
    REQUIRE(masked.test(4));
    REQUIRE_FALSE(masked.test(0));
}

TEST_CASE("RowWordBlocks counts and iterates like the filter's mask", "[kernel][selection]") {
    // keep rows 1, 2, 7 in the first word and 64+3 in the second, over a
    // range beginning at absolute row 5.
    const std::vector<std::uint64_t> words{0b10000110U, 0b1000U};
    const ibex::runtime::kernel::RowWordBlocks blocks{
        .words = words.data(), .word_count = words.size(), .row_base = 5};
    REQUIRE(ibex::runtime::kernel::selection_rows(ibex::runtime::kernel::Selection{blocks}, 100) ==
            4);

    Column<std::int64_t> col;
    for (int i = 0; i < 80; ++i) {
        col.push_back(i);
    }
    std::vector<std::int64_t> out(4, -1);
    ibex::runtime::kernel::gather_selected(
        ibex::runtime::kernel::ColumnView<std::int64_t>(col.data(), col.size(), nullptr),
        ibex::runtime::kernel::Selection{blocks},
        ibex::runtime::kernel::OutputSpan<std::int64_t>{
            .data = out.data(), .begin = 0, .count = 4});
    // word 0, bits 1/2/7 are absolute rows 6/7/12; word 1, bit 3 is row 5+67.
    REQUIRE(out[0] == 6);
    REQUIRE(out[1] == 7);
    REQUIRE(out[2] == 12);
    REQUIRE(out[3] == 72);
}

TEST_CASE("gather_selected over every selection shape agrees", "[kernel][gather]") {
    Column<std::int64_t> col;
    for (int i = 0; i < 10; ++i) {
        col.push_back(100 + i);
    }
    using ibex::runtime::kernel::ColumnView;
    using ibex::runtime::kernel::OutputSpan;
    using ibex::runtime::kernel::Selection;
    const ColumnView<std::int64_t> view(col.data(), col.size(), nullptr);

    // Survivors: rows 2, 3, 5, 8 — expressed three ways.
    const std::vector<std::size_t> indices{2, 3, 5, 8};
    ibex::runtime::ValidityBitmap keep;
    for (int i = 0; i < 10; ++i) {
        keep.push_back(i == 2 || i == 3 || i == 5 || i == 8);
    }
    // rows 2,3,5,8 are bits 2,3,5,8 of word 0; word 1 is empty and must be
    // skipped by the iteration.
    const std::vector<std::uint64_t> blocks{(1U << 2) | (1U << 3) | (1U << 5) | (1U << 8), 0U};

    std::vector<std::int64_t> a(4), b(4), c(4), d(4);
    ibex::runtime::kernel::gather_selected(
        view, Selection{ibex::runtime::kernel::RowIndices{indices.data(), indices.size()}},
        OutputSpan<std::int64_t>{a.data(), 0, 4});
    ibex::runtime::kernel::gather_selected(view, Selection{ibex::runtime::kernel::RowBitmap{&keep}},
                                           OutputSpan<std::int64_t>{b.data(), 0, 4});
    ibex::runtime::kernel::gather_selected(
        view, Selection{ibex::runtime::kernel::RowWordBlocks{blocks.data(), blocks.size(), 0}},
        OutputSpan<std::int64_t>{c.data(), 0, 4});
    // A contiguous range: rows 2..5 (4 rows) — same count, different rows, to
    // prove the copy arm does not just agree with the others by accident.
    ibex::runtime::kernel::gather_selected(view, Selection{ibex::runtime::kernel::RowRange{2, 6}},
                                           OutputSpan<std::int64_t>{d.data(), 0, 4});

    REQUIRE(a[0] == 102);
    REQUIRE(a[1] == 103);
    REQUIRE(a[2] == 105);
    REQUIRE(a[3] == 108);
    REQUIRE(b == a);
    REQUIRE(c == a);
    REQUIRE(d[0] == 102);
    REQUIRE(d[1] == 103);
    REQUIRE(d[2] == 104);
    REQUIRE(d[3] == 105);
}

TEST_CASE("gather_selected honours the output offset for disjoint windows", "[kernel][gather]") {
    Column<std::int64_t> col{1, 2, 3, 4, 5, 6};
    using ibex::runtime::kernel::ColumnView;
    using ibex::runtime::kernel::OutputSpan;
    using ibex::runtime::kernel::Selection;
    std::vector<std::int64_t> out(6, -1);
    // Two workers, disjoint windows of one presized buffer: rows [0,3) at
    // offset 0, rows [3,6) at offset 3 — the two-phase filter's shape.
    ibex::runtime::kernel::gather_selected(
        ColumnView<std::int64_t>(col.data(), col.size(), nullptr),
        Selection{ibex::runtime::kernel::RowRange{0, 3}},
        OutputSpan<std::int64_t>{out.data(), 0, 3});
    ibex::runtime::kernel::gather_selected(
        ColumnView<std::int64_t>(col.data(), col.size(), nullptr),
        Selection{ibex::runtime::kernel::RowRange{3, 6}},
        OutputSpan<std::int64_t>{out.data(), 3, 3});
    REQUIRE(out == std::vector<std::int64_t>{1, 2, 3, 4, 5, 6});
}

TEST_CASE("Bool gather kernel: word-block selection matches per-bit truth",
          "[kernel][gather][bool]") {
    // 130 bools so the word machinery straddles word boundaries.
    Column<bool> src;
    std::vector<bool> truth;
    for (int i = 0; i < 130; ++i) {
        const bool v = (i % 3) == 0 || i == 97;
        src.push_back(v);
        truth.push_back(v);
    }
    // Keep rows 1, 64, 65, 129 (bits 1 of word 0; bits 0,1 of word 1; bit 1 of word 2).
    const std::vector<std::uint64_t> sel_words{(1U << 1), 0b11U, (1U << 1)};
    const ibex::runtime::kernel::RowWordBlocks blocks{
        .words = sel_words.data(), .word_count = sel_words.size(), .row_base = 0};

    std::vector<std::uint64_t> out_words(1, 0);  // 4 survivors fit one word
    ibex::runtime::kernel::gather_selected_bool(
        ibex::runtime::kernel::BoolView(src), ibex::runtime::kernel::Selection{blocks},
        ibex::runtime::kernel::BoolOutputSpan{.words = out_words.data(), .begin = 0, .count = 4});

    const auto bit = [&](std::size_t i) { return (out_words[i / 64] >> (i % 64)) & 1U; };
    REQUIRE(bit(0) == truth[1]);
    REQUIRE(bit(1) == truth[64]);
    REQUIRE(bit(2) == truth[65]);
    REQUIRE(bit(3) == truth[129]);
    // Nothing beyond the run was set.
    REQUIRE((out_words[0] >> 4) == 0);
}

TEST_CASE("Bool gather kernel: disjoint windows OR without clobbering", "[kernel][gather][bool]") {
    Column<bool> src;
    for (int i = 0; i < 100; ++i) {
        src.push_back(i % 2 == 0);
    }
    std::vector<std::uint64_t> out_words(2, 0);
    // Window A: rows [10, 20) at bit offset 10; window B: rows [20, 30) at 20.
    ibex::runtime::kernel::gather_selected_bool(
        ibex::runtime::kernel::BoolView(src),
        ibex::runtime::kernel::Selection{ibex::runtime::kernel::RowRange{10, 20}},
        ibex::runtime::kernel::BoolOutputSpan{.words = out_words.data(), .begin = 10, .count = 10});
    ibex::runtime::kernel::gather_selected_bool(
        ibex::runtime::kernel::BoolView(src),
        ibex::runtime::kernel::Selection{ibex::runtime::kernel::RowRange{20, 30}},
        ibex::runtime::kernel::BoolOutputSpan{.words = out_words.data(), .begin = 20, .count = 10});
    // Output bits [10, 30) hold source rows 10..29: even rows are true.
    for (std::size_t i = 10; i < 30; ++i) {
        const bool got = ((out_words[i / 64] >> (i % 64)) & 1U) != 0;
        INFO("bit " << i);
        REQUIRE(got == (i % 2 == 0));
    }
    REQUIRE((out_words[0] & 0x3FFU) == 0);  // bits [0,10) untouched
}

TEST_CASE("String gather kernel: word-block selection packs slabs and offsets",
          "[kernel][gather][string]") {
    // Build a source: "a", "bb", "", "ccc", "dd" with offsets 0,1,3,3,6,8.
    Column<std::string> src{"a", "bb", "", "ccc", "dd"};
    // Keep rows 0, 2, 3, 4.
    const std::vector<std::uint64_t> sel_words{(1U << 0) | (1U << 2) | (1U << 3) | (1U << 4)};
    const ibex::runtime::kernel::RowWordBlocks blocks{
        .words = sel_words.data(), .word_count = sel_words.size(), .row_base = 0};

    std::vector<char> chars(6);
    std::vector<std::uint32_t> offsets(5, 9999);
    ibex::runtime::kernel::gather_selected_strings(
        ibex::runtime::kernel::StringView{
            .offsets = src.offsets_data(), .chars = src.chars_data(), .rows = src.size()},
        ibex::runtime::kernel::Selection{blocks},
        ibex::runtime::kernel::StringOutputSpan{.offsets = offsets.data(),
                                                .chars = chars.data(),
                                                .begin = 0,
                                                .count = 4,
                                                .char_base = 0});

    // Rows kept: "a" (len 1), "" (0), "ccc" (3), "dd" (2) -> 6 chars total.
    REQUIRE(offsets[1] == 1);
    REQUIRE(offsets[2] == 1);
    REQUIRE(offsets[3] == 4);
    REQUIRE(offsets[4] == 6);
    const std::string packed(chars.begin(), chars.end());
    REQUIRE(packed == "acccdd");
}

TEST_CASE("String gather kernel: offset window continues a prior run", "[kernel][gather][string]") {
    Column<std::string> src{"xx", "yy", "zzz"};
    // Window over rows [2,3) written at output begin=1, continuing after a
    // hypothetical first window that ended at char_base=4 ("xxyy" equivalent).
    std::vector<char> chars(7);
    std::vector<std::uint32_t> offsets(3, 9999);
    offsets[1] = 4;  // the previous window's end offset — this kernel's contract
    ibex::runtime::kernel::gather_selected_strings(
        ibex::runtime::kernel::StringView{
            .offsets = src.offsets_data(), .chars = src.chars_data(), .rows = src.size()},
        ibex::runtime::kernel::Selection{ibex::runtime::kernel::RowRange{2, 3}},
        ibex::runtime::kernel::StringOutputSpan{.offsets = offsets.data(),
                                                .chars = chars.data(),
                                                .begin = 1,
                                                .count = 1,
                                                .char_base = 4});
    REQUIRE(offsets[2] == 7);
    REQUIRE(std::string(chars.begin() + 4, chars.begin() + 7) == "zzz");
}

// A parallel row-local update used to reach its kernels only by converting the
// chunk to a Table so `update_table` could split the field. The chunk kernel
// now owns that split: the field is planned once and its ranges write disjoint
// windows, with no bridge in between. `chunk_direct_updates` observes the seam
// itself, since the bridge answers identically — just via a Table.
TEST_CASE("Parallel row-local update splits inside the chunk kernel",
          "[kernel][update][parallel]") {
    constexpr std::size_t kRows = 40'000;
    Column<double> price;
    for (std::size_t r = 0; r < kRows; ++r) {
        price.push_back(static_cast<double>(r));
    }
    runtime::Chunk chunk;
    chunk.add_column("price", std::move(price));
    chunk.sequence = 7;
    chunk.row_offset = 512;
    const std::vector<ir::FieldSpec> fields{
        {.alias = "doubled",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 2.0}})}}}};

    runtime::ParallelIslandStats stats;
    runtime::ExecutionContext exec;
    exec.parallel = true;
    exec.parallel_threads = 4;
    exec.parallel_min_rows = 0;
    exec.parallel_min_cells = 0;
    exec.parallel_grain = 4'096;
    exec.parallel_stats = &stats;

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->sequence == 7);
    REQUIRE(updated->row_offset == 512);
    REQUIRE(updated->columns.size() == 2);
    const auto& doubled = std::get<Column<double>>(*updated->columns[1].column);
    REQUIRE(doubled.size() == kRows);
    // Spot-check the first row of several windows, which is where a mis-sized
    // window would land its first wrong value.
    CHECK(doubled[0] == 0.0);
    CHECK(doubled[4'096] == 8'192.0);
    CHECK(doubled[20'001] == 40'002.0);
    CHECK(doubled[kRows - 1] == static_cast<double>(2 * (kRows - 1)));
    CHECK(stats.chunk_direct_updates.load() == 1);
    // The kernel did split it, and reported the split under the same counters
    // the table evaluator used when it owned this loop.
    CHECK(stats.parallel_fields.load() == 1);
    CHECK(stats.parallel_direct_numeric_fields.load() == 1);
}

// The same field below the size gates: the split is declined, and the chunk is
// still written directly rather than handed to the table evaluator.
TEST_CASE("A declined split still avoids the table bridge", "[kernel][update][parallel]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<double>{1.0, 2.0, 3.0});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "doubled",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}),
                 .right = ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = 2.0}})}}}};

    runtime::ParallelIslandStats stats;
    runtime::ExecutionContext exec;
    exec.parallel = true;
    exec.parallel_threads = 4;
    exec.parallel_stats = &stats;

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& doubled = std::get<Column<double>>(*updated->columns[1].column);
    CHECK(std::vector<double>(doubled.begin(), doubled.end()) ==
          std::vector<double>{2.0, 4.0, 6.0});
    CHECK(stats.chunk_direct_updates.load() == 1);
    CHECK(stats.parallel_fields.load() == 0);
}

// An expression the direct route does not name keeps the bridge, deliberately:
// the table evaluator can still split it through its own range writer, and
// declining is how such a field keeps its parallelism rather than how it loses
// it. `sqrt` is one — the compiled numeric tree owns it, and that tree has no
// range-writing form on this side of the seam yet.
TEST_CASE("An unplanned expression keeps the table bridge", "[kernel][update][parallel]") {
    constexpr std::size_t kRows = 40'000;
    Column<double> price;
    for (std::size_t r = 0; r < kRows; ++r) {
        price.push_back(static_cast<double>(r));
    }
    runtime::Chunk chunk;
    chunk.add_column("price", std::move(price));
    std::vector<ir::ExprPtr> args;
    args.push_back(ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "price"}}));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "root",
         .expr = ir::Expr{
             .node = ir::CallExpr{.callee = "sqrt", .args = std::move(args), .named_args = {}}}}};

    runtime::ParallelIslandStats stats;
    runtime::ExecutionContext exec;
    exec.parallel = true;
    exec.parallel_threads = 4;
    exec.parallel_min_rows = 0;
    exec.parallel_min_cells = 0;
    exec.parallel_grain = 4'096;
    exec.parallel_stats = &stats;

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    const auto& root = std::get<Column<double>>(*updated->columns[1].column);
    CHECK(root[9] == Catch::Approx(3.0));
    CHECK(stats.chunk_direct_updates.load() == 0);
}
