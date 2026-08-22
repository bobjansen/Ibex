// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_update.hpp"

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_types.hpp"

namespace ibex::runtime::kernel {

namespace {

auto try_metadata_alias_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* source_ref = std::get_if<ir::ColumnRef>(&fields.front().expr.node);
    if (source_ref == nullptr || source_ref->lexical) {
        return std::nullopt;
    }

    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index()) {
        return std::nullopt;
    }
    const auto source_position = view.find_column(source_ref->name);
    if (!source_position.has_value()) {
        return std::nullopt;
    }

    std::vector<MappedChunkColumn> map;
    map.reserve(view.columns() + 1);
    bool replaced = false;
    for (std::size_t position = 0; position < view.columns(); ++position) {
        if (view.entry(position).name == fields.front().alias) {
            map.push_back({.source_position = *source_position, .name = fields.front().alias});
            replaced = true;
        } else {
            map.push_back({.source_position = position, .name = view.entry(position).name});
        }
    }
    if (!replaced) {
        map.push_back({.source_position = *source_position, .name = fields.front().alias});
    }
    const auto properties = TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == fields.front().alias ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve);
    return map_chunk(view, map, properties);
}

auto try_fixed_width_int_binary_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* binary = std::get_if<ir::BinaryExpr>(&fields.front().expr.node);
    if (binary == nullptr || binary->op == ir::ArithmeticOp::Div ||
        binary->op == ir::ArithmeticOp::Mod) {
        return std::nullopt;
    }
    const auto* left = std::get_if<ir::ColumnRef>(&binary->left->node);
    const auto* right = std::get_if<ir::ColumnRef>(&binary->right->node);
    if (left == nullptr || right == nullptr || left->lexical || right->lexical) {
        return std::nullopt;
    }

    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index()) {
        return std::nullopt;
    }
    const auto left_position = view.find_column(left->name);
    const auto right_position = view.find_column(right->name);
    if (!left_position.has_value() || !right_position.has_value() ||
        view.validity(*left_position).has_value() || view.validity(*right_position).has_value() ||
        !std::holds_alternative<Column<std::int64_t>>(view.column(*left_position)) ||
        !std::holds_alternative<Column<std::int64_t>>(view.column(*right_position))) {
        return std::nullopt;
    }

    const auto lhs = view.view<std::int64_t>(*left_position);
    const auto rhs = view.view<std::int64_t>(*right_position);
    Column<std::int64_t> values;
    values.resize_for_overwrite(view.rows());
    std::int64_t* output = values.data();
    switch (binary->op) {
        case ir::ArithmeticOp::Add:
            for (std::size_t row = 0; row < view.rows(); ++row)
                output[row] = lhs.value(row) + rhs.value(row);
            break;
        case ir::ArithmeticOp::Sub:
            for (std::size_t row = 0; row < view.rows(); ++row)
                output[row] = lhs.value(row) - rhs.value(row);
            break;
        case ir::ArithmeticOp::Mul:
            for (std::size_t row = 0; row < view.rows(); ++row)
                output[row] = lhs.value(row) * rhs.value(row);
            break;
        case ir::ArithmeticOp::Mod:
        case ir::ArithmeticOp::Div:
            return std::nullopt;
    }

    Chunk result = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(std::move(values)),
                            .validity = std::nullopt};
    if (existing.has_value()) {
        result.columns[*existing] = entry;
    } else {
        result.columns.push_back(entry);
    }
    result.set_properties(TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == fields.front().alias ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve));
    return result;
}

}  // namespace

auto update_row_local_chunk(Chunk input, const std::vector<ir::FieldSpec>& fields,
                            const ScalarRegistry* scalars, const ExternRegistry* externs,
                            const ExecutionContext& exec) -> std::expected<Chunk, std::string> {
    if (auto output = try_metadata_alias_update(input, fields); output.has_value()) {
        return std::move(*output);
    }
    if (auto output = try_fixed_width_int_binary_update(input, fields); output.has_value()) {
        return std::move(*output);
    }
    const std::uint64_t sequence = input.sequence;
    const std::size_t row_offset = input.row_offset;
    auto output = update_table(chunk_to_table(std::move(input)), fields, scalars, externs, exec);
    if (!output.has_value()) {
        return std::unexpected(std::move(output.error()));
    }
    Chunk chunk = table_to_chunk(std::move(output.value()));
    chunk.sequence = sequence;
    chunk.row_offset = row_offset;
    return chunk;
}

}  // namespace ibex::runtime::kernel
