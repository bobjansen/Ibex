// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_update.hpp"

#include <ibex/runtime/safe_arith.hpp>

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

auto try_literal_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* literal = std::get_if<ir::Literal>(&fields.front().expr.node);
    if (literal == nullptr) {
        return std::nullopt;
    }

    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index()) {
        return std::nullopt;
    }

    Chunk result = input;
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(broadcast_scalar_column(
                                scalar_from_literal(*literal), view.rows())),
                            .validity = std::nullopt};
    if (const auto existing = view.find_column(fields.front().alias); existing.has_value()) {
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

auto try_fixed_width_int_binary_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* binary = std::get_if<ir::BinaryExpr>(&fields.front().expr.node);
    if (binary == nullptr) {
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
        !std::holds_alternative<Column<std::int64_t>>(view.column(*left_position)) ||
        !std::holds_alternative<Column<std::int64_t>>(view.column(*right_position))) {
        return std::nullopt;
    }

    const auto lhs = view.view<std::int64_t>(*left_position);
    const auto rhs = view.view<std::int64_t>(*right_position);
    if (binary->op == ir::ArithmeticOp::Div) {
        Column<double> values;
        values.resize_for_overwrite(view.rows());
        double* output = values.data();
        for (std::size_t row = 0; row < view.rows(); ++row) {
            output[row] = static_cast<double>(lhs.value(row)) / static_cast<double>(rhs.value(row));
        }
        std::optional<ValidityBitmap> validity;
        if (view.validity(*left_position).has_value() ||
            view.validity(*right_position).has_value()) {
            ValidityBitmap bits(view.rows(), true);
            bool any_invalid = false;
            for (std::size_t row = 0; row < view.rows(); ++row) {
                const bool valid = lhs.is_valid(row) && rhs.is_valid(row);
                bits.set(row, valid);
                any_invalid = any_invalid || !valid;
            }
            if (any_invalid) {
                validity = std::move(bits);
            }
        }
        Chunk result = input;
        const auto existing = view.find_column(fields.front().alias);
        const ColumnEntry entry{.name = fields.front().alias,
                                .column = std::make_shared<ColumnValue>(std::move(values)),
                                .validity = std::move(validity)};
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
        case ir::ArithmeticOp::Div:
            invariant_violation("Int64 division was handled before the Int64 output kernel");
        case ir::ArithmeticOp::Mod:
            for (std::size_t row = 0; row < view.rows(); ++row)
                output[row] = safe_imod(lhs.value(row), rhs.value(row));
            break;
    }

    std::optional<ValidityBitmap> validity;
    if (view.validity(*left_position).has_value() || view.validity(*right_position).has_value()) {
        ValidityBitmap bits(view.rows(), true);
        bool any_invalid = false;
        for (std::size_t row = 0; row < view.rows(); ++row) {
            const bool valid = lhs.is_valid(row) && rhs.is_valid(row);
            bits.set(row, valid);
            any_invalid = any_invalid || !valid;
        }
        if (any_invalid) {
            validity = std::move(bits);
        }
    }

    Chunk result = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(std::move(values)),
                            .validity = std::move(validity)};
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

auto try_fixed_width_int_literal_update(const Chunk& input,
                                        const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1)
        return std::nullopt;
    const auto* binary = std::get_if<ir::BinaryExpr>(&fields.front().expr.node);
    if (binary == nullptr)
        return std::nullopt;
    const auto* left_column = std::get_if<ir::ColumnRef>(&binary->left->node);
    const auto* right_column = std::get_if<ir::ColumnRef>(&binary->right->node);
    const auto* left_literal = std::get_if<ir::Literal>(&binary->left->node);
    const auto* right_literal = std::get_if<ir::Literal>(&binary->right->node);
    const bool column_left = left_column != nullptr && right_literal != nullptr;
    const bool column_right = right_column != nullptr && left_literal != nullptr;
    if ((!column_left && !column_right) || (column_left && left_column->lexical) ||
        (column_right && right_column->lexical))
        return std::nullopt;
    const auto* literal = column_left ? right_literal : left_literal;
    const auto* scalar = std::get_if<std::int64_t>(&literal->value);
    if (scalar == nullptr)
        return std::nullopt;

    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index())
        return std::nullopt;
    const auto source_position =
        view.find_column(column_left ? left_column->name : right_column->name);
    if (!source_position.has_value() ||
        !std::holds_alternative<Column<std::int64_t>>(view.column(*source_position)))
        return std::nullopt;
    const auto source = view.view<std::int64_t>(*source_position);
    if (binary->op == ir::ArithmeticOp::Div) {
        Column<double> values;
        values.resize_for_overwrite(view.rows());
        double* output = values.data();
        for (std::size_t row = 0; row < view.rows(); ++row) {
            const double value = static_cast<double>(source.value(row));
            const double divisor = static_cast<double>(*scalar);
            output[row] = column_left ? value / divisor : divisor / value;
        }
        std::optional<ValidityBitmap> validity;
        if (view.validity(*source_position).has_value()) {
            ValidityBitmap bits(view.rows(), true);
            bool any_invalid = false;
            for (std::size_t row = 0; row < view.rows(); ++row) {
                const bool valid = source.is_valid(row);
                bits.set(row, valid);
                any_invalid = any_invalid || !valid;
            }
            if (any_invalid) {
                validity = std::move(bits);
            }
        }
        Chunk result = input;
        const auto existing = view.find_column(fields.front().alias);
        const ColumnEntry entry{.name = fields.front().alias,
                                .column = std::make_shared<ColumnValue>(std::move(values)),
                                .validity = std::move(validity)};
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
    Column<std::int64_t> values;
    values.resize_for_overwrite(view.rows());
    std::int64_t* output = values.data();
    for (std::size_t row = 0; row < view.rows(); ++row) {
        const std::int64_t value = source.value(row);
        switch (binary->op) {
            case ir::ArithmeticOp::Add:
                output[row] = column_left ? value + *scalar : *scalar + value;
                break;
            case ir::ArithmeticOp::Sub:
                output[row] = column_left ? value - *scalar : *scalar - value;
                break;
            case ir::ArithmeticOp::Mul:
                output[row] = value * *scalar;
                break;
            case ir::ArithmeticOp::Div:
                invariant_violation("Int64 division was handled before the Int64 output kernel");
            case ir::ArithmeticOp::Mod:
                output[row] = column_left ? safe_imod(value, *scalar) : safe_imod(*scalar, value);
                break;
        }
    }
    std::optional<ValidityBitmap> validity;
    if (view.validity(*source_position).has_value()) {
        ValidityBitmap bits(view.rows(), true);
        bool any_invalid = false;
        for (std::size_t row = 0; row < view.rows(); ++row) {
            bits.set(row, source.is_valid(row));
            any_invalid = any_invalid || !source.is_valid(row);
        }
        if (any_invalid)
            validity = std::move(bits);
    }
    Chunk result = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(std::move(values)),
                            .validity = std::move(validity)};
    if (existing.has_value())
        result.columns[*existing] = entry;
    else
        result.columns.push_back(entry);
    result.set_properties(TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == fields.front().alias ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve));
    return result;
}

auto try_fixed_width_double_binary_update(const Chunk& input,
                                          const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1)
        return std::nullopt;
    const auto* binary = std::get_if<ir::BinaryExpr>(&fields.front().expr.node);
    if (binary == nullptr || binary->op == ir::ArithmeticOp::Mod)
        return std::nullopt;
    const auto* left = std::get_if<ir::ColumnRef>(&binary->left->node);
    const auto* right = std::get_if<ir::ColumnRef>(&binary->right->node);
    if (left == nullptr || right == nullptr || left->lexical || right->lexical)
        return std::nullopt;
    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index())
        return std::nullopt;
    const auto left_position = view.find_column(left->name);
    const auto right_position = view.find_column(right->name);
    if (!left_position.has_value() || !right_position.has_value() ||
        !std::holds_alternative<Column<double>>(view.column(*left_position)) ||
        !std::holds_alternative<Column<double>>(view.column(*right_position)))
        return std::nullopt;

    const auto lhs = view.view<double>(*left_position);
    const auto rhs = view.view<double>(*right_position);
    Column<double> values;
    values.resize_for_overwrite(view.rows());
    double* output = values.data();
    for (std::size_t row = 0; row < view.rows(); ++row) {
        switch (binary->op) {
            case ir::ArithmeticOp::Add:
                output[row] = lhs.value(row) + rhs.value(row);
                break;
            case ir::ArithmeticOp::Sub:
                output[row] = lhs.value(row) - rhs.value(row);
                break;
            case ir::ArithmeticOp::Mul:
                output[row] = lhs.value(row) * rhs.value(row);
                break;
            case ir::ArithmeticOp::Div:
                output[row] = lhs.value(row) / rhs.value(row);
                break;
            case ir::ArithmeticOp::Mod:
                return std::nullopt;
        }
    }
    std::optional<ValidityBitmap> validity;
    if (view.validity(*left_position).has_value() || view.validity(*right_position).has_value()) {
        ValidityBitmap bits(view.rows(), true);
        bool any_invalid = false;
        for (std::size_t row = 0; row < view.rows(); ++row) {
            const bool valid = lhs.is_valid(row) && rhs.is_valid(row);
            bits.set(row, valid);
            any_invalid = any_invalid || !valid;
        }
        if (any_invalid)
            validity = std::move(bits);
    }
    Chunk result = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(std::move(values)),
                            .validity = std::move(validity)};
    if (existing.has_value())
        result.columns[*existing] = entry;
    else
        result.columns.push_back(entry);
    result.set_properties(TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == fields.front().alias ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve));
    return result;
}

auto try_fixed_width_double_literal_update(const Chunk& input,
                                           const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1)
        return std::nullopt;
    const auto* binary = std::get_if<ir::BinaryExpr>(&fields.front().expr.node);
    if (binary == nullptr || binary->op == ir::ArithmeticOp::Mod)
        return std::nullopt;
    const auto* left_column = std::get_if<ir::ColumnRef>(&binary->left->node);
    const auto* right_column = std::get_if<ir::ColumnRef>(&binary->right->node);
    const auto* left_literal = std::get_if<ir::Literal>(&binary->left->node);
    const auto* right_literal = std::get_if<ir::Literal>(&binary->right->node);
    const bool column_left = left_column != nullptr && right_literal != nullptr;
    const bool column_right = right_column != nullptr && left_literal != nullptr;
    if ((!column_left && !column_right) || (column_left && left_column->lexical) ||
        (column_right && right_column->lexical))
        return std::nullopt;
    const auto* literal = column_left ? right_literal : left_literal;
    const double* double_scalar = std::get_if<double>(&literal->value);
    const std::int64_t* int_scalar = std::get_if<std::int64_t>(&literal->value);
    if (double_scalar == nullptr && int_scalar == nullptr)
        return std::nullopt;
    const double scalar =
        double_scalar != nullptr ? *double_scalar : static_cast<double>(*int_scalar);

    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index())
        return std::nullopt;
    const auto source_position =
        view.find_column(column_left ? left_column->name : right_column->name);
    if (!source_position.has_value() ||
        !std::holds_alternative<Column<double>>(view.column(*source_position)))
        return std::nullopt;
    const auto source = view.view<double>(*source_position);
    Column<double> values;
    values.resize_for_overwrite(view.rows());
    double* output = values.data();
    for (std::size_t row = 0; row < view.rows(); ++row) {
        const double value = source.value(row);
        switch (binary->op) {
            case ir::ArithmeticOp::Add:
                output[row] = column_left ? value + scalar : scalar + value;
                break;
            case ir::ArithmeticOp::Sub:
                output[row] = column_left ? value - scalar : scalar - value;
                break;
            case ir::ArithmeticOp::Mul:
                output[row] = value * scalar;
                break;
            case ir::ArithmeticOp::Div:
                output[row] = column_left ? value / scalar : scalar / value;
                break;
            case ir::ArithmeticOp::Mod:
                return std::nullopt;
        }
    }
    std::optional<ValidityBitmap> validity;
    if (view.validity(*source_position).has_value()) {
        ValidityBitmap bits(view.rows(), true);
        bool any_invalid = false;
        for (std::size_t row = 0; row < view.rows(); ++row) {
            bits.set(row, source.is_valid(row));
            any_invalid = any_invalid || !source.is_valid(row);
        }
        if (any_invalid)
            validity = std::move(bits);
    }
    Chunk result = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(std::move(values)),
                            .validity = std::move(validity)};
    if (existing.has_value())
        result.columns[*existing] = entry;
    else
        result.columns.push_back(entry);
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
    if (!exec.parallel) {
        if (auto output = try_literal_update(input, fields); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_fixed_width_int_binary_update(input, fields); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_fixed_width_int_literal_update(input, fields); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_fixed_width_double_binary_update(input, fields); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_fixed_width_double_literal_update(input, fields);
            output.has_value()) {
            return std::move(*output);
        }
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
