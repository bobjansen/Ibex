// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_update.hpp"

#include <ibex/runtime/safe_arith.hpp>

#include <cmath>
#include <type_traits>

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"

namespace ibex::runtime::kernel {

namespace {

auto predicate_input(const ChunkView& view) -> PredicateInput {
    return PredicateInput{
        &view,
        [](const void* state) noexcept { return static_cast<const ChunkView*>(state)->rows(); },
        [](const void* state, const std::string& name) noexcept -> const ColumnEntry* {
            const auto& input = *static_cast<const ChunkView*>(state);
            const auto position = input.find_column(name);
            return position.has_value() ? &input.entry(*position) : nullptr;
        }};
}

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

auto try_native_bool_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                            const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1 || !supports_native_chunk_predicate(fields.front().expr)) {
        return std::nullopt;
    }
    const bool is_boolean = std::holds_alternative<ir::CompareExpr>(fields.front().expr.node) ||
                            std::holds_alternative<ir::LogicalExpr>(fields.front().expr.node) ||
                            std::holds_alternative<ir::IsNullExpr>(fields.front().expr.node);
    if (!is_boolean) {
        return std::nullopt;
    }
    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index()) {
        return std::nullopt;
    }
    auto mask = compute_mask(fields.front().expr, predicate_input(view), scalars,
                             ::ibex::runtime::RowRange::whole(view.rows()));
    if (!mask.has_value()) {
        return std::nullopt;
    }
    Column<bool> values;
    values.resize(view.rows());
    for (std::size_t row = 0; row < view.rows(); ++row) {
        values.set(row, mask->value[row] != 0);
    }
    std::optional<ValidityBitmap> validity;
    if (mask->valid.has_value()) {
        ValidityBitmap bits(view.rows(), false);
        for (std::size_t row = 0; row < view.rows(); ++row) {
            bits.set(row, (*mask->valid)[row] != 0);
        }
        validity = std::move(bits);
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
    if (binary == nullptr)
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
    if (!left_position.has_value() || !right_position.has_value())
        return std::nullopt;

    std::optional<Chunk> result;
    std::visit(
        [&](const auto& left_values) {
            using L = typename std::decay_t<decltype(left_values)>::value_type;
            if constexpr (std::is_same_v<L, std::int64_t> || std::is_same_v<L, double>) {
                std::visit(
                    [&](const auto& right_values) {
                        using R = typename std::decay_t<decltype(right_values)>::value_type;
                        if constexpr (std::is_same_v<R, std::int64_t> ||
                                      std::is_same_v<R, double>) {
                            // Int64/Int64 was already claimed by the exact-width
                            // kernels above. Any pair reaching this visitor has
                            // a Double input and therefore a Double result.
                            if constexpr (std::is_same_v<L, std::int64_t> &&
                                          std::is_same_v<R, std::int64_t>) {
                                return;
                            }
                            const ColumnView<L> lhs(left_values,
                                                    view.validity(*left_position).has_value()
                                                        ? &*view.validity(*left_position)
                                                        : nullptr);
                            const ColumnView<R> rhs(right_values,
                                                    view.validity(*right_position).has_value()
                                                        ? &*view.validity(*right_position)
                                                        : nullptr);
                            Column<double> values;
                            values.resize_for_overwrite(view.rows());
                            double* output = values.data();
                            for (std::size_t row = 0; row < view.rows(); ++row) {
                                const double l = static_cast<double>(lhs.value(row));
                                const double r = static_cast<double>(rhs.value(row));
                                switch (binary->op) {
                                    case ir::ArithmeticOp::Add:
                                        output[row] = l + r;
                                        break;
                                    case ir::ArithmeticOp::Sub:
                                        output[row] = l - r;
                                        break;
                                    case ir::ArithmeticOp::Mul:
                                        output[row] = l * r;
                                        break;
                                    case ir::ArithmeticOp::Div:
                                        output[row] = l / r;
                                        break;
                                    case ir::ArithmeticOp::Mod:
                                        output[row] = std::fmod(l, r);
                                        break;
                                }
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
                                if (any_invalid)
                                    validity = std::move(bits);
                            }
                            Chunk output_chunk = input;
                            const auto existing = view.find_column(fields.front().alias);
                            const ColumnEntry entry{
                                .name = fields.front().alias,
                                .column = std::make_shared<ColumnValue>(std::move(values)),
                                .validity = std::move(validity)};
                            if (existing.has_value())
                                output_chunk.columns[*existing] = entry;
                            else
                                output_chunk.columns.push_back(entry);
                            output_chunk.set_properties(TableProperties::derive(
                                view.properties(),
                                [&](const std::string& name) -> KeyFate {
                                    return name == fields.front().alias ? KeyFate::overwritten()
                                                                        : KeyFate::kept(name);
                                },
                                RowTransform::Preserve));
                            result = std::move(output_chunk);
                        }
                    },
                    view.column(*right_position));
            }
        },
        view.column(*left_position));
    return result;
}

auto try_fixed_width_double_literal_update(const Chunk& input,
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
    if (!source_position.has_value())
        return std::nullopt;

    std::optional<Chunk> result;
    std::visit(
        [&](const auto& source_values) {
            using T = typename std::decay_t<decltype(source_values)>::value_type;
            if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>) {
                // The exact Int64 kernel above owns Int64-with-Int64. This
                // route covers any expression involving a Double instead.
                if constexpr (std::is_same_v<T, std::int64_t>) {
                    if (double_scalar == nullptr) {
                        return;
                    }
                }
                const ColumnView<T> source(source_values,
                                           view.validity(*source_position).has_value()
                                               ? &*view.validity(*source_position)
                                               : nullptr);
                Column<double> values;
                values.resize_for_overwrite(view.rows());
                double* output = values.data();
                for (std::size_t row = 0; row < view.rows(); ++row) {
                    const double value = static_cast<double>(source.value(row));
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
                            output[row] =
                                column_left ? std::fmod(value, scalar) : std::fmod(scalar, value);
                            break;
                    }
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
                    if (any_invalid)
                        validity = std::move(bits);
                }
                Chunk output_chunk = input;
                const auto existing = view.find_column(fields.front().alias);
                const ColumnEntry entry{.name = fields.front().alias,
                                        .column = std::make_shared<ColumnValue>(std::move(values)),
                                        .validity = std::move(validity)};
                if (existing.has_value())
                    output_chunk.columns[*existing] = entry;
                else
                    output_chunk.columns.push_back(entry);
                output_chunk.set_properties(TableProperties::derive(
                    view.properties(),
                    [&](const std::string& name) -> KeyFate {
                        return name == fields.front().alias ? KeyFate::overwritten()
                                                            : KeyFate::kept(name);
                    },
                    RowTransform::Preserve));
                result = std::move(output_chunk);
            }
        },
        view.column(*source_position));
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
        if (auto output = try_native_bool_update(input, fields, scalars); output.has_value()) {
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
