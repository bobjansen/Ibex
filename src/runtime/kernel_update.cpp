// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_update.hpp"

#include <ibex/runtime/safe_arith.hpp>

#include <cmath>
#include <limits>
#include <type_traits>

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_gather.hpp"
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

struct NumericOperand {
    bool is_column = false;
    const ColumnValue* column = nullptr;
    ScalarValue scalar;
    ExprType kind = ExprType::Int;
};

auto resolve_numeric_operand(const ir::Expr& expr, const PredicateInput& input,
                             const ScalarRegistry* scalars) -> std::optional<NumericOperand> {
    if (const auto* ref = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (!ref->lexical) {
            if (const auto* entry = input.find(ref->name); entry != nullptr) {
                const ExprType kind = expr_type_for_column(*entry->column);
                if (kind == ExprType::Int || kind == ExprType::Double) {
                    return NumericOperand{.is_column = true,
                                          .column = entry->column.get(),
                                          .scalar = ScalarValue{},
                                          .kind = kind};
                }
                return std::nullopt;
            }
        }
        if (scalars != nullptr) {
            if (const auto it = scalars->find(ref->name); it != scalars->end()) {
                const ExprType kind = scalar_kind_from_value(it->second);
                if (kind == ExprType::Int || kind == ExprType::Double) {
                    return NumericOperand{
                        .is_column = false, .column = nullptr, .scalar = it->second, .kind = kind};
                }
            }
        }
        return std::nullopt;
    }
    if (const auto* literal = std::get_if<ir::Literal>(&expr.node)) {
        const ScalarValue scalar = scalar_from_literal(*literal);
        const ExprType kind = scalar_kind_from_value(scalar);
        if (kind == ExprType::Int || kind == ExprType::Double) {
            return NumericOperand{
                .is_column = false, .column = nullptr, .scalar = scalar, .kind = kind};
        }
    }
    return std::nullopt;
}

auto numeric_binary_operands(const ir::Expr& expr, const PredicateInput& input,
                             const ScalarRegistry* scalars)
    -> std::optional<std::pair<NumericOperand, NumericOperand>> {
    const auto* binary = std::get_if<ir::BinaryExpr>(&expr.node);
    if (binary == nullptr) {
        return std::nullopt;
    }
    auto left = resolve_numeric_operand(*binary->left, input, scalars);
    auto right = resolve_numeric_operand(*binary->right, input, scalars);
    if (!left.has_value() || !right.has_value()) {
        return std::nullopt;
    }
    return std::pair{std::move(*left), std::move(*right)};
}

auto numeric_int_value(const NumericOperand& operand) -> std::int64_t {
    return operand.kind == ExprType::Int
               ? std::get<std::int64_t>(operand.scalar)
               : static_cast<std::int64_t>(std::get<double>(operand.scalar));
}

auto numeric_double_value(const NumericOperand& operand) -> double {
    return operand.kind == ExprType::Int
               ? static_cast<double>(std::get<std::int64_t>(operand.scalar))
               : std::get<double>(operand.scalar);
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

/// A template string is the first computed variable-width update that can use
/// the same presized offsets/chars shape as string gather.  Keep the accepted
/// leaves deliberately narrow: string columns, string scalars, and literal
/// segments need no per-row formatting or allocation.  Other interpolation
/// arguments retain the reference evaluator below (notably numeric display
/// formatting and date/time formatting).
auto resolve_string_interpolation_operand(const ir::Expr& expr, const PredicateInput& input,
                                          const ScalarRegistry* scalars)
    -> std::optional<StringInterpolationOperand> {
    if (const auto* literal = std::get_if<ir::Literal>(&expr.node)) {
        if (const auto* value = std::get_if<std::string>(&literal->value)) {
            return StringInterpolationOperand{.column = std::nullopt, .literal = *value};
        }
        return std::nullopt;
    }
    const auto* ref = std::get_if<ir::ColumnRef>(&expr.node);
    if (ref == nullptr) {
        return std::nullopt;
    }
    if (!ref->lexical) {
        if (const auto* entry = input.find(ref->name); entry != nullptr) {
            if (const auto* values = std::get_if<Column<std::string>>(entry->column.get())) {
                return StringInterpolationOperand{
                    .column = StringView{.offsets = values->offsets_data(),
                                         .chars = values->chars_data(),
                                         .rows = values->size()},
                    .literal = {}};
            }
            return std::nullopt;
        }
    }
    if (scalars != nullptr) {
        if (const auto it = scalars->find(ref->name); it != scalars->end()) {
            if (const auto* value = std::get_if<std::string>(&it->second)) {
                return StringInterpolationOperand{.column = std::nullopt, .literal = *value};
            }
        }
    }
    return std::nullopt;
}

auto try_shared_string_interpolation_update(const Chunk& input,
                                            const std::vector<ir::FieldSpec>& fields,
                                            const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index()) {
        return std::nullopt;
    }
    const auto source = predicate_input(view);
    auto plan = make_string_interpolation_plan(fields.front().expr, source, scalars);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    auto validity = collect_expr_validity(fields.front().expr, source,
                                          ::ibex::runtime::RowRange::whole(view.rows()));

    // Pass one is precisely the string gather presize contract: establish the
    // single character slab before handing its raw window to the writer.
    const auto total_chars =
        string_interpolation_bytes(*plan, ::ibex::runtime::RowRange::whole(view.rows()),
                                   validity.has_value() ? &*validity : nullptr);
    if (!total_chars.has_value()) {
        return std::nullopt;
    }

    Column<std::string> values;
    values.resize_for_gather(view.rows(), *total_chars);
    StringOutputSpan output{.offsets = values.offsets_data(),
                            .chars = values.chars_data(),
                            .begin = 0,
                            .count = view.rows(),
                            .char_base = 0};
    output.offsets[0] = 0;
    if (!write_string_interpolation(*plan, ::ibex::runtime::RowRange::whole(view.rows()),
                                    validity.has_value() ? &*validity : nullptr, output)) {
        return std::nullopt;
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

/// The shared Table/Chunk binary writer owns values only; this Chunk adapter
/// supplies the transport-preserving result shape and the expression's merged
/// validity.  It intentionally runs before the older representation-specific
/// helpers below, which remain as a conservative fallback during the port.
auto try_shared_fixed_width_numeric_binary_update(const Chunk& input,
                                                  const std::vector<ir::FieldSpec>& fields,
                                                  const ScalarRegistry* scalars)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const ChunkView view(input);
    if (view.properties().time_index().has_value() &&
        fields.front().alias == *view.properties().time_index()) {
        return std::nullopt;
    }
    const auto source = predicate_input(view);
    const auto kind = fixed_width_numeric_binary_kind(fields.front().expr, source, scalars);
    if (!kind.has_value()) {
        return std::nullopt;
    }
    ColumnValue values;
    NumericOutputSpan output;
    if (*kind == FixedWidthNumericKind::Int) {
        Column<std::int64_t> ints;
        ints.resize_for_overwrite(view.rows());
        output.ints = ints.data();
        values = std::move(ints);
    } else {
        Column<double> doubles;
        doubles.resize_for_overwrite(view.rows());
        output.doubles = doubles.data();
        values = std::move(doubles);
    }
    if (!write_fixed_width_numeric_binary(fields.front().expr, source,
                                          ::ibex::runtime::RowRange::whole(view.rows()), scalars,
                                          *kind, output)) {
        return std::nullopt;
    }
    Chunk result = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{
        .name = fields.front().alias,
        .column = std::make_shared<ColumnValue>(std::move(values)),
        .validity = collect_expr_validity(fields.front().expr, source,
                                          ::ibex::runtime::RowRange::whole(view.rows()))};
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

auto make_string_interpolation_plan(const ir::Expr& expr, const PredicateInput& input,
                                    const ScalarRegistry* scalars)
    -> std::optional<StringInterpolationPlan> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || call->callee != "__interp" || !call->named_args.empty()) {
        return std::nullopt;
    }
    StringInterpolationPlan plan;
    plan.operands.reserve(call->args.size());
    for (const auto& arg : call->args) {
        auto operand = resolve_string_interpolation_operand(*arg, input, scalars);
        if (!operand.has_value()) {
            return std::nullopt;
        }
        plan.operands.push_back(std::move(*operand));
    }
    return plan;
}

auto string_interpolation_bytes(const StringInterpolationPlan& plan,
                                ::ibex::runtime::RowRange range, const ValidityBitmap* validity)
    -> std::optional<std::uint32_t> {
    std::size_t total = 0;
    for (std::size_t offset = 0; offset < range.count; ++offset) {
        if (validity != nullptr && !(*validity)[offset]) {
            continue;
        }
        const std::size_t row = range.begin + offset;
        std::size_t row_bytes = 0;
        for (const auto& operand : plan.operands) {
            row_bytes +=
                operand.column.has_value() ? operand.column->row_len(row) : operand.literal.size();
        }
        if (row_bytes > std::numeric_limits<std::uint32_t>::max() - total) {
            return std::nullopt;
        }
        total += row_bytes;
    }
    return static_cast<std::uint32_t>(total);
}

auto write_string_interpolation(const StringInterpolationPlan& plan,
                                ::ibex::runtime::RowRange range, const ValidityBitmap* validity,
                                StringOutputSpan output) -> bool {
    if (range.count != output.count) {
        return false;
    }
    std::uint32_t cursor = output.char_base;
    for (std::size_t offset = 0; offset < range.count; ++offset) {
        if (validity != nullptr && !(*validity)[offset]) {
            output.offsets[output.begin + offset + 1] = cursor;
            continue;
        }
        const std::size_t row = range.begin + offset;
        for (const auto& operand : plan.operands) {
            const char* source_chars = operand.literal.data();
            std::size_t length = operand.literal.size();
            if (operand.column.has_value()) {
                const auto start = operand.column->offsets[row];
                source_chars = operand.column->chars + start;
                length = operand.column->row_len(row);
            }
            if (length != 0) {
                std::memcpy(output.chars + cursor, source_chars, length);
            }
            cursor += static_cast<std::uint32_t>(length);
        }
        output.offsets[output.begin + offset + 1] = cursor;
    }
    return true;
}

auto fixed_width_numeric_binary_kind(const ir::Expr& expr, const PredicateInput& input,
                                     const ScalarRegistry* scalars)
    -> std::optional<FixedWidthNumericKind> {
    const auto* binary = std::get_if<ir::BinaryExpr>(&expr.node);
    const auto operands = numeric_binary_operands(expr, input, scalars);
    if (binary == nullptr || !operands.has_value()) {
        return std::nullopt;
    }
    return binary->op == ir::ArithmeticOp::Div || operands->first.kind == ExprType::Double ||
                   operands->second.kind == ExprType::Double
               ? FixedWidthNumericKind::Double
               : FixedWidthNumericKind::Int;
}

auto write_fixed_width_numeric_binary(const ir::Expr& expr, const PredicateInput& input,
                                      ::ibex::runtime::RowRange range,
                                      const ScalarRegistry* scalars,
                                      FixedWidthNumericKind output_kind, NumericOutputSpan output)
    -> bool {
    const auto* binary = std::get_if<ir::BinaryExpr>(&expr.node);
    const auto operands = numeric_binary_operands(expr, input, scalars);
    if (binary == nullptr || !operands.has_value() ||
        fixed_width_numeric_binary_kind(expr, input, scalars) != output_kind) {
        return false;
    }
    const auto& left = operands->first;
    const auto& right = operands->second;
    const std::size_t rows = range.count;
    const std::size_t begin = range.begin;
    if (output_kind == FixedWidthNumericKind::Double && output.doubles != nullptr) {
        const double* lp = left.is_column && left.kind == ExprType::Double
                               ? std::get<Column<double>>(*left.column).data() + begin
                               : nullptr;
        const double* rp = right.is_column && right.kind == ExprType::Double
                               ? std::get<Column<double>>(*right.column).data() + begin
                               : nullptr;
        auto write = [&](auto op) {
            if (lp != nullptr && rp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    output.doubles[i] = op(lp[i], rp[i]);
            } else if (lp != nullptr) {
                const double scalar = numeric_double_value(right);
                for (std::size_t i = 0; i < rows; ++i)
                    output.doubles[i] = op(lp[i], scalar);
            } else if (rp != nullptr) {
                const double scalar = numeric_double_value(left);
                for (std::size_t i = 0; i < rows; ++i)
                    output.doubles[i] = op(scalar, rp[i]);
            } else {
                const double lhs = numeric_double_value(left);
                const double rhs = numeric_double_value(right);
                for (std::size_t i = 0; i < rows; ++i)
                    output.doubles[i] = op(lhs, rhs);
            }
        };
        if ((!left.is_column || lp != nullptr) && (!right.is_column || rp != nullptr)) {
            switch (binary->op) {
                case ir::ArithmeticOp::Add:
                    write([](double a, double b) { return a + b; });
                    break;
                case ir::ArithmeticOp::Sub:
                    write([](double a, double b) { return a - b; });
                    break;
                case ir::ArithmeticOp::Mul:
                    write([](double a, double b) { return a * b; });
                    break;
                case ir::ArithmeticOp::Div:
                    write([](double a, double b) { return a / b; });
                    break;
                case ir::ArithmeticOp::Mod:
                    write([](double a, double b) { return std::fmod(a, b); });
                    break;
            }
            return true;
        }
        const auto read = [](const NumericOperand& operand, std::size_t row) {
            if (!operand.is_column)
                return numeric_double_value(operand);
            if (operand.kind == ExprType::Int) {
                return static_cast<double>(std::get<Column<std::int64_t>>(*operand.column)[row]);
            }
            return std::get<Column<double>>(*operand.column)[row];
        };
        for (std::size_t i = 0; i < rows; ++i) {
            const double lhs = read(left, begin + i);
            const double rhs = read(right, begin + i);
            switch (binary->op) {
                case ir::ArithmeticOp::Add:
                    output.doubles[i] = lhs + rhs;
                    break;
                case ir::ArithmeticOp::Sub:
                    output.doubles[i] = lhs - rhs;
                    break;
                case ir::ArithmeticOp::Mul:
                    output.doubles[i] = lhs * rhs;
                    break;
                case ir::ArithmeticOp::Div:
                    output.doubles[i] = lhs / rhs;
                    break;
                case ir::ArithmeticOp::Mod:
                    output.doubles[i] = std::fmod(lhs, rhs);
                    break;
            }
        }
        return true;
    }
    if (output_kind == FixedWidthNumericKind::Int && output.ints != nullptr) {
        const std::int64_t* lp =
            left.is_column ? std::get<Column<std::int64_t>>(*left.column).data() + begin : nullptr;
        const std::int64_t* rp = right.is_column
                                     ? std::get<Column<std::int64_t>>(*right.column).data() + begin
                                     : nullptr;
        const std::int64_t ls = left.is_column ? 0 : numeric_int_value(left);
        const std::int64_t rs = right.is_column ? 0 : numeric_int_value(right);
        auto write = [&](auto op) {
            if (lp != nullptr && rp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    output.ints[i] = op(lp[i], rp[i]);
            } else if (lp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    output.ints[i] = op(lp[i], rs);
            } else if (rp != nullptr) {
                for (std::size_t i = 0; i < rows; ++i)
                    output.ints[i] = op(ls, rp[i]);
            } else {
                for (std::size_t i = 0; i < rows; ++i)
                    output.ints[i] = op(ls, rs);
            }
        };
        switch (binary->op) {
            case ir::ArithmeticOp::Add:
                write([](std::int64_t a, std::int64_t b) { return a + b; });
                break;
            case ir::ArithmeticOp::Sub:
                write([](std::int64_t a, std::int64_t b) { return a - b; });
                break;
            case ir::ArithmeticOp::Mul:
                write([](std::int64_t a, std::int64_t b) { return a * b; });
                break;
            case ir::ArithmeticOp::Div:
                return false;
            case ir::ArithmeticOp::Mod:
                write([](std::int64_t a, std::int64_t b) { return safe_imod(a, b); });
                break;
        }
        return true;
    }
    return false;
}

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
        if (auto output = try_shared_string_interpolation_update(input, fields, scalars);
            output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_shared_fixed_width_numeric_binary_update(input, fields, scalars);
            output.has_value()) {
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
