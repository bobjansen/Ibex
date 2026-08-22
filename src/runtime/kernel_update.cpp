// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_update.hpp"

#include <ibex/runtime/safe_arith.hpp>
#include <ibex/runtime/table_format.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string_view>
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

using UnaryDoubleFn = double (*)(double);

auto lookup_unary_double(std::string_view name) -> UnaryDoubleFn {
    if (name == "sqrt")
        return [](double value) { return std::sqrt(value); };
    if (name == "log")
        return [](double value) { return std::log(value); };
    if (name == "exp")
        return [](double value) { return std::exp(value); };
    if (name == "log2")
        return [](double value) { return std::log2(value); };
    if (name == "log10")
        return [](double value) { return std::log10(value); };
    if (name == "sin")
        return [](double value) { return std::sin(value); };
    if (name == "cos")
        return [](double value) { return std::cos(value); };
    if (name == "tan")
        return [](double value) { return std::tan(value); };
    if (name == "asin")
        return [](double value) { return std::asin(value); };
    if (name == "acos")
        return [](double value) { return std::acos(value); };
    if (name == "atan")
        return [](double value) { return std::atan(value); };
    if (name == "sinh")
        return [](double value) { return std::sinh(value); };
    if (name == "cosh")
        return [](double value) { return std::cosh(value); };
    if (name == "tanh")
        return [](double value) { return std::tanh(value); };
    if (name == "abs")
        return [](double value) { return std::fabs(value); };
    if (name == "floor")
        return [](double value) { return std::floor(value); };
    if (name == "ceil")
        return [](double value) { return std::ceil(value); };
    if (name == "trunc")
        return [](double value) { return std::trunc(value); };
    return nullptr;
}

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

struct NumericTreeNode {
    enum class Kind : std::uint8_t {
        IntColumn,
        DoubleColumn,
        IntScalar,
        DoubleScalar,
        Binary,
        Min,
        Max,
        Unary
    };

    Kind kind = Kind::IntScalar;
    ExprType type = ExprType::Int;
    ir::ArithmeticOp op = ir::ArithmeticOp::Add;
    std::uint32_t left = 0;
    std::uint32_t right = 0;
    const std::int64_t* ints = nullptr;
    const double* doubles = nullptr;
    std::int64_t int_scalar = 0;
    double double_scalar = 0.0;
    UnaryDoubleFn unary = nullptr;
};

auto compile_numeric_tree(const ir::Expr& expr, const PredicateInput& input,
                          const ScalarRegistry* scalars, std::vector<NumericTreeNode>& nodes)
    -> std::optional<std::uint32_t> {
    if (const auto* binary = std::get_if<ir::BinaryExpr>(&expr.node)) {
        const auto left = compile_numeric_tree(*binary->left, input, scalars, nodes);
        const auto right = compile_numeric_tree(*binary->right, input, scalars, nodes);
        if (!left.has_value() || !right.has_value()) {
            return std::nullopt;
        }
        const ExprType type = binary->op == ir::ArithmeticOp::Div ||
                                      nodes[*left].type == ExprType::Double ||
                                      nodes[*right].type == ExprType::Double
                                  ? ExprType::Double
                                  : ExprType::Int;
        nodes.push_back({.kind = NumericTreeNode::Kind::Binary,
                         .type = type,
                         .op = binary->op,
                         .left = *left,
                         .right = *right});
        return static_cast<std::uint32_t>(nodes.size() - 1);
    }
    if (const auto* call = std::get_if<ir::CallExpr>(&expr.node)) {
        if ((call->callee == "pmin" || call->callee == "pmax") && call->args.size() >= 2 &&
            call->named_args.empty()) {
            auto accumulated = compile_numeric_tree(*call->args.front(), input, scalars, nodes);
            if (!accumulated.has_value()) {
                return std::nullopt;
            }
            for (std::size_t arg = 1; arg < call->args.size(); ++arg) {
                const auto next = compile_numeric_tree(*call->args[arg], input, scalars, nodes);
                if (!next.has_value()) {
                    return std::nullopt;
                }
                const ExprType type = nodes[*accumulated].type == ExprType::Double ||
                                              nodes[*next].type == ExprType::Double
                                          ? ExprType::Double
                                          : ExprType::Int;
                nodes.push_back({.kind = call->callee == "pmin" ? NumericTreeNode::Kind::Min
                                                                : NumericTreeNode::Kind::Max,
                                 .type = type,
                                 .left = *accumulated,
                                 .right = *next});
                accumulated = static_cast<std::uint32_t>(nodes.size() - 1);
            }
            return accumulated;
        }
        if (call->args.size() == 1 && call->named_args.empty()) {
            const UnaryDoubleFn unary = lookup_unary_double(call->callee);
            if (unary != nullptr) {
                const auto child = compile_numeric_tree(*call->args.front(), input, scalars, nodes);
                if (!child.has_value() || ((call->callee == "abs" || call->callee == "floor" ||
                                            call->callee == "ceil" || call->callee == "trunc") &&
                                           nodes[*child].type != ExprType::Double)) {
                    return std::nullopt;
                }
                nodes.push_back({.kind = NumericTreeNode::Kind::Unary,
                                 .type = ExprType::Double,
                                 .left = *child,
                                 .unary = unary});
                return static_cast<std::uint32_t>(nodes.size() - 1);
            }
        }
    }
    const auto operand = resolve_numeric_operand(expr, input, scalars);
    if (!operand.has_value()) {
        return std::nullopt;
    }
    NumericTreeNode node{.type = operand->kind};
    if (operand->is_column) {
        if (operand->kind == ExprType::Int) {
            node.kind = NumericTreeNode::Kind::IntColumn;
            node.ints = std::get<Column<std::int64_t>>(*operand->column).data();
        } else {
            node.kind = NumericTreeNode::Kind::DoubleColumn;
            node.doubles = std::get<Column<double>>(*operand->column).data();
        }
    } else if (operand->kind == ExprType::Int) {
        node.kind = NumericTreeNode::Kind::IntScalar;
        node.int_scalar = numeric_int_value(*operand);
    } else {
        node.kind = NumericTreeNode::Kind::DoubleScalar;
        node.double_scalar = numeric_double_value(*operand);
    }
    nodes.push_back(node);
    return static_cast<std::uint32_t>(nodes.size() - 1);
}

auto try_numeric_tree_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
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
    std::vector<NumericTreeNode> nodes;
    nodes.reserve(8);
    const auto root = compile_numeric_tree(fields.front().expr, source, scalars, nodes);
    if (!root.has_value()) {
        return std::nullopt;
    }
    const auto eval_double = [&](auto&& self, std::uint32_t index, std::size_t row) -> double {
        const auto& node = nodes[index];
        switch (node.kind) {
            case NumericTreeNode::Kind::IntColumn:
                return static_cast<double>(node.ints[row]);
            case NumericTreeNode::Kind::DoubleColumn:
                return node.doubles[row];
            case NumericTreeNode::Kind::IntScalar:
                return static_cast<double>(node.int_scalar);
            case NumericTreeNode::Kind::DoubleScalar:
                return node.double_scalar;
            case NumericTreeNode::Kind::Min:
                return std::min(self(self, node.left, row), self(self, node.right, row));
            case NumericTreeNode::Kind::Max:
                return std::max(self(self, node.left, row), self(self, node.right, row));
            case NumericTreeNode::Kind::Unary:
                return node.unary(self(self, node.left, row));
            case NumericTreeNode::Kind::Binary: {
                const double left = self(self, node.left, row);
                const double right = self(self, node.right, row);
                switch (node.op) {
                    case ir::ArithmeticOp::Add:
                        return left + right;
                    case ir::ArithmeticOp::Sub:
                        return left - right;
                    case ir::ArithmeticOp::Mul:
                        return left * right;
                    case ir::ArithmeticOp::Div:
                        return left / right;
                    case ir::ArithmeticOp::Mod:
                        return std::fmod(left, right);
                }
            }
        }
        invariant_violation("numeric tree: unhandled double node");
    };
    const auto eval_int = [&](auto&& self, std::uint32_t index, std::size_t row) -> std::int64_t {
        const auto& node = nodes[index];
        switch (node.kind) {
            case NumericTreeNode::Kind::IntColumn:
                return node.ints[row];
            case NumericTreeNode::Kind::IntScalar:
                return node.int_scalar;
            case NumericTreeNode::Kind::Min:
                return std::min(self(self, node.left, row), self(self, node.right, row));
            case NumericTreeNode::Kind::Max:
                return std::max(self(self, node.left, row), self(self, node.right, row));
            case NumericTreeNode::Kind::Binary: {
                const std::int64_t left = self(self, node.left, row);
                const std::int64_t right = self(self, node.right, row);
                switch (node.op) {
                    case ir::ArithmeticOp::Add:
                        return left + right;
                    case ir::ArithmeticOp::Sub:
                        return left - right;
                    case ir::ArithmeticOp::Mul:
                        return left * right;
                    case ir::ArithmeticOp::Div:
                        invariant_violation("numeric tree: Int division widens to Double");
                    case ir::ArithmeticOp::Mod:
                        return safe_imod(left, right);
                }
                return 0;  // exhaustive switch; keeps strict compilers aware.
            }
            case NumericTreeNode::Kind::DoubleColumn:
            case NumericTreeNode::Kind::DoubleScalar:
            case NumericTreeNode::Kind::Unary:
                invariant_violation("numeric tree: Double node in Int expression");
        }
        invariant_violation("numeric tree: unhandled Int node");
    };

    ColumnValue values;
    if (nodes[*root].type == ExprType::Int) {
        Column<std::int64_t> output;
        output.resize_for_overwrite(view.rows());
        for (std::size_t row = 0; row < view.rows(); ++row) {
            output.data()[row] = eval_int(eval_int, *root, row);
        }
        values = std::move(output);
    } else {
        Column<double> output;
        output.resize_for_overwrite(view.rows());
        for (std::size_t row = 0; row < view.rows(); ++row) {
            output.data()[row] = eval_double(eval_double, *root, row);
        }
        values = std::move(output);
    }
    Chunk output = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{
        .name = fields.front().alias,
        .column = std::make_shared<ColumnValue>(std::move(values)),
        .validity = collect_expr_validity(fields.front().expr, source,
                                          ::ibex::runtime::RowRange::whole(view.rows()))};
    if (existing.has_value()) {
        output.columns[*existing] = entry;
    } else {
        output.columns.push_back(entry);
    }
    output.set_properties(TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == fields.front().alias ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve));
    return output;
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

/// Whole-column unary Double transforms are a useful next step beyond binary
/// arithmetic: they are common in row-local map pipelines and have the same
/// simple null contract as their source.  Keep the accepted shape deliberately
/// narrow.  In particular, the type-preserving Int forms (abs/floor/ceil/trunc)
/// remain with the evaluator, whose overflow and inferred-type rules are the
/// authority for those calls.
auto utf8_codepoint_count(std::string_view text) noexcept -> std::int64_t {
    std::int64_t count = 0;
    for (std::size_t offset = 0; offset < text.size();) {
        const auto lead = static_cast<unsigned char>(text[offset]);
        std::size_t advance = 1;
        if (lead >= 0xF0U) {
            advance = 4;
        } else if (lead >= 0xE0U) {
            advance = 3;
        } else if (lead >= 0xC0U) {
            advance = 2;
        }
        offset += std::min(advance, text.size() - offset);
        ++count;
    }
    return count;
}

/// `length` and `byte_length` are the string representation's equivalent of
/// a fixed-width whole-column transform.  Categorical inputs count each
/// dictionary value once before reading its flat code stream; slab strings
/// read their offsets directly.  The evaluator remains responsible for
/// literals and computed-string arguments.
auto try_string_length_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* call = std::get_if<ir::CallExpr>(&fields.front().expr.node);
    if (call == nullptr || call->args.size() != 1 || !call->named_args.empty() ||
        (call->callee != "length" && call->callee != "byte_length")) {
        return std::nullopt;
    }
    const auto* source_ref = std::get_if<ir::ColumnRef>(&call->args.front()->node);
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
    const bool bytes = call->callee == "byte_length";
    const auto measure = [bytes](std::string_view text) -> std::int64_t {
        return bytes ? static_cast<std::int64_t>(text.size()) : utf8_codepoint_count(text);
    };

    Column<std::int64_t> values;
    values.resize_for_overwrite(view.rows());
    bool handled = false;
    if (const auto* strings = std::get_if<Column<std::string>>(&view.column(*source_position))) {
        const StringView source{.offsets = strings->offsets_data(),
                                .chars = strings->chars_data(),
                                .rows = strings->size()};
        for (std::size_t row = 0; row < source.rows; ++row) {
            values.data()[row] =
                measure(std::string_view{source.chars + source.offsets[row], source.row_len(row)});
        }
        handled = true;
    } else if (const auto* categorical =
                   std::get_if<Column<Categorical>>(&view.column(*source_position))) {
        const auto& dictionary = categorical->dictionary();
        std::vector<std::int64_t> dictionary_lengths(dictionary.size());
        for (std::size_t code = 0; code < dictionary.size(); ++code) {
            dictionary_lengths[code] = measure(dictionary[code]);
        }
        const auto* codes = categorical->codes_data();
        for (std::size_t row = 0; row < view.rows(); ++row) {
            values.data()[row] = dictionary_lengths[static_cast<std::size_t>(codes[row])];
        }
        handled = true;
    }
    if (!handled) {
        return std::nullopt;
    }

    Chunk output = input;
    const auto existing = view.find_column(fields.front().alias);
    const ColumnEntry entry{.name = fields.front().alias,
                            .column = std::make_shared<ColumnValue>(std::move(values)),
                            .validity = view.validity(*source_position)};
    if (existing.has_value()) {
        output.columns[*existing] = entry;
    } else {
        output.columns.push_back(entry);
    }
    output.set_properties(TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == fields.front().alias ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve));
    return output;
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
/// leaves deliberately narrow: string/categorical/temporal columns, string
/// scalars, and literal segments. Numeric display formatting remains with the
/// reference evaluator; temporal formatting calls the same canonical runtime
/// formatters in both passes.
auto resolve_string_interpolation_operand(const ir::Expr& expr, const PredicateInput& input,
                                          const ScalarRegistry* scalars)
    -> std::optional<StringInterpolationOperand> {
    if (const auto* literal = std::get_if<ir::Literal>(&expr.node)) {
        if (const auto* value = std::get_if<std::string>(&literal->value)) {
            return StringInterpolationOperand{
                .column = std::nullopt, .categorical = {}, .literal = *value};
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
                    .categorical = {},
                    .dates = nullptr,
                    .timestamps = nullptr,
                    .literal = {}};
            }
            if (const auto* values = std::get_if<Column<Categorical>>(entry->column.get())) {
                return StringInterpolationOperand{
                    .column = std::nullopt,
                    .categorical = {.codes = values->codes_data(),
                                    .dictionary = &values->dictionary(),
                                    .rows = values->size()},
                    .dates = nullptr,
                    .timestamps = nullptr,
                    .literal = {}};
            }
            if (const auto* values = std::get_if<Column<Date>>(entry->column.get())) {
                return StringInterpolationOperand{.column = std::nullopt,
                                                  .categorical = {},
                                                  .dates = values->data(),
                                                  .timestamps = nullptr,
                                                  .literal = {}};
            }
            if (const auto* values = std::get_if<Column<Timestamp>>(entry->column.get())) {
                return StringInterpolationOperand{.column = std::nullopt,
                                                  .categorical = {},
                                                  .dates = nullptr,
                                                  .timestamps = values->data(),
                                                  .literal = {}};
            }
            return std::nullopt;
        }
    }
    if (scalars != nullptr) {
        if (const auto it = scalars->find(ref->name); it != scalars->end()) {
            if (const auto* value = std::get_if<std::string>(&it->second)) {
                return StringInterpolationOperand{
                    .column = std::nullopt, .categorical = {}, .literal = *value};
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
            if (operand.column.has_value()) {
                row_bytes += operand.column->row_len(row);
            } else if (operand.categorical.dictionary != nullptr) {
                row_bytes += operand.categorical.value(row).size();
            } else if (operand.dates != nullptr) {
                row_bytes += format_date(operand.dates[row]).size();
            } else if (operand.timestamps != nullptr) {
                row_bytes += format_timestamp(operand.timestamps[row]).size();
            } else {
                row_bytes += operand.literal.size();
            }
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
            } else if (operand.categorical.dictionary != nullptr) {
                const auto value = operand.categorical.value(row);
                source_chars = value.data();
                length = value.size();
            } else if (operand.dates != nullptr) {
                const auto value = format_date(operand.dates[row]);
                source_chars = value.data();
                length = value.size();
                if (length != 0) {
                    std::memcpy(output.chars + cursor, source_chars, length);
                }
                cursor += static_cast<std::uint32_t>(length);
                continue;
            } else if (operand.timestamps != nullptr) {
                const auto value = format_timestamp(operand.timestamps[row]);
                source_chars = value.data();
                length = value.size();
                if (length != 0) {
                    std::memcpy(output.chars + cursor, source_chars, length);
                }
                cursor += static_cast<std::uint32_t>(length);
                continue;
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
    // An update's fields are ordered: a later field reads the chunk produced by
    // every earlier one.  Keep the direct route all-or-nothing, though.  If a
    // field is outside its narrow vocabulary, discard this tentative chunk and
    // let update_table evaluate the original complete update.
    if (!exec.parallel && fields.size() > 1) {
        Chunk current = input;
        bool all_direct = true;
        for (const auto& field : fields) {
            const std::vector<ir::FieldSpec> one_field{field};
            std::optional<Chunk> next = try_metadata_alias_update(current, one_field);
            if (!next.has_value()) {
                next = try_literal_update(current, one_field);
            }
            if (!next.has_value()) {
                next = try_numeric_tree_update(current, one_field, scalars);
            }
            if (!next.has_value()) {
                next = try_string_length_update(current, one_field);
            }
            if (!next.has_value()) {
                next = try_native_bool_update(current, one_field, scalars);
            }
            if (!next.has_value()) {
                next = try_shared_string_interpolation_update(current, one_field, scalars);
            }
            if (!next.has_value()) {
                all_direct = false;
                break;
            }
            current = std::move(*next);
        }
        if (all_direct) {
            return current;
        }
    }
    if (auto output = try_metadata_alias_update(input, fields); output.has_value()) {
        return std::move(*output);
    }
    if (!exec.parallel) {
        if (auto output = try_literal_update(input, fields); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_numeric_tree_update(input, fields, scalars); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_string_length_update(input, fields); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_native_bool_update(input, fields, scalars); output.has_value()) {
            return std::move(*output);
        }
        if (auto output = try_shared_string_interpolation_update(input, fields, scalars);
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
