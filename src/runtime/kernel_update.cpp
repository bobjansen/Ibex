// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "kernel_update.hpp"

#include <ibex/runtime/safe_arith.hpp>
#include <ibex/runtime/table_format.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <limits>
#include <span>
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

/// Land a direct row-local write without letting representation-specific
/// kernels make metadata claims.  In particular, an alias copy is allowed to
/// share its source's ColumnValue, while every newly produced validity bitmap
/// remains owned by the output ColumnEntry.
auto write_direct_update(const Chunk& input, std::string alias, std::shared_ptr<ColumnValue> column,
                         std::optional<ValidityBitmap> validity) -> std::optional<Chunk> {
    const ChunkView view(input);
    if (view.properties().time_index().has_value() && alias == *view.properties().time_index()) {
        return std::nullopt;
    }

    Chunk output = input;
    const ColumnEntry entry{
        .name = std::move(alias), .column = std::move(column), .validity = std::move(validity)};
    if (const auto existing = view.find_column(entry.name); existing.has_value()) {
        output.columns[*existing] = entry;
    } else {
        output.columns.push_back(entry);
    }
    output.set_properties(TableProperties::derive(
        view.properties(),
        [&](const std::string& name) -> KeyFate {
            return name == entry.name ? KeyFate::overwritten() : KeyFate::kept(name);
        },
        RowTransform::Preserve));
    return output;
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

auto eval_numeric_tree_double(const NumericTreeNode* nodes, std::uint32_t index, std::size_t row)
    -> double {
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
            return std::min(eval_numeric_tree_double(nodes, node.left, row),
                            eval_numeric_tree_double(nodes, node.right, row));
        case NumericTreeNode::Kind::Max:
            return std::max(eval_numeric_tree_double(nodes, node.left, row),
                            eval_numeric_tree_double(nodes, node.right, row));
        case NumericTreeNode::Kind::Unary:
            return node.unary(eval_numeric_tree_double(nodes, node.left, row));
        case NumericTreeNode::Kind::Binary: {
            const double left = eval_numeric_tree_double(nodes, node.left, row);
            const double right = eval_numeric_tree_double(nodes, node.right, row);
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
}

auto eval_numeric_tree_int(const NumericTreeNode* nodes, std::uint32_t index, std::size_t row)
    -> std::int64_t {
    const auto& node = nodes[index];
    switch (node.kind) {
        case NumericTreeNode::Kind::IntColumn:
            return node.ints[row];
        case NumericTreeNode::Kind::IntScalar:
            return node.int_scalar;
        case NumericTreeNode::Kind::Min:
            return std::min(eval_numeric_tree_int(nodes, node.left, row),
                            eval_numeric_tree_int(nodes, node.right, row));
        case NumericTreeNode::Kind::Max:
            return std::max(eval_numeric_tree_int(nodes, node.left, row),
                            eval_numeric_tree_int(nodes, node.right, row));
        case NumericTreeNode::Kind::Binary: {
            const std::int64_t left = eval_numeric_tree_int(nodes, node.left, row);
            const std::int64_t right = eval_numeric_tree_int(nodes, node.right, row);
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
}

auto try_numeric_tree_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                             const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const ChunkView view(input);
    const auto source = predicate_input(view);
    const auto plan = try_plan_direct_numeric_tree(fields.front().expr, source, scalars);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    const auto range = ::ibex::runtime::RowRange::whole(view.rows());
    ColumnValue values;
    NumericOutputSpan window;
    if (plan->type == ExprType::Int) {
        Column<std::int64_t> output;
        output.resize_for_overwrite(view.rows());
        values = std::move(output);
        window.ints = std::get<Column<std::int64_t>>(values).data();
    } else {
        Column<double> output;
        output.resize_for_overwrite(view.rows());
        values = std::move(output);
        window.doubles = std::get<Column<double>>(values).data();
    }
    if (!write_direct_numeric_tree_range(*plan, range, window)) {
        return std::nullopt;
    }
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move(values)),
                               collect_expr_validity(fields.front().expr, source, range));
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
    const auto source_position = view.find_column(source_ref->name);
    if (!source_position.has_value()) {
        return std::nullopt;
    }

    const auto& source = view.entry(*source_position);
    return write_direct_update(input, fields.front().alias, source.column, source.validity);
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
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(broadcast_scalar_column(
                                   scalar_from_literal(*literal), view.rows())),
                               std::nullopt);
}

template <typename T>
auto direct_literal_value(const ir::Literal& literal) -> std::optional<T> {
    if constexpr (std::is_same_v<T, std::int64_t>) {
        if (const auto* value = std::get_if<std::int64_t>(&literal.value))
            return *value;
        if (const auto* value = std::get_if<double>(&literal.value))
            return static_cast<std::int64_t>(*value);
    } else if constexpr (std::is_same_v<T, double>) {
        if (const auto* value = std::get_if<double>(&literal.value))
            return *value;
        if (const auto* value = std::get_if<std::int64_t>(&literal.value))
            return static_cast<double>(*value);
    } else if constexpr (std::is_same_v<T, bool>) {
        if (const auto* value = std::get_if<bool>(&literal.value))
            return *value;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
        if (const auto* value = std::get_if<std::string>(&literal.value))
            return std::string_view(*value);
    } else if constexpr (std::is_same_v<T, Date>) {
        if (const auto* value = std::get_if<Date>(&literal.value))
            return *value;
    } else if constexpr (std::is_same_v<T, Timestamp>) {
        if (const auto* value = std::get_if<Timestamp>(&literal.value))
            return *value;
    }
    return std::nullopt;
}

/// `fill_null` is deliberately a direct writer's first validity-changing
/// family: it preserves every valid payload, replaces only null payloads, and
/// clears validity because its literal fallback makes every result row valid.
auto try_fill_null_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* call = std::get_if<ir::CallExpr>(&fields.front().expr.node);
    if (call == nullptr || call->callee != "fill_null" || call->args.size() != 2 ||
        !call->named_args.empty()) {
        return std::nullopt;
    }
    const auto* source_ref = std::get_if<ir::ColumnRef>(&call->args[0]->node);
    const auto* literal = std::get_if<ir::Literal>(&call->args[1]->node);
    if (source_ref == nullptr || source_ref->lexical || literal == nullptr) {
        return std::nullopt;
    }
    const ChunkView view(input);
    const auto source_position = view.find_column(source_ref->name);
    if (!source_position.has_value()) {
        return std::nullopt;
    }
    const auto& source = view.entry(*source_position);

    return std::visit(
        [&](const auto& values) -> std::optional<Chunk> {
            using Col = std::decay_t<decltype(values)>;
            const auto fill = direct_literal_value<typename Col::value_type>(*literal);
            if (!fill.has_value()) {
                return std::nullopt;
            }
            if (!source.validity.has_value()) {
                return write_direct_update(input, fields.front().alias, source.column,
                                           std::nullopt);
            }
            Col output;
            ColumnAppender<Col> writer(output, view.rows());
            for (std::size_t row = 0; row < view.rows(); ++row) {
                writer.push(source.validity->operator[](row) ? values[row] : *fill);
            }
            return write_direct_update(input, fields.front().alias,
                                       std::make_shared<ColumnValue>(std::move(output)),
                                       std::nullopt);
        },
        *source.column);
}

struct DirectCoalesceOperand {
    const ColumnEntry* column = nullptr;
    const ir::Literal* literal = nullptr;
};

/// Coalesce raw column and literal leaves without invoking the Table evaluator.
/// More elaborate arguments retain the established evaluator, whose arbitrary
/// expression support is the semantic authority for those shapes.
auto try_coalesce_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields)
    -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* call = std::get_if<ir::CallExpr>(&fields.front().expr.node);
    if (call == nullptr || call->callee != "coalesce" || call->args.size() < 2 ||
        !call->named_args.empty()) {
        return std::nullopt;
    }
    const ChunkView view(input);
    std::vector<DirectCoalesceOperand> operands;
    operands.reserve(call->args.size());
    const ColumnEntry* type_source = nullptr;
    for (const auto& arg : call->args) {
        if (const auto* ref = std::get_if<ir::ColumnRef>(&arg->node)) {
            if (ref->lexical) {
                return std::nullopt;
            }
            const auto position = view.find_column(ref->name);
            if (!position.has_value()) {
                return std::nullopt;
            }
            const auto& column = view.entry(*position);
            if (type_source == nullptr) {
                type_source = &column;
            } else if (column.column->index() != type_source->column->index()) {
                return std::nullopt;
            }
            operands.push_back({.column = &column, .literal = nullptr});
        } else if (const auto* literal = std::get_if<ir::Literal>(&arg->node)) {
            operands.push_back({.column = nullptr, .literal = literal});
        } else {
            return std::nullopt;
        }
    }
    if (type_source == nullptr) {
        return std::nullopt;
    }

    return std::visit(
        [&](const auto& first) -> std::optional<Chunk> {
            using Col = std::decay_t<decltype(first)>;
            using Value = typename Col::value_type;
            std::vector<std::optional<Value>> literals;
            literals.reserve(operands.size());
            for (const auto& operand : operands) {
                if (operand.literal != nullptr) {
                    auto value = direct_literal_value<Value>(*operand.literal);
                    if (!value.has_value()) {
                        return std::nullopt;
                    }
                    literals.push_back(std::move(value));
                } else {
                    literals.push_back(std::nullopt);
                }
            }

            Col output;
            ColumnAppender<Col> writer(output, view.rows());
            ValidityBitmap validity(view.rows(), true);
            bool any_invalid = false;
            for (std::size_t row = 0; row < view.rows(); ++row) {
                bool found = false;
                for (std::size_t index = 0; index < operands.size(); ++index) {
                    const auto& operand = operands[index];
                    if (operand.column == nullptr) {
                        writer.push(*literals[index]);
                        found = true;
                        break;
                    }
                    if (operand.column->validity.has_value() && !(*operand.column->validity)[row]) {
                        continue;
                    }
                    const auto* values = std::get_if<Col>(operand.column->column.get());
                    if (values == nullptr) {
                        return std::nullopt;
                    }
                    writer.push((*values)[row]);
                    found = true;
                    break;
                }
                if (!found) {
                    if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                        writer.push(std::string_view{});
                    } else {
                        writer.push(Value{});
                    }
                    validity.set(row, false);
                    any_invalid = true;
                }
            }
            return write_direct_update(
                input, fields.front().alias, std::make_shared<ColumnValue>(std::move(output)),
                any_invalid ? std::optional<ValidityBitmap>{std::move(validity)} : std::nullopt);
        },
        *type_source->column);
}

struct DirectCaseValue {
    const ColumnEntry* column = nullptr;
    const ir::Literal* literal = nullptr;
    bool null = false;
};

struct DirectCasePlan {
    std::vector<Mask> conditions;
    std::vector<DirectCaseValue> values;
    const ColumnEntry* type_column = nullptr;
    std::optional<ColumnValue> literal_type;

    [[nodiscard]] auto type_source() const -> const ColumnValue& {
        return type_column != nullptr ? *type_column->column : *literal_type;
    }

    [[nodiscard]] auto selected_arm(std::size_t row) const noexcept -> std::size_t {
        for (std::size_t arm = 0; arm < conditions.size(); ++arm) {
            const auto& condition = conditions[arm];
            if (condition.value[row] != 0 &&
                (!condition.valid.has_value() || (*condition.valid)[row] != 0)) {
                return arm;
            }
        }
        return conditions.size();  // else arm
    }
};

/// Validate and prepare raw CASE arms once. Representation-specific writers
/// share this 3VL selection plan but keep their own output ownership contract.
auto make_direct_case_plan(const ChunkView& view, const ir::CallExpr& call,
                           const ScalarRegistry* scalars) -> std::optional<DirectCasePlan> {
    if (call.callee != "__case" || call.args.empty() || call.args.size() % 2 == 0 ||
        !call.named_args.empty()) {
        return std::nullopt;
    }
    const auto source = predicate_input(view);
    DirectCasePlan plan;
    plan.conditions.reserve((call.args.size() - 1) / 2);
    for (std::size_t index = 0; index + 1 < call.args.size(); index += 2) {
        auto mask = compute_mask(*call.args[index], source, scalars,
                                 ::ibex::runtime::RowRange::whole(view.rows()));
        if (!mask.has_value()) {
            return std::nullopt;
        }
        plan.conditions.push_back(std::move(*mask));
    }

    plan.values.reserve(plan.conditions.size() + 1);
    const auto add_value = [&](const ir::Expr& expr) -> bool {
        if (const auto* ref = std::get_if<ir::ColumnRef>(&expr.node)) {
            if (ref->lexical) {
                return false;
            }
            const auto position = view.find_column(ref->name);
            if (!position.has_value()) {
                return false;
            }
            const auto& column = view.entry(*position);
            if (plan.type_column == nullptr && !plan.literal_type.has_value()) {
                plan.type_column = &column;
            } else if (column.column->index() != plan.type_source().index()) {
                return false;
            }
            plan.values.push_back({.column = &column, .literal = nullptr, .null = false});
            return true;
        }
        if (const auto* literal = std::get_if<ir::Literal>(&expr.node)) {
            if (plan.type_column == nullptr && !plan.literal_type.has_value()) {
                plan.literal_type = broadcast_scalar_column(scalar_from_literal(*literal), 1);
            }
            plan.values.push_back({.column = nullptr, .literal = literal, .null = false});
            return true;
        }
        if (const auto* null_call = std::get_if<ir::CallExpr>(&expr.node);
            null_call != nullptr && null_call->callee == "__null" && null_call->args.empty()) {
            plan.values.push_back({.column = nullptr, .literal = nullptr, .null = true});
            return true;
        }
        return false;
    };
    for (std::size_t index = 1; index < call.args.size(); index += 2) {
        if (!add_value(*call.args[index])) {
            return std::nullopt;
        }
    }
    if (!add_value(*call.args.back()) ||
        (plan.type_column == nullptr && !plan.literal_type.has_value())) {
        return std::nullopt;
    }
    return plan;
}

auto write_fixed_width_case(const Chunk& input, const ir::FieldSpec& field,
                            const DirectCasePlan& plan) -> std::optional<Chunk> {
    const ChunkView view(input);

    return std::visit(
        [&](const auto& type_values) -> std::optional<Chunk> {
            using Col = std::decay_t<decltype(type_values)>;
            using Value = typename Col::value_type;
            if constexpr (std::is_same_v<Col, Column<std::string>> ||
                          std::is_same_v<Col, Column<Categorical>>) {
                return std::nullopt;
            } else {
                std::vector<std::optional<Value>> literals;
                literals.reserve(plan.values.size());
                for (const auto& value : plan.values) {
                    if (value.literal != nullptr) {
                        auto literal = direct_literal_value<Value>(*value.literal);
                        if (!literal.has_value()) {
                            return std::nullopt;
                        }
                        literals.push_back(std::move(literal));
                    } else {
                        literals.push_back(std::nullopt);
                    }
                }

                Col output;
                ColumnAppender<Col> writer(output, view.rows());
                ValidityBitmap validity(view.rows(), true);
                bool any_invalid = false;
                for (std::size_t row = 0; row < view.rows(); ++row) {
                    const std::size_t selected = plan.selected_arm(row);
                    const auto& value = plan.values[selected];
                    bool valid = !value.null;
                    if (value.column != nullptr) {
                        const auto* column = std::get_if<Col>(value.column->column.get());
                        if (column == nullptr) {
                            return std::nullopt;
                        }
                        writer.push((*column)[row]);
                        valid =
                            !value.column->validity.has_value() || (*value.column->validity)[row];
                    } else if (value.literal != nullptr) {
                        writer.push(*literals[selected]);
                    } else {
                        writer.push(Value{});
                    }
                    validity.set(row, valid);
                    any_invalid = any_invalid || !valid;
                }
                return write_direct_update(
                    input, field.alias, std::make_shared<ColumnValue>(std::move(output)),
                    any_invalid ? std::optional<ValidityBitmap>{std::move(validity)}
                                : std::nullopt);
            }
        },
        plan.type_source());
}

auto write_string_case(const Chunk& input, const ir::FieldSpec& field, const DirectCasePlan& plan)
    -> std::optional<Chunk> {
    const ChunkView view(input);
    if (!std::holds_alternative<Column<std::string>>(plan.type_source())) {
        return std::nullopt;
    }

    std::vector<std::optional<std::string_view>> literals;
    literals.reserve(plan.values.size());
    for (const auto& value : plan.values) {
        if (value.literal != nullptr) {
            auto literal = direct_literal_value<std::string_view>(*value.literal);
            if (!literal.has_value()) {
                return std::nullopt;
            }
            literals.push_back(std::move(literal));
        } else {
            literals.push_back(std::nullopt);
        }
    }
    const auto selected_value = [&](std::size_t row) -> std::optional<std::string_view> {
        const std::size_t selected = plan.selected_arm(row);
        const auto& value = plan.values[selected];
        if (value.null) {
            return std::nullopt;
        }
        if (value.literal != nullptr) {
            return *literals[selected];
        }
        const auto* column = std::get_if<Column<std::string>>(value.column->column.get());
        if (column == nullptr ||
            (value.column->validity.has_value() && !(*value.column->validity)[row])) {
            return std::nullopt;
        }
        return (*column)[row];
    };

    std::size_t total_chars = 0;
    for (std::size_t row = 0; row < view.rows(); ++row) {
        const auto value = selected_value(row);
        if (value.has_value()) {
            if (value->size() > std::numeric_limits<std::uint32_t>::max() - total_chars) {
                return std::nullopt;
            }
            total_chars += value->size();
        }
    }
    Column<std::string> output;
    output.resize_for_gather(view.rows(), static_cast<std::uint32_t>(total_chars));
    auto* offsets = output.offsets_data();
    auto* chars = output.chars_data();
    offsets[0] = 0;
    std::uint32_t cursor = 0;
    ValidityBitmap validity(view.rows(), true);
    bool any_invalid = false;
    for (std::size_t row = 0; row < view.rows(); ++row) {
        const auto value = selected_value(row);
        if (value.has_value()) {
            if (!value->empty()) {
                std::memcpy(chars + cursor, value->data(), value->size());
            }
            cursor += static_cast<std::uint32_t>(value->size());
        } else {
            validity.set(row, false);
            any_invalid = true;
        }
        offsets[row + 1] = cursor;
    }
    return write_direct_update(
        input, field.alias, std::make_shared<ColumnValue>(std::move(output)),
        any_invalid ? std::optional<ValidityBitmap>{std::move(validity)} : std::nullopt);
}

auto write_categorical_case(const Chunk& input, const ir::FieldSpec& field,
                            const DirectCasePlan& plan) -> std::optional<Chunk> {
    const ChunkView view(input);
    if (!std::holds_alternative<Column<Categorical>>(plan.type_source())) {
        return std::nullopt;
    }
    std::vector<std::optional<std::string_view>> literals;
    literals.reserve(plan.values.size());
    for (const auto& value : plan.values) {
        if (value.literal != nullptr) {
            auto literal = direct_literal_value<std::string_view>(*value.literal);
            if (!literal.has_value()) {
                return std::nullopt;
            }
            literals.push_back(std::move(literal));
        } else {
            literals.push_back(std::nullopt);
        }
        if (value.column != nullptr) {
            const auto* column = std::get_if<Column<Categorical>>(value.column->column.get());
            if (column == nullptr) {
                return std::nullopt;
            }
        }
    }

    const auto selected_value = [&](std::size_t row) -> std::optional<std::string_view> {
        const std::size_t selected = plan.selected_arm(row);
        const auto& value = plan.values[selected];
        if (value.null) {
            return std::nullopt;
        }
        if (value.literal != nullptr) {
            return *literals[selected];
        }
        const auto& source = std::get<Column<Categorical>>(*value.column->column);
        if (value.column->validity.has_value() && !(*value.column->validity)[row]) {
            return std::nullopt;
        }
        return source[row];
    };

    std::size_t total_chars = 0;
    for (std::size_t row = 0; row < view.rows(); ++row) {
        if (const auto value = selected_value(row); value.has_value()) {
            if (value->size() > std::numeric_limits<std::uint32_t>::max() - total_chars) {
                return std::nullopt;
            }
            total_chars += value->size();
        }
    }
    Column<std::string> output;
    output.resize_for_gather(view.rows(), static_cast<std::uint32_t>(total_chars));
    auto* offsets = output.offsets_data();
    auto* chars = output.chars_data();
    offsets[0] = 0;
    std::uint32_t cursor = 0;
    ValidityBitmap validity(view.rows(), true);
    bool any_invalid = false;
    for (std::size_t row = 0; row < view.rows(); ++row) {
        if (const auto value = selected_value(row); value.has_value()) {
            if (!value->empty()) {
                std::memcpy(chars + cursor, value->data(), value->size());
            }
            cursor += static_cast<std::uint32_t>(value->size());
        } else {
            validity.set(row, false);
            any_invalid = true;
        }
        offsets[row + 1] = cursor;
    }
    return write_direct_update(
        input, field.alias, std::make_shared<ColumnValue>(std::move(output)),
        any_invalid ? std::optional<ValidityBitmap>{std::move(validity)} : std::nullopt);
}

auto try_case_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                     const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto* call = std::get_if<ir::CallExpr>(&fields.front().expr.node);
    if (call == nullptr) {
        return std::nullopt;
    }
    const ChunkView view(input);
    auto plan = make_direct_case_plan(view, *call, scalars);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    if (std::holds_alternative<Column<std::string>>(plan->type_source())) {
        return write_string_case(input, fields.front(), *plan);
    }
    if (std::holds_alternative<Column<Categorical>>(plan->type_source())) {
        return write_categorical_case(input, fields.front(), *plan);
    }
    return write_fixed_width_case(input, fields.front(), *plan);
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

auto try_native_bool_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                            const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const auto plan = try_plan_direct_predicate_field(fields.front().expr);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    const ChunkView view(input);
    Column<bool> values;
    values.resize(view.rows());
    auto validity = write_direct_predicate_range(
        *plan, predicate_input(view), ::ibex::runtime::RowRange::whole(view.rows()), scalars,
        BoolOutputSpan{.words = values.words_data(), .begin = 0, .count = view.rows()});
    if (!validity.has_value()) {
        return std::nullopt;
    }
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move(values)),
                               std::move(*validity));
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

    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move(values)),
                               std::move(validity));
}

/// Serial ChunkView execution uses the same caller-owned output window as the
/// table splitter, just for one whole-range assignment. This makes the
/// plan/write boundary real before parallel dispatch starts selecting it.
auto try_planned_fixed_width_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                                    const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const ChunkView view(input);
    const auto source = predicate_input(view);
    const auto plan = try_plan_direct_fixed_width_field(fields.front().expr, source, scalars);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    const auto range = ::ibex::runtime::RowRange::whole(view.rows());
    auto validity = collect_expr_validity(fields.front().expr, source, range);
    if (plan->numeric_kind == FixedWidthNumericKind::Int) {
        Column<std::int64_t> values;
        values.resize_for_overwrite(view.rows());
        if (!write_direct_field_range(*plan, source, range, scalars,
                                      {.numeric = {.ints = values.data(), .doubles = nullptr}})) {
            return std::nullopt;
        }
        return write_direct_update(input, fields.front().alias,
                                   std::make_shared<ColumnValue>(std::move(values)),
                                   std::move(validity));
    }

    Column<double> values;
    values.resize_for_overwrite(view.rows());
    if (!write_direct_field_range(*plan, source, range, scalars,
                                  {.numeric = {.ints = nullptr, .doubles = values.data()}})) {
        return std::nullopt;
    }
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move(values)),
                               std::move(validity));
}

/// Nullable numeric direct families use the same output ownership contract as
/// the table splitter: the writer fills values and returns a fresh validity
/// bitmap, while this wrapper alone installs the resulting ColumnEntry.
auto try_planned_validity_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                                 const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const ChunkView view(input);
    const auto source = predicate_input(view);
    const auto plan = try_plan_direct_validity_field(fields.front().expr, source, scalars);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    const auto range = ::ibex::runtime::RowRange::whole(view.rows());
    if (plan->numeric_kind == FixedWidthNumericKind::Int) {
        Column<std::int64_t> values;
        values.resize_for_overwrite(view.rows());
        auto validity = write_direct_validity_field_range(
            *plan, source, range, scalars,
            {.numeric = {.ints = values.data(), .doubles = nullptr}});
        if (!validity.has_value()) {
            return std::nullopt;
        }
        return write_direct_update(input, fields.front().alias,
                                   std::make_shared<ColumnValue>(std::move(values)),
                                   std::move(*validity));
    }
    Column<double> values;
    values.resize_for_overwrite(view.rows());
    auto validity = write_direct_validity_field_range(
        *plan, source, range, scalars, {.numeric = {.ints = nullptr, .doubles = values.data()}});
    if (!validity.has_value()) {
        return std::nullopt;
    }
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move(values)),
                               std::move(*validity));
}

auto try_planned_categorical_update(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                                    const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (fields.size() != 1) {
        return std::nullopt;
    }
    const ChunkView view(input);
    const auto source = predicate_input(view);
    const auto plan = try_plan_direct_categorical_field(fields.front().expr, source, scalars);
    if (!plan.has_value()) {
        return std::nullopt;
    }
    std::vector<Column<Categorical>::code_type> codes(view.rows(), 0);
    auto validity = write_direct_categorical_field_range(
        *plan, source, ::ibex::runtime::RowRange::whole(view.rows()), scalars,
        {.codes = codes.data(), .begin = 0, .count = view.rows()});
    if (!validity.has_value()) {
        return std::nullopt;
    }
    Column<Categorical> values(plan->dictionary, plan->index, std::move(codes));
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move(values)),
                               std::move(*validity));
}

/// Try exactly one field on every direct ChunkView family.  Multi-field
/// updates call this in declaration order; if any field declines, their caller
/// discards the tentative chunks and delegates the original update intact.
auto try_direct_update_field(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                             const ScalarRegistry* scalars) -> std::optional<Chunk> {
    if (auto output = try_metadata_alias_update(input, fields); output.has_value()) {
        return output;
    }
    if (auto output = try_literal_update(input, fields); output.has_value()) {
        return output;
    }
    if (auto output = try_planned_categorical_update(input, fields, scalars); output.has_value()) {
        return output;
    }
    if (auto output = try_planned_validity_update(input, fields, scalars); output.has_value()) {
        return output;
    }
    if (auto output = try_fill_null_update(input, fields); output.has_value()) {
        return output;
    }
    if (auto output = try_coalesce_update(input, fields); output.has_value()) {
        return output;
    }
    if (auto output = try_case_update(input, fields, scalars); output.has_value()) {
        return output;
    }
    if (auto output = try_planned_fixed_width_update(input, fields, scalars); output.has_value()) {
        return output;
    }
    if (auto output = try_numeric_tree_update(input, fields, scalars); output.has_value()) {
        return output;
    }
    if (auto output = try_native_bool_update(input, fields, scalars); output.has_value()) {
        return output;
    }
    return try_shared_string_interpolation_update(input, fields, scalars);
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

auto try_plan_direct_numeric_field(const ir::Expr& expr, const PredicateInput& input,
                                   const ScalarRegistry* scalars)
    -> std::optional<DirectFieldPlan> {
    const auto kind = fixed_width_numeric_binary_kind(expr, input, scalars);
    if (!kind.has_value()) {
        return std::nullopt;
    }
    return DirectFieldPlan{.kind = DirectFieldKind::NumericBinary,
                           .expression = &expr,
                           .numeric_kind = *kind,
                           .categorical_lengths = nullptr};
}

auto try_plan_direct_temporal_field(const ir::Expr& expr, const PredicateInput& input)
    -> std::optional<DirectFieldPlan> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || call->args.size() != 1 || !call->named_args.empty()) {
        return std::nullopt;
    }
    const auto* source = std::get_if<ir::ColumnRef>(&call->args.front()->node);
    if (source == nullptr || source->lexical) {
        return std::nullopt;
    }
    const auto part = call->callee == "year"     ? std::optional{TemporalPart::Year}
                      : call->callee == "month"  ? std::optional{TemporalPart::Month}
                      : call->callee == "day"    ? std::optional{TemporalPart::Day}
                      : call->callee == "hour"   ? std::optional{TemporalPart::Hour}
                      : call->callee == "minute" ? std::optional{TemporalPart::Minute}
                      : call->callee == "second" ? std::optional{TemporalPart::Second}
                                                 : std::nullopt;
    if (!part.has_value()) {
        return std::nullopt;
    }
    const auto* entry = input.find(source->name);
    if (entry == nullptr) {
        return std::nullopt;
    }
    if (const auto* dates = std::get_if<Column<Date>>(entry->column.get())) {
        if (*part == TemporalPart::Hour || *part == TemporalPart::Minute ||
            *part == TemporalPart::Second) {
            return std::nullopt;
        }
        return DirectFieldPlan{.kind = DirectFieldKind::TemporalPart,
                               .expression = &expr,
                               .numeric_kind = FixedWidthNumericKind::Int,
                               .temporal_part = *part,
                               .dates = dates->data(),
                               .categorical_lengths = nullptr};
    }
    if (const auto* timestamps = std::get_if<Column<Timestamp>>(entry->column.get())) {
        return DirectFieldPlan{.kind = DirectFieldKind::TemporalPart,
                               .expression = &expr,
                               .numeric_kind = FixedWidthNumericKind::Int,
                               .temporal_part = *part,
                               .timestamps = timestamps->data(),
                               .categorical_lengths = nullptr};
    }
    return std::nullopt;
}

auto try_plan_direct_string_length_field(const ir::Expr& expr, const PredicateInput& input)
    -> std::optional<DirectFieldPlan> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || call->args.size() != 1 || !call->named_args.empty() ||
        (call->callee != "length" && call->callee != "byte_length")) {
        return std::nullopt;
    }
    const auto* source = std::get_if<ir::ColumnRef>(&call->args.front()->node);
    if (source == nullptr || source->lexical) {
        return std::nullopt;
    }
    const auto* entry = input.find(source->name);
    if (entry == nullptr) {
        return std::nullopt;
    }
    const bool bytes = call->callee == "byte_length";
    if (const auto* strings = std::get_if<Column<std::string>>(entry->column.get())) {
        return DirectFieldPlan{.kind = DirectFieldKind::StringLength,
                               .expression = &expr,
                               .numeric_kind = FixedWidthNumericKind::Int,
                               .byte_length = bytes,
                               .strings = strings,
                               .categorical_lengths = nullptr};
    }
    if (const auto* categorical = std::get_if<Column<Categorical>>(entry->column.get())) {
        auto lengths = std::make_shared<std::vector<std::int64_t>>();
        lengths->reserve(categorical->dictionary_size());
        for (const auto& label : categorical->dictionary()) {
            lengths->push_back(bytes ? static_cast<std::int64_t>(label.size())
                                     : utf8_codepoint_count(label));
        }
        return DirectFieldPlan{.kind = DirectFieldKind::StringLength,
                               .expression = &expr,
                               .numeric_kind = FixedWidthNumericKind::Int,
                               .byte_length = bytes,
                               .categoricals = categorical,
                               .categorical_lengths = std::move(lengths)};
    }
    return std::nullopt;
}

auto try_plan_direct_fixed_width_field(const ir::Expr& expr, const PredicateInput& input,
                                       const ScalarRegistry* scalars)
    -> std::optional<DirectFieldPlan> {
    if (auto plan = try_plan_direct_numeric_field(expr, input, scalars); plan.has_value()) {
        return plan;
    }
    if (auto plan = try_plan_direct_temporal_field(expr, input); plan.has_value()) {
        return plan;
    }
    return try_plan_direct_string_length_field(expr, input);
}

auto write_direct_field_range(const DirectFieldPlan& plan, const PredicateInput& input,
                              ::ibex::runtime::RowRange range, const ScalarRegistry* scalars,
                              DirectOutputWindow output) -> bool {
    if (plan.kind == DirectFieldKind::NumericBinary) {
        return plan.expression != nullptr &&
               write_fixed_width_numeric_binary(*plan.expression, input, range, scalars,
                                                plan.numeric_kind, output.numeric);
    }
    if (plan.kind == DirectFieldKind::StringLength) {
        if (output.numeric.ints == nullptr ||
            (plan.strings == nullptr && plan.categoricals == nullptr)) {
            return false;
        }
        for (std::size_t offset = 0; offset < range.count; ++offset) {
            const std::size_t row = range.begin + offset;
            if (plan.strings != nullptr) {
                const auto value = (*plan.strings)[row];
                output.numeric.ints[offset] = plan.byte_length
                                                  ? static_cast<std::int64_t>(value.size())
                                                  : utf8_codepoint_count(value);
            } else {
                const auto code = static_cast<std::size_t>(plan.categoricals->code_at(row));
                output.numeric.ints[offset] = (*plan.categorical_lengths)[code];
            }
        }
        return true;
    }
    if (output.numeric.ints == nullptr || (plan.dates == nullptr && plan.timestamps == nullptr)) {
        return false;
    }
    using namespace std::chrono;
    for (std::size_t offset = 0; offset < range.count; ++offset) {
        const std::size_t row = range.begin + offset;
        if (plan.dates != nullptr) {
            const year_month_day ymd{sys_days{days{plan.dates[row].days}}};
            output.numeric.ints[offset] =
                plan.temporal_part == TemporalPart::Year    ? static_cast<int>(ymd.year())
                : plan.temporal_part == TemporalPart::Month ? static_cast<unsigned>(ymd.month())
                                                            : static_cast<unsigned>(ymd.day());
            continue;
        }
        const sys_time<nanoseconds> time{nanoseconds{plan.timestamps[row].nanos}};
        const auto day_point = floor<days>(time);
        const year_month_day ymd{day_point};
        const hh_mm_ss<nanoseconds> hms{time - day_point};
        output.numeric.ints[offset] =
            plan.temporal_part == TemporalPart::Year     ? static_cast<int>(ymd.year())
            : plan.temporal_part == TemporalPart::Month  ? static_cast<unsigned>(ymd.month())
            : plan.temporal_part == TemporalPart::Day    ? static_cast<unsigned>(ymd.day())
            : plan.temporal_part == TemporalPart::Hour   ? hms.hours().count()
            : plan.temporal_part == TemporalPart::Minute ? hms.minutes().count()
                                                         : hms.seconds().count();
    }
    return true;
}

auto try_plan_direct_predicate_field(const ir::Expr& expr) -> std::optional<DirectPredicatePlan> {
    if (!supports_native_chunk_predicate(expr)) {
        return std::nullopt;
    }
    const bool is_boolean = std::holds_alternative<ir::CompareExpr>(expr.node) ||
                            std::holds_alternative<ir::LogicalExpr>(expr.node) ||
                            std::holds_alternative<ir::IsNullExpr>(expr.node);
    return is_boolean ? std::optional<DirectPredicatePlan>{{.expression = &expr}} : std::nullopt;
}

auto write_direct_predicate_range(const DirectPredicatePlan& plan, const PredicateInput& input,
                                  ::ibex::runtime::RowRange range, const ScalarRegistry* scalars,
                                  BoolOutputSpan output)
    -> std::expected<std::optional<ValidityBitmap>, std::string> {
    if (plan.expression == nullptr || output.words == nullptr || output.begin != range.begin ||
        output.count != range.count) {
        return std::unexpected("write_direct_predicate_range: output window shape mismatch");
    }
    auto mask = compute_mask(*plan.expression, input, scalars, range);
    if (!mask.has_value()) {
        return std::unexpected(std::move(mask.error()));
    }
    const SharedBitWords shared = SharedBitWords::of_run(output.begin, output.count);
    for (std::size_t offset = 0; offset < output.count; ++offset) {
        if (mask->value[offset] != 0) {
            const std::size_t row = output.begin + offset;
            or_bits_into_word(output.words, row / 64, std::uint64_t{1} << (row % 64), shared);
        }
    }
    if (!mask->valid.has_value()) {
        return std::optional<ValidityBitmap>{};
    }
    ValidityBitmap validity(output.count, false);
    for (std::size_t offset = 0; offset < output.count; ++offset) {
        validity.set(offset, (*mask->valid)[offset] != 0);
    }
    return std::optional<ValidityBitmap>{std::move(validity)};
}

auto try_plan_direct_validity_field(const ir::Expr& expr, const PredicateInput& input,
                                    const ScalarRegistry* /*scalars*/)
    -> std::optional<DirectValidityPlan> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || !call->named_args.empty()) {
        return std::nullopt;
    }

    DirectValidityPlan plan;
    if (call->callee == "fill_null" && call->args.size() == 2) {
        plan.kind = DirectValidityKind::FillNull;
    } else if (call->callee == "coalesce" && call->args.size() >= 2) {
        plan.kind = DirectValidityKind::Coalesce;
    } else if (call->callee == "__case" && !call->args.empty() && call->args.size() % 2 == 1) {
        plan.kind = DirectValidityKind::Case;
        plan.conditions.reserve((call->args.size() - 1) / 2);
        for (std::size_t index = 0; index + 1 < call->args.size(); index += 2) {
            if (!supports_native_chunk_predicate(*call->args[index])) {
                return std::nullopt;
            }
            plan.conditions.push_back(call->args[index].get());
        }
    } else {
        return std::nullopt;
    }

    std::optional<FixedWidthNumericKind> kind;
    const auto add_value = [&](const ir::Expr& value) -> bool {
        if (const auto* ref = std::get_if<ir::ColumnRef>(&value.node)) {
            if (ref->lexical) {
                return false;
            }
            const auto* entry = input.find(ref->name);
            if (entry == nullptr) {
                return false;
            }
            const auto source_kind = std::holds_alternative<Column<std::int64_t>>(*entry->column)
                                         ? std::optional{FixedWidthNumericKind::Int}
                                     : std::holds_alternative<Column<double>>(*entry->column)
                                         ? std::optional{FixedWidthNumericKind::Double}
                                         : std::nullopt;
            if (!source_kind.has_value() || (kind.has_value() && *kind != *source_kind)) {
                return false;
            }
            kind = *source_kind;
            plan.values.push_back(
                {.column = entry, .literal = {}, .is_literal = false, .is_null = false});
            return true;
        }
        if (const auto* literal = std::get_if<ir::Literal>(&value.node)) {
            const auto literal_kind = std::holds_alternative<std::int64_t>(literal->value)
                                          ? std::optional{FixedWidthNumericKind::Int}
                                      : std::holds_alternative<double>(literal->value)
                                          ? std::optional{FixedWidthNumericKind::Double}
                                          : std::nullopt;
            if (!literal_kind.has_value() || (kind.has_value() && *kind != *literal_kind)) {
                return false;
            }
            kind = *literal_kind;
            plan.values.push_back({.column = nullptr,
                                   .literal = scalar_from_literal(*literal),
                                   .is_literal = true,
                                   .is_null = false});
            return true;
        }
        if (const auto* null_call = std::get_if<ir::CallExpr>(&value.node);
            null_call != nullptr && null_call->callee == "__null" && null_call->args.empty() &&
            null_call->named_args.empty()) {
            plan.values.push_back(
                {.column = nullptr, .literal = {}, .is_literal = false, .is_null = true});
            return true;
        }
        return false;
    };

    if (plan.kind == DirectValidityKind::Case) {
        for (std::size_t index = 1; index + 1 < call->args.size(); index += 2) {
            if (!add_value(*call->args[index])) {
                return std::nullopt;
            }
        }
        if (!add_value(*call->args.back())) {
            return std::nullopt;
        }
    } else {
        for (const auto& arg : call->args) {
            if (!add_value(*arg)) {
                return std::nullopt;
            }
        }
    }
    if (!kind.has_value() || plan.values.empty()) {
        return std::nullopt;
    }
    plan.numeric_kind = *kind;
    return plan;
}

auto write_direct_validity_field_range(const DirectValidityPlan& plan, const PredicateInput& input,
                                       ::ibex::runtime::RowRange range,
                                       const ScalarRegistry* scalars, DirectOutputWindow output)
    -> std::expected<std::optional<ValidityBitmap>, std::string> {
    if ((plan.numeric_kind == FixedWidthNumericKind::Int && output.numeric.ints == nullptr) ||
        (plan.numeric_kind == FixedWidthNumericKind::Double && output.numeric.doubles == nullptr)) {
        return std::unexpected("write_direct_validity_field_range: output window shape mismatch");
    }
    std::vector<Mask> conditions;
    conditions.reserve(plan.conditions.size());
    for (const ir::Expr* condition : plan.conditions) {
        if (condition == nullptr) {
            return std::unexpected("write_direct_validity_field_range: missing CASE condition");
        }
        auto mask = compute_mask(*condition, input, scalars, range);
        if (!mask.has_value()) {
            return std::unexpected(std::move(mask.error()));
        }
        conditions.push_back(std::move(*mask));
    }

    const auto available = [](const DirectValidityValue& value, std::size_t row) {
        return !value.is_null && (value.is_literal || !value.column->validity.has_value() ||
                                  (*value.column->validity)[row]);
    };
    ValidityBitmap validity(range.count, true);
    bool any_invalid = false;
    for (std::size_t offset = 0; offset < range.count; ++offset) {
        const std::size_t row = range.begin + offset;
        const DirectValidityValue* selected = nullptr;
        if (plan.kind == DirectValidityKind::FillNull) {
            if (plan.values.size() != 2) {
                return std::unexpected(
                    "write_direct_validity_field_range: malformed fill_null plan");
            }
            selected = available(plan.values[0], row) ? &plan.values[0] : &plan.values[1];
        } else if (plan.kind == DirectValidityKind::Coalesce) {
            for (const auto& value : plan.values) {
                if (available(value, row)) {
                    selected = &value;
                    break;
                }
            }
        } else {
            std::size_t arm = conditions.size();
            for (std::size_t index = 0; index < conditions.size(); ++index) {
                if (conditions[index].value[offset] != 0 &&
                    (!conditions[index].valid.has_value() || (*conditions[index].valid)[offset])) {
                    arm = index;
                    break;
                }
            }
            if (arm >= plan.values.size()) {
                return std::unexpected("write_direct_validity_field_range: malformed CASE plan");
            }
            selected = &plan.values[arm];
        }

        const bool is_valid = selected != nullptr && available(*selected, row);
        if (!is_valid) {
            validity.set(offset, false);
            any_invalid = true;
        }
        if (plan.numeric_kind == FixedWidthNumericKind::Int) {
            output.numeric.ints[offset] =
                is_valid ? selected->is_literal
                               ? std::get<std::int64_t>(selected->literal)
                               : std::get<Column<std::int64_t>>(*selected->column->column)[row]
                         : 0;
        } else {
            output.numeric.doubles[offset] =
                is_valid ? selected->is_literal
                               ? std::get<double>(selected->literal)
                               : std::get<Column<double>>(*selected->column->column)[row]
                         : 0.0;
        }
    }
    return any_invalid ? std::optional<ValidityBitmap>{std::move(validity)}
                       : std::optional<ValidityBitmap>{};
}

auto try_plan_direct_categorical_field(const ir::Expr& expr, const PredicateInput& input,
                                       const ScalarRegistry* /*scalars*/)
    -> std::optional<DirectCategoricalPlan> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || !call->named_args.empty()) {
        return std::nullopt;
    }
    DirectCategoricalPlan plan;
    bool has_categorical_source = false;
    if (call->callee == "coalesce" && call->args.size() >= 2) {
        plan.kind = DirectValidityKind::Coalesce;
    } else if (call->callee == "__case" && !call->args.empty() && call->args.size() % 2 == 1) {
        plan.kind = DirectValidityKind::Case;
        for (std::size_t index = 0; index + 1 < call->args.size(); index += 2) {
            if (!supports_native_chunk_predicate(*call->args[index])) {
                return std::nullopt;
            }
            plan.conditions.push_back(call->args[index].get());
        }
    } else {
        return std::nullopt;
    }

    const auto add_value = [&](const ir::Expr& value) -> bool {
        if (const auto* ref = std::get_if<ir::ColumnRef>(&value.node)) {
            if (ref->lexical) {
                return false;
            }
            const auto* entry = input.find(ref->name);
            if (entry == nullptr || !std::holds_alternative<Column<Categorical>>(*entry->column)) {
                return false;
            }
            has_categorical_source = true;
            plan.values.push_back(
                {.column = entry, .literal = {}, .is_literal = false, .is_null = false});
            return true;
        }
        if (const auto* literal = std::get_if<ir::Literal>(&value.node)) {
            const auto* label = std::get_if<std::string>(&literal->value);
            if (label == nullptr) {
                return false;
            }
            plan.values.push_back({.column = nullptr,
                                   .literal = scalar_from_literal(*literal),
                                   .is_literal = true,
                                   .is_null = false});
            return true;
        }
        if (const auto* null_call = std::get_if<ir::CallExpr>(&value.node);
            null_call != nullptr && null_call->callee == "__null" && null_call->args.empty() &&
            null_call->named_args.empty()) {
            plan.values.push_back(
                {.column = nullptr, .literal = {}, .is_literal = false, .is_null = true});
            return true;
        }
        return false;
    };
    if (plan.kind == DirectValidityKind::Case) {
        for (std::size_t index = 1; index + 1 < call->args.size(); index += 2) {
            if (!add_value(*call->args[index])) {
                return std::nullopt;
            }
        }
        if (!add_value(*call->args.back())) {
            return std::nullopt;
        }
    } else {
        for (const auto& arg : call->args) {
            if (!add_value(*arg)) {
                return std::nullopt;
            }
        }
    }

    if (!has_categorical_source) {
        return std::nullopt;
    }
    plan.dictionary = std::make_shared<std::vector<std::string>>();
    plan.index = std::make_shared<Column<Categorical>::index_map>();
    const auto intern = [&](std::string_view label) {
        if (const auto it = plan.index->find(label); it != plan.index->end()) {
            return it->second;
        }
        const auto code = static_cast<Column<Categorical>::code_type>(plan.dictionary->size());
        plan.dictionary->emplace_back(label);
        plan.index->emplace(plan.dictionary->back(), code);
        return code;
    };
    plan.remaps.resize(plan.values.size());
    plan.literal_codes.assign(plan.values.size(), 0);
    for (std::size_t index = 0; index < plan.values.size(); ++index) {
        const auto& value = plan.values[index];
        if (value.column != nullptr) {
            const auto& source = std::get<Column<Categorical>>(*value.column->column);
            auto& remap = plan.remaps[index];
            remap.reserve(source.dictionary_size());
            for (const auto& label : source.dictionary()) {
                remap.push_back(intern(label));
            }
        } else if (value.is_literal) {
            plan.literal_codes[index] = intern(std::get<std::string>(value.literal));
        }
    }
    return plan;
}

auto write_direct_categorical_field_range(const DirectCategoricalPlan& plan,
                                          const PredicateInput& input,
                                          ::ibex::runtime::RowRange range,
                                          const ScalarRegistry* scalars,
                                          CategoricalOutputSpan output)
    -> std::expected<std::optional<ValidityBitmap>, std::string> {
    if (output.codes == nullptr || output.begin != range.begin || output.count != range.count) {
        return std::unexpected(
            "write_direct_categorical_field_range: output window shape mismatch");
    }
    std::vector<Mask> conditions;
    for (const ir::Expr* condition : plan.conditions) {
        auto mask = compute_mask(*condition, input, scalars, range);
        if (!mask.has_value()) {
            return std::unexpected(std::move(mask.error()));
        }
        conditions.push_back(std::move(*mask));
    }
    ValidityBitmap validity(range.count, true);
    bool any_invalid = false;
    for (std::size_t offset = 0; offset < range.count; ++offset) {
        const std::size_t row = range.begin + offset;
        std::size_t selected = plan.values.size();
        if (plan.kind == DirectValidityKind::Coalesce) {
            for (std::size_t index = 0; index < plan.values.size(); ++index) {
                const auto& value = plan.values[index];
                if (!value.is_null && (value.is_literal || !value.column->validity.has_value() ||
                                       (*value.column->validity)[row])) {
                    selected = index;
                    break;
                }
            }
        } else {
            selected = conditions.size();
            for (std::size_t index = 0; index < conditions.size(); ++index) {
                if (conditions[index].value[offset] != 0 &&
                    (!conditions[index].valid.has_value() || (*conditions[index].valid)[offset])) {
                    selected = index;
                    break;
                }
            }
        }
        if (selected >= plan.values.size() || plan.values[selected].is_null ||
            (!plan.values[selected].is_literal &&
             plan.values[selected].column->validity.has_value() &&
             !(*plan.values[selected].column->validity)[row])) {
            output.codes[offset] = 0;
            validity.set(offset, false);
            any_invalid = true;
            continue;
        }
        const auto& value = plan.values[selected];
        output.codes[offset] =
            value.is_literal
                ? plan.literal_codes[selected]
                : plan.remaps[selected][static_cast<std::size_t>(
                      std::get<Column<Categorical>>(*value.column->column).code_at(row))];
    }
    return any_invalid ? std::optional<ValidityBitmap>{std::move(validity)}
                       : std::optional<ValidityBitmap>{};
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

auto try_plan_direct_numeric_tree(const ir::Expr& expr, const PredicateInput& input,
                                  const ScalarRegistry* scalars)
    -> std::optional<DirectNumericTreePlan> {
    DirectNumericTreePlan plan;
    plan.nodes.reserve(8);
    const auto root = compile_numeric_tree(expr, input, scalars, plan.nodes);
    if (!root.has_value()) {
        return std::nullopt;
    }
    plan.root = *root;
    plan.type = plan.nodes[*root].type;
    return plan;
}

auto write_direct_numeric_tree_range(const DirectNumericTreePlan& plan,
                                     ::ibex::runtime::RowRange range, NumericOutputSpan output)
    -> bool {
    // Node column pointers are absolute-row, so a range reads its own rows and
    // writes the window the caller positioned at `range.begin`.
    if (plan.type == ExprType::Int) {
        if (output.ints == nullptr) {
            return false;
        }
        for (std::size_t offset = 0; offset < range.count; ++offset) {
            output.ints[offset] =
                eval_numeric_tree_int(plan.nodes.data(), plan.root, range.begin + offset);
        }
        return true;
    }
    if (output.doubles == nullptr) {
        return false;
    }
    for (std::size_t offset = 0; offset < range.count; ++offset) {
        output.doubles[offset] =
            eval_numeric_tree_double(plan.nodes.data(), plan.root, range.begin + offset);
    }
    return true;
}

auto plan_direct_field(const ir::Expr& expr, const PredicateInput& input,
                       const ScalarRegistry* scalars) -> DirectFieldRoute {
    DirectFieldRoute route;
    route.string = make_string_interpolation_plan(expr, input, scalars);
    if (route.string.has_value()) {
        return route;
    }
    route.categorical = try_plan_direct_categorical_field(expr, input, scalars);
    if (route.categorical.has_value()) {
        return route;
    }
    route.predicate = try_plan_direct_predicate_field(expr);
    if (route.predicate.has_value()) {
        return route;
    }
    route.validity = try_plan_direct_validity_field(expr, input, scalars);
    if (route.validity.has_value()) {
        return route;
    }
    route.fixed_width = try_plan_direct_fixed_width_field(expr, input, scalars);
    if (route.fixed_width.has_value()) {
        return route;
    }
    route.numeric_tree = try_plan_direct_numeric_tree(expr, input, scalars);
    return route;
}

namespace {

/// Merge the per-range validity bitmaps a split field produced. Each range owns
/// a dense bitmap of its own rows, so this is positional and needs no lock; a
/// range that produced none was wholly valid.
auto merge_range_validity(
    std::span<const std::expected<std::optional<ValidityBitmap>, std::string>> pieces,
    std::size_t rows, std::size_t grain) -> std::optional<ValidityBitmap> {
    if (!std::ranges::any_of(pieces, [](const auto& piece) { return piece->has_value(); })) {
        return std::nullopt;
    }
    ValidityBitmap merged(rows, true);
    for (std::size_t index = 0; index < pieces.size(); ++index) {
        if (!pieces[index]->has_value()) {
            continue;
        }
        const std::size_t begin = index * grain;
        const ValidityBitmap& source = **pieces[index];
        for (std::size_t offset = 0; offset < source.size(); ++offset) {
            merged.set(begin + offset, source[offset]);
        }
    }
    return merged;
}

}  // namespace

auto evaluate_field_windows(const ir::Expr& expr, const DirectFieldRoute& route,
                            const PredicateInput& input, std::optional<ExprType> inferred,
                            const ScalarRegistry* scalars, const ExecutionContext& exec,
                            const DirectFieldRangeWriter* fallback,
                            std::atomic<bool>* direct_numeric_writer)
    -> std::expected<std::optional<ComputedColumn>, std::string> {
    if (!route.has_plan() && fallback == nullptr) {
        return std::optional<ComputedColumn>{};
    }
    // Reentrancy: a fused pipeline operator calls its update from a worker
    // thread. Submitting from there deadlocks the pool (WorkerPool::submit
    // aborts rather than let it happen), and that morsel is already one
    // worker's share -- splitting it again would only oversubscribe.
    if (!exec.parallel || on_worker_pool_thread()) {
        return std::optional<ComputedColumn>{};
    }
    const std::size_t rows = input.rows();
    if (rows < exec.parallel_min_rows) {
        return std::optional<ComputedColumn>{};
    }
    // Same derivation as an pipeline's, so the two parallel paths partition
    // alike. Note a zero `parallel_grain` now means "derive", not "one row per
    // morsel" — reading it directly here would have split a 20M-row update into
    // 20M tasks.
    const std::size_t grain = morsel_grain(exec, rows);
    if (grain == 0) {
        return std::optional<ComputedColumn>{};
    }
    const std::size_t morsels = (rows + grain - 1) / grain;
    auto& pool = process_worker_pool();
    const std::size_t threads =
        std::min(morsels, exec.parallel_threads != 0 ? exec.parallel_threads : pool.size());
    if (threads < 2 || morsels < 2) {
        return std::optional<ComputedColumn>{};
    }
    const auto range_of = [&](std::size_t index) {
        const std::size_t begin = index * grain;
        return ::ibex::runtime::RowRange{.begin = begin, .count = std::min(grain, rows - begin)};
    };

    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
    }

    if (route.string.has_value()) {
        // The count pass supplies both the byte prefix for each window and the
        // dense validity each writer must honour.  Invalid rows deliberately
        // contribute no bytes, matching the reference evaluator's empty
        // payload and avoiding reads of their undefined source payloads.
        std::vector<std::expected<std::optional<ValidityBitmap>, std::string>> pieces(morsels);
        std::vector<std::expected<std::uint32_t, std::string>> bytes(morsels);
        std::atomic<std::size_t> cursor{0};
        auto count_batch = pool.submit(threads, [&](std::size_t) {
            while (true) {
                const std::size_t index = cursor.fetch_add(1, std::memory_order_relaxed);
                if (index >= morsels) {
                    return;
                }
                const auto range = range_of(index);
                auto validity = collect_expr_validity(expr, input, range);
                const auto count = string_interpolation_bytes(
                    *route.string, range, validity.has_value() ? &*validity : nullptr);
                if (!count.has_value()) {
                    pieces[index] = std::unexpected(
                        "evaluate_field_windows: string output exceeds uint32 offsets");
                    bytes[index] = std::unexpected(
                        "evaluate_field_windows: string output exceeds uint32 offsets");
                    continue;
                }
                pieces[index] = std::move(validity);
                bytes[index] = *count;
            }
        });
        count_batch.wait();
        for (std::size_t index = 0; index < morsels; ++index) {
            if (!pieces[index].has_value()) {
                return std::unexpected(pieces[index].error());
            }
            if (!bytes[index].has_value()) {
                return std::unexpected(bytes[index].error());
            }
        }

        std::vector<std::uint32_t> char_prefix(morsels + 1, 0);
        for (std::size_t index = 0; index < morsels; ++index) {
            if (*bytes[index] > std::numeric_limits<std::uint32_t>::max() - char_prefix[index]) {
                return std::unexpected(
                    "evaluate_field_windows: string output exceeds uint32 offsets");
            }
            char_prefix[index + 1] = char_prefix[index] + *bytes[index];
        }
        Column<std::string> strings;
        strings.resize_for_gather(rows, char_prefix.back());
        auto* offsets = strings.offsets_data();
        for (std::size_t index = 0; index < morsels; ++index) {
            offsets[index * grain] = char_prefix[index];
        }
        offsets[rows] = char_prefix.back();

        std::atomic<bool> write_failed{false};
        cursor.store(0, std::memory_order_relaxed);
        auto write_batch = pool.submit(threads, [&](std::size_t) {
            while (true) {
                const std::size_t index = cursor.fetch_add(1, std::memory_order_relaxed);
                if (index >= morsels) {
                    return;
                }
                const auto range = range_of(index);
                if (!write_string_interpolation(
                        *route.string, range,
                        pieces[index]->has_value() ? &**pieces[index] : nullptr,
                        StringOutputSpan{.offsets = offsets,
                                         .chars = strings.chars_data(),
                                         .begin = range.begin,
                                         .count = range.count,
                                         .char_base = char_prefix[index]})) {
                    write_failed.store(true, std::memory_order_relaxed);
                }
            }
        });
        write_batch.wait();
        if (write_failed.load(std::memory_order_relaxed)) {
            return std::unexpected("evaluate_field_windows: string window shape mismatch");
        }
        return std::optional<ComputedColumn>{
            ComputedColumn{.column = ColumnValue{std::move(strings)},
                           .validity = merge_range_validity(pieces, rows, grain)}};
    }

    if (route.categorical.has_value()) {
        using Code = Column<Categorical>::code_type;
        std::vector<Code> codes(rows, 0);
        std::vector<std::expected<std::optional<ValidityBitmap>, std::string>> pieces(morsels);
        std::atomic<std::size_t> cursor{0};
        auto batch = pool.submit(threads, [&](std::size_t) {
            while (true) {
                const std::size_t index = cursor.fetch_add(1, std::memory_order_relaxed);
                if (index >= morsels) {
                    return;
                }
                const auto range = range_of(index);
                auto validity =
                    write_direct_categorical_field_range(*route.categorical, input, range, scalars,
                                                         {.codes = codes.data() + range.begin,
                                                          .begin = range.begin,
                                                          .count = range.count});
                if (!validity.has_value()) {
                    pieces[index] = std::unexpected(std::move(validity.error()));
                    continue;
                }
                pieces[index] = std::move(*validity);
            }
        });
        batch.wait();
        for (auto& piece : pieces) {
            if (!piece.has_value()) {
                return std::unexpected(piece.error());
            }
        }
        Column<Categorical> categorical(route.categorical->dictionary, route.categorical->index,
                                        std::move(codes));
        return std::optional<ComputedColumn>{
            ComputedColumn{.column = ColumnValue{std::move(categorical)},
                           .validity = merge_range_validity(pieces, rows, grain)}};
    }

    // The output representation: the update's inferred type when the caller has
    // one, else the selected plan's own output kind. They agree wherever both
    // exist — a plan is only selected for an expression whose result it can
    // name — and the plan is the only answer a chunk caller has.
    const auto plan_kind = [&]() -> std::optional<ExprType> {
        if (route.predicate.has_value()) {
            return ExprType::Bool;
        }
        if (route.validity.has_value()) {
            return route.validity->numeric_kind == FixedWidthNumericKind::Int ? ExprType::Int
                                                                              : ExprType::Double;
        }
        if (route.fixed_width.has_value()) {
            return route.fixed_width->numeric_kind == FixedWidthNumericKind::Int ? ExprType::Int
                                                                                 : ExprType::Double;
        }
        if (route.numeric_tree.has_value()) {
            return route.numeric_tree->type;
        }
        return std::nullopt;
    }();
    const auto destination = inferred.has_value() ? inferred : plan_kind;
    if (!destination.has_value() ||
        (*destination != ExprType::Int && *destination != ExprType::Double &&
         *destination != ExprType::Bool)) {
        return std::optional<ComputedColumn>{};
    }

    // Allocate the destination up front and let each worker copy its own morsel
    // into it. Merging the pieces serially after the barrier was a second full
    // pass over the result — at 8M rows, 64MB read plus 64MB write — and a
    // memory-bound kernel like `abs(price)` only moves ~128MB in total, so the
    // merge cost what the threads saved and then some (measured: 8.8ms
    // parallel vs 5.8ms serial). Copying inside the task does it while the
    // piece is still hot in that worker's cache, and does it in parallel.
    ColumnValue out = *destination == ExprType::Int      ? ColumnValue{Column<std::int64_t>{}}
                      : *destination == ExprType::Double ? ColumnValue{Column<double>{}}
                                                         : ColumnValue{Column<bool>{}};
    std::int64_t* dst_int = nullptr;
    double* dst_double = nullptr;
    std::uint64_t* dst_bool_words = nullptr;
    if (auto* ints = std::get_if<Column<std::int64_t>>(&out)) {
        ints->resize_for_overwrite(rows);
        dst_int = ints->data();
    } else if (auto* doubles = std::get_if<Column<double>>(&out)) {
        doubles->resize_for_overwrite(rows);
        dst_double = doubles->data();
    } else {
        auto& bools = std::get<Column<bool>>(out);
        bools.resize(rows);
        dst_bool_words = bools.words_data();
    }

    std::atomic<bool> local_direct_numeric{false};
    std::atomic<bool>& used_direct_numeric =
        direct_numeric_writer != nullptr ? *direct_numeric_writer : local_direct_numeric;

    // One slot per morsel, each written by exactly one worker — no lock, and
    // the result order is positional rather than dependent on completion order.
    // Only the validity survives the task; the values are already in `out`.
    std::vector<std::expected<std::optional<ValidityBitmap>, std::string>> pieces(morsels);
    std::atomic<std::size_t> cursor{0};
    auto batch = pool.submit(threads, [&](std::size_t) {
        while (true) {
            const std::size_t index = cursor.fetch_add(1, std::memory_order_relaxed);
            if (index >= morsels) {
                return;
            }
            const auto range = range_of(index);
            const std::size_t begin = range.begin;
            if (dst_bool_words != nullptr) {
                if (!route.predicate.has_value()) {
                    pieces[index] =
                        std::unexpected("evaluate_field_windows: missing native predicate plan");
                    continue;
                }
                auto validity = write_direct_predicate_range(
                    *route.predicate, input, range, scalars,
                    BoolOutputSpan{.words = dst_bool_words, .begin = begin, .count = range.count});
                if (!validity.has_value()) {
                    pieces[index] = std::unexpected(std::move(validity.error()));
                    continue;
                }
                pieces[index] = std::move(*validity);
                continue;
            }
            const NumericOutputSpan window{
                .ints = dst_int != nullptr ? dst_int + begin : nullptr,
                .doubles = dst_double != nullptr ? dst_double + begin : nullptr};
            const bool validity_destination =
                route.validity.has_value() &&
                ((route.validity->numeric_kind == FixedWidthNumericKind::Int &&
                  dst_int != nullptr) ||
                 (route.validity->numeric_kind == FixedWidthNumericKind::Double &&
                  dst_double != nullptr));
            if (validity_destination) {
                auto validity = write_direct_validity_field_range(*route.validity, input, range,
                                                                  scalars, {.numeric = window});
                if (!validity.has_value()) {
                    pieces[index] = std::unexpected(std::move(validity.error()));
                    continue;
                }
                pieces[index] = std::move(*validity);
                continue;
            }
            const bool fixed_width_destination =
                route.fixed_width.has_value() &&
                ((route.fixed_width->numeric_kind == FixedWidthNumericKind::Int &&
                  dst_int != nullptr) ||
                 (route.fixed_width->numeric_kind == FixedWidthNumericKind::Double &&
                  dst_double != nullptr));
            if (fixed_width_destination &&
                write_direct_field_range(*route.fixed_width, input, range, scalars,
                                         {.numeric = window})) {
                pieces[index] = collect_expr_validity(expr, input, range);
                if (route.fixed_width->kind == DirectFieldKind::NumericBinary) {
                    used_direct_numeric.store(true, std::memory_order_relaxed);
                }
                continue;
            }
            const bool tree_destination =
                route.numeric_tree.has_value() &&
                ((route.numeric_tree->type == ExprType::Int && dst_int != nullptr) ||
                 (route.numeric_tree->type == ExprType::Double && dst_double != nullptr));
            if (tree_destination &&
                write_direct_numeric_tree_range(*route.numeric_tree, range, window)) {
                pieces[index] = collect_expr_validity(expr, input, range);
                used_direct_numeric.store(true, std::memory_order_relaxed);
                continue;
            }
            if (fallback == nullptr) {
                pieces[index] = std::unexpected(
                    "evaluate_field_windows: no direct plan covers this range and the caller "
                    "supplied no writer");
                continue;
            }
            pieces[index] = (*fallback)(range, window);
        }
    });
    batch.wait();

    if (used_direct_numeric.load(std::memory_order_relaxed) && exec.parallel_stats != nullptr) {
        exec.parallel_stats->parallel_direct_numeric_fields.fetch_add(1, std::memory_order_relaxed);
    }
    for (auto& piece : pieces) {
        if (!piece.has_value()) {
            return std::unexpected(piece.error());
        }
    }
    return std::optional<ComputedColumn>{ComputedColumn{
        .column = std::move(out), .validity = merge_range_validity(pieces, rows, grain)}};
}

namespace {

/// One field through the direct vocabulary in parallel mode: split across
/// worker ranges when `evaluate_field_windows` accepts the split, else the same
/// whole-chunk write the serial route uses. Declines by returning nullopt,
/// which leaves the original update to the table evaluator.
///
/// The route is required rather than optional here, unlike in serial mode. A
/// field the route does not name (the legacy null-handling arms, an expression
/// only the general evaluator reaches) has no range writer on this side of the
/// seam, and the
/// table evaluator can still split it through its own fallback hook -- so
/// declining is how such a field keeps its parallelism, not how it loses it.
auto try_direct_update_field_parallel(const Chunk& input, const std::vector<ir::FieldSpec>& fields,
                                      const ScalarRegistry* scalars, const ExecutionContext& exec)
    -> std::optional<Chunk> {
    if (auto output = try_metadata_alias_update(input, fields); output.has_value()) {
        return output;
    }
    if (fields.size() != 1) {
        return std::nullopt;
    }
    if (auto output = try_literal_update(input, fields); output.has_value()) {
        return output;  // a broadcast has no ranges worth splitting
    }
    const ChunkView view(input);
    const auto source = predicate_input(view);
    const auto& expr = fields.front().expr;
    const auto route = plan_direct_field(expr, source, scalars);
    if (!route.has_plan()) {
        return std::nullopt;
    }
    auto windows =
        evaluate_field_windows(expr, route, source, std::nullopt, scalars, exec, nullptr);
    if (!windows.has_value()) {
        // The established evaluator owns this diagnostic: it reaches the same
        // expression and has the error contract the tests pin.
        return std::nullopt;
    }
    if (!windows->has_value()) {
        // The split was declined (too few rows, one morsel, a worker thread),
        // not attempted. The chunk is still ours to write directly.
        return try_direct_update_field(input, fields, scalars);
    }
    return write_direct_update(input, fields.front().alias,
                               std::make_shared<ColumnValue>(std::move((*windows)->column)),
                               std::move((*windows)->validity));
}

}  // namespace

auto update_row_local_chunk(Chunk input, const std::vector<ir::FieldSpec>& fields,
                            const ScalarRegistry* scalars, const ExternRegistry* externs,
                            const ExecutionContext& exec) -> std::expected<Chunk, std::string> {
    // An update's fields are ordered: a later field reads the chunk produced by
    // every earlier one.  Keep the direct route all-or-nothing, though.  If a
    // field is outside its narrow vocabulary, discard this tentative chunk and
    // let update_table evaluate the original complete update.
    // Counted where the chunk kernel keeps the work, so the bridge below is
    // observable by its absence -- see `chunk_direct_updates`.
    const auto direct = [&](Chunk output, std::size_t field_count) {
        if (exec.parallel_stats != nullptr) {
            exec.parallel_stats->chunk_direct_updates.fetch_add(field_count,
                                                                std::memory_order_relaxed);
        }
        return output;
    };
    // Fold the fields one at a time, each planned against the chunk the
    // previous one produced. `attempt` is the per-field route, so serial and
    // parallel mode differ only in which one they fold with -- the ordering and
    // the all-or-nothing rule are the same in both.
    const auto fold_fields = [&](auto&& attempt) -> std::optional<Chunk> {
        Chunk current = input;
        for (const auto& field : fields) {
            const std::vector<ir::FieldSpec> one_field{field};
            auto next = attempt(current, one_field);
            if (!next.has_value()) {
                return std::nullopt;
            }
            current = std::move(*next);
        }
        return current;
    };
    if (fields.size() > 1) {
        auto folded =
            exec.parallel
                ? fold_fields([&](const Chunk& current, const std::vector<ir::FieldSpec>& one) {
                      return try_direct_update_field_parallel(current, one, scalars, exec);
                  })
                : fold_fields([&](const Chunk& current, const std::vector<ir::FieldSpec>& one) {
                      return try_direct_update_field(current, one, scalars);
                  });
        if (folded.has_value()) {
            return direct(std::move(*folded), fields.size());
        }
    }
    if (!exec.parallel) {
        if (auto output = try_direct_update_field(input, fields, scalars); output.has_value()) {
            return direct(std::move(*output), 1);
        }
    } else if (auto output = try_direct_update_field_parallel(input, fields, scalars, exec);
               output.has_value()) {
        return direct(std::move(*output), 1);
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
