// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// extern_call.cpp — extern-function invocation: `invoke_extern_call` (scalar
// and chunked-table-returning externs) and `execute_program_preamble` (the
// leading extern-call statements a Program runs before its plan). Split out of
// runtime_entry.cpp (formerly chunked.cpp); declared in interpreter_internal.hpp.

#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "interpreter_internal.hpp"

namespace ibex::runtime {

namespace {

// An extern's arguments where there is no row to read: each is a scalar
// expression. Externs take null-free arguments: a null is rejected here, in
// front of the call, rather than reaching extern code.
auto eval_extern_args(const std::vector<ir::Expr>& exprs, const ScalarRegistry* scalars,
                      const ExternRegistry* externs) -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(exprs.size());
    for (const auto& arg : exprs) {
        auto value = evaluate_scalar_expr(arg, scalars, externs);
        if (!value.has_value()) {
            return std::unexpected(std::move(value.error()));
        }
        if (is_null_scalar(*value)) {
            return std::unexpected("null argument in extern function call");
        }
        args.push_back(std::move(*value));
    }
    return args;
}

// The argument columns of an extern call over a field's rows, from `begin`.
auto eval_arg_columns(const std::vector<ir::ExprPtr>& exprs, std::size_t begin, const Table& input,
                      RowRange range, const ColumnEvalCtx& ctx)
    -> std::expected<std::vector<ColResult>, std::string> {
    std::vector<ColResult> columns;
    columns.reserve(exprs.size() - std::min(begin, exprs.size()));
    for (std::size_t i = begin; i < exprs.size(); ++i) {
        auto column = eval_value_vec(*exprs[i], input, ctx.scalars, range, ctx.window,
                                     ctx.window_aligned, ctx.externs);
        if (!column) {
            return std::unexpected(std::move(column.error()));
        }
        columns.push_back(std::move(*column));
    }
    return columns;
}

// Row `row`'s arguments, read from their columns.
auto row_args(const std::vector<ColResult>& columns, std::size_t row, std::string_view callee)
    -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(columns.size());
    for (const auto& column : columns) {
        auto value = cell_value(column, row);
        if (is_null_scalar(value)) {
            return std::unexpected(std::string(callee) + ": null argument in extern function call");
        }
        args.push_back(std::move(value));
    }
    return args;
}

// A table-producing extern call, as a consumer's table argument.
auto call_table_extern(const ExternFunction& fn, const std::string& callee, const ExternArgs& args)
    -> std::expected<Table, std::string> {
    if (fn.chunked_table_func) {
        auto source = fn.chunked_table_func(args);
        if (source) {
            return materialize_operator(std::move(*source));
        }
    }
    if (!fn.func) {
        return std::unexpected("extern table function has no materializing implementation: " +
                               callee);
    }
    auto result = fn.func(args);
    if (!result) {
        return std::unexpected(std::move(result.error()));
    }
    if (auto* table = std::get_if<Table>(&*result)) {
        return std::move(*table);
    }
    return std::unexpected(
        "extern function returned a non-table value where a table was required: " + callee);
}

}  // namespace

auto eval_extern_values(const ir::CallExpr& call, const Table& input, RowRange range,
                        const ColumnEvalCtx& ctx)
    -> std::expected<std::vector<ScalarValue>, std::string> {
    if (ctx.externs == nullptr) {
        return std::unexpected("extern call with no registry: " + call.callee);
    }
    const auto* fn = ctx.externs->find(call.callee);
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + call.callee);
    }
    if (fn->kind != ExternReturnKind::Scalar) {
        return std::unexpected("function not usable in expression: " + call.callee);
    }
    // A table first argument is itself an extern call producing a table, whose
    // own arguments are read per row like the consumer's.
    const ExternFunction* table_fn = nullptr;
    const ir::CallExpr* table_call = nullptr;
    std::vector<ColResult> table_args;
    if (fn->first_arg_is_table) {
        if (call.args.empty() || !fn->table_consumer_func) {
            return std::unexpected(call.callee + " requires a table first argument");
        }
        table_call = std::get_if<ir::CallExpr>(&call.args.front()->node);
        if (table_call == nullptr) {
            return std::unexpected("table argument to an extern consumer must be an extern call");
        }
        table_fn = ctx.externs->find(table_call->callee);
        if (table_fn == nullptr) {
            return std::unexpected("unknown extern function: " + table_call->callee);
        }
        if (table_fn->kind != ExternReturnKind::Table || table_fn->first_arg_is_table) {
            return std::unexpected("function does not produce a table: " + table_call->callee);
        }
        auto columns = eval_arg_columns(table_call->args, 0, input, range, ctx);
        if (!columns) {
            return std::unexpected(std::move(columns.error()));
        }
        table_args = std::move(*columns);
    } else if (!fn->func) {
        return std::unexpected("extern scalar function has no implementation: " + call.callee);
    }
    auto columns = eval_arg_columns(call.args, fn->first_arg_is_table ? 1 : 0, input, range, ctx);
    if (!columns) {
        return std::unexpected(std::move(columns.error()));
    }

    std::vector<ScalarValue> values;
    values.reserve(range.count);
    for (std::size_t row = 0; row < range.count; ++row) {
        auto args = row_args(*columns, row, call.callee);
        if (!args) {
            return std::unexpected(std::move(args.error()));
        }
        std::expected<ExternValue, std::string> result =
            std::unexpected("extern function has no implementation: " + call.callee);
        if (table_fn != nullptr) {
            auto table_arguments = row_args(table_args, row, table_call->callee);
            if (!table_arguments) {
                return std::unexpected(std::move(table_arguments.error()));
            }
            auto table = call_table_extern(*table_fn, table_call->callee, *table_arguments);
            if (!table) {
                return std::unexpected(std::move(table.error()));
            }
            result = fn->table_consumer_func(*table, *args);
        } else {
            result = fn->func(*args);
        }
        if (!result) {
            return std::unexpected(std::move(result.error()));
        }
        if (auto* scalar = std::get_if<ScalarValue>(&*result)) {
            values.push_back(*scalar);
        } else if (std::holds_alternative<ResourcePtr>(*result)) {
            return std::unexpected("a resource cannot be used in a query expression: " +
                                   call.callee);
        } else {
            return std::unexpected("extern function returned table in expression: " + call.callee);
        }
    }
    return values;
}

auto invoke_extern_call(const ir::ExternCallNode& ec, const ScalarRegistry* scalars,
                        const ExternRegistry* externs) -> std::expected<ExternValue, std::string> {
    if (externs == nullptr) {
        return std::unexpected("extern call with no registry: " + ec.callee());
    }
    const auto* fn = externs->find(ec.callee());
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + ec.callee());
    }
    if (fn->first_arg_is_table) {
        return std::unexpected("extern function requires a table input: " + ec.callee());
    }
    auto args = eval_extern_args(ec.args(), scalars, externs);
    if (!args.has_value()) {
        return std::unexpected(std::move(args.error()));
    }
    if (fn->kind == ExternReturnKind::Table && fn->chunked_table_func) {
        auto source = fn->chunked_table_func(args.value());
        if (source.has_value()) {
            auto materialized = materialize_operator(std::move(source.value()));
            if (!materialized.has_value()) {
                return std::unexpected(std::move(materialized.error()));
            }
            return ExternValue{std::move(materialized.value())};
        }
    }
    auto result = fn->func(args.value());
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return result;
}

auto execute_program_preamble(const std::vector<ir::NodePtr>& preamble,
                              const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<void, std::string> {
    for (const auto& node : preamble) {
        if (node->kind() != ir::NodeKind::ExternCall) {
            return std::unexpected("program preamble only supports extern calls");
        }
        const auto& ec = ir::node_cast<ir::ExternCallNode>(*node);
        auto result = invoke_extern_call(ec, scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
    }
    return {};
}

}  // namespace ibex::runtime
