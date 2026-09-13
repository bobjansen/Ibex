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
#include <ibex/runtime/pipeline.hpp>

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <memory>
#include <pdqsort.h>
#include <robin_hood.h>
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

auto eval_extern_args(const std::vector<ir::Expr>& exprs, const ScalarRegistry* scalars,
                      const ExternRegistry* externs) -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(exprs.size());
    for (const auto& arg : exprs) {
        auto val = eval_expr(arg, Table{}, 0, scalars, externs);
        if (!val.has_value()) {
            return std::unexpected(std::move(val.error()));
        }
        // Externs take null-free ScalarValue arguments: a null is rejected here,
        // in front of the call, rather than reaching extern code. Static typing
        // catches most; this is the runtime backstop.
        auto scalar = scalar_from_expr(val.value());
        if (is_null_scalar(scalar)) {
            return std::unexpected("null argument in extern function call");
        }
        args.push_back(std::move(scalar));
    }
    return args;
}

auto eval_extern_args(const std::vector<ir::ExprPtr>& exprs, std::size_t begin, const Table& input,
                      std::size_t row, const ScalarRegistry* scalars, const ExternRegistry* externs,
                      std::string_view callee) -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(exprs.size() - begin);
    for (std::size_t i = begin; i < exprs.size(); ++i) {
        auto value = eval_expr(*exprs[i], input, row, scalars, externs);
        if (!value) {
            return std::unexpected(std::move(value.error()));
        }
        auto scalar = scalar_from_expr(*value);
        if (is_null_scalar(scalar)) {
            return std::unexpected(std::string(callee) + ": null argument in extern function call");
        }
        args.push_back(std::move(scalar));
    }
    return args;
}

auto eval_extern_table_expr(const ir::Expr& expr, const Table& input, std::size_t row,
                            const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string> {
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr) {
        return std::unexpected("table argument to an extern consumer must be an extern call");
    }
    const auto* fn = externs->find(call->callee);
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + call->callee);
    }
    if (fn->kind != ExternReturnKind::Table || fn->first_arg_is_table) {
        return std::unexpected("function does not produce a table: " + call->callee);
    }
    auto args = eval_extern_args(call->args, 0, input, row, scalars, externs, call->callee);
    if (!args) {
        return std::unexpected(std::move(args.error()));
    }
    if (fn->chunked_table_func) {
        auto source = fn->chunked_table_func(*args);
        if (source) {
            return materialize_operator(std::move(*source));
        }
    }
    if (!fn->func) {
        return std::unexpected("extern table function has no materializing implementation: " +
                               call->callee);
    }
    auto result = fn->func(*args);
    if (!result) {
        return std::unexpected(std::move(result.error()));
    }
    if (auto* table = std::get_if<Table>(&*result)) {
        return std::move(*table);
    }
    return std::unexpected("extern function returned a scalar where a table was required: " +
                           call->callee);
}

}  // namespace

auto eval_extern_expr(const ir::CallExpr& call, const Table& input, std::size_t row,
                      const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<ExprValue, std::string> {
    if (externs == nullptr) {
        return std::unexpected("extern call with no registry: " + call.callee);
    }
    const auto* fn = externs->find(call.callee);
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + call.callee);
    }
    if (fn->kind != ExternReturnKind::Scalar) {
        return std::unexpected("function not usable in expression: " + call.callee);
    }

    std::expected<ExternValue, std::string> result =
        std::unexpected("extern function has no implementation: " + call.callee);
    if (fn->first_arg_is_table) {
        if (call.args.empty() || !fn->table_consumer_func) {
            return std::unexpected(call.callee + " requires a table first argument");
        }
        auto table = eval_extern_table_expr(*call.args.front(), input, row, scalars, externs);
        if (!table) {
            return std::unexpected(std::move(table.error()));
        }
        auto args = eval_extern_args(call.args, 1, input, row, scalars, externs, call.callee);
        if (!args) {
            return std::unexpected(std::move(args.error()));
        }
        result = fn->table_consumer_func(*table, *args);
    } else {
        if (!fn->func) {
            return std::unexpected("extern scalar function has no implementation: " + call.callee);
        }
        auto args = eval_extern_args(call.args, 0, input, row, scalars, externs, call.callee);
        if (!args) {
            return std::unexpected(std::move(args.error()));
        }
        result = fn->func(*args);
    }
    if (!result) {
        return std::unexpected(std::move(result.error()));
    }
    if (auto* scalar = std::get_if<ScalarValue>(&*result)) {
        return expr_from_scalar(*scalar);
    }
    return std::unexpected("extern function returned table in expression: " + call.callee);
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
