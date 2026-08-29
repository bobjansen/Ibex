// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <expected>
#include <string>

#include "physical_plan.hpp"

namespace ibex::runtime {

/// Validate and execute a plan whose root is owned by the physical executor.
[[nodiscard]] auto build_migrated_physical_operator(
    const physical::Plan& plan, const ir::Node& node, const TableRegistry& registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    ModelResult* model_out) -> std::expected<OperatorPtr, std::string>;

/// Construction primitives supplied by the operator and pipeline families.
/// The physical executor owns dispatch; these functions keep concrete operator
/// types in the translation units that implement them.
namespace physical_executor_detail {

[[nodiscard]] auto resolved_join_parallelism(const ExecutionContext& exec)
    -> physical::JoinParallelism;

[[nodiscard]] auto build_physical_map_step(
    const physical::Plan& plan, std::size_t index, const TableRegistry& registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    ModelResult* model_out) -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_join(
    const physical::Plan& plan, const ir::Node& node, const TableRegistry& registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    ModelResult* model_out) -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_aggregate(
    const physical::Plan& plan, const ir::Node& node, const TableRegistry& registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    ModelResult* model_out) -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_order(
    const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_head(
    const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_tail(
    const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_topk(
    const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_filter_head_tail(
    const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_physical_distinct(
    const physical::Plan& plan, const ir::Node& node, const TableRegistry& registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    ModelResult* model_out) -> std::expected<OperatorPtr, std::string>;

}  // namespace physical_executor_detail
}  // namespace ibex::runtime
