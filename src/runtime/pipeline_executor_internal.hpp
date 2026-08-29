// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <expected>
#include <optional>
#include <string>
#include <vector>

#include "execution_profile_internal.hpp"
#include "join_chunked_internal.hpp"
#include "physical_plan.hpp"

namespace ibex::runtime::pipeline_executor_detail {

[[nodiscard]] auto build_row_local_map_operator(const ir::Node& node, OperatorPtr child,
                                                const ScalarRegistry* scalars,
                                                const ExternRegistry* externs,
                                                const ExecutionContext& exec,
                                                bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto probe_morsel_workers(const Table& input, const ExecutionContext& exec)
    -> std::size_t;
[[nodiscard]] auto build_probe_morsel_pipeline(Table input, const JoinProbeFactory& probe,
                                               std::size_t workers, const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto build_map_pipeline_parallel(const physical::Plan& plan,
                                               const TableRegistry& registry,
                                               const ScalarRegistry* scalars,
                                               const ExternRegistry* externs,
                                               const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto make_pipelined_stage_if(OperatorPtr child, bool eligible,
                                           const ExecutionContext& exec,
                                           ExecutionProfileEntry* entry) -> OperatorPtr;

[[nodiscard]] auto scan_pipeline_worker_count(std::size_t unit_count) -> std::size_t;
[[nodiscard]] auto make_deferred_scan_source(const DeferredScan& scan,
                                             std::vector<SourceUnit> units,
                                             const ExecutionContext& exec) -> OperatorPtr;
[[nodiscard]] auto build_pipelined_scan(const std::vector<MapStep>& operators,
                                        bool count_as_pipeline, const DeferredScan& scan,
                                        std::vector<SourceUnit> units,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto has_multi_unit_deferred_scan(const ir::Node& node, const TableRegistry& registry,
                                                const ExecutionContext& exec) -> bool;

}  // namespace ibex::runtime::pipeline_executor_detail
