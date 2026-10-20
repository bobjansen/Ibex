// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <expected>
#include <functional>
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

/// One unit's rows, produced on the worker that runs the unit's chain. Each
/// worker gets its own producer (`UnitProducerFactory`), so a producer may keep
/// mutable state; `exec` is the context that worker runs under.
using UnitProducer = std::function<std::expected<Table, std::string>(std::size_t unit,
                                                                     const ExecutionContext& exec)>;
using UnitProducerFactory = std::function<UnitProducer()>;

/// A streaming pipeline over `units` whose rows come from `make_producer`
/// rather than a scan decode: the same workers, ordered ring and dictionary
/// remap as the pipelined scan, emitting one chunk per non-empty unit in unit
/// order. On a pool with no worker to spare the calling thread runs every
/// unit, so the emitted chunks are the same at every core count.
[[nodiscard]] auto build_unit_pipeline(std::vector<SourceUnit> units,
                                       const UnitProducerFactory& make_producer,
                                       const ExecutionContext& exec) -> OperatorPtr;

/// Wraps one worker chain in a further operator, under that chain's context.
using WorkerStepAttach =
    std::function<OperatorPtr(OperatorPtr chain, const ExecutionContext& exec)>;

/// Append `attach` to every worker chain of `op` when `op` is a streaming
/// pipeline that has not started (and carries no worker sink). False leaves
/// `op` untouched and the caller runs the step above it as before.
[[nodiscard]] auto try_attach_worker_step(Operator& op, const WorkerStepAttach& attach) -> bool;

/// Whether `try_attach_worker_step` would accept a step on `op` now. A join
/// asks before it decides its build side: a left that can take the probe is
/// worth building the right for, without draining the left to measure it.
[[nodiscard]] auto is_attachable_worker_pipeline(const Operator& op) -> bool;

[[nodiscard]] auto has_multi_unit_deferred_scan(const ir::Node& node, const TableRegistry& registry,
                                                const ExecutionContext& exec) -> bool;

}  // namespace ibex::runtime::pipeline_executor_detail
