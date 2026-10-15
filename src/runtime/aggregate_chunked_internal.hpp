// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "aggregate_prefilter.hpp"
#include "physical_plan.hpp"
#include "scan_worker_sink_internal.hpp"

namespace ibex::ir {
struct AggSpec;
struct ColumnRef;
}  // namespace ibex::ir

namespace ibex::runtime {

/// How many times, process-wide, an aggregate partitioned its group discovery
/// on ONE core -- for cache locality, run inline, so not a fan-out and not in
/// `ParallelPipelineStats::parallel_aggregate_partitions`. For tests.
[[nodiscard]] auto single_core_partitioned_discoveries() -> std::atomic<std::uint64_t>&;

/// How many chunks, process-wide, an aggregate grouped on its packed key
/// while some key column carried a validity bitmap. For tests: the generic
/// path gives the same answer, so only this tells the two apart.
[[nodiscard]] auto packed_nullable_key_chunks() -> std::atomic<std::uint64_t>&;

/// How many Categorical key columns, process-wide, an aggregate's key store
/// began holding as codes alone, its text read through the dictionary. For
/// tests: storing the text too gives the same answer.
[[nodiscard]] auto code_only_key_columns() -> std::atomic<std::uint64_t>&;

/// Construct the adaptive sorted/hash aggregate implementation. Kept as one
/// factory boundary so the complete aggregate family can live in its own
/// translation unit without exposing its state types to the pipeline builder.
[[nodiscard]] auto make_chunked_aggregate_operator(
    OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
    const std::vector<ir::AggSpec>* aggregations, const ExecutionContext& exec,
    physical::AggregateParallelism parallelism,
    std::optional<physical::AggregateColumnMapping> columns, AggregatePrefilter prefilter = {},
    const std::shared_ptr<ScanWorkerSink>& scan_sink = nullptr) -> OperatorPtr;

/// A sink the aggregate can use to accumulate on its input's scan workers.
/// Offer it (`ScanWorkerSinkOffer`) while building the aggregate's input, and
/// pass it to `make_chunked_aggregate_operator`; unused if nothing takes it.
[[nodiscard]] auto make_aggregate_scan_worker_sink(
    const std::vector<ir::ColumnRef>* group_by, const std::vector<ir::AggSpec>* aggregations,
    const ExecutionContext& exec, physical::AggregateParallelism parallelism,
    std::optional<physical::AggregateColumnMapping> columns) -> std::shared_ptr<ScanWorkerSink>;

}  // namespace ibex::runtime
