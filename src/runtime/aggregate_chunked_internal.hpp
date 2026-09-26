// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

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

/// Construct the adaptive sorted/hash aggregate implementation. Kept as one
/// factory boundary so the complete aggregate family can live in its own
/// translation unit without exposing its state types to the pipeline builder.
[[nodiscard]] auto make_chunked_aggregate_operator(
    OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
    const std::vector<ir::AggSpec>* aggregations, const ExecutionContext& exec,
    physical::AggregateParallelism parallelism,
    std::optional<physical::AggregateColumnMapping> columns, AggregatePrefilter prefilter = {},
    std::shared_ptr<ScanWorkerSink> scan_sink = nullptr) -> OperatorPtr;

/// A sink the aggregate can use to accumulate on its input's scan workers.
/// Offer it (`ScanWorkerSinkOffer`) while building the aggregate's input, and
/// pass it to `make_chunked_aggregate_operator`; unused if nothing takes it.
[[nodiscard]] auto make_aggregate_scan_worker_sink(
    const std::vector<ir::ColumnRef>* group_by, const std::vector<ir::AggSpec>* aggregations,
    const ExecutionContext& exec, physical::AggregateParallelism parallelism,
    std::optional<physical::AggregateColumnMapping> columns) -> std::shared_ptr<ScanWorkerSink>;

}  // namespace ibex::runtime
