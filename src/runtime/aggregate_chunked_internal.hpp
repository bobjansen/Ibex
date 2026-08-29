// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <memory>
#include <optional>
#include <vector>

#include "physical_plan.hpp"

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
    std::optional<physical::AggregateColumnMapping> columns) -> OperatorPtr;

}  // namespace ibex::runtime
