// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace ibex::runtime::physical {

/// Phase 1 of plans/kernel-pipeline-execution-plan.md: an explicit, data-only
/// physical plan computed *beside* `build_operator`, before any of its
/// per-kind branches run. The plan is the decision record — which pipelines
/// exist, and why anything else materialized — not a second executor. Phase 2
/// replaces per-step execution with kernels; until then a migrated pipeline
/// constructs exactly the operators the per-kind switch would have.
///
/// Scope (deliberately tiny, per the plan's "first vocabulary"):
/// a `MapPipeline` is a non-empty top-down chain of row-local map nodes
/// (Filter, Project, Rename, row-local Update, and the fused FilterProject /
/// FilterUpdateProject forms canonicalize produces) over a source. A source is
/// a Scan, a chunked extern call, or the materialized output of a subtree that
/// keeps the existing executor — a pipeline breaker feeding this pipeline.
/// Everything else is a `MaterializedCall` placeholder naming the logical
/// subtree that keeps the existing executor.
enum class SourceKind : std::uint8_t {
    TableScan,     ///< Scan of a table in the registry
    LazyScan,      ///< Scan resolved lazily (deferred/reader-backed)
    ExternSource,  ///< chunked extern call (read_csv, read_parquet, ...)
    /// The materialized output of a pipeline breaker (a join, an aggregate, an
    /// order — whatever the walk bottomed out in). The subtree keeps the
    /// existing executor and this pipeline consumes what it produces, which is
    /// exactly the relationship a breaker has to the pipeline above it. This is
    /// what lets one plan describe every map chain, not only the chains that
    /// happen to sit directly on a scan.
    MaterializedInput,
};

enum class FallbackReason : std::uint8_t {
    /// The root is not a row-local map kind (join, aggregate, order, ...).
    NotMapChain,
    /// The root is a bare source; there is no map work to migrate.
    EmptyChain,
    /// A map node with no single child, or one whose proven capability has no
    /// factory. Structurally broken rather than unsupported: the existing
    /// executor owns the diagnostic.
    MalformedMapNode,
};

/// How a migrated pipeline may execute. This is the plan's answer to "may the
/// morsel executor run this", decided once during lowering from the same rules
/// that used to live in the island analysis — a pipeline that carries its own
/// mode is what lets parallel execution become a mode of a pipeline rather than
/// a second executor with its own analysis.
enum class PipelineMode : std::uint8_t {
    /// Every step runs on the single build thread, in order.
    Serial,
    /// A leading run of `parallel_steps` steps may run over morsels. The rest
    /// of the chain, if any, stays serial above it.
    MorselParallel,
};

/// Why a migrated pipeline is serial-only. `None` when it is not.
enum class SerialOnlyReason : std::uint8_t {
    None,
    /// No leading step is a `ParallelMap` — a bare row-local `Update` is the
    /// case that matters. It is a map step, and deliberately not a parallel
    /// one: an update is 1:1 and splits its field computation inside the
    /// operator, so a morsel island would buy two whole-table copies. See
    /// `execution_capability(const ir::Node&)`.
    NotParallelMap,
    /// A step reads rows a morsel does not contain (rolling, rank, generator,
    /// unknown call), so it cannot be evaluated over a subset.
    UnsupportedExpression,
    /// Every step is metadata-only (Project/Rename), which costs O(columns);
    /// morselizing it would gather and concatenate the whole table to
    /// parallelize a pointer assignment.
    NoRowWork,
};

/// One lowered pipeline. Immutable by convention: built by `plan_physical`,
/// consumed read-only. Step nodes are `const ir::Node*` rather than copied
/// payloads so there is exactly one definition of each step's semantics; the
/// plan records *shape*, the IR remains the program. Like
/// every analysis over the IR, the plan **borrows** the IR it was lowered
/// from and must not outlive it. `steps` is ordered top-down (sink first),
/// matching the direction the executor composes in.
struct Plan {
    bool migrated = false;
    FallbackReason reason = FallbackReason::NotMapChain;
    SourceKind source = SourceKind::TableScan;
    /// The node this pipeline's input is built from: the scan or extern call
    /// for the three scan-like kinds, and the breaker's root for
    /// `MaterializedInput`. Recorded for every plan whose walk completed,
    /// migrated or not.
    const ir::Node* source_node = nullptr;
    std::vector<const ir::Node*> steps;
    std::vector<MapKernelDispatch> kernel_dispatch;
    PipelineMode mode = PipelineMode::Serial;
    SerialOnlyReason serial_reason = SerialOnlyReason::NotParallelMap;
    /// Length of the leading run of steps that may execute over morsels; zero
    /// unless `mode` is `MorselParallel`. The chain above it — a row-local
    /// `Update` and anything over it — executes serially on the pipeline's
    /// output, which is exactly what the island seam does today by rooting a
    /// shorter chain.
    std::size_t parallel_steps = 0;
    std::vector<ColumnKernelSignature> source_signature;
    const ir::Node* root = nullptr;
};

/// Lower `root` into a Phase 1 plan. Read-only over the IR, the registry, and
/// the context; safe to call from tests and `explain` tooling with no intent
/// to execute. A step is admitted only when the per-kind switch would build
/// it as a map too (`is_map_step` mirrors that routing exactly), so the plan
/// can never claim a shape the executor would construct differently.
[[nodiscard]] auto plan_physical(const ir::Node& root, const TableRegistry& registry,
                                 const ExternRegistry* externs) -> Plan;

/// Deterministic multi-line rendering for tests and debugging. A plan that
/// cannot explain why it materialized is not an acceptable plan, so every
/// fallback prints its reason.
[[nodiscard]] auto explain_physical(const Plan& plan) -> std::string;

/// The node feeding the parallel prefix: the first step below it when the
/// chain continues past what may run in parallel, else the pipeline's source.
/// Null for a serial pipeline, which has no such boundary.
[[nodiscard]] auto parallel_input_node(const Plan& plan) -> const ir::Node*;

/// Reason name as used by `explain_physical`.
[[nodiscard]] auto serial_only_reason_name(SerialOnlyReason reason) -> std::string_view;
[[nodiscard]] auto fallback_reason_name(FallbackReason reason) -> std::string_view;

// Process-wide counters. An executed pipeline and the pre-planner construction
// are indistinguishable from the outside, so — like ParallelIslandStats — the
// path needs its own proof it fired. Read the deltas around a query in tests;
// report them from profiling tooling later.
[[nodiscard]] auto physical_plans_built() -> std::uint64_t;
[[nodiscard]] auto physical_map_pipelines() -> std::uint64_t;
[[nodiscard]] auto physical_materialized_calls() -> std::uint64_t;

/// Record that a pipeline is executing (called once per plan that migrates,
/// whether the executor is the Phase 1 serial composer or the parallel
/// island, which is the same pipeline's parallel mode).
void note_map_pipeline_executed();

/// Record a fallback with its reason.
void note_materialized_call(FallbackReason reason);

}  // namespace ibex::runtime::physical
