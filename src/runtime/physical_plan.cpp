// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "physical_plan.hpp"

#include <ibex/format.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "interpreter_internal.hpp"
#include "join_internal.hpp"

namespace ibex::runtime::physical {

namespace {

/// Fallbacks by node kind and by reason. `NodeKind` is a `std::uint8_t` enum, so
/// 256 slots covers it by construction and no sentinel enumerator has to be
/// maintained alongside the IR.
constexpr std::size_t kKindSlots = 256;

/// Process-wide observability counters for the physical planner. Monotonic,
/// relaxed atomics, read only as `after - before` deltas (tests) or dumped in
/// `physical_fallback_report()`. Bundled behind a function-local static so the
/// state has one owner and dodges the global-mutable-state lint.
struct PlanStats {
    std::atomic<std::uint64_t> plans_built{0};
    std::atomic<std::uint64_t> map_pipelines{0};
    std::atomic<std::uint64_t> materialized_calls{0};
    std::array<std::atomic<std::uint64_t>, kKindSlots> fallback_by_kind{};
    std::array<std::atomic<std::uint64_t>, kKindSlots> fallback_by_reason{};
};

auto plan_stats() -> PlanStats& {
    static PlanStats stats;
    return stats;
}

/// The row count below which `ChunkedDistinctOperator` stays serial. It has
/// lived as a bare `1U << 15U` inside that operator — twice, once per dedup
/// path. The plan is now the single owner (src/runtime/PARALLELISM.md); the
/// operator will read it in the follow-up slice.
constexpr std::size_t kDistinctRowFloor = 1U << 15U;

/// The most workers the packed-key partition strategy will use, matching the
/// `std::size_t{64}` cap the operator applies today.
constexpr std::size_t kPackedKeyMaxWorkers = 64;

/// A streaming join's two fan-out floors, matching the private constants in
/// `chunked.cpp`: `build_partitions`'s `1U << 17U` and `probe_parallel_workers`'s
/// `1U << 14U`. Both phases share the same `min(budget, pool, 64)` worker cap.
constexpr std::size_t kJoinBuildRowFloor = 1U << 17U;
constexpr std::size_t kJoinProbeRowFloor = 1U << 14U;
constexpr std::size_t kJoinMaxWorkers = 64;

/// Hash-aggregate fan-out floors, matching the private constants in
/// `chunked.cpp`. `kAggPartitionRowFloor` is Discovery's general radix path's
/// `kDefaultPartitionMinRows` -- the row count below which `try_discover_
/// partitioned` stays serial. `try_owned`'s lower `kPairOwnedMinRows` (65536)
/// is not the phase floor: it is the operator-resolved "is the owned
/// specialization worth it" gate, the same kind of runtime strategy choice the
/// join operator makes for its build orientation, and it stays in the operator.
/// `kAggFinalizeRowFloor` matches the `1U << 17U` group-count gate on
/// `finalize_owned`'s parallel co-ranking merge. Discovery, Accumulation, and
/// FinalOrdering retain the existing 64-worker ceiling; Emission is bounded by
/// its output-column count and the shared compute budget.
constexpr std::size_t kAggPartitionRowFloor = 1U << 18U;
constexpr std::size_t kAggFinalizeRowFloor = 1U << 17U;
constexpr std::size_t kAggMaxWorkers = 64;

/// The exact cardinality of a registered table scan, or `None` when its input
/// cardinality is altered or otherwise unavailable. Project, Rename, and a
/// row-local Update preserve row count; a Filter or Join deliberately makes it
/// unknowable at plan time.
auto table_input_row_estimate(const ir::Node& root, const TableRegistry& registry) -> RowEstimate {
    const ir::Node* cur = root.children().empty() ? nullptr : root.children().front().get();
    while (cur != nullptr &&
           (cur->kind() == ir::NodeKind::Project || cur->kind() == ir::NodeKind::Rename ||
            cur->kind() == ir::NodeKind::Update)) {
        cur = cur->children().empty() ? nullptr : cur->children().front().get();
    }
    if (cur == nullptr || cur->kind() != ir::NodeKind::Scan) {
        return {};
    }
    const auto& scan = ir::node_cast<ir::ScanNode>(*cur);
    const auto it = registry.find(scan.source_name());
    if (it == registry.end()) {
        return {};
    }
    return {.rows = it->second.rows(), .source = RowEstimate::Source::TableExact};
}

auto runtime_column_type(const ColumnValue& column) -> ir::ColumnType {
    return std::visit(
        []<typename ColumnT>(const ColumnT&) {
            using ColumnType = std::remove_cvref_t<ColumnT>;
            if constexpr (std::same_as<ColumnType, Column<std::int64_t>>) {
                return ir::ColumnType::Int64;
            } else if constexpr (std::same_as<ColumnType, Column<double>>) {
                return ir::ColumnType::Float64;
            } else if constexpr (std::same_as<ColumnType, Column<bool>>) {
                return ir::ColumnType::Bool;
            } else if constexpr (std::same_as<ColumnType, Column<std::string>>) {
                return ir::ColumnType::String;
            } else if constexpr (std::same_as<ColumnType, Column<Categorical>>) {
                return ir::ColumnType::Categorical;
            } else if constexpr (std::same_as<ColumnType, Column<Date>>) {
                return ir::ColumnType::Date;
            } else {
                static_assert(std::same_as<ColumnType, Column<Timestamp>>);
                return ir::ColumnType::Timestamp;
            }
        },
        column);
}

auto planning_source_schemas(const TableRegistry& registry, const ir::SourceSchemas& declared)
    -> ir::SourceSchemas {
    ir::SourceSchemas schemas = declared;
    schemas.reserve(schemas.size() + registry.size());
    for (const auto& [name, table] : registry) {
        std::vector<ir::SchemaField> fields;
        fields.reserve(table.columns.size());
        for (const ColumnEntry& column : table.columns) {
            fields.push_back({.name = column.name, .type = runtime_column_type(*column.column)});
        }
        schemas.insert_or_assign(name, ir::SchemaInfo::known(std::move(fields), /*open=*/false));
    }
    return schemas;
}

auto known_join_column_mapping(const ir::JoinNode& join, const ir::SourceSchemas& schemas)
    -> std::optional<ir::JoinColumnMapping> {
    if (join.children().size() != 2) {
        return std::nullopt;
    }
    const ir::SchemaInfo left = ir::infer_schema(*join.children()[0], schemas);
    const ir::SchemaInfo right = ir::infer_schema(*join.children()[1], schemas);
    if (!left.is_known() || left.is_open() || !right.is_known() || right.is_open()) {
        return std::nullopt;
    }
    std::vector<std::string_view> left_names;
    left_names.reserve(left.fields().size());
    for (const ir::SchemaField& field : left.fields()) {
        left_names.push_back(field.name);
    }
    std::vector<std::string_view> right_names;
    right_names.reserve(right.fields().size());
    for (const ir::SchemaField& field : right.fields()) {
        right_names.push_back(field.name);
    }
    auto mapping =
        ir::resolve_join_columns(join.kind(), join.keys(), left_names, right_names, join.suffix());
    if (!mapping.has_value()) {
        return std::nullopt;
    }
    return std::move(*mapping);
}

auto known_aggregate_column_mapping(const ir::AggregateNode& aggregate,
                                    const ir::SourceSchemas& schemas)
    -> std::optional<AggregateColumnMapping> {
    if (aggregate.children().size() != 1) {
        return std::nullopt;
    }
    const ir::SchemaInfo input = ir::infer_schema(*aggregate.children().front(), schemas);
    if (!input.is_known() || input.is_open()) {
        return std::nullopt;
    }
    std::vector<std::string_view> input_names;
    input_names.reserve(input.fields().size());
    for (const ir::SchemaField& field : input.fields()) {
        input_names.push_back(field.name);
    }
    auto mapping =
        resolve_aggregate_columns(aggregate.group_by(), aggregate.aggregations(), input_names);
    if (!mapping.has_value()) {
        return std::nullopt;
    }
    return std::move(*mapping);
}

auto join_strategy_name(JoinStrategy strategy) -> std::string_view {
    switch (strategy) {
        case JoinStrategy::StreamingProbe:
            return "StreamingProbe";
        case JoinStrategy::MaterializeBoth:
            return "MaterializeBoth";
    }
    return "?";
}

auto aggregate_strategy_name(AggregateStrategy strategy) -> std::string_view {
    switch (strategy) {
        case AggregateStrategy::FusedLeftJoinCount:
            return "FusedLeftJoinCount";
        case AggregateStrategy::StreamingSorted:
            return "StreamingSorted";
        case AggregateStrategy::MaterializeAll:
            return "MaterializeAll";
    }
    return "?";
}

auto join_branch_name(JoinBranch branch) -> std::string_view {
    switch (branch) {
        case JoinBranch::None:
            return "None";
        case JoinBranch::SemiAnti:
            return "SemiAnti";
        case JoinBranch::SingleKeyInner:
            return "SingleKeyInner";
        case JoinBranch::PairIntInner:
            return "PairIntInner";
    }
    return "?";
}

auto join_decline_name(JoinDeclineReason reason) -> std::string_view {
    switch (reason) {
        case JoinDeclineReason::None:
            return "None";
        case JoinDeclineReason::UnsupportedKind:
            return "UnsupportedKind";
        case JoinDeclineReason::HasPredicate:
            return "HasPredicate";
        case JoinDeclineReason::MultipleKeys:
            return "MultipleKeys";
        case JoinDeclineReason::NullsEqual:
            return "NullsEqual";
        case JoinDeclineReason::AssertsCardinality:
            return "AssertsCardinality";
        case JoinDeclineReason::TakeSelection:
            return "TakeSelection";
        case JoinDeclineReason::KeyTypesUnsupported:
            return "KeyTypesUnsupported";
    }
    return "?";
}

/// Printable name for a step kind. Only map kinds appear here — the planner
/// never admits anything else into `steps`.
auto map_step_kind_name(ir::NodeKind kind) -> std::string_view {
    switch (kind) {
        case ir::NodeKind::Filter:
            return "Filter";
        case ir::NodeKind::Project:
            return "Project";
        case ir::NodeKind::Rename:
            return "Rename";
        case ir::NodeKind::Update:
            return "Update";
        default:
            return "Unknown";
    }
}

/// Printable name for the breaker feeding a `MaterializedInput` pipeline. The
/// common breakers are named; anything else prints its numeric kind, which is
/// enough to look up and better than claiming a name this table does not know.
auto node_kind_name_impl(ir::NodeKind kind) -> std::string_view {
    switch (kind) {
        case ir::NodeKind::Join:
            return "Join";
        case ir::NodeKind::Aggregate:
            return "Aggregate";
        case ir::NodeKind::Order:
            return "Order";
        case ir::NodeKind::Distinct:
            return "Distinct";
        case ir::NodeKind::TopK:
            return "TopK";
        case ir::NodeKind::Head:
            return "Head";
        case ir::NodeKind::Tail:
            return "Tail";
        case ir::NodeKind::FilterHead:
            return "FilterHead";
        case ir::NodeKind::FilterTail:
            return "FilterTail";
        case ir::NodeKind::Window:
            return "Window";
        case ir::NodeKind::Resample:
            return "Resample";
        case ir::NodeKind::Ascribe:
            return "Ascribe";
        case ir::NodeKind::Melt:
            return "Melt";
        case ir::NodeKind::Dcast:
            return "Dcast";
        case ir::NodeKind::Rbind:
            return "Rbind";
        case ir::NodeKind::Construct:
            return "Construct";
        case ir::NodeKind::Columns:
            return "Columns";
        case ir::NodeKind::Update:
            return "Update";
        case ir::NodeKind::Stream:
            return "Stream";
        // The map kinds too: a chain can fall back with one of these at its
        // root (`MalformedMapNode`), and an unlabeled bucket is exactly what
        // made the first backlog reading unusable.
        case ir::NodeKind::Scan:
            return "Scan";
        case ir::NodeKind::Filter:
            return "Filter";
        case ir::NodeKind::Project:
            return "Project";
        case ir::NodeKind::Rename:
            return "Rename";
        case ir::NodeKind::ExternCall:
            return "ExternCall";
        case ir::NodeKind::Program:
            return "Program";
        case ir::NodeKind::Cov:
            return "Cov";
        case ir::NodeKind::Corr:
            return "Corr";
        case ir::NodeKind::Transpose:
            return "Transpose";
        case ir::NodeKind::Matmul:
            return "Matmul";
        case ir::NodeKind::Model:
            return "Model";
        case ir::NodeKind::AsTimeframe:
            return "AsTimeframe";
        default:
            return "Other";
    }
}

auto representation_name(ColumnRepresentation representation) -> std::string_view {
    switch (representation) {
        case ColumnRepresentation::FixedWidth:
            return "fixed-width";
        case ColumnRepresentation::PackedBool:
            return "packed-bool";
        case ColumnRepresentation::StringSlabs:
            return "string-slabs";
        case ColumnRepresentation::CategoricalCodes:
            return "categorical-codes";
    }
    return "unknown";
}

auto kernel_null_policy_name(KernelNullPolicy policy) -> std::string_view {
    switch (policy) {
        case KernelNullPolicy::AllValid:
            return "all-valid";
        case KernelNullPolicy::Nullable:
            return "nullable";
    }
    return "unknown";
}

/// Whether `node` is a map step this planner lowers. Filter-shaped kinds are
/// maps unconditionally; an `Update` is a map exactly when the per-kind
/// switch's own gate says so — no guard, no `by`, no tuple assignment, every
/// field row-local (`is_row_local_update_expr`). That gate, not
/// `execution_capability()`, is the authority here: capability encoding also
/// declines a bare row-local Update, but for *morsel copy-cost* reasons
/// (updates parallelize inside the operator instead), which is an execution
/// choice the physical plan must not inherit as a shape decision.
auto is_map_step(const ir::Node& node) -> bool {
    return map_kernel_capability(node).has_value();
}

/// Whether `node` is a scan-like source, and with which kind. Anything else is
/// a pipeline breaker: the walk still ends there, but the subtree becomes this
/// pipeline's materialized input rather than a reason to decline.
auto classify_source(const ir::Node& node, const TableRegistry& registry,
                     const ExternRegistry* externs, SourceKind& kind) -> bool {
    if (node.kind() == ir::NodeKind::Scan) {
        const auto& scan = ir::node_cast<ir::ScanNode>(node);
        kind = registry.contains(scan.source_name()) ? SourceKind::TableScan : SourceKind::LazyScan;
        return true;
    }
    if (node.kind() == ir::NodeKind::ExternCall && externs != nullptr) {
        const auto& call = ir::node_cast<ir::ExternCallNode>(node);
        const auto* fn = externs->find(call.callee());
        if (fn != nullptr && fn->chunked_table_func != nullptr) {
            kind = SourceKind::ExternSource;
            return true;
        }
    }
    return false;
}

/// A node with exactly one child, and that child.
auto single_child(const ir::Node& node, ir::NodeKind kind) -> const ir::Node* {
    if (node.kind() != kind || node.children().size() != 1) {
        return nullptr;
    }
    const ir::Node* child = node.children().front().get();
    return child != nullptr && !child->children().empty() ? child : nullptr;
}

/// What a `Project` at the top of the chain can absorb below it: a plain
/// `Filter` (canonicalize R5's shape), or a row-local `Update` over a plain
/// `Filter` (R6's). Returns the leading `Filter` and, for the three-node form,
/// the `Update` between them. An already-fused IR kind is one step and is not
/// re-fused here.
struct FusibleChain {
    const ir::FilterNode* filter = nullptr;
    const ir::UpdateNode* update = nullptr;
};

auto fusible_chain_below(const ir::Node& node) -> FusibleChain {
    const ir::Node* below = single_child(node, ir::NodeKind::Project);
    if (below == nullptr) {
        return {};
    }
    if (below->kind() == ir::NodeKind::Filter && below->children().size() == 1 &&
        below->children().front() != nullptr) {
        return {.filter = ir::node_cast<ir::FilterNode>(below)};
    }
    // The update must be one the row-local kernel owns; a guarded or grouped
    // one is a different operator entirely and `map_kernel_capability` is the
    // authority on which.
    if (!map_kernel_capability(*below).has_value() ||
        // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        *map_kernel_capability(*below) != MapKernelCapability::RowLocalUpdate) {
        return {};
    }
    const ir::Node* filter = single_child(*below, ir::NodeKind::Update);
    if (filter == nullptr || filter->kind() != ir::NodeKind::Filter ||
        filter->children().size() != 1 || filter->children().front() == nullptr) {
        return {};
    }
    return {.filter = ir::node_cast<ir::FilterNode>(filter),
            .update = ir::node_cast<ir::UpdateNode>(below)};
}

/// Decide the pipeline's execution mode from its own steps. These are the
/// rules the deleted pipeline analysis applied while walking the IR itself;
/// deciding them here means the chain is peeled once and its mode travels with
/// it.
void resolve_pipeline_mode(Plan& plan) {
    // Search top-down for the outermost run of steps that may run over morsels.
    // Outermost-first is the existing policy, not a new one: when a chain's root
    // was ineligible, the per-kind recursion re-planned one node lower and took
    // the first pipeline it found on the way down. This finds the same run without
    // re-planning anything.
    SerialOnlyReason reason = SerialOnlyReason::NotParallelMap;
    std::size_t index = 0;
    while (index < plan.steps.size()) {
        if (execution_capability(*plan.steps[index].node) != ExecutionCapability::ParallelMap) {
            // Bounds a run from above (a row-local Update is the case that
            // matters) and cannot join one. Keep looking below it.
            ++index;
            continue;
        }
        if (!map_step_expressions_are_subset_evaluable(*plan.steps[index].node)) {
            // A step a morsel cannot evaluate. It joins no run, and a run above
            // it would have to consume a partial result it was not planned over.
            reason = SerialOnlyReason::UnsupportedExpression;
            ++index;
            continue;
        }
        std::size_t end = index;
        while (end < plan.steps.size() &&
               execution_capability(*plan.steps[end].node) == ExecutionCapability::ParallelMap &&
               map_step_expressions_are_subset_evaluable(*plan.steps[end].node)) {
            ++end;
        }
        const auto run_begin = plan.steps.begin() + static_cast<std::ptrdiff_t>(index);
        const auto run_end = plan.steps.begin() + static_cast<std::ptrdiff_t>(end);
        if (std::all_of(run_begin, run_end, [](const MapStep& step) {
                return is_metadata_only_node(step.node->kind());
            })) {
            // Nothing per-row to spread. Another run may still exist below.
            reason = SerialOnlyReason::NoRowWork;
            index = end + 1;
            continue;
        }
        plan.mode = PipelineMode::MorselParallel;
        plan.serial_reason = SerialOnlyReason::None;
        plan.parallel_begin = index;
        plan.parallel_end = end;
        return;
    }
    plan.mode = PipelineMode::Serial;
    plan.serial_reason = reason;
}

}  // namespace

auto plan_physical(const ir::Node& root, const TableRegistry& registry,
                   const ExternRegistry* externs, const ir::SourceSchemas& source_schemas) -> Plan {
    Plan plan;
    plan.root = &root;
    const ir::SourceSchemas schemas = planning_source_schemas(registry, source_schemas);
    plan_stats().plans_built.fetch_add(1, std::memory_order_relaxed);

    // Describe a join even though the plan does not execute one yet. The plan
    // is meant to be the single description of what a query does; letting it
    // stay silent about 51% of the backlog until the day execution moves would
    // mean the description and the executor land together, untested against
    // each other.
    // Single-implementation breakers: one operator runs each, nothing to
    // classify, no fan-out point. The plan owns construction and `explain
    // physical` names them; the per-kind switch's branch for each is deleted.
    // `Head` is `ChunkedHeadOperator`; `Tail` materializes and calls
    // `tail_table`; `TopK` is `ChunkedOrderedLimitOperator` (a serial
    // bounded-heap select, O(n log k) -- deliberately not parallel, see
    // src/runtime/PARALLELISM.md); `FilterHead` / `FilterTail` are the fused
    // `ChunkedFilter{Head,Tail}Operator`.
    if (root.kind() == ir::NodeKind::Head || root.kind() == ir::NodeKind::Tail ||
        root.kind() == ir::NodeKind::TopK || root.kind() == ir::NodeKind::FilterHead ||
        root.kind() == ir::NodeKind::FilterTail) {
        plan.migrated = true;
        plan.source_node = &root;
        return plan;
    }
    if (root.kind() == ir::NodeKind::Distinct) {
        // One fan-out phase. The planner sets the policy (floor, strategy,
        // ceiling) and the estimate; `build_physical_distinct` resolves the
        // worker cap and the operator reads it. See src/runtime/PARALLELISM.md,
        // "Target: parallelism as a plan decision".
        plan.migrated = true;
        plan.source_node = &root;
        plan.breaker_phases.push_back(
            {.name = "dedup",
             .parallelism = distinct_dedup_parallelism(table_input_row_estimate(root, registry))});
        return plan;
    }
    if (root.kind() == ir::NodeKind::Order) {
        // One operator runs every Order (`ChunkedOrderOperator` → `order_table`).
        // Its one fan-out point (the radix sort + row gather) is described so
        // `explain physical` is not silent about it; the fan-out itself already
        // lives in `sort.cpp` on the shared knobs, so the phase is descriptive
        // rather than something the operator reads.
        plan.migrated = true;
        plan.source_node = &root;
        plan.breaker_phases.push_back({.name = "sort", .parallelism = order_sort_parallelism()});
        return plan;
    }
    if (root.kind() == ir::NodeKind::Aggregate) {
        const auto& aggregate = ir::node_cast<ir::AggregateNode>(root);
        plan.aggregate = plan_aggregate(aggregate);
        // Streaming and fused aggregates are executed by the plan now.
        // `MaterializeAll` is not: it still falls back and still counts, which
        // is what keeps the backlog measuring the port rather than the label.
        if (plan.aggregate.strategy != AggregateStrategy::MaterializeAll) {
            plan.migrated = true;
            plan.source_node = &root;
            // Plan the adaptive streamable aggregate's structural nodes and
            // attach the fan-out policy each node owns. The fused left-join
            // count is whole-table and has no hash fallback to describe.
            if (plan.aggregate.strategy == AggregateStrategy::StreamingSorted) {
                plan.aggregate.columns = known_aggregate_column_mapping(aggregate, schemas);
                const RowEstimate input_estimate = table_input_row_estimate(root, registry);
                plan.hash_aggregate = HashAggregateNodes{
                    .discovery = {.source = aggregate.children().front().get(),
                                  .input = AggregateDataKind::InputChunks,
                                  .output = AggregateDataKind::DiscoveredGroups,
                                  .parallelism =
                                      aggregate_discovery_parallelism(input_estimate)},
                    .accumulation = {.input = AggregateDataKind::DiscoveredGroups,
                                     .output = AggregateDataKind::AccumulatedGroups,
                                     .parallelism =
                                         aggregate_accumulation_parallelism(input_estimate)},
                    .final_ordering = {.input = AggregateDataKind::AccumulatedGroups,
                                       .output = AggregateDataKind::OrderedGroups,
                                       .parallelism =
                                           aggregate_final_ordering_parallelism(input_estimate)},
                    .emission = {.input = AggregateDataKind::OrderedGroups,
                                 .output = AggregateDataKind::OutputChunks,
                                 .parallelism = aggregate_emission_parallelism(input_estimate)},
                };
            }
            return plan;
        }
    }
    if (root.kind() == ir::NodeKind::Join) {
        const auto& join = ir::node_cast<ir::JoinNode>(root);
        plan.join = plan_join(join);
        // A streaming join is executed by the plan now: `build_physical_join`
        // builds it, not the per-kind switch. A materializing one is still a
        // fallback and says so, which is why the backlog drops by the streaming
        // joins only -- the ones actually ported.
        if (plan.join.strategy == JoinStrategy::StreamingProbe) {
            plan.migrated = true;
            plan.source_node = &root;
            // An inner join is two explicit physical nodes joined by a typed
            // build-output edge. Both retain the textual inputs because
            // orientation is resolved only after the build has measured them.
            // Semi/anti still uses its separate operator and is not described
            // by the inner join's runtime-oriented output type.
            if (plan.join.branch != JoinBranch::SemiAnti) {
                plan.streaming_join = StreamingJoinNodes{
                    .build = {.left_input = plan.join.left_input,
                              .right_input = plan.join.right_input,
                              .output = JoinDataKind::RuntimeOrientedBuildOutput,
                              .parallelism = join_hash_build_parallelism()},
                    .probe = {.build_input = JoinDataKind::RuntimeOrientedBuildOutput,
                              .left_input = plan.join.left_input,
                              .right_input = plan.join.right_input,
                              .parallelism = join_probe_parallelism()},
                    .columns = known_join_column_mapping(join, schemas),
                };
            }
            return plan;
        }
    }

    // Peel map kinds top-down. `is_map_step` mirrors the per-kind switch's
    // own routing decisions, so the plan can never admit a step the switch
    // would build differently.
    const ir::Node* cur = &root;
    while (is_map_step(*cur)) {
        const auto& children = cur->children();
        if (children.size() != 1 || children.front() == nullptr) {
            // Malformed map node: leave it to the existing executor, which
            // produces the structural error message.
            plan.source_node = cur;
            plan.reason = FallbackReason::MalformedMapNode;
            return plan;
        }
        // Physical fusion: a Project over a Filter, or over a row-local Update
        // over a Filter, is one gather pass rather than two or three.
        // Fusion is a property of the pipeline, not of the logical tree.
        if (const FusibleChain fusible = fusible_chain_below(*cur); fusible.filter != nullptr) {
            const MapKernelCapability fused_capability =
                fusible.update != nullptr ? MapKernelCapability::FilterUpdateProjectGather
                                          : MapKernelCapability::FilterProjectGather;
            const MapKernelFactory fused_factory = map_kernel_factory(fused_capability);
            if (fused_factory != nullptr) {
                plan.steps.push_back(
                    {.node = fusible.filter,
                     .fused_update = fusible.update,
                     .fused_project = cur,
                     .filter_predicate = &fusible.filter->predicate(),
                     .update_fields =
                         fusible.update == nullptr ? nullptr : &fusible.update->fields(),
                     .project_columns = &ir::node_cast<ir::ProjectNode>(*cur).columns(),
                     .capability = fused_capability,
                     .factory = fused_factory});
                cur = fusible.filter->children().front().get();
                continue;
            }
        }
        const auto step_capability = map_kernel_capability(*cur);
        if (!step_capability.has_value()) {
            // `is_map_step` is `map_kernel_capability(...).has_value()`; the loop
            // condition already proved this. Restated so the deref stays checked.
            invariant_violation("physical plan: is_map_step admitted a node with no capability");
        }
        const MapKernelCapability capability = *step_capability;
        const MapKernelFactory factory = map_kernel_factory(capability);
        if (factory == nullptr) {
            // Keep a malformed internal dispatch table on the established
            // executor instead of constructing an invalid physical plan.
            plan.steps.clear();
            plan.source_node = cur;
            plan.reason = FallbackReason::MalformedMapNode;
            return plan;
        }
        plan.steps.push_back({.node = cur, .capability = capability, .factory = factory});
        cur = children.front().get();
    }

    plan.source_node = cur;
    SourceKind source = SourceKind::TableScan;
    if (!classify_source(*cur, registry, externs, source)) {
        if (plan.steps.empty()) {
            // Not a map chain at all — a breaker at the root is the executor's
            // to build, and there is no pipeline here to describe.
            plan.source_node = nullptr;
            plan.reason = FallbackReason::NotMapChain;
            return plan;
        }
        // A map chain over a breaker. The chain is a pipeline; the breaker is
        // its source, materialized by the existing executor. Constructing it is
        // what the per-kind switch does for this subtree anyway -- the source
        // goes through the public `build_operator` either way -- so this
        // records the shape rather than changing it.
        source = SourceKind::MaterializedInput;
    }
    if (plan.steps.empty()) {
        // A bare source: no map work to migrate, and the Scan/ExternCall
        // branches below own its streaming decisions.
        plan.reason = FallbackReason::EmptyChain;
        return plan;
    }
    plan.source = source;
    if (source == SourceKind::TableScan) {
        const auto& scan = ir::node_cast<ir::ScanNode>(*cur);
        const auto source_it = registry.find(scan.source_name());
        if (source_it != registry.end()) {
            plan.source_signature.reserve(source_it->second.columns.size());
            for (const auto& entry : source_it->second.columns) {
                plan.source_signature.push_back(
                    column_kernel_signature(*entry.column, entry.validity));
            }
        }
    }
    resolve_pipeline_mode(plan);
    plan.migrated = true;
    return plan;
}

auto fallback_reason_name(FallbackReason reason) -> std::string_view {
    switch (reason) {
        case FallbackReason::NotMapChain:
            return "root is not a row-local map";
        case FallbackReason::EmptyChain:
            return "bare source, no map steps";
        case FallbackReason::MalformedMapNode:
            return "map node is structurally malformed";
    }
    return "unknown";
}

auto parallel_input_node(const Plan& plan) -> const ir::Node* {
    if (plan.mode != PipelineMode::MorselParallel) {
        return nullptr;
    }
    return plan.parallel_end < plan.steps.size() ? plan.steps[plan.parallel_end].node
                                                 : plan.source_node;
}

auto serial_only_reason_name(SerialOnlyReason reason) -> std::string_view {
    switch (reason) {
        case SerialOnlyReason::None:
            return "none";
        case SerialOnlyReason::NotParallelMap:
            return "no leading parallel-map step";
        case SerialOnlyReason::UnsupportedExpression:
            return "expression needs rows outside the morsel";
        case SerialOnlyReason::NoRowWork:
            return "metadata-only chain, no per-row work";
    }
    return "unknown";
}

namespace {
// Defined below, next to the other `explain_breaker` helpers.
void append_phase_lines(std::string& out, const std::vector<BreakerPhase>& phases);
}  // namespace

auto explain_physical(const Plan& plan) -> std::string {
    std::string out;
    if (!plan.migrated) {
        out += "MaterializedCall(";
        out += fallback_reason_name(plan.reason);
        out += ")\n";
        // A described-but-not-executed node still explains itself. Without
        // this, `explain physical` would print the same opaque line for a join
        // it fully understands and one it knows nothing about.
        if (plan.join.describes) {
            out += "  " + explain_join(plan.join) + "\n";
        }
        if (plan.aggregate.describes) {
            out += "  " + explain_aggregate(plan.aggregate) + "\n";
        }
        return out;
    }
    // A migrated breaker at the root -- its own operator, no map steps. Route
    // every such shape here: without it, anything `migrated` with no `steps`
    // (order, distinct, head, a streaming join/aggregate) fell through to the
    // MapPipeline branch below and printed `MapPipeline\n  source: TableScan()`
    // -- a lie, since the root is not a scan and there is no pipeline.
    if (plan.steps.empty() && plan.root != nullptr) {
        const std::string_view kind = node_kind_name_impl(plan.root->kind());
        if (plan.join.describes) {
            // The strategy line, explicit build → probe edge, then each node's
            // fan-out policy. `breaker_phases` remains for untyped breakers;
            // a streaming join no longer hides its dataflow in two labels.
            out += "Breaker(Join)\n  " + explain_join(plan.join);
            if (plan.streaming_join.has_value()) {
                out += "\n  edge: HashBuild.RuntimeOrientedBuildOutput -> HashProbe.build_input";
                if (plan.streaming_join->columns.has_value()) {
                    out += "\n  columns: resolved";
                    for (const ir::JoinKeyColumns& key : plan.streaming_join->columns->keys) {
                        out += "  left[" + std::to_string(key.left_index) + "]=right[" +
                               std::to_string(key.right_index) + "]";
                    }
                    out +=
                        "  output=" + std::to_string(plan.streaming_join->columns->output.size());
                } else {
                    out += "\n  columns: deferred (bind once from concrete inputs)";
                }
                const std::vector<BreakerPhase> nodes{
                    {.name = "HashBuild", .parallelism = plan.streaming_join->build.parallelism},
                    {.name = "HashProbe", .parallelism = plan.streaming_join->probe.parallelism},
                };
                append_phase_lines(out, nodes);
            }
            out += '\n';
            return out;
        }
        if (plan.aggregate.describes) {
            // The adaptive path shows its structural hash-fallback chain and
            // each node's own fan-out policy. A fused left-join count has
            // neither and prints just the strategy.
            out += "Breaker(Aggregate)\n  " + explain_aggregate(plan.aggregate);
            if (plan.hash_aggregate.has_value()) {
                out += "\n  hash-fallback: Discovery -> Accumulation -> FinalOrdering -> Emission";
                out += "\n    edge: InputChunks -> DiscoveredGroups -> AccumulatedGroups -> "
                       "OrderedGroups -> OutputChunks";
                const std::vector<BreakerPhase> nodes{
                    {.name = "Discovery",
                     .parallelism = plan.hash_aggregate->discovery.parallelism},
                    {.name = "Accumulation",
                     .parallelism = plan.hash_aggregate->accumulation.parallelism},
                    {.name = "FinalOrdering",
                     .parallelism = plan.hash_aggregate->final_ordering.parallelism},
                    {.name = "Emission", .parallelism = plan.hash_aggregate->emission.parallelism},
                };
                append_phase_lines(out, nodes);
            }
            out += '\n';
            return out;
        }
        if (!plan.breaker_phases.empty()) {
            return explain_breaker(kind, plan.breaker_phases) + "\n";
        }
        // Head / Tail / TopK: a single-operator breaker with no fan-out point.
        out += "Breaker(";
        out += kind;
        out += ")\n  serial (single-operator breaker, no fan-out point)\n";
        return out;
    }
    out += "MapPipeline\n";
    for (const MapStep& step : plan.steps) {
        out += "  ";
        out += map_step_kind_name(step.node->kind());
        // A fused step executes several IR nodes in one pass; printing only
        // the first would make the plan look like it dropped the rest.
        for (const ir::Node* fused : {step.fused_update, step.fused_project}) {
            if (fused != nullptr) {
                out += '+';
                out += map_step_kind_name(fused->kind());
            }
        }
        if (step.fused_update != nullptr || step.fused_project != nullptr) {
            out += "(fused)";
        }
        out += '\n';
    }
    out += "  source: ";
    switch (plan.source) {
        case SourceKind::TableScan:
        case SourceKind::LazyScan:
            out += plan.source == SourceKind::TableScan ? "TableScan(" : "LazyScan(";
            if (plan.source_node != nullptr && plan.source_node->kind() == ir::NodeKind::Scan) {
                out += ir::node_cast<ir::ScanNode>(*plan.source_node).source_name();
            }
            out += ')';
            break;
        case SourceKind::ExternSource:
            out += "ExternSource(";
            if (plan.source_node != nullptr &&
                plan.source_node->kind() == ir::NodeKind::ExternCall) {
                out += ir::node_cast<ir::ExternCallNode>(*plan.source_node).callee();
            }
            out += ')';
            break;
        case SourceKind::MaterializedInput:
            // Naming the breaker's kind is what makes this plan inspectable:
            // "which operator feeds this pipeline" is the question a reader has.
            out += "MaterializedInput(";
            out +=
                plan.source_node != nullptr ? node_kind_name_impl(plan.source_node->kind()) : "?";
            out += ')';
            break;
    }
    out += '\n';
    out += "  mode: ";
    if (plan.mode == PipelineMode::MorselParallel) {
        out += "morsel-parallel(steps ";
        out += std::to_string(plan.parallel_begin);
        out += "..";
        out += std::to_string(plan.parallel_end);
        out += " of ";
        out += std::to_string(plan.steps.size());
        out += ')';
    } else {
        out += "serial(";
        out += serial_only_reason_name(plan.serial_reason);
        out += ')';
    }
    out += '\n';
    if (!plan.source_signature.empty()) {
        out += "  source signature:";
        for (const auto& signature : plan.source_signature) {
            out += ' ';
            out += representation_name(signature.representation);
            out += '/';
            out += kernel_null_policy_name(signature.null_policy);
        }
        out += '\n';
    }
    return out;
}

auto resolve_aggregate_columns(std::span<const ir::ColumnRef> group_by,
                               std::span<const ir::AggSpec> aggregations,
                               std::span<const std::string_view> input_names)
    -> std::expected<AggregateColumnMapping, std::string> {
    AggregateColumnMapping mapping;
    mapping.group_by.reserve(group_by.size());
    for (const ir::ColumnRef& key : group_by) {
        const auto found = std::ranges::find(input_names, key.name);
        if (found == input_names.end()) {
            return std::unexpected("group-by column not found: " + key.name);
        }
        mapping.group_by.push_back(
            static_cast<std::size_t>(std::distance(input_names.begin(), found)));
    }

    mapping.aggregate_inputs.reserve(aggregations.size());
    for (const ir::AggSpec& aggregation : aggregations) {
        if (aggregation.func == ir::AggFunc::Count) {
            mapping.aggregate_inputs.push_back(std::nullopt);
            continue;
        }
        const auto found = std::ranges::find(input_names, aggregation.column.name);
        if (found == input_names.end()) {
            return std::unexpected("aggregate column not found: " + aggregation.column.name);
        }
        mapping.aggregate_inputs.push_back(
            static_cast<std::size_t>(std::distance(input_names.begin(), found)));
    }
    return mapping;
}

auto plan_aggregate(const ir::AggregateNode& agg) -> AggregatePlan {
    AggregatePlan out;
    out.describes = true;
    // Relayed, in the builder's own order: the fusion is tested first because
    // it consumes the join below rather than reading its output, so it is not a
    // refinement of the streaming choice but an alternative to it.
    if (auto fusion = plan_fused_left_join_count(agg); fusion.has_value()) {
        out.strategy = AggregateStrategy::FusedLeftJoinCount;
        out.fused_join = fusion->join;
        out.counted_column = std::move(fusion->counted_column);
        return out;
    }
    out.strategy = aggregate_is_streamable(agg) ? AggregateStrategy::StreamingSorted
                                                : AggregateStrategy::MaterializeAll;
    return out;
}

auto explain_aggregate(const AggregatePlan& plan) -> std::string {
    if (!plan.describes) {
        return "";
    }
    std::string out = "Aggregate(";
    out += aggregate_strategy_name(plan.strategy);
    if (plan.fused_join != nullptr) {
        out += " fused=";
        out += node_kind_name_impl(plan.fused_join->kind());
        out += " counted=" + plan.counted_column;
    }
    out += ')';
    return out;
}

namespace {
auto partition_strategy_name(PartitionStrategy strategy) -> std::string_view {
    switch (strategy) {
        case PartitionStrategy::PackedKey:
            return "packed-key";
        case PartitionStrategy::RadixHash:
            return "radix-hash";
        case PartitionStrategy::Owned:
            return "owned";
        case PartitionStrategy::RowRange:
            return "row-range (sort + gather)";
        case PartitionStrategy::HeadTable:
            return "head-table (partition by key hash)";
        case PartitionStrategy::Range:
            return "range (contiguous probe-row slices)";
        case PartitionStrategy::Morsel:
            return "morsel (deterministic row ranges)";
        case PartitionStrategy::Column:
            return "column (independent output columns)";
    }
    return "?";
}
}  // namespace

auto distinct_dedup_parallelism(RowEstimate estimate) -> BreakerParallelism {
    return {.row_floor = kDistinctRowFloor,
            .breaker_max_workers = kPackedKeyMaxWorkers,
            .strategy = PartitionStrategy::PackedKey,
            .estimate = estimate};
}

auto order_sort_parallelism() -> BreakerParallelism {
    // row_floor 0 -> resolve fills it from exec.parallel_min_rows; no per-breaker
    // ceiling; no plan-time estimate (order's input is buffered, not sampled).
    return {.strategy = PartitionStrategy::RowRange};
}

auto join_hash_build_parallelism() -> BreakerParallelism {
    return {.row_floor = kJoinBuildRowFloor,
            .breaker_max_workers = kJoinMaxWorkers,
            .strategy = PartitionStrategy::HeadTable};
}

auto join_probe_parallelism() -> BreakerParallelism {
    return {.row_floor = kJoinProbeRowFloor,
            .breaker_max_workers = kJoinMaxWorkers,
            .strategy = PartitionStrategy::Range};
}

auto aggregate_discovery_parallelism(RowEstimate estimate) -> BreakerParallelism {
    // RadixHash is the general strategy; `try_owned` is a specialization of it
    // (partition-owned key maps instead of whole scattered partitions). The
    // operator resolves which at run time from the key type and cardinality,
    // the way the join resolves its build orientation.
    return {.row_floor = kAggPartitionRowFloor,
            .breaker_max_workers = kAggMaxWorkers,
            .strategy = PartitionStrategy::RadixHash,
            .estimate = estimate};
}

auto aggregate_accumulation_parallelism(RowEstimate estimate) -> BreakerParallelism {
    return {.row_floor = kAggFinalizeRowFloor,
            .breaker_max_workers = kAggMaxWorkers,
            .strategy = PartitionStrategy::Morsel,
            .estimate = estimate};
}

auto aggregate_final_ordering_parallelism(RowEstimate estimate) -> BreakerParallelism {
    return {.row_floor = kAggFinalizeRowFloor,
            .breaker_max_workers = kAggMaxWorkers,
            .strategy = PartitionStrategy::Owned,
            .estimate = estimate};
}

auto aggregate_emission_parallelism(RowEstimate estimate) -> BreakerParallelism {
    return {.strategy = PartitionStrategy::Column, .estimate = estimate};
}

void resolve_breaker_parallelism(BreakerParallelism& bp, const ExecutionContext& exec,
                                 std::size_t pool_size) {
    // A phase that names no floor of its own uses the shared knob. distinct's
    // 32768 is a deliberate, now-visible override of it.
    if (bp.row_floor == 0) {
        bp.row_floor = exec.parallel_min_rows;
    }
    if (!exec.can_fan_out()) {
        bp.decline = FanOutDecline::SingleCore;
        bp.worker_cap = 1;
        return;
    }
    if (bp.estimate.confident() && bp.estimate.rows < bp.row_floor) {
        bp.decline = FanOutDecline::BelowFloor;
        bp.worker_cap = 1;
        return;
    }
    bp.decline = FanOutDecline::None;
    // The one place the worker cap is computed. It used to be open-coded as
    // `std::min({budget, pool_size, 64})` inside each breaker's next().
    std::size_t cap = exec.compute_budget();
    if (pool_size != 0) {
        cap = std::min(cap, pool_size);
    }
    if (bp.breaker_max_workers != 0) {
        cap = std::min(cap, bp.breaker_max_workers);
    }
    bp.worker_cap = std::max<std::size_t>(cap, 1);
}

namespace {
/// One indented line per fan-out phase, appended to whatever header the caller
/// already built (`Breaker(Distinct)`, or a join's strategy line).
void append_phase_lines(std::string& out, const std::vector<BreakerPhase>& phases) {
    for (const auto& phase : phases) {
        const BreakerParallelism& bp = phase.parallelism;
        out += "\n  ";
        out += phase.name;
        out += ": ";
        if (bp.worker_cap == 0) {
            // Unresolved: printed straight off plan_physical, before a builder
            // with an ExecutionContext ran resolve_breaker_parallelism. A
            // `row_floor` of 0 means "inherit the shared parallel_min_rows".
            out += "parallel-capable  cap=unresolved  floor ";
            out += bp.row_floor == 0 ? "(shared parallel_min_rows)" : std::to_string(bp.row_floor);
        } else if (bp.decline == FanOutDecline::SingleCore) {
            out += "serial (single core)";
            continue;
        } else if (bp.decline == FanOutDecline::BelowFloor) {
            out += "serial (estimate " + std::to_string(bp.estimate.rows) + " < floor " +
                   std::to_string(bp.row_floor) + ")";
            continue;
        } else {
            out += "parallel-capable  cap<=" + std::to_string(bp.worker_cap) + "  floor " +
                   std::to_string(bp.row_floor);
        }
        out += "  partitions=derived  ";
        out += partition_strategy_name(bp.strategy);
        if (!bp.estimate.confident()) {
            out += "\n         no row estimate -> decided on first chunk";
            continue;
        }
        out += "\n         input estimate " + std::to_string(bp.estimate.rows) + " rows (";
        switch (bp.estimate.source) {
            case RowEstimate::Source::Footer:
                out += "footer";
                break;
            case RowEstimate::Source::TableExact:
                out += "table";
                break;
            case RowEstimate::Source::ChildExact:
                out += "child";
                break;
            case RowEstimate::Source::None:
                out += "?";
                break;
        }
        out += ")";
    }
}
}  // namespace

auto explain_breaker(std::string_view kind, const std::vector<BreakerPhase>& phases)
    -> std::string {
    std::string out = "Breaker(";
    out += kind;
    out += ')';
    append_phase_lines(out, phases);
    return out;
}

auto validate_streaming_join_edge(const StreamingJoinNodes& nodes) -> std::optional<std::string> {
    if (nodes.build.left_input == nullptr || nodes.build.right_input == nullptr ||
        nodes.probe.left_input == nullptr || nodes.probe.right_input == nullptr) {
        return "physical join: HashBuild/HashProbe edge has a missing candidate input";
    }
    if (nodes.build.left_input != nodes.probe.left_input ||
        nodes.build.right_input != nodes.probe.right_input) {
        return "physical join: HashBuild and HashProbe candidate inputs disagree";
    }
    if (nodes.build.output != JoinDataKind::RuntimeOrientedBuildOutput ||
        nodes.probe.build_input != nodes.build.output) {
        return "physical join: HashProbe does not consume HashBuild's runtime-oriented output";
    }
    return std::nullopt;
}

auto validate_hash_aggregate_edges(const HashAggregateNodes& nodes)
    -> std::optional<std::string> {
    if (nodes.discovery.source == nullptr) {
        return "physical aggregate: Discovery has no input";
    }
    if (nodes.discovery.input != AggregateDataKind::InputChunks ||
        nodes.discovery.output != AggregateDataKind::DiscoveredGroups ||
        nodes.accumulation.input != nodes.discovery.output) {
        return "physical aggregate: Discovery -> Accumulation edge is invalid";
    }
    if (nodes.accumulation.output != AggregateDataKind::AccumulatedGroups ||
        nodes.final_ordering.input != nodes.accumulation.output) {
        return "physical aggregate: Accumulation -> FinalOrdering edge is invalid";
    }
    if (nodes.final_ordering.output != AggregateDataKind::OrderedGroups ||
        nodes.emission.input != nodes.final_ordering.output) {
        return "physical aggregate: FinalOrdering -> Emission edge is invalid";
    }
    if (nodes.emission.output != AggregateDataKind::OutputChunks) {
        return "physical aggregate: Emission output is invalid";
    }
    return std::nullopt;
}

auto plan_join(const ir::JoinNode& join) -> JoinPlan {
    JoinPlan out;
    out.describes = true;
    out.kind = join.kind();
    out.key_count = join.keys().size();

    // The DECISION is relayed, not restated: these are the same three functions
    // the builder branches on. Reimplementing them is what made the first
    // version of this planner wrong about two-key Int64 joins, and
    // `interpreter_internal.hpp` had already written down why -- "a six-clause
    // predicate duplicated across two files, where a later clause added to one
    // copy silently routes a join the operator cannot handle".
    // Ask each gate by name and remember which one answered, in the order the
    // seam used to try them -- semi/anti first, since a semi join with one key
    // satisfies nothing below it.
    if (is_streamable_semi_anti_join(join)) {
        out.branch = JoinBranch::SemiAnti;
    } else if (is_streamable_inner_join(join)) {
        out.branch = JoinBranch::SingleKeyInner;
    } else if (is_streamable_pair_int_join(join)) {
        out.branch = JoinBranch::PairIntInner;
    }
    out.strategy = out.branch == JoinBranch::None ? JoinStrategy::MaterializeBoth
                                                  : JoinStrategy::StreamingProbe;

    // The clause walk below only EXPLAINS a decline. It is deliberately not the
    // decision, so it cannot route anything; if it ever disagrees with the
    // relay above, that is a bug in the explanation and the assert says so
    // rather than letting a plan claim a reason it did not act on.
    const bool kind_ok = join.kind() == ir::JoinKind::Inner || join.kind() == ir::JoinKind::Semi ||
                         join.kind() == ir::JoinKind::Anti;
    if (!kind_ok) {
        out.decline = JoinDeclineReason::UnsupportedKind;
    } else if (join.predicate().has_value()) {
        out.decline = JoinDeclineReason::HasPredicate;
        // NOLINTNEXTLINE(bugprone-branch-clone)
    } else if (join.keys().size() > 2) {
        out.decline = JoinDeclineReason::MultipleKeys;
    } else if (join.keys().size() == 2 && !is_streamable_pair_int_join(join)) {
        // Two keys stream only as the all-Int64 pair shape, and only the
        // semi/anti and single-key gates admit one key. A two-key semi join is
        // therefore not streamable even though each half of that sentence
        // sounds like it should be.
        out.decline = join.kind() == ir::JoinKind::Inner ? JoinDeclineReason::KeyTypesUnsupported
                                                         : JoinDeclineReason::MultipleKeys;
    } else if (join.keys().size() == 2 && join.kind() != ir::JoinKind::Inner) {
        out.decline = JoinDeclineReason::MultipleKeys;
    } else if (join.null_match() != ir::NullMatch::Never) {
        out.decline = JoinDeclineReason::NullsEqual;
    } else if (join.expect().asserts_anything()) {
        out.decline = JoinDeclineReason::AssertsCardinality;
    } else if (join.take() != ir::MatchSelection::All) {
        out.decline = JoinDeclineReason::TakeSelection;
    }

    if (out.strategy == JoinStrategy::MaterializeBoth) {
        if (out.decline == JoinDeclineReason::None) {
            // Declined for a reason the clause walk does not model. Better to
            // say so than to print `decline=None` beside `MaterializeBoth`.
            out.decline = JoinDeclineReason::KeyTypesUnsupported;
        }
        return out;
    }
    out.decline = JoinDeclineReason::None;
    if (join.children().size() != 2) {
        out.strategy = JoinStrategy::MaterializeBoth;
        out.branch = JoinBranch::None;
        return out;
    }
    // The two inputs, in textual order. Which one is hashed is decided by the
    // build phase at run time from measured row counts, so the plan records
    // the inputs and not an orientation. Which side *should* build is a cost
    // question this plan still does not answer -- that is q12's diagnosed
    // regression -- but the previous version of these two lines went further
    // than "not answering" and asserted left-probes/right-builds, which the
    // operator contradicts every time it swaps.
    out.left_input = join.children()[0].get();
    out.right_input = join.children()[1].get();
    return out;
}

auto explain_join(const JoinPlan& plan) -> std::string {
    if (!plan.describes) {
        return "";
    }
    std::string out = "Join(";
    out += join_strategy_name(plan.strategy);
    if (plan.branch != JoinBranch::None) {
        out += " branch=";
        out += join_branch_name(plan.branch);
    }
    out += " keys=" + std::to_string(plan.key_count);
    if (plan.decline != JoinDeclineReason::None) {
        out += " decline=";
        out += join_decline_name(plan.decline);
    }
    if (plan.left_input != nullptr) {
        out += " left=";
        out += node_kind_name_impl(plan.left_input->kind());
        out += " right=";
        out += node_kind_name_impl(plan.right_input->kind());
        out += " orientation=runtime";
    }
    out += ')';
    return out;
}

auto physical_plans_built() -> std::uint64_t {
    return plan_stats().plans_built.load(std::memory_order_relaxed);
}

auto physical_map_pipelines() -> std::uint64_t {
    return plan_stats().map_pipelines.load(std::memory_order_relaxed);
}

auto physical_materialized_calls() -> std::uint64_t {
    return plan_stats().materialized_calls.load(std::memory_order_relaxed);
}

void note_map_pipeline_executed() {
    plan_stats().map_pipelines.fetch_add(1, std::memory_order_relaxed);
}

void note_materialized_call(FallbackReason reason, ir::NodeKind kind) {
    auto& stats = plan_stats();
    stats.materialized_calls.fetch_add(1, std::memory_order_relaxed);
    stats.fallback_by_kind[static_cast<std::size_t>(kind)].fetch_add(1, std::memory_order_relaxed);
    stats.fallback_by_reason[static_cast<std::size_t>(reason)].fetch_add(1,
                                                                         std::memory_order_relaxed);
}

auto physical_fallbacks_for(ir::NodeKind kind) -> std::uint64_t {
    return plan_stats().fallback_by_kind[static_cast<std::size_t>(kind)].load(
        std::memory_order_relaxed);
}

auto node_kind_name(ir::NodeKind kind) -> std::string_view {
    return node_kind_name_impl(kind);
}

auto physical_fallback_report() -> std::string {
    // Descending by count: the top line is the next thing worth porting, which
    // is the whole point of keeping this by kind.
    std::vector<std::pair<std::uint64_t, ir::NodeKind>> rows;
    for (std::size_t i = 0; i < kKindSlots; ++i) {
        const std::uint64_t n = plan_stats().fallback_by_kind[i].load(std::memory_order_relaxed);
        if (n != 0) {
            rows.emplace_back(n, static_cast<ir::NodeKind>(i));
        }
    }
    std::ranges::sort(rows, [](const auto& a, const auto& b) { return a.first > b.first; });
    std::string out;
    for (const auto& [count, kind] : rows) {
        out += "plan fallback: kind=";
        out += node_kind_name_impl(kind);
        out += " count=" + std::to_string(count) + "\n";
    }
    return out;
}

namespace {

/// `IBEX_PLAN_STATS=1` prints the migration backlog at exit: how much of the
/// query surface the physical plan describes, and what it does not.
///
/// The counters existed before this and nothing read them, which meant the plan
/// document's own mitigation -- "every fallback explicit, profiled, and covered
/// by a migration backlog keyed by its measured cost" -- was written down but
/// not in place, and Phase 4's port order stayed the a-priori guess it was
/// drafted as.
struct FallbackReporter {
    // The struct is only declared const in this file.
    bool enabled = std::getenv("IBEX_PLAN_STATS") != nullptr;

    FallbackReporter() = default;
    FallbackReporter(const FallbackReporter&) = delete;
    FallbackReporter& operator=(const FallbackReporter&) = delete;
    FallbackReporter(FallbackReporter&&) = delete;
    FallbackReporter& operator=(FallbackReporter&&) = delete;

    ~FallbackReporter() noexcept {
        if (!enabled) {
            return;
        }
        try {
            ibex::formatting::print(
                stderr,
                "plan stats: plans={} pipelines={} fallbacks={} not_map_chain={} "
                "empty_chain={} malformed={}\n",
                physical_plans_built(), physical_map_pipelines(), physical_materialized_calls(),
                plan_stats()
                    .fallback_by_reason[static_cast<std::size_t>(FallbackReason::NotMapChain)]
                    .load(std::memory_order_relaxed),
                plan_stats()
                    .fallback_by_reason[static_cast<std::size_t>(FallbackReason::EmptyChain)]
                    .load(std::memory_order_relaxed),
                plan_stats()
                    .fallback_by_reason[static_cast<std::size_t>(FallbackReason::MalformedMapNode)]
                    .load(std::memory_order_relaxed));
            const std::string report = physical_fallback_report();
            if (!report.empty()) {
                ibex::formatting::print(stderr, "{}", report);
            }
        } catch (...) {  // NOLINT(bugprone-empty-catch) -- best-effort exit diagnostics
            // Exit-time diagnostics are best effort; never unwind from a destructor.
        }
    }
};

const FallbackReporter g_fallback_reporter;

}  // namespace

}  // namespace ibex::runtime::physical
