// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "physical_plan.hpp"

#include <ibex/format.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "interpreter_internal.hpp"
#include "join_internal.hpp"

namespace ibex::runtime::physical {

namespace {

std::atomic<std::uint64_t> g_plans_built{0};
std::atomic<std::uint64_t> g_map_pipelines{0};
std::atomic<std::uint64_t> g_materialized_calls{0};

/// The row count below which `ChunkedDistinctOperator` stays serial. It has
/// lived as a bare `1U << 15U` inside that operator — twice, once per dedup
/// path. The plan is now the single owner (src/runtime/PARALLELISM.md); the
/// operator will read it in the follow-up slice.
constexpr std::size_t kDistinctRowFloor = 1U << 15U;

/// The most workers the packed-key partition strategy will use, matching the
/// `std::size_t{64}` cap the operator applies today.
constexpr std::size_t kPackedKeyMaxWorkers = 64;

/// A footer row estimate for `distinct`'s input, or `None` when the input is
/// not a bare registered scan. Deliberately conservative: a Filter or Join
/// under the Distinct makes the count unknowable at plan time, and the planner
/// never guesses — `None` means the operator decides on its first chunk, which
/// is exactly today's behaviour.
auto distinct_row_estimate(const ir::Node& distinct, const TableRegistry& registry) -> RowEstimate {
    const ir::Node* cur = distinct.children().empty() ? nullptr : distinct.children().front().get();
    while (cur != nullptr &&
           (cur->kind() == ir::NodeKind::Project || cur->kind() == ir::NodeKind::Rename)) {
        cur = cur->children().empty() ? nullptr : cur->children().front().get();
    }
    if (cur == nullptr || cur->kind() != ir::NodeKind::Scan) {
        return {};
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& scan = static_cast<const ir::ScanNode&>(*cur);
    const auto it = registry.find(scan.source_name());
    if (it == registry.end()) {
        return {};
    }
    return {.rows = it->second.rows(), .source = RowEstimate::Source::Footer};
}

/// Fallbacks by node kind and by reason. `NodeKind` is a `std::uint8_t` enum, so
/// 256 slots covers it by construction and no sentinel enumerator has to be
/// maintained alongside the IR.
constexpr std::size_t kKindSlots = 256;
std::array<std::atomic<std::uint64_t>, kKindSlots> g_fallback_by_kind{};
std::array<std::atomic<std::uint64_t>, kKindSlots> g_fallback_by_reason{};

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
        case ir::NodeKind::FilterProject:
            return "FilterProject";
        case ir::NodeKind::FilterUpdateProject:
            return "FilterUpdateProject";
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
        case ir::NodeKind::FilterProject:
            return "FilterProject";
        case ir::NodeKind::FilterUpdateProject:
            return "FilterUpdateProject";
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
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& scan = static_cast<const ir::ScanNode&>(node);
        kind = registry.contains(scan.source_name()) ? SourceKind::TableScan : SourceKind::LazyScan;
        return true;
    }
    if (node.kind() == ir::NodeKind::ExternCall && externs != nullptr) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& call = static_cast<const ir::ExternCallNode&>(node);
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
    const ir::Node* filter = nullptr;
    const ir::Node* update = nullptr;
};

auto fusible_chain_below(const ir::Node& node) -> FusibleChain {
    const ir::Node* below = single_child(node, ir::NodeKind::Project);
    if (below == nullptr) {
        return {};
    }
    if (below->kind() == ir::NodeKind::Filter && below->children().size() == 1 &&
        below->children().front() != nullptr) {
        return {.filter = below};
    }
    // The update must be one the row-local kernel owns; a guarded or grouped
    // one is a different operator entirely and `map_kernel_capability` is the
    // authority on which.
    if (!map_kernel_capability(*below).has_value() ||
        *map_kernel_capability(*below) != MapKernelCapability::RowLocalUpdate) {
        return {};
    }
    const ir::Node* filter = single_child(*below, ir::NodeKind::Update);
    if (filter == nullptr || filter->kind() != ir::NodeKind::Filter ||
        filter->children().size() != 1 || filter->children().front() == nullptr) {
        return {};
    }
    return {.filter = filter, .update = below};
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
                   const ExternRegistry* externs) -> Plan {
    Plan plan;
    plan.root = &root;
    g_plans_built.fetch_add(1, std::memory_order_relaxed);

    // Describe a join even though the plan does not execute one yet. The plan
    // is meant to be the single description of what a query does; letting it
    // stay silent about 51% of the backlog until the day execution moves would
    // mean the description and the executor land together, untested against
    // each other.
    if (root.kind() == ir::NodeKind::Head) {
        // Single-implementation breaker: nothing to classify, the plan owns
        // construction. Head is `OrderedStream`, not a fan-out point.
        plan.migrated = true;
        plan.source_node = &root;
        return plan;
    }
    if (root.kind() == ir::NodeKind::Distinct) {
        // One fan-out phase, described. The operator still decides for now; the
        // planner records the policy (floor, strategy, breaker ceiling) and the
        // estimate so `explain physical` and a test can see it. See
        // src/runtime/PARALLELISM.md, "Target: parallelism as a plan decision".
        plan.migrated = true;
        plan.source_node = &root;
        plan.breaker_phases.push_back(
            {.name = "dedup",
             .parallelism = {.row_floor = kDistinctRowFloor,
                             .breaker_max_workers = kPackedKeyMaxWorkers,
                             .strategy = PartitionStrategy::PackedKey,
                             .estimate = distinct_row_estimate(root, registry)}});
        return plan;
    }
    if (root.kind() == ir::NodeKind::Order) {
        // Nothing to classify: one operator runs every Order. The plan owns
        // construction, which is the whole content of this port.
        plan.migrated = true;
        plan.source_node = &root;
        return plan;
    }
    if (root.kind() == ir::NodeKind::Aggregate) {
        // Described, not executed: `migrated` stays false and the per-kind
        // switch still builds every aggregate. The description is proven equal
        // to the builder's branches first, exactly as the join's was.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        plan.aggregate = plan_aggregate(static_cast<const ir::AggregateNode&>(root));
        // Streaming and fused aggregates are executed by the plan now.
        // `MaterializeAll` is not: it still falls back and still counts, which
        // is what keeps the backlog measuring the port rather than the label.
        if (plan.aggregate.strategy != AggregateStrategy::MaterializeAll) {
            plan.migrated = true;
            plan.source_node = &root;
            return plan;
        }
    }
    if (root.kind() == ir::NodeKind::Join) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        plan.join = plan_join(static_cast<const ir::JoinNode&>(root));
        // A streaming join is executed by the plan now: `build_physical_join`
        // builds it, not the per-kind switch. A materializing one is still a
        // fallback and says so, which is why the backlog drops by the streaming
        // joins only -- the ones actually ported.
        if (plan.join.strategy == JoinStrategy::StreamingProbe) {
            plan.migrated = true;
            plan.source_node = &root;
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
        // Canonicalize R5/R6 already fuse these shapes into FilterProject and
        // FilterUpdateProject *nodes*, so today this fires only on IR that
        // reached the runtime unfused; expressing it here is what lets those
        // logical rewrites be retired, since fusion becomes a property of the
        // pipeline rather than of the tree (plan Phase 2 item 5).
        if (const FusibleChain fusible = fusible_chain_below(*cur); fusible.filter != nullptr) {
            const MapKernelCapability fused_capability =
                fusible.update != nullptr ? MapKernelCapability::FilterUpdateProjectGather
                                          : MapKernelCapability::FilterProjectGather;
            const MapKernelFactory fused_factory = map_kernel_factory(fused_capability);
            if (fused_factory != nullptr) {
                plan.steps.push_back({.node = fusible.filter,
                                      .fused_update = fusible.update,
                                      .fused_project = cur,
                                      .capability = fused_capability,
                                      .factory = fused_factory});
                cur = fusible.filter->children().front().get();
                continue;
            }
        }
        const MapKernelCapability capability = *map_kernel_capability(*cur);
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
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& scan = static_cast<const ir::ScanNode&>(*cur);
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

auto explain_physical(const Plan& plan) -> std::string {
    std::string out;
    if (!plan.breaker_phases.empty()) {
        // A breaker whose parallelism the plan describes (Distinct today).
        // `migrated` is true but there are no map steps — the breaker is its
        // own operator, and the phases are what there is to explain.
        const std::string_view kind =
            plan.root != nullptr ? node_kind_name_impl(plan.root->kind()) : "?";
        return explain_breaker(kind, plan.breaker_phases) + "\n";
    }
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
    out += "MapPipeline\n";
    for (const MapStep& step : plan.steps) {
        out += "  ";
        out += map_step_kind_name(step.node->kind());
        // A fused step executes several IR nodes in one pass; printing only
        // the first would make the plan look like it dropped the rest.
        for (const ir::Node* fused : {step.fused_update, step.fused_project}) {
            if (fused != nullptr) {
                out += "+";
                out += map_step_kind_name(fused->kind());
            }
        }
        if (step.fused_update != nullptr || step.fused_project != nullptr) {
            out += "(fused)";
        }
        out += "\n";
    }
    out += "  source: ";
    switch (plan.source) {
        case SourceKind::TableScan:
        case SourceKind::LazyScan:
            out += plan.source == SourceKind::TableScan ? "TableScan(" : "LazyScan(";
            if (plan.source_node != nullptr && plan.source_node->kind() == ir::NodeKind::Scan) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
                out += static_cast<const ir::ScanNode&>(*plan.source_node).source_name();
            }
            out += ")";
            break;
        case SourceKind::ExternSource:
            out += "ExternSource(";
            if (plan.source_node != nullptr &&
                plan.source_node->kind() == ir::NodeKind::ExternCall) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
                out += static_cast<const ir::ExternCallNode&>(*plan.source_node).callee();
            }
            out += ")";
            break;
        case SourceKind::MaterializedInput:
            // Naming the breaker's kind is what makes this plan inspectable:
            // "which operator feeds this pipeline" is the question a reader has.
            out += "MaterializedInput(";
            out +=
                plan.source_node != nullptr ? node_kind_name_impl(plan.source_node->kind()) : "?";
            out += ")";
            break;
    }
    out += "\n";
    out += "  mode: ";
    if (plan.mode == PipelineMode::MorselParallel) {
        out += "morsel-parallel(steps ";
        out += std::to_string(plan.parallel_begin);
        out += "..";
        out += std::to_string(plan.parallel_end);
        out += " of ";
        out += std::to_string(plan.steps.size());
        out += ")";
    } else {
        out += "serial(";
        out += serial_only_reason_name(plan.serial_reason);
        out += ")";
    }
    out += "\n";
    if (!plan.source_signature.empty()) {
        out += "  source signature:";
        for (const auto& signature : plan.source_signature) {
            out += " ";
            out += representation_name(signature.representation);
            out += "/";
            out += kernel_null_policy_name(signature.null_policy);
        }
        out += "\n";
    }
    return out;
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
    out += ")";
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
    }
    return "?";
}
}  // namespace

void resolve_breaker_parallelism(BreakerParallelism& bp, const ExecutionContext& exec,
                                 std::size_t pool_size) {
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

auto explain_breaker(std::string_view kind, const std::vector<BreakerPhase>& phases)
    -> std::string {
    std::string out = "Breaker(";
    out += kind;
    out += ")";
    for (const auto& phase : phases) {
        const BreakerParallelism& bp = phase.parallelism;
        out += "\n  ";
        out += phase.name;
        out += ": ";
        if (bp.worker_cap == 0) {
            // Unresolved: printed straight off plan_physical, before a builder
            // with an ExecutionContext ran resolve_breaker_parallelism.
            out += "parallel-capable  cap=unresolved  floor " + std::to_string(bp.row_floor);
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
        out += bp.estimate.confident()
                   ? "\n         estimate " + std::to_string(bp.estimate.rows) + " rows (footer)"
                   : "\n         no row estimate -> decided on first chunk";
    }
    return out;
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
    out += ")";
    return out;
}

auto physical_plans_built() -> std::uint64_t {
    return g_plans_built.load(std::memory_order_relaxed);
}

auto physical_map_pipelines() -> std::uint64_t {
    return g_map_pipelines.load(std::memory_order_relaxed);
}

auto physical_materialized_calls() -> std::uint64_t {
    return g_materialized_calls.load(std::memory_order_relaxed);
}

void note_map_pipeline_executed() {
    g_map_pipelines.fetch_add(1, std::memory_order_relaxed);
}

void note_materialized_call(FallbackReason reason, ir::NodeKind kind) {
    g_materialized_calls.fetch_add(1, std::memory_order_relaxed);
    g_fallback_by_kind[static_cast<std::size_t>(kind)].fetch_add(1, std::memory_order_relaxed);
    g_fallback_by_reason[static_cast<std::size_t>(reason)].fetch_add(1, std::memory_order_relaxed);
}

auto physical_fallbacks_for(ir::NodeKind kind) -> std::uint64_t {
    return g_fallback_by_kind[static_cast<std::size_t>(kind)].load(std::memory_order_relaxed);
}

auto node_kind_name(ir::NodeKind kind) -> std::string_view {
    return node_kind_name_impl(kind);
}

auto physical_fallback_report() -> std::string {
    // Descending by count: the top line is the next thing worth porting, which
    // is the whole point of keeping this by kind.
    std::vector<std::pair<std::uint64_t, ir::NodeKind>> rows;
    for (std::size_t i = 0; i < kKindSlots; ++i) {
        const std::uint64_t n = g_fallback_by_kind[i].load(std::memory_order_relaxed);
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
    const bool enabled = std::getenv("IBEX_PLAN_STATS") != nullptr;

    ~FallbackReporter() {
        if (!enabled) {
            return;
        }
        ibex::formatting::print(
            stderr,
            "plan stats: plans={} pipelines={} fallbacks={} not_map_chain={} "
            "empty_chain={} malformed={}\n",
            physical_plans_built(), physical_map_pipelines(), physical_materialized_calls(),
            g_fallback_by_reason[static_cast<std::size_t>(FallbackReason::NotMapChain)].load(
                std::memory_order_relaxed),
            g_fallback_by_reason[static_cast<std::size_t>(FallbackReason::EmptyChain)].load(
                std::memory_order_relaxed),
            g_fallback_by_reason[static_cast<std::size_t>(FallbackReason::MalformedMapNode)].load(
                std::memory_order_relaxed));
        const std::string report = physical_fallback_report();
        if (!report.empty()) {
            ibex::formatting::print(stderr, "{}", report);
        }
    }
};

const FallbackReporter g_fallback_reporter;

}  // namespace

}  // namespace ibex::runtime::physical
