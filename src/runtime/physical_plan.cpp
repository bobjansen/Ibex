// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "physical_plan.hpp"

#include <ibex/ir/expr_predicates.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <algorithm>
#include <atomic>
#include <string>

namespace ibex::runtime::physical {

namespace {

std::atomic<std::uint64_t> g_plans_built{0};
std::atomic<std::uint64_t> g_map_pipelines{0};
std::atomic<std::uint64_t> g_materialized_calls{0};

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
auto source_node_kind_name(ir::NodeKind kind) -> std::string_view {
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
/// declines a bare row-local Update, but for *island copy-cost* reasons
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

/// Decide the pipeline's execution mode from its own steps. These are the
/// rules the deleted island analysis applied while walking the IR itself;
/// deciding them here means the chain is peeled once and its mode travels with
/// it.
void resolve_pipeline_mode(Plan& plan) {
    std::size_t prefix = 0;
    for (const ir::Node* step : plan.steps) {
        if (execution_capability(*step) != ExecutionCapability::ParallelMap) {
            // The chain continues into something a morsel may not hold. The
            // prefix so far is still a pipeline; this step is its boundary.
            break;
        }
        if (!map_step_expressions_are_subset_evaluable(*step)) {
            // One unsupported expression makes the whole chain serial rather
            // than shortening it: the steps above it would have to consume a
            // partial result they were not planned over.
            plan.mode = PipelineMode::Serial;
            plan.serial_reason = SerialOnlyReason::UnsupportedExpression;
            return;
        }
        ++prefix;
    }
    if (prefix == 0) {
        plan.mode = PipelineMode::Serial;
        plan.serial_reason = SerialOnlyReason::NotParallelMap;
        return;
    }
    if (std::all_of(plan.steps.begin(), plan.steps.begin() + static_cast<std::ptrdiff_t>(prefix),
                    [](const ir::Node* step) { return is_metadata_only_node(step->kind()); })) {
        plan.mode = PipelineMode::Serial;
        plan.serial_reason = SerialOnlyReason::NoRowWork;
        return;
    }
    plan.mode = PipelineMode::MorselParallel;
    plan.serial_reason = SerialOnlyReason::None;
    plan.parallel_steps = prefix;
}

}  // namespace

auto plan_physical(const ir::Node& root, const TableRegistry& registry,
                   const ExternRegistry* externs) -> Plan {
    Plan plan;
    plan.root = &root;
    g_plans_built.fetch_add(1, std::memory_order_relaxed);

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
        plan.steps.push_back(cur);
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
        plan.kernel_dispatch.push_back({.capability = capability, .factory = factory});
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
    return plan.parallel_steps < plan.steps.size() ? plan.steps[plan.parallel_steps]
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
    if (!plan.migrated) {
        out += "MaterializedCall(";
        out += fallback_reason_name(plan.reason);
        out += ")\n";
        return out;
    }
    out += "MapPipeline\n";
    for (const ir::Node* step : plan.steps) {
        out += "  ";
        out += map_step_kind_name(step->kind());
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
                plan.source_node != nullptr ? source_node_kind_name(plan.source_node->kind()) : "?";
            out += ")";
            break;
    }
    out += "\n";
    out += "  mode: ";
    if (plan.mode == PipelineMode::MorselParallel) {
        out += "morsel-parallel(";
        out += std::to_string(plan.parallel_steps);
        out += " of ";
        out += std::to_string(plan.steps.size());
        out += " steps)";
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

void note_materialized_call(FallbackReason reason) {
    g_materialized_calls.fetch_add(1, std::memory_order_relaxed);
    (void)reason;
}

}  // namespace ibex::runtime::physical
