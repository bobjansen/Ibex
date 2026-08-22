// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "physical_plan.hpp"

#include <ibex/ir/expr_predicates.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <algorithm>
#include <atomic>

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

/// Whether `node` is a map step this planner lowers. Filter-shaped kinds are
/// maps unconditionally; an `Update` is a map exactly when the per-kind
/// switch's own gate says so — no guard, no `by`, no tuple assignment, every
/// field row-local (`is_row_local_update_expr`). That gate, not
/// `execution_capability()`, is the authority here: capability encoding also
/// declines a bare row-local Update, but for *island copy-cost* reasons
/// (updates parallelize inside the operator instead), which is an execution
/// choice the physical plan must not inherit as a shape decision.
auto is_map_step(const ir::Node& node) -> bool {
    switch (node.kind()) {
        case ir::NodeKind::Filter:
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
        case ir::NodeKind::FilterProject:
        case ir::NodeKind::FilterUpdateProject:
            return true;
        case ir::NodeKind::Update: {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            return update.guard() == nullptr && update.group_by().empty() &&
                   update.tuple_fields().empty() &&
                   std::all_of(
                       update.fields().begin(), update.fields().end(),
                       [](const ir::FieldSpec& f) { return ir::is_row_local_update_expr(f.expr); });
        }
        default:
            return false;
    }
}

/// Whether `node` can serve as a pipeline source, and with which kind.
/// Anything else ends the walk and the plan records `NonSourceInput` — the
/// subtree keeps the existing executor, whatever it is.
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
            plan.reason = FallbackReason::NonSourceInput;
            return plan;
        }
        plan.steps.push_back(cur);
        cur = children.front().get();
    }

    SourceKind source = SourceKind::TableScan;
    if (!classify_source(*cur, registry, externs, source)) {
        plan.reason =
            plan.steps.empty() ? FallbackReason::NotMapChain : FallbackReason::NonSourceInput;
        return plan;
    }
    if (plan.steps.empty()) {
        // A bare source: no map work to migrate, and the Scan/ExternCall
        // branches below own its streaming decisions.
        plan.reason = FallbackReason::EmptyChain;
        return plan;
    }
    plan.source = source;
    plan.source_node = cur;
    plan.migrated = true;
    return plan;
}

auto fallback_reason_name(FallbackReason reason) -> std::string_view {
    switch (reason) {
        case FallbackReason::NotMapChain:
            return "root is not a row-local map";
        case FallbackReason::EmptyChain:
            return "bare source, no map steps";
        case FallbackReason::NonSourceInput:
            return "map chain input is not a source";
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
    }
    out += "\n";
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
