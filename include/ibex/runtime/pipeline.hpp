#pragma once

#include <ibex/ir/node.hpp>

#include <cstdint>
#include <vector>

namespace ibex::runtime {

/// How a node participates in a chunk pipeline.
enum class PipelineRole : std::uint8_t {
    Source,       ///< Produces chunks (Scan, ExternCall, Construct).
    Passthrough,  ///< Processes chunks 1:1 without materializing (Filter, Project, Rename, Update).
    Breaker,      ///< Must materialize input before producing output (Aggregate, Order, Join, …).
};

/// Classify an IR node kind into its pipeline role.
[[nodiscard]] auto classify_node(ir::NodeKind kind) noexcept -> PipelineRole;

/// A contiguous segment of the IR tree that forms one chunk pipeline.
///
/// `nodes` is ordered bottom-up: source first, then passthrough operators,
/// then the terminal breaker (if any).  A segment whose last node is a
/// Passthrough has no breaker — it feeds directly into the query output.
struct PipelineSegment {
    std::vector<const ir::Node*> nodes;

    [[nodiscard]] auto source() const noexcept -> const ir::Node* {
        return nodes.empty() ? nullptr : nodes.front();
    }
    [[nodiscard]] auto sink() const noexcept -> const ir::Node* {
        return nodes.empty() ? nullptr : nodes.back();
    }
    [[nodiscard]] auto size() const noexcept -> std::size_t { return nodes.size(); }
};

/// The result of pipeline analysis on an IR tree.
///
/// Segments are in execution order: segment 0 runs first, its breaker
/// produces the source table for segment 1, and so on.  The final
/// segment's output is the query result.
struct PipelinePlan {
    std::vector<PipelineSegment> segments;
};

/// Walk an IR tree and group nodes into pipeline segments.
///
/// The walk is bottom-up: it starts from the leaves (Scan / ExternCall)
/// and accumulates passthrough nodes until it hits a breaker.  Each
/// breaker ends one segment and starts a new one (the breaker's output
/// becomes the implicit source of the next segment).
[[nodiscard]] auto plan_pipelines(const ir::Node& root) -> PipelinePlan;

}  // namespace ibex::runtime
