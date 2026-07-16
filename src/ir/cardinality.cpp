#include <ibex/ir/cardinality.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <optional>

namespace ibex::ir {
namespace {

auto estimate(const Node& node, const SourceRowCounts& sources, CardinalityOptions options)
    -> CardinalityEstimate {
    const auto child = [&](std::size_t index = 0) -> CardinalityEstimate {
        if (index >= node.children().size() || node.children()[index] == nullptr) {
            return {};
        }
        return estimate(*node.children()[index], sources, options);
    };
    switch (node.kind()) {
        case NodeKind::Scan: {
            const auto& scan = static_cast<const ScanNode&>(node);
            const auto found = sources.find(scan.source_name());
            return found == sources.end() ? CardinalityEstimate{}
                                          : CardinalityEstimate{.rows = found->second};
        }
        case NodeKind::Program:
            return estimate(static_cast<const ProgramNode&>(node).main_node(), sources, options);

        // These operators retain row count exactly.
        case NodeKind::Project:
        case NodeKind::Update:
        case NodeKind::Rename:
        case NodeKind::Order:
        case NodeKind::AsTimeframe:
        case NodeKind::Ascribe:
            return child();

        case NodeKind::Filter:
        case NodeKind::FilterProject:
        case NodeKind::FilterUpdateProject: {
            auto input = child();
            if (!input.rows.has_value()) {
                return input;
            }
            const auto selected = static_cast<std::size_t>(
                std::llround(static_cast<double>(*input.rows) * options.filter_selectivity));
            return {.rows = selected, .heuristic = true};
        }
        case NodeKind::FilterHead:
        case NodeKind::FilterTail: {
            auto input = child();
            if (!input.rows.has_value()) {
                return input;
            }
            const auto selected = static_cast<std::size_t>(
                std::llround(static_cast<double>(*input.rows) * options.filter_selectivity));
            return {.rows = selected, .heuristic = true};
        }
        case NodeKind::Head: {
            auto input = child();
            const auto& head = static_cast<const HeadNode&>(node);
            if (!input.rows.has_value() || !head.count_literal().has_value()) {
                return input;
            }
            return {.rows = std::min(*input.rows, *head.count_literal()),
                    .heuristic = input.heuristic};
        }
        case NodeKind::Tail: {
            auto input = child();
            const auto& tail = static_cast<const TailNode&>(node);
            if (!input.rows.has_value() || !tail.count_literal().has_value()) {
                return input;
            }
            return {.rows = std::min(*input.rows, *tail.count_literal()),
                    .heuristic = input.heuristic};
        }
        default:
            return {};
    }
}

}  // namespace

auto estimate_cardinality(const Node& root, const SourceRowCounts& sources,
                          CardinalityOptions options) -> CardinalityEstimate {
    return estimate(root, sources, options);
}

}  // namespace ibex::ir
