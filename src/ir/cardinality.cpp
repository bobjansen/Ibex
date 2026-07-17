#include <ibex/ir/cardinality.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
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
        // A join is estimated only where the answer is sound. Semi and Anti
        // select from their left input -- a left row survives or does not,
        // never duplicates -- so the left's count is a hard upper bound and the
        // Filter arm's selectivity guess applies unchanged. Cross is exact.
        //
        // Inner and the outer kinds are deliberately left unestimated. Inner
        // fans out on duplicate keys and the outer kinds add null rows, so a
        // real estimate needs distinct-key counts: Parquet footers carry them
        // per row group but nothing plumbs them through yet. Returning nothing
        // makes the join-order cost model decline, which is the honest outcome
        // -- a cost model that reorders on an invented number regresses
        // individual queries to improve an average.
        case NodeKind::Join: {
            const auto& join = static_cast<const JoinNode&>(node);
            switch (join.kind()) {
                case JoinKind::Semi:
                case JoinKind::Anti: {
                    const auto left = child(0);
                    if (!left.rows.has_value()) {
                        return {};
                    }
                    const double keep = join.kind() == JoinKind::Semi
                                            ? options.filter_selectivity
                                            : 1.0 - options.filter_selectivity;
                    return {.rows = static_cast<std::size_t>(
                                std::llround(static_cast<double>(*left.rows) * keep)),
                            .heuristic = true};
                }
                case JoinKind::Cross: {
                    const auto left = child(0);
                    const auto right = child(1);
                    if (!left.rows.has_value() || !right.rows.has_value()) {
                        return {};
                    }
                    // The common case is a scalar subquery's 1-row right side.
                    // Refuse rather than wrap on a genuine cartesian blowup.
                    if (*right.rows != 0 &&
                        *left.rows > std::numeric_limits<std::size_t>::max() / *right.rows) {
                        return {};
                    }
                    return {.rows = *left.rows * *right.rows,
                            .heuristic = left.heuristic || right.heuristic};
                }
                case JoinKind::Inner:
                case JoinKind::Left:
                case JoinKind::Right:
                case JoinKind::Outer:
                case JoinKind::Asof:
                    return {};
            }
            return {};
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
