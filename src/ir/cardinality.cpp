#include <ibex/ir/cardinality.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast) -- every cast below is
// guarded by the switch on node.kind() matching the target node type.
auto estimate(const Node& node, const SourceRowCounts& sources, const SourceSchemas& schemas,
              CardinalityOptions options) -> CardinalityEstimate {
    const auto child = [&](std::size_t index = 0) -> CardinalityEstimate {
        if (index >= node.children().size() || node.children()[index] == nullptr) {
            return {};
        }
        return estimate(*node.children()[index], sources, schemas, options);
    };
    switch (node.kind()) {
        case NodeKind::Scan: {
            const auto& scan = static_cast<const ScanNode&>(node);
            const auto found = sources.find(scan.source_name());
            return found == sources.end() ? CardinalityEstimate{}
                                          : CardinalityEstimate{.rows = found->second};
        }
        case NodeKind::Program:
            return estimate(static_cast<const ProgramNode&>(node).main_node(), sources, schemas,
                            options);

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
        // An aggregate emits one row per distinct group, so its size is the
        // distinct cardinality of its group keys -- which needs statistics we
        // do not have, so a grouped aggregate goes unestimated. An UNgrouped
        // one collapses to a single row by construction, whatever it reads.
        case NodeKind::Aggregate:
            if (!static_cast<const AggregateNode&>(node).group_by().empty()) {
                return {};
            }
            return {.rows = 1};

        // A join is estimated only where the answer is sound. Semi and Anti
        // select from their left input -- a left row survives or does not,
        // never duplicates -- so the left's count is a hard upper bound and the
        // Filter arm's selectivity guess applies unchanged. Cross is exact.
        //
        // Inner is estimated only from *proven* uniqueness (see the Join arm of
        // `infer_schema`), never from a guess. The outer kinds stay unestimated:
        // they pad with null rows, so the argument below does not reach them.
        // Returning nothing makes the join-order cost model decline, which is
        // the honest outcome -- a cost model that reorders on an invented
        // number regresses individual queries to improve an average.
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
                case JoinKind::Inner: {
                    // When one side is unique on (a subset of) the join keys,
                    // every row of the other side matches at most one row
                    // across it -- so that side's row count is a hard upper
                    // bound: |PK ⋈ FK| <= |FK|. No distinct-key counts are
                    // needed; knowing *that* a side is unique is enough, never
                    // how many values its key holds.
                    //
                    // The bound is proved. Using it as a point estimate assumes
                    // every row on the many side finds a match, so it reads
                    // high where the unique side is filtered -- hence
                    // `heuristic`.
                    if (node.children().size() != 2 || node.children()[0] == nullptr ||
                        node.children()[1] == nullptr) {
                        return {};
                    }
                    const auto left = child(0);
                    const auto right = child(1);
                    const auto side_schema = [&](std::size_t index) {
                        return infer_schema(*node.children()[index], schemas);
                    };
                    std::optional<std::size_t> bound;
                    if (right.rows.has_value() && side_schema(0).is_unique_within(join.keys())) {
                        bound = *right.rows;
                    }
                    if (left.rows.has_value() && side_schema(1).is_unique_within(join.keys())) {
                        bound = bound.has_value() ? std::min(*bound, *left.rows) : *left.rows;
                    }
                    if (!bound.has_value()) {
                        return {};
                    }
                    return {.rows = *bound, .heuristic = true};
                }
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
// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

/// The column `alias` is computed from, when a field simply renames one --
/// `select { o_orderkey = l_orderkey }`. Anything computed (`a * b`, a call) has
/// no single source column and gives nullopt, which stops the walk.
auto renamed_from(const std::vector<FieldSpec>& fields, const std::string& alias)
    -> std::optional<std::string> {
    for (const auto& field : fields) {
        if (field.alias != alias) {
            continue;
        }
        if (const auto* ref = std::get_if<ColumnRef>(&field.expr.node)) {
            return ref->name;
        }
        return std::nullopt;  // computed: not a rename of anything
    }
    return alias;  // untouched by this update
}

auto distinct_below(const Node& node, const std::string& column, const SourceStats& stats)
    -> std::optional<std::size_t>;

/// Follow `column` into the single child of a row-wise node, under whatever name
/// it has down there.
auto distinct_through(const Node& node, const std::string& column, const SourceStats& stats)
    -> std::optional<std::size_t> {
    if (node.children().size() != 1 || node.children().front() == nullptr) {
        return std::nullopt;
    }
    return distinct_below(*node.children().front(), column, stats);
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast) -- every cast below is
// guarded by the switch on node.kind() matching the target node type.
auto distinct_below(const Node& node, const std::string& column, const SourceStats& stats)
    -> std::optional<std::size_t> {
    switch (node.kind()) {
        case NodeKind::Scan: {
            const auto& scan = static_cast<const ScanNode&>(node);
            const auto source = stats.distinct.find(scan.source_name());
            if (source == stats.distinct.end()) {
                return std::nullopt;
            }
            const auto found = source->second.find(column);
            return found == source->second.end() ? std::nullopt : std::optional{found->second};
        }
        // Row-wise and column-preserving: the name means the same thing below.
        case NodeKind::Filter:
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
        case NodeKind::Distinct:
        case NodeKind::Project:
        case NodeKind::FilterProject:
        case NodeKind::FilterHead:
        case NodeKind::FilterTail:
        case NodeKind::TopK:
        case NodeKind::Ascribe:
        case NodeKind::AsTimeframe:
            return distinct_through(node, column, stats);

        case NodeKind::Update: {
            const auto below = renamed_from(static_cast<const UpdateNode&>(node).fields(), column);
            return below ? distinct_through(node, *below, stats) : std::nullopt;
        }
        case NodeKind::FilterUpdateProject: {
            const auto below =
                renamed_from(static_cast<const FilterUpdateProjectNode&>(node).fields(), column);
            return below ? distinct_through(node, *below, stats) : std::nullopt;
        }
        case NodeKind::Rename: {
            std::string below = column;
            for (const auto& spec : static_cast<const RenameNode&>(node).renames()) {
                if (spec.new_name == column) {
                    below = spec.old_name;
                    break;
                }
            }
            return distinct_through(node, below, stats);
        }
        default:
            // Joins, aggregates, anything else: a proof may still answer for
            // these (see `distinct_estimate`); metadata cannot.
            return std::nullopt;
    }
}
// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace

auto estimate_cardinality(const Node& root, const SourceRowCounts& sources,
                          const SourceSchemas& schemas, CardinalityOptions options)
    -> CardinalityEstimate {
    return estimate(root, sources, schemas, options);
}

auto distinct_estimate(const Node& node, const std::string& column, const SourceStats& stats)
    -> std::optional<std::size_t> {
    const auto rows = estimate_cardinality(node, stats.rows, stats.schemas).rows;

    // A proof beats any statistic: unique on {column} means every row holds a
    // different value, so the row count *is* the distinct count.
    if (rows.has_value() && infer_schema(node, stats.schemas).is_unique_within({column})) {
        return rows;
    }

    const auto below = distinct_below(node, column, stats);
    if (!below.has_value()) {
        return std::nullopt;
    }
    // Nothing on the way up invents a value, and no result can hold more
    // distinct values than it has rows.
    return rows.has_value() ? std::min(*below, *rows) : below;
}

}  // namespace ibex::ir
