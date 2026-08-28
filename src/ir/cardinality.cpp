// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

/// Count the top-level AND-conjuncts in `expr` (an OR, a bare comparison, a
/// call, ... all count as one). A compound predicate like `p_size == 15 &&
/// like(p_type, '%BRASS')` is far more selective than either conjunct alone;
/// applying `filter_selectivity` once regardless of conjunct count made every
/// multi-conjunct filter look ~4x too big to the join-order cost model below
/// (PDS-H q02: a 2-conjunct part filter, whose true selectivity is ~1/250,
/// was estimated at 0.25 -- 60x too high -- which was enough to make the
/// filtered leaf look bigger than its unfiltered join partners and push it
/// out of the build position).
auto count_conjuncts(const Expr& expr) -> int {
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node);
        logical != nullptr && logical->op == LogicalOp::And && logical->left != nullptr &&
        logical->right != nullptr) {
        return count_conjuncts(*logical->left) + count_conjuncts(*logical->right);
    }
    return 1;
}

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
            const auto& scan = node_cast<ScanNode>(node);
            const auto found = sources.find(scan.source_name());
            if (found == sources.end()) {
                return CardinalityEstimate{};
            }
            const auto absorbed = options.absorbed_scan_selectivity.find(scan.source_name());
            if (absorbed == options.absorbed_scan_selectivity.end()) {
                return CardinalityEstimate{.rows = found->second};
            }
            // A filter fused into this scan's decode still removes its rows,
            // even though the Filter node that expressed it is gone.
            return {.rows = static_cast<std::size_t>(
                        std::llround(static_cast<double>(found->second) * absorbed->second)),
                    .heuristic = true};
        }
        case NodeKind::Program:
            return estimate(node_cast<ProgramNode>(node).main_node(), sources, schemas, options);

        // These operators retain row count exactly.
        case NodeKind::Project:
        case NodeKind::Update:
        case NodeKind::Rename:
        case NodeKind::Order:
        case NodeKind::AsTimeframe:
        case NodeKind::Ascribe:
            return child();

        case NodeKind::Filter: {
            auto input = child();
            if (!input.rows.has_value()) {
                return input;
            }
            const auto selectivity = compound_selectivity(node_cast<FilterNode>(node).predicate(),
                                                          options.filter_selectivity);
            const auto selected = static_cast<std::size_t>(
                std::llround(static_cast<double>(*input.rows) * selectivity));
            return {.rows = selected, .heuristic = true};
        }
        case NodeKind::FilterHead: {
            auto input = child();
            if (!input.rows.has_value()) {
                return input;
            }
            const auto selectivity = compound_selectivity(
                node_cast<FilterHeadNode>(node).predicate(), options.filter_selectivity);
            const auto selected = static_cast<std::size_t>(
                std::llround(static_cast<double>(*input.rows) * selectivity));
            return {.rows = selected, .heuristic = true};
        }
        case NodeKind::FilterTail: {
            auto input = child();
            if (!input.rows.has_value()) {
                return input;
            }
            const auto selectivity = compound_selectivity(
                node_cast<FilterTailNode>(node).predicate(), options.filter_selectivity);
            const auto selected = static_cast<std::size_t>(
                std::llround(static_cast<double>(*input.rows) * selectivity));
            return {.rows = selected, .heuristic = true};
        }
        case NodeKind::Head: {
            auto input = child();
            const auto& head = node_cast<HeadNode>(node);
            if (!input.rows.has_value() || !head.count_literal().has_value()) {
                return input;
            }
            return {.rows = std::min(*input.rows, *head.count_literal()),
                    .heuristic = input.heuristic};
        }
        case NodeKind::Tail: {
            auto input = child();
            const auto& tail = node_cast<TailNode>(node);
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
            if (!node_cast<AggregateNode>(node).group_by().empty()) {
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
            const auto& join = node_cast<JoinNode>(node);
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
                    if (right.rows.has_value() &&
                        side_schema(0).is_unique_within(left_join_key_names(join.keys()))) {
                        bound = *right.rows;
                    }
                    if (left.rows.has_value() &&
                        side_schema(1).is_unique_within(right_join_key_names(join.keys()))) {
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

/// The column `alias` is computed from, when a field simply renames one --
/// `select { o_orderkey = l_orderkey }`. Anything computed (`a * b`, a call) has
/// no single source column and gives nullopt, which stops the walk.
auto renamed_from(const std::vector<FieldSpec>& fields, const std::string& alias)
    -> std::optional<std::string> {
    for (const auto& field : fields) {
        if (field.alias != alias) {
            continue;
        }
        if (const auto* ref = std::get_if<ColumnRef>(&field.expr.node);
            ref != nullptr && !ref->lexical) {
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

auto distinct_below(const Node& node, const std::string& column, const SourceStats& stats)
    -> std::optional<std::size_t> {
    switch (node.kind()) {
        case NodeKind::Scan: {
            const auto& scan = node_cast<ScanNode>(node);
            // A real sample beats the footer: `stats.distinct` below is a
            // `min(rows, max-min+1)` span estimate, exact for a dense key but
            // far too high for a sparse or skip-patterned one (TPC-H's
            // l_orderkey generator skips values, so a 6M-row footer span
            // overstates its true ~1.5M distinct count by 4x -- the error
            // that broke an earlier unguarded join-reorder attempt on q09).
            // Extrapolating a sample's observed distinct count by how much of
            // the source it covers tracks the true count far better, because
            // it's reading the actual skip pattern instead of assuming a
            // dense fill.
            if (stats.sample) {
                if (auto sample = stats.sample(scan.source_name(), nullptr, {column})) {
                    const auto found = sample->distinct.find(column);
                    const auto total = stats.rows.find(scan.source_name());
                    if (found != sample->distinct.end() && sample->sampled_rows > 0 &&
                        total != stats.rows.end() && total->second > 0) {
                        const double scale = static_cast<double>(total->second) /
                                             static_cast<double>(sample->sampled_rows);
                        const double extrapolated = static_cast<double>(found->second) * scale;
                        return static_cast<std::size_t>(
                            std::min(extrapolated, static_cast<double>(total->second)));
                    }
                }
            }
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
        case NodeKind::FilterHead:
        case NodeKind::FilterTail:
        case NodeKind::TopK:
        case NodeKind::Ascribe:
        case NodeKind::AsTimeframe:
            return distinct_through(node, column, stats);

        case NodeKind::Update: {
            const auto below = renamed_from(node_cast<UpdateNode>(node).fields(), column);
            return below ? distinct_through(node, *below, stats) : std::nullopt;
        }
        case NodeKind::Rename: {
            const ColumnNameMap names(node_cast<RenameNode>(node).renames());
            const std::string below(names.input_name(column));
            return distinct_through(node, below, stats);
        }
        default:
            // Joins, aggregates, anything else: a proof may still answer for
            // these (see `distinct_estimate`); metadata cannot.
            return std::nullopt;
    }
}

}  // namespace

auto compound_selectivity(const Expr& predicate, double filter_selectivity) -> double {
    return std::pow(filter_selectivity, count_conjuncts(predicate));
}

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
