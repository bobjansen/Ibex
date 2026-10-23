// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/required_columns.hpp>

#include <map>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

void collect_refs(const Expr& expr, ColumnDemand& demand);

void collect_refs(const ExprPtr& expr, ColumnDemand& demand) {
    if (expr != nullptr) {
        collect_refs(*expr, demand);
    }
}

/// Add the columns `expr` reads *from one side of a join* to `demand`.
///
/// A `left(x)` / `right(x)` reference names its side outright, so the other
/// side is not asked for it. An unqualified reference could be either side (or
/// a scalar binding), so it is demanded from both — the conservative reading
/// that keeps pruning sound.
void collect_side_refs(const Expr& expr, ColumnDemand& demand, JoinSide side);

void collect_side_refs(const ExprPtr& expr, ColumnDemand& demand, JoinSide side) {
    if (expr != nullptr) {
        collect_side_refs(*expr, demand, side);
    }
}

void collect_side_refs(const Expr& expr, ColumnDemand& demand, JoinSide side) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                if (!n.lexical && (n.side == side || n.side == JoinSide::Any)) {
                    demand.add(n.name);
                }
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr> ||
                                 std::is_same_v<T, LogicalExpr>) {
                collect_side_refs(n.left, demand, side);
                collect_side_refs(n.right, demand, side);
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                collect_side_refs(n.operand, demand, side);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (const auto& arg : n.args) {
                    collect_side_refs(arg, demand, side);
                }
                for (const auto& named : n.named_args) {
                    collect_side_refs(named.value, demand, side);
                }
            } else {
                // Literal and RankExpr read no columns of the join's inputs.
            }
        },
        expr.node);
}

/// Add every column `expr` reads to `demand`.
void collect_refs(const Expr& expr, ColumnDemand& demand) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                // `^name` reads a scalar binding, not a column of the input.
                if (!n.lexical) {
                    demand.add(n.name);
                }
            } else if constexpr (std::is_same_v<T, Literal>) {
                // Reads nothing.
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr> ||
                                 std::is_same_v<T, LogicalExpr>) {
                collect_refs(n.left, demand);
                collect_refs(n.right, demand);
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                collect_refs(n.operand, demand);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (const auto& arg : n.args) {
                    collect_refs(arg, demand);
                }
                for (const auto& named : n.named_args) {
                    collect_refs(named.value, demand);
                }
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                for (const auto& key : n.order_keys) {
                    demand.add(key.name);
                }
            } else {
                demand.widen();
            }
        },
        expr.node);
}

void add_refs(const std::vector<ColumnRef>& columns, ColumnDemand& demand) {
    for (const auto& column : columns) {
        demand.add(column.name);
    }
}

using DemandMap = std::map<std::string, ColumnDemand>;

/// Both answers this one downward walk can give: what each **source** is read
/// for, and what each **join's own output** is read for.
///
/// They deliberately come from the same walk. A caller asking "does anything
/// above this join read that column?" is asking to *act* on the answer being
/// no, and a second demand model — even a careful one — would eventually be a
/// shade more permissive than this one and license dropping a column something
/// reads. There is one answer, computed once, or there is a latent wrong-answer
/// bug.
struct DemandSink {
    DemandMap sources;
    std::map<const Node*, ColumnDemand> joins;
    std::map<const Node*, ColumnDemand> filters;
    std::map<const Node*, ColumnDemand> source_nodes;
};

void visit(const Node& node, const ColumnDemand& need, DemandSink& out);

/// `need` as seen below an ascription listing `fields`: "every column" becomes
/// the listed ones. An ascription listing no fields narrows nothing.
auto narrow_to_ascribed(const ColumnDemand& need, const std::vector<SchemaField>& fields)
    -> ColumnDemand {
    if (!need.all || fields.empty()) {
        return need;
    }
    ColumnDemand narrowed;
    for (const auto& field : fields) {
        narrowed.add(field.name);
    }
    return narrowed;
}

/// Recurse into every child carrying `need`.
void visit_children(const Node& node, const ColumnDemand& need, DemandSink& out) {
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            visit(*child, need, out);
        }
    }
}

/// Recurse into every child, demanding all of their columns. Used for nodes
/// this pass does not model: it cannot prove which columns they read, so it
/// assumes they read everything.
void visit_children_widened(const Node& node, DemandSink& out) {
    ColumnDemand all;
    all.widen();
    visit_children(node, all, out);
}

void visit(const Node& node, const ColumnDemand& need, DemandSink& out) {
    switch (node.kind()) {
        case NodeKind::Scan: {
            // A fused ascription narrows the scan exactly as the Ascribe node
            // it replaced did.
            const auto& scan = node_cast<ScanNode>(node);
            const ColumnDemand narrowed = scan.ascribed_schema().has_value()
                                              ? narrow_to_ascribed(need, scan.ascribed_schema()->fields)
                                              : need;
            out.sources[scan.source_name()].merge(narrowed);
            out.source_nodes[&node].merge(narrowed);
            return;
        }

        // A table-valued call, or a `Table { ... }` literal, is a source too:
        // what is read of its result is `need`. What it is built from is
        // consumed whole.
        case NodeKind::ExternCall:
        case NodeKind::Construct: {
            out.source_nodes[&node].merge(need);
            visit_children_widened(node, out);
            return;
        }

        case NodeKind::Program: {
            const auto& program = node_cast<ProgramNode>(node);
            // Preamble entries are side-effecting calls (csv::write, …) whose
            // results are discarded; each consumes its own subtree in full.
            ColumnDemand all;
            all.widen();
            for (const auto& stmt : program.preamble()) {
                if (stmt != nullptr) {
                    visit(*stmt, all, out);
                }
            }
            visit(program.main_node(), need, out);
            return;
        }

        case NodeKind::Filter: {
            // Record the parent's demand before adding the predicate's own
            // reads.  A rewrite that removes the predicate needs to know
            // whether a referenced column was observable above it or existed
            // only as the predicate's marker.
            out.filters[&node].merge(need);
            ColumnDemand below = need;
            collect_refs(node_cast<FilterNode>(node).predicate(), below);
            visit_children(node, below, out);
            return;
        }

        // Project fixes its own output, so what the parent wants is irrelevant:
        // the input demand is exactly the projected columns.
        case NodeKind::Project: {
            ColumnDemand below;
            add_refs(node_cast<ProjectNode>(node).columns(), below);
            visit_children(node, below, out);
            return;
        }

        // Aggregate likewise fixes its output: the group keys plus whatever the
        // aggregations read.
        case NodeKind::Aggregate: {
            const auto& agg = node_cast<AggregateNode>(node);
            ColumnDemand below;
            add_refs(agg.group_by(), below);
            for (const auto& spec : agg.aggregations()) {
                // count() carries no column.
                if (!spec.column.name.empty()) {
                    below.add(spec.column.name);
                }
                if (!spec.paired.name.empty()) {
                    below.add(spec.paired.name);
                }
            }
            visit_children(node, below, out);
            return;
        }

        // Map fixes its own output too: the input demand is exactly the columns
        // its field expressions read, whatever the parent asked for.
        case NodeKind::Map: {
            ColumnDemand below;
            for (const auto& field : node_cast<MapNode>(node).fields()) {
                collect_refs(field.expr, below);
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Update: {
            const auto& update = node_cast<UpdateNode>(node);
            ColumnDemand below;
            if (need.all) {
                // Update passes every input column through, so "all of the
                // output" implies all of the input.
                below.widen();
            } else {
                // A name the update itself produces comes from the update, not
                // from below — unless one of its expressions reads it back.
                for (const auto& name : need.names) {
                    bool produced = false;
                    for (const auto& field : update.fields()) {
                        produced = produced || field.alias == name;
                    }
                    for (const auto& tuple : update.tuple_fields()) {
                        for (const auto& alias : tuple.aliases) {
                            produced = produced || alias == name;
                        }
                    }
                    if (!produced) {
                        below.add(name);
                    }
                }
            }
            for (const auto& field : update.fields()) {
                collect_refs(field.expr, below);
            }
            if (update.guard() != nullptr) {
                collect_refs(*update.guard(), below);
            }
            add_refs(update.group_by(), below);

            // A tuple field's source is an independent sub-plan; it may scan
            // sources of its own, and nothing here bounds what it reads.
            ColumnDemand all;
            all.widen();
            for (const auto& tuple : update.tuple_fields()) {
                if (tuple.source != nullptr) {
                    visit(*tuple.source, all, out);
                }
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Rename: {
            const auto& rename = node_cast<RenameNode>(node);
            const ColumnNameMap names(rename.renames());
            ColumnDemand below;
            if (need.all) {
                below.widen();
            } else {
                for (const auto& name : need.names) {
                    below.add(std::string(names.input_name(name)));
                }
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Order: {
            ColumnDemand below = need;
            for (const auto& key : node_cast<OrderNode>(node).keys()) {
                below.add(key.name);
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::TopK: {
            const auto& topk = node_cast<TopKNode>(node);
            ColumnDemand below = need;
            for (const auto& key : topk.keys()) {
                below.add(key.name);
            }
            add_refs(topk.group_by(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Head: {
            const auto& head = node_cast<HeadNode>(node);
            ColumnDemand below = need;
            collect_refs(head.count_expr(), below);
            add_refs(head.group_by(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Tail: {
            const auto& tail = node_cast<TailNode>(node);
            ColumnDemand below = need;
            collect_refs(tail.count_expr(), below);
            add_refs(tail.group_by(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::FilterHead: {
            ColumnDemand below = need;
            collect_refs(node_cast<FilterHeadNode>(node).predicate(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::FilterTail: {
            ColumnDemand below = need;
            collect_refs(node_cast<FilterTailNode>(node).predicate(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Join: {
            const auto& join = node_cast<JoinNode>(node);
            // An asof join matches on each side's time index, which the node
            // does not name — so it cannot be bounded here.
            if (join.kind() == JoinKind::Asof) {
                visit_children_widened(node, out);
                return;
            }
            // What the plan reads from THIS join's output, before the keys are
            // added below. Recorded for `join_output_demand`, whose callers ask
            // whether a key column is read above the join at all.
            out.joins[&node].merge(need);
            ColumnDemand left_below;
            ColumnDemand right_below;
            if (need.all) {
                left_below.widen();
                right_below.widen();
            } else {
                std::vector<RenameSpec> left_folded_names;
                std::vector<RenameSpec> right_folded_names;
                for (const auto& key : join.keys()) {
                    if (key.folds_output()) {
                        left_folded_names.push_back(RenameSpec{
                            .new_name = std::string(key.output_name()), .old_name = key.left});
                        right_folded_names.push_back(RenameSpec{
                            .new_name = std::string(key.output_name()), .old_name = key.right});
                    }
                }
                const ColumnNameMap left_names(left_folded_names);
                const ColumnNameMap right_names(right_folded_names);
                // A suffix policy renames the columns BOTH inputs hold, and
                // only those: `X` on both sides becomes `X` + left suffix and
                // `X` + right suffix. So a read of a suffixed name needs `X`
                // from both inputs -- the collision is what gives the name
                // its suffix, and dropping `X` from either side would leave
                // the other one unrenamed. A name merely ending in a suffix
                // pulls in a stripped name nothing reads, which costs a column,
                // never a wrong answer.
                const auto& suffix = join.suffix();
                const auto add_both = [&](const std::string& name) {
                    left_below.add(left_names.input_name(name));
                    right_below.add(right_names.input_name(name));
                };
                for (const auto& name : need.names) {
                    add_both(name);
                    if (!suffix.present) {
                        continue;
                    }
                    for (const auto* applied : {&suffix.left, &suffix.right}) {
                        if (!applied->empty() && name.size() > applied->size() &&
                            name.ends_with(*applied)) {
                            add_both(name.substr(0, name.size() - applied->size()));
                        }
                    }
                }
            }
            for (const auto& key : join.keys()) {
                left_below.add(key.left);
                right_below.add(key.right);
            }
            if (join.predicate().has_value()) {
                collect_side_refs(*join.predicate(), left_below, JoinSide::Left);
                collect_side_refs(*join.predicate(), right_below, JoinSide::Right);
            }
            if (join.children().size() != 2 || join.children()[0] == nullptr ||
                join.children()[1] == nullptr) {
                visit_children_widened(node, out);
                return;
            }
            visit(*join.children()[0], left_below, out);
            visit(*join.children()[1], right_below, out);
            return;
        }

        // An ascription asserts that the named columns exist with the named
        // types, and its output carries exactly those columns (SPEC.md 3.6).
        // So "every column" above it means every LISTED column, and nothing
        // unlisted is ever demanded below it. Demand what the parent wants
        // plus the names the check itself reads, and no more -- naming a
        // column asserts its shape, it does not ask for data.
        case NodeKind::Ascribe: {
            const auto& asc = node_cast<AscribeNode>(node);
            const ColumnDemand narrowed =
                asc.narrows() ? narrow_to_ascribed(need, asc.schema()) : need;
            if (asc.checked()) {
                // Already proven against the input's schema, so it reads nothing
                // at all: pass the parent's demand straight through.
                visit_children(node, narrowed, out);
                return;
            }
            ColumnDemand below = narrowed;
            for (const auto& field : asc.schema()) {
                below.add(field.name);
            }
            visit_children(node, below, out);
            return;
        }

        // Distinct de-duplicates over every input column, so narrowing its
        // input would change which rows survive — not just which columns.
        case NodeKind::Distinct:
        // Everything else — Window, Resample, AsTimeframe, Columns,
        // Melt, Dcast, Cov, Corr, Transpose, Matmul, Rbind, Model, Stream —
        // either reads columns this pass cannot name or
        // consumes its input whole.
        default:
            visit_children_widened(node, out);
            return;
    }
}

}  // namespace

auto required_columns(const Node& root) -> DemandMap {
    DemandSink out;
    ColumnDemand all;
    all.widen();
    visit(root, all, out);
    return std::move(out.sources);
}

auto join_output_demand(const Node& root) -> std::map<const Node*, ColumnDemand> {
    DemandSink out;
    ColumnDemand all;
    all.widen();
    visit(root, all, out);
    return std::move(out.joins);
}

auto source_output_demand(const Node& root) -> std::map<const Node*, ColumnDemand> {
    DemandSink out;
    ColumnDemand all;
    all.widen();
    visit(root, all, out);
    return std::move(out.source_nodes);
}

auto filter_output_demand(const Node& root) -> std::map<const Node*, ColumnDemand> {
    DemandSink out;
    ColumnDemand all;
    all.widen();
    visit(root, all, out);
    return std::move(out.filters);
}

}  // namespace ibex::ir
