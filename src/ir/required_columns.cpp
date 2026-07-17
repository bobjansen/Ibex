#include <ibex/ir/required_columns.hpp>

#include <cstddef>
#include <map>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace ibex::ir {
namespace {

void collect_refs(const Expr& expr, ColumnDemand& demand);

void collect_refs(const ExprPtr& expr, ColumnDemand& demand) {
    if (expr != nullptr) {
        collect_refs(*expr, demand);
    }
}

/// Add every column `expr` reads to `demand`.
void collect_refs(const Expr& expr, ColumnDemand& demand) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                demand.add(n.name);
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

void visit(const Node& node, const ColumnDemand& need, DemandMap& out);

/// Recurse into every child carrying `need`.
void visit_children(const Node& node, const ColumnDemand& need, DemandMap& out) {
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            visit(*child, need, out);
        }
    }
}

/// Recurse into every child, demanding all of their columns. Used for nodes
/// this pass does not model: it cannot prove which columns they read, so it
/// assumes they read everything.
void visit_children_widened(const Node& node, DemandMap& out) {
    ColumnDemand all;
    all.widen();
    visit_children(node, all, out);
}

void visit(const Node& node, const ColumnDemand& need, DemandMap& out) {
    switch (node.kind()) {
        case NodeKind::Scan: {
            out[static_cast<const ScanNode&>(node).source_name()].merge(need);
            return;
        }

        case NodeKind::Program: {
            const auto& program = static_cast<const ProgramNode&>(node);
            // Preamble entries are side-effecting calls (write_csv, …) whose
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
            ColumnDemand below = need;
            collect_refs(static_cast<const FilterNode&>(node).predicate(), below);
            visit_children(node, below, out);
            return;
        }

        // Project fixes its own output, so what the parent wants is irrelevant:
        // the input demand is exactly the projected columns.
        case NodeKind::Project: {
            ColumnDemand below;
            add_refs(static_cast<const ProjectNode&>(node).columns(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::FilterProject: {
            const auto& fp = static_cast<const FilterProjectNode&>(node);
            ColumnDemand below;
            add_refs(fp.columns(), below);
            collect_refs(fp.predicate(), below);
            visit_children(node, below, out);
            return;
        }

        // Aggregate likewise fixes its output: the group keys plus whatever the
        // aggregations read.
        case NodeKind::Aggregate: {
            const auto& agg = static_cast<const AggregateNode&>(node);
            ColumnDemand below;
            add_refs(agg.group_by(), below);
            for (const auto& spec : agg.aggregations()) {
                // count() carries no column.
                if (!spec.column.name.empty()) {
                    below.add(spec.column.name);
                }
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Update: {
            const auto& update = static_cast<const UpdateNode&>(node);
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

        case NodeKind::FilterUpdateProject: {
            const auto& fup = static_cast<const FilterUpdateProjectNode&>(node);
            ColumnDemand below;
            for (const auto& column : fup.project_columns()) {
                bool produced = false;
                for (const auto& field : fup.fields()) {
                    produced = produced || field.alias == column.name;
                }
                if (!produced) {
                    below.add(column.name);
                }
            }
            for (const auto& field : fup.fields()) {
                collect_refs(field.expr, below);
            }
            collect_refs(fup.predicate(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Rename: {
            const auto& rename = static_cast<const RenameNode&>(node);
            ColumnDemand below;
            if (need.all) {
                below.widen();
            } else {
                for (const auto& name : need.names) {
                    const std::string* source = &name;
                    for (const auto& spec : rename.renames()) {
                        if (spec.new_name == name) {
                            source = &spec.old_name;
                            break;
                        }
                    }
                    below.add(*source);
                }
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Order: {
            ColumnDemand below = need;
            for (const auto& key : static_cast<const OrderNode&>(node).keys()) {
                below.add(key.name);
            }
            visit_children(node, below, out);
            return;
        }

        case NodeKind::TopK: {
            const auto& topk = static_cast<const TopKNode&>(node);
            ColumnDemand below = need;
            for (const auto& key : topk.keys()) {
                below.add(key.name);
            }
            add_refs(topk.group_by(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Head: {
            const auto& head = static_cast<const HeadNode&>(node);
            ColumnDemand below = need;
            collect_refs(head.count_expr(), below);
            add_refs(head.group_by(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Tail: {
            const auto& tail = static_cast<const TailNode&>(node);
            ColumnDemand below = need;
            collect_refs(tail.count_expr(), below);
            add_refs(tail.group_by(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::FilterHead: {
            ColumnDemand below = need;
            collect_refs(static_cast<const FilterHeadNode&>(node).predicate(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::FilterTail: {
            ColumnDemand below = need;
            collect_refs(static_cast<const FilterTailNode&>(node).predicate(), below);
            visit_children(node, below, out);
            return;
        }

        case NodeKind::Join: {
            const auto& join = static_cast<const JoinNode&>(node);
            // An asof join matches on each side's time index, which the node
            // does not name — so it cannot be bounded here.
            if (join.kind() == JoinKind::Asof) {
                visit_children_widened(node, out);
                return;
            }
            ColumnDemand below = need;
            for (const auto& key : join.keys()) {
                below.add(key);
            }
            if (join.predicate().has_value()) {
                collect_refs(*join.predicate(), below);
            }
            // `below` is the union across both sides; a name absent from one
            // side's schema is simply not read there.
            visit_children(node, below, out);
            return;
        }

        // A wildcard ascription (`as { a: Int, * }`) asserts only that the named
        // columns exist with the named types; extra columns are explicitly
        // allowed, so whether they are materialized is unobservable. Demand what
        // the parent wants plus the names the check itself reads, and no more --
        // naming a column asserts its shape, it does not ask for its data.
        //
        // An exact ascription is different and must keep widening: it also
        // asserts the input has *no* column it does not list, and that can only
        // be checked against the whole input. Narrowing it first would hide the
        // extras it exists to reject.
        case NodeKind::Ascribe: {
            const auto& asc = static_cast<const AscribeNode&>(node);
            if (asc.checked()) {
                // Already proven against the input's schema, so it reads nothing
                // at all: pass the parent's demand straight through.
                visit_children(node, need, out);
                return;
            }
            if (!asc.open()) {
                visit_children_widened(node, out);
                return;
            }
            ColumnDemand below = need;
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
        // Melt, Dcast, Cov, Corr, Transpose, Matmul, Rbind, Model, Stream,
        // ExternCall, Construct — either reads columns this pass cannot name or
        // consumes its input whole.
        default:
            visit_children_widened(node, out);
            return;
    }
}

}  // namespace

auto required_columns(const Node& root) -> DemandMap {
    DemandMap out;
    ColumnDemand all;
    all.widen();
    visit(root, all, out);
    return out;
}

}  // namespace ibex::ir
