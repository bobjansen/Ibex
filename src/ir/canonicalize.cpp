#include <ibex/ir/canonicalize.hpp>
#include <ibex/ir/expr_predicates.hpp>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace ibex::ir {

namespace {

// Move-steal a unique child out of `parent`, returning it and leaving `parent`
// with an empty children vector. Precondition: parent has exactly one child.
auto take_unique_child(Node& parent) -> NodePtr {
    auto& kids = parent.mutable_children();
    NodePtr child = std::move(kids.front());
    kids.clear();
    return child;
}

// Returns true when every Order key names a column that appears in the
// projection's output (no Project may have computed columns after lowering,
// so a key passes iff its name is one of the projected column names).
auto keys_preserved_by_project(const std::vector<OrderKey>& keys, const ProjectNode& proj) -> bool {
    std::unordered_set<std::string> out;
    out.reserve(proj.columns().size());
    for (const auto& col : proj.columns()) {
        out.insert(col.name);
    }
    return std::all_of(keys.begin(), keys.end(),
                       [&](const OrderKey& k) { return out.contains(k.name); });
}

// Returns true when every group_by column appears in the projection output.
auto group_by_preserved_by_project(const std::vector<ColumnRef>& group_by, const ProjectNode& proj)
    -> bool {
    std::unordered_set<std::string> out;
    out.reserve(proj.columns().size());
    for (const auto& col : proj.columns()) {
        out.insert(col.name);
    }
    return std::all_of(group_by.begin(), group_by.end(),
                       [&](const ColumnRef& c) { return out.contains(c.name); });
}

// Remap an Order key list through a Rename: any key whose name matches a
// RenameSpec's new_name is rewritten to the corresponding old_name, so the
// sort comparator references the pre-rename schema beneath.
auto remap_keys_through_rename(std::vector<OrderKey> keys, const std::vector<RenameSpec>& renames)
    -> std::vector<OrderKey> {
    std::unordered_map<std::string, std::string> new_to_old;
    new_to_old.reserve(renames.size());
    for (const auto& rs : renames) {
        new_to_old.emplace(rs.new_name, rs.old_name);
    }
    for (auto& k : keys) {
        auto it = new_to_old.find(k.name);
        if (it != new_to_old.end()) {
            k.name = it->second;
        }
    }
    return keys;
}

auto remap_group_by_through_rename(std::vector<ColumnRef> group_by,
                                   const std::vector<RenameSpec>& renames)
    -> std::vector<ColumnRef> {
    std::unordered_map<std::string, std::string> new_to_old;
    new_to_old.reserve(renames.size());
    for (const auto& rs : renames) {
        new_to_old.emplace(rs.new_name, rs.old_name);
    }
    for (auto& c : group_by) {
        auto it = new_to_old.find(c.name);
        if (it != new_to_old.end()) {
            c.name = it->second;
        }
    }
    return group_by;
}

// Collect the set of column names referenced by a FilterExpr tree.
void collect_filter_column_refs(const FilterExpr& expr, std::unordered_set<std::string>& out) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, FilterColumn>) {
                out.insert(n.name);
            } else if constexpr (std::is_same_v<T, FilterArith> || std::is_same_v<T, FilterCmp> ||
                                 std::is_same_v<T, FilterAnd> || std::is_same_v<T, FilterOr>) {
                collect_filter_column_refs(*n.left, out);
                collect_filter_column_refs(*n.right, out);
            } else if constexpr (std::is_same_v<T, FilterNot> || std::is_same_v<T, FilterIsNull> ||
                                 std::is_same_v<T, FilterIsNotNull>) {
                collect_filter_column_refs(*n.operand, out);
            }
        },
        expr.node);
}

// Remap FilterColumn references inside a FilterExpr tree new→old.
void remap_filter_expr_through_rename(FilterExpr& expr,
                                      const std::unordered_map<std::string, std::string>& n2o) {
    std::visit(
        [&](auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, FilterColumn>) {
                auto it = n2o.find(n.name);
                if (it != n2o.end()) {
                    n.name = it->second;
                }
            } else if constexpr (std::is_same_v<T, FilterArith> || std::is_same_v<T, FilterCmp> ||
                                 std::is_same_v<T, FilterAnd> || std::is_same_v<T, FilterOr>) {
                remap_filter_expr_through_rename(*n.left, n2o);
                remap_filter_expr_through_rename(*n.right, n2o);
            } else if constexpr (std::is_same_v<T, FilterNot> || std::is_same_v<T, FilterIsNull> ||
                                 std::is_same_v<T, FilterIsNotNull>) {
                remap_filter_expr_through_rename(*n.operand, n2o);
            }
        },
        expr.node);
}

// Compose outer(inner): the outer rename's old_name that matches an inner
// new_name should reach through to the inner old_name. Identity pairs
// (new == old) are dropped in the result.
auto compose_renames(const std::vector<RenameSpec>& outer, const std::vector<RenameSpec>& inner)
    -> std::vector<RenameSpec> {
    std::unordered_map<std::string, std::string> inner_new_to_old;
    inner_new_to_old.reserve(inner.size());
    for (const auto& rs : inner) {
        inner_new_to_old.emplace(rs.new_name, rs.old_name);
    }
    std::unordered_set<std::string> used_inner;
    used_inner.reserve(inner.size());
    std::vector<RenameSpec> out;
    out.reserve(outer.size() + inner.size());
    for (const auto& rs : outer) {
        std::string old = rs.old_name;
        auto it = inner_new_to_old.find(old);
        if (it != inner_new_to_old.end()) {
            used_inner.insert(it->first);
            old = it->second;
        }
        if (rs.new_name != old) {
            out.push_back(RenameSpec{.new_name = rs.new_name, .old_name = old});
        }
    }
    for (const auto& rs : inner) {
        if (!used_inner.contains(rs.new_name) && rs.new_name != rs.old_name) {
            out.push_back(rs);
        }
    }
    return out;
}

auto canon(NodePtr node) -> NodePtr;

// Apply the rewrite rules at this node's root, looping until a fixpoint is
// reached. Children are assumed already canonicalized. Returns the new root
// (may differ from `node` if a rewrite fired).
auto rewrite_root(NodePtr node) -> NodePtr {
    bool changed = true;
    while (changed) {
        changed = false;
        if (!node) {
            return node;
        }
        const auto kind = node->kind();

        // R1: Filter(Order(x)) → Order(Filter(x))
        if (kind == NodeKind::Filter && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Order) {
            NodePtr filter = std::move(node);
            NodePtr order = take_unique_child(*filter);
            NodePtr x = take_unique_child(*order);
            filter->add_child(std::move(x));
            filter = rewrite_root(std::move(filter));
            order->add_child(std::move(filter));
            node = std::move(order);
            changed = true;
            continue;
        }

        // R2: Project(Order(x)) → Order(Project(x)) (keys preserved)
        if (kind == NodeKind::Project && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Order) {
            const auto& proj = static_cast<const ProjectNode&>(*node);
            const auto& order = static_cast<const OrderNode&>(*node->children().front());
            if (keys_preserved_by_project(order.keys(), proj)) {
                NodePtr project = std::move(node);
                NodePtr order_n = take_unique_child(*project);
                NodePtr x = take_unique_child(*order_n);
                project->add_child(std::move(x));
                project = rewrite_root(std::move(project));
                order_n->add_child(std::move(project));
                node = std::move(order_n);
                changed = true;
                continue;
            }
        }

        // R3: Order(Rename(x)) → Rename(Order(x)) (keys remapped new→old)
        if (kind == NodeKind::Order && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Rename) {
            auto& order_n = static_cast<OrderNode&>(*node);
            const auto& rename = static_cast<const RenameNode&>(*node->children().front());
            auto remapped = remap_keys_through_rename(order_n.keys(), rename.renames());
            NodePtr order_owned = std::move(node);
            NodePtr rename_owned = take_unique_child(*order_owned);
            NodePtr x = take_unique_child(*rename_owned);
            // Replace the order node with a new one carrying the remapped keys,
            // since OrderNode's keys_ member is private.
            NodePtr new_order = std::make_unique<OrderNode>(order_owned->id(), std::move(remapped));
            new_order->add_child(std::move(x));
            new_order = rewrite_root(std::move(new_order));
            rename_owned->add_child(std::move(new_order));
            node = std::move(rename_owned);
            changed = true;
            continue;
        }

        // R9: Rename(Rename(x)) → Rename(x) with composed mappings. Identity
        // pairs (new == old) drop out of the result.
        if (kind == NodeKind::Rename && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Rename) {
            const auto& outer = static_cast<const RenameNode&>(*node);
            const auto& inner = static_cast<const RenameNode&>(*node->children().front());
            auto composed = compose_renames(outer.renames(), inner.renames());
            const auto merged_id = node->id();
            NodePtr outer_owned = std::move(node);
            NodePtr inner_owned = take_unique_child(*outer_owned);
            NodePtr x = take_unique_child(*inner_owned);
            if (composed.empty()) {
                node = std::move(x);
            } else {
                auto merged = std::make_unique<RenameNode>(merged_id, std::move(composed));
                merged->add_child(std::move(x));
                node = std::move(merged);
            }
            changed = true;
            continue;
        }

        // R10: Drop a Rename whose entries are all identity (new == old) or
        // empty. Safe: produces the same schema as its input.
        if (kind == NodeKind::Rename && !node->children().empty()) {
            const auto& rn = static_cast<const RenameNode&>(*node);
            const bool all_identity =
                std::all_of(rn.renames().begin(), rn.renames().end(),
                            [](const RenameSpec& rs) { return rs.new_name == rs.old_name; });
            if (rn.renames().empty() || all_identity) {
                NodePtr rename_owned = std::move(node);
                node = take_unique_child(*rename_owned);
                changed = true;
                continue;
            }
        }

        // R11: Filter(Rename(x)) → Rename(Filter'(x)) with the predicate's
        // column references remapped new→old. Symmetric to R3 for Order;
        // bubbles Rename toward the root so it can compose via R9 and stop
        // blocking Project/Filter fusions beneath it.
        if (kind == NodeKind::Filter && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Rename) {
            auto& filter = static_cast<FilterNode&>(*node);
            const auto& rename = static_cast<const RenameNode&>(*node->children().front());
            std::unordered_map<std::string, std::string> n2o;
            n2o.reserve(rename.renames().size());
            for (const auto& rs : rename.renames()) {
                n2o.emplace(rs.new_name, rs.old_name);
            }
            FilterExprPtr pred = filter.take_predicate();
            remap_filter_expr_through_rename(*pred, n2o);
            const auto filter_id = node->id();
            NodePtr filter_owned = std::move(node);
            NodePtr rename_owned = take_unique_child(*filter_owned);
            NodePtr x = take_unique_child(*rename_owned);
            auto new_filter = std::make_unique<FilterNode>(filter_id, std::move(pred));
            new_filter->add_child(std::move(x));
            NodePtr bubbled = rewrite_root(std::move(new_filter));
            rename_owned->add_child(std::move(bubbled));
            node = rewrite_root(std::move(rename_owned));
            changed = true;
            continue;
        }

        // R12: Filter(Update(x)) → Update(Filter(x)) when the Update is
        // row-local and the predicate reads no column the Update produces.
        // Exposes Project(Update(Filter(x))) shapes to R6 for fusion.
        if (kind == NodeKind::Filter && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Update) {
            auto& filter = static_cast<FilterNode&>(*node);
            const auto& upd = static_cast<const UpdateNode&>(*node->children().front());
            const bool update_eligible =
                !upd.children().empty() && upd.group_by().empty() && upd.tuple_fields().empty() &&
                std::all_of(upd.fields().begin(), upd.fields().end(),
                            [](const FieldSpec& f) { return is_row_local_update_expr(f.expr); });
            if (update_eligible) {
                std::unordered_set<std::string> pred_cols;
                collect_filter_column_refs(filter.predicate(), pred_cols);
                const bool predicate_independent =
                    std::none_of(upd.fields().begin(), upd.fields().end(),
                                 [&](const FieldSpec& f) { return pred_cols.contains(f.alias); });
                if (predicate_independent) {
                    FilterExprPtr pred = filter.take_predicate();
                    const auto filter_id = node->id();
                    NodePtr filter_owned = std::move(node);
                    NodePtr update_owned = take_unique_child(*filter_owned);
                    NodePtr x = take_unique_child(*update_owned);
                    auto new_filter = std::make_unique<FilterNode>(filter_id, std::move(pred));
                    new_filter->add_child(std::move(x));
                    NodePtr bubbled = rewrite_root(std::move(new_filter));
                    update_owned->add_child(std::move(bubbled));
                    node = std::move(update_owned);
                    changed = true;
                    continue;
                }
            }
        }

        // R5: Project(Filter(x)) → FilterProject(x) — move the filter+project
        // fusion from build_operator into the IR so the interpreter and codegen
        // both see a single fused node instead of pattern-matching a shape.
        if (kind == NodeKind::Project && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Filter) {
            auto& proj = static_cast<ProjectNode&>(*node);
            std::vector<ColumnRef> cols = proj.columns();
            NodePtr project_owned = std::move(node);
            NodePtr filter_owned = take_unique_child(*project_owned);
            auto& filter = static_cast<FilterNode&>(*filter_owned);
            if (filter.children().empty()) {
                // Malformed — put it back and stop rewriting this position.
                project_owned->add_child(std::move(filter_owned));
                node = std::move(project_owned);
                break;
            }
            NodePtr x = take_unique_child(filter);
            FilterExprPtr pred = filter.take_predicate();
            auto fused = std::make_unique<FilterProjectNode>(project_owned->id(), std::move(pred),
                                                             std::move(cols));
            fused->add_child(std::move(x));
            node = std::move(fused);
            changed = true;
            continue;
        }

        // R6: Project(Update(Filter(x))) → FilterUpdateProject(x) when the
        // update is row-local (no tuple_fields, no group_by, no cross-row
        // callees in any field expression).
        if (kind == NodeKind::Project && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Update) {
            auto& proj = static_cast<ProjectNode&>(*node);
            auto& upd = static_cast<UpdateNode&>(*node->children().front());
            const bool update_eligible =
                !upd.children().empty() && upd.group_by().empty() && upd.tuple_fields().empty() &&
                std::all_of(upd.fields().begin(), upd.fields().end(),
                            [](const FieldSpec& f) { return is_row_local_update_expr(f.expr); });
            if (update_eligible && upd.children().front()->kind() == NodeKind::Filter) {
                auto& filter = static_cast<FilterNode&>(*upd.children().front());
                if (!filter.children().empty()) {
                    std::vector<ColumnRef> proj_cols = proj.columns();
                    // UpdateNode stores fields privately — reconstruct a new
                    // UpdateNode temporarily to steal the fields via a move
                    // from the mutable children path. Since the UpdateNode is
                    // about to be discarded, do a const_cast-free move through
                    // a temporary: build a moved copy of the fields vector.
                    // (The accessor returns const&; we copy once here.)
                    std::vector<FieldSpec> fields = upd.fields();
                    NodePtr project_owned = std::move(node);
                    NodePtr update_owned = take_unique_child(*project_owned);
                    NodePtr filter_owned = take_unique_child(*update_owned);
                    auto& filter_ref = static_cast<FilterNode&>(*filter_owned);
                    NodePtr x = take_unique_child(filter_ref);
                    FilterExprPtr pred = filter_ref.take_predicate();
                    auto fused = std::make_unique<FilterUpdateProjectNode>(
                        project_owned->id(), std::move(pred), std::move(fields),
                        std::move(proj_cols));
                    fused->add_child(std::move(x));
                    node = std::move(fused);
                    changed = true;
                    continue;
                }
            }
        }

        // R7/R8: Head(Filter(x)) → FilterHead(x), Tail(Filter(x)) → FilterTail(x)
        // when the Head/Tail has no group_by. The fused operator stops pulling
        // from the source as soon as n surviving rows are found (Head) or
        // maintains only a rolling tail buffer (Tail).
        if ((kind == NodeKind::Head || kind == NodeKind::Tail) && !node->children().empty() &&
            node->children().front()->kind() == NodeKind::Filter) {
            const auto& group_by = (kind == NodeKind::Head)
                                       ? static_cast<const HeadNode&>(*node).group_by()
                                       : static_cast<const TailNode&>(*node).group_by();
            if (group_by.empty()) {
                const auto count = (kind == NodeKind::Head)
                                       ? static_cast<const HeadNode&>(*node).count()
                                       : static_cast<const TailNode&>(*node).count();
                const auto fused_id = node->id();
                NodePtr limit_owned = std::move(node);
                NodePtr filter_owned = take_unique_child(*limit_owned);
                auto& filter = static_cast<FilterNode&>(*filter_owned);
                if (!filter.children().empty()) {
                    NodePtr x = take_unique_child(filter);
                    FilterExprPtr pred = filter.take_predicate();
                    NodePtr fused;
                    if (kind == NodeKind::Head) {
                        fused = std::make_unique<FilterHeadNode>(fused_id, std::move(pred), count);
                    } else {
                        fused = std::make_unique<FilterTailNode>(fused_id, std::move(pred), count);
                    }
                    fused->add_child(std::move(x));
                    node = std::move(fused);
                    changed = true;
                    continue;
                }
                // Malformed: put it back.
                limit_owned->add_child(std::move(filter_owned));
                node = std::move(limit_owned);
            }
        }

        // R4: Head/Tail past Project and Rename.
        if (kind == NodeKind::Head || kind == NodeKind::Tail) {
            if (node->children().empty()) {
                break;
            }
            const auto child_kind = node->children().front()->kind();
            if (child_kind == NodeKind::Project || child_kind == NodeKind::Rename) {
                const auto& group_by = (kind == NodeKind::Head)
                                           ? static_cast<const HeadNode&>(*node).group_by()
                                           : static_cast<const TailNode&>(*node).group_by();
                bool safe = false;
                std::vector<ColumnRef> remapped_group_by = group_by;
                if (child_kind == NodeKind::Project) {
                    const auto& proj = static_cast<const ProjectNode&>(*node->children().front());
                    safe = group_by.empty() || group_by_preserved_by_project(group_by, proj);
                } else {
                    const auto& ren = static_cast<const RenameNode&>(*node->children().front());
                    remapped_group_by = remap_group_by_through_rename(group_by, ren.renames());
                    safe = true;
                }
                if (safe) {
                    const auto count = (kind == NodeKind::Head)
                                           ? static_cast<const HeadNode&>(*node).count()
                                           : static_cast<const TailNode&>(*node).count();
                    const auto limit_id = node->id();
                    NodePtr limit = std::move(node);
                    NodePtr wrapper = take_unique_child(*limit);
                    NodePtr x = take_unique_child(*wrapper);
                    NodePtr new_limit;
                    if (kind == NodeKind::Head) {
                        new_limit = std::make_unique<HeadNode>(limit_id, count,
                                                               std::move(remapped_group_by));
                    } else {
                        new_limit = std::make_unique<TailNode>(limit_id, count,
                                                               std::move(remapped_group_by));
                    }
                    new_limit->add_child(std::move(x));
                    new_limit = rewrite_root(std::move(new_limit));
                    wrapper->add_child(std::move(new_limit));
                    node = std::move(wrapper);
                    changed = true;
                    continue;
                }
            }
        }
    }
    return node;
}

auto canon(NodePtr node) -> NodePtr {
    if (!node) {
        return node;
    }
    // Post-order: canonicalize children first, then rewrite at root.
    for (auto& child : node->mutable_children()) {
        child = canon(std::move(child));
    }
    // ProgramNode's main is stored separately; descend into it too.
    if (node->kind() == NodeKind::Program) {
        auto& prog = static_cast<ProgramNode&>(*node);
        if (prog.mutable_main_node()) {
            prog.mutable_main_node() = canon(std::move(prog.mutable_main_node()));
        }
        for (auto& pre : prog.mutable_preamble()) {
            pre = canon(std::move(pre));
        }
    }
    return rewrite_root(std::move(node));
}

}  // namespace

auto canonicalize(NodePtr root) -> NodePtr {
    return canon(std::move(root));
}

auto CanonicalizePass::run(NodePtr root, const OptimizationContext& /*context*/,
                           OptimizationStats& /*stats*/) const -> NodePtr {
    return canonicalize(std::move(root));
}

}  // namespace ibex::ir
