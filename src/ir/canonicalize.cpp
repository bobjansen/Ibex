#include <ibex/ir/canonicalize.hpp>
#include <ibex/ir/expr_predicates.hpp>

#include <algorithm>
#include <array>
#include <optional>
#include <robin_hood.h>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace ibex::ir {

namespace {

// Counter for synthesizing fresh NodeIds during a canonicalize pass. Seeded at
// canonicalize() entry to (max id in tree + 1) so it never collides with
// existing IDs in the input.
thread_local std::uint64_t g_next_id = 0;

auto fresh_id() -> NodeId {
    return NodeId{g_next_id++};
}

auto collect_max_id(const Node& n, std::uint64_t& m) -> void {
    m = std::max(m, n.id().value);
    for (const auto& c : n.children()) {
        collect_max_id(*c, m);
    }
    if (n.kind() == NodeKind::Program) {
        const auto& prog = static_cast<const ProgramNode&>(n);
        for (const auto& p : prog.preamble()) {
            if (p) {
                collect_max_id(*p, m);
            }
        }
        const auto& main_ptr = const_cast<ProgramNode&>(prog).mutable_main_node();
        if (main_ptr) {
            collect_max_id(*main_ptr, m);
        }
    }
}

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
    robin_hood::unordered_map<std::string, std::string> new_to_old;
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
    robin_hood::unordered_map<std::string, std::string> new_to_old;
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

// Collect the set of column names referenced by a predicate expression.
void collect_filter_column_refs(const Expr& expr, std::unordered_set<std::string>& out) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                out.insert(n.name);
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                collect_filter_column_refs(*n.left, out);
                collect_filter_column_refs(*n.right, out);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                collect_filter_column_refs(*n.left, out);
                if (n.right) {
                    collect_filter_column_refs(*n.right, out);
                }
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (const auto& arg : n.args) {
                    collect_filter_column_refs(*arg, out);
                }
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                collect_filter_column_refs(*n.operand, out);
            }
        },
        expr.node);
}

// Remap ColumnRef references inside a predicate expression new→old.
void remap_filter_expr_through_rename(
    Expr& expr, const robin_hood::unordered_map<std::string, std::string>& n2o) {
    std::visit(
        [&](auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                auto it = n2o.find(n.name);
                if (it != n2o.end()) {
                    n.name = it->second;
                }
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                remap_filter_expr_through_rename(*n.left, n2o);
                remap_filter_expr_through_rename(*n.right, n2o);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                remap_filter_expr_through_rename(*n.left, n2o);
                if (n.right) {
                    remap_filter_expr_through_rename(*n.right, n2o);
                }
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (auto& arg : n.args) {
                    remap_filter_expr_through_rename(*arg, n2o);
                }
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
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
    robin_hood::unordered_map<std::string, std::string> inner_new_to_old;
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

// Collect columns pinned to a literal constant by a conjunctive predicate.
// Walks through FilterAnd nodes; any `col == literal` (or `literal == col`)
// contributes `col`. Other shapes (OR, NOT, arithmetic, non-Eq comparisons)
// are skipped — the column is not provably constant under those.
void collect_equality_pinned_cols(const Expr& expr, std::unordered_set<std::string>& out) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, LogicalExpr>) {
                if (n.op == LogicalOp::And) {
                    collect_equality_pinned_cols(*n.left, out);
                    collect_equality_pinned_cols(*n.right, out);
                }
            } else if constexpr (std::is_same_v<T, CompareExpr>) {
                if (n.op == CompareOp::Eq) {
                    const auto* lcol = std::get_if<ColumnRef>(&n.left->node);
                    const auto* llit = std::get_if<Literal>(&n.left->node);
                    const auto* rcol = std::get_if<ColumnRef>(&n.right->node);
                    const auto* rlit = std::get_if<Literal>(&n.right->node);
                    if ((lcol != nullptr) && (rlit != nullptr)) {
                        out.insert(lcol->name);
                    } else if ((rcol != nullptr) && (llit != nullptr)) {
                        out.insert(rcol->name);
                    }
                }
            }
        },
        expr.node);
}

auto canon(NodePtr node) -> NodePtr;
auto rewrite_root(NodePtr node) -> NodePtr;

// Helpers for predicate simplification (R17).

auto make_bool_lit(bool b) -> Expr {
    return Expr{.node = Literal{.value = b}};
}

auto try_get_bool(const Expr& expr) -> std::optional<bool> {
    if (const auto* lit = std::get_if<Literal>(&expr.node)) {
        if (const auto* b = std::get_if<bool>(&lit->value)) {
            return *b;
        }
    }
    return std::nullopt;
}

// Evaluate a binary comparison between two FilterLiteral values, when their
// underlying types match. Returns std::nullopt if the comparison isn't
// well-defined (mismatched types, or the variant alternative isn't ordered).
auto fold_cmp(CompareOp op, const Literal& l, const Literal& r) -> std::optional<bool> {
    if (l.value.index() != r.value.index()) {
        return std::nullopt;
    }
    return std::visit(
        [&](const auto& lv) -> std::optional<bool> {
            using T = std::decay_t<decltype(lv)>;
            const auto& rv = std::get<T>(r.value);
            switch (op) {
                case CompareOp::Eq:
                    return lv == rv;
                case CompareOp::Ne:
                    return lv != rv;
                case CompareOp::Lt:
                    return lv < rv;
                case CompareOp::Le:
                    return lv <= rv;
                case CompareOp::Gt:
                    return lv > rv;
                case CompareOp::Ge:
                    return lv >= rv;
            }
            return std::nullopt;
        },
        l.value);
}

// Fold an arithmetic op on two literals when both are numeric (int64 or
// double, matching types). Mod is integer-only. Returns nullopt otherwise.
auto fold_arith(ArithmeticOp op, const Literal& l, const Literal& r) -> std::optional<Literal> {
    if (const auto* li = std::get_if<std::int64_t>(&l.value)) {
        const auto* ri = std::get_if<std::int64_t>(&r.value);
        if (ri == nullptr) {
            return std::nullopt;
        }
        switch (op) {
            case ArithmeticOp::Add:
                return Literal{.value = *li + *ri};
            case ArithmeticOp::Sub:
                return Literal{.value = *li - *ri};
            case ArithmeticOp::Mul:
                return Literal{.value = *li * *ri};
            case ArithmeticOp::Div:
                if (*ri == 0) {
                    return std::nullopt;
                }
                return Literal{.value = *li / *ri};
            case ArithmeticOp::Mod:
                if (*ri == 0) {
                    return std::nullopt;
                }
                return Literal{.value = *li % *ri};
        }
        return std::nullopt;
    }
    if (const auto* ld = std::get_if<double>(&l.value)) {
        const auto* rd = std::get_if<double>(&r.value);
        if (rd == nullptr) {
            return std::nullopt;
        }
        switch (op) {
            case ArithmeticOp::Add:
                return Literal{.value = *ld + *rd};
            case ArithmeticOp::Sub:
                return Literal{.value = *ld - *rd};
            case ArithmeticOp::Mul:
                return Literal{.value = *ld * *rd};
            case ArithmeticOp::Div:
                return Literal{.value = *ld / *rd};
            case ArithmeticOp::Mod:
                return std::nullopt;
        }
    }
    return std::nullopt;
}

// Bottom-up predicate simplification. Sets `*changed` to true if the result
// differs structurally from the input. Rewrites:
//   - boolean identity/absorbing: x AND true → x, x OR false → x, etc.
//   - double-negation: NOT NOT x → x; NOT literal → folded literal.
//   - literal-only Cmp / Arith → folded literal (Eq, Ne, Lt, …, +, -, …).
//   - IsNull/IsNotNull on a literal → folded bool (literals are never null).
auto simplify_expr(Expr expr, bool* changed) -> Expr {
    return std::visit(
        [&](auto& n) -> Expr {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, LogicalExpr>) {
                if (n.op == LogicalOp::Not) {
                    *n.left = simplify_expr(std::move(*n.left), changed);
                    if (auto b = try_get_bool(*n.left); b.has_value()) {
                        *changed = true;
                        return make_bool_lit(!*b);
                    }
                    if (auto* inner = std::get_if<LogicalExpr>(&n.left->node);
                        (inner != nullptr) && inner->op == LogicalOp::Not) {
                        *changed = true;
                        return std::move(*inner->left);
                    }
                    return std::move(expr);
                }
                *n.left = simplify_expr(std::move(*n.left), changed);
                *n.right = simplify_expr(std::move(*n.right), changed);
                const bool is_and = n.op == LogicalOp::And;
                if (auto lb = try_get_bool(*n.left); lb.has_value()) {
                    *changed = true;
                    if (is_and) {
                        return *lb ? std::move(*n.right) : make_bool_lit(false);
                    }
                    return *lb ? make_bool_lit(true) : std::move(*n.right);
                }
                if (auto rb = try_get_bool(*n.right); rb.has_value()) {
                    *changed = true;
                    if (is_and) {
                        return *rb ? std::move(*n.left) : make_bool_lit(false);
                    }
                    return *rb ? make_bool_lit(true) : std::move(*n.left);
                }
                return std::move(expr);
            } else if constexpr (std::is_same_v<T, CompareExpr>) {
                *n.left = simplify_expr(std::move(*n.left), changed);
                *n.right = simplify_expr(std::move(*n.right), changed);
                const auto* ll = std::get_if<Literal>(&n.left->node);
                const auto* rl = std::get_if<Literal>(&n.right->node);
                if ((ll != nullptr) && (rl != nullptr)) {
                    if (auto folded = fold_cmp(n.op, *ll, *rl); folded.has_value()) {
                        *changed = true;
                        return make_bool_lit(*folded);
                    }
                }
                return std::move(expr);
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                *n.left = simplify_expr(std::move(*n.left), changed);
                *n.right = simplify_expr(std::move(*n.right), changed);
                const auto* ll = std::get_if<Literal>(&n.left->node);
                const auto* rl = std::get_if<Literal>(&n.right->node);
                if ((ll != nullptr) && (rl != nullptr)) {
                    if (auto folded = fold_arith(n.op, *ll, *rl); folded.has_value()) {
                        *changed = true;
                        return Expr{.node = std::move(*folded)};
                    }
                }
                return std::move(expr);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (auto& arg : n.args) {
                    *arg = simplify_expr(std::move(*arg), changed);
                }
                return std::move(expr);
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                *n.operand = simplify_expr(std::move(*n.operand), changed);
                if (std::holds_alternative<Literal>(n.operand->node)) {
                    *changed = true;
                    return make_bool_lit(n.negated);
                }
                return std::move(expr);
            } else {
                return std::move(expr);
            }
        },
        expr.node);
}

// Result of a single rule attempt: if the rule fired, `changed` is true and
// `node` holds the rewritten tree; otherwise `node` is returned unchanged.
struct TryResult {
    bool changed;
    NodePtr node;
};

// R17: Simplify a Filter's predicate (boolean identity/absorption, double
// negation, literal-only comparison/arithmetic folding). If the predicate
// reduces to literal `true`, the Filter is dropped; if it reduces to literal
// `false`, the Filter is replaced by Head(0, x) so downstream stages see an
// empty input without a custom node.
auto try_simplify_predicate(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Filter || node->children().empty()) {
        return {false, std::move(node)};
    }
    auto& filter = static_cast<FilterNode&>(*node);
    Expr pred = filter.take_predicate();
    bool changed = false;
    pred = simplify_expr(std::move(pred), &changed);
    if (auto b = try_get_bool(pred); b.has_value()) {
        const auto filter_id = node->id();
        NodePtr filter_owned = std::move(node);
        NodePtr x = take_unique_child(*filter_owned);
        if (*b) {
            return {true, std::move(x)};
        }
        auto h = std::make_unique<HeadNode>(filter_id, 0, std::vector<ColumnRef>{});
        h->add_child(std::move(x));
        return {true, std::move(h)};
    }
    if (!changed) {
        // Put the predicate back unchanged.
        const auto filter_id = node->id();
        NodePtr filter_owned = std::move(node);
        NodePtr x = take_unique_child(*filter_owned);
        auto restored = std::make_unique<FilterNode>(filter_id, std::move(pred));
        restored->add_child(std::move(x));
        return {false, std::move(restored)};
    }
    const auto filter_id = node->id();
    NodePtr filter_owned = std::move(node);
    NodePtr x = take_unique_child(*filter_owned);
    auto rebuilt = std::make_unique<FilterNode>(filter_id, std::move(pred));
    rebuilt->add_child(std::move(x));
    return {true, std::move(rebuilt)};
}

// R1: Filter(Order(x)) → Order(Filter(x))
auto try_filter_past_order(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Filter || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Order) {
        return {false, std::move(node)};
    }
    NodePtr filter = std::move(node);
    NodePtr order = take_unique_child(*filter);
    NodePtr x = take_unique_child(*order);
    filter->add_child(std::move(x));
    filter = rewrite_root(std::move(filter));
    order->add_child(std::move(filter));
    return {true, std::move(order)};
}

// R2: Project(Order(x)) → Order(Project(x)) when all keys are preserved.
auto try_project_past_order(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Project || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Order) {
        return {false, std::move(node)};
    }
    const auto& proj = static_cast<const ProjectNode&>(*node);
    const auto& order = static_cast<const OrderNode&>(*node->children().front());
    if (!keys_preserved_by_project(order.keys(), proj)) {
        return {false, std::move(node)};
    }
    NodePtr project = std::move(node);
    NodePtr order_n = take_unique_child(*project);
    NodePtr x = take_unique_child(*order_n);
    project->add_child(std::move(x));
    project = rewrite_root(std::move(project));
    order_n->add_child(std::move(project));
    return {true, std::move(order_n)};
}

// R3: Order(Rename(x)) → Rename(Order(x)) with keys remapped new→old.
auto try_order_past_rename(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Order || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Rename) {
        return {false, std::move(node)};
    }
    auto& order_n = static_cast<OrderNode&>(*node);
    const auto& rename = static_cast<const RenameNode&>(*node->children().front());
    auto remapped = remap_keys_through_rename(order_n.keys(), rename.renames());
    NodePtr order_owned = std::move(node);
    NodePtr rename_owned = take_unique_child(*order_owned);
    NodePtr x = take_unique_child(*rename_owned);
    NodePtr new_order = std::make_unique<OrderNode>(order_owned->id(), std::move(remapped));
    new_order->add_child(std::move(x));
    new_order = rewrite_root(std::move(new_order));
    rename_owned->add_child(std::move(new_order));
    return {true, std::move(rename_owned)};
}

// R9: Rename(Rename(x)) → single Rename with composed mappings.
auto try_rename_compose(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Rename || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Rename) {
        return {false, std::move(node)};
    }
    const auto& outer = static_cast<const RenameNode&>(*node);
    const auto& inner = static_cast<const RenameNode&>(*node->children().front());
    auto composed = compose_renames(outer.renames(), inner.renames());
    const auto merged_id = node->id();
    NodePtr outer_owned = std::move(node);
    NodePtr inner_owned = take_unique_child(*outer_owned);
    NodePtr x = take_unique_child(*inner_owned);
    if (composed.empty()) {
        return {true, std::move(x)};
    }
    auto merged = std::make_unique<RenameNode>(merged_id, std::move(composed));
    merged->add_child(std::move(x));
    return {true, std::move(merged)};
}

// R10: Drop a Rename whose entries are empty or all identity.
auto try_rename_drop_identity(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Rename || node->children().empty()) {
        return {false, std::move(node)};
    }
    const auto& rn = static_cast<const RenameNode&>(*node);
    const bool all_identity =
        std::all_of(rn.renames().begin(), rn.renames().end(),
                    [](const RenameSpec& rs) { return rs.new_name == rs.old_name; });
    if (!rn.renames().empty() && !all_identity) {
        return {false, std::move(node)};
    }
    NodePtr rename_owned = std::move(node);
    return {true, take_unique_child(*rename_owned)};
}

// R11: Filter(Rename(x)) → Rename(Filter'(x)) with predicate column refs remapped.
auto try_filter_past_rename(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Filter || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Rename) {
        return {false, std::move(node)};
    }
    auto& filter = static_cast<FilterNode&>(*node);
    const auto& rename = static_cast<const RenameNode&>(*node->children().front());
    robin_hood::unordered_map<std::string, std::string> n2o;
    n2o.reserve(rename.renames().size());
    for (const auto& rs : rename.renames()) {
        n2o.emplace(rs.new_name, rs.old_name);
    }
    Expr pred = filter.take_predicate();
    remap_filter_expr_through_rename(pred, n2o);
    const auto filter_id = node->id();
    NodePtr filter_owned = std::move(node);
    NodePtr rename_owned = take_unique_child(*filter_owned);
    NodePtr x = take_unique_child(*rename_owned);
    auto new_filter = std::make_unique<FilterNode>(filter_id, std::move(pred));
    new_filter->add_child(std::move(x));
    NodePtr bubbled = rewrite_root(std::move(new_filter));
    rename_owned->add_child(std::move(bubbled));
    return {true, rewrite_root(std::move(rename_owned))};
}

// R13/R14: Head(Head(x)) / Tail(Tail(x)) → single node with tighter bound.
auto try_limit_collapse(NodePtr node) -> TryResult {
    const auto kind = node->kind();
    if ((kind != NodeKind::Head && kind != NodeKind::Tail) || node->children().empty() ||
        node->children().front()->kind() != kind) {
        return {false, std::move(node)};
    }
    const auto outer_count = (kind == NodeKind::Head)
                                 ? static_cast<const HeadNode&>(*node).count_literal()
                                 : static_cast<const TailNode&>(*node).count_literal();
    const auto& outer_gb = (kind == NodeKind::Head)
                               ? static_cast<const HeadNode&>(*node).group_by()
                               : static_cast<const TailNode&>(*node).group_by();
    const auto& inner = *node->children().front();
    const auto inner_count = (kind == NodeKind::Head)
                                 ? static_cast<const HeadNode&>(inner).count_literal()
                                 : static_cast<const TailNode&>(inner).count_literal();
    const auto& inner_gb = (kind == NodeKind::Head)
                               ? static_cast<const HeadNode&>(inner).group_by()
                               : static_cast<const TailNode&>(inner).group_by();
    if (!outer_gb.empty() || !inner_gb.empty() || !outer_count.has_value() ||
        !inner_count.has_value()) {
        return {false, std::move(node)};
    }
    const auto merged_count = std::min(*outer_count, *inner_count);
    const auto merged_id = node->id();
    NodePtr outer_owned = std::move(node);
    NodePtr inner_owned = take_unique_child(*outer_owned);
    NodePtr x = take_unique_child(*inner_owned);
    NodePtr merged;
    if (kind == NodeKind::Head) {
        merged = std::make_unique<HeadNode>(merged_id, merged_count, std::vector<ColumnRef>{});
    } else {
        merged = std::make_unique<TailNode>(merged_id, merged_count, std::vector<ColumnRef>{});
    }
    merged->add_child(std::move(x));
    return {true, std::move(merged)};
}

// R15: Drop Order keys pinned to constants by an immediate Filter child.
auto try_order_drop_pinned_keys(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Order || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Filter) {
        return {false, std::move(node)};
    }
    const auto& order_n = static_cast<const OrderNode&>(*node);
    const auto& filter = static_cast<const FilterNode&>(*node->children().front());
    std::unordered_set<std::string> pinned;
    collect_equality_pinned_cols(filter.predicate(), pinned);
    if (pinned.empty()) {
        return {false, std::move(node)};
    }
    std::vector<OrderKey> surviving;
    surviving.reserve(order_n.keys().size());
    for (const auto& k : order_n.keys()) {
        if (!pinned.contains(k.name)) {
            surviving.push_back(k);
        }
    }
    if (surviving.size() == order_n.keys().size()) {
        return {false, std::move(node)};
    }
    const auto order_id = node->id();
    NodePtr order_owned = std::move(node);
    NodePtr filter_owned = take_unique_child(*order_owned);
    if (surviving.empty()) {
        return {true, std::move(filter_owned)};
    }
    auto new_order = std::make_unique<OrderNode>(order_id, std::move(surviving));
    new_order->add_child(std::move(filter_owned));
    return {true, std::move(new_order)};
}

// R12: Filter(Update(x)) → Update(Filter(x)) when Update is row-local and
// the predicate reads none of the Update's output columns.
auto try_filter_past_update(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Filter || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Update) {
        return {false, std::move(node)};
    }
    auto& filter = static_cast<FilterNode&>(*node);
    const auto& upd = static_cast<const UpdateNode&>(*node->children().front());
    const bool update_eligible =
        !upd.children().empty() && upd.group_by().empty() && upd.tuple_fields().empty() &&
        std::all_of(upd.fields().begin(), upd.fields().end(),
                    [](const FieldSpec& f) { return is_row_local_update_expr(f.expr); });
    if (!update_eligible) {
        return {false, std::move(node)};
    }
    std::unordered_set<std::string> pred_cols;
    collect_filter_column_refs(filter.predicate(), pred_cols);
    const bool predicate_independent =
        std::none_of(upd.fields().begin(), upd.fields().end(),
                     [&](const FieldSpec& f) { return pred_cols.contains(f.alias); });
    if (!predicate_independent) {
        return {false, std::move(node)};
    }
    Expr pred = filter.take_predicate();
    const auto filter_id = node->id();
    NodePtr filter_owned = std::move(node);
    NodePtr update_owned = take_unique_child(*filter_owned);
    NodePtr x = take_unique_child(*update_owned);
    auto new_filter = std::make_unique<FilterNode>(filter_id, std::move(pred));
    new_filter->add_child(std::move(x));
    NodePtr bubbled = rewrite_root(std::move(new_filter));
    update_owned->add_child(std::move(bubbled));
    return {true, std::move(update_owned)};
}

// R18: Filter(Aggregate(x)) → Aggregate(Filter(x)) when the predicate only
// references group_by columns. Group_by columns pass through the aggregate
// unchanged, so filtering them above is observably the same as below — but
// below the aggregate the input is typically much larger (every row), so the
// filter eliminates work for the aggregate itself. Predicates referencing
// aggregation aliases (HAVING-style) cannot push down and are left in place.
// R21: collapse a redundant Project below a Project / FilterProject. The outer
// node already restricts columns to its own list; the inner Project's column
// list must be a superset (otherwise the outer would reference missing cols),
// so dropping it is always sound and removes a needless schema-shuffle pass.
auto try_project_collapse(NodePtr node) -> TryResult {
    const auto kind = node->kind();
    if ((kind != NodeKind::Project && kind != NodeKind::FilterProject) ||
        node->children().empty() || node->children().front()->kind() != NodeKind::Project) {
        return {false, std::move(node)};
    }
    auto& outer_kids = node->mutable_children();
    NodePtr inner = std::move(outer_kids.front());
    outer_kids.clear();
    NodePtr grandchild = take_unique_child(*inner);
    node->add_child(std::move(grandchild));
    return {true, std::move(node)};
}

// R20: Aggregate(gb, aggs, x) → Aggregate(gb, aggs, Project(needed, x)) where
// `needed` = unique columns referenced by `gb` ∪ each agg's input column. Drops
// unused columns before the breaker so the aggregate scans less data. Skipped
// when `x` is already a projecting node, which both avoids redundant work and
// prevents an infinite loop (the inserted Project would re-trigger the rule).
auto try_project_prune_above_aggregate(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Aggregate || node->children().empty()) {
        return {false, std::move(node)};
    }
    // Peek past any leading Order nodes: once this rule inserts a Project, R2
    // (project-past-order) pushes it *below* the Order, leaving the Aggregate's
    // direct child as Order again. Without looking through it, R20 would not see
    // the Project it just created and would re-fire forever (R20 ↔ R2 loop).
    const Node* descendant = node->children().front().get();
    while (descendant->kind() == NodeKind::Order && !descendant->children().empty()) {
        descendant = descendant->children().front().get();
    }
    const auto child_kind = descendant->kind();
    if (child_kind == NodeKind::Project || child_kind == NodeKind::FilterProject ||
        child_kind == NodeKind::FilterUpdateProject) {
        return {false, std::move(node)};
    }
    auto& agg = static_cast<AggregateNode&>(*node);
    std::vector<ColumnRef> needed;
    std::unordered_set<std::string> seen;
    needed.reserve(agg.group_by().size() + agg.aggregations().size());
    for (const auto& g : agg.group_by()) {
        if (seen.insert(g.name).second) {
            needed.push_back(g);
        }
    }
    for (const auto& a : agg.aggregations()) {
        if (a.column.name.empty()) {
            // count(*) / unbound input — can't safely prune since the agg
            // doesn't name a specific column.
            return {false, std::move(node)};
        }
        if (seen.insert(a.column.name).second) {
            needed.push_back(a.column);
        }
    }
    if (needed.empty()) {
        return {false, std::move(node)};
    }
    NodePtr agg_owned = std::move(node);
    NodePtr child = take_unique_child(*agg_owned);
    auto proj = std::make_unique<ProjectNode>(fresh_id(), std::move(needed));
    proj->add_child(std::move(child));
    // Rewrite the freshly inserted Project at root so any pending rules (e.g.
    // R5 Project(Filter) → FilterProject) fire before the Aggregate sees it.
    NodePtr proj_canon = rewrite_root(std::move(proj));
    agg_owned->add_child(std::move(proj_canon));
    return {true, std::move(agg_owned)};
}

// R19: Filter(p1, Filter(p2, x)) → Filter(p1 AND p2, x). Merges adjacent
// filters so downstream rules see one combined predicate (richer column-ref
// set, more chances to fuse/push).
auto try_filter_merge(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Filter || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Filter) {
        return {false, std::move(node)};
    }
    auto& outer = static_cast<FilterNode&>(*node);
    auto& inner = static_cast<FilterNode&>(*node->children().front());
    Expr p1 = outer.take_predicate();
    Expr p2 = inner.take_predicate();
    Expr combined{.node = LogicalExpr{.op = LogicalOp::And,
                                      .left = make_expr_ptr(std::move(p1)),
                                      .right = make_expr_ptr(std::move(p2))}};
    const auto outer_id = node->id();
    NodePtr outer_owned = std::move(node);
    NodePtr inner_owned = take_unique_child(*outer_owned);
    NodePtr x = take_unique_child(*inner_owned);
    auto merged = std::make_unique<FilterNode>(outer_id, std::move(combined));
    merged->add_child(std::move(x));
    return {true, std::move(merged)};
}

auto try_filter_past_aggregate(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Filter || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Aggregate) {
        return {false, std::move(node)};
    }
    auto& filter = static_cast<FilterNode&>(*node);
    const auto& agg = static_cast<const AggregateNode&>(*node->children().front());
    if (agg.children().empty() || agg.group_by().empty()) {
        // No group_by ⇒ exactly one output row; predicate either keeps it or
        // drops it, but that's a HAVING-style filter — not pushable.
        return {false, std::move(node)};
    }
    std::unordered_set<std::string> gb_names;
    gb_names.reserve(agg.group_by().size());
    for (const auto& c : agg.group_by()) {
        gb_names.insert(c.name);
    }
    std::unordered_set<std::string> pred_cols;
    collect_filter_column_refs(filter.predicate(), pred_cols);
    const bool only_group_by =
        std::all_of(pred_cols.begin(), pred_cols.end(),
                    [&](const std::string& c) { return gb_names.contains(c); });
    if (!only_group_by) {
        return {false, std::move(node)};
    }
    Expr pred = filter.take_predicate();
    const auto filter_id = node->id();
    NodePtr filter_owned = std::move(node);
    NodePtr agg_owned = take_unique_child(*filter_owned);
    NodePtr x = take_unique_child(*agg_owned);
    auto new_filter = std::make_unique<FilterNode>(filter_id, std::move(pred));
    new_filter->add_child(std::move(x));
    NodePtr bubbled = rewrite_root(std::move(new_filter));
    agg_owned->add_child(std::move(bubbled));
    return {true, std::move(agg_owned)};
}

// R5: Project(Filter(x)) → FilterProject(x).
auto try_fuse_filter_project(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Project || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Filter) {
        return {false, std::move(node)};
    }
    auto& proj = static_cast<ProjectNode&>(*node);
    std::vector<ColumnRef> cols = proj.columns();
    NodePtr project_owned = std::move(node);
    NodePtr filter_owned = take_unique_child(*project_owned);
    auto& filter = static_cast<FilterNode&>(*filter_owned);
    if (filter.children().empty()) {
        project_owned->add_child(std::move(filter_owned));
        return {false, std::move(project_owned)};
    }
    NodePtr x = take_unique_child(filter);
    Expr pred = filter.take_predicate();
    auto fused =
        std::make_unique<FilterProjectNode>(project_owned->id(), std::move(pred), std::move(cols));
    fused->add_child(std::move(x));
    return {true, std::move(fused)};
}

// R6: Project(Update(Filter(x))) → FilterUpdateProject(x) when update is row-local.
auto try_fuse_filter_update_project(NodePtr node) -> TryResult {
    if (node->kind() != NodeKind::Project || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Update) {
        return {false, std::move(node)};
    }
    auto& proj = static_cast<ProjectNode&>(*node);
    auto& upd = static_cast<UpdateNode&>(*node->children().front());
    const bool update_eligible =
        !upd.children().empty() && upd.group_by().empty() && upd.tuple_fields().empty() &&
        std::all_of(upd.fields().begin(), upd.fields().end(),
                    [](const FieldSpec& f) { return is_row_local_update_expr(f.expr); });
    if (!update_eligible || upd.children().front()->kind() != NodeKind::Filter) {
        return {false, std::move(node)};
    }
    auto& filter = static_cast<FilterNode&>(*upd.children().front());
    if (filter.children().empty()) {
        return {false, std::move(node)};
    }
    std::vector<ColumnRef> proj_cols = proj.columns();
    std::vector<FieldSpec> fields = upd.fields();
    NodePtr project_owned = std::move(node);
    NodePtr update_owned = take_unique_child(*project_owned);
    NodePtr filter_owned = take_unique_child(*update_owned);
    auto& filter_ref = static_cast<FilterNode&>(*filter_owned);
    NodePtr x = take_unique_child(filter_ref);
    Expr pred = filter_ref.take_predicate();
    auto fused = std::make_unique<FilterUpdateProjectNode>(project_owned->id(), std::move(pred),
                                                           std::move(fields), std::move(proj_cols));
    fused->add_child(std::move(x));
    return {true, std::move(fused)};
}

// R7/R8: Head(Filter(x)) → FilterHead(x), Tail(Filter(x)) → FilterTail(x)
// when the limit has no group_by.
auto try_fuse_filter_limit(NodePtr node) -> TryResult {
    const auto kind = node->kind();
    if ((kind != NodeKind::Head && kind != NodeKind::Tail) || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Filter) {
        return {false, std::move(node)};
    }
    const auto& group_by = (kind == NodeKind::Head)
                               ? static_cast<const HeadNode&>(*node).group_by()
                               : static_cast<const TailNode&>(*node).group_by();
    if (!group_by.empty()) {
        return {false, std::move(node)};
    }
    const auto count = (kind == NodeKind::Head)
                           ? static_cast<const HeadNode&>(*node).count_literal()
                           : static_cast<const TailNode&>(*node).count_literal();
    if (!count.has_value()) {
        return {false, std::move(node)};
    }
    const auto fused_id = node->id();
    NodePtr limit_owned = std::move(node);
    NodePtr filter_owned = take_unique_child(*limit_owned);
    auto& filter = static_cast<FilterNode&>(*filter_owned);
    if (filter.children().empty()) {
        limit_owned->add_child(std::move(filter_owned));
        return {false, std::move(limit_owned)};
    }
    NodePtr x = take_unique_child(filter);
    Expr pred = filter.take_predicate();
    NodePtr fused;
    if (kind == NodeKind::Head) {
        fused = std::make_unique<FilterHeadNode>(fused_id, std::move(pred), *count);
    } else {
        fused = std::make_unique<FilterTailNode>(fused_id, std::move(pred), *count);
    }
    fused->add_child(std::move(x));
    return {true, std::move(fused)};
}

// R16: Head(Order(x)) → TopK(..., First), Tail(Order(x)) → TopK(..., Last).
// Fusing into a single IR node lets the runtime use a partial heap-select
// (O(n log k)) instead of the default full-sort-then-truncate, and lets
// codegen emit the fast path as a single op call.
auto try_fuse_topk(NodePtr node) -> TryResult {
    const auto kind = node->kind();
    if ((kind != NodeKind::Head && kind != NodeKind::Tail) || node->children().empty() ||
        node->children().front()->kind() != NodeKind::Order) {
        return {false, std::move(node)};
    }
    const auto count = (kind == NodeKind::Head)
                           ? static_cast<const HeadNode&>(*node).count_literal()
                           : static_cast<const TailNode&>(*node).count_literal();
    if (!count.has_value()) {
        return {false, std::move(node)};
    }
    std::vector<ColumnRef> group_by = (kind == NodeKind::Head)
                                          ? static_cast<const HeadNode&>(*node).group_by()
                                          : static_cast<const TailNode&>(*node).group_by();
    const auto keep_mode =
        (kind == NodeKind::Head) ? TopKNode::KeepMode::First : TopKNode::KeepMode::Last;
    const auto fused_id = node->id();
    NodePtr limit_owned = std::move(node);
    NodePtr order_owned = take_unique_child(*limit_owned);
    auto& order_n = static_cast<OrderNode&>(*order_owned);
    if (order_n.children().empty()) {
        limit_owned->add_child(std::move(order_owned));
        return {false, std::move(limit_owned)};
    }
    std::vector<OrderKey> keys = order_n.keys();
    NodePtr x = take_unique_child(order_n);
    auto fused = std::make_unique<TopKNode>(fused_id, std::move(keys), *count, std::move(group_by),
                                            keep_mode);
    fused->add_child(std::move(x));
    return {true, std::move(fused)};
}

// R4: Head/Tail past Project or Rename.
auto try_limit_past_metadata(NodePtr node) -> TryResult {
    const auto kind = node->kind();
    if ((kind != NodeKind::Head && kind != NodeKind::Tail) || node->children().empty()) {
        return {false, std::move(node)};
    }
    const auto child_kind = node->children().front()->kind();
    if (child_kind != NodeKind::Project && child_kind != NodeKind::Rename) {
        return {false, std::move(node)};
    }
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
    if (!safe) {
        return {false, std::move(node)};
    }
    const auto count = (kind == NodeKind::Head) ? static_cast<const HeadNode&>(*node).count_expr()
                                                : static_cast<const TailNode&>(*node).count_expr();
    const auto limit_id = node->id();
    NodePtr limit = std::move(node);
    NodePtr wrapper = take_unique_child(*limit);
    NodePtr x = take_unique_child(*wrapper);
    NodePtr new_limit;
    if (kind == NodeKind::Head) {
        new_limit = std::make_unique<HeadNode>(limit_id, count, std::move(remapped_group_by));
    } else {
        new_limit = std::make_unique<TailNode>(limit_id, count, std::move(remapped_group_by));
    }
    new_limit->add_child(std::move(x));
    new_limit = rewrite_root(std::move(new_limit));
    wrapper->add_child(std::move(new_limit));
    return {true, std::move(wrapper)};
}

using RuleFn = TryResult (*)(NodePtr);

// Ordered list of rules. The driver tries each in turn; on any fire, it
// restarts from the top, so earlier rules are re-tried against shapes exposed
// by later ones. Rule names mirror the Rx labels in canonicalize.hpp.
constexpr std::array<std::pair<std::string_view, RuleFn>, 19> kRules{{
    {"R19:filter-merge", try_filter_merge},
    {"R17:simplify-predicate", try_simplify_predicate},
    {"R1:filter-past-order", try_filter_past_order},
    {"R2:project-past-order", try_project_past_order},
    {"R3:order-past-rename", try_order_past_rename},
    {"R9:rename-compose", try_rename_compose},
    {"R10:rename-drop-identity", try_rename_drop_identity},
    {"R11:filter-past-rename", try_filter_past_rename},
    {"R13-14:limit-collapse", try_limit_collapse},
    {"R15:order-drop-pinned-keys", try_order_drop_pinned_keys},
    {"R12:filter-past-update", try_filter_past_update},
    {"R18:filter-past-aggregate", try_filter_past_aggregate},
    {"R20:project-prune-above-aggregate", try_project_prune_above_aggregate},
    {"R21:project-collapse", try_project_collapse},
    {"R5:fuse-filter-project", try_fuse_filter_project},
    {"R6:fuse-filter-update-project", try_fuse_filter_update_project},
    {"R7-8:fuse-filter-limit", try_fuse_filter_limit},
    {"R16:fuse-topk", try_fuse_topk},
    {"R4:limit-past-metadata", try_limit_past_metadata},
}};

// Apply the rewrite rules at this node's root, looping until a fixpoint is
// reached. Children are assumed already canonicalized. Returns the new root
// (may differ from `node` if a rewrite fired).
auto rewrite_root(NodePtr node) -> NodePtr {
    bool changed = true;
    // Safety valve: rules should reach a fixpoint at a single node in a handful
    // of fires (the longest legitimate chain is well under a dozen). If two
    // rules ever oscillate, bail rather than hang — canonicalization is an
    // optimization, so returning a less-canonical (but correct) tree is safe.
    int iters = 0;
    constexpr int kMaxRewrites = 256;
    while (changed) {
        changed = false;
        if (!node) {
            return node;
        }
        for (const auto& [name, fn] : kRules) {
            (void)name;
            auto result = fn(std::move(node));
            node = std::move(result.node);
            if (result.changed) {
                changed = true;
                if (++iters >= kMaxRewrites) {
                    return node;
                }
                break;
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
    if (root) {
        std::uint64_t max_id = 0;
        collect_max_id(*root, max_id);
        g_next_id = max_id + 1;
    }
    return canon(std::move(root));
}

auto CanonicalizePass::run(NodePtr root, const OptimizationContext& /*context*/,
                           OptimizationStats& /*stats*/) const -> NodePtr {
    return canonicalize(std::move(root));
}

}  // namespace ibex::ir
