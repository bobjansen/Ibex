#include <ibex/ir/expr_predicates.hpp>

#include <type_traits>
#include <variant>

namespace ibex::ir {

auto fn_kind(std::string_view name) -> FnKind {
    if (is_rolling_func(name) || is_cum_func(name) || name == "lag" || name == "lead" ||
        name == "fill_forward" || name == "fill_backward") {
        return FnKind::Transform;
    }
    if (is_rng_func(name) || name == "rep") {
        return FnKind::Generator;
    }
    if (is_aggregate_func(name)) {
        return FnKind::Aggregate;
    }
    return FnKind::Scalar;
}

namespace {

// Walk `expr`; a call is disqualifying when `bad(fn_kind(callee))`. A `RankExpr`
// node is always disqualifying (non-row-local). Shared by the row-local and
// subset-evaluable predicates so both classify through `fn_kind`.
template <typename BadKind>
auto no_call_of_kind(const Expr& expr, BadKind bad) -> bool {
    return std::visit(
        [&](const auto& n) -> bool {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef> || std::is_same_v<T, Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                return no_call_of_kind(*n.left, bad) && no_call_of_kind(*n.right, bad);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                if (bad(fn_kind(n.callee))) {
                    return false;
                }
                for (const auto& arg : n.args) {
                    if (!no_call_of_kind(*arg, bad)) {
                        return false;
                    }
                }
                for (const auto& na : n.named_args) {
                    if (!no_call_of_kind(*na.value, bad)) {
                        return false;
                    }
                }
                return true;
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                return false;
            } else {
                return false;
            }
        },
        expr.node);
}

}  // namespace

auto is_row_local_update_expr(const Expr& expr) -> bool {
    return no_call_of_kind(
        expr, [](FnKind k) { return k == FnKind::Transform || k == FnKind::Generator; });
}

auto is_subset_evaluable_expr(const Expr& expr) -> bool {
    return no_call_of_kind(expr, [](FnKind k) { return k != FnKind::Scalar; });
}

void collect_expr_column_refs(const Expr& expr, std::unordered_set<std::string>& out) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                out.insert(n.name);
            } else if constexpr (std::is_same_v<T, Literal>) {
                // nothing
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                collect_expr_column_refs(*n.left, out);
                collect_expr_column_refs(*n.right, out);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (const auto& arg : n.args) {
                    collect_expr_column_refs(*arg, out);
                }
                for (const auto& na : n.named_args) {
                    collect_expr_column_refs(*na.value, out);
                }
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                for (const auto& key : n.order_keys) {
                    out.insert(key.name);
                }
            }
        },
        expr.node);
}

}  // namespace ibex::ir
