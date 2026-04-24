#include <ibex/ir/expr_predicates.hpp>

#include <type_traits>
#include <variant>

namespace ibex::ir {

auto is_row_local_update_expr(const Expr& expr) -> bool {
    return std::visit(
        [](const auto& n) -> bool {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef> || std::is_same_v<T, Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                return is_row_local_update_expr(*n.left) && is_row_local_update_expr(*n.right);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                const auto& name = n.callee;
                if (name == "lag" || name == "lead" || name == "rep" || is_rolling_func(name) ||
                    is_cum_func(name) || is_rng_func(name) || name == "fill_forward" ||
                    name == "fill_backward") {
                    return false;
                }
                for (const auto& arg : n.args) {
                    if (!is_row_local_update_expr(*arg)) {
                        return false;
                    }
                }
                for (const auto& na : n.named_args) {
                    if (!is_row_local_update_expr(*na.value)) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        },
        expr.node);
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
            }
        },
        expr.node);
}

}  // namespace ibex::ir
