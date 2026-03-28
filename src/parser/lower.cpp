#include <ibex/ir/builder.hpp>
#include <ibex/ir/optimizer.hpp>
#include <ibex/parser/effects.hpp>
#include <ibex/parser/lower.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>

namespace ibex::parser {

namespace {

auto to_ir_effect_summary(const EffectSummary& src) -> ir::EffectSummary {
    return ir::EffectSummary{
        .mask = src.mask,
        .io_read_unscoped = src.io_read_unscoped,
        .io_write_unscoped = src.io_write_unscoped,
        .io_read_resources = src.io_read_resources,
        .io_write_resources = src.io_write_resources,
    };
}

auto to_ir_arg_mode(Param::Effect effect) -> ir::ArgMode {
    switch (effect) {
        case Param::Effect::Const:
            return ir::ArgMode::Const;
        case Param::Effect::Mutable:
            return ir::ArgMode::Mutable;
        case Param::Effect::Consume:
            return ir::ArgMode::Consume;
    }
    return ir::ArgMode::Const;
}

auto build_optimization_context(const EffectAnalysis& analysis) -> ir::OptimizationContext {
    ir::OptimizationContext context;
    context.callee_summaries.reserve(analysis.user_functions.size() + analysis.externs.size() +
                                     analysis.builtins.size());

    const auto append = [&](const std::unordered_map<std::string, CallableSummary>& src) {
        for (const auto& [name, callable] : src) {
            ir::CallableSummary dst;
            dst.effects = to_ir_effect_summary(callable.effects);
            dst.arg_modes.reserve(callable.param_modes.size());
            for (const auto& mode : callable.param_modes) {
                dst.arg_modes.push_back(to_ir_arg_mode(mode));
            }
            context.callee_summaries.insert_or_assign(name, std::move(dst));
        }
    };

    append(analysis.user_functions);
    append(analysis.externs);
    append(analysis.builtins);
    return context;
}

class Lowerer {
   public:
    explicit Lowerer(std::unordered_map<std::string, ir::NodePtr>* bindings,
                     std::unordered_set<std::string> initial_table_externs = {},
                     std::unordered_set<std::string> initial_sink_externs = {})
        : bindings_(bindings),
          table_externs_(std::move(initial_table_externs)),
          sink_externs_(std::move(initial_sink_externs)) {}

    auto lower_program(const Program& program) -> LowerResult {
        ir::NodePtr last_expr;
        std::vector<ir::NodePtr> preamble_calls;
        for (const auto& stmt : program.statements) {
            if (const auto* ext = std::get_if<ExternDecl>(&stmt)) {
                // Track externs that return a table so lower_table_call can
                // produce ExternCallNodes for them.
                if (ext->return_type.kind == Type::Kind::DataFrame ||
                    ext->return_type.kind == Type::Kind::TimeFrame) {
                    table_externs_.insert(ext->name);
                }
                // Track externs whose first argument is a DataFrame — these are sink candidates
                // (e.g. write_csv, udp_send).  lower_stream uses this to validate sink calls.
                if (!ext->params.empty() && ext->params[0].type.kind == Type::Kind::DataFrame) {
                    sink_externs_.insert(ext->name);
                }
                continue;
            }
            if (std::holds_alternative<FunctionDecl>(stmt)) {
                continue;
            }
            if (std::holds_alternative<ImportDecl>(stmt)) {
                // Import declarations are resolved by the REPL before lowering;
                // they have no IR representation.
                continue;
            }
            if (std::holds_alternative<LetStmt>(stmt)) {
                const auto& let_stmt = std::get<LetStmt>(stmt);
                auto value = lower_expr(*let_stmt.value);
                if (!value.has_value()) {
                    // Scalar let bindings are handled by the REPL/tooling layer;
                    // the IR lowerer only needs to accept them so later table
                    // expressions can still be lowered.
                    auto scalar = lower_expr_to_ir(*let_stmt.value);
                    if (!scalar.has_value()) {
                        return std::unexpected(value.error());
                    }
                    continue;
                }
                if (bindings_ != nullptr) {
                    (*bindings_)[let_stmt.name] = std::move(value.value());
                }
                continue;
            }
            if (std::holds_alternative<TupleLetStmt>(stmt)) {
                // Tuple destructuring is resolved at the REPL level; the lowerer
                // cannot bind individual columns into the IR name registry.
                continue;
            }
            if (std::holds_alternative<ExprStmt>(stmt)) {
                const auto& expr_stmt = std::get<ExprStmt>(stmt);
                auto value = lower_expr(*expr_stmt.expr);
                if (!value.has_value()) {
                    // Not a table expression — check whether it's a scalar call
                    // (e.g. ws_listen(8765)) used purely for its side effect.
                    if (const auto* call = std::get_if<CallExpr>(&expr_stmt.expr->node)) {
                        std::vector<ir::Expr> args;
                        args.reserve(call->args.size());
                        bool args_ok = true;
                        for (const auto& arg : call->args) {
                            auto a = lower_expr_to_ir(*arg);
                            if (!a.has_value()) {
                                args_ok = false;
                                break;
                            }
                            args.push_back(std::move(*a));
                        }
                        if (args_ok) {
                            preamble_calls.push_back(
                                builder_.extern_call(call->callee, std::move(args)));
                            continue;
                        }
                    }
                    return std::unexpected(value.error());
                }
                last_expr = std::move(value.value());
            }
        }
        if (!last_expr) {
            return std::unexpected(LowerError{.message = "no expression to lower"});
        }
        if (!preamble_calls.empty()) {
            return builder_.program(std::move(preamble_calls), std::move(last_expr));
        }
        return last_expr;
    }

    auto lower_expression(const Expr& expr) -> LowerResult { return lower_expr(expr); }

   private:
    auto lower_expr(const Expr& expr) -> LowerResult {
        if (const auto* block = std::get_if<BlockExpr>(&expr.node)) {
            return lower_block(*block);
        }
        if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
            return lower_identifier(*ident);
        }
        if (const auto* join = std::get_if<JoinExpr>(&expr.node)) {
            return lower_join(*join);
        }
        if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
            return lower_table_call(*call);
        }
        if (auto* stream = std::get_if<StreamExpr>(const_cast<decltype(expr.node)*>(&expr.node))) {
            return lower_stream(*stream);
        }
        if (const auto* tbl = std::get_if<TableExpr>(&expr.node)) {
            return lower_table_expr(*tbl);
        }
        return std::unexpected(LowerError{.message = "expected DataFrame expression"});
    }

    /// Lower a `Table { col = expr, ... }` expression into a ConstructNode.
    ///
    /// Each column value may be:
    ///   - An array literal `[v, ...]` — lowered inline to a vector of ir::Literal.
    ///   - Any other expression — lowered recursively to a child IR node that is
    ///     expected to produce a single-column Table (or a Table containing a column
    ///     named after the column being defined) at interpret/codegen time.
    auto lower_table_expr(const TableExpr& tbl) -> LowerResult {
        std::vector<ir::ConstructColumn> construct_cols;
        construct_cols.reserve(tbl.columns.size());

        for (const auto& col_def : tbl.columns) {
            const auto* arr = std::get_if<ArrayLiteralExpr>(&col_def.expr->node);
            if (arr == nullptr) {
                // Non-literal expression: lower it as a child IR sub-tree.
                auto sub = lower_expr(*col_def.expr);
                if (!sub.has_value()) {
                    return std::unexpected(sub.error());
                }
                ir::ConstructColumn cc;
                cc.name = col_def.name;
                cc.expr_node = std::move(*sub);
                construct_cols.push_back(std::move(cc));
                continue;
            }

            std::vector<ir::Literal> elements;
            elements.reserve(arr->elements.size());

            // Determine the type from the first element and validate uniformity.
            int type_tag = -1;  // 0=int, 1=double, 2=bool, 3=string, 4=Date, 5=Timestamp
            for (const auto& elem_ptr : arr->elements) {
                const auto* lit = std::get_if<LiteralExpr>(&elem_ptr->node);
                if (lit == nullptr) {
                    return std::unexpected(LowerError{.message = "Table constructor: column '" +
                                                                 col_def.name +
                                                                 "' elements must be literals"});
                }
                int elem_tag = static_cast<int>(lit->value.index());
                // Map LiteralExpr variant index to our type tag:
                // LiteralExpr::value = variant<int64, double, bool, string, DurationLiteral, Date,
                // Timestamp> DurationLiteral (index 4) is not a valid column element type.
                if (elem_tag == 4) {
                    return std::unexpected(
                        LowerError{.message = "Table constructor: column '" + col_def.name +
                                              "' duration literals are not valid column elements"});
                }
                // Remap: DurationLiteral is index 4, so Date=5→4, Timestamp=6→5
                int mapped_tag = elem_tag < 4 ? elem_tag : elem_tag - 1;
                if (type_tag == -1) {
                    type_tag = mapped_tag;
                } else if (type_tag != mapped_tag) {
                    return std::unexpected(LowerError{.message = "Table constructor: column '" +
                                                                 col_def.name +
                                                                 "' has mixed element types"});
                }

                // Convert LiteralExpr value to ir::Literal
                ir::Literal ir_lit;
                std::visit(
                    [&](const auto& v) {
                        using T = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<T, std::int64_t>) {
                            ir_lit.value = v;
                        } else if constexpr (std::is_same_v<T, double>) {
                            ir_lit.value = v;
                        } else if constexpr (std::is_same_v<T, bool>) {
                            ir_lit.value = v;
                        } else if constexpr (std::is_same_v<T, std::string>) {
                            ir_lit.value = v;
                        } else if constexpr (std::is_same_v<T, Date>) {
                            ir_lit.value = v;
                        } else if constexpr (std::is_same_v<T, Timestamp>) {
                            ir_lit.value = v;
                        }
                        // DurationLiteral already excluded above.
                    },
                    lit->value);
                elements.push_back(std::move(ir_lit));
            }

            construct_cols.push_back(ir::ConstructColumn{
                .name = col_def.name,
                .elements = std::move(elements),
                .expr_node = nullptr,
            });
        }

        return builder_.construct(std::move(construct_cols));
    }

    auto lower_table_call(const CallExpr& call) -> LowerResult {
        if (call.callee == "matmul") {
            if (call.args.size() != 2) {
                return std::unexpected(LowerError{.message = "matmul expects exactly 2 arguments"});
            }
            auto left = lower_expr(*call.args[0]);
            if (!left.has_value()) {
                return left;
            }
            auto right = lower_expr(*call.args[1]);
            if (!right.has_value()) {
                return right;
            }
            auto node = builder_.matmul();
            node->add_child(std::move(left.value()));
            node->add_child(std::move(right.value()));
            return node;
        }
        if (call.callee == "as_timeframe") {
            if (call.args.size() != 2) {
                return std::unexpected(LowerError{.message = "as_timeframe expects 2 arguments"});
            }
            auto base = lower_expr(*call.args[0]);
            if (!base.has_value()) {
                return base;
            }
            const auto* lit = std::get_if<LiteralExpr>(&call.args[1]->node);
            const std::string* col_name = lit ? std::get_if<std::string>(&lit->value) : nullptr;
            if (col_name == nullptr) {
                return std::unexpected(LowerError{
                    .message =
                        "as_timeframe: second argument must be a string literal column name"});
            }
            auto node = builder_.as_timeframe(*col_name);
            node->add_child(std::move(base.value()));
            return node;
        }
        if (!table_externs_.contains(call.callee)) {
            return std::unexpected(LowerError{.message = "unknown table function: " + call.callee});
        }
        std::vector<ir::Expr> args;
        args.reserve(call.args.size());
        for (const auto& arg : call.args) {
            auto expr = lower_expr_to_ir(*arg);
            if (!expr.has_value()) {
                return std::unexpected(expr.error());
            }
            args.push_back(std::move(expr.value()));
        }
        return builder_.extern_call(call.callee, std::move(args));
    }

    auto lower_identifier(const IdentifierExpr& ident) -> LowerResult {
        if (bindings_ != nullptr) {
            if (auto it = bindings_->find(ident.name); it != bindings_->end()) {
                return clone_node(*it->second);
            }
        }
        return builder_.scan(ident.name);
    }

    auto lower_join(const JoinExpr& join) -> LowerResult {
        auto left = lower_expr(*join.left);
        if (!left) {
            return left;
        }
        auto right = lower_expr(*join.right);
        if (!right) {
            return right;
        }

        ir::JoinKind kind = ir::JoinKind::Inner;
        switch (join.kind) {
            case JoinKind::Inner:
                kind = ir::JoinKind::Inner;
                break;
            case JoinKind::Left:
                kind = ir::JoinKind::Left;
                break;
            case JoinKind::Right:
                kind = ir::JoinKind::Right;
                break;
            case JoinKind::Outer:
                kind = ir::JoinKind::Outer;
                break;
            case JoinKind::Semi:
                kind = ir::JoinKind::Semi;
                break;
            case JoinKind::Anti:
                kind = ir::JoinKind::Anti;
                break;
            case JoinKind::Cross:
                kind = ir::JoinKind::Cross;
                break;
            case JoinKind::Asof:
                kind = ir::JoinKind::Asof;
                break;
        }

        // Resolve the `on` clause: equikeys from the braced list path are
        // already in join.keys; an expression stored in join.predicate is
        // either a bare identifier (equikey) or a general predicate.
        std::vector<std::string> keys = join.keys;
        std::optional<ir::FilterExprPtr> predicate;

        if (join.predicate.has_value()) {
            const Expr& on_expr = **join.predicate;
            if (const auto* ident = std::get_if<IdentifierExpr>(&on_expr.node)) {
                // Bare identifier → equikey (backward compat: `A join B on key`)
                keys.push_back(ident->name);
            } else {
                // General expression → non-equijoin predicate
                auto pred = lower_filter_expr(on_expr);
                if (!pred) {
                    return std::unexpected(pred.error());
                }
                predicate = std::move(*pred);
            }
        }

        auto node = builder_.join(kind, std::move(keys), std::move(predicate));
        node->add_child(std::move(left.value()));
        node->add_child(std::move(right.value()));
        return node;
    }

    auto lower_block(const BlockExpr& block) -> LowerResult {
        return lower_block(*block.base, block.clauses);
    }

    auto lower_block(const Expr& base_expr, const std::vector<Clause>& clauses) -> LowerResult {
        auto base = lower_expr(base_expr);
        if (!base.has_value()) {
            return base;
        }

        ClauseState state;
        for (const auto& clause : clauses) {
            if (!state.record(clause)) {
                return std::unexpected(LowerError{.message = state.error});
            }
        }
        if (state.select && state.update) {
            return std::unexpected(
                LowerError{.message = "select and update are mutually exclusive"});
        }
        if (state.distinct && (state.select || state.update)) {
            return std::unexpected(
                LowerError{.message = "distinct is mutually exclusive with select/update"});
        }
        if (state.distinct && state.by) {
            return std::unexpected(LowerError{.message = "distinct cannot be used with by"});
        }
        if (state.by && !state.select && !state.update && !state.resample && !state.dcast) {
            return std::unexpected(LowerError{.message = "by requires select or update"});
        }
        if (state.resample && state.window) {
            return std::unexpected(
                LowerError{.message = "resample and window are mutually exclusive"});
        }
        if (state.resample && !state.select) {
            return std::unexpected(LowerError{.message = "resample requires a select clause"});
        }
        if (state.resample && state.update) {
            return std::unexpected(LowerError{.message = "resample cannot be used with update"});
        }
        if (state.melt && (state.update || state.distinct || state.dcast || state.by ||
                           state.window || state.resample || state.rename)) {
            return std::unexpected(LowerError{
                .message = "melt is mutually exclusive with update, distinct, dcast, by, window, "
                           "resample, and rename"});
        }
        if (state.dcast && (state.update || state.distinct || state.melt || state.window ||
                            state.resample || state.rename)) {
            return std::unexpected(LowerError{
                .message = "dcast is mutually exclusive with update, distinct, melt, window, "
                           "resample, and rename"});
        }
        if (state.dcast && !state.select) {
            return std::unexpected(
                LowerError{.message = "dcast requires a select clause for the value column"});
        }
        if (state.dcast && !state.by) {
            return std::unexpected(
                LowerError{.message = "dcast requires a by clause for the row keys"});
        }
        // cov / corr / transpose are standalone — mutually exclusive with most other clauses.
        const bool has_matrix_op = state.cov || state.corr || state.transpose;
        if (has_matrix_op &&
            (state.select || state.update || state.by || state.distinct || state.melt ||
             state.dcast || state.window || state.resample || state.rename)) {
            return std::unexpected(LowerError{
                .message = "cov/corr/transpose are mutually exclusive with select, update, by, "
                           "distinct, melt, dcast, window, resample, and rename"});
        }
        if ((state.cov ? 1 : 0) + (state.corr ? 1 : 0) + (state.transpose ? 1 : 0) > 1) {
            return std::unexpected(
                LowerError{.message = "cov, corr, and transpose are mutually exclusive"});
        }

        // model is standalone — mutually exclusive with most other clauses.
        if (state.model &&
            (state.select || state.update || state.by || state.distinct || state.melt ||
             state.dcast || state.window || state.resample || state.rename || state.cov ||
             state.corr || state.transpose)) {
            return std::unexpected(LowerError{
                .message = "model is mutually exclusive with select, update, by, distinct, "
                           "melt, dcast, window, resample, rename, cov, corr, and transpose"});
        }

        auto node = std::move(base.value());

        if (state.filter) {
            auto predicate = lower_filter(*state.filter);
            if (!predicate.has_value()) {
                return std::unexpected(predicate.error());
            }
            auto filter_node = builder_.filter(std::move(predicate.value()));
            filter_node->add_child(std::move(node));
            node = std::move(filter_node);
        }

        if (state.rename) {
            auto renames = lower_rename(*state.rename);
            if (!renames.has_value()) {
                return std::unexpected(renames.error());
            }
            auto rename_node = builder_.rename(std::move(renames.value()));
            rename_node->add_child(std::move(node));
            node = std::move(rename_node);
        }

        if (!state.resample && !state.melt && !state.dcast && state.select &&
            (state.by || select_has_aggregate(*state.select))) {
            auto aggregate = lower_aggregate(state.by, *state.select, std::move(node));
            if (!aggregate.has_value()) {
                return std::unexpected(aggregate.error());
            }
            node = std::move(aggregate.value());
        } else if (!state.resample && !state.melt && !state.dcast && state.select) {
            auto project = lower_select_projection(state.select->fields, state.select->tuple_fields,
                                                   std::move(node));
            if (!project.has_value()) {
                return std::unexpected(project.error());
            }
            node = std::move(project.value());
        } else if (state.distinct) {
            auto project = lower_select_projection(state.distinct->fields, {}, std::move(node));
            if (!project.has_value()) {
                return std::unexpected(project.error());
            }
            auto distinct = builder_.distinct();
            distinct->add_child(std::move(project.value()));
            node = std::move(distinct);
        }

        if (state.update) {
            auto update = lower_update(state.by, *state.update);
            if (!update.has_value()) {
                return std::unexpected(update.error());
            }
            update.value()->add_child(std::move(node));
            node = std::move(update.value());
        }

        if (state.order) {
            auto keys = lower_order(*state.order);
            if (!keys.has_value()) {
                return std::unexpected(keys.error());
            }
            auto order_node = builder_.order(std::move(keys.value()));
            order_node->add_child(std::move(node));
            node = std::move(order_node);
        }

        if (state.window) {
            auto duration = parse_duration(state.window->duration.text);
            if (!duration.has_value()) {
                return std::unexpected(duration.error());
            }
            auto window_node = builder_.window(duration.value());
            window_node->add_child(std::move(node));
            node = std::move(window_node);
        }

        if (state.resample) {
            auto duration = parse_duration(state.resample->duration.text);
            if (!duration.has_value())
                return std::unexpected(duration.error());
            std::vector<ir::ColumnRef> extra_group_by;
            if (state.by) {
                auto keys = lower_group_by(*state.by);
                if (!keys.has_value())
                    return std::unexpected(keys.error());
                extra_group_by = std::move(keys.value());
            }
            auto agg_specs = lower_resample_aggs(*state.select);
            if (!agg_specs.has_value())
                return std::unexpected(agg_specs.error());
            auto resample_node = builder_.resample(duration.value(), std::move(extra_group_by),
                                                   std::move(agg_specs.value()));
            resample_node->add_child(std::move(node));
            node = std::move(resample_node);
        }

        if (state.melt) {
            std::vector<std::string> id_cols;
            for (const auto& field : state.melt->id_fields) {
                id_cols.push_back(field.name);
            }
            std::vector<std::string> measure_cols;
            if (state.select) {
                for (const auto& field : state.select->fields) {
                    measure_cols.push_back(field.name);
                }
            }
            auto melt_node = builder_.melt(std::move(id_cols), std::move(measure_cols));
            melt_node->add_child(std::move(node));
            node = std::move(melt_node);
        }

        if (state.dcast) {
            // Extract the value column name from the select clause.
            // dcast expects exactly one field in select (the value column).
            if (state.select->fields.size() != 1) {
                return std::unexpected(
                    LowerError{.message = "dcast select must specify exactly one value column"});
            }
            std::string value_col = state.select->fields[0].name;
            std::vector<std::string> row_keys;
            for (const auto& key : state.by->keys) {
                row_keys.push_back(key.name);
            }
            auto dcast_node = builder_.dcast(state.dcast->pivot_column, std::move(value_col),
                                             std::move(row_keys));
            dcast_node->add_child(std::move(node));
            node = std::move(dcast_node);
        }

        if (state.cov) {
            auto cov_node = builder_.cov();
            cov_node->add_child(std::move(node));
            node = std::move(cov_node);
        }

        if (state.corr) {
            auto corr_node = builder_.corr();
            corr_node->add_child(std::move(node));
            node = std::move(corr_node);
        }

        if (state.transpose) {
            auto transpose_node = builder_.transpose();
            transpose_node->add_child(std::move(node));
            node = std::move(transpose_node);
        }

        if (state.model) {
            auto model_result = lower_model(*state.model);
            if (!model_result.has_value()) {
                return std::unexpected(model_result.error());
            }
            auto& [formula, method, params] = model_result.value();
            auto model_node =
                builder_.model(std::move(formula), std::move(method), std::move(params));
            model_node->add_child(std::move(node));
            node = std::move(model_node);
        }

        return node;
    }

    struct ClauseState {
        const FilterClause* filter = nullptr;
        const SelectClause* select = nullptr;
        const DistinctClause* distinct = nullptr;
        const UpdateClause* update = nullptr;
        const RenameClause* rename = nullptr;
        const OrderClause* order = nullptr;
        const ByClause* by = nullptr;
        const WindowClause* window = nullptr;
        const ResampleClause* resample = nullptr;
        const MeltClause* melt = nullptr;
        const DcastClause* dcast = nullptr;
        const CovClause* cov = nullptr;
        const CorrClause* corr = nullptr;
        const TransposeClause* transpose = nullptr;
        const ModelClause* model = nullptr;
        std::string error;

        auto record(const Clause& clause) -> bool {
            if (std::holds_alternative<FilterClause>(clause)) {
                if (filter != nullptr) {
                    error = "duplicate filter clause";
                    return false;
                }
                filter = &std::get<FilterClause>(clause);
                return true;
            }
            if (std::holds_alternative<SelectClause>(clause)) {
                if (select != nullptr) {
                    error = "duplicate select clause";
                    return false;
                }
                select = &std::get<SelectClause>(clause);
                return true;
            }
            if (std::holds_alternative<DistinctClause>(clause)) {
                if (distinct != nullptr) {
                    error = "duplicate distinct clause";
                    return false;
                }
                distinct = &std::get<DistinctClause>(clause);
                return true;
            }
            if (std::holds_alternative<UpdateClause>(clause)) {
                if (update != nullptr) {
                    error = "duplicate update clause";
                    return false;
                }
                update = &std::get<UpdateClause>(clause);
                return true;
            }
            if (std::holds_alternative<RenameClause>(clause)) {
                if (rename != nullptr) {
                    error = "duplicate rename clause";
                    return false;
                }
                rename = &std::get<RenameClause>(clause);
                return true;
            }
            if (std::holds_alternative<OrderClause>(clause)) {
                if (order != nullptr) {
                    error = "duplicate order clause";
                    return false;
                }
                order = &std::get<OrderClause>(clause);
                return true;
            }
            if (std::holds_alternative<ByClause>(clause)) {
                if (by != nullptr) {
                    error = "duplicate by clause";
                    return false;
                }
                by = &std::get<ByClause>(clause);
                return true;
            }
            if (std::holds_alternative<WindowClause>(clause)) {
                if (window != nullptr) {
                    error = "duplicate window clause";
                    return false;
                }
                window = &std::get<WindowClause>(clause);
                return true;
            }
            if (std::holds_alternative<ResampleClause>(clause)) {
                if (resample != nullptr) {
                    error = "duplicate resample clause";
                    return false;
                }
                resample = &std::get<ResampleClause>(clause);
                return true;
            }
            if (std::holds_alternative<MeltClause>(clause)) {
                if (melt != nullptr) {
                    error = "duplicate melt clause";
                    return false;
                }
                melt = &std::get<MeltClause>(clause);
                return true;
            }
            if (std::holds_alternative<DcastClause>(clause)) {
                if (dcast != nullptr) {
                    error = "duplicate dcast clause";
                    return false;
                }
                dcast = &std::get<DcastClause>(clause);
                return true;
            }
            if (std::holds_alternative<CovClause>(clause)) {
                if (cov != nullptr) {
                    error = "duplicate cov clause";
                    return false;
                }
                cov = &std::get<CovClause>(clause);
                return true;
            }
            if (std::holds_alternative<CorrClause>(clause)) {
                if (corr != nullptr) {
                    error = "duplicate corr clause";
                    return false;
                }
                corr = &std::get<CorrClause>(clause);
                return true;
            }
            if (std::holds_alternative<TransposeClause>(clause)) {
                if (transpose != nullptr) {
                    error = "duplicate transpose clause";
                    return false;
                }
                transpose = &std::get<TransposeClause>(clause);
                return true;
            }
            if (std::holds_alternative<ModelClause>(clause)) {
                if (model != nullptr) {
                    error = "duplicate model clause";
                    return false;
                }
                model = &std::get<ModelClause>(clause);
                return true;
            }
            return true;
        }
    };

    static auto lower_filter_expr(const Expr& expr)
        -> std::expected<ir::FilterExprPtr, LowerError> {
        // Logical NOT: !expr
        if (const auto* unary = std::get_if<UnaryExpr>(&expr.node)) {
            if (unary->op == UnaryOp::Not) {
                auto operand = lower_filter_expr(*unary->expr);
                if (!operand)
                    return std::unexpected(operand.error());
                return std::make_unique<ir::FilterExpr>(
                    ir::FilterExpr{ir::FilterNot{.operand = std::move(*operand)}});
            }
            if (unary->op == UnaryOp::IsNull || unary->op == UnaryOp::IsNotNull) {
                auto operand = lower_filter_expr(*unary->expr);
                if (!operand)
                    return std::unexpected(operand.error());
                if (unary->op == UnaryOp::IsNull)
                    return std::make_unique<ir::FilterExpr>(
                        ir::FilterExpr{ir::FilterIsNull{.operand = std::move(*operand)}});
                return std::make_unique<ir::FilterExpr>(
                    ir::FilterExpr{ir::FilterIsNotNull{.operand = std::move(*operand)}});
            }
            return std::unexpected(LowerError{.message = "unsupported unary op in filter"});
        }
        // Binary ops: And, Or, comparisons, arithmetic
        if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
            if (binary->op == BinaryOp::And) {
                auto left = lower_filter_expr(*binary->left);
                if (!left)
                    return std::unexpected(left.error());
                auto right = lower_filter_expr(*binary->right);
                if (!right)
                    return std::unexpected(right.error());
                return std::make_unique<ir::FilterExpr>(
                    ir::FilterExpr{ir::FilterAnd{std::move(*left), std::move(*right)}});
            }
            if (binary->op == BinaryOp::Or) {
                auto left = lower_filter_expr(*binary->left);
                if (!left)
                    return std::unexpected(left.error());
                auto right = lower_filter_expr(*binary->right);
                if (!right)
                    return std::unexpected(right.error());
                return std::make_unique<ir::FilterExpr>(
                    ir::FilterExpr{ir::FilterOr{std::move(*left), std::move(*right)}});
            }
            if (is_compare_op(binary->op)) {
                auto left = lower_filter_expr(*binary->left);
                if (!left)
                    return std::unexpected(left.error());
                auto right = lower_filter_expr(*binary->right);
                if (!right)
                    return std::unexpected(right.error());
                return std::make_unique<ir::FilterExpr>(ir::FilterExpr{
                    ir::FilterCmp{to_compare_op(binary->op), std::move(*left), std::move(*right)}});
            }
            if (auto arith = to_arithmetic_op(binary->op)) {
                auto left = lower_filter_expr(*binary->left);
                if (!left)
                    return std::unexpected(left.error());
                auto right = lower_filter_expr(*binary->right);
                if (!right)
                    return std::unexpected(right.error());
                return std::make_unique<ir::FilterExpr>(
                    ir::FilterExpr{ir::FilterArith{*arith, std::move(*left), std::move(*right)}});
            }
            return std::unexpected(LowerError{.message = "unsupported operator in filter"});
        }
        // Identifier → column/scalar reference (resolved at eval time)
        if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
            return std::make_unique<ir::FilterExpr>(
                ir::FilterExpr{ir::FilterColumn{.name = ident->name}});
        }
        // Literal
        if (const auto* lit = std::get_if<LiteralExpr>(&expr.node)) {
            auto value = lower_literal(*lit);
            if (!value)
                return std::unexpected(value.error());
            auto wrapped = std::visit(
                [](const auto& v)
                    -> std::variant<std::int64_t, double, std::string, Date, Timestamp> {
                    return v;
                },
                value.value());
            return std::make_unique<ir::FilterExpr>(
                ir::FilterExpr{ir::FilterLiteral{.value = std::move(wrapped)}});
        }
        // Parenthesized expression
        if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
            return lower_filter_expr(*group->expr);
        }
        return std::unexpected(LowerError{.message = "unsupported expression in filter predicate"});
    }

    static auto lower_filter(const FilterClause& clause)
        -> std::expected<ir::FilterExprPtr, LowerError> {
        return lower_filter_expr(*clause.predicate);
    }

    static auto clone_filter_expr(const ir::FilterExpr& expr) -> ir::FilterExprPtr {
        return std::visit(
            [](const auto& node) -> ir::FilterExprPtr {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, ir::FilterColumn> ||
                              std::is_same_v<T, ir::FilterLiteral>) {
                    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{node});
                } else if constexpr (std::is_same_v<T, ir::FilterArith>) {
                    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterArith{
                        node.op, clone_filter_expr(*node.left), clone_filter_expr(*node.right)}});
                } else if constexpr (std::is_same_v<T, ir::FilterCmp>) {
                    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterCmp{
                        node.op, clone_filter_expr(*node.left), clone_filter_expr(*node.right)}});
                } else if constexpr (std::is_same_v<T, ir::FilterAnd>) {
                    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterAnd{
                        clone_filter_expr(*node.left), clone_filter_expr(*node.right)}});
                } else if constexpr (std::is_same_v<T, ir::FilterOr>) {
                    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterOr{
                        clone_filter_expr(*node.left), clone_filter_expr(*node.right)}});
                } else if constexpr (std::is_same_v<T, ir::FilterNot>) {
                    return std::make_unique<ir::FilterExpr>(
                        ir::FilterExpr{ir::FilterNot{clone_filter_expr(*node.operand)}});
                } else if constexpr (std::is_same_v<T, ir::FilterIsNull>) {
                    return std::make_unique<ir::FilterExpr>(
                        ir::FilterExpr{ir::FilterIsNull{clone_filter_expr(*node.operand)}});
                } else {
                    static_assert(std::is_same_v<T, ir::FilterIsNotNull>);
                    return std::make_unique<ir::FilterExpr>(
                        ir::FilterExpr{ir::FilterIsNotNull{clone_filter_expr(*node.operand)}});
                }
            },
            expr.node);
    }

    auto lower_select_projection(const std::vector<Field>& clause_fields,
                                 const std::vector<TupleField>& tuple_fields, ir::NodePtr base)
        -> std::expected<ir::NodePtr, LowerError> {
        std::vector<ir::FieldSpec> fields;
        std::vector<ir::ColumnRef> columns;
        fields.reserve(clause_fields.size());
        columns.reserve(clause_fields.size());

        for (const auto& field : clause_fields) {
            if (field.expr == nullptr) {
                columns.push_back(ir::ColumnRef{.name = field.name});
                continue;
            }
            auto expr = lower_expr_to_ir(*field.expr);
            if (!expr.has_value()) {
                return std::unexpected(expr.error());
            }
            fields.push_back(ir::FieldSpec{
                .alias = field.name,
                .expr = std::move(expr.value()),
            });
            columns.push_back(ir::ColumnRef{.name = field.name});
        }

        std::vector<ir::TupleFieldSpec> tuple_specs;
        for (const auto& tf : tuple_fields) {
            auto src = lower_expr(*tf.expr);
            if (!src.has_value()) {
                return std::unexpected(src.error());
            }
            for (const auto& name : tf.names) {
                columns.push_back(ir::ColumnRef{.name = name});
            }
            tuple_specs.push_back(
                ir::TupleFieldSpec{.aliases = tf.names, .source = std::move(src.value())});
        }

        if (fields.empty() && tuple_specs.empty()) {
            auto project = builder_.project(std::move(columns));
            project->add_child(std::move(base));
            return project;
        }

        auto update = builder_.update(std::move(fields), std::move(tuple_specs));
        update->add_child(std::move(base));

        auto project = builder_.project(std::move(columns));
        project->add_child(std::move(update));
        return project;
    }

    auto lower_update(const ByClause* by, const UpdateClause& clause)
        -> std::expected<ir::NodePtr, LowerError> {
        // `update = expr`: merge all columns of the result table.
        if (clause.merge_expr) {
            auto src = lower_expr(*clause.merge_expr);
            if (!src.has_value()) {
                return std::unexpected(src.error());
            }
            std::vector<ir::TupleFieldSpec> tuple_specs;
            tuple_specs.push_back(
                ir::TupleFieldSpec{.aliases = {}, .source = std::move(src.value())});
            return builder_.update({}, std::move(tuple_specs));
        }
        std::vector<ir::FieldSpec> fields;
        for (const auto& field : clause.fields) {
            if (field.expr == nullptr) {
                return std::unexpected(LowerError{.message = "update field requires expression"});
            }
            auto expr = lower_expr_to_ir(*field.expr);
            if (!expr.has_value()) {
                return std::unexpected(expr.error());
            }
            fields.push_back(ir::FieldSpec{
                .alias = field.name,
                .expr = std::move(expr.value()),
            });
        }
        std::vector<ir::TupleFieldSpec> tuple_specs;
        for (const auto& tf : clause.tuple_fields) {
            auto src = lower_expr(*tf.expr);
            if (!src.has_value()) {
                return std::unexpected(src.error());
            }
            tuple_specs.push_back(
                ir::TupleFieldSpec{.aliases = tf.names, .source = std::move(src.value())});
        }
        std::vector<ir::ColumnRef> group_by;
        if (by != nullptr) {
            auto keys = lower_group_by(*by);
            if (!keys.has_value()) {
                return std::unexpected(keys.error());
            }
            group_by = std::move(keys.value());
        }
        return builder_.update(std::move(fields), std::move(tuple_specs), std::move(group_by));
    }

    auto lower_model(const ModelClause& clause) -> std::expected<
        std::tuple<ir::ModelFormula, std::string, std::vector<ir::ModelParamSpec>>, LowerError> {
        // Convert AST formula → IR formula.
        ir::ModelFormula formula;
        formula.response = clause.formula.response;
        formula.has_intercept = clause.formula.has_intercept;
        for (const auto& term : clause.formula.terms) {
            formula.terms.push_back(
                ir::ModelTerm{.columns = term.columns, .is_dot = term.is_dot});
        }

        // Extract method name (required).
        std::string method;
        std::vector<ir::ModelParamSpec> params;
        for (const auto& p : clause.params) {
            if (p.name == "method") {
                const auto* ident = std::get_if<IdentifierExpr>(&p.value->node);
                if (ident == nullptr) {
                    return std::unexpected(
                        LowerError{.message = "model: method must be an identifier (e.g. ols)"});
                }
                method = ident->name;
            } else {
                auto expr = lower_expr_to_ir(*p.value);
                if (!expr.has_value()) {
                    return std::unexpected(expr.error());
                }
                params.push_back(
                    ir::ModelParamSpec{.name = p.name, .value = std::move(expr.value())});
            }
        }
        if (method.empty()) {
            return std::unexpected(
                LowerError{.message = "model clause requires a method parameter (e.g. method = ols)"});
        }
        return std::make_tuple(std::move(formula), std::move(method), std::move(params));
    }

    static auto lower_rename(const RenameClause& clause)
        -> std::expected<std::vector<ir::RenameSpec>, LowerError> {
        std::vector<ir::RenameSpec> renames;
        renames.reserve(clause.fields.size());
        for (const auto& field : clause.fields) {
            if (field.expr == nullptr) {
                return std::unexpected(
                    LowerError{.message = "rename field requires a right-hand side column name"});
            }
            const auto* ident = std::get_if<IdentifierExpr>(&field.expr->node);
            if (ident == nullptr) {
                return std::unexpected(
                    LowerError{.message = "rename: right-hand side must be a plain column name"});
            }
            renames.push_back(ir::RenameSpec{.new_name = field.name, .old_name = ident->name});
        }
        return renames;
    }

    static auto lower_order(const OrderClause& clause)
        -> std::expected<std::vector<ir::OrderKey>, LowerError> {
        std::vector<ir::OrderKey> keys;
        keys.reserve(clause.keys.size());
        for (const auto& key : clause.keys) {
            keys.push_back(ir::OrderKey{.name = key.name, .ascending = key.ascending});
        }
        return keys;
    }

    auto lower_expr_to_ir(const Expr& expr) -> std::expected<ir::Expr, LowerError> {
        if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
            return ir::Expr{.node = ir::ColumnRef{.name = ident->name}};
        }
        if (const auto* literal = std::get_if<LiteralExpr>(&expr.node)) {
            if (const auto* int_value = std::get_if<std::int64_t>(&literal->value)) {
                return ir::Expr{.node = ir::Literal{.value = *int_value}};
            }
            if (const auto* double_value = std::get_if<double>(&literal->value)) {
                return ir::Expr{.node = ir::Literal{.value = *double_value}};
            }
            if (const auto* bool_value = std::get_if<bool>(&literal->value)) {
                return ir::Expr{.node = ir::Literal{.value = *bool_value}};
            }
            if (const auto* str_value = std::get_if<std::string>(&literal->value)) {
                return ir::Expr{.node = ir::Literal{.value = *str_value}};
            }
            if (const auto* date_value = std::get_if<Date>(&literal->value)) {
                return ir::Expr{.node = ir::Literal{.value = *date_value}};
            }
            if (const auto* ts_value = std::get_if<Timestamp>(&literal->value)) {
                return ir::Expr{.node = ir::Literal{.value = *ts_value}};
            }
            return std::unexpected(LowerError{.message = "unsupported literal in expression"});
        }
        if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
            ir::CallExpr lowered_call;
            lowered_call.callee = call->callee;
            lowered_call.args.reserve(call->args.size());
            for (const auto& arg : call->args) {
                auto lowered_arg = lower_expr_to_ir(*arg);
                if (!lowered_arg.has_value()) {
                    return std::unexpected(lowered_arg.error());
                }
                lowered_call.args.push_back(
                    std::make_shared<ir::Expr>(std::move(lowered_arg.value())));
            }
            lowered_call.named_args.reserve(call->named_args.size());
            for (const auto& narg : call->named_args) {
                auto lowered_val = lower_expr_to_ir(*narg.value);
                if (!lowered_val.has_value()) {
                    return std::unexpected(lowered_val.error());
                }
                lowered_call.named_args.push_back(ir::NamedArg{
                    .name = narg.name,
                    .value = std::make_shared<ir::Expr>(std::move(lowered_val.value()))});
            }
            return ir::Expr{.node = std::move(lowered_call)};
        }
        if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
            auto left = lower_expr_to_ir(*binary->left);
            if (!left.has_value()) {
                return std::unexpected(left.error());
            }
            auto right = lower_expr_to_ir(*binary->right);
            if (!right.has_value()) {
                return std::unexpected(right.error());
            }
            auto op = to_arithmetic_op(binary->op);
            if (!op.has_value()) {
                return std::unexpected(
                    LowerError{.message = "unsupported binary operator in expression"});
            }
            ir::BinaryExpr bin{
                .op = op.value(),
                .left = std::make_shared<ir::Expr>(std::move(left.value())),
                .right = std::make_shared<ir::Expr>(std::move(right.value())),
            };
            return ir::Expr{.node = std::move(bin)};
        }
        if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
            return lower_expr_to_ir(*group->expr);
        }
        return std::unexpected(LowerError{.message = "unsupported expression"});
    }

    auto lower_aggregate(const ByClause* by, const SelectClause& select, ir::NodePtr child)
        -> std::expected<ir::NodePtr, LowerError> {
        std::vector<ir::ColumnRef> group_by;
        if (by != nullptr) {
            auto group_by_result = lower_group_by(*by);
            if (!group_by_result.has_value()) {
                return std::unexpected(group_by_result.error());
            }
            group_by = std::move(group_by_result.value());
        }

        std::unordered_map<std::string, bool> group_keys;
        for (const auto& key : group_by) {
            group_keys.emplace(key.name, true);
        }

        std::vector<ir::AggSpec> aggs;
        std::vector<ir::FieldSpec> updates;
        std::vector<std::string> final_columns;
        std::unordered_map<std::string, bool> temp_columns;
        std::size_t temp_counter = 0;

        auto make_temp = [&]() -> std::string {
            return "_agg" + std::to_string(temp_counter++);
        };

        std::function<std::expected<ir::Expr, LowerError>(const Expr&)> lower_agg_expr;
        lower_agg_expr = [&](const Expr& expr) -> std::expected<ir::Expr, LowerError> {
            if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
                if (group_keys.contains(ident->name)) {
                    return ir::Expr{.node = ir::ColumnRef{.name = ident->name}};
                }
                return std::unexpected(LowerError{
                    .message = "non-aggregate column in aggregate expression: " + ident->name,
                });
            }
            if (const auto* literal = std::get_if<LiteralExpr>(&expr.node)) {
                if (const auto* int_value = std::get_if<std::int64_t>(&literal->value)) {
                    return ir::Expr{.node = ir::Literal{.value = *int_value}};
                }
                if (const auto* double_value = std::get_if<double>(&literal->value)) {
                    return ir::Expr{.node = ir::Literal{.value = *double_value}};
                }
                if (const auto* str_value = std::get_if<std::string>(&literal->value)) {
                    return ir::Expr{.node = ir::Literal{.value = *str_value}};
                }
                return std::unexpected(LowerError{.message = "unsupported literal in expression"});
            }
            if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
                auto func = parse_agg_func(call->callee);
                if (!func.has_value()) {
                    return std::unexpected(
                        LowerError{.message = "unknown aggregate function: " + call->callee});
                }
                std::string alias = make_temp();
                if (call->callee == "count") {
                    if (!call->args.empty()) {
                        return std::unexpected(LowerError{.message = "count() takes no arguments"});
                    }
                    aggs.push_back(ir::AggSpec{
                        .func = func.value(),
                        .column = ir::ColumnRef{.name = ""},
                        .alias = alias,
                    });
                    temp_columns[alias] = true;
                    return ir::Expr{.node = ir::ColumnRef{.name = alias}};
                }
                if (call->callee == "ewma") {
                    if (call->args.size() != 2) {
                        return std::unexpected(LowerError{
                            .message = "ewma() takes two arguments: ewma(column, alpha)"});
                    }
                    const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                    if (ident == nullptr) {
                        return std::unexpected(LowerError{
                            .message = "first argument of ewma() must be a column name"});
                    }
                    double alpha = 0.0;
                    if (const auto* lit = std::get_if<LiteralExpr>(&call->args[1]->node)) {
                        if (const auto* dv = std::get_if<double>(&lit->value)) {
                            alpha = *dv;
                        } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                            alpha = static_cast<double>(*iv);
                        } else {
                            return std::unexpected(LowerError{
                                .message =
                                    "second argument of ewma() must be a numeric literal (alpha)"});
                        }
                    } else {
                        return std::unexpected(LowerError{
                            .message =
                                "second argument of ewma() must be a numeric literal (alpha)"});
                    }
                    aggs.push_back(ir::AggSpec{
                        .func = func.value(),
                        .column = ir::ColumnRef{.name = ident->name},
                        .alias = alias,
                        .param = alpha,
                    });
                    temp_columns[alias] = true;
                    return ir::Expr{.node = ir::ColumnRef{.name = alias}};
                }
                if (call->callee == "quantile") {
                    if (call->args.size() != 2) {
                        return std::unexpected(LowerError{
                            .message = "quantile() takes two arguments: quantile(column, p)"});
                    }
                    const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                    if (ident == nullptr) {
                        return std::unexpected(LowerError{
                            .message = "first argument of quantile() must be a column name"});
                    }
                    double p = 0.0;
                    if (const auto* lit = std::get_if<LiteralExpr>(&call->args[1]->node)) {
                        if (const auto* dv = std::get_if<double>(&lit->value)) {
                            p = *dv;
                        } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                            p = static_cast<double>(*iv);
                        } else {
                            return std::unexpected(LowerError{
                                .message =
                                    "second argument of quantile() must be a numeric literal (p)"});
                        }
                    } else {
                        return std::unexpected(LowerError{
                            .message =
                                "second argument of quantile() must be a numeric literal (p)"});
                    }
                    aggs.push_back(ir::AggSpec{
                        .func = func.value(),
                        .column = ir::ColumnRef{.name = ident->name},
                        .alias = alias,
                        .param = p,
                    });
                    temp_columns[alias] = true;
                    return ir::Expr{.node = ir::ColumnRef{.name = alias}};
                }
                if (call->args.size() != 1) {
                    return std::unexpected(
                        LowerError{.message = "aggregate functions take one argument"});
                }
                const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                if (ident == nullptr) {
                    return std::unexpected(
                        LowerError{.message = "aggregate argument must be a column"});
                }
                aggs.push_back(ir::AggSpec{
                    .func = func.value(),
                    .column = ir::ColumnRef{.name = ident->name},
                    .alias = alias,
                });
                temp_columns[alias] = true;
                return ir::Expr{.node = ir::ColumnRef{.name = alias}};
            }
            if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
                auto left = lower_agg_expr(*binary->left);
                if (!left.has_value()) {
                    return std::unexpected(left.error());
                }
                auto right = lower_agg_expr(*binary->right);
                if (!right.has_value()) {
                    return std::unexpected(right.error());
                }
                auto op = to_arithmetic_op(binary->op);
                if (!op.has_value()) {
                    return std::unexpected(
                        LowerError{.message = "unsupported binary operator in expression"});
                }
                ir::BinaryExpr bin{
                    .op = op.value(),
                    .left = std::make_shared<ir::Expr>(std::move(left.value())),
                    .right = std::make_shared<ir::Expr>(std::move(right.value())),
                };
                return ir::Expr{.node = std::move(bin)};
            }
            if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
                return lower_agg_expr(*group->expr);
            }
            return std::unexpected(LowerError{.message = "unsupported aggregate expression"});
        };

        for (const auto& field : select.fields) {
            if (field.expr == nullptr) {
                if (!group_keys.contains(field.name)) {
                    return std::unexpected(LowerError{
                        .message = "non-aggregate column in aggregate select: " + field.name,
                    });
                }
                final_columns.push_back(field.name);
                continue;
            }

            if (const auto* call = std::get_if<CallExpr>(&field.expr->node)) {
                auto func = parse_agg_func(call->callee);
                if (func.has_value()) {
                    if (call->callee == "count") {
                        if (!call->args.empty()) {
                            return std::unexpected(
                                LowerError{.message = "count() takes no arguments"});
                        }
                        aggs.push_back(ir::AggSpec{
                            .func = func.value(),
                            .column = ir::ColumnRef{.name = ""},
                            .alias = field.name,
                        });
                        final_columns.push_back(field.name);
                        continue;
                    }
                    if (call->callee == "ewma") {
                        if (call->args.size() != 2) {
                            return std::unexpected(LowerError{
                                .message = "ewma() takes two arguments: ewma(column, alpha)"});
                        }
                        const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                        if (ident == nullptr) {
                            return std::unexpected(LowerError{
                                .message = "first argument of ewma() must be a column name"});
                        }
                        double alpha = 0.0;
                        if (const auto* lit = std::get_if<LiteralExpr>(&call->args[1]->node)) {
                            if (const auto* dv = std::get_if<double>(&lit->value)) {
                                alpha = *dv;
                            } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                                alpha = static_cast<double>(*iv);
                            } else {
                                return std::unexpected(LowerError{
                                    .message = "second argument of ewma() must be a numeric "
                                               "literal (alpha)"});
                            }
                        } else {
                            return std::unexpected(
                                LowerError{.message = "second argument of ewma() must be a "
                                                      "numeric literal (alpha)"});
                        }
                        aggs.push_back(ir::AggSpec{
                            .func = func.value(),
                            .column = ir::ColumnRef{.name = ident->name},
                            .alias = field.name,
                            .param = alpha,
                        });
                        final_columns.push_back(field.name);
                        continue;
                    }
                    if (call->callee == "quantile") {
                        if (call->args.size() != 2) {
                            return std::unexpected(LowerError{
                                .message = "quantile() takes two arguments: quantile(column, p)"});
                        }
                        const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                        if (ident == nullptr) {
                            return std::unexpected(LowerError{
                                .message = "first argument of quantile() must be a column name"});
                        }
                        double p = 0.0;
                        if (const auto* lit = std::get_if<LiteralExpr>(&call->args[1]->node)) {
                            if (const auto* dv = std::get_if<double>(&lit->value)) {
                                p = *dv;
                            } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                                p = static_cast<double>(*iv);
                            } else {
                                return std::unexpected(LowerError{
                                    .message = "second argument of quantile() must be a numeric "
                                               "literal (p)"});
                            }
                        } else {
                            return std::unexpected(
                                LowerError{.message = "second argument of quantile() must be a "
                                                      "numeric literal (p)"});
                        }
                        aggs.push_back(ir::AggSpec{
                            .func = func.value(),
                            .column = ir::ColumnRef{.name = ident->name},
                            .alias = field.name,
                            .param = p,
                        });
                        final_columns.push_back(field.name);
                        continue;
                    }
                    if (call->args.size() != 1) {
                        return std::unexpected(
                            LowerError{.message = "aggregate functions take one argument"});
                    }
                    const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                    if (ident == nullptr) {
                        return std::unexpected(
                            LowerError{.message = "aggregate argument must be a column"});
                    }
                    aggs.push_back(ir::AggSpec{
                        .func = func.value(),
                        .column = ir::ColumnRef{.name = ident->name},
                        .alias = field.name,
                    });
                    final_columns.push_back(field.name);
                    continue;
                }
            }

            auto expr_ir = lower_agg_expr(*field.expr);
            if (!expr_ir.has_value()) {
                return std::unexpected(expr_ir.error());
            }
            updates.push_back(ir::FieldSpec{
                .alias = field.name,
                .expr = std::move(expr_ir.value()),
            });
            final_columns.push_back(field.name);
        }

        auto aggregate = builder_.aggregate(std::move(group_by), std::move(aggs));
        aggregate->add_child(std::move(child));

        ir::NodePtr node = std::move(aggregate);
        if (!updates.empty()) {
            auto update = builder_.update(std::move(updates));
            update->add_child(std::move(node));
            node = std::move(update);
        }

        bool needs_project = !updates.empty();
        if (needs_project) {
            std::vector<ir::ColumnRef> columns;
            columns.reserve(final_columns.size());
            for (const auto& name : final_columns) {
                columns.push_back(ir::ColumnRef{.name = name});
            }
            auto project = builder_.project(std::move(columns));
            project->add_child(std::move(node));
            node = std::move(project);
        }

        return node;
    }

    static auto select_has_aggregate(const SelectClause& select) -> bool {
        std::function<bool(const Expr&)> has_agg;
        has_agg = [&](const Expr& expr) -> bool {
            if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
                if (parse_agg_func(call->callee).has_value()) {
                    return true;
                }
                return std::ranges::any_of(call->args,
                                           [&](const auto& arg) { return has_agg(*arg); });
            }
            if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
                return has_agg(*binary->left) || has_agg(*binary->right);
            }
            if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
                return has_agg(*group->expr);
            }
            return false;
        };

        return std::ranges::any_of(select.fields, [&](const auto& field) {
            return field.expr != nullptr && has_agg(*field.expr);
        });
    }

    static auto lower_resample_aggs(const SelectClause& select)
        -> std::expected<std::vector<ir::AggSpec>, LowerError> {
        std::vector<ir::AggSpec> aggs;
        for (const auto& field : select.fields) {
            if (field.expr == nullptr) {
                return std::unexpected(
                    LowerError{.message = "resample select: bare column reference not supported — "
                                          "use an aggregate function"});
            }
            const auto* call = std::get_if<CallExpr>(&field.expr->node);
            if (call == nullptr) {
                return std::unexpected(LowerError{
                    .message = "resample select: only aggregate function calls are supported"});
            }
            auto func = parse_agg_func(call->callee);
            if (!func.has_value()) {
                return std::unexpected(LowerError{
                    .message = "resample select: unknown aggregate function: " + call->callee});
            }
            if (call->callee == "count") {
                if (!call->args.empty())
                    return std::unexpected(LowerError{.message = "count() takes no arguments"});
                aggs.push_back(
                    ir::AggSpec{.func = func.value(), .column = {.name = ""}, .alias = field.name});
                continue;
            }
            if (call->callee == "ewma") {
                if (call->args.size() != 2)
                    return std::unexpected(
                        LowerError{.message = "ewma() takes two arguments: ewma(column, alpha)"});
                const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                if (ident == nullptr)
                    return std::unexpected(
                        LowerError{.message = "first argument of ewma() must be a column name"});
                double alpha = 0.0;
                if (const auto* lit = std::get_if<LiteralExpr>(&call->args[1]->node)) {
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        alpha = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        alpha = static_cast<double>(*iv);
                    } else {
                        return std::unexpected(LowerError{
                            .message =
                                "second argument of ewma() must be a numeric literal (alpha)"});
                    }
                } else {
                    return std::unexpected(LowerError{
                        .message = "second argument of ewma() must be a numeric literal (alpha)"});
                }
                aggs.push_back(ir::AggSpec{.func = func.value(),
                                           .column = {.name = ident->name},
                                           .alias = field.name,
                                           .param = alpha});
                continue;
            }
            if (call->callee == "quantile") {
                if (call->args.size() != 2)
                    return std::unexpected(LowerError{
                        .message = "quantile() takes two arguments: quantile(column, p)"});
                const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
                if (ident == nullptr)
                    return std::unexpected(LowerError{
                        .message = "first argument of quantile() must be a column name"});
                double p = 0.0;
                if (const auto* lit = std::get_if<LiteralExpr>(&call->args[1]->node)) {
                    if (const auto* dv = std::get_if<double>(&lit->value)) {
                        p = *dv;
                    } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                        p = static_cast<double>(*iv);
                    } else {
                        return std::unexpected(LowerError{
                            .message =
                                "second argument of quantile() must be a numeric literal (p)"});
                    }
                } else {
                    return std::unexpected(LowerError{
                        .message = "second argument of quantile() must be a numeric literal (p)"});
                }
                aggs.push_back(ir::AggSpec{.func = func.value(),
                                           .column = {.name = ident->name},
                                           .alias = field.name,
                                           .param = p});
                continue;
            }
            if (call->args.size() != 1)
                return std::unexpected(
                    LowerError{.message = "aggregate functions take one argument"});
            const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
            if (ident == nullptr)
                return std::unexpected(
                    LowerError{.message = "aggregate argument must be a column name"});
            aggs.push_back(ir::AggSpec{
                .func = func.value(), .column = {.name = ident->name}, .alias = field.name});
        }
        return aggs;
    }

    static auto lower_group_by(const ByClause& by)
        -> std::expected<std::vector<ir::ColumnRef>, LowerError> {
        std::vector<ir::ColumnRef> group_by;
        for (const auto& field : by.keys) {
            if (field.expr != nullptr) {
                return std::unexpected(
                    LowerError{.message = "computed group keys not supported yet"});
            }
            group_by.push_back(ir::ColumnRef{.name = field.name});
        }
        return group_by;
    }

    static auto lower_literal(const LiteralExpr& literal)
        -> std::expected<std::variant<std::int64_t, double, std::string, Date, Timestamp>,
                         LowerError> {
        if (const auto* int_value = std::get_if<std::int64_t>(&literal.value)) {
            return *int_value;
        }
        if (const auto* double_value = std::get_if<double>(&literal.value)) {
            return *double_value;
        }
        if (const auto* str_value = std::get_if<std::string>(&literal.value)) {
            return *str_value;
        }
        if (const auto* date_value = std::get_if<Date>(&literal.value)) {
            return *date_value;
        }
        if (const auto* ts_value = std::get_if<Timestamp>(&literal.value)) {
            return *ts_value;
        }
        return std::unexpected(LowerError{.message = "literal type not supported in filter"});
    }

    static auto parse_duration(std::string_view text) -> std::expected<ir::Duration, LowerError> {
        if (text.size() < 2) {
            return std::unexpected(LowerError{.message = "invalid duration literal"});
        }
        std::size_t unit_pos = 0;
        while (unit_pos < text.size() &&
               std::isdigit(static_cast<unsigned char>(text[unit_pos])) != 0) {
            unit_pos += 1;
        }
        if (unit_pos == 0 || unit_pos == text.size()) {
            return std::unexpected(LowerError{.message = "invalid duration literal"});
        }
        auto number_part = text.substr(0, unit_pos);
        auto unit_part = text.substr(unit_pos);
        std::uint64_t value = 0;
        auto result = std::from_chars(number_part.begin(), number_part.end(), value);
        if (result.ec != std::errc()) {
            return std::unexpected(LowerError{.message = "invalid duration literal"});
        }
        std::uint64_t multiplier = 0;
        if (unit_part == "ns") {
            multiplier = 1;
        } else if (unit_part == "us") {
            multiplier = 1'000;
        } else if (unit_part == "ms") {
            multiplier = 1'000'000;
        } else if (unit_part == "s") {
            multiplier = 1'000'000'000;
        } else if (unit_part == "m") {
            multiplier = 60ULL * 1'000'000'000;
        } else if (unit_part == "h") {
            multiplier = 60ULL * 60ULL * 1'000'000'000;
        } else if (unit_part == "d") {
            multiplier = 24ULL * 60ULL * 60ULL * 1'000'000'000;
        } else if (unit_part == "w") {
            multiplier = 7ULL * 24ULL * 60ULL * 60ULL * 1'000'000'000;
        } else if (unit_part == "mo") {
            multiplier = 30ULL * 24ULL * 60ULL * 60ULL * 1'000'000'000;
        } else if (unit_part == "y") {
            multiplier = 365ULL * 24ULL * 60ULL * 60ULL * 1'000'000'000;
        } else {
            return std::unexpected(LowerError{.message = "invalid duration literal"});
        }
        return ir::Duration(static_cast<std::int64_t>(value * multiplier));
    }

    static auto is_compare_op(BinaryOp op) -> bool {
        switch (op) {
            case BinaryOp::Eq:
            case BinaryOp::Ne:
            case BinaryOp::Lt:
            case BinaryOp::Le:
            case BinaryOp::Gt:
            case BinaryOp::Ge:
                return true;
            default:
                return false;
        }
    }

    static auto to_compare_op(BinaryOp op) -> ir::CompareOp {
        switch (op) {
            case BinaryOp::Eq:
                return ir::CompareOp::Eq;
            case BinaryOp::Ne:
                return ir::CompareOp::Ne;
            case BinaryOp::Lt:
                return ir::CompareOp::Lt;
            case BinaryOp::Le:
                return ir::CompareOp::Le;
            case BinaryOp::Gt:
                return ir::CompareOp::Gt;
            case BinaryOp::Ge:
                return ir::CompareOp::Ge;
            default:
                return ir::CompareOp::Eq;
        }
    }

    static auto to_arithmetic_op(BinaryOp op) -> std::optional<ir::ArithmeticOp> {
        switch (op) {
            case BinaryOp::Add:
                return ir::ArithmeticOp::Add;
            case BinaryOp::Sub:
                return ir::ArithmeticOp::Sub;
            case BinaryOp::Mul:
                return ir::ArithmeticOp::Mul;
            case BinaryOp::Div:
                return ir::ArithmeticOp::Div;
            case BinaryOp::Mod:
                return ir::ArithmeticOp::Mod;
            default:
                return std::nullopt;
        }
    }

    static auto parse_agg_func(std::string_view name) -> std::optional<ir::AggFunc> {
        if (name == "sum") {
            return ir::AggFunc::Sum;
        }
        if (name == "mean") {
            return ir::AggFunc::Mean;
        }
        if (name == "min") {
            return ir::AggFunc::Min;
        }
        if (name == "max") {
            return ir::AggFunc::Max;
        }
        if (name == "count") {
            return ir::AggFunc::Count;
        }
        if (name == "first") {
            return ir::AggFunc::First;
        }
        if (name == "last") {
            return ir::AggFunc::Last;
        }
        if (name == "median") {
            return ir::AggFunc::Median;
        }
        if (name == "std") {
            return ir::AggFunc::Stddev;
        }
        if (name == "ewma") {
            return ir::AggFunc::Ewma;
        }
        if (name == "quantile") {
            return ir::AggFunc::Quantile;
        }
        if (name == "skew") {
            return ir::AggFunc::Skew;
        }
        if (name == "kurtosis") {
            return ir::AggFunc::Kurtosis;
        }
        return std::nullopt;
    }

    // NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
    auto clone_node(const ir::Node& node) -> ir::NodePtr {
        auto clone_tuple_fields = [&](const std::vector<ir::TupleFieldSpec>& tuple_fields) {
            std::vector<ir::TupleFieldSpec> cloned;
            cloned.reserve(tuple_fields.size());
            for (const auto& tf : tuple_fields) {
                cloned.push_back(ir::TupleFieldSpec{
                    .aliases = tf.aliases,
                    .source = tf.source ? clone_node(*tf.source) : nullptr,
                });
            }
            return cloned;
        };
        auto clone_construct_columns = [&](const std::vector<ir::ConstructColumn>& columns) {
            std::vector<ir::ConstructColumn> cloned;
            cloned.reserve(columns.size());
            for (const auto& col : columns) {
                ir::ConstructColumn cc;
                cc.name = col.name;
                cc.elements = col.elements;
                if (col.expr_node) {
                    cc.expr_node = clone_node(*col.expr_node);
                }
                cloned.push_back(std::move(cc));
            }
            return cloned;
        };

        ir::NodePtr clone;
        switch (node.kind()) {
            case ir::NodeKind::Scan: {
                const auto& scan = static_cast<const ir::ScanNode&>(node);
                clone = builder_.scan(scan.source_name());
                break;
            }
            case ir::NodeKind::Filter: {
                const auto& filter = static_cast<const ir::FilterNode&>(node);
                clone = builder_.filter(clone_filter_expr(filter.predicate()));
                break;
            }
            case ir::NodeKind::Project: {
                const auto& project = static_cast<const ir::ProjectNode&>(node);
                clone = builder_.project(project.columns());
                break;
            }
            case ir::NodeKind::Distinct: {
                clone = builder_.distinct();
                break;
            }
            case ir::NodeKind::Order: {
                const auto& order = static_cast<const ir::OrderNode&>(node);
                clone = builder_.order(order.keys());
                break;
            }
            case ir::NodeKind::Aggregate: {
                const auto& agg = static_cast<const ir::AggregateNode&>(node);
                clone = builder_.aggregate(agg.group_by(), agg.aggregations());
                break;
            }
            case ir::NodeKind::Update: {
                const auto& update = static_cast<const ir::UpdateNode&>(node);
                clone = builder_.update(update.fields(), clone_tuple_fields(update.tuple_fields()),
                                        update.group_by());
                break;
            }
            case ir::NodeKind::Rename: {
                const auto& rename = static_cast<const ir::RenameNode&>(node);
                clone = builder_.rename(rename.renames());
                break;
            }
            case ir::NodeKind::Window: {
                const auto& window = static_cast<const ir::WindowNode&>(node);
                clone = builder_.window(window.duration());
                break;
            }
            case ir::NodeKind::Resample: {
                const auto& rs = static_cast<const ir::ResampleNode&>(node);
                clone = builder_.resample(rs.duration(), rs.group_by(), rs.aggregations());
                break;
            }
            case ir::NodeKind::AsTimeframe: {
                const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
                clone = builder_.as_timeframe(atf.column());
                break;
            }
            case ir::NodeKind::ExternCall: {
                const auto& ec = static_cast<const ir::ExternCallNode&>(node);
                clone = builder_.extern_call(ec.callee(), ec.args());
                break;
            }
            case ir::NodeKind::Join: {
                const auto& join = static_cast<const ir::JoinNode&>(node);
                std::optional<ir::FilterExprPtr> pred_clone;
                if (join.predicate().has_value()) {
                    pred_clone = clone_filter_expr(**join.predicate());
                }
                clone = builder_.join(join.kind(), join.keys(), std::move(pred_clone));
                break;
            }
            case ir::NodeKind::Melt: {
                const auto& mn = static_cast<const ir::MeltNode&>(node);
                clone = builder_.melt(mn.id_columns(), mn.measure_columns());
                break;
            }
            case ir::NodeKind::Dcast: {
                const auto& dn = static_cast<const ir::DcastNode&>(node);
                clone = builder_.dcast(dn.pivot_column(), dn.value_column(), dn.row_keys());
                break;
            }
            case ir::NodeKind::Cov: {
                clone = builder_.cov();
                break;
            }
            case ir::NodeKind::Corr: {
                clone = builder_.corr();
                break;
            }
            case ir::NodeKind::Transpose: {
                clone = builder_.transpose();
                break;
            }
            case ir::NodeKind::Matmul: {
                clone = builder_.matmul();
                break;
            }
            case ir::NodeKind::Model: {
                const auto& mn = static_cast<const ir::ModelNode&>(node);
                clone = builder_.model(mn.formula(), mn.method(),
                                       std::vector<ir::ModelParamSpec>(mn.params()));
                break;
            }
            case ir::NodeKind::Construct: {
                const auto& cn = static_cast<const ir::ConstructNode&>(node);
                clone = builder_.construct(clone_construct_columns(cn.columns()));
                break;
            }
            case ir::NodeKind::Stream: {
                const auto& stream = static_cast<const ir::StreamNode&>(node);
                clone = builder_.stream(stream.source_callee(), stream.source_args(),
                                        stream.sink_callee(), stream.sink_args(),
                                        stream.stream_kind(), stream.bucket_duration());
                break;
            }
            case ir::NodeKind::Program: {
                const auto& prog = static_cast<const ir::ProgramNode&>(node);
                std::vector<ir::NodePtr> preamble;
                preamble.reserve(prog.preamble().size());
                for (const auto& preamble_node : prog.preamble()) {
                    preamble.push_back(preamble_node ? clone_node(*preamble_node) : nullptr);
                }
                clone = builder_.program(std::move(preamble), clone_node(prog.main_node()));
                break;
            }
        }

        if (!clone) {
            return nullptr;
        }
        for (const auto& child : node.children()) {
            if (!child) {
                clone->add_child(nullptr);
                continue;
            }
            clone->add_child(clone_node(*child));
        }
        return clone;
    }
    // NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

    /// Recursively walk an IR tree and return true if any node has the given kind.
    static auto contains_node_kind(const ir::Node& node, ir::NodeKind target) -> bool {
        if (node.kind() == target) {
            return true;
        }
        for (const auto& child : node.children()) {
            if (contains_node_kind(*child, target)) {
                return true;
            }
        }
        return false;
    }

    /// Walk an IR tree and return the Duration of the first ResampleNode found.
    static auto find_resample_duration(const ir::Node& node) -> std::optional<ir::Duration> {
        if (node.kind() == ir::NodeKind::Resample) {
            return static_cast<const ir::ResampleNode&>(node).duration();
        }
        for (const auto& child : node.children()) {
            if (auto dur = find_resample_duration(*child)) {
                return dur;
            }
        }
        return std::nullopt;
    }

    /// Lower a `StreamExpr` into a `StreamNode`.
    ///
    /// The transform is lowered as an anonymous block with `ScanNode("__stream_input__")` as
    /// its implicit base.  The stream kind is inferred from the transform IR:
    ///   - ResampleNode present → TimeBucket (emit when bucket boundary crossed)
    ///   - otherwise            → PerRow     (emit on every incoming row)
    auto lower_stream(StreamExpr& stream) -> LowerResult {
        // --- source ---
        const auto* src_call = std::get_if<CallExpr>(&stream.source->node);
        if (src_call == nullptr) {
            return std::unexpected(
                LowerError{.message = "Stream 'source' must be a function call expression"});
        }
        if (!table_externs_.contains(src_call->callee)) {
            return std::unexpected(
                LowerError{.message = "Stream source '" + src_call->callee +
                                      "' is not a known table-returning extern"});
        }
        std::vector<ir::Expr> source_args;
        source_args.reserve(src_call->args.size());
        for (const auto& arg : src_call->args) {
            auto lowered = lower_expr_to_ir(*arg);
            if (!lowered.has_value()) {
                return std::unexpected(lowered.error());
            }
            source_args.push_back(std::move(lowered.value()));
        }

        // --- sink ---
        if (!sink_externs_.contains(stream.sink_callee)) {
            return std::unexpected(LowerError{.message = "Stream sink '" + stream.sink_callee +
                                                         "' is not a known table-consumer extern"});
        }
        std::vector<ir::Expr> sink_args;
        sink_args.reserve(stream.sink_args.size());
        for (const auto& arg : stream.sink_args) {
            auto lowered = lower_expr_to_ir(*arg);
            if (!lowered.has_value()) {
                return std::unexpected(lowered.error());
            }
            sink_args.push_back(std::move(lowered.value()));
        }

        // --- transform ---
        // Synthesise a BlockExpr with "__stream_input__" as the base so that the existing
        // lower_block path handles clause ordering and validation.
        auto base_ident = std::make_unique<Expr>();
        base_ident->node = IdentifierExpr{.name = "__stream_input__"};
        BlockExpr synthetic_block{.base = std::move(base_ident),
                                  .clauses = std::move(stream.transform)};
        auto transform_ir = lower_block(synthetic_block);
        if (!transform_ir.has_value()) {
            return transform_ir;
        }

        // --- infer stream kind ---
        ir::StreamKind kind = ir::StreamKind::PerRow;
        ir::Duration bucket_duration{};
        if (contains_node_kind(*transform_ir.value(), ir::NodeKind::Resample)) {
            kind = ir::StreamKind::TimeBucket;
            auto dur = find_resample_duration(*transform_ir.value());
            if (dur.has_value()) {
                bucket_duration = *dur;
            }
        }

        // Build the StreamNode; transform IR is stored as child[0].
        auto node = builder_.stream(src_call->callee, std::move(source_args), stream.sink_callee,
                                    std::move(sink_args), kind, bucket_duration);
        node->add_child(std::move(transform_ir.value()));
        return node;
    }

    ir::Builder builder_;
    std::unordered_map<std::string, ir::NodePtr>* bindings_ = nullptr;
    std::unordered_set<std::string> table_externs_;
    std::unordered_set<std::string> sink_externs_;
};

}  // namespace

auto lower(const Program& program) -> LowerResult {
    auto effects = analyze_effects(program);
    if (!effects.has_value()) {
        return std::unexpected(LowerError{.message = effects.error().format()});
    }

    std::unordered_map<std::string, ir::NodePtr> bindings;
    Lowerer lowerer(&bindings);
    auto lowered = lowerer.lower_program(program);
    if (!lowered.has_value()) {
        return lowered;
    }

    const auto optimization_context = build_optimization_context(*effects);
    ir::OptimizationStats optimization_stats;
    auto optimized =
        ir::optimize_plan(std::move(*lowered), optimization_context, &optimization_stats);
    return optimized;
}

auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult {
    Lowerer lowerer(&context.bindings, context.table_externs, context.sink_externs);
    return lowerer.lower_expression(expr);
}

}  // namespace ibex::parser
