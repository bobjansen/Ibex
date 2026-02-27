#include <ibex/ir/builder.hpp>
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

class Lowerer {
   public:
    explicit Lowerer(std::unordered_map<std::string, ir::NodePtr>* bindings)
        : bindings_(bindings) {}

    auto lower_program(const Program& program) -> LowerResult {
        ir::NodePtr last_expr;
        for (const auto& stmt : program.statements) {
            if (const auto* ext = std::get_if<ExternDecl>(&stmt)) {
                // Track externs that return a table so lower_table_call can
                // produce ExternCallNodes for them.
                if (ext->return_type.kind == Type::Kind::DataFrame ||
                    ext->return_type.kind == Type::Kind::TimeFrame) {
                    table_externs_.insert(ext->name);
                }
                continue;
            }
            if (std::holds_alternative<FunctionDecl>(stmt)) {
                continue;
            }
            if (std::holds_alternative<LetStmt>(stmt)) {
                const auto& let_stmt = std::get<LetStmt>(stmt);
                auto value = lower_expr(*let_stmt.value);
                if (!value.has_value()) {
                    return std::unexpected(value.error());
                }
                if (bindings_ != nullptr) {
                    (*bindings_)[let_stmt.name] = std::move(value.value());
                }
                continue;
            }
            if (std::holds_alternative<ExprStmt>(stmt)) {
                const auto& expr_stmt = std::get<ExprStmt>(stmt);
                auto value = lower_expr(*expr_stmt.expr);
                if (!value.has_value()) {
                    return std::unexpected(value.error());
                }
                last_expr = std::move(value.value());
            }
        }
        if (!last_expr) {
            return std::unexpected(LowerError{.message = "no expression to lower"});
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
        return std::unexpected(LowerError{.message = "expected DataFrame expression"});
    }

    auto lower_table_call(const CallExpr& call) -> LowerResult {
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
            case JoinKind::Asof:
                kind = ir::JoinKind::Asof;
                break;
        }

        auto node = builder_.join(kind, join.keys);
        node->add_child(std::move(left.value()));
        node->add_child(std::move(right.value()));
        return node;
    }

    auto lower_block(const BlockExpr& block) -> LowerResult {
        auto base = lower_expr(*block.base);
        if (!base.has_value()) {
            return base;
        }

        ClauseState state;
        for (const auto& clause : block.clauses) {
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
        if (state.by && !state.select && !state.update && !state.resample) {
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

        if (!state.resample && state.select && (state.by || select_has_aggregate(*state.select))) {
            auto aggregate = lower_aggregate(state.by, *state.select, std::move(node));
            if (!aggregate.has_value()) {
                return std::unexpected(aggregate.error());
            }
            node = std::move(aggregate.value());
        } else if (!state.resample && state.select) {
            auto project = lower_select_projection(state.select->fields, std::move(node));
            if (!project.has_value()) {
                return std::unexpected(project.error());
            }
            node = std::move(project.value());
        } else if (state.distinct) {
            auto project = lower_select_projection(state.distinct->fields, std::move(node));
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

        return node;
    }

    struct ClauseState {
        const FilterClause* filter = nullptr;
        const SelectClause* select = nullptr;
        const DistinctClause* distinct = nullptr;
        const UpdateClause* update = nullptr;
        const OrderClause* order = nullptr;
        const ByClause* by = nullptr;
        const WindowClause* window = nullptr;
        const ResampleClause* resample = nullptr;
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

    auto lower_select_projection(const std::vector<Field>& clause_fields, ir::NodePtr base)
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

        if (fields.empty()) {
            auto project = builder_.project(std::move(columns));
            project->add_child(std::move(base));
            return project;
        }

        auto update = builder_.update(std::move(fields));
        update->add_child(std::move(base));

        auto project = builder_.project(std::move(columns));
        project->add_child(std::move(update));
        return project;
    }

    auto lower_update(const ByClause* by, const UpdateClause& clause)
        -> std::expected<ir::NodePtr, LowerError> {
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
        std::vector<ir::ColumnRef> group_by;
        if (by != nullptr) {
            auto keys = lower_group_by(*by);
            if (!keys.has_value()) {
                return std::unexpected(keys.error());
            }
            group_by = std::move(keys.value());
        }
        return builder_.update(std::move(fields), std::move(group_by));
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
        return std::nullopt;
    }

    // NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
    auto clone_node(const ir::Node& node) -> ir::NodePtr {
        switch (node.kind()) {
            case ir::NodeKind::Scan: {
                const auto& scan = static_cast<const ir::ScanNode&>(node);
                auto clone = builder_.scan(scan.source_name());
                return clone;
            }
            case ir::NodeKind::Filter: {
                const auto& filter = static_cast<const ir::FilterNode&>(node);
                auto clone = builder_.filter(clone_filter_expr(filter.predicate()));
                return clone;
            }
            case ir::NodeKind::Project: {
                const auto& project = static_cast<const ir::ProjectNode&>(node);
                auto clone = builder_.project(project.columns());
                return clone;
            }
            case ir::NodeKind::Distinct: {
                return builder_.distinct();
            }
            case ir::NodeKind::Order: {
                const auto& order = static_cast<const ir::OrderNode&>(node);
                return builder_.order(order.keys());
            }
            case ir::NodeKind::Aggregate: {
                const auto& agg = static_cast<const ir::AggregateNode&>(node);
                auto clone = builder_.aggregate(agg.group_by(), agg.aggregations());
                return clone;
            }
            case ir::NodeKind::Update: {
                const auto& update = static_cast<const ir::UpdateNode&>(node);
                auto clone = builder_.update(update.fields(), update.group_by());
                return clone;
            }
            case ir::NodeKind::Window: {
                const auto& window = static_cast<const ir::WindowNode&>(node);
                auto clone = builder_.window(window.duration());
                return clone;
            }
            case ir::NodeKind::Resample: {
                const auto& rs = static_cast<const ir::ResampleNode&>(node);
                return builder_.resample(rs.duration(), rs.group_by(), rs.aggregations());
            }
            case ir::NodeKind::AsTimeframe: {
                const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
                return builder_.as_timeframe(atf.column());
            }
            case ir::NodeKind::ExternCall: {
                const auto& ec = static_cast<const ir::ExternCallNode&>(node);
                return builder_.extern_call(ec.callee(), ec.args());
            }
            case ir::NodeKind::Join: {
                const auto& join = static_cast<const ir::JoinNode&>(node);
                return builder_.join(join.kind(), join.keys());
            }
        }
        return nullptr;
    }
    // NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

    ir::Builder builder_;
    std::unordered_map<std::string, ir::NodePtr>* bindings_ = nullptr;
    std::unordered_set<std::string> table_externs_;
};

}  // namespace

auto lower(const Program& program) -> LowerResult {
    std::unordered_map<std::string, ir::NodePtr> bindings;
    Lowerer lowerer(&bindings);
    return lowerer.lower_program(program);
}

auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult {
    Lowerer lowerer(&context.bindings);
    return lowerer.lower_expression(expr);
}

}  // namespace ibex::parser
