#include <ibex/parser/lower.hpp>

#include <ibex/ir/builder.hpp>

#include <charconv>
#include <cctype>
#include <memory>
#include <unordered_map>

namespace ibex::parser {

namespace {

class Lowerer {
public:
    explicit Lowerer(std::unordered_map<std::string, ir::NodePtr>* bindings)
        : bindings_(bindings) {}

    auto lower_program(const Program& program) -> LowerResult {
        ir::NodePtr last_expr;
        for (const auto& stmt : program.statements) {
            if (std::holds_alternative<ExternDecl>(stmt)) {
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
        return std::unexpected(LowerError{.message = "expected DataFrame expression"});
    }

    auto lower_identifier(const IdentifierExpr& ident) -> LowerResult {
        if (bindings_ != nullptr) {
            if (auto it = bindings_->find(ident.name); it != bindings_->end()) {
                return clone_node(*it->second);
            }
        }
        return builder_.scan(ident.name);
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
            return std::unexpected(LowerError{.message = "select and update are mutually exclusive"});
        }
        if (state.by && !state.select && !state.update) {
            return std::unexpected(LowerError{.message = "by requires select or update"});
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

        if (state.by && state.select) {
            auto aggregate = lower_aggregate(*state.by, *state.select);
            if (!aggregate.has_value()) {
                return std::unexpected(aggregate.error());
            }
            aggregate.value()->add_child(std::move(node));
            node = std::move(aggregate.value());
        } else if (state.select) {
            auto project = lower_project(*state.select);
            if (!project.has_value()) {
                return std::unexpected(project.error());
            }
            project.value()->add_child(std::move(node));
            node = std::move(project.value());
        }

        if (state.update) {
            auto update = lower_update(state.by, *state.update);
            if (!update.has_value()) {
                return std::unexpected(update.error());
            }
            update.value()->add_child(std::move(node));
            node = std::move(update.value());
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

        return node;
    }

    struct ClauseState {
        const FilterClause* filter = nullptr;
        const SelectClause* select = nullptr;
        const UpdateClause* update = nullptr;
        const ByClause* by = nullptr;
        const WindowClause* window = nullptr;
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
            if (std::holds_alternative<UpdateClause>(clause)) {
                if (update != nullptr) {
                    error = "duplicate update clause";
                    return false;
                }
                update = &std::get<UpdateClause>(clause);
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
            return true;
        }
    };

    auto lower_filter(const FilterClause& clause) -> std::expected<ir::FilterPredicate, LowerError> {
        const auto* binary = std::get_if<BinaryExpr>(&clause.predicate->node);
        if (binary == nullptr) {
            return std::unexpected(LowerError{.message = "filter predicate must be a comparison"});
        }
        if (!is_compare_op(binary->op)) {
            return std::unexpected(LowerError{.message = "filter predicate must be a comparison"});
        }
        const auto* ident = std::get_if<IdentifierExpr>(&binary->left->node);
        if (ident == nullptr) {
            return std::unexpected(LowerError{.message = "filter predicate left side must be a column"});
        }
        const auto* literal = std::get_if<LiteralExpr>(&binary->right->node);
        if (literal == nullptr) {
            return std::unexpected(LowerError{.message = "filter predicate right side must be a literal"});
        }
        auto value = lower_literal(*literal);
        if (!value.has_value()) {
            return std::unexpected(value.error());
        }
        return ir::FilterPredicate{
            .column = ir::ColumnRef{.name = ident->name},
            .op = to_compare_op(binary->op),
            .value = std::move(value.value()),
        };
    }

    auto lower_project(const SelectClause& clause) -> std::expected<ir::NodePtr, LowerError> {
        std::vector<ir::ColumnRef> columns;
        for (const auto& field : clause.fields) {
            if (field.expr != nullptr) {
                return std::unexpected(LowerError{.message = "select computed fields not supported yet"});
            }
            columns.push_back(ir::ColumnRef{.name = field.name});
        }
        return builder_.project(std::move(columns));
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
            return std::unexpected(LowerError{.message = "unsupported literal in expression"});
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
                return std::unexpected(LowerError{.message = "unsupported binary operator in expression"});
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

    auto lower_aggregate(const ByClause& by, const SelectClause& select)
        -> std::expected<ir::NodePtr, LowerError> {
        auto group_by = lower_group_by(by);
        if (!group_by.has_value()) {
            return std::unexpected(group_by.error());
        }
        std::vector<ir::AggSpec> aggs;
        for (const auto& field : select.fields) {
            if (field.expr == nullptr) {
                continue;
            }
            const auto* call = std::get_if<CallExpr>(&field.expr->node);
            if (call == nullptr) {
                return std::unexpected(LowerError{.message = "aggregate field must be a function call"});
            }
            auto func = parse_agg_func(call->callee);
            if (!func.has_value()) {
                return std::unexpected(LowerError{.message = "unknown aggregate function: " + call->callee});
            }
            if (call->callee == "count") {
                if (!call->args.empty()) {
                    return std::unexpected(LowerError{.message = "count() takes no arguments"});
                }
                aggs.push_back(ir::AggSpec{
                    .func = func.value(),
                    .column = ir::ColumnRef{.name = ""},
                    .alias = field.name,
                });
                continue;
            }
            if (call->args.size() != 1) {
                return std::unexpected(LowerError{.message = "aggregate functions take one argument"});
            }
            const auto* ident = std::get_if<IdentifierExpr>(&call->args[0]->node);
            if (ident == nullptr) {
                return std::unexpected(LowerError{.message = "aggregate argument must be a column"});
            }
            aggs.push_back(ir::AggSpec{
                .func = func.value(),
                .column = ir::ColumnRef{.name = ident->name},
                .alias = field.name,
            });
        }
        return builder_.aggregate(std::move(group_by.value()), std::move(aggs));
    }

    auto lower_group_by(const ByClause& by) -> std::expected<std::vector<ir::ColumnRef>, LowerError> {
        std::vector<ir::ColumnRef> group_by;
        for (const auto& field : by.keys) {
            if (field.expr != nullptr) {
                return std::unexpected(LowerError{.message = "computed group keys not supported yet"});
            }
            group_by.push_back(ir::ColumnRef{.name = field.name});
        }
        return group_by;
    }

    auto lower_literal(const LiteralExpr& literal)
        -> std::expected<std::variant<std::int64_t, double, std::string>, LowerError> {
        if (const auto* int_value = std::get_if<std::int64_t>(&literal.value)) {
            return *int_value;
        }
        if (const auto* double_value = std::get_if<double>(&literal.value)) {
            return *double_value;
        }
        if (const auto* str_value = std::get_if<std::string>(&literal.value)) {
            return *str_value;
        }
        return std::unexpected(LowerError{.message = "literal type not supported in filter"});
    }

    auto parse_duration(std::string_view text) -> std::expected<ir::Duration, LowerError> {
        if (text.size() < 2) {
            return std::unexpected(LowerError{.message = "invalid duration literal"});
        }
        std::size_t unit_pos = 0;
        while (unit_pos < text.size() && std::isdigit(static_cast<unsigned char>(text[unit_pos])) != 0) {
            unit_pos += 1;
        }
        if (unit_pos == 0 || unit_pos == text.size()) {
            return std::unexpected(LowerError{.message = "invalid duration literal"});
        }
        auto number_part = text.substr(0, unit_pos);
        auto unit_part = text.substr(unit_pos);
        std::uint64_t value = 0;
        auto result = std::from_chars(number_part.data(), number_part.data() + number_part.size(), value);
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

    auto parse_agg_func(std::string_view name) -> std::optional<ir::AggFunc> {
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

    auto clone_node(const ir::Node& node) -> ir::NodePtr {
        switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = static_cast<const ir::ScanNode&>(node);
            auto clone = builder_.scan(scan.source_name());
            return clone;
        }
        case ir::NodeKind::Filter: {
            const auto& filter = static_cast<const ir::FilterNode&>(node);
            auto clone = builder_.filter(filter.predicate());
            return clone;
        }
        case ir::NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            auto clone = builder_.project(project.columns());
            return clone;
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
        }
        return nullptr;
    }

    ir::Builder builder_;
    std::unordered_map<std::string, ir::NodePtr>* bindings_ = nullptr;
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
