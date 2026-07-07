#include <ibex/ir/builder.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/optimizer.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/effects.hpp>
#include <ibex/parser/lower.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <functional>
#include <memory>
#include <robin_hood.h>

namespace ibex::parser {

namespace {

struct LoweredAggList {
    std::vector<ir::AggSpec> aggs;
    std::vector<ir::FieldSpec> preagg_updates;
};

struct CompileTimeValue {
    std::variant<std::int64_t, bool, std::string> value;
};

auto clone_expr(const Expr& expr) -> ExprPtr;

auto column_type_name(ir::ColumnType type) -> std::string_view {
    switch (type) {
        case ir::ColumnType::Int32:
            return "Int32";
        case ir::ColumnType::Int64:
            return "Int64";
        case ir::ColumnType::Float32:
            return "Float32";
        case ir::ColumnType::Float64:
            return "Float64";
        case ir::ColumnType::Bool:
            return "Bool";
        case ir::ColumnType::String:
            return "String";
        case ir::ColumnType::Date:
            return "Date";
        case ir::ColumnType::Timestamp:
            return "Timestamp";
    }
    return "?";
}

auto to_ir_schema_fields(const SchemaType& schema) -> std::vector<ir::SchemaField>;

/// Build an `ir::SourceSchemas` env from extern declarations: each extern whose
/// return type is a DataFrame/TimeFrame carrying a schema contributes its
/// declared output schema, keyed by the extern name. Used so a typed reader call
/// (`read_typed(...)`) lowers to an ExternCall with a statically Known schema.
auto build_source_schemas(const robin_hood::unordered_map<std::string, const ExternDecl*>& decls)
    -> ir::SourceSchemas {
    ir::SourceSchemas sources;
    for (const auto& [name, decl] : decls) {
        const auto& ret = decl->return_type;
        if (ret.kind != Type::Kind::DataFrame && ret.kind != Type::Kind::TimeFrame) {
            continue;
        }
        const auto* schema = std::get_if<SchemaType>(&ret.arg);
        if (schema == nullptr || (schema->fields.empty() && !schema->open)) {
            continue;  // bare DataFrame carries no schema -> leave Unknown
        }
        sources.emplace(name, ir::SchemaInfo::known(to_ir_schema_fields(*schema), schema->open));
    }
    return sources;
}

/// Map a parser scalar type to the IR column-type used by schemas.
auto to_ir_column_type(ScalarType type) -> ir::ColumnType {
    switch (type) {
        case ScalarType::Int32:
            return ir::ColumnType::Int32;
        case ScalarType::Int64:
            return ir::ColumnType::Int64;
        case ScalarType::Float32:
            return ir::ColumnType::Float32;
        case ScalarType::Float64:
            return ir::ColumnType::Float64;
        case ScalarType::Bool:
            return ir::ColumnType::Bool;
        case ScalarType::String:
            return ir::ColumnType::String;
        case ScalarType::Date:
            return ir::ColumnType::Date;
        case ScalarType::Timestamp:
            return ir::ColumnType::Timestamp;
    }
    return ir::ColumnType::Int64;
}

auto to_ir_schema_fields(const SchemaType& schema) -> std::vector<ir::SchemaField> {
    std::vector<ir::SchemaField> fields;
    fields.reserve(schema.fields.size());
    for (const auto& field : schema.fields) {
        fields.push_back(
            ir::SchemaField{.name = field.name, .type = to_ir_column_type(field.type)});
    }
    return fields;
}

auto clone_field(const Field& field) -> Field {
    return Field{
        .name = field.name,
        .expr = field.expr ? clone_expr(*field.expr) : nullptr,
    };
}

auto clone_tuple_field(const TupleField& field) -> TupleField {
    return TupleField{
        .names = field.names,
        .expr = field.expr ? clone_expr(*field.expr) : nullptr,
    };
}

auto clone_map_field(const MapField& field) -> MapField {
    return MapField{
        .bindings = field.bindings,
        .where_expr = field.where_expr ? clone_expr(*field.where_expr) : nullptr,
        .alias_template = field.alias_template,
        .expr = field.expr ? clone_expr(*field.expr) : nullptr,
    };
}

auto clone_clause(const Clause& clause) -> Clause {
    return std::visit(
        [](const auto& c) -> Clause {
            using T = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<T, FilterClause>) {
                return FilterClause{.predicate = clone_expr(*c.predicate)};
            } else if constexpr (std::is_same_v<T, SelectClause>) {
                SelectClause out;
                out.fields.reserve(c.fields.size());
                for (const auto& f : c.fields) {
                    out.fields.push_back(clone_field(f));
                }
                out.tuple_fields.reserve(c.tuple_fields.size());
                for (const auto& tf : c.tuple_fields) {
                    out.tuple_fields.push_back(clone_tuple_field(tf));
                }
                out.map_fields.reserve(c.map_fields.size());
                for (const auto& mf : c.map_fields) {
                    out.map_fields.push_back(clone_map_field(mf));
                }
                return out;
            } else if constexpr (std::is_same_v<T, DistinctClause>) {
                DistinctClause out;
                out.fields.reserve(c.fields.size());
                for (const auto& f : c.fields) {
                    out.fields.push_back(clone_field(f));
                }
                return out;
            } else if constexpr (std::is_same_v<T, UpdateClause>) {
                UpdateClause out;
                out.fields.reserve(c.fields.size());
                for (const auto& f : c.fields) {
                    out.fields.push_back(clone_field(f));
                }
                out.tuple_fields.reserve(c.tuple_fields.size());
                for (const auto& tf : c.tuple_fields) {
                    out.tuple_fields.push_back(clone_tuple_field(tf));
                }
                out.map_fields.reserve(c.map_fields.size());
                for (const auto& mf : c.map_fields) {
                    out.map_fields.push_back(clone_map_field(mf));
                }
                out.merge_expr = c.merge_expr ? clone_expr(*c.merge_expr) : nullptr;
                return out;
            } else if constexpr (std::is_same_v<T, RenameClause>) {
                RenameClause out;
                out.fields.reserve(c.fields.size());
                for (const auto& f : c.fields) {
                    out.fields.push_back(clone_field(f));
                }
                return out;
            } else if constexpr (std::is_same_v<T, HeadClause>) {
                return HeadClause{.count = clone_expr(*c.count)};
            } else if constexpr (std::is_same_v<T, TailClause>) {
                return TailClause{.count = clone_expr(*c.count)};
            } else if constexpr (std::is_same_v<T, ByClause>) {
                ByClause out{.keys = {}, .is_braced = c.is_braced};
                out.keys.reserve(c.keys.size());
                for (const auto& f : c.keys) {
                    out.keys.push_back(clone_field(f));
                }
                return out;
            } else if constexpr (std::is_same_v<T, MeltClause>) {
                MeltClause out;
                out.id_fields.reserve(c.id_fields.size());
                for (const auto& f : c.id_fields) {
                    out.id_fields.push_back(clone_field(f));
                }
                return out;
            } else if constexpr (std::is_same_v<T, ModelClause>) {
                ModelClause out{.formula = c.formula, .params = {}};
                out.params.reserve(c.params.size());
                for (const auto& p : c.params) {
                    out.params.push_back(ModelParam{.name = p.name, .value = clone_expr(*p.value)});
                }
                return out;
            } else {
                return Clause{c};
            }
        },
        clause);
}

auto clone_expr(const Expr& expr) -> ExprPtr {
    return std::visit(
        [](const auto& node) -> ExprPtr {
            using T = std::decay_t<decltype(node)>;
            auto out = std::make_unique<Expr>();
            if constexpr (std::is_same_v<T, IdentifierExpr> || std::is_same_v<T, LiteralExpr>) {
                out->node = node;
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                CallExpr call{.callee = node.callee, .args = {}, .named_args = {}};
                call.args.reserve(node.args.size());
                for (const auto& arg : node.args) {
                    call.args.push_back(clone_expr(*arg));
                }
                call.named_args.reserve(node.named_args.size());
                for (const auto& named : node.named_args) {
                    call.named_args.push_back(
                        NamedArg{.name = named.name, .value = clone_expr(*named.value)});
                }
                out->node = std::move(call);
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                RankExpr rank{
                    .order_keys = node.order_keys,
                    .explicit_order = node.explicit_order,
                    .named_args = {},
                };
                rank.named_args.reserve(node.named_args.size());
                for (const auto& named : node.named_args) {
                    rank.named_args.push_back(
                        NamedArg{.name = named.name, .value = clone_expr(*named.value)});
                }
                out->node = std::move(rank);
            } else if constexpr (std::is_same_v<T, UnaryExpr>) {
                out->node = UnaryExpr{.op = node.op, .expr = clone_expr(*node.expr)};
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                out->node = BinaryExpr{
                    .op = node.op,
                    .left = clone_expr(*node.left),
                    .right = clone_expr(*node.right),
                };
            } else if constexpr (std::is_same_v<T, GroupExpr>) {
                out->node = GroupExpr{.expr = clone_expr(*node.expr)};
            } else if constexpr (std::is_same_v<T, BlockExpr>) {
                BlockExpr block{.base = clone_expr(*node.base), .clauses = {}};
                block.clauses.reserve(node.clauses.size());
                for (const auto& clause : node.clauses) {
                    block.clauses.push_back(clone_clause(clause));
                }
                out->node = std::move(block);
            } else if constexpr (std::is_same_v<T, JoinExpr>) {
                JoinExpr join{.kind = node.kind,
                              .left = clone_expr(*node.left),
                              .right = clone_expr(*node.right),
                              .keys = node.keys,
                              .predicate = {}};
                if (node.predicate.has_value()) {
                    join.predicate = clone_expr(**node.predicate);
                }
                out->node = std::move(join);
            } else if constexpr (std::is_same_v<T, StreamExpr>) {
                StreamExpr stream{.source = clone_expr(*node.source),
                                  .transform = {},
                                  .sink_callee = node.sink_callee,
                                  .sink_args = {}};
                stream.transform.reserve(node.transform.size());
                for (const auto& clause : node.transform) {
                    stream.transform.push_back(clone_clause(clause));
                }
                stream.sink_args.reserve(node.sink_args.size());
                for (const auto& arg : node.sink_args) {
                    stream.sink_args.push_back(clone_expr(*arg));
                }
                out->node = std::move(stream);
            } else if constexpr (std::is_same_v<T, ArrayLiteralExpr>) {
                ArrayLiteralExpr arr;
                arr.elements.reserve(node.elements.size());
                for (const auto& elem : node.elements) {
                    arr.elements.push_back(clone_expr(*elem));
                }
                out->node = std::move(arr);
            } else if constexpr (std::is_same_v<T, AscribeExpr>) {
                out->node = AscribeExpr{.base = clone_expr(*node.base), .type = node.type};
            } else {
                TableExpr table;
                table.columns.reserve(node.columns.size());
                for (const auto& col : node.columns) {
                    table.columns.push_back(
                        TableColumnDef{.name = col.name, .expr = clone_expr(*col.expr)});
                }
                out->node = std::move(table);
            }
            return out;
        },
        expr.node);
}

/// Clone `expr` while substituting any IdentifierExpr whose name appears in
/// `subs` with a clone of the bound replacement. Used to inline an aggregate
/// UDF body's parameter references with the call's argument expressions.
auto substitute_params(const Expr& expr,
                       const robin_hood::unordered_map<std::string, const Expr*>& subs) -> ExprPtr {
    return std::visit(
        [&](const auto& node) -> ExprPtr {
            using T = std::decay_t<decltype(node)>;
            auto out = std::make_unique<Expr>();
            if constexpr (std::is_same_v<T, IdentifierExpr>) {
                if (auto it = subs.find(node.name); it != subs.end()) {
                    return clone_expr(*it->second);
                }
                out->node = node;
            } else if constexpr (std::is_same_v<T, LiteralExpr>) {
                out->node = node;
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                CallExpr call{.callee = node.callee, .args = {}, .named_args = {}};
                call.args.reserve(node.args.size());
                for (const auto& arg : node.args) {
                    call.args.push_back(substitute_params(*arg, subs));
                }
                call.named_args.reserve(node.named_args.size());
                for (const auto& named : node.named_args) {
                    call.named_args.push_back(NamedArg{
                        .name = named.name, .value = substitute_params(*named.value, subs)});
                }
                out->node = std::move(call);
            } else if constexpr (std::is_same_v<T, UnaryExpr>) {
                out->node = UnaryExpr{.op = node.op, .expr = substitute_params(*node.expr, subs)};
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                out->node = BinaryExpr{
                    .op = node.op,
                    .left = substitute_params(*node.left, subs),
                    .right = substitute_params(*node.right, subs),
                };
            } else if constexpr (std::is_same_v<T, GroupExpr>) {
                out->node = GroupExpr{.expr = substitute_params(*node.expr, subs)};
            } else {
                // Other expression shapes don't appear inside scalar/aggregate
                // UDF bodies in the supported single-expression form; fall back
                // to a plain deep clone to keep the substitution total.
                return clone_expr(expr);
            }
            return out;
        },
        expr.node);
}

auto extract_string_list(const Expr& expr) -> std::optional<std::vector<std::string>> {
    const auto* array = std::get_if<ArrayLiteralExpr>(&expr.node);
    if (array == nullptr) {
        return std::nullopt;
    }
    std::vector<std::string> values;
    values.reserve(array->elements.size());
    for (const auto& elem : array->elements) {
        const auto* lit = std::get_if<LiteralExpr>(&elem->node);
        if (lit == nullptr) {
            return std::nullopt;
        }
        const auto* text = std::get_if<std::string>(&lit->value);
        if (text == nullptr) {
            return std::nullopt;
        }
        values.push_back(*text);
    }
    return values;
}

auto infer_output_column_names(const ir::Node& node) -> std::optional<std::vector<std::string>> {
    using ir::NodeKind;

    // NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
    switch (node.kind()) {
        case NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            std::vector<std::string> names;
            names.reserve(project.columns().size());
            for (const auto& col : project.columns()) {
                names.push_back(col.name);
            }
            return names;
        }
        case NodeKind::Aggregate: {
            const auto& agg = static_cast<const ir::AggregateNode&>(node);
            std::vector<std::string> names;
            names.reserve(agg.group_by().size() + agg.aggregations().size());
            for (const auto& col : agg.group_by()) {
                names.push_back(col.name);
            }
            for (const auto& spec : agg.aggregations()) {
                names.push_back(spec.alias);
            }
            return names;
        }
        case NodeKind::Update: {
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            if (update.children().empty()) {
                return std::nullopt;
            }
            auto names = infer_output_column_names(*update.children().front());
            if (!names.has_value()) {
                return std::nullopt;
            }
            if (!update.tuple_fields().empty()) {
                return std::nullopt;
            }
            for (const auto& field : update.fields()) {
                auto it = std::find(names->begin(), names->end(), field.alias);
                if (it == names->end()) {
                    names->push_back(field.alias);
                } else {
                    *it = field.alias;
                }
            }
            return names;
        }
        case NodeKind::Rename: {
            const auto& rename = static_cast<const ir::RenameNode&>(node);
            if (rename.children().empty()) {
                return std::nullopt;
            }
            auto names = infer_output_column_names(*rename.children().front());
            if (!names.has_value()) {
                return std::nullopt;
            }
            for (const auto& spec : rename.renames()) {
                auto it = std::find(names->begin(), names->end(), spec.old_name);
                if (it == names->end()) {
                    return std::nullopt;
                }
                *it = spec.new_name;
            }
            return names;
        }
        case NodeKind::FilterProject: {
            const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
            std::vector<std::string> names;
            names.reserve(fp.columns().size());
            for (const auto& col : fp.columns()) {
                names.push_back(col.name);
            }
            return names;
        }
        case NodeKind::FilterUpdateProject: {
            const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
            std::vector<std::string> names;
            names.reserve(fup.project_columns().size());
            for (const auto& col : fup.project_columns()) {
                names.push_back(col.name);
            }
            return names;
        }
        case NodeKind::Resample: {
            const auto& rs = static_cast<const ir::ResampleNode&>(node);
            std::vector<std::string> names;
            names.reserve(rs.group_by().size() + rs.aggregations().size() + 1);
            names.push_back("_bucket");
            for (const auto& col : rs.group_by()) {
                names.push_back(col.name);
            }
            for (const auto& spec : rs.aggregations()) {
                names.push_back(spec.alias);
            }
            return names;
        }
        case NodeKind::AsTimeframe:
        case NodeKind::Ascribe:
        case NodeKind::Filter:
        case NodeKind::Distinct:
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
        case NodeKind::FilterHead:
        case NodeKind::FilterTail:
        case NodeKind::TopK:
        case NodeKind::Rbind:
        case NodeKind::Window: {
            if (node.children().empty()) {
                return std::nullopt;
            }
            return infer_output_column_names(*node.children().front());
        }
        case NodeKind::Columns:
            return std::vector<std::string>{"name"};
        case NodeKind::Construct: {
            const auto& cn = static_cast<const ir::ConstructNode&>(node);
            std::vector<std::string> names;
            names.reserve(cn.columns().size());
            for (const auto& col : cn.columns()) {
                names.push_back(col.name);
            }
            return names;
        }
        case NodeKind::Program: {
            const auto& program = static_cast<const ir::ProgramNode&>(node);
            return infer_output_column_names(program.main_node());
        }
        case NodeKind::Scan:
        case NodeKind::ExternCall:
        case NodeKind::Join:
        case NodeKind::Melt:
        case NodeKind::Dcast:
        case NodeKind::Stream:
        case NodeKind::Cov:
        case NodeKind::Corr:
        case NodeKind::Transpose:
        case NodeKind::Matmul:
        case NodeKind::Model:
            return std::nullopt;
    }
    // NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast
    return std::nullopt;
}

auto compile_value_as_bool(const CompileTimeValue& value) -> std::optional<bool> {
    if (const auto* b = std::get_if<bool>(&value.value)) {
        return *b;
    }
    return std::nullopt;
}

auto compile_value_as_int(const CompileTimeValue& value) -> std::optional<std::int64_t> {
    if (const auto* i = std::get_if<std::int64_t>(&value.value)) {
        return *i;
    }
    return std::nullopt;
}

auto compile_values_equal(const CompileTimeValue& lhs, const CompileTimeValue& rhs) -> bool {
    return lhs.value == rhs.value;
}

auto eval_compile_expr(const Expr& expr,
                       const robin_hood::unordered_map<std::string, CompileTimeValue>& env)
    -> std::expected<CompileTimeValue, LowerError> {
    return std::visit(
        [&](const auto& node) -> std::expected<CompileTimeValue, LowerError> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, IdentifierExpr>) {
                auto it = env.find(node.name);
                if (it == env.end()) {
                    return std::unexpected(
                        LowerError{.message = "unknown compile-time map variable: " + node.name});
                }
                return it->second;
            } else if constexpr (std::is_same_v<T, LiteralExpr>) {
                if (const auto* i = std::get_if<std::int64_t>(&node.value)) {
                    return CompileTimeValue{.value = *i};
                }
                if (const auto* b = std::get_if<bool>(&node.value)) {
                    return CompileTimeValue{.value = *b};
                }
                if (const auto* s = std::get_if<std::string>(&node.value)) {
                    return CompileTimeValue{.value = *s};
                }
                return std::unexpected(
                    LowerError{.message = "unsupported literal in compile-time map expression"});
            } else if constexpr (std::is_same_v<T, GroupExpr>) {
                return eval_compile_expr(*node.expr, env);
            } else if constexpr (std::is_same_v<T, UnaryExpr>) {
                auto inner = eval_compile_expr(*node.expr, env);
                if (!inner.has_value()) {
                    return inner;
                }
                if (node.op == UnaryOp::Not) {
                    auto b = compile_value_as_bool(*inner);
                    if (!b.has_value()) {
                        return std::unexpected(LowerError{
                            .message = "compile-time map 'where' expects boolean expression"});
                    }
                    return CompileTimeValue{.value = !*b};
                }
                if (node.op == UnaryOp::Negate) {
                    auto i = compile_value_as_int(*inner);
                    if (!i.has_value()) {
                        return std::unexpected(LowerError{
                            .message = "compile-time map arithmetic expects integer operands"});
                    }
                    return CompileTimeValue{.value = -*i};
                }
                return std::unexpected(
                    LowerError{.message = "unsupported unary operator in compile-time map where"});
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                auto left = eval_compile_expr(*node.left, env);
                if (!left.has_value()) {
                    return left;
                }
                auto right = eval_compile_expr(*node.right, env);
                if (!right.has_value()) {
                    return right;
                }
                switch (node.op) {
                    case BinaryOp::And: {
                        auto lb = compile_value_as_bool(*left);
                        auto rb = compile_value_as_bool(*right);
                        if (!lb.has_value() || !rb.has_value()) {
                            return std::unexpected(LowerError{
                                .message = "compile-time map 'where' expects boolean operands"});
                        }
                        return CompileTimeValue{.value = *lb && *rb};
                    }
                    case BinaryOp::Or: {
                        auto lb = compile_value_as_bool(*left);
                        auto rb = compile_value_as_bool(*right);
                        if (!lb.has_value() || !rb.has_value()) {
                            return std::unexpected(LowerError{
                                .message = "compile-time map 'where' expects boolean operands"});
                        }
                        return CompileTimeValue{.value = *lb || *rb};
                    }
                    case BinaryOp::Eq:
                        return CompileTimeValue{
                            .value = compile_values_equal(*left, *right),
                        };
                    case BinaryOp::Ne:
                        return CompileTimeValue{
                            .value = !compile_values_equal(*left, *right),
                        };
                    case BinaryOp::Lt:
                    case BinaryOp::Le:
                    case BinaryOp::Gt:
                    case BinaryOp::Ge:
                    case BinaryOp::Add:
                    case BinaryOp::Sub: {
                        auto li = compile_value_as_int(*left);
                        auto ri = compile_value_as_int(*right);
                        if (!li.has_value() || !ri.has_value()) {
                            return std::unexpected(LowerError{
                                .message = "compile-time map arithmetic expects integer operands"});
                        }
                        switch (node.op) {
                            case BinaryOp::Lt:
                                return CompileTimeValue{.value = *li < *ri};
                            case BinaryOp::Le:
                                return CompileTimeValue{.value = *li <= *ri};
                            case BinaryOp::Gt:
                                return CompileTimeValue{.value = *li > *ri};
                            case BinaryOp::Ge:
                                return CompileTimeValue{.value = *li >= *ri};
                            case BinaryOp::Add:
                                return CompileTimeValue{.value = *li + *ri};
                            case BinaryOp::Sub:
                                return CompileTimeValue{.value = *li - *ri};
                            default:
                                break;
                        }
                        break;
                    }
                    default:
                        break;
                }
                return std::unexpected(
                    LowerError{.message = "unsupported operator in compile-time map where"});
            } else {
                return std::unexpected(
                    LowerError{.message = "unsupported expression in compile-time map where"});
            }
        },
        expr.node);
}

auto render_alias_template(const std::string& alias_template,
                           const robin_hood::unordered_map<std::string, CompileTimeValue>& env)
    -> std::expected<std::string, LowerError> {
    std::string rendered;
    rendered.reserve(alias_template.size());
    for (std::size_t i = 0; i < alias_template.size(); ++i) {
        if (alias_template[i] == '$' && i + 1 < alias_template.size() &&
            alias_template[i + 1] == '{') {
            std::size_t end = alias_template.find('}', i + 2);
            if (end == std::string::npos) {
                return std::unexpected(
                    LowerError{.message = "unterminated ${...} in map field alias"});
            }
            std::string key = alias_template.substr(i + 2, end - (i + 2));
            auto it = env.find(key);
            if (it == env.end()) {
                return std::unexpected(
                    LowerError{.message = "unknown compile-time map variable in alias: " + key});
            }
            if (const auto* s = std::get_if<std::string>(&it->second.value)) {
                rendered += *s;
            } else if (const auto* n = std::get_if<std::int64_t>(&it->second.value)) {
                rendered += std::to_string(*n);
            } else if (const auto* b = std::get_if<bool>(&it->second.value)) {
                rendered += *b ? "true" : "false";
            }
            i = end;
            continue;
        }
        rendered.push_back(alias_template[i]);
    }
    return rendered;
}

auto substitute_map_expr(const Expr& expr,
                         const robin_hood::unordered_map<std::string, CompileTimeValue>& env)
    -> std::expected<ExprPtr, LowerError> {
    return std::visit(
        [&](const auto& node) -> std::expected<ExprPtr, LowerError> {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, IdentifierExpr> || std::is_same_v<T, LiteralExpr>) {
                auto out = std::make_unique<Expr>();
                out->node = node;
                return out;
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                if (node.callee == "get" && node.args.size() == 1 && node.named_args.empty()) {
                    if (const auto* ident = std::get_if<IdentifierExpr>(&node.args[0]->node)) {
                        auto it = env.find(ident->name);
                        if (it == env.end()) {
                            return std::unexpected(LowerError{
                                .message =
                                    "unknown compile-time map variable in get(): " + ident->name});
                        }
                        const auto* name = std::get_if<std::string>(&it->second.value);
                        if (name == nullptr) {
                            return std::unexpected(LowerError{
                                .message = "get() expects a compile-time string variable"});
                        }
                        auto out = std::make_unique<Expr>();
                        out->node = IdentifierExpr{.name = *name};
                        return out;
                    }
                    if (const auto* lit = std::get_if<LiteralExpr>(&node.args[0]->node)) {
                        if (const auto* name = std::get_if<std::string>(&lit->value)) {
                            auto out = std::make_unique<Expr>();
                            out->node = IdentifierExpr{.name = *name};
                            return out;
                        }
                    }
                    return std::unexpected(LowerError{
                        .message =
                            "get() inside compile-time map expects an identifier or string"});
                }
                CallExpr call{.callee = node.callee, .args = {}, .named_args = {}};
                call.args.reserve(node.args.size());
                for (const auto& arg : node.args) {
                    auto lowered = substitute_map_expr(*arg, env);
                    if (!lowered.has_value()) {
                        return lowered;
                    }
                    call.args.push_back(std::move(*lowered));
                }
                call.named_args.reserve(node.named_args.size());
                for (const auto& named : node.named_args) {
                    auto lowered = substitute_map_expr(*named.value, env);
                    if (!lowered.has_value()) {
                        return lowered;
                    }
                    call.named_args.push_back(
                        NamedArg{.name = named.name, .value = std::move(*lowered)});
                }
                auto out = std::make_unique<Expr>();
                out->node = std::move(call);
                return out;
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                RankExpr rank{
                    .order_keys = node.order_keys,
                    .explicit_order = node.explicit_order,
                    .named_args = {},
                };
                rank.named_args.reserve(node.named_args.size());
                for (const auto& named : node.named_args) {
                    auto lowered = substitute_map_expr(*named.value, env);
                    if (!lowered.has_value()) {
                        return lowered;
                    }
                    rank.named_args.push_back(
                        NamedArg{.name = named.name, .value = std::move(*lowered)});
                }
                auto out = std::make_unique<Expr>();
                out->node = std::move(rank);
                return out;
            } else if constexpr (std::is_same_v<T, UnaryExpr>) {
                auto inner = substitute_map_expr(*node.expr, env);
                if (!inner.has_value()) {
                    return inner;
                }
                auto out = std::make_unique<Expr>();
                out->node = UnaryExpr{.op = node.op, .expr = std::move(*inner)};
                return out;
            } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                auto left = substitute_map_expr(*node.left, env);
                if (!left.has_value()) {
                    return left;
                }
                auto right = substitute_map_expr(*node.right, env);
                if (!right.has_value()) {
                    return right;
                }
                auto out = std::make_unique<Expr>();
                out->node = BinaryExpr{
                    .op = node.op,
                    .left = std::move(*left),
                    .right = std::move(*right),
                };
                return out;
            } else if constexpr (std::is_same_v<T, GroupExpr>) {
                auto inner = substitute_map_expr(*node.expr, env);
                if (!inner.has_value()) {
                    return inner;
                }
                auto out = std::make_unique<Expr>();
                out->node = GroupExpr{.expr = std::move(*inner)};
                return out;
            } else {
                auto cloned = clone_expr(expr);
                return cloned;
            }
        },
        expr.node);
}

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

    const auto append = [&](const robin_hood::unordered_map<std::string, CallableSummary>& src) {
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
    explicit Lowerer(
        robin_hood::unordered_map<std::string, ir::NodePtr>* bindings,
        robin_hood::unordered_map<std::string, std::vector<std::string>>
            initial_compile_time_lists = {},
        robin_hood::unordered_set<std::string> initial_table_externs = {},
        robin_hood::unordered_set<std::string> initial_sink_externs = {},
        robin_hood::unordered_map<std::string, const ExternDecl*> initial_table_extern_decls = {},
        ir::SourceSchemas initial_source_schemas = {},
        robin_hood::unordered_map<std::string, const FunctionDecl*> initial_functions = {})
        : bindings_(bindings),
          compile_time_lists_(std::move(initial_compile_time_lists)),
          table_externs_(std::move(initial_table_externs)),
          sink_externs_(std::move(initial_sink_externs)),
          table_extern_decls_(std::move(initial_table_extern_decls)),
          binding_schemas_(std::move(initial_source_schemas)),
          functions_(std::move(initial_functions)) {}

    [[nodiscard]] auto table_extern_decls() const
        -> const robin_hood::unordered_map<std::string, const ExternDecl*>& {
        return table_extern_decls_;
    }

    /// The source-schema env for static checks: declared reader return schemas
    /// overlaid with in-scope table-binding schemas (bindings shadow readers).
    [[nodiscard]] auto source_schemas() const -> ir::SourceSchemas {
        ir::SourceSchemas sources = build_source_schemas(table_extern_decls_);
        for (const auto& [name, schema] : binding_schemas_) {
            sources.insert_or_assign(name, schema);
        }
        return sources;
    }

    auto lower_program(const Program& program) -> LowerResult {
        ir::NodePtr last_expr;
        std::vector<ir::NodePtr> preamble_calls;
        const auto infer_compile_time_list =
            [this](const Expr& expr) -> std::optional<std::vector<std::string>> {
            if (auto string_list = extract_string_list(expr); string_list.has_value()) {
                return string_list;
            }
            const auto* call = std::get_if<CallExpr>(&expr.node);
            if (call == nullptr || call->callee != "columns" || call->args.size() != 1 ||
                !call->named_args.empty()) {
                return std::nullopt;
            }
            auto lowered = lower_expr(*call->args.front());
            if (!lowered.has_value()) {
                return std::nullopt;
            }
            return infer_output_column_names(*lowered.value());
        };
        for (const auto& stmt : program.statements) {
            if (const auto* ext = std::get_if<ExternDecl>(&stmt)) {
                // Track externs that return a table so lower_table_call can
                // produce ExternCallNodes for them.
                if (ext->return_type.kind == Type::Kind::DataFrame ||
                    ext->return_type.kind == Type::Kind::TimeFrame) {
                    table_externs_.insert(ext->name);
                    table_extern_decls_.insert_or_assign(ext->name, ext);
                }
                // Track externs whose first argument is a DataFrame — these are sink candidates
                // (e.g. write_csv, udp_send).  lower_stream uses this to validate sink calls.
                if (!ext->params.empty() && ext->params[0].type.kind == Type::Kind::DataFrame) {
                    sink_externs_.insert(ext->name);
                }
                continue;
            }
            if (const auto* fn = std::get_if<FunctionDecl>(&stmt)) {
                functions_.insert_or_assign(fn->name, fn);
                continue;
            }
            if (std::holds_alternative<ImportDecl>(stmt)) {
                // Import declarations are resolved by the REPL before lowering;
                // they have no IR representation.
                continue;
            }
            if (std::holds_alternative<LetStmt>(stmt)) {
                const auto& let_stmt = std::get<LetStmt>(stmt);
                if (auto compile_time_list = infer_compile_time_list(*let_stmt.value);
                    compile_time_list.has_value()) {
                    compile_time_lists_[let_stmt.name] = std::move(*compile_time_list);
                } else {
                    compile_time_lists_.erase(let_stmt.name);
                }
                auto value = lower_expr(*let_stmt.value);
                if (!value.has_value()) {
                    // Scalar let bindings are handled by the REPL/tooling layer;
                    // the IR lowerer only needs to accept them so later table
                    // expressions can still be lowered.
                    auto scalar = lower_expr_to_ir(*let_stmt.value);
                    if (!scalar.has_value()) {
                        if (infer_compile_time_list(*let_stmt.value).has_value()) {
                            continue;
                        }
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
        if (const auto* stream = std::get_if<StreamExpr>(&expr.node)) {
            return lower_stream(*stream);
        }
        if (const auto* tbl = std::get_if<TableExpr>(&expr.node)) {
            return lower_table_expr(*tbl);
        }
        if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
            return lower_expr(*group->expr);
        }
        if (const auto* ascribe = std::get_if<AscribeExpr>(&expr.node)) {
            return lower_ascribe(*ascribe);
        }
        return std::unexpected(LowerError{.message = "expected DataFrame expression"});
    }

    /// Lower `base as DataFrame<{...}>` into an AscribeNode over the lowered base,
    /// converting the declared schema fields to the IR column-type representation.
    auto lower_ascribe(const AscribeExpr& ascribe) -> LowerResult {
        auto base = lower_expr(*ascribe.base);
        if (!base.has_value()) {
            return base;
        }
        const auto* schema_type = std::get_if<SchemaType>(&ascribe.type.arg);
        std::vector<ir::SchemaField> fields;
        bool open = false;
        if (schema_type != nullptr) {
            fields = to_ir_schema_fields(*schema_type);
            open = schema_type->open;
        }
        // When the input schema is statically known and closed, an ascription
        // the input provably cannot satisfy is a lower-time error. When the
        // input is Unknown (e.g. an I/O source) or open, the check is deferred to
        // the runtime validation in the interpreter.
        const ir::SchemaInfo input = ir::infer_schema(*base.value(), source_schemas());
        if (input.is_known() && !input.is_open()) {
            for (const auto& field : fields) {
                const auto* have = input.find(field.name);
                if (have == nullptr) {
                    return std::unexpected(LowerError{.message = "schema ascription: column '" +
                                                                 field.name +
                                                                 "' is not present in the input"});
                }
                if (field.type.has_value() && have->type.has_value() &&
                    *have->type != *field.type) {
                    return std::unexpected(
                        LowerError{.message = "schema ascription: column '" + field.name + "' is " +
                                              std::string(column_type_name(*have->type)) +
                                              " but the ascription requires " +
                                              std::string(column_type_name(*field.type))});
                }
            }
            // Exact (non-wildcard) ascription forbids columns the input has that
            // the ascription does not list. Add a `*` wildcard to allow extras.
            if (!open) {
                for (const auto& have : input.fields()) {
                    const bool listed =
                        std::any_of(fields.begin(), fields.end(),
                                    [&](const ir::SchemaField& f) { return f.name == have.name; });
                    if (!listed) {
                        return std::unexpected(LowerError{
                            .message = "schema ascription: input has extra column '" + have.name +
                                       "' not in the ascribed schema (add `*` to allow extras)"});
                    }
                }
            }
        }
        auto node = builder_.ascribe(std::move(fields), open);
        node->add_child(std::move(base.value()));
        return node;
    }

    /// Lower a `Table { col = expr, ... }` expression into a ConstructNode.
    ///
    /// Each column value may be:
    ///   - An array literal `[v, ...]` — lowered inline to a vector of ir::Literal.
    ///   - Any other expression — lowered recursively to a child IR node that is
    ///     expected to produce a single-column Table (or a Table containing a column
    ///     named after the column being defined) at interpret/codegen time.
    auto lower_table_expr(const TableExpr& tbl) -> LowerResult {
        // `Table(n)` form: an empty frame with `n` rows and no columns.
        if (tbl.row_count) {
            auto count = lower_expr_to_ir(*tbl.row_count);
            if (!count.has_value()) {
                return std::unexpected(count.error());
            }
            return builder_.construct_rows(std::move(*count));
        }

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
                        if constexpr (std::is_same_v<T, std::int64_t> ||
                                      std::is_same_v<T, double> || std::is_same_v<T, bool> ||
                                      std::is_same_v<T, std::string> || std::is_same_v<T, Date> ||
                                      std::is_same_v<T, Timestamp>) {
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
        if (call.callee == "rbind") {
            if (!call.named_args.empty()) {
                return std::unexpected(
                    LowerError{.message = "rbind does not accept named arguments"});
            }
            if (call.args.size() < 2) {
                return std::unexpected(
                    LowerError{.message = "rbind expects at least 2 table arguments"});
            }
            auto node = builder_.rbind();
            for (const auto& arg : call.args) {
                auto operand = lower_expr(*arg);
                if (!operand.has_value()) {
                    return operand;
                }
                node->add_child(std::move(operand.value()));
            }
            return node;
        }
        if (call.callee == "columns") {
            if (call.args.size() != 1 || !call.named_args.empty()) {
                return std::unexpected(LowerError{.message = "columns expects exactly 1 argument"});
            }
            auto base = lower_expr(*call.args[0]);
            if (!base.has_value()) {
                return base;
            }
            auto node = builder_.columns();
            node->add_child(std::move(base.value()));
            return node;
        }
        if (!table_externs_.contains(call.callee)) {
            return std::unexpected(LowerError{.message = "unknown table function: " + call.callee});
        }
        auto bound_args = bind_extern_call_args(call);
        if (!bound_args.has_value()) {
            return std::unexpected(std::move(bound_args.error()));
        }
        std::vector<ir::Expr> args;
        args.reserve(bound_args->size());
        for (const auto* arg : *bound_args) {
            auto expr = lower_expr_to_ir(*arg);
            if (!expr.has_value()) {
                return std::unexpected(expr.error());
            }
            args.push_back(std::move(expr.value()));
        }
        return builder_.extern_call(call.callee, std::move(args));
    }

    auto bind_extern_call_args(const CallExpr& call)
        -> std::expected<std::vector<const Expr*>, LowerError> {
        const auto decl_it = table_extern_decls_.find(call.callee);
        if (decl_it == table_extern_decls_.end()) {
            if (!call.named_args.empty()) {
                return std::unexpected(LowerError{
                    .message = call.callee + ": named arguments require an extern declaration"});
            }
            std::vector<const Expr*> positional;
            positional.reserve(call.args.size());
            for (const auto& arg : call.args) {
                positional.push_back(arg.get());
            }
            return positional;
        }

        const auto& params = decl_it->second->params;
        std::vector<const Expr*> bound(params.size(), nullptr);
        robin_hood::unordered_map<std::string, std::size_t> param_index;
        param_index.reserve(params.size());
        for (std::size_t i = 0; i < params.size(); ++i) {
            param_index.emplace(params[i].name, i);
        }

        if (call.args.size() > params.size()) {
            return std::unexpected(
                LowerError{.message = call.callee + ": too many positional arguments"});
        }
        for (std::size_t i = 0; i < call.args.size(); ++i) {
            bound[i] = call.args[i].get();
        }
        for (const auto& named_arg : call.named_args) {
            const auto it = param_index.find(named_arg.name);
            if (it == param_index.end()) {
                return std::unexpected(LowerError{
                    .message = call.callee + ": unknown named argument '" + named_arg.name + "'"});
            }
            auto& slot = bound[it->second];
            if (slot != nullptr) {
                return std::unexpected(
                    LowerError{.message = call.callee + ": duplicate argument for parameter '" +
                                          params[it->second].name + "'"});
            }
            slot = named_arg.value.get();
        }
        for (std::size_t i = 0; i < params.size(); ++i) {
            if (bound[i] != nullptr) {
                continue;
            }
            if (params[i].default_value != nullptr) {
                bound[i] = params[i].default_value.get();
                continue;
            }
            return std::unexpected(LowerError{
                .message = call.callee + ": missing required argument '" + params[i].name + "'"});
        }
        return bound;
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
        std::optional<ir::Expr> predicate;

        if (join.predicate.has_value()) {
            const Expr& on_expr = **join.predicate;
            if (const auto* ident = std::get_if<IdentifierExpr>(&on_expr.node)) {
                // Bare identifier → equikey (backward compat: `A join B on key`)
                keys.push_back(ident->name);
            } else {
                // General expression → non-equijoin predicate
                auto pred = lower_expr_to_ir(on_expr);
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

    auto expand_map_fields(const std::vector<Field>& base_fields,
                           const std::vector<MapField>& map_fields)
        -> std::expected<std::vector<Field>, LowerError> {
        std::vector<Field> expanded;
        expanded.reserve(base_fields.size() + map_fields.size());
        for (const auto& field : base_fields) {
            expanded.push_back(clone_field(field));
        }
        for (const auto& map_field : map_fields) {
            auto generated = expand_one_map_field(map_field);
            if (!generated.has_value()) {
                return std::unexpected(generated.error());
            }
            for (auto& field : *generated) {
                expanded.push_back(std::move(field));
            }
        }
        return expanded;
    }

    auto expand_one_map_field(const MapField& map_field)
        -> std::expected<std::vector<Field>, LowerError> {
        std::vector<Field> expanded;
        robin_hood::unordered_map<std::string, CompileTimeValue> env;
        auto step = [&](auto&& self, std::size_t binding_idx) -> std::expected<void, LowerError> {
            if (binding_idx == map_field.bindings.size()) {
                if (map_field.where_expr) {
                    auto where_value = eval_compile_expr(*map_field.where_expr, env);
                    if (!where_value.has_value()) {
                        return std::unexpected(where_value.error());
                    }
                    auto keep = compile_value_as_bool(*where_value);
                    if (!keep.has_value()) {
                        return std::unexpected(LowerError{
                            .message = "compile-time map where-clause must evaluate to Bool"});
                    }
                    if (!*keep) {
                        return {};
                    }
                }
                auto alias = render_alias_template(map_field.alias_template, env);
                if (!alias.has_value()) {
                    return std::unexpected(alias.error());
                }
                auto expr = substitute_map_expr(*map_field.expr, env);
                if (!expr.has_value()) {
                    return std::unexpected(expr.error());
                }
                expanded.push_back(Field{.name = std::move(*alias), .expr = std::move(*expr)});
                return {};
            }

            const auto& binding = map_field.bindings[binding_idx];
            auto it = compile_time_lists_.find(binding.source_name);
            if (it == compile_time_lists_.end()) {
                return std::unexpected(LowerError{
                    .message = "unknown compile-time string list in map: " + binding.source_name});
            }
            for (std::size_t idx = 0; idx < it->second.size(); ++idx) {
                env[binding.value_name] = CompileTimeValue{.value = it->second[idx]};
                if (binding.index_name.has_value()) {
                    env[*binding.index_name] =
                        CompileTimeValue{.value = static_cast<std::int64_t>(idx)};
                }
                auto next = self(self, binding_idx + 1);
                if (!next.has_value()) {
                    return next;
                }
            }
            return {};
        };

        auto status = step(step, 0);
        if (!status.has_value()) {
            return std::unexpected(status.error());
        }
        return expanded;
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
        if (state.head && state.tail) {
            return std::unexpected(LowerError{.message = "head and tail are mutually exclusive"});
        }
        if (state.distinct && state.by) {
            return std::unexpected(LowerError{.message = "distinct cannot be used with by"});
        }
        if (state.by && !state.select && !state.update && !state.head && !state.tail &&
            !state.resample && !state.dcast) {
            return std::unexpected(
                LowerError{.message = "by requires select, update, head, or tail"});
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
        if (state.model && (state.select || state.update || state.by || state.distinct ||
                            state.melt || state.dcast || state.window || state.resample ||
                            state.rename || state.cov || state.corr || state.transpose)) {
            return std::unexpected(LowerError{
                .message = "model is mutually exclusive with select, update, by, distinct, "
                           "melt, dcast, window, resample, rename, cov, corr, and transpose"});
        }

        auto node = std::move(base.value());

        if (state.filter) {
            auto predicate = lower_expr_to_ir(*state.filter->predicate);
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

        std::vector<Field> expanded_select_fields;
        if (state.select) {
            auto expanded = expand_map_fields(state.select->fields, state.select->map_fields);
            if (!expanded.has_value()) {
                return std::unexpected(expanded.error());
            }
            expanded_select_fields = std::move(*expanded);
        }

        if (!state.resample && !state.melt && !state.dcast && state.select &&
            select_has_aggregate(expanded_select_fields)) {
            auto aggregate = lower_aggregate(state.by, expanded_select_fields, std::move(node));
            if (!aggregate.has_value()) {
                return std::unexpected(aggregate.error());
            }
            node = std::move(aggregate.value());
        } else if (!state.resample && !state.melt && !state.dcast && state.select) {
            auto project = lower_select_projection(expanded_select_fields,
                                                   state.select->tuple_fields, std::move(node));
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
            auto expanded = expand_map_fields(state.update->fields, state.update->map_fields);
            if (!expanded.has_value()) {
                return std::unexpected(expanded.error());
            }
            auto update = lower_update(state.by, *expanded, state.update->tuple_fields,
                                       state.update->merge_expr);
            if (!update.has_value()) {
                return std::unexpected(update.error());
            }
            if (state.update->guard) {
                auto guard = lower_expr_to_ir(*state.update->guard);
                if (!guard.has_value()) {
                    return std::unexpected(guard.error());
                }
                update.value()->set_guard(std::move(guard.value()));
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
                auto keys = lower_group_by_bare_only(*state.by, "resample");
                if (!keys.has_value())
                    return std::unexpected(keys.error());
                extra_group_by = std::move(keys.value());
            }
            auto lowered_resample = lower_resample_aggs(*state.select);
            if (!lowered_resample.has_value())
                return std::unexpected(lowered_resample.error());
            auto lowered = std::move(lowered_resample.value());
            if (!lowered.preagg_updates.empty()) {
                auto preagg = builder_.update(std::move(lowered.preagg_updates));
                preagg->add_child(std::move(node));
                node = std::move(preagg);
            }
            auto resample_node = builder_.resample(duration.value(), std::move(extra_group_by),
                                                   std::move(lowered.aggs));
            resample_node->add_child(std::move(node));
            node = std::move(resample_node);
        }

        if (state.melt) {
            std::vector<std::string> id_cols;
            id_cols.reserve(state.melt->id_fields.size());
            for (const auto& field : state.melt->id_fields) {
                id_cols.push_back(field.name);
            }
            std::vector<std::string> measure_cols;
            if (state.select) {
                measure_cols.reserve(state.select->fields.size());
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
            row_keys.reserve(state.by->keys.size());
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

        if (state.head) {
            std::vector<ir::ColumnRef> head_group_by;
            if (state.by != nullptr) {
                auto group_by_result = lower_group_by_bare_only(*state.by, "head");
                if (!group_by_result.has_value()) {
                    return std::unexpected(group_by_result.error());
                }
                head_group_by = std::move(group_by_result.value());
            }
            auto count_expr = lower_expr_to_ir(*state.head->count);
            if (!count_expr.has_value()) {
                return std::unexpected(count_expr.error());
            }
            auto head_node = builder_.head(std::move(count_expr.value()), std::move(head_group_by));
            head_node->add_child(std::move(node));
            node = std::move(head_node);
        }

        if (state.tail) {
            std::vector<ir::ColumnRef> tail_group_by;
            if (state.by != nullptr) {
                auto group_by_result = lower_group_by_bare_only(*state.by, "tail");
                if (!group_by_result.has_value()) {
                    return std::unexpected(group_by_result.error());
                }
                tail_group_by = std::move(group_by_result.value());
            }
            auto count_expr = lower_expr_to_ir(*state.tail->count);
            if (!count_expr.has_value()) {
                return std::unexpected(count_expr.error());
            }
            auto tail_node = builder_.tail(std::move(count_expr.value()), std::move(tail_group_by));
            tail_node->add_child(std::move(node));
            node = std::move(tail_node);
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
        const HeadClause* head = nullptr;
        const TailClause* tail = nullptr;
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
            if (std::holds_alternative<HeadClause>(clause)) {
                if (head != nullptr) {
                    error = "duplicate head clause";
                    return false;
                }
                head = &std::get<HeadClause>(clause);
                return true;
            }
            if (std::holds_alternative<TailClause>(clause)) {
                if (tail != nullptr) {
                    error = "duplicate tail clause";
                    return false;
                }
                tail = &std::get<TailClause>(clause);
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

    auto lower_update(const ByClause* by, const std::vector<Field>& clause_fields,
                      const std::vector<TupleField>& tuple_fields, const ExprPtr& merge_expr)
        -> std::expected<std::unique_ptr<ir::UpdateNode>, LowerError> {
        // `update = expr`: merge all columns of the result table.
        if (merge_expr) {
            auto src = lower_expr(*merge_expr);
            if (!src.has_value()) {
                return std::unexpected(src.error());
            }
            std::vector<ir::TupleFieldSpec> tuple_specs;
            tuple_specs.push_back(
                ir::TupleFieldSpec{.aliases = {}, .source = std::move(src.value())});
            return builder_.update({}, std::move(tuple_specs));
        }
        std::vector<ir::FieldSpec> fields;
        for (const auto& field : clause_fields) {
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
        for (const auto& tf : tuple_fields) {
            auto src = lower_expr(*tf.expr);
            if (!src.has_value()) {
                return std::unexpected(src.error());
            }
            tuple_specs.push_back(
                ir::TupleFieldSpec{.aliases = tf.names, .source = std::move(src.value())});
        }
        std::vector<ir::ColumnRef> group_by;
        if (by != nullptr) {
            auto keys = lower_group_by_bare_only(*by, "update");
            if (!keys.has_value()) {
                return std::unexpected(keys.error());
            }
            group_by = std::move(keys.value());
        }
        return builder_.update(std::move(fields), std::move(tuple_specs), std::move(group_by));
    }

    auto lower_model(const ModelClause& clause)
        -> std::expected<std::tuple<ir::ModelFormula, std::string, std::vector<ir::ModelParamSpec>>,
                         LowerError> {
        // Convert AST formula → IR formula.
        ir::ModelFormula formula;
        formula.response = clause.formula.response;
        formula.has_intercept = clause.formula.has_intercept;
        for (const auto& term : clause.formula.terms) {
            formula.terms.push_back(ir::ModelTerm{.columns = term.columns, .is_dot = term.is_dot});
        }

        // Extract method name; default to OLS when omitted.
        std::string method = "ols";
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

    /// Shape recognised by the UDF inliner: zero or more `let` bindings
    /// followed by a single trailing expression. Returned when the body
    /// matches; nullopt for anything else (multi-statement, tuple destructure,
    /// expression-then-let, etc.). Lets are inlined by folding into the
    /// substitution scope so the trailing expression sees `let`-bound names.
    struct InlinableBodyShape {
        std::vector<const LetStmt*> lets;
        const Expr* final_expr{};
    };
    static auto inlinable_body_shape(const FunctionDecl& fn) -> std::optional<InlinableBodyShape> {
        if (fn.body.empty()) {
            return std::nullopt;
        }
        const auto* tail = std::get_if<ExprStmt>(&fn.body.back());
        if (tail == nullptr || tail->expr == nullptr) {
            return std::nullopt;
        }
        InlinableBodyShape out;
        out.lets.reserve(fn.body.size() - 1);
        for (std::size_t i = 0; i + 1 < fn.body.size(); ++i) {
            const auto* lp = std::get_if<LetStmt>(&fn.body[i]);
            if (lp == nullptr || lp->value == nullptr) {
                return std::nullopt;
            }
            out.lets.push_back(lp);
        }
        out.final_expr = tail->expr.get();
        return out;
    }

    /// Inline an aggregate UDF call at the AST level: returns a clone of the
    /// function body with each parameter IdentifierExpr substituted by the
    /// matching argument expression. The caller re-lowers the result inside
    /// `lower_agg_expr`, so the body's built-in aggregate calls reduce to
    /// regular `AggSpec`s through the same machinery as inline aggregates.
    /// Parameter binding mirrors `inline_scalar_udf` (positional, then named,
    /// then defaults). The recursion guard is managed by the caller so this
    /// helper has no side effects on `inlining_active_`.
    auto inline_agg_udf_body(const CallExpr& call) -> std::expected<ExprPtr, LowerError> {
        auto it = functions_.find(call.callee);
        const FunctionDecl& fn = *it->second;
        auto body_shape = inlinable_body_shape(fn);
        if (!body_shape.has_value()) {
            return std::unexpected(
                LowerError{.message = "aggregate function '" + fn.name +
                                      "' cannot be inlined: only single-expression or "
                                      "let-prefixed bodies are inlined"});
        }
        if (call.args.size() > fn.params.size()) {
            return std::unexpected(LowerError{.message = fn.name + ": too many arguments"});
        }
        std::vector<const Expr*> bound(fn.params.size(), nullptr);
        for (std::size_t i = 0; i < call.args.size(); ++i) {
            bound[i] = call.args[i].get();
        }
        for (const auto& narg : call.named_args) {
            const auto param = std::find_if(fn.params.begin(), fn.params.end(),
                                            [&](const Param& p) { return p.name == narg.name; });
            if (param == fn.params.end()) {
                return std::unexpected(LowerError{
                    .message = fn.name + ": unknown named argument '" + narg.name + "'"});
            }
            const auto pos = static_cast<std::size_t>(std::distance(fn.params.begin(), param));
            if (bound[pos] != nullptr) {
                return std::unexpected(LowerError{
                    .message = fn.name + ": duplicate argument for '" + narg.name + "'"});
            }
            bound[pos] = narg.value.get();
        }
        robin_hood::unordered_map<std::string, const Expr*> subs;
        for (std::size_t i = 0; i < fn.params.size(); ++i) {
            const Expr* arg = bound[i] != nullptr ? bound[i] : fn.params[i].default_value.get();
            if (arg == nullptr) {
                return std::unexpected(LowerError{.message = fn.name + ": missing argument '" +
                                                             fn.params[i].name + "'"});
            }
            subs.emplace(fn.params[i].name, arg);
        }
        // Fold let bindings into the substitution map. Each let's rhs is
        // substituted through the current subs (params + earlier lets), and
        // the resulting cloned expression is owned in `let_storage` so the
        // raw pointer in `subs` stays valid through the final substitution.
        std::vector<ExprPtr> let_storage;
        let_storage.reserve(body_shape->lets.size());
        for (const auto* let : body_shape->lets) {
            auto rhs_sub = substitute_params(*let->value, subs);
            let_storage.push_back(std::move(rhs_sub));
            subs.insert_or_assign(let->name, let_storage.back().get());
        }
        return substitute_params(*body_shape->final_expr, subs);
    }

    /// Inline a call to a scalar user function into a clause expression: bind
    /// its parameters to the (lowered) argument expressions and lower its body
    /// with those substitutions in scope. Only single-expression bodies inline;
    /// recursion is rejected.
    auto inline_scalar_udf(const FunctionDecl& fn, const CallExpr& call)
        -> std::expected<ir::Expr, LowerError> {
        if (fn.return_type.kind != Type::Kind::Scalar) {
            return std::unexpected(LowerError{
                .message = "function '" + fn.name +
                           "' does not return a scalar and cannot be used in this expression"});
        }
        // Accept bodies of the shape: zero or more `let` bindings followed by a
        // single trailing expression. Other shapes (multiple expressions, tuple
        // destructuring, control flow) are not yet inlinable.
        auto body_shape = inlinable_body_shape(fn);
        if (!body_shape.has_value()) {
            return std::unexpected(LowerError{
                .message = "scalar function '" + fn.name +
                           "' cannot be used in a clause expression: only single-expression "
                           "or let-prefixed bodies are inlined"});
        }
        if (call.args.size() > fn.params.size()) {
            return std::unexpected(LowerError{.message = fn.name + ": too many arguments"});
        }
        // Bind arguments: positional, then named, then defaults.
        std::vector<const Expr*> bound(fn.params.size(), nullptr);
        for (std::size_t i = 0; i < call.args.size(); ++i) {
            bound[i] = call.args[i].get();
        }
        for (const auto& narg : call.named_args) {
            const auto param = std::find_if(fn.params.begin(), fn.params.end(),
                                            [&](const Param& p) { return p.name == narg.name; });
            if (param == fn.params.end()) {
                return std::unexpected(LowerError{
                    .message = fn.name + ": unknown named argument '" + narg.name + "'"});
            }
            const auto pos = static_cast<std::size_t>(std::distance(fn.params.begin(), param));
            if (bound[pos] != nullptr) {
                return std::unexpected(LowerError{
                    .message = fn.name + ": duplicate argument for '" + narg.name + "'"});
            }
            bound[pos] = narg.value.get();
        }
        robin_hood::unordered_map<std::string, ir::Expr> scope;
        for (std::size_t i = 0; i < fn.params.size(); ++i) {
            const Expr* arg = bound[i] != nullptr ? bound[i] : fn.params[i].default_value.get();
            if (arg == nullptr) {
                return std::unexpected(LowerError{.message = fn.name + ": missing argument '" +
                                                             fn.params[i].name + "'"});
            }
            auto lowered = lower_expr_to_ir(*arg);  // lowered in the current scope
            if (!lowered.has_value()) {
                return std::unexpected(lowered.error());
            }
            scope.insert_or_assign(fn.params[i].name, std::move(lowered.value()));
        }
        if (!inlining_active_.insert(fn.name).second) {
            return std::unexpected(LowerError{.message = "recursive scalar function '" + fn.name +
                                                         "' cannot be inlined in a clause "
                                                         "expression"});
        }
        inline_scopes_.push_back(std::move(scope));
        // Fold any `let x = rhs;` bindings into the inline scope before
        // lowering the trailing expression. Each `rhs` is lowered against the
        // scope built up so far, so later lets and the body see the bindings.
        for (const auto* let : body_shape->lets) {
            auto lowered_rhs = lower_expr_to_ir(*let->value);
            if (!lowered_rhs.has_value()) {
                inline_scopes_.pop_back();
                inlining_active_.erase(fn.name);
                return std::unexpected(lowered_rhs.error());
            }
            inline_scopes_.back().insert_or_assign(let->name, std::move(lowered_rhs.value()));
        }
        auto result = lower_expr_to_ir(*body_shape->final_expr);
        inline_scopes_.pop_back();
        inlining_active_.erase(fn.name);
        return result;
    }

    auto lower_expr_to_ir(const Expr& expr) -> std::expected<ir::Expr, LowerError> {
        if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
            // Inside an inlined scalar UDF body, a parameter name resolves to the
            // (already-lowered) argument expression rather than a column.
            if (!inline_scopes_.empty()) {
                const auto& scope = inline_scopes_.back();
                if (auto it = scope.find(ident->name); it != scope.end()) {
                    return it->second;
                }
            }
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
            // A call to a scalar user function is inlined: its body replaces the
            // call, with parameters substituted by the argument expressions.
            if (auto it = functions_.find(call->callee); it != functions_.end()) {
                return inline_scalar_udf(*it->second, *call);
            }
            // Function-call form for null checks: `is_null(col)` and
            // `is_not_null(col)` are sugar for the postfix `col is null` /
            // `col is not null` and lower to the same IsNullExpr node.
            if (call->callee == "is_null" || call->callee == "is_not_null") {
                if (call->args.size() != 1 || !call->named_args.empty()) {
                    return std::unexpected(LowerError{
                        .message = call->callee + " expects exactly one positional argument"});
                }
                auto operand = lower_expr_to_ir(*call->args.front());
                if (!operand.has_value()) {
                    return std::unexpected(operand.error());
                }
                return ir::Expr{
                    .node = ir::IsNullExpr{.operand = ir::make_expr_ptr(std::move(operand.value())),
                                           .negated = call->callee == "is_not_null"}};
            }
            // rep([e0, e1, ...], ...) — array-literal first arg. Expand the
            // elements as positional literal args and mark with __array_len so
            // the runtime knows they form the pattern to cycle over.
            if (call->callee == "rep" && !call->args.empty()) {
                if (const auto* arr = std::get_if<ArrayLiteralExpr>(&call->args.front()->node)) {
                    if (arr->elements.empty()) {
                        return std::unexpected(
                            LowerError{.message = "rep: array literal must not be empty"});
                    }
                    ir::CallExpr lowered_rep;
                    lowered_rep.callee = "rep";
                    lowered_rep.args.reserve(arr->elements.size());
                    for (const auto& elem : arr->elements) {
                        auto lowered_elem = lower_expr_to_ir(*elem);
                        if (!lowered_elem.has_value()) {
                            return std::unexpected(lowered_elem.error());
                        }
                        if (!std::holds_alternative<ir::Literal>(lowered_elem->node)) {
                            return std::unexpected(LowerError{
                                .message = "rep: array literal elements must be literals"});
                        }
                        lowered_rep.args.push_back(
                            ir::make_expr_ptr(std::move(lowered_elem.value())));
                    }
                    // sentinel: the positional args are array elements, not one scalar
                    const auto array_len = static_cast<std::int64_t>(arr->elements.size());
                    lowered_rep.named_args.push_back(ir::NamedArg{
                        .name = "__array_len",
                        .value =
                            ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = array_len}})});
                    // pass through length_out / times / each named args unchanged
                    for (const auto& narg : call->named_args) {
                        auto lowered_val = lower_expr_to_ir(*narg.value);
                        if (!lowered_val.has_value()) {
                            return std::unexpected(lowered_val.error());
                        }
                        lowered_rep.named_args.push_back(ir::NamedArg{
                            .name = narg.name,
                            .value = ir::make_expr_ptr(std::move(lowered_val.value()))});
                    }
                    return ir::Expr{.node = std::move(lowered_rep)};
                }
            }
            // Rolling aggregates accept an optional trailing per-call window arg:
            //   rolling_mean(px, 20)  -> last-20-rows count window
            //   rolling_mean(px, 60s) -> 60-second duration window
            // The IR Literal has no duration alternative, so we peel the window
            // arg off the positionals and re-attach it as a sentinel named arg
            // (__window_n / __window_ns), mirroring rep's __array_len. This keeps
            // the positional args (col, plus alpha/p) exactly as before.
            if (ir::is_rolling_func(call->callee)) {
                std::size_t base = 1;  // most rolling funcs take just `col`
                if (call->callee == "rolling_ewma" || call->callee == "rolling_quantile") {
                    base = 2;  // col + alpha/p precede the window
                } else if (call->callee == "rolling_count") {
                    base = 0;  // takes no column
                }
                std::optional<ir::NamedArg> window_named;
                std::size_t n_positional = call->args.size();
                if (call->args.size() > base) {
                    if (const auto* lit = std::get_if<LiteralExpr>(&call->args.back()->node)) {
                        if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                            window_named = ir::NamedArg{
                                .name = "__window_n",
                                .value =
                                    ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = *iv}})};
                            n_positional = call->args.size() - 1;
                        } else if (const auto* dl = std::get_if<DurationLiteral>(&lit->value)) {
                            auto dur = parse_duration(dl->text);
                            if (!dur.has_value()) {
                                return std::unexpected(dur.error());
                            }
                            window_named = ir::NamedArg{
                                .name = "__window_ns",
                                .value = ir::make_expr_ptr(
                                    ir::Expr{.node = ir::Literal{.value = dur->count()}})};
                            n_positional = call->args.size() - 1;
                        }
                    }
                }
                ir::CallExpr rolling;
                rolling.callee = call->callee;
                rolling.args.reserve(n_positional);
                for (std::size_t i = 0; i < n_positional; ++i) {
                    auto la = lower_expr_to_ir(*call->args[i]);
                    if (!la.has_value()) {
                        return std::unexpected(la.error());
                    }
                    rolling.args.push_back(ir::make_expr_ptr(std::move(la.value())));
                }
                if (window_named.has_value()) {
                    rolling.named_args.push_back(std::move(*window_named));
                }
                for (const auto& narg : call->named_args) {
                    auto lv = lower_expr_to_ir(*narg.value);
                    if (!lv.has_value()) {
                        return std::unexpected(lv.error());
                    }
                    rolling.named_args.push_back(ir::NamedArg{
                        .name = narg.name, .value = ir::make_expr_ptr(std::move(lv.value()))});
                }
                return ir::Expr{.node = std::move(rolling)};
            }
            ir::CallExpr lowered_call;
            lowered_call.callee = call->callee;
            lowered_call.args.reserve(call->args.size());
            for (const auto& arg : call->args) {
                auto lowered_arg = lower_expr_to_ir(*arg);
                if (!lowered_arg.has_value()) {
                    return std::unexpected(lowered_arg.error());
                }
                lowered_call.args.push_back(ir::make_expr_ptr(std::move(lowered_arg.value())));
            }
            lowered_call.named_args.reserve(call->named_args.size());
            for (const auto& narg : call->named_args) {
                auto lowered_val = lower_expr_to_ir(*narg.value);
                if (!lowered_val.has_value()) {
                    return std::unexpected(lowered_val.error());
                }
                lowered_call.named_args.push_back(ir::NamedArg{
                    .name = narg.name, .value = ir::make_expr_ptr(std::move(lowered_val.value()))});
            }
            return ir::Expr{.node = std::move(lowered_call)};
        }
        if (const auto* rank = std::get_if<RankExpr>(&expr.node)) {
            ir::RankExpr lowered_rank;
            lowered_rank.order_keys.reserve(rank->order_keys.size());
            for (const auto& key : rank->order_keys) {
                lowered_rank.order_keys.push_back(
                    ir::OrderKey{.name = key.name, .ascending = key.ascending});
            }
            robin_hood::unordered_set<std::string> seen_named;
            for (const auto& narg : rank->named_args) {
                if (!seen_named.insert(narg.name).second) {
                    return std::unexpected(LowerError{
                        .message = "rank(): duplicate named argument '" + narg.name + "'"});
                }
                if (narg.name == "method") {
                    std::optional<std::string> value_name;
                    if (const auto* ident = std::get_if<IdentifierExpr>(&narg.value->node)) {
                        value_name = ident->name;
                    } else if (const auto* lit = std::get_if<LiteralExpr>(&narg.value->node)) {
                        if (const auto* text = std::get_if<std::string>(&lit->value)) {
                            value_name = *text;
                        }
                    }
                    if (!value_name.has_value()) {
                        return std::unexpected(LowerError{
                            .message = "rank(): method must be a bare identifier or string"});
                    }
                    if (*value_name == "average") {
                        lowered_rank.method = ir::RankMethod::Average;
                    } else if (*value_name == "min") {
                        lowered_rank.method = ir::RankMethod::Min;
                    } else if (*value_name == "max") {
                        lowered_rank.method = ir::RankMethod::Max;
                    } else if (*value_name == "first") {
                        lowered_rank.method = ir::RankMethod::First;
                    } else if (*value_name == "dense") {
                        lowered_rank.method = ir::RankMethod::Dense;
                    } else {
                        return std::unexpected(
                            LowerError{.message = "rank(): unknown method '" + *value_name + "'"});
                    }
                } else if (narg.name == "ascending") {
                    if (rank->explicit_order) {
                        return std::unexpected(LowerError{
                            .message =
                                "rank(order {...}): ascending must be expressed per order key"});
                    }
                    bool ascending = true;
                    if (const auto* lit = std::get_if<LiteralExpr>(&narg.value->node)) {
                        if (const auto* flag = std::get_if<bool>(&lit->value)) {
                            ascending = *flag;
                        } else {
                            return std::unexpected(
                                LowerError{.message = "rank(): ascending must be Bool"});
                        }
                    } else if (const auto* ident = std::get_if<IdentifierExpr>(&narg.value->node)) {
                        if (ident->name == "true") {
                            ascending = true;
                        } else if (ident->name == "false") {
                            ascending = false;
                        } else {
                            return std::unexpected(
                                LowerError{.message = "rank(): ascending must be Bool"});
                        }
                    } else {
                        return std::unexpected(
                            LowerError{.message = "rank(): ascending must be Bool"});
                    }
                    for (auto& key : lowered_rank.order_keys) {
                        key.ascending = ascending;
                    }
                } else if (narg.name == "na_option") {
                    std::optional<std::string> value_name;
                    if (const auto* ident = std::get_if<IdentifierExpr>(&narg.value->node)) {
                        value_name = ident->name;
                    } else if (const auto* lit = std::get_if<LiteralExpr>(&narg.value->node)) {
                        if (const auto* text = std::get_if<std::string>(&lit->value)) {
                            value_name = *text;
                        }
                    }
                    if (!value_name.has_value()) {
                        return std::unexpected(LowerError{
                            .message = "rank(): na_option must be a bare identifier or string"});
                    }
                    if (*value_name == "keep") {
                        lowered_rank.na_option = ir::RankNaOption::Keep;
                    } else if (*value_name == "top") {
                        lowered_rank.na_option = ir::RankNaOption::Top;
                    } else if (*value_name == "bottom") {
                        lowered_rank.na_option = ir::RankNaOption::Bottom;
                    } else {
                        return std::unexpected(LowerError{.message = "rank(): unknown na_option '" +
                                                                     *value_name + "'"});
                    }
                } else if (narg.name == "pct") {
                    if (const auto* lit = std::get_if<LiteralExpr>(&narg.value->node)) {
                        if (const auto* flag = std::get_if<bool>(&lit->value)) {
                            lowered_rank.pct = *flag;
                        } else {
                            return std::unexpected(
                                LowerError{.message = "rank(): pct must be Bool"});
                        }
                    } else if (const auto* ident = std::get_if<IdentifierExpr>(&narg.value->node)) {
                        if (ident->name == "true") {
                            lowered_rank.pct = true;
                        } else if (ident->name == "false") {
                            lowered_rank.pct = false;
                        } else {
                            return std::unexpected(
                                LowerError{.message = "rank(): pct must be Bool"});
                        }
                    } else {
                        return std::unexpected(LowerError{.message = "rank(): pct must be Bool"});
                    }
                } else {
                    return std::unexpected(LowerError{
                        .message = "rank(): unknown named argument '" + narg.name + "'"});
                }
            }
            return ir::Expr{.node = std::move(lowered_rank)};
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
            auto lhs = ir::make_expr_ptr(std::move(left.value()));
            auto rhs = ir::make_expr_ptr(std::move(right.value()));
            if (is_compare_op(binary->op)) {
                return ir::Expr{.node = ir::CompareExpr{
                                    .op = to_compare_op(binary->op), .left = lhs, .right = rhs}};
            }
            if (binary->op == BinaryOp::And || binary->op == BinaryOp::Or) {
                return ir::Expr{.node = ir::LogicalExpr{.op = binary->op == BinaryOp::And
                                                                  ? ir::LogicalOp::And
                                                                  : ir::LogicalOp::Or,
                                                        .left = lhs,
                                                        .right = rhs}};
            }
            auto op = to_arithmetic_op(binary->op);
            if (!op.has_value()) {
                return std::unexpected(
                    LowerError{.message = "unsupported binary operator in expression"});
            }
            return ir::Expr{.node = ir::BinaryExpr{.op = op.value(), .left = lhs, .right = rhs}};
        }
        if (const auto* unary = std::get_if<UnaryExpr>(&expr.node)) {
            auto operand = lower_expr_to_ir(*unary->expr);
            if (!operand.has_value()) {
                return std::unexpected(operand.error());
            }
            auto operand_ptr = ir::make_expr_ptr(std::move(operand.value()));
            if (unary->op == UnaryOp::Not) {
                return ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::Not,
                                                        .left = operand_ptr,
                                                        .right = nullptr}};
            }
            if (unary->op == UnaryOp::IsNull || unary->op == UnaryOp::IsNotNull) {
                return ir::Expr{.node = ir::IsNullExpr{.operand = operand_ptr,
                                                       .negated = unary->op == UnaryOp::IsNotNull}};
            }
            if (unary->op == UnaryOp::Negate) {
                // No dedicated IR negate node: lower `-x` as `0 - x`. Negation
                // of a numeric literal is already folded away in the parser, so
                // this path handles columns and compound expressions, where the
                // integer zero promotes to the operand's type as needed.
                auto zero =
                    ir::make_expr_ptr(ir::Expr{.node = ir::Literal{.value = std::int64_t{0}}});
                return ir::Expr{.node = ir::BinaryExpr{.op = ir::ArithmeticOp::Sub,
                                                       .left = std::move(zero),
                                                       .right = operand_ptr}};
            }
            return std::unexpected(
                LowerError{.message = "unsupported unary operator in expression"});
        }
        if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
            return lower_expr_to_ir(*group->expr);
        }
        return std::unexpected(LowerError{.message = "unsupported expression"});
    }

    auto lower_aggregate(const ByClause* by, const std::vector<Field>& select_fields,
                         ir::NodePtr child) -> std::expected<ir::NodePtr, LowerError> {
        std::vector<ir::ColumnRef> group_by;
        std::vector<ir::FieldSpec> preagg_updates;
        if (by != nullptr) {
            auto group_by_result = lower_group_by(*by);
            if (!group_by_result.has_value()) {
                return std::unexpected(group_by_result.error());
            }
            group_by = std::move(group_by_result->keys);
            preagg_updates = std::move(group_by_result->preagg_updates);
        }

        robin_hood::unordered_map<std::string, bool> group_keys;
        for (const auto& key : group_by) {
            group_keys.emplace(key.name, true);
        }

        std::vector<ir::AggSpec> aggs;
        std::vector<ir::FieldSpec> updates;
        std::vector<std::string> final_columns;
        robin_hood::unordered_map<std::string, bool> temp_columns;
        std::size_t temp_counter = 0;

        auto make_temp = [&]() -> std::string {
            return "_agg" + std::to_string(temp_counter++);
        };

        std::function<const IdentifierExpr*(const Expr&)> extract_column_ident;
        extract_column_ident = [&](const Expr& expr) -> const IdentifierExpr* {
            if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
                return ident;
            }
            if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
                return extract_column_ident(*group->expr);
            }
            return nullptr;
        };

        auto expr_contains_aggregate = [this](const Expr& expr) -> bool {
            return expr_contains_builtin_aggregate(expr);
        };

        auto lower_agg_arg = [&](const Expr& expr) -> std::expected<std::string, LowerError> {
            if (const auto* ident = extract_column_ident(expr)) {
                return ident->name;
            }
            if (expr_contains_aggregate(expr)) {
                return std::unexpected(
                    LowerError{.message = "nested aggregate function calls are not allowed"});
            }
            auto lowered = lower_expr_to_ir(expr);
            if (!lowered.has_value()) {
                return std::unexpected(lowered.error());
            }
            std::string alias = make_temp();
            preagg_updates.push_back(ir::FieldSpec{
                .alias = alias,
                .expr = std::move(lowered.value()),
            });
            temp_columns[alias] = true;
            return alias;
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
                    // Aggregate UDF: inline the body with parameters substituted
                    // by the argument expressions, then re-lower in this same
                    // aggregate-expression context. The inlined body's built-in
                    // aggregate calls (sum, mean, ...) are handled by the
                    // recursive descent below.
                    if (const Expr* body = aggregate_udf_body(call->callee)) {
                        if (!inlining_active_.insert(call->callee).second) {
                            return std::unexpected(
                                LowerError{.message = "recursive aggregate function '" +
                                                      call->callee + "' cannot be inlined"});
                        }
                        auto inlined = inline_agg_udf_body(*call);
                        (void)body;
                        if (!inlined.has_value()) {
                            inlining_active_.erase(call->callee);
                            return std::unexpected(inlined.error());
                        }
                        auto result = lower_agg_expr(*inlined.value());
                        inlining_active_.erase(call->callee);
                        return result;
                    }
                    // Generic scalar call wrapping aggregate result(s): lower
                    // each argument in agg-expression context (so any nested
                    // aggregate calls reduce to AggSpec column refs) and emit
                    // the call as a post-aggregate scalar expression in the
                    // trailing update.
                    ir::CallExpr lowered_call;
                    lowered_call.callee = call->callee;
                    lowered_call.args.reserve(call->args.size());
                    for (const auto& arg : call->args) {
                        auto lowered_arg = lower_agg_expr(*arg);
                        if (!lowered_arg.has_value()) {
                            return std::unexpected(lowered_arg.error());
                        }
                        lowered_call.args.push_back(
                            ir::make_expr_ptr(std::move(lowered_arg.value())));
                    }
                    lowered_call.named_args.reserve(call->named_args.size());
                    for (const auto& narg : call->named_args) {
                        auto lowered_val = lower_agg_expr(*narg.value);
                        if (!lowered_val.has_value()) {
                            return std::unexpected(lowered_val.error());
                        }
                        lowered_call.named_args.push_back(ir::NamedArg{
                            .name = narg.name,
                            .value = ir::make_expr_ptr(std::move(lowered_val.value()))});
                    }
                    return ir::Expr{.node = std::move(lowered_call)};
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
                    auto column_name = lower_agg_arg(*call->args[0]);
                    if (!column_name.has_value()) {
                        return std::unexpected(column_name.error());
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
                        .column = ir::ColumnRef{.name = std::move(column_name.value())},
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
                    auto column_name = lower_agg_arg(*call->args[0]);
                    if (!column_name.has_value()) {
                        return std::unexpected(column_name.error());
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
                        .column = ir::ColumnRef{.name = std::move(column_name.value())},
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
                auto column_name = lower_agg_arg(*call->args[0]);
                if (!column_name.has_value()) {
                    return std::unexpected(column_name.error());
                }
                aggs.push_back(ir::AggSpec{
                    .func = func.value(),
                    .column = ir::ColumnRef{.name = std::move(column_name.value())},
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
                    .left = ir::make_expr_ptr(std::move(left.value())),
                    .right = ir::make_expr_ptr(std::move(right.value())),
                };
                return ir::Expr{.node = std::move(bin)};
            }
            if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
                return lower_agg_expr(*group->expr);
            }
            return std::unexpected(LowerError{.message = "unsupported aggregate expression"});
        };

        for (const auto& field : select_fields) {
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
                        auto column_name = lower_agg_arg(*call->args[0]);
                        if (!column_name.has_value()) {
                            return std::unexpected(column_name.error());
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
                            .column = ir::ColumnRef{.name = std::move(column_name.value())},
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
                        auto column_name = lower_agg_arg(*call->args[0]);
                        if (!column_name.has_value()) {
                            return std::unexpected(column_name.error());
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
                            .column = ir::ColumnRef{.name = std::move(column_name.value())},
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
                    auto column_name = lower_agg_arg(*call->args[0]);
                    if (!column_name.has_value()) {
                        return std::unexpected(column_name.error());
                    }
                    aggs.push_back(ir::AggSpec{
                        .func = func.value(),
                        .column = ir::ColumnRef{.name = std::move(column_name.value())},
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

        ir::NodePtr node = std::move(child);
        if (!preagg_updates.empty()) {
            auto preagg = builder_.update(std::move(preagg_updates));
            preagg->add_child(std::move(node));
            node = std::move(preagg);
        }

        auto aggregate = builder_.aggregate(std::move(group_by), std::move(aggs));
        aggregate->add_child(std::move(node));
        node = std::move(aggregate);
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

    /// If `name` is a user function with at least one `Series<T>` parameter
    /// (any other parameters must be scalar), whose return type is a scalar,
    /// whose body is inlinable (zero or more `let` bindings followed by a
    /// single expression), and whose trailing expression (or any `let` rhs)
    /// contains at least one built-in aggregate call, returns the trailing
    /// expression. This is the inference rule that distinguishes an aggregate
    /// UDF from an ordinary scalar UDF; see `plans/aggregate-udf-plan.md`.
    auto aggregate_udf_body(const std::string& name) const -> const Expr* {
        auto it = functions_.find(name);
        if (it == functions_.end()) {
            return nullptr;
        }
        const FunctionDecl& fn = *it->second;
        if (fn.return_type.kind != Type::Kind::Scalar) {
            return nullptr;
        }
        if (fn.params.empty()) {
            return nullptr;
        }
        bool has_series = false;
        for (const auto& p : fn.params) {
            if (p.type.kind == Type::Kind::Series) {
                has_series = true;
            } else if (p.type.kind != Type::Kind::Scalar) {
                return nullptr;
            }
        }
        if (!has_series) {
            return nullptr;
        }
        auto shape = inlinable_body_shape(fn);
        if (!shape.has_value()) {
            return nullptr;
        }
        // The aggregate may live in a `let` rhs (e.g. `let total = sum(p)`)
        // rather than the trailing expression, so check both.
        const bool let_has_agg = std::ranges::any_of(shape->lets, [&](const LetStmt* l) {
            return expr_contains_builtin_aggregate(*l->value);
        });
        if (!let_has_agg && !expr_contains_builtin_aggregate(*shape->final_expr)) {
            return nullptr;
        }
        return shape->final_expr;
    }

    auto expr_contains_builtin_aggregate(const Expr& expr) const -> bool {
        if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
            if (parse_agg_func(call->callee).has_value()) {
                return true;
            }
            // A call to a UDF recognised as an aggregate UDF should propagate
            // "contains aggregate" to its callers (otherwise a body shaped
            // like `let total = sum(p); total;` would not be detected because
            // the trailing expression itself has no aggregate call).
            if (aggregate_udf_body(call->callee) != nullptr) {
                return true;
            }
            return std::ranges::any_of(
                call->args, [&](const auto& arg) { return expr_contains_builtin_aggregate(*arg); });
        }
        if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
            return expr_contains_builtin_aggregate(*binary->left) ||
                   expr_contains_builtin_aggregate(*binary->right);
        }
        if (const auto* unary = std::get_if<UnaryExpr>(&expr.node)) {
            return expr_contains_builtin_aggregate(*unary->expr);
        }
        if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
            return expr_contains_builtin_aggregate(*group->expr);
        }
        return false;
    }

    auto select_has_aggregate(const std::vector<Field>& fields) const -> bool {
        return std::ranges::any_of(fields, [&](const auto& field) {
            return field.expr != nullptr && expr_contains_builtin_aggregate(*field.expr);
        });
    }

    auto lower_resample_aggs(const SelectClause& select)
        -> std::expected<LoweredAggList, LowerError> {
        LoweredAggList lowered;
        std::size_t temp_counter = 0;

        auto make_temp = [&]() -> std::string {
            return "_agg" + std::to_string(temp_counter++);
        };

        std::function<const IdentifierExpr*(const Expr&)> extract_column_ident;
        extract_column_ident = [&](const Expr& expr) -> const IdentifierExpr* {
            if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
                return ident;
            }
            if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
                return extract_column_ident(*group->expr);
            }
            return nullptr;
        };

        auto expr_contains_aggregate = [this](const Expr& expr) -> bool {
            return expr_contains_builtin_aggregate(expr);
        };

        auto lower_agg_arg = [&](const Expr& expr) -> std::expected<std::string, LowerError> {
            if (const auto* ident = extract_column_ident(expr)) {
                return ident->name;
            }
            if (expr_contains_aggregate(expr)) {
                return std::unexpected(
                    LowerError{.message = "nested aggregate function calls are not allowed"});
            }
            auto lowered_expr = lower_expr_to_ir(expr);
            if (!lowered_expr.has_value()) {
                return std::unexpected(lowered_expr.error());
            }
            std::string alias = make_temp();
            lowered.preagg_updates.push_back(ir::FieldSpec{
                .alias = alias,
                .expr = std::move(lowered_expr.value()),
            });
            return alias;
        };

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
                lowered.aggs.push_back(
                    ir::AggSpec{.func = func.value(), .column = {.name = ""}, .alias = field.name});
                continue;
            }
            if (call->callee == "ewma") {
                if (call->args.size() != 2)
                    return std::unexpected(
                        LowerError{.message = "ewma() takes two arguments: ewma(column, alpha)"});
                auto column_name = lower_agg_arg(*call->args[0]);
                if (!column_name.has_value())
                    return std::unexpected(column_name.error());
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
                lowered.aggs.push_back(
                    ir::AggSpec{.func = func.value(),
                                .column = {.name = std::move(column_name.value())},
                                .alias = field.name,
                                .param = alpha});
                continue;
            }
            if (call->callee == "quantile") {
                if (call->args.size() != 2)
                    return std::unexpected(LowerError{
                        .message = "quantile() takes two arguments: quantile(column, p)"});
                auto column_name = lower_agg_arg(*call->args[0]);
                if (!column_name.has_value())
                    return std::unexpected(column_name.error());
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
                lowered.aggs.push_back(
                    ir::AggSpec{.func = func.value(),
                                .column = {.name = std::move(column_name.value())},
                                .alias = field.name,
                                .param = p});
                continue;
            }
            if (call->args.size() != 1)
                return std::unexpected(
                    LowerError{.message = "aggregate functions take one argument"});
            auto column_name = lower_agg_arg(*call->args[0]);
            if (!column_name.has_value())
                return std::unexpected(column_name.error());
            lowered.aggs.push_back(ir::AggSpec{.func = func.value(),
                                               .column = {.name = std::move(column_name.value())},
                                               .alias = field.name});
        }
        return lowered;
    }

    struct LoweredGroupBy {
        std::vector<ir::ColumnRef> keys;
        std::vector<ir::FieldSpec> preagg_updates;
    };

    /// Lower a `by { ... }` clause. Bare keys (`by x`) become `ColumnRef`s.
    /// Computed keys (`by { hour = hour(ts) }`) lower the expression and
    /// emit a `FieldSpec` in `preagg_updates`; the caller is responsible for
    /// materializing those as an `update` node upstream of the aggregate /
    /// grouping operator. Callers that don't yet support pre-update injection
    /// should call `lower_group_by_bare_only` instead.
    auto lower_group_by(const ByClause& by) -> std::expected<LoweredGroupBy, LowerError> {
        LoweredGroupBy out;
        out.keys.reserve(by.keys.size());
        for (const auto& field : by.keys) {
            if (field.expr != nullptr) {
                auto lowered = lower_expr_to_ir(*field.expr);
                if (!lowered.has_value()) {
                    return std::unexpected(lowered.error());
                }
                out.preagg_updates.push_back(
                    ir::FieldSpec{.alias = field.name, .expr = std::move(lowered.value())});
            }
            out.keys.push_back(ir::ColumnRef{.name = field.name});
        }
        return out;
    }

    /// Wrapper for call sites that don't yet support pre-update injection:
    /// rejects computed keys with a workaround hint and returns bare names.
    auto lower_group_by_bare_only(const ByClause& by, std::string_view context)
        -> std::expected<std::vector<ir::ColumnRef>, LowerError> {
        auto lowered = lower_group_by(by);
        if (!lowered.has_value()) {
            return std::unexpected(lowered.error());
        }
        if (!lowered->preagg_updates.empty()) {
            return std::unexpected(
                LowerError{.message = std::string{"computed group keys are not yet supported in "} +
                                      std::string{context} +
                                      "; materialize the key with [update { name = expr }] first"});
        }
        return std::move(lowered->keys);
    }

    static auto lower_literal(const LiteralExpr& literal)
        -> std::expected<std::variant<std::int64_t, double, bool, std::string, Date, Timestamp>,
                         LowerError> {
        if (const auto* int_value = std::get_if<std::int64_t>(&literal.value)) {
            return *int_value;
        }
        if (const auto* double_value = std::get_if<double>(&literal.value)) {
            return *double_value;
        }
        if (const auto* bool_value = std::get_if<bool>(&literal.value)) {
            return *bool_value;
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
        auto result =
            std::from_chars(number_part.data(), number_part.data() + number_part.size(), value);
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
                clone = builder_.filter(filter.predicate());
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
            case ir::NodeKind::Head: {
                const auto& head = static_cast<const ir::HeadNode&>(node);
                clone = builder_.head(head.count_expr(), head.group_by());
                break;
            }
            case ir::NodeKind::Tail: {
                const auto& tail = static_cast<const ir::TailNode&>(node);
                clone = builder_.tail(tail.count_expr(), tail.group_by());
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
            case ir::NodeKind::Ascribe: {
                const auto& asc = static_cast<const ir::AscribeNode&>(node);
                clone = builder_.ascribe(asc.schema());
                break;
            }
            case ir::NodeKind::Columns: {
                clone = builder_.columns();
                break;
            }
            case ir::NodeKind::ExternCall: {
                const auto& ec = static_cast<const ir::ExternCallNode&>(node);
                clone = builder_.extern_call(ec.callee(), ec.args());
                break;
            }
            case ir::NodeKind::Join: {
                const auto& join = static_cast<const ir::JoinNode&>(node);
                std::optional<ir::Expr> pred_clone = join.predicate();
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
            case ir::NodeKind::Rbind: {
                clone = builder_.rbind();
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
            case ir::NodeKind::FilterProject: {
                const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
                clone = builder_.filter_project(fp.predicate(), fp.columns());
                break;
            }
            case ir::NodeKind::FilterUpdateProject: {
                const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
                clone = builder_.filter_update_project(fup.predicate(), fup.fields(),
                                                       fup.project_columns());
                break;
            }
            case ir::NodeKind::FilterHead: {
                const auto& fh = static_cast<const ir::FilterHeadNode&>(node);
                clone = builder_.filter_head(fh.predicate(), fh.count());
                break;
            }
            case ir::NodeKind::FilterTail: {
                const auto& ft = static_cast<const ir::FilterTailNode&>(node);
                clone = builder_.filter_tail(ft.predicate(), ft.count());
                break;
            }
            case ir::NodeKind::TopK: {
                const auto& topk = static_cast<const ir::TopKNode&>(node);
                clone =
                    builder_.top_k(topk.keys(), topk.count(), topk.group_by(), topk.keep_mode());
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
        return std::ranges::any_of(node.children(), [target](const auto& child) {
            return contains_node_kind(*child, target);
        });
    }

    /// Walk an IR tree and return the Duration of the first ResampleNode found.
    static auto find_resample_duration(const ir::Node& node) -> std::optional<ir::Duration> {
        if (node.kind() == ir::NodeKind::Resample) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
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
    auto lower_stream(const StreamExpr& stream) -> LowerResult {
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
        if (src_call->named_args.empty()) {
            source_args.reserve(src_call->args.size());
            for (const auto& arg : src_call->args) {
                auto lowered = lower_expr_to_ir(*arg);
                if (!lowered.has_value()) {
                    return std::unexpected(lowered.error());
                }
                source_args.push_back(std::move(lowered.value()));
            }
        } else {
            auto bound = bind_extern_call_args(*src_call);
            if (!bound.has_value()) {
                return std::unexpected(bound.error());
            }
            source_args.reserve(bound.value().size());
            for (const auto* arg : bound.value()) {
                auto lowered = lower_expr_to_ir(*arg);
                if (!lowered.has_value()) {
                    return std::unexpected(lowered.error());
                }
                source_args.push_back(std::move(lowered.value()));
            }
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
        // Use the two-arg lower_block directly with a synthetic base identifier,
        // avoiding the need to move clauses out of the const StreamExpr.
        Expr base_ident;
        base_ident.node = IdentifierExpr{.name = "__stream_input__"};
        auto transform_ir = lower_block(base_ident, stream.transform);
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
    robin_hood::unordered_map<std::string, ir::NodePtr>* bindings_ = nullptr;
    robin_hood::unordered_map<std::string, std::vector<std::string>> compile_time_lists_;
    robin_hood::unordered_set<std::string> table_externs_;
    robin_hood::unordered_set<std::string> sink_externs_;
    robin_hood::unordered_map<std::string, const ExternDecl*> table_extern_decls_;
    ir::SourceSchemas binding_schemas_;
    robin_hood::unordered_map<std::string, const FunctionDecl*> functions_;
    // Scratch for inlining scalar UDF calls in clause expressions: a stack of
    // parameter substitutions (top = innermost inlined body) and a guard set to
    // reject recursive inlining.
    std::vector<robin_hood::unordered_map<std::string, ir::Expr>> inline_scopes_;
    robin_hood::unordered_set<std::string> inlining_active_;
};

}  // namespace

auto lower(const Program& program) -> LowerResult {
    auto effects = analyze_effects(program);
    if (!effects.has_value()) {
        return std::unexpected(LowerError{.message = effects.error().format()});
    }

    robin_hood::unordered_map<std::string, ir::NodePtr> bindings;
    Lowerer lowerer(&bindings);
    auto lowered = lowerer.lower_program(program);
    if (!lowered.has_value()) {
        return lowered;
    }
    // Validate column references against statically known schemas before the
    // optimizer fuses nodes (the pass understands the un-fused operators).
    if (auto err = ir::check_column_refs(*lowered.value(),
                                         build_source_schemas(lowerer.table_extern_decls()))) {
        return std::unexpected(LowerError{.message = *err});
    }

    const auto optimization_context = build_optimization_context(*effects);
    ir::OptimizationStats optimization_stats;
    auto optimized =
        ir::optimize_plan(std::move(*lowered), optimization_context, &optimization_stats);
    return optimized;
}

auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult {
    Lowerer lowerer(&context.bindings, context.compile_time_lists, context.table_externs,
                    context.sink_externs, context.table_extern_decls, context.source_schemas,
                    context.functions);
    auto lowered = lowerer.lower_expression(expr);
    if (lowered.has_value()) {
        // The REPL supplies the complete set of in-scope lexical names and the
        // schemas of in-scope table bindings, so filter/computed-expression
        // references and references to let-bound tables can be checked too.
        if (auto err = ir::check_column_refs(*lowered.value(), lowerer.source_schemas(),
                                             context.lexical_names,
                                             /*check_expressions=*/true)) {
            return std::unexpected(LowerError{.message = *err});
        }
    }
    return lowered;
}

}  // namespace ibex::parser
