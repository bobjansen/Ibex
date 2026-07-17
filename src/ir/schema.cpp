#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {

auto SchemaInfo::find(std::string_view name) const -> const SchemaField* {
    if (!known_) {
        return nullptr;
    }
    for (const auto& field : fields_) {
        if (field.name == name) {
            return &field;
        }
    }
    return nullptr;
}

namespace {

auto contains_all(const std::vector<std::string>& haystack, const UniqueKey& needles) -> bool {
    return std::ranges::all_of(needles, [&](const std::string& needle) {
        return std::ranges::find(haystack, needle) != haystack.end();
    });
}

}  // namespace

auto SchemaInfo::is_unique_within(const std::vector<std::string>& columns) const -> bool {
    return std::ranges::any_of(unique_keys_,
                               [&](const UniqueKey& key) { return contains_all(columns, key); });
}

void SchemaInfo::add_unique_key(UniqueKey key) {
    if (is_unique_within(key)) {
        return;  // already implied by a key we hold
    }
    std::ranges::sort(key);
    key.erase(std::ranges::unique(key).begin(), key.end());
    unique_keys_.push_back(std::move(key));
}

namespace {

auto literal_type(const Literal& lit) -> ColumnType {
    return std::visit(
        [](const auto& value) -> ColumnType {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return ColumnType::Int64;
            } else if constexpr (std::is_same_v<T, double>) {
                return ColumnType::Float64;
            } else if constexpr (std::is_same_v<T, bool>) {
                return ColumnType::Bool;
            } else if constexpr (std::is_same_v<T, std::string>) {
                return ColumnType::String;
            } else if constexpr (std::is_same_v<T, Date>) {
                return ColumnType::Date;
            } else {
                return ColumnType::Timestamp;
            }
        },
        lit.value);
}

auto is_float(ColumnType t) -> bool {
    return t == ColumnType::Float32 || t == ColumnType::Float64;
}
auto is_int(ColumnType t) -> bool {
    return t == ColumnType::Int32 || t == ColumnType::Int64;
}
auto is_numeric(ColumnType t) -> bool {
    return is_float(t) || is_int(t);
}

/// Target type of a scalar cast call (`Int64(x)`, `Float64(x)`, ...).
auto cast_target(std::string_view callee) -> std::optional<ColumnType> {
    if (callee == "Int" || callee == "Int64") {
        return ColumnType::Int64;
    }
    if (callee == "Int32") {
        return ColumnType::Int32;
    }
    if (callee == "Float64") {
        return ColumnType::Float64;
    }
    if (callee == "Float32") {
        return ColumnType::Float32;
    }
    return std::nullopt;
}

/// Best-effort result type of a computed-field expression given the input
/// schema. Only the cases that are certain are inferred; anything uncertain is
/// nullopt, which keeps the result sound. The runtime `infer_expr_type`
/// (interpreter.cpp) remains the authoritative typing; this mirrors its
/// common, unambiguous cases for the static pass.
auto expr_type(const Expr& expr, const SchemaInfo& input) -> std::optional<ColumnType> {
    if (const auto* col = std::get_if<ColumnRef>(&expr.node)) {
        if (const auto* field = input.find(col->name)) {
            return field->type;
        }
        return std::nullopt;
    }
    if (const auto* lit = std::get_if<Literal>(&expr.node)) {
        return literal_type(*lit);
    }
    if (std::holds_alternative<CompareExpr>(expr.node) ||
        std::holds_alternative<LogicalExpr>(expr.node) ||
        std::holds_alternative<IsNullExpr>(expr.node)) {
        return ColumnType::Bool;  // boolean-valued expressions
    }
    if (const auto* bin = std::get_if<BinaryExpr>(&expr.node)) {
        const auto left = expr_type(*bin->left, input);
        const auto right = expr_type(*bin->right, input);
        if (!left.has_value() || !right.has_value()) {
            return std::nullopt;
        }
        if (!is_numeric(*left) || !is_numeric(*right)) {
            return std::nullopt;  // non-numeric arithmetic is unsupported / uncertain
        }
        if (bin->op == ArithmeticOp::Div) {
            return ColumnType::Float64;
        }
        if (is_float(*left) || is_float(*right)) {
            return ColumnType::Float64;
        }
        return ColumnType::Int64;
    }
    if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
        if (auto target = cast_target(call->callee)) {
            return target;
        }
        // Functions that always return Float64.
        if (call->callee == "sqrt" || call->callee == "log" || call->callee == "exp" ||
            call->callee == "rolling_mean" || call->callee == "rolling_median" ||
            call->callee == "rolling_std" || call->callee == "rolling_ewma" ||
            call->callee == "rolling_quantile" || call->callee == "rolling_skew" ||
            call->callee == "rolling_kurtosis") {
            return ColumnType::Float64;
        }
        if (call->callee == "rolling_count") {
            return ColumnType::Int64;
        }
        if (call->callee == "like" || call->callee == "is_nan") {
            return ColumnType::Bool;
        }
        // `abs` and type-preserving columnar functions return their first
        // argument's type.
        if (call->callee == "abs" || call->callee == "cumsum" || call->callee == "cumprod" ||
            call->callee == "lag" || call->callee == "lead" || call->callee == "rolling_sum" ||
            call->callee == "rolling_min" || call->callee == "rolling_max") {
            if (!call->args.empty()) {
                const auto arg = expr_type(*call->args.front(), input);
                if (arg.has_value() && is_numeric(*arg)) {
                    return arg;
                }
            }
            return std::nullopt;
        }
        return std::nullopt;
    }
    return std::nullopt;
}

/// Output type of an aggregate, when known with certainty.
auto agg_result_type(const AggSpec& agg, const SchemaInfo& input) -> std::optional<ColumnType> {
    switch (agg.func) {
        case AggFunc::Count:
            return ColumnType::Int64;
        case AggFunc::Mean:
        case AggFunc::Median:
        case AggFunc::Stddev:
        case AggFunc::Ewma:
        case AggFunc::Quantile:
        case AggFunc::Skew:
        case AggFunc::Kurtosis:
            // Always produce a Float64 result.
            return ColumnType::Float64;
        case AggFunc::Sum:
        case AggFunc::Min:
        case AggFunc::Max:
        case AggFunc::First:
        case AggFunc::Last:
            // These preserve the input column's type.
            if (const auto* field = input.find(agg.column.name)) {
                return field->type;
            }
            return std::nullopt;
    }
    return std::nullopt;
}

auto child_schema(const Node& node, const SourceSchemas& sources, std::size_t index = 0)
    -> SchemaInfo {
    if (index >= node.children().size()) {
        return SchemaInfo::unknown();
    }
    return infer_schema(*node.children()[index], sources);
}

/// Drop the unique constraints from a schema that is otherwise passed through,
/// for operators whose row multiplicity breaks them.
auto without_unique_keys(SchemaInfo info) -> SchemaInfo {
    if (!info.is_known()) {
        return info;
    }
    return SchemaInfo::known(info.fields(), info.is_open(), info.time_index());
}

/// Carry `input`'s unique constraints into `out` for an operator that only
/// drops or reorders rows, keeping each key whose columns all survive. Dropping
/// rows cannot make a unique tuple repeat, so every surviving key still holds.
void inherit_unique_keys(const SchemaInfo& input, SchemaInfo& out) {
    for (const auto& key : input.unique_keys()) {
        const bool survives = std::ranges::all_of(
            key, [&](const std::string& name) { return out.find(name) != nullptr; });
        if (survives) {
            out.add_unique_key(key);
        }
    }
}

/// Schema of projecting `columns` out of `input`. Shared by `Project` and the
/// fused nodes that end in a projection, so the two cannot drift apart.
///
/// Known even from an Unknown input: the projection itself fixes the output
/// column set, whatever it reads.
auto project_schema(const std::vector<ColumnRef>& columns, const SchemaInfo& input) -> SchemaInfo {
    std::vector<SchemaField> out;
    out.reserve(columns.size());
    for (const auto& ref : columns) {
        std::optional<ColumnType> type;
        if (const auto* field = input.find(ref.name)) {
            type = field->type;
        }
        out.push_back(SchemaField{.name = ref.name, .type = type});
    }
    // Keep the time index only if the projection retains that column.
    std::optional<std::string> time_index;
    if (input.time_index().has_value()) {
        const bool kept = std::ranges::any_of(
            columns, [&](const ColumnRef& ref) { return ref.name == *input.time_index(); });
        if (kept) {
            time_index = input.time_index();
        }
    }
    SchemaInfo result = SchemaInfo::known(std::move(out), /*open=*/false, std::move(time_index));
    // Row-preserving, so a key survives iff the projection keeps all its columns.
    inherit_unique_keys(input, result);
    return result;
}

/// Schema of adding/replacing `fields` (and `tuple_fields`) on `input`. Shared
/// by `Update` and the fused node that contains one.
///
/// Existing columns are retained, so they must be known: an Unknown input gives
/// an Unknown result.
auto update_schema(const std::vector<FieldSpec>& fields,
                   const std::vector<TupleFieldSpec>& tuple_fields, const SchemaInfo& input)
    -> SchemaInfo {
    if (!input.is_known()) {
        return SchemaInfo::unknown();
    }
    std::vector<SchemaField> out = input.fields();
    auto upsert = [&out](const std::string& name, std::optional<ColumnType> type) {
        for (auto& field : out) {
            if (field.name == name) {
                field.type = type;
                return;
            }
        }
        out.push_back(SchemaField{.name = name, .type = type});
    };
    for (const auto& field : fields) {
        upsert(field.alias, expr_type(field.expr, input));
    }
    for (const auto& tuple : tuple_fields) {
        for (const auto& alias : tuple.aliases) {
            upsert(alias, std::nullopt);
        }
    }
    // Update retains every existing column, so the time index survives.
    SchemaInfo result = SchemaInfo::known(std::move(out), input.is_open(), input.time_index());
    // A key survives only if the update rewrote none of its columns: assigning a
    // column keeps its name but replaces its values, and nothing says the new
    // ones are still distinct.
    robin_hood::unordered_set<std::string> assigned;
    for (const auto& field : fields) {
        assigned.insert(field.alias);
    }
    for (const auto& tuple : tuple_fields) {
        assigned.insert(tuple.aliases.begin(), tuple.aliases.end());
    }
    for (const auto& key : input.unique_keys()) {
        const bool untouched = std::ranges::none_of(
            key, [&](const std::string& name) { return assigned.contains(name); });
        if (untouched) {
            result.add_unique_key(key);
        }
    }
    return result;
}

/// Unique constraints a join's output inherits from its inputs. Each follows
/// from the join's definition alone -- no data, no statistics.
///
/// The pivot is the same fact the cardinality estimator turns on: when one side
/// is unique on (a subset of) the join keys, every row of the *other* side
/// matches at most one row across it, so the output rows are in bijection with
/// a subset of that side's rows and every constraint that held there still
/// holds.
void add_join_unique_keys(const JoinNode& join, const SchemaInfo& left, const SchemaInfo& right,
                          SchemaInfo& out) {
    // Left columns always reach the output under their own names.
    const auto left_key_survives = [&](const UniqueKey& key) {
        return std::ranges::all_of(
            key, [&](const std::string& name) { return out.find(name) != nullptr; });
    };
    // A right column whose name collides with a left one is dropped -- the
    // left's is kept -- so a right constraint naming it carries over only where
    // the join equates the two, which is exactly on a join key.
    const auto right_key_survives = [&](const UniqueKey& key) {
        return std::ranges::all_of(key, [&](const std::string& name) {
            return out.find(name) != nullptr &&
                   (left.find(name) == nullptr ||
                    std::ranges::find(join.keys(), name) != join.keys().end());
        });
    };
    const auto keep = [&](const SchemaInfo& side, const auto& survives) {
        for (const auto& key : side.unique_keys()) {
            if (survives(key)) {
                out.add_unique_key(key);
            }
        }
    };

    switch (join.kind()) {
        case JoinKind::Semi:
        case JoinKind::Anti:
            // Selects left rows; the right contributes neither rows nor columns.
            keep(left, left_key_survives);
            return;
        case JoinKind::Inner:
            if (right.is_unique_within(join.keys())) {
                keep(left, left_key_survives);
            }
            if (left.is_unique_within(join.keys())) {
                keep(right, right_key_survives);
            }
            return;
        case JoinKind::Cross:
            // Every (l, r) pair appears exactly once, so a left key and a right
            // key *together* identify a row -- neither does alone. The common
            // shape is a decorrelated scalar subquery, whose right side is an
            // ungrouped aggregate: its key is empty, so the pair is just the
            // left's key.
            for (const auto& left_key : left.unique_keys()) {
                for (const auto& right_key : right.unique_keys()) {
                    if (!left_key_survives(left_key) || !right_key_survives(right_key)) {
                        continue;
                    }
                    UniqueKey combined = left_key;
                    combined.insert(combined.end(), right_key.begin(), right_key.end());
                    out.add_unique_key(std::move(combined));
                }
            }
            return;
        case JoinKind::Left:
        case JoinKind::Right:
        case JoinKind::Outer:
        case JoinKind::Asof:
            // Not modelled: the outer kinds pad with null rows and Asof matches
            // on an inequality, so neither reduces to the argument above.
            return;
    }
}

}  // namespace

namespace {

auto type_name(ColumnType type) -> std::string_view {
    switch (type) {
        case ColumnType::Int32:
            return "Int32";
        case ColumnType::Int64:
            return "Int64";
        case ColumnType::Float32:
            return "Float32";
        case ColumnType::Float64:
            return "Float64";
        case ColumnType::Bool:
            return "Bool";
        case ColumnType::String:
            return "String";
        case ColumnType::Date:
            return "Date";
        case ColumnType::Timestamp:
            return "Timestamp";
    }
    return "?";
}

auto check_one(const AscribeNode& asc, const SchemaInfo& input)
    -> std::expected<void, std::string> {
    for (const auto& field : asc.schema()) {
        const auto* have = input.find(field.name);
        if (have == nullptr) {
            return std::unexpected("schema ascription: missing column '" + field.name + "'");
        }
        // Plain equality is right here even though the interpreter's check is
        // lenient about Categorical: `column_ir_type` maps both Column<string>
        // and Column<Categorical> to ColumnType::String, so the distinction the
        // interpreter has to tolerate does not exist at the schema level.
        if (field.type.has_value() && have->type.has_value() && *have->type != *field.type) {
            return std::unexpected("schema ascription: column '" + field.name + "' is " +
                                   std::string(type_name(*have->type)) +
                                   " but the ascription requires " +
                                   std::string(type_name(*field.type)));
        }
    }
    if (asc.open()) {
        return {};
    }
    // An exact ascription also asserts the input has no column it does not
    // list. That is a question about the schema, so the schema answers it --
    // no need to materialize the extras just to notice them.
    for (const auto& have : input.fields()) {
        const bool listed = std::any_of(asc.schema().begin(), asc.schema().end(),
                                        [&](const SchemaField& f) { return f.name == have.name; });
        if (!listed) {
            return std::unexpected("schema ascription: input has extra column '" + have.name +
                                   "' not in the ascribed schema (add `*` to allow extras)");
        }
    }
    return {};
}

}  // namespace

auto check_ascriptions(Node& root, const SourceSchemas& sources)
    -> std::expected<void, std::string> {
    for (const auto& child : root.children()) {
        if (child != nullptr) {
            if (auto below = check_ascriptions(*child, sources); !below.has_value()) {
                return below;
            }
        }
    }
    if (root.kind() != NodeKind::Ascribe || root.children().empty() ||
        root.children().front() == nullptr) {
        return {};
    }
    const SchemaInfo input = infer_schema(*root.children().front(), sources);
    if (!input.is_known() || input.is_open()) {
        // Nothing to prove it against; the interpreter checks it against data.
        return {};
    }
    auto& asc = static_cast<AscribeNode&>(root);
    if (auto ok = check_one(asc, input); !ok.has_value()) {
        return ok;
    }
    asc.set_checked();
    return {};
}

auto extern_call_site_key(const std::string& callee, const std::vector<Expr>& args)
    -> std::optional<std::string> {
    std::string key = callee;
    key += '(';
    for (const auto& arg : args) {
        const auto* literal = std::get_if<Literal>(&arg.node);
        if (literal == nullptr) {
            return std::nullopt;
        }
        std::visit(
            [&](const auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, std::string>) {
                    key += '"';
                    key += value;
                    key += '"';
                } else if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double> ||
                                     std::is_same_v<T, bool>) {
                    key += std::to_string(value);
                } else if constexpr (std::is_same_v<T, Date>) {
                    key += "d" + std::to_string(value.days);
                } else {
                    key += "t" + std::to_string(value.nanos);
                }
            },
            literal->value);
        key += ',';
    }
    key += ')';
    return key;
}

auto infer_schema(const Node& node, const SourceSchemas& sources) -> SchemaInfo {
    switch (node.kind()) {
        case NodeKind::Program:
            return infer_schema(static_cast<const ProgramNode&>(node).main_node(), sources);

        case NodeKind::Scan: {
            const auto& scan = static_cast<const ScanNode&>(node);
            if (auto it = sources.find(scan.source_name()); it != sources.end()) {
                return it->second;
            }
            return SchemaInfo::unknown();
        }
        case NodeKind::ExternCall: {
            const auto& call = static_cast<const ExternCallNode&>(node);
            // A call-site schema is what this specific reader call returns, and
            // beats the callee's declared one, which is per-function and so
            // cannot describe a generic reader like read_parquet.
            if (const auto key = extern_call_site_key(call.callee(), call.args())) {
                if (auto it = sources.find(*key); it != sources.end()) {
                    return it->second;
                }
            }
            if (auto it = sources.find(call.callee()); it != sources.end()) {
                return it->second;
            }
            return SchemaInfo::unknown();
        }

        // Pure row-shaping operators: schema, time index and unique constraints
        // all pass through. These only drop or reorder rows.
        case NodeKind::Filter:
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
            return child_schema(node, sources);

        case NodeKind::Distinct: {
            // Deduplicates whole rows, so all columns together are unique by
            // construction -- but only once we know what "all columns" are. An
            // open schema may hide the column that separates two rows agreeing
            // on every listed one.
            SchemaInfo out = child_schema(node, sources);
            if (out.is_known() && !out.is_open()) {
                UniqueKey all;
                all.reserve(out.fields().size());
                for (const auto& field : out.fields()) {
                    all.push_back(field.name);
                }
                out.add_unique_key(std::move(all));
            }
            return out;
        }

        // Schema passes through; unique constraints do not. Rbind concatenates
        // rows (its operands share child[0]'s schema), so a tuple unique within
        // one operand can repeat across them. Window's row multiplicity is not
        // modelled here.
        case NodeKind::Window:
        case NodeKind::Rbind:
            return without_unique_keys(child_schema(node, sources));

        case NodeKind::AsTimeframe: {
            // Designates the time-index column. The index column is materialised
            // as a Timestamp (integer time columns are converted at run time).
            const auto& atf = static_cast<const AsTimeframeNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            if (!input.is_known()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out = input.fields();
            for (auto& field : out) {
                if (field.name == atf.column()) {
                    field.type = ColumnType::Timestamp;
                }
            }
            SchemaInfo result = SchemaInfo::known(std::move(out), input.is_open(), atf.column());
            inherit_unique_keys(input, result);  // row-preserving
            return result;
        }

        case NodeKind::Project:
            return project_schema(static_cast<const ProjectNode&>(node).columns(),
                                  child_schema(node, sources));

        case NodeKind::Rename: {
            const auto& rename = static_cast<const RenameNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            if (!input.is_known()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out = input.fields();
            std::optional<std::string> time_index = input.time_index();
            std::vector<UniqueKey> keys = input.unique_keys();
            for (const auto& spec : rename.renames()) {
                for (auto& field : out) {
                    if (field.name == spec.old_name) {
                        field.name = spec.new_name;
                    }
                }
                if (time_index.has_value() && *time_index == spec.old_name) {
                    time_index = spec.new_name;  // the index column was renamed
                }
                // Renaming a column changes what to call a constraint, never
                // whether it holds. Applied in the same sequence as the fields
                // above so the two cannot disagree.
                for (auto& key : keys) {
                    for (auto& name : key) {
                        if (name == spec.old_name) {
                            name = spec.new_name;
                        }
                    }
                }
            }
            SchemaInfo result =
                SchemaInfo::known(std::move(out), input.is_open(), std::move(time_index));
            for (auto& key : keys) {
                result.add_unique_key(std::move(key));
            }
            return result;
        }

        case NodeKind::Update: {
            const auto& update = static_cast<const UpdateNode&>(node);
            return update_schema(update.fields(), update.tuple_fields(),
                                 child_schema(node, sources));
        }

        case NodeKind::Aggregate: {
            // Output: group-by key columns followed by one column per aggregate.
            // Known even from an Unknown child (the output column set is fixed),
            // though key/aggregate types may be unresolved.
            const auto& agg = static_cast<const AggregateNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            std::vector<SchemaField> out;
            out.reserve(agg.group_by().size() + agg.aggregations().size());
            for (const auto& key : agg.group_by()) {
                std::optional<ColumnType> type;
                if (const auto* field = input.find(key.name)) {
                    type = field->type;
                }
                out.push_back(SchemaField{.name = key.name, .type = type});
            }
            for (const auto& spec : agg.aggregations()) {
                out.push_back(
                    SchemaField{.name = spec.alias, .type = agg_result_type(spec, input)});
            }
            SchemaInfo result = SchemaInfo::known(std::move(out));
            // One row per distinct group, so the group keys are unique by
            // construction -- and, like the column set above, true whatever the
            // input turns out to be. An ungrouped aggregate collapses to a
            // single row: that is the empty key, no column needed to tell its
            // rows apart.
            UniqueKey keys;
            keys.reserve(agg.group_by().size());
            for (const auto& key : agg.group_by()) {
                keys.push_back(key.name);
            }
            result.add_unique_key(std::move(keys));
            return result;
        }

        case NodeKind::Join: {
            // A ∪ B: left columns, then right columns whose names are not
            // already present (shared join keys appear once).
            const auto& join = static_cast<const JoinNode&>(node);
            const SchemaInfo left = child_schema(node, sources, 0);
            const SchemaInfo right = child_schema(node, sources, 1);
            if (!left.is_known() || !right.is_known()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out = left.fields();
            for (const auto& field : right.fields()) {
                const bool present = std::any_of(out.begin(), out.end(), [&](const SchemaField& f) {
                    return f.name == field.name;
                });
                if (!present) {
                    out.push_back(field);
                }
            }
            SchemaInfo result =
                SchemaInfo::known(std::move(out), left.is_open() || right.is_open());
            add_join_unique_keys(join, left, right, result);
            return result;
        }

        case NodeKind::Construct: {
            const auto& construct = static_cast<const ConstructNode&>(node);
            std::vector<SchemaField> out;
            out.reserve(construct.columns().size());
            for (const auto& col : construct.columns()) {
                std::optional<ColumnType> type;
                if (!col.elements.empty()) {
                    type = literal_type(col.elements.front());
                } else if (col.expr_node != nullptr) {
                    const SchemaInfo sub = infer_schema(*col.expr_node, sources);
                    if (sub.is_known() && sub.fields().size() == 1) {
                        type = sub.fields().front().type;
                    }
                }
                out.push_back(SchemaField{.name = col.name, .type = type});
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Ascribe: {
            // The ascription fixes the result schema (validated at run time); a
            // wildcard ascription is open (extra columns allowed).
            const auto& asc = static_cast<const AscribeNode&>(node);
            return SchemaInfo::known(asc.schema(), asc.open());
        }

        case NodeKind::Columns:
            // Exposes child column names as a single String column named "name".
            return SchemaInfo::known({SchemaField{.name = "name", .type = ColumnType::String}});

        case NodeKind::Melt: {
            // Output: the id columns (types from input), then `variable: String`
            // and `value` (the common type of the melted measure columns, when
            // statically determinable). The column set is fixed -> closed.
            const auto& melt = static_cast<const MeltNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            std::vector<SchemaField> out;
            for (const auto& id : melt.id_columns()) {
                std::optional<ColumnType> type;
                if (const auto* field = input.find(id)) {
                    type = field->type;
                }
                out.push_back(SchemaField{.name = id, .type = type});
            }
            out.push_back(SchemaField{.name = "variable", .type = ColumnType::String});
            std::optional<ColumnType> value_type;
            if (input.is_known()) {
                std::vector<std::string> measures(melt.measure_columns().begin(),
                                                  melt.measure_columns().end());
                if (measures.empty()) {  // empty list melts every non-id column
                    for (const auto& field : input.fields()) {
                        const bool is_id =
                            std::any_of(melt.id_columns().begin(), melt.id_columns().end(),
                                        [&](const std::string& n) { return n == field.name; });
                        if (!is_id) {
                            measures.push_back(field.name);
                        }
                    }
                }
                bool consistent = !measures.empty();
                for (std::size_t i = 0; i < measures.size(); ++i) {
                    const auto* field = input.find(measures[i]);
                    std::optional<ColumnType> t = std::nullopt;
                    if (field) {
                        t = field->type;
                    }
                    if (i == 0) {
                        value_type = t;
                    } else if (value_type.has_value() != t.has_value() ||
                               (value_type.has_value() && *value_type != *t)) {
                        consistent = false;
                        break;
                    }
                }
                if (!consistent) {
                    value_type = std::nullopt;
                }
            }
            out.push_back(SchemaField{.name = "value", .type = value_type});
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Cov:
        case NodeKind::Corr: {
            // Output: a leading `column: String` then one `Float64` column per
            // numeric input column. Determinable only from a fully-typed closed
            // input (we must know exactly which columns are numeric).
            const SchemaInfo input = child_schema(node, sources);
            if (!input.is_known() || input.is_open()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out;
            out.push_back(SchemaField{.name = "column", .type = ColumnType::String});
            for (const auto& field : input.fields()) {
                if (!field.type.has_value()) {
                    return SchemaInfo::unknown();  // can't tell if this column is numeric
                }
                if (is_numeric(*field.type)) {
                    out.push_back(SchemaField{.name = field.name, .type = ColumnType::Float64});
                }
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Resample: {
            // Output: a time-bucket column (Timestamp, named after the input's
            // time index) + group keys + one column per aggregate. When the
            // input's time index is known the column set is fully pinned down
            // (closed); otherwise the bucket column cannot be named, so the
            // result is left OPEN.
            const auto& rs = static_cast<const ResampleNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            const std::optional<std::string>& bucket = input.time_index();
            std::vector<SchemaField> out;
            if (bucket.has_value()) {
                out.push_back(SchemaField{.name = *bucket, .type = ColumnType::Timestamp});
            }
            for (const auto& key : rs.group_by()) {
                std::optional<ColumnType> type;
                if (const auto* field = input.find(key.name)) {
                    type = field->type;
                }
                out.push_back(SchemaField{.name = key.name, .type = type});
            }
            for (const auto& spec : rs.aggregations()) {
                out.push_back(
                    SchemaField{.name = spec.alias, .type = agg_result_type(spec, input)});
            }
            SchemaInfo result =
                SchemaInfo::known(std::move(out), /*open=*/!bucket.has_value(), bucket);
            // One row per (bucket, group), by the same construction as
            // Aggregate -- but only claimable once the bucket column has a name.
            if (bucket.has_value()) {
                UniqueKey keys{*bucket};
                keys.reserve(rs.group_by().size() + 1);
                for (const auto& key : rs.group_by()) {
                    keys.push_back(key.name);
                }
                result.add_unique_key(std::move(keys));
            }
            return result;
        }

        // The fused nodes canonicalize produces are exactly the operators above
        // run back to back, so their schemas are those operators' schemas. They
        // are not a detail: canonicalize fuses `Project(Filter(scan))` -- the
        // shape of every hand-written scan leaf -- so leaving these Unknown
        // meant no real plan's leaf had a schema, and anything gated on a Known
        // input (the join-order cost model, ambiguity checks) silently declined
        // on every query. R5/R6/R7/R8/R16 in canonicalize.cpp define the
        // equivalences these mirror.
        case NodeKind::FilterProject: {
            // Project(Filter(x)): the projection fixes the output columns.
            const auto& fused = static_cast<const FilterProjectNode&>(node);
            return project_schema(fused.columns(), child_schema(node, sources));
        }
        case NodeKind::FilterUpdateProject: {
            // Project(Update(Filter(x))). The update's computed fields are only
            // observable through the projection, and a projected name resolves
            // against the update's output -- so type the update first, then
            // project it.
            const auto& fused = static_cast<const FilterUpdateProjectNode&>(node);
            SchemaInfo updated = update_schema(fused.fields(), {}, child_schema(node, sources));
            return project_schema(fused.project_columns(), updated);
        }
        // Head(Filter(x)) / Tail(Filter(x)) / Head(Order(x)): row-subsetting
        // only, so schema, time index and unique constraints pass through.
        case NodeKind::FilterHead:
        case NodeKind::FilterTail:
        case NodeKind::TopK:
            return child_schema(node, sources);

        // Data-dependent output columns or not yet modelled: Unknown is sound.
        case NodeKind::Dcast:
        case NodeKind::Transpose:
        case NodeKind::Matmul:
        case NodeKind::Model:
        case NodeKind::Stream:
            return SchemaInfo::unknown();
    }
    return SchemaInfo::unknown();
}

namespace {

auto missing_column(std::string_view clause, const std::string& name) -> std::string {
    return std::string(clause) + ": column '" + name + "' not found in input";
}

// Collect the column names referenced by an expression (value or predicate).
// Function callees and bound scalars are filtered out by the caller against the
// schema / lexical bindings; here we just gather every ColumnRef name.
void collect_expr_columns(const Expr& expr, std::vector<std::string>& out) {
    std::visit(
        [&](const auto& node) {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                out.push_back(node.name);
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                collect_expr_columns(*node.left, out);
                collect_expr_columns(*node.right, out);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                collect_expr_columns(*node.left, out);
                if (node.right) {  // null for unary Not
                    collect_expr_columns(*node.right, out);
                }
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                collect_expr_columns(*node.operand, out);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                // round(x, mode): the second argument is a bare mode identifier
                // (nearest/bankers/floor/ceil/trunc), not a column reference, so
                // it must not be validated against the input schema.
                const bool round_mode = (node.callee == "round" && node.args.size() == 2);
                for (std::size_t i = 0; i < node.args.size(); ++i) {
                    if (round_mode && i == 1) {
                        continue;
                    }
                    collect_expr_columns(*node.args[i], out);
                }
                for (const auto& named : node.named_args) {
                    collect_expr_columns(*named.value, out);
                }
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                for (const auto& key : node.order_keys) {
                    out.push_back(key.name);
                }
            }
        },
        expr.node);
}

}  // namespace

auto check_column_refs(const Node& node, const SourceSchemas& sources,
                       const robin_hood::unordered_set<std::string>& lexical_names,
                       bool check_expressions) -> std::optional<std::string> {
    if (node.kind() == NodeKind::Program) {
        const auto& program = static_cast<const ProgramNode&>(node);
        for (const auto& pre : program.preamble()) {
            if (auto err = check_column_refs(*pre, sources, lexical_names, check_expressions)) {
                return err;
            }
        }
        return check_column_refs(program.main_node(), sources, lexical_names, check_expressions);
    }

    for (const auto& child : node.children()) {
        if (auto err = check_column_refs(*child, sources, lexical_names, check_expressions)) {
            return err;
        }
    }

    // Missing-column checks are only sound on a closed Known input: an open
    // (wildcard) schema may have extra columns, so an absent name is not
    // provably missing.
    const SchemaInfo input = node.children().empty()
                                 ? SchemaInfo::unknown()
                                 : infer_schema(*node.children().front(), sources);
    if (!input.is_known() || input.is_open()) {
        return std::nullopt;
    }

    // A name in an expression position is valid if it is a column of the input
    // or any in-scope lexical binding (scalar etc.).
    auto resolvable = [&](const std::string& name) {
        return input.find(name) != nullptr || lexical_names.contains(name);
    };

    if (check_expressions && node.kind() == NodeKind::Filter) {
        std::vector<std::string> refs;
        collect_expr_columns(static_cast<const FilterNode&>(node).predicate(), refs);
        for (const auto& name : refs) {
            if (!resolvable(name)) {
                return missing_column("filter", name);
            }
        }
    }
    if (check_expressions && node.kind() == NodeKind::Update) {
        std::vector<std::string> refs;
        for (const auto& field : static_cast<const UpdateNode&>(node).fields()) {
            collect_expr_columns(field.expr, refs);
        }
        for (const auto& name : refs) {
            if (!resolvable(name)) {
                return missing_column("update", name);
            }
        }
    }

    switch (node.kind()) {
        case NodeKind::Project:
            for (const auto& ref : static_cast<const ProjectNode&>(node).columns()) {
                if (input.find(ref.name) == nullptr) {
                    return missing_column("select", ref.name);
                }
            }
            break;
        case NodeKind::Order:
            for (const auto& key : static_cast<const OrderNode&>(node).keys()) {
                if (input.find(key.name) == nullptr) {
                    return missing_column("order", key.name);
                }
            }
            break;
        case NodeKind::Rename:
            for (const auto& spec : static_cast<const RenameNode&>(node).renames()) {
                if (input.find(spec.old_name) == nullptr) {
                    return missing_column("rename", spec.old_name);
                }
            }
            break;
        case NodeKind::Aggregate: {
            const auto& agg = static_cast<const AggregateNode&>(node);
            for (const auto& key : agg.group_by()) {
                if (input.find(key.name) == nullptr) {
                    return missing_column("by", key.name);
                }
            }
            for (const auto& spec : agg.aggregations()) {
                // Count takes no source column; computed inputs are materialised
                // upstream and may legitimately be absent from this input.
                if (spec.func == AggFunc::Count || spec.column.name.empty()) {
                    continue;
                }
                if (input.find(spec.column.name) == nullptr) {
                    return missing_column("aggregate", spec.column.name);
                }
            }
            break;
        }
        case NodeKind::Head:
            for (const auto& ref : static_cast<const HeadNode&>(node).group_by()) {
                if (input.find(ref.name) == nullptr) {
                    return missing_column("by", ref.name);
                }
            }
            break;
        case NodeKind::Tail:
            for (const auto& ref : static_cast<const TailNode&>(node).group_by()) {
                if (input.find(ref.name) == nullptr) {
                    return missing_column("by", ref.name);
                }
            }
            break;
        case NodeKind::Update:
            for (const auto& ref : static_cast<const UpdateNode&>(node).group_by()) {
                if (input.find(ref.name) == nullptr) {
                    return missing_column("by", ref.name);
                }
            }
            break;
        default:
            break;
    }
    return std::nullopt;
}

}  // namespace ibex::ir
