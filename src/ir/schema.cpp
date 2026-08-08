// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/join_output.hpp>
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

// haystack/needles is self-documenting; UniqueKey is only a std::vector<std::string> alias,
// not a genuinely confusable role.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
        // A lexical `^name` is a scalar binding whose type this pass does not
        // know; it is deliberately not looked up in the input schema.
        if (col->lexical) {
            return std::nullopt;
        }
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
            call->callee == "rolling_min" || call->callee == "rolling_max" ||
            call->callee == "rolling_first" || call->callee == "rolling_last") {
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

/// Whether a computed field can hold a null, given the input schema.
///
/// The shape mirrors `expr_type`: `Maybe` is the answer for anything not
/// argued, so an unmodelled expression costs precision and never soundness.
/// The one structural rule is that Ibex's scalars propagate null -- a null
/// operand yields a null result -- so an arithmetic or comparison expression is
/// null-free exactly when all of its operands are. The exceptions are the
/// functions whose *purpose* is null: `is_null`/`is_not_null` answer for a null
/// rather than propagating it, and `coalesce`/`fill_null` consume one.
auto expr_nullability(const Expr& expr, const SchemaInfo& input) -> Nullability {
    // Fold over sub-expressions: null-free only if every operand is.
    const auto all_of = [&](const auto&... operands) {
        Nullability result = Nullability::Never;
        ((result = weaker(result, expr_nullability(*operands, input))), ...);
        return result;
    };

    if (const auto* col = std::get_if<ColumnRef>(&expr.node)) {
        // A lexical `^name` is a scalar binding, not a column; this pass knows
        // nothing about its value, exactly as `expr_type` knows nothing about
        // its type.
        if (col->lexical) {
            return Nullability::Maybe;
        }
        const auto* field = input.find(col->name);
        return field != nullptr ? field->nulls : Nullability::Maybe;
    }
    if (std::holds_alternative<Literal>(expr.node)) {
        // There is no null literal in the surface language, so every literal
        // that parsed is a value.
        return Nullability::Never;
    }
    if (std::holds_alternative<IsNullExpr>(expr.node)) {
        // `is_null(x)` / `is_not_null(x)` are total: they answer true or false
        // for a null input rather than propagating it.
        return Nullability::Never;
    }
    if (const auto* cmp = std::get_if<CompareExpr>(&expr.node)) {
        // Three-valued: a null operand makes the comparison null, not false.
        return all_of(cmp->left, cmp->right);
    }
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node)) {
        // Deliberately not Kleene-precise. `false && null` is false whatever
        // the right operand does, but proving that needs a value-level
        // argument this pass does not make; the conservative fold under-claims.
        //
        // `Not` carries no right operand at all (see `LogicalExpr`), so the
        // fold has to be written out rather than handed both unconditionally.
        const Nullability left = expr_nullability(*logical->left, input);
        return logical->right != nullptr ? weaker(left, expr_nullability(*logical->right, input))
                                         : left;
    }
    if (const auto* bin = std::get_if<BinaryExpr>(&expr.node)) {
        // Including Div: integer division by zero is an error and float
        // division yields an infinity, so neither manufactures a null.
        return all_of(bin->left, bin->right);
    }
    if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
        // `coalesce(a, ..., z)` is the first non-null argument, so one
        // null-free argument anywhere makes the result null-free.
        if (call->callee == "coalesce") {
            return std::ranges::any_of(call->args,
                                       [&](const auto& arg) {
                                           return expr_nullability(*arg, input) ==
                                                  Nullability::Never;
                                       })
                       ? Nullability::Never
                       : Nullability::Maybe;
        }
        // `fill_null(x, v)` is `x` when present and `v` otherwise, so it is
        // null-free when the replacement is -- whatever `x` does.
        if (call->callee == "fill_null" && call->args.size() == 2) {
            return expr_nullability(*call->args[1], input);
        }
        // `null_if_nan(x)` and `null_if_not_finite(x)` manufacture a null from
        // a perfectly present value, which is the whole point of them. They are
        // `Scalar` like the rest, so they have to be named here.
        if (call->callee == "null_if_nan" || call->callee == "null_if_not_finite") {
            return Nullability::Maybe;
        }
        // Every other row-local builtin propagates: `sqrt(null)` is null, so
        // `sqrt(x)` is null-free exactly when `x` is. Only `Scalar` qualifies.
        // A `Transform` is excluded even when its argument is null-free --
        // `lag(x)` is null in the first row of each group whatever `x` does,
        // and so is every function with a warm-up -- and an unknown callee is
        // a plugin, about which this pass assumes nothing (see
        // `BuiltinFunctionInfo`).
        if (call->args.empty() || fn_kind(call->callee) != FnKind::Scalar) {
            return Nullability::Maybe;
        }
        Nullability result = Nullability::Never;
        for (const auto& arg : call->args) {
            result = weaker(result, expr_nullability(*arg, input));
        }
        return result;
    }
    return Nullability::Maybe;
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

/// Whether an aggregate's result column can hold a null.
///
/// Two conditions have to hold together, and `grouped` is the one that is easy
/// to lose: an aggregate emits one row per group that *occurred*, so a grouped
/// aggregate's every row summarizes at least one input row. An ungrouped one
/// emits its single row even over an empty input, where `min` has no value to
/// return -- so it proves nothing regardless of the column it reads.
auto agg_nullability(const AggSpec& agg, const SchemaInfo& input, bool grouped) -> Nullability {
    if (agg.func == AggFunc::Count) {
        // A count is a row count. Nothing it reads can make it absent, and an
        // empty group counts zero rather than null.
        return Nullability::Never;
    }
    if (!grouped) {
        return Nullability::Maybe;
    }
    const auto* field = input.find(agg.column.name);
    if (field == nullptr || !field->non_null()) {
        return Nullability::Maybe;
    }
    switch (agg.func) {
        case AggFunc::Sum:
        case AggFunc::Min:
        case AggFunc::Max:
        case AggFunc::First:
        case AggFunc::Last:
        case AggFunc::Mean:
        case AggFunc::Median:
            // Each is a value drawn from, or an arithmetic mean over, at least
            // one non-null input value.
            return Nullability::Never;
        case AggFunc::Stddev:
        case AggFunc::Ewma:
        case AggFunc::Quantile:
        case AggFunc::Skew:
        case AggFunc::Kurtosis:
            // Deliberately under-claimed: each is undefined for a group below
            // some size -- a sample stddev needs two rows, skew and kurtosis
            // more -- and this pass has no bound on group size. Proving these
            // means reading each kernel's small-group behaviour, which is a
            // separate piece of work from establishing the flag.
            return Nullability::Maybe;
        case AggFunc::Count:
            break;  // handled above
    }
    return Nullability::Maybe;
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
        // A projection moves a column, so every proof about its values -- type
        // and nullability alike -- moves with it.
        if (const auto* field = input.find(ref.name)) {
            out.push_back(*field);
        } else {
            out.push_back(SchemaField{.name = ref.name, .type = std::nullopt});
        }
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
    auto upsert = [&out](const std::string& name, std::optional<ColumnType> type,
                         Nullability nulls) {
        for (auto& field : out) {
            if (field.name == name) {
                field.type = type;
                // An assignment replaces the column's values outright, so any
                // proof the old column carried is gone with them -- including
                // when the new expression is the unproven one.
                field.nulls = nulls;
                return;
            }
        }
        out.push_back(SchemaField{.name = name, .type = type, .nulls = nulls});
    };
    for (const auto& field : fields) {
        // Each field reads the *input* schema, not the running one: `update`
        // evaluates its fields against the rows as they arrived.
        upsert(field.alias, expr_type(field.expr, input), expr_nullability(field.expr, input));
    }
    for (const auto& tuple : tuple_fields) {
        for (const auto& alias : tuple.aliases) {
            upsert(alias, std::nullopt, Nullability::Maybe);
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

/// Collect the columns whose nullness would make `expr` evaluate to null,
/// into `out`. The mirror of `expr_nullability`: that answers "is this
/// null-free", this answers "which columns would have to be".
void collect_null_propagating_refs(const Expr& expr, robin_hood::unordered_set<std::string>& out) {
    if (const auto* col = std::get_if<ColumnRef>(&expr.node)) {
        if (!col->lexical) {
            out.insert(col->name);
        }
        return;
    }
    if (const auto* bin = std::get_if<BinaryExpr>(&expr.node)) {
        collect_null_propagating_refs(*bin->left, out);
        collect_null_propagating_refs(*bin->right, out);
        return;
    }
    if (const auto* cmp = std::get_if<CompareExpr>(&expr.node)) {
        collect_null_propagating_refs(*cmp->left, out);
        collect_null_propagating_refs(*cmp->right, out);
        return;
    }
    if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
        // The same three exclusions as `expr_nullability`, for the same
        // reasons: a null-consuming function's argument need not be present,
        // and a non-`Scalar` callee's relationship to its arguments is not
        // row-local (or, for an unknown callee, not known at all).
        if (call->callee == "coalesce" || call->callee == "fill_null" ||
            fn_kind(call->callee) != FnKind::Scalar) {
            return;
        }
        for (const auto& arg : call->args) {
            collect_null_propagating_refs(*arg, out);
        }
        return;
    }
    // Literal: no column. IsNullExpr: total, so its operand's nullness does not
    // reach the result. LogicalExpr and RankExpr: not descended into -- see
    // `collect_filtered_non_null_refs`, which handles `&&` where it can say
    // something stronger.
}

/// Collect the columns that `predicate` proves null-free in the rows it keeps.
///
/// `filter` keeps a row only when its predicate is *true*, and Ibex is
/// three-valued everywhere: null is not true, so a row whose predicate went
/// null is dropped along with the rows that went false. Every column the
/// predicate had to read to reach true is therefore present in every surviving
/// row.
void collect_filtered_non_null_refs(const Expr& predicate,
                                    robin_hood::unordered_set<std::string>& out) {
    if (const auto* logical = std::get_if<LogicalExpr>(&predicate.node)) {
        if (logical->op == LogicalOp::And) {
            // Both conjuncts are true, so both prove.
            collect_filtered_non_null_refs(*logical->left, out);
            if (logical->right != nullptr) {
                collect_filtered_non_null_refs(*logical->right, out);
            }
            return;
        }
        if (logical->op == LogicalOp::Not) {
            // `!` proves nothing in general -- `!(x > 0)` is true for no null
            // `x`, but it is *false* for one, and neither is a row this filter
            // keeps. The exception is a null test, which is total: `!is_null(x)`
            // is true exactly when `x` is present, so negating one is the same
            // statement as the other spelled directly.
            //
            // Worth the special case rather than left to the general rule,
            // because it is not an exotic spelling: `filter(!is.na(x))` is how
            // dplyr says this, and `is.na()` lowers to `is_null()`, so a
            // frontend cannot reach `is_not_null` from the idiom its users
            // actually write.
            if (const auto* inner = std::get_if<IsNullExpr>(&logical->left->node)) {
                if (!inner->negated) {
                    collect_null_propagating_refs(*inner->operand, out);
                }
            }
            return;
        }
        // `||` proves only what both branches prove. Left unclaimed rather than
        // intersected: an under-claim costs precision, and the intersection is
        // worth writing when something needs it.
        return;
    }
    if (const auto* is_null = std::get_if<IsNullExpr>(&predicate.node)) {
        // `is_not_null(x)` true says exactly this and nothing else; `is_null(x)`
        // true says the opposite, and there is no way to record that here.
        if (is_null->negated) {
            collect_null_propagating_refs(*is_null->operand, out);
        }
        return;
    }
    // Anything else -- a comparison, a bare Bool column, an arithmetic
    // expression read as a predicate -- had to be non-null to be true.
    collect_null_propagating_refs(predicate, out);
}

/// Apply a filter's proofs to the schema flowing through it.
auto filtered_schema(const Expr& predicate, SchemaInfo input) -> SchemaInfo {
    if (!input.is_known()) {
        return input;
    }
    robin_hood::unordered_set<std::string> proved;
    collect_filtered_non_null_refs(predicate, proved);
    if (proved.empty()) {
        return input;
    }
    std::vector<SchemaField> fields = input.fields();
    for (auto& field : fields) {
        if (proved.contains(field.name)) {
            field.nulls = Nullability::Never;
        }
    }
    SchemaInfo result = SchemaInfo::known(std::move(fields), input.is_open(), input.time_index());
    // Row-dropping, so every unique key survives -- the same reason
    // `inherit_unique_keys` gives.
    inherit_unique_keys(input, result);
    return result;
}

/// Whether a join may emit a row in which one side contributed nothing, per
/// side. This is the only reason a join weakens a nullability proof: an
/// unmatched row's columns on the absent side are filled with nulls, whatever
/// that input's own schema said.
struct JoinUnmatched {
    bool left;   ///< An output row may have no left row (right/outer).
    bool right;  ///< An output row may have no right row (left/outer/asof).
};

auto join_unmatched_sides(JoinKind kind) -> JoinUnmatched {
    switch (kind) {
        case JoinKind::Left:
        case JoinKind::Asof:
            // `asof` keeps every left row whether or not a right row precedes
            // it, so its right columns null out exactly as a left join's do.
            return {.left = false, .right = true};
        case JoinKind::Right:
            return {.left = true, .right = false};
        case JoinKind::Outer:
            return {.left = true, .right = true};
        case JoinKind::Inner:
        case JoinKind::Semi:
        case JoinKind::Anti:
        case JoinKind::Cross:
            // Semi and anti emit left columns only, and emit whole left rows;
            // cross pairs every row with every row. None of them fills.
            return {.left = false, .right = false};
    }
    return {.left = true, .right = true};
}

/// Nullability of each planned output column of a join.
///
/// Two rules, in this order. First, an equijoin key column of a join that emits
/// only matched rows is null-free: under `NullMatch::Never` a null key matches
/// nothing, so every row that survived has a value in every key. This is a
/// proof the inputs did not have, and the reason nullability is worth carrying
/// through a join at all rather than just weakening it.
///
/// Second, a column on a side that may be unmatched loses whatever it had. The
/// exception is a *folded* same-name key: `plan_join_output` emits one column
/// for it, and the executor fills that column from the right key for rows with
/// no left row (`materialize`, join.cpp), so it is null-free when both sides'
/// keys are.
void apply_join_nullability(const JoinNode& join, const SchemaInfo& left, const SchemaInfo& right,
                            const std::vector<JoinOutputColumn>& plan,
                            std::vector<SchemaField>& out) {
    const JoinUnmatched unmatched = join_unmatched_sides(join.kind());
    const bool only_matched_rows = join.kind() == JoinKind::Inner || join.kind() == JoinKind::Semi;
    // `nulls equal` admits null keys into a match, which is exactly what the
    // key proof below rests on being impossible.
    const bool keys_are_null_free =
        only_matched_rows && join.null_match() == NullMatch::Never && !join.keys().empty();

    for (std::size_t i = 0; i < plan.size(); ++i) {
        const JoinOutputColumn& column = plan[i];
        const bool from_left = column.side == JoinOutputSide::Left;
        const SchemaInfo& source = from_left ? left : right;
        const std::string& source_name = source.fields()[column.source_index].name;

        const auto key = std::ranges::find_if(join.keys(), [&](const JoinKey& k) {
            return (from_left ? k.left : k.right) == source_name;
        });
        if (key == join.keys().end()) {
            out[i].nulls =
                (from_left ? unmatched.left : unmatched.right) ? Nullability::Maybe : out[i].nulls;
            continue;
        }

        if (keys_are_null_free) {
            out[i].nulls = Nullability::Never;
            continue;
        }
        // A key folded into one output column (same name on both sides) draws
        // from whichever side is present, so it needs both proofs -- but only
        // when a left row can actually be missing.
        if (from_left && unmatched.left && key->left == key->right) {
            const auto* right_field = right.find(key->right);
            out[i].nulls = weaker(out[i].nulls,
                                  right_field != nullptr ? right_field->nulls : Nullability::Maybe);
            continue;
        }
        if (from_left ? unmatched.left : unmatched.right) {
            out[i].nulls = Nullability::Maybe;
        }
    }
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
                          const std::vector<JoinOutputColumn>& plan, SchemaInfo& out) {
    // Left columns always reach the output under their own names.
    const auto left_key_survives = [&](const UniqueKey& key) {
        return std::ranges::all_of(
            key, [&](const std::string& name) { return out.find(name) != nullptr; });
    };

    // Where each right column ends up in the output. The planner renames a
    // colliding right column rather than dropping it, so a right constraint
    // naming one carries over — under the planner's name. The exception is a
    // same-name key, which the planner folds into the left column of that
    // name: the join equates the two, so the constraint carries over there.
    robin_hood::unordered_map<std::string, std::string> right_out_name;
    for (const auto& column : plan) {
        if (column.side == JoinOutputSide::Right) {
            right_out_name.emplace(right.fields()[column.source_index].name, column.name);
        }
    }
    for (const auto& key : join.keys()) {
        if (key.left == key.right) {
            right_out_name.emplace(key.right, key.right);
        }
    }
    // Restates a right-side constraint in output names, or fails if any of its
    // columns did not reach the output (semi/anti drop the right side whole).
    const auto right_key_in_output = [&](const UniqueKey& key) -> std::optional<UniqueKey> {
        UniqueKey renamed;
        renamed.reserve(key.size());
        for (const std::string& name : key) {
            const auto found = right_out_name.find(name);
            if (found == right_out_name.end()) {
                return std::nullopt;
            }
            renamed.push_back(found->second);
        }
        return renamed;
    };
    const auto keep_left = [&]() {
        for (const auto& key : left.unique_keys()) {
            if (left_key_survives(key)) {
                out.add_unique_key(key);
            }
        }
    };
    const auto keep_right = [&]() {
        for (const auto& key : right.unique_keys()) {
            if (auto renamed = right_key_in_output(key)) {
                out.add_unique_key(std::move(*renamed));
            }
        }
    };

    switch (join.kind()) {
        case JoinKind::Semi:
        case JoinKind::Anti:
            // Selects left rows; the right contributes neither rows nor columns.
            keep_left();
            return;
        case JoinKind::Inner:
            // A proof, or a declaration the executor is about to check. The
            // two are interchangeable here: `expect n:1` says each left row
            // matches at most one right row, which is exactly what makes a
            // left-side unique key still unique in the output — and a run whose
            // data disagrees fails instead of producing rows this describes
            // wrongly. Same standing as an ascription.
            if (join.expect().right_at_most_one() ||
                right.is_unique_within(right_join_key_names(join.keys()))) {
                keep_left();
            }
            if (join.expect().left_at_most_one() ||
                left.is_unique_within(left_join_key_names(join.keys()))) {
                keep_right();
            }
            return;
        case JoinKind::Cross:
            // Every (l, r) pair appears exactly once, so a left key and a right
            // key *together* identify a row -- neither does alone. The common
            // shape is a decorrelated scalar subquery, whose right side is an
            // ungrouped aggregate: its key is empty, so the pair is just the
            // left's key.
            for (const auto& left_key : left.unique_keys()) {
                if (!left_key_survives(left_key)) {
                    continue;
                }
                for (const auto& right_key : right.unique_keys()) {
                    auto renamed = right_key_in_output(right_key);
                    if (!renamed.has_value()) {
                        continue;
                    }
                    UniqueKey combined = left_key;
                    combined.insert(combined.end(), renamed->begin(), renamed->end());
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
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    auto& asc = static_cast<AscribeNode&>(root);
    if (auto ok = check_one(asc, input); !ok.has_value()) {
        return ok;
    }
    asc.set_checked();
    return {};
}

namespace {

/// The granularity at which the *runtime* compares two join keys. Its column
/// variant holds one integer width and one float width, and reports a
/// dictionary-encoded column as a string, so several IR types share a kind.
/// Comparing at this granularity is what keeps the static check from rejecting
/// a join the executor would have run.
enum class KeyKind : std::uint8_t {
    Int,
    Float,
    Bool,
    String,
    Date,
    Timestamp,
};

auto key_kind(ColumnType type) -> KeyKind {
    switch (type) {
        case ColumnType::Int32:
        case ColumnType::Int64:
            return KeyKind::Int;
        case ColumnType::Float32:
        case ColumnType::Float64:
            return KeyKind::Float;
        case ColumnType::Bool:
            return KeyKind::Bool;
        case ColumnType::String:
            return KeyKind::String;
        case ColumnType::Date:
            return KeyKind::Date;
        case ColumnType::Timestamp:
            return KeyKind::Timestamp;
    }
    return KeyKind::String;
}

auto format_field_names(const SchemaInfo& info) -> std::string {
    std::string out;
    for (const auto& field : info.fields()) {
        if (!out.empty()) {
            out.append(", ");
        }
        out.append(field.name);
    }
    if (info.is_open()) {
        out.append(out.empty() ? "..." : ", ...");
    }
    return out;
}

auto check_one_join(const JoinNode& join, const SchemaInfo& left, const SchemaInfo& right)
    -> std::optional<std::string> {
    for (const auto& key : join.keys()) {
        const auto* left_field = left.find(key.left);
        const auto* right_field = right.find(key.right);
        // Only a closed schema proves absence: an open one lists the columns it
        // knows about and admits others.
        if (left_field == nullptr && !left.is_open()) {
            return "join key '" + key.left + "' not found on the left side (available: " +
                   format_field_names(left) + ")";
        }
        if (right_field == nullptr && !right.is_open()) {
            return "join key '" + key.right + "' not found on the right side (available: " +
                   format_field_names(right) + ")";
        }
        if (left_field == nullptr || right_field == nullptr) {
            continue;
        }
        if (!left_field->type.has_value() || !right_field->type.has_value()) {
            continue;  // an untyped column is still known to exist
        }
        if (key_kind(*left_field->type) != key_kind(*right_field->type)) {
            return "join key type mismatch: left '" + key.left + "' is " +
                   std::string(type_name(*left_field->type)) + " but right '" + key.right +
                   "' is " + std::string(type_name(*right_field->type));
        }
    }

    std::vector<std::string_view> left_names;
    std::vector<std::string_view> right_names;
    left_names.reserve(left.fields().size());
    right_names.reserve(right.fields().size());
    for (const auto& field : left.fields()) {
        left_names.emplace_back(field.name);
    }
    for (const auto& field : right.fields()) {
        right_names.emplace_back(field.name);
    }
    // An open schema hides columns, so it can hide a collision -- but never
    // invent one. Reporting what the listed names prove is still sound.
    auto planned =
        plan_join_output(join.kind(), join.keys(), left_names, right_names, join.suffix());
    if (!planned.has_value()) {
        return planned.error();
    }
    return std::nullopt;
}

}  // namespace

auto check_joins(const Node& node, const SourceSchemas& sources) -> std::optional<std::string> {
    if (node.kind() == NodeKind::Program) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& program = static_cast<const ProgramNode&>(node);
        for (const auto& pre : program.preamble()) {
            if (auto err = check_joins(*pre, sources)) {
                return err;
            }
        }
        return check_joins(program.main_node(), sources);
    }

    for (const auto& child : node.children()) {
        if (child != nullptr) {
            if (auto err = check_joins(*child, sources)) {
                return err;
            }
        }
    }

    if (node.kind() != NodeKind::Join) {
        return std::nullopt;
    }
    const SchemaInfo left = child_schema(node, sources, 0);
    const SchemaInfo right = child_schema(node, sources, 1);
    if (!left.is_known() || !right.is_known()) {
        return std::nullopt;  // nothing to prove it against; the runtime checks the data
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    return check_one_join(static_cast<const JoinNode&>(node), left, right);
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

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast) -- every cast below is
// guarded by the switch on node.kind() matching the target node type.
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
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
            return child_schema(node, sources);

        // Filter is row-shaping too, but the rows it drops are informative: a
        // predicate that reached `true` read a value from every column it
        // propagated null through.
        case NodeKind::Filter:
            return filtered_schema(static_cast<const FilterNode&>(node).predicate(),
                                   child_schema(node, sources));

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
        case NodeKind::Window: {
            const auto& win = static_cast<const WindowNode&>(node);
            SchemaInfo child = without_unique_keys(child_schema(node, sources));
            if (!win.select_only() || !child.is_known()) {
                return child;
            }
            // `window` + `select`: the child Update computes S ∪ fields; the
            // result keeps only [time index, group keys, listed fields].
            const auto& update = static_cast<const UpdateNode&>(*node.children().front());
            std::vector<ColumnRef> keep;
            robin_hood::unordered_set<std::string> seen;
            auto keep_col = [&](const std::string& name) {
                if (seen.insert(name).second) {
                    keep.push_back(ColumnRef{.name = name});
                }
            };
            if (child.time_index().has_value()) {
                keep_col(*child.time_index());
            }
            for (const auto& key : update.group_by()) {
                keep_col(key.name);
            }
            for (const auto& field : update.fields()) {
                keep_col(field.alias);
            }
            return project_schema(keep, child);
        }
        case NodeKind::Rbind: {
            // The operands share child[0]'s column set, so its names and types
            // describe the result -- but its *rows* do not. A column null-free
            // in the first operand is only null-free in the concatenation when
            // it is null-free in every operand, so each proof has to survive a
            // vote rather than ride along with the schema it was written on.
            SchemaInfo result = without_unique_keys(child_schema(node, sources));
            if (!result.is_known()) {
                return result;
            }
            std::vector<SchemaField> fields = result.fields();
            for (std::size_t i = 1; i < node.children().size(); ++i) {
                const SchemaInfo operand = child_schema(node, sources, i);
                for (auto& field : fields) {
                    const auto* other = operand.is_known() ? operand.find(field.name) : nullptr;
                    field.nulls =
                        weaker(field.nulls, other != nullptr ? other->nulls : Nullability::Maybe);
                }
            }
            return SchemaInfo::known(std::move(fields), result.is_open(), result.time_index());
        }

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
            const bool grouped = !agg.group_by().empty();
            for (const auto& key : agg.group_by()) {
                // A key column holds one of the values it grouped, so it is
                // null-free exactly when the column it grouped was. (A null key
                // forms its own group and reaches the output as a null.)
                if (const auto* field = input.find(key.name)) {
                    out.push_back(*field);
                } else {
                    out.push_back(SchemaField{.name = key.name, .type = std::nullopt});
                }
            }
            for (const auto& spec : agg.aggregations()) {
                out.push_back(SchemaField{.name = spec.alias,
                                          .type = agg_result_type(spec, input),
                                          .nulls = agg_nullability(spec, input, grouped)});
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
            const auto& join = static_cast<const JoinNode&>(node);
            const SchemaInfo left = child_schema(node, sources, 0);
            const SchemaInfo right = child_schema(node, sources, 1);
            if (!left.is_known() || !right.is_known()) {
                return SchemaInfo::unknown();
            }
            // Output columns and their names come from the shared planner, so
            // inference cannot drift from what the executors materialize.
            const auto field_names = [](const SchemaInfo& info) {
                std::vector<std::string_view> names;
                names.reserve(info.fields().size());
                for (const auto& field : info.fields()) {
                    names.emplace_back(field.name);
                }
                return names;
            };
            const std::vector<std::string_view> left_names = field_names(left);
            const std::vector<std::string_view> right_names = field_names(right);
            const auto planned =
                plan_join_output(join.kind(), join.keys(), left_names, right_names, join.suffix());
            if (!planned.has_value()) {
                // An unresolved collision has no output schema to describe.
                // Inference stays total; `check_joins()` raises the diagnostic
                // where both schemas are known, and the executor where they
                // are not.
                return SchemaInfo::unknown();
            }
            const std::vector<JoinOutputColumn>& plan = *planned;

            std::vector<SchemaField> out;
            out.reserve(plan.size());
            for (const auto& column : plan) {
                const SchemaInfo& source = column.side == JoinOutputSide::Left ? left : right;
                SchemaField emitted = source.fields()[column.source_index];
                emitted.name = column.name;
                out.push_back(std::move(emitted));
            }
            apply_join_nullability(join, left, right, plan, out);

            const bool left_only = join.kind() == JoinKind::Semi || join.kind() == JoinKind::Anti;
            SchemaInfo result = SchemaInfo::known(
                std::move(out), left_only ? left.is_open() : (left.is_open() || right.is_open()));
            add_join_unique_keys(join, left, right, plan, result);
            return result;
        }

        case NodeKind::Construct: {
            const auto& construct = static_cast<const ConstructNode&>(node);
            std::vector<SchemaField> out;
            out.reserve(construct.columns().size());
            for (const auto& col : construct.columns()) {
                std::optional<ColumnType> type;
                Nullability nulls = Nullability::Maybe;
                if (!col.elements.empty()) {
                    type = literal_type(col.elements.front());
                    // A literal list is written out value by value, and the
                    // surface language has no null literal to write.
                    nulls = Nullability::Never;
                } else if (col.expr_node != nullptr) {
                    const SchemaInfo sub = infer_schema(*col.expr_node, sources);
                    if (sub.is_known() && sub.fields().size() == 1) {
                        type = sub.fields().front().type;
                        nulls = sub.fields().front().nulls;
                    }
                }
                out.push_back(SchemaField{.name = col.name, .type = type, .nulls = nulls});
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Ascribe: {
            // The ascription fixes the statically visible result schema. Extra
            // physical columns pass through at run time, but are deliberately
            // hidden until a later ascription names them.
            //
            // It asserts nothing about nullability -- there is no surface
            // syntax to assert it with -- but it erases nothing either: an
            // ascription is an identity on the data, so a proof the input
            // carried is still true of the rows that come out. Assertion and
            // preservation are different questions, and answering the first
            // with "no" is not a reason to answer the second with it.
            const auto& asc = static_cast<const AscribeNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            std::vector<SchemaField> out = asc.schema();
            for (auto& field : out) {
                if (const auto* from_input = input.find(field.name)) {
                    field.nulls = from_input->nulls;
                }
            }
            return SchemaInfo::known(std::move(out), /*open=*/false);
        }

        case NodeKind::Columns:
            // Exposes child column names as a single String column named "name".
            // Every row is a column name, and a column always has one.
            return SchemaInfo::known({SchemaField{
                .name = "name", .type = ColumnType::String, .nulls = Nullability::Never}});

        case NodeKind::Melt: {
            // Output: the id columns (types from input), then `variable: String`
            // and `value` (the common type of the melted measure columns, when
            // statically determinable). The column set is fixed -> closed.
            const auto& melt = static_cast<const MeltNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            std::vector<SchemaField> out;
            for (const auto& id : melt.id_columns()) {
                // An id column is repeated once per measure, values unchanged.
                if (const auto* field = input.find(id)) {
                    out.push_back(*field);
                } else {
                    out.push_back(SchemaField{.name = id, .type = std::nullopt});
                }
            }
            // Each row's `variable` is the name of the column it was melted
            // from, so it is present by construction.
            out.push_back(SchemaField{
                .name = "variable", .type = ColumnType::String, .nulls = Nullability::Never});
            std::optional<ColumnType> value_type;
            Nullability value_nulls = Nullability::Maybe;
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
                // `value` holds one measure column's value per row, so it is
                // null-free only if every melted column is.
                value_nulls = measures.empty() ? Nullability::Maybe : Nullability::Never;
                for (const auto& measure : measures) {
                    const auto* field = input.find(measure);
                    value_nulls =
                        weaker(value_nulls, field != nullptr ? field->nulls : Nullability::Maybe);
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
            out.push_back(SchemaField{.name = "value", .type = value_type, .nulls = value_nulls});
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
            // One row per numeric input column, labelled with its name.
            out.push_back(SchemaField{
                .name = "column", .type = ColumnType::String, .nulls = Nullability::Never});
            for (const auto& field : input.fields()) {
                if (!field.type.has_value()) {
                    return SchemaInfo::unknown();  // can't tell if this column is numeric
                }
                if (is_numeric(*field.type)) {
                    // Left unclaimed however null-free the inputs are: a
                    // correlation with a zero-variance column divides by zero.
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
                // A generated bucket boundary, so never absent.
                out.push_back(SchemaField{
                    .name = *bucket, .type = ColumnType::Timestamp, .nulls = Nullability::Never});
            }
            for (const auto& key : rs.group_by()) {
                if (const auto* field = input.find(key.name)) {
                    out.push_back(*field);
                } else {
                    out.push_back(SchemaField{.name = key.name, .type = std::nullopt});
                }
            }
            for (const auto& spec : rs.aggregations()) {
                // Not `agg_nullability`: unlike `Aggregate`, a resample emits a
                // row for every bucket in the range, including buckets no input
                // row fell into. Its aggregates summarize an empty set as
                // readily as a non-empty one.
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
            return project_schema(fused.columns(),
                                  filtered_schema(fused.predicate(), child_schema(node, sources)));
        }
        case NodeKind::FilterUpdateProject: {
            // Project(Update(Filter(x))). The update's computed fields are only
            // observable through the projection, and a projected name resolves
            // against the update's output -- so type the update first, then
            // project it.
            const auto& fused = static_cast<const FilterUpdateProjectNode&>(node);
            SchemaInfo filtered = filtered_schema(fused.predicate(), child_schema(node, sources));
            SchemaInfo updated = update_schema(fused.fields(), {}, std::move(filtered));
            return project_schema(fused.project_columns(), updated);
        }
        // Head(Filter(x)) / Tail(Filter(x)): row-subsetting only, so schema,
        // time index and unique constraints pass through -- and the filter's
        // proofs come through with them, as they do for the unfused shapes.
        case NodeKind::FilterHead:
            return filtered_schema(static_cast<const FilterHeadNode&>(node).predicate(),
                                   child_schema(node, sources));
        case NodeKind::FilterTail:
            return filtered_schema(static_cast<const FilterTailNode&>(node).predicate(),
                                   child_schema(node, sources));
        // Head(Order(x)): no predicate, so nothing to prove.
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
// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

namespace {

auto missing_column(std::string_view clause, const std::string& name) -> std::string {
    return std::string(clause) + ": column '" + name + "' not found in input";
}

// Collect the column names referenced by an expression (value or predicate).
// Function callees and bound scalars are filtered out by the caller against the
// schema / lexical bindings; here we just gather every ColumnRef name.
void collect_expr_columns(const Expr& expr, std::vector<ColumnRef>& out) {
    std::visit(
        [&](const auto& node) {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                out.push_back(node);
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
                    out.push_back(ColumnRef{.name = key.name});
                }
            }
        },
        expr.node);
}

}  // namespace

auto check_column_refs(const Node& node, const SourceSchemas& sources,
                       const robin_hood::unordered_set<std::string>& lexical_names,
                       bool check_expressions) -> std::optional<std::string> {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast) -- every cast below is
    // guarded by a node.kind() check (or switch) matching the target node type.
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

    // A Known schema contains every column that may be referenced statically.
    // An open schema may carry extra physical columns, but those are anonymous
    // until a later ascription names them.
    const SchemaInfo input = node.children().empty()
                                 ? SchemaInfo::unknown()
                                 : infer_schema(*node.children().front(), sources);
    if (!input.is_known()) {
        return std::nullopt;
    }

    // A name in an expression position is valid if it is a column of the input
    // or any in-scope lexical binding (scalar etc.).
    auto resolvable = [&](const std::string& name) {
        return input.find(name) != nullptr || lexical_names.contains(name);
    };

    // `^name` skips column scope entirely, so it must name a lexical binding.
    auto check_refs = [&](std::string_view clause,
                          const std::vector<ColumnRef>& refs) -> std::optional<std::string> {
        for (const auto& ref : refs) {
            if (ref.lexical) {
                if (!lexical_names.contains(ref.name)) {
                    return std::string(clause) + ": '^" + ref.name +
                           "' does not resolve to a lexical binding";
                }
                continue;
            }
            if (!resolvable(ref.name)) {
                return missing_column(clause, ref.name);
            }
        }
        return std::nullopt;
    };

    if (check_expressions && node.kind() == NodeKind::Filter) {
        std::vector<ColumnRef> refs;
        collect_expr_columns(static_cast<const FilterNode&>(node).predicate(), refs);
        if (auto err = check_refs("filter", refs)) {
            return err;
        }
    }
    if (check_expressions && node.kind() == NodeKind::Update) {
        std::vector<ColumnRef> refs;
        for (const auto& field : static_cast<const UpdateNode&>(node).fields()) {
            collect_expr_columns(field.expr, refs);
        }
        if (auto err = check_refs("update", refs)) {
            return err;
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
    // NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)
    return std::nullopt;
}

}  // namespace ibex::ir
