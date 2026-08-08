// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/nullability.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

/// Null-free only if every operand is.
template <typename... Operands>
auto all_present(const SchemaInfo& input, const Operands&... operands) -> Nullability {
    Nullability result = Nullability::Never;
    ((result = weaker(result, expr_nullability(*operands, input))), ...);
    return result;
}

}  // namespace

auto expr_nullability(const Expr& expr, const SchemaInfo& input) -> Nullability {
    if (const auto* col = std::get_if<ColumnRef>(&expr.node)) {
        // A lexical `^name` is a scalar binding, not a column, and this pass
        // knows no more about its value than `expr_type` knows about its type.
        if (col->lexical) {
            return Nullability::Maybe;
        }
        const auto* field = input.find(col->name);
        return field != nullptr ? field->nulls : Nullability::Maybe;
    }
    if (std::holds_alternative<Literal>(expr.node)) {
        // The surface language has no null literal to write.
        return Nullability::Never;
    }
    if (std::holds_alternative<IsNullExpr>(expr.node)) {
        // Total: `is_null(x)` answers true or false for a null rather than
        // propagating it.
        return Nullability::Never;
    }
    if (const auto* cmp = std::get_if<CompareExpr>(&expr.node)) {
        // Three-valued: a null operand makes the comparison null, not false.
        return all_present(input, cmp->left, cmp->right);
    }
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node)) {
        // Not Kleene-precise: `false && null` is false whatever the right
        // operand does, but proving that needs a value-level argument this pass
        // does not make. `Not` carries no right operand (see `LogicalExpr`).
        const Nullability left = expr_nullability(*logical->left, input);
        return logical->right != nullptr ? weaker(left, expr_nullability(*logical->right, input))
                                         : left;
    }
    if (const auto* bin = std::get_if<BinaryExpr>(&expr.node)) {
        // Including Div: integer division by zero is an error and float
        // division yields an infinity, so neither manufactures a null.
        return all_present(input, bin->left, bin->right);
    }
    if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
        // The registry is the authority on what a built-in does to nulls; this
        // pass only turns its answer into a lattice value. `nullopt` is an
        // unknown callee or a non-row-local one -- no claim either way.
        const auto behavior = scalar_null_behavior(call->callee);
        if (!behavior.has_value() || call->args.empty()) {
            return Nullability::Maybe;
        }
        switch (*behavior) {
            case NullBehavior::Introduces:
                return Nullability::Maybe;
            case NullBehavior::Absorbs:
                // One argument known present is enough to carry every row.
                return std::ranges::any_of(call->args,
                                           [&](const auto& arg) {
                                               return expr_nullability(*arg, input) ==
                                                      Nullability::Never;
                                           })
                           ? Nullability::Never
                           : Nullability::Maybe;
            case NullBehavior::Propagates:
                Nullability result = Nullability::Never;
                for (const auto& arg : call->args) {
                    result = weaker(result, expr_nullability(*arg, input));
                }
                return result;
        }
        return Nullability::Maybe;
    }
    return Nullability::Maybe;
}

auto agg_nullability(const AggSpec& agg, const SchemaInfo& input, bool grouped) -> Nullability {
    if (agg.func == AggFunc::Count) {
        // A row count. Nothing it reads can make it absent, and an empty group
        // counts zero rather than null.
        return Nullability::Never;
    }
    if (!grouped) {
        return Nullability::Maybe;
    }
    const auto* field = input.find(agg.column.name);
    if (field == nullptr || field->may_be_null()) {
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
            // A value drawn from, or a mean over, at least one present value.
            return Nullability::Never;
        case AggFunc::Stddev:
        case AggFunc::Ewma:
        case AggFunc::Quantile:
        case AggFunc::Skew:
        case AggFunc::Kurtosis:
            // Under-claimed on purpose: each is undefined for a group below
            // some size and this pass has no bound on group size.
            return Nullability::Maybe;
        case AggFunc::Count:
            break;  // handled above
    }
    return Nullability::Maybe;
}

namespace {

/// The columns whose nullness would make `expr` evaluate to null. The mirror of
/// `expr_nullability`: that answers "is this null-free", this answers "which
/// columns would have to be".
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
        // Only a propagating call requires its arguments to be present, and the
        // registry decides which those are -- the same answer `expr_nullability`
        // reads, so the two walkers cannot disagree about a built-in.
        if (scalar_null_behavior(call->callee) != NullBehavior::Propagates) {
            return;
        }
        for (const auto& arg : call->args) {
            collect_null_propagating_refs(*arg, out);
        }
        return;
    }
    // Literal: no column. IsNullExpr: total, so its operand's nullness does not
    // reach the result. LogicalExpr and RankExpr: handled by the caller, which
    // can say something stronger about a conjunction.
}

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
            // A null test is total, so negating one is still a statement about
            // presence: `!is_null(x)` says what `is_not_null(x)` says. Worth
            // the case because it is not an exotic spelling -- dplyr's
            // `filter(!is.na(x))` lowers to exactly this.
            if (const auto* inner = std::get_if<IsNullExpr>(&logical->left->node)) {
                if (!inner->negated) {
                    collect_null_propagating_refs(*inner->operand, out);
                }
            }
            return;
        }
        // `||` proves only what both branches prove, and is left unclaimed
        // rather than intersected: an under-claim costs precision only.
        return;
    }
    if (const auto* is_null = std::get_if<IsNullExpr>(&predicate.node)) {
        // `is_null(x)` true is exactly the rows a proof would exclude.
        if (is_null->negated) {
            collect_null_propagating_refs(*is_null->operand, out);
        }
        return;
    }
    // Anything else -- a comparison, a bare Bool column, an arithmetic
    // expression read as a predicate -- had to be non-null to be true.
    collect_null_propagating_refs(predicate, out);
}

/// Whether a join may emit a row in which one side contributed nothing, per
/// side. The only reason a join weakens a proof: an unmatched row's columns on
/// the absent side are filled with nulls whatever that input's schema said.
struct JoinUnmatched {
    bool left;
    bool right;
};

auto join_unmatched_sides(JoinKind kind) -> JoinUnmatched {
    switch (kind) {
        case JoinKind::Left:
        case JoinKind::Asof:
            // `asof` keeps every left row whether or not a right row precedes
            // it, so its right columns null out as a left join's do.
            return {.left = false, .right = true};
        case JoinKind::Right:
            return {.left = true, .right = false};
        case JoinKind::Outer:
            return {.left = true, .right = true};
        case JoinKind::Inner:
        case JoinKind::Semi:
        case JoinKind::Anti:
        case JoinKind::Cross:
            // Semi and anti emit whole left rows; cross pairs every row with
            // every row. None of them fills.
            return {.left = false, .right = false};
    }
    return {.left = true, .right = true};
}

}  // namespace

auto filter_proved_non_null(const Expr& predicate) -> robin_hood::unordered_set<std::string> {
    robin_hood::unordered_set<std::string> proved;
    collect_filtered_non_null_refs(predicate, proved);
    return proved;
}

void apply_join_nullability(const JoinNode& join, const SchemaInfo& left, const SchemaInfo& right,
                            const std::vector<JoinOutputColumn>& plan,
                            std::vector<SchemaField>& out) {
    const JoinUnmatched unmatched = join_unmatched_sides(join.kind());
    // Under `nulls never` a null key matches nothing, so a join that emits only
    // matched rows has a value in every key of every row it emitted. That is a
    // proof neither input carried. `nulls equal` is precisely the option that
    // makes a null key match, and withdraws it.
    const bool keys_are_null_free =
        (join.kind() == JoinKind::Inner || join.kind() == JoinKind::Semi) &&
        join.null_match() == NullMatch::Never && !join.keys().empty();

    // A folded key is one output column two inputs feed, so what can put a null
    // in it is exactly what survives. A matched pair contributes a key that
    // matched, and under `nulls never` a null key matches nothing -- leaving
    // only the unmatched rows the kind preserves, each carrying its own side's
    // key. So a kind that drops a side's unmatched rows does not inherit that
    // side's missing proof: a right join's folded key is as proved as the right
    // input's, whatever the left's says.
    const auto folded_key_nullability = [&](Nullability own, Nullability peer) {
        if (join.null_match() != NullMatch::Never) {
            // `nulls equal` is the option that lets a null key match, so a
            // matched row proves nothing here and both sides' rows count.
            return weaker(own, peer);
        }
        Nullability result = Nullability::Never;
        if (unmatched.right) {  // left-only rows, carrying the left key
            result = weaker(result, own);
        }
        if (unmatched.left) {  // right-only rows, carrying the right key
            result = weaker(result, peer);
        }
        return result;
    };

    for (std::size_t i = 0; i < plan.size(); ++i) {
        const JoinOutputColumn& column = plan[i];
        if (column.folded_peer_index.has_value()) {
            const SchemaInfo& peer = column.side == JoinOutputSide::Left ? right : left;
            const auto& peer_fields = peer.fields();
            const Nullability peer_nulls = *column.folded_peer_index < peer_fields.size()
                                               ? peer_fields[*column.folded_peer_index].nulls
                                               : Nullability::Maybe;
            out[i].nulls = folded_key_nullability(out[i].nulls, peer_nulls);
            continue;
        }
        // An unfolded key column -- a mapped key, or either side's native
        // column when the names differ -- still holds only keys that matched.
        if (column.is_key && keys_are_null_free) {
            out[i].nulls = Nullability::Never;
            continue;
        }
        const bool own_side_may_be_missing =
            column.side == JoinOutputSide::Left ? unmatched.left : unmatched.right;
        if (!own_side_may_be_missing) {
            continue;
        }
        out[i].nulls = Nullability::Maybe;
    }
}

}  // namespace ibex::ir
