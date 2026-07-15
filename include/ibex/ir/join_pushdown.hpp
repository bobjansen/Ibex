#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Predicate pushdown through joins.
///
/// Rewrites `Filter(pred, Join(kind, keys, a, b))` by splitting `pred` into its
/// top-level AND conjuncts and moving each conjunct below the join when its
/// column references are provably confined to one side:
///
///   Filter(pred, Join(a, b))
///     → [Filter(rest,)] Join(Filter(pred_a, a), Filter(pred_b, b))
///
/// This is the difference between joining 6M rows and filtering the result,
/// and filtering 6M rows down to a few hundred thousand and joining those
/// (PDS-H q19: join+filter 951ms → 37ms measured by hand).
///
/// Unlike `canonicalize`, this rule needs schemas: whether a conjunct belongs
/// to the left or the right side depends on which columns each side produces.
/// It therefore runs as a separate schema-aware pass after lowering — where
/// `SourceSchemas` for every in-scope binding is available — and must run
/// before `required_columns`, so projection pushdown analyses the final tree.
///
/// A conjunct moves only when ALL of the following hold:
///   - it is row-local (`is_subset_evaluable_expr`): no Transform, Generator,
///     Aggregate, or rank call, whose value depends on rows beyond its own;
///   - its refs are all found in the receiving side's Known schema;
///   - for a push to the RIGHT side, every ref that the left side also
///     produces is a join key. Above the join a shared name resolves to the
///     LEFT column, so a non-key shared ref evaluated on the right would read
///     different values. This test needs a closed (non-open) left schema.
///
/// Join-kind safety (see plans/join-predicate-pushdown-plan.md):
///   - Inner: push both sides; a conjunct whose refs are all join keys present
///     on both sides is duplicated to BOTH sides (keys are equal on surviving
///     rows by construction).
///   - Left:  push left-side conjuncts only. Pushing a right-side predicate
///     would let non-matching left rows survive null-extended instead of
///     being filtered out.
///   - Right: mirror image — push right-only conjuncts. Key conjuncts stay
///     above: for an unmatched right row the output key column (owned by the
///     left side) is null-extended, so above/below disagree.
///   - Outer, Semi, Anti, Cross, Asof: never rewritten.
///
/// Conjuncts that reference a name in neither schema (a lexical scalar, an
/// unknown column) stay above the join. Pure on IR: takes ownership and
/// returns the rewritten tree.
[[nodiscard]] auto push_filters_into_joins(NodePtr root, const SourceSchemas& sources) -> NodePtr;

/// Semi/anti-join pushdown through an inner join.
///
/// Rewrites `(X ⋈ Y) ⋉ Z on k` into `(X ⋉ Z on k) ⋈ Y` when the semi/anti join's
/// keys `k` are all confined to one side of the inner join:
///
///   Join(Semi, k, Join(Inner, k', X, Y), Z)
///     → Join(Inner, k', Join(Semi, k, X, Z), Y)      (k ⊆ X)
///
/// A semi/anti join adds no columns and depends only on its left key values,
/// which an inner join leaves untouched, so the pair set is identical. The
/// point is to filter before the join instead of after: PDS-H q18 as written
/// joins all 6M line items and keeps 57 orders (~430ms), versus semi-filtering
/// to 57 orders and joining a few hundred line items (~17ms).
///
/// Which side receives the push follows the same schema rules as
/// `push_filters_into_joins`: a left push needs the keys in the Known left
/// schema; a right push additionally needs a closed Known left proving each key
/// is absent from the left or is an inner-join key. Only an equi Inner join is
/// descended through (Left/Right/Outer decide left-row survival, which the
/// semi/anti filter would race). Runs after `push_filters_into_joins` and before
/// `required_columns`. Pure on IR: takes ownership and returns the rewritten
/// tree.
[[nodiscard]] auto push_semi_joins_down(NodePtr root, const SourceSchemas& sources) -> NodePtr;

}  // namespace ibex::ir
