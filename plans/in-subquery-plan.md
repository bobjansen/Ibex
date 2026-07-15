---
name: in_subquery
description: "Proposal: x in (table_expr) / not in as a semi / null-aware anti join — the subquery family, NOT a scalar like like(). Unblocks q18, q20."
metadata:
  node_type: memory
  type: project
---

# Proposal: `in` / `not in` subquery terms

Status: **proposed**, not built. Reuses the capture/decorrelation machinery of
the shipped scalar subquery (`plans/correlated-subquery-q02-plan.md`, SPEC 5.7)
and is a close sibling of the `exists` proposal (`plans/exists-subquery-plan.md`).

## `in` is not a scalar, and there are two of them

It is tempting to model `in` the way `like()` is modelled — a row-wise scalar
builtin in `scalar_builtins()`. That is right for exactly one form and wrong for
the one that matters.

- **`x in (v1, v2, …)` — a literal value list.** This *is* a `like()` sibling:
  row-wise, RHS known at eval time, one Bool per row. But **no PDS-H query needs
  it** — q19 already hand-expands its `IN`-lists to `||`-chains of equality. Nice
  to have, not a query-unblocker. If built, it belongs in `scalar_builtins()`.

- **`x in (table_expr)` — membership against a one-column subquery.** This is
  what q18 and q20 use, and it is not a scalar at all. A scalar builtin has its
  arguments evaluated to *values* per row; here the RHS is a whole *table* probed
  set-at-a-time. Forcing it through the row-wise path means re-scanning the
  subquery per outer row — the nested-loop trap the whole subquery design
  refuses. It belongs in the **subquery/join family**.

The rest of this plan is only about the second form.

## Semantics: `in` is a semi join, `not in` is a null-aware anti join

`x in (S)` ≡ `exists(S[filter s_col == outer(x)])`. So it is Tier 1 of the
`exists` proposal with one equality synthesized between the outer value and the
subquery's single column:

```
filter <local> && x in (S)

    SemiJoin(on x)
      Filter(<local>)(outer)
      Rename(s_col -> x)
        <lowered S>
```

`not in` is an `AntiJoin` — but not a plain one. This is the whole reason the
proposed README says it "must not be silently implemented as a plain anti join."

### The three-valued truth, and where it hides

| predicate | result |
|---|---|
| `x IN (S)`     | TRUE on a match; FALSE on no match if S has no null; **UNKNOWN** on no match if S contains a null |
| `x NOT IN (S)` | FALSE on a match; **UNKNOWN** on no match if S contains a null; TRUE on no match if S has no null |

A `filter` keeps TRUE and drops both FALSE and UNKNOWN. Reading the table through
that lens:

- **`in` (positive) needs no null handling at all.** "Keep on a match" is exactly
  what a plain semi join does — equality never matches a null, so the
  UNKNOWN-vs-FALSE distinction is invisible in a filter. **A plain semi join is
  already correct.**

- **`not in` diverges from a plain anti join in two ways:**
  1. **If S contains any null, `not in` keeps *no* rows** — every row is either a
     match (FALSE) or a non-match-against-a-null (UNKNOWN). A plain anti join
     would return the non-matches.
  2. **A null on the probe side (`x` is null) drops**, because `null = anything`
     is UNKNOWN. A plain anti join treats a null `x` as unmatched and *keeps* it.
     (Verified: our anti join keeps unmatched rows, and the null-key work makes a
     null key its own non-matching group — so it survives an anti join today.)

### When a plain anti join is exact — and q16 proves it

If **both** the subquery column and the outer key are non-nullable, neither
divergence can fire, and `not in` *is* a plain anti join. That is not
hypothetical: **q16 already ships `s_suppkey not in (…)` as a plain `anti join`**
and passes the official answer check, because `s_suppkey` is a primary key. The
null-aware version is only needed when the subquery column can actually be null.

So the deliverable is a **null-aware anti join**: an anti join that (1) yields
empty when its build column contains a null, and (2) drops null probe keys. Give
it a distinct spelling in the IR (a flag on the existing anti-join lowering, or a
dedicated node) so the existing `anti join` operator's cheaper, null-oblivious
semantics stay available for the q16 case, where they are provably fine.

## V1 restrictions (each rejected with a diagnostic)

Mirroring the scalar subquery's boundary:

- **Filter-position only, as a top-level conjunct.** All three PDS-H `in`s sit at
  the top level of a filter. `in` under an `||`, or negated inside a larger
  boolean, needs the mark-join / null-aware-boolean-column form — defer it, same
  as `exists` Tier 2.
- **Bare-column LHS.** `x` must be a column, so the semi/anti join is a plain
  equijoin. A computed LHS (`a + b in (S)`) must be materialised first.
- **One-column subquery.** The RHS must produce exactly one column.

```ibex
select { flag = x in (S) };   // boolean subqueries live in filters, for now
(a + b) in (S);               // LHS must be a bare column
x in (t[select { a, b }]);    // subquery must have one column
```

## Correlated vs uncorrelated, and a freebie

`in` correlates the same way `scalar`/`exists` do: if S captures with `outer(...)`
it is per-key; if not it is a fixed set. **Both q18 and q20 use only uncorrelated
`in`s** — the RHS is a computed set that does not reference the outer row.

An uncorrelated positive `in` is therefore a plain semi join against a fixed
subquery, which — exactly like q04's `exists` — **is expressible today without the
feature**: compute the set into a `let`, rename its column to the key, and
`semi join` on it. So q18 is a q04-shaped freebie whenever we want it, and q20's
real blocker is not a missing primitive but the intricacy of assembling three
nested subqueries (two `in`s and the already-working correlated `scalar`) by
hand. `in` syntax buys readability here, and the null-aware anti join buys
correctness for the general `not in` — neither q18 nor q20 needs that second part.

## Parser / AST

`in` is infix (`value in ( table_expr )`), unlike the call-shaped `scalar(...)`
and `exists(...)`. `in` is already a soft keyword (`map … in`). Parse it at
comparison precedence with a value-expression LHS and a parenthesised
table-expression RHS, and lower it through the same filter-conjunct splitter that
handles `scalar`/`exists`, synthesising the `x == s_col` equality. `not in` is
the negation; reuse `!`-style handling rather than adding `not` as a prefix
operator (Ibex has none — negation is `!`, cf. q13's `!like(...)`).

## Test plan

- parser: `x in (S)`, `x not in (S)`, and the three rejections above;
- lower: `in` → Semi join, no aggregate added; `not in` → the null-aware anti
  join variant, distinct from the plain one;
- a positive `in` keeps each outer row at most once (no duplication);
- `not in` against a subquery **with** a null in its column keeps no rows;
- `not in` against a **null-free** subquery equals a plain anti join (the q16
  case) — and a null probe key still drops;
- interpreter/codegen parity;
- no duplicated source read for a binding used by both sides.

## Priority

This is the highest-value subquery increment left: two queries (q18, q20), and
it makes q16's hand-rolled anti join expressible as the `not in` it came from.
Ranked against its neighbours: **`in`/`not in` (q18, q20) > `exists` (0 new
queries, ergonomics only) > `exists` Tier 3 (q21, needs `row_number()`)**. The
positive-`in` half is small (it reuses the semi join outright); the null-aware
anti join is the one genuinely new operator, and no current query needs it.
