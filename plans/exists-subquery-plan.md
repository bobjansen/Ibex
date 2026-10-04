---
name: exists_subquery
description: "Proposal: exists(table_expr) as a boolean subquery term — semi/anti join, mark join, and the residual-predicate case (q21's shape)"
metadata:
  node_type: memory
  type: project
---

# Proposal: `exists` subquery terms

Status: **Tier 1 built** (cdff12fa, SPEC 5.8); Tiers 2 and 3 proposed. Follows the shipped correlated scalar subquery
(`plans/done/correlated-subquery-q02-plan.md`, SPEC 5.7), whose capture machinery
this reuses wholesale.

Revised 2026-10-04: every TPC-H query this plan used to call "blocked" (q11,
q18, q20, q21) now ships as a hand-written relational form, and uncorrelated
`scalar` has landed. The motivation and priority sections below reflect that.

## Start here: `exists` buys no new queries by itself

The obvious motivation for `exists` is q04, and **q04 does not need it**. Its
`exists` is an equality-correlated subquery, which *is* a semi join, and Ibex has
had `semi join` since long before subqueries:

```sql
and exists (select * from lineitem
            where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
```

```ibex
orders semi join lineitem[filter l_commitdate < l_receiptdate][select { o_orderkey = l_orderkey }]
    on o_orderkey
```

q04 shipped that way and passes the official SF-1 answer check. The same is true
of `not exists`, which is exactly `anti join` (verified: an outer row with three
matches survives a semi join once, and is dropped by an anti join).

So this proposal must be honest about what it is for. `exists` is:

- **Ergonomics** for the common case — writing the correlation where SQL writes
  it, instead of hand-decorrelating into a semi join and hoisting the inner-only
  predicates out by eye. Real value, but no new capability.
- **Expressiveness** for one shape: a correlation that a semi join *cannot*
  express, which is q21's. That is Tier 3 below. It no longer unblocks a query:
  q21 ships as a set-based rewrite (per-order supplier counts; see the header of
  `benchmarking/tpch/queries/q21.ibex`). What Tier 3 buys is writing such a
  query without finding that rewrite by hand — and without getting it subtly
  wrong, as the shipped q21's Polars-matching line-count proxy shows.

So this is a language-quality feature, not a coverage one — see "Priority" at
the end.

## Syntax

```ibex
exists(t[filter k == outer(j) && <local terms>])
!exists(t[filter k == outer(j)])
```

`exists` takes one table-expression argument and yields a Bool. It is a plain
identifier call, like `scalar` — no lexer change. Negation is the existing `!`,
as in q13's `!like(...)`; `not` is not a general prefix operator in Ibex (it
appears only in `is not null`), and adding `not exists` as a second spelling
would buy nothing but a second way to say it.

Unlike the scalar subquery, `exists` has **no select clause**: it asks whether a
row exists, so a projection would be meaningless. Only `filter` is permitted.

### Nulls

**`exists` is never null.** It is true or false, even when the subquery's
columns are null, and even when the subquery is empty. This is the one place it
differs sharply from its neighbours:

- `scalar(...)` yields null when no inner row matches, and `x == null` drops the row.
- `x in (...)` yields *unknown* when the subquery contains a null, and `x not in (...)`
  with a null in the subquery is famously never true.

Those three must **not** share null rules even though they will share join
machinery. Writing that down now is the point of writing it down at all.

## Lowering, in three tiers

All three reuse the capture-splitting already built for `scalar()`: partition the
subquery's filter conjuncts into *captures* (terms mentioning `outer(...)`) and
*local terms* (terms mentioning only the subquery's own columns). Local terms
always filter the inner side before any join.

### Tier 1 — a top-level conjunct, equality captures → Semi / Anti join

The common case, and what q04 hand-writes today.

```
filter local && exists(inner[filter inner_local && k == outer(j)])

    SemiJoin(on j)
      Filter(local)(outer)
      Rename(k -> j)
        Project({k})
          Filter(inner_local)(inner)
```

`!exists(...)` emits `AntiJoin` instead. Two properties fall out, both worth
noting because they are *better* than the scalar subquery's:

- **No generated column, so no schema widening.** The scalar subquery has to
  project `__ibex_scalar_N` back off, which forces its "the outer schema must be
  statically known" restriction. `exists` has no such restriction.
- **No aggregate.** A semi join is a hash probe, strictly cheaper than the
  scalar subquery's group-by-then-left-join.

The subquery may capture several columns (`k1 == outer(j1) && k2 == outer(j2)`),
giving a multi-key semi join — composite captures already work for `scalar()`.

**Buys: q04, and q22's `not exists`. Neither is newly unblocked.**

### Tier 2 — any other boolean position → mark join

A semi join is a *row filter*, not a *boolean value*, so it cannot express
`exists` under an `||`, or negated inside a larger expression:

```ibex
filter o_priority == "1-URGENT" || exists(lineitem[filter l_orderkey == outer(o_orderkey)])
```

Here `exists` must become an actual Bool column. That is a **mark join**, and it
is the scalar-subquery lowering almost verbatim — swap `min(v)` for a constant
aggregate and null-test the result:

```
    Project(outer columns)                 -- strip the generated column
      Filter(o_priority == "1-URGENT" || __ibex_exists_0 is not null)
        Join(Left, on o_orderkey)
          outer
          Rename(l_orderkey -> o_orderkey)
            Aggregate(by l_orderkey: count() as __ibex_exists_0)
```

An outer row with no match gets a null, `is not null` renders it false, and the
predicate composes normally. Because this widens the schema, it inherits the
scalar subquery's closed-outer-schema requirement and its trailing projection —
both already built.

Tier 1 is then a *fast path*: when the `exists` is a top-level conjunct, use the
semi/anti join; otherwise fall back to the mark join. The two must agree, which
is a good parity test.

**Buys: generality. No TPC-H query needs it.**

### Tier 3 — a residual correlated predicate → equijoin + residual filter + dedup

This is the one tier a semi join cannot replace, and q21 is the motivating shape:

```sql
and exists (select * from lineitem l2
            where l2.l_orderkey = l1.l_orderkey
              and l2.l_suppkey <> l1.l_suppkey)      -- <-- not an equality
```

Tier 1 cannot touch this. A semi join on `l_orderkey` alone is not merely
imprecise, it is **vacuously true**: every `l1` row matches *itself*, because it
shares its own order key. The `<>` against the *outer* row is what carries the
meaning ("some **other** supplier is on this order"), and it can only be
evaluated once both rows are in hand.

The lowering pairs the rows on the equality captures, then applies the rest:

1. give the outer a synthetic identity column (`__ibex_row_0`);
2. rename the subquery's columns to generated names, so the residual can name
   both sides without colliding (`l_suppkey` vs `__ibex_inner_l_suppkey`) —
   relying on the join's `_right` suffixing would be fragile;
3. `Join(Inner, on <equality captures>)` — a hash join, *not* a nested loop;
4. `Filter(<residual captures>)` — now a plain predicate over both sides;
5. `Project({__ibex_row_0})`, `Distinct` — the outer rows with ≥1 surviving pair;
6. `SemiJoin` (or `AntiJoin` for `!exists`) the outer against that on
   `__ibex_row_0`, then project it back off.

The cost is bounded by the fan-out of the equality key — for q21, the ~4 line
items of an order — so this stays a hash join with a filter, never an O(N*M)
scan. **An `exists` whose captures contain no equality at all is a genuine
nested loop, and must be rejected with a diagnostic rather than silently
executed.** That rule is the same one the scalar subquery already enforces, and
it is the line this whole design refuses to cross.

**Row identity:** step 1 needs a column that is unique per outer row. The
runtime already computes one: `rank(x, method = first)` assigns distinct
ordinals (`RankMethod::First` in `src/runtime/rank_window.cpp`). But `rank` needs
an ordering column, and the outer may have no unique one, so the lowering wants
an internal row-index builtin (positional, no sort) rather than the user-facing
`rank`. That is small, but it is still a step to schedule, and it must stay
correct under morsel-parallel evaluation (indices assigned per chunk need a
global offset).

**Buys: q21 written like its SQL** (with its `not exists` sibling, same
machinery). The shipped q21 is a hand-written approximation of exactly this.

## Rejections

Each with a diagnostic, in the style the scalar subquery already uses:

```ibex
exists(t[select { x }]);            // a select clause is meaningless in exists
exists(t[filter x > 0]);            // no capture — uncorrelated; see below
exists(t[filter x > outer(y)]);     // no EQUALITY capture — a nested loop; rejected
update { flag = exists(...) };      // boolean subqueries live in filters, for now
```

An *uncorrelated* `exists` is well-defined (true iff the subquery has any row)
but it is a constant. Reject it in V1. If it is wanted later, it lowers the
way uncorrelated `scalar` already does (SPEC 5.7): a one-row aggregate
(`count() > 0`) cross-joined onto the outer.

## Test plan

Mirroring what the scalar subquery shipped with:

- parser: `exists(...)`, `!exists(...)`, and the diagnostics above;
- lower: the Tier-1 plan shape contains a `Semi`/`Anti` join and **no**
  aggregate; the Tier-2 shape contains a Left join and a generated column;
- **Tier 1 and Tier 2 must agree on every input** — the same query written as a
  conjunct and under a trivial `|| false` must return the same rows;
- an outer row with *many* matching inner rows appears exactly once (the
  duplication bug a naive inner join would introduce);
- Tier 3: a self-referential subquery (q21's shape) must not match a row against
  itself;
- interpreter/codegen parity case;
- no duplicated source read for a binding used by both sides.

## Priority — read this before building it

Every TPC-H subquery shape is now covered without subquery syntax:

| SQL shape | Query | How it ships today |
|---|---|---|
| uncorrelated `scalar` | q11, q22 | `cross join` against a one-row aggregate (syntax built, SPEC 5.7) |
| correlated `scalar` | q02, q17 | group-by + join (syntax built, SPEC 5.7) |
| `exists`, equality | q04 | `semi join` |
| `not exists`, equality | q22 | anti join (left join + `is_null`) |
| `in` / `not in` | q16, q18, q20 | `semi join` / anti join |
| `exists` with `<>` | q21 | set-based rewrite (Polars line-count shape) |

(The queries follow Polars' explicit relational shapes for benchmark
comparability — "Match query shapes", 482eb583 — so even the built `scalar`
syntax is not what they use.) Nothing on the subquery track unblocks a query;
it is all language quality. Ranked on that basis:

| Work | Value | Notes |
|---|---|---|
| `exists` Tier 1 | q04, q22 read like their SQL | Small and self-contained: capture split exists, emits a Semi/Anti join, no schema widening. |
| `in` / `not in` (`plans/in-subquery-plan.md`) | q16, q18, q20 read like their SQL | Shares Tier 1's semi/anti lowering — the one real argument for building them together. The `not in` null rule is the trap. |
| `exists` Tier 2 | generality | Mark join; mostly the scalar-subquery lowering reused. Needed for parity testing of Tier 1. |
| `exists` Tier 3 | q21 exact, without a hand rewrite | Needs the internal row-index builtin (see Tier 3). Most expensive part. |

The honest summary: **`exists` is a readability feature, and so is everything
left on the subquery track.** If it is built, start with Tier 1 (ideally
alongside `in`), add Tier 2 for parity, and treat Tier 3 as optional.
