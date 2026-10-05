---
name: in_subquery
description: "Proposal: x in (table_expr) / !(x in ...) as a semi / null-aware anti join, a sibling of the built exists terms. Readability for q18/q20/q16, not new queries."
metadata:
  node_type: memory
  type: project
---

# Proposal: `in` subquery terms

Status: **proposed**, not built. Revised 2026-10-05 against the tree after
`exists` Tiers 1 and 2 shipped (SPEC 5.8, `ef1594cd`). It reuses the
capture/decorrelation machinery of the scalar subquery (SPEC 5.7,
`plans/done/correlated-subquery-q02-plan.md`) and the whole-conjunct semi / anti
lowering of `exists` (`lower_filter` → `lower_exists` in `src/parser/lower.cpp`).

## What it buys

All 22 PDS-H queries already run, so `in` unblocks no query. It buys
readability and a closer match to the SQL in three of them:

- **q18** writes its uncorrelated `o_orderkey in (…)` by hand: compute the set
  into a `let`, then `orders semi join big_orders on { o_orderkey = l_orderkey }`.
- **q16** writes its `s_suppkey not in (…)` as `left join … filter
  is_null(excluded_suppkey)` (its header comment still says "anti join"; it is
  not).
- **q20** is written as Polars' explicit join shape, deliberately, to keep the
  engine comparison about execution. `in` would let a *readable* q20 exist
  beside it, not replace it.

So this is ergonomics plus one correctness primitive (the null-aware anti join)
that no current query needs. Rank it accordingly: below any measured perf work.

## `in` is not a scalar, and there are two of them

- **`x in (v1, v2, …)` — a literal value list.** A row-wise `like()` sibling
  that belongs in `scalar_builtins()`. q16 and q19 would drop their `||` chains
  of equality. Out of scope here; a separate, small builtin.
- **`x in (table_expr)` — membership against a one-column subquery.** Not a
  scalar: the RHS is a whole table probed set-at-a-time. Through the row-wise
  path it would re-scan the subquery per outer row, the nested-loop trap the
  subquery design refuses. It belongs in the subquery/join family, and the rest
  of this plan is only about it.

## Semantics: `in` is a semi join, `!(x in …)` a null-aware anti join

`x in (S)` keeps a row when some row of S equals `x`. With `s` the single
column of S, it is `exists(S[filter s == outer(x)])` with that one equality
synthesized:

```
filter <local> && x in (S)

    Join(Semi, on { x = s })
      Filter(<local>)(outer)
      <lowered S>
```

### The three-valued truth, and where it hides

| predicate | result |
|---|---|
| `x in (S)`    | TRUE on a match; FALSE on no match if S has no null; **UNKNOWN** on no match if S has a null, or if `x` is null |
| `!(x in (S))` | FALSE on a match; **UNKNOWN** in the same two cases; TRUE otherwise |

A `filter` keeps TRUE and drops FALSE and UNKNOWN alike, so:

- **Positive `in` as a whole conjunct needs no null handling.** "Keep on a
  match" is a plain semi join; equality never matches a null.
- **Negated `in` as a whole conjunct differs from a plain anti join twice:**
  1. if S contains any null, it keeps **no** rows;
  2. a row whose `x` is null **drops**. A plain anti join (`NullMatch::Never`)
     treats a null probe key as unmatched and keeps it.

### The null-aware anti join is a run-time flag, not a planning choice

The earlier draft kept the plain anti join "for the q16 case, where `s_suppkey`
is provably non-null". The planner cannot prove that. The IR does track
nullability (`SchemaField::nulls`, rules in `ir/nullability.hpp`), but as a
proof by construction, never a source's promise: a file column is always
`Maybe`, so a primary key read from Parquet is too. A plan-time `Never` on S's
column would let the planner drop the flag, but the run-time check is cheap
enough that a second path is not worth it:

- the build side visits every right key already (the streaming operator in
  `src/runtime/semi_anti_join.cpp` skips null right keys at exactly that point),
  so "the build saw a null → emit nothing" is one flag set at build time;
- "drop a null probe key" is one validity test on a key the probe reads anyway.

So: one new `JoinNode` setting for the anti join — a third `NullMatch` value
(e.g. `NullAware`) or a separate flag; pick whichever keeps the existing
`NullMatch` switches exhaustive with less churn — set by every negated `in`,
honoured by the streaming semi/anti operator and the materialized fallback, and
printed by `explain`. When S has no null, the result equals a plain anti join
except for null probe keys, so q16 rewritten as `!(s_suppkey in (…))` gives the
same answer.

`is_streamable_semi_anti_join` (`semi_anti_join.cpp`) admits only
`NullMatch::Never`, so a new `NullMatch` value would send every negated `in` to
the materialized fallback. Teaching the streaming operator the flag is part of
the work, not a follow-up.

## Spelling: `!(x in (S))`

Ibex has no `not`; negation is `!` (q13's `!like(...)`, SPEC 5.8's
`!exists(...)`). `!(x in (S))` adds no syntax, and as a whole conjunct it fits
the `!exists(...)` handling `lower_filter` already has. `x not in (S)` reads
like SQL but adds a word that exists nowhere else in the language; `x !in (S)`
adds an operator. Neither is worth it for V1. Revisit only if the parenthesised
form proves error-prone.

`in` is already a hard keyword (`TokenKind::KeywordIn`, used by `map … in`), so
no identifier can collide. Parse `value in ( table_expr )` at comparison
precedence. Note the parse ambiguity to settle in the parser, not in the plan:
`x in (a)` with `a` a bare name is a one-element table expression here, never a
literal list, until the literal-list builtin exists and the two are told apart
by the RHS's kind.

## Correlation

Uncorrelated is the main case, unlike `exists`: SPEC 5.8 rejects an `exists`
with no capture, but for `in` the synthesized `s == outer(x)` *is* the capture.
q18 and q20 use only uncorrelated `in`s.

A correlated `in` (S itself uses `outer(...)`) adds its captures beside the
synthesized one, and the semi/anti join keys on all of them — the same
multi-capture shape `exists` lowers today.

## V1 restrictions (each rejected with a diagnostic)

- **Whole `&&` conjunct only, positive or negated.** `in` under `||` or inside a
  larger boolean would need a mark join, and unlike `exists` (never null) the
  mark is three-valued: an unmatched row is UNKNOWN, not FALSE, when S holds a
  null or `x` is null, and that matters under `!` (`!(x in S) || p`). The
  built mark join reads "count is not null"; `in` would also need "S has a
  null" and "`x` is null". Defer until a query wants it.
- **Bare-column LHS**, so the join is a plain equijoin. `(a + b) in (S)` must be
  materialised first.
- **One-column subquery.**
- **Filter position only.**

```ibex
t[filter p || x in (S)];        // in only as a whole && conjunct, for now
update { flag = x in (S) };     // only in filters
(a + b) in (S);                 // LHS must be a bare column
x in (t[select { a, b }]);      // subquery must have one column
```

## Performance: the readable q18 must plan like the hand-written one

- **Semi-join pushdown** (`src/ir/join_pushdown.cpp`) only sees a
  `Join(Semi(Join(Inner …)))` when both share one IR tree (q18's comment). The
  lowering must put the semi join in the enclosing expression's tree, not behind
  a binding.
- **The Stage C left-scan filter** covers inner joins only; semi/anti joins on a
  streamed left input are listed as not yet covered
  (`plans/beat-both-plan.md` §3 item 2). Not a blocker; worth knowing when
  comparing q18 forms.

## Test plan

- parser: `x in (S)`, `!(x in (S))`, and each rejection above;
- lower: positive `in` → `Join(Semi)` with no aggregate; negated → the
  null-aware anti join, distinct in the IR from a plain anti join;
- **the readable q18 and the hand-written q18 lower to the same plan** (covers
  pushdown and duplicated source reads; see
  [[project_parity_cannot_see_lowering_bugs]] — assert on the IR);
- a positive `in` keeps each outer row at most once;
- negated `in` against an S holding a null keeps no rows;
- negated `in` against a null-free S equals a plain anti join except that a null
  probe key drops — hand-computed expected rows, not a second Ibex query
  ([[project_rewrite_test_reference_trap]]);
- the streaming path honours the null-aware flag (single- and multi-threaded),
  and fails when the flag is ignored;
- interpreter/codegen parity.
