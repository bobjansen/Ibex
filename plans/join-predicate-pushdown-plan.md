# Predicate pushdown through joins

## Why

q19 is the worst PDS-H query by a wide margin — **520 ms against single-threaded
Polars' 58 ms**, while the other five sit at 1.0–3.9× — and the reason is
structural, not a constant factor. It joins 6M lineitem rows to part, then
filters the 6M-row result down to about 120 rows.

Most of that predicate only touches lineitem:

```
filter (l_shipinstruct == "DELIVER IN PERSON")
    && (l_shipmode == "AIR" || l_shipmode == "AIR REG")
    && ( ...brand/container/size/quantity terms mixing both sides... )
```

Applying just those two conjuncts to lineitem *before* the join leaves **214,377
of 6,001,215 rows** (3.6%). Measured by hand, that collapses the join + filter
from **951 ms to 37 ms**. Everything else in this plan is about doing that
automatically and safely.

The same shape recurs in any join-then-filter query, so this is a general
capability, not a q19 special case.

## What is already in place

- **The whole query can now be one IR tree.** `read_parquet(...)` in *any* table
  position — including inside a join operand — is bound lazily and lowers to a
  `Scan`, so a single-expression query reaches `interpret` as one tree with
  projection pushdown intact (commit 2a9748d). Before that fix the
  single-expression form was 3.5× *slower* than the `let`-split form, which would
  have made this whole plan pointless.
- **`ir::required_columns`** (`src/ir/required_columns.hpp`) already walks a plan
  top-down collecting the columns each `Scan` needs, including a `collect_refs`
  expression walker this rule can reuse to find a conjunct's column references.
- **`ir::SchemaInfo` / schema propagation** exists in `src/ir/schema.cpp`, and the
  REPL already builds `context.source_schemas` for every in-scope binding
  (materialized and lazy alike) before lowering.

## The obstacle

**`canonicalize` knows nothing about schemas.** It is a pure structural IR
rewrite — `grep -c 'NodeKind::Join' src/ir/canonicalize.cpp` returns **0**; there
are no join rules at all. To decide whether a conjunct belongs to the left side or
the right, the rule must know which columns each side produces, and canonicalize
cannot currently answer that.

**This is the design decision to make first, before writing any rule.** Three
options:

1. **Schema-aware optimizer pass, run after lowering.** Propagate `SchemaInfo`
   down from the leaves (the REPL has `source_schemas` for every binding), then
   run the rule with each `JoinNode`'s child schemas known. Keeps canonicalize
   pure and structural; adds a new pass that needs schemas threaded in.
2. **Rewrite inside the lowerer**, where `source_schemas` is already to hand. No
   new plumbing, but it buries a relational optimization in the lowerer, and the
   compiled path (`ibex_compile`) would need the same treatment.
3. **Runtime, in the `Join` interpreter case.** Schemas are exact there, but the
   node's children have not been evaluated yet at the point the decision is
   needed, so their schemas are not actually available without evaluating them —
   likely a dead end. Recorded so nobody re-derives it.

**Recommend (1).** It is where a planner belongs, it serves the compiled path too,
and `src/ir/schema.cpp` already exists to build on.

## The rule

```
Filter(pred, Join(kind, keys, a, b))
  → Join(kind, keys, Filter(pred_a, a), Filter(pred_b, b))   [+ Filter(pred_rest, …)]
```

Split `pred` into its top-level `AND` conjuncts. For each conjunct, collect its
column references (reuse `collect_refs`) and classify:

- refs ⊆ left schema  → push to the left child
- refs ⊆ right schema → push to the right child
- otherwise (mixed, or references a column produced by neither) → keep above the
  join

A conjunct that references a **join key** is safe to push on an equi-join: the key
columns are equal by construction on surviving rows.

### Join-kind safety — get this right first

This is where the rule goes wrong if rushed. **Stage 1 should handle `Inner`
only**, which is unambiguous and covers q19 (and every other PDS-H join).

| kind | left conjuncts | right conjuncts | note |
|---|---|---|---|
| `Inner` | push (and drop from above) | push (and drop from above) | Stage 1 |
| `Left` | push | **do not push** | pushing a right-side predicate lets non-matching left rows survive null-extended instead of being filtered out |
| `Right` | **do not push** | push | mirror image |
| `Outer` | do not push | do not push | both sides are null-supplying |
| `Semi` / `Anti` | push | care needed — the right side only gates existence | leave for later |
| `Asof` | **skip entirely** | skip | the match is over a time index the node does not name; do not touch |

A subtlety worth recording rather than rediscovering: for an outer join it *is*
sound to push a predicate down **and also keep it above**, since the retained
filter re-drops the null-extended rows. That is a valid later refinement — but it
only pays when the pre-filter is selective, and it is not needed for q19. Do not
attempt it in stage 1.

### Ordering against the other passes

The rewrite must run **before** `required_columns`, so that the pass sees the
pushed-down filters and demands their columns from the right scans. (The columns
are demanded either way — the filter references them wherever it sits — but the
tree it analyses must be the final one.)

## Stages

1. **Schema plumbing.** Propagate `SchemaInfo` through a lowered tree so each
   `JoinNode`'s child schemas are known. Decide and implement the option-(1) pass
   boundary. No rewriting yet — land this with a test that asserts the schemas
   reaching a `JoinNode` are correct.
2. **The rule, `Inner` only.** Conjunct splitting + classification + rewrite.
   Unit-test in `tests/test_ir_*` on hand-built trees: pure-left, pure-right,
   mixed, join-key, and a conjunct referencing an unknown column (must stay above,
   not crash).
3. **Join-kind gating.** Extend to `Left`/`Right`; explicitly refuse `Outer` and
   `Asof`. Test each kind against the table above — in particular, a left join with
   a right-side predicate must produce the *same rows* before and after.
4. **Wire in + measure.** Enable in the REPL path, re-run PDS-H. Expect q19 to
   fall sharply; verify the other five are unchanged and that all six answers still
   match the official SF-1 results (`benchmarking/tpch/check_answers.py`).

## What this does NOT solve

The rule only fires when the filter and the join are in **one IR tree** — i.e. the
query is written as a single expression. The idiomatic-looking split form:

```
let joined = lineitem join part on l_partkey;
let result = joined[filter ...];
```

still materializes the 6M-row join first, because the `let` boundary hides the
filter from the planner. Closing *that* requires deferred plans (a `let` bound to
a relational expression staying lazy until used) — the "whole program
optimization" this plan is a stepping stone toward, not a substitute for. Worth
deciding separately whether Ibex wants `let` to be lazy for pure relational
expressions, since it changes evaluation semantics, not just performance.
