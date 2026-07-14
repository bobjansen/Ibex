# Proposed subquery syntax — TPC-H syntax experiment

These files are deliberately **not executable Ibex yet**.  They are a design
exercise for the TPC-H queries that need a subquery, using the official SF-1
qualification parameters from `benchmarking/data/tpch/dbgen/queries/`.

**Shipped:** the *correlated* scalar aggregate — `scalar(...)` with an
`outer(...)` capture — is implemented and specified (SPEC 5.7).  Q2 and Q17 moved
out of this directory.

**Q4 also moved out, and it needed no new syntax at all.**  An equality-correlated
`exists` *is* a semi join, and `not exists` *is* an anti join — both of which Ibex
has always had.  `exists` is therefore an ergonomic feature, not a capability one:
it buys readability, plus exactly one shape a semi join cannot express (Q21's
correlated *inequality*, `l2.l_suppkey <> l1.l_suppkey`, where a semi join on the
order key alone is vacuously true because every row matches itself).  See
`plans/exists-subquery-plan.md`.

Note that the drafts here take liberties the real language does not:
`on p_partkey == ps_partkey` is a theta join (a nested loop), so the shipped
queries rename keys to a shared name and join on that, and a `[...]` block binds
to the operand immediately left of it, so a join chain needs parentheses before
its clauses.  Read the drafts for intent, not for syntax.

The remaining proposed additions are intentionally small:

```ibex
// A boolean subquery term.  Sugar for a semi/anti join in the common case;
// genuinely new only for a correlated inequality (Q21).
exists (lineitem[filter l_orderkey == outer(o_orderkey)])
!exists (orders[filter o_custkey == outer(c_custkey)])

// An UNcorrelated scalar: exactly one row, exactly one column, evaluated once.
// (The correlated form — with an outer(...) capture — already works.)
scalar(lineitem[select { average = mean(l_quantity) }])

// Membership against a one-column table expression.  `not in` follows SQL
// three-valued/null semantics; it must not be silently implemented as a plain
// anti join.
ps_suppkey in (supplier[select { s_suppkey }])
```

Ranked by queries unblocked per unit of work: uncorrelated `scalar` (Q11, Q22)
and `in` / `not in` (Q18, Q20) come before `exists`.

`outer(...)` is explicit by design: it makes the correlation boundary visible,
allows an inner column to shadow an outer column, and avoids guessing which
scope an unqualified column belongs to.  Ordinary `let` bindings remain the
names of tables; aliases are deliberately not required for this first design.
Even Q21's repeated `lineitem` scan is unambiguous because every nested
subquery has its own lexical scope.

Each draft keeps the normal Ibex bracket clauses and uses `scalar(...)` only
where SQL requires a scalar subquery.  The following unrelated gap is left
spelled as a proposed call so the corpus shows the full remaining scope:

- `prefix(text, n)` in Q22.

Neither is part of the correlated-subquery feature.

The intended lowering is: `exists`/`not exists` to semi/anti joins;
correlated aggregate scalars to an aggregate by captured key followed by a
join; and an uncorrelated `scalar` to a one-time value.  A nested-loop Apply
operator is not the target execution plan.  Reusing a `let`-bound table inside
a subquery (as Q17 does) denotes the same source/materialization; decorrelation
must not turn that reuse into another I/O read.
