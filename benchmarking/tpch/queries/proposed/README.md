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

// Membership against a one-column table expression — a SEMI JOIN, not a scalar
// (`plans/in-subquery-plan.md`).  `not in` is an ANTI join, but a null-aware
// one: a plain anti join is only exact when the subquery column is non-nullable,
// which is why Q16 could ship its `not in` as one (s_suppkey is a primary key).
ps_suppkey in (supplier[select { s_suppkey }])
```

The uncorrelated `scalar` needed for Q11 now works, and Q11 has moved out of
this directory.  The uncorrelated scalar in Q22 works too; Q22's remaining
blockers are `not exists` (an anti join) and `prefix`/`substring`.

**Q18 has also moved out**, shipped exactly the way it is described above — its
uncorrelated positive `in` written as the plain semi join it reduces to, no `in`
syntax required, just as Q04's `exists` shipped as a semi join.  The `in` feature
would only make it read closer to the SQL.

Ranked by queries unblocked per unit of work: `in` / `not in` (Q20, and the
readability of Q18) come before `exists` (0 new queries — an equality `exists` is
already a semi join).  Q20 uses only uncorrelated positive `in`s, so like Q18 it
is expressible today; its blocker is the intricacy of assembling three nested
subqueries by hand, not a missing primitive.  The null-aware anti join buys
correctness for a general `not in`, which no remaining query needs.

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
