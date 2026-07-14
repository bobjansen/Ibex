# Proposed subquery syntax — TPC-H syntax experiment

These files are deliberately **not executable Ibex yet**.  They are a design
exercise for the TPC-H queries that need a subquery, using the official SF-1
qualification parameters from `benchmarking/data/tpch/dbgen/queries/`.

The proposed additions are intentionally small:

```ibex
// A table-producing expression in a boolean predicate.
exists (lineitem[filter l_orderkey == outer(o_orderkey)])
not exists (orders[filter o_custkey == outer(c_custkey)])

// Convert an exactly-one-row, exactly-one-column table into a scalar.
scalar(lineitem[select { average = mean(l_quantity) }])

// Membership against a one-column table expression.  `not in` follows SQL
// three-valued/null semantics; it must not be silently implemented as a plain
// anti join.
ps_suppkey in (supplier[select { s_suppkey }])
```

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
