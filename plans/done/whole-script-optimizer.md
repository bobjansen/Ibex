# Whole-script optimizer: what changed

This work establishes a planning boundary for relational Ibex scripts. Before
it, the REPL executed a script statement by statement: an external reader was
called as soon as its `let` ran, each table effect was evaluated in place, and
the runtime only saw a single already-materialized query at a time. That made
local rewrites possible, but prevented the executor from using information
from the complete relational computation before decoding files or choosing a
join order.

The new path keeps ordinary language semantics intact and uses the existing
statement executor whenever a script is outside the supported relational
shape. For eligible scripts it lowers the table part of the program into a set
of relational plans and schedules those plans after applying source-aware
optimization.

## New capabilities

| Area | What is now available |
| --- | --- |
| Stable source identity | Repeated deterministic reader calls with literal arguments are hoisted into named scans. The optimizer can recognize that they are sources rather than opaque calls. |
| Script-level plans | `lower_script` separates table-consuming extern calls (for example `write_csv`) from the final relational result. A `ScriptPlan` preserves source order for effects while exposing each effect's input DAG. |
| Planned lazy reads | Registered lazy readers contribute their schema and row count before decoding. The executor can then request only demanded columns and pass scan-local predicates to `project_where`. |
| Existing relational rewrites on every planned input | Filter-into-join and semi-join pushdown run before source materialization for both sink inputs and the final result. |
| Cardinality estimates | The IR can estimate logical row counts from source metadata, propagating exact sizes where possible and marking filter estimates as heuristic. |
| Guarded join reordering | For order-insensitive aggregates directly over safe left-deep inner equijoin chains, known source sizes choose a lower-cost join order. The rewrite declines unsafe or ambiguous cases. |
| Independent sinks | A planned script may write or otherwise consume an intermediate table, then continue with later transformations and sinks. Sinks no longer need to consume the final binding. |
| Reuse of named sink inputs | When a sink receives a simple `let` binding, its materialized table is cached. A later sink or the final identifier result reuses it instead of evaluating that binding again. |
| Explicit consumer seam | Table-consuming externs are invoked through `runtime::invoke_table_consumer`, rather than being entangled with relational interpretation. |

## Execution model

For each planned table input, the batch executor now does the following:

```text
relational DAG
  -> hoist lazy reader calls into scans
  -> read source schemas and row counts
  -> push filters / semi joins and guarded join reorder
  -> derive required columns and scan predicates
  -> selectively materialize sources
  -> interpret the optimized DAG
  -> invoke the sink (if any)
```

Sinks remain source-ordered, so observable write ordering does not change.
The final expression is evaluated after the sinks unless it is the cached
input of a preceding sink.

## Deliberate current boundary

This is a relational batch planner, not yet a planner for every Ibex effect.
Scripts with scalar lets, tuple lets, imports, or function declarations use
the mature statement-at-a-time executor. The planned path also requires
literal arguments for lazy reader calls, because those are what give a source
a stable compile-time identity.

Join reordering is intentionally conservative. It only applies beneath an
aggregate that does not use `first` or `last`, requires closed known schemas
and source row counts, accepts only inner equijoins in a left-deep chain, and
rejects duplicate non-key column names or predicate joins. Declining a rewrite
preserves the original plan and semantics.

Independent planned inputs currently create independent lazy-source instances.
This preserves correctness and lets each input have its own required-column
set, but a future shared-source cache could avoid reopening the same file when
several distinct sink plans read it.

## Why this matters for the Polars gap

Ibex now has the mechanism needed to make decoding informed by each *complete
planned relational input* rather than by the first statement that happens to
run.
It can select source columns, push row-local filters to a lazy decoder, and
choose a safe join order using file metadata before data is materialized. That
is the prerequisite for deeper decoder work (row-group pruning, richer
predicate pushdown, and selected decode) and for widening the optimizer across
more script effects.

## Verification

Coverage includes source hoisting, script sink separation, lazy source
planning, cardinality estimates, join-order selection, and guarded join
reordering. The REPL regression `REPL batch planner executes non-final table
sinks` proves that an intermediate sink runs in order, a later sink gets its
own optimized input, and the final sink binding is reused for the final result.

The complete Release suite passed after this work: 1,141 tests, with four
network-dependent tests skipped as expected.
