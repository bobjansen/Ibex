# Filtering Parquet scan: direct decode + late materialization

Implemented in `588d8e5` and `400315c`. Detailed profiles, Polars comparison,
rejected decoder prototypes, and follow-up directions are recorded in
[`parquet-filtering-scan-observations.md`](parquet-filtering-scan-observations.md).

## Why

With join predicate pushdown done, every PDS-H query is **scan-dominated**.
Measured 2026-07-13 (WSL2, warm page cache, both engines single-threaded):

| component | ibex | polars-st |
|---|---|---|
| q19: decode the 10 projected columns | ~145 ms | ~50 ms (~32 ms with the filter applied *during* the scan) |
| q19: join + filter + aggregate, in memory | ~33 ms | ~26 ms (residual of its 58 ms total) |
| q06: decode 4 numeric lineitem columns | ~100 ms | ~25 ms |

The relational engine is already at parity. The 2.3× local geomean gap
(2.6–2.9× clean-box) is almost entirely Parquet decode, which the PDS-H
methodology deliberately re-runs every iteration. Two causes, in order:

1. **Two materializations.** `read_parquet_lazy`'s decode callback
   (`libs/parquet/parquet.hpp`, ~line 755) does
   `reader->ReadTable(column_indices)` — a full Arrow `Table` — then
   `populate_from_arrow_table` copies it wholesale into Ibex columns. Polars
   decodes pages directly into its final column memory. Per-column decode of a
   6M-row column costs Ibex ~20–90 ms; Polars does four of them in 25 ms.
2. **No late materialization.** Ibex decodes all 6M rows of every projected
   column, then filters. Polars filters *during* the scan, so at q19's 3.6%
   selectivity the non-predicate columns are mostly never materialized — its
   filtered scan (32 ms) is faster than its bare scan (50 ms).

## One design, not two

These are one work item because fixing (2) properly forces the rewrite (1)
wants. Filtering during the scan only pays if non-predicate columns are
decoded *with a row selection* — decode a row group, keep only survivors.
That cannot be bolted onto "materialize the whole Arrow Table, then convert";
the conversion loop must first become a per-row-group decode straight into
the destination Ibex column. Doing (1) alone without a selection parameter in
the new loop's signature means rewriting the same loop again for (2).

## What is already in place

- **The IR now puts predicates on top of scans.** `push_filters_into_joins`
  (src/ir/join_pushdown.cpp) leaves shapes like `Filter(pred_a, Scan(a))` as
  join children, and single-table queries lower to `Filter(Scan)` directly. A
  `required_columns`-style walk can hand each scan its row-local conjuncts.
- **`LazyTable`** (`include/ibex/runtime/lazy_table.hpp`) is the seam between
  plan analysis and the plugin: the REPL calls `lazy->project(names)`
  (repl.cpp, projection-pushdown block) and the plugin supplies a
  `ColumnDecodeFn`. Both sides of the new interface are in one place each.
- **Filter mask machinery** in the runtime is vectorized and battle-tested.
  The plugin never needs to learn expression evaluation: the runtime decodes
  predicate columns, computes the mask, and passes a *selection* down.
- **Bulk conversion + validity copy** (July 2026) already made the copy cheap
  per element; what's left is that it exists at all, plus Arrow's own decode.

## Design

New decode interface (plugin side, `libs/parquet/parquet.hpp`):

```
ColumnDecodeFn: (names, const Selection*) -> Table
```

where `Selection` is a row-index vector (or bitmap) over the file's rows,
null meaning "all rows". Implementation: iterate row groups; for each group,
decode the requested column chunk and append either all values or only the
selected ones directly into the destination Ibex column (`reserve` to the
exact final size — the selection's length is known up front). This kills the
intermediate full-column Arrow materialization even in the null-selection
case. GOTCHA from the bulk-conversion work: `vector::resize`
value-initializes and cancels a memcpy win — stick to reserve+insert.

Runtime side, `LazyTable` grows:

```
project_where(names, conjuncts) -> Table   // rows() no longer the row count!
```

which (a) decodes the predicate columns (null selection), (b) evaluates the
conjuncts with the existing filter kernels to get a selection, (c) decodes
the remaining columns with that selection, (d) gathers the predicate columns
by the same selection. Wiring: the REPL's projection-pushdown block gains a
scan-predicate extraction pass next to `ir::required_columns`.

**Soundness freebie:** keep the `Filter` node above the scan untouched at
first. Re-applying an already-applied conjunct is idempotent, so scan-side
filtering is correct even while partial (only row-local conjuncts, only some
sources). Removing fully-applied Filters is a later optimization, not a
correctness requirement.

**The cache trap (do not skip):** `LazyTable` caches decoded columns and
shares handles across queries and function bodies. A filtered decode MUST NOT
populate the whole-column cache — a later unfiltered query would silently see
214k rows where the file has 6M. Simplest sound policy: `project_where`
bypasses the cache entirely (reads are per-query anyway in the benchmark
shape); revisit caching keyed by predicate only if profiling demands it.

**ABI:** changing `ColumnDecodeFn`'s signature is a plugin ABI change — the
`.so` must be rebuilt in lockstep with the runtime (known failure mode:
`bad_alloc` weirdness from stale plugins, see Table(n) memory).

## Stages

1. **Direct decode, null selection.** Rewrite `populate_from_arrow_table`
   users into per-row-group decode straight into Ibex columns, with the
   `Selection*` parameter present but always null. Pure refactor; identical
   results; benchmark the decode win alone (expect the q06 gap to shrink
   most — its predicate columns are 3 of its 4 columns).
2. **Selection-aware decode.** Implement the selected path per Arrow type
   (numeric memcpy-gather, string/dictionary, date/timestamp). Unit-test
   against decode-then-gather on small files, including nulls and multiple
   row groups (dictionary codes are per-row-group — remap before gather).
3. **Scan predicate extraction + `project_where`.** IR walk collecting
   row-local conjuncts sitting directly above each `Scan`; REPL wiring;
   Filter stays above. Verify all six PDS-H answers; measure q19 + q06.
4. **Trimmings** (each optional, in value order): row-group min/max stats
   pruning from the same conjuncts; dropping fully-applied Filter nodes;
   compiled-path (`ibex_compile`) wiring.

## What this does NOT solve

- **Cold-process reads** — `ibex_eval` still pays dynamic-library + first-read
  cost; the benchmark's warm-REPL methodology sidesteps it deliberately.
- **polars-mt** — multi-threaded decode belongs to the multithreading
  milestone; this plan keeps the single-threaded apples-to-apples framing.
- **The `let joined = a join b` split form** — still needs deferred plans
  before any of this reaches predicates hidden behind a `let` boundary.
