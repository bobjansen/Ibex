# Serial Parity Comparator Plan

**Prerequisite for [runtime-multithreading-plan.md](runtime-multithreading-plan.md)**
— to be completed *before* that plan's Phase 0. The master plan's Phase 0 item 1
("Interpreter/transpiled serial parity (first gate)") and its Test Plan both
demand a comparison the current parity harness structurally cannot perform. This
plan builds the comparator that gate actually needs.

## Problem

The master plan requires the parity gate to compare:

> schema, table metadata (including ordering, time index, and logical row
> count), row order, values, categorical representation, and validity bitmaps.

The current harness compares none of that structure. `tests/parity/run_parity.sh`
runs each `.ibex` case through `ibex` (interpreter) and the transpiled binary,
captures **stdout**, and does `diff -u` on the two text renderings
(`run_parity.sh:90`). Rendered CLI text cannot distinguish:

- **validity backing** — a genuine null vs. a value that renders as the same
  placeholder;
- **categorical representation** — dictionary codes vs. materialized values
  print the identical string;
- **table metadata** — `ordering`, `time_index`, and column-less `logical_rows`
  are never printed at all;
- **schema nuance** — column type/order collapses to whatever the renderer shows.

So the master plan's item 1 is not "extend the existing parity suite." It is a
**new artifact**: an in-process structured table comparator. Treating the
stdout-diff as the gate would let the interpreter and transpiled paths diverge in
exactly the representation dimensions Phase 1/2 start making observable
(validity carried through partitioned filters, categorical backing across
morsels, `time_index` re-stamped after a merge).

## Work

1. **In-process structured comparator.** A function that takes two `Table`s and
   returns a structured mismatch (not a text diff), comparing, field by field:
   - schema: column count, names, order, and `ColumnValue::index()` type tag;
   - table metadata: `ordering`, `time_index`, and `logical_rows` (the last only
     meaningful for a column-less frame — mirror `MaterializeOperator`'s rule);
   - row count and row order;
   - per-cell values;
   - **validity bitmaps** per column (compared explicitly, not via rendering);
   - **categorical representation**: compare code-space *and* the resolved
     dictionary values, so a code remap that changes backing but not surface
     strings is caught (cf. the "one dictionary per row group — remap or values
     are silently wrong" gotcha in `project_parquet_categorical_decode`).
   Report the first divergence with column/row coordinates, not a boolean.

2. **Drive both routes in-process.** Run each `.ibex` case through
   `runtime::interpret()` and through the transpiled `ibex::ops::*` output,
   obtaining two `Table`s (not two stdout captures), and feed them to the
   comparator. The transpiled route needs an execution entry point that returns
   a `Table`; if none exists yet, that dependency is itself a Phase-0
   observation the master plan already flags ("the generated path has an
   equivalent execution entry point") — surface it here rather than silently
   falling back to stdout.

3. **Enumerate the case matrix.** The master plan's Test Plan lists the required
   coverage; make it concrete cases:
   - filters, projects, updates, fused filter/update/project;
   - empty frames and zero-column frames;
   - categoricals (including a remap-sensitive case);
   - strings, booleans;
   - nullable columns with interior and trailing nulls;
   - a time-indexed / ordered frame so `time_index` and `ordering` are
     non-empty and actually compared.

4. **Wire as a gate, keep the old script honest.** The structured comparator is
   the parity gate. Either replace `run_parity.sh`'s stdout-diff or keep it as a
   fast smoke check clearly labelled as *not* the metadata/validity gate, so no
   one mistakes a green stdout-diff for structural parity again.

## Done when

- A structured `Table`-vs-`Table` comparator exists and reports coordinate-level
  mismatches across schema, metadata, values, validity, and categorical backing.
- The enumerated case matrix runs both routes in-process and passes.
- The gate is documented as the authority for master-plan Phase 0 item 1, and
  the stdout-diff is either retired or explicitly demoted to a smoke check.

## Relationship to the master plan

- This comparator is reused later: the master plan's Test Plan runs the **same
  cases across worker counts** and requires each route to match its own serial
  result *and* the two routes to match each other. The comparator built here is
  that shared assertion; parallel testing extends the case matrix, not the tool.
- It does not, by itself, establish "one serial semantics" — it only *detects*
  divergence. Any divergence it surfaces is a correctness bug to fix before
  parallelism, per the master plan's sequencing.
