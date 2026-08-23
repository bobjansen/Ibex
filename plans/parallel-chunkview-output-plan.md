# Parallel ChunkView output protocol

## Goal

Enable computed ChunkView kernels under parallel execution without making them
responsible for field scheduling, statistics, error ordering, column landing,
or table metadata.  `update_table` remains the semantic and ownership
authority; a direct kernel only plans a field and writes one assigned range.

## Contract

Each supported family exposes an immutable `DirectFieldPlan`, made from a
`PredicateInput` (Table or ChunkView), an expression, and scalar bindings.  It
contains resolved read-only inputs and the physical output shape, but no output
storage and no mutable table state.

The evaluator allocates the complete output column and hands workers a
non-owning `DirectOutputWindow` for disjoint row ranges.  A writer may only
write its supplied window.  It returns a range-local validity bitmap; the
evaluator merges those bitmaps in range order and chooses the lowest failing
range's error.  Kernels never add or replace `ColumnEntry`s.

## Output shapes

- Fixed width: caller-owned `int64_t` or `double` window.
- Packed bool: zero-filled words and set-only writes; shared boundary words use
  the existing atomic OR primitive.
- Nullable numeric: workers write `Int64`/`Double` values and return the
  replacement validity for `fill_null`, `coalesce`, and native-predicate CASE.
- String: count each range, prefix-sum byte counts, allocate one slab, then
  write prefix-assigned string windows.
- Categorical: a planned output dictionary and source-code remaps; see the
  ownership contract below.

Validity stays worker-local.  No worker receives a pointer into a shared
validity bitmap.

## Delivery order

1. Introduce the plan/window vocabulary and adapt fixed-width numeric binary
   output to it.  Keep all existing scheduling and accounting in
   `evaluate_field_maybe_parallel`.
2. Use the same plan/window protocol from serial ChunkView execution, with one
   whole-range window, without enabling additional direct families yet.
3. Port packed booleans, then interpolation strings with their count/prefix
   phase.
4. Port the remaining direct families by output shape, with one testable slice
   per change:
   1. fixed-width `Int64` maps: `length`/`byte_length`, then simple numeric
      unary transforms;
   2. packed bool predicates, including shared-word boundary coverage;
   3. interpolation strings, retaining the count/prefix/write protocol;
   4. validity-changing fixed-width `fill_null`, `coalesce`, and CASE
      (`Int64`/`Double` first; other physical representations retain the
      evaluator until their own output shape is explicit).
5. Port categorical CASE/coalesce only with the following tested dictionary
   merge strategy.

## Categorical CASE/coalesce dictionary and ownership contract

Categorical values are logically strings, but their physical result must not
be assembled by parallel workers. A categorical CASE or coalesce therefore
has a distinct three-part plan:

1. The planner materializes each input dictionary once and builds a fresh,
   output-owned dictionary before scheduling. Its order is deterministic:
   operands/CASE arms in lexical order, then labels in each source dictionary's
   code order, followed by literal labels in arm order. The first occurrence
   of a label wins. Every source dictionary code receives a precomputed output
   code remap; literals receive their output code at planning time.
2. The evaluator owns a zero-initialized `vector<code_type>` for the complete
   result. A worker is given only its disjoint code window, the immutable
   remaps, and its row range. It writes output codes and returns a dense,
   range-local validity bitmap. It never inserts a dictionary label, mutates a
   remap, or receives a pointer to shared validity.
3. After the write barrier, the evaluator moves the code vector together with
   the fresh dictionary and index into `Column<Categorical>`, then lands that
   column through the existing shared metadata writer. Source dictionaries are
   read-only and never alias the output dictionary; only an explicit metadata
   alias update may retain a source `ColumnValue`.

The IR carries `Categorical` as an internal expression result type. CASE and
coalesce retain that representation whenever at least one non-null arm is
categorical (plain String literal arms are compatible); unrelated String-only
expressions remain String. This makes both guarded and unguarded categorical
results use the same physical protocol without changing ordinary String
semantics.

## Acceptance tests

- serial/parallel parity at multiple worker counts and grains;
- range-boundary and nullable-output coverage;
- declaration-order behavior for multiple update fields;
- deterministic errors and parallel-field accounting;
- string prefix and packed-bool shared-word coverage when those shapes land;
- table metadata remains installed only through the shared writer.
