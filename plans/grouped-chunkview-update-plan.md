# ChunkView-native grouped update plan

## Objective

Replace the current `grouped_update_table` gather → per-group Table → scatter
implementation incrementally, without changing the semantics of `update ...,
by ...`: groups span input chunks, output rows retain original order, fields
run in declaration order, and null group keys form a real, distinct group.

The existing table implementation remains the reference and fallback while
each state shape is introduced.

## Non-negotiable contract

### Group identity and ownership

1. Group discovery consumes the complete input key stream before any grouped
   result is emitted. It assigns a global group id to every absolute row.
2. The ids are deterministic: the serial first occurrence of each key owns its
   id, including the all-null key tuple. Parallel discovery may partition and
   hash locally, but a row-order merge performs the final id assignment.
3. A `GroupRows` CSR owns `(group id → absolute row ids)`. It is immutable
   after discovery and is the sole mapping used for per-group reads and output
   scatters. It may outlive input chunks, so it owns row ids rather than
   borrowing ChunkView pointers.
4. Key columns and input chunks are read-only. A group worker owns only its
   local result windows and local validity; it never lands a `ColumnEntry` or
   modifies table metadata.

### Field execution and landing

1. The grouped executor plans fields in declaration order. A later field sees
   the preceding field's landed result, exactly as `update_table` does today.
2. A field declares one of three state shapes:
   - **group reduction/broadcast:** private per-group state, then a deterministic
     merge and a scatter to the group's absolute rows;
   - **group row-local:** one group's CSR rows are read through ChunkViews and
     written to evaluator-owned output windows;
   - **materialized barrier:** arbitrary calls, tuple assignments, externs, or
     order-sensitive expressions without a stated state protocol.
3. The evaluator owns the final complete column and merges local validity in
   absolute-row order. The existing shared field writer remains the only place
   that replaces/appends a field and derives table properties.
4. Group workers never share writable output words/slabs except through the
   established representation contracts: fixed-width disjoint windows, packed
   bool atomic boundary OR, strings count/prefix/write, and planned categorical
   code remaps.

### Ordering and materialization barriers

- `lag`, `lead`, cumulative functions, fill-forward/backward, rank, and rolling
  windows require an explicit per-group order/state contract. They remain on
  the materialized grouped path initially.
- A `window` clause adds time-order validation plus a group boundary/halo
  protocol. It is deliberately not folded into the first grouped ChunkView
  slice.
- `by` grouping itself is still global: no chunk may independently decide that
  equal keys in a later chunk are a new group.

## Delivery slices

1. **Extract planning/discovery.** **Completed:** the table grouped path now
   receives an immutable `GroupedRowPlan` that owns global row ids and CSR
   rows. It resolves keys once and supplies the same deterministic group ids
   and CSR ownership to every later stage.
2. **Group reductions.** **Fixed-width family completed:** bare
   `sum`/`mean`/`min`/`max` over `Int` or `Double`, plus `count()` and
   `count(Int|Double)`, reduce directly over CSR group rows and scatter
   evaluator-owned fixed-width broadcasts. Numeric reductions skip nulls and
   own an output validity bitmap for all-null groups; `count` remains non-null.
   The path currently opts in only when every field is an independent supported
   reduction, so declaration-dependent and mixed fields retain the
   materialized evaluator. **Mixed-field staging completed:** a clause that
   combines a native reduction with a barrier now executes ordered single-field
   stages, so every field observes precisely the columns landed by its
   predecessors. This reuses native CSR reductions where eligible while the
   all-materialized case keeps its existing one-pass evaluator. These ordered
   stages now share one group plan (slice 7).
   **Parallel reduction completed:** workers claim complete CSR groups,
   reducing and scattering only their disjoint fixed-width rows. All-null
   group markers are merged into validity serially afterward, avoiding races
   on packed bitmap bytes while preserving deterministic per-group reduction
   order.
3. **Group row-local fields.** Run already-supported direct output plans over
   CSR row windows, preserving the same output/validity contracts as ordinary
   updates. **Initial slice completed:** when every field is truly row-local
   and aggregate-free, grouping is bypassed entirely and the ordinary direct
   update protocols run over original-order rows (numeric, packed predicates,
   string/categorical/temporal, and validity outputs). This avoids a needless
   CSR gather. Mixed group-sensitive clauses still use the ordered staged or
   materialized route; CSR-window execution there remains the next extension.
4. **Variable-width and categorical grouped results.** **Direct-output slice
   completed:** aggregate-free row-local String expressions stage through the
   existing count/prefix/slab writer, and Categorical CASE/coalesce stage
   through the existing planned dictionary/remap writer, even when adjacent
   fields need group state such as `lag`. Their output ownership and validity
   therefore remain exactly the ordinary direct-update contracts. Variable
   results whose values themselves depend on group state still use the mature
   per-group assembly path; scattering those directly by absolute row id is
   the remaining extension of this step.
5. **Ordered group state.** **Fixed-width state slice completed:** bare
   `lag`/`lead`, `cumsum`/`cumprod`, and forward/backward fill over `Int` or
   `Double` now walk each CSR group in original row order and scatter to
   absolute output rows. Group workers own their complete state chains; byte
   validity staging is merged only after the worker barrier. `rank` remains on
   the evaluator until its order-key, null placement, and tie-method contract
   is stated as a separate native plan; variable-width ordered state likewise
   remains on the materialized path.
6. **Windowed grouped updates.** **Completed.** *Time-index validation:* a
   grouped window now establishes, before any kernel relies on it, that each
   group's rows are time-ascending. A stated ordering proves it for free when it
   reaches the time index ascending after nothing but grouping keys; otherwise
   one sequential pass over the index proves it globally; only then is the
   strided per-group walk paid. A group that genuinely is out of order is
   rejected, naming the group's key values and the `order` that repairs it.
   This closed a silent wrong-answer path rather than a hypothetical one: this
   operator emits GROUP-MAJOR rows while keeping the time index, so a second
   grouped window over a *different* key was reading a later row into an earlier
   row's window and returning a plausible wrong number.
   *Halo/boundary protocol:* `expr_window_lookback` states, per field, how far
   back it can read — the clause duration, a per-call `__window_ns`, or a
   `__window_n` row count — or refuses when a call (`cumsum`, `lag`, a bare
   aggregate, an extern) reaches the group's first row. When every field is
   bounded, a group is cut into pieces of at least the minimum size and each
   piece is handed the rows within that bound before it, which it evaluates for
   state and then drops. This lifts the old cap of "parallelism = group count"
   for TRAILING windows, which previously could not be split at all; the aligned
   bucket-boundary cut is still preferred where it applies, since it needs no
   halo. A split is refused when the halos would exceed a quarter of the rows —
   a long window over dense ticks re-evaluates more than it parallelizes.
   Measured interleaved and pinned at 5M rows, 3 groups, 8 cores: 0.74-0.77x on
   the trailing-window shape, with the aligned (1.00x) and 24-group (0.89-1.00x)
   shapes as unchanged in-run controls. `window_halo_pieces` in
   `IBEX_PARALLEL_STATS` counts the cuts, so a gate that silently stops matching
   is visible.
   *Known and deliberate:* the halo reproduces a window's CONTENTS exactly, not
   its floating-point rounding — the rolling kernels carry a running accumulator
   that never resets, so a piece reaches a row by a different sequence of
   additions. Measured divergence ~2e-11 relative, against ~1e-10 for the
   aligned cut that has shipped since before this. Not a new property of the
   operator.
   *Not covered:* `lag`/`lead`/`diff` under a window clause still force the
   whole-group path. A backward-only `lag` has an obvious row bound, but `lead`
   reads forward and this halo is a prefix; a forward halo is the natural next
   extension.

7. **Aggregates mixed into row-local expressions.** **Completed.** A field
   whose expression *contains* aggregates rather than *being* one — `x -
   mean(x)`, `x / sum(x)`, `sum(a * b)`, `count() * 2` — is the canonical
   grouped update, and no native gate matched it: its root is not a bare
   aggregate call, so the whole clause fell to the per-group gather-and-rebuild
   evaluator. It needs no new kernel. Each aggregate subterm is a reduction the
   CSR path already owns, and once its group value is a broadcast column what
   remains of the expression is row-local by construction and belongs to
   `update_table`'s direct ChunkView output protocols. A field is therefore
   executed as: stage any aggregate ARGUMENT that is an expression into a
   column (`sum(a * b)` — the same lowering `select` has always done for
   grouped aggregate inputs), reduce and broadcast each aggregate, then
   evaluate the residual expression over original-order rows. The staging
   columns live on a local table that shares the input's column storage and are
   dropped before the result is returned, so the caller never sees them.
   Aggregates over the same bare column collapse onto one reduction within a
   field; two aggregates over equal argument *expressions* do not, since
   deciding that two trees are the same expression needs a structural
   comparison the IR does not offer.
   *Admission:* the whole field must be row-local once aggregates are ignored
   (which rejects `rank`, rolling calls, generators and externs), and every
   aggregate must be `sum`/`mean`/`min`/`max`/`count` over an Int or Double
   column — the existing fixed-width reduction contract, now stated once in
   `native_reduction_for` and shared by the bare-field planner and this lifter
   so the two cannot disagree. Anything else keeps the entire field on the
   materialized evaluator, unchanged. `grouped_lifted_aggregates` in
   `IBEX_PARALLEL_STATS` counts the lifts, since a gate that stops matching
   here still answers correctly — just at many times the cost.
   *Shared group plan:* the ordered staging loop now runs its fields against
   one already-discovered `GroupedRowPlan` instead of rediscovering the
   grouping per field. A stage only ever adds a column, so the row count and
   the key columns — and therefore the group ids and their CSR rows — are
   identical for every stage. This is what makes admitting more fields to
   staging free rather than a per-field group-discovery tax.

## Initial acceptance tests

- groups span at least three chunks, including a null-key group;
- serial/parallel group ids and scattered output are byte-identical at several
  grains and worker counts;
- multiple fields prove declaration-order visibility;
- a supported reduction uses the native group path, while `lag` and an extern
  demonstrably select the materialized fallback;
- key ordering/time-index metadata and source ColumnValue alias ownership are
  unchanged.
