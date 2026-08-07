# Ibex join contract

## Purpose

Ibex's join model has gaps that were first noticed while building the dplyr
backend, but almost none of them are dplyr's business. Key naming, output
schema, row order, null equality, collision handling, and key typing are
decisions the *language* owes every caller: the interpreter, the transpiled
C++, the Python bridge, and any adapter written later.

This plan fixes the contract on Ibex's own terms. `ribex` is one consumer of
it, and gets one section near the end. Where dplyr made a good call, it is
worth taking; where it made a call that suits R's dynamic, interactive style,
it is worth declining. Both kinds are listed explicitly rather than absorbed by
default.

## Status

Landed work keeps the stage numbers it was committed under; they are historic
labels, not positions in a sequence. Remaining work is named, not numbered.

- **Stage 3 — asymmetric equijoin keys.** `JoinNode` stores
  `std::vector<JoinKey>` (a `{left, right}` pair) from the AST through
  lowering, optimization, interpretation and codegen, with `on { left_id =
  right_id }` as the surface spelling. Commit *Represent asymmetric equijoin
  keys natively*.
- **Stage 1 — canonical output planner.** `ir::plan_join_output()`
  (`include/ibex/ir/join_output.hpp`) is the single authority on a join's
  output columns and their names; IR inference, the materialized join and the
  chunked join all derive from it, and codegen inherits it through the runtime
  ops. Commit *Share one canonical join-output schema planner*.
- **Ordering contract.** Row order is unspecified by design, stated as such in
  `SPEC.md` and the reference page, with `asof join` on a `TimeFrame` as the
  one exception. Documentation only — the runtime already behaved this way.
  Commit *Put join row order outside the contract, explicitly*.
- **Side-qualified predicate references.** `left(col)` / `right(col)` parse as
  a keyword-triggered call form, lower to `ir::ColumnRef::side`, and resolve
  against each input's own schema in `join_table_impl` before the row loop.
  The nested-loop batch holds both sides under `#`-prefixed names no identifier
  can spell. Commit *Give join predicates side-qualified column references*.
- **Collision resolution and the `suffix` clause.** A name held by both inputs
  is a collision and an error; `suffix { "_old", "_new" }` names both sides
  explicitly. `ir::plan_join_output()` resolves names and returns
  `expected<>`; IR inference, the materialized join and the chunked join all
  carry the policy in and the failure out. Commits *Make a join collision an
  error with an explicit escape hatch* and *Document the join suffix clause in
  the reference*.
- **Order-aware join planning.** A join states an ordering when its path
  produced one, `order` skips its work when the input already claims what it
  was asked for, and `ir::annotate_pending_orders` lets the build-side choice
  weigh a pending sort. Commits *Let a join say when it produced an order, and
  `order` believe it* and *Weigh a pending sort when picking the join's build
  side*.
- **Null-match policy.** `nulls equal` opts a join's equality keys into
  matching null to null; `Never` stays the default and the language's
  three-valued rule. Commit *Give a join keys that can match on null*.
- **Early key validation.** `ir::check_joins()` proves a join's `on` keys
  against both inputs' inferred schemas — each key names a column of its own
  side, the two sides agree on type, the output names resolve — and names the
  side, the key and the type in the message. It runs at all three lowering
  entry points and again in the driver once reader footers have supplied the
  source schemas.

## The contract

### Row order is not part of it

A join makes no promise about row order. If you want an order, ask for one:

```ibex
(trades join symbols on symbol)[order { ts asc }]
```

Settled in `SPEC.md` (commit *Put join row order outside the contract,
explicitly*). It used to say this in one place and contradict it in another:
the ordering-constraint section said any `join` drops ordering, while the join
section promised the output "preserves the left-table row order for all
left-side rows". The join section's promise was deleted.

Not promising an order is what lets the executor choose a build side by size,
which is where the current right-scan emission comes from. The tests that pin
that emission (`join: left join emits matches in right-scan order when left
side is smaller`, and its outer/right/multi-key/swapped siblings) document a
path's incidental behaviour, not a contract.

The value that a guarantee would have bought is better obtained in the
optimizer, where it costs nothing to skip when it does not apply:

- **Fold a following `order` into the join.** When `order` sits above a join,
  some join paths can emit in that order for free — probing the left side and
  emitting per left row yields left-key order when the left already has it.
  Choosing the build side with the pending sort in mind turns "hash join, then
  sort two million rows" into "hash join". This is only expressible because the
  order is written down.
- **Let the join prove an ordering constraint when it happens to hold one.**
  Not a promise: a proof, in the same shape as the uniqueness proofs in
  `add_join_unique_keys`. A path that emits per left row, over a left input
  carrying an ordering constraint, produces output that still satisfies it —
  duplicate matches are adjacent, which a non-strict lexicographic ordering
  tolerates. A right-scan path proves nothing and a downstream `order` stands.

One exception is not optional: a `TimeFrame` always carries the ordering
constraint on its time index (SPEC.md, ordering constraints). `asof join`
returns one and emits in time order, which the implementation already does by
keeping every left row in input order. Turning any other join's result into a
`TimeFrame` goes through `as_timeframe`, which sorts — the invariant is
established there, not by the join.

### Colliding columns are an error, with an explicit escape hatch

Two useful behaviours hide behind one silent default. Comparing two wide
snapshots — `prices_2023` against `prices_2024`, forty shared columns — wants
the collisions renamed, and hand-renaming forty columns first is miserable. An
accidental collision, where a column is simply overloaded across two unrelated
tables, wants to be caught. Ibex has static schemas, so it can have both:

```ibex
old join new on id suffix { "_old", "_new" }
```

- The clause names both suffixes. The left one applies to left columns that
  collide, the right one to right columns that collide; the empty string means
  "leave that side alone".
- Suffixes apply only to actual collisions, not to every column.
- Without a `suffix` clause, a non-key collision is an **error** naming the
  columns and both sides — statically when both schemas are known, at runtime
  otherwise. Never a silent rename.
- If a suffixed name *still* collides, that is an error too. Repeated
  suffixing (`val_right_right`) is exactly the silent behaviour being removed.
- Same-name equijoin keys fold into one output column, so they are not
  collisions. Semi and anti joins emit no right columns, so they cannot
  collide.

This replaces the current automatic `_right` suffixing. Every existing Ibex
program lives in this repo, so the migration is a mechanical sweep
(`grep -rn "_right"` over `examples/`, `tests/`, `benchmarking/queries/`,
`demo/`, `docs/`, `python/` and `r/`) rather than a compatibility burden.

`ir::plan_join_output()` stops being "compute names" and becomes "resolve
names, or report a collision", with the suffix policy as an input. That the
change lands in one function for all three consumers is the point of having
built it.

### Theta predicates read the inputs, not the output (fixed in Ibex core)

Two namespaces used to be built from the same two inputs, by different code,
with different rules: the output namespace from `ir::plan_join_output()` (keys
folded, collisions renamed), and a predicate namespace built inline in
`join_table_impl` (every right column, `_right` on collision, no key folding).
`id_right` was referenceable in a predicate while no output column of that name
could exist.

A predicate now reads the two inputs' own namespaces. `ir::ColumnRef` carries a
`JoinSide`, lowered from `left(col)` / `right(col)`:

```ibex
ticks join windows on right(start) <= left(ts) && left(ts) < right(end)
```

A bare name resolves against whichever input has it; a name both inputs have is
an error naming the two qualifiers rather than a silent pick; a name neither has
falls through to scalar-binding resolution as before. The nested-loop batch
still holds both sides, but under internal names no identifier can spell, so it
is no longer a namespace anyone can reference.

This was the prerequisite for the collision work: automatic `_right` suffixing
can now be removed without taking the predicate namespace with it.

### Null keys do not match, unless asked

Ibex uses three-valued logic everywhere else: `filter x == y` does not match a
null to a null. A join that matched null keys by default would be inconsistent
with the rest of the language, and the inconsistency would be silent.

So `Never` stays the default, and null-equality becomes an explicitly spelled
option:

```cpp
enum class NullMatch {
    Never,
    Equal,
};
```

Under `Equal`, hash keys carry a tagged null and null equals only null. No
sentinel substitution — a sentinel can collide with real data. For composite
keys every corresponding component must match under the selected policy. The
policy applies to equijoins only; theta comparisons keep ordinary three-valued
predicate semantics.

Callers that want R's `NA`-matching default ask for it by name. That is the
adapter's job, not the language's default.

### Key types are checked early and strictly

The runtime currently checks only that the physical key kinds match, and only
once execution reaches the join. When both schemas are known this is decidable
much earlier, and the message can name the side, the key and the type instead
of failing opaquely mid-query. `ir::check_ascriptions` is the model: same shape
of pass, same place in the pipeline, same style of diagnostic.

Ibex stays strict rather than silently picking a common type. A frontend with
its own coercion rules — vctrs for R, NumPy-ish promotion for Python — lowers
supported cases to explicit Ibex casts and rejects the rest before submitting
the query. The rules differ per frontend, which is precisely why they do not
belong in the core.

### Cardinality is an optimizer asset before it is a validation feature

Ibex already proves uniqueness (`SchemaInfo::is_unique_within`,
`add_join_unique_keys`) and already exploits build-side uniqueness for fast
paths. Letting a join *state* its expected cardinality extends that: a declared
1:1 join carries both sides' proofs into the output, unlocks identity fast
paths, and lets a pipeline fail fast on data that violates the assumption
instead of silently fanning out.

That is a different motivation from "dplyr has an argument called
`relationship`", and it produces a different design: the assertion is checked
by the build/probe that already counts duplicates and unmatched rows, and it
returns a structured failure the frontend renders. No preflight counting query
— it duplicates the work and validates a different execution than the one that
runs.

Match selection belongs here too:

```cpp
enum class MatchSelection {
    All,
    First,
    Last,
};
```

`First` and `Last` are meaningful only against a stated order, so they compose
with `order`, not with an implicit emission order. There is deliberately no
`Any`: "whichever is fastest" is an anti-contract, and the first caller whose
output depends on the observed choice makes it load-bearing forever.

### Time-domain joins are designed from the `TimeFrame` end

As-of joins already exist natively, and time-series work is Ibex's own domain.
Rolling and interval joins should be specified in those terms — direction,
bounds, tolerance, tie behaviour, and the time-index ordering invariant — and
not derived from another language's spelling of them. A frontend's
`join_by(closest())` or `between()` then becomes one way to *reach* the
operator rather than the thing that defines it.

### The IR schema cannot express nullability

`SchemaField` carries a name and a type, nothing else. A `left join` therefore
infers an output schema claiming the right columns are as non-null as their
source, which is false for every unmatched row. The output planner has nowhere
to record it, so it deliberately does not try.

The cost is small today and grows: early key checking wants it, any pass
wanting to prove a column null-free wants it, and adapters currently maintain
their own parallel nullability tracking because the core has none.

## Borrowing from dplyr

dplyr has thought carefully about joins and several of its decisions are worth
taking. Others suit a dynamic, interactive language and should be declined
deliberately rather than inherited by accident.

| Take | Why |
|---|---|
| Named key mappings (`by = c(left_id = "right_id")`) | Already landed as `on { left_id = right_id }`. Renaming a whole input to join it is worse in every language. |
| Explicit `keep` for key columns | Retaining both mapped keys and letting the caller drop or coalesce is the composable default. |
| Distinguishing filtering joins from mutating joins | Semi/anti are a different operation, not a join with columns dropped, and their proofs differ. |
| Erroring on unexpected cardinality | Silent fan-out is a data bug amplifier. Worth having as an assertion. |

| Decline | Why |
|---|---|
| `na_matches = "na"` as the default | Inconsistent with Ibex's three-valued logic in `filter` and comparisons. Available, not default. |
| Automatic `.x`/`.y` suffixing | A silent rename. Ibex has static schemas and can offer the choice instead. |
| Guaranteed left row order | Costs the build-side choice permanently; `order` says it better and the optimizer can fold it in. |
| `multiple = "any"` | Undefined-on-purpose semantics that become load-bearing on first contact with a test. |

## Work packages

### Ordering contract

Status: implemented, documentation only. Commit *Put join row order outside the
contract, explicitly*.

- ~~Delete the join section's left-order promise in `SPEC.md`; leave the
  ordering-constraint section as the single truth.~~
- ~~State that row order is unspecified by design, and that build-side
  selection and the materialized/chunked route may both affect it.~~
- ~~State the `TimeFrame` exception.~~ Scoped to `asof join`; `as_timeframe`
  establishes the invariant for anything else.
- ~~Mirror in the reference page; adjust the right-scan test and runtime
  comments so they describe a path, not a gap in the spec.~~

### Side-qualified predicate references

Status: implemented. `left(col)` / `right(col)` parse as a keyword-triggered
call form (the same shape as `outer(col)`), lower to `ir::ColumnRef::side`, and
resolve in `join_table_impl` before the row loop. `filter_col_side()` carries
the tag into generated C++. Required-column analysis now demands a qualified
reference from its own side only, which tightens pruning above a theta join.

- ~~Give theta-predicate column references an explicit side in the AST and
  IR.~~
- ~~Resolve them against each input's own schema instead of a combined
  namespace.~~
- ~~Keep the nested-loop batch construction an implementation detail with no
  observable naming.~~
- ~~Parser, lowering, interpreter and codegen tests; `SPEC.md`, the reference
  page, an `.ibex` example, and a parity case.~~

### Collision resolution and the `suffix` clause

Status: implemented. Commits *Make a join collision an error with an explicit
escape hatch* and *Document the join suffix clause in the reference*.

- ~~Add `suffix { "_left", "_right" }` to the join grammar, AST, IR node,
  builder API, codegen and public runtime ops.~~
- ~~Extend `ir::plan_join_output()` to take the suffix policy and to report a
  collision instead of inventing a name.~~
- ~~Report collisions statically where both schemas are known, at runtime
  otherwise, naming both sides and the column.~~ Inference stays total and
  falls to `Unknown` rather than failing; an unresolved collision has no schema
  to describe, and the error arrives from execution.
- ~~Error when a suffixed name still collides.~~
- ~~Sweep the repo for programs relying on automatic `_right`.~~
- ~~`SPEC.md`, `docs/index.html`, an `.ibex` example, and parity coverage for
  the suffixed and error paths.~~ `docs/reference.html` too; its
  `left(...)`/`right(...)` example was premised on the old silent renaming.

### Early key validation

Status: implemented as `ir::check_joins()` (`include/ibex/ir/schema.hpp`),
modelled on `check_ascriptions`.

- ~~Validate key existence and type compatibility before execution when both
  schemas are known.~~ Types compare at the *runtime's* granularity, not the
  IR's: the runtime carries one integer and one float width, so Int32 meets
  Int64 as the same physical column and rejecting the pair statically would
  reject a join the executor runs.
- ~~Name side, key and type in the message; improve the runtime diagnostic for
  the unknown-schema case the same way.~~ Both now read `join key type
  mismatch: left 'a' is Int64 but right 'b' is String`.
- ~~Tests for missing keys, mismatched types, mapped keys, and open schemas.~~

The pass also raises the collision diagnostic, which `infer_schema` cannot:
inference is total, so it falls to Unknown on an unresolved collision and
leaves the join with no schema to complain about. That is the caveat in the
collision package above, closed.

Where it runs matters as much as what it checks. The three lowering entry
points (`lower`, `lower_script`, `lower_expr`) cover the interpreter, the
transpiler and the REPL, but a reader call site has no schema at lowering, so
a join over `read_csv`/`read_parquet` was still checked by the runtime. The
driver runs it a second time after the footers are read, where `schemas` holds
every source's names and types — the same place `check_ascriptions` gets its
second run, and for the same reason.

### Order-aware join planning

The proof turned out to belong in the runtime, not the optimizer. Which path a
join takes is a build-side decision made from the two actual row counts
(`build_left = n_left < n_right`), so the optimizer cannot know whether the
chosen path emits in left order — it does not know the row counts. What it
*can* do is arrange for the claim to be usable, which is what the ordering
metadata on `TableProperties` already is.

So the shape is: the join states an ordering when it produced one, and `order`
skips its work when the input already claims what it was asked for. A redundant
`order` does not fold away in the plan; it becomes O(1) at execution, which buys
the same sort.

- ~~Let a join prove an ordering constraint when its chosen path preserves
  one.~~ Done, and proved from the emitted left-row index array rather than
  from which path ran — the paths are many and change for performance reasons,
  the indices are what actually determine row order. Covers inner/left/semi/
  anti, and right/outer whenever no unmatched right row is appended. The claim
  is restated in the output's names, so a suffixed key follows the rename and a
  dropped key voids it. Implemented in both join routes: `join_table_impl`
  (materialized) and `ChunkedInnerJoinOperator::assemble_output` (chunked).
- ~~So a redundant downstream `order` folds away.~~ `TableProperties::satisfies`
  decides it: the requested keys must be a prefix of the claim with matching
  directions. Consulted by `order_table_resolved` and by
  `ChunkedOrderOperator`, where it also replaces an O(n) data scan that only
  ever covered a single ascending Timestamp/Date/Int key — the claim covers
  multi-key, descending and string orderings too.
- ~~**Include the pending sort in build-side selection.**~~ Done.
  `ir::annotate_pending_orders` writes an `order`'s keys into the join beneath
  it (`JoinNode::pending_order`), looking through the operators that neither
  move a row relative to its neighbours nor rename a column. Both executors
  then decline to index the smaller side when doing so would give up an order
  the left already carries and the query is about to ask for.

  The cost comparison, since this can make a join slower to make a sort
  disappear. Splitting each measurement into a query and a control differing
  only in the trailing `order` separates the two effects:

  | ratio (right/left) | extra join cost of declining to swap | sort saved | net |
  |---|---|---|---|
  | 1.33 | −105 ms | 674 ms | −779 ms |
  | 4 | +51 ms | 160 ms | −109 ms |
  | 8 | +65 ms | 67 ms | −2 ms |
  | 16 | +80 ms | 33 ms | **+47 ms** |

  Break-even is around 8. The guard is 4, deliberately below it: the sort saved
  depends on the key type, and this measures a *string* key, the expensive
  case. An integer key reaches the pre-sorted data check, so far less is saved
  and break-even moves down with it. At 1.33 declining to swap is cheaper on
  its own, before the sort is counted at all.

  Neutrality re-checked on the same suite after this: 28 queries all `noise`,
  geometric mean 0.998, total +0.33%.

  Worth recording how nearly this shipped inert. The first run measured no
  effect whatsoever, because `lower_expr` -- the statement planner, which any
  script using `import` takes -- never calls `optimize_plan`, so the annotation
  pass sitting in the pass manager never ran. Every unit test passed throughout,
  since they go through `lower()`, which does optimize.
- ~~**Benchmark.**~~ Two measurements, because the suite could only supply one
  of them.

  *Neutrality*, `compare_ibex_git.sh --replica-control` over
  `join,null,events,sort,timeframe`, 15 repeats, `replica_binary=identical`:
  all 28 queries `noise`, geometric mean 0.999, total +0.13%. That is the
  regression check the added work needs — a scan over the emitted left-index
  array in both join routes, and a claim check in `order`.

  *Sort avoidance* had to be measured outside the suite, which has no
  join-then-order query. A new bench query would have existed on one side only
  and been silently dropped, so instead: one worktree at base, built, binary
  saved, the diff applied, rebuilt. Two scripts differing only in a trailing
  `order`, so the CSV read and the join cancel in the pair difference:

  | | cost of the second `order` (median of 9 paired repeats) |
  |---|---|
  | base | **+40 ms** (every repeat positive, 29–78) |
  | target | **+2 ms** (repeats straddle zero, −14 to +34) |

  38 ms off a 306 ms query. The key is a string, deliberately: the pre-existing
  pre-sorted data check covers a single ascending Timestamp/Date/Int key only,
  so before this change the sort was paid in full.

### Null-match policy

Status: implemented. The surface spelling is `nulls equal` / `nulls never`,
trailing the `suffix` clause.

- ~~Carry `NullMatch` through IR, interpreter, chunked execution, public runtime
  ops and codegen, with a surface spelling.~~ Chunked execution carries it by
  *declining* it: a join asking for `nulls equal` routes to the materialized
  implementation instead of teaching each streaming operator its own null
  tagging. One definition of the semantics, and the chunked hot path is
  untouched for every join that does not ask.
- ~~Tagged-null hashing and equality for scalar and composite keys.~~ Mostly
  already there: `hash_key_row` tags a null by position for group-by. What was
  missing was `key_rows_equal` comparing null-ness (it had never needed to) and
  the generic path passing validity into its `KeyCol`s at all.
- ~~Cover null/null, null/value, partially null composite keys, duplicate
  nulls, categorical nulls, and every join kind.~~
- ~~`SPEC.md` and `docs/index.html`.~~ Plus `docs/reference.html`, an `.ibex`
  example and a parity case.

`nulls`, `equal` and `never` are matched contextually in the join trailer, not
reserved. Reserving `nulls` broke `read_csv(path, nulls: String = "", ...)`,
whose own parameter is named that — the cost of a hard keyword, arriving
immediately rather than hypothetically.

Two pre-existing bugs surfaced, both the same shape as the one the null-key
rules exist to prevent — a null cell holds its type's zero, so anything
comparing values alone conflates the two:

- the materialized join dropped source-column validity while gathering, so a
  null came back as `0`. Commit *Stop the materialized join turning a source
  null into a zero*;
- the streaming semi/anti join built a set of key *values*, so a null key
  matched every genuine zero — `semi` kept rows it should drop and `anti`
  dropped rows it should keep. Commit *Stop a null key matching a genuine zero
  in semi and anti joins*.

Each landed on its own, before the feature: both touch the join hot path, which
is exactly what this package must not share a commit with.

Benchmark: 28 queries all `noise`, geometric mean 1.006, total +0.25%.

### Cardinality assertions and match selection

- Declared cardinality (1:1, 1:many, many:1, many:many) checked by the existing
  build/probe, returning a structured failure.
- Carry the resulting proofs into the output schema.
- `MatchSelection::First` / `Last` against an explicit order.
- Fast paths unlocked by a declared 1:1 join.

### Schema nullability

- Add a nullability flag to `ir::SchemaField`.
- Propagate through scan, filter, project, update, aggregate and join,
  including the outer-join rules.
- Decide what `Ascribe` asserts about nullability.
- Let adapters defer to the core once the core is authoritative.

### Time-domain joins

- Specify nearest-match/rolling joins on the `TimeFrame` time index: direction,
  bounds, tolerance, tie behaviour.
- Interval/overlap joins, initially expandable to compound inequalities over
  the theta path.
- Keep the ordering invariant intact through both.

## Sequencing

1. ~~**Ordering contract**~~ — done.
2. ~~**Side-qualified predicate references**~~ — done; the prerequisite for the
   next item is in place.
3. ~~**Collision resolution and `suffix`**~~ — done, while the whole corpus of
   Ibex programs was still this repo.
4. ~~**Early key validation**~~ — done; cheap, static, and it improves every
   frontend's errors.
5. ~~**Order-aware join planning**~~ — done; the payoff for step 1.
6. ~~**Null-match policy**~~ — done, on the benchmarked baseline step 5 left.
7. **Cardinality assertions and match selection**. Next.
8. **Schema nullability** — independent; pull forward if step 4 wants it.
9. **Time-domain joins** — the largest new feature, and the one most worth
   designing slowly.

Frontend work (below) interleaves: each adapter stage becomes possible as the
contract piece it depends on lands.

## Frontends

The contract above serves the interpreter, the transpiled C++, the Python
bridge and any adapter. Adapter-specific behaviour stays in the adapter.

### ribex (dplyr)

Currently native: `inner_join()` and `left_join()`, same-name keys,
`na_matches = "never"`, both inputs in one session.

- **Mapped keys.** Lower character `by` mappings to `on { left = right }`
  instead of renaming whole inputs. Available now.
- ~~**Suffixes.**~~ Done: the backend emits `suffix { ".x", ".y" }` and the
  post-join rename step is gone. dplyr's suffixes are an argument passed
  through, not a translation. A collision with an empty suffix on either side
  falls back rather than translating, since Ibex would then reject the join.
- **Row order.** dplyr guarantees left order; Ibex does not. The adapter adds
  an explicit `order` to the plan it builds. Any dplyr guarantee Ibex declines
  is the adapter's to reconstruct.
- **`na_matches = "na"`.** Maps to the explicit `Equal` policy once it exists.
- **Grouping.** Preserve `x$groups` for joins retaining the left columns; remap
  a group name if its column takes a suffix.
- **Filtering joins first.** `semi_join()` and `anti_join()` need none of the
  contract work above: left columns only, left grouping preserved, no
  collisions possible. They can land ahead of everything else here.
- **Then** `right_join()`, `full_join()`, `cross_join()`, keyed on nullability
  and collision work. `nest_join()` stays on fallback — Ibex has no
  list-column representation.
- **Coercion.** vctrs common-type rules live here: lower proven cases to
  explicit Ibex casts, reject the rest before submitting.

Unsupported forms must raise `ribex_translation_error` under
`fallback = "error"` so unsupported behaviour is never mistaken for native
execution.

### Python bridge and generated C++

Both consume the same contract and neither has an adapter layer to hide a
mismatch: a collision error or a null-match policy surfaces exactly as the
core defines it. Any contract change needs its parity case in
`tests/parity/cases/` so the interpreted and transpiled paths are checked
against each other.

## Test strategy

### Core tests

For each contract change, cover:

- parser and lowering shape;
- IR schema, required-column, cardinality, uniqueness and optimizer behaviour;
- materialized interpreter behaviour;
- chunked/deferred execution parity;
- transpiled C++ behaviour;
- diagnostics for missing keys, incompatible types and collisions;
- public runtime operation behaviour.

Parser/lexer/AST changes require the full suite. Public header/runtime changes
require plugin rebuilds. Every language-semantic change updates both `SPEC.md`
and `docs/index.html`, and new syntax needs an `.ibex` example.

When a change alters something existing tests assert, a green suite proves
nothing by itself: derive the new expectations independently, and say in the
commit message which tests changed meaning and why.

### Frontend oracle tests

For ribex, use local dplyr as the oracle for supported cases: run on a tibble,
run the same call on `ibex_tbl(..., fallback = "error")`, assert the result is
still an `ibex_tbl`, then compare values, row order, names, types, suffixes,
nulls, factor behaviour and grouping after collection.

The matrix must include: left smaller than right and the reverse; zero-row
inputs; duplicate keys on either and both sides; interleaved right matches;
null keys and partially null composite keys; same-name and mapped keys; grouped
keys and grouped non-key collisions; categorical/string and numeric common
types; collisions with and without a suffix clause; every supported kind and
option combination. Keep unsupported forms in a classification block expecting
`ribex_translation_error`.

Where a frontend reconstructs a guarantee Ibex declines — dplyr's row order —
test that reconstruction, not just the final agreement.

### Performance checks

Join key representation, collision resolution, ordering-aware planning,
nullable hashing, validation and match selection all touch hot paths. For each:

1. build the affected release target;
2. run the relevant workload against a pre-change baseline with
   `benchmarking/compare_ibex_git.sh`;
3. report correctness and performance together;
4. investigate materialized and chunked routes separately when their algorithms
   differ.

Never infer performance neutrality from identical output or from debug builds,
and land changes to the same hot path one at a time so a regression has one
candidate cause.

Run the comparison with `--replica-control`, which builds the base source a
second time and runs it as a third side. Its base-versus-replica delta is the
noise floor measured under the same conditions; `replica_binary=identical`
confirms the two builds agree bit for bit, so anything it reports is
measurement spread rather than code. Pass `IBEX_PERFCMP_TMPDIR` to a real disk:
`/tmp` is a RAM-backed tmpfs here, and three Release trees in it will exhaust
memory. Cap `CMAKE_BUILD_PARALLEL_LEVEL` well under the core count for the same
reason.

Check the query list in the report before reading the numbers. `bench_ibex.sh`
defaults each optional table to a path inside the worktree, where benchmarking
data does not exist, so a table the comparison fails to forward silently
removes every query needing it — from both sides at once, which keeps the
report self-consistent and gives nothing to notice.

### A reproducible delta is not a caused delta

Small effects need pairing, and pairing needs adjacency. Difference the two
sides *within* each repeat rather than comparing per-side medians: run-to-run
drift is common-mode and cancels, which is what pulls a sub-millisecond effect
out of a millisecond of spread. Two sides interleaved adjacently pair far
better than the same two sides several slots apart in a longer rotation.

Before believing a localized regression, confirm the changed code actually runs
in the affected query. A commit that inserts a function ahead of a hot one in
the same translation unit moves that hot code relative to cache lines and
branch-predictor state, and the resulting shift is stable across rebuilds
because layout is deterministic. It will reproduce under every protocol, at any
sample size, with an arbitrarily small p-value, and it means nothing. The
replica control cannot catch this: identical source yields identical layout, so
it correctly reports zero while the artifact sits between two *different*
commits.

So reproducibility is not evidence of causation here — it is exactly what the
artifact predicts. Read the diff and ask which changed lines the benchmarked
query executes. When every changed path is guarded off for that query, and
struct sizes are unchanged, layout is the remaining explanation and the delta
should not be recorded as a regression. A bisection over a range of commits
gets a free null control wherever two adjacent commits compile to identical
binaries, which is worth checking for with `cmp` before spending measurement
time on that rung.
