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

- Add `suffix { "_left", "_right" }` to the join grammar, AST, IR node, builder
  API, codegen and public runtime ops.
- Extend `ir::plan_join_output()` to take the suffix policy and to report a
  collision instead of inventing a name.
- Report collisions statically where both schemas are known, at runtime
  otherwise, naming both sides and the column.
- Error when a suffixed name still collides.
- Sweep the repo for programs relying on automatic `_right`.
- `SPEC.md`, `docs/index.html`, an `.ibex` example, and parity coverage for the
  suffixed and error paths.

### Early key validation

- Validate key existence and type compatibility before execution when both
  schemas are known.
- Name side, key and type in the message; improve the runtime diagnostic for
  the unknown-schema case the same way.
- Tests for missing keys, mismatched types, mapped keys, and open schemas.

### Order-aware join planning

- Recognize an `order` above a join and let the join emit in that order when a
  path can do so without extra work.
- Include the pending sort in build-side selection.
- Let a join prove an ordering constraint when its chosen path preserves one,
  so a redundant downstream `order` folds away.
- Benchmark: this is the payoff for not promising an order, so it needs a
  measured sort-avoidance result, not just correctness.

### Null-match policy

Must not share a commit with order-aware planning or any other hot-path change:
both rewrite hashing and probing, and a combined benchmark cannot attribute a
regression.

- Carry `NullMatch` through IR, interpreter, chunked execution, public runtime
  ops and codegen, with a surface spelling.
- Tagged-null hashing and equality for scalar and composite keys.
- Cover null/null, null/value, partially null composite keys, duplicate nulls,
  categorical nulls, and every join kind.
- `SPEC.md` and `docs/index.html`.

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
3. **Collision resolution and `suffix`** — the largest language-visible change;
   do it while the whole corpus of Ibex programs is still this repo.
4. **Early key validation** — cheap, static, improves every frontend's errors.
5. **Order-aware join planning** — the payoff for step 1.
6. **Null-match policy** — after step 5 has a benchmarked baseline.
7. **Cardinality assertions and match selection**.
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
`na_matches = "never"`, both inputs in one session. The backend renames
Ibex's `_right` columns to dplyr's `.x`/`.y` after the join.

- **Mapped keys.** Lower character `by` mappings to `on { left = right }`
  instead of renaming whole inputs. Available now.
- **Suffixes.** Once the `suffix` clause exists, ask for `.x`/`.y` directly
  and delete the post-join rename step entirely. dplyr's suffixes become an
  argument passed through, not a translation.
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
