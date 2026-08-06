# Join correctness and dplyr support plan

## Purpose

`ribex` join development exposed several limitations in Ibex's native join
model. Some missing behavior belongs only in the R adapter, but asymmetric key
names, output-schema inference, row ordering, null equality, and key validation
are flaws or missing capabilities in Ibex proper.

The core join contract should be corrected before the dplyr backend exposes
more verbs. Otherwise each new translation will add projections, renames, and
schema reconstruction around an IR that cannot represent the operation
directly.

## How to read the stages

Stage numbers are **labels, not an order**. They are referenced from commit
messages and should stay stable. The recommended order of the remaining work is
in [Sequencing](#sequencing).

Landed so far:

- Stage 3, asymmetric keys — commit *Represent asymmetric equijoin keys
  natively*.
- Stage 1, canonical output planner — commit *Share one canonical join-output
  schema planner*.

## Current native subset

`ribex` currently translates only these joins natively:

```r
inner_join(x, y, by = "id", na_matches = "never")
left_join(x, y, by = "id", na_matches = "never")
```

Both inputs must use the same Ibex session. A local right-hand data frame is
bound into the left-hand session. Keys must have the same name on both inputs,
and the join uses ordinary all-match fan-out.

The backend preserves dplyr's `.x`/`.y` suffixes for simple overlapping
non-key columns, translating Ibex's internal `_right` names after the join.

## Core findings

### Asymmetric equijoin keys (implemented in Ibex core)

`JoinNode` now stores `std::vector<JoinKey>`, so a pair such as
`left_id = right_id` survives from parsing through lowering, optimization,
interpretation, and generated C++. Required-column and cardinality analysis,
schema and uniqueness proofs, the chunked executor, and public runtime
operations use the paired representation too.

Temporary renames in ribex could bypass the restriction, but they would make
key retention, `keep`, optimizer proofs, diagnostics, and collision handling
progressively more fragile. Native key pairs avoid that workaround.

Implemented shape:

```cpp
struct JoinKey {
    std::string left;
    std::string right;
};
```

A same-name key is represented as `{"id", "id"}`. The mapped surface spelling
is:

```ibex
x left join y on { left_id = right_id }
```

### Static schema inference disagreed with runtime output (fixed in Ibex core)

Runtime join materialization suffixed colliding right-side columns with
`_right`, repeatedly if necessary. IR schema inference instead dropped every
right-side column whose name was already present, and unioned right columns
into semi/anti join schemas even though those joins return only the left
columns at runtime.

`ir::plan_join_output()` (`include/ibex/ir/join_output.hpp`) is now the single
canonical join-output schema/name planner. IR inference, the materialized
interpreter and the chunked executor all derive their column list from it;
codegen inherits it through the runtime ops, and ribex's own reconstruction
follows the same rules. It defines:

- semi/anti output as the left schema only;
- one output column for a same-name equijoin key;
- both native columns for differently named equijoin keys;
- deterministic `_right`, `_right_right`, ... naming for other collisions;
- which uniqueness proofs survive the join and any key-name mapping.

Native Ibex retains both differently named key columns. The R adapter can drop,
coalesce, cast, or rename them to reproduce dplyr's `keep` contract.

### The IR schema cannot express outer-side nullability

`SchemaField` carries a name and a type, and nothing else. A `left join`
therefore infers an output schema claiming the right-side columns are exactly
as non-null as their source, which is false for every unmatched left row. The
planner has nowhere to record the difference, so it deliberately does not try.

Today this costs little: ribex tracks nullability in its own schema proxy and
sets it for the outer side itself. It starts to cost something as soon as
Stage 6 reasons about key types, or any pass wants to prove a column null-free.
The fix is a field-level nullability flag in `ir::SchemaField` plus propagation
rules through the row-wise operators — a change with reach well beyond joins,
which is why it is its own stage (Stage 10) rather than a clause inside another.

### Row ordering is contradictory and data-size-dependent

`SPEC.md` contradicts itself. The join section ("Row ordering") says the output
preserves left-table row order for all left-side rows and appends unmatched
right rows in right-table order. The ordering-constraint section says that any
`join` drops ordering unless a particular order is proved. Both are normative
text about the same operation.

The implementation matches neither consistently. The materialized and chunked
hash joins switch to right-scan output order when the left input is the smaller
build side, so the same logical join changes row order with input size or
execution route. dplyr's mutating joins instead emit rows in left order, with
each left row's matches in right order; unmatched right rows follow for
right/full joins.

The intended Ibex contract is deterministic, left-stable output:

1. visit left rows in their original order;
2. emit each left row's matching right rows in right order;
3. append unmatched right rows in right order for right/full joins.

Building a hash table on the smaller left side may remain an execution choice,
but recorded hits must be scattered or reassembled into the semantic order.

Note that the current behavior is *pinned by tests*, not merely unspecified.
`tests/test_join.cpp` asserts right-scan emission explicitly, each against a
hand-built reference:

- `join: left join emits matches in right-scan order when left side is smaller`
- `join: outer join emits matches in right-scan order when left side is smaller`
- `join: right join emits matches in right-scan order, then unmatched right
  rows, when left ...`
- `join: multi-key outer join emits matches in right-scan order when left side
  is smaller`
- `join: swapped-mode inner join emits in right-scan order`

Those tests are the old contract, so they must be inverted as part of the
change. "The suite still passes" is not evidence for this stage — the new
expectations have to be derived from dplyr or from a hand-built left-stable
reference.

An explicit join-order policy with a `LeftStable` mode requested by ribex is
**not** the preferred fallback. Two orderings selected by an adapter flag is a
permanent tax: every later feature has to be specified twice, and
`multiple = "first"` (Stage 9) is meaningless without a single defined order.
Reassembly is a counting sort over output rows — one extra pass and one extra
index array against a gather that already dominates. If that measures badly,
that is a result worth having in writing before conceding the design.

### Theta predicates and join output use different namespaces

Two namespaces are built from the same two inputs, by different code, with
different rules:

- The **output** namespace comes from `ir::plan_join_output()`. It folds a
  same-name equijoin key into one column and suffixes the remaining
  collisions.
- The **predicate** namespace is built inline in `join_table_impl` for the
  nested-loop path. It keeps *every* right column and suffixes purely on
  collision, with no key folding.

So in a join whose inputs both have `id`, a theta predicate can reference
`id_right`, but no output column of that name can ever exist. The same query
text resolves against two different column sets depending on where it appears.

This was always latent; it is now a definite divergence rather than an accident,
because the output side has a specification and the predicate side does not.
Side-qualified join-predicate references in the IR are the principled fix, and
Stage 8 should not start without them — adapter-generated temporary
right-column names would bake this into ribex's surface.

### Null matching needs a real equijoin policy

Ibex currently excludes any row with a null key from both hash-index insertion
and probing. This correctly implements `na_matches = "never"`, but cannot
implement dplyr's default `na_matches = "na"`.

Add an equijoin option such as:

```cpp
enum class NullMatch {
    Never,
    Equal,
};
```

Under `Equal`, hash keys include a tagged null value and null equals only null.
No sentinel substitution may be used because a sentinel can collide with real
data. For composite keys, every corresponding component must match under the
selected policy. Start in the generic hash path, then optimize nullable
single-key and chunked paths.

This policy applies to equijoins only. Theta-join comparisons retain ordinary
three-valued/null predicate semantics.

### Type incompatibility is detected too late

The runtime currently checks only that the physical key kinds match and emits
a terse error after execution reaches the join. When both input schemas are
known, Ibex should validate key existence and compatibility before execution
and report both key names and types.

Ibex should remain strict rather than silently choosing a common type. The
ribex adapter should use vctrs/dplyr common-type rules, materialize temporary
casted keys with explicit Ibex casts for proven cases, and reject unsupported
cases before submitting the query.

An initial safe coercion set is:

- `Int64` plus `Float64` to `Float64`;
- String plus categorical through textual equality;
- identical Date types;
- identical Timestamp types with compatible timezone metadata.

Factor-level reconciliation, classed vctrs types, lossy numeric conversions,
and Date/Timestamp mixtures remain fallback until covered individually.

## Semantic differences that must remain explicit

Do not silently translate any case below until its dplyr semantics are
implemented and covered by conformance tests.

| dplyr convention | Current Ibex / ribex position |
|---|---|
| Mutating joins preserve left row order | Some Ibex hash-join shapes emit right-scan order. |
| Default `na_matches = "na"` matches R `NA` keys | Ibex null keys never match. Native joins require `na_matches = "never"`. |
| Named key mappings, e.g. `by = c(left_id = "right_id")` | Ibex supports key pairs; ribex lowering still needs to emit them. |
| `join_by()` equality, inequality, rolling, and overlap predicates | Not translated by ribex. Ibex supports theta joins, but the R expression and output contract need dedicated lowering. |
| `keep = TRUE` | Not translated. Native same-name joins omit the right key columns. |
| `multiple`, `unmatched`, and `relationship` | Ibex performs all-match fan-out and has no cardinality validation layer. |
| dplyr/vctrs common-type coercion | Ibex requires compatible native key types. |
| Grouped input | dplyr retains left grouping; the current native path clears grouping metadata. |
| `right_join()`, `full_join()`, `semi_join()`, `anti_join()`, and `cross_join()` | Implemented by Ibex but not exposed through ribex. |
| `nest_join()` | Ibex has no list-column/table-column representation; retain fallback. |

Fallback with `fallback = "warn"` or `"collect"` is acceptable for these
forms. With `fallback = "error"`, unsupported forms must raise
`ribex_translation_error` so unsupported behavior cannot be mistaken for
native execution.

## Sequencing

Recommended order for the remaining work, and why:

1. **Stage 0 (oracle baselines)** — cheap, and the only external oracle that
   exists while current behavior is still observable. Must precede Stage 2.
2. **Stage 2a (resolve the `SPEC.md` contradiction)** — a documentation-only
   change that decides the contract before any hot path moves.
3. **Stage 6 (key validation and diagnostics)** — the best value-per-risk item
   left. It is a static pass with no hot-path exposure, and every later ribex
   stage benefits from errors that name the side, key, and type.
4. **Stage 5, semi/anti only** — safe to expose ahead of the ordering work; see
   the stage for why.
5. **Stage 2b (left-stable ordering)** — the keystone. Everything below either
   depends on a defined order or would confound its benchmarks.
6. **Stage 4, then the rest of Stage 5** — ribex mapped keys, grouping, then
   the remaining join kinds.
7. **Stage 7 (null-safe equality)** — deliberately *after* Stage 2b has a
   benchmarked baseline: both rewrite the same hashing and probing paths, and
   landing them together makes a regression unattributable.
8. **Stage 10 (schema nullability)** — independent of the rest; pull it forward
   if Stage 6 starts wanting it.
9. **Stage 8 (`join_by()`)**, gated on side-qualified predicate references.
10. **Stage 9 (match selection and cardinality validation)** last: it is the
    only stage that needs everything else to be true first.

## Implementation stages

### Stage 0: Lock down the current boundary

- Add adversarial oracle cases to the existing inner/left support for a small
  left input and larger, interleaved right input. These should expose the
  current right-scan ordering difference rather than silently accepting it.
- Keep every unsupported option in a classification block that expects
  `ribex_translation_error` under `fallback = "error"`.
- Record local dplyr output for duplicate keys, null keys, suffix collisions,
  mapped keys, grouping, and key coercion before changing the implementation.

Do this before Stage 2. Once ordering changes, the current outputs cannot be
reproduced to compare against.

### Stage 1: Repair native join schema inference

Status: implemented. `ir::plan_join_output()` (`include/ibex/ir/join_output.hpp`)
is the canonical planner; IR schema inference, the materialized join and the
chunked inner join all derive their output columns from it, and codegen
inherits it through the runtime ops. Uniqueness proofs stay in
`add_join_unique_keys` — the planner decides names, not proofs — but they now
follow the planner's names instead of assuming a colliding right column is
dropped.

- ~~Introduce the canonical join-output name/schema planner.~~
- ~~Make semi/anti inference return only the left schema.~~
- ~~Make inferred collision names match runtime `_right` naming exactly.~~
- ~~Correct uniqueness propagation for retained/dropped columns.~~ A right-side
  proof now survives under the planner's output name (`code` → `code_right`),
  and mapped joins no longer forfeit their right-side proofs.
- ~~Use the same planner in both materialized and chunked execution.~~
- ~~Add IR schema tests, interpreter tests, chunked parity tests, and
  transpiled code tests for every join kind and repeated collision.~~

Outer-side nullability was explicitly left out; it is Stage 10.

### Stage 2a: Resolve the ordering contract in `SPEC.md`

A documentation-only change, landed on its own:

- Decide in favor of the left-stable contract described in the findings.
- Rewrite the ordering-constraint section's blanket "any `join` drops ordering"
  so it agrees with the join section, or vice versa — but only one survives.
- State explicitly that the contract is independent of build-side selection and
  of the materialized/chunked route.
- Mirror the wording in `docs/index.html`.

Landing this first means the implementation stage has something to be checked
against, and a reviewer can disagree with the contract before the hot path is
touched.

### Stage 2b: Implement deterministic ordering

- Make materialized inner/left/right/outer joins left-stable.
- Make chunked and deferred-probe inner joins produce the same order.
- Preserve left order for semi/anti joins and nested-loop theta joins.
- Append unmatched right rows in right order for right/full joins.
- Invert the five right-scan-order tests listed in the findings; derive the new
  expectations from dplyr or a hand-built left-stable reference, not from the
  new implementation's own output.
- Test duplicates and fan-out on both sides, including paths selected by
  opposite build-side decisions.
- Benchmark the affected release-path workloads against the pre-change commit,
  materialized and chunked routes separately.

Do not expose more ribex *mutating* joins until this stage is complete. Semi
and anti joins are not gated on it (Stage 5).

### Stage 3: Represent asymmetric keys in the core

Status: implemented. Optimizer rewrites whose proofs require a shared column
name conservatively skip mapped joins; execution and analysis still use the
correct left/right names.

- ~~Replace `vector<string>` join keys with `vector<JoinKey>` throughout
  AST/IR, builder APIs, cloning, optimizer passes, schema/cardinality proofs,
  required columns, interpreter, chunked runtime, public operations, and
  codegen.~~
- ~~Keep a convenience constructor for same-name keys to reduce call-site
  noise.~~
- ~~Add mapped-key surface syntax and parser/lowering tests.~~
- ~~Ensure as-of joins identify their left and right time-index names
  explicitly.~~
- ~~Add a mapped-key usage example in an `.ibex` file.~~
- ~~Update both `SPEC.md` and `docs/index.html` in the same change.~~

### Stage 4: Add ribex named equality keys and preserve grouping

- Parse character `by` mappings into left/right key pairs.
- Use the native mapped join rather than renaming whole inputs.
- For `keep = FALSE`, reconstruct dplyr's key output by dropping or coalescing
  the right key as required by the join kind.
- Preserve `x$groups` for joins that retain the left columns.
- Remap a group name if its left column receives the configured left suffix.
- Clear or preserve ribex ordering metadata according to the now-explicit core
  ordering contract.
- Test grouped keys, grouped colliding non-keys, mapped keys, suffixes, and
  `keep = FALSE` against local dplyr.

### Stage 5: Expose existing native join kinds

- Render join kinds through an explicit mapping instead of the current
  inner-versus-left conditional.
- Add `semi_join()` and `anti_join()` **first, and ahead of Stage 2b**. They are
  filtering joins, not mutating ones: the output is the left columns, in left
  order, on every execution route — `ChunkedSemiAntiJoinOperator` filters left
  chunks rather than reassembling matches, and the materialized path walks the
  left side. They preserve left grouping too, so they do not depend on the
  ordering work at all.
- Add `right_join()` and `full_join()` after nullability, key coalescing,
  suffix, and unmatched-right ordering are covered.
- Add `cross_join()` with collision naming and left-major ordering tests.
- Keep `nest_join()` on explicit fallback.

### Stage 6: Add compatibility checks and proven coercions

The cheapest real win left: a static pass, no hot-path exposure, and it turns an
opaque mid-execution error into a diagnostic naming side, key, and type.
`ir::check_ascriptions` is the model to copy — same shape of pass, same place in
the pipeline, same style of message.

- Validate known key schemas before query execution.
- Improve runtime diagnostics for unknown schemas by naming the side, key, and
  physical type involved.
- Have ribex determine the dplyr/vctrs common type from schema proxies.
- Lower supported coercions to temporary explicitly casted keys while retaining
  original columns needed for final output.
- Ensure the surviving/coalesced key has dplyr's common output type.
- Reject every unproven coercion as unsupported rather than deferring to an
  opaque execution error.

### Stage 7: Implement null-safe equijoin equality

Must not land in the same change as Stage 2b — both rewrite the hashing and
probing paths, and a combined benchmark cannot attribute a regression.

- Carry `NullMatch` through IR, interpreter, chunked execution, public runtime
  operations, and codegen.
- Implement tagged-null hashing and equality for scalar and composite keys.
- Cover null/null, null/value, partially null composite keys, duplicate nulls,
  categorical nulls, and every outer/semi/anti join kind.
- Expose `na_matches = "na"` in ribex only after oracle tests cover values,
  types, multiplicity, ordering, and output keys.
- Update both `SPEC.md` and `docs/index.html` because this changes language
  behavior/API semantics.

### Stage 8: Split `join_by()` into separate feature families

Blocked on side-qualified join-predicate references in the IR (see the
namespace finding). Do not treat `join_by()` as one lowering project:

1. Same-name equality reuses ordinary `JoinKey` pairs.
2. Mapped equality reuses asymmetric `JoinKey` pairs.
3. Inequality lowers to theta joins, with explicit left/right column identity.
4. Overlap helpers can initially expand to compound inequalities and use the
   O(N×M) theta path.
5. Rolling joins require a nearest-match/as-of-style operator with explicit
   direction, bounds, tolerance, and tie behavior.

The prerequisite is its own piece of work: give predicate column references an
explicit side, so `on a < b` names left `a` and right `b` without routing
through combined output names. Adapter-generated temporary right-column names
are an acceptable short-term bridge for an experiment, but not something to
expose through ribex.

### Stage 9: Add match selection and cardinality validation

Avoid an eager ribex preflight made from separate count queries. It duplicates
work and can separate validation from the data execution being validated. The
join build/probe already observes duplicate keys, per-row match counts, and
unmatched rows.

Introduce generic execution options such as:

```cpp
enum class MatchSelection {
    All,
    First,
    Last,
};
```

Then add optional constraints for left/right uniqueness and unmatched-row
errors. Return structured join failures from the core; ribex is responsible for
translating them into dplyr-compatible R condition classes, warnings, and
messages.

Suggested progression:

1. `relationship` validation;
2. `unmatched = "error"`;
3. `multiple = "first"` and `"last"`.

`multiple = "any"` is deliberately absent from the enum. dplyr defines it as
"whichever is fastest", which is an anti-contract: the first user whose test
depends on the observed choice makes it load-bearing forever. Keep it on
fallback. If it is ever implemented, it must be spelled as a distinct,
documented choice rule, not as permission to be arbitrary.

Unsupported combinations remain fallback until the relevant dplyr error side,
warning behavior, and output order are preserved.

### Stage 10: Carry nullability in the IR schema

Independent of the join work in principle, but the joins are what expose it.

- Add a nullability flag to `ir::SchemaField`.
- Propagate it through scan, filter, project, update, aggregate, and join —
  including the outer-join rule that right columns become nullable in a `left
  join`, and both sides in an `outer join`.
- Extend `ir::plan_join_output()`'s consumers to set it; the planner itself
  stays a naming authority and does not need to change.
- Decide what an `Ascribe` node asserts about nullability, since it currently
  fixes the visible schema.
- Reconcile with ribex's own schema proxy, which tracks nullability today and
  should defer to the core once the core is authoritative.

## Test strategy

### Ribex oracle tests

Use local dplyr as the oracle for every supported case:

1. Execute once on a tibble.
2. Execute the same call on `ibex_tbl(..., fallback = "error")`.
3. Assert the result is still an `ibex_tbl` before collecting it.
4. Compare values, row order, names, types, suffixes, nulls, factor behavior,
   and grouping metadata after collection.

The matrix must include:

- left smaller than right and right smaller than left;
- zero-row inputs on either side;
- duplicate keys on either and both sides;
- interleaved right matches for successive left rows;
- null keys and partially null composite keys;
- same-name and differently named keys;
- grouped keys and grouped non-key collisions;
- categorical/string and supported numeric common types;
- repeated output-column collisions;
- `keep = TRUE` and `FALSE` where supported;
- every supported join kind and option combination.

Keep unsupported dplyr forms in a separate classification block that expects
`ribex_translation_error`.

### Ibex core tests

For each core stage, cover:

- parser and lowering shape;
- IR schema, required-column, cardinality, uniqueness, and optimizer behavior;
- materialized interpreter behavior;
- chunked/deferred execution parity;
- transpiled C++ behavior;
- diagnostics for missing keys and incompatible types;
- public runtime operation behavior.

Parser/lexer/AST changes require the full test suite. Public header/runtime
changes require plugin rebuilds. Every language-semantic change must update
both `SPEC.md` and `docs/index.html`, and new syntax requires an `.ibex` usage
example.

When a stage changes a contract that existing tests assert (Stage 2b is the
clear case), a green suite proves nothing by itself. Derive the new
expectations from dplyr or from an independent hand-built reference, and say in
the commit message which tests were inverted and why.

### Performance checks

Join ordering, key representation, nullable hashing, validation, and match
selection all touch hot execution paths. For each such change:

1. build the affected release target;
2. run the relevant join workload against a pre-change baseline with
   `benchmarking/compare_ibex_git.sh`;
3. report correctness and performance together;
4. investigate materialized and chunked routes separately when their algorithms
   differ.

Never infer performance neutrality from identical output or from debug builds.
Land changes to the same hot path one at a time, so a regression has one
candidate cause.
