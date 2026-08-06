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

### Static schema inference disagrees with runtime output

Runtime join materialization suffixes colliding right-side columns with
`_right`, repeatedly if necessary. IR schema inference instead drops every
right-side column whose name is already present. It also unions right columns
into semi/anti join schemas even though those joins return only the left
columns at runtime.

There must be one canonical join-output schema/name planner shared in semantics
by IR inference, interpreter execution, chunked execution, and ribex schema
construction. It must define:

- semi/anti output as the left schema only;
- one output column for a same-name equijoin key;
- both native columns for differently named equijoin keys;
- deterministic `_right`, `_right_right`, ... naming for other collisions;
- outer-side nullability where the schema representation can carry it;
- which uniqueness proofs survive the join and any key-name mapping.

Native Ibex should retain both differently named key columns. The R adapter can
drop, coalesce, cast, or rename them to reproduce dplyr's `keep` contract.

### Row ordering is contradictory and data-size-dependent

The join section of `SPEC.md` says that joins preserve left-table row order and
append unmatched right rows in right-table order. The general ordering section
says that joins drop ordering unless a particular order is proved.

The materialized and chunked hash joins currently switch to right-scan output
order when the left input is the smaller build side. Consequently, the same
logical join can change row order with input size or execution route. dplyr's
mutating joins instead emit rows in left order, with each left row's matches in
right order; unmatched right rows follow for right/full joins.

The preferred Ibex contract is deterministic, left-stable output:

1. visit left rows in their original order;
2. emit each left row's matching right rows in right order;
3. append unmatched right rows in right order for right/full joins.

Building a hash table on the smaller left side may remain an execution choice,
but recorded hits must be scattered or reassembled into the semantic order.
This changes a hot path and may give back the measured locality benefit of
right-scan output, so it requires a release build and
`benchmarking/compare_ibex_git.sh` against the pre-change baseline.

If the cost proves unacceptable, the fallback design is an explicit join-order
policy with a `LeftStable` mode requested by ribex. An implicit size-dependent
order is not an acceptable semantic contract.

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

## Implementation stages

### Stage 0: Lock down the current boundary

- Add adversarial oracle cases to the existing inner/left support for a small
  left input and larger, interleaved right input. These should expose the
  current right-scan ordering difference rather than silently accepting it.
- Keep every unsupported option in a classification block that expects
  `ribex_translation_error` under `fallback = "error"`.
- Record local dplyr output for duplicate keys, null keys, suffix collisions,
  mapped keys, grouping, and key coercion before changing the implementation.

### Stage 1: Repair native join schema inference

Status: partially implemented alongside Stage 3. Semi/anti schemas, mapped-key
retention, and repeated `_right` collision naming now agree with runtime. A
shared output-schema planner and broader per-kind parity coverage remain.

- Introduce the canonical join-output name/schema planner.
- Make semi/anti inference return only the left schema.
- Make inferred collision names match runtime `_right` naming exactly.
- Correct uniqueness propagation for retained/dropped columns.
- Use the same planner in both materialized and chunked execution.
- Add IR schema tests, interpreter tests, chunked parity tests, and transpiled
  code tests for every join kind and repeated collision.

This stage can use same-name keys and should land before widening the key IR.

### Stage 2: Define and implement deterministic ordering

- Resolve the contradictory `SPEC.md` sections in favor of a single contract.
- Make materialized inner/left/right/outer joins left-stable.
- Make chunked and deferred-probe inner joins produce the same order.
- Preserve left order for semi/anti joins and nested-loop theta joins.
- Append unmatched right rows in right order for right/full joins.
- Test duplicates and fan-out on both sides, including paths selected by
  opposite build-side decisions.
- Benchmark the affected release-path workloads against the pre-change commit.

Do not expose more ribex mutating joins until this stage is complete, unless an
explicit stable-order policy has been implemented for the adapter.

### Stage 3: Represent asymmetric keys in the core

Status: implemented. Optimizer rewrites whose proofs require a shared column
name conservatively skip mapped joins; execution and analysis still use the
correct left/right names.

- Replace `vector<string>` join keys with `vector<JoinKey>` throughout AST/IR,
  builder APIs, cloning, optimizer passes, schema/cardinality proofs, required
  columns, interpreter, chunked runtime, public operations, and codegen.
- Keep a convenience constructor for same-name keys to reduce call-site noise.
- Add mapped-key surface syntax and parser/lowering tests.
- Ensure as-of joins identify their left and right time-index names explicitly.
- Add a mapped-key usage example in an `.ibex` file.
- Update both `SPEC.md` and `docs/index.html` in the same change.

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
- Add `semi_join()` and `anti_join()` first; their output is left-only and they
  preserve left grouping and row order.
- Add `right_join()` and `full_join()` after nullability, key coalescing, suffix,
  and unmatched-right ordering are covered.
- Add `cross_join()` with collision naming and left-major ordering tests.
- Keep `nest_join()` on explicit fallback.

### Stage 6: Add compatibility checks and proven coercions

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

Do not treat `join_by()` as one lowering project:

1. Same-name equality reuses ordinary `JoinKey` pairs.
2. Mapped equality reuses asymmetric `JoinKey` pairs.
3. Inequality lowers to theta joins, with explicit left/right column identity.
4. Overlap helpers can initially expand to compound inequalities and use the
   O(N×M) theta path.
5. Rolling joins require a nearest-match/as-of-style operator with explicit
   direction, bounds, tolerance, and tie behavior.

The current theta predicate namespace relies on combined output names and
`_right` suffixes. Before general inequality lowering, prefer side-qualified
join-predicate references in the IR; adapter-generated temporary right-column
names are an acceptable short-term bridge but not the long-term core model.

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
    Any,
};
```

Then add optional constraints for left/right uniqueness and unmatched-row
errors. Return structured join failures from the core; ribex is responsible for
translating them into dplyr-compatible R condition classes, warnings, and
messages.

Suggested progression:

1. `relationship` validation;
2. `unmatched = "error"`;
3. `multiple = "first"` and `"last"`;
4. `multiple = "any"` only after its determinism contract is explicit.

Unsupported combinations remain fallback until the relevant dplyr error side,
warning behavior, and output order are preserved.

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
