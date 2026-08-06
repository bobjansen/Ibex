# dplyr join support plan

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

## Semantic differences that must remain explicit

Do not silently translate any case below until its dplyr semantics are
implemented and covered by conformance tests.

| dplyr convention | Current Ibex / ribex position |
|---|---|
| Default `na_matches = "na"` matches R `NA` keys | Ibex null keys never match. Native joins require `na_matches = "never"`. |
| Named key mappings, e.g. `by = c(left_id = "right_id")` | Not translated. Ibex's simple equijoin surface requires common key names. |
| `join_by()` equality, inequality, rolling, and overlap predicates | Not translated by ribex. Ibex supports theta joins, but the R expression and output contract still need a dedicated lowering. |
| `keep = TRUE` | Not translated. Native joins omit the right key columns. |
| `multiple`, `unmatched`, and `relationship` | Not translated. Ibex performs all-match fan-out and has no dplyr cardinality validation layer. |
| dplyr/vctrs common-type coercion | Not translated. Ibex requires compatible native key types. |
| Grouped input | Not native-equivalent yet: dplyr retains left grouping, while the current native path clears grouping metadata. |
| `right_join()`, `full_join()`, `semi_join()`, `anti_join()`, `cross_join()`, and `nest_join()` | Not exposed through the ribex dplyr backend yet. |

Fallback with `fallback = "warn"` or `"collect"` is acceptable for these
forms. With `fallback = "error"`, they must raise `ribex_translation_error` so
unsupported behavior cannot be mistaken for native execution.

## Implementation stages

1. Preserve left grouping for native inner and left joins, and add oracle tests
   for grouped inputs.
2. Support named equality keys by adding temporary projections/renames around
   the Ibex join and reconstructing dplyr's key-column output.
3. Add `right_join()`, `full_join()`, `semi_join()`, and `anti_join()` where
   Ibex's output and grouping semantics match dplyr. Keep output ordering and
   suffix behavior under explicit tests.
4. Add type compatibility checks before query execution. Either lower a proven
   dplyr-compatible coercion or reject the call as unsupported; never defer an
   avoidable mismatch to an opaque execution error.
5. Design `join_by()` lowering. Start with same-name equality, then mapped
   equality. Treat inequalities, rolling joins, and overlap helpers as separate
   semantic features because their key retention, multiplicity, and ordering
   rules differ.
6. Decide whether `relationship`, `multiple`, and `unmatched` should become a
   preflight validation layer or remain explicit fallback. Do not claim native
   support until their dplyr error/warning behavior is preserved.
7. Investigate a native implementation of `na_matches = "na"`. It requires a
   null-safe equality relation; sentinel substitution is not acceptable because
   it can collide with real data.

## Test strategy

Use local dplyr as the oracle for every supported case:

1. Execute once on a tibble.
2. Execute the same call on `ibex_tbl(..., fallback = "error")`.
3. Assert the result is still an `ibex_tbl` before collecting it.
4. Compare values, names, types, suffixes, and grouping metadata after
   collection.

Keep unsupported dplyr forms in a separate classification block that expects
`ribex_translation_error`. Include cases for null keys, duplicate keys,
multiple overlaps, differently named keys, grouped left inputs, categorical
keys, and each supported output-column collision pattern.
