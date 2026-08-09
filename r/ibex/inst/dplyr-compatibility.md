# ibex dplyr compatibility

This table describes the native `ibex_tbl` MVP. “Fallback” means the complete
native prefix is collected once and the current verb is replayed by local
dplyr. The default in-memory policy warns; `fallback = "error"` prohibits that
collection and `fallback = "collect"` makes it silent.

| Area | Native contract | Falls back or errors |
|---|---|---|
| Sources | In-memory data frames, tibbles, and nanoarrow-compatible tables; ordered schema and row count are read without a query | Session-resident/plugin sources are not public constructors yet |
| `select`, `rename`, `relocate` | tidyselect is resolved against the stored schema in R; non-syntactic names are quoted centrally | A column name containing the Ibex template marker `${` is rejected until the lexer can quote it unambiguously |
| `filter` | Arithmetic, comparison, `%in%` with an atomic candidate vector, `&`, `|`, `!`, registered scalar functions, `between()`, `is.na()`, and `is.nan()`; `.by` | `.preserve = TRUE`, unknown/masked calls, R recycling, and unsupported captures |
| `mutate`, `transmute` | Named sequential fields, row-local registered expressions, captured length-one scalars; grouped aggregates broadcast with Ibex `by`; `min_rank(x)`, `dense_rank(x)`, `row_number(x)`, and `cume_dist(x)`; `.by` | `.keep` other than `"all"`, placement options, unknown/masked calls, arbitrary R closures, and unsupported rank helpers |
| `group_by`, `ungroup` | Existing columns, `.add`, `.drop = TRUE`; grouping remains lazy metadata | Computed grouping expressions and `.drop = FALSE` |
| `summarise` | `sum`, `mean`, `min`, `max`, `first`, `last`, and `n`; `.groups = "drop"`, `"drop_last"`, or `"keep"`; `.by` | Non-scalar summaries and unsupported aggregates |
| `arrange` | Column names and registered `dplyr::desc(column)`; `.by_group` | Computed sort expressions and masked `desc()` |
| `slice_head`, `head` | Constant non-negative `n`, including current groups | `prop`, `by`, and `slice_tail` |
| `distinct` | Existing selected columns; all columns by default | Computed/renamed keys and subset `.keep_all = TRUE` |
| `count`, `tally` | Lower to native grouping, aggregate, and optional ordering | Same restrictions as `group_by`, `summarise`, and `arrange` |
| joins | `inner_join`, `left_join`, `right_join` and `full_join` with character `by` keys, same-named or mapped (`by = c(a = "b")`), otherwise-default join options, and either `na_matches`; a local right data frame is bound into the left session; a captured scalar on one side survives the join | `join_by()` and non-equality conditions, `na_matches = "na"` on a floating-point key, `keep`, multiplicity and relationship options, a mapped key whose name also occurs in the other input, captured scalars on *both* sides, and `nest_join`, which needs a list column Ibex has no representation for |
| `cross_join` | The Cartesian product, with `suffix`; it takes no `by` or `na_matches`, and every name both inputs hold is a collision, since no key folds two columns into one | `copy = TRUE`, and a collision under an empty suffix |
| filtering joins | `semi_join` and `anti_join` under the same key and `na_matches` rules; they return the left columns unchanged and keep the input's grouping, since no column can be renamed or gain nulls | `copy = TRUE` and the key restrictions above, except that a mapped key cannot collide here |

## Types and missing values

R integer and `bit64::integer64` both map to Ibex `Int64`; double, logical,
character, factor/categorical, `Date`, and `POSIXct` use the existing
ibex/Arrow bridge.

A `factor` binds as Ibex `Categorical` and collects back as a `factor`: the
Arrow bridge carries a dictionary array in both directions, so the codes and
levels survive the round trip rather than being decoded to one R string per
row. A `character` column binds as Ibex `String` and collects back as
`character`.

`ibex_tbl(categorical_strings = TRUE)` binds `character` columns as
`Categorical` instead. Grouping, `distinct()` and sorting then compare dense
integer codes rather than hashing text per row, which on 8M rows over 252
distinct symbols is 3ms against 100ms; the encoding itself is free, because R
interns strings and the codes come from pointer identity. The cost is on the
way back — those columns collect as factors, and a result that returns most of
its rows pays to rebuild them — so it is off by default and worth turning on
for grouping and sorting workloads.

`as.Date(x)` on a `POSIXct` or `Date` column translates to Ibex's `Date()`
cast, which truncates an instant to the calendar day containing it on **UTC** —
the same boundary `as.Date.POSIXct` uses, since its own `tz` default is
`"UTC"`. Passing any other `tz`, a `format`, or a character column is not
translated, because each would cut days somewhere Ibex's cast does not.

Int64 comes back to R as a double, because R has no 64-bit integer vector.
That widening is not undone when the values would fit in an `integer()`: a
column's R type is taken from the schema, never from its contents, so
filtering out one large row cannot change it. A double holds every integer up
to 2^53 exactly and only some beyond, so collecting a column with a value at
or past that bound warns and names the column. Values that need to survive
intact should be carried with `bit64::integer64`, which binds as `Int64`
exactly. The schema endpoint
reports nullability, categorical encoding, time zone, ordering, and time-index
metadata. Operator metadata propagation remains the authority after execution.

Ibex null is the native representation of R `NA`. IEEE `NaN` remains a present
floating-point value. Consequently, translated `is.na(x)` tests Ibex nulls and
does not include `NaN`; use `is.nan(x)` when that distinction matters.

The same distinction bounds `na_matches = "na"`, dplyr's default. It maps to
Ibex's `nulls equal`, which matches null keys to each other — but dplyr also
matches `NaN` to `NaN`, and to Ibex a `NaN` is a present value like any other.
R keeps the two apart as well (a `NaN` key never matches an `NA` one), so no
rewrite recovers dplyr's answer. A join with a floating-point key therefore
falls back under `"na"`; `na_matches = "never"` matches no `NaN` on either
side, so it stays native for every key type.

A mapped key (`by = c(a = "b")`) is where the two column models differ rather
than agree. Ibex keeps both halves of the pair, because for an unmatched row
they hold different things; dplyr reports one column under the left name. The
backend reconciles this by projecting the right key away — and, for the kinds
that emit rows with no left side, by merging with `coalesce()` first, so a
`right_join`'s key carries the right value exactly where dplyr's does. The one
shape that falls back is a mapped key whose name also occurs in the other
input: Ibex reads that as a collision and suffixes it, while dplyr, which
drops the right key outright, has nothing to resolve and leaves both names
alone.

`.by` and `group_by()` lower to the same Ibex `by` clause and differ only in
what survives the call: `.by` leaves the result ungrouped, so there is no
`.groups` to honour and no trailing key to drop. Its three misuses — `.by` on
an already-grouped table, `.by` together with `.groups`, and renaming inside
`.by` — raise dplyr's own errors rather than falling back, since each is a
mistake in the call and not a limit of the translation. On `filter()` the
argument is checked and then discarded: a predicate with no aggregate in it
asks the same question of a row whatever group the row is in, and native
`filter()` refuses aggregates anyway.

Neither spelling makes a claim about the order of the groups in the result.
dplyr sorts them under `group_by()` and reports them in order of first
appearance under `.by`; Ibex is asked for neither, and an aggregate discards
ordering (see below), so a pipeline that depends on group order should end in
`arrange()`.

A captured scalar — an R value that reached the plan through the scalar
registry — is carried across a join, so a `filter(x > threshold)` before an
`inner_join()` stays native. Both inputs are evaluated in one scalar
environment, and captures are named per table counting from one, so two sides
that each captured something claim the same name for different values. That
case falls back; anything with captures on at most one side does not.

Ibex aggregates skip nulls. For a nullable input, native aggregate translation
therefore requires an explicit `na.rm = TRUE` (or `na_rm = TRUE` for
`first`/`last`).

Whether an input *is* nullable is decided by Ibex, not by the backend: each verb
hands the plan it has built to the core's schema inference and takes the answer
back. So a column the plan proves null-free needs no `na.rm`, and the proofs are
the core's — a `filter` proves the columns its predicate had to read (`x > 0`
and `!is.na(x)` alike, since a null predicate drops its row just as a false one
does), an inner join proves its key columns, and a left join withdraws every
proof on its right side. The key column a join folds is the interesting one:
it holds a value from whichever side turned up, so it keeps its proof exactly
when every side whose unmatched rows survive has one — both for `full_join`,
only the right for `right_join`, only the left for `left_join`. The filtering
joins answer the same question from opposite ends: under `na_matches =
"never"` a null key matches nothing, so `semi_join` — which keeps the rows
that matched — proves its key, while `anti_join` keeps exactly the rows that
did not and cannot. A plan the core cannot describe falls back to assuming
every column nullable, which only ever costs native execution.

All-null and empty inputs can still differ: Ibex returns null
for value-bearing aggregates, while some R aggregates return a sentinel such as
zero or infinity. Pipelines that require R's empty-input sentinel should use
local dplyr.

Ibex does not implement R vector recycling. Only columns and supported
length-one scalar captures are translated. Captured values cross through the
scalar registry and appear as typed, redacted placeholders in `show_query()`;
their values are never interpolated into source.

## Ordering, grouping, and lifecycle

An in-memory source starts with observable encounter order. Filters and
row-local updates preserve it, projections retain it only while every ordering
key survives, and aggregation/distinct/grouped update discard it. `arrange()`
establishes an explicit ordering. `slice_head()` without `arrange()` uses the
current encounter order, as it does for an in-memory dplyr input.

`ibex_tbl` plans are immutable R descriptions: branching a source does not
mutate either branch. `compute()` materializes to a fresh binding in the same
session without transferring values to R. `collect()` returns a tibble and
reapplies current grouping metadata. Resetting the owning session changes its
generation and invalidates every dependent lazy table with a dedicated error.

`ibex_tbl()` binds into one shared per-R-session default, `ibex_default_session()`,
because tables can only be combined natively when they live in the same
session: a fresh session per table would make even
`inner_join(ibex_tbl(a), ibex_tbl(b))` fall back to local dplyr. Bindings in
it live as long as the R session, so `ibex_reset_default_session()` discards
them, and an explicit `session =` argument opts out.

Arbitrary R callbacks are deliberately absent from the MVP. An explicit,
vectorized main-thread R UDF barrier remains a post-MVP design; no R object or
closure is placed in `ExternRegistry` or invoked on an Ibex worker.
