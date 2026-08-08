# ribex dplyr compatibility

This table describes the native `ibex_tbl` MVP. “Fallback” means the complete
native prefix is collected once and the current verb is replayed by local
dplyr. The default in-memory policy warns; `fallback = "error"` prohibits that
collection and `fallback = "collect"` makes it silent.

| Area | Native contract | Falls back or errors |
|---|---|---|
| Sources | In-memory data frames, tibbles, and nanoarrow-compatible tables; ordered schema and row count are read without a query | Session-resident/plugin sources are not public constructors yet |
| `select`, `rename`, `relocate` | tidyselect is resolved against the stored schema in R; non-syntactic names are quoted centrally | A column name containing the Ibex template marker `${` is rejected until the lexer can quote it unambiguously |
| `filter` | Arithmetic, comparison, `%in%` with an atomic candidate vector, `&`, `|`, `!`, registered scalar functions, `between()`, `is.na()`, and `is.nan()` | `.by`, `.preserve = TRUE`, unknown/masked calls, R recycling, and unsupported captures |
| `mutate`, `transmute` | Named sequential fields, row-local registered expressions, captured length-one scalars; grouped aggregates broadcast with Ibex `by`; `min_rank(x)`, `dense_rank(x)`, `row_number(x)`, and `cume_dist(x)` | `.by`, `.keep` other than `"all"`, placement options, unknown/masked calls, arbitrary R closures, and unsupported rank helpers |
| `group_by`, `ungroup` | Existing columns, `.add`, `.drop = TRUE`; grouping remains lazy metadata | Computed grouping expressions and `.drop = FALSE` |
| `summarise` | `sum`, `mean`, `min`, `max`, `first`, `last`, and `n`; `.groups = "drop"`, `"drop_last"`, or `"keep"` | `.by`, non-scalar summaries, unsupported aggregates |
| `arrange` | Column names and registered `dplyr::desc(column)`; `.by_group` | Computed sort expressions and masked `desc()` |
| `slice_head`, `head` | Constant non-negative `n`, including current groups | `prop`, `by`, and `slice_tail` |
| `distinct` | Existing selected columns; all columns by default | Computed/renamed keys and subset `.keep_all = TRUE` |
| `count`, `tally` | Lower to native grouping, aggregate, and optional ordering | Same restrictions as `group_by`, `summarise`, and `arrange` |
| joins | `inner_join`, `left_join`, `right_join` and `full_join` with same-named character keys, otherwise-default join options, and `na_matches = "never"`; a local right data frame is bound into the left session | Different key names / `join_by()`, dplyr's default NA matching, `keep`, multiplicity and relationship options, and the remaining kinds (`semi_join`, `anti_join`, `cross_join`, `nest_join`) |

## Types and missing values

R integer maps to Ibex `Int64`; double, logical, character, factor/categorical,
`Date`, and `POSIXct` use the existing ribex/Arrow bridge. The schema endpoint
reports nullability, categorical encoding, time zone, ordering, and time-index
metadata. Operator metadata propagation remains the authority after execution.

Ibex null is the native representation of R `NA`. IEEE `NaN` remains a present
floating-point value. Consequently, translated `is.na(x)` tests Ibex nulls and
does not include `NaN`; use `is.nan(x)` when that distinction matters.

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
only the right for `right_join`, only the left for `left_join`. A plan the
core cannot describe falls back to assuming every column nullable, which only
ever costs native execution.

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

Arbitrary R callbacks are deliberately absent from the MVP. An explicit,
vectorized main-thread R UDF barrier remains a post-MVP design; no R object or
closure is placed in `ExternRegistry` or invoked on an Ibex worker.
