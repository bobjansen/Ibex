# Ibex + R · nycflights13

An integrated example of the [`ribex`](../../r/ribex) bindings, split the way a
real project would split it:

- [`flights.ibex`](flights.ibex) — every query, as named `let` bindings.
- [`flights.R`](flights.R) — binds the data, sources that file once, pulls each
  named result back as a `data.frame`, and plots it with ggplot2.

No Ibex source is embedded in the R script, so the queries stay readable, stay
diffable, and can be run on their own in the REPL.

The same six results are also written idiomatically in two other dialects, so
the query styles can be read side by side:

- [`flights_dplyr.R`](flights_dplyr.R) — pipes, `summarise(.by =)`, `inner_join`,
  and `slider::slide_index_dbl` for the trailing window.
- [`flights_datatable.R`](flights_datatable.R) — chained `[`, `.N`, `by =`,
  `X[Y, on =]` joins, and an adaptive `frollmean`.
- [`plots.R`](plots.R) — the ggplot layer, shared by all three, so each script
  is query code and nothing else.
- [`compare.R`](compare.R) — runs all three and checks every result agrees.

## Running it

```sh
# 1. an Ibex build must exist
cmake -B build-release -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release
cmake --build build-release

# 2. install the bindings against it
IBEX_ROOT=$PWD IBEX_BUILD_DIR=$PWD/build-release R CMD INSTALL r/ribex

# 3. the R-side dependencies
R -e 'install.packages(c("nycflights13", "ggplot2", "dplyr", "slider", "data.table"))'

# 4. run, from the repository root
Rscript examples/nycflights13/flights.R
Rscript examples/nycflights13/flights_dplyr.R
Rscript examples/nycflights13/flights_datatable.R
Rscript examples/nycflights13/compare.R
```

Plots land in `plots/`, `plots-dplyr/`, and `plots-datatable/` under the example
directory; set `IBEX_EXAMPLE_OUT` to write them somewhere else.

`ribex` finds the runtime through `IBEX_BUILD_DIR`, or by falling back to `build-release/tools` and `build/tools` under the working
directory — which is why the script is meant to be run from the repository
root.

## How the two halves meet

`session_eval_file()` binds the R tables and scalars for the duration of that
one call:

```r
session_eval_file(sess, "examples/nycflights13/flights.ibex",
                  tables  = list(flights = as.data.frame(flights), ...),
                  scalars = list(min_flights = 1000L))
```

Inside the file those names are ordinary values, and the `let` bindings built
from them outlive the call — so the 336,776-row flights table crosses the
boundary once, not once per query. Afterwards a result is a one-line fetch on
the same session, because naming a binding is a complete Ibex statement:

```r
carriers <- session_eval(sess, "carriers;")
```

`flights.ibex` publishes `carriers`, `hourly`, `daily`, `by_age`, `by_visib`,
and `origin_counts`.

The file is written as one `let` per published result. Clauses compose inside a
bracket and brackets chain, so an intermediate earns a name only where the
language forces the break — `aged`, because `update` and `select` are mutually
exclusive and the aggregate groups by the column the `update` derives.

## The same query in three dialects

Step 1, side by side. Ibex:

```
let carriers = (flights[filter arr_delay is not null,
                        select { flights = count(), avg_arr_delay = mean(arr_delay) },
                        by carrier]
                  [filter flights >= min_flights]
                join airlines on carrier)
               [order { avg_arr_delay desc }];
```

dplyr:

```r
carriers <- flights |>
    filter(!is.na(arr_delay)) |>
    summarise(flights = n(), avg_arr_delay = mean(arr_delay), .by = carrier) |>
    filter(flights >= min_flights) |>
    inner_join(airlines, by = "carrier") |>
    arrange(desc(avg_arr_delay))
```

data.table:

```r
carriers <- fl[!is.na(arr_delay), .(flights = .N, avg_arr_delay = mean(arr_delay)), by = carrier
             ][flights >= min_flights
             ][al, on = "carrier", nomatch = NULL
             ][order(-avg_arr_delay)]
```

Ibex sits closer to data.table than to dplyr: both apply clause blocks to a
table in brackets and chain them, and both keep the filter, the aggregate, and
the grouping in one block rather than as separate pipeline stages.

Two places worth dwelling on, for opposite reasons.

**The trailing window — Ibex is the one that is right.** `resample 1d` +
`window 7d` is time-indexed: the window is seven *days*, not seven rows.
dplyr expresses the same thing with `slider::slide_index_dbl(..., .before = 6)`,
which also indexes by date. data.table's `frollmean(x, 7)` is row-based and is
only equivalent because this daily series happens to be gapless; it also opens
with six `NA`s, so the script uses an adaptive `frollmean` with lengths
`1, 2, ... 7` to reproduce Ibex's partial leading window. Insert a day with no
flights and the row-based version silently starts averaging across a longer
span than it claims.

**Day bucketing — this one is an Ibex limitation.** All three scripts bucket in
UTC, and they agree, but UTC is not obviously the right answer for flights out
of New York: a departure at 23:30 local on 31 December lands in the following
year's first bucket. It is not a rounding detail: 37,037 of the 328,521 scored
departures — 11% — fall in a different calendar day under `America/New_York`
than under UTC, and the year has 365 local days against 366 UTC ones. The other
two dialects can bucket in any zone
(`as.Date(time_hour, tz = "America/New_York")`, `lubridate::floor_date`); Ibex
cannot. `Timestamp` is a bare instant — nanoseconds since the epoch with no
zone attached — so `resample 1d` always cuts on UTC midnight, and no argument
exists to say otherwise. Shifting the instants by a fixed offset first is not a
workaround, because the offset changes at the two DST transitions inside 2013.
See [Timestamps carry no time zone](#timestamps-carry-no-time-zone) below.

`compare.R` runs all three and asserts they agree column by column, so the claim
is checked rather than asserted.

## What each step demonstrates

| Step | Ibex features | Output |
| --- | --- | --- |
| — | `rename` to resolve the `planes.year` / `flights.year` collision | — |
| 1 | `filter` / `select` / `by` / `order`, a join to `airlines`, and an R scalar used inside the predicate | `carrier_delays.png` |
| 2 | multi-key group-by (`by { origin, hour }`) | `hourly_profile.png` |
| 3 | `as_timeframe` + `resample 1d` + `window 7d, rolling_mean` | `daily_rolling.png` |
| 4 | join to `planes`, a derived column via `update` | `aircraft_age.png` |
| 5 | two-key join to the hourly `weather` table | `visibility.png` |
| 6 | `format = "nanoarrow"` for the raw Arrow C Data handle | — |

## A note on the aircraft-age plot

The `flights >= 500` cutoff in step 4 leaves the surviving age cohorts
discontinuous. Fleets thin out after age 28 (ages 29-36 fly 24-231 flights
each), but one old cohort at age 37 still clears the bar with 518. A smooth
fitted to all of it would draw a trend through a nine-year hole, so the curve is
fitted only to the leading run of consecutive ages and weighted by flight count.
The isolated cohort is still plotted — it is a real number, just not something
to interpolate towards.

## Notes and gotchas this example works around

- **Reinstall `ribex` after rebuilding Ibex.** The package statically links the
  Ibex runtime, so a `ribex.so` older than `build-release/` runs old engine code
  and reports lowering errors for syntax the current REPL accepts. Rerun
  `R CMD INSTALL r/ribex` whenever the C++ build changes.
- **Run from the repository root, so renv picks the right `ribex`.** A second,
  older copy may sit in the system library; loading that one is the same trap as
  above wearing a different hat.
- **`update` and `select` cannot share one bracket.** Step 4 computes `age` in
  its own `let` and aggregates it in the next.
- **Boolean conjunction is `&&`, not `and`**, inside a `filter` predicate.
- **Timestamps are instants; the zone is metadata.** `Timestamp` is
  `{int64_t nanos}`. A column may carry an IANA zone beside its instants, and a
  `POSIXct` bound through the Arrow path comes back in the zone it arrived in,
  but nothing in the engine interprets that zone yet: calendar boundaries —
  `resample` above all — still cut on UTC. That a zone is a property of a
  *column* rather than of a row is deliberate (SPEC 2.4); per-row zones belong
  in a column of their own beside the instant. The zone also does not yet
  survive a query, so results that have been through an operator come back
  labelled UTC.
- **`count()` returns `Int64`.** Converting a result containing Int64 or
  Timestamp columns to a data.frame can emit a nanoarrow warning about possible
  loss of precision in the conversion to `double`. It is expected here; the
  counts and daily timestamps are far inside the range where doubles are exact.
