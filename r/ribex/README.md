# ribex

Experimental pure-R bindings for Ibex.

Current shape:
- `eval_ibex()` evaluates an inline Ibex query.
- `eval_file()` evaluates a `.ibex` file.
- `create_session()`, `session_eval()`, and `session_eval_file()` keep table-valued `let`
  bindings alive across calls.
- `register_knitr_engines()` adds a `{ibex}` knitr engine for R Markdown.
- `knitr_session(name)` returns the named engine-backed session for mixed R / Ibex notebooks.
- `tables = list(name = data.frame(...))` binds R tables into Ibex by copy.
- `tables = list(name = nanoarrow_array)` binds supported Arrow-backed tables
  zero-copy through the Arrow C Data Interface. Other Arrow-ish R objects are
  normalized through `nanoarrow` when possible.
- `scalars = list(x = 1L, flag = TRUE, day = as.Date(...), ts = as.POSIXct(...))`
  binds R scalars into Ibex by copy.
- results return as a `data.frame` by default for immediate `ggplot2` use
- `format = "nanoarrow"` returns the lower-level Arrow-backed nanoarrow array

Nanoarrow inputs use `nanoarrow_pointer_export()` to create an independent,
thread-safe ownership lease. Ibex adopts that lease, not the caller's external
pointer, so the original R object remains valid and an Ibex session may retain
its buffers after the call returns. Results likewise own their buffers
independently of the session that produced them. Primitive Int64/Double, Bool,
Date/Timestamp, UTF-8, and dictionary-encoded UTF-8 categorical columns retain
their payload and validity buffers, including nullable slices; a mutation in
Ibex detaches the affected storage first.

Narrower integers (8/16/32-bit, signed or unsigned) are accepted and widened to
the Int64 Ibex stores, so an ordinary R `integer` column crosses the boundary.
Widening is a conversion, so those columns are copied rather than borrowed.
`uint64` is not accepted: it does not fit in an Int64.

An R `POSIXct` arrives as an Arrow timestamp in microseconds with an IANA zone
(`tsu:America/New_York`). Every Arrow timestamp resolution is accepted and
rescaled to the nanoseconds Ibex stores, and the zone is carried on the column
and re-emitted, so a `POSIXct` comes back in the zone it was handed over in
rather than relabelled UTC. The instant is exact in both directions: an Arrow
timestamp is UTC-relative whenever a zone is present. Only nanosecond input can
be adopted zero-copy — the other resolutions are rescaled into owned storage, so
a `POSIXct` column is copied rather than borrowed.

The zone is metadata in transit. No operation interprets it yet: calendar
boundaries, `resample` above all, still cut on UTC. It also does not yet survive
a query — an operator rebuilds its output columns and the zone is not among the
metadata they propagate, so a result that has been through `filter` or `select`
comes back labelled UTC.

Arrow C Data buffers are immutable while shared. The lease prevents R garbage
collection from releasing them and Ibex serializes cooperating query access,
but it cannot protect against native code that writes to the same allocation
outside either binding. Copy such writable foreign data to a `data.frame`
before binding it. Descriptor-only nanoarrow operations such as creating a
shallow slice do not mutate the shared buffers.

Install from the repo checkout with an existing `build-release`:

```sh
IBEX_ROOT=/path/to/ibex \
IBEX_BUILD_DIR=/path/to/ibex/build-release \
R CMD INSTALL r/ribex
```

Example:

```r
library(ribex)
library(ggplot2)

df <- eval_ibex('Table { x = [1, 2, 3], y = [10.0, 20.0, 30.0] };')
ggplot(df, aes(x, y)) + geom_line()
```

Input binding example:

```r
base <- data.frame(x = c(10L, 20L))

out <- eval_ibex(
  'base[update { off = offset, when = day, tag = label }];',
  tables = list(base = base),
  scalars = list(
    offset = 7L,
    day = as.Date("2024-02-03"),
    label = "demo"
  )
)
```

Session example:

```r
sess <- create_session()
session_eval(sess, '
  extern fn read_csv(path: String, nulls: String) -> DataFrame from "csv.hpp";
  let train = read_csv("data/iris.csv", "");
')

summary <- session_eval(sess, '
  train[select { avg_sepal = mean(Sepal_Length) }, by Species];
')

ggplot(summary, aes(Species, avg_sepal)) + geom_col()
```

R Markdown engine example:

```r
library(ribex)
ribex::register_knitr_engines()
```

````
```{ibex, session="demo", quiet=TRUE}
extern fn read_csv(path: String, nulls: String) -> DataFrame from "csv.hpp";
let iris = read_csv("data/iris.csv", "");
```

```{ibex, session="demo", assign="summary"}
iris[select { avg_sepal = mean(Sepal_Length) }, by Species, order Species];
```

```{r}
ggplot(summary, aes(Species, avg_sepal)) + geom_col()
```
````

For plugin-backed queries, make sure the plugin path is discoverable:
- set `IBEX_BUILD_DIR`
- or set `IBEX_LIBRARY_PATH`
- or pass `plugin_paths = c(".../build-release/tools")`
