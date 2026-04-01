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
- `scalars = list(x = 1L, flag = TRUE, day = as.Date(...), ts = as.POSIXct(...))`
  binds R scalars into Ibex by copy.
- results return as a `data.frame` by default for immediate `ggplot2` use
- `format = "nanoarrow"` returns the lower-level Arrow-backed nanoarrow array

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
