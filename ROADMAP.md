# Ibex Roadmap

## FFI — Python and R bindings

Goal: allow Ibex query results to be consumed directly by Python and R so users
can plot with matplotlib/plotly/ggplot2 without implementing a plotting library
in Ibex itself.

### Design

`Column<T>` is contiguous (`std::vector<T>`), so numeric columns are a raw
pointer + length — exactly what numpy and R vectors expect. The approach is:

1. **Thin C API** (`ibex_c.h` / `ibex_c.cpp`) — `extern "C"` wrapper over
   `runtime::interpret` and `Table`. Avoids ABI issues and lets pybind11, Rcpp,
   Julia, Go, etc. all share the same entry point.

2. **Python (`ibex_py`, pybind11)** — expose `runtime::Table` via the numpy
   buffer protocol. Numeric columns (`f64`, `i64`) are zero-copy; strings and
   dates require a copy. A `to_pandas()` helper returns a dict of arrays.
   Optionally accept a pandas DataFrame as a registered input table.

3. **R (`ribex`, Rcpp)** — copy columns into `Rcpp::NumericVector` / `IntegerVector`
   / `CharacterVector` and return a `data.frame`. True zero-copy is possible via
   the Arrow C Data Interface (a stable ABI requiring no Arrow dependency on the
   Ibex side) but copy-on-result is the right starting point.

### Sketch of C API

```c
ibex_session_t* ibex_session_create(const char* plugin_path);
ibex_table_t*   ibex_session_eval(ibex_session_t*, const char* query);
int64_t         ibex_table_rows(ibex_table_t*);
int64_t         ibex_table_ncols(ibex_table_t*);
const char*     ibex_col_name(ibex_table_t*, int col);
int             ibex_col_type(ibex_table_t*, int col);  // f64, i64, str, date, ts
const double*   ibex_col_f64_data(ibex_table_t*, int col);
const int64_t*  ibex_col_i64_data(ibex_table_t*, int col);
void            ibex_table_free(ibex_table_t*);
void            ibex_session_free(ibex_session_t*);
```

### Intended usage

**Python**
```python
import ibex_py

session = ibex_py.Session(plugin_path="./build-release/libraries")
session.load("examples/quant.ibex")
df = session["sym_stats"].to_pandas()

import plotly.express as px
px.bar(df, x="symbol", y="avg_ret").show()
```

**R**
```r
library(ribex)
result <- ibex_eval(
  'daily[select { avg_ret = mean(ret), n = count() }, by symbol]',
  plugin_path = "./build-release/libraries"
)
ggplot(result, aes(x = symbol, y = avg_ret)) + geom_col()
```

### Work items

| Piece | Notes |
|-------|-------|
| C API (`ibex_c.h` + `ibex_c.cpp`) | Thin wrapper; first step |
| pybind11 module (`ibex_py`) | Buffer protocol for f64/i64; copy for strings/dates |
| Rcpp package (`ribex`) | Copy into R vectors; return `data.frame` |
| Pass DataFrames into Ibex | numpy → `Column<T>`; enables Ibex as a pandas accelerator |
| Arrow C Data Interface export | Zero-copy for R; removes the copy concern |
