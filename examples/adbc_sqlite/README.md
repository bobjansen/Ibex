# SQLite → Ibex over ADBC

A self-contained walkthrough: build a small trades database, read it into
Ibex through the ADBC SQLite driver in deliberately small batches, filter,
compute notional, aggregate by symbol, and export the result.

## Run it

```sh
conda install -c conda-forge libadbc-driver-manager libadbc-driver-sqlite
conda activate <env>        # sets CONDA_PREFIX, where run.sh finds the driver

cmake -B build -G Ninja -DIBEX_BUILD_ADBC=ON
cmake --build build -j 6
examples/adbc_sqlite/run.sh build
```

`run.sh` creates the database in a temporary directory, runs `trades.ibex`,
and diffs the exported CSV against `expected_summary.csv`. It also runs as
the `adbc:sqlite_demo` ctest. If the driver is not in an active conda
environment, set `ADBC_DRIVER_SQLITE=/path/to/libadbc_driver_sqlite.so`.

## What it shows

| File | Role |
| --- | --- |
| `make_trades_db.py` | 11 trades, standard-library `sqlite3`; trade 4 has a NULL price, trade 7 a NULL quantity |
| `trades.ibex` | The walkthrough; takes `--driver`, `--db`, `--out` via `parse_args` |
| `expected_summary.csv` | The exported result `run.sh` checks against |

1. **Batched read.** `stmt.adbc.sqlite.query.batch_rows=4` makes the driver
   return three Arrow batches (4 + 4 + 3); Ibex assembles one table.
2. **Nulls.** SQL NULLs arrive as Ibex nulls. `count()` counts rows and
   `count(col)` non-null values:

   | symbol | rows | priced | sized |
   | --- | --- | --- | --- |
   | AAPL | 4 | 4 | 3 |
   | MSFT | 3 | 3 | 3 |
   | NVDA | 4 | 3 | 4 |

3. **Processing in Ibex.** The SQL is a plain `select`; the filter
   (`qty > 0 && price > 0`, which drops the two incomplete trades), the
   notional, the per-symbol aggregate, and the VWAP all run in Ibex:

   | symbol | trades | shares | notional | vwap |
   | --- | --- | --- | --- | --- |
   | AAPL | 3 | 200 | 38165 | 190.825 |
   | MSFT | 3 | 80 | 32840 | 410.5 |
   | NVDA | 3 | 70 | 8430 | 120.4286 |

4. **Export.** `write_csv(summary, out)`.
5. **Empty result.** A query matching no rows keeps its columns and types, so
   downstream steps still type-check and return an empty table.

## Scope

This is a functional demo, not a benchmark: Apache describes the SQLite
driver as a reference implementation with little optimization work. Every
`read_adbc` call opens its own connection; there are no reusable
connections, bound parameters, or writes back to the database yet.
