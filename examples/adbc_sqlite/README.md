# SQLite → Ibex over ADBC

A self-contained walkthrough: build a small trades database, read it into
Ibex through the ADBC SQLite driver in deliberately small batches, filter,
compute notional, aggregate by symbol, and export the result.

## Run it

```sh
cmake -B build -G Ninja -DIBEX_BUILD_ADBC=ON   # builds the ADBC driver manager too
cmake --build build -j 6
scripts/install_adbc_driver.sh sqlite           # pinned, hash-checked Apache driver
examples/adbc_sqlite/run.sh build
```

`install_adbc_driver.sh` puts the driver in `~/.config/adbc/drivers` with a
manifest, so `read_adbc("sqlite", ...)` finds it by name; it needs `curl`,
`unzip` and `sha256sum`, not Python or conda. A conda env with
`libadbc-driver-sqlite` works too, as does
`ADBC_DRIVER_SQLITE=/path/to/libadbc_driver_sqlite.so`. On Windows, install
the driver with
`powershell -ExecutionPolicy Bypass -File scripts\install_adbc_driver.ps1 sqlite`
(the bypass covers that one process; Windows otherwise refuses an unsigned
script that came from a download).

`run.sh` creates the database in a temporary directory, runs `trades.ibex`,
and diffs the exported CSV against `expected_summary.csv`. It also runs as
the `adbc:sqlite_demo` ctest.

## What it shows

| File | Role |
| --- | --- |
| `make_trades_db.py` | 11 trades, standard-library `sqlite3`; trade 4 has a NULL price, trade 7 a NULL quantity |
| `trades.ibex` | The walkthrough; takes `--driver` (default `sqlite`), `--db`, `--out` via `parse_args` |
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
