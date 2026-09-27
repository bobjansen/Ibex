# Finishing ADBC support

Status: **proposed, next up** (2026-09-27; performance work is parked behind it,
see `beat-duckdb-plan.md`). This plan says what "finished" means for the ADBC
plugin, what exists today, and the order to build the rest in. The design of
reusable connections already exists as a separate plan
(`opaque-resource-lifetime-plan.md`) and is referenced, not repeated. The work
happens on the `adbc` branch.

## What "finished" means

A user can do the everyday database work of a data-frame tool through ADBC,
against the common drivers, without dropping to Python:

1. **Read** any query result whose column types Ibex can represent, with a
   clear error or a documented conversion for the rest.
2. **Write** a data frame into a table (create, append, replace).
3. **Run statements** that return no rows (DDL, `insert`/`update`/`delete`),
   and get the affected-row count.
4. **Pass parameters** instead of splicing values into SQL text.
5. **Reuse a connection** across statements: session state, temporary tables,
   and one login instead of one per call.
6. **Discover** what is there: list tables, read a table's schema.
7. The above works and is **tested against SQLite and PostgreSQL**, with
   **DuckDB** and **Flight SQL** installable the same way, on Linux, macOS and
   Windows, and is documented in SPEC.md and `docs/io.html`.

Out of scope for "finished": SQL pushdown of Ibex filters/projections into the
remote query, connection pooling, bulk ingestion tuning, and Decimal256.

## Where it stands (main, 2026-09-27)

| Area | State |
|---|---|
| Read | `read_adbc(driver, uri, sql, options = "")` (`libs/adbc/adbc.cpp`, 411 lines), registered as both a materialized and a chunked (streaming) source. One connection per call. |
| Options | `db.`/`conn.`/`conn.post.`/`stmt.` prefixed `key=value` list with escaping, parsed in the ADBC-free `adbc_options.hpp` (tested without a driver). `entrypoint=` override. |
| Arrow import | Shared with Arrow C Data and Parquet (`src/interop/arrow_c_data.cpp`); zero-copy where the layout allows; `d:p,s` decimals exact. Empty results keep their schema. |
| Types refused | `float32`, `binary` (bytea, uuid), `time64`, `month-day-nano interval`, `list` (`text[]`), each with a SQL-cast workaround in the PostgreSQL walkthrough. PostgreSQL `numeric` and `jsonb` arrive as text; the walkthrough converts `numeric` to `Decimal(p, s)` in Ibex. |
| Drivers | Bare names resolved through ADBC manifests. `scripts/install_adbc_driver.{sh,ps1}` install Apache's pinned, SHA-256-checked PyPI wheels for **sqlite** and **postgresql** on Linux x86-64/arm64, macOS x86-64/arm64 and Windows, with no Python needed. The driver manager is built from the pinned apache-arrow-adbc-24 tarball (or a system one). |
| Tests | `tests/test_adbc.cpp` (8 cases, SQLite: batches, nulls, empty schema, materialized = chunked, options, errors, manifest names) and `tests/test_adbc_options.cpp`; ctest `adbc:sqlite_demo` runs `examples/adbc_sqlite/`. |
| CI | `.github/workflows/adbc.yml`: Linux (g++) and Windows (MSVC) jobs, SQLite; the Windows job publishes the `ibex-windows-adbc` artifact. |
| Docs | `docs/io.html` covers SQLite only. SPEC.md does not describe `read_adbc`. `examples/adbc_postgresql/` is a walkthrough with a type matrix. |

**Stranded work, now on the `adbc` branch.** Two commits existed only on the
local `adbc-reliability` branch, not on `origin` or `main`; they are
cherry-picked onto `adbc` as `2acc5a22` and `b8ab0f5f`:

- `01189d96` "ADBC postgresql work": a PostgreSQL service in `adbc.yml`, the
  driver installed in CI, and `examples/adbc_postgresql/check.py`, which checks
  the walkthrough's results (grouped totals, timestamp precision, 38-digit
  values, nulls, empty schema, Decimal overflow rejection). It also switches
  the walkthrough's aggregate from a `::float8` cast to `Decimal(price, 12, 2)`.
- `06d2875e` "Plan updates": `plans/opaque-resource-lifetime-plan.md`, the
  reusable-connection design this plan's Phase 4 builds.

## Phases

Each phase lands on its own, with tests, and leaves the one-off `read_adbc`
unchanged.

### Phase 0 — land what exists

- ~~Cherry-pick `01189d96` and `06d2875e`~~ done on `adbc` (`2acc5a22`,
  `b8ab0f5f`). Still to do: push `adbc` and confirm the PostgreSQL CI job
  passes on GitHub (it has never run).
- ~~Clearer "driver not found" errors~~ done (see the commit after
  `197bb707`). Offered in September and never done:
  name the driver, the manifest search path it tried, and the install command.
  ADBC 24 on Windows reads a nonexistent `C:/…` path as `driver:uri` and reports
  "Could not load `C`"; that case in particular.

Done when: PostgreSQL runs in CI on every push, and the three not-found cases
(bad name, bad path, Windows drive-letter path) have tests with the new message.

### Phase 1 — type coverage on import

Per type, one of three answers, chosen once and documented:

| Arrow type | Proposed | Why |
|---|---|---|
| `float32` | widen to Float64 | Lossless; the walkthrough calls it an Ibex gap, not a driver choice. |
| `binary` / `large_binary` | refuse, suggest a cast | No Ibex binary column. Revisit only with a real use. |
| `fixed_size_binary(16)` from uuid | refuse, suggest `::text` | Same. A uuid extension-type check could map it to its canonical string later. |
| `time32` / `time64` | **decision needed**: refuse, or Int64 nanoseconds since midnight | Ibex has no time-of-day type. |
| intervals | refuse, suggest `::text` | No Ibex interval type. |
| `list` / `large_list` | refuse, suggest `array_to_string` | No nested columns. |
| dictionary-encoded strings | Categorical | Check this works as it does for Parquet. |
| `timestamp` with zone | already UTC nanoseconds | Document; Ibex timestamps carry no zone (memory: timestamps-no-TZ). |

Also check the PostgreSQL driver's option for returning `numeric` as Arrow
decimal instead of text. If it exists, the walkthrough and the docs recommend it.

Done when: every row above has a test with the chosen behaviour, and the
walkthrough's "refused" table matches.

### Phase 2 — write and execute (one-off form)

Mirror `read_adbc`'s shape: one connection per call, same `options` string.

- `write_adbc(df, driver, uri, table, mode = "create", options = "")` → Int
  (rows written). ADBC bulk ingestion: `AdbcStatementSetOption` with
  `ADBC_INGEST_OPTION_TARGET_TABLE` and `ADBC_INGEST_OPTION_MODE`
  (create / append / replace / create_append), then `AdbcStatementBindStream`
  with the table exported through the existing Arrow C Data export
  (`arrow_c_data.cpp` exports every Ibex column type, Decimal included). It is
  a table sink like `write_csv`, so it goes through the script driver's sink
  path (`ScriptSink`).
- `execute_adbc(driver, uri, sql, options = "")` → Int (affected rows, or −1
  when the driver does not report it). `AdbcStatementExecuteUpdate`. Today the
  tests seed SQLite by running DDL through `read_adbc`, which works only by
  accident.

Round-trip tests on SQLite and PostgreSQL: every Ibex column type, nulls, an
empty table, Decimal precision, the four modes, and a failure halfway through a
write.

Done when: a script can create, fill and query a table with no other tool, and
the round trip is byte-identical per type.

### Phase 3 — parameters (one-off form)

`read_adbc` and `execute_adbc` take an optional parameter table: one row binds
once, `n` rows execute `n` times (batch insert/update). `AdbcStatementPrepare`,
then `AdbcStatementBind` with the parameter table exported as Arrow. SQL
placeholder syntax is the driver's (`?` for SQLite, `$1` for PostgreSQL);
document that rather than rewriting it.

Signature question: a table argument, or a list of scalars. The table form
covers batches and reuses the export path, so start there; a scalar form can be
sugar over it.

Done when: a quoted string, a null, a Decimal and a timestamp all bind
correctly on both drivers, and a many-row batch runs as one prepared statement.

### Phase 4 — reusable connections

Build `opaque-resource-lifetime-plan.md`: a typed opaque `AdbcConnection`,
`adbc_connect` / `adbc_query` / `adbc_close`, deterministic scope and
ownership rules, rejection of resource use inside query expressions, and one
active statement per connection. It is the largest phase, and the only one that
changes the language: a new nominal resource type, a resource path through
extern dispatch (a registry ABI change, so plugins need rebuilding), and effect
summaries in the planner. Its own acceptance list stands.

Then give Phases 2 and 3 their connection forms (`adbc_write(db, df, table,
mode)`, `adbc_execute(db, sql[, params])`, parameters on `adbc_query`), and add
the transaction options (`adbc.connection.autocommit` off, commit, rollback),
which only make sense on a reused connection.

Done when: the plan's acceptance tests pass, including Docker PostgreSQL
checks that one connection keeps a temporary table across statements and two
connections stay isolated.

### Phase 5 — discovery

- `adbc_tables(...)` → a table of catalog / schema / table / type, from
  `AdbcConnectionGetObjects` (depth `tables`).
- `adbc_table_schema(..., table)` → one row per column: name, Arrow type, the
  Ibex type it would import as (or why it would not), nullability. From
  `AdbcConnectionGetTableSchema`.

One-off forms first (driver, uri), connection forms once Phase 4 lands.

Done when: both work on SQLite and PostgreSQL, and a refused type shows its
reason in the schema listing before any query is run.

### Phase 6 — drivers, platforms, docs

- Add **duckdb** and **flightsql** to `install_adbc_driver.{sh,ps1}` (pinned
  wheels, as for the other two), each with a smoke test. DuckDB's driver needs
  a non-default entrypoint (`duckdb_adbc_init`); check the manifest the script
  writes carries it. Snowflake and BigQuery follow the same mechanism but need
  accounts, so they stay documented-but-untested.
- **macOS in CI**: the install script supports it, but no job runs it.
- **SPEC.md**: a section for the ADBC functions, the `options` grammar, the type
  mapping table, and the connection rules once Phase 4 lands.
- **`docs/io.html`**: PostgreSQL next to SQLite, writing, parameters,
  connections. Use `import "adbc"` in examples (AGENTS.md).
- **`ibex_compile`**: generated C++ either supports the ADBC functions or
  rejects them with a clear message. Check what it does with `read_adbc` today.

Done when: the four drivers install by name on the three platforms, and SPEC,
docs and the walkthroughs cover every function.

## Order and dependencies

Phase 0 → 1 → 2 → 3 run in that order: each small, and 2 and 3 share the Arrow
export and the prepared-statement path. Phase 4 is independent of 1–3 and can
start in parallel once Phase 0 lands its design doc, but it is the long pole.
Phase 5's one-off form needs nothing; its connection form needs Phase 4. Phase 6
closes the arc; its driver and CI items can go in as early as useful.

## Decisions needed

1. **`time` columns:** refuse, or Int64 nanoseconds since midnight?
2. **Phase 4 in "finished"?** Everything else is library work; Phase 4 is a
   language feature. Without it ADBC is complete for scripting (one connection
   per call) but not for session-style use.
3. **Parameter form:** table only, or also a scalar list?
4. **Function naming:** `write_adbc`/`execute_adbc` mirror `read_adbc`; the
   opaque-resource plan uses `adbc_query`/`adbc_close` for the connection
   forms. Keep both families, or rename the one-off ones `adbc_read` etc. while
   there are few users?

## Testing

- **SQLite** in every build with ADBC on: no service needed; seed through
  `execute_adbc` once it exists.
- **PostgreSQL** in CI as a service (Phase 0), and locally through Docker.
  Always ask before starting a container.
- **Round trips** per type as the core check for writing and parameters.
- **Instrumented fake resource** for Phase 4's exact acquire/release counts.
- Mutation-check each test against the code it guards (memory:
  `feedback_verify_the_test_fails_first`). Never let a failed configure run
  stale test binaries; gate ctest on configure and build success (memory:
  `project_adbc_reliability`).
