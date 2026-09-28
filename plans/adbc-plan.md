# Finishing ADBC support

Status: **in progress** (2026-09-28: Phases 0, 1 and 2 done, Phase 4 slices
1-2 done; 2026-09-27: performance work is parked behind it,
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
| Read | `adbc_read(driver, uri, sql, options = "")` (`libs/adbc/adbc.cpp`, 411 lines), registered as both a materialized and a chunked (streaming) source. One connection per call. |
| Options | `db.`/`conn.`/`conn.post.`/`stmt.` prefixed `key=value` list with escaping, parsed in the ADBC-free `adbc_options.hpp` (tested without a driver). `entrypoint=` override. |
| Arrow import | Shared with Arrow C Data and Parquet (`src/interop/arrow_c_data.cpp`); zero-copy where the layout allows; `d:p,s` decimals exact. Empty results keep their schema. |
| Types refused | `binary` (bytea), `time64`, `month-day-nano interval`, `list` (`text[]`), each with a SQL-cast workaround in the PostgreSQL walkthrough. PostgreSQL `numeric` and `jsonb` arrive as text; the walkthrough converts `numeric` to `Decimal(p, s)` in Ibex. |
| Drivers | Bare names resolved through ADBC manifests. `scripts/install_adbc_driver.{sh,ps1}` install Apache's pinned, SHA-256-checked PyPI wheels for **sqlite** and **postgresql** on Linux x86-64/arm64, macOS x86-64/arm64 and Windows, with no Python needed. The driver manager is built from the pinned apache-arrow-adbc-24 tarball (or a system one). |
| Tests | `tests/test_adbc.cpp` (8 cases, SQLite: batches, nulls, empty schema, materialized = chunked, options, errors, manifest names) and `tests/test_adbc_options.cpp`; ctest `adbc:sqlite_demo` runs `examples/adbc_sqlite/`. |
| CI | `.github/workflows/adbc.yml`: Linux (g++) and Windows (MSVC) jobs, SQLite; the Windows job publishes the `ibex-windows-adbc` artifact. |
| Docs | `docs/io.html` covers SQLite only. SPEC.md does not describe `adbc_read`. `examples/adbc_postgresql/` is a walkthrough with a type matrix. |

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

Each phase lands on its own, with tests, and leaves the one-off `adbc_read`
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

### Phase 1 — type coverage on import: **done** (2026-09-28)

Per type, one answer, chosen once and documented (decisions 2026-09-28):

| Arrow type | Behaviour | Why |
|---|---|---|
| `float32` | widen to Float64 | Lossless. Shared Arrow C Data importer, so Parquet and Arrow input widen too. |
| uuid: `fixed_size_binary(16)` tagged `arrow.uuid`, or any binary the PostgreSQL driver tags `ADBC:postgresql:typname=uuid` | String, canonical lowercase 8-4-4-4-12 | Lossless, joinable, what users write in SQL. Untagged 16-byte columns are not guessed. |
| `time32` / `time64` | refuse | No time-of-day type; Int64 nanoseconds would print as a bare number whose meaning lives only in the docs. `t::text` or `extract(epoch from t)` says which one you want. |
| `binary` / `large_binary`, intervals, `list` / `large_list`, opaque driver types (`inet`, `timetz`, ...) | refuse | No Ibex column for them. |
| dictionary-encoded strings | Categorical | Already, via the shared importer (`[interop][arrow]` tests). |
| `timestamp` with zone | UTC nanoseconds, zone kept as column metadata | Unchanged. |

Every refusal names the column and the Arrow type (and the PostgreSQL type
when the driver tagged it), e.g. ``column `t`: Arrow time64[us] has no Ibex
column type``. The importer ends it with `kUnsupportedColumnAdvice`; the ADBC
plugin replaces that with ``cast it in the query, e.g. CAST(t AS TEXT)``
(standard SQL, so right for every driver). A PostgreSQL-tagged column gets
`t::text` instead, plus `extract(epoch ...)`, `encode(..., 'hex')` or
`array_to_string` where they fit.

`numeric`: the PostgreSQL driver (1.12) has no option to return it as an Arrow
decimal (its options are `batch_size_hint_bytes`, `use_copy`,
`disable_decimal_fast_path`, `transaction_status`), and the wire type carries
no precision, so it stays text and the walkthrough converts it with
`Decimal(x, p, s)`.

### Phase 2 — write and execute: **done** (2026-09-28, connection form only)

Extern declarations are keyed by name, so one name cannot take both a
`(driver, uri, ...)` and a `(db, ...)` form. Decided: connection form only. A
one-off write is `adbc_connect`, `adbc_write`, `adbc_close`; `adbc_read`
stays as the one-off streaming read.

As built:

- `adbc_execute(db, sql)` → Int: `AdbcStatementExecuteQuery` with no output
  stream; the affected rows, or −1 when the driver reports none. For DDL the
  count is the driver's: PostgreSQL −1, SQLite repeats the previous
  statement's `sqlite3_changes()`.
- `adbc_write(db, df, table, mode = "create")` → Int (rows written: the
  driver's count, else the table's rows). Ingest options `target_table` and
  `mode` (create / append / replace / create_append), then
  `AdbcStatementBind` with one struct array from `export_table_to_arrow`
  (zero-copy; the statement is released before the export).
- Both run on a `LeasedStatement` (statement + the session's one-statement
  lease) in `libs/adbc/adbc.cpp`.
- Resource functions can now take DataFrame/TimeFrame arguments:
  `ExternArgs::push_table` / `table(i)`, filled by `ResourceCalls::call` with
  `eval_table_expr`. Registry ABI change: rebuild plugins. A connection call
  nested under a query clause inside another connection call's argument
  (`adbc_write(db, adbc_query(db, ...)[filter ...], ...)`) is still refused by
  the placement check; bind it with `let` first. Allowing it means hoisting
  inside resource-call arguments.

Measured per type (tests `[adbc][write]`, PostgreSQL gated on
`IBEX_TEST_POSTGRES_URI`):

| Ibex | PostgreSQL column, read back | SQLite column, read back |
|---|---|---|
| Int64 | bigint, Int64 | integer, Int64 |
| Float64 | double precision, Float64 | real, Float64 |
| Bool | boolean, Bool | integer 0/1, Int64 |
| String / Categorical | text, String | text, String |
| Date | date, Date | ISO text, String |
| Timestamp | timestamp, Timestamp truncated to microseconds | ISO text with nanoseconds, String |
| Decimal(p, s) | numeric (unconstrained), text on read (Phase 1) | refused by the driver ("unsupported type decimal128") |

Nulls survive in every column on both. An empty table creates its columns. A
write that fails part way (duplicate key) leaves no rows on either driver and
the connection usable.

Open: the round trip is not byte-identical for Decimal (numeric reads back as
text, and the declared precision/scale are not kept) or for sub-microsecond
timestamps on PostgreSQL. Writing Decimal to SQLite needs a conversion the
user chooses.

### Phase 3 — parameters (connection form)

`adbc_query` and `adbc_execute` take an optional parameter table: one row binds
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

**Slice 1 done** on `adbc` (2026-09-27): `extern type`, `adbc_connect` /
`adbc_query` / `adbc_close` at the top level of a script, with fake-resource and
SQLite tests; details in `opaque-resource-lifetime-plan.md` ("Slice 1 as built").
Still to do: resources in user functions, the Docker PostgreSQL acceptance
checks, and the connection forms below.

Build `opaque-resource-lifetime-plan.md`: a typed opaque `AdbcConnection`,
`adbc_connect` / `adbc_query` / `adbc_close`, deterministic scope and
ownership rules, rejection of resource use inside query expressions, and one
active statement per connection. It is the largest phase, and the only one that
changes the language: a new nominal resource type, a resource path through
extern dispatch (a registry ABI change, so plugins need rebuilding), and effect
summaries in the planner. Its own acceptance list stands.

Then add parameters (Phase 3) and the transaction options
(`adbc.connection.autocommit` off, commit, rollback), which only make sense on
a reused connection.

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
  rejects them with a clear message. Check what it does with `adbc_read` today.

Done when: the four drivers install by name on the three platforms, and SPEC,
docs and the walkthroughs cover every function.

## Order and dependencies

Phase 0 → 1 → 2 → 3 run in that order: each small, and 2 and 3 share the Arrow
export and the prepared-statement path. Phase 4 is independent of 1–3 and can
start in parallel once Phase 0 lands its design doc, but it is the long pole.
Phase 5's one-off form needs nothing; its connection form needs Phase 4. Phase 6
closes the arc; its driver and CI items can go in as early as useful.

## Decisions needed

1. ~~**`time` columns**~~ decided 2026-09-28: refuse, with an error naming the
   cast. uuid: canonical string.
2. **Phase 4 in "finished"?** Everything else is library work; Phase 4 is a
   language feature. Without it ADBC is complete for scripting (one connection
   per call) but not for session-style use.
3. **Parameter form:** table only, or also a scalar list?
4. ~~**Function naming**~~ decided 2026-09-28: every function is `adbc_*`
   (`adbc_read`, `adbc_write`, `adbc_execute`, `adbc_connect`, `adbc_query`,
   ...); `read_adbc` was renamed with no alias. Namespaces are the next
   question, to be considered separately.

## Testing

- **SQLite** in every build with ADBC on: no service needed; tests seed through
  `adbc_execute`.
- **PostgreSQL** in CI as a service (Phase 0), and locally through Docker.
  Always ask before starting a container.
- **Round trips** per type as the core check for writing and parameters.
- **Instrumented fake resource** for Phase 4's exact acquire/release counts.
- Mutation-check each test against the code it guards (memory:
  `feedback_verify_the_test_fails_first`). Never let a failed configure run
  stale test binaries; gate ctest on configure and build success (memory:
  `project_adbc_reliability`).
