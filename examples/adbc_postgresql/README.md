# PostgreSQL → Ibex over ADBC

Start PostgreSQL in Docker, load one table with a column per common type, and
read it into Ibex through Apache's ADBC PostgreSQL driver. The walkthrough
shows which types arrive, what they become in Ibex, and which ones are refused
(with the SQL cast that works around each).

Every step has a Linux (bash) and a Windows (PowerShell) form. Run the Linux
commands from the repository root. On Windows, run them from the unpacked
`ibex-windows-adbc` CI artifact, which holds `build\tools`, `scripts` and this
folder.

## 1. Start PostgreSQL

Needs Docker (Docker Desktop on Windows). The container listens on port
**55432**, so it does not clash with a PostgreSQL you may already run on 5432.

```bash
docker run -d --name ibex-pg -e POSTGRES_PASSWORD=ibex -p 55432:5432 postgres:17
until docker exec ibex-pg pg_isready -U postgres; do sleep 1; done
```

```powershell
docker run -d --name ibex-pg -e POSTGRES_PASSWORD=ibex -p 55432:5432 postgres:17
do { Start-Sleep 1; docker exec ibex-pg pg_isready -U postgres } until ($LASTEXITCODE -eq 0)
```

## 2. Load the table

```bash
docker exec -i ibex-pg psql -U postgres -v ON_ERROR_STOP=1 < examples/adbc_postgresql/seed.sql
```

```powershell
Get-Content examples\adbc_postgresql\seed.sql | docker exec -i ibex-pg psql -U postgres -v ON_ERROR_STOP=1
```

Expect `DROP TABLE`, `CREATE TABLE`, `INSERT 0 3`. (The first run also prints
a notice that `ibex_types` did not exist; that is fine.)

## 3. Install the driver

```bash
scripts/install_adbc_driver.sh postgresql
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install_adbc_driver.ps1 postgresql
```

The driver is Apache's own build, pinned and SHA-256 checked, with `libpq`
linked in; nothing else is installed. `read_adbc("postgresql", ...)` then finds
it by name.

## 4. Read the supported types

Linux needs a build configured with `-DIBEX_BUILD_ADBC=ON` (see the top-level
README); the Windows artifact already has one.

```bash
build/tools/ibex --plugin-path build/tools examples/adbc_postgresql/types.ibex
```

```powershell
.\build\tools\ibex.exe --plugin-path .\build\tools examples\adbc_postgresql\types.ibex
```

`types.ibex` prints four tables (strings are shown in double quotes, with
inner quotes escaped). Expected:

1. **Native types**, 3 rows. `small_n`, `int_n` and `big_n` are integers
   (`10000000000` and `-20000000000` for `big_n`), `dbl` is `1.5` / `-2.25`,
   `flag` is true / false, `label` and `code` are strings, `day` is a date.
   `ts` keeps its microseconds (`2026-01-02 03:04:05.123456`). `tstz` is shown
   in UTC, so row 2's `1999-12-31 23:59:59+01` reads `1999-12-31 22:59:59`.
   Row 3 is null in every column except `id` and `label`.
2. **Text-typed**: `price` (`numeric`) arrives as the strings `12.34`, `-0.50`
   and null; `doc` (`jsonb`) as the JSON text, shown as `"{\"k\": 1}"` and
   `"[1, 2]"`.
3. **Aggregate** by label, with `price` cast to `float8` in SQL:
   `alpha` has 2 rows, 1 priced, total `12.34`; `beta` has 1 row, 1 priced,
   total `-0.5`.
4. **Empty result**: columns `id`, `big_n` and `tstz`, zero rows.

## 5. Try the types Ibex refuses

Each query reads a single column. The first form of each is expected to fail
with an error mentioning `unsupported Arrow column format`; the second, with
the cast, is expected to work.

| Column | PostgreSQL type | Arrives as | Workaround |
| --- | --- | --- | --- |
| `real_n` | `real` | float32 | `real_n::float8` |
| `bytes` | `bytea` | binary | `encode(bytes, 'hex')` |
| `uid` | `uuid` | binary | `uid::text` |
| `t` | `time` | time64 | `t::text` |
| `iv` | `interval` | month-day-nano interval | `iv::text` |
| `tags` | `text[]` | list | `array_to_string(tags, ',')` |

```bash
for sql in "select real_n from ibex_types" "select real_n::float8 as real_n from ibex_types" \
           "select bytes from ibex_types"  "select encode(bytes, 'hex') as bytes from ibex_types" \
           "select uid from ibex_types"    "select uid::text as uid from ibex_types" \
           "select t from ibex_types"      "select t::text as t from ibex_types" \
           "select iv from ibex_types"     "select iv::text as iv from ibex_types" \
           "select tags from ibex_types"   "select array_to_string(tags, ',') as tags from ibex_types"; do
    echo "== $sql"
    build/tools/ibex --plugin-path build/tools examples/adbc_postgresql/query.ibex -- --sql "$sql"
done
```

```powershell
$queries = "select real_n from ibex_types", "select real_n::float8 as real_n from ibex_types",
           "select bytes from ibex_types",  "select encode(bytes, 'hex') as bytes from ibex_types",
           "select uid from ibex_types",    "select uid::text as uid from ibex_types",
           "select t from ibex_types",      "select t::text as t from ibex_types",
           "select iv from ibex_types",     "select iv::text as iv from ibex_types",
           "select tags from ibex_types",   "select array_to_string(tags, ',') as tags from ibex_types"
foreach ($sql in $queries) {
    "== $sql"
    .\build\tools\ibex.exe --plugin-path .\build\tools examples\adbc_postgresql\query.ibex -- --sql $sql
}
```

`real` is an Ibex gap rather than a driver choice: Ibex has no 32-bit float
column and does not yet widen one to `Float64` on import. The others are types
Ibex has no column for.

## 6. Clean up

```bash
docker rm -f ibex-pg
```

(Same command in PowerShell.) The driver stays installed; the install script's
header says where.

## Connecting to another server

Both scripts take `--uri` (a libpq connection URI, e.g.
`postgresql://user:password@host:5432/dbname`), so the same steps work against
any PostgreSQL you can reach; load `seed.sql` there with `psql` first.
