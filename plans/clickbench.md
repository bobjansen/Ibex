# ClickBench Parity Plan

Goal: reach feature parity so the ClickBench suite (43 queries over a single wide
`hits` table) can be implemented in Ibex.

Assessment cross-checked against `SPEC.md`, the aggregate/scalar registries in
`src/ir/expr_predicates.cpp`, and the three grouped-aggregation code paths
(`src/runtime/aggregate.cpp`, `src/runtime/chunked.cpp` — two impls).

---

## Hard blockers — no reasonable workaround

### 1. `count_distinct` aggregate
- **Queries:** Q5, Q6, Q9, Q10, Q11, Q12, Q14, Q23
- **State:** not in the aggregate list (`expr_predicates.cpp` ~L59-72).
  `repl.cpp:1574` has `count_distinct_in_column` but only for the `distinct`
  *display*, not as a `select` aggregate.
- **Partial workaround:** ungrouped and grouped-alone cases decompose to
  `t[distinct { k, UserID }][select { u = count() }, by k]`. Covers Q5, Q6, Q9,
  Q11, Q12, Q14.
- **No workaround:** Q10 and Q23 mix `COUNT(DISTINCT UserID)` with `SUM`/`AVG`/
  `MIN` in a single grouped select. Needs the real aggregate (or an awkward
  self-join on the group key).
- **Highest-leverage single item.**

### 2. String `min` / `max` aggregate
- **Queries:** Q22, Q23, Q29 (`MIN(URL)`, `MIN(Title)`, `MIN(Referer)`)
- **State:** hard-rejected on every path — `aggregate.cpp:210` "string
  aggregation not supported"; `chunked.cpp:6976` allows String only for
  `First`/`Last`.

### 3. `regexp_replace` (with `\1` capture groups)
- **Queries:** Q29
- **State:** no regex anywhere. `SPEC.md` §12.6 explicitly declares regex "out of
  scope" for `like`.
- **Work:** new dependency (regex engine) + `regexp_replace` (and probably
  `regexp_extract`). Largest single piece of work; only Q29 needs it.

### 4. Date / Timestamp `min` / `max` aggregate
- **Queries:** Q7 (`MIN(EventDate), MAX(EventDate)`)
- **State:** `aggregate.cpp:206` "date/time aggregation not supported";
  `chunked.cpp:6980` is Int/Double only.
- Same code site as item 2.

### 5. `date_trunc` / timestamp-truncation scalar
- **Queries:** Q43 (`DATE_TRUNC('minute', EventTime)` as a GROUP BY + ORDER BY key)
- **State:** `resample` exists but requires a `TimeFrame` operand and produces
  fixed buckets — it does not compose with a plain filtered/grouped/ordered
  DataFrame query.
- **Work:** add a row-wise `date_trunc(ts, minute)` (or `truncate`) scalar.
  Small.

---

## Soft gaps — verify or add a cast

### 6. `EventTime` / `EventDate` column typing from parquet
- **Queries:** Q19 (`minute()`), Q24-Q27, Q37-Q43 (date-range filters)
- If the ClickBench `hits.parquet` stores these as raw `int64` / `int32` rather
  than Arrow `timestamp` / `date`, then `minute(EventTime)` and
  `EventDate >= date"2013-07-01"` will not typecheck. `SPEC.md` §3.1.1 has no
  Int -> Timestamp cast (`Date(x)` accepts Timestamp/Date/Int64-as-days only).
- **Action:** confirm the file's Arrow schema; if raw ints, add
  `Timestamp(int)` / `Date(int)` casts.

### 7. UInt64 -> Int64 bit-cast
- **Queries:** Q20, Q32, Q33, Q41 (`WatchID`, `*Hash`, `ClientIP`)
- `libs/parquet/parquet.hpp:1722` bit-casts UInt64 identity. Equality filters,
  GROUP BY, and COUNT are bit-exact, so ClickBench works; ordering/printing of
  values >= 2^63 is wrong. Ibex has no unsigned types.
- **Latent correctness risk, not a blocker.**

### 8. Q30: 90x `SUM(ResolutionWidth + N)`
- `map` expansion needs a string-array or `columns()` source and compile-time
  arithmetic on the index var. Verify `map (i, _) in [...] => s_${i} =
  sum(ResolutionWidth + <i>)` can inject the integer index.
- Fallback: 90 hand-written terms (works, verbose).

### 9. `SELECT 1` constant column
- **Queries:** Q35
- Verify a bare literal RHS (`k = 1`) broadcasts to a column; else use `rep(1)`.

---

## Non-issues (idiom already exists)

| SQL | Ibex |
|---|---|
| `HAVING COUNT(*) > N` (Q28, Q29) | chained block `[select {...}, by k][filter c > N]` |
| `IN (-1, 6)` (Q41) | `x == -1 \|\| x == 6` |
| `CASE WHEN` (Q40) | `case { cond => v, else => "" }` |
| `SELECT *` + `ORDER BY` + `LIMIT` (Q24) | block with no `select` clause |
| `ORDER BY COUNT(*) DESC LIMIT N` | `select { k, c = count() }, order { c desc }, head N` |
| computed GROUP BY key `ClientIP - 1` (Q36) | `by { ClientIP, a = ClientIP - 1, ... }` |
| `STRLEN` / `OCTET_LENGTH` (Q28) | `byte_length` (present) |
| `length`, `substring`, `like`, `minute` | all present |

Wide 105-column schema with no ascription: column refs are runtime-checked
(acceptable), no static checking.

---

## Suggested build order

1. **String `min`/`max` + Date/Timestamp `min`/`max` aggregates** (items 2, 4) —
   same code, three paths (`aggregate.cpp`, `chunked.cpp` x2). Unblocks Q7, Q22.
2. **`count_distinct` aggregate** (item 1) — 7 queries directly; Q10/Q23 have no
   clean workaround.
3. **`date_trunc` scalar** (item 5) — unblocks Q43; small, row-wise.
4. **`regexp_replace`** (item 3) — new dependency; only Q29.
5. Confirm parquet `EventTime`/`EventDate` types; add `Timestamp(int)`/`Date(int)`
   casts if raw (item 6).

With items 1-3, ~40/43 queries are runnable. Item 4 adds Q29. The remainder are
already expressible with existing idioms.
