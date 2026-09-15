# Decimal(precision, scale) — semantics and plan

Status: in progress on branch `decimal` (2026-09-14). This is the foundation the
ADBC work builds on: exact values must survive ingestion → computation → export.

## Scope

In: `Decimal(p, s)` with `1 <= p <= 38`, `0 <= s <= p`, stored as a signed
128-bit count of `10^-s` units. Out (deliberately): arbitrary precision,
Decimal256, negative scale, optimized 64-bit storage for small `p`.

## Representation

- **Column element** `ibex::Decimal { Int128 units; }` — 16 bytes, no scale.
  Precision and scale are *column-level*: they live in `ColumnMeta::decimal`,
  which already travels with a column through every copy/gather/slice
  (`with_meta_of`). A per-row scale would double the width and make every
  kernel re-check what the type already guarantees.
- **Scalar** `ibex::DecimalValue { Int128 units; DecimalType type; }` — a
  scalar has no column to hang metadata on, so it carries its own type. This is
  the alternative in `ScalarValue`, `ExprValue` and literals.
- `Int128` is `__int128` on GCC/Clang and `std::_Signed128` on MSVC. Every
  checked operation is written as a comparison against `±(10^38 - 1)` rather
  than a compiler overflow builtin, so it is portable and every stored value is
  always inside decimal128's range — no intermediate can overflow the int128.
- IR/parser type: `ColumnType::Decimal` / `ScalarType::Decimal` plus an
  optional `DecimalType` on the schema field, so ascriptions check `p`/`s`.

## Literals and text

- `decimal"12.30"` is an exact literal: scale = digits after the point (2),
  precision = significant integer digits + scale, minimum 1. Leading sign and
  an exponent (`1.5e3`) are accepted; `p > 38` is a parse error.
- Formatting always prints exactly `s` fractional digits (`12.30`, `-0.05`).
  That is the text CSV writes and the REPL prints, so text round-trips.

## Type rules (result types are static; values are checked at runtime)

| Expression | Result type |
|---|---|
| `a + b`, `a - b` | `s = max(s1, s2)`, `p = min(38, max(p1 - s1, p2 - s2) + s + 1)` |
| `a * b` | `s = s1 + s2`, `p = min(38, p1 + p2)`; error if `s1 + s2 > 38` |
| `a / b` | `Float64` — `/` has one result type in Ibex (SPEC 3.1); cast back with `Decimal(x, p, s)` |
| `a % b` | rejected |
| `-a` | same type |
| `Decimal ⊕ Int64` | the Int64 side is `Decimal(19, 0)`, exactly |
| `Decimal ⊕ Float64` column | rejected: silent float contamination is the bug this type exists to prevent |
| `Decimal ⊕ float literal` | the literal is read exactly from its shortest round-trip text (`10.5` is `10.5`) |
| comparisons | exact, across scales (scale-aligned), also against Int64 and float literals |

Overflow is checked: any result whose magnitude exceeds `10^p - 1` of its
result type is a runtime error (`decimal overflow`), never a wrap or a silent
null. Because `p` is capped at 38 the static rules above only bind at the cap.

## Rounding

One rule everywhere a value loses scale: **half away from zero** (`1.005 →
1.01`, `-1.005 → -1.01`), the rule of PostgreSQL, DuckDB and SQL Server
`CAST`. It applies to `Decimal(x, p, s)` casts, parsing text with more
fractional digits than the target scale (CSV), and float→decimal casts.

## Casts

- `Decimal(x, p, s)` from Int64 (exact), Decimal (rescale + round), String
  (exact parse + round; not-a-number is an error), Float64 (shortest
  round-trip text, then round). NaN/inf and values exceeding `p` are errors.
- `Float64(d)` — correctly rounded nearest double.
- `Int64(d)` — like Float→Int: succeeds only for whole values.

## Aggregates

| Aggregate | Result |
|---|---|
| `sum` | `Decimal(38, s)`, checked |
| `min`, `max`, `first`, `last` | input type |
| `mean` | `Float64`: the exact sum divided in decimal, with fractional digits beyond scale 38 when needed, then converted |
| `count`, `count_distinct` | `Int64` |
| `median`, `std`, `var`, `quantile`, … | rejected (cast to Float64 explicitly) |

Nulls follow the existing rules (ignored by aggregates, null in → null out).

## Relational operations

Filter, sort, group-by, distinct and joins operate on the units, which are
exact within a column. Join keys whose scales differ are **refused** with an
error naming both types and the cast that fixes it. Aligning them silently
would put a per-join rescale (and a possible overflow) out of sight, and
`Decimal(x, p, s)` on one side states the same thing explicitly. Sorting keys
that fit int64 flatten directly; wider ones use dense ordinal ranks, which are
exact.

Aggregates with a Decimal input run in `decimal_aggregate.cpp`: the chunked
hash aggregate's first chunk decides, and a Decimal input materializes and
takes that path. Non-Decimal aggregates in the same query are delegated back
to `aggregate_table` keyed on the same group ids, so they keep their usual
semantics exactly.

## I/O

- Arrow C Data: format `d:p,s` / `d:p,s,128` (import and export, zero-copy
  import). `d:p,s,32`/`,64` import by widening. `,256`, `p > 38`, negative
  scale: rejected with an explicit error.
- Parquet: read via Arrow's decimal128 (any physical encoding Arrow decodes);
  write as decimal128 with `p`/`s` preserved.
- CSV: schema hint `decimal(p,s)` parses exactly (no double on the path);
  write_csv formats exactly.

## Step checklist

1. [x] Core `decimal.hpp`: types, checked arithmetic, rounding, parse/format —
   `tests/test_decimal.cpp`.
2. [x] Storage: `Column<Decimal>` in `ColumnValue`, `DecimalValue` in scalars,
   `ColumnMeta::decimal`; every exhaustive visit handled.
3. [x] Language: `decimal"…"` literal, `Decimal(p, s)` type and
   `Decimal(x, p, s)` cast, lowering, IR schema, codegen (parity case
   `tests/parity/cases/decimal_money.ibex`).
4. [x] Runtime ops: compare/filter/sort/group/join/arith/aggregate —
   `tests/test_decimal_e2e.cpp`.
5. [x] I/O: Arrow C Data (zero-copy decimal128), Parquet (every physical
   encoding; cross-checked with pyarrow both ways), CSV `decimal(p,s)`.
6. [x] Validate: full suite + parity gate, SPEC + docs + `examples/decimal_money.ibex`;
   release benchmarks: `ibex_bench --suite decimal` and an A/B of the
   existing suites against HEAD.

## Measured cost (2026-09-14, release, 4M rows, `taskset -c 0-7`)

`ibex_bench --suite decimal`, the same values as Int64 cents / Float64 /
Decimal(18,2); min of 7 iterations after 3 warmups:

| Kernel | Int64 | Float64 | Decimal(18,2) | vs Int64 |
|---|---|---|---|---|
| sum | 0.25 ms | 0.54 ms | 9.9 ms (Decimal(38,2): 9.4) | ~40× |
| grouped sum, 1000 groups | 16.1 ms | 12.0 ms | 35.0 ms | 2.2× |
| filter `> 250` | 1.23 ms | 1.33 ms | 2.12 ms | 1.7× |
| `x * 3` | 1.11 ms | 1.11 ms | 35.2 ms | ~32× |
| `x + x` | 1.12 ms | 0.99 ms | 29.2 ms | ~26× |
| sort | 146 ms | 118 ms | 151 ms (Decimal(38,2): 737) | 1.04× |

Where the cost is: arithmetic and `sum` are serial, checked int128 loops (the
Int64/Float64 kernels are fused, vectorized and parallel); an ungrouped `sum`
also materializes its input for the decimal aggregate path. Before routing
Decimal `update` arithmetic to the vectorized kernel it went through the
per-row evaluator at ~150× Int64. Sorting keys that fit int64 flattens like
Int64; wider ones pay for dense ordinal ranks (5×).

## Follow-ups (not in this slice)

- Performance: parallel/morselized decimal arithmetic and sum; a streaming
  decimal sum in the chunked aggregate instead of materializing; a direct
  radix key for wide decimal sorts.

- `abs`, `round(d, mode)` and the rolling/cumulative kernels over Decimal.
- Scalar-context aggregates (`sum(x)` inside an `update` broadcast, REPL
  `aggregate_series`) have not been exercised over Decimal.
- ADBC: map `d:p,s` through the Arrow C Data path this slice established.
