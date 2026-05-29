# Schema ascription & static column checking

A self-contained demo of typed schemas in Ibex — no external data or plugins
needed.

Run it:

```sh
build-release/tools/ibex_eval demo/schema/schema.ibex
```

## What it shows

- **`as { ... }` ascription** — gives a statically-unknown table (as produced by
  an I/O source) a known schema. `as { ... }` is shorthand for
  `as DataFrame<{ ... }>`.
- **Exact by default** — the ascribed schema must match the table exactly;
  a missing column, a wrong type, or an *unlisted extra* column is an error. A
  trailing `*` (e.g. `as { symbol: String, * }`) opts into allowing extras.
- **Compile-time column checking** — once the schema is known, column references
  in `select` / `order` / `update` later in the same pipeline are checked at
  lowering time. Edit `price` to `prize` in `schema.ibex` and re-run to see a
  lowering error (`update: column 'prize' not found in input`) instead of a
  run-time one.

## Typed readers

A reader can declare its output schema directly, so every call site is checked
without re-stating the columns:

```
extern fn read_trades(path: String)
    -> DataFrame<{ symbol: String, price: Float64, qty: Int64 }>
    from "csv.hpp";

read_trades("trades.csv")[select { symbol, prize }];  // lowering error: 'prize'
```

Add `*` to the declared schema (`-> DataFrame<{ symbol: String, * }>`) if the
reader may return more columns than it names.

## Current limitation

Static checking follows a schema **through a pipeline expression**, but a schema
does not yet flow across `let` boundaries: a let-bound table is re-read as an
opaque source in later statements, so references to it fall back to run-time
validation. Chain within one expression, or re-ascribe after a `let`.
