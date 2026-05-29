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
  in `select` / `order` / `update` / `by` are checked at lowering time, including
  across `let` bindings (a let-bound table carries its schema into later
  statements). Edit `price` to `prize` in `schema.ibex` and re-run to see a
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

## Scope

Static checking applies wherever the operand's schema is statically known —
`Table { ... }` literals, `as`-ascribed expressions, typed readers, let-bound
tables built from any of these, and pipelines over them. A source with an
unknown schema (e.g. an untyped `read_csv`) or an open (`*`) schema falls back
to run-time validation.
