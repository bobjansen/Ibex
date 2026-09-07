# `parse_args` extern + nullable scalar bindings

Two coupled changes:

1. **Nullable scalars** — a scalar binding / `scalar()` result may be null. This
   is what makes "optional CLI option with no default" expressible.
2. **`parse_args` extern** — declarative argument parsing. A spec string goes in;
   a **long table, one row per argument**, comes out. Everything downstream is
   ordinary `filter` / `select` / `map` over that table.

Part 1 is a prerequisite for Part 2 (an absent optional argument is an empty
filter result, and `scalar()` of that must yield null, not throw), and is worth
doing on its own — everything else in the language is nullable, scalars are the
one exception, and only because extraction throws.

---

## Part 1 — Nullable scalar bindings

### Status

**Slice 1 landed (uncommitted, 2026-09-07)** — interpreter/REPL path:

- `ScalarValue` gains a leading `std::monostate` null alternative in all three
  aliases; `runtime::is_null_scalar()` helper.
- `expr_from_scalar` / `scalar_from_expr` are now a total bijection with
  `ExprValue` (`scalar_from_expr` no longer returns `optional`).
- `extract_scalar`: null cell → null scalar (was an error); new
  `zero_rows_is_null` param for the one-argument form.
- REPL `scalar()` now accepts the **one-argument form** `scalar(<one-col table>)`
  → null on an empty result; two-arg `scalar(df, col)` keeps "exactly one row".
- Null propagates: `Int64(null)`/`Date(null)` → null (`apply_scalar_cast` +
  `numeric_cast_kernel` via `broadcast_cast_result`); `${null}` → `null`;
  `coalesce(null, d)` → `d`. Filter compares against a null lexical scalar → all
  rows dropped.
- Extern args stay null-free — `is_null_scalar` guard at every extern call site
  (`extern_call.cpp`, `expr.cpp`, `interpreter.cpp` stream args,
  `runtime_entry.cpp`).
- Tests: `test_repl.cpp` "scalar() of an empty one-column table is null", updated
  `test_interpreter.cpp` "extract_scalar yields a null scalar". Full suite green
  (1849 cases).

**Slice 2 landed (uncommitted, 2026-09-07):**

- Row-count expressions (`take` / `head` / `tail` / `rep` length / `Table(n)` /
  `Series(n)` / RNG shape — all route through `evaluate_row_count_expr_impl`)
  now give a clear "row count expression is null" error on a null scalar.
- `aggregate_series` (the `max(<series>)` REPL path) returns a null scalar for
  an all-null series instead of erroring — consistent with the column path.
- SPEC.md: §3, §5.7, §6.7 (new), §11.2, §12.2 edited.

**Not yet done:** codegen (scalar bindings still lower to bare `T`;
`emitter.cpp` emits a placeholder for the monostate arm — `collect_scalar_bindings`
never produces one so it's unreachable); per-call rolling `__window_n` /
`__window_ns` guard (separate path from row-count); a null scalar has no static
type (`infer_expr_type` falls through to String); the one-arg `scalar(<table>)`
form is REPL-only — not wired in `lower.cpp` for the compiled path.

### Semantics

A scalar binding carries a value of its scalar type **plus a validity bit**,
exactly as a column cell does. A null scalar propagates through arithmetic,
comparison, scalar functions, and `${}` interpolation per the existing §3 null
rules (`null + x = null`, `null > x = null`, `abs(null) = null`,
`` `px=${null}` `` = null). `is null` / `is not null` / `coalesce` / `fill_null`
consume it.

A null scalar is a **runtime error only at use sites that cannot represent
absence** — every position the spec currently describes as "a scalar expression
that evaluates to a (non-negative) integer":

- `take n`, `head n`, `tail n`
- rolling `__window_n` / `__window_ns`
- `rep(x, n)` count, and the future `seq` / `cycle` lengths
- Series / DataFrame constructor dimension arguments
- RNG shape/count arguments (§12.7)

It is **not** an error in a `filter` comparison: `filter x == scalar(...)` with a
null RHS yields a null predicate and the row is dropped (§3), which matches SQL's
scalar-subquery behaviour.

### SPEC.md edits

| Location | Change |
|---|---|
| §3, lines ~670–673 ("A null aggregate result…") | Replace "extracting it with `scalar()` is a runtime error — scalars cannot hold null." with "extracting it with `scalar()` yields a **null scalar** (§6.7, §12.2)." |
| §5.7 Scalar Subqueries | Add: "An uncorrelated subquery over an empty input, or whose aggregate is null (empty / all-null group), yields a null scalar; in `filter … == scalar(…)` the comparison is then null and the row is dropped (§3), matching SQL. For a correlated subquery, a captured key with no matching inner rows yields null for that key." |
| §6 — new **§6.7 Null Scalars** | Full definition: scalar binding = typed value + validity bit; propagation defers to §3; the runtime-error use-site list above; `let x: Int64 = scalar(t, c)` may bind null. |
| §11.2 Parameter Types / §11 return types | Add: "A scalar return type may be null; the extern signals this through the runtime nullable-scalar return convention. Callers receive a null scalar and the §6.7 rules apply." |
| §12.2 Scalar Extraction | Replace the final sentence with: "It is a runtime error if the DataFrame has any row count other than 1. If the single cell is null, the result is a **null scalar** of `col`'s type (§6.7). A **zero-row** input is a runtime error unless the extraction is the one-argument `scalar(<table>)` form, which yields null (§5.7)." |
| §~4286 host bridge | Python `None` / R `NA` ↔ null scalar binding, both directions. |

> Note the §12.2 tweak: the one-arg `scalar(<one-column table>)` form must return
> **null on zero rows** (not throw) so `scalar(args[filter name == "out", select
> { value }])` gives null for an absent optional argument. The two-arg
> `scalar(df, col)` form keeps the "row count must be exactly 1" rule.

### Implementation approach — **Option A** (decided)

`ScalarValue` gains a `Null` alternative in all three duplicated aliases
(`interpreter.hpp`, `parser/scalar_bindings.hpp`, `codegen/emitter.hpp`), which
stay identical. `expr_from_scalar` / `scalar_from_expr` become a total bijection
with `ExprValue`. `ScalarRegistry` and every `eval_scalar_expr` signature are
unchanged — null is just another alternative that flows through.

**Extern arguments stay null-free — enforced by the type system, not by
per-call-site checks.** An extern parameter is non-nullable unless its declared
type says otherwise; passing a possibly-null scalar to a non-nullable parameter
is rejected — statically where provable, otherwise a runtime assertion with a
message naming the parameter (`argument 'mu' of zscore is null`). So the extern
boundary keeps receiving null-free `ScalarValue`; the check just moves in front
of the call, joining the strict-use-site list. (Symmetric with §11's "an extern
*returns* null" — both are type-declared, not ad hoc.)

### Implementation notes (not SPEC)

- **Interpreter:** `ExprValue` already has a null arm. Remove the throw in the
  `scalar` builtin (zero-row → null for the 1-arg form; null cell → null for
  both); keep/add throws at the use-site list above (`take`, window sizes,
  `rep` count, ctor dims, RNG shape, non-nullable extern args).
- **Codegen:** scalar bindings lower to `ibex::Scalar<T>` (`{ T value; bool
  valid; }`) instead of a bare `T`. Scalar arithmetic / scalar-function helpers
  gain a null-propagating overload (mirror the column-cell path). Emit an
  `ibex_error("null scalar where a value is required")` guard at each use site.
- **Typed host captures:** wrap in the optional representation; `None`/`NA` →
  `{valid=false}`.
- Tests: `scalar()` of empty subquery → null; `coalesce(scalar(...), d)`;
  `take scalar(...)` with null → error; interpolation of null scalar → null.

---

## Part 2 — `parse_args`

### Declaration

```ibex
import "args";                                    // or explicit:
extern fn parse_args(spec: String) -> DataFrame from "args.hpp";
```

### Return: one row per argument, fixed schema

```
DataFrame<{
  kind:  String,   // "option" | "flag" | "positional"
  name:  String,   // canonical (long) option name; "" for a bare positional
  index: Int64,    // 0-based occurrence within (kind, name) — repeats, positionals
  value: String,   // the string value; a flag is "true" / "false"
}>
```

(This is the `argc / argv / value` shape from the original sketch: `index` is
`argc`, `name` is `argv`.)

The schema is **the same regardless of the spec** — no dynamic schema, no
compiler special-casing, no ascription. `value` is `String`; the script casts.

One row is emitted for:

- every option occurrence (`--threads 4` → one row; `-I a -I b` → two rows,
  `index` 0 and 1);
- every flag that resolves to a value (present, `--no-x`, or a default) →
  `kind = "flag"`, `value = "true" | "false"`;
- every positional, in argv order (`kind = "positional"`, `name` from the spec's
  positional declaration or `""`, `index` the ordinal);
- every option with a `= default` that was **not** passed → one row carrying the
  default (so a defaulted option is always present in the table);
- optional options (`?`) that were **not** passed → **no row** (⇒ `scalar(...)`
  → null, Part 1).

### Access patterns

```ibex
let spec = "
  threads (t)  : int    = 4      env IBEX_THREADS  # worker threads
  verbose (v)  : flag                              # extra logging
  out          : string?                           # output path; stdout if absent
  since        : date?                             # lower bound on trade date
  input        : positional+                       # one or more parquet files
";

let args = parse_args(spec);

let threads = Int64(scalar(args[filter name == "threads", select { value }]));  // 4 if unset
let verbose = Bool( scalar(args[filter name == "verbose", select { value }]));
let out     =       scalar(args[filter name == "out",     select { value }]);   // null if absent
let since   = Date( scalar(args[filter name == "since",   select { value }]));  // null propagates

let files = args[filter kind == "positional", select { path = value }];
let data  = files.flat_map(p => read_parquet(p));
```

A one-line helper removes the repetition (ship it in a prelude, or let scripts
write it):

```ibex
fn arg(a, nm) = scalar(a[filter name == nm, select { value }]);

let threads = Int64(arg(args, "threads"));
let out     = arg(args, "out");            // String or null
```

Repeated options and positionals stay as multi-row selections and feed `map` /
`flat_map` / joins directly — the whole point of the long shape.

### Spec mini-language

One option per line (newline- or `;`-separated). Blank lines and `#`-only lines
ignored. The spec drives **parsing** (aliases, which tokens take a value,
defaults, required-checks, `--help`, lexical type validation) — it does **not**
shape the output schema.

```
<name> [(<aliases>)] : <type><arity?><opt?> [= <default>] [env <VAR>] [# help]
```

| Field | Meaning |
|---|---|
| `<name>` | canonical long option, and the `name` value in the table. Matched against `--name` and, if it contains `_`, the `-` spelling too (`max_rows` ↔ `--max-rows`). |
| `(<aliases>)` | comma-separated; 1 char → `-x`, longer → `--word`. |
| `<type>` | `int` `int64` `float` `float64` `bool` `string` `date` `timestamp` — used for **lexical validation only** (`value` stays `String`). |
| `<arity>` | *(none)* single value `--name V`; `flag` boolean, `--no-name` ⇒ false; `+` repeated ≥1; `*` repeated ≥0; `positional`; `positional+` / `positional*` variadic trailing positionals. |
| `<opt?>` | trailing `?` — may be absent, and then **emits no row**. Without `?`, without a default, and not `flag` / `*` ⇒ **required** (missing ⇒ usage + exit 2). |
| `= <default>` | literal; on absence, emitted as a row carrying this string. |
| `env <VAR>` | option absent ⇒ read `$VAR`, then `= <default>`. Adds `reads_env` to the extern's effects. |
| `# help` | trailing help text for `--help`. |

### Behaviour

- `--help` / `-h`: print generated usage (synopsis from arities + `#` text), then
  **stop** — via a host-provided hook: `std::exit(0)` in the CLI, an exception in
  the Python / embedded host. The same hook serves the `exit 2` error paths.
- `--`: ends option parsing; the rest are positionals.
- Missing required / lexical type error (`--threads abc`, checked against
  `<type>`) / unknown option ⇒ usage to stderr, exit 2. A lone `...` line in the
  spec instead routes unknowns into rows with `kind = "unknown"`.
- Globs are **not** expanded — the shell does it, or the script filters the
  positional rows before the scan.
- Purity: pure given fixed `(argv, env)`; effects `{ reads_env }` when any option
  uses `env`, else `pure`. Evaluated once.

### `flat_map(read_*)` → multi-path scan

`<paths>.flat_map(read_parquet)` / `.flat_map(read_csv)` — lowering recognises the
bare `flat_map(read_*)` shape and rewrites it to a single multi-input `ScanNode`
(the node a first-class `read_parquet(paths_column)` would build), so
projection/predicate pushdown and parallel row-group decode still reach the scan.
`map` / `flat_map` stays the general escape hatch for per-file custom logic.
Requires `map` / `flat_map` to return a Table per element + vertical `concat`
(today `map` is barely used).

### Warts

1. **`value` is always `String`; every scalar read needs a cast.** The `arg(...)`
   helper + a top-of-script binding block keep it to one cast per option. This is
   inherent to the long shape (one `value` column, heterogeneous contents) and
   accepted.
2. **Cast errors would be ugly if they fired.** `Int64("abc")` → a generic cast
   error, not `invalid value for --threads: abc` + usage. Kept from firing by the
   plugin's own lexical validation against `<type>` during parse, which emits the
   good message and exits *before* returning — the in-script cast is
   belt-and-suspenders.
3. **`flag` round-trips through a string** (`"true"` / `"false"`); `Bool()` must
   accept exactly those, or scripts compare `== "true"`.
4. **`name`-based filtering is stringly-typed.** A typo (`name == "thrads"`)
   silently yields an empty selection → null (or a `scalar()` error at a
   non-optional site). No compile-time check — `parse_args` has a fixed schema
   but the compiler doesn't know the *set of names*. Acceptable; the `--help` /
   required-check paths catch most real mistakes at runtime.
5. **`--help` / errors terminate from inside an extern.** Needs the exit-or-throw
   host hook (wart applies to every host but the CLI).
6. **REPL has no meaningful argv.** `parse_args` in an interactive session
   returns only the defaulted rows (or errors "no argv in interactive session") —
   pick one, document it.
7. **Malformed spec** → runtime error at the `parse_args` call site (the plugin
   parses the spec at runtime; no compile-time spec check).
8. **Positional `name`.** Decide: `""` for all bare positionals (distinguish by
   `index`), or the spec's positional declaration name (`"input"`) for all of
   them. The examples above assume the latter.

### Host wiring

- **CLI / REPL:** capture `argv` at `main` — everything after the script path (or
  after `--`), ibex's own flags removed. Stash in `ExternRegistry` context, along
  with the exit-or-throw hook.
- **Python:** `ibex.run(script, args=[...])`, default `sys.argv[1:]`; hook raises.
- **Transpiled binary:** generated `main` forwards its own `argv`; hook exits.

### Extern-restriction check

One `String` arg, `DataFrame` return, no overload, fixed output schema — fully
within §11.4. No new language concept beyond Part 1.
