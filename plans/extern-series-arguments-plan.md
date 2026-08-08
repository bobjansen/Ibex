---
name: extern_series_arguments
description: "Make Series<T> a first-class extern argument, beginning with CSV null tokens"
metadata:
  node_type: memory
  type: project
---

# First-class `Series<T>` Arguments for Extern Functions

## Goal

Replace CSV's string-encoded null specification with a typed list and establish
the general ABI needed for plugins to accept Ibex series values.

```ibex
import "csv";

let cars = read_csv(
    "cars.csv",
    nulls = ["<empty>", "NA", "null"]
);
```

`<empty>` remains the spelling for an empty CSV field. Every other element is
matched as an exact, case-sensitive field value. A null entry in the list is an
error: a null token cannot describe a token to match.

There are no Ibex users to preserve, so this is an intentional API replacement:
the comma-separated `String` form is removed, not retained as an overload or
deprecated compatibility path.

## Why this is an ABI project

The current extern boundary is scalar-only:

```cpp
using ExternArgs = std::vector<ScalarValue>;
```

That is why the CSV plugin currently accepts `nulls: String` and parses an
embedded comma-separated mini-language. Accepting `Series<String>` correctly
requires more than parser sugar:

- lowering must preserve a series argument rather than reject an array literal;
- interpreted execution and the REPL must pass a materialized typed column;
- generated C++ must call a plugin function with the corresponding typed column;
- the plugin-facing registration ABI must expose that argument without erasure or
  conversion to a comma-delimited string.

CSV is the motivating user-facing change, but the resulting ABI is general for
any future `extern fn f(values: Series<T>, ...)` declaration.

## Decisions

### 1. The public Ibex type is `Series<T>`

The CSV import declaration becomes:

```ibex
extern fn read_csv(
    path: String,
    nulls: Series<String> = [],
    delimiter: String = ",",
    has_header: Bool = true,
    schema: String = ""
) -> DataFrame from "csv.hpp";
```

`Column<T>` remains an alias for `Series<T>` and is accepted wherever the type
checker accepts the alias today. The default empty list must carry the parameter
type; it must not rely on the general unannotated-empty-list `Int64` default.

### 2. Extern arguments are a tagged scalar-or-column value

Replace the scalar-only argument type with a public, move-aware value such as:

```cpp
using ExternArg = std::variant<ScalarValue, ColumnValue>;
using ExternArgs = std::vector<ExternArg>;
```

`ColumnValue` already carries the concrete column storage and validity bitmap,
so this preserves the physical and logical type of `Series<T>`. Do not expose a
`std::vector<std::string>` in the general registry ABI: it would make every
plugin reimplement type dispatch and discard nullable-series semantics.

Scalar-only externs retain their existing call shape; their implementations use
`std::get_if<ScalarValue>` (or a small checked helper) as they do today for
individual scalar alternatives.

### 3. Native codegen calls typed C++ overloads

For direct generated C++, `Series<String>` is emitted as the native Ibex column
type (`ibex::Column<std::string>` / the project's canonical string-column
alias), passed by `const&`. The CSV header exposes matching overloads:

```cpp
read_csv(std::string_view path, const ibex::Column<std::string>& nulls, ...)
```

This preserves a typed path for generated programs and avoids inventing a
runtime `ExternArgs` container for native calls. The implementation builds
`CsvReadOptions` directly from that column.

### 4. Series arguments are whole-value arguments

An extern `Series<T>` parameter receives one full column, not an element-wise
invocation. It is therefore valid only where a whole expression is evaluated
(table sources, scalar/top-level extern calls, and table consumers), not as a
row-local scalar function inside `select`, `filter`, or `update`.

This distinction must be diagnosed clearly; no implicit broadcast or per-row
plugin calls are added by this project.

### 5. Arrays are values at the extern boundary

`["NA", "null"]` lowers to a typed string column when bound to a
`Series<String>` extern parameter. A `let tokens = [...]` binding may be passed
as well. Dynamic series expressions of the right type are supported, including
selected/projected string columns where the calling context can materialize a
single column. Non-string series passed to `nulls` are a lowering/type error.

### 6. Enum members are contextual, strongly typed literals

Replace `round`'s ad-hoc bare-identifier handling with a nominal
`RoundingMode` enum whose members are `nearest`, `bankers`, `floor`, `ceil`,
and `trunc`. More generally, a bare enum member is admitted only when its
receiving position supplies one exact enum type:

```ibex
round(price, bankers)  // bankers is inferred as RoundingMode
let mode: RoundingMode = bankers;
```

This is bidirectional type checking, not a second identifier namespace. An
unconstrained `let mode = bankers` is an error, as is a member belonging to a
different enum. Outside an enum-expected position, normal column, lexical, and
built-in identifier resolution remains unchanged.

The language type is nominal and must be checked before lowering, but the
interpreted plugin ABI need not carry a runtime enum tag. Lower a validated
enum value to its fixed integral discriminant in `ScalarValue`; a registry
callback knows the declared parameter type and may cast that discriminant to
its C++ enum. This is deliberately the existing native-plugin trust boundary:
hand-written C++ can circumvent it and create an invalid enum, but Ibex source
cannot. Direct C++ codegen emits the corresponding named C++ enum member,
preserving compile-time checking on that path.

Enum declarations/mappings used by extern signatures must define a stable
member-to-discriminant mapping and qualified C++ enum spelling for codegen. Do
not add `Series<Enum>` merely for this work; scalar enum arguments cover
`round` and the initial extern use cases.

## Work plan

### Phase 0 — Map and lock the current boundary

1. Inventory every construction and consumption of `ExternArgs`, including:
   `runtime::invoke_extern_call`, REPL evaluation paths, stream source/sink
   setup, table consumers, lazy source registration, and plugin registration.
2. Identify which paths are explicitly scalar-only by design. Preserve that
   restriction temporarily where series values cannot have a meaningful whole
   value, but make error messages say so.
3. Add focused baseline tests for scalar extern calls, named/default argument
   binding, CSV's existing string specification, and generated `read_csv` calls.
   These establish what Phase 1 must not regress.

### Phase 1 — Generalize the interpreted extern ABI

1. Introduce `ExternArg` and update the registry function types and all callers.
2. Add checked helpers for plugin authors, e.g. `extern_arg_scalar<T>()` and
   `extern_arg_column<T>()`, so plugins do not hand-roll variant and column
   checks.
3. Refactor the interpreter's table-source, scalar-extern, and
   table-consumer invocation paths to evaluate parameters according to their
   declared `Type`:
   - scalar declarations produce `ScalarValue`;
   - `Series<T>` declarations materialize and pass `ColumnValue`;
   - wrong shape or element type fails before calling the plugin.
4. Apply the same binding logic in the REPL. Do not leave the REPL accepting a
   different set of extern arguments from `interpret()`.
5. Decide and implement ownership consistently: `ColumnValue` passed into a
   registry callback is moveable; plugins that only inspect it take `const&`.
   No caller may borrow storage from a temporary that dies during invocation.
6. Update stream paths explicitly. A stream source/sink must either support
   declared series arguments with the same whole-value semantics or reject them
   at lowering with a targeted message; it must not silently call
   `scalar_from_expr` on a column.

### Phase 2 — Lowering, type checking, enum resolution, and array materialization

1. Add nominal enum types and their member/discriminant/C++-spelling metadata to
   the type environment. Convert `round` to `RoundingMode`; remove its
   special-case bare-mode identifier parser/evaluator path.
2. Resolve bare enum members only against an expected parameter, annotation, or
   collection element type. Diagnose an unconstrained member, an unknown member,
   and a member from the wrong enum before IR lowering. Lower validated scalar
   members to their integral `ScalarValue` discriminant for interpreted extern
   calls, while retaining enough enum metadata for native codegen to emit the
   named C++ member.
3. Extend extern parameter validation so `Series<T>` is accepted as a parameter
   type and its default can be a typed empty array literal.
4. Make `bind_extern_call_args` retain parameter type information through
   lowering; it currently only binds expression positions.
5. Add a dedicated whole-series lowering path for extern arguments. It must
   handle:
   - non-empty homogeneous array literals;
   - `let`-bound array/series values;
   - a one-column table expression if the language's existing series extraction
     rules allow it;
   - an empty literal typed by the declared parameter type.
6. Represent a series argument in IR explicitly rather than encoding it as a
   string or a synthetic `Table`. Either add an `ir::SeriesLiteral`/series-value
   expression variant or an argument wrapper on `ExternCallNode`; choose the
   smallest representation that can also hold non-literal series expressions.
7. Update IR cloning, schema/source-key logic, printing/debugging, and all
   visitors for the new representation.
8. Reject a series argument in row-local expressions with a diagnostic naming
   the parameter and expected whole-value context.

### Phase 3 — Native code generation

1. Teach the emitter to materialize a typed `Series<T>` argument before the
   direct extern call. String array literals should emit a proper
   `ibex::Column<std::string>` with correct escaping and validity.
2. Emit `const ibex::Column<T>&`-compatible calls for series parameters while
   preserving current scalar call emission and named/default parameter ordering.
3. Support let-bound series arguments without emitting duplicate materialization
   or dangling references.
4. Make generated-code compilation a required test for:
   - a literal string list;
   - a let-bound list;
   - an empty default list;
   - a wrong element type (lowering fails before emission).
5. Compile and run `round` plus one enum-typed extern argument. Verify that
   codegen emits the declared C++ enum member rather than an untyped integer.
6. Verify interpreter/transpiler parity for values, null validity, errors, and
   default-argument behavior.

### Phase 4 — Move CSV to the typed API

1. Change `libs/csv/csv.ibex` to `nulls: Series<String> = []`.
2. Replace `csv_parse_null_spec(std::string_view)` at the public reader boundary
   with a constructor that consumes `Column<std::string>`:
   - `"<empty>"` sets `null_if_empty`;
   - other valid strings populate `null_tokens` unchanged;
   - null elements return a readable error;
   - duplicates are harmless;
   - commas inside a token are preserved as literal token content.
3. Update all `read_csv` C++ overloads and generated-call signatures to take the
   column argument. Keep the no-argument reader overload for defaults.
4. Remove the comma-splitting parser and all string-null-spec overloads. This is
   a clean break, including internal C++ test helpers where appropriate.
5. Update CSV's interpreted plugin registration to read a string `ColumnValue`
   from `ExternArgs`; its messages should say `Series<String> nulls list`, not
   `string null spec`.
6. Preserve chunked-reader eligibility when the null list is empty. For a
   non-empty token list, retain the existing eager fallback unless the chunked
   reader can consume the same typed options without semantic divergence.

### Phase 5 — Documentation, examples, and migration sweep

1. Update `SPEC.md` with contextual enum-member resolution, `RoundingMode`,
   and enum-typed extern parameters, as well as the extern declarations and
   CSV null examples.
2. Update `docs/index.html`, `docs/getting-started.html`, `docs/io.html`,
   `docs/reference.html`, and the README. This is a language/API semantic change,
   so the specification and public website must land in the same change.
3. Update bundled examples, notebooks, test `.ibex` programs, and comments in
   `csv.hpp` to use list syntax.
4. Remove every user-facing occurrence of `"<empty>,NA"` and explain the
   special `"<empty>"` list element once in the CSV documentation.
5. Add a concise migration note: `nulls = "<empty>,NA"` becomes
   `nulls = ["<empty>", "NA"]`.

## Test matrix

| Area | Required cases |
| --- | --- |
| Parser/lowering | `Series<String>` parameter and typed `[]` default; literal list; let-bound list; named argument; wrong element type; non-series scalar rejected; contextual enum member, typed binding, wrong/unknown member, and unconstrained-member errors |
| Interpreter/REPL | CSV values `""`, `NA`, `null`, ordinary strings, token containing a comma, duplicate tokens, and null token entry error |
| Plugin registry ABI | Scalar externs still work; a test extern receives a `Column<String>` with values and validity intact; validated enum arguments arrive as documented integral discriminants |
| Codegen | Literal and bound list compile and run; generated calls use the column overload; defaults work; enum arguments emit named C++ members |
| CSV behavior | Typed null bitmap is correct for Int64, Float64, Bool, and String inference; no null list preserves current empty-field behavior |
| Chunked/lazy path | Empty list preserves the optimized eligible path; non-empty list never bypasses null handling |
| Documentation | Every rendered example parses as shown, including multiline REPL form and no trailing commas in `Table` literals |

## Acceptance criteria

1. `read_csv("cars.csv", nulls = ["<empty>", "NA", "null"])` works in the
   REPL, interpreter, and transpiled executable with identical typed nulls.
2. `nulls = "<empty>,NA"` is rejected because the parameter is no longer a
   string.
3. A plugin test proves that an arbitrary `extern fn` can receive a
   `Series<String>` without CSV-specific lowering or ABI exceptions.
4. Series argument handling does not regress any scalar-only extern, table
   consumer, stream, or plugin test.
5. The full debug test suite, end-to-end plugin checks, and relevant release
   parity checks pass. Rebuild bundled plugins after the public runtime/header
   ABI change.
6. Because this touches externally-invoked hot paths, compare the affected
   release workload with `benchmarking/compare_ibex_git.sh` before declaring it
   complete; report the baseline and result even if performance is unchanged.

## Deliberately out of scope

- Arbitrary `List<T>` as a new language collection type. This project uses the
  existing `Series<T>` vocabulary and runtime storage.
- Element-wise external UDF invocation inside table clauses.
- Backward-compatible parsing of the old comma-separated `nulls` string.
- Extending every plugin API speculatively. The ABI is general, but CSV is the
  only required consumer in this change.
- `Series<Enum>` and arbitrary enum collection operations; enum support here is
  for scalar parameter values.
