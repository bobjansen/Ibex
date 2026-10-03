# Namespaces

Status: **proposed** (2026-10-03, branch `namespaces`). Nothing is built. This
plan says what a namespace is in Ibex, what it replaces, and the order to build
it in. It is written to be argued with: section "Open questions" lists what the
design does not settle.

## Decisions already made

These come from the author and are not reopened here.

1. The separator is `::`.
2. **Files declare their own namespaces**, as in C++. The name is written in
   the file; it is not derived from the file's name or from what `import`
   asked for. That allows two files to contribute to one namespace, and a file
   to declare something unrelated to its name. This is deliberate and is not
   magic.
3. There are no users, so namespaces are applied wherever they make sense,
   existing functions included. No aliases for the old names.
4. No overloading yet. One qualified name, one signature.

## Why

Plugin functions live in one flat namespace today. `import "adbc"` puts eleven
`adbc_*` names in it, and the prefix is typed by hand because nothing else
separates them. The cost has shown up three times:

- **Prefixes as a convention.** `adbc_`, `ws_`, `udp_`, `kafka_`, `gen_`,
  `read_`/`write_`. The convention is applied unevenly (`list_files`,
  `parse_args`) and nothing checks it.
- **One signature per name.** Extern declarations are keyed by name
  (`libs/adbc/adbc.ibex`: "a second overload would replace the first rather than
  add to it"). That forced `adbc_execute` and `adbc_write` to be connection-only
  because `adbc_read` already owns the one-off form's slot. Namespaces do not
  fix this (decision 4), but they remove the pressure to encode the form in the
  name.
- **Collisions.** Two plugins that both want `read` or `send` cannot coexist.
  `kafka_send`, `udp_send` and `ws_send` are the same word with a prefix.

## What a namespace is

A namespace is a named scope that holds declarations: `fn`, `extern fn` and
`extern type`. Nothing else. Bindings (`let`), columns and the language's own
built-ins are not in namespaces.

### Syntax

```
namespace adbc {
    extern type Connection from "adbc.hpp";
    extern fn connect(driver: String, uri: String, options: String = "")
        -> Connection from "adbc.hpp";
    extern fn query(mutable db: Connection, sql: String) -> DataFrame
        from "adbc.hpp";
}

let db = adbc::connect("sqlite", ":memory:");
adbc::query(db, "select 1 as x");
```

- `namespace name { ... }` is a statement that holds declarations. Blocks may
  nest, and `namespace a::b { ... }` is shorthand for `namespace a { namespace b
  { ... } }`.
- The same name may be opened again, in the same file or another. Declarations
  merge. Declaring the same qualified name twice is an error (decision 4: no
  overloads, no silent replacement).
- A file may hold several namespaces, or none. A library stub with no
  `namespace` block declares into the global scope, as every stub does today.
- `namespace` is a keyword (soft-reserved, like the built-in names in SPEC §2.3:
  a column named `namespace` keeps working where it is unambiguous, see "Lexing
  and parsing").

### Qualified names

`a::b` names `b` in namespace `a`; `a::b::c` names `c` in `a::b`. A leading
`::x` means global scope.

Resolution is the point where this differs from every other name. SPEC §6
resolves a bare identifier as **column scope, then lexical scope, then built-in
scope**. A qualified name skips the first two entirely: it is looked up only in
the namespace table. A column called `csv` or a `let adbc = 1` cannot shadow
`csv::read` or `adbc::query`. This is what makes the `::` form safe inside
`[filter ...]` and `[select ...]`, where a bare function name is already
ambiguous with a column.

Inside a `namespace` block, a bare name is looked up in the block's namespace
first, then the enclosing ones, then global. That lets a function in a
namespace call its siblings without repeating the prefix. It does not reach
into other namespaces: there is no argument-dependent lookup.

### Bringing names into scope

Typing `adbc::` every time is noisy for scripts that use one library heavily.
The design adds one declaration:

```
using adbc::query;        // one name
using adbc;               // every name in adbc (not its nested namespaces)
```

A `using` applies from its statement to the end of the file or script. It adds
the name at the lexical-scope level of resolution, so a column of the same name
still wins in a query clause and `^name` still reaches it. A name made visible
by two `using` declarations that disagree is an error at the point of use, not at
the declaration. There is no `using` inside a namespace block in the first
slice (see Open questions).

`import` and `using` stay separate on purpose. `import "adbc"` loads
`adbc.ibex` and its plugin library, and now brings no unqualified name in with
it. Seeing `adbc::query` needs `import`. Writing `query` needs `import` and
`using`.

## What does not move

- Language built-ins stay global and unqualified: the aggregates (`sum`,
  `mean`), `scalar`, the type constructors (`Int64(...)`, `Date(...)`), `rep`,
  `seq`, `rank`, `model`, `Table`, string and math functions. The entry in
  SPEC §2.3 ("Built-ins are intentionally minimal; prefer `extern fn` hooks")
  already draws this line: the core language has built-ins, plugins do not.
- Query clauses (`filter`, `select`, `by`, `window`, ...) are keywords and are
  not touched.

## Renames

Decision 3 applies namespaces to existing functions. The proposed mapping,
which is the part of this plan most worth arguing with:

| Library | Today | After |
|---|---|---|
| csv | `read_csv`, `write_csv` | `csv::read`, `csv::write` |
| json | `read_json`, `write_json` | `json::read`, `json::write` |
| parquet | `read_parquet`, `write_parquet` | `parquet::read`, `parquet::write` |
| fs | `list_files` | `fs::list` |
| args | `parse_args` | `args::parse` |
| adbc | `adbc_read`, `adbc_connect`, `adbc_query`, `adbc_execute`, `adbc_write`, `adbc_tables`, `adbc_table_schema`, `adbc_begin`, `adbc_commit`, `adbc_rollback`, `adbc_close` | `adbc::read`, `adbc::connect`, `adbc::query`, `adbc::execute`, `adbc::write`, `adbc::tables`, `adbc::table_schema`, `adbc::begin`, `adbc::commit`, `adbc::rollback`, `adbc::close` |
| websocket | `ws_listen`, `ws_connect`, `ws_recv`, `ws_send` | `ws::listen`, `ws::connect`, `ws::recv`, `ws::send` |
| udp | `udp_recv`, `udp_send` | `udp::recv`, `udp::send` |
| kafka | `kafka_recv`, `kafka_recv_avro`, `kafka_send` | `kafka::recv`, `kafka::recv_avro`, `kafka::send` |
| data_gen | `gen_ticks`, `gen_walk`, `gen_normal`, `gen_uniform`, `gen_ids`, `gen_reference` | `gen::ticks`, `gen::walk`, `gen::normal`, `gen::uniform`, `gen::ids`, `gen::reference` |
| types | `AdbcConnection` | `adbc::Connection` |

Two things in the table are judgment calls:

- `csv::read("x.csv")` against `read_csv("x.csv")`: the namespace form is
  shorter once there is a `using csv;`, but in a script that uses two formats
  `csv::read` and `parquet::read` read better than `read_csv` and
  `read_parquet`. The choice is made for consistency.
- `gen_reference` is registered by the plugin but has no `extern fn` in
  `data_gen.ibex`; the rename pass adds the declaration (it is used by
  `src/ui/server.cpp`).
- `kmeans`, `pca` and `lightgbm` have no extern declarations in their stubs
  today (their functions are reached through `model`), so they stay as they
  are.

The size of the rename is known: `read_csv` appears on 419 lines in 114 files,
`read_parquet` 377/86, `adbc_` 1043/59, `parse_args` 168/63, `write_csv`
187/65 (counted across the tree, including tests, examples, benchmarks, docs and
SPEC). Nearly all of it is mechanical, and the compiler core hardly names plugin
functions. Checked 2026-10-03 with a word-boundary grep over `src/`,
`include/` and `tools/ibex_compile.cpp`: the hits are comments, plus two real
dependencies that the rename pass must not miss: `tools/ibex_compile.cpp:356`
special-cases the extern name `"parse_args"` (it becomes `"args::parse"`), and
`src/ui/server.cpp:106` embeds Ibex source that calls `gen_ticks`,
`gen_reference` and `gen_walk`. The rename is a scripted pass over `.ibex`, `.md`, `.html`, `.py` and `.R` files and
the C++ tests that embed Ibex source, done once, in one commit, after the
feature works (see Phases).

## Implementation

The design leans on one fact: **the registry key is a string**, and a qualified
name can be that string. Plugins already register a flat name
(`registry->register_table("read_csv", ...)` in `libs/csv/csv.cpp`); after the
change they register `"csv::read"`. The registry, the plugin ABI, the effect
summaries and the `from "csv.hpp"` plugin loader all keep working unchanged.
The new work is in front of them (parsing and name resolution) and behind them
(the C++ emitter).

### Lexing and parsing

- **Lexer.** Add `TokenKind::ColonColon`, produced when two `:` are adjacent
  (`src/parser/lexer.cpp:373`). `::` has no meaning today: `:` is used in
  parameter and `let` types (`x: Int`), schema fields, `expect` and formula
  interactions (`a:b`, `src/parser/parser.cpp:1929`). Maximal munch turns `a::b`
  into one token where it used to be an error everywhere except possibly
  `a: :b`, which is not valid either. A formula `a::b` becomes a different
  error; there is nothing to preserve.
- **Parser.** A call's `callee` is a `std::string` (`CallExpr`,
  `include/ibex/parser/ast.hpp:330`). A qualified identifier is parsed into
  that same string, `"adbc::query"`, so a qualified name is a plain string
  through lowering, the effect pass, the REPL registries and the emitter. The
  parser is the only place that knows it is made of parts. `ExternDecl`,
  `FunctionDecl` and `ExternTypeDecl` get their names qualified at the point a
  `namespace` block is parsed, so they already carry the full string.
- **New statements.** `NamespaceDecl { name, body }` and `UsingDecl { target }`.
  The parser flattens namespace blocks: a namespace block does not survive past
  parsing, it prefixes the names inside it. This is why nothing downstream needs
  a scope tree.
- **Contextual keyword.** `namespace` and `using` are recognised only at
  statement start followed by an identifier, the same way `type` is contextual
  in `extern type` (`parse_extern_decl`). Existing scripts that use them as
  column or binding names keep working.
- **Type names.** `Connection` inside `namespace adbc` is `adbc::Connection`
  and is written that way outside (`mutable db: adbc::Connection`). Resource
  types are nominal (SPEC §11.5), and the qualified name is the identity.
  Parsing a type must accept a qualified identifier.

### Resolution

One function resolves a name at the places that look up callees (the lowering
pass, the REPL's function and extern registries, `effects.cpp`,
`scalar_bindings.hpp`, `ibex_compile.cpp`; all key by string today):

1. Qualified name: look it up in the declared set by its full string. No column
   or lexical scope. No match is an error naming the closest declared
   neighbour in the same namespace.
2. Bare name inside a namespace block: current namespace, enclosing ones,
   then the existing rule.
3. Bare name otherwise: the existing rule, with `using` additions at the
   lexical-scope level.

`using` is expanded at parse time into an alias table consulted in step 3; it
does not rewrite the AST. A dedicated function keeps the three cases in one
place so the REPL, the compiler and the effect pass cannot disagree.

### REPL and imports

`import "adbc"` already loads a stub and registers its declarations under the
names written in the stub. With qualified names it registers
`adbc::connect`, ... and nothing else. The REPL's `:functions` and `:imports`
listings group by `source_path` today; they gain a namespace column. Tab
completion offers `adbc::` after the prefix is typed.

The stub-to-library mapping (`plugin_stem("adbc.hpp")` -> `adbc.so`) is a
function of `source_path`, not of the namespace, so a file that declares a
namespace unrelated to its own name still loads the right library. That is
decision 2's "shenanigans" and it works without a rule.

### `ibex_compile`

The emitter turns an extern call into a C++ call. For ADBC it emits calls
into `libs/adbc/adbc.hpp` by C++ name (`ibex::adbc::detail::adbc_query`). The
mapping from a qualified Ibex name to a C++ one is a single function in the
emitter; the proposal is that `a::b` maps to the C++ function `b` in
`ibex::ext::a` (`adbc.hpp` already lives in `ibex::adbc`, and the header-per-
library structure matches). This is the one part of the plan that needs a
look at W6 before it is final (the open question below).

## Phases

Each phase lands on its own with tests, and leaves the tree green.

1. **Lexer and parser.** `::`, `namespace`, `using`, qualified calls and
   qualified types. Tests in the parser suite, including: column named
   `namespace`, `::` in a formula (error message), qualified name in a
   `filter` clause. No behaviour change for existing scripts.
2. **Resolution and registration.** One resolver, wired into lowering, the
   REPL registries, effects and `scalar_bindings.hpp`. Plugin stubs declare
   namespaces and register qualified keys. Done for one plugin first
   (`fs::list`, the smallest) to prove the path end to end, including the
   `import` plus `using` combinations, a column shadowing test and the
   duplicate-name error.
3. **`ibex_compile`.** The name mapping and the parity cases. W6's resource
   calls (`AdbcConnection` variables) are the case that exercises it.
4. **Rename pass.** The table above, scripted, in one commit: stubs, plugin
   registrations, tests, examples, benchmarks, SPEC, `docs/*.html`, README and
   AGENTS.md. Done after 1 to 3 so the feature is proven before the churn.
   SPEC gets a new section for namespaces and `using`, and §12.1's plugin table
   and §13.4 are rewritten. Per AGENTS.md: a `.ibex` example for the new
   syntax, and SPEC and `docs/index.html` kept in sync.
5. **Plugins.** Rebuild with `scripts/ibex-plugin-build.sh` (a registered name
   changed; no ABI change). Out-of-tree plugins break; there are no users.

Phases 1 to 3 can go in with the old flat names still working, because a stub
with no `namespace` block behaves as today. Phase 4 is the break.

## Verification

- **Unit:** lexer (`::` tokens, `a: :b`, formula), parser (nesting, merging,
  duplicate declarations, qualified types, contextual keywords), resolver (all
  three lookup rules; a column and a `let` named like a namespace).
- **Parity:** every `tests/parity/` case that calls a plugin function runs
  after the rename and must match. Parity cannot see lowering bugs
  (`project_parity_cannot_see_lowering_bugs`), so also assert on the lowered
  IR for one qualified call in a filter clause.
- **Mutation-check** each new test against the code it guards (resolver
  returning the column instead of the namespaced function must fail the
  shadowing test).
- **Rename pass:** gate on a grep that finds no remaining occurrence of any
  old name outside the changelog or a plan describing the rename, and run the
  full `ctest` without `-LE slow` (parser and lexer change, per AGENTS.md).

## Open questions

1. **`using` inside a namespace block**, and whether `using` is needed at all
   in the first slice. Scripts could just write the prefix. Proposal: ship
   `using` with the first slice, since without it `adbc::query` appears in
   every statement of every ADBC example and the docs will push to add it
   immediately.
2. **Aliases:** `using c = csv;` or `namespace c = csv;`. Not in the first
   slice. Short prefixes matter more than nested namespaces in a language
   without user libraries.
3. **User code:** can a script or an `.ibex` file the user writes declare a
   namespace and `fn`s in it? The design says yes (decision 2 is not specific to
   plugins) and a user `fn` in a namespace is just a qualified name. The
   interaction with `fn` resolution inside a namespace block is the part to
   test hardest.
4. **The C++ mapping for `ibex_compile`** (`a::b` -> `ibex::ext::a::b`) and
   whether a plugin header must use the same namespaces. Needs a read of the
   emitter's W6 resource path before it is settled.
5. **Where namespaces stop.** Built-ins stay global in this plan. If a built-in
   family wants a namespace later (string functions, `math::`), the rule is
   the same and nothing here prevents it. Not proposed now.
6. **Shadowing a namespace name** with a `let`: `let adbc = 1; adbc::query(...)`
   is legal under the rule above (qualified lookup ignores `let`). A lint would
   flag it; the language does not need to.

## Non-goals

- Overloading by arity or type (decision 4).
- Visibility or access control (`private`), ADL, inline namespaces, or a
  namespace tree that survives the parser.
- Renaming the language's own built-ins.
- Any change to the plugin ABI, the registry, or `ExternArgs`.
