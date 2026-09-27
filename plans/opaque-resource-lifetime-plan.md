# Opaque resources: scope, lifetime, and ADBC connections

Status: slice 1 implemented on the `adbc` branch (2026-09-27): resources at
the top level of a script, end to end with ADBC. See "Slice 1 as built" below;
functions, returns from user functions, and compile support are still to do.

## Decision proposed

Add typed opaque resources with deterministic shared ownership. Users bind,
pass, return, and explicitly close them; they cannot inspect the handle or put
it into a column. ADBC is the first implementation.

For the first version, database operations execute at statement/function level,
not inside column expressions. ADBC results finish execution before escaping a
statement as a table. This keeps connection lifetime predictable without
requiring a redesign of all lazy sources or a new general-purpose scope syntax.

## What exists today

- `SPEC.md` §6 describes column -> lexical -> built-in name resolution. Query
  brackets introduce column scope, not a general resource-owning block.
- `SPEC.md`'s compile-time `map` generates ordinary select/update fields. It is
  not a runtime loop over handles; generated expressions can nevertheless be
  evaluated repeatedly by query execution.
- `src/repl/repl.cpp`'s user-function evaluator copies the caller's table,
  lazy-table, scalar, column, and model registries into local registries. Lazy
  tables are shared. The last expression is the function's return value.
  This is not an adequate definition of lexical resource capture: copying all
  caller resources would retain unrelated connections for the whole call.
- `src/parser/effects.cpp` already infers effects through user-function calls.
  Externs without annotations conservatively have all core effects. Existing
  effects include state, I/O, blocking, and failure; they do not encode resource
  ownership or enforce all the restrictions proposed below.
- `ExternArgs` currently holds only scalar values; `ExternValue` holds tables,
  scalars, and stream timeouts. Fitted models have an opaque `shared_ptr<void>`
  payload, but this is not a general resource type or close protocol.
- ADBC registers chunked and materialized sources, not a lazy-table factory.
  Its source opens the database/connection/statement and owns their cleanup.

## User-visible API

Illustrative new API (not currently accepted syntax/signatures):

```ibex
import "adbc";

let db = adbc_connect("postgresql", uri);
let trades = adbc_query(db, "select * from trades");
let positions = adbc_query(db, "select * from positions");
adbc_close(db);
```

`AdbcConnection` is a nominal opaque type exported by the plugin. Different
resource types are incompatible even if their implementation uses the same
pointer representation. No integer/string conversion, arithmetic, equality,
hashing, serialization, or user-visible address. Display is just the type and
open/closed state, never a URI or credentials.

Use `adbc_query` for the handle-taking form initially. Keep
`read_adbc(driver, uri, sql[, options])` unchanged. Extern declarations are
currently keyed by name; adding overloaded `read_adbc` signatures would require
separate overload-resolution work and previously broke its optional argument.

`adbc_close` is idempotent: return Int64 1 for the first successful close request,
0 if already closed. Driver cleanup failure is reported as an error; the handle
remains closed. Query/open operations use the existing failure mechanism.

Functions take a connection explicitly:

```ibex
fn load_trades(mutable db: AdbcConnection) -> DataFrame {
    adbc_query(db, "select * from trades");
}

fn load_once(uri: String) -> DataFrame {
    let db = adbc_connect("postgresql", uri);
    adbc_query(db, "select * from trades");
} // local owner released automatically, including if the query fails
```

Resource use changes connection state, so query and close take a `mutable`
parameter. This shares ownership; it does not consume the caller's binding.
Do not use `consume` to represent close: aliases would still exist.

## Scope and escape rules

| Situation | Required behavior |
|---|---|
| `let alias = db` | Another owner of the same connection; no new connection |
| Pass `db` to a function | Parameter owns a reference for that invocation |
| Local connection, scalar/table return | Local reference released on exit |
| Return a connection as the last expression | Result owns it before locals are released |
| Function error or cancellation | Release invocation-local owners and temporaries |
| Binding replacement/shadowing | Evaluate RHS first; on success release replaced owner; on failure preserve old binding |
| Unbound temporary | Release at end of its full statement, after dependent execution |
| REPL top-level binding | Owned by the session until replacement, explicit close, or session teardown |
| Batch script top-level binding | Owned by that script's execution session; no process-global resource registry |
| Resource in a column/list/model parameter or serialized result | Reject before execution |
| Free resource name inside a function | Reject; require an explicit parameter in version 1 |

No general free-variable scoping change is proposed for existing scalars/tables.
Resources get explicit lexical bindings and parameters in their own registry;
never copy the caller's whole resource registry into a function frame. If a
column has the same name, existing name resolution still applies; `^db` escapes
column scope but does not waive the query-expression restrictions below.

Locals release in reverse binding-creation order. Physical cleanup happens when
the last owner releases, so aliasing can extend that order. No early last-use
destruction optimization initially: destructor timing is observable database
behavior. A failed REPL statement releases its temporaries, but does not close
connections retained by earlier successful statements or roll back their work.

Returning a resource is supported; implicit closure capture is not. Existing
data columns do not acquire a resource payload. This prevents user-constructed
reference cycles. Native plugins must likewise avoid ownership back-edges.

## Mapping and effects

Resource acquisition, use, and close are forbidden in `select`, `update`,
`filter`, aggregate arguments, window expressions, and other per-row/per-group
expression positions, including calls reached transitively through helpers.
The rule concerns the effect, even when the function returns an ordinary scalar.

For example, reject this pattern before opening any connection:

```ibex
fn remote_count(uri: String) -> Int64 {
    let db = adbc_connect("postgresql", uri);
    scalar(adbc_query(db, "select count(*) as n from trades"), "n");
}
let names = ["a", "b"];
trades[update { map name in names => `count_${name}` = remote_count(uri) }];
```

Suggested diagnostic: `remote_count uses an external resource inside a query
expression; evaluate it in a preceding let binding and use the result here`.
Calling it once in `let n = remote_count(uri);` is allowed. Querying a connection
as the source of a table transformation is also allowed:
`adbc_query(db, sql)[filter qty > 0]`.

Add a transitive resource-operation summary (acquire/use/close) alongside the
existing effect mask. Do not ban all `state` or `nondet` expressions: that would
also ban existing RNG and unrelated operations. Resource-producing/accepting
extern registrations must supply this summary, and wrappers inherit it even
if they hide the handle. Missing/unknown summaries cannot certify a resource
call as safe for expression execution. Validate before and after map expansion.

Resource operations run in source order on the statement coordinator. They
cannot be eliminated as unused, duplicated, commoned, speculatively executed,
or automatically parallelized. Arbitrary SQL is conservatively both I/O read
and write, plus state/blocking/may_fail: a function named query can execute SQL
with side effects. This requires auditing planner barriers, not merely adding
an annotation to the plugin stub.

## Execution and lazy ownership

Version 1 makes no new user-visible lazy ADBC binding. A query may stream batches
through a pipeline within its enclosing statement, but that statement completes
the pipeline before returning/binding a reusable Table. A query-only statement
must still execute even when its result is unused. One-off reads follow the same
ordering rule. Document the materialization cost at a table-binding boundary.

Use a shared connection control block and an internal statement lease:

```text
bindings / parameters / returned handle --> connection control block
active query operator ------------------> statement lease --> control block
retained Arrow buffers -----------------> required backing owners
```

The control block never owns the operators that own it. A statement lease keeps
the driver module, database, and connection alive and owns its statement/stream.
Release stream before statement, connection before database, module last.
Retained batch buffers must keep any required backing storage alive; do not
assume destroying a source makes its exported data independent of the driver.

Allow one active statement per connection in version 1. A second use returns
`connection busy`; do not silently buffer, wait indefinitely, or open a second
connection. Separate connections may be used independently. Preserve this guard
even though normal statement evaluation is sequential.

`adbc_close` marks the shared control block closed immediately: all aliases
reject new operations. An already-started query retains its lease and may finish;
physical release is deferred until leases/backing owners permit it. Close does
not cancel a query. In ordinary version-1 code the preceding query has already
finished by the time close runs.

Future lazy ADBC results need an explicit extension: descriptor capture must own
the connection, but must not secretly reopen/reexecute SQL. Define snapshot,
replay, cancellation, and close-before-first-read semantics before allowing
such descriptors to escape a statement. Existing lazy Parquet behavior is
unchanged by this proposal.

## Cleanup and failures

Successful acquisitions immediately install an RAII owner, including partial
open failures. Last-owner destruction is noexcept and attempts cleanup once;
never throw during unwinding. Preserve the original query error and send any
secondary cleanup failure to the runtime diagnostic sink without credentials.
An explicit close can surface cleanup errors synchronously when no lease delays
release. Delayed cleanup errors use the same diagnostic sink.

Session teardown stops scheduling, requests cancellation of active work, joins
that work, drops intermediate results, and releases bindings before unloading
plugins. Cancellation support and responsiveness depend on the driver; reference
counting does not guarantee a bounded shutdown time. Process crashes/forced kill
cannot promise destructor execution. Automatic cleanup never implies commit;
explicit transaction operations will define commit/rollback behavior later.

## Implementation sequence and acceptance

1. **Resource representation and scopes.** Add a nominal resource type to parser
   types/function signatures and an opaque runtime value with shared control
   block. Keep it out of numeric `ScalarValue`/`ColumnValue`. Extend extern
   argument/result dispatch with a resource-capable path while retaining adapters
   for existing scalar-only plugins. Add ordered resource frames and return-value
   ownership. Rebuild plugins for the registry ABI change.
2. **Placement/effect enforcement.** Propagate resource summaries through helpers,
   validate query contexts/map expansions, and preserve statement ordering in
   planning. Prove invalid placements make zero plugin calls.
3. **ADBC adapter.** Implement connect/query/close, idempotent close and busy guards,
   lease ownership and error unwinding. Preserve the one-off API. Add driver
   module retention rather than relying on accidental session-wide loading.
4. **Parity and documentation.** Generated C++ must either implement the same
   lifetime rules or reject resource-bearing programs clearly before emission;
   never emit raw pointer/integer substitutes. Existing limitations on compiling
   user functions remain explicit. Publish accepted rules in SPEC and docs only
   after implementation, with a reusable-connection example.

Acceptance tests use an instrumented fake resource for exact acquire/release
counts, plus Docker PostgreSQL for real behavior:

- normal exit, failed argument evaluation, partial open, query failure, and cancel;
- local function cleanup, resource returns, aliases, rebinding and session end;
- ignored query results still execute exactly once;
- mapped/row/group helper calls rejected before any connection opens;
- unreferenced caller connections not retained by another function frame;
- one connection retains session state across statements, two remain isolated;
- close invalidates all aliases, repeated close is harmless, busy is deterministic;
- lease and Arrow-buffer lifetimes survive dropping the original binding;
- cleanup failure does not replace a query error; plugins unload after cleanup;
- repeated helper calls returning ordinary values keep live connection count bounded.

## Slice 1 as built (2026-09-27)

Commits on `adbc`: `6d6c2db4` parser, `8c1be262` runtime, `e7d0bedf` REPL,
`16cc5c38` ADBC adapter, then SPEC/docs/example and the compile rejection.

- **Syntax.** `extern type Name from "x.hpp";` (`type` is contextual). A resource
  type may appear only in `extern fn` parameter and return types; a user `fn`
  cannot take or return one yet.
- **Runtime.** `ibex::runtime::Resource` (virtual `type_name()`, cleanup in the
  destructor) behind `ResourcePtr = shared_ptr<Resource>`. `ExternValue` gained
  a `ResourcePtr` alternative. `ExternArgs` is now a class deriving from
  `vector<ScalarValue>` that keeps resource arguments by position (the scalar
  slot is null), so scalar-only plugins compiled unchanged; a resource-taking
  extern reads `args.resource_as<T>(i)`. `register_resource` registers a
  resource-returning function (`ExternReturnKind::Resource`).
- **REPL.** A `ResourceRegistry` that only `execute_statements` and the session
  hold, instead of threading a registry through every evaluator. Resource calls
  run on the statement coordinator: a direct call is the statement; a call in a
  table operand or call argument is evaluated first, in source order, and bound
  as a materialized temporary (restored after the statement, like
  `InlineSourceRewrites`). Placement is validated before any call runs, with the
  same traversal as the hoisting, so a misplaced call makes zero plugin calls.
  Function bodies are checked when called; the whole-script planner declines
  scripts that call resource functions; `ibex_compile` rejects them.
- **ADBC.** `AdbcSession` is the `AdbcConnection` resource (database +
  connection, closed/busy flags); the query operator holds a lease. Close marks
  closed at once and releases now or when the lease ends. Driver-module
  retention: the pinned driver manager never unloads a driver
  (`ManagedLibrary::Release` is a no-op), so zero-copy buffers stay valid.

Acceptance status: fake-resource tests (`tests/test_repl_resources.cpp`) cover
aliases, rebinding, failed rebinding, session end, type checks, zero-call
rejection in clauses/expressions/function bodies, and the script path; SQLite
tests cover session state across statements, isolation from one-off reads, and
idempotent close. Not yet: Docker PostgreSQL checks, busy-guard test through the
language (unreachable while statements are sequential), cleanup-failure
reporting through a diagnostic sink (cleanup errors in destructors are dropped),
cancellation.

No pooling, transactions API, parameter binding, general closure capture,
resource-valued columns, new standalone block syntax, or lazy SQL replay in this
slice. These can build on the ownership rules without being prerequisites.
