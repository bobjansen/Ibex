# Parity cases

Each `<name>.ibex` here is run two ways and the results are compared:

1. **transpiled** — `ibex_compile <name>.ibex --table-entry-point`, the emitted
   C++ compiled and linked against `structured_runner.cpp`.
2. **interpreted** — `structured_runner` parses + lowers + `interpret()`s the
   same source and `compare_tables()` against the transpiled `ibex_generated_execute()`.

A mismatch, or a case that fails to transpile, fails `ctest`
(`ibex_parity_interpreter_vs_transpiled`). This is the load-bearing check that
`ibex_compile` and the interpreter agree.

## `<name>.unsupported` markers — the conformance gate

The harness is a **conformance gate**, not an allowlist. Historically a case
that `ibex_compile` could not yet handle simply wasn't added, so
interpreter-only features were invisible. Now every case must either transpile
**or** carry a `<name>.unsupported` marker file whose **first line** is a
one-line reason (workstream + plan reference).

For a marked case the harness instead checks:

- `ibex_eval <name>.ibex` **succeeds** — it is still valid Ibex the interpreter
  runs (catches an invalid case, or an interpreter regression).
- `ibex_compile <name>.ibex` still **fails** — if it starts succeeding the
  marker is stale: the suite fails and tells you to delete the marker and let
  the case run for real.

So closing a transpiler gap = deleting its markers. Add a marked case for a
construct the interpreter supports but `ibex_compile` does not, pointing at the
workstream that will fix it (`plans/ibex-compile-conformance-plan.md`).

Current markers:

| Case | Reason |
|------|--------|
| `map_rows` | W1a — `ibex_compile` has no `MapNode` case |
