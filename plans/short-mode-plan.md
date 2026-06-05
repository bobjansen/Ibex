# Ibex Short Mode Plan

Goal: support a golf/short authoring mode for fast interactive work while keeping
canonical Ibex unchanged. Short mode should parse to the same AST as long-form
Ibex and provide a formatter path back to readable canonical source.

Working slogan: **Ibex: tables at the speed of thought.**

## Shape

Long form:

```ibex
trades[
  select { symbol, price }
]
```

Short form:

```ibex
t[s{s,p}]
```

Possibly even:

```ibex
t[s{s p}]
```

The whitespace-separated select-list form is unambiguous in a `select` block when
the parser knows it is reading field expressions rather than binary expressions or
clause separators. This should be explored as part of short-mode grammar, not
added to canonical syntax by accident.

## Resolution rule

Short mode uses prefix matching:

1. Built-in keywords and built-in functions are matched first in positions where
   they are syntactically valid.
2. Column names are matched next.
3. Lexical bindings are matched after column names, preserving the existing
   column-scope-before-lexical-scope rule.
4. Ambiguous prefixes are errors with candidate lists.

Examples:

```ibex
t[s{s,p}]
```

could expand to:

```ibex
trades[select { symbol, price }]
```

assuming `t`, `s`, and `p` are unique prefixes for `trades`, `symbol`, and
`price` in their respective contexts. The inner `s` can resolve to `symbol`
because `select` is not valid as a field expression there, while the outer `s`
resolves to `select` because it is in clause position.

## Mode gate

Do not accept prefix abbreviations by default in normal Ibex source. Require an
explicit mode so typos and future built-ins cannot silently change meaning:

- REPL flag: `:short on`
- CLI flag: `ibex run --short file.ibex`
- Formatter input mode: `ibex fmt --short-input`
- File pragma if needed later: `# ibex: short`

Diagnostics should report the expansion for abbreviated identifiers, especially
when an error occurs after expansion.

## Imports

Once wildcard imports land, `import *` should be the default import behavior for
short-mode ergonomics. The point of short mode is low-friction interactive
authoring, so requiring explicit imported names works against the mode.

Long-form canonical output should still make import behavior explicit if needed,
for example by emitting `import *` or concrete imports according to the formatter
policy chosen for normal source.

## Formatting / round-trip

Formatter support is the safety valve:

- `ibex fmt --long` expands short mode into canonical source.
- `ibex fmt --short` compresses canonical source to the shortest unambiguous
  prefix form under the available schema.
- CI and docs should prefer long form unless a page explicitly demonstrates short
  mode.

The long-form expansion should be deterministic and should preserve source spans
well enough for useful diagnostics.

## Open design questions

- Whether whitespace-separated select lists (`s{s p}`) should be short-mode only
  or supported anywhere braced select/update lists are already comma-separated.
- Whether table names should use the same prefix matching as columns, or require
  a table alias declaration to avoid surprises.
- How schema-dependent formatting works when a file contains unresolved table
  references or imports.
- Whether `ibex fmt --short` should prefer one-letter prefixes or a readability
  threshold, for example `sym` over `s` when several nearby names share a stem.
