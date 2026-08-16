# String Pattern Filter Plan

## Summary

Add a soft-reserved scalar builtin for SQL-LIKE-style string matching:

```ibex
like(value: String, pattern: String) -> Bool
```

Keep this in function form rather than adding an infix `LIKE` keyword. That
fits Ibex's non-SQL surface syntax, avoids reserving another hard keyword, and
uses the existing `CallExpr` path through lowering, interpreted execution,
generated C++, and chunked filtering.

Negation uses the existing logical operator:

```ibex
parts[filter like(p_name, "%green%")]
parts[filter like(p_type, "%BRASS")]
orders[filter !like(o_comment, "%special%requests%")]
```

This removes a string-pattern blocker from the PDS-H/TPC-H query shapes used by
Q2, Q9, Q13, Q14, Q16, and Q20.

## Semantics

- Matching covers the complete input string.
- `%` matches zero or more UTF-8 code points.
- `_` matches exactly one UTF-8 code point.
- Matching is case-sensitive and locale-independent in v1.
- `\` is the fixed escape character for `%`, `_`, and `\`. Because Ibex string
  literals also process escapes, a literal percent is written as
  `like(value, "100\\%")` in source.
- A trailing unescaped `\` is an invalid pattern and produces a clear runtime
  error.
- Null propagates like other ordinary scalar functions. A null value or pattern
  produces null; a null predicate does not retain the row in `filter`.
- Both arguments may be scalar or column expressions. The common
  column-plus-literal-pattern form receives the optimized kernel.
- `like(...)` returns `Bool`, so it is also valid in `select` and `update`:

```ibex
parts[update { is_green = like(p_name, "%green%") }]
```

Do not add `ILIKE`, regex syntax, collation, or configurable escape characters
in v1. These can be separate functions later if real use cases require them.

## Architecture

### Function-call route

Use an ordinary `ir::CallExpr` named `like`.

- No lexer, parser, AST, comparison-operator, or IR-node additions are needed.
- `lower_expr_to_ir` already lowers unknown function calls generically.
- `Emitter::emit_filter_expr` and `Emitter::emit_expr` already emit generic
  calls through `filter_call` and `fn_call`.
- `compute_mask` already accepts a `Bool`-returning call as a filter predicate.
- Chunked filter operators reuse `filter_table`, so they receive the behavior
  without a separate implementation.

Do not implement this solely as an `extern fn`. The vectorized filter evaluator
currently does not carry an extern registry, so a bundled runtime builtin is the
smallest implementation that behaves uniformly in filters, fields, the REPL,
and generated programs.

### Builtin registry

Add `like` to the runtime builtin registry in `src/runtime/expr.cpp`:

- arity: exactly two positional arguments;
- inference: both arguments must be `String`, result is `Bool`;
- null policy: `Propagate`;
- scalar evaluator: the semantic reference implementation for scalar and
  computed-argument fallback paths.

Also teach the best-effort static schema pass in `src/ir/schema.cpp` that
`like(...)` returns `ColumnType::Bool`. Runtime inference remains authoritative
for argument validation.

### Whole-column kernel

Do not ship only the generic per-row scalar path: it materializes/copies string
values into `ExprValue` for each row. Add a `ScalarKernel::Like` entry and a
whole-column evaluator for calls whose arguments are bare columns, literals, or
lexical scalars.

The kernel should:

- operate on `std::string_view` over `Column<std::string>` storage;
- preserve/merge the validity bitmaps of both arguments;
- support string literals, lexical String scalars, dense String columns, and
  `Column<Categorical>`;
- evaluate a categorical dictionary entry once when the pattern is scalar,
  then map the Boolean result through the codes;
- avoid allocating one `std::string` per row;
- return `Column<bool>` so existing mask conversion remains unchanged.

Classify a scalar pattern once before scanning:

- no wildcard: exact equality;
- trailing `%` only: prefix (`starts_with`);
- leading `%` only: suffix (`ends_with`);
- leading and trailing `%` only: substring (`find`);
- otherwise: the general wildcard matcher.

Use a linear wildcard matcher that remembers the most recent `%` rather than
translating to `std::regex`. Repeated `%` tokens should be collapsed. `_` must
advance by one UTF-8 code point, not one byte; malformed input may treat an
invalid leading byte as one unit so the matcher always makes progress.

For a pattern column, evaluate row-by-row with the same matcher. Optional
last-pattern caching is worthwhile only if benchmarks show repeated dynamic
patterns are common.

## Implementation Steps

1. Add focused matcher helpers and scalar semantic tests.
2. Register `like` for inference and scalar evaluation.
3. Add `ScalarKernel::Like` and the dense/categorical whole-column kernel.
4. Add `Bool` result inference to `src/ir/schema.cpp`.
5. Add REPL `:doc like` metadata.
6. Add an `examples/string_filter.ibex` usage example.
7. Update `SPEC.md`, `docs/index.html`, and the public function/reference pages.
8. Run formatting, the relevant test targets, the full parser/runtime suite,
   parity tests, plugin rebuilds if a public runtime header changes, and the
   end-to-end script.

## Test Plan

### Matching semantics

- exact, prefix, suffix, and substring matches;
- `%` matching zero characters and the whole string;
- multiple ordered fragments such as `%special%requests%`;
- `_` with ASCII and multibyte UTF-8 input;
- repeated `%`, empty input, empty pattern, and pattern `%`;
- escaped `%`, `_`, and `\`;
- invalid trailing escape;
- case sensitivity.

### Types and nulls

- dense String column plus literal/scalar pattern;
- categorical column plus literal/scalar pattern;
- String column plus String pattern column;
- null value, null pattern, and mixed validity bitmaps;
- wrong arity and non-String arguments produce stable, specific errors;
- Boolean output in `filter`, `select`, and `update`;
- `!like(...)`, `like(...) && other_predicate`, and `like(...) || ...` preserve
  three-valued Boolean behavior.

### Execution surfaces

- interpreter tests in `tests/test_interpreter.cpp`;
- code-generation shape tests in `tests/test_codegen.cpp`;
- an end-to-end compiled `.ibex` case;
- parity coverage for interpreted versus generated execution;
- chunked String and categorical inputs;
- REPL scalar evaluation and `:doc like`.

## Performance Validation

Add a release-only benchmark covering:

- exact, prefix, suffix, substring, and general wildcard patterns;
- dense high-cardinality strings;
- categorical low-cardinality strings;
- positive match rates near 0%, 10%, and 100%;
- Ibex versus Polars single-threaded and default multi-threaded, following the
  repository's normal comparison policy.

The acceptance criterion is that the optimized literal-pattern path performs a
single scan without per-row string allocation or regex construction. Treat the
scalar evaluator as the correctness fallback, not the performance target.

## Documentation Requirements

This is a new builtin and therefore a language-semantics change. Keep
`SPEC.md` and `docs/index.html` synchronized, and document:

- the signature and examples;
- full-string matching;
- `%`, `_`, and escaping;
- case and UTF-8 behavior;
- null propagation;
- `!like(...)` as the spelling of NOT LIKE.

Also update the public function catalogue/reference and add the `.ibex` example
required for new language functionality. Do not use local filesystem paths in
user-facing documentation.

## Non-Goals

- An infix `LIKE` or `NOT LIKE` keyword.
- General regular expressions.
- Case-insensitive or locale-aware matching.
- Unicode normalization or collation.
- Parquet predicate/statistics pushdown. The initial implementation decodes the
  required string column and applies the shared filter kernel; pushdown can be
  investigated independently later.
