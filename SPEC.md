# Ibex Language Specification

**Version 0.1.0 — Draft**

Ibex is a statically typed domain-specific language for columnar DataFrame and
TimeFrame manipulation. It transpiles to C++23 and supports typed C++ interop
via extern function declarations.

This document specifies the surface syntax, semantics, and type rules of the
Ibex language. It does not describe the runtime, build system, or transpilation
strategy.

---

## 1. Core Syntax Philosophy

### 1.1 Named Clauses over Positional Arguments

R's `data.table` uses positional slots — `DT[i, j, by]` — where `i` filters
rows, `j` computes columns, and `by` groups. Positional semantics create three
problems for a statically typed, compiled language:

1. **Parsing ambiguity.** The meaning of an expression depends on its slot
   position, not its syntactic form. An integer in position `i` is a row index;
   the same integer in position `j` is a column index.
2. **Non-standard evaluation.** `data.table` uses R's metaprogramming to
   capture unevaluated expressions, making static analysis impossible.
3. **Implicit defaults.** Omitting `j` means "return all columns." Omitting `i`
   means "no filter." These defaults require the parser to handle ambiguous
   comma placement.

Ibex replaces positional slots with **named clauses**:

```
df[filter price > 100.0, select { symbol, avg = mean(price) }, by symbol]
```

Every clause is self-identifying. No position is significant.

### 1.2 Clause Ordering

Clause order within a block is **semantically insignificant**. The compiler
normalizes all blocks to the canonical evaluation order:

1. `filter` — row selection (relational σ)
2. `by` — grouping key computation
3. `select` or `update` — column projection / mutation (relational π or ε)
4. `window` — temporal windowing (relational ω)

The following two expressions are equivalent:

```
df[select { symbol }, filter price > 0.0]
df[filter price > 0.0, select { symbol }]
```

The canonical order is recommended for readability but not enforced.

### 1.3 Mapping to Relational Algebra

| Clause              | Relational Operator       | IR Node         |
|---------------------|---------------------------|-----------------|
| `filter`            | σ (selection)             | `FilterNode`    |
| `select`            | π (projection)            | `ProjectNode`   |
| `select` + `by`     | γ (aggregation)           | `AggregateNode` |
| `update`            | ε (extended projection)   | `UpdateNode`    |
| `update` + `by`     | grouped ε (window-like)   | `UpdateNode`    |
| `window`            | ω (temporal window)       | `WindowNode`    |

Every valid block expression maps to a composition of these operators. The
compiler emits the corresponding IR node tree.

### 1.4 Immutability

All `let` bindings are immutable by default. The `let mut` form permits
reassignment of the binding (not mutation of the underlying data). DataFrame
and TimeFrame values are logically immutable; `select`, `update`, and `filter`
produce new values rather than modifying existing ones.

### 1.5 REPL Model

In interactive mode:

- An expression statement (without `let`) is evaluated and its result is
  displayed.
- A `let` statement binds a name and produces no output.
- Trailing semicolons are optional for single-line inputs. In script files,
  semicolons are required.
- All bindings persist for the duration of the session.
- `:load <file>` loads and executes a script in the current REPL context.

---

## 2. Lexical Structure

### 2.1 Comments

```
// Line comment (to end of line)
/* Block comment (may span lines) */
```

Block comments do not nest.

### 2.2 Identifiers

```
IDENT = IDENT_START { IDENT_CONT } ;
IDENT_START = LETTER | "_" ;
IDENT_CONT  = LETTER | DIGIT | "_" ;
LETTER = "a".."z" | "A".."Z" ;
DIGIT  = "0".."9" ;
```

Identifiers are case-sensitive. Leading underscores are permitted but reserved
by convention for internal use.

Column names may also be written as **quoted identifiers** using backticks,
which allows arbitrary characters (including dots and spaces):

```
`Sepal.Length`
`foo bar`
```

Backtick-quoted identifiers can be used wherever a column name is expected
(field lists, `by` keys, schema fields, and column references in expressions).

### 2.3 Keywords

The following identifiers are reserved and may not be used as binding names,
column names, or function names:

```
let    mut    extern  fn     from
filter select update  by     window
join   left   asof    on
true   false
Int32  Int64  Float32 Float64
Bool   String Timestamp
Int    Column  Series DataFrame TimeFrame
```

Built-in function names are **soft-reserved**: they cannot be shadowed by user
bindings but are not syntactic keywords. Built-ins are intentionally minimal;
prefer `extern fn` hooks for functionality implemented in C++.

### 2.4 Literals

**Integers:**

```
INT_LIT = DIGIT { DIGIT | "_" } ;
```

Underscores are permitted as visual separators and carry no semantic meaning.
Examples: `42`, `1_000_000`, `0`.

**Floating-point:**

```
FLOAT_LIT = DIGIT { DIGIT } "." DIGIT { DIGIT } [ EXPONENT ] ;
EXPONENT  = ( "e" | "E" ) [ "+" | "-" ] DIGIT { DIGIT } ;
```

A digit is required on both sides of the decimal point. Examples: `3.14`,
`0.5`, `1.0e10`, `2.5E-3`.

**Strings:**

```
STRING_LIT  = '"' { STRING_CHAR } '"' ;
STRING_CHAR = <any UTF-8 codepoint except '"' and '\'> | ESCAPE_SEQ ;
ESCAPE_SEQ  = '\' ( '"' | '\' | 'n' | 'r' | 't' | '0' ) ;
```

Single-quoted strings are not supported. No string interpolation.

**Booleans:**

```
BOOL_LIT = "true" | "false" ;
```

**Durations:**

```
DURATION_LIT  = INT_LIT DURATION_UNIT ;
DURATION_UNIT = "ns" | "us" | "ms" | "s" | "m" | "h" | "d" | "w" | "mo" | "y" ;
```

No whitespace is permitted between the integer and the unit suffix. Examples:
`5m`, `100ms`, `1h`, `30s`, `7d`.

### 2.5 Operators

**Binary (in precedence order, lowest first):**

| Precedence | Operators                    | Associativity |
|------------|------------------------------|---------------|
| 1          | `\|\|`                       | Left          |
| 2          | `&&`                         | Left          |
| 3          | `==`  `!=`                   | None          |
| 4          | `<`  `<=`  `>`  `>=`         | None          |
| 5          | `+`  `-`                     | Left          |
| 6          | `*`  `/`  `%`                | Left          |

Non-associative operators cannot be chained: `a < b < c` is a parse error.
Use `a < b && b < c`.

**Unary prefix (highest precedence among operators):**

| Operator | Meaning              |
|----------|----------------------|
| `-`      | Arithmetic negation  |
| `!`      | Logical negation     |
| `^`      | Scope escape (Section 6.2) |

`^` binds only to a following `IDENT` and is not a general-purpose operator.

**Postfix (highest precedence overall):**

| Form          | Meaning              |
|---------------|----------------------|
| `expr(args)`  | Function call        |
| `expr[clauses]` | DataFrame block   |

Postfix operators bind tighter than all prefix and infix operators.
Join expressions (`A join B on key`, etc.) have **lower precedence** than all
binary operators and associate **left**.

---

## 3. Type System

### 3.1 Scalar Types

| Type        | Description                  | C++ Mapping       |
|-------------|------------------------------|--------------------|
| `Int`       | Alias for `Int64`            | `std::int64_t`     |
| `Int32`     | 32-bit signed integer        | `std::int32_t`     |
| `Int64`     | 64-bit signed integer        | `std::int64_t`     |
| `Float32`   | 32-bit IEEE 754 float        | `float`            |
| `Float64`   | 64-bit IEEE 754 float        | `double`           |
| `Bool`      | Boolean                      | `bool`             |
| `String`    | UTF-8 string                 | `std::string`      |
| `Timestamp` | Nanosecond-precision instant | `std::chrono::...` |

Integer literals default to `Int64`. Float literals default to `Float64`.
Implicit narrowing conversions are prohibited; explicit widening is permitted
(`Int32` → `Int64`, `Float32` → `Float64`).

### 3.2 Compound Types

```
Column<T>         — alias for Series<T>
Series<T>         — a column of scalar values of type T
DataFrame<S>      — a relation with schema S
TimeFrame<S>      — a time-indexed relation with schema S
```

`TimeFrame<S>` is a `DataFrame<S>` with the additional invariant that exactly
one column of type `Timestamp` is designated as the time index, and rows are
sorted by that index in ascending order.

When a `DataFrame` or `TimeFrame` type omits its schema argument (e.g.
`DataFrame`), the schema is inferred by the implementation (for example, from
`read_csv`). This form is intended for external I/O and does not change the
language’s static typing rules.

### 3.3 Schema Types

A schema describes the typed column layout of a DataFrame or TimeFrame:

```
schema_type = "{" schema_field { "," schema_field } [ "," ] "}" ;
schema_field = IDENT ":" scalar_type ;
```

Example:

```
{ symbol: String, price: Float64, volume: Int64, timestamp: Timestamp }
```

Schemas are structural: two schemas with the same fields in the same order are
the same type. Field order is significant.

### 3.4 Local Type Inference

`let` bindings may omit the type annotation. When omitted, the binding type is
inferred from the right-hand side expression. The language remains statically
typed; inference is local and does not alter function signatures (parameters
and return types remain required).

---

## 4. Formal Grammar

```ebnf
(* ================================================================ *)
(* Ibex Formal Grammar — EBNF                                      *)
(* Operator precedence is resolved by a Pratt parser (Section 2.5). *)
(* This grammar defines surface forms only.                         *)
(* ================================================================ *)

(* --- Programs --- *)

program         = { statement } ;

statement       = let_stmt
                | assign_stmt
                | fn_decl
                | extern_decl
                | expr_stmt ;

let_stmt        = "let" [ "mut" ] IDENT [ ":" type ] "=" expr ";" ;
assign_stmt     = IDENT "=" expr ";" ;
extern_decl     = "extern" "fn" IDENT "(" [ param_list ] ")"
                  "->" type "from" STRING_LIT ";" ;
fn_decl         = "fn" IDENT "(" [ param_list ] ")" "->" type
                  "{" { fn_stmt } "}" ;
expr_stmt       = expr ";" ;

fn_stmt         = let_stmt
                | expr_stmt ;

(* --- Types --- *)

type            = scalar_type
                | "Column" "<" scalar_type ">"
                | type_ctor [ "<" type_arg ">" ] ;

scalar_type     = "Int" | "Int32" | "Int64" | "Float32" | "Float64"
                | "Bool"  | "String" | "Timestamp" ;

type_ctor       = "Series" | "DataFrame" | "TimeFrame" ;

type_arg        = scalar_type
                | schema_type ;

schema_type     = "{" schema_field { "," schema_field } [ "," ] "}" ;
schema_field    = IDENT ":" scalar_type ;

(* --- Expressions --- *)

expr            = primary
                | unary_op expr
                | expr binary_op expr
                | expr "[" clause_list "]"
                | expr "join" expr "on" join_keys
                | expr "left" "join" expr "on" join_keys
                | expr "asof" "join" expr "on" join_keys ;

primary         = IDENT [ "(" [ arg_list ] ")" ]
                | "^" IDENT                      (* scope escape *)
                | literal
                | schema_lit
                | "(" expr ")" ;

arg_list        = expr { "," expr } [ "," ] ;

schema_lit      = "{" schema_field { "," schema_field } [ "," ] "}" ;

(* --- Block Clauses --- *)

clause_list     = clause { "," clause } [ "," ] ;

clause          = filter_clause
                | select_clause
                | update_clause
                | by_clause
                | window_clause ;

filter_clause   = "filter" expr ;

select_clause   = "select" field_or_list ;

update_clause   = "update" field_or_list ;

by_clause       = "by" IDENT
                | "by" "{" field_list "}" ;

window_clause   = "window" DURATION_LIT ;

field_or_list   = field
                | "{" field_list "}" ;

field_list      = field { "," field } [ "," ] ;

field           = IDENT "=" expr
                | IDENT ;

join_keys       = IDENT
                | "{" IDENT { "," IDENT } [ "," ] "}" ;

(* --- Parameters --- *)

param_list      = param { "," param } [ "," ] ;
param           = IDENT ":" type ;

(* --- Operators --- *)

unary_op        = "-" | "!" ;

(* Note: "^" IDENT is a scope-escape primary, not a unary_op.          *)
(* It is restricted to bare identifiers and does not compose with       *)
(* arbitrary expressions. See Section 6.2.                              *)

binary_op       = "+" | "-" | "*" | "/" | "%"
                | "==" | "!=" | "<" | "<=" | ">" | ">="
                | "&&" | "||" ;

(* --- Literals --- *)

literal         = INT_LIT | FLOAT_LIT | STRING_LIT
                | BOOL_LIT | DURATION_LIT ;
```

### 4.1 Disambiguation Rules

The grammar above is intentionally flat for readability. The following rules
resolve all ambiguities in a recursive-descent or Pratt parser:

1. **Statement start.** If the first token is `let`, parse `let_stmt`. If it
   is `extern`, parse `extern_decl`. Otherwise, tentatively parse `expr`. If
   the expression is a bare `IDENT` followed by `=` (not `==`), reinterpret
   as `assign_stmt`.

2. **Call vs. identifier.** After parsing `IDENT` as a primary, if the next
   token is `(`, consume it and parse `arg_list`. Otherwise the primary is a
   bare identifier.

3. **Schema literal vs. field list.** Both use `{ ... }`. They appear in
   different grammatical positions: `schema_lit` is a primary expression;
   field lists appear only after `select`, `update`, or `by`. Within `{ ... }`,
   the token after the first `IDENT` disambiguates: `:` indicates a schema
   field; `=`, `,`, or `}` indicates a field list entry.

4. **`by` clause form.** After `by`, if the next token is `{`, parse a field
   list. Otherwise, parse a single `IDENT`.

5. **Block vs. other postfix.** `[` after an expression always begins a
   clause list. There is no array indexing in Ibex.

6. **Scope escape.** `^` followed by `IDENT` is a scope-escape primary
   (Section 6.2). `^` cannot appear before a non-identifier token; `^(expr)`
   and `^42` are parse errors.
7. **Join precedence.** Join forms parse after all binary operators and bind
   left: `a join b on k join c on k` parses as `(a join b on k) join c on k`.

---

## 5. DataFrame Block Syntax

### 5.1 General Form

A **block expression** applies relational operations to a DataFrame or
TimeFrame value:

```
<expr> "[" <clause>, ... "]"
```

The expression before `[` must evaluate to a `DataFrame<S>` or
`TimeFrame<S>`. The result is a new DataFrame or TimeFrame whose schema is
determined by the clauses.

Block expressions may be chained:

```
df[filter price > 100.0][select { symbol, price }]
```

This is equivalent to a single block with both clauses. The single-block form
is preferred.

### 5.2 Clause Constraints

Within a single block:

| Rule | Constraint |
|------|-----------|
| C1 | At most **one** `filter` clause. |
| C2 | At most **one** `select` clause. |
| C3 | At most **one** `update` clause. |
| C4 | `select` and `update` are **mutually exclusive**. |
| C5 | At most **one** `by` clause. |
| C6 | At most **one** `window` clause. |
| C7 | `by` requires either `select` or `update`. |
| C8 | `window` requires the operand to be a `TimeFrame`. |

Violation of any constraint is a **compile-time error**.

### 5.3 Clause Semantics

**`filter <expr>`**

The expression must have type `Bool` after column resolution (Section 6). Rows
for which the expression evaluates to `false` are excluded. The output schema
is identical to the input schema.

```
trades[filter price > 100.0 && volume > 0]
```

Current implementation restricts filter predicates to simple comparisons of
the form `column <op> literal` or `column <op> scalar`.

**`select { field, ... }`**

Produces a new DataFrame containing only the listed fields. Each field is
either a **pass-through** (bare identifier naming an input column) or a
**computed field** (`name = expr`).

```
trades[select { symbol, notional = price * volume }]
```

Every computed field must be named. Bare non-identifier expressions are a
compile-time error:

```
trades[select { price * 2 }]    // ERROR: computed field must be named
trades[select { doubled = price * 2 }]  // OK
```

The output schema contains exactly the fields listed, in the order listed.

**`update { field, ... }`**

Adds or replaces columns in the DataFrame while retaining all existing columns.
Every field in `update` must be a computed field (`name = expr`). Bare
identifiers are a compile-time error (they would be no-ops).

```
trades[update { log_price = log(price) }]
```

If `name` matches an existing column, it is replaced. If `name` is new, it is
appended. The output schema is the input schema with the specified
modifications.

**`by <keys>`**

Specifies grouping columns for aggregation or grouped update. Two forms:

```
by symbol                             // single key (bare identifier)
by { symbol, yr = year(timestamp) }   // one or more keys, possibly computed
```

The bare-identifier form groups by a single existing column. The braced form
allows multiple keys and computed grouping keys (which must be named).

Interaction with `select` and `update`:

- `select` + `by` → **aggregation**. One output row per group.
- `update` + `by` → **grouped update**. The update expression is evaluated per
  group, and the result is broadcast back to every row in that group (similar
  to SQL window functions without explicit framing).

**`window <duration>`**

Specifies a time-based window for rolling computations. Only valid when the
operand is a `TimeFrame`. See Section 8.

### 5.4 Result Type Rules

| Clauses Present       | Output Type | Schema Derivation |
|----------------------|-------------|-------------------|
| `filter` only         | Same as input | Identical schema |
| `select` only         | `DataFrame<S'>` | S' = listed fields |
| `update` only         | `DataFrame<S'>` | S' = S ∪ new fields |
| `select` + `by`       | `DataFrame<S'>` | S' = listed fields (one row per group) |
| `update` + `by`       | `DataFrame<S'>` | S' = S ∪ new fields (same row count) |
| `window` + `update`   | `TimeFrame<S'>` | S' = S ∪ new fields |
| `window` + `select`   | `TimeFrame<S'>` | S' = listed fields |

If the input is a `TimeFrame` and no clause removes the time index column,
the output remains a `TimeFrame`.

### 5.5 Join Expressions

Join expressions combine two DataFrames or TimeFrames using one or more key
columns. The supported surface forms are:

```
A join B on key
A left join B on key
A asof join B on time
A join B on { key1, key2 }
A left join B on { key1, key2 }
A asof join B on { time }
```

Semantics:

- `A join B on key` is an inner join (rows must match on `key`).
- `A left join B on key` is a left outer join (all rows from `A` are preserved).
- `A asof join B on time` is an as-of join on the TimeFrame index column.

The `on` list must be one or more unqualified column names present in both
input schemas. `asof join` requires both operands to be `TimeFrame`s and the
`on` list must include their shared time index column. Additional columns are
treated as equality keys.

Join expressions are **syntactic sugar** for the built-in join functions:

- `A join B on key` → `inner_join(A, B, key)`
- `A left join B on key` → `left_join(A, B, key)`
- `A asof join B on time` → `asof_join(A, B, time, tolerance = 0s)`
- `A join B on { k1, k2 }` → `inner_join(A, B, k1, k2)`
- `A left join B on { k1, k2 }` → `left_join(A, B, k1, k2)`
- `A asof join B on { time }` → `asof_join(A, B, time, tolerance = 0s)`
- `A asof join B on { time, k1, k2 }` → `asof_join(A, B, time, k1, k2, tolerance = 0s)`

For non-zero as-of tolerances, use the function forms directly (Section 11.3).

---

## 6. Column Resolution Rules

### 6.1 Resolution Order

Inside a DataFrame block `E[clauses]`, identifiers within clause expressions
are resolved as follows:

1. **Column scope.** The identifier is matched against the schema of `E`. If a
   column with that name exists, the identifier resolves to a column reference.

2. **Lexical scope.** If no column matches, the identifier is matched against
   enclosing `let` bindings, from innermost to outermost.

3. **Built-in scope.** If no lexical binding matches, the identifier is matched
   against built-in function names.

4. **Error.** If no match is found in any scope, it is a compile-time error.

```
let threshold = 100.0;
let result = trades[filter price > threshold];
//                         ^^^^^   ^^^^^^^^^
//                         col     lexical
```

### 6.2 Shadowing and the Scope-Escape Operator `^`

A column name may shadow a lexical binding. When this occurs, bare identifiers
resolve to the column. The **scope-escape operator** `^` bypasses column scope
and resolves the identifier directly in lexical scope:

```
let price = 50.0;
let result = trades[
    filter price > ^price,      // 'price' = column; '^price' = lexical (50.0)
];
```

`^` is a prefix restricted to bare identifiers. It is **not** a general unary
operator — `^(expr)`, `^42`, and `^f(x)` are parse errors. Only `^IDENT` is
valid.

**Resolution of `^IDENT`:**

1. The identifier is matched against enclosing `let` bindings (lexical scope).
2. If no lexical binding matches, it is matched against built-in function names.
3. Column scope is **never** consulted.
4. If no match is found, it is a compile-time error.

Using `^` outside a DataFrame block is permitted but redundant — it resolves
identically to the bare identifier since there is no column scope to bypass.

Shadowing without `^` does **not** generate a warning. The resolution rule is
deterministic: bare identifiers prefer column scope; `^` identifiers skip it.

### 6.3 Qualified Access

Qualified column access (`df.price`) is **not supported**. Column references
are only valid inside a block whose operand defines that column. This prevents
ambiguity when multiple DataFrames are in scope and eliminates the need for
join-qualified column resolution in v0.1.

### 6.4 Nested Blocks

Block expressions may appear inside clause expressions:

```
let result = outer[
    filter price > inner[select { threshold = max(price) }],
];
```

Each block introduces its own column scope. Inside the inner block, identifiers
resolve against the inner DataFrame's schema. Inside the outer block (but
outside the inner block), identifiers resolve against the outer DataFrame's
schema.

Nested blocks are allowed but discouraged for readability. Prefer `let`
bindings:

```
let threshold = inner[select { threshold = max(price) }];
let result = outer[filter price > threshold];
```

### 6.5 Field-List Name Binding

In `select` and `update` field lists, the left-hand side of `=` introduces a
new column name. This name is **not** in scope within the same field list:

```
trades[select { x = price * 2, y = x + 1 }]  // ERROR: 'x' not in scope
```

To reference a computed column, use a separate block or `let` binding.

---

## 7. Aggregation Rules

### 7.1 Aggregate Functions

The following built-in functions are **aggregate functions**. They consume a
`Series<T>` (the full column or a per-group slice) and produce a scalar:

| Function   | Input           | Output     |
|------------|-----------------|------------|
| `sum(col)` | `Series<Numeric>` | Same numeric type |
| `mean(col)` | `Series<Numeric>` | `Float64`  |
| `min(col)` | `Series<T>`     | `T`        |
| `max(col)` | `Series<T>`     | `T`        |
| `count()`  | (none)          | `Int64`    |
| `first(col)` | `Series<T>`   | `T`        |
| `last(col)` | `Series<T>`    | `T`        |

`Numeric` denotes `Int32 | Int64 | Float32 | Float64`.

All other functions (user-defined externs, built-in scalars) are **scalar
functions**.

### 7.2 Grouped Select Well-formedness

When `by` and `select` are both present, the block performs aggregation. Each
field in the `select` clause must satisfy the **aggregation well-formedness
rule**:

> Every column reference in the field expression must either:
>
> **(a)** name a grouping key column (listed in the `by` clause), or
>
> **(b)** be contained (at any depth) within the argument list of an aggregate
>     function call.

**Valid examples:**

```
// 'symbol' is a group key; sum() is aggregate
df[select { symbol, total = sum(volume) }, by symbol]

// Arithmetic on aggregate results is permitted
df[select { symbol, adjusted = sum(price) + 1.0 }, by symbol]

// Multiple columns inside one aggregate
df[select { symbol, notional = sum(price * volume) }, by symbol]
```

**Invalid examples:**

```
// 'price' is not a group key and not inside an aggregate
df[select { symbol, price }, by symbol]       // ERROR

// 'volume' is bare (not a group key, not aggregated)
df[select { x = sum(price) + volume }, by symbol]  // ERROR
```

### 7.3 Nested Aggregates

Aggregate function calls must **not** contain other aggregate function calls:

```
df[select { x = sum(mean(price)) }, by symbol]  // ERROR: nested aggregate
```

To compose aggregations, use intermediate `let` bindings or separate block
expressions.

### 7.4 Grouped Update

When `by` and `update` are both present, expressions in `update` are evaluated
**per group** and the results are broadcast back to every row in the group:

```
df[update { group_avg = mean(price) }, by symbol]
```

Each row receives the `mean(price)` of its group. The aggregation
well-formedness rule (Section 7.2) applies identically: all column references
must be group keys or inside aggregate functions.

### 7.5 Ungrouped Aggregation

Aggregate functions may appear in `select` **without** a `by` clause. In this
case, the entire DataFrame is treated as a single group. The result is a
single-row DataFrame:

```
df[select { total = sum(price), n = count() }]
// Result: DataFrame with one row
```

The aggregation well-formedness rule still applies: mixing bare column
references with aggregate functions is an error.

```
df[select { symbol, total = sum(price) }]  // ERROR: 'symbol' is bare
```

---

## 8. TimeFrame Extensions

### 8.1 TimeFrame Construction

A `TimeFrame` is created from a `DataFrame` by designating a `Timestamp`
column as the time index:

```
let tf = as_timeframe(trades, timestamp);
```

The second argument must be an unqualified identifier naming a `Timestamp`
column in the first argument's schema. The resulting `TimeFrame` maintains the
sortedness invariant: rows are ordered by the index column in ascending order.
If the input is not sorted, `as_timeframe` sorts it.

### 8.2 Duration Literals

Duration literals specify time spans for windowing and tolerance parameters:

| Suffix | Unit         | Nanoseconds            |
|--------|--------------|------------------------|
| `ns`   | nanosecond   | 1                      |
| `us`   | microsecond  | 1,000                  |
| `ms`   | millisecond  | 1,000,000              |
| `s`    | second       | 1,000,000,000          |
| `m`    | minute       | 60,000,000,000         |
| `h`    | hour         | 3,600,000,000,000      |
| `d`    | day          | 86,400,000,000,000     |
| `w`    | week         | 604,800,000,000,000    |
| `mo`   | month        | ~30.44d (calendar)     |
| `y`    | year         | ~365.25d (calendar)    |

`mo` (not `m`) distinguishes months from minutes. Month and year durations are
calendar-aware: `1mo` from Jan 15 means Feb 15, not a fixed nanosecond count.
They may only appear in `window` clauses and `tolerance` arguments, not in
arithmetic.

Duration literals are compile-time constants. Arithmetic on durations is not
supported in v0.1.

### 8.3 Window Clause

The `window` clause specifies a lookback duration for rolling computations:

```
tf[window 5m, update { avg_5m = rolling_mean(price) }]
```

**Semantics.** For each row at time `t`, the window defines the range
`[t - duration, t]` (inclusive on both ends). Rolling functions operate on the
subset of rows within this range.

**Constraint.** `window` is valid only when the operand is a `TimeFrame`
(constraint C8). Using `window` on a `DataFrame` is a compile-time error.

### 8.4 Temporal Functions

The following functions are available inside `window`-scoped blocks. Rolling
functions require an active `window` clause; `lag` and `lead` do not.

**Rolling aggregates (require `window`):**

| Function            | Description                    |
|---------------------|--------------------------------|
| `rolling_sum(col)`  | Sum within window              |
| `rolling_mean(col)` | Mean within window             |
| `rolling_min(col)`  | Minimum within window          |
| `rolling_max(col)`  | Maximum within window          |
| `rolling_count()`   | Row count within window        |

Rolling functions are **aggregate-like**: they produce one scalar per row
(evaluated over the window) and are valid in both `select` and `update`.

**Positional shift (no `window` required):**

| Function           | Description                              |
|--------------------|------------------------------------------|
| `lag(col, n)`      | Value `n` rows before current, by index order |
| `lead(col, n)`     | Value `n` rows after current, by index order  |

`n` must be a non-negative integer literal. If the offset exceeds the available
range, the result is the type's default value (0 for numerics, empty string for
`String`, epoch for `Timestamp`).

`lag` and `lead` respect the TimeFrame's index ordering. They are valid in any
TimeFrame block, with or without a `window` clause.

### 8.5 Sortedness Invariant

A `TimeFrame` guarantees ascending order by its time index at all times. Any
operation that would violate this invariant (e.g., updating the index column)
is a compile-time error.

```
tf[update { timestamp = timestamp + 1s }]  // ERROR: cannot mutate time index
```

---

## 9. Extern Function Interop

## 9. User-Defined Functions

User-defined functions group multiple statements under a single name.

```
fn <name>(<params>) -> <return_type> { <statements> }
```

- All parameters and the return type are **required**.
- The function body may contain `let` statements and expression statements.
- The **last expression statement** is the return value.
- Nested function declarations are not supported.

`Column<T>` is accepted in function signatures as an alias for `Series<T>`.
Functions may return scalars, tables, or columns; the return expression must
match the declared return type.

Function calls inside DataFrame clause expressions are resolved against
built-ins or extern functions. User-defined functions are evaluated at the
statement level in the REPL/runtime.

> Note: transpilation of user-defined function bodies is planned. The current
> implementation evaluates them in the REPL/runtime.

---

## 10. Extern Function Interop

### 10.1 Declaration Syntax

External C++ functions are declared with `extern fn`:

```
extern fn <name>(<params>) -> <return_type> from <header_path> ;
```

Example:

```
extern fn variance(x: Series<Float64>) -> Float64 from "stats.hpp";
extern fn zscore(x: Float64, mu: Float64, sigma: Float64) -> Float64
    from "math_utils.hpp";
```

The `from` clause specifies the C++ header that provides the function. The
compiler generates the appropriate `#include` directive in the transpiled
output.

### 10.2 Parameter Types

Extern function parameters may be any scalar type (`Int`, `Int64`, `Float64`,
`Bool`, `String`, `Timestamp`). Series/column parameters are not supported in
the current runtime.

Return types may be scalar or table types (`DataFrame`, `TimeFrame`). Extern
functions cannot return `Series`.

### 10.3 Calling Convention

Extern functions are called with the same syntax as built-in functions:

```
let summary = df[
    select { symbol, vol = variance(price) },
    by symbol,
];
```

Extern functions currently accept only scalar arguments and are evaluated as
**scalar functions** (applied element-wise). Column/series arguments are
reserved for a future extension.

### 9.4 Restrictions

| Restriction                           | Rationale                         |
|---------------------------------------|-----------------------------------|
| No C++ templates in extern signatures | Simplifies type resolution        |
| No overloads (one name = one function)| Prevents dispatch ambiguity       |
| No C++ namespaces in names            | Names are Ibex identifiers        |
| No default arguments                  | All arguments must be explicit    |
| No variadic parameters                | Simplifies IR generation          |

If a C++ function is templated or overloaded, provide a non-templated wrapper
with a unique name and declare that wrapper as the extern.

---

## 11. Built-in Functions and Forms

Ibex keeps built-ins intentionally minimal. Implementations may provide
additional functions, but the recommended path for custom functionality is
`extern fn` interop with C++.

### 11.1 I/O Functions

I/O is provided via externs rather than built-ins. A common example:

```
extern fn read_csv(path: String) -> DataFrame<Schema> from "csv.hpp";
```

`read_csv` infers column types from the input file. The resulting schema is
implementation-defined.

### 11.2 Scalar Extraction

```
scalar(df: DataFrame<S>, col: Ident) -> T
```

Extracts a single scalar from a one-row DataFrame. `col` names a column in `S`.
It is a runtime error if the DataFrame has any row count other than 1.

### 11.3 Join Functions

```
inner_join(left: DataFrame<A>, right: DataFrame<B>, key1, ..., keyN) -> DataFrame<A ∪ B>
left_join(left: DataFrame<A>, right: DataFrame<B>, key1, ..., keyN) -> DataFrame<A ∪ B>
asof_join(left: TimeFrame<A>, right: TimeFrame<B>, key1, ..., keyN, tolerance: Duration) -> TimeFrame<A ∪ B>
```

Key arguments are unqualified identifiers naming columns present in **both**
input schemas. The output schema is the union of both input schemas (duplicate
key columns appear once).

`asof_join` is valid only on `TimeFrame` operands. The final argument is a
duration literal specifying the maximum time difference for a match. Both
TimeFrames must share the same time index column. Additional key arguments
apply equality matching in addition to the time-based match. Join expressions
(Section 5.5) always pass a tolerance of `0s`.

### 11.4 Ordering

```
sort(df: DataFrame<S>, col: Ident) -> DataFrame<S>
```

Returns a new DataFrame sorted by the named column in ascending order. Sorting
a `TimeFrame` by its index column is a no-op (already sorted).

### 11.5 Display

```
print(value: Any) -> ()
```

Outputs a human-readable representation of the value. In REPL mode, expression
statements are implicitly printed without requiring `print`.

### 11.6 Scalar Functions

These scalar functions are optional and may be provided by the runtime or by
extern implementations. The recommended path for custom scalar logic is
`extern fn`.

| Function        | Signature                          |
|-----------------|------------------------------------|
| `abs(x)`        | `Numeric -> Numeric`               |
| `log(x)`        | `Numeric -> Float64`               |
| `sqrt(x)`       | `Numeric -> Float64`               |
| `year(t)`       | `Timestamp -> Int32`               |
| `month(t)`      | `Timestamp -> Int32`               |
| `day(t)`        | `Timestamp -> Int32`               |
| `hour(t)`       | `Timestamp -> Int32`               |
| `minute(t)`     | `Timestamp -> Int32`               |
| `second(t)`     | `Timestamp -> Int32`               |

---

## 12. Minimal Complete Example

The following program is syntactically and semantically valid under this
specification. It demonstrates DataFrame loading, grouped aggregation, extern
function usage, and TimeFrame windowing.

```
// --------------------------------------------------
// Extern: CSV loader + scalar helper
// --------------------------------------------------
extern fn read_csv(path: String) -> DataFrame<Schema> from "csv.hpp";
extern fn clamp(x: Float64, lo: Float64, hi: Float64) -> Float64 from "stats.hpp";

// --------------------------------------------------
// Load iris dataset (schema inferred)
// --------------------------------------------------
let iris = read_csv("iris.csv");

// --------------------------------------------------
// Grouped aggregation with extern function call
// --------------------------------------------------
let summary = iris[
    select {
        species,
        mean_sl = mean(sepal_length),
        n = count(),
    },
    by species,
];

// --------------------------------------------------
// Filter and project (with scope escape)
// --------------------------------------------------
let sepal_width = 3.0;
let wide_sepals = iris[
    filter sepal_width > ^sepal_width,  // column > lexical
    select { species, sepal_length, sepal_width },
];

// --------------------------------------------------
// Grouped update (broadcast group mean to each row)
// --------------------------------------------------
let annotated = iris[
    update {
        group_mean_sl = mean(sepal_length),
        clamped_sl = clamp(sepal_length, 0.0, 10.0),
    },
    by species,
];

// --------------------------------------------------
// TimeFrame: load tick data, compute rolling returns
// --------------------------------------------------
let ticks = read_csv("ticks.csv");

let tf = as_timeframe(ticks, timestamp);

let enriched = tf[
    window 5m,
    update {
        ret = log(price / lag(price, 1)),
        avg_price_5m = rolling_mean(price),
    },
];

// --------------------------------------------------
// Output
// --------------------------------------------------
write_csv(summary, "summary.csv");
print(enriched);
```

### IR Lowering Sketch

The `summary` expression above lowers to the following IR node tree:

```
AggregateNode {
    group_by: [ColumnRef("species")]
    aggregations: [
        AggSpec { func: Mean,  column: "sepal_length", alias: "mean_sl" }
        AggSpec { func: Extern("std_dev"), column: "sepal_length", alias: "sd_sl" }
        AggSpec { func: Count, column: null, alias: "n" }
    ]
    children: [
        ScanNode { source: "iris" }
    ]
}
```

This maps directly to the `AggregateNode` and `ScanNode` types in the
existing Ibex IR (see `include/ibex/ir/node.hpp`).

---

## Appendix A: Operator Precedence Table

Highest precedence at top.

| Level | Category     | Operators                         | Assoc. |
|-------|--------------|-----------------------------------|--------|
| 9     | Postfix      | `()` call, `[]` block             | Left   |
| 8     | Scope escape | `^` IDENT                         | —      |
| 7     | Unary        | `-` `!`                           | Right  |
| 6     | Mult.        | `*` `/` `%`                       | Left   |
| 5     | Additive     | `+` `-`                           | Left   |
| 4     | Relational   | `<` `<=` `>` `>=`                 | None   |
| 3     | Equality     | `==` `!=`                         | None   |
| 2     | Logical AND  | `&&`                              | Left   |
| 1     | Logical OR   | `\|\|`                            | Left   |
| 0     | Join         | `join` `left join` `asof join`    | Left   |

---

## Appendix B: Reserved Words

**Hard keywords** (always reserved, used by grammar):

```
let  mut  extern  fn  from  filter  select  update  by  window  join  left  asof  on  true  false
```

**Type keywords** (reserved in type position and as identifiers):

```
Int  Int32  Int64  Float32  Float64  Bool  String  Timestamp
Column  Series  DataFrame  TimeFrame
```

**Soft-reserved** (cannot be shadowed by user bindings):

```
scalar
sum  mean  min  max  count  first  last
```

---

## Appendix C: Grammar Railroad Summary

For implementors. The core parsing loop:

```
parse_statement:
    "let"    → let_stmt
    "fn"     → fn_decl
    "extern" → extern_decl
    IDENT    → peek "=" (not "==") → assign_stmt
             → otherwise → expr_stmt

parse_expr (Pratt):
    NUD: IDENT, "^" IDENT, literal, schema_lit, "(", unary_op
    LED: binary_op, join_form, "[" (block), "(" (call — only after IDENT NUD)

parse_clause:
    "filter" → filter_clause
    "select" → select_clause
    "update" → update_clause
    "by"     → peek "{" → braced field list
             → otherwise → single IDENT
    "window" → DURATION_LIT

join_form:
    expr "join" expr "on" IDENT
    expr "left" "join" expr "on" IDENT
    expr "asof" "join" expr "on" IDENT
```

The parser is fully deterministic with 1-token lookahead after consuming the
leading token of each production.
