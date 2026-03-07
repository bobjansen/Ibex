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
3. `select`, `update`, or `distinct` — column projection / mutation (relational π or ε)
4. `rename` — column renaming (relational ρ)
5. `order` — row ordering (relational τ)
6. `window` — temporal windowing (relational ω)

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
| `distinct`          | δ (duplicate removal)     | `DistinctNode`  |
| `rename`            | ρ (renaming)              | `RenameNode`    |
| `order`             | τ (ordering)              | `OrderNode`     |
| `select` + `by`     | γ (aggregation)           | `AggregateNode` |
| `update`            | ε (extended projection)   | `UpdateNode`    |
| `update` + `by`     | grouped ε (window-like)   | `UpdateNode`    |
| `window`            | ω (temporal window)       | `WindowNode`    |
| `melt`              | unpivot (wide → long)     | `MeltNode`      |
| `dcast`             | pivot (long → wide)       | `DcastNode`     |
| `Table { … }`       | literal construction      | `ConstructNode` |

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
let    mut    extern  fn      from
filter select update  distinct order by window
rename resample melt  dcast
join   left   right   outer   asof   on
import Stream
asc    desc
true   false
Int32  Int64  Float32 Float64
Bool   String Date    Timestamp
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

**Dates:**

```
DATE_LIT = date"YYYY-MM-DD" ;
```

Dates are parsed as calendar days in the proleptic Gregorian calendar and are
stored as signed days since `1970-01-01`.

**Timestamps:**

```
TIMESTAMP_LIT = ("timestamp" | "ts") "\"" YYYY "-" MM "-" DD ( "T" | " " )
                HH ":" MM ":" SS [ "." FRACTION ] [ "Z" ] "\"" ;
FRACTION = DIGIT { DIGIT } ;  // 1..9 digits, interpreted as fractional seconds
```

Timestamps are parsed as instants in UTC and stored as signed nanoseconds since
`1970-01-01T00:00:00Z`. Fractional seconds are truncated to nanosecond
precision.

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
| `Date`      | Calendar day                 | `std::int32_t`     |
| `Timestamp` | Nanosecond-precision instant | `std::int64_t`     |

Integer literals default to `Int64`. Float literals default to `Float64`.
Implicit narrowing conversions are prohibited; explicit widening is permitted
(`Int32` → `Int64`, `Float32` → `Float64`). Implicit `Int`↔`Float` coercion
is also rejected — use explicit cast constructors instead (Section 3.1.1).

`Date` values are stored as signed days since `1970-01-01` (Unix epoch).
`Timestamp` values are stored as signed nanoseconds since
`1970-01-01T00:00:00Z`.

#### 3.1.1 Explicit Cast Constructors

A scalar type name followed by `(expr)` is an explicit cast:

```
Int64(x)    Float64(x)
Int32(x)    Float32(x)
Int(x)      -- alias for Int64(x)
```

**Float → Int casts** succeed only when the value is already a whole number
(i.e. `trunc(x) == x`). Passing a value with a fractional part — e.g.
`Int64(3.9)` — is a runtime error. Use `round(x, mode)` (Section 11.6) to
convert to the nearest integer before casting.

**Int → Float casts** always succeed (subject to precision loss for very large
`Int64` values converted to `Float32` or `Float64`).

**Column casts** apply element-wise: `Int64(price_col)` produces a
`Series<Int64>` from a `Series<Float64>`, checking every element.

```
// Scalar cast
let n: Int64 = Int64(3.0);     // ok — 3.0 is a whole number
let bad = Int64(3.9);           // runtime error: 3.9 is not a whole number

// Column cast
prices[update { vol_int = Int64(volume_f) }];

// Round first, then cast
prices[update { vol_int = Int64(round(volume_f, nearest)) }];
```

### 3.2 Compound Types

```
Column<T>         — alias for Series<T>
Series<T>         — a column of scalar values of type T
DataFrame<S>      — a relation with schema S
TimeFrame<S>      — a time-indexed relation with schema S
```

`Column<Bool>` is a first-class column type (boolean mask). It is produced by
`rep(true)` / `rep(false)` and can appear in `update`/`select` field lists.
Boolean column values are stored as bytes (0 = false, non-zero = true) and are
not implicitly convertible to `Int64` in arithmetic expressions.

`TimeFrame<S>` is a `DataFrame<S>` with the additional invariant that exactly
one column of type `Timestamp` is designated as the time index, and rows are
sorted by that index in ascending order.

DataFrame values may carry an **ordering constraint**: an optional declaration
that rows are ordered by a list of keys with directions (e.g. `{ symbol asc,
time desc }`). Ordering constraints are **not part of the static type**; they
are metadata used for optimization and for validating operations that require
ordering. A `TimeFrame` always carries the ordering constraint on its time
index column in ascending order.

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

### 3.4 Local Type Inference and Annotation Validation

`let` bindings may omit the type annotation. When omitted, the binding type is
inferred from the right-hand side expression. The language remains statically
typed; inference is local and does not alter function signatures (parameters
and return types remain required).

When a type annotation **is** present, it is validated at the point of binding
or call:

- **Scalar annotations** (`Int64`, `Float64`, etc.) require an exact type
  match. Passing an `Int64` where `Float64` is expected — or vice versa — is
  an error.
- **DataFrame/TimeFrame schema annotations** require that all declared columns
  are present with the correct types. Extra columns in the value are allowed
  (structural subtyping); missing or mistyped declared columns are errors.
- **Bare `DataFrame`** (no schema fields) skips validation and accepts any
  DataFrame value.

```
let x: Int64 = 42;                // ok
let y: Float64 = 42;              // error — Int64 is not Float64
let y: Float64 = Float64(42);     // ok — explicit cast

fn compute(n: Int64) -> Float64 { ... }
compute(3.0);                      // error — Float64 argument for Int64 param
compute(Int64(3.0));               // ok
```

### 3.5 Nullable Columns and Three-Valued Logic

Every column carries an **Arrow-style validity bitmap** alongside its value
buffer. A cell is either *valid* (non-null) or *null* (no value). Ibex uses
SQL-style **three-valued logic (3VL)**: every predicate can evaluate to `true`,
`false`, or `null`.

#### Null Propagation

Null propagates through arithmetic and comparison expressions:

```
null + x     = null
null * x     = null
null > x     = null     // not false — the result is unknown
null = null  = null     // equality with null is never true
```

A `filter` clause keeps only rows where the predicate evaluates to `true`.
Rows where the predicate is `null` are **silently dropped** (not an error).

#### IS NULL / IS NOT NULL

The special predicates `is null` and `is not null` test the validity bitmap
directly and always return a valid `Bool` — never null:

```
expr is null        // true iff expr's validity bitmap is false
expr is not null    // true iff expr's validity bitmap is true
```

These are the only predicates that can reliably detect null values:

```
enriched[filter { dept_name is null }]      // rows with no matched department
enriched[filter { dept_name is not null }]  // rows with a known department
```

#### 3VL Boolean Logic

Boolean operators follow SQL 3VL rules, where null means "unknown":

| Expression            | Result  |
|-----------------------|---------|
| `true  OR  null`      | `true`  |
| `false OR  null`      | `null`  |
| `true  AND null`      | `null`  |
| `false AND null`      | `false` |
| `NOT null`            | `null`  |

Known-true wins in OR; known-false wins in AND.

#### Null Sources

Nulls arise from:
- **Left join** — unmatched right-side columns for a row with no join partner
- **`dcast`** — missing pivot combinations are filled with null
- **`melt`** — a null measure value in the input produces a null `value` cell
- **Aggregate functions** — some functions return null for empty groups (e.g.
  `ewma` on an empty group, `std` on a group with fewer than 2 non-null values)

#### Aggregate Functions and Null

Aggregate functions skip null rows by default:

```
sum(col)    // sums only non-null values; returns 0 for an all-null group
count()     // counts all rows regardless of null
mean(col)   // averages only non-null values
median(col) // ignores null rows
std(col)    // ignores null rows; returns null if fewer than 2 non-null values
ewma(col, alpha)  // ignores null rows; returns null for an empty group
```

#### Null-Fill Functions

Three built-in functions replace or propagate null values within a column.
They are valid in both `select` and `update` blocks and require no `window`
clause.

| Function                 | Description                                                        |
|--------------------------|--------------------------------------------------------------------|
| `fill_null(col, value)`  | Replace every null cell with the constant `value`                 |
| `fill_forward(col)`      | LOCF — carry the last valid value forward; leading nulls stay null |
| `fill_backward(col)`     | NOCB — carry the next valid value backward; trailing nulls stay null |

All three functions accept any column type and return the same type as `col`.

**`fill_null(col, value)`** — constant fill:

```
// Replace null prices with 0
df[update { price = fill_null(price, 0) }]

// Replace null labels with "unknown"
df[update { label = fill_null(label, "unknown") }]
```

The `value` literal must be type-compatible with `col`. After filling, the
result column has no validity bitmap (all rows are valid).

**`fill_forward(col)`** — last observation carried forward (LOCF):

```
// Carry the previous valid price into any null gaps
df[update { price = fill_forward(price) }]
```

Fills each null cell with the most recent preceding valid value. Leading nulls
(rows before the first valid value) cannot be filled and remain null.

**`fill_backward(col)`** — next observation carried backward (NOCB):

```
// Fill null prices from the next available price
df[update { price = fill_backward(price) }]
```

Fills each null cell with the nearest following valid value. Trailing nulls
(rows after the last valid value) cannot be filled and remain null.

**Combining fill strategies:**

Chain operations using multiple `update` blocks to combine strategies:

```
// Fill forward first, then replace any remaining leading nulls with 0
df[update { price = fill_forward(price) }]
  [update { price = fill_null(price, 0) }]
```

#### Column Storage

Null is represented as a validity bit in an Arrow-compatible bitmap, not as a
sentinel value in the data buffer. The stored value in a null cell is
unspecified and must not be read directly. This design avoids ambiguity between
the value zero / empty string and "no value".

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
                | "Bool"  | "String" | "Date" | "Timestamp" ;

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
                | expr "right" "join" expr "on" join_keys
                | expr "outer" "join" expr "on" join_keys
                | expr "asof" "join" expr "on" join_keys
                | expr "join" expr "on" expr
                | expr "left" "join" expr "on" expr
                | expr "right" "join" expr "on" expr
                | expr "outer" "join" expr "on" expr
                | expr "semi" "join" expr "on" expr
                | expr "anti" "join" expr "on" expr ;

primary         = IDENT [ "(" [ arg_list ] ")" ]
                | "Table" "{" [ table_col_def { "," table_col_def } [ "," ] ] "}"
                | "^" IDENT                      (* scope escape *)
                | literal
                | array_lit
                | schema_lit
                | "(" expr ")" ;

table_col_def   = IDENT "=" array_lit ;

array_lit       = "[" [ expr { "," expr } [ "," ] ] "]" ;

arg_list        = arg { "," arg } [ "," ] ;

arg             = expr
                | IDENT "=" expr ;    (* named argument *)

schema_lit      = "{" schema_field { "," schema_field } [ "," ] "}" ;

(* --- Block Clauses --- *)

clause_list     = clause { "," clause } [ "," ] ;

clause          = filter_clause
                | select_clause
                | distinct_clause
                | update_clause
                | order_clause
                | by_clause
                | window_clause
                | melt_clause
                | dcast_clause ;

filter_clause   = "filter" expr ;

select_clause   = "select" field_or_list ;

distinct_clause = "distinct" field_or_list ;

update_clause   = "update" field_or_list ;

order_clause    = "order"
                | "order" order_keys ;

order_keys      = order_key
                | "{" order_key { "," order_key } [ "," ] "}" ;

order_key       = IDENT [ "asc" | "desc" ] ;

by_clause       = "by" IDENT
                | "by" "{" field_list "}" ;

window_clause   = "window" DURATION_LIT ;

melt_clause     = "melt" field_or_list ;

dcast_clause    = "dcast" IDENT ;

field_or_list   = field
                | "{" field_list "}" ;

field_list      = field_item { "," field_item } [ "," ] ;

field_item      = field
                | tuple_field ;

field           = col_name "=" expr
                | col_name ;

tuple_field     = "(" col_name { "," col_name } ")" "=" expr ;

col_name        = IDENT | QUOTED_IDENT ;

update_clause   = "update" field_or_list
                | "update" "=" expr ;      (* merge-all form *)

join_keys       = IDENT
                | "{" IDENT { "," IDENT } [ "," ] "}" ;

(* When `on` is followed by a brace-list, the identifiers are equijoin keys.
   When `on` is followed by a bare identifier, that identifier is an equijoin key.
   When `on` is followed by any other expression (comparison, logical, arithmetic),
   the expression is a non-equijoin predicate (theta join). *)

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

8. **Named arguments.** Within a call's argument list, `IDENT "=" expr` is a
   named argument. The `IDENT` must be immediately followed by a single `=`
   (not `==`). Named arguments may be mixed with positional arguments; all
   positional arguments must precede named arguments. Order of named arguments
   is not semantically significant — built-in functions that accept them
   interpret them by name.

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
| C3 | At most **one** `distinct` clause. |
| C4 | At most **one** `update` clause. |
| C5 | `select`, `distinct`, and `update` are **mutually exclusive**. |
| C6 | At most **one** `order` clause. |
| C7 | At most **one** `by` clause. |
| C8 | At most **one** `window` clause. |
| C9 | `by` requires either `select` or `update`. |
| C10 | `window` requires the operand to be a `TimeFrame`. |
| C11 | At most **one** `rename` clause. |
| C12 | `rename` is mutually compatible with `filter`, `select`, `update`, `distinct`, `order`, and `by`. |
| C13 | At most **one** `melt` clause. |
| C14 | At most **one** `dcast` clause. |
| C15 | `melt` and `dcast` are **mutually exclusive**. |
| C16 | `melt` and `dcast` are mutually exclusive with `distinct`, `update`, `order`, `window`, and `resample`. |
| C17 | `melt` may combine with `select` (to specify measure columns). |
| C18 | `dcast` may combine with `select` (to specify the value column) and `by` (to specify row keys). |

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

**Tuple-LHS assignment in `select`**

When the RHS of an assignment evaluates to a multi-column DataFrame, the
result can be destructured into multiple named columns using a parenthesised
tuple on the left-hand side:

```
trades[select { (x, y) = compute_xy() }]
```

The RHS must return exactly as many columns as there are names in the tuple.
Each name is bound to the corresponding column by position.

The output schema contains exactly the fields listed (including all tuple
names), in the order listed.

**`distinct { field, ... }`**

`distinct` projects the listed fields (same rules as `select`), then removes
duplicate rows. Two rows are considered equal if all projected field values
are equal. The output schema contains exactly the fields listed, in the order
listed.

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

**Tuple-LHS assignment in `update`**

When an expression returns a multi-column DataFrame, multiple new columns can
be added in a single assignment using a tuple on the left-hand side:

```
trades[update { (delta, gamma) = compute_greeks() }]
```

The RHS must return exactly as many columns as there are names in the tuple.
Each name is bound to the corresponding column by position. Mixed regular and
tuple fields are allowed in the same block:

```
trades[update { log_price = log(price), (delta, gamma) = compute_greeks() }]
```

**`update = expr`**

When the RHS is a table-returning expression, the unkeyed form merges **all**
columns of that result into the base DataFrame:

```
prices[update = gen_prices(symbols)]
```

Every column in the result of `gen_prices(symbols)` is added to (or replaces a
column in) `prices`. No column enumeration is required. This is useful when the
set of new columns is determined by the called function rather than statically
known at the call site.

**`rename { new_name = old_name, ... }`**

Renames one or more columns while keeping all other columns intact. Each
mapping is of the form `new_name = old_name`, where `old_name` must be an
existing column and `new_name` is the desired output column name.

Single-rename shorthand (no braces required):

```
trades[rename p = price]
```

Multi-rename form (braces required):

```
trades[rename { p = price, q = volume }]
```

The output schema is identical to the input schema except that the named
columns are relabelled. Column order is preserved; renamed columns remain in
their original positions.

It is a **runtime error** if any `old_name` does not exist in the input
schema. Renaming a column to a name already in the schema is a compile-time
error.

`rename` is compatible with all other clauses. When combined with `select`,
the `rename` is applied first (in canonical evaluation order), so the
`select` clause sees the renamed names:

```
trades[filter price > 15, rename p = price, select p]
```

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

**`order [keys]`**

Sorts rows by one or more keys in ascending order by default. Keys are column
names, optionally followed by `asc` or `desc`. Examples:

```
trades[order symbol]
trades[order { symbol desc, price asc }]
```

If no keys are provided (`order` or `order {}`), all columns in schema order
are used as keys with ascending order.

**Ordering constraints**

An ordering constraint is a property of a DataFrame value, not a distinct type.
The `order` clause **establishes** the ordering constraint to match its key
list. Implementations may exploit ordering for optimized joins, windowing, and
scans, but are not required to do so.

Preservation rules (normative minimums):

- `filter` preserves ordering.
- `select` preserves ordering only if all ordering keys are passed through
  unchanged (no computed replacement) and remain in the output schema.
- `update` preserves ordering only if it does not update any ordering key.
- `distinct`, `by` (aggregation or grouped update), and any `join` drop ordering
  unless the implementation can prove a specific order.
- `order` always sets the ordering constraint to its key list (or schema order
  if no keys are provided).

For `TimeFrame`, the only valid ordering constraint is ascending order on the
time index column. Using `order` with any other key list is a compile-time
error; ordering by the time index is a no-op.

**`window <duration>`**

Specifies a time-based window for rolling computations. Only valid when the
operand is a `TimeFrame`. See Section 8.

**`melt { id_cols }`**

Reshapes a wide DataFrame to long format (unpivot). The fields listed in `melt`
are the **id columns** — they are repeated for each measure column. All columns
*not* listed as id columns become measure columns by default.

The output has three kinds of columns:
1. The id columns (preserved from the input).
2. A `variable` column (`String`) containing the original measure column names.
3. A `value` column containing the corresponding values.

```
// Wide format: symbol, open, high, low, close
ohlc[melt symbol]
// → symbol | variable | value
//   AAPL   | open     | 150.0
//   AAPL   | high     | 155.0
//   AAPL   | low      | 148.0
//   AAPL   | close    | 152.0
//   ...
```

When combined with `select`, the select clause specifies which **measure
columns** to include (instead of using all non-id columns):

```
ohlc[melt symbol, select { open, close }]
// Only unpivots the open and close columns
```

Multiple id columns use the braced form:

```
ohlc[melt { symbol, date }]
```

Null handling: if a measure value is null in the input (validity bitmap is
`false`), the corresponding `value` cell in the output is also null.

**`dcast <pivot_column>`**

Reshapes a long DataFrame to wide format (pivot). The named column is the
**pivot column** — its distinct values become new column names in the output.

The output schema is:
1. The row key columns (specified via `by`, or inferred as all columns except
   the pivot and value columns).
2. One new column for each distinct value in the pivot column, filled with the
   corresponding value column data.

```
// Long format: symbol, variable, value
long[dcast variable, select value, by symbol]
// → symbol | open  | high  | low   | close
//   AAPL   | 150.0 | 155.0 | 148.0 | 152.0
//   ...
```

When `select` is present, it specifies the **value column** (must be a single
bare identifier). When `by` is present, it specifies the **row key columns**.
If `by` is omitted, all columns except the pivot and value columns are used as
row keys.

Missing cells (combinations of row keys and pivot values not present in the
input) are filled with null (validity bitmap set to `false`).

`melt` and `dcast` are inverses:

```
// Roundtrip: wide → long → wide
let wide  = ohlc;
let long  = wide[melt symbol];
let wide2 = long[dcast variable, select value, by symbol];
// wide2 has the same data as wide (column order may differ)
```

### 5.4 Result Type Rules

| Clauses Present       | Output Type | Schema Derivation |
|----------------------|-------------|-------------------|
| `filter` only         | Same as input | Identical schema |
| `select` only         | `DataFrame<S'>` | S' = listed fields |
| `distinct` only       | `DataFrame<S'>` | S' = listed fields |
| `update` only         | `DataFrame<S'>` | S' = S ∪ new fields |
| `rename` only         | Same type as input | S' = S with specified columns relabelled |
| `order` only          | Same as input | Identical schema |
| `select` + `by`       | `DataFrame<S'>` | S' = listed fields (one row per group) |
| `update` + `by`       | `DataFrame<S'>` | S' = S ∪ new fields (same row count) |
| `window` + `update`   | `TimeFrame<S'>` | S' = S ∪ new fields |
| `window` + `select`   | `TimeFrame<S'>` | S' = listed fields |
| `melt`                | `DataFrame<S'>` | S' = id_cols + {variable: String, value: T} |
| `dcast`               | `DataFrame<S'>` | S' = row_keys + one column per pivot value |

If the input is a `TimeFrame` and no clause removes the time index column,
the output remains a `TimeFrame`.

### 5.5 Join Expressions

Join expressions combine two DataFrames or TimeFrames using one or more key
columns (equijoin) or an arbitrary boolean predicate (non-equijoin / theta join).
The supported surface forms are:

```
A join B on key
A left join B on key
A right join B on key
A outer join B on key
A semi join B on key
A anti join B on key
A cross join B
A asof join B on time
A join B on { key1, key2 }
A left join B on { key1, key2 }
A right join B on { key1, key2 }
A outer join B on { key1, key2 }
A semi join B on { key1, key2 }
A anti join B on { key1, key2 }
A asof join B on { time }

(* Non-equijoin / theta join — any comparison or boolean expression *)
A join B on a < b
A left join B on lo <= val && val < hi
A semi join B on a != b
A anti join B on score > threshold
```

Semantics:

- `A join B on key` is an inner join (rows must match on `key`).
- `A left join B on key` is a left outer join (all rows from `A` are preserved).
  Unmatched left rows receive null values for all right-side non-key columns.
- `A right join B on key` is a right outer join (all rows from `B` are preserved).
  Unmatched right rows receive null values for all left-side non-key columns.
- `A outer join B on key` is a full outer join (rows from both sides are preserved).
  Unmatched left rows receive null values for all right-side non-key columns.
  Unmatched right rows receive null values for all left-side non-key columns.
- `A semi join B on key` keeps left rows that have at least one right match.
- `A anti join B on key` keeps left rows that have no right match.
- `A cross join B` returns the Cartesian product of rows from `A` and `B`.
- `A asof join B on time` is an as-of join on the TimeFrame index column.

**Non-equijoin / theta join.** When the expression after `on` is a comparison or
boolean expression (not a bare column name or brace-list), it is treated as a
join predicate rather than an equality key. A pair of rows `(a, b)` is included
in the output when the predicate evaluates to true for that pair. All join
semantics (left outer, semi, anti, etc.) apply — unmatched left rows are
null-padded in the same way as equijoins.

**Column name resolution in predicates.** Inside a non-equijoin predicate,
column names are resolved against the combined output schema — the same schema
that a `cross join` of the two inputs would produce. Right-side column names
that collide with left-side column names receive a `_right` suffix (e.g. if
both sides have column `id`, the predicate would reference `id` for the
left-side value and `id_right` for the right-side value).

Non-equijoin joins use a nested-loop algorithm (O(N×M)) and are therefore
suited for smaller tables or selective predicates. Equijoin hash-join paths
are not used when the `on` clause is a predicate expression.

**Row ordering.** The output preserves the left-table row order for all left-side
rows (both matched and unmatched). For `outer join` and `right join`, unmatched
right-side rows are appended after all left-side rows, in right-table order.

The `on` list must be one or more unqualified column names present in both
input schemas. `asof join` requires both operands to be `TimeFrame`s and the
`on` list must include their shared time index column. Additional columns are
treated as equality keys.

Join expressions are **syntactic sugar** for the built-in join functions:

- `A join B on key` → `inner_join(A, B, key)`
- `A left join B on key` → `left_join(A, B, key)`
- `A right join B on key` → `right_join(A, B, key)`
- `A outer join B on key` → `outer_join(A, B, key)`
- `A semi join B on key` → `semi_join(A, B, key)`
- `A anti join B on key` → `anti_join(A, B, key)`
- `A cross join B` → `cross_join(A, B)`
- `A asof join B on time` → `asof_join(A, B, time, tolerance = 0s)`
- `A join B on { k1, k2 }` → `inner_join(A, B, k1, k2)`
- `A left join B on { k1, k2 }` → `left_join(A, B, k1, k2)`
- `A right join B on { k1, k2 }` → `right_join(A, B, k1, k2)`
- `A outer join B on { k1, k2 }` → `outer_join(A, B, k1, k2)`
- `A semi join B on { k1, k2 }` → `semi_join(A, B, k1, k2)`
- `A anti join B on { k1, k2 }` → `anti_join(A, B, k1, k2)`
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

| Function              | Input              | Output     | Notes |
|-----------------------|--------------------|------------|-------|
| `sum(col)`            | `Series<Numeric>`  | Same numeric type | |
| `mean(col)`           | `Series<Numeric>`  | `Float64`  | |
| `min(col)`            | `Series<T>`        | `T`        | |
| `max(col)`            | `Series<T>`        | `T`        | |
| `count()`             | (none)             | `Int64`    | |
| `first(col)`          | `Series<T>`        | `T`        | |
| `last(col)`           | `Series<T>`        | `T`        | |
| `median(col)`         | `Series<Numeric>`  | `Float64`  | Middle value; null rows are ignored. Even-length groups return the average of the two middle values. |
| `std(col)`            | `Series<Numeric>`  | `Float64`  | Sample standard deviation (denominator n − 1). Returns null for groups with fewer than 2 non-null values. |
| `ewma(col, alpha)`    | `Series<Numeric>`  | `Float64`  | Exponentially weighted moving average. `alpha` ∈ (0, 1] is a numeric literal; rows are processed in storage order within each group. Returns null if the group is empty. |

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

## 8. Inline Table Construction

The `Table { ... }` expression constructs a `DataFrame` from literal column
vectors without requiring an extern data source. It is the primary way to
define small, self-contained tables directly in Ibex source.

### 8.1 Syntax

```
Table { col_name = [ elem, ... ], col_name = [ elem, ... ] }
```

`Table` is a **contextual keyword**: it is only recognised as a constructor
when immediately followed by `{`. It is not reserved and can be used as a
regular binding or function name in other positions.

### 8.2 Column Vectors

Each column is specified as an **array literal** — a comma-separated list of
literal values enclosed in `[ ]`:

```
let t = Table {
    symbol = ["AAPL", "GOOG", "MSFT"],
    price  = [150.0, 140.0, 300.0],
    volume = [1000, 2000, 1500],
    active = [true, false, true],
};
```

Supported element types and their inferred column types:

| Literal kind    | Inferred column type  |
|-----------------|-----------------------|
| Integer (`42`)  | `Int64`               |
| Float (`3.14`)  | `Float64`             |
| Boolean         | `Bool`                |
| String          | `String`              |
| `date "…"`      | `Date`                |
| `ts "…"`        | `Timestamp`           |

### 8.3 Constraints

1. **Uniform element type.** All elements within one array must have the same
   literal kind. Mixing integers and floats, or strings and integers, is a
   lowering error.

2. **Equal column lengths.** Every column in the constructor must have the
   same number of elements. A mismatch is detected at interpret time and
   returns an error.

3. **No duration literals.** Duration values (e.g. `1m`, `30s`) are not valid
   column elements.

### 8.4 Usage

The result is an ordinary `DataFrame` and can be used anywhere a `DataFrame`
is expected — including as the input to `as_timeframe`, joins, and block
operations:

```
// Filter and aggregate inline data
Table { x = [1, 2, 3, 4, 5] }[filter x > 3, select { x }];

// Promote to TimeFrame
let tf = as_timeframe(
    Table { ts = [1000, 2000, 3000], price = [10, 20, 30] },
    "ts"
);

// Join an inline reference table with a loaded DataFrame
let ref = Table { symbol = ["AAPL", "GOOG"], tier = [1, 2] };
prices join ref on symbol;
```

An empty constructor `Table { }` produces a zero-row, zero-column DataFrame.
An array with no elements `col = []` produces a zero-row `Int64` column by
default.

### 8.5 IR Representation

The lowerer converts `Table { ... }` into a `ConstructNode` containing a list
of `ConstructColumn` entries. Each entry holds the column name and a
`std::vector<ir::Literal>`. The interpreter materialises the node into a
`runtime::Table` directly, with no external registry lookup.

---

## 9. TimeFrame Extensions

### 9.1 TimeFrame Construction

A `TimeFrame` is created from a `DataFrame` by designating a `Timestamp`
column as the time index:

```
let tf = as_timeframe(trades, timestamp);
```

The second argument must be an unqualified identifier naming a `Timestamp`
column in the first argument's schema. The resulting `TimeFrame` maintains the
sortedness invariant: rows are ordered by the index column in ascending order.
If the input is not sorted, `as_timeframe` sorts it.

### 9.2 Duration Literals

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

### 9.3 Window Clause

The `window` clause specifies a lookback duration for rolling computations:

```
tf[window 5m, update { avg_5m = rolling_mean(price) }]
```

**Semantics.** For each row at time `t`, the window defines the range
`[t - duration, t]` (inclusive on both ends). Rolling functions operate on the
subset of rows within this range.

**Constraint.** `window` is valid only when the operand is a `TimeFrame`
(constraint C8). Using `window` on a `DataFrame` is a compile-time error.

### 9.4 Temporal Functions

The following functions are available inside `window`-scoped blocks. Rolling
functions require an active `window` clause; `lag` and `lead` do not.

**Rolling aggregates (require `window`):**

| Function                   | Description                                                         |
|----------------------------|---------------------------------------------------------------------|
| `rolling_sum(col)`         | Sum within window                                                   |
| `rolling_mean(col)`        | Mean within window                                                  |
| `rolling_min(col)`         | Minimum within window                                               |
| `rolling_max(col)`         | Maximum within window                                               |
| `rolling_count()`          | Row count within window                                             |
| `rolling_median(col)`      | Median within window (`Float64`); O(n log w) via sliding two-heap  |
| `rolling_std(col)`         | Sample standard deviation within window (`Float64`; 0.0 when fewer than 2 rows) |
| `rolling_ewma(col, alpha)` | EWMA within window (`Float64`); `alpha` is a numeric literal        |

Rolling functions are **aggregate-like**: they produce one scalar per row
(evaluated over the window) and are valid in both `select` and `update`.

**Positional shift (no `window` required):**

| Function           | Description                              |
|--------------------|------------------------------------------|
| `lag(col, n)`      | Value `n` rows before current, by index order |
| `lead(col, n)`     | Value `n` rows after current, by index order  |

`n` must be a non-negative integer literal. If the offset exceeds the available
range, the result is the type's default value (0 for numerics, empty string for
`String`, epoch for `Date` and `Timestamp`).

`lag` and `lead` respect the TimeFrame's index ordering. They are valid in any
TimeFrame block, with or without a `window` clause.

**Cumulative functions (no `window` required):**

| Function        | Description                                                        |
|-----------------|--------------------------------------------------------------------|
| `cumsum(col)`   | Running sum: result[i] = col[0] + col[1] + ... + col[i]           |
| `cumprod(col)`  | Running product: result[i] = col[0] * col[1] * ... * col[i]       |

Both functions accept `Int` or `Float` columns and return the same type as the
input. They are valid in both `select` and `update` blocks (DataFrame or
TimeFrame), with or without a `window` clause.

```
df[select { cs = cumsum(price) }]
df[update { cs = cumsum(price) }]
tf[update { cp = cumprod(returns) }]
```

### 9.5 Sortedness Invariant

A `TimeFrame` guarantees ascending order by its time index at all times. Any
operation that would violate this invariant (e.g., updating the index column)
is a compile-time error.

```
tf[update { timestamp = timestamp + 1s }]  // ERROR: cannot mutate time index
```

This invariant is represented as an ordering constraint on the time index
column in ascending order, preserved across all TimeFrame operations.

---

## 10. User-Defined Functions

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

## 11. Extern Function Interop

### 11.1 Declaration Syntax

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

### 11.2 Parameter Types

Extern function parameters may be any scalar type (`Int`, `Int64`, `Float64`,
`Bool`, `String`, `Date`, `Timestamp`). Series/column parameters are not
supported in the current runtime.

Return types may be scalar or table types (`DataFrame`, `TimeFrame`). Extern
functions cannot return `Series`.

### 11.3 Calling Convention

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

### 11.4 REPL Plugin Loading

When the REPL processes an `extern fn` declaration whose `from` path is
non-empty, it automatically loads the corresponding shared library plugin:

1. The stem of the `from` path is derived (e.g. `"csv.hpp"` → `"csv"`).
2. The REPL searches for `<stem>.so` in each directory on its plugin search
   path (set via `--plugin-path` flag or `IBEX_LIBRARY_PATH` environment
   variable).
3. The first matching file is loaded via `dlopen`.
4. The library's `ibex_register` symbol is resolved and called with the current
   `ExternRegistry*`, registering the function for use in the session.

A compliant plugin exports exactly:

```cpp
extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry);
```

Use `scripts/ibex-plugin-build.sh` to compile a plugin `.cpp` with the correct
flags and include paths for the current build tree.

### 11.4 Restrictions

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

## 12. Built-in Functions and Forms

Ibex keeps built-ins intentionally minimal. Implementations may provide
additional functions, but the recommended path for custom functionality is
`extern fn` interop with C++.

### 12.1 I/O Functions

I/O is provided exclusively via `extern fn` plugins rather than built-ins. A
common example using the bundled CSV plugin:

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
let iris = read_csv("iris.csv");
```

`read_csv` infers column types from the input file (Int64, Float64, or String
per column). The resulting schema is implementation-defined.

The transpiler emits the `from` path as a `#include` in the generated C++.
The REPL loads `<stem>.so` from the plugin search path at the point the
`extern fn` declaration is evaluated (Section 10.4).

### 12.2 Scalar Extraction

```
scalar(df: DataFrame<S>, col: Ident) -> T
```

Extracts a single scalar from a one-row DataFrame. `col` names a column in `S`.
It is a runtime error if the DataFrame has any row count other than 1.

### 12.3 Join Functions

```
inner_join(left: DataFrame<A>, right: DataFrame<B>, key1, ..., keyN) -> DataFrame<A ∪ B>
left_join(left: DataFrame<A>, right: DataFrame<B>, key1, ..., keyN) -> DataFrame<A ∪ B>
right_join(left: DataFrame<A>, right: DataFrame<B>, key1, ..., keyN) -> DataFrame<A ∪ B>
outer_join(left: DataFrame<A>, right: DataFrame<B>, key1, ..., keyN) -> DataFrame<A ∪ B>
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

### 12.4 Ordering

The surface syntax uses the `order` clause (Section 5.3). Implementations may
also provide an equivalent built-in:

```
order(df: DataFrame<S>, key1, ..., keyN) -> DataFrame<S>
```

Returns a new DataFrame sorted by the named keys in ascending order unless
explicit directions are provided in the `order` clause. Sorting a `TimeFrame`
by its index column is a no-op (already sorted).

### 12.5 Display

```
print(value: Any) -> ()
```

Outputs a human-readable representation of the value. In REPL mode, expression
statements are implicitly printed without requiring `print`.

### 12.6 Scalar Functions

These scalar functions are optional and may be provided by the runtime or by
extern implementations. The recommended path for custom scalar logic is
`extern fn`.

| Function        | Signature                          |
|-----------------|------------------------------------|
| `abs(x)`        | `Numeric -> Numeric`               |
| `log(x)`        | `Numeric -> Float64`               |
| `sqrt(x)`       | `Numeric -> Float64`               |
| `year(t)`       | `Date|Timestamp -> Int32`          |
| `month(t)`      | `Date|Timestamp -> Int32`          |
| `day(t)`        | `Date|Timestamp -> Int32`          |
| `hour(t)`       | `Timestamp -> Int32`               |
| `minute(t)`     | `Timestamp -> Int32`               |
| `second(t)`     | `Timestamp -> Int32`               |
| `round(x, mode)`| `Float -> Int64`                   |

`round(x, mode)` converts a `Float64` scalar or `Series<Float64>` to `Int64` /
`Series<Int64>`. The mode is a bare identifier (not a string):

| Mode      | Behaviour                                  | C++ equivalent         |
|-----------|--------------------------------------------|------------------------|
| `nearest` | Round to nearest, ties away from zero      | `std::llround`         |
| `bankers` | Round to nearest, ties to even (banker's)  | `std::llrint`          |
| `floor`   | Round toward −∞                            | `std::floor` + cast    |
| `ceil`    | Round toward +∞                            | `std::ceil`  + cast    |
| `trunc`   | Round toward zero (truncate)               | `std::trunc` + cast    |

`bankers` uses IEEE 754 default rounding (`FE_TONEAREST`): 0.5 rounds to the
nearest even integer, so 2.5 → 2 and 3.5 → 4. This is the statistically
unbiased choice for repeated rounding.

Passing an `Int` or `Int` column is a type error. An unknown mode identifier
is a runtime error.

```
round(3.7, nearest)   // → 4
round(3.7, bankers)   // → 4
round(2.5, nearest)   // → 3  (ties away from zero)
round(2.5, bankers)   // → 2  (ties to even)
round(3.5, bankers)   // → 4  (ties to even)
round(3.7, floor)     // → 3
round(3.7, ceil)      // → 4
round(3.7, trunc)     // → 3
round(-3.7, nearest)  // → -4
round(-3.7, trunc)    // → -3

// Typical use: round a Float column to Int before an explicit cast
prices[update { vol_int = round(volume_f, bankers) }];
```

### 12.7 Vectorized RNG Functions

These built-in functions generate a full column of independent random draws —
one value per row — in a single vectorized pass.  They are valid in `update`
expressions (both plain and windowed) and in `select`.  Arguments must be
numeric literals.

Each call uses a **thread-local `std::mt19937_64`** seeded from
`std::random_device`, so parallel queries on different threads produce
independent streams without locking.

**Continuous distributions (return `Float64`):**

| Function | Parameters | Distribution |
|---|---|---|
| `rand_uniform(low, high)` | `low < high` | Uniform on [low, high) |
| `rand_normal(mean, stddev)` | `stddev > 0` | Normal(mean, stddev²) |
| `rand_student_t(df)` | `df > 0` | Student-t with df degrees of freedom |
| `rand_gamma(shape, scale)` | `shape > 0`, `scale > 0` | Gamma(shape, scale) |
| `rand_exponential(lambda)` | `lambda > 0` | Exponential with rate lambda |

**Discrete distributions (return `Int64`):**

| Function | Parameters | Distribution |
|---|---|---|
| `rand_bernoulli(p)` | `p ∈ [0, 1]` | Bernoulli(p) — yields 0 or 1 |
| `rand_poisson(lambda)` | `lambda > 0` | Poisson(lambda) |
| `rand_int(lo, hi)` | `lo ≤ hi` | Uniform integer on [lo, hi] |

**Examples:**

```
// Add Gaussian noise to a price column
df[update { noisy_price = price + rand_normal(0.0, 0.5) }]

// Simulate a biased coin flip per row
df[update { flip = rand_bernoulli(0.7) }]

// Uniform random weight in [0, 1)
df[update { w = rand_uniform(0.0, 1.0) }]

// Die rolls
df[update { die = rand_int(1, 6) }]
```

**Constraint.** RNG functions are **not** aggregate functions and must not
appear inside aggregate function calls (Section 7.3). Each call produces
exactly one value per row of the current table.

### 12.8 `rep` — Repeat and Fill

```
rep(x, times=1, each=1, length_out=-1)
```

Produces a column by repeating a scalar literal or an existing column. Mirrors
R's `rep()` semantics within the columnar context.

**Parameters:**

| Parameter | Default | Meaning |
|---|---|---|
| `x` | *(required)* | Scalar literal (`Int`, `Float`, `Bool`, `String`) or column reference |
| `times` | `1` | Repeat the whole sequence this many times |
| `each` | `1` | Repeat each individual element this many times before advancing |
| `length_out` | *(row count)* | Final output length; shorter sequences are cycled, longer ones truncated |

`times`, `each`, and `length_out` must be positive integer literals and are
passed as **named arguments**.  When `length_out` is omitted, the output
length equals the number of rows in the current table (the normal case for
`update`/`select`).

**Scalar `x` — constant-fill column:**

```
// Fill every row with the integer 0
df[update { zero = rep(0) }]

// All-true boolean mask
df[update { mask = rep(true) }]

// Constant string tag
df[update { source = rep("live") }]
```

When `x` is a scalar the value of `times` and `each` are redundant (all
repetitions of a scalar produce the same value); only `length_out` affects
the output size.

**Column `x` — element-wise repetition:**

The logical output sequence is built as:
1. Each element of `x` is repeated `each` times.
2. The resulting sequence is repeated `times` times.
3. The sequence is cycled or truncated to `length_out` entries.

```
// Each element repeated twice: [10,10,20,20,30,30] → same row count
df[update { rep2 = rep(price, each=2) }]

// Cycle the first two values across all rows
df[update { flag = rep(flag_col, times=50) }]
```

**Return type:** identical to the type of `x`.  A `Bool` literal produces a
`Bool` column (usable as a boolean mask).

**Constraint.** `rep` is not an aggregate function; it must not appear inside
aggregate function calls (Section 7.3).

---

## 13. Stream Runtime

The Stream runtime connects a **source** extern, an anonymous **transform**
block, and a **sink** extern into a continuous event loop. It is the primary
mechanism for processing real-time data in Ibex.

### 13.1 Syntax

```
let <name> = Stream {
    source    = <source_call>,
    transform = [<clause>, ...],
    sink      = <sink_call>,
};
```

All three fields are required. Field order within the braces is
**semantically insignificant** (mirrors clause ordering in DataFrame blocks).

`Stream` is a **capitalized keyword** and is not a valid user identifier.

### 13.2 Fields

**`source = <extern_call>`**

An extern function call that returns one of three values on each invocation:

| Return value | Meaning |
|---|---|
| `DataFrame` / `TimeFrame` with rows > 0 | A batch of data to process |
| `DataFrame` / `TimeFrame` with rows = 0 | **End-of-stream (EOF)** — the event loop stops |
| `StreamTimeout` | **Receive timeout** — no data arrived; keep listening |

`StreamTimeout` is a C++ sentinel type (`ibex::runtime::StreamTimeout{}`)
that sources return when an internal timeout fires with no data. The event
loop runs the wall-clock bucket flush check and then calls the source again,
so that the closed bucket is delivered promptly even during idle periods.

**Ibex does not buffer data on behalf of the source.** Whether messages
that arrive during the `StreamTimeout` handling window are lost or preserved
depends entirely on the transport:

- **OS-kernel-backed sockets (UDP, TCP):** the kernel maintains a socket
  receive buffer (`SO_RCVBUF`) independently of the application. Datagrams
  arriving while the event loop is processing `StreamTimeout` queue up in the
  kernel buffer and are returned on the next `recvfrom`/`recv` call. No
  application-level buffering is needed. Packets are dropped only if
  `SO_RCVBUF` overflows — a normal UDP property unrelated to `StreamTimeout`.

- **User-space / in-process transports:** if the source reads from a
  user-space queue and there is no independent producer thread, messages
  produced during the `StreamTimeout` window may be lost. The plugin author
  is responsible for arranging OS-level or thread-level buffering so that
  data accumulates safely until the source is called again. For a convenient
  zero-extra-copy solution see `StreamBuffered` (§12.7).

`StreamTimeout` is meaningful only for `TimeBucket` streams; for `PerRow`
streams it is silently ignored.

The source extern must be declared via `extern fn` (Section 10) or loaded
with `import` before the `Stream` expression is evaluated.

```
source = udp_recv(9001)
```

**`transform = [<clause>, ...]`**

An anonymous block (square-bracket clause list) applied to each batch
produced by the source. The transform is applied to a special binding named
`__stream_input__`; users do not reference this name directly. The transform
syntax is identical to a DataFrame block expression — no stream-specific
syntax is needed inside the transform.

```
transform = [resample 1m, select {
    open  = first(price),
    high  = max(price),
    low   = min(price),
    close = last(price)
}]
```

**`sink = <extern_call>`**

An extern function call that consumes the output of the transform. The sink
extern must accept a `DataFrame` or `TimeFrame` as its first argument; the
stream runtime prepends the transform output automatically. Any remaining
arguments after the first are supplied in the `sink` field.

```
sink = udp_send("127.0.0.1", 9002)
```

### 13.3 Stream Kinds

The compiler infers how and when to emit output from the transform IR.

| Kind         | Trigger condition                                        | IR presence                    |
|--------------|----------------------------------------------------------|--------------------------------|
| `PerRow`     | Emit one output row for every incoming row               | No `ResampleNode` in transform |
| `TimeBucket` | Buffer rows; emit when the wall-clock bucket end passes  | `ResampleNode` in transform    |

The stream kind is **never specified by the user**; it is derived
automatically during lowering. `TimeBucket` is selected when the transform
contains a `resample` clause; otherwise `PerRow` is used.

### 13.4 Plugin Loading with `import`

Extern functions used as stream sources or sinks are typically provided by
plugins. The `import` statement loads a named `.ibex` library stub (which
itself contains `extern fn` declarations) from the import search path:

```
import "udp";
```

This locates `udp.ibex` on the import search path, parses its `extern fn`
declarations, and loads the corresponding shared library (e.g. `udp.so`) via
the plugin mechanism (Section 10.4). The `import` statement is shorthand for
manually writing a series of `extern fn` declarations.

`import` accepts either a quoted string or a bare identifier:

```
import "udp";      // searches for udp.ibex
import udp;        // equivalent
```

### 13.5 Execution Model

The REPL **blocks** in the Stream event loop until the source signals
end-of-stream (returns an empty DataFrame). There is currently no mechanism
to run multiple streams concurrently within a single session. The event loop:

1. Calls the source extern to obtain a batch.
2. If the batch is empty, stops and returns.
3. For `PerRow` streams: applies the transform to the batch and passes each
   output row to the sink.
4. For `TimeBucket` streams:
   a. Checks whether the open bucket's wall-clock duration has elapsed (see
      §12.5.1). If so, flushes the buffer immediately — before processing
      the new batch.
   b. Appends the batch to an internal buffer row-by-row. Whenever a row's
      data timestamp falls in a later bucket than the currently open one,
      the buffer is flushed and a new bucket is started.
   c. On end-of-stream, any remaining buffered rows are flushed.

### 13.5.1 TimeBucket Flush Timing

A `TimeBucket` stream has **two independent flush triggers**:

**Wall-clock trigger (primary for real-time sources)**

When the first row of a new bucket arrives, the runtime records the current
`CLOCK_REALTIME` instant as `bucket_open_wall`. At the top of each
subsequent source call, it checks:

```
wall_now() − bucket_open_wall ≥ bucket_duration
```

If true, the buffer is flushed immediately — before the new batch is even
inspected. This delivers the closed bucket at (approximately) the moment the
frame expires on the real-time clock, not when the next message happens to
carry a future timestamp.

**Data-timestamp trigger (fallback for historical replay)**

Within each batch, rows are inspected one-by-one. When a row's floored
timestamp is greater than the open bucket's floor, the buffer is flushed and
the new bucket is opened. This trigger handles replayed or historical data
where wall-clock time bears no relation to the timestamps in the data.

Both triggers can fire independently; whichever fires first wins.

#### Timing caveats

- **Source blocking.** The wall-clock check runs only when the source call
  *returns*. A source that blocks indefinitely — e.g. a UDP `recvfrom` with
  no timeout — prevents the check from firing until the next message arrives.
  The recommended pattern is to set a short socket timeout and return
  `StreamTimeout{}` when it fires. For kernel-backed sockets this is safe:
  the OS buffers arriving packets in `SO_RCVBUF` while the event loop is
  processing `StreamTimeout`, so no data is lost. For user-space transports
  the plugin author must ensure independent buffering (see §12.2).

- **Clock vs. data time.** The wall-clock trigger measures elapsed time since
  the bucket was opened on the real-time clock. For historical replay the
  bucket is opened, the entire replay completes in milliseconds, and the
  wall-clock duration never reaches `bucket_duration`; the data-timestamp
  trigger handles replay correctly without any special configuration.

- **Emission timing jitter.** The wall-clock check is opportunistic — it
  fires at the next source return after the deadline, not at the deadline
  itself. For a source using `StreamTimeout`, jitter is bounded by the socket
  timeout duration (typically a few milliseconds). For a blocking source,
  jitter can be up to one full inter-message interval.

### 13.6 Example

The following example reads tick data from UDP port 9001, resamples it into
1-minute OHLC bars, and forwards the bars to UDP port 9002:

```
import "udp";

let ohlc_stream = Stream {
    source    = udp_recv(9001),
    transform = [resample 1m, select {
        open  = first(price),
        high  = max(price),
        low   = min(price),
        close = last(price)
    }],
    sink = udp_send("127.0.0.1", 9002)
};
```

The `resample 1m` in the transform causes the stream kind to be inferred as
`TimeBucket`. Each 1-minute OHLC bar is emitted approximately one minute
after the first tick in that bar is received (wall-clock trigger), or
immediately when the first tick belonging to the next minute arrives
(data-timestamp trigger) — whichever occurs first. The REPL blocks until
`udp_recv` returns an empty DataFrame (the sender signals end-of-stream).

**Note on `udp_recv` timeout:** for prompt end-of-bucket delivery, the
`udp_recv` implementation should use a short socket receive timeout (e.g.
5–10 ms) and return `StreamTimeout{}` when it fires with no data. This allows
the wall-clock check to fire close to the bucket boundary while the source
stays live and misses no messages. A `udp_recv` that blocks indefinitely
delays emission until the next tick arrives.

### 13.7 StreamBuffered: Ready-Made Producer Queue for In-Process Sources

For user-space / in-process transports the runtime provides
`ibex::runtime::StreamBuffered` — a helper that combines an SPSC
(single-producer, single-consumer) ring buffer with a compatible `ExternFn`,
so the plugin author does not need to implement their own thread-safe queue.

> **Note:** Ibex still maintains its own internal TimeBucket accumulation buffer.
> `StreamBuffered` provides the producer-side queue that feeds that buffer — it
> replaces whatever ad-hoc buffering the plugin would otherwise need.

#### How it works

1. The plugin registers a source using `make_buffered_source(producer_fn)`.
2. When the Ibex event loop first calls the source it reads the ring capacity
   from the first Int argument, initialises `StreamBuffered(capacity)`, and
   launches the producer callback in a detached thread.
3. The **producer thread** pushes batches via `buf.write(table)` and signals
   completion with `buf.close()`.
4. On every subsequent call the **event loop** drains the ring:
   - Ring non-empty → returns the next `Table` (rows > 0).
   - Ring empty, not closed → returns `StreamTimeout{}` so the wall-clock
     bucket flush can fire without blocking the consumer.
   - Ring empty, closed → returns an empty `Table` (EOF) to stop the loop.

#### API summary

There are two usage styles depending on whether capacity should come from
the Ibex query or from C++.

**Preferred — capacity from the Ibex query (`make_buffered_source`)**

```cpp
#include <ibex/runtime/stream_buffered.hpp>

// C++ plugin: only the data-production logic, no capacity decision.
registry.register_table("my_src",
    ibex::runtime::make_buffered_source([](ibex::runtime::StreamBuffered& buf) {
        for (auto& batch : my_data_source) buf.write(batch);
        buf.close();
    }));
```

```
// Ibex query: capacity is a tuning parameter alongside resample, filters, etc.
extern fn my_src(capacity: Int) -> TimeFrame from "plugin.hpp";
Stream {
    source    = my_src(512),
    transform = [resample 1s, select { close = last(price) }],
    sink      = my_sink()
};
```

**Manual — capacity chosen in C++ (`StreamBuffered` directly)**

```cpp
// Use when the plugin owns the producer lifecycle explicitly.
auto buf = std::make_shared<ibex::runtime::StreamBuffered>(/*capacity=*/256);
registry.register_table("my_src", buf->make_source_fn());

std::thread producer([buf] {
    buf->write(my_table);   // blocks (yields) if ring is full
    buf->close();
});
```

#### Comparison with kernel-backed sockets

| Property | `StreamBuffered` | UDP with SO_RCVTIMEO |
|---|---|---|
| Producer-side buffer | User-space SPSC ring (provided) | Kernel socket buffer (OS-managed) |
| Plugin author writes own queue | No | No |
| Overflow behaviour | Producer blocks (backpressure) | Kernel drops datagrams |
| Suitable for | In-process queues, shared memory | Network sources |

For kernel-backed transports (UDP/TCP), use the `StreamTimeout{}` pattern
directly without `StreamBuffered` — the kernel already provides an equivalent
ring buffer at no extra cost.

#### Concurrency guarantees

`write()` and `close()` must be called from a single producer thread.
The `ExternFn` returned by `make_source_fn()` is called only from the Ibex
event loop (single consumer).  The two sides are separated by cache-line-
aligned atomic indices so no mutex is required on the hot path.

---

## 14. Minimal Complete Example

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
| 0     | Join         | `join` `left join` `right join` `outer join` `semi join` `anti join` `cross join` `asof join` | Left   |

---

## Appendix B: Reserved Words

**Hard keywords** (always reserved, used by grammar):

```
let  mut  extern  fn  from  filter  select  update  distinct  order  by  window  join  left  right  outer  semi  anti  cross  asof  on
rename  resample  import  Stream
asc  desc  true  false
```

**Type keywords** (reserved in type position and as identifiers):

```
Int  Int32  Int64  Float32  Float64  Bool  String  Date  Timestamp
Column  Series  DataFrame  TimeFrame
```

**Soft-reserved** (cannot be shadowed by user bindings):

```
scalar
date  timestamp  ts
sum  mean  min  max  count  first  last  median  std  ewma
```

---

## Appendix C: Grammar Railroad Summary

For implementors. The core parsing loop:

```
parse_statement:
    "let"    → let_stmt
    "fn"     → fn_decl
    "extern" → extern_decl
    "import" → import_decl
    IDENT    → peek "=" (not "==") → assign_stmt
             → otherwise → expr_stmt

parse_expr (Pratt):
    NUD: IDENT, "^" IDENT, literal, schema_lit, "(", unary_op, "Stream" → stream_expr
    LED: binary_op, join_form, "[" (block), "(" (call — only after IDENT NUD)

parse_clause:
    "filter"   → filter_clause
    "select"   → select_clause
    "update"   → update_clause
    "rename"   → peek "{" → braced rename list (new_name "=" old_name, ...)
               → otherwise → single rename (new_name "=" old_name)
    "resample" → DURATION_LIT
    "by"       → peek "{" → braced field list
               → otherwise → single IDENT
    "window"   → DURATION_LIT
    "melt"     → peek "{" → braced field list (id columns)
               → otherwise → single IDENT (single id column)
    "dcast"    → IDENT (pivot column)

import_decl:
    "import" (STRING_LIT | IDENT) ";"

stream_expr:
    "Stream" "{" stream_field ("," stream_field)* [","] "}"

stream_field:
    "source"    "=" expr
    "transform" "=" "[" clause ("," clause)* [","] "]"
    "sink"      "=" expr

join_form:
    expr "join" expr "on" IDENT
    expr "left" "join" expr "on" IDENT
    expr "asof" "join" expr "on" IDENT
```

The parser is fully deterministic with 1-token lookahead after consuming the
leading token of each production.
