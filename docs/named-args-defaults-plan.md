# Named Arguments And Default Parameters

## Goal

Make function calls, especially `extern fn` calls such as `read_csv(...)`,
more readable without adding a large amount of language machinery.

The intended user-facing shape is:

```ibex
import "csv";

let trades = read_csv("trades.csv");

let wide = read_csv(
    "measurements.txt",
    nulls = "",
    delimiter = ";",
    has_header = false
);
```

The design should stay strict:

- positional arguments must come first
- named arguments must match declared parameter names exactly
- duplicate binding of the same parameter is a compile-time error
- unknown names are a compile-time error
- defaults should be simple and predictable, not “magical”

## Why

The current function surface becomes wordy quickly for I/O and option-heavy
APIs. `read_csv(...)` is the clearest example:

```ibex
read_csv("examples/measurements.txt", "", ";", false)
```

This is compact, but the meaning of the later arguments is not obvious at the
call site. Named arguments solve the readability problem directly:

```ibex
read_csv("examples/measurements.txt", nulls = "", delimiter = ";", has_header = false)
```

Default parameter values then let the common case stay short:

```ibex
read_csv("prices.csv")
```

## Current State

Some of the plumbing already exists:

- the grammar already accepts `IDENT = expr` inside call argument lists
- the spec already documents named arguments in the grammar
- the built-in `rep(...)` already interprets named arguments

But this is not yet a general function feature. In practice:

- normal `fn` and `extern fn` calls are still effectively positional
- default parameter values are not available
- spec wording is broader than the implementation

## Scope

Build this in two stages.

### Stage 1: general named arguments

Support named arguments uniformly for:

- user-defined `fn`
- `extern fn`
- built-ins that already use call syntax

Example:

```ibex
extern fn read_csv(
    path: String,
    nulls: String,
    delimiter: String,
    has_header: Bool
) -> DataFrame;

let df = read_csv("examples/measurements.txt", delimiter = ";", has_header = false, nulls = "");
```

### Stage 2: default parameter values

Allow defaults in function declarations:

```ibex
extern fn read_csv(
    path: String,
    nulls: String = "",
    delimiter: String = ",",
    has_header: Bool = true
) -> DataFrame;
```

and:

```ibex
fn top_n(df: DataFrame<{salary: Int64}>, n: Int64 = 3) -> DataFrame {
    df[distinct { salary }, order { salary desc }, head n]
}
```

## Semantics

### Named arguments

Keep the rules narrow and explicit:

1. Positional arguments must precede named arguments.
2. Once a named argument appears, later positional arguments are forbidden.
3. A parameter may be bound at most once.
4. Unknown argument names are compile-time errors.
5. Arity checking happens after matching positional and named arguments.
6. Reordering by name is allowed only among named arguments.

Valid:

```ibex
read_csv("x.csv", delimiter = ";", has_header = false)
read_csv(path = "x.csv", delimiter = ";", has_header = false)
```

Invalid:

```ibex
read_csv(delimiter = ";", "x.csv")      // positional after named
read_csv("x.csv", delimiter = ";", delimiter = ",")  // duplicate
read_csv("x.csv", sep = ";")            // unknown name
```

### Default parameters

Defaults should be deliberately simple:

1. Defaults are attached to trailing parameters only.
2. A required parameter cannot follow a parameter with a default.
3. A call may omit only trailing parameters, unless later parameters are
   provided by name.
4. Defaults are evaluated in declaration order.
5. V1 should restrict defaults to closed expressions with no dependence on the
   current input table.

This keeps the model unsurprising and easy to typecheck.

## Recommendation On Default Expressions

Use a deliberately conservative rule first:

- allow literals
- allow scalar-only expressions
- allow references to earlier parameters
- disallow references to table columns or the ambient row context

Good:

```ibex
fn f(x: Int64, y: Int64 = x + 1) -> Int64 { y }
extern fn read_csv(path: String, delimiter: String = ",") -> DataFrame;
```

Not in V1:

```ibex
fn bad(x: Int64 = price) -> Int64 { x }   // refers to row context
```

## Parser And AST Work

### Named arguments

The parser already recognizes `IDENT = expr` in call argument lists. The main
remaining task is to make resolution and validation uniform across all call
sites rather than only ad hoc built-ins.

### Default parameters

Parameter declarations need to grow from:

```text
name: Type
```

to:

```text
name: Type = expr
```

for both `fn` and `extern fn`.

## Typechecking And Resolution

Add a dedicated call-binding step:

1. collect declared parameters in order
2. assign positional arguments from left to right
3. assign named arguments by declared name
4. fill remaining unbound parameters from defaults
5. fail if any required parameter remains unbound
6. typecheck each bound/defaulted expression against the parameter type

This logic should be shared by:

- REPL/runtime function invocation
- interpreter extern calls
- codegen

## Runtime And Codegen

Prefer to lower calls after argument binding has already been normalized.

That is, once a call has been checked, internal execution should see a
canonical positional argument vector in declared parameter order, with any
defaults already filled in. This keeps:

- interpreter call code simpler
- extern invocation simpler
- codegen behavior aligned with runtime behavior

It also avoids carrying “named argument” complexity throughout the backend.

## Spec Changes Needed

When implemented, update:

- `SPEC.md`
  - function parameter grammar
  - named-argument semantics
  - default-parameter semantics
  - examples for `fn` and `extern fn`
- `docs/index.html`
  - short call-site example
  - I/O example using named args
- `docs/functions.html`
  - if function-call syntax examples are shown there

The current spec should also be tightened so it does not imply broader
named-argument support than the implementation actually has.

## Suggested Rollout

1. Generalize named argument binding for all calls.
2. Add tests for strict call validation.
3. Add named-argument examples for `read_csv(...)`.
4. Add default values for `fn` and `extern fn`.
5. Update bundled examples to use named arguments where readability improves.

## Initial Test Cases

### Named arguments

- all-positional call still works
- mixed positional + named call works
- all-named call works
- duplicate named argument fails
- unknown named argument fails
- positional after named fails
- missing required argument fails

### Default parameters

- omitted trailing argument uses default
- multiple omitted trailing arguments use defaults
- explicit named argument overrides default
- earlier-parameter reference in default works
- non-trailing required parameter after default is rejected

## First Targets

The first APIs that should benefit are:

- `read_csv(...)`
- `read_parquet(...)` and similar I/O functions
- reusable user-defined helpers like `nth_highest_salary(...)`

`read_csv(...)` is the clearest motivating example because it combines:

- one obviously required parameter (`path`)
- several optional configuration parameters
- high call-site frequency in examples and notebooks

