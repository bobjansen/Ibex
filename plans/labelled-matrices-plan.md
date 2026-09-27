# Labelled matrices

Status: proposed, branch `matrix` (2026-09-27). No implementation yet. This plan
fixes the semantics first because the representation choice is load-bearing and
hard to reverse once `Matrix` is a value kind users can name.

## Purpose

Ibex's matrix support is three clauses (`cov`, `corr`, `transpose`) plus one
function (`matmul`), and all four lose the information that makes a matrix
readable. `matmul_table` (`src/runtime/reshape.cpp:332`) drops every row label,
names its output columns after the *right* operand's numeric columns, and checks
only that the inner arity agrees. Two matrices whose columns are in different
orders multiply cleanly and produce a wrong answer with plausible magnitudes.

The proposal: a matrix carries a **label frame per axis** — an ordered, typed,
key-unique `DataFrame` with one row per matrix row (resp. column) — and every
matrix operation propagates and checks them. This generalises R's `dimnames`
from a character vector per dimension to a frame per dimension, which is what
makes composite labels (`(date, symbol)`) and a total `transpose` possible at
all.

The payoff is not ergonomics. It is that the most expensive class of bug in
quantitative code — a filter applied to `X` but not to `y`, a factor loading
matrix multiplied in the wrong orientation, a covariance matrix whose asset
order drifted from the weight vector's — becomes an error at the contraction
instead of a number nobody can audit.

## Scope

In: real (`Float64`) matrices; two axes; label frames with arbitrary non-numeric
label columns; runtime label-schema and label-extent checking;
expression-level transpose and multiply; `chol`/`solve`/`diag`/`inv` over a
factorization; a `design` constructor that reuses the existing formula
machinery; `as_frame` back to the relational world.

Out (deliberately, this slice): semirings other than $(+,\times)$ over the
reals; sparse storage; more than two axes; complex values; batch axes via `by`;
element types other than `Float64`; `Matrix` as an I/O format.

Out (permanently): implicit inner/outer joins, including filling or discarding
unmatched labels. Exact matching label sets may be reordered to each other's
axis order during an operation; missing or extra labels are a runtime error.

## Current state

| Thing | Where | What it does now |
|---|---|---|
| `cov` / `corr` | SPEC §5.3 | N×N plus a leading `column: String` label column; non-numerics silently dropped, Int64 widened |
| `transpose` | `src/runtime/reshape.cpp:227` | Clause, not an expression. Requires homogeneous data columns; takes *one* optional String/Categorical label column; synthesises `r0`, `r1`, … otherwise. Already works on non-numeric element types |
| `matmul` | `src/runtime/reshape.cpp:332` | Naive `vector<vector<double>>` triple loop, no BLAS, no parallelism. Labels discarded |
| `model {}` | SPEC §5.3 | Formula → design matrix → Cholesky OLS, all internal. `model_coef` returns `{term: String, estimate: Float64}` |

Two observations that shape the plan:

1. **`transpose` already admits non-numeric element types.** The type system is
   not the obstacle to generalising element types; only `matmul` is numeric-only.
   Element-type generality is therefore a later, separable slice (see
   *Semirings*).
2. **`model_coef`'s `{term, estimate}` table is already a labelled vector**,
   written relationally. `model_summary`'s five columns are five labelled vectors
   joined on `term`. The matrix layer is not a new concept beside the model
   machinery; it is the thing the model machinery currently fakes.

## Representation

A `Matrix` is a dense column-major `Float64` block plus two label frames and a
transpose flag. It is a refinement of a materialized table in exactly the sense
a `TimeFrame` is: extra metadata, extra invariants, restricted operations. The
precedent to follow is `TableProperties`
(`include/ibex/runtime/table_properties.hpp`), including its standing rule that
an operator which moves rows must not blindly copy the metadata through.

- **Label frame.** A `DataFrame` with `nrow` = that axis's extent, whose columns
  are the axis's label columns. Column *names* are the axis names and carry
  semantics: contraction matches on them. Types are the column types, so a
  `Date` label stays a `Date`.
- **Transpose flag.** A bool per matrix. `X'` swaps the label frames and flips
  the flag; no data moves. The flag feeds `dgemm`'s `TRANSA`/`TRANSB`, so
  `X' @ X` never materializes `X'`.
- **Non-empty axes.** Both present axes have positive extent. An `0×N` or
  `M×0` matrix is invalid, including the result of a row or column filter that
  selects no labels. Construction and slicing report a runtime error. A
  one-row or one-column result remains a matrix; a `1×1` matrix is not a scalar.
  No singleton dimension is implicitly dropped.
- **Vector = absent second axis.** A vector is a `Matrix` whose column label
  frame has *no columns and no rows*, not one with a single column. An absent
  axis is not a degenerate axis. This is deliberate: R's `drop = TRUE` is a
  perennial bug source precisely because a length-one axis sometimes vanishes
  and sometimes does not.
- **Scalar** has both axes absent and is a distinct scalar value. A `1×1`
  matrix still has two present axes and retains both labels. A contraction whose
  result has no free axes (for example, `w' @ S @ w`) produces a scalar; a
  `1×1` matrix requires explicit scalar extraction if its value is wanted.

Rejected alternative — **long/COO as the canonical form** (row-label columns,
column-label columns, value). It makes labels symmetric by construction and the
contraction-as-join framing native, and it was the first proposal. It loses on
two counts that matter here. Financial matrices are dense: a 3000-asset
covariance matrix becomes 9M rows of repeated keys to carry 9M doubles.
And the two axes of `X' @ X` land in one flat record, so both are named `term`
and must be distinguished by a naming convention (`term_i` / `term_j`) carried
through every operator. With a frame per axis the roles are structural and that
problem does not exist. Long form stays available as the `as_frame` output shape
and as the natural sparse representation if sparsity is ever wanted.

Rejected alternative — **serialising composite labels into column names**
(`"AAPL|2024-01-02"`). This is what the current wide representation would force,
and it is why `transpose` today takes only a single label column. Lossy, needs
escaping, and produces a data-dependent schema.

## Type rules

**Axis compatibility is checked at runtime, by design.** A matrix is a strongly
typed `Matrix` value with `Float64` elements and runtime descriptors for each
axis: the label column names, their column types, and their ordered values.
Those descriptors travel with the matrix and constructors validate them. The
compiler does not try to prove that two matrices have compatible axis schemas
or labels.

This is necessary for ordinary data-driven code: a matrix can be built from a
selection such as `select { date, symbol, price }`, and its labels and extents
come from data that is only available at runtime. Static axis types would make
those matrices awkward to use and would not prove that their runtime labels
match anyway. The safety guarantee is instead that every operation which
requires compatible axes checks them before computing and reports a runtime
error on mismatch; incompatible labels must never silently produce a result.

The language type system may still check ordinary expression and element types
where those are statically known. That is separate from axis compatibility and
must not be presented as a prerequisite for using `Matrix`.

| Expression | Type | Axis checks |
|---|---|---|
| `X'` | `Matrix` | swaps the runtime axis descriptors; free |
| `A @ B` | `Matrix` | runtime inner-axis schema and label compatibility |
| `A + B`, `A - B`, `A * B` (Hadamard) | `Matrix` | runtime checks for both axis schemas and labels |
| `A @ v` where `v` is a vector `Matrix` | `Matrix` | as `@` |
| `diag(A)` | `Matrix` vector | runtime check that the two axes are compatible |
| `chol(A)` | `Chol` | runtime axis compatibility and positive-definiteness |
| `solve(Chol, B)` | `Matrix` | runtime check that the factorization's axis matches `B`'s row axis |
| `inv(Chol)` | `Matrix` | carries the factorization's axis descriptor on both axes |
| `nrow(A)`, `ncol(A)` | `Int64` | — |
| `as_frame(A)` | `DataFrame` | runtime output built from the axis descriptors |

For a contraction, the inner axis schemas must have the same label column names
and types (column order within a composite label is immaterial), and the label
tuples must satisfy the alignment rule below. For pointwise operations and
factorizations, corresponding axes must satisfy the same runtime schema and
label checks. For example, `{symbol: String}` against `{factor: String}`, or
`{symbol: String}` against `{symbol: Categorical}`, produces a runtime error,
not a compile error. Any explicit conversion needed to reconcile label types is
also performed before the operation.

This design deliberately accepts runtime errors for incompatible axes. The
alternative is to make axis schemas part of static matrix types and require the
compiler to prove compatibility, which does not fit axes selected from runtime
data. Runtime validation is the contract, not a temporary fallback for missing
static analysis.

## Syntax

Two new operators, both currently unclaimed in `src/parser/lexer.cpp`
(`^` is the scope-escape operator, `~` is formula syntax, `` ` `` is quoted
identifiers, `%` is modulo):

- **`@`** — matrix multiply. Precedence at `*`. PEP 465 precedent.
- **`'`** — postfix transpose. Binds tightest. Unambiguous in the Pratt parser;
  note it forecloses single-quoted strings and character literals forever, which
  is the cost to accept knowingly.

Constructors are functions, not clauses:

```
let X = design(salaries, { 1 + age + educ + gender }, rows = person_id);
let y = vector(salaries, salary, rows = person_id);

let f     = chol(X' @ X);
let beta  = solve(f, X' @ y);
let resid = y - X @ beta;
let s2    = sum(resid * resid) / (nrow(X) - nrow(beta));
let se    = sqrt(diag(inv(f)) * s2);
```

`design` reuses the formula machinery `model {}` already has — intercept,
treatment coding, interactions, `- 1` — but yields a labelled matrix instead of
a `ModelResult`. Its column labels are `{term: String}`, the same strings
`model_coef` already produces. Note that the dummy expansion of `gender`
produces labels that are *data values* (`gender=F`), not identifiers knowable at
parse time; this is the ordinary case in regression and it is the direct
argument for labels living in a frame rather than in the schema.

**Where the boundary sits:** brackets stay relational and row-oriented;
linear algebra is expression-level. `design`/`vector`/`as_matrix` and `as_frame`
are the airlocks. Forcing `X' @ X` into a `[ ]` block would grow a second,
parallel clause vocabulary, and we should not.

`transpose` the clause becomes sugar for `'` rather than a separate
implementation. `matmul(a, b)` stays as the function spelling of `@`.

### Label-based slicing

Matrix slicing filters an axis's label frame and gathers the corresponding
rows or columns of the dense block. `filter` addresses row labels and
`filter_col` addresses column labels; predicates run against the selected
axis's label columns, not the numeric matrix values. For example:

```ibex
let one_symbol = X[filter symbol == "AAPL"];
let block = X[
  filter date >= start_date,
  filter_col symbol in selected_symbols
];
```

Slicing preserves the schemas and order of retained labels, and cannot create
duplicate or null labels. A selection that leaves either present axis empty is
a runtime error under the non-empty-axis rule above. Singleton axes remain
present, so a `1×1` result is still a matrix. The implementation may use a view
when the selected positions permit it, or gather into owned storage otherwise;
that storage choice does not change the observable labels or shape.

## Contraction rule

Stated generally, because it is what the design should be tested against even
though this slice only implements the two-axis case: for a binary contraction,
an axis name present in both operands is **summed over**; an axis name present in
one is **free** and carried to the output. (A third case — shared and *kept* —
is a batch axis; out of scope, but the rule should not be written in a way that
forecloses it, since per-date covariance via `by` is the obvious next ask.)

`X' @ X` is the case where the same axis name appears on both output axes. With
a frame per axis that is representable without renaming: both are `{term}`, in
different slots.

## Alignment

Contraction requires the inner label frames to have the same schema and the same
unique label tuples. Their tuple order may differ; in that case the operation
reorders one operand to match the other before computing.

- **Fast path:** intern label frames (hash-cons on construction). Matrices
  derived from the same source share a frame by pointer, so the check is a
  pointer comparison and the `dgemm` call needs no permutation. This is the
  common case and it must cost nothing.
- **Slow path:** equal as sets but not as sequences — build a permutation from
  the exact matching label tuples, permute once, then `dgemm`. This is an exact
  reordering, not a join: no labels are added or discarded.
- **Mismatch:** error by default, naming the axis, the count on each side, and
  up to three labels present on one side only. Explicit opt-in modifiers
  `align inner` / `align outer fill 0` are a follow-up, not this slice.

Labels must be Categorical-encoded internally, so the extent comparison is over
dictionary codes rather than strings.

## Nulls, duplicates, determinism

Three decisions, all following `as_timeframe`'s precedent that a null index is
rejected at construction rather than given a meaning ("a null has no position in
time", SPEC §9.1):

- **A null label is a construction error.** A null has no identity, so it cannot
  name a row. Same argument, same place to enforce it.
- **A duplicate label tuple is a construction error.** Key-uniqueness on the
  label frame. COO semantics would say duplicates sum; that is mathematically
  defensible and operationally astonishing.
- **A null *value* in the block is a construction error.** Matrices are total;
  absent means zero and is filled at construction. Linear algebra over
  three-valued logic has no good answer and every library that has tried it
  regrets it. This is the one place the matrix type is stricter than a
  `DataFrame` and the error message should say why.

Determinism: the contraction order over `k` is fixed by the inner label frame's
order, and the fast path preserves the operand's own order rather than
canonicalising. Two runs of the same script produce byte-identical results;
reordering a source's rows may not, and that is stated, not hidden.

## Numerics

`solve(chol(X' @ X), X' @ y)` forms the normal equations and squares
$\kappa(X)$. This matches what `model { …, method = ols }` already does, so the
plan does not regress anything — but the flagship example in the docs should be
the QR path. Ship `lstsq(X, y)` alongside and use *it* in
`examples/` and SPEC, with the Cholesky route shown as the explicit
normal-equations alternative. Otherwise the first reader with a numerical
background files an issue, correctly.

## Performance

The whole point of the dense representation is that labels are a planning-time
concern and the inner loop is BLAS. Concretely:

- `X' @ X` must lower to one `dgemm` with `TRANSA = 'T'`, no materialized
  transpose, no copy.
- The current triple loop in `matmul_table` is the baseline to beat. Measure it
  before touching it (`benchmarking/compare_ibex_git.sh`, release build only —
  see AGENTS.md), and record the multiple: it is a `vector<vector<double>>`
  scatter with no blocking, so a two-orders-of-magnitude improvement on large
  operands is plausible and should be verified rather than assumed.
- Which BLAS: decide between a vendored reference kernel and linking an external
  BLAS. An external dependency conflicts with the FetchContent-only build; a
  hand-written blocked kernel is a real cost. This is the largest open question
  in the plan and it should be answered with a measurement, not a preference.

## Step checklist

1. [ ] `include/ibex/runtime/matrix.hpp`: the value type — block, two label
   frames, transpose flag, interning. Construction validators (empty axis, null
   label, null value, duplicate key). Unit tests for every validator's message.
2. [ ] Runtime value and operator checks: `Matrix` axis descriptors and
   `Chol` descriptors; runtime schema and label compatibility checks with
   clear errors. Tests cover compatible axes and mismatches (name, type,
   extent, labels) for contractions, pointwise operations, and factorizations.
3. [ ] Parser: `@` infix, `'` postfix, precedence. **Full `ctest`, not
   `-LE slow`** (AGENTS.md: parser/lexer/AST changes).
4. [ ] Kernels: `@` via `dgemm`-shaped path plus the interning fast path and the
   permutation slow path; Hadamard, `+`, `-`, scalar scaling; `diag`.
   A/B against the existing `matmul_table` on the release build.
5. [ ] Factorizations: `chol`, `solve`, `inv(Chol)`, `lstsq` (QR).
6. [ ] Constructors and slicing: `as_matrix`, `vector`, `design` (reusing the
   formula expansion), `as_frame`, row `filter` and column `filter_col`.
   `transpose` clause redirected to `'`.
7. [ ] Conformance: reimplement `model { y ~ …, method = ols }` on the matrix
   primitives and check byte-identity of `model_coef`/`model_summary` against
   the current Cholesky path on `examples/regression.ibex`. If the matrix layer
   cannot reproduce the existing API exactly it is not general enough yet. This
   is the plan's real acceptance test.
8. [ ] Docs and examples together, per AGENTS.md: SPEC §5.3 rewritten,
   `docs/index.html` in sync, `examples/matrix_ols.ibex` plus a
   labelled-factor-model example ($\Sigma = BFB^\top + D$). Rebuild plugins if
   public headers moved (`scripts/ibex-plugin-build.sh`).

## Follow-ups (not in this slice)

- `align inner` / `align outer fill 0` modifiers on `@`.
- Batch axes: `by date` over a contraction, i.e. the shared-and-kept case of the
  contraction rule. Needs the three-axis question answered first.
- Sparse storage behind the same type. The label frames are already orthogonal
  to how the block is stored, which is the reason to prefer this representation
  over COO rather than an argument against sparsity.
- Semirings. `matmul` parameterised by $(\oplus, \otimes)$ gives $(\min,+)$ for
  best-path FX conversion over a currency-labelled matrix, boolean closure for
  transitive counterparty exposure, and counting for settlement graphs. Two hard
  constraints to write down before anyone starts: $\oplus$ must be commutative
  or the reduction over `k` stops being reassociable and the columnar engine
  cannot split it across threads (concatenation is associative but *not*
  commutative, which is why a bare string semiring is unsafe here); and no
  vendor BLAS exists for any semiring but the reals, so every other one is a
  hand-written kernel an order of magnitude slower. Expose a fixed vetted menu
  (`min_plus`, `max_plus`, `boolean`, `counting`) before user-defined ones.
- Element types beyond `Float64`. `transpose` already admits them; the blocker
  is only that `String` under concatenation is a monoid, not a semiring — no
  additive identity, no inverses, so no `solve` and no determinant. Needs
  `Set<String>` or an explicit $\bot$ to be well-formed.
- Units on axes (Hart, *Multidimensional Analysis*): level 5 of the checking
  lattice, catching shares-times-price-equals-dollars. Leave room, build later.

## Prior art

Worth reading before implementing, roughly in order of usefulness here:

- **R `dimnames`** — and note `%*%` already propagates them
  (`dimnames(A %*% B) = list(rownames(A), colnames(B))`) but never *checks*
  `colnames(A) == rownames(B)`. That gap is this plan's reason to exist. R's
  other three defects not to inherit: dimnames are unnamed (the axis has no
  name), untyped (dates become strings), and silently dropped by half of base R.
- **pandas** — `MultiIndex` is the composite label; `DataFrame.dot` does align
  on the inner index and raises on mismatch.
- **xarray** — named dims and `xr.dot` contracting over shared names: the
  closest shipped semantics to the contraction rule above.
- **AxisKeys.jl / DimensionalData.jl** — keyed arrays propagating keys through
  `*`, in a fast language. Read for the edge cases.
- **kdb+ keyed tables** — "rows labelled by non-numeric key columns" as a
  shipped concept in a finance language.
- **Dex** — array indices as types, including record types. Useful as a point of
  comparison for the runtime axis descriptors chosen here; Ibex does not adopt
  static axis schemas.
- **LARA / LaraDB** (Hutchison, Howe, Suciu), **Tensor Relational Algebra**
  (Yuan et al., VLDB 2021), **SDQL / semiring dictionaries** (Shaikhha et al.,
  OOPSLA 2022) — the contraction-as-join framing, formalised, with compilation
  strategies. Relevant to whether `@` should ever lower into the relational
  optimizer and inherit predicate pushdown.
- **GraphBLAS** (SuiteSparse) — the mature semiring implementation, for the
  follow-up.
- **Dolan, "Fun with Semirings" (ICFP 2013)** — short, and the clearest
  demonstration that the same matrix code computes shortest paths and regular
  expressions.
- **Green, Karvounarakis & Tannen, "Provenance Semirings" (PODS 2007)** — the
  same idea applied to relational algebra rather than matrices. Possibly a
  higher-value direction than matrix semirings, since lineage over existing
  operators has regulatory pull.
