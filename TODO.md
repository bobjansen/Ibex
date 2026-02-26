Todo list

Correctness / completeness:
1. Implement compound filter predicates — extend FilterPredicate to an expression tree so price > 10 AND qty < 5 and price
* 2 > 100 work
2. Implement asof join in interpreter (or at minimum give a cleaner error pointing users at what to use instead)
3. Decide what to do with user-defined fn declarations — either implement or remove from the grammar/SPEC and add a parse
error

CSV library:
4. Fix csv_split_line to handle RFC 4180 quoted fields ("foo,bar" and "foo""bar")
5. Apply categorical string detection to the parquet plugin (mirrors CSV auto-encoding).

Tests:
6. Expand test_repl.cpp — cover :schema, :load, multi-statement files, error recovery
7. Add tests for order + distinct combinations and edge cases (empty table, single row)
8. Expand test_csv.cpp — quoted fields, mixed types, empty fields, large file type inference

Benchmarking:
9. Add the compiled benchmark queries (mean_by_symbol, ohlc_by_symbol, update_price_x2) to the interpreter benchmark
results table in README so both rows appear side by side in a single table

Window / TimeFrame (larger scope):
10. Either implement window operations or remove them from SPEC.md with a clear "not yet implemented" note — currently the
gap between spec and reality is silent

11. Make filter fast

● Two main reasons:

1. Parallelism — Polars uses Rayon (Rust's work-stealing thread pool) to split the column into chunks processed in
parallel. Filter is embarrassingly parallel: each core computes a mask for its chunk and gathers its output independently.
On 8 cores you'd expect ~8x speedup, which matches: 52ms / 7ms ≈ 7×. The low-selectivity queries (filter AND ~10%, filter
OR ~12%) already show a narrowing gap because the gather work is proportionally less.

2. Cheap string gather — This is the other big factor. The symbol column in ibex is Column<std::string> — every gathered
row does a std::string copy (24-byte SSO struct copy + potential heap allocation for long strings). In Polars, string
columns are Apache Arrow StringArray — a single contiguous byte buffer plus an i32 offsets array. Gathering a string is
just copying an 8-byte offset pair, not a heap object. For filter_simple at 50% selectivity, ibex copies 2M std::strings
from the symbol column; Polars copies 2M × 4-byte offsets.

You can see the selectivity effect clearly in ibex's own numbers:

┌───────────────┬─────────────┬─────────┐
│     query     │ selectivity │  ibex   │
├───────────────┼─────────────┼─────────┤
│ filter_simple │ ~50%        │ 52.6 ms │
├───────────────┼─────────────┼─────────┤
│ filter_arith  │ ~50%        │ 50.1 ms │
├───────────────┼─────────────┼─────────┤
│ filter_AND    │ ~10%        │ 21.6 ms │
├───────────────┼─────────────┼─────────┤
│ filter_OR     │ ~12%        │ 23.7 ms │
└───────────────┴─────────────┴─────────┘

The mask computation (the part we just vectorized) is fast for all of them. The cost scales with output size, not input
size — it's the gather that dominates.

To close the gap with Polars, the two levers are parallelising compute_mask + gather (straightforward with
std::execution::par_unseq or a thread pool), and a dictionary/categorical column type so the symbol gather becomes an
integer copy. Neither is trivial but the parallelism alone would likely get ibex within 2× of Polars.
