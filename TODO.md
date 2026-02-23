Todo list

Correctness / completeness:
1. Implement compound filter predicates — extend FilterPredicate to an expression tree so price > 10 AND qty < 5 and price
* 2 > 100 work
2. Implement asof join in interpreter (or at minimum give a cleaner error pointing users at what to use instead)
3. Decide what to do with user-defined fn declarations — either implement or remove from the grammar/SPEC and add a parse
error

CSV library:
4. Fix csv_split_line to handle RFC 4180 quoted fields ("foo,bar" and "foo""bar")

Stale TODO cleanup:
5. Remove stale /// TODO: Implement full lexer from lexer.hpp
6. Remove stale /// TODO: Implement evaluation pipeline from repl.hpp

Tests:
7. Expand test_repl.cpp — cover :schema, :load, multi-statement files, error recovery
8. Add tests for order + distinct combinations and edge cases (empty table, single row)
9. Expand test_csv.cpp — quoted fields, mixed types, empty fields, large file type inference

Benchmarking:
10. Add the compiled benchmark queries (mean_by_symbol, ohlc_by_symbol, update_price_x2) to the interpreter benchmark
results table in README so both rows appear side by side in a single table

Window / TimeFrame (larger scope):
11. Either implement window operations or remove them from SPEC.md with a clear "not yet implemented" note — currently the
gap between spec and reality is silent

