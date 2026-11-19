# Composite-key benchmark harnesses

Keep generated measurements and profiles in ignored storage. This directory
contains reproduction code and query fixtures, not recorded run results.

## Hash distribution

Compile the standalone comparison of the old boxed combiner and upstream XXH3:

```bash
clang++ -O3 -DXXH_INLINE_ALL benchmarking/composite-hash/distribution.cpp \
    -o build-release/hash-distribution
mkdir -p benchmarking/composite-hash/results
build-release/hash-distribution \
    > benchmarking/composite-hash/results/distribution.csv
```

Output columns: first cardinality, second cardinality, implementation,
distinct keys, distinct full hashes, smallest and largest of eight partitions.
This checks distribution, not operator performance.

## Operator queries

The `queries/` fixtures cover composite-key aggregation, grouped head and a
single-key control. Compare explicitly rebuilt Release binaries with
`benchmarking/ab_queries.py`; use balanced interleaved repeats and leave its
output checks enabled. Follow `MEASURING.md` for build and measurement rules.

## Cross-chunk transitions

The `[categorical-growth]` and `[nullable-codes]` Catch fixtures prepare input
outside their optional timed interpretation intervals, then verify every output
key/count (and sums for nullable pairs) against independent references.

`measure_growth.py BASE_TEST_BINARY TARGET_TEST_BINARY` and
`measure_nullable.py BASE_TEST_BINARY TARGET_TEST_BINARY` run balanced paired
comparisons using the same fixture in both explicitly rebuilt Release test
binaries. They preserve raw samples under ignored `results/` and print median
costs, paired ratios, wins and Wilcoxon p-values. Redirect reports there too.

Normal fixture execution checks route assertions and invalid null payloads.
Timing mode omits route assertions while retaining output checks; nullable
benchmarks use identical valid placeholder payloads under null bits on both sides.

`IBEX_CAT_TRANSITION_BENCH=1 build-release/tests/ibex_tests '[categorical-transitions]'`
times dense dictionary growth and packed-layout misses using three or nine key
columns at one and eight cores. It includes a mixed-key control. The timing
covers interpretation, and the fixture checks every emitted key and count.
The scenarios share one process, so allocator reuse can affect comparisons.
Restrict the scenario list identically on both sides to time a single scenario
in a fresh process.
