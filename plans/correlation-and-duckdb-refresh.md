# Correlation reduction and DuckDB comparison follow-up

## Correlation

The in-memory row `corr_price_vol` computes a single `corr(price, qty)`,
not a correlation matrix. Its display label is now "pairwise correlation".

The dedicated global paired-reduction path skips the generic per-row group
lookup and group-ID array. It dispatches numeric column types once, computes
two-pass moments in fixed 4096-row blocks, and merges in block order. A common
observed origin protects large-offset inputs without making block means lose
their small residuals. Repeated `corr(x,y)` / `cov(x,y)` share the same moments.
Grouped and mixed-aggregate queries retain their existing path.

Correctness validation completed on this worktree:

- Full Debug build, including the enabled runtime plugins.
- `ctest --test-dir build -LE slow`: no failures among 2349 tests; four socket
  tests skipped by the environment.
- Focused `[corr]` tests: 5957 assertions across six cases. The two new cases
  also passed 2170 assertions when linked against the new kernel compiled at
  `-O3`. Optimized code contains SIMD subtract/add and FMA instructions.
- Deliberately zeroing the cross-moment made the new reference test fail;
  restoring the implementation restored passing tests.
- Both changed runtime translation units passed strict GCC syntax checks with
  `-Wpedantic -Wconversion -Wshadow -Werror` (not a complete GCC build).
- The new global corr/cov parity case transpiled and matched the interpreter,
  including computed arguments and constant-column null results. The full slow
  parity suite has not been rerun.

Performance validation is deferred while the local machine is busy. Do not
report a speedup from the implementation alone. On a quiet machine, rebuild the
complete Release tree and use `benchmarking/compare_ibex_git.sh` with
`--base a0e373a3 --target WORKTREE --interleave --ibex-suite stats`, forwarding
the actual `--csv-trades` path. Check operator profiles, answers, and thread
byte identity as well as wall time. Repeat for multiple row counts and nullable
versus non-null inputs. Check mixed/global and H2O grouped corr/cov for regressions.

## Pending DuckDB refresh

The user requested retaining a full-suite comparison with DuckDB compressed
in-memory VARCHAR storage. Run it when the machine is quiet, alongside the
existing uncompressed configuration; do not silently replace historical results.

Diagnostic setup: `ATTACH ':memory:' AS compressed (COMPRESS)`, load the same
CSV inputs and `CHECKPOINT compressed` before timing. Keep logical strings as
VARCHAR. Verify storage metadata: an explicit checkpoint also compressed an
unflagged in-memory catalog in the tested DuckDB 2.0.0, so an uncompressed
control must not be checkpointed. Include load/checkpoint preparation cost
separately and confirm newly seen strings can be appended and checkpointed.

The seven-query 4M-row diagnostic showed about a 1.86x geomean improvement,
with large gains for low-cardinality symbol operations, a small user-join
regression, and little change for join/compute/aggregate. This selected sample
does not establish a full-suite win. The separate ENUM experiment recovered
more performance but required fixed dictionaries and is not equivalent to
unrestricted VARCHAR. Switching Arrow export to CREATE TEMP TABLE did not
improve the join timings. Preserve correctness gates and the thread/affinity
configuration when refreshing the full suite.
