# poorman conformance source

The ribex dplyr conformance cases in `testthat/test-poorman-conformance.R`
are adapted from the following immutable poorman snapshot:

- Repository: <https://github.com/nathaneastwood/poorman>
- Commit: `c9eb1f1429e6934e1b3233bb10c50c82adf05bd2` (`c9eb1f1`)
- Snapshot URL: <https://github.com/nathaneastwood/poorman/commit/c9eb1f1429e6934e1b3233bb10c50c82adf05bd2>

Each adapted case identifies its originating `inst/tinytest/test_*.R` file and
this commit beside the test. The tests use the locally installed dplyr as the
oracle, execute the equivalent operation on `ibex_tbl(..., fallback = "error")`,
and assert the result remains an `ibex_tbl` before collection. Unsupported
poorman cases are intentionally kept in a separate test and must produce a
`ribex_translation_error`; they cannot pass through collection fallback.

Excluded at this stage: `across()`, pivots, list columns, arbitrary R closures,
and row-name behaviour. Native rank coverage includes `min_rank()`,
`dense_rank()`, `row_number(x)`, and `cume_dist()`. Native join coverage
includes same-named equality `inner_join()` and `left_join()` with
`na_matches = "never"`; other join forms and options remain separately
classified until their dplyr semantics can be represented exactly.

## Attribution

The source package's `DESCRIPTION` declares `MIT + file LICENSE` and identifies
Nathan Eastwood as copyright holder for 2020. The full MIT notice is included in
`inst/third-party/poorman/LICENSE`.
