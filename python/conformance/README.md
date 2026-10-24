# Upstream Polars compatibility target

This optional harness runs **unmodified upstream tests and assertions** from
[Polars 2.0.0](https://github.com/pola-rs/polars/tree/22a147de3d2bb2e44b97338a2510816c7105c9f2/py-polars/tests).
`upstream.json` pins the release commit, the content digest of the complete
Python test tree and its pytest configuration, eight initial test modules,
and query comparisons that must keep passing. Nothing is vendored or rewritten.

The initial modules cover projection, updates, comparisons, filtering, sorting,
grouping, vertical reductions, and arithmetic. This is a compatibility target,
not a claim that all these operations or dtypes are implemented.

## Run

Use the Python environment selected for the bridge build:

```bash
.venv/bin/python -m pip install -r python/conformance/requirements.txt
git clone --depth 1 --branch py-2.0.0 https://github.com/pola-rs/polars.git .polars-upstream
git -C .polars-upstream rev-parse HEAD
# Must be 22a147de3d2bb2e44b97338a2510816c7105c9f2.

# Build ibex_pyarrow as described in python/README.md first.
PYTHONPATH=build-release/python:python .venv/bin/python -m conformance.run \
  --upstream .polars-upstream \
  --report-dir build-release/polars-conformance-results
```

A release archive from the same commit also works. The content pin rejects
changed tests, fixtures, or pytest configuration even if Git HEAD is unchanged.
Generated Python bytecode is ignored by the digest.

For a focused run, repeat `--select` with test files or parametrized node IDs
relative to upstream `py-polars`, for example:

```bash
PYTHONPATH=build-release/python:python .venv/bin/python -m conformance.run \
  --upstream .polars-upstream \
  --select tests/unit/operations/test_select.py \
  --select tests/unit/operations/test_filter.py::test_simplify_expression_lit_true_4376
```

CTest registration is opt-in; configuration does not download dependencies:

```bash
cmake -S . -B build-release \
  -DIBEX_POLARS_UPSTREAM_DIR="$PWD/.polars-upstream"
ctest --test-dir build-release -R '^ibex_polars_(conformance_harness|upstream)$' \
  --output-on-failure
```

Linux Release CI checks out the exact upstream commit, runs both CTest targets,
and uploads the JSON reports. Other build configurations need no upstream tree.

## What is counted

The runner first executes the original tests against Polars in a separate
process. Only tests whose baseline passes are probed with Ibex. The candidate
process intercepts explicit `LazyFrame.collect` calls during test bodies;
eager operations used to prepare input or expected results stay in Polars.
Each intercepted query runs against the reference engine and `IbexEngine`.
Accepted results are compared for values, names, dtypes, nulls and row order,
then returned to the original upstream assertions. Floating-point tolerances
match the existing engine tests (`rel_tol=1e-12`, `abs_tol=1e-9`). Error cases
must raise the same exception type; the candidate error is passed to upstream
assertions so their message checks also apply.

Query outcomes are:

* **pass**: matching results with observed native execution, or matching errors.
* **unsupported**: `NotImplementedError` before any native execution.
* **mismatch**: different results/errors, unexpected execution failures, or a
  successful result without native execution. A late `NotImplementedError`
  also counts as a mismatch.
* **excluded**: collection modes such as GPU/background/callbacks outside the
  harness's scope.

After a rejection or execution failure, the harness supplies the reference
result solely to continue the test and inspect later queries. That event
remains unsupported or mismatched; it never becomes a passing Ibex query.
This continuation exists only in the harness. The engine has no fallback.

Test outcomes retain this distinction: a test passes only if every intercepted
query passes and its assertions pass. Tests containing both passing and
unsupported queries are unsupported, with their individual results retained.
**Unexercised** means the test never reached an intercepted lazy collection;
eager/Series-only tests, fixture execution and `collect_all` are not Ibex
coverage. Upstream skips and baseline failures are recorded separately.
Unsupported assertions cannot swallow and hide a recorded mismatch.

`report.json` contains test and query counts, node IDs, query ordinals,
rejection reasons, native call counts, dependency versions and the source pin.
The separate `reference.json` and `candidate.json` retain pytest outcomes.
The run fails on mismatches, baseline/collection failures, incomplete runs,
zero passing comparisons, or regressions of previously passing queries in
`required_pass_queries`. Explicitly unsupported features do not fail the run.

Row order is checked by default. `unordered_tests` names individual tests whose
upstream assertions explicitly set `check_row_order=False`, with the reason
recorded in each report. Currently this covers `test_group_by_cse_alias_26423`:
its unordered grouping can produce a different group order on each run.
There is no blanket sorting or dtype conversion to make comparisons pass.

## Initial survey and expansion

See the [unsupported-feature survey](unsupported-survey.md) for grouped
rejection counts, representative blockers, coverage gaps and suggested work.

The initial local survey ran 1,076 upstream tests (3,347 cases excluded by
upstream fast-test marker policy): 16 tests passed through Ibex, 330 contained
unsupported queries, and 730 were unexercised. Across 1,630 intercepted queries,
52 passed, 1,578 were unsupported, and none mismatched. These are observations,
not an overall compatibility percentage. The native test bridge was linked
against the existing release libraries with the branch's borrowed-column
read fixes; CI builds the branch from source.

After adding boundary-only Int32 support, the same survey has 20 passing tests,
326 tests with unsupported queries, and 730 unexercised tests. There are 73
matching queries and 1,557 rejections, with zero mismatches. These newly passing
comparisons are now part of the required regression baseline.

With Float32 input support, matches increase to 76 queries across 23 fully
passing tests, with 1,554 query rejections, 323 unsupported tests and 730
unexercised tests. There are no mismatches in these selected upstream tests.
The engine's documented Float64-intermediate policy can deliberately differ
from Polars' Float32 rounding; dedicated engine tests verify that policy.
The upstream comparisons retain their original strict checks and tolerances.

Expand `targets` to more upstream modules or use `--select` to investigate.
When support improves, add newly passing query ordinals from the report to
`required_pass_queries` so that losing support fails CI rather than silently
becoming another rejection. Do not weaken upstream expected results or mark
an accepted mismatch as unsupported. A Polars version upgrade requires a
reviewed adapter update, new source/dependency pins, and a fresh baseline.

The harness bookkeeping tests run with:

```bash
PYTHONPATH=build-release/python:python .venv/bin/python python/test_polars_conformance.py
```
