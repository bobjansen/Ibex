# R-only benchmark suite

`run_r_only.sh` is the focused comparison for the three R-facing paths:
`data.table`, ordinary in-memory `dplyr`, and Ibex's native lazy dplyr backend.

It uses the generated CSV fixtures and query IDs from the website scale suite,
but includes only operations that `ibex_tbl(..., fallback = "error")` currently
translates and executes natively. This prevents a local dplyr fallback from
being reported as an Ibex result.

Run a quick representative sweep:

```sh
benchmarking/run_r_only.sh --sizes 1M,4M --warmup 1 --iters 3
```

The runner installs the checkout's `r/ibex` package into a temporary R library
against `build-release` (or `BUILD_DIR`) before running. It writes each scale to
`benchmarking/results/r_only/<rows>/r_only.tsv` and the combined result to
`benchmarking/results/r_only.tsv`. Pass `--skip-install` only when `ibex` is
already available through `R_LIBS_USER`; pass `--keep-data` to retain generated
fixtures.

Importing an R table into Ibex happens once per fixture before timing. Timed
Ibex rows include lazy-plan construction, native execution, and collection back
to R, but not that one-time binding cost. The two other frameworks likewise load
their fixture before timing.

## Memory discipline

The large scales exhaust an ordinary box if every fixture is held at once, so
the suite runs in four phases — `prices` + `lookup`, then `prices_multi`, then
`trades`, then `events` + `users`. Each phase loads only its own fixtures and
releases them before the next begins: `rm()` plus a collection on the R side,
and `reset_session()` on the Ibex side, since dplyr-backend bindings are
otherwise held for the lifetime of the session. `lookup` shares the `prices`
phase because its joins probe against `prices`.

Within a phase the data.table and dplyr frontends *share* their column vectors
rather than each holding a full copy; R's copy-on-modify keeps them honest, as
no query overwrites an existing column. Collection (`gc()`) happens between
timed iterations, after the prior result is dropped — never inside a measured
interval.

One caveat on hosts without a monotonic clock (WSL2 among them): base R times
with the wall clock, which can step backwards mid-measurement. The harness
discards and re-measures the resulting negative samples, but a *forward* step
is indistinguishable from a slow run, so prefer `min_ms` over `avg_ms` there.
