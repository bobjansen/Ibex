# Existing-workload regression repair

Raw measurement JSONs and generated provenance are kept in persistent ignored
storage at `benchmarking/results/local/int32/2026-11-09/aggregate-regressions/`. The scripts, patches and findings are versioned; JSON
filenames below refer to that local evidence directory.

This follows the native Int32 integration at `dba1d6cc`. Compact ingestion
remains opt-in. These changes improve existing wide workloads without changing
integer width, overflow behavior, or aggregate arithmetic.

## Diagnosis and retained changes

A fresh original-baseline comparison (`7f5e27c4` versus `dba1d6cc`, 20 paired
repeats, one pinned core) reproduced median +2.72% and quantile +2.85%. The
historical standard-deviation regression did **not** reproduce: its paired
ratio was 0.9954, p=0.076. This is unresolved, not an equivalence result.
See original-groupagg.json (`original-groupagg.json`, local evidence).

Profiles put the median/quantile cost in collected aggregation. The selection
routine has identical normalized instructions in the two binaries, but different
addresses; that does not rule out code-layout effects. The serial group-count
loop repeatedly loaded captured bounds and checked validity. A small out-of-line
kernel takes the group pointer, row count and destination bins by value and
branches on validity before iterating. It improves median/quantile in the
same executable. It does not explain the whole initial cross-build difference.

An interim broad gate after that change exposed additional arithmetic-filter,
categorical-join and OHLC deltas. Its report is
count-only-gate.json (`count-only-gate.json`, local evidence). This is the count-only interim
gate, not the final candidate.
Profiles and isolation led to two further retained changes:

* Categorical dictionary-memo probes bind the physical code pointer and resolved
  head pointer once. This removes owned/external storage checks from each probe.
  The pointers remain valid through synchronous probing and parallel task waits;
  null filtering and slice offsets keep their existing handling.
* Float64 grouped accumulation runs in an outlined kernel with row bounds,
  slot stride and scratch layout passed by value. Nullable and non-nullable
  loops are selected before iteration. The existing update operations and
  row order are unchanged. Integer, temporal, Boolean and text paths are untouched.

## Same-executable counterfactuals

Each diagnostic executable contains both old and new paths. Twenty-four repeats
rotate and reverse old/new/unchanged-control positions, with 12 timed iterations
and two warmups, CPU 2, one Ibex core, compact ingestion off. The reported effect
is the median paired new/old ratio; p-values below are Holm-adjusted across the
queries analyzed in each experiment. Unchanged controls were unresolved.

| Experiment | Query | Change | Adjusted p | Decision |
|---|---|---:|---:|---|
| Count loop | median | -0.71% | 0.000570 | retain |
| Count loop | quantile | -0.91% | 0.000242 | retain |
| Physical collect scatter | quantile | -0.33% | 0.0532 | discard; unresolved |
| Categorical probe pointers | inner join symbol | -18.30% | 0.0000182 | retain |
| Float64 accumulation | OHLC | -16.99% | 0.0000729 | retain |
| Float64 accumulation | OHLC including parse | -17.02% | 0.0000729 | retain |
| Float64 accumulation | mean including parse | -7.80% | 0.0000729 | supporting result |
| Float64 accumulation | mean | -7.51% | 0.0593 | unresolved |
| Four-at-a-time gather | arithmetic filter | +5.58% | 0.0000729 | discard |
| Outlined arithmetic predicate | arithmetic filter | -0.27% | 1.0 | discard; unresolved |

Raw samples, orders, row counts and statistics (local files):
count (`count-toggle.json`, local evidence), scatter (`scatter-toggle.json`, local evidence),
probe (`probe-toggle.json`, local evidence), Float64 (`float-toggle.json`, local evidence),
arithmetic filter (`arithmetic-toggle.json`, local evidence).

The diagnostic builds use ordinary Clang Release AVX2/FMA settings. The git
comparisons use `-march=native`. Compare ratios within an experiment; absolute
times between those build configurations are not a causal comparison.

## Reproduction and retained evidence

Diagnostic build metadata (`diagnostic-builds.json`, local evidence) names patches relative to
`dba1d6cc`, hashes and persistent frozen binaries. Each patch describes the
**cumulative** source state of its corresponding executable, including earlier
retained changes. Apply one patch with `git apply --unidiff-zero` to a separate branch from that revision,
build Release, and use [toggle_kernel.py](toggle_kernel.py), for example:

```bash
.venv/bin/python benchmarking/int32/aggregate-regressions/toggle_kernel.py \
  --binary build-release/tools/ibex_bench --suite core \
  --queries ohlc_by_symbol parse_ohlc_by_symbol mean_by_symbol parse_mean_by_symbol \
  --switch IBEX_DIAG_BIND_FLOAT_ACCUMULATION \
  --out benchmarking/results/local/int32/aggregate-regressions/float-rerun.json
```

Use the exact query names recorded in each JSON report when overriding the
analyzed set. All benchmark queries in the selected suite still execute.
Production contains no diagnostic switches. Raw sample arrays are retained locally;
per-repeat stdout from the earliest count/scatter experiments was overwritten
by subsequent diagnostics. Later experiment logs and frozen sources/binaries
remain in the ignored persistent `build-int32-regressions/` directory.

The original and intermediate operator measurements are retained in
initial-operators.json (`initial-operators.json`, local evidence) and
remaining-operators.json (`remaining-operators.json`, local evidence). Binary provenance is in
binaries.json (`binaries.json`, local evidence); selection-code normalization is in
selection-code.json (`selection-code.json`, local evidence). Detailed perf recordings and annotated
assembly remain under `build-int32-regressions/`, outside Git.

## Three-fix broad comparison

The 23-query, 20-pair comparison (`three-fix-gate.json`, local evidence) against `7f5e27c4`
uses one pinned core, seven inner iterations, two warmups, all six explicit
CSV paths and an unchanged replica. Median/quantile are ahead of baseline by
0.50%/0.62%, OHLC by 18.54%, categorical join by 15.11%. These survive Holm
correction across 23 queries. The standard-deviation query is also 2.93% faster
in this build comparison; this is not attributed to the group-count kernel.

The arithmetic-filter estimate is +1.54%, Holm p=0.179: unresolved, **not cleared
as equivalent**. Its predicate's normalized instructions match the original
build (instruction hashes and normalization limits (`arithmetic-code.json`, local evidence)).
Outlining that predicate did not resolve an improvement in the same-executable
counterfactual, so that change was discarded. A four-at-a-time gather experiment was also rejected: it slowed all four
filters by 3.76–6.56% (Holm p=0.0000729), with unresolved unchanged controls.
Raw samples (`gather-toggle.json`, local evidence) and its diagnostic patch preserve that result.
The existing gather loop is retained.


## Final source and checks

The final production source contains the three retained changes only. Removing
an empty scope and a diagnostic-only include produces a **byte-identical native
benchmark binary** to the measured three-fix candidate; hashes are in
binaries.json (`binaries.json`, local evidence). No predicate, gather-unroll or scatter experiment
is included in production.

The full Release build (including plugins) and strict GCC runtime build pass.
Both ingestion modes pass the 2,488-case fast CTest suite; with compact ingestion
off, two opt-in Parquet cases are skipped. The Python lazy/slice/Polars/join/
first-last suites pass 133 tests in each mode, and both standalone Arrow bridge
checks pass. A separately linked doubled-count mutant fails the existing grouped
median/quantile tests; no mutation is applied to production sources.


All 23 full typed CSV answers match the original at one/eight cores, with compact
ingestion off/on: hashes (`answer-hashes.json`, local evidence),
[reproducible checker](check_answers.py). Actual compact Arrow/Parquet paths are
covered by the runtime and Python checks; setting the flag does not itself make
CSV inputs compact.

The eight-core gate (`eight-core-gate.json`, local evidence) uses 12 paired repeats, five inner
iterations, two warmups and CPU affinity 0,2,4,6,8,10,12,14. OHLC improves 14.53%
(11.27% including parse), both Holm p=0.01123. All other comparisons are
unresolved; this is not a claim of equivalence for every query. The unchanged
OHLC control comparisons are unresolved.

Final operator profiles (`final-operators.json`, local evidence), collected separately from the
timing gates with [profile.py](profile.py), corroborate the relevant operators:
one-core OHLC discovery/accumulation falls 16.35 → 13.50 ms and the categorical
join's next time falls 30.16 → 25.59 ms, with the same output rows/calls. At eight
cores OHLC pool work falls 17.91 → 15.10 ms with eight tasks in each case. The
parallel categorical-join profile is unchanged, consistent with its unresolved
eight-core timing result. Stage and operator fields are not added together.

The historical aggregation regressions are repaired in the one-core baseline
comparison. The arithmetic-filter +1.54% estimate remains an open question;
the complete promotion gate is **not** declared clear. Compact ingestion stays
opt-in.

CSV/JSON plugins were also rebuilt with `scripts/ibex-plugin-build.sh`; loading
them and joining grouped CSV medians to JSON sums passes the smoke check.
