# Expanded native regression audit, 2026-11-10

Four additional one-core regressions resolve in the focused confirmation:
integer `abs`, symbol semi/anti joins, and rolling EWMA. Integer `abs` also
regresses substantially at eight cores. The promotion gate remains open.
No runtime changes were made during this audit.

The subsequent [integer-abs repair](../integer-abs/README.md) records the fix,
its same-executable isolation and fresh production gates. Measurements below
describe the runtime before that repair.

The [semi/anti membership probe repair](../symbol-semi-anti/README.md) records
the fix for the symbol semi/anti joins, and a categorical null-key bug found
along the way.

The comparison is the pre-Int32 baseline `99eaa20c` against `291e4ca1`, whose
runtime is unchanged from `d05888c0`. Compact ingestion is **off** throughout:
these measurements check existing wide workloads, not compact-input speedups.

## Confirmed regressions

| Workload | One-core baseline ms | Current ms | Paired slowdown | Holm p | Eight-core confirmation |
| --- | ---: | ---: | ---: | ---: | --- |
| `abs_int_price` | 12.7345 | 13.3330 | +4.77% | 0.00570 | +15.56%, p=0.00236 |
| `null_semi_join` | 6.3405 | 6.4850 | +2.07% | 0.00570 | unresolved in broad screen |
| `null_anti_join` | 6.3245 | 6.4740 | +2.22% | 0.00570 | unresolved in broad screen |
| `tf_rolling_ewma_1m` | 6.8795 | 6.9665 | +1.29% | 0.00785 | unresolved in broad screen |

These are medians of paired ratios; percentages need not equal the ratio of
the separately reported median times. One-core confirmation uses 18 interleaved
triplets, nine inner iterations and two warmups, with correction across all
29 cases in `scalar,null,timeframe`. The first three cases were slower in all
18 pairs; EWMA was slower in 17. The eight-core scalar confirmation uses 18
triplets, 15 inner iterations and two warmups, corrected across 12 scalar cases.
Its `abs_int_price` medians are 1.5405 versus 1.7725 ms, slower in all 18 pairs.

`abs_int_price` evaluates `abs(round(price, Floor) - 300)` on four million
prices. It is a compound expression, so the name does not isolate the `abs`
kernel. The semi/anti cases use categorical symbol membership against a lookup
containing half the symbols. Their placement in the `null` suite does not mean
they probe nullable integer keys.

## Broad screen and unresolved cases

The expanded gate covers **91 timing rows** (88 regular and three parse variants),
including sorts, cumulative operations, windows, pipelines, scalar functions,
fill/null handling, RNG, multi-key aggregation, event data, reshaping and
TimeFrame work. All six CSV paths were passed explicitly. `all` excludes the
opt-in Boolean, Decimal, merge-validity, RNG-micro and filter-micro suites.

At one core, 12 triplets with five iterations/two warmups resolve integer
`abs` +4.25%, semi join +1.82%, and anti join +1.75% after Holm correction
across 91 rows. At eight cores, 12 triplets with three iterations/one warmup
resolve only integer `abs` as slower (+17.40%). The paired-ratio geomean is
-1.06% at one core and -0.007% at eight; individual regressions remain despite
those aggregate estimates. These are resident warm-loop measurements, with
loading excluded and parsing included, not fresh-process query latency.

Two other leads remain unresolved:

- Time-only as-of join: +6.91% in the broad one-core screen, falling to +2.13%
  in confirmation (Holm p=0.947). The early ~12% estimate was not stable.
- Rolling median: +1.38% in confirmation, Holm p=0.0569. This is not cleared
  as equivalent.

The known arithmetic-filter issue remains open. Its +1.92% broad-screen
estimate is unresolved here; this does not overturn the earlier focused
[arithmetic-filter investigation](../arithmetic-filter/README.md).

Every timed process completed with the same query set and output row counts.
The independently built baseline and replica executables are byte-identical.
No control/base comparison resolves after applying Holm within each run's
control family. None of this establishes a cause for the cross-build deltas.

## Operator corroboration and correctness

Separate six-iteration operator profiles, after two warmups, locate the cost:

- Integer-expression update `next_self_ms`: 12.6930 to 13.1845 at one core.
  At eight cores its `pool_work_ms` rises from 9.9080 to 11.5165.
- Symbol semi join `next_self_ms`: 6.2845 to 6.3885 at one core; anti join
  6.3080 to 6.3540.
- EWMA window `build_self_ms`: 6.3995 to 6.4700 at one core. Its
  `as_timeframe` setup is essentially unchanged (0.4500 versus 0.4490).

The relevant row/call counters match. These sequential diagnostic profiles
corroborate operator location; paired unprofiled runs supply the effect estimates.
Further isolation is needed before attributing these costs to dispatch,
materialization or code placement. Integer expression evaluation is the first
priority, particularly its parallel path.

[check_answers.py](check_answers.py) compares every typed output byte for all
29 deterministic focused cases, using 10,000 price rows and 10,000 synthetic
TimeFrame rows, at one and eight cores. All baseline/current answers match.
This bounded check does not claim full-scale cell validation for all 91 cases.

## Reproduction and local evidence

Release Clang builds use the existing git-comparison harness and its native
architecture settings. CPU 2 is used for one core; eight cores use
`0,2,4,6,8,10,12,14`. The box is locked to 3.2 GHz. No builds run during timing.

```bash
out="$PWD/benchmarking/results/local/int32/2026-11-10-regression-audit"
mkdir -p "$out"
IBEX_CORES=1 IBEX_COMPACT_INT32=0 IBEX_PERFCMP_JOBS=6 \
IBEX_PERFCMP_TMPDIR="$out" \
benchmarking/compare_ibex_git.sh --base 99eaa20c --target 291e4ca1 \
  --ibex-suite all --interleave --replica-control \
  --repeats 12 --warmup 2 --iters 5 --taskset 2 \
  --csv "$PWD/benchmarking/data/prices.csv" \
  --csv-multi "$PWD/benchmarking/data/prices_multi.csv" \
  --csv-trades "$PWD/benchmarking/data/trades.csv" \
  --csv-events "$PWD/benchmarking/data/events.csv" \
  --csv-lookup "$PWD/benchmarking/data/lookup.csv" \
  --csv-users "$PWD/benchmarking/data/users.csv" --keep-temp

.venv/bin/python benchmarking/int32/paired_report.py <retained-run-directory> \
  --out "$out/paired-rerun.json"
```

Use `IBEX_CORES=8`, eight-core affinity, `--warmup 1 --iters 3` for the broad
parallel screen. One-core confirmation changes the suite to
`scalar,null,timeframe` and uses `--repeats 18 --warmup 2 --iters 9`.
Parallel confirmation uses `scalar`, 18 repeats, two warmups and 15 iterations.

Raw results stay in that ignored persistent directory: `one-core-paired.json`,
`eight-core-paired.json`, `focused-paired.json`, `scalar-eight-core-paired.json`,
`conditions.json`, `answer-hashes.json`, and `operators-{one,eight}-core.json`.
Each retained harness directory includes every repeat TSV and stdout/build log.
Conditions record binary/input SHA-256 hashes, references and machine settings.
[profile.py](profile.py) records operator evidence separately from the timing gate.
Both verification drivers accept `--base`, `--target` and `--out`; the profiler
also requires `--suite` and `--queries` and accepts `--cores 1|8`.
