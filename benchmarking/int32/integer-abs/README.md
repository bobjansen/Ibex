# Integer-expression abs repair, 2026-11-10

The production change (`a9443d01`) improves `abs(round(price, Floor) - 300)` by **3.58% at
one core and 12.69% at eight cores** against the current runtime (`16de4eb3`,
code-equivalent to `98aa0d7b`). Both survive Holm correction across the 91-case
resident suite. No new slowdown resolves in either broad before/after gate.

The fix selects physical input storage once, resolves the mutable destination
once, and passes the source, destination, offset and count by value to a small
typed kernel. The existing storage visitor supplies the two physical widths;
there is one math body. Values widen to Int64 before `safe_iabs`, preserving
Int32-minimum behavior and the existing Int64-minimum wrap contract. No new
column type, allocator policy, narrowing rule or production environment switch
is introduced.

## What caused the extra work

The unchanged native executable reaches separate round, subtraction and integer
unary stages. A GDB-controlled perf recording of the actual abs query, with
breakpoints outside its iterations, places the largest increased sample share
in `unary_math_over_column`. Whole-expression timings supply the regression
estimate; sample percentages alone do not establish its size.

Assembly identifies a concrete problem: the current unary loop checks the
`IntegerView` width and mutable `Column::data()` detach state on every row.
The original wide-only build could vectorize. Two intermediate experiments
show why binding the destination alone is insufficient:

1. **Output pointer only.** In one diagnostic executable this improves the
   expression by 4.93%/19.67% at one/eight cores. Removing the diagnostic branch
   changes Clang's decision: the production candidate combines both widths
   into masked vector loads, and is **7.74 times as slow** over eight complete
   paired triplets. That broad gate was stopped early and rejected; it is not
   a completed passing gate.
2. **Storage visitor with an inline loop.** This removes width selection from
   the row loop, but Clang outlines the visitor wrapper and reloads its lambda's
   captured offset and count on each row. It improves only 0.87%/9.02% in the
   same-executable experiment.
3. **Typed kernel with parameters passed by value.** This removes both sources
   of repeated work. Its two generated kernels vectorize, contain no calls or
   masked width-selection loads, and read their loop bounds independently of
   the visitor's capture object. `IBEX_NOINLINE` preserves that boundary.

The final same-executable counterfactual improves the expression by **5.09% and
19.16%**, respectively. All 12 pairs improve at each core count; Holm p=0.00586
across the 12 scalar cases. All unchanged controls and other scalar-case
comparisons are unresolved. These diagnostic absolute times differ from the
production executable; production performance is measured separately below.

## Production measurements

| Comparison | Cores | Base median ms | Target median ms | Paired effect | Holm p |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before/after repair | 1 | 13.2570 | 12.7710 | -3.58% | 0.04443 |
| Before/after repair | 8 | 1.8085 | 1.5420 | -12.69% | 0.04443 |
| Pre-Int32 baseline / repair | 1 | 12.7140 | 12.6925 | -0.06% | 1.0 |
| Pre-Int32 baseline / repair | 8 | 1.5145 | 1.5355 | +1.37% | 1.0 |
| Pre-Int32 baseline / repair, fresh repeat | 8 | 1.4925 | 1.4895 | -0.31% | 1.0 |

Effects are medians of paired ratios, so they need not equal ratios of separate
median times. The original-baseline comparisons are unresolved, not equivalence tests.
The old baseline is `0caffc17`. The one-core observed gap is recovered; the
eight-core estimates are much smaller than the earlier +15.56% result but
are not declared zero. The first original-eight run resolved an unrelated
cosine control at -3.48%, so it was repeated independently. No candidate/base
or control/base comparison resolves in the fresh 18-triplet scalar repeat;
abs is -0.31%. Both runs are retained.

The two broad gates use 12 interleaved triplets, three inner iterations and one
warmup, corrected across 91 timing rows each. All six CSV inputs are passed
explicitly. The original-baseline scalar gates use 18 triplets, two warmups,
and nine/fifteen iterations at one/eight cores, corrected across 12 cases.
All builds are Release/native. Compact ingestion is off. CPU affinity is `2`
or `0,2,4,6,8,10,12,14`, at the existing 3.2 GHz frequency lock. No build or
other benchmark runs during timing.

Both broad baseline replicas are byte-identical to their respective baseline
binary, and their control comparisons are unresolved after Holm correction.
The one-core broad gate also resolves EWMA -0.85% and rolling median -2.69%.
Those operators were not changed or isolated here, so these are unassigned
cross-build effects, not claimed consequences of the abs kernel. Suite paired
geomeans are -0.31%/-0.35% at one/eight cores.

In separate diagnostic profiles, abs update `next_self_ms` falls from 13.2575
to 12.7540 at one core and from 1.7335 to 1.4760 at eight. Eight-core pool work
falls from 11.6010 to 10.0260 ms, with matching row/call counters and eight tasks.
These sequential profiles corroborate operator location; the unprofiled paired
runs provide effect estimates.

## Correctness and retained evidence

- Full Release rebuild, CSV/JSON plugin rebuilds, and strict GCC runtime build
  with warnings as errors pass.
- Both compact-ingestion settings pass the 2,489-case fast CTest suite; wide
  mode skips the two existing opt-in Parquet cases. The five Python lazy,
  slice, Polars, join and first/last suites pass 133 tests per setting.
- The new direct column-evaluator test covers compact/wide external buffers,
  source and evaluation offsets, signed minima, nulls, empty ranges and vector
  tails. It passes 2,772 assertions. A zero-output mutant fails 1,014 assertions,
  proving the test reaches the changed kernel. The mutation is restored before
  checks and timing.
- All 29 focused deterministic workloads have byte-identical full typed answers
  on bounded fixtures at one and eight cores, before/after the final native
  change. This does not claim full-scale cell validation of all 91 workloads.

Only the integer unary kernel is repaired here. Other open regressions from the
[expanded audit](../regression-audit/README.md), and the previously diagnosed
[arithmetic filter](../arithmetic-filter/README.md), need their own isolation.
No compact-input performance claim or complete Int32 promotion is made.

Raw reports, every timed stdout/TSV, assembly, profiles, failed attempts and
build/test logs remain under ignored persistent
`benchmarking/results/local/int32/2026-11-10-integer-abs/`. Frozen binaries and
source snapshots remain under `build-integer-abs/`. The earliest toggle attempt
used the unchanged cache slot and was rejected before interpreting results;
its samples are marked `rejected-cache-slot`. The driver now verifies that the
chosen binary contains its diagnostic switch.

Final gate reports are `before-after-{one,eight}-paired.json` and
`original-{one,eight}-paired.json`.
`original-eight-repeat/paired.json` records the additional control recheck.
Same-executable final reports are
`kernel-toggle-{one,eight}.json`; `storage-toggle-*` and `toggle-*` describe the
intermediate experiments. `rejected-output-only-summary.json` identifies the
stopped production gate. `final-answer-hashes.json`, `final-operators-*.json`,
and `conditions.json` record checks and provenance. Full local JSONs are not
versioned.

## Reproduction

From the repository root, with the candidate source in the working tree:

```bash
bash benchmarking/int32/integer-abs/run_gates.sh
```

[run_gates.sh](run_gates.sh) builds all compared native benchmark executables
before timing, retains all repeats locally and invokes the existing paired
Wilcoxon/Holm analyzer. It compares the source before this repair against the
working tree, then the original baseline against the working tree.

For the final same-executable experiment, apply
[bind-kernel-diagnostic.patch](bind-kernel-diagnostic.patch) to `16de4eb3` in a
separate branch/worktree with `git apply --unidiff-zero`, and build
Release/native. Run:

```bash
.venv/bin/python benchmarking/int32/integer-abs/toggle.py \
  --binary /path/to/diagnostic/ibex_bench --suite scalar --cores 1 \
  --queries abs_price sqrt_price log_price exp_price round_price floor_price \
    ceil_price sin_price cos_price tanh_price abs_int_price sqrt_coalesce_price \
  --repeats 12 --iters 12 \
  --out benchmarking/results/local/int32/integer-abs/kernel-rerun.json
```

Use eight cores and 30 iterations for the parallel counterfactual. The same
driver also runs [bind-output-diagnostic.patch](bind-output-diagnostic.patch)
and [bind-storage-diagnostic.patch](bind-storage-diagnostic.patch). The retained
[profile_unchanged.py](profile_unchanged.py) records perf samples for the actual
abs query in an unchanged executable through GDB on Linux x86-64. The expanded
audit's [profiling](../regression-audit/profile.py) and
[answer-checking](../regression-audit/check_answers.py) drivers provide the
final operator and bounded-answer checks.
