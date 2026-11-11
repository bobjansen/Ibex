# Arithmetic-filter follow-up

Raw measurement JSONs and generated provenance are kept in persistent ignored
storage at `benchmarking/results/local/int32/2026-11-09/arithmetic-filter/`. The scripts, patches and findings are versioned; JSON
filenames below refer to that local evidence directory.

**Outcome: layout sensitivity established; local alignment candidate rejected.**
The runtime remains unchanged from `d05888c0`.

Starting point: `d05888c0`, after the aggregate and categorical-probe repairs.
Original native baseline: `99eaa20c`. Compact ingestion stays opt-in. There is
no allocator replacement or arithmetic-width change in this work.

## Reproducing and locating the difference

A focused git comparison with 30 interleaved repeats, 25 timed iterations,
three warmups, CPU 2 and an unchanged replica resolves arithmetic +1.03%
(Holm p=0.0103 across four filters), versus +1.51% against the replica.
Raw paired samples (`focused-gate.json`, local evidence).

A separate 24-pair comparison of the unchanged executables with/without the
unused prices-table preload reproduces arithmetic +1.46% / +1.21%. It also
resolves simple-filter +1.16% / +1.31%; the sparse AND/OR comparisons are
unresolved. This is a dense-result effect, not uniquely multiplication, and
removing the unused input does not remove it.
Samples and statistics (`input-contexts.json`, local evidence), [driver](contexts.py).

The [production-stage probe](stages.cpp) calls the runtime's actual mask,
selection, allocation and gather APIs. Each revision uses its own matching
headers/libraries: the old/current Column layouts differ, so sharing a caller
object would be invalid. Inputs and full output verification are untimed;
allocation and destruction are excluded from gather-only times. Each repeat
starts fresh processes, with an unchanged baseline replica.
[Build driver](build_stages.py), [paired driver](run_stages.py),
raw samples (`stages-paired.json`, local evidence).

The probe resolves faster current materialization (-2.61%), categorical gather
(-6.30%) and integer gather (-5.00%); it does not resolve a predicate slowdown.
These separate-entry-point results do **not** substitute for the unchanged
whole-query measurement. Re-linking the probe also changes code placement.

The first probe omitted the main harness's `malloc_conf` retention setting.
Matching that setting changes its 60-call diagnostic from 915,607 to 97,892
minor faults and from 24.57 to 10.29 ms/query. Those first probe timings were
rejected; their raw files/binaries remain under ignored persistent
`build-arithmetic-filter/`. This establishes matching measurement conditions,
not an allocator recommendation. The native fresh-query jemalloc trial was
previously rejected (+3.7%, recorded in `plans/beat-both-plan.md`).

## Unchanged executable and instruction evidence

[Debugger-controlled profiling](profile_unchanged.py) stops at `run_benchmark`
entry/return, outside timed iterations. perf attaches while the inferior is
stopped. Preceding filters are retained for 25 iterations or skipped, with no
recompilation; all benchmark executables retain their original bytes. GDB
script state is local to a function so library auto-load scripts cannot
replace its variables. After counters stop, the debugger detaches and the
program finishes normally. The driver verifies the prefix actually executed
by checking its benchmark lines.

The 12-pair counter experiment (`profiled-contexts.json`, local evidence), with 150 timed
arithmetic iterations, shows about 0.51% **fewer** retired instructions and
4.7–5.5% fewer branch misses in the current build. A/A instruction differences
are tiny but statistically resolved (~0.00025%); they are not called zero.
The timing comparison at this smaller sample count is unresolved.
[Driver](run_profiled.py). There is no evidence here of extra per-row integer
width work or increased branch-miss count explaining the slowdown.

Loop inspection (`gather-loops.json`, local evidence), reproducible with
[inspect_loops.py](inspect_loops.py), finds:

* The original Int64 selected-row loop starts at a 32-byte boundary.
* The current loop starts 16 bytes into a 32-byte block. Its six instructions
  have exactly the same byte hash as the original, including register choices.
* Several categorical selected-row loops also start 16 bytes into a block.
* The standalone probe puts the original loops at different positions, which
  is why its timings cannot establish the unchanged executable's cause alone.

## Controlled alignment experiment

A diagnostic executable contains the ordinary and explicitly aligned gather
kernels, selected once per gather by `IBEX_DIAG_ALIGN_GATHER`. Clang's
[`code_align(32)` loop attribute](https://clang.llvm.org/docs/AttributeReference.html#clang-code-align)
changes placement. All corresponding selected-row loop bodies have identical
actual instruction-byte hashes. It introduces no per-value width branch or
mathematical operation.

Thirty position-balanced repeats, 25 timed iterations and two warmups resolve
arithmetic -0.83% (Holm p=0.0000230) and OR -0.32% (Holm p=0.0396). Simple and
AND are unresolved. All unchanged controls are unresolved.
Raw samples (`alignment-toggle.json`, local evidence). In this diagnostic the off-path Double
loops also start at offset 16; the pre-fix production Double loops were already
aligned. Thus this is a controlled alignment effect, not an exact measurement
of the eventual production patch's improvement.

The native diagnostic binary and source snapshot remain in
`build-arithmetic-filter/frozen/`. Builds and all raw stdout/perf logs are in
persistent ignored directories. No builds or profiling run during timing gates.


The 12-pair same-executable counter run (`alignment-counters.json`, local evidence) corroborates
a front-end effect: `idq_uops_not_delivered.core` falls 31.55% and `idq.mite_uops`
falls 59.56%, both Holm p=0.00342. Retired instructions **increase** 0.139% from
padding outside the selected-row loops, while those loops' actual bytes remain
identical. Branch misses increase 1.66% (Holm p=0.0371), so fewer mispredictions
do not explain this win. Timing under the profiler is unresolved; the timing
claim comes from the separate unprofiled 30-pair test.

## Rejected production candidate

The candidate adds only a capability-guarded `clang::code_align(32)` annotation
to the existing selected-row loop. Compilers without that attribute retain the
existing behavior. It adds no datatype case, widening, allocation policy, or
per-value operation. Full Release/plugin rebuild and both 2,488-case fast CTest
modes pass, as does the strict GCC runtime build.

The 30-pair production/original comparison (`production-original-gate.json`, local evidence)
leaves arithmetic at +0.67% against the original and +0.30% against its unchanged
replica; both are unresolved (p=0.382 and 0.658). All four candidate/base
comparisons are unresolved after Holm correction. This is a smaller estimate,
not evidence of equivalence or a claim that the whole historical gap is removed.
The same-executable alignment effect is established separately.

Candidate assembly (`production-loops.json`, local evidence) verifies every inspected gather
loop starts on a 32-byte boundary, with the same selected-row byte hashes as
before. Moving only the explanatory comment afterwards produces a byte-identical
native benchmark binary to the one measured in the original-baseline gate.


## Broad gate and decision

The 20-pair, 23-query before/after gate (`candidate-wide-gate.json`, local evidence) rejects the
local hint. Categorical join regresses **2.82%**, all 20 pairs slower, Holm
p=0.00204. Grouped head top-K regresses 0.31%, Holm p=0.0291. The arithmetic
estimate is -0.52% in this broader gate but is unresolved after correction.
The unchanged controls for those two regressions are unresolved.

Separate operator profiles (`join-profile.json`, local evidence) put the join increase in its
next time (25.70 → 26.10 ms), with identical 1,999,913 output rows and two calls.
The same-executable join check (`alignment-join-control.json`, local evidence) also finds a
smaller +0.42% (p=0.00664); its unchanged replica is unresolved. This is not a
clean negative control, and the join mechanism is not fully isolated. We do
not attribute the entire 2.82% cross-build change to one loop or padding alone.

The runtime hint was removed. The proposed five-line change remains reviewable
as [candidate-gather-alignment.patch](candidate-gather-alignment.patch), and the
counterfactual source remains in [alignment-diagnostic.patch](alignment-diagnostic.patch).
Both apply to `d05888c0` with `git apply --unidiff-zero`.
Diagnostic build hashes (`diagnostic-build.json`, local evidence), stage build provenance (`stage-build.json`, local evidence),
and conditions/checks (`conditions.json`, local evidence) identify the evidence.

The evidence supports code placement as a real performance component; it does
not explain the entire original 1–1.5% difference or justify merging this
narrow hint. No additional Int32 case or overflow-related operation was added
to the arithmetic/copy row loops. Subsequent work should isolate hot-kernel
placement while guarding join/TopK performance, rather than add dtype cases or
change the native allocator.

The compact-input performance sweep and additional candidate answer exports
were not run after the broad gate rejected the candidate. Candidate correctness
checks already passed (2,488 tests per ingestion mode, 133 Python tests per mode,
strict GCC and WebAssembly runtime builds, and standalone Arrow checks).
The active Release/native builds are restored to the unchanged runtime.

## Reproduce

The original focused run (from the repository root):

```bash
IBEX_CORES=1 IBEX_COMPACT_INT32=0 IBEX_PERFCMP_JOBS=6 \
benchmarking/compare_ibex_git.sh --base 99eaa20c --target d05888c0 \
  --ibex-suite filter --csv "$PWD/benchmarking/data/prices.csv" \
  --csv-trades "$PWD/benchmarking/data/trades.csv" \
  --interleave --replica-control --repeats 30 --warmup 3 --iters 25 \
  --taskset 2 --keep-temp
```

For the controlled alignment test, build the diagnostic patch in a separate
branch/worktree at `d05888c0`, with Clang Release `-march=native`, and run this
checkout's retained driver:

```bash
.venv/bin/python benchmarking/int32/aggregate-regressions/toggle_kernel.py \
  --binary /path/to/diagnostic/ibex_bench --suite filter \
  --csv-trades benchmarking/data/trades.csv \
  --queries filter_arith filter_simple filter_and filter_or \
  --switch IBEX_DIAG_ALIGN_GATHER --repeats 30 --iters 25 \
  --out benchmarking/results/local/int32/arithmetic-filter/alignment-rerun.json
```

Use `run_profiled.py --base <binary> --target <binary>` for unchanged-executable
counters; the alignment counter run uses the same diagnostic binary on both
sides plus `--switch IBEX_DIAG_ALIGN_GATHER --prefixes keep`. perf events and
full commands are retained in the reports and raw logs. The extra front-end
events are machine-specific; their names are recorded in `alignment-counters.json`.
