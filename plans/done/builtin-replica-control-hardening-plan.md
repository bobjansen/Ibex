# Builtin replica control and performance-comparison hardening

Status (2026-07-12): implemented.

## Motivation

The function-kind registry cleanup exposed two separate risks that had become
conflated:

- `BuiltinFn` must remain compact and internally consistent. A payload split
  temporarily grew entries from 40 to 48 bytes, while earlier AWS measurements
  associated a registry change with a large `fill_forward` regression.
- release binaries built from identical source at different temporary paths
  have produced materially different benchmark results. Runtime-repeat IQRs do
  not capture this build-persistent variation.

The first response added a 32-byte `BuiltinFn` size guard and a same-source
control build. The size guard is useful, but the control initially used one
same-source build delta as a statistical floor. One replica cannot
bound a layout distribution, and the fixed base/target/control run order also
mixes run-position drift into that delta.

## Goals

1. Make every builtin execution alternative valid by construction-time
   checking, not only correctly classified by `FnKind`.
2. Preserve the 32-byte `BuiltinFn` ceiling and the scalar-kernel ID design.
3. Make comparison builds independent of temporary source/build path strings
   where the GNU-compatible compiler supports prefix mapping.
4. Treat the same-source build as a diagnostic replica, never as evidence that
   clears a regression.
5. Balance run position across repeats so thermal or background drift is not
   systematically assigned to one side.
6. Report whether the two same-source benchmark binaries are byte-identical.

## Design

### Registry invariants

At registry construction, validate every entry:

- `infer` is non-null;
- `ScalarExec::eval` is non-null;
- `TransformExec::column_eval` and `GeneratorExec::column_eval` are non-null;
- only scalar entries may use `NullPolicy::Handles` or a `ScalarKernel`;
- every non-`None` scalar-kernel ID resolves to an evaluator;
- the existing registry-kind/IR-kind agreement remains enforced.

This closes the remaining illegal states left inside the typed variant. A
mistyped entry fails deterministically during registry initialization rather
than reaching a null indirect call.

### Reproducible comparison builds

For Clang/GCC-style comparison builds, add `-ffile-prefix-map` mappings for the
temporary source and build roots to stable virtual roots. Apply the same
canonical paths to every side. This removes accidental worktree names from
compiler-generated paths and macros without changing normal project builds.

When a replica is requested, compare the resulting `ibex_bench` files with
`cmp` and report `identical` or `different`. The result is metadata, not a
verdict input. A different binary means the build still contains an
uncontrolled input; an identical binary makes the replica a runtime-noise
control.

### Replica semantics and ordering

Use `--replica-control` as the option name. Enabling it implies interleaving.

For two sides, alternate `base,target` and `target,base`. For three sides,
rotate a Latin-square order:

1. `base,target,control`
2. `target,control,base`
3. `control,base,target`

Continue the cycle for additional repeats. Report the absolute replica delta
and replica IQR, plus a diagnostic relation such as `within-replica-delta`.
The base/target verdict remains determined by the existing runtime-IQR rule;
the replica never changes `regression` or `improvement` to noise.

## Non-goals

- Do not claim that one replica estimates a layout-noise floor.
- Do not implement linker-section randomization using pathname length.
- Do not add a seeded multi-layout verdict until the supported linker/compiler
  combination provides a controlled perturbation that can be paired across
  base and target builds.
- Do not change language semantics or public documentation.

## Validation

- `bash -n benchmarking/compare_ibex_git.sh`
- exercise help/argument validation and the generated run-order schedule;
- configure/build the current tree with warnings enabled;
- run focused builtin/interpreter tests, then the full test suite;
- run a small release comparison with the replica enabled when build time and
  local resources permit, recording binary identity and report columns;
- `git diff --check` and clang-format checks on touched C++ files.

## Completion criteria

- Invalid registry payloads fail at registry construction with the builtin name
  in the error.
- `BuiltinFn` remains at most four pointers wide.
- replica mode cannot run as three unbalanced blocks;
- control data is visible in the report but cannot suppress a regression;
- comparison output states whether same-source binaries matched;
- the plan status and `plans/README.md` reflect the validation actually run.

## Outcome

- Registry construction now validates arity metadata, inference and execution
  pointers, scalar-only metadata placement, scalar-kernel resolution, aggregate
  IDs, and the existing IR/runtime kind agreement. `BuiltinFn` remains within
  its 32-byte ceiling.
- `--replica-control` enables replica mode, implies interleaving, and rotates
  `base,target,control`, `target,control,base`, and `control,base,target`.
- Comparison builds use stable `-ffile-prefix-map` roots with Clang/GCC-style
  drivers. A three-build `HEAD` vs `HEAD` validation produced byte-identical
  base and replica `ibex_bench` binaries.
- The report now carries `replica_delta_ms`, `replica_iqr_ms`, and
  `replica_relation`. These fields never alter the runtime-IQR verdict.
- Worktree creation failures now propagate, and state-directory resolution no
  longer hides cleanup bookkeeping inside command-substitution subshells.

Validation performed:

- `bash -n benchmarking/compare_ibex_git.sh` and help/argument checks;
- focused `[null][fill]`: 11 cases, 94 assertions passed;
- debug `ibex_tests` build and Release `ibex_bench` build passed;
- full CTest: 995 tests passed directly; the parity test initially hit the
  environment's unsupported LeakSanitizer-under-ptrace mode, then passed alone
  with `LSAN_OPTIONS=detect_leaks=0`;
- three-sided `merge_validity` smoke comparison, 3 balanced repeats: binary
  identity `identical`, expected run order observed, diagnostic report columns
  populated, all base/target verdicts retained;
- invalid-target failure-path check confirmed temporary base-worktree cleanup;
- clang-format dry run and `git diff --check` passed.
