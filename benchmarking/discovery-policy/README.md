# Aggregate Discovery policy sweep

Build `ibex_bench_discovery_policy` in Release. The tool compares current defaults
with one runtime-configured policy, using prepared inputs and balanced paired
runs. The serial reference is first checked against the input's known group
order, counts and sums. Every run is compared cell-for-cell with that reference;
an input-row counter detects duplicated or lost source rows. Input creation and verification
are outside the measured interval. Wall time and process CPU time are recorded.

The policy is optional in `ExecutionContext::aggregate_discovery_policy` and
copied into the resolved aggregate phases. Its admission floor is derived before
row-estimate checks. Default row thresholds are unchanged. Zero row thresholds,
grains and upper cutoffs are rejected.

```bash
cmake --build build-release -j 6
IBEX_CORES=8 IBEX_DECODE_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
  python3 benchmarking/discovery-policy/sweep.py \
    --output-dir benchmarking/discovery-policy/results/pair \
    -- --rows 65536,262144 --cardinalities 64,131072 \
    --chunks 0,8192 --cores 1,8 --shape pair --aggregate sum \
    --owned-pair-rows 4096 --repeats 12
```

A chunk size of zero emits one whole-table chunk; positive sizes emit prepared
chunks. Cardinality is capped at the row count. Keys have a wide integer span so
the integer dense-array specialization does not hide discovery-policy effects.
Choose one of `--shape int`, `--shape pair`, `--shape packed`, or
`--shape generic`. Aggregate choices are
`count`, `sum`, and `both`; a single sum permits async/owned specializations,
while two aggregates exercise the general discovery paths.

Runtime policy options:

- `--radix-rows`, `--packed-rows`, `--owned-int-rows`, `--owned-pair-rows`,
  `--sorted-rows`: strategy admission floors.
- `--sorted-range-rows`, `--hot-job-rows`: grains, not admission floors.
- `--pair-max-first-chunk-rows`: whole-chunk cutoff for pair-owned strategies.

Use an invocation without policy options as a same-policy calibration. The
runner keeps CSV samples, command metadata, stderr and summaries under ignored
`results/`. Do not commit generated outputs. Read `MEASURING.md` before timing;
never run builds concurrently with a sweep.

`cpu_work_x` compares target-policy process CPU at the selected core count with
its one-core case. It screens work inflation; contention can also raise CPU
cost. Row counts and exact output checks establish input conservation, not the
number of intermediate probes or cache misses. Report unresolved effects as
unclear and independently confirm them before adjusting defaults.

## Accumulation and FinalOrdering

The same harness accepts `ExecutionContext::aggregate_phase_policy` overrides:

- `--accumulation-morsel-rows`, `--accumulation-sorted-rows`,
  `--accumulation-sorted-range-rows`, `--dense-gid-rows`.
- `--owned-range-rows`, `--owned-range-groups`, `--owned-range-morsel-rows`,
  `--owned-range-window-rows`, `--slot-init-bytes`.
- `--merge-groups`, `--merge-grain-groups`, `--ordered-runs`,
  `--ordered-grain-runs`.

`--key-fill-range-groups` controls Discovery's key-copy grain. Use
`--dense-keys` to exercise dense/private-slot accumulation, or `--ordered-keys`
for ordered runs and sorted ranges. The input generator and independent reference
check both handle these shapes. Fixed-policy reduction cuts remain independent
of core count; explicitly changing the morsel grain can change floating-point
rounding between policies. Use exact integer sums for policy comparisons.

For example, sweep private-slot accumulation without changing Discovery:

```bash
IBEX_CORES=8 IBEX_DECODE_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
  python3 benchmarking/discovery-policy/sweep.py \
    --output-dir benchmarking/discovery-policy/results/accumulation \
    -- --rows 20000,65536,262144 --cardinalities 8,64 --chunks 0,8192 \
    --cores 1,8 --shape int --aggregate both --dense-keys \
    --accumulation-morsel-rows 4096 --repeats 12
```

The ordered-prefix fixture covers the old plan-floor mismatch: a repeated first
8192 rows selects ordered-run compression, followed by unique ordered keys.
At 80000 rows it has 75904 runs, above the 65536-run kernel gate and below the
old 131072-input-row plan floor. Require cardinality `rows - prefix_rows / 2`:

```bash
IBEX_CORES=8 IBEX_DECODE_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
  python3 benchmarking/discovery-policy/sweep.py \
    --output-dir benchmarking/discovery-policy/results/ordered-prefix \
    -- --rows 80000 --cardinalities 75904 --chunks 0,8192 --cores 1,8 \
    --shape int --aggregate count --ordered-prefix-rows 8192 \
    --ordered-runs 131072 --repeats 16
```

The registered-table regression test checks the exact-estimate plan veto. This
prepared-source sweep has no confident estimate and isolates the merge cost.
In that invocation the default side admits the parallel merge, while the target
suppresses it to isolate its cost. Raw samples also record `fields` and
`finalizes` fan-out counts. Fused kernels can submit accumulation-authorized work
inside the Discovery profile stage; phase timing labels alone do not identify
which policy admitted that work.
