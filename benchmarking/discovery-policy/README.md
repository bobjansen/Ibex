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
