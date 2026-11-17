# I7: width and parallel copy admission

Build `ibex_bench_width` in Release. It compares serial and parallel execution
in balanced pairs over rows and logical Int64 column counts. Every output value
and its row order are checked against the input generator, outside timing.
The runtime may retain compact storage for the small payload integers.

The harness enables the normal huge-page policy and enters through `interpret`
once before measurement to initialize the normal allocator policy. Set
`IBEX_ALLOCATOR_POLICY` before launching to compare another allocator policy.
Prepared inputs, IR, plans and index arrays are outside timed intervals.

```bash
cmake --build build-release --target ibex_bench_width -j 6
mkdir -p benchmarking/width-policy/results
IBEX_CORES=8 IBEX_DECODE_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
  build-release/tools/ibex_bench_width --mode emit \
    --rows 65536,131072,262144 --widths 2,6,16 --cores 8 --repeats 20 \
    > benchmarking/width-policy/results/emit.csv
python3 benchmarking/width-policy/summarize.py benchmarking/width-policy/results/emit.csv
```

Modes:

- `emit`: one unique key per input row and one integer sum per payload column.
  Both sides keep the same query budget, Discovery, Accumulation and
  FinalOrdering. Only Emission's physical worker ceiling changes, from one to
  `--cores`. `ms` measures the `Aggregate.Emission` profile stage; `total_ms`
  and `cpu_ms` cover the complete query including its other phases.
- `gather`: call the payload gather used by joins/reshaping directly, with
  reverse indices and known output values. The serial callback cannot fan out.
  `--keep-every N` selects every Nth input row in reverse order; admission work
  would be output rows times jobs, rather than input rows times table width.
- `project`: the same filter followed by `select { k, v1 }`, always returning
  two columns. Here `width` records input width. `pipelines` records actual
  parallel map pipelines and distinguishes admission from other work that might
  submit pool tasks.
- `filter`: execute `input[filter v1 >= 50]` through its physical map plan,
  returning roughly 51.5% of the input rows and all columns. `ms` and
  `total_ms` both measure the complete filter query. A profile scope records
  the actual submitted tasks.

For calibration use `--cores 1`: both sides then execute serially. Repeat the
selected effects independently after the exploratory matrix. The row admission
floor is disabled to compare the kernels' costs; Emission's copied plan also has
a minimal row floor. This does not alter runtime defaults. Default row counts
clear the ordinary 65536-row admission floor.

`cpu_ms` is process CPU, and its paired ratio includes dispatch and contention.
For Emission it cannot isolate that phase's CPU from the rest of the aggregate.
`pool_ms` is summed worker wall time, not process CPU. Output checks establish
result conservation, not the number of intermediate reads. Examine inflated CPU
before attributing it to redundant work.

These fixtures do not cover variable-width strings, nulls, complex predicates,
arbitrary join-index locality, or overlapping queries. A profitable narrow
fixed-width case is evidence about that case only; it does not establish new
production thresholds.
Keep raw CSV, summaries, logs and command metadata under ignored `results/`.

These probes are why the shared cell floor (`parallel_min_cells`) was removed
on 2026-11-17: every measured narrow case was faster in parallel, including
the 131,072-row, two-column shape that floor was written to refuse. A
projection probe with otherwise identical output:

```bash
IBEX_CORES=8 IBEX_DECODE_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
  build-release/tools/ibex_bench_width --mode project --rows 131072 \
    --widths 2,6,16 --cores 8 --repeats 20 \
    > benchmarking/width-policy/results/project.csv
python3 benchmarking/width-policy/summarize.py benchmarking/width-policy/results/project.csv
```
