# I9: worker ceilings and async admission

The operator inventory is in `src/runtime/PARALLELISM.md`. This probe tests
whether asynchronous aggregate Discovery respects its resolved worker ceiling.
It uses prepared integer columns and the existing Discovery sweep's independent
reference checks and exact input-row accounting. There is no scan decoder in
the timed source, so decoder work cannot explain an excess.

```bash
cmake --build build-release --target ibex_bench_discovery_policy -j 6
mkdir -p benchmarking/worker-caps/results
IBEX_CORES=8 IBEX_DECODE_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
  build-release/tools/ibex_bench_discovery_policy \
    --rows 1048576 --cardinalities 64 --chunks 0,65536 --cores 1,2,4,8 \
    --shape int --aggregate sum --hot-job-rows 65536 --profile-workers --repeats 16 \
    > benchmarking/worker-caps/results/integer.csv
python3 benchmarking/worker-caps/summarize.py benchmarking/worker-caps/results/integer.csv
```

`--profile-workers` adds `discovery_pool_ms` and `pool_threads` to the raw CSV.
Other timing runs can omit profiling to avoid its overhead. The original wall,
CPU, row and output checks remain. Whole-table jobs can be split with the hot-job
grain; streamed chunks already produce independently submitted jobs with the
production grain.

Also test the ordinary pool policy rather than forcing a large pool:

```bash
env -u IBEX_DECODE_THREADS IBEX_CORES=2 IBEX_DECODE_SATURATION=8 \
  taskset -c 0,2,4,6,8,10,12,14 \
  build-release/tools/ibex_bench_discovery_policy \
    --rows 1048576 --cardinalities 64 --chunks 65536 --cores 2 \
    --shape int --aggregate sum --profile-workers --repeats 16 \
    > benchmarking/worker-caps/results/default-pool.csv
python3 benchmarking/worker-caps/summarize.py benchmarking/worker-caps/results/default-pool.csv
```

For pair sums use `--shape pair --rows 4194304 --cardinalities 64
--chunks 262144,1048576 --cores 2 --aggregate sum --profile-workers` with a
known eight-thread pool. Both sides may have identical effective policy; this
is a concurrency audit, not a claimed speed optimization.

## Reading the bound

Let W be the sum of worker wall intervals attributed to Discovery and Q the
complete query wall interval. These jobs finish inside Q. Thus peak concurrent
Discovery work is at least `ceil(W/Q)`. The profiler subtracts nested task and
parked time; this fixture's async hot/pair callbacks do no nested pool work.
Serial preparation, finalization and emission make Q larger, so this is a
conservative bound, not a direct measurement of the peak. It does not depend
on counting the calling thread as a worker. Process CPU alone cannot provide
that distinction.

The benchmark uses the standard Discovery ceiling of 64, an unknown source
row estimate and no custom worker-cap override. Its expected cap is therefore
`min(query budget, actual pool size, 64)`, with one-core execution serial.
A bound above that cap proves an enforcement gap. Lack of an excess does not
prove the cap works: short jobs, source preparation, or the final merge can
hide a brief overrun. Preserve raw results and metadata in ignored `results/`.

`TaskGroup` itself is deliberately generic and has no concurrency argument.
Elastic pipeline producers enforce their own active-worker count. Async sums
currently use the phase cap to choose partition count, then submit independent
jobs without a corresponding active-worker bound. A future fix should bound
admission or draining worker lanes without parking pool threads on a semaphore;
it must preserve job boundaries, deterministic merging, exception handling and
stream overlap. Do not change fixed reduction cuts to enforce worker limits.
