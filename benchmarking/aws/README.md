# AWS benchmark harness

Run the ibex scale-benchmark suite on EC2 against a clean, reproducible box and
pull the results back as a CSV. Three layers:

| Script               | Runs where | What it does |
|----------------------|------------|--------------|
| `setup.sh`           | once, local | Creates the S3 bucket, IAM role/profile and security group. |
| `build-ami.sh`       | local       | Bakes a reusable AMI (toolchain + R + uv + a warm ibex/Arrow build). Repeatable. |
| `run-all.sh`         | local       | **Orchestrator**: launches every website suite in parallel under one run id + index manifest. |
| `run.sh`             | local       | One instance runs the **whole** suite. |
| `run-per-engine.sh`  | local       | **One instance per engine**, in parallel, then combines results. |
| `run-thread-scaling.sh` | local    | One box, the suite once per thread count — how each engine converts cores into throughput. |
| `run-tpch.sh`        | local       | One instance runs the TPC-H/PDS-H quartet and downloads a TSV artifact. |
| `run-window-ohlc.sh` | local       | One instance runs the window-OHLC suite (Ibex/Polars/DuckDB) and downloads a TSV artifact. |
| `run-r-only.sh`      | local       | One 8-core instance runs the R-only suite (data.table/dplyr/ibex-r) across a size sweep. |
| `compare-git.sh`     | local       | A/B **two git commits** of ibex on one clean box (low-noise perf verdict). |
| `bisect-git.sh`      | local       | Single-instance performance `git bisect` for one benchmark query. |
| `compare-compilers.sh` | local     | A/B latest **Clang vs GCC** full Ibex builds for one commit. |
| `bootstrap.sh`       | on instance | Provision (if needed) → build ibex → run suite (or compare) → upload → self-terminate. |
| `lib.sh`             | sourced     | Shared helpers (config, AMI resolution, user-data builder, topology, manifests). |

All scripts read `S3_BUCKET` / `AWS_REGION` / `IBEX_AMI` from `.config` (written
by `setup.sh` and `build-ami.sh`); override via env or `--region`.

## vCPUs are hyperthreads

Read this before quoting a core count. On every current x86 instance family a
vCPU is one SMT thread, so an instance advertises **twice** the physical cores
it has:

| Type          | vCPU | Physical cores | RAM |
|---------------|-----:|---------------:|----:|
| `r7i.2xlarge` |    8 |          **4** |  64 GiB |
| `r7i.4xlarge` |   16 |          **8** | 128 GiB |
| `r7i.8xlarge` |   32 |         **16** | 256 GiB |

Every runner now prints the real topology in its launch banner and records it in
a manifest, because "8 cores" and "8 vCPU on 4 cores" are different claims and
only one of them is true of the published numbers.

`run-thread-scaling.sh --threads-per-core 1` launches with SMT **disabled**
(`CpuOptions`), which is how you measure physical-core scaling without a sibling
thread quietly doing part of the work.

## Provenance manifests

Every runner writes a `*.manifest.json` next to its artifact recording commit,
branch, UTC timestamp, region, instance type, vCPU/physical-core/threads-per-core
counts, and the suite's own settings. It is written at **launch**, so a run that
dies is still attributable. A published chart whose numbers cannot be traced to
a commit and a box is a screenshot, not a measurement.

## 0. Run everything (orchestrator)

```bash
git push
./benchmarking/aws/run-all.sh                       # main + window-ohlc + thread-scaling
./benchmarking/aws/run-all.sh --suites main
./benchmarking/aws/run-all.sh --suites thread-scaling --scaling-threads 1,2,4,8,16
```

Launches each suite on its own box **in parallel** and waits for all of them,
under one run id and one index manifest at
`benchmarking/results/runs/<timestamp>_<commit>/manifest.json`. That is what
makes the website's pages citable as one result rather than three measurements
that happen to sit near each other. One suite failing does not abandon the
others; per-suite logs land in the same directory.

Two tiers, deliberately. `main` carries every engine and is the headline
cross-engine comparison. The deep dives (`window-ohlc`, `thread-scaling`) drop
pandas, dplyr and rivals' `-st` variants: they exist to answer one question
well, and an engine that loses by 50x on every cell is a column nobody reads.

## 1. One-time setup

```bash
AWS_REGION=us-east-1 ./benchmarking/aws/setup.sh
```

## 2. Bake the AMI (repeatable)

```bash
git push                                   # the AMI baseline must be on origin
./benchmarking/aws/build-ami.sh            # ~10-20 min; saves IBEX_AMI to .config
```

`build-ami.sh` launches a builder from the stock Ubuntu image, runs
`bootstrap.sh` in **provision-only** mode (installs the clang-21/cmake toolchain,
R + data.table/dplyr, uv + the Python engines, and — by default — builds ibex
once so the heavy Arrow/FetchContent tree is baked in), then snapshots the
stopped instance into an AMI and records its id in `.config`.

After this, both runners boot from the baked AMI automatically and skip
provisioning on every instance — the big win when fanning out per engine.

Options:

- `--no-prebuild` — thin AMI (deps + warmed caches only, no baked build).
- `--commit REF` — baseline ref to prebuild (default `HEAD`; must be pushed).
- `--type`, `--name`, `--region`, `--key`, `--keep-instance`.

Re-run any time (e.g. after a toolchain bump) to refresh the AMI.

> Without an AMI the runners still work — they boot stock Ubuntu and provision
> on the instance, just slower. The AMI is an optional speed-up, not required.

## 3a. Run the whole suite on one box

```bash
./benchmarking/aws/run.sh                              # 1M..50M, spot
./benchmarking/aws/run.sh --on-demand --sizes 1M,4M,16M
```

Result → `benchmarking/results/scales_aws_<timestamp>.csv`.

## 3a.1 TPC-H/PDS-H quartet

```bash
git push
./benchmarking/aws/run-tpch.sh --on-demand --sf 1 --warmup 1 --iters 5
```

This runs Ibex and this repository's Polars implementation in default and
single-threaded modes, plus the upstream Polars PDS-H Polars implementation in
both modes and its DuckDB SQL. The runner pins the upstream PDS-H revision and
downloads `benchmarking/results/tpch_aws_<timestamp>.tar.gz`; extract it to get
one TSV per framework and a `versions.txt` manifest. Use `--sf 1,10` to run
multiple scale factors sequentially. SF-10 needs materially more disk/RAM; use
`--type r7i.4xlarge` when in doubt.

## 3a.2 Window-OHLC (Ibex vs Polars vs DuckDB)

```bash
git push
./benchmarking/aws/run-window-ohlc.sh --on-demand
```

Rolling open/close bars per time window per symbol, on identical
Ibex-generated Parquet. Two sweeps, because the query has two independent
scaling axes: **symbol count** at a fixed row count (`--symbols`, default
`3 8 20 100` at `--sweep-rows 20000000`) and **row count** at a fixed symbol
count (`--rows`, default `5000000 20000000 50000000` at `--sweep-symbols 3`).

Every engine is pinned to the same cores with `taskset` and given the same
thread budget (`IBEX_CORES`, `POLARS_MAX_THREADS`, DuckDB `PRAGMA threads`),
so a row reads as "this engine on N cores" rather than "this engine on
whatever pool it chose for itself" — which is the whole reason to run it on a
clean box rather than a laptop. `--cores "8 16 32"` repeats both sweeps at each
core count for a scaling curve; the default uses every vCPU on the instance.

Default instance is `m7i.8xlarge` (32 vCPU / 128 GiB): the memory is sized so a
50M-row frame fits in three engines at once. Downloads
`benchmarking/results/window_ohlc_aws_<timestamp>.tar.gz` — one TSV per sweep
per core count, plus a `versions.txt` recording the instance type, core count
and engine versions. Data files are generated on the box and are not part of
the artifact.

## 3a.4 Thread scaling (how cores turn into throughput)

```bash
git push
./benchmarking/aws/run-thread-scaling.sh --on-demand
./benchmarking/aws/run-thread-scaling.sh --threads 1,2,4,8,16 --rows 16M
./benchmarking/aws/run-thread-scaling.sh --type r7i.8xlarge --threads 1,2,4,8,16,32
./benchmarking/aws/run-thread-scaling.sh --threads-per-core 1 --threads 1,2,4,8
```

One box, one dataset size, the **whole suite run once per thread count**. The
per-query curve across the `threads` column is the deliverable: it separates the
queries that scale from the ones that are still serial, which a single geomean
cannot do. Default box is `r7i.4xlarge` (16 vCPU / **8 physical cores**) —
double the physical cores of the published `r7i.2xlarge`.

Varying the *thread budget* on a fixed box, rather than varying the instance
size, is the point: changing instance type confounds software scaling with
memory bandwidth, sustained clocks and NUMA. Holding the box fixed makes the
answer a property of the engines.

Every engine gets the same budget at each point (`run_scale_suite.sh --threads`
sets `IBEX_CORES`, `POLARS_MAX_THREADS`, `RAYON_NUM_THREADS`, `OMP_NUM_THREADS`,
`R_DATATABLE_NUM_THREADS` and the SQL harnesses' `--threads`), and each pass is
pinned to cores `0..T-1`. Linux numbers one thread per physical core first, so
the low half of the sweep is physical cores and the SMT knee stays visible
instead of smeared.

Engines default to `ibex,python,duckdb` — the three that thread and whose curves
are comparable. The `-st` variants are skipped because **`threads=1` is that
measurement**; running both would be the same number under two names.

The launcher refuses a sweep point larger than the box's vCPU count *before*
launching, and warns when a sweep tops out below the physical core count.
Downloads `benchmarking/results/thread_scaling_aws_<timestamp>.csv` plus a
`.box.txt` of `lscpu`-derived facts (the box, not the launcher, is the authority
on core counts once `CpuOptions` is in play) and a `.manifest.json`.

## 3a.3 R-only (data.table vs dplyr vs ibex-r)

```bash
git push
./benchmarking/aws/run-r-only.sh
```

The three R-facing paths over identical fixtures: `data.table`, in-memory
`dplyr`, and Ibex's native lazy dplyr backend. R against R, in one process,
with no engine reading a different file format — the comparison the website's
`data.table` column cannot make.

Sizes sweep `1M,2M,4M,8M,16M,32M` (`--sizes`), five timed iterations after one
warmup (`--iters`, `--warmup`).

**Eight cores, deliberately** (`--cores`, default 8). The three frameworks
scale differently with thread count, so an unpinned box turns a query
comparison into a thread-count comparison. `taskset` bounds the process and
`IBEX_CORES`/`OMP_NUM_THREADS` stop Ibex's worker pool and data.table's
OpenMP pool from each sizing themselves from `nproc` and oversubscribing the
pinned set.

The instance type is therefore chosen for **memory**, not cores: the default
`r7i.2xlarge` is 8 vCPU / 64 GiB, because 32M rows has to fit in three
frameworks at once and the `events`+`users` phase holds a 32M-row join output
in both R and Ibex. `--type m7i.2xlarge` (8 vCPU / 32 GiB) is cheaper and fine
up to 16M.

One size is run per invocation of `run_r_only.sh`, which buys two things: the
artifact is refreshed under a separate **partial** key after every size, so an
interrupted sweep still yields everything that finished; and each size's
fixtures are deleted before the next is generated, which is what keeps a 32M
sweep inside the box's disk. A size that fails is logged and skipped rather
than discarding the sizes below it.

Downloads `benchmarking/results/r_only_aws_<timestamp>.tar.gz`: one TSV per
size, a `combined.tsv` carrying a `dataset_rows` column, and a `versions.txt`
recording the instance type, core count and R/data.table/dplyr versions.
AWS provisioning installs R from CRAN's Ubuntu repository (R 4.4 or newer),
because Ubuntu 24.04's stock R 4.3 cannot install the Ibex R package.
The launcher waits for a separate completion marker, so a bootstrap failure
cannot be reported as a completed archive; on failure it instead recovers the
latest partial archive. `versions.txt` records the runner exit code, stage,
and any sizes that failed without discarding earlier measurements. Bootstrap
failures also include `failure.txt` and the final 200 lines of the instance
log as `failure.log` in that partial archive.
Analyse it with Ibex itself — `benchmarking/analyze_r_only.ibex` reads a
`r_only.tsv` and prints the win/loss table.

## 3b. Run one instance per engine (parallel, isolated)

```bash
./benchmarking/aws/run-per-engine.sh                          # all engines
./benchmarking/aws/run-per-engine.sh --engines ibex,duckdb,r  # a subset
./benchmarking/aws/run-per-engine.sh --on-demand --sizes 1M,4M,16M
```

Each engine gets a whole box to itself (no cross-engine memory pressure or
thread contention) and they run concurrently, so wall-clock is roughly the
slowest single engine rather than the sum. Each instance runs the suite with
only its engine enabled, uploads its slice, and self-terminates;
`run-per-engine.sh` polls all of them and concatenates the slices into one
`benchmarking/results/scales_aws_<timestamp>.csv` (same shape as `run.sh`).

Engine workers also avoid unrelated native build work: only the `ibex` worker
configures CMake, and it builds only the `ibex_bench` target. Python, R,
DuckDB, DataFusion, ClickHouse, and SQLite workers skip the Ibex build entirely.
Downloaded per-engine slices live in a run-specific directory so an interrupted
run cannot accidentally reuse a result file from an earlier sweep.

Engine groups (each = one instance):

| `--engines` token | Frameworks produced |
|-------------------|---------------------|
| `ibex`            | ibex (+ ibex+parse) |
| `python`          | pandas, polars, polars-st |
| `r`               | data.table, dplyr |
| `duckdb`          | duckdb, duckdb-st (all sizes) |
| `datafusion`      | datafusion, datafusion-st |
| `clickhouse`      | clickhouse, clickhouse-st |
| `sqlite`          | sqlite (off by default) |

Default set: `ibex,python,r,duckdb,datafusion,clickhouse`.

## 3c. Compare two git commits (low-noise A/B)

`compare-git.sh` answers "did this commit change ibex's performance?" on a
dedicated, idle, fixed-clock box — the noise floor a laptop or WSL2 can't reach.
It runs the same `compare_ibex_git.sh` A/B as locally, but on EC2: both commits
are built and timed on the **one** instance, repeats **interleaved** (base and
target alternate, so slow machine drift cancels instead of biasing whichever
side runs second). Runs default to one logical CPU (`--taskset 2`) so the
single-threaded Ibex process cannot migrate between cores during a sample.

```bash
git push                                            # both commits must be on origin
./benchmarking/aws/compare-git.sh                   # HEAD~1 vs HEAD, all suites
./benchmarking/aws/compare-git.sh --base v0.3.0 --target HEAD
./benchmarking/aws/compare-git.sh --suite sort,groupagg,join --repeats 7
./benchmarking/aws/compare-git.sh --base v0.3.0 --target HEAD --replica-control
./benchmarking/aws/compare-git.sh --base v0.3.0 --target HEAD --suite fill --artifacts
```

The instance regenerates the (untracked) 4M-row benchmark CSVs so both commits
read identical inputs, runs the comparison, uploads the report, and self-
terminates. The report (per-query base/target/delta + verdict, plus a summary
with geometric-mean speedup) is printed locally and saved to
`benchmarking/results/compare_aws_<timestamp>.txt`.

Pass `--artifacts` to also download the exact unstripped `ibex_bench` binaries
built on the instance, along with hashes, CPU/toolchain details, and dynamic
library metadata. The archive is written beside the report as
`benchmarking/results/compare_aws_<timestamp>_artifacts.tar.gz`.

Run at scale to check the wins hold as the working set leaves cache:

```bash
./benchmarking/aws/compare-git.sh --base 2dcbd58 --target HEAD --data-rows 16000000
./benchmarking/aws/compare-git.sh --base 2dcbd58 --target HEAD --data-rows 32000000
```

`--data-rows` auto-sizes the instance (the reshape/group benchmarks are the RAM
ceiling — ~28GB at 16M): ≤4M → `c7i.2xlarge` (16GB), ≤16M → `r7i.2xlarge`
(64GB), ≤32M → `r7i.4xlarge` (128GB). `--type` overrides. Each size is its own
instance, so 16M and 32M can run concurrently.

Scale runs (>4M) also default to **on-demand** — they're long enough that a spot
reclaim near the end is worse than the ~$0.50 extra (the short 4M run stays spot,
where a reclaim is cheap to retry). Force either with `--on-demand` / `--spot`.

Sampling defaults are lean (`--repeats 3 --iters 5`) because the dedicated box
barely drifts — that's enough to pin a verdict. Bump them only if a specific
delta looks marginal; the cost is real at scale (the suite runs
`(1+iters)×repeats×2` times, or `×3` with replica control).

Key options: `--base/--target REF`, `--suite a,b,c`, `--repeats N` (default 3),
`--iters N` (default 5), `--data-rows N` (default 4000000),
`--replica-control` (third same-source build with balanced run positions),
`--serial` (disable interleaving when no replica is requested), `--taskset
CPUSET` (default `2-3`), `--type` (default: auto), `--on-demand`. A 4M run is
typically 15-30 min and well under $0.20; a 32M run is slower and on a bigger
box, so budget more.

> Locally, `benchmarking/compare_ibex_git.sh --interleave` gives the same
> drift-cancelling A/B without EC2 — just noisier on a shared/thermal-throttling
> box.

## 3d. Bisect a Performance Regression

`bisect-git.sh` runs `git bisect` on one EC2 instance instead of launching a
fresh machine for every candidate. For each candidate it compares the fixed
known-good commit against that candidate with `compare_ibex_git.sh`, then marks
the candidate bad when the selected query's `delta_pct` is at or above the
threshold.

Example for a fill-forward regression:

```bash
git push
./benchmarking/aws/bisect-git.sh \
  --good a778f43dd0d6ce15223867ee5be1cbc9664adc28 \
  --bad HEAD \
  --suite fill \
  --query fill_forward \
  --threshold-pct 10 \
  --repeats 7 \
  --iters 9 \
  --on-demand
```

The final report is saved to
`benchmarking/results/bisect_aws_<timestamp>.txt` and includes every candidate
comparison plus the `git bisect log`.

## 3e. Compare latest Clang vs latest GCC

`compare-compilers.sh` answers "does the C++ compiler matter for Ibex?" for a
single commit. The EC2 instance builds the benchmark-relevant Ibex C++ targets
twice in Release mode with `-DIBEX_ENABLE_MARCH_NATIVE=ON`: the compiler,
runtime, IR, core, and their dependencies. Optional plugin/test/example/Python
targets are skipped so the comparison is about the generated-query execution
path, not Arrow/parquet/LightGBM build cost.

- latest configured Clang (`clang++-21` from apt.llvm.org)
- latest versioned GCC available from Ubuntu's toolchain apt source

It then regenerates the benchmark CSVs and compiles/links each generated query
against the matching compiler's build tree. The generated query translation
units use aggressive native flags by default:

- Clang: `-O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native -flto=thin -fuse-ld=lld` when supported
- GCC: `-O3 -DNDEBUG -std=gnu++23 -march=native -mtune=native -flto=auto` when supported

Run it:

```bash
git push
./benchmarking/aws/compare-compilers.sh
./benchmarking/aws/compare-compilers.sh --data-rows 16000000 --on-demand
./benchmarking/aws/compare-compilers.sh --repeats 5 --iters 9 --taskset 2
```

The report is printed locally and saved to
`benchmarking/results/compare_compilers_aws_<timestamp>.txt`. Per-query rows
show `gcc_delta_pct` relative to Clang; `gcc_vs_clang_x > 1` means GCC was
faster. The summary includes total, median, and geometric-mean GCC-vs-Clang
ratios.

Use this result to separate two questions:

- Compiler sensitivity: large query-specific deltas point at generated C++ or
  runtime code patterns that one compiler optimizes and the other misses.
- Ibex codegen quality: if both compilers are slow on the same query family,
  inspect the generated C++ shape rather than blaming one backend.

## Watching a run

```bash
# Live partial progress (single-box run.sh):
aws s3 cp s3://<bucket>/benchmarks/<ts>_<commit>/scales.partial.csv - | column -t -s,

# Per-engine (run-per-engine.sh) — one partial key per engine:
aws s3 cp s3://<bucket>/benchmarks/<ts>_<commit>/per-engine/<engine>/scales.partial.csv - | column -t -s,

# Per-instance console (printed by the runners on launch):
aws ec2 get-console-output --instance-id <id> --region <region> --latest --output text
```

### Partial results are returned, not just written

The box syncs its in-progress CSV to a `*.partial.csv` key every 60s (a
separate key on purpose — a partial written to the final key would make the
poll loop declare the run finished). Both runners now FALL BACK to that key
when the final upload never happens: a spot reclaim, a failed bootstrap, an
OOM at a large size, or the runner's own 6h timeout. `run-per-engine.sh` does
this per engine, so one engine dying at 50M no longer discards the sizes it
did finish, and labels each engine `complete` or `PARTIAL` in its summary.

**A partial engine is missing its LARGEST sizes** — which is exactly where
cross-engine comparisons get drawn. Check which sizes each engine reached
before reading a scaling curve across them.

## Notes

- Instances self-terminate on completion **or** failure (uploading partial
  results first), and use the IAM instance profile for the result upload — no
  credentials are baked into the AMI.
- `bootstrap.sh` reuses the baked `build-release` incrementally: ninja rebuilds
  only the ibex objects that changed since the AMI's baseline commit; the
  version-pinned Arrow tree is reused.
- Toolchain versions (`CLANG_VERSION`, `CMAKE_VERSION`) live in both
  `bootstrap.sh` and `install-deps.sh` — keep them in sync when bumping, then
  rebuild the AMI.
- Spot is the default (cheapest); `--on-demand` avoids capacity-reclaim risk on
  long full-suite runs.
