# AWS benchmark harness

Run the ibex scale-benchmark suite on EC2 against a clean, reproducible box and
pull the results back as a CSV. Three layers:

| Script               | Runs where | What it does |
|----------------------|------------|--------------|
| `setup.sh`           | once, local | Creates the S3 bucket, IAM role/profile and security group. |
| `build-ami.sh`       | local       | Bakes a reusable AMI (toolchain + R + uv + a warm ibex/Arrow build). Repeatable. |
| `run.sh`             | local       | One instance runs the **whole** suite. |
| `run-per-engine.sh`  | local       | **One instance per engine**, in parallel, then combines results. |
| `compare-git.sh`     | local       | A/B **two git commits** of ibex on one clean box (low-noise perf verdict). |
| `bootstrap.sh`       | on instance | Provision (if needed) → build ibex → run suite (or compare) → upload → self-terminate. |
| `lib.sh`             | sourced     | Shared helpers (config, AMI resolution, user-data builder). |

All scripts read `S3_BUCKET` / `AWS_REGION` / `IBEX_AMI` from `.config` (written
by `setup.sh` and `build-ami.sh`); override via env or `--region`.

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
side runs second).

```bash
git push                                            # both commits must be on origin
./benchmarking/aws/compare-git.sh                   # HEAD~1 vs HEAD, all suites
./benchmarking/aws/compare-git.sh --base v0.3.0 --target HEAD
./benchmarking/aws/compare-git.sh --suite sort,groupagg,join --repeats 7
```

The instance regenerates the (untracked) 4M-row benchmark CSVs so both commits
read identical inputs, runs the comparison, uploads the report, and self-
terminates. The report (per-query base/target/delta + verdict, plus a summary
with geometric-mean speedup) is printed locally and saved to
`benchmarking/results/compare_aws_<timestamp>.txt`.

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

Key options: `--base/--target REF`, `--suite a,b,c`, `--repeats N` (default 5),
`--iters N`, `--data-rows N` (default 4000000), `--serial` (disable
interleaving), `--taskset CPUSET` (default `2-3`), `--type` (default: auto),
`--on-demand`. A 4M run is typically 15-30 min and well under $0.20; a 32M run
is slower and on a bigger box, so budget more.

> Locally, `benchmarking/compare_ibex_git.sh --interleave` gives the same
> drift-cancelling A/B without EC2 — just noisier on a shared/thermal-throttling
> box.

## Watching a run

```bash
# Live partial progress (single-box run.sh):
aws s3 cp s3://<bucket>/benchmarks/<ts>_<commit>/scales.partial.csv - | column -t -s,

# Per-instance console (printed by the runners on launch):
aws ec2 get-console-output --instance-id <id> --region <region> --latest --output text
```

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
