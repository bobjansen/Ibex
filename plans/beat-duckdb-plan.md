# Beating DuckDB multi-core

Status: **proposed, parked until ADBC support is finished** (decided
2026-09-27). This plan sets the target and ranks the work; nothing in it is
started. It is the successor target to `beat-polars-plan.md`: on PDS-H SF-10 on
the AWS benchmark box, Ibex is now ahead of Polars at 8 and 16 cores (total
0.67 and 0.64), and **DuckDB is the faster of the two reference engines**.

**Where Ibex stands (SF-10, 16 physical cores):** ahead of DuckDB on one core
(0.86 total), level at 8 cores (1.02 total, geomean 1.00), behind at 16 (1.17
total, geomean 1.13). The gap is multi-core scaling, not per-core speed: 13 of
the 17 queries Ibex loses at 16 cores it wins or ties on one core.

## 1. Baseline

PDS-H **SF-10**, one AWS `r7i.8xlarge` with SMT disabled
(`--threads-per-core 1`): 16 physical cores (Xeon Platinum 8488C), 256 GiB.
Commit `546ce652` (clean), run with default parameters:

    ./benchmarking/aws/run-tpch.sh --on-demand --type r7i.8xlarge --sf 10 \
        --threads-per-core 1 --cores 8,16

All engines `taskset` to cores `0..N-1`, min of 5 after 1 warm-up. The answer
check against Polars passed on all 22 queries before timing (q11 has tied rows,
compared as sets since `c72a9185`). Data is polars-benchmark's own (tpchgen-cli,
then Polars' Parquet writer: 122,880-row row groups, ZSTD). Polars ran
in-memory. Versions: DuckDB 1.5.4, Polars 1.42.1. Artifact
`benchmarking/results/tpch_aws_20260927T160251.tar.gz`
(`s3://…/benchmarks/20260927T160251_546ce652/`), runs
`20260927T16{3217,5508}Z_546ce652_sf10`.

The two sittings drifted: the 16-core sitting ran every engine slower (Ibex's
1-core suite 55.5 s against 49.8 s, DuckDB 61.7 s against 60.0 s). The 1-core
row below is the median of the two sittings' 1-core rows. **Compare engines
within a sitting, never Ibex against an earlier Ibex run.**

| cores | Ibex ms | DuckDB ms | Polars ms | Ibex/DuckDB | geomean | Ibex/Polars | Ibex scaling | DuckDB scaling | Ibex fraction | DuckDB fraction |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 52,638 | 60,855 | 76,717 | **0.86** | 0.84 | 0.69 | | | | |
| 8 | 8,189 | 8,031 | 12,186 | **1.02** | 1.00 | 0.67 | 6.43× | 7.58× | 96.5% | 99.2% |
| 16 | 5,749 | 4,915 | 8,928 | **1.17** | 1.13 | 0.64 | 9.16× | 12.38× | 95.0% | 98.1% |

"Fraction" is the Amdahl-implied parallel fraction, `(1 - T_N/T_1) / (1 - 1/N)`.

**What parity takes.** At 16 cores Ibex needs an implied fraction of about
**96.7%** to match DuckDB's total, against 95.0% today. In time terms, Ibex's
non-parallel share is about 2.6 s of its 52.6 s single-core suite and must fall
to about 1.7 s. DuckDB's is about 1.2 s of 60.9 s. At 8 cores parity needs
96.9% against 96.5%, which is within noise of where Ibex is.

Per query, sorted by the 16-core ratio (ms, min of 5):

| query | ibex 1c | duckdb 1c | ratio 1c | ratio 8c | ibex 16c | duckdb 16c | ratio 16c | ibex scal 16c | duckdb scal 16c | ms lost 16c |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q04 | 1,456 | 1,414 | 1.03 | 1.32 | 211 | 139 | **1.52** | 6.9× | 10.2× | +73 |
| q12 | 1,375 | 1,922 | 0.72 | 1.22 | 221 | 154 | **1.43** | 6.2× | 12.5× | +67 |
| q10 | 2,770 | 3,239 | 0.86 | 1.17 | 371 | 261 | **1.42** | 7.5× | 12.4× | +111 |
| q01 | 3,991 | 3,683 | 1.08 | 1.23 | 401 | 283 | **1.42** | 10.0× | 13.0× | +118 |
| q11 | 260 | 250 | 1.04 | 1.26 | 41 | 30 | **1.39** | 6.3× | 8.4× | +12 |
| q03 | 2,450 | 2,575 | 0.95 | 1.17 | 292 | 210 | **1.39** | 8.4× | 12.3× | +82 |
| q14 | 2,029 | 2,218 | 0.91 | 1.18 | 227 | 164 | **1.38** | 8.9× | 13.5× | +63 |
| q19 | 2,534 | 2,911 | 0.87 | 1.22 | 284 | 213 | **1.34** | 8.9× | 13.7× | +72 |
| q15 | 1,887 | 1,931 | 0.98 | 1.10 | 197 | 156 | **1.26** | 9.6× | 12.4× | +40 |
| q07 | 2,988 | 3,439 | 0.87 | 1.10 | 324 | 264 | **1.23** | 9.2× | 13.0× | +60 |
| q20 | 2,024 | 2,634 | 0.77 | 1.00 | 242 | 201 | **1.20** | 8.4× | 13.1× | +40 |
| q05 | 2,970 | 3,023 | 0.98 | 1.10 | 300 | 253 | **1.19** | 9.9× | 12.0× | +48 |
| q21 | 6,874 | 7,651 | 0.90 | 0.94 | 755 | 664 | **1.14** | 9.1× | 11.5× | +91 |
| q09 | 4,940 | 5,909 | 0.84 | 1.02 | 503 | 448 | **1.12** | 9.8× | 13.2× | +55 |
| q13 | 2,618 | 3,469 | 0.75 | 0.90 | 292 | 264 | **1.11** | 9.0× | 13.1× | +28 |
| q06 | 1,444 | 1,916 | 0.75 | 0.95 | 156 | 147 | **1.06** | 9.3× | 13.0× | +9 |
| q08 | 3,374 | 3,455 | 0.98 | 0.98 | 297 | 293 | **1.01** | 11.3× | 11.8× | +4 |
| q16 | 417 | 509 | 0.82 | 1.00 | 88 | 89 | **0.99** | 4.7× | 5.7× | −1 |
| q18 | 3,960 | 4,749 | 0.83 | 0.76 | 300 | 316 | **0.95** | 13.2× | 15.0× | −17 |
| q22 | 489 | 801 | 0.61 | 0.75 | 67 | 79 | **0.84** | 7.3× | 10.1× | −12 |
| q17 | 1,506 | 2,651 | 0.57 | 0.61 | 137 | 208 | **0.66** | 11.0× | 12.7× | −71 |
| q02 | 282 | 508 | 0.56 | 0.50 | 43 | 79 | **0.54** | 6.6× | 6.4× | −36 |

At 16 cores Ibex loses 971 ms to DuckDB on 17 queries and wins 138 ms back on 5.

## 2. Where the gap is

- **Scaling, almost everywhere.** DuckDB scales 12–15× on most queries at 16
  cores; Ibex 7–11×. The widest spreads are on queries Ibex wins on one core:
  q12 (0.72 at 1c, 6.2× against 12.5×), q10 (0.86, 7.5× against 12.4×), q19,
  q14, q20, q07, q03.
- **Three queries are also slower per core:** q01 (1.08), q04 (1.03), q11
  (1.04). q01 is the largest single loss. The memory note on q01 says it is now
  bounded by the workers' own decode, filter, update and aggregate work, so the
  lever there is per-core cost, not parallelism.
- **Small queries scale poorly for both engines** (q02, q11, q16: 4.7–8.4×).
  Ibex wins q02 and ties q16; q11 is 12 ms and noisy.
- **Wins to keep:** q17 (0.66), q02 (0.54), q22, q18, q16.

## 3. What happened on the way here (2026-09-27)

The SF-10 run on `c72a9185` the same morning had Ibex at 1.05 / 1.23 geomean
against DuckDB (8c / 16c). The day's commits, in order:

| commit | change | measured |
|---|---|---|
| `3005a3cf` | columnar group keys (`GroupKeyStore`) + partitioned discovery for the generic multi-column key | q10 SF-10 −18.6% |
| `e69a8dbc` | `Column<std::string>::append`: block copy when concatenating chunks | q22 −8% / −13% (SF-8 / SF-10) |
| `0231282c` | bulk, column-parallel copy of new generic group keys | q10 SF-10 −7.6% |
| `9094c8e0` | FD payload lift over a fused `TopK`; prune lifted columns from projections below | canonical q10 +34% fixed |
| `9db40c24` | push semi joins below an inner join, not anti joins | canonical q16 +56% fixed |
| `5c64cfcb` | optimize sink inputs and shared bindings, not just the result | every PDS-H query now runs its canonical plan; `TopK` fires |
| `764f0a69` | `first`/`last`/`min`/`max` over `Date` and `Timestamp` | enabled the next one |
| `546ce652` | no row cap on the group-key uniqueness proof when the key is read in full | q10 SF-10 −29% |

q10 on AWS went 984 → 500 ms (8c) and 826 → 371 ms (16c). Its SF-8 → SF-10
growth went from 1.7–1.9× to about 1.1×, in line with the data.

**Lesson worth keeping:** before `5c64cfcb` every PDS-H script ran an
**un-canonicalized** plan, because `write_csv(result, …); result;` executes the
sink's input, and `lower_script` never optimized sink inputs. Benchmark numbers
before that commit measured a plan no sink-less user script would run.

## 4. Workstreams, ranked

### W1 — the scaling losers, by ms lost at 16 cores

q01 (118), q10 (111), q21 (91), q03 (82), q04 (73), q19 (72), q12 (67), q14
(63), q07 (60), q09 (55). Start with **q12**: the widest scaling gap, and it
already wins on one core.

Method, which found three fixes in q10 in one day:

1. `IBEX_PROFILE_OPERATORS=1` at 8 and 16 cores: rank nodes by main-thread
   self time, and look at `serial_self_ms`, `barrier_wait_ms` and `ring_wait_ms`.
2. `perf record --call-graph dwarf` and bucket the samples by time (50 ms
   windows, busy threads per window, the main thread's top frame). **Check the
   phase is on the critical path** (other threads idle while it runs) before
   fixing it. The q10 build-side string concat was serial on the main thread
   and fixing it moved q10 by 0%, because it overlapped other work.
3. When a fix does not move the wall, look for what ate the gain. Partitioned
   discovery saved ~120 ms and teardown of boxed keys gave it back; an
   experiment that leaked the keys proved it before any redesign.

### W2 — scale cliffs

q10's cliff was a fixed row cap (the uniqueness proof, 1,310,720 rows) crossed
between SF-8 and SF-10. The symptom was specific: **1-core time scaled with the
data, multi-core time did not**, because the fallback plan was serial. Other
fixed thresholds that can do the same: the ordinary uniqueness-proof cap
(`kMaxProofRows`, 1M rows), `kDenseCellLimit` (4M cells), the dense partial
budget in `dense_morsel_count` (32 MiB), the partition floors
(`kDefaultPartitionMinRows`, `kPackedPartitionMinRows`), and the deferred-probe
gates.

Sweep: SF 1, 2, 4, 8, 10, 16, 30 at 1 and 16 cores. Flag any query whose time
grows faster than its data between adjacent scales, then diff its plan and
profile across that step. SF-30 needs ~48 GB of disk and runs on the same box.

### W3 — canonical-plan audit

Canonicalization reached the benchmarks only with `5c64cfcb`, and it exposed two
later passes that misread canonical shapes (the lift and the anti-join
pushdown). Diff every query's plan with and without its `write_csv(result…)`
line (`IBEX_PROFILE_OPERATORS` node list), and A/B the two, to find any other
pass tuned to the old shapes.

### W4 — q01 per-core cost

q01 is 1.08 on one core and scales 10.0× against 13.0×. See the memory note
`project_q01_scan_aggregate_contention`: the main thread's serial work is gone,
and what remains is worker-side (decode, filter, update, the fused dense
aggregate). Candidates: skip the per-row Categorical remap for chunks the worker
aggregated (done for the keys); cheaper `update` evaluation; profile the worker
threads, not the main one.

### W5 — small items

- `Aggregate.Emission` appends text key columns row by row
  (`Column<std::string>::push_back` per group, reallocating): ~10–15 ms on q10
  at SF-10. Bulk-append from `GroupKeyStore`, which knows each column's bytes.
- Confirm the q16 (−33% / −50% on AWS, unchanged locally) and q11 (+17% at 16c)
  movements with a second run before quoting them.

## 5. How to measure

- **Cross-engine claims: AWS only**, SMT off, `run-tpch.sh` as in §1. Read the
  ratios within one sitting. Sittings drift by ~10% (the 16-core sitting in §1
  ran every engine slower); DuckDB is the most stable control.
- **Ibex-only A/B: local**, `benchmarking/ab_queries.py`, interleaved, 8 cores,
  byte-identity on. Re-run anything flagged at `--repeats 16` before believing
  it; at 8 repeats with 22 queries about one false flag per run is expected.
  Build the base binary from the exact parent commit. A binary from an older
  commit silently mixes in every commit between.
- **Local SF-10 data:** `~/polars-benchmark/data/tables/scale-10.0`
  (`make data-tables SCALE_FACTOR=10.0`, ~30 s). Point
  `benchmarking/data/tpch/parquet` at it for the run and back at `scale-8.0`
  after.
- **Plan checks:** a query that behaves differently with and without its
  trailing `write_csv` line is taking a different plan; compare the node lists.
