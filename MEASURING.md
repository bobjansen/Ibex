# Measuring and verifying changes in Ibex

A short guide for agents doing performance or correctness work here. Everything
below is a rule that was learned by getting it wrong, usually more than once.

## 0. The reporting rule

**Deliver a result, or say in one sentence that you did not.**

> "The runner terminated during the first 1-core pass before it wrote any result
> rows. The initial sandbox failed on the uv cache; the approved rerun got past
> that but was stopped externally, so there is no valid scaling report yet."

That is three sentences of narration around "I have no data." Write "No
measurement yet — the harness died twice, cause not yet diagnosed" and then go
diagnose it. A dead run is a bug to investigate, not an event that happened to
you. "Stopped externally" is not a finding; find out *what* stopped it.

The same applies to work: if you did not finish, say what is missing and why.
Never let a summary imply progress the tree does not contain.

## 1. Start with the smallest thing that discriminates

Do **not** open with the big harness. `benchmarking/aws/run-thread-scaling.sh`
and the 16M two-tier suite are deliberately **not run** as a routine step (see
`plans/README.md`); they take a long time, need AWS or a quiet box, and tell you
nothing you can act on until you already know which operator is slow.

The fast loop is seconds, not hours:

```bash
# write a synthetic case with ibex_eval, 1M rows is usually plenty
IBEX_PROFILE_OPERATORS=1 IBEX_CORES=8 \
  ./build-release/tools/ibex_eval --plugin-path build-release/tools case.ibex \
  2>&1 >/dev/null | grep 'op="aggregate'
```

`IBEX_PROFILE_OPERATORS=1` prints a per-node line (`build_self_ms`,
`next_self_ms`, `pool_work_ms`, `barriers`, `pool_tasks`) and a per-statement
`operator profile:` summary. That is where a 500ms serial block shows up, and it
costs one run.

Reach for the full suite only to confirm a change you already believe in, or to
check you did not regress something else.

## 1b. Before believing any scaling number: is the work being DIVIDED?

`IBEX_PROFILE_OPERATORS` and everything built on it measures how work is
*distributed* -- occupancy, barrier wait, serial fraction, Amdahl ceiling. None
of it can see work that should not exist. Parallelism that MULTIPLIES work
still improves wall time, because N threads doing kN work finish sooner than
one thread doing k, so every one of those metrics reads healthy while most of
the machine is wasted.

That is not hypothetical. q17 reported **85% pool occupancy, 2ms of serial
self-time and a 22.79x Amdahl ceiling** while burning 4.6x the CPU: the fused
key scan sharded each Parquet row group by row offset, and a shard's `Skip`
decodes and discards, so shard k re-decoded every shard before it. Three days
of aggregate metrics pointed at "scheduling". Fixed in `7fae1146` -- q17
-33.6%, q08 -20.2%, q05 -15.8%, q09 -11.6%, suite geomean -3.9%.

The measurement that sees it is total CPU at one core against N:

```bash
python benchmarking/work_multiplication.py                 # whole suite
python benchmarking/work_multiplication.py --queries q17    # one query
```

`work_x = cpu(N cores) / cpu(1 core)`. **1.00 means CPU was conserved.** Read
`work_x` first and `speedup` second -- a query can look fine on speedup and
still waste most of the machine.

**A high `work_x` is a screen, not a verdict.** Two very different causes:

* **Redundant work** -- the parallel path genuinely does more. q17's sharded
  key scan re-decoded every preceding shard (`7fae1146`, -33.6%). Deletable.
* **Contention stalls** -- the same work, slowed. `task-clock` counts a
  stalled core as busy, so threads competing for L3 and bandwidth inflate CPU
  without doing anything extra. q06 reads 2.27x, and a row counter on its
  filter shows IDENTICAL rows at 1 and 8 cores (12 calls, 11,997,996 rows
  both). Nothing to delete.

**Confirm which before acting**, by counting the work directly -- rows, calls,
bytes -- at 1 core and at N. Matching counts mean contention. This costs one
temporary counter and it is the difference between a 33% win and a week spent
chasing an artifact. A plain streaming read does not inflate on this box
(400MB, 1 vs 8 threads: 13.8 -> 52.7 GB/s, task-clock flat), so contention
needs concurrent competitors, not merely a large working set.

CPU comes from `getrusage(RUSAGE_CHILDREN)`, so no `perf` is needed; it agrees
with `perf stat -e task-clock` to a millisecond or two. Workers park on a
condition variable rather than spinning, so an idle thread costs nothing and
the number is real work, not wait.

When it flags something, the order that worked was: (1) `work_x` at 1 vs N,
(2) `perf record` at both and diff the symbol shares, (3) `perf record -g` to
get that symbol's CALLERS -- the memmove was inside Parquet's `PlainDecoder`,
not Ibex's morsel merge where it was assumed to be -- (4) isolate with the knob
that separates the two pools, `IBEX_DECODE_THREADS` against `IBEX_CORES`.

## 1a. The mechanics: what to run

### Running a query

```bash
# a .ibex file, one process, fresh each time. This is the measurement binary.
./build-release/tools/ibex_eval --plugin-path build-release/tools case.ibex

# `ibex` is the REPL and DOES read stdin; `ibex_eval` does not ("file is
# required"). Bindings need `let`, and the last statement needs a trailing `;`.
printf 'let t = Table(3);\nt;\n' | ./build-release/tools/ibex
```

`--plugin-path build-release/tools` is required for anything touching
parquet/csv/json. `scripts/ibex-run.sh <file.ibex>` does the transpile → compile
→ run path instead, when you specifically want compiled codegen rather than the
interpreter.

Generating synthetic input inside the script beats wiring up data files:

```
seed_rng(12345);
let t = Table(1000000)[update {
  a = rand_int(0, 99), b = rand_int(0, 97), v = rand_normal(0.0, 1.0)
}];
t[select { m = median(v) }, by {a, b}][select { n = count() }];
```

Note `by {a, b}` — repeating `by a, by b` is a parse error, and the row-index
`idx` does not exist; use `rep([...])` or `rand_int` to build key columns.

### Environment variables

These are the whole set (`grep -rhoE '"IBEX_[A-Z0-9_]+"' src/ include/`). The
ones that matter for measurement:

| Variable | Effect |
|---|---|
| `IBEX_CORES` | compute budget. **Pin it** — unset means `hardware_concurrency()`. Cap local cross-engine runs at 8; never 24 (polars thrashes and inflates Ibex ~1.7×) |
| `IBEX_PROFILE_OPERATORS=1` | per-node + per-statement profile to stderr. The main diagnostic |
| `IBEX_CORES=1` | one compute thread — the byte-identity A/B setting. There is no separate on/off switch: a budget of one *is* serial, and the aggregate morsel cut is keyed on rows, so every core count answers bit for bit. |
| `IBEX_STREAM_SCAN=0` | opt out of streaming scans |
| `IBEX_JOIN_PROBE=0` | opt out of the parallel join probe |
| `IBEX_DECODE_THREADS` | absolute pool size, bypassing policy |
| `IBEX_DECODE_SATURATION` | where extra decode threads stop paying (default 8; a property of the box, so re-sweep on new hardware) |
| `IBEX_MORSEL_ROWS`, `IBEX_CHUNK_ROWS` | grain overrides; `IBEX_CHUNK_ROWS` exists to *exercise* cross-chunk paths, not for perf |
| `IBEX_PARALLEL_STATS=1`, `IBEX_UNIQUE_KEY_STATS=1` | extra diagnostics |

`IBEX_THREADS` is **dead** — it was split into `IBEX_CORES` and
`IBEX_DECODE_THREADS`. It is read only to print a one-time warning, so a script
still setting it silently gets `hardware_concurrency()`. All the on/off switches
accept `1/on/true/yes` and `0/off/false/no`.

### Reading the profile

```bash
IBEX_PROFILE_OPERATORS=1 IBEX_CORES=8 \
  ./build-release/tools/ibex_eval --plugin-path build-release/tools case.ibex \
  2>&1 >/dev/null | grep 'operator profile:'
```

Per statement you get `wall_ms`, `self_ms`, `serial_self_ms`, `barrier_wait_ms`,
`ring_wait_ms`, `pool_work_ms`, `pool_idle_ms`, `pool_unqueued_ms`,
`pool_capacity_ms`, `stage_self_ms`, `stage_park_ms`, `stage_live_ms`,
`occupancy`. Per node (`profile node=N op="..."`), `build_self_ms` /
`next_self_ms` / `pool_work_ms` / `pool_tasks` / `barriers`.

Two things to know when reading it:

* **`build_self_ms`, not `next_self_ms`, is where a materializing fallback
  shows up.** A node whose work happens in `build_operator` (anything routed
  through `interpret_node`) reports there, and its `occupancy` is meaningless
  because `span_ns` only covers `next()`.
* The accounting closes: `pool_work + pool_idle + pool_unqueued ≈
  pool_capacity` (99.6% over PDS-H) and `stage_self + stage_ring_wait +
  stage_park ≈ stage_live` (99.9%). If your change makes either stop closing at
  suite scale, you broke the instrument, not the engine. **Do not check closure
  on a sub-millisecond statement** — the ledgers are sampled at statement start
  and end, so on a 0.1ms query the sampling skew is larger than the quantity and
  `pool_unqueued` routinely exceeds `pool_capacity`.

### Benchmark suites, cheapest first

### Rebuild the complete Release tree before timing

Run this immediately before **every** benchmark or profile:

```bash
cmake --build build-release
```

Do not build only the executable used by a focused experiment (`ibex_eval`,
`ibex_tests`, etc.) and then time a different one. `run_bench.sh` drives the
REPL binary (`build-release/tools/ibex`) while focused query A/B work often
builds `ibex_eval`; an incremental target build can therefore leave the actual
benchmark executable stale while Git records the current commit in its result
archive. Rebuilding the complete Release tree is cheap relative to a suite
run, refreshes every executable and plugin, and is the required guard against
that false result.

```bash
# 1. PDS-H, 22 queries, one warm process — the usual gate
uv run --project . python benchmarking/tpch/bench_ibex.py --iters 5
uv run --project . python benchmarking/tpch/check_answers.py    # NO arguments

# 2. A/B two git states (persistent build cache, incremental rebuilds)
./benchmarking/compare_ibex_git.sh --base HEAD --target WORKTREE \
    --interleave --repeats 15 --taskset 2

# 3. cross-engine scale suite (slow)
./benchmarking/run_scale_suite.sh --threads 8
```

Run `check_answers.py` with **no arguments**. Naming queries filters to ones it
has a reference answer for, and anything else prints `SKIP (not implemented)`
and exits 0 — `check_answers.py q01 q06` is two SKIPs and a green exit code,
having verified nothing. `bench_ibex.py` does take query names (`q01 q06`) and
runs them for real, so the two are not symmetric.

`compare_ibex_git.sh` defaults to `--base HEAD --target WORKTREE`, so it
measures uncommitted changes with no arguments. Always pass `--interleave` on
this box: serial blocks drift. `--replica-control` builds the base twice and
runs it as a third side, which is how you tell a real regression from layout
noise. Each side's worktree and Release build dir are cached under
`~/.cache/ibex/perfcmp` (`IBEX_PERFCMP_CACHE` to relocate): cmake configures
once per side and later runs are incremental ninja builds with ccache hits on
unchanged files. Delete the cache dir — or pass `--no-cache` — when a
from-scratch build is required.

The competitor harnesses need the project's **uv** environment, not system
python or conda (those lack polars/pandas and fail in a way that looks like
"deps not installed"):

```bash
uv run --project /home/brj/ibex python benchmarking/bench_python.py --csv ... --iters 1
Rscript benchmarking/bench_r.R --csv ...          # R needs no wrapper
```

`benchmarking/aws/run-all.sh` and `run-thread-scaling.sh` are the AWS two-tier
framework. They are **deliberately not run** as a routine step — do not reach
for them unless the task is explicitly "publish new numbers".

### Profiling the whole suite

`benchmarking/profile_suite.py` sums the per-statement profile across all 22
queries and prints the closure columns. Every accounting table in
`plans/parallelism-overview.md` came from it.

```bash
python3 benchmarking/profile_suite.py 8
```

Two things it does that you must also do if you write your own:

* **Run each query twice, parse only the second.** The first pays cold page
  cache on the Parquet files, and that time lands in decode before any worker
  starts — parsing it makes every I/O-heavy query look serial.
* **Print closure, do not assume it.** `pool_work + pool_idle + pool_unqueued`
  should exhaust `pool_capacity`. If it stops closing, a bucket is unmeasured.
  That has happened five times here and always in the same direction.

### Timing A/B over queries

`compare_ibex_git.sh` compares two git STATES via the `ibex_bench` suite.
`benchmarking/ab_queries.py` compares two BINARIES over `.ibex` query files,
which is what you want when the thing you care about is a PDS-H query. It
interleaves the sides, alternates which goes first, takes medians, checks
byte-identity, and reports a geomean.

```bash
# 1. keep the candidate
cp build-release/tools/ibex_eval /tmp/eval_target

# 2. build the base (git stash, or check specific files out of a ref)
git stash -q
CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-release --target ibex_eval
cp build-release/tools/ibex_eval /tmp/eval_base
git stash pop -q
CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-release --target ibex_eval   # NOT optional

# 3. compare
python3 benchmarking/ab_queries.py --base /tmp/eval_base --target /tmp/eval_target
```

Step 2's final rebuild is the easy one to skip: after `git stash pop` the binary
in `build-release/` is still the BASE build, so forgetting it means measuring
the base against itself while believing otherwise.

Exit code is **2** when any output differs, so it can gate. A divergence is a
correctness result — stop and explain it before reading any timing.

It does not use a fixed noise band. The effect size is the **median of the
per-pair ratios** and significance is a **Wilcoxon signed-rank test** on the
paired runs, so sensitivity scales with `--repeats` instead of being frozen at
whatever the box was doing the day someone picked a number. A query it cannot
resolve is reported `unclear`, not `noise` — those are different claims, and
`unclear` tells you to raise `--repeats` rather than to drop the idea.

`--repeats` is forced **even**: whichever side runs first in a pair pays a
first-position penalty, so an odd count leaves that bias uncancelled. With 7
repeats a same-binary comparison reported `FASTER -12.6%, p=0.047`.

**Run it against itself when you doubt a result.** `--base X --target X` should
produce `same` and `unclear`, never a verdict. If it produces one, the box is
too busy to measure on.

Calibration on this box, verified by injecting a known slowdown into a wrapper
script:

| | 8 repeats | 12–16 repeats |
|---|---|---|
| true null (same binary) | no false verdicts | no false verdicts |
| known ~6% regression | `unclear`, p≈0.08 | `SLOWER`, p≤0.006 |

So **a 5% win is detectable here** — it just needs more than the default number
of repeats on a noisy query. Read `disp` (how far the base median sits above its
min) as the box-quietness indicator: single digits is fine, 15%+ means
corroborate before believing anything.

### Proving a code path is reached

Distinct from mutation-testing a test. Make the function itself fail and run the
query:

```cpp
auto distinct_table(...) -> std::expected<Table, std::string> {
    return std::unexpected(std::string("MUTATED reached"));   // temporary
```

If the query still succeeds, that path is not what runs, and any measurement or
test you attributed to it is about something else. This is how the `let`-breaks-
the-chain rule in §4 was found — the obvious test shape did not fire the mutation
at all.

For the parallel/serial contract, the equivalent is a straight diff:

```bash
diff <(IBEX_CORES=8 ./build-release/tools/ibex_eval ... q.ibex) \
     <(IBEX_CORES=1 ./build-release/tools/ibex_eval ... q.ibex)
```

### perf, and what does not work here

`perf record` works; **hardware counters do not**. This is WSL2, so
`perf stat -e cache-misses,L1-dcache-load-misses` returns `<not supported>` for
every PMU event. Do not build an argument on a cache-miss number you did not
actually get — if you need to test a locality hypothesis, change the layout and
measure wall time instead.

```bash
perf record -q -g --call-graph=dwarf,16384 -F 999 -o perf.data -- \
  ./build-release/tools/ibex_eval --plugin-path build-release/tools case.ibex
perf report -i perf.data --stdio --no-children --percent-limit 2
```

Release builds inline aggressively, so expect one enormous symbol
(`aggregate_table` was 88% of samples) with the actual hot loop invisible inside
it. perf tells you *which function*, rarely *which line*. Raise `-F` and lengthen
the run if you get fewer than a few hundred samples — 185 samples is not enough
to trust a 5% attribution, which is exactly how a component gets wrongly
exonerated.

### Do not measure right after committing

`.githooks/post-commit` starts a **background scale regression** whenever the
commit touched `src/runtime/`, `src/codegen/`, `src/ir/` or `include/`. It logs
to `build-release/post_commit_perf.log`. Anything you time in the next few
minutes is competing with it and has read up to 30% high in the past.

```bash
IBEX_SKIP_PERF=1 git commit ...     # skip it for one commit
tail -f build-release/post_commit_perf.log   # or wait for it
```

Its own FAIL lines are a prompt to investigate, not evidence — confirm with
`compare_ibex_git.sh --base HEAD~1 --target HEAD`.

### PDS-H data

`benchmarking/data/tpch/parquet` is a **symlink** the scale scripts flip between
`parquet_sf1` / `parquet_sf2` / `parquet_sf4`. For an A/B, pin the explicit
`parquet_sf<N>` path rather than trusting the symlink to still point where it
did when you started.

## 2. Vary two dimensions, not one

A one-dimensional sweep will confidently tell you there is no bug.

A real example: `median(v) by {a,b}` cost 517ms at 9800 groups and 20ms at 5000.
Sweeping group count alone looked like a cliff at 6000. Sweeping key count alone
looked fine (one key handled 100k groups in 67ms). The actual trigger needed
**both** multiple keys and a particular hash-table size, and neither sweep could
see it. See `plans/parallelism-overview.md`.

When something looks like a threshold, ask what else changed at that threshold.

## 3. Form a hypothesis, then try to kill it cheaply

Being wrong fast is the job. In the case above, two plausible hypotheses were
measured and discarded before the real one:

* per-group heap indirection — flattened the slot array, changed nothing;
* group count itself — one key does 100k groups in 67ms.

Each cost one build. Write down the discarded ones; they stop the next person
repeating them.

## 4. Prove the code path you are testing actually runs

The single most common way to "verify" nothing. Two checks:

* **Mutation-test every test you add.** Break the code the test claims to cover
  (return an error, delete a term, `if (false)`) and confirm the test fails.
  If it still passes, the test is decorative. Restore immediately afterwards.
* **Check reachability.** The whole-table functions (`distinct_table`,
  `inner_join_table`, `aggregate_table`) run only for a subtree beneath a node
  the chunked builder declined, **within one statement**. A `let` materializes
  and breaks the chain, so

  ```
  let d = t[distinct { g, v }];
  d[select { m = median(v) }];      // does NOT reach distinct_table
  t[distinct { g, v }][select { m = median(v) }];   // does
  ```

  A test written the first way passes while covering nothing.

## 5. Anything that can change output gets a byte-identity check

The engine's contract is that parallel output equals serial output, byte for
byte. Before and after your change:

```bash
for q in benchmarking/tpch/queries/q??.ibex; do
  n=$(basename "$q" .ibex)
  IBEX_CORES=8 ./build-release/tools/ibex_eval \
    --plugin-path build-release/tools "$q" > out_new/$n.txt 2>/dev/null
done
diff -rq out_base out_new
```

Use `git stash` to build the baseline binary, and keep both binaries so you can
interleave. This applies to any edit inside an operator, not just "optimizations"
— a schema-carrier fix in the join needed it too.

## 6. Timing A/B: pair the runs, then test the pairs

Use `benchmarking/ab_queries.py` (§1a) rather than rolling your own — the design
details below are the ones that bit while writing it.

Serial runs drift on this box, so sides must be **interleaved and paired**: run
`i` of each side under the same machine conditions, then reason about the paired
differences. An unpaired comparison of two sets of timings throws away the only
thing keeping drift out of the answer.

**Do not use a fixed percentage band.** A wide band is not conservatism, it is
insensitivity: real optimizations in this codebase land at 5–15% per query, and
a ±13% band declares most of them invisible. The figure that band came from was
two untouched queries moving ±13% *in single configurations*, and the conclusion
drawn at the time was to corroborate a delta across core counts and statistics —
not to stop looking below 13%.

What to do instead: estimate the effect from the paired ratios, test it with a
signed-rank test, and let `--repeats` buy sensitivity. Distinguish three
outcomes, not two — **same** (too small to matter), **unclear** (an effect the
data cannot separate from noise; raise repeats), and a verdict. Reporting
`unclear` as "no change" is how a real regression ships.

Check the box is quiet first (`ps --sort=-pcpu -eo pcpu,comm | head`); WSL2
load average lies. Never run a build while benchmarking, and see §1a on the
post-commit hook.

### Statistics cannot tell you a delta is CAUSED by your change

This is the limit of everything above, and it bites hardest on the results that
look best.

Builds here are bit-reproducible (verified: three rebuilds, including a header
touch that recompiled 11 TUs, produce identical bytes). So two binaries that
differ by one edit also differ in code LAYOUT everywhere, and a layout-induced
per-query delta is perfectly stable — it reproduces across every repeat, in
every run, at any p-value you care to demand. No amount of repeats separates it
from a real speedup, because it is not noise.

A worked example from the hash-clustering fix. Over 22 PDS-H queries at 10
repeats, q22 came back −8.6% at p=0.002 and survived Holm correction; re-run
independently at 20 repeats it strengthened to **−9.5%, 19-1 pairs, p<0.001**.
By every statistical standard, a result. Then the profile:

| node | base | target |
|---|---|---|
| `update` | 17.26 ms | 14.47 ms |
| `scan` | 6.23 ms | 4.54 ms |
| `join anti keys=1` | 3.43 ms | 3.18 ms |

The time moved in `update` and `scan`, neither of which hashes anything, and
q22 groups by a single key — the multi-key pathology that commit fixed cannot
occur in it. So the −9.5% is code layout, not the change.

**Two independent re-runs and a corrected p-value would all have endorsed a
wrong causal claim.** What settled it was asking which operator moved and
whether that operator has anything to do with the edit. Always close the loop
with `IBEX_PROFILE_OPERATORS` on a per-query win before attributing it, and
prefer a mechanism you can point at over a p-value you can quote.

## 7. Build discipline

* **`-j 6` total across the whole box**, not per invocation. Two concurrent
  `cmake --build ... -j 4` calls is 8 compiles and has OOM-killed this machine.
* Build the specific target (`--target ibex_tests ibex_eval`), not everything.
* Before committing anything non-trivial, run the strict g++ leg —
  CI uses `-Wpedantic -Wconversion -Wshadow` with warnings-as-errors and local
  clang does not:
  `CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-gcc --target ibex_runtime`

## 8. Do not let `pgrep`/`pkill` match itself

`pkill -f ibex_eval` matches the shell running that very command and kills it.
`until ! pgrep -f "compare.sh"` never exits for the same reason. Use the bracket
trick:

```bash
pkill -f "[i]bex_eval"
```

This has cost two separate incidents, including one that looked exactly like a
harness "stopped externally".

## 9. When you change a constant, grep for its other copies

Hash mixers, magic seeds, gate predicates. A six-clause join gate was written out
identically in two files; a hash mixer had **three** copies and finalizing two of
them silently duplicated groups until a test caught it. Before editing one:

```bash
grep -rn "0x9e3779b97f4a7c15" src/ include/ libs/
```

Then extract the shared thing rather than updating each copy.

## 10. Trust a profiler number only after something independent agrees

This profiler has had five attribution bugs, all of which made "serial" look
bigger than it was. A figure that surprises you is a claim to check, not a
finding to report. Corroborate with wall-clock A/B before building a plan on it.
