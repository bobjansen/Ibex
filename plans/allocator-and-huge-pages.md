# Allocator, page faults and huge pages: findings (parked)

Status: **analysis only, nothing landed. Parked 2026-09-24.** Single command-line
runs are not a priority at this stage. This file records what was measured, so
the question can be picked up again without re-deriving it.
Date: 2026-09-24
Baseline: PDS-H SF-8, dev box (i7-13700 under WSL2), `build-release/` at
`2b15ad18` (runtime code identical to `e7a56740`). Every comparison is Ibex
against Ibex, alternating runs in the same sitting (`ab_queries.py`, 8 paired
repeats), except where Polars is named.

## How this came up

The question was whether Ibex's q21 lead over Polars, its biggest win (0.43 at
8 cores in `run_bench.sh`), is really there. The answers match Polars at SF-8.
`queries/q21.ibex` is a line-for-line port of upstream's Polars q21. And
Polars' streaming engine, the reference, is its fast engine for q21 (in-memory
is about 60% slower). So the win is real in kind. But **its size depends on
the process being warm:**

| q21, 8 cores | fresh process (1 run) | warm loop (best of 5) | gain from warmth |
|---|---:|---:|---:|
| Ibex (`ibex` REPL, `:run`) | ~880–1,000 ms | 541 ms | **~38%** |
| Polars streaming | ~1,390 ms | 1,272 ms | ~8% |
| Polars in-memory | ~2,080–2,275 ms | 2,041 ms | small |

Both harnesses time a warm loop in one process and keep the fastest run
(`bench_ibex.py` repeats `:run` in one REPL; `bench_pdsh.py` repeats `collect()`
in one Python process). The method is symmetric, but Ibex gains far more from
it. **q21 is about 0.43 warm against warm and about 0.63 fresh against fresh.**
Both ratios are from the dev box at 8 cores, where cross-engine ratios flatter
Ibex (`beat-polars-plan.md` §1b), so confirm them on AWS before quoting them.

## Why a fresh Ibex process is slow: first-touch page faults

`perf stat` on the `ibex` REPL, one `:run` against five:

| | wall | CPU | page faults |
|---|---:|---:|---:|
| 1 run (fresh) | 908–1,222 ms | 3.0–3.9 s | ~762,000 |
| each further run (warm) | ~600 ms | ~2.7 s | ~75,000 |

A fresh q21 touches about 2.7 GB of new memory. In 4 KB pages, that is about
690,000 extra faults, each of which the kernel has to zero and map. A warm run
reuses memory that glibc's allocator kept after the previous run. The runtime's
`mallopt(M_MMAP_MAX=0, M_TRIM_THRESHOLD=-1)`, called once at the first
`interpret()`, stops glibc from returning it to the kernel.

There is exactly one cache that persists across queries: the Parquet reader's
`cached_dictionary_column_indices` (`libs/parquet/parquet.hpp`), a per-process
memo of which string columns are fully dictionary-encoded. It explains at most
a small part of the gap, since CPU per run differs far less than wall time or
faults.

## Huge pages in one paragraph

x86-64 can map memory in 2 MB pages instead of 4 KB: one fault instead of 512,
and more memory per TLB entry. The zeroing work is the same. Linux's
*transparent* huge pages (THP) do this without a reservation. This box, like
stock Ubuntu, runs THP in `madvise` mode: only memory marked with
`madvise(MADV_HUGEPAGE)` gets huge pages. glibc's
`GLIBC_TUNABLES=glibc.malloc.hugetlb=1` makes `malloc` mark its own heap and
mappings. It is read **only at process start**. The costs are memory bloat
from 2 MB granularity, compaction stalls under memory pressure, and pages that
split when memory is partly freed.

## Allocator A/B: jemalloc does not help; whole-heap huge pages do

The two setups against today's glibc + `mallopt`, over the whole suite, with
byte-identical output everywhere. Wrappers set `LD_PRELOAD` or
`GLIBC_TUNABLES`, because `ab_queries.py` clears the environment.

| | fresh, 1 core | fresh, 8 cores | warm suite, 8 cores (2 rounds) |
|---|---:|---:|---:|
| jemalloc 5.3 (`dirty_decay_ms:-1,muzzy_decay_ms:-1`) | **+3.7%** (12 slower, 0 faster) | +0.3% (4 faster, 3 slower) | −3.0% (noise ±3.6%) |
| glibc + `hugetlb=1` | **−8.0%** (17 faster, 0 slower) | **−4.9%** (8 faster, 0 slower) | −2.3% (noise) |

- **jemalloc: no.** It fails the 1-core floor (q21 +14%, q16 +11%, q13 +8%),
  and at 8 cores it is a wash. Even if it were faster, it could only go into
  Ibex's own executables (`ibex`, `ibex_eval`). The Python module and the R
  package run inside someone else's process, and it is Linux-only in practice.
  Arrow is built with `ARROW_JEMALLOC` off, so `arrow::default_memory_pool()`
  is plain `malloc` and follows whatever the process uses. jemalloc stays where
  it already is, in `ibex_bench` and `ibex_join_bench` and in `ibex-build.sh`'s
  transpiled programs. The reason for that choice (glibc giving freed memory
  back to the kernel) is now covered by the runtime's `mallopt`.
- **Whole-heap huge pages: yes, for fresh processes.** They cut q21's faults
  762k → ~57k (−93%) and fresh q21 by 15–24%. Across the suite the big gains
  are on the heavy queries: q10, q16, q04, q18, q22, q09 and q21 are 10–17%
  faster at 1 core. **When warm they make no difference.** So the published
  PDS-H ratios, which are warm, would not move.

## Tried and reverted: huge pages for column buffers only

The in-process alternative to the environment variable is to mark Ibex's own
large buffers. `detail::NoInitAllocator` (`include/ibex/core/column.hpp`) is
the one chokepoint for every `Column<T>`'s storage and every `NoInitVector`.
The prototype allocated buffers of 2 MiB and up 2 MiB-aligned
(`aligned_alloc`) and marked them with `madvise(MADV_HUGEPAGE)`:

- q21 faults 764k → 265k, so **column storage is about 65% of the faults.**
  Peak VMA count went from 271 to 292, nowhere near `vm.max_map_count`.
- **Across the suite the gain mostly disappeared.** −0.6% at 1 core (q04
  **+5.2% slower**) and −1.8% at 8 cores (q13 and q21 about −11%, but q18
  flagged +9.9%; that flag was not re-checked with `perf stat`, since the
  prototype was dropped anyway).

Huge pages pay off through *every* allocation (the robin_hood hash tables,
plain `std::vector` selection vectors, Arrow's decode buffers) and through TLB
reach for all of them, not through one class of buffers. Aligning and
rounding each large buffer to 2 MiB also costs something. **An Arrow
`MemoryPool` that marks its buffers would be partial coverage again, and is
not worth doing on its own.** Consider it only as part of a whole-heap
approach.

## Options, if single runs start to matter

1. **The command-line tools re-launch themselves with the tunable.** At the
   start of `main()` in `ibex` and `ibex_eval`: if `GLIBC_TUNABLES` does not
   enable `glibc.malloc.hugetlb`, append it and `execv("/proc/self/exe", argv)`.
   It costs a few milliseconds, needs an opt-out variable, and older glibc
   ignores unknown tunables. It is the only in-process way to get the measured
   −8% / −4.9%, and it does not reach the Python or R modules.
2. **Set the tunable in the launch scripts** (`ibex-run.sh` already sets
   jemalloc's `MALLOC_CONF`) and document it. No engine change, but it only
   helps script users.
3. **Leave it to the system** (THP `always`) and document the trade-off.

Recommended when reopened: (1) plus a line in the README's performance section.

## Consequences that apply now

- **Quote fresh-process timing next to the warm loop** whenever a claim leans on
  a large Ibex win (`beat-polars-plan.md` §5). The warm loop is not unfair to
  Polars, whose warm gain is small, but it hides Ibex's first-run cost. A
  `--fresh` mode in `bench_ibex.py` would make this routine.
- **Do not re-try jemalloc for PDS-H or column-only huge pages** without new
  evidence (listed in `beat-polars-plan.md` §6).

## Reopen when

- one-shot command-line or scripted use becomes a target (users running
  `ibex query.ibex` in pipelines, or a published "time to answer" number);
- or a profile shows page faults in a *warm* process, e.g. a memory-hungry
  query that outgrows what the allocator retains.

## Reproducing

```sh
# page faults and wall time for one fresh run
IBEX_CORES=8 taskset -c 0-7 perf stat -e page-faults,duration_time \
    build-release/tools/ibex_eval --plugin-path build-release/tools \
    benchmarking/tpch/queries/q21.ibex
# the same with whole-heap huge pages
GLIBC_TUNABLES=glibc.malloc.hugetlb=1 IBEX_CORES=8 taskset -c 0-7 perf stat ...
# whole-suite A/B: ab_queries clears the environment, so wrap the setting
printf '#!/usr/bin/env bash\nGLIBC_TUNABLES=glibc.malloc.hugetlb=1 exec %s "$@"\n' \
    "$PWD/build-release/tools/ibex_eval" > /tmp/eval_thp && chmod +x /tmp/eval_thp
python3 benchmarking/ab_queries.py --base build-release/tools/ibex_eval \
    --target /tmp/eval_thp --cores 1 --taskset 0-7
```

Check `cat /sys/kernel/mm/transparent_hugepage/enabled` first. Under `never`,
the tunable does nothing.
