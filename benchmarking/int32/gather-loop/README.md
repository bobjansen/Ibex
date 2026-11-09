# Bind physical inputs before gathering

Raw experiment output is retained locally under `benchmarking/results/local/`;
the scripts and summarized findings are versioned here.

`ColumnView<Int64>` now stores physical Int32/Int64 base pointers instead of an
`IntegerView` iterator. A kernel view does not need an iterator position. The
gather dispatches on physical width once, then passes a plain pointer to its
selection visitor. Range, index, byte-bitmap and word-bitmap loops all operate
on that pointer. Int32 values widen to Int64 before they reach the output;
contiguous same-width ranges retain memcpy. No column-owned widening cache is
restored, and the general IntegerView iterator is unchanged.

[Disassembly](assembly.md) confirms six instructions per selected value in the
native masked-copy loop for both widths. Int32 uses `movslq` instead of `mov`;
there is no extra instruction for the widening, no offset load, and no width
branch in either inner loop. Width selection occurs once per gather call,
rather than once per mask word. Each full selection visitor occupies 426 bytes.

## Focused performance gate

The existing git A/B harness compared frozen 6e1f4938 against this working-tree
change, including a byte-identical unchanged baseline replica. Twenty paired,
interleaved repeats, two warmups, twenty timed iterations per process, one core
pinned to CPU 2 at the existing 3.2GHz lock. Inputs are the existing 4M-row wide
CSV trades table (Categorical symbol, Double price, Int64 qty); compact ingestion
is disabled. CSV loading is outside timers. This is a wide-input regression
check, not an Int32-versus-Int64 speed comparison.

| Filter | Median paired candidate/base | Improvement | Holm-adjusted p |
|---|---:|---:|---:|
| Simple | 0.9695 | 3.05% | 0.000354 |
| AND | 0.9725 | 2.75% | 0.000354 |
| Arithmetic | 0.9770 | 2.30% | 0.000354 |
| OR | 0.9691 | 3.09% | 0.000354 |

Raw paired samples and analysis (`paired.json`, local raw output), conditions and hashes (`conditions.json`, local raw output).
All unchanged-control comparisons are unresolved (unadjusted p >= 0.65).
The paired improvements and removed instructions agree with the earlier
same-binary causal diagnosis. Cross-build layout can still contribute to the
precise effect size; these results do not clear the broader Int32 performance
gate or establish equivalence to the older cached checkpoint.

Operator profiles (`operator-summary.json`, local raw output) retain identical filter row/call
counts; median filter next time decreases in all four cases. Scan time remains
0–0.001 ms. Profiles are separate diagnostic runs, not the paired timings above.

Reproduce from this worktree after rebuilding the complete Release tree:

```bash
mkdir -p build-loop-check/perfcmp
IBEX_CORES=1 IBEX_COMPACT_INT32=0 IBEX_PERFCMP_JOBS=6 \
IBEX_PERFCMP_TMPDIR="$PWD/build-loop-check/perfcmp" \
benchmarking/compare_ibex_git.sh --base 6e1f4938 --target WORKTREE \
  --ibex-suite filter --csv-trades /home/brj/ibex/benchmarking/data/trades.csv \
  --csv /home/brj/ibex/benchmarking/data/prices.csv \
  --interleave --replica-control --repeats 20 --warmup 2 --iters 20 \
  --taskset 2 --keep-temp
```

Run `benchmarking/int32/paired_report.py <retained-directory> --out <report.json>`
with the project Python environment to analyze the repeat TSVs.

## Correctness

Both modes pass 2,472 fast CTest cases (four network cases skipped). The focused
test covers both physical widths and all four selection shapes, Int32 sign
extension, Int64 extrema, empty selections, repeated/reordered indices, empty
mask words, nonzero mask row bases, and output-window sentinels. Separate
mutants flip a selected value in each physical-width arm; the test rejects both.
The unmodified standalone test passes. Strict GCC runtime compilation passes
with `-Werror -Wpedantic -Wconversion -Wshadow`.

Full typed answer hashes (`answer-hashes.json`, local raw output) agree for every filter between
the baseline at one core and the candidate at one/eight cores, including row
order, exact integer values, strings and Float64 formatting. This validates
complete results, not just row counts or numeric sums.

Raw profiles, complete answer CSVs, standalone mutation sources/logs, frozen
benchmark binaries and build/test logs remain in persistent ignored directories
`build-loop-check/` and `build-release/` in this worktree.

The matching Parquet plugin was rebuilt against the current runtime. Physical
Int32 ingestion checks pass at one/eight cores (119 assertions each), and
six native Parquet queries (`parquet-correctness.json`, local raw output), including selected
reads, are byte-identical across compact on/off, one/eight cores, and the
original noncompact implementation.
