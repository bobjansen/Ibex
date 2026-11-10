# Semi/anti membership probe repair, 2026-11-10

The production change binds the probe-key storage, membership structure and
validity bitmap once per chunk, then calls a typed kernel once per 64-row
selection word. The kernel is selected before probing by physical key width
(Int64, compact Int32, or categorical codes) and by nullability. Anti joins
invert the completed word once. All other filter, allocation and gather work
is shared with the existing `filter_rows` path. No column type, narrowing rule,
allocator policy or environment switch is introduced.

Against `f1eb185c` it resolves `null_semi_join` -2.3% and `null_anti_join`
-2.1% at one core in the 91-row broad gate. The earlier
[regression audit](../regression-audit/README.md) measured these two
categorical queries at +2.07% / +2.22% against the pre-Int32 baseline
`0caffc17`. Rerunning that exact configuration against `60ac018e` (see
[Against the pre-Int32 baseline](#against-the-pre-int32-baseline)) gives
-1.9% / -1.7%: the regression is gone, and the small improvement is not
resolved after correction. In the dedicated integer-key benchmark, both widths
are 25-49% faster.

It also fixes a correctness bug. The categorical path indexed its memo with
the dictionary code of null rows, whose payload is arbitrary. A null row could
therefore match a semi join or be dropped from an anti join. The kernel now
reads the validity bit before it touches membership storage.

## What caused the extra work

The integer path called `right_i64_.contains((*col)[row])` through a per-row
lambda. Each row went through `Column` storage dispatch (width and offset) and
the `probe_is_null` check. The categorical path also called `code_at(row)`
per row. A same-executable diagnostic
([`integer-probe-diagnostic.patch`](integer-probe-diagnostic.patch), switched
by `IBEX_DIAG_BIND_SEMI_KEYS`) resolves the storage once with
`visit_integer_storage` and captures the raw key pointer. In this diagnostic:

| Diagnostic toggle | Cases | On/off paired ratio | Max Holm p | Replica |
| --- | ---: | ---: | ---: | ---: |
| One core | 16 | 0.627-0.883 | 0.0031 | 0.985-1.008 |
| Eight cores | 32 | 0.675-0.955 | 0.145 | 0.971-1.030 |

## Rejected form: whole filter inside the width visitor

The first production candidate wrapped the full `filter_rows` call in
`visit_integer_storage`
(`rejected-full-filter-visitor.patch` in local evidence). This instantiates
the complete filter, allocation and gather machinery once per width. The
integer benchmarks kept their gains, but the one-core broad gate resolved
regressions in code this change does not touch:

| Query | Paired effect | Wilcoxon p |
| --- | ---: | ---: |
| `distinct_symbol` | +6.3% | 0.0005 |
| `median_by_symbol` | +1.8% | 0.0005 |
| `quantile_by_symbol` | +1.7% | 0.0005 |
| `null_semi_join` | +1.3% | 0.0005 |
| `null_anti_join` | +1.0% | 0.0015 |

This form was not promoted.

## Production form: one typed call per word

`pack_membership<Key, Membership, Nullable>` is `IBEX_NOINLINE` and takes all
loop state by value: the key pointer, membership pointer, validity buffer and
bit offset, row begin and count. It returns a 64-bit match word. A
`MembershipProbe` holds the chosen function pointer, and `filter_rows` calls
its `word(base, count)` when the predicate provides one. Other predicates keep
the existing per-row loop. There is one filter body, and the generated code
per width is limited to the small packing kernel.

Integer keys widen to Int64 before hashing, so Int32 minimum and maximum
values match their Int64 build-side equivalents.

## Production measurements

All builds are Release/native, compact ingestion is off, and the frequency is
locked at 3.2 GHz. CPU affinity is `2` or `0,2,4,6,8,10,12,14`. Base is
`f1eb185c`; target is the working tree with this change. Each gate has an
unchanged replica that is byte-identical to the base. No build or other
benchmark ran during timing. Effects are medians of paired ratios.

**Broad gates** (`run_gates.sh`). Each uses 12 interleaved triplets, three
iterations, one warmup and all six CSV inputs, with Holm correction across
91 timing rows.

| Cores | Resolved after Holm | Effect | Holm p |
| ---: | --- | ---: | ---: |
| 1 | `null_anti_join` | -2.1% | 0.044 |
| 1 | `filter_arith` | +1.9% | 0.044 |
| 8 | none | | |

At one core, `null_semi_join` is -2.3% (Holm p=0.087). The rejected form's
regressions are gone: `distinct_symbol` 1.003, `median_by_symbol` 0.998 and
`quantile_by_symbol` 0.998, all unresolved. At eight cores, `null_semi_join`
is -4.4% (raw p=0.021, unresolved after correction).

`filter_arith` is a code-placement effect, not a code change. In the two
cached perfcmp builds, `semi_anti_join.cpp.o` is the only object file that
differs; every filter object is byte-identical. The
[arithmetic-filter investigation](../arithmetic-filter/README.md) already
showed that dense filters move by about 1-1.5% when unchanged code is placed
differently. A same-binary counterfactual was not repeated for this run.
`filter_simple` (+1.7%) and `filter_or` (+1.3%) point the same way and are
unresolved.

**Integer-key benchmark** ([bench.cpp](bench.cpp), [build.py](build.py),
[run.py](run.py)). This is the real interpreter running `semi join` and
`anti join` over Int64 or compact-Int32 keys, with and without nulls. Each
state is compiled against its own headers and libraries. It uses 18
interleaved repeats, three iterations and two warmups, and every result
checksum must agree between sides.

| Comparison | Cases | Target/base paired ratio | Max Holm p |
| --- | ---: | ---: | ---: |
| 1M/4M rows, 128/4096 keys, 1 and 8 cores | 64 | 0.494-0.862 | 0.014 |
| Dense control, 100k right rows | 32 | 0.955-1.015 | 1.0 |

At one million rows, 1 core, without nulls, the ratios are Int64 0.68 / 0.55
(128 / 4096 keys) and Int32 0.62 / 0.51. With nulls they are 0.63-0.73.

The focused core/groupagg/null suite (one core, 12 triplets, five iterations)
has no slowdown. Null semi and anti joins resolve at -1.7% / -1.85% there.

## Against the pre-Int32 baseline

This uses the audit's one-core focused confirmation unchanged: base
`0caffc17`, target `60ac018e`, suites `scalar,null,timeframe`, 18 interleaved
triplets, nine iterations, two warmups, CPU 2, Holm correction across 29 rows.
An eight-core run of the `null` suite uses the same repeat counts. Both
replicas are byte-identical to the base, and no control/base comparison
resolves.

| Query | Audit (`d0ee5dd1`) | Now (`60ac018e`) | Holm p now |
| --- | ---: | ---: | ---: |
| `null_semi_join`, 1 core | +2.07% (p=0.0057) | -1.87% | 0.39 |
| `null_anti_join`, 1 core | +2.22% (p=0.0057) | -1.73% | 0.32 |
| `null_semi_join`, 8 cores | | -1.97% | 0.99 |
| `null_anti_join`, 8 cores | | +1.97% | 1.0 |

The same one-core run also resolves four TimeFrame rows. This change does not
touch them:

- `tf_rolling_min_1m` -3.0%, `tf_rolling_max_1m` -2.1% and
  `tf_rolling_median_1m` -1.5%, all faster.
- `tf_rolling_sum_1m` **+4.9%** (Holm p=0.041). The audit measured it flat
  (+0.07%) in this configuration. The broad gate for this change has it at
  +1.0% / -0.2% (1 / 8 cores, unresolved). The earlier integer-abs gate
  (`a9443d01`, which changed only `expr.cpp`) already had it at +2.5% / +5.2%
  (p=0.064 / 0.052, unresolved). The shift therefore lines up with that
  earlier commit. Four follow-up timeframe gates did **not** reproduce the
  +4.9%. They put the integer-abs commit alone at +2.9% / +3.2% and the
  pre-Int32 baseline comparison at +1.2% / +1.7%, none resolved. The fastest
  repeats match the base, which points to code placement rather than a
  slower loop. See the
  [integer-abs follow-up](../integer-abs/README.md#follow-up-tf_rolling_sum_1m).

`tf_rolling_ewma_1m` is +0.5% (audit: +1.29%) and is unresolved.

Raw results: `vs-0caffc17/` in the local evidence directory, including its
`run.sh`.

## Correctness checks

- `categorical semi and anti joins never match nulls across dictionaries`
  (`tests/test_join.cpp`). It covers 65 and 300001 rows, 1 and 4 threads,
  and independent dictionaries. It **fails on the unfixed tree** (`0 == 3`
  at 65 rows, semi).
- `integer membership joins retain signed boundaries in offset storage`
  (`tests/test_integer_no_cache.cpp`). It covers wide and compact offset
  buffers, INT32_MIN/MAX, with and without nulls.
- `test_native_categorical_membership_joins_skip_null_dictionary_codes`
  (`python/test_ibex_joins.py`). It covers sliced Arrow dictionary input and
  null payloads that name matching values.
- An inverted-membership mutant fails all three tests.
- `ctest -LE slow` passes 2491/2491 with `IBEX_COMPACT_INT32=0` and `=1`.
  The GCC build, clang-tidy (no findings in this file) and the clang-format
  dry-run are clean.

## Reproducing

```sh
bash benchmarking/int32/symbol-semi-anti/run_gates.sh \
    benchmarking/results/local/int32/2026-11-10-symbol-semi-anti/word-gate
```

Raw samples, the rejected patch, profiles and test logs are kept in ignored
persistent storage at
`benchmarking/results/local/int32/2026-11-10-symbol-semi-anti/`. The
production gate is in `word-gate/` and the verification logs are in
`word-verify/`. The rejected form's gates are at the top level of that
directory.
