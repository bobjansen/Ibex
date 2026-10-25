# Polars versus Ibex join performance

Measured 2026-10-25 on a 13th Gen Intel(R) Core(TM) i7-13700, Linux, with Polars 2.0.0 and a Release Ibex bridge. The two runs use identical seeded resident inputs and the same source/binary hashes.

Each number below is the median of 12 alternating-order repetitions after two warmups. Collection includes execution and conversion to a real Polars DataFrame. Ibex plans and bindings are prepared before timing; Polars uses ordinary `collect()` with its default auto engine. Input creation, preparation, and full-result correctness comparisons are outside the timed section.

The one-thread run sets both `IBEX_CORES=1` and `POLARS_MAX_THREADS=1`. The default run unsets both settings; Polars reports 24 threads and Ibex derives a 24-CPU compute budget from the recorded affinity.

| Workload | Left / right rows | Output rows | 1 thread Polars ms | 1 thread Ibex ms | Default Polars ms | Default Ibex ms |
|---|---:|---:|---:|---:|---:|---:|
| Inner, unique integer RHS | 1,000,000 / 200,000 | 1,000,000 | 10.77 | 6.68 | 5.60 | 6.36 |
| Left, half the keys matched | 1,000,000 / 100,000 | 1,000,000 | 16.85 | 19.06 | 5.42 | 6.61 |
| Full, larger RHS key domain | 1,000,000 / 240,000 | 1,041,270 | 19.70 | 30.70 | 8.98 | 19.69 |
| Semi, about half retained | 1,000,000 / 100,000 | 500,090 | 11.90 | 4.70 | 2.33 | 1.22 |
| Anti, about half retained | 1,000,000 / 100,000 | 499,910 | 12.56 | 4.97 | 2.82 | 1.44 |
| Inner, many-to-many | 250,000 / 50,000 | 500,000 | 3.56 | 3.11 | 1.21 | 2.46 |
| Inner, two integer keys | 1,000,000 / 200,000 | 1,000,000 | 31.05 | 63.24 | 9.63 | 22.57 |
| Inner, categorical key | 1,000,000 / 10,000 | 1,000,000 | 7.72 | 14.67 | 2.14 | 13.16 |
| Cross | 1,000 / 100 | 100,000 | 0.15 | 0.49 | 0.23 | 0.38 |

With one thread, Ibex has lower medians for the unique integer inner join (1.61×), semi/anti joins (about 2.53×), and the bounded many-to-many join (1.14×). With default threading, semi/anti remain faster in Ibex (about 1.9×), while Polars has lower medians for the other seven cases. Categorical keys (6.15×), composite keys (2.34×), and full joins (2.19×) show the largest default-thread gaps in this set. These are end-to-end frontend measurements and do not isolate the native join kernel.

Preparation is currently a major first-use cost. The visitor omits join cardinality validation, so the adapter serializes original plan metadata once to reject unsupported validation assertions. Resident input serialization is included in that work. Prepared queries cache the resulting plan and input bindings.

| Workload | Default Ibex prepare ms | Default Polars first collect ms | Default Ibex prepare + first collect ms |
|---|---:|---:|---:|
| Inner, unique integer RHS | 687.04 | 5.91 | 692.47 |
| Left, half the keys matched | 616.37 | 4.19 | 620.68 |
| Full, larger RHS key domain | 654.50 | 8.58 | 672.06 |
| Semi, about half retained | 593.24 | 3.33 | 594.42 |
| Anti, about half retained | 575.97 | 3.30 | 577.26 |
| Inner, many-to-many | 156.04 | 2.93 | 158.62 |
| Inner, two integer keys | 911.39 | 7.59 | 929.82 |
| Inner, categorical key | 459.80 | 3.32 | 474.12 |
| Cross | 0.83 | 0.32 | 1.46 |

Warm semi/anti speedups therefore do not imply first-call wins. For example, default-thread semi collection takes 1.22 ms after preparation, but preparation plus the first collection takes 594 ms versus a 3.33 ms Polars first collection. Improving metadata access is a separate integration priority.

All output rows, values and dtypes were compared against Polars before timing. Row order is unspecified in these benchmark joins and is excluded from equality checks. Timings are a local synthetic sample, not a general workload ranking; default-thread full-join samples in particular have substantial variation (Ibex IQR 7.71 ms). Raw samples and IQRs are retained below.

- [One-thread samples and provenance](join-performance-results/single.json)
- [Default-thread samples and provenance](join-performance-results/default.json)
- [Benchmark runner](bench_polars_joins.py)

```bash
PYTHONPATH=build-release/python:python IBEX_CORES=1 POLARS_MAX_THREADS=1 \
  .venv/bin/python python/bench_polars_joins.py --rows 1000000 --keys 200000 \
  --iters 12 --out /tmp/joins-single.json
env -u IBEX_CORES -u POLARS_MAX_THREADS PYTHONPATH=build-release/python:python \
  .venv/bin/python python/bench_polars_joins.py --rows 1000000 --keys 200000 \
  --iters 12 --out /tmp/joins-default.json
```

## Categorical output conversion follow-up

The table above records the original converter. Commit `f0144ade` replaces
Polars' per-row Arrow dictionary import with dictionary-label translation once
per chunk and an integer gather into the requested Polars category mapping.
Native execution and Arrow export are unchanged.

A focused million-row / 10,000-category inner join compares both converters
using the **same Release binary, prepared native plan and input bindings**.
Twenty alternating-order repetitions follow two warmups; complete outputs match
Polars, including dtypes. Preparation and correctness checks are outside timing.
Collection includes native execution and conversion; conversion-only samples
reuse one native Arrow result. Previous result destruction is outside timing.
These focused inputs have their own seeded RNG sequence, so compare before and
after here rather than treating these as replacements for the original suite.

| Mode | Conversion before ms | Conversion after ms | Collection before ms | Collection after ms | Polars collection ms |
|---|---:|---:|---:|---:|---:|
| One thread | 11.65 | 2.54 | 14.47 | 5.03 | 7.95 |
| Default (24 threads) | 11.52 | 2.51 | 13.17 | 4.41 | 2.11 |

This reduces conversion time about 4.6× and full collection time about 2.9–3.0×.
Default-thread Polars remains faster. The one-time preparation cost described
above is unchanged. Tests cover different per-chunk dictionary orders, sliced
indices, null indices and dictionary entries, empty outputs, named category
mappings, UInt8/UInt16 physical category codes, and native projection/grouping.
A mutation replacing null indices with zero causes the boundary test to fail.

- [Focused A/B runner](bench_categorical_export.py)
- [One-thread samples](join-performance-results/categorical-export-single.json)
- [Default-thread samples](join-performance-results/categorical-export-default.json)

```bash
PYTHONPATH=build-release/python:python IBEX_CORES=1 POLARS_MAX_THREADS=1 \
  .venv/bin/python python/bench_categorical_export.py --out /tmp/categorical-single.json
env -u IBEX_CORES -u POLARS_MAX_THREADS PYTHONPATH=build-release/python:python \
  .venv/bin/python python/bench_categorical_export.py --out /tmp/categorical-default.json
```
