# Compact historical benchmark summaries

The four JSON summaries under `20261108/` are inputs to
`python/summarize_polars_engines.py`. They retain validation status, timing
summaries and provenance, with full answers, iteration dumps and generated
query sources removed. They describe historical runs, not fresh measurements.

Original full reports and failed answer-check attempts are preserved locally
under `benchmarking/results/local/archived-json/python/polars-engines-results/`.
The associated TSVs were never tracked. Repaired join findings and reproducible
scripts are described in [the join-gap plan](../../plans/polars-engine-join-gap.md).
