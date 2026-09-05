# Ibex analytics engine description

`engine.tex` is an intentionally incomplete technical foundation, complementary
to `SPEC.md`. It describes the inspected implementation and identifies questions
for author review. `references.bib` contains the cited research literature.

Build with a standard LaTeX installation, from the repository root:

```sh
bash docs/engine/build.sh
```

Equivalently, run these commands from this directory:

```sh
pdflatex -interaction=nonstopmode -halt-on-error engine.tex
bibtex engine
pdflatex -interaction=nonstopmode -halt-on-error engine.tex
pdflatex -interaction=nonstopmode -halt-on-error engine.tex
```

The output is `engine.pdf`. Generated files are ignored by Git. Required packages
are listed in the TeX preamble; no shell escape or external fonts are required.

Interpreted and compiled execution share a functional-equivalence requirement.
The draft's parity bug appendix records encountered divergences; an unsupported
diagnostic alone does not establish an intentional exception.

Local source paths are repository-relative. Each substantive section includes an
implementation evidence paragraph; citations to papers supply context and do
not establish that Ibex implements every technique in those papers.

Agreed direction: address external database-systems readers with operational
detail, using a time-series running workload. Preserve row-order guarantees;
floating-point reproducibility may be relaxed, with the precise contract still
to be defined. `operations.tex` now covers lazy decoding, map kernels, streaming
equality joins, aggregation phases, Arrow ownership, and retained memory.
`cost.tex` explains cost drivers using the September 3, 2026 benchmark snapshot
in `docs/benchmarks.html`, checked against its archived source CSV. It records
hardware, timing boundaries, and the limits of interpreting the measurements.
No new benchmark was run. Detailed time-series semantics, numerical
reproducibility rules, and new performance experiments remain deferred.
The agreed-direction section records remaining investigations.

`trade_summary.ibex` is the executable running example, included directly in
the LaTeX document. From the repository root:

```sh
build-release/tools/ibex --no-history --report-planner docs/engine/trade_summary.ibex
```

This was checked using the existing release executable: two output rows with
volume/notional totals of 30/3040 (AAPL) and 20/4030 (MSFT). The planner reported
statement execution for this in-memory example. This is an example check, not
a benchmark or a compiled/interpreted parity test. No engine rebuild or test
suite run was needed for this documentation change.
