# Ibex for Python

Ibex runs your existing Polars queries. Write ordinary Polars code and pass
`engine=IbexEngine()` when you collect; you get back a normal Polars
DataFrame.

## Install

Prebuilt `ibex-polars` wheels cover Python 3.11–3.14 on Linux x86_64
(glibc 2.28+) and Windows x86_64; no compiler is needed:

```bash
pip install --pre ibex-polars --find-links https://github.com/bobjansen/Ibex/releases/expanded_assets/wheels-latest
```

That installs the current `main`; pip needs `--pre` for its development
versions. To install a tagged release (v0.5.0 onwards, for example), replace
`wheels-latest` with the release tag and drop `--pre`. The wheel pulls in
`polars==2.1.0`, `pyarrow` and `numpy`; the engine only works with that exact
Polars release.

## Use

```python
import polars as pl
from ibex_polars import IbexEngine

engine = IbexEngine()
result = (
    pl.scan_parquet("trades.parquet")
    .filter(pl.col("qty") > 10)
    .group_by("symbol")
    .agg(avg_price=pl.mean("price"))
    .collect(engine=engine)
)
```

Queries the engine cannot run raise an error instead of silently falling back
to Polars. [The engine README](ibex_polars/README.md) covers what is
supported, writing results with `sink_parquet`, and macOS wheels.

## Performance

On the 22 queries of the PDS-H benchmark (TPC-H style, scale factor 10),
`IbexEngine` gives the same results as Polars and takes about 7% less time
than Polars' own streaming engine on 8 threads, and about 12% less on
1 thread. The [performance summary](performance.md) has the per-query
numbers, other benchmarks, and how to rerun them.

## Building from source

To work on the bridge itself, build the extension with a Python that has
development headers, PyArrow and Polars 2.1.0 installed:

```bash
cmake -S . -B build-release -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release \
  -DIBEX_BUILD_PYTHON_BRIDGE=ON -DIBEX_BUILD_PARQUET=ON \
  -DIBEX_PYTHON_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build build-release --target ibex_pyarrow --parallel 4
PYTHONPATH=build-release/python:python .venv/bin/python python/test_ibex_polars.py
```

If CMake has cached headers from a different interpreter, also set
`Python3_INCLUDE_DIR` to the selected interpreter's include directory.

Python has to find both `build-release/python` (the extension) and `python/`
(the packages) from whatever directory it runs in. Register them with the
venv once, from the repository root:

```bash
.venv/bin/python -c 'import pathlib, site; root = pathlib.Path.cwd(); pathlib.Path(site.getsitepackages()[0], "ibex.pth").write_text(f"{root}/build-release/python\n{root}/python\n")'
```

Alternatively, set `PYTHONPATH="$PWD/build-release/python:$PWD/python"` from
the repository root in every shell.

To build a wheel from a checkout instead (compiles Arrow, so the first build
takes a while), run `pip wheel --no-deps ./python -w dist`.
