# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Run the unchanged upstream q06 function with Polars and independent Ibex."""

import argparse
import importlib
from pathlib import Path
import sys
from unittest.mock import patch

import polars
import ibex
import ibex_pyarrow


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdsh-root", type=Path, required=True)
    parser.add_argument("--lineitem", type=Path, required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(args.pdsh_root.resolve()))
    module = importlib.import_module("queries.polars.q6")
    # Only the module's frontend binding and explicit input differ. Upstream
    # files, query body, loader utilities and reference engine remain intact.
    reference = module.q(lineitem=polars.scan_parquet(args.lineitem)).collect(engine="streaming")
    with patch.object(module, "pl", ibex):
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            query = module.q(lineitem=ibex.scan_parquet(args.lineitem))
            assert execute.call_count == 0, "query construction executed data"
            result = query.collect()
            assert execute.call_count == 1, "expected exactly one native pipeline invocation"
    assert result.columns == reference.columns
    assert result.shape == reference.shape
    assert result.to_arrow().schema.types == reference.to_arrow().schema.types
    import math
    assert math.isclose(result.item(), reference.item(), rel_tol=1e-12, abs_tol=1e-8), (
        result.item(), reference.item()
    )
    print(f"upstream q06 passed: revenue={result.item():.12g}; Polars {polars.__version__}; native calls=1")


if __name__ == "__main__":
    main()
