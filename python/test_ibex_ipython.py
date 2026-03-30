from __future__ import annotations

import os
import tempfile
from pathlib import Path

os.environ.setdefault("IPYTHONDIR", "/tmp/ibex-ipython")

import pandas as pd
import pyarrow as pa
from IPython.core.interactiveshell import InteractiveShell

import ibex_ipython


def main() -> int:
    shell = InteractiveShell.instance()
    ibex_ipython.load_ipython_extension(shell)

    shell.user_ns["trades_df"] = pd.DataFrame(
        {
            "symbol": ["A", "A", "B"],
            "qty": [3, 4, 5],
        }
    )
    result = shell.run_cell_magic(
        "ibex",
        "--bind trades=trades_df --as pandas --out grouped --quiet",
        """
        trades[select { total_qty = sum(qty) }, by symbol, order symbol];
        """,
    )
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict(orient="list") == {
        "symbol": ["A", "B"],
        "total_qty": [7, 5],
    }
    assert shell.user_ns["grouped"].to_dict(orient="list") == {
        "symbol": ["A", "B"],
        "total_qty": [7, 5],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "demo.ibex"
        script_path.write_text(
            """
            Table { x = [1, 2, 3], y = [10, 20, 30] }[filter x >= 2];
            """,
            encoding="utf-8",
        )
        file_result = shell.run_line_magic(
            "ibexfile",
            f"--as pyarrow --out filtered --quiet {script_path}",
        )
        assert isinstance(file_result, pa.Table)
        assert file_result.to_pydict() == {
            "x": [2, 3],
            "y": [20, 30],
        }
    assert shell.user_ns["filtered"].to_pydict() == {
        "x": [2, 3],
        "y": [20, 30],
    }

    csv_result = shell.run_cell_magic(
        "ibex",
        "--as pyarrow --out iris_count --quiet",
        """
        extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

        read_csv("data/iris.csv")[select total = count()];
        """,
    )
    assert isinstance(csv_result, pa.Table)
    assert csv_result.to_pydict() == {"total": [150]}
    assert shell.user_ns["iris_count"].to_pydict() == {"total": [150]}

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
