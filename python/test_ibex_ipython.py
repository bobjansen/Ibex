from __future__ import annotations

import datetime as dt
import os
import tempfile
from pathlib import Path

os.environ.setdefault("IPYTHONDIR", "/tmp/ibex-ipython")

import pandas as pd
import pyarrow as pa
from IPython.core.interactiveshell import InteractiveShell

import ibex_ipython


def main() -> int:
    iris_csv = Path(__file__).resolve().parents[1] / "data" / "iris.csv"

    shell = InteractiveShell.instance()
    ibex_ipython.load_ipython_extension(shell)

    shell.user_ns["trades_df"] = pd.DataFrame(
        {
            "symbol": ["A", "A", "B"],
            "qty": [3, 4, 5],
        }
    )
    shell.user_ns["offset"] = 10
    shell.user_ns["enabled"] = True
    result = shell.run_cell_magic(
        "ibex",
        "--bind trades=trades_df --bind offset=offset --bind enabled=enabled --as pandas --out grouped --quiet",
        """
        trades[
            update { qty_plus_offset = qty + offset, keep = enabled }
        ]
        [select { total_qty = sum(qty_plus_offset) }, by symbol, order symbol];
        """,
    )
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict(orient="list") == {
        "symbol": ["A", "B"],
        "total_qty": [27, 15],
    }
    assert shell.user_ns["grouped"].to_dict(orient="list") == {
        "symbol": ["A", "B"],
        "total_qty": [27, 15],
    }

    shell.user_ns["trade_day"] = dt.date(2024, 1, 2)
    shell.user_ns["cutoff"] = dt.datetime(2024, 1, 2, 3, 4, 5, 6000)
    temporal = shell.run_cell_magic(
        "ibex",
        "--bind trade_day=trade_day --bind cutoff=cutoff --as pyarrow --out temporal --quiet",
        """
        Table { x = [1] }[update { d = trade_day, ts = cutoff }];
        """,
    )
    assert isinstance(temporal, pa.Table)
    assert temporal.to_pydict() == {
        "x": [1],
        "d": [dt.date(2024, 1, 2)],
        "ts": [dt.datetime(2024, 1, 2, 3, 4, 5, 6000)],
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
        f"""
        extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

        read_csv("{iris_csv}")[select total = count()];
        """,
    )
    assert isinstance(csv_result, pa.Table)
    assert csv_result.to_pydict() == {"total": [150]}
    assert shell.user_ns["iris_count"].to_pydict() == {"total": [150]}

    define_only = shell.run_cell_magic(
        "ibex",
        "--quiet",
        """
        let train = Table { x = [1, 2, 3], y = [10, 20, 30] };
        """,
    )
    assert define_only is None

    persisted = shell.run_cell_magic(
        "ibex",
        "--as pyarrow --out persisted --quiet",
        """
        train[select { total = sum(y) }];
        """,
    )
    assert isinstance(persisted, pa.Table)
    assert persisted.to_pydict() == {"total": [60]}
    assert shell.user_ns["persisted"].to_pydict() == {"total": [60]}

    shell.run_line_magic("ibexreset", "")
    try:
        shell.run_cell_magic(
            "ibex",
            "--quiet",
            """
            train[select { total = sum(y) }];
            """,
        )
    except Exception:
        pass
    else:
        raise AssertionError("expected train binding to be cleared after %ibexreset")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
