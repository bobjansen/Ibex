from __future__ import annotations

import datetime as dt
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa


def add_bridge_module_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    for build_dir_name in ("build-release", "build"):
        candidate = repo_root / build_dir_name / "python"
        if candidate.is_dir():
            sys.path.insert(0, str(candidate))
            return
    raise RuntimeError("could not find a built ibex_pyarrow module under build-release/python or build/python")


add_bridge_module_path()

import ibex_pyarrow


def default_plugin_paths() -> list[str]:
    repo_root = Path(__file__).resolve().parents[1]
    paths: list[str] = []
    for build_dir_name in ("build-release", "build"):
        for relative in ("tools", "libraries"):
            candidate = repo_root / build_dir_name / relative
            if candidate.is_dir():
                paths.append(str(candidate))
    return paths


def main() -> int:
    table = ibex_pyarrow.eval_table(
        """
        Table {
            price = [10, 20, 30],
            active = [true, false, true],
            label = ["A", "B", "C"]
        };
        """
    )

    assert isinstance(table, pa.Table)
    print("base table:")
    print(table)
    print(table.to_pydict())
    assert table.column_names == ["price", "active", "label"]
    assert table.to_pydict() == {
        "price": [10, 20, 30],
        "active": [True, False, True],
        "label": ["A", "B", "C"],
    }

    filtered = ibex_pyarrow.eval_table(
        """
        Table { x = [1, 2, 3, 4], flag = [true, false, true, false] }[filter x >= 3];
        """
    )
    print("\nfiltered table:")
    print(filtered)
    print(filtered.to_pydict())
    assert filtered.to_pydict() == {
        "x": [3, 4],
        "flag": [True, False],
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = Path(tmpdir) / "demo.ibex"
        script_path.write_text(
            """
            Table { symbol = ["A", "B"], qty = [7, 9] };
            """,
            encoding="utf-8",
        )
        from_file = ibex_pyarrow.eval_file(str(script_path))
        print("\nfile table:")
        print(from_file)
        print(from_file.to_pydict())
        assert from_file.to_pydict() == {
            "symbol": ["A", "B"],
            "qty": [7, 9],
        }

    from_dict = ibex_pyarrow.eval_table(
        """
        trades[select { total_qty = sum(qty) }, by symbol, order symbol];
        """,
        tables={
            "trades": {
                "symbol": ["A", "A", "B"],
                "qty": [3, 4, 5],
            }
        },
    )
    print("\ndict-bound table:")
    print(from_dict)
    print(from_dict.to_pydict())
    assert from_dict.to_pydict() == {
        "symbol": ["A", "B"],
        "total_qty": [7, 5],
    }

    scalar_bound = ibex_pyarrow.eval_table(
        """
        trades[update { qty_plus_offset = qty + offset }][select { qty_plus_offset }];
        """,
        tables={
            "trades": {
                "qty": [3, 4, 5],
            }
        },
        scalars={"offset": 10},
    )
    print("\nscalar-bound table:")
    print(scalar_bound)
    print(scalar_bound.to_pydict())
    assert scalar_bound.to_pydict() == {
        "qty_plus_offset": [13, 14, 15],
    }

    temporal_scalars = ibex_pyarrow.eval_table(
        """
        Table { x = [1] }[update {
            keep = enabled,
            d = trade_day,
            ts = cutoff
        }];
        """,
        scalars={
            "enabled": True,
            "trade_day": dt.date(2024, 1, 2),
            "cutoff": dt.datetime(2024, 1, 2, 3, 4, 5, 6000),
        },
    )
    print("\ntemporal-scalar table:")
    print(temporal_scalars)
    print(temporal_scalars.to_pydict())
    assert temporal_scalars.to_pydict() == {
        "x": [1],
        "keep": [True],
        "d": [dt.date(2024, 1, 2)],
        "ts": [dt.datetime(2024, 1, 2, 3, 4, 5, 6000)],
    }

    arrow_orders = pa.table(
        {
            "symbol": pa.array(["A", "B", "B", None]),
            "px": pa.array([10.5, 20.0, 21.5, 99.0]),
        }
    )
    from_arrow = ibex_pyarrow.eval_table(
        """
        orders[filter symbol is not null, select { avg_px = mean(px) }, by symbol, order symbol];
        """,
        tables={"orders": arrow_orders},
    )
    print("\npyarrow-bound table:")
    print(from_arrow)
    print(from_arrow.to_pydict())
    assert from_arrow.to_pydict() == {
        "symbol": ["A", "B"],
        "avg_px": [10.5, 20.75],
    }

    pandas_quotes = pd.DataFrame(
        {
            "venue": ["XNAS", "BATS", "XNAS"],
            "spread_bps": [5.0, 7.5, 6.0],
        }
    )
    from_pandas = ibex_pyarrow.eval_table(
        """
        quotes[select { avg_spread = mean(spread_bps) }, by venue, order venue];
        """,
        tables={"quotes": pandas_quotes},
    )
    print("\npandas-bound table:")
    print(from_pandas)
    print(from_pandas.to_pydict())
    assert from_pandas.to_pydict() == {
        "venue": ["BATS", "XNAS"],
        "avg_spread": [7.5, 5.5],
    }

    from_csv = ibex_pyarrow.eval_table(
        """
        extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

        read_csv("data/iris.csv")[select total = count()];
        """,
        plugin_paths=default_plugin_paths(),
    )
    print("\nplugin-backed csv table:")
    print(from_csv)
    print(from_csv.to_pydict())
    assert from_csv.to_pydict() == {"total": [150]}

    session = ibex_pyarrow.create_session(plugin_paths=default_plugin_paths())
    define_result = ibex_pyarrow.session_eval_table(
        session,
        """
        extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
        let iris = read_csv("data/iris.csv");
        """,
    )
    assert define_result is None
    session_result = ibex_pyarrow.session_eval_table(
        session,
        """
        iris[select total = count()];
        """,
    )
    print("\nsession-backed table:")
    print(session_result)
    print(session_result.to_pydict())
    assert session_result.to_pydict() == {"total": [150]}
    ibex_pyarrow.reset_session(session)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
