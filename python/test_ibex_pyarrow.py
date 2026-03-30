from __future__ import annotations

import pyarrow as pa

import ibex_pyarrow


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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
