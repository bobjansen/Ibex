#!/usr/bin/env python3
"""Import the first N rows of examples/measurements.txt into a SQLite database."""

from __future__ import annotations

import argparse
import pathlib
import sqlite3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the first N rows of a semicolon-delimited measurements file into SQLite."
    )
    parser.add_argument("--input", default="examples/measurements.txt",
                        help="Path to the source measurements.txt file.")
    parser.add_argument("--output", default="/tmp/measurements_1m.sqlite",
                        help="Path to the output SQLite database.")
    parser.add_argument("--limit", type=int, default=1_000_000,
                        help="Number of rows to import.")
    parser.add_argument("--batch-size", type=int, default=50_000,
                        help="Rows per executemany batch.")
    return parser.parse_args()


def flush(cur: sqlite3.Cursor, rows: list[tuple[str, float]]) -> None:
    if rows:
        cur.executemany("insert into measurements(station, temp) values (?, ?)", rows)
        rows.clear()


def main() -> int:
    args = parse_args()
    input_path = pathlib.Path(args.input)
    output_path = pathlib.Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(output_path)
    cur = conn.cursor()
    cur.execute("pragma journal_mode = WAL")
    cur.execute("pragma synchronous = OFF")
    cur.execute("drop table if exists measurements")
    cur.execute("create table measurements(station text not null, temp real not null)")

    imported = 0
    pending: list[tuple[str, float]] = []
    with input_path.open("r", encoding="utf-8") as f:
        for line in f:
            if imported >= args.limit:
                break
            station, temp = line.rstrip("\n").split(";", 1)
            pending.append((station, float(temp)))
            imported += 1
            if len(pending) >= args.batch_size:
                flush(cur, pending)

    flush(cur, pending)
    cur.execute("create index idx_measurements_station on measurements(station)")
    conn.commit()
    conn.close()

    print(f"imported_rows={imported}")
    print(f"sqlite_db={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
