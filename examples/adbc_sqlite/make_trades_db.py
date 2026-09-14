#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Create the small, deterministic trades database used by trades.ibex.

Standard library only. Two rows are deliberately incomplete (a NULL price and
a NULL quantity) so the walkthrough can show how nulls arrive in Ibex.
"""

import sqlite3
import sys
from pathlib import Path

TRADES = [
    # id, ts,                    symbol, side, qty,  price
    (1, "2026-03-02 09:30:01", "AAPL", "B", 100, 190.50),
    (2, "2026-03-02 09:30:04", "MSFT", "B", 50, 410.00),
    (3, "2026-03-02 09:31:10", "AAPL", "S", 40, 191.00),
    (4, "2026-03-02 09:31:15", "NVDA", "B", 10, None),
    (5, "2026-03-02 09:32:00", "MSFT", "S", 20, 412.50),
    (6, "2026-03-02 09:32:30", "NVDA", "B", 25, 120.00),
    (7, "2026-03-02 09:33:05", "AAPL", "B", None, 190.75),
    (8, "2026-03-02 09:34:12", "NVDA", "S", 30, 121.50),
    (9, "2026-03-02 09:35:40", "MSFT", "B", 10, 409.00),
    (10, "2026-03-02 09:36:02", "AAPL", "S", 60, 191.25),
    (11, "2026-03-02 09:37:30", "NVDA", "B", 15, 119.00),
]


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} OUTPUT.sqlite", file=sys.stderr)
        return 2
    out = Path(sys.argv[1])
    out.unlink(missing_ok=True)
    with sqlite3.connect(out) as db:
        db.execute(
            "create table trades ("
            " id integer primary key, ts text not null, symbol text not null,"
            " side text not null, qty integer, price real)"
        )
        db.executemany("insert into trades values (?, ?, ?, ?, ?, ?)", TRADES)
    db.close()
    print(f"wrote {len(TRADES)} trades to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
