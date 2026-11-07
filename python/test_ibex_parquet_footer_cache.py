"""parquet::read reuses a parsed footer only for the exact file version it came from.

The Parquet plugin caches each local file's parsed footer, dictionary-column
choice and column statistics across queries, validated on every open against
the file's device, inode, size, mtime and ctime. These tests build a cached
entry first -- a file is only cached once its timestamps are two seconds old --
and then change the file in ways the cache must notice, checking answers that
come straight from the footer (the row count), from the pages (a sum), and
from row-group statistics (a filter the old min/max would prune).
"""

from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

import ibex_pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

SETTLE_SECONDS = 2.2  # kParquetFooterSettleNs is 2 s


def query(path: Path) -> tuple[int, int, int]:
    """(row count, sum of x, rows with x > 5) as Ibex reads `path` now."""
    source = (
        'extern fn parquet::read(path: String) -> DataFrame from "parquet.hpp";\n'
        f'let t = parquet::read("{path}");\n'
        "t[select { n = count(), s = sum(x) }];\n"
    )
    totals = ibex_pyarrow.execute_pipeline(source, tables={}, scalars={}).to_pydict()
    filtered = ibex_pyarrow.execute_pipeline(
        'extern fn parquet::read(path: String) -> DataFrame from "parquet.hpp";\n'
        f'parquet::read("{path}")[filter x > 5][select {{ m = count() }}];\n',
        tables={},
        scalars={},
    ).to_pydict()
    return totals["n"][0], totals["s"][0], filtered["m"][0]


def write(path: Path, values: list[int]) -> None:
    pq.write_table(pa.table({"x": pa.array(values, type=pa.int64())}), path)


@unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "Parquet backend disabled")
class ParquetFooterCacheTests(unittest.TestCase):
    def test_a_changed_file_is_never_answered_from_its_old_footer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            grown, same_size, renamed, restored = (
                d / "grown.parquet",
                d / "same_size.parquet",
                d / "renamed.parquet",
                d / "restored.parquet",
            )
            for path in (grown, same_size, renamed, restored):
                write(path, [1, 2, 3])
            time.sleep(SETTLE_SECONDS)  # old enough to be cached
            for path in (grown, same_size, renamed, restored):
                self.assertEqual(query(path), (3, 6, 0))
                self.assertEqual(query(path), (3, 6, 0))  # served from the entry

#Rewritten in place with more rows : size and timestamps move.
            write(grown, list(range(1000)))
            self.assertEqual(query(grown), (1000, sum(range(1000)), 994))

#Same byte size, different values : only the timestamps move.
            old = same_size.stat()
            write(same_size, [7, 8, 9])
            self.assertEqual(same_size.stat().st_size, old.st_size)
            self.assertEqual(query(same_size), (3, 24, 3))

#Replaced by rename, with the old mtime restored on the new file:
#same size and mtime, but a new inode.
            old = renamed.stat()
            replacement = d / "replacement.parquet"
            write(replacement, [7, 8, 9])
            os.utime(replacement, ns=(old.st_atime_ns, old.st_mtime_ns))
            os.replace(replacement, renamed)
            self.assertEqual(renamed.stat().st_size, old.st_size)
            self.assertEqual(renamed.stat().st_mtime_ns, old.st_mtime_ns)
            self.assertEqual(query(renamed), (3, 24, 3))

#Rewritten in place, same size, mtime set back with utime : size,
#inode and mtime all match the cached entry; only ctime differs.
            old = restored.stat()
            write(restored, [7, 8, 9])
            os.utime(restored, ns=(old.st_atime_ns, old.st_mtime_ns))
            now = restored.stat()
            self.assertEqual((now.st_ino, now.st_size, now.st_mtime_ns),
                             (old.st_ino, old.st_size, old.st_mtime_ns))
            self.assertNotEqual(now.st_ctime_ns, old.st_ctime_ns)
            self.assertEqual(query(restored), (3, 24, 3))

    def test_a_just_written_file_reads_its_current_contents(self) -> None:
#A file inside the settle window is read but not stored.That rule
#guards a same - size rewrite inside one timestamp tick, which leaves
#ctime unchanged too; a test cannot produce one, since any rewrite
#it makes moves ctime, so this checks only the visible behaviour.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fresh.parquet"
            write(path, [1, 2, 3])
            self.assertEqual(query(path), (3, 6, 0))
            stat = path.stat()
            write(path, [7, 8, 9])
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            self.assertEqual(query(path), (3, 24, 3))


if __name__ == "__main__":
    unittest.main()
