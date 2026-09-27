#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Check the walkthrough against a PostgreSQL database already seeded with seed.sql.

Only reads the database. Pass --uri explicitly; no database is created or reset.
"""
import argparse
import csv
import io
import json
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ibex", default="build/tools/ibex")
    parser.add_argument("--plugins", default="build/tools")
    parser.add_argument("--driver", default="postgresql")
    parser.add_argument("--uri", required=True)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent

    def run(script, extra=(), success=True):
        result = subprocess.run(
            [args.ibex, "--no-history", "--plugin-path", args.plugins, str(script), *extra],
            capture_output=True, text=True, timeout=60,
        )
        if (result.returncode == 0) != success:
            raise RuntimeError(result.stdout + result.stderr)
        return result.stdout + result.stderr

    output = run(here / "types.ibex", ["--", "--driver", args.driver, "--uri", args.uri])
    rows = [tuple(part.strip() for part in line.split("|")[1:-1])
            for line in output.splitlines() if line.startswith("|")]
    for expected in [("label", "rows", "priced", "total"),
                     ('"alpha"', "2", "1", "12.34"), ('"beta"', "1", "1", "-0.50"),
                     ("id", "big_n", "tstz")]:
        if expected not in rows:
            raise RuntimeError(f"Missing walkthrough result {expected!r}:\n{output}")
    if "rows: 0" not in output or "2026-01-02 03:04:05.123456" not in output:
        raise RuntimeError(f"Missing empty result or timestamp precision:\n{output}")

    # Exercise text -> Decimal at the precision boundary, without changing the
    # walkthrough fixture. CSV comparisons avoid the REPL's display formatting.
    with tempfile.TemporaryDirectory(prefix="ibex-postgresql-") as directory:
        script = Path(directory) / "check.ibex"
        result_csv = Path(directory) / "result.csv"
        maximum = "9" * 36 + ".99"
        sql = ("select price from (values (1, '" + maximum + "'::numeric(38,2)), "
               "(2, '-0.01'::numeric(38,2)), (3, null::numeric(38,2))) "
               "as t(id, price) order by id")

        def source(query):
            return ('import "adbc"; import "csv";\nlet t = read_adbc('
                    + ", ".join(json.dumps(s) for s in (args.driver, args.uri, query))
                    + ')[update { price = Decimal(price, 38, 2) }];\n'
                    + 'write_csv(t, ' + json.dumps(result_csv.as_posix()) + ');\n')

        script.write_text(source(sql), encoding="utf-8")
        run(script)
        actual = list(csv.reader(io.StringIO(result_csv.read_text(encoding="utf-8"))))
        # A null in a single-column CSV is an empty line (csv.reader yields []).
        if actual != [["price"], [maximum], ["-0.01"], []]:
            raise RuntimeError(f"Decimal values or null did not survive: {actual!r}")

        script.write_text(source("select price from (values (null::numeric(38,2))) "
                                 "as t(price) where false"), encoding="utf-8")
        run(script)
        if result_csv.read_text(encoding="utf-8").strip() != "price":
            raise RuntimeError("Empty Decimal result lost its schema")

        script.write_text(source("select '" + "9" * 37 + ".99'::numeric as price"),
                          encoding="utf-8")
        error = run(script, success=False)
        if "decimal" not in error.lower() or "38" not in error:
            raise RuntimeError(f"Expected a decimal precision error: {error}")
    print("PostgreSQL walkthrough, exact Decimal values, nulls, empty result and precision checks passed")


if __name__ == "__main__":
    main()
