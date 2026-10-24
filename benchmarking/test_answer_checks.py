# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Adversarial tests for the benchmark correctness gate (no timings required)."""
import contextlib
import csv
import io
import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import check_scale_answers as answers
import check_scale_run as scale


class AnswerChecks(unittest.TestCase):
    def compare(self, query, header, left, right):
        return answers.compare_answer(query, (header, left), (header, right))

    def test_string_corruption_and_key_value_association(self):
        header = ["symbol", "total"]
        left = [["A", 1.0], ["B", 2.0]]
        self.assertTrue(self.compare("sum", header, left, [["X", 1.0], ["B", 2.0]]))
        self.assertTrue(self.compare("sum", header, left, [["A", 2.0], ["B", 1.0]]))
        self.assertFalse(self.compare("sum", header, left, list(reversed(left))))

    def test_equal_global_sums_cannot_hide_wrong_columns(self):
        self.assertTrue(self.compare("aggregate", ["a", "b"], [[1., 2.]], [[2., 1.]]))
        self.assertTrue(answers.compare_answer("aggregate", (["a"], [[1.]]), (["b"], [[1.]])))
        self.assertTrue(self.compare("distinct", ["s"], [["A"], ["B"]], [["A"], ["A"]]))

    def test_sort_order_and_ties(self):
        header = ["symbol", "price"]
        rows = [["A", 1.], ["B", 2.]]
        self.assertTrue(self.compare("sort_price", header, rows, list(reversed(rows))))
        self.assertFalse(self.compare("sort_price", header, [["A", 1.], ["B", 1.]],
                                      [["B", 1.], ["A", 1.]]))
        self.assertTrue(self.compare("order_head_topk_by_symbol", header,
                                     [["A", 2.], ["A", 1.]], [["A", 1.], ["A", 2.]]))

    def test_null_nan_empty_and_exact_integers(self):
        self.assertTrue(self.compare("x", ["x"], [[None]], [[float("nan")]]))
        self.assertTrue(self.compare("x", ["x"], [[None]], [[""]]))
        self.assertTrue(self.compare("x", ["x"], [[2**53]], [[2**53 + 1]]))
        self.assertFalse(self.compare("x", ["x"], [[float("nan")]], [[float("nan")]]))
        self.assertFalse(self.compare("x", ["x"], [[1.]], [[1. + 1e-12]]))
        self.assertTrue(self.compare("x", ["x"], [[float("inf")]], [[float("-inf")]]))

    def test_digest_cli_is_strict_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            a, b = pathlib.Path(tmp) / "a.tsv", pathlib.Path(tmp) / "b.tsv"
            a.write_text("query\trows\tnumeric_sum\nsort_price\t2\t3\n")
            b.write_text("query\trows\tnumeric_sum\nsort_price\t2\t999\n")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(answers.main([str(a), str(b)]), 1)
                self.assertEqual(answers.main([str(a), str(b), "--report-only"]), 0)
                b.write_text("query\trows\tnumeric_sum\n")
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(answers.main([str(a), str(b), "--report-only"]), 2)
            self.assertTrue(answers.digest_errors({"a": (1, 1.)}, {"b": (1, 1.)}))
            self.assertTrue(answers.digest_errors({"a": (1, 1.)}, {"a": (1, 1.)}, ["missing"]))

    def test_malformed_and_duplicate_digests_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "digest.tsv"
            for body in ("x\t1\t1\nx\t1\t1\n", "x\t1\tnan\n", "x\t-1\t1\n"):
                path.write_text("query\trows\tnumeric_sum\n" + body)
                with self.assertRaises(ValueError):
                    answers.load(path)

    def test_full_artifacts_required_and_roundtrip_strings(self):
        import bench_python as bp
        import polars as pl
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            a, b = root / "a", root / "b"
            values = pl.DataFrame({"s": ['α,"line\nbreak', "", None]})
            for directory in (a, b):
                with patch.dict(os.environ, {"IBEX_ANSWERS_DIR": str(directory)}):
                    bp.record_full_answer("distinct_symbol", values)
            self.assertFalse(answers.compare_directories(a, b, ["distinct_symbol"]))
            self.assertTrue(answers.compare_directories(a, b, ["missing"]))
            self.assertEqual(answers.read_answer(a / "distinct_symbol.csv")[1],
                             [['α,"line\nbreak'], [""], [None]])
            (b / "distinct_symbol.csv").unlink()
            self.assertTrue(answers.compare_directories(a, b))

    def test_truncated_snapshot_cannot_match_declared_rows(self):
        from verify_scale_answers import check_exported_count
        with tempfile.TemporaryDirectory() as tmp:
            directory = pathlib.Path(tmp)
            (directory / "q.csv").write_text("x\ni:1\n")
            with self.assertRaisesRegex(ValueError, "exported 1 rows, harness reported 2"):
                check_exported_count(directory, "q", 2)

    def test_scale_checks_measured_coverage_and_preflight_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            report = root / "answers.json"
            report.write_text(json.dumps({"status": "passed", "checks": [{"query_names": ["q"]}]}))
            for stem, fw, filename in (("ibex", "ibex", "ibex.tsv"),
                                       ("polars", "polars", "python.tsv")):
                (root / filename).write_text("framework\tquery\tavg_ms\trows\n" + fw + "\tq\t1\t1\n")
                (root / (stem + "_digest.tsv")).write_text("query\trows\tnumeric_sum\nq\t1\t2\n")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(scale.check(root, ["ibex"], ["polars"], report))
            report.write_text(json.dumps({"status": "failed", "checks": []}))
            with self.assertRaises(ValueError):
                scale.check(root, ["ibex"], ["polars"], report)
            report.write_text(json.dumps({"status": "passed", "checks": [{"query_names": ["q", "lost"]}]}))
            with self.assertRaisesRegex(ValueError, "expected timed queries missing"):
                scale.check(root, ["ibex"], ["polars"], report)
            report.write_text(json.dumps({"status": "passed", "checks": [{"query_names": ["q"]}]}))
            (root / "polars_digest.tsv").write_text("query\trows\tnumeric_sum\nother\t1\t2\n")
            with self.assertRaisesRegex(ValueError, "missing from digest"):
                scale.check(root, ["ibex"], ["polars"], report)

    def test_fleet_uses_worker_sidecars_and_rejects_mismatch(self):
        import check_scale_fleet as fleet
        import package_scale_checks as package
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            archives = []
            for worker, variant in (("left", "ibex"), ("right", "polars")):
                base = root / worker
                directory = base / "scales/720"
                directory.mkdir(parents=True)
                (base / "checks_run.json").write_text(json.dumps({"sizes": [720], "variants": [variant]}))
                (directory / "answer_plan.json").write_text(json.dumps({"rows": 720, "reshape_rows": 720, "timeframe_rows": 720}))
                name = "python.tsv" if variant == "polars" else "ibex.tsv"
                (directory / name).write_text("framework\tquery\tavg_ms\trows\n" + variant + "\tq\t1\t1\n")
                (directory / (variant + "_digest.tsv")).write_text("query\trows\tnumeric_sum\nq\t1\t2\n")
                if variant == "ibex":
                    (base / "correctness").mkdir()
                    (base / "correctness/answers.json").write_text(json.dumps({"status": "passed", "checks": [{"query_names": ["q"]}]}))
                archive = root / (worker + ".tar.gz")
                package.package(base, archive)
                archives.append(archive)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(fleet.verify(*archives))
                (root / "right/scales/720/polars_digest.tsv").write_text("query\trows\tnumeric_sum\nq\t1\t99\n")
                package.package(root / "right", archives[1])
                self.assertTrue(fleet.verify(*archives))
            (root / "right/scales/720/answer_plan.json").write_text(json.dumps({"rows": 720, "reshape_rows": 0, "timeframe_rows": 720}))
            package.package(root / "right", archives[1])
            with self.assertRaisesRegex(ValueError, "plans differ"):
                fleet.verify(*archives)

    def test_failed_preflight_preserves_accepted_reports(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            uv = bin_dir / "uv"
            uv.write_text("#!/bin/sh\nexit 42\n")
            uv.chmod(0o755)
            for name in ("scales.tsv", "scales.csv"):
                (root / name).write_text("previous validated results\n")
            repository = pathlib.Path(__file__).resolve().parents[1]
            result = subprocess.run(["bash", str(repository / "benchmarking/run_scale_suite.sh"),
                                     "--sizes", "720"],
                                    env=os.environ | {"PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
                                                      "IBEX_SCALE_OUTPUT_DIR": str(root),
                                                      "BUILD_DIR": str(repository / "build-release")},
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 42, result.stdout + result.stderr)
            for name in ("scales.tsv", "scales.csv"):
                self.assertEqual((root / name).read_text(), "previous validated results\n")

    def test_duckdb_fleet_requires_checks_and_matching_worker_versions(self):
        import check_scale_fleet as fleet
        import package_scale_checks as package
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            archives = []
            origin = {"harness": {"bench_duckdb.py": "a" * 64},
                      "versions": {"polars": "2.0.0", "duckdb": "2.0.0"},
                      "build_type": "Release", "artifacts": {"tools/ibex_bench": "b" * 64}}
            for variant in ("ibex", "polars", "duckdb"):
                base = root / variant
                directory = base / "scales/720"
                directory.mkdir(parents=True)
                (base / "checks_run.json").write_text(json.dumps({"sizes": [720], "variants": [variant],
                                                                  "provenance_required": True}))
                (base / "runtime_provenance.json").write_text(json.dumps(origin))
                (directory / "answer_plan.json").write_text(json.dumps({"rows": 720, "reshape_rows": 720,
                                                                        "timeframe_rows": 720}))
                name = "python.tsv" if variant == "polars" else variant + ".tsv"
                (directory / name).write_text("framework\tquery\tavg_ms\trows\n" + variant + "\tq\t1\t1\n")
                (directory / (variant + "_digest.tsv")).write_text("query\trows\tnumeric_sum\nq\t1\t2\n")
                if variant == "ibex":
                    (base / "correctness").mkdir()
                    (base / "correctness/answers.json").write_text(json.dumps({"status": "passed", "checks": [
                        {"engine": "streaming", "query_names": ["q"]},
                        {"engine": "duckdb", "query_names": ["q"]}]}))
                archive = root / (variant + ".tar.gz")
                package.package(base, archive)
                archives.append(archive)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(fleet.verify(*archives))
                self.assertFalse(fleet.verify(archives[0], duckdb_archive=archives[2]))
                (root / "duckdb/scales/720/duckdb_digest.tsv").write_text("query\trows\tnumeric_sum\nq\t1\t99\n")
                package.package(root / "duckdb", archives[2])
                self.assertTrue(fleet.verify(*archives))
            origin["versions"]["duckdb"] = "1.5.4"
            (root / "duckdb/runtime_provenance.json").write_text(json.dumps(origin))
            package.package(root / "duckdb", archives[2])
            with self.assertRaisesRegex(ValueError, "package versions differ"):
                fleet.verify(*archives)


if __name__ == "__main__":
    unittest.main()
