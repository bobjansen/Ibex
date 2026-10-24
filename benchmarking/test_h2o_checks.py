# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Adversarial H2O gate tests; no builds, engine queries or benchmark data."""
import contextlib
import copy
import csv
import hashlib
import io
import json
import pathlib
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from h2o import answers, publication, provenance, run


class H2OChecks(unittest.TestCase):
    def test_polars_all_null_sum_is_null_instead_of_zero(self):
        import polars as pl
        expression = run.groupby_questions(True)[0][4]
        frame = pl.DataFrame({"id1": ["empty", "mixed", "mixed"], "v1": [None, None, 3]},
                             schema={"id1": pl.String, "v1": pl.Int32})
        result = expression(frame.lazy()).collect(engine="streaming")
        self.assertEqual(dict(result.iter_rows()), {"empty": None, "mixed": 3})

    def test_batching_and_row_order_do_not_change_fingerprint(self):
        import polars as pl
        frame = pl.DataFrame({"id3": ['α,"quoted"', "", None, "duplicate", "duplicate"],
                              "v1": [2**53 + 1, 1, 2, 3, 3], "v3": [1., None, float("nan"), 4., 5.]})
        def check(frames):
            return answers.fingerprint_frames("groupby", "q3", "polars", frame.columns, frames, True)
        expected = check([frame])
        self.assertFalse(answers.compare(expected, check(frame.iter_slices(2)), True))
        self.assertFalse(answers.compare(expected, check([frame.reverse()]), True))

    def test_stale_binary_source_or_debug_build_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "CMakeLists.txt").write_text("project(test)\n")
            build = root / "build-release"
            (build / "tools").mkdir(parents=True)
            cache = build / "CMakeCache.txt"
            cache.write_text(f"CMAKE_BUILD_TYPE:STRING=Release\nCMAKE_HOME_DIRECTORY:INTERNAL={root}\n")
            for name in ("ibex", "ibex_bench", "csv.so", "csv.ibex"):
                (build / "tools" / name).write_text("original")
            record = dict(sources=provenance.sources(root), artifacts=provenance.artifacts(build))
            (build / "h2o-build.json").write_text(json.dumps(record))
            self.assertEqual(provenance.verify(build, root), record)
            (root / "CMakeLists.txt").write_text("project(changed)\n")
            with self.assertRaises(ValueError):
                provenance.verify(build, root)
            (root / "CMakeLists.txt").write_text("project(test)\n")
            (build / "tools/ibex").write_text("replaced")
            with self.assertRaises(ValueError):
                provenance.verify(build, root)
            cache.write_text(f"CMAKE_BUILD_TYPE:STRING=Debug\nCMAKE_HOME_DIRECTORY:INTERNAL={root}\n")
            with self.assertRaises(ValueError):
                provenance.verify(build, root)

    def answer(self, rows, q="q1", task="groupby", full=True):
        schema = answers.contract(task, q)
        return answers.fingerprint(task, q, "duckdb" if task == "groupby" else "polars",
                                   list(schema), rows, full)

    def test_exact_integers_keys_duplicates_and_associations(self):
        base = self.answer([["A", 30_000_000], ["B", 2]])
        self.assertEqual(answers.compare(base, self.answer([["B", 2], ["A", 30_000_000]]), True), "")
        for rows in ([["A", 30_000_001], ["B", 2]], [["X", 30_000_000], ["B", 2]],
                     [["A", 2], ["B", 30_000_000]], [["A", 30_000_000], ["A", 30_000_000]]):
            self.assertTrue(answers.compare(base, self.answer(rows)))
        big = self.answer([["A", 2**53]])
        self.assertTrue(answers.compare(big, self.answer([["A", 2**53 + 1]])))

    def test_float_moments_are_bound_to_keys_and_columns(self):
        rows = [["A", 1, 2.], ["B", 1, 9.]]
        left = self.answer(rows, "q3")
        self.assertTrue(answers.compare(left, self.answer([["A", 1, 9.], ["B", 1, 2.]], "q3")))
        self.assertFalse(answers.compare(left, self.answer([["A", 1, 2.00000001], ["B", 1, 9.]], "q3")))
        # All exact fields are identical here: independent column sums alone
        # cannot see a broken v1/v2 association, but cross moments can.
        schema = answers.contract("join", "q1")
        row = {name: "S" if kind == "s" else 1 if kind == "i" else 0.
               for name, kind in schema.items()}
        def joined(pairs):
            data = [[(dict(row, v1=a, v2=b))[name] for name in schema] for a, b in pairs]
            return answers.fingerprint("join", "q1", "ibex", answers.ibex_columns("join", "q1"), data)
        self.assertTrue(answers.compare(joined([(1., 10.), (2., 20.)]), joined([(1., 20.), (2., 10.)])))

    def test_null_nan_empty_and_full_rows(self):
        self.assertTrue(answers.compare(self.answer([[None, 1]]), self.answer([["", 1]])))
        a = self.answer([["A", 1, None]], "q3")
        self.assertTrue(answers.compare(a, self.answer([["A", 1, float("nan")]], "q3")))
        self.assertTrue(answers.compare(self.answer([["A", 1, float("inf")]], "q3"),
                                        self.answer([["A", 1, float("-inf")]], "q3")))
        base = self.answer([["A", 1], ["B", 2]])
        altered = copy.deepcopy(base)
        altered["full"] = [["A", 2], ["B", 1]]
        self.assertTrue(answers.compare(base, altered, require_full=True))
        altered["full"] = None
        self.assertTrue(answers.compare(base, altered, require_full=True))
        altered["full"] = [["A"], ["B"]]
        self.assertTrue(answers.compare(base, altered, require_full=True))
        altered = copy.deepcopy(base)
        del altered["counts"]["v1"]
        self.assertTrue(answers.compare(altered, altered))

    def test_pipe_capture_preserves_null_empty_and_quoted_unicode(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "answer.pipe"
            with answers.PipeCapture(path, "groupby", "q1", True) as capture:
                with path.open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["id1", "v1", "__check_null_0", "__check_null_1"])
                    writer.writerows([["", "1", "0", "0"], ["", "2", "1", "0"],
                                      ['α,"line\nbreak', str(2**53 + 1), "0", "0"]])
            self.assertIsNone(capture.error)
            self.assertEqual(capture.answer["full"], [["", 1], [None, 2], ['α,"line\nbreak', 2**53 + 1]])
            path = pathlib.Path(tmp) / "failed.pipe"
            with answers.PipeCapture(path, "groupby", "q1", True) as capture:
                pass  # REPL failed before opening csv::write: must finish at EOF.
            self.assertTrue(capture.error)

    def result(self, engine="ibex", q="q1"):
        return run.ok(engine, q, "test", [1.], 0, [0.], self.answer([], q, full=False))

    def test_failure_or_missing_engine_never_marks_survivors_ok(self):
        rows = [self.result(e) for e in answers.ENGINES]
        self.assertEqual(run.verify(rows), "ok")
        self.assertNotEqual(run.verify(rows[:2]), "ok")
        self.assertNotEqual(run.verify(rows + [rows[0]]), "ok")
        rows[-1] = run.failed("duckdb", "q1", "test", "timeout")
        self.assertNotEqual(run.verify(rows), "ok")
        with self.assertRaises(ValueError):
            run.validate_coverage(rows, ["q1"], answers.ENGINES)
        with contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertFalse(run.report("test", {"q1": rows}, list(answers.ENGINES)))
        self.assertNotIn("geomean", out.getvalue())

    def artifact(self):
        data = "G1_1e7_1e2_0_0"
        engines = list(answers.ENGINES)
        rows = [self.result(e, f"q{i}") for i in range(1, 11) for e in engines]
        checks = []
        for small, task in (("G1_768_2_0_0", "groupby"), ("G1_768_2_5_1", "groupby"),
                            ("J1_1e7_NA_0_0", "join"), ("J1_1e7_NA_5_1", "join")):
            for cores in (1, 2):
                for i in range(1, 11 if task == "groupby" else 6):
                    checks.append(dict(data=small, task=task, question=f"q{i}", cores=cores,
                                       engines=engines, status="ok"))
        manifest = dict(version=answers.CHECK_VERSION, status="passed", run_id="test-id",
                        data=[data], engines=engines, threads=8,
                        questions={data: [f"q{i}" for i in range(1, 11)]},
                        preflight=dict(status="passed", checks=checks),
                        results=[dict(data=data, task="groupby", rows=rows)])
        manifest.update(config=answers.RUN_CONFIG, input_null_token="", harness={"run.py": "a" * 64},
                        provenance={"build_type": "Release", "sources": {"src/example.cpp": "b" * 64},
                                    "artifacts": dict.fromkeys(("tools/ibex", "tools/csv.so", "tools/csv.ibex",
                                                                "CMakeCache.txt"), "c" * 64)},
                        versions={"ibex": "sha256:" + "c" * 64, "polars": "2.0.0", "duckdb": "2.0.0"})
        handle = io.StringIO(newline="")
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(["data", "question", "engine", "status", "validation", "run_id", "threads",
                         "out_rows", "iters", "min_ms", "version"])
        for row in rows:
            writer.writerow([data, row["question"], row["engine"], "ok", answers.CHECK_VERSION,
                             "test-id", 8, 0, 1, "1.000", manifest["versions"][row["engine"]]])
        text = handle.getvalue()
        manifest["tsv_sha256"] = hashlib.sha256(text.encode()).hexdigest()
        return text, manifest

    def test_publication_requires_complete_consistent_validated_run(self):
        text, manifest = self.artifact()
        self.assertEqual(len(publication.validate(text, manifest)), 30)
        for mutation in (lambda m: m.update(status="failed"),
                         lambda m: m["results"][0]["rows"].pop(),
                         lambda m: m["results"][0]["rows"].append(m["results"][0]["rows"][0]),
                         lambda m: m["results"][0]["rows"][0].update(error="timeout"),
                         lambda m: m["preflight"]["checks"].pop(),
                         lambda m: m["questions"][m["data"][0]].pop(),
                         lambda m: m.update(engines=["ibex", "polars"]),
                         lambda m: m["provenance"].update(build_type="Debug"),
                         lambda m: m["provenance"]["artifacts"].update({"tools/ibex": "d" * 64}),
                         lambda m: m.update(config={}),
                         lambda m: m.update(run_id="other-run")):
            bad = copy.deepcopy(manifest)
            mutation(bad)
            with self.assertRaises(ValueError):
                publication.validate(text, bad)
        with self.assertRaises(ValueError):
            publication.validate(text.replace("1.000", "0.001"), manifest)

    def test_archive_requires_requested_cores_and_zero_exit(self):
        text, manifest = self.artifact()
        files = {"h2o_8c.tsv": text, "h2o_8c.tsv.checks.json": json.dumps(manifest),
                 "run.json": json.dumps({"data": manifest["data"], "cores": [8]}),
                 "exit_code.txt": "exit_code=0\n"}
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "artifact.tar.gz"
            def archive():
                with tarfile.open(path, "w:gz") as tar:
                    for name, content in files.items():
                        raw = content.encode()
                        info = tarfile.TarInfo("results/" + name)
                        info.size = len(raw)
                        tar.addfile(info, io.BytesIO(raw))
            archive()
            self.assertEqual(len(publication.archive_inputs(path)), 1)
            files["run.json"] = json.dumps({"data": manifest["data"], "cores": [1, 8]})
            archive()
            with self.assertRaises(ValueError):
                publication.archive_inputs(path)
            files["exit_code.txt"] = "exit_code=1\n"
            archive()
            with self.assertRaises(ValueError):
                publication.archive_inputs(path)

    def test_failed_preflight_preserves_previous_accepted_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "accepted.tsv"
            sidecar = out.with_name(out.name + ".checks.json")
            out.write_text("previous accepted results\n")
            sidecar.write_text("previous checks\n")
            argv = ["run.py", "G1_1e7_1e2_0_0", "--engines", "polars", "duckdb", "--out", str(out)]
            with (patch.object(run.sys, "argv", argv), patch.object(run, "tool_versions", return_value={}),
                  patch.object(run, "questions_for", return_value=[("q1",)]),
                  patch.object(run.preflight, "run", side_effect=ValueError("bad full answer")),
                  contextlib.redirect_stderr(io.StringIO())):
                self.assertEqual(run.main(), 1)
            self.assertEqual(out.read_text(), "previous accepted results\n")
            self.assertEqual(sidecar.read_text(), "previous checks\n")


if __name__ == "__main__":
    unittest.main()
