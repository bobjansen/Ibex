# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Bookkeeping must never turn unsupported or broken queries into passes."""

import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

import polars as pl

from conformance.plugin import classify
from conformance.probe import Probe
from conformance.run import support_regressions, upstream_digest


class ProbeTests(unittest.TestCase):
    def compare(self, *, reference=None, candidate=None, error=None, calls=1, reference_error=None, check_row_order=True, allowed_rows=None):
        import ibex_pyarrow
        reference = pl.DataFrame({"x": [1]}) if reference is None else reference
        candidate = reference if candidate is None else candidate
        engine = object()

        def collect(frame, **kwargs):
            if kwargs.get("engine") is engine:
                for _ in range(calls):
                    ibex_pyarrow.execute_pipeline("not executed by this bookkeeping test")
                if error is not None:
                    raise error
                return candidate
            if reference_error is not None:
                raise reference_error
            return reference

        probe = Probe(collect, check_row_order=check_row_order, allowed_rows=allowed_rows)
        with patch("conformance.probe.IbexEngine", return_value=engine), \
                patch.object(ibex_pyarrow, "execute_pipeline", return_value=None):
            try:
                result = probe.collect(None)
            except Exception as exception:
                result = exception
        return probe.events[0], result

    def test_matching_result_requires_native_execution(self):
        event, _ = self.compare()
        self.assertEqual(event["status"], "pass")
        event, _ = self.compare(calls=0)
        self.assertEqual(event["status"], "mismatch")

    def test_unsupported_only_before_execution(self):
        event, _ = self.compare(error=NotImplementedError("join"), calls=0)
        self.assertEqual(event["status"], "unsupported")
        event, _ = self.compare(error=NotImplementedError("late failure"))
        self.assertEqual(event["status"], "mismatch")

    def test_different_values_and_types_are_mismatches(self):
        for candidate in (pl.DataFrame({"x": [2]}), pl.DataFrame({"x": [1.0]})):
            with self.subTest(candidate=candidate):
                event, actual = self.compare(candidate=candidate)
                self.assertEqual(event["status"], "mismatch")
                self.assertIs(actual, candidate)

    def test_order_is_checked(self):
        event, _ = self.compare(reference=pl.DataFrame({"x": [1, 2]}),
                                candidate=pl.DataFrame({"x": [2, 1]}))
        self.assertEqual(event["status"], "mismatch")

    def test_explicit_unordered_contract_still_checks_values(self):
        for values, status in (([2, 1], "pass"), ([3, 1], "mismatch")):
            event, _ = self.compare(reference=pl.DataFrame({"x": [1, 2]}),
                                    candidate=pl.DataFrame({"x": values}), check_row_order=False)
            self.assertEqual(event["status"], status)

    def test_pinned_unordered_selection_checks_values_types_size_and_uniqueness(self):
        reference = pl.DataFrame({"x": [1, 2]})
        for candidate, status in ((pl.DataFrame({"x": [2, 3]}), "pass"),
                                  (pl.DataFrame({"x": [3, 2]}), "pass"),
                                  (pl.DataFrame({"x": [3, 4]}), "mismatch"),
                                  (pl.DataFrame({"x": [2, 2]}), "mismatch"),
                                  (pl.DataFrame({"x": [3]}), "mismatch"),
                                  (pl.DataFrame({"x": [2.0, 3.0]}), "mismatch")):
            with self.subTest(candidate=candidate):
                event, _ = self.compare(reference=reference, candidate=candidate, allowed_rows=[[1], [2], [3]])
                self.assertEqual(event["status"], status)

    def test_native_failure_is_not_unsupported(self):
        event, _ = self.compare(error=RuntimeError("failed"))
        self.assertEqual(event["status"], "mismatch")

    def test_matching_error_is_raised_for_upstream_message_assertions(self):
        candidate_error = ValueError("candidate message")
        event, result = self.compare(reference_error=ValueError("reference message"),
                                     error=candidate_error, calls=0)
        self.assertEqual(event["status"], "pass")
        self.assertIs(result, candidate_error)

    def test_different_error_type_is_a_mismatch(self):
        event, _ = self.compare(reference_error=ValueError("reference"), error=RuntimeError("candidate"))
        self.assertEqual(event["status"], "mismatch")

    def test_candidate_success_when_reference_errors_is_a_mismatch(self):
        event, _ = self.compare(reference_error=ValueError("reference"))
        self.assertEqual(event["status"], "mismatch")

    def test_eager_fixture_operations_do_not_count(self):
        flags = pl.QueryOptFlags()
        flags._pyoptflags.eager = True
        probe = Probe(lambda *args, **kwargs: "fixture")
        self.assertEqual(probe.collect(None, optimizations=flags), "fixture")
        self.assertEqual(probe.events, [])

    def test_special_engine_modes_are_excluded(self):
        probe = Probe(lambda *args, **kwargs: "reference")
        self.assertEqual(probe.collect(None, engine="gpu"), "reference")
        self.assertEqual(probe.events[0]["status"], "excluded")

    def test_test_status_preserves_partial_coverage_and_caught_mismatches(self):
        for events, expected in [([], "unexercised"), (["pass"], "pass"),
                                 (["pass", "unsupported"], "unsupported"),
                                 (["unsupported", "mismatch"], "mismatch"),
                                 (["excluded"], "excluded")]:
            with self.subTest(events=events):
                self.assertEqual(classify({"pytest": "passed", "queries": [{"status": status} for status in events]}), expected)
        self.assertEqual(classify({"pytest": "failed", "queries": [{"status": "unsupported"}]}), "mismatch")


class PinTests(unittest.TestCase):
    def test_previously_passing_queries_cannot_silently_become_unsupported(self):
        tests = {"a": {"queries": [{"query": 1, "status": "unsupported"}, {"query": 2, "status": "pass"}]}}
        required = {"a": [1, 2], "b": [1]}
        self.assertEqual(len(support_regressions(tests, required, full_selection=True)), 2)
        self.assertEqual(len(support_regressions(tests, required, full_selection=False)), 1)

    def test_test_tree_content_changes_invalidate_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tests = root / "py-polars/tests"
            tests.mkdir(parents=True)
            (root / "py-polars/pyproject.toml").write_text("[project]\n")
            test = tests / "test_example.py"
            test.write_text("original\n")
            original = upstream_digest(root)
            test.write_text("changed\n")
            self.assertNotEqual(upstream_digest(root), original)
            test.write_text("original\n")
            self.assertEqual(upstream_digest(root), original)


if __name__ == "__main__":
    unittest.main()
