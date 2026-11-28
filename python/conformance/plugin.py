# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Pytest integration; baseline and candidate run in separate processes."""

from collections import Counter
import json
import os
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from .probe import Probe


class Plugin:
    def __init__(self, mode, output, baseline=None, unordered_tests=None, selection_tests=None, random_rank_tests=()):
        self.mode = mode
        self.output = Path(output)
        self.baseline = baseline or {}
        self.unordered_tests = unordered_tests or {}
        self.selection_tests = selection_tests or {}
        self.random_rank_tests = set(random_rank_tests)
        self.tests = {}
        self.collection_errors = []

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item):
        row = self.tests.setdefault(item.nodeid, {"queries": []})
        if self.mode == "reference":
            yield
        else:
            selection = self.selection_tests.get(item.nodeid)
            # An entry names one node, or a test function and so every one of
            # its parametrizations.
            unordered = self.unordered_tests.get(item.nodeid,
                                                 self.unordered_tests.get(item.nodeid.split("[")[0]))
            random_rank = item.nodeid in self.random_rank_tests
            if random_rank:
                row["comparison_policy"] = {"random_rank": "unseeded ties within exact min/max bounds"}
            probe = Probe(pl.LazyFrame.collect, check_row_order=unordered is None,
                          allowed_rows=selection["allowed_rows"] if selection else None, random_rank=random_rank)
            if selection:
                row["comparison_policy"] = {"unordered_selection": selection}
            if unordered is not None:
                row["comparison_policy"] = {"check_row_order": False, "reason": unordered}
            def collect(frame, *args, **kwargs):
                return probe.collect(frame, *args, **kwargs)
            with patch.object(pl.LazyFrame, "collect", collect):
                yield
            row["queries"] = probe.events

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_setup(self, item):
        if self.mode == "candidate" and self.baseline.get(item.nodeid, {}).get("pytest") != "passed":
            row = self.tests.setdefault(item.nodeid, {"queries": []})
            row["baseline_failure"] = self.baseline.get(item.nodeid, {}).get("pytest") != "skipped"
            pytest.skip("upstream baseline did not pass")

    def pytest_runtest_logreport(self, report):
        row = self.tests.setdefault(report.nodeid, {"queries": []})
        if report.when == "call" or report.failed or (report.when == "setup" and report.skipped):
            row["pytest"] = report.outcome
            if not report.passed:
                row["detail"] = str(report.longrepr)

    def pytest_collectreport(self, report):
        if report.failed:
            self.collection_errors.append(str(report.longrepr))

    def pytest_sessionfinish(self, session, exitstatus):
        for row in self.tests.values():
            row["status"] = classify(row, self.mode)
        counts = dict(Counter(row["status"] for row in self.tests.values()))
        queries = dict(Counter(event["status"] for row in self.tests.values() for event in row["queries"]))
        self.output.write_text(json.dumps({"mode": self.mode, "pytest_exit": int(exitstatus),
                                          "tests": self.tests, "summary": counts,
                                          "queries": queries, "collection_errors": self.collection_errors}, indent=2) + "\n")


def classify(row, mode="candidate"):
    if mode == "reference":
        return "baseline_pass" if row.get("pytest") == "passed" else "baseline_failure"
    if row.get("baseline_failure"):
        return "baseline_failure"
    statuses = {event["status"] for event in row["queries"]}
    if "mismatch" in statuses or row.get("pytest") == "failed":
        return "mismatch"
    if row.get("pytest") != "passed":
        return "skipped"
    if "unsupported" in statuses:
        return "unsupported"
    if "excluded" in statuses:
        return "excluded"
    return "pass" if "pass" in statuses else "unexercised"


def pytest_configure(config):
    pin = json.loads(Path(__file__).with_name("upstream.json").read_text())
    config.pluginmanager.register(Plugin(os.environ["IBEX_CONFORMANCE_MODE"],
                                        os.environ["IBEX_CONFORMANCE_OUTPUT"],
                                        json.loads(Path(os.environ["IBEX_CONFORMANCE_BASELINE"]).read_text())["tests"]
                                        if "IBEX_CONFORMANCE_BASELINE" in os.environ else None,
                                        pin.get("unordered_tests"),
                                        pin.get("unordered_selection_tests"), pin.get("random_rank_tests", ())),
                                  "ibex-conformance-recorder")
