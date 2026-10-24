# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare explicit lazy collections without changing upstream test bodies."""

from contextvars import ContextVar
from unittest.mock import patch

import polars as pl
from polars.testing import assert_frame_equal

from ibex_polars import IbexEngine


class Probe:
    def __init__(self, original, *, check_row_order=True):
        self.original = original
        self.check_row_order = check_row_order
        self.events = []
        self.active = ContextVar("ibex_conformance_active", default=False)

    def collect(self, frame, *args, **kwargs):
        flags = kwargs.get("optimizations")
        if self.active.get() or (flags is not None and flags._pyoptflags.eager):
            return self.original(frame, *args, **kwargs)
        token = self.active.set(True)
        try:
            return self._compare(frame, args, kwargs)
        finally:
            self.active.reset(token)

    def _compare(self, frame, args, kwargs):
        import ibex_pyarrow

        event = {"query": len(self.events) + 1, "status": "mismatch", "native_calls": 0}
        self.events.append(event)
        reference = reference_error = None
        try:
            reference = self.original(frame, *args, **kwargs)
        except Exception as error:
            reference_error = error

        # Different engine modes/background callbacks are outside this harness.
        # They are visible exclusions, never successful Ibex comparisons.
        if (args or kwargs.get("background") or kwargs.get("post_opt_callback") is not None
                or kwargs.get("engine", "auto") not in ("auto", "in-memory", "streaming")):
            event.update(status="excluded", reason="collection mode outside harness scope")
        else:
            candidate_error = None
            with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
                try:
                    candidate = self.original(frame, **(kwargs | {"engine": IbexEngine(cache_size=0)}))
                except Exception as error:
                    candidate_error = error
                event["native_calls"] = execute.call_count
            if isinstance(candidate_error, NotImplementedError) and not event["native_calls"]:
                event.update(status="unsupported", reason=str(candidate_error))
            elif reference_error is not None:
                if type(candidate_error) is type(reference_error):
                    event.update(status="pass", result="matching_error", error_type=type(candidate_error).__name__)
                    # Let upstream pytest.raises verify the candidate's message.
                    raise candidate_error
                event["reason"] = f"reference raised {type(reference_error).__name__}; Ibex: {candidate_error!r}"
            elif candidate_error is not None:
                event["reason"] = f"Ibex raised {type(candidate_error).__name__}: {candidate_error}"
            elif not event["native_calls"]:
                event["reason"] = "Ibex returned a result without native execution"
            else:
                try:
                    assert_frame_equal(candidate, reference, check_row_order=self.check_row_order,
                                       rel_tol=1e-12, abs_tol=1e-9)
                except AssertionError as error:
                    event["reason"] = str(error)
                else:
                    event.update(status="pass", result="matching_frame")
                # Upstream assertions also inspect actual accepted Ibex results.
                return candidate

        # Continue the upstream test to discover later queries after a rejection
        # or execution error. These events NEVER count as Ibex passes.
        if reference_error is not None:
            raise reference_error
        return reference
