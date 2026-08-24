#!/usr/bin/env python3
"""Live-Python oracle for finalize_sync_run's zero-unit classification.

Emits what the REAL production functions
``dev_health_ops.workers.sync_units._aggregate_run_status`` and
``._zero_unit_reason`` return for a fixed table of inputs, as stable JSON on
stdout. The Go side (native_finalize_sync_run_oracle_test.go) executes this
script and diffs its own aggregateRunStatus/zeroUnitReasonFrom against it --
per AGENTS.md's live-python-oracle mandate, this is a DIFFERENTIAL check
against the actual producer, not a hand-authored fixture that could drift
from what sync_units.py really does.

Importing dev_health_ops.workers.sync_units has real side effects on stdout
(Sentry/OTel/Celery instrumentation init, a git subprocess probe) because it
transitively imports celery_app at module scope -- unlike
providersync/testdata/python_registry_oracle.py's target (datasets.py),
there is no lightweight isolated-file import available here since
_aggregate_run_status/_zero_unit_reason live in the same module as that
heavy top-level import graph. So the import happens with stdout redirected
to /dev/null, and only the final JSON is written to the real stdout
afterwards.
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
from typing import Any


def main() -> int:
    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull):
            from dev_health_ops.workers.sync_units import (
                _aggregate_run_status,
                _zero_unit_reason,
            )

    aggregate_cases = [
        {"total": 0, "success": 0, "failed": 0},
        {"total": 3, "success": 3, "failed": 0},
        {"total": 3, "success": 0, "failed": 3},
        {"total": 3, "success": 1, "failed": 2},
        {"total": 1, "success": 1, "failed": 0},
        {"total": 1, "success": 0, "failed": 1},
        # total=0 dominates regardless of success/failed being non-zero --
        # a shape that should never occur in practice, but the oracle must
        # cover what the real function actually returns for it, not what
        # "should" happen.
        {"total": 0, "success": 1, "failed": 0},
    ]
    aggregate_results = [
        {
            **case,
            "status": _aggregate_run_status(
                case["total"], case["success"], case["failed"]
            ),
        }
        for case in aggregate_cases
    ]

    reason_cases: list[dict[str, Any]] = [
        {"planner_result": {"reason": "pagerduty_credential_unavailable"}},
        {"planner_result": {"error_category": "feature_disabled"}},
        {
            "planner_result": {
                "reason": "pagerduty_credential_unavailable",
                "error_category": "feature_disabled",
            }
        },
        {"planner_result": {}},
        {"planner_result": {"reason": ""}},
        {"planner_result": {"reason": "   "}},
        {"planner_result": {"reason": 123}},
        {"planner_result": {"error_category": None}},
    ]
    reason_results = [
        {**case, "reason_out": _zero_unit_reason(case["planner_result"])}
        for case in reason_cases
    ]

    json.dump(
        {"aggregate": aggregate_results, "reason": reason_results},
        sys.stdout,
        sort_keys=True,
        separators=(",", ":"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
