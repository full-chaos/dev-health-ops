#!/usr/bin/env python3
"""Regenerate the cross-runtime oracle for the Go scheduled-reports producer.

WHY THIS FILE IS CHECKED IN
---------------------------
``internal/scheduler/fixed/testdata/python_scheduled_report_oracle.json`` pins the
Go producer against the REAL Python authorities. A static fixture with no
committed generator decays into a self-authored one the first time someone edits
it to make a test pass: a wrong Go derivation plus a matching fixture edit is
green in CI while live Python derives a different occurrence identity and executes
the same report twice. The generator makes regeneration reproducible and, more
importantly, makes it REVIEWABLE — a diff to the JSON must be accompanied by a
diff to the inputs here, or it is someone editing the answer key.

The values come from importing the production functions, never from
reimplementing them:
  * ``dev_health_ops.reports.execution_trigger.scheduled_report_occurrence_identity``
  * ``dev_health_ops.workers.task_utils.cron_next_run``

REGENERATE WITH
---------------
    PYTHONPATH=src .venv/bin/python scripts/generate_scheduled_report_oracle.py \
        internal/scheduler/fixed/testdata/python_scheduled_report_oracle.json

Then run the Go tests; they compare against this file and never against a second
call to the Go function under test.

Note the file is written by path rather than to stdout on purpose: importing
dev_health_ops initialises tracing, which prints to stdout and would corrupt a
redirected JSON stream.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from dev_health_ops.reports.execution_trigger import (
    SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION,
    scheduled_report_occurrence_identity,
)
from dev_health_ops.workers.task_utils import cron_next_run

# (report_id, scheduled_for) pairs the Go tests need identities for.
#
# The entries tagged INTEGRATION-FIXTURE are the exact values used by the Go
# integration tests' seeded report. They are here so those tests can look the
# identity up rather than recompute it with the function under test — which is
# what made two of them unable to fail.
OCCURRENCE_INPUTS = [
    # INTEGRATION-FIXTURE: testReportID / the 06:00 due time in
    # reports_integration_test.go and reports_replay_integration_test.go.
    ("8d4e2f10-5a61-4b7c-8d9e-1f2a3b4c5d6e", "2026-07-25T06:00:00+00:00"),
    # INTEGRATION-FIXTURE: the second day, for the after-completion case.
    ("8d4e2f10-5a61-4b7c-8d9e-1f2a3b4c5d6e", "2026-07-26T06:00:00+00:00"),
    # General coverage, including boundary and sub-second-free instants.
    ("00000000-0000-4000-8000-000000000001", "2026-07-25T01:05:00+00:00"),
    ("3f2504e0-4f89-41d3-9a0c-0305e82c3301", "2026-01-01T00:00:00+00:00"),
    ("ffffffff-ffff-4fff-bfff-ffffffffffff", "2026-12-31T23:59:00+00:00"),
    ("9b5e1a44-0c2f-4f6b-8d31-7ac0f5e29b10", "2026-03-08T07:30:00+00:00"),
]

# (expression, base, timezone) triples, covering UTC, offset zones, a DST
# spring-forward boundary, and a month rollover in a southern-hemisphere zone.
CRON_INPUTS = [
    ("*/5 * * * *", "2026-07-25T01:02:00+00:00", "UTC"),
    ("0 6 * * *", "2026-07-24T06:00:00+00:00", "UTC"),
    ("0 6 * * *", "2026-07-24T06:00:00+00:00", "America/New_York"),
    ("30 2 * * 1", "2026-07-20T02:30:00+00:00", "Europe/London"),
    ("0 1 * * *", "2026-03-07T01:00:00+00:00", "America/New_York"),
    ("0 0 1 * *", "2026-01-01T00:00:00+00:00", "Australia/Sydney"),
]


def main(destination: str) -> None:
    identities = []
    for report_id, instant in OCCURRENCE_INPUTS:
        when = datetime.fromisoformat(instant)
        identities.append(
            {
                "report_id": report_id,
                "scheduled_for": when.astimezone(timezone.utc).isoformat(),
                "occurrence_id": scheduled_report_occurrence_identity(report_id, when),
            }
        )

    crons = []
    for expression, base, tz in CRON_INPUTS:
        when = datetime.fromisoformat(base)
        crons.append(
            {
                "expression": expression,
                "base": when.astimezone(timezone.utc).isoformat(),
                "timezone": tz,
                "next": cron_next_run(expression, when, tz)
                .astimezone(timezone.utc)
                .isoformat(),
            }
        )

    with open(destination, "w") as handle:
        json.dump(
            {
                "_generated_by": "scripts/generate_scheduled_report_oracle.py",
                "_regenerate": (
                    "PYTHONPATH=src .venv/bin/python "
                    "scripts/generate_scheduled_report_oracle.py "
                    "internal/scheduler/fixed/testdata/"
                    "python_scheduled_report_oracle.json"
                ),
                "identity_version": SCHEDULED_REPORT_OCCURRENCE_IDENTITY_VERSION,
                "occurrence_identities": identities,
                "cron_occurrences": crons,
            },
            handle,
            indent=2,
        )
        handle.write("\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <destination.json>")
    main(sys.argv[1])
