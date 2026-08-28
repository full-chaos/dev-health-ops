"""Wiring-level test for job_daily._write_team_cognitive_load_for_day
(CHAOS-4365 finalize-step fix).

The pure aggregator (``build_team_cognitive_load_rows_for_day``,
tests/metrics/test_team_cognitive_load.py) already correctly sums a team's
signals across every repo it owns -- WHEN given all of those repos' rows in
one call. The bug lived entirely in the caller: ``run_daily_metrics_job``
processes one repo per call (CHAOS-4264), so calling
``_write_team_cognitive_load_for_day`` from there fed it only one repo's
rows at a time, and each call's "complete" team row then collided with the
next repo's own write under argMax(computed_at) dedup -- silently dropping
every owned repo but the last one written (confirmed live: contributing_
repo_count stuck at 1 for a 2-repo team, see CHAOS-4365 sub-issue).

This test locks in the finalize-step contract directly: a single call to
_write_team_cognitive_load_for_day with BOTH of a team's repos' rows
present produces exactly one row with contributing_repo_count == 2 and
summed counters -- the shape run_daily_metrics_finalize's whole-org CH
readback now guarantees, and the per-repo path no longer does (it stopped
calling this function entirely -- see run_daily_metrics_job's per-repo
write block, which now only calls _write_compounding_risk_for_day).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from dev_health_ops.metrics import job_daily

DAY = date(2026, 5, 20)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


@dataclass
class _UserRow:
    repo_id: uuid.UUID
    author_email: str
    pr_interruption_load: int = 0
    context_spread_count: int = 0
    review_request_load: int = 0


class _Sink:
    def __init__(self) -> None:
        self.written: list[Any] = []

    def write_team_cognitive_load_daily(self, rows: list[Any]) -> None:
        self.written.extend(rows)


def test_finalize_call_sums_a_two_repo_team_in_one_call(monkeypatch: Any) -> None:
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    user_rows = [
        _UserRow(repo_id=repo_a, author_email="a@example.com", pr_interruption_load=2),
        _UserRow(repo_id=repo_b, author_email="b@example.com", pr_interruption_load=5),
    ]
    sink = _Sink()

    # Both repos resolve to the SAME team via team_repo_ownership (CHAOS-4321
    # source) -- mirrors run_daily_metrics_finalize loading this map once,
    # org-wide, for the whole day, not per repo.
    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {
            str(repo_a): "team-platform",
            str(repo_b): "team-platform",
        },
    )

    written_count = job_daily._write_team_cognitive_load_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        user_metrics_rows=user_rows,
        team_wellbeing_rows=[],
        computed_at=NOW,
        repo_names_by_id={},
        repo_team_resolver=None,
    )

    assert written_count == 1
    row = sink.written[0]
    assert row.team_id == "team-platform"
    assert row.contributing_repo_count == 2
    assert row.pr_interruption_load == 7.0  # 2 + 5, both repos in ONE call
