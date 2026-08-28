"""Wiring-level test for job_daily._write_team_complexity_for_day
(CHAOS-4365 item 3 / 4347-C).

Mirrors tests/metrics/test_job_daily_team_cognitive_load_wiring.py's shape:
the pure aggregator (``build_team_complexity_rows_for_day``,
tests/metrics/test_team_complexity.py) already correctly sums a team's
complexity across every repo it owns -- WHEN given all of those repos' rows
in one call. This test locks in the finalize-step contract at the call-site
level: a single call to ``_write_team_complexity_for_day``, reading back
BOTH of a team's repos' ``repo_complexity_daily`` rows via the (faked)
``query_dicts`` readback, produces exactly one row with
``contributing_repo_count == 2`` and summed totals -- never written per-repo
inside the daily partition loop (the CHAOS-4399 bug class).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from dev_health_ops.metrics import job_daily

DAY = date(2026, 5, 20)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


class _Sink:
    def __init__(self, repo_complexity_rows: list[dict[str, Any]]) -> None:
        self.written: list[Any] = []
        self._repo_complexity_rows = repo_complexity_rows

    def query_dicts(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        assert "repo_complexity_daily" in query
        return self._repo_complexity_rows

    def write_team_complexity_daily(self, rows: list[Any]) -> None:
        self.written.extend(rows)


def test_finalize_call_sums_a_two_repo_team_in_one_call(monkeypatch: Any) -> None:
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    sink = _Sink(
        repo_complexity_rows=[
            {
                "repo_id": repo_a,
                "loc_total": 1000,
                "cyclomatic_total": 50,
                "high_complexity_functions": 2,
                "very_high_complexity_functions": 0,
            },
            {
                "repo_id": repo_b,
                "loc_total": 9000,
                "cyclomatic_total": 90,
                "high_complexity_functions": 1,
                "very_high_complexity_functions": 1,
            },
        ]
    )

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

    written_count = job_daily._write_team_complexity_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        computed_at=NOW,
        # Both repos must be in the current catalog (same guard
        # _repo_to_team_map_for_compounding_risk already enforces).
        repo_names_by_id={repo_a: "acme/a", repo_b: "acme/b"},
        repo_team_resolver=None,
    )

    assert written_count == 1
    row = sink.written[0]
    assert row.team_id == "team-platform"
    assert row.contributing_repo_count == 2
    assert row.loc_total == 10000
    assert row.cyclomatic_total == 140
    assert row.cyclomatic_per_kloc == 14.0


def test_no_repo_complexity_rows_this_day_writes_nothing(monkeypatch: Any) -> None:
    """A day with no repo_complexity_daily rows yet (the complexity scan
    job runs on its own cadence) must degrade to zero rows, never raise.
    """
    sink = _Sink(repo_complexity_rows=[])

    written_count = job_daily._write_team_complexity_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        computed_at=NOW,
        repo_names_by_id={},
        repo_team_resolver=None,
    )

    assert written_count == 0
    assert sink.written == []


def test_stale_ownership_for_a_repo_outside_the_catalog_is_rejected(
    monkeypatch: Any,
) -> None:
    """team_repo_ownership rows never expire on their own -- a repo removed
    or renamed since auto-import last ran can still carry a stale, unexpired
    ownership row. Without the current-catalog guard
    (_repo_to_team_map_for_compounding_risk, reused here) that row would
    keep attributing complexity to a team for a repo no longer in the org's
    inventory.
    """
    orphan_repo_id = uuid.uuid4()
    sink = _Sink(
        repo_complexity_rows=[
            {
                "repo_id": orphan_repo_id,
                "loc_total": 5000,
                "cyclomatic_total": 500,
                "high_complexity_functions": 9,
                "very_high_complexity_functions": 3,
            },
        ]
    )

    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {str(orphan_repo_id): "gh:stale-owner"},
    )

    written_count = job_daily._write_team_complexity_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        computed_at=NOW,
        # orphan_repo_id is NOT in the current catalog.
        repo_names_by_id={},
        repo_team_resolver=None,
    )

    assert written_count == 0
    assert sink.written == []
