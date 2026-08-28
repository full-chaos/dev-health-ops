from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import pytest

from dev_health_ops.metrics import job_daily

DAY = date(2026, 5, 20)
NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


@dataclass
class _RepoMetricsRow:
    repo_id: uuid.UUID
    rework_churn_ratio_30d: float = 0.15
    single_owner_file_ratio_30d: float = 0.5
    code_ownership_gini: float = 0.4
    bus_factor: int = 2
    pr_first_review_p90_hours: float = 24.0


class _Sink:
    def __init__(self) -> None:
        self.written: list[Any] = []

    def query_dicts(
        self, query: str, parameters: dict[str, Any]
    ) -> list[dict[str, float]]:
        return [{"first_half": 100.0, "second_half": 130.0}]

    def write_compounding_risk_daily(self, rows: list[Any]) -> None:
        self.written.extend(rows)


class _Resolver:
    def resolve(self, full_name: str) -> tuple[str | None, str | None]:
        return (
            ("team-platform", "Platform")
            if full_name == "acme/backend"
            else (None, None)
        )


def test_daily_job_writes_compounding_risk_from_persisted_repo_metrics(
    monkeypatch: Any,
) -> None:
    repo_id = uuid.uuid4()
    persisted_repo_metrics = [_RepoMetricsRow(repo_id=repo_id)]
    sink = _Sink()

    def fake_fetch_repo_metrics_for_day(
        primary_sink: Any, org_id: str, day: date
    ) -> list[_RepoMetricsRow]:
        assert primary_sink is sink
        assert org_id == "acme"
        assert day == DAY
        return persisted_repo_metrics

    monkeypatch.setattr(
        job_daily, "_fetch_repo_metrics_for_day", fake_fetch_repo_metrics_for_day
    )

    written_count = job_daily._write_compounding_risk_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=[],
        computed_at=NOW,
        repo_names_by_id={repo_id: "acme/backend"},
        repo_team_resolver=_Resolver(),
    )

    # CHAOS-4365 finalize-step fix: this function is called once per repo
    # (CHAOS-4264), so it now writes REPO-scope rows only -- team-scope rows
    # move to _write_compounding_risk_team_rows_for_day, called once per
    # org/day from run_daily_metrics_finalize. See that function's tests
    # below for the team-row assertions this test used to make.
    assert written_count == 1
    assert {row.scope for row in sink.written} == {"repo"}
    repo_row = sink.written[0]
    assert repo_row.scope_id == str(repo_id)
    assert repo_row.org_id == "acme"
    assert repo_row.compounding_risk is not None
    assert repo_row.complexity_delta == 0.30


def test_finalize_resolves_team_via_ownership_map_when_pattern_resolver_misses(
    monkeypatch: Any,
) -> None:
    """CHAOS-4365: a real org's native-imported teams carry no
    ``repo_patterns`` (CHAOS-4321 bars membership-based inference), so
    ``repo_team_resolver`` alone never resolves such a repo. The
    ``team_repo_ownership``-sourced map must still produce a team-scope row.
    """
    repo_id = uuid.uuid4()
    persisted_repo_metrics = [_RepoMetricsRow(repo_id=repo_id)]
    sink = _Sink()

    monkeypatch.setattr(
        job_daily,
        "_fetch_repo_metrics_for_day",
        lambda primary_sink, org_id, day: persisted_repo_metrics,
    )
    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {str(repo_id): "gh:platform"},
    )

    written_count = job_daily._write_compounding_risk_team_rows_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        repo_names_by_id={repo_id: "acme/unmatched-by-any-pattern"},
        # Resolves nothing for this repo -- mirrors a native GitHub org whose
        # teams all carry repo_patterns=[].
        repo_team_resolver=_Resolver(),
        computed_at=NOW,
    )

    assert written_count == 1
    team_row = sink.written[0]
    assert team_row.scope == "team"
    assert team_row.scope_id == "gh:platform"


def test_finalize_prefers_ownership_map_over_conflicting_pattern_resolver(
    monkeypatch: Any,
) -> None:
    repo_id = uuid.uuid4()
    persisted_repo_metrics = [_RepoMetricsRow(repo_id=repo_id)]
    sink = _Sink()

    monkeypatch.setattr(
        job_daily,
        "_fetch_repo_metrics_for_day",
        lambda primary_sink, org_id, day: persisted_repo_metrics,
    )
    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {str(repo_id): "gh:platform"},
    )

    written_count = job_daily._write_compounding_risk_team_rows_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        # _Resolver resolves this exact full_name to "team-platform".
        repo_names_by_id={repo_id: "acme/backend"},
        repo_team_resolver=_Resolver(),
        computed_at=NOW,
    )

    assert written_count == 1
    team_row = sink.written[0]
    assert team_row.scope_id == "gh:platform"


def test_finalize_sums_a_two_repo_team_instead_of_keeping_only_the_last_repo(
    monkeypatch: Any,
) -> None:
    """RED before the CHAOS-4365 finalize-step fix: run_daily_metrics_job
    processes one repo per call (CHAOS-4264), so a team-scope row built
    in-process from a single call's own repo only ever reflected THAT repo
    -- the SECOND repo's per-call write then argMax(computed_at)-deduped
    over the first, silently dropping it (confirmed live: contributing_repo_
    count stuck at 1 for a 2-repo team). This exercises the finalize-step
    replacement with BOTH of a team's repos read back together, as CH
    readback now does for real, and asserts the team row's raw inputs are
    the MEAN across both repos (_build_team_rows' documented contract) --
    not either repo's own value alone.
    """
    repo_a = uuid.uuid4()
    repo_b = uuid.uuid4()
    persisted_repo_metrics = [
        _RepoMetricsRow(repo_id=repo_a, rework_churn_ratio_30d=0.10),
        _RepoMetricsRow(repo_id=repo_b, rework_churn_ratio_30d=0.30),
    ]
    sink = _Sink()

    class _TwoRepoResolver:
        def resolve(self, full_name: str) -> tuple[str | None, str | None]:
            return ("team-platform", "Platform")

    monkeypatch.setattr(
        job_daily,
        "_fetch_repo_metrics_for_day",
        lambda primary_sink, org_id, day: persisted_repo_metrics,
    )
    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {},
    )

    written_count = job_daily._write_compounding_risk_team_rows_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        repo_names_by_id={repo_a: "acme/a", repo_b: "acme/b"},
        repo_team_resolver=_TwoRepoResolver(),
        computed_at=NOW,
    )

    # Exactly ONE team row for the day, not one per repo.
    assert written_count == 1
    team_row = sink.written[0]
    assert team_row.scope == "team"
    assert team_row.scope_id == "team-platform"
    # Mean of both repos' rework_churn (0.10, 0.30) -> 0.20, not either
    # repo's own 0.10 or 0.30 alone.
    assert team_row.rework_churn == pytest.approx(0.20)


def test_repo_to_team_map_ignores_ownership_for_a_repo_outside_the_current_catalog() -> (
    None
):
    """CHAOS-4365 codex round 2 (P1): ``team_repo_ownership`` rows never
    expire on their own writers only ever INSERT (CHAOS-2610 tracks
    writer-side ``valid_to`` retirement) -- so a repo removed/renamed since
    auto-import last ran can still carry a stale ownership row. The map must
    not attribute a team-scope row to a repo absent from
    ``repo_names_by_id`` (this run's current ``repos`` catalog), matching
    what the pattern-resolver-only path already required.
    """
    orphan_repo_id = uuid.uuid4()
    catalog_repo_id = uuid.uuid4()

    mapping = job_daily._repo_to_team_map_for_compounding_risk(
        repo_metrics_rows=[
            _RepoMetricsRow(repo_id=orphan_repo_id),
            _RepoMetricsRow(repo_id=catalog_repo_id),
        ],
        # Only catalog_repo_id is in the current repos catalog.
        repo_names_by_id={catalog_repo_id: "acme/still-exists"},
        repo_team_resolver=_Resolver(),
        team_repo_ownership_map={
            str(orphan_repo_id): "gh:stale-owner",
            str(catalog_repo_id): "gh:platform",
        },
    )

    assert str(orphan_repo_id) not in mapping
    assert mapping[str(catalog_repo_id)] == "gh:platform"
