from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

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

    assert written_count == 2
    assert {row.scope for row in sink.written} == {"repo", "team"}
    repo_row = next(row for row in sink.written if row.scope == "repo")
    assert repo_row.scope_id == str(repo_id)
    assert repo_row.org_id == "acme"
    assert repo_row.compounding_risk is not None
    assert repo_row.complexity_delta == 0.30


def test_daily_job_resolves_team_via_ownership_map_when_pattern_resolver_misses(
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

    def fake_fetch_repo_metrics_for_day(
        primary_sink: Any, org_id: str, day: date
    ) -> list[_RepoMetricsRow]:
        return persisted_repo_metrics

    monkeypatch.setattr(
        job_daily, "_fetch_repo_metrics_for_day", fake_fetch_repo_metrics_for_day
    )
    # CHAOS-4365 codex round 3: _write_compounding_risk_for_day now loads
    # the ownership map itself (per day, as-of that day) rather than
    # accepting it as a parameter, so tests inject it via this module-level
    # loader instead of a direct kwarg.
    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {str(repo_id): "gh:platform"},
    )

    written_count = job_daily._write_compounding_risk_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=[],
        computed_at=NOW,
        repo_names_by_id={repo_id: "acme/unmatched-by-any-pattern"},
        # Resolves nothing for this repo -- mirrors a native GitHub org whose
        # teams all carry repo_patterns=[].
        repo_team_resolver=_Resolver(),
    )

    assert written_count == 2
    assert {row.scope for row in sink.written} == {"repo", "team"}
    team_row = next(row for row in sink.written if row.scope == "team")
    assert team_row.scope_id == "gh:platform"


def test_daily_job_prefers_ownership_map_over_conflicting_pattern_resolver(
    monkeypatch: Any,
) -> None:
    repo_id = uuid.uuid4()
    persisted_repo_metrics = [_RepoMetricsRow(repo_id=repo_id)]
    sink = _Sink()

    def fake_fetch_repo_metrics_for_day(
        primary_sink: Any, org_id: str, day: date
    ) -> list[_RepoMetricsRow]:
        return persisted_repo_metrics

    monkeypatch.setattr(
        job_daily, "_fetch_repo_metrics_for_day", fake_fetch_repo_metrics_for_day
    )
    monkeypatch.setattr(
        job_daily,
        "load_team_repo_ownership_map",
        lambda sink, org_id, *, as_of: {str(repo_id): "gh:platform"},
    )

    written_count = job_daily._write_compounding_risk_for_day(
        sinks=[sink],
        primary_sink=sink,
        day=DAY,
        org_id="acme",
        repo_metrics_rows=[],
        computed_at=NOW,
        # _Resolver resolves this exact full_name to "team-platform".
        repo_names_by_id={repo_id: "acme/backend"},
        repo_team_resolver=_Resolver(),
    )

    assert written_count == 2
    team_row = next(row for row in sink.written if row.scope == "team")
    assert team_row.scope_id == "gh:platform"


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
