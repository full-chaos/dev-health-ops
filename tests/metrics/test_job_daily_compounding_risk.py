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
