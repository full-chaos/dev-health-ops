"""CHAOS-4246: cicd/deploy/incident/testops_risk visibility when a family
computes zero rows.

Before this fix, ``run_daily_metrics_job`` wrote whatever
``compute_cicd_metrics_daily``/``compute_deploy_metrics_daily``/
``compute_incident_metrics_daily``/``compute_release_confidence``/
``compute_quality_drag``/``compute_pipeline_stability`` returned with no
signal when that was an empty list -- ``write_*`` methods no-op on an empty
sequence, so a day where these families produced nothing was
indistinguishable from a day that succeeded normally (the real-world
incident: 4 of these tables went stale for 16 days while every
``metrics.daily_partition`` reported ``succeeded``).

These tests pin: (1) an empty family is recorded in the job's returned
``families_zero_rows`` map and via the
``dev_health_metrics_family_failures_total`` counter, for EVERY day
processed; (2) a family that produced rows is not recorded; (3) the job
never raises for an empty family -- this is a degrade, not a failure
(deliberate: a day with no CI/deploy/incident activity is legitimate).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import pytest

import dev_health_ops.connectors  # noqa: F401  # lgtm[py/unused-import]
from dev_health_ops.metrics import job_daily

DAY = date(2025, 12, 18)
ORG_ID = "22222222-2222-2222-2222-222222222222"


class _RecordingSink:
    org_id = ""
    teams: list[Any] = []

    def __init__(self, db_url: str) -> None:
        pass

    def ensure_tables(self) -> None:
        return None

    async def get_all_teams(self) -> list[Any]:
        return []

    def __getattr__(self, name: str) -> Any:
        if name.startswith("write_"):
            return lambda *a, **k: None
        raise AttributeError(name)


class _FakeLoader:
    """All sources empty by default; `cicd_rows` overrides load_cicd_data."""

    def __init__(self, cicd_rows: tuple[list, list] | None = None) -> None:
        self._cicd_rows = cicd_rows or ([], [])

    async def load_git_rows(self, *a: Any, **k: Any) -> tuple[list, list, list]:
        return [], [], []

    async def load_cicd_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return self._cicd_rows

    async def load_testops_pipeline_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_test_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

    async def load_testops_historical_failed_case_names(
        self, *a: Any, **k: Any
    ) -> dict:
        return {}

    async def load_testops_coverage_data(self, *a: Any, **k: Any) -> list:
        return []

    async def load_incidents(self, *a: Any, **k: Any) -> list:
        return []

    async def load_work_items(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []


class _NullResolver:
    def resolve(self, *a: Any, **k: Any) -> tuple[None, None]:
        return (None, None)


def _neutralize_daily_job(monkeypatch: Any, *, sink: Any, loader: Any) -> None:
    monkeypatch.setattr(job_daily, "ClickHouseMetricsSink", lambda db_url: sink)

    async def fake_get_loader(*a: Any, **k: Any) -> Any:
        return loader

    monkeypatch.setattr(job_daily, "_get_loader", fake_get_loader)

    async def _noop_init_team_resolver(*a: Any, **k: Any) -> None:
        return None

    monkeypatch.setattr(job_daily, "init_team_resolver", _noop_init_team_resolver)
    monkeypatch.setattr(job_daily, "get_team_resolver", _NullResolver)
    monkeypatch.setattr(
        job_daily, "build_repo_pattern_resolver", lambda *a, **k: _NullResolver()
    )
    monkeypatch.setattr(job_daily, "load_identity_resolver", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "discover_repos", lambda **k: [])
    monkeypatch.setattr(
        job_daily, "build_governance_rows_for_day", lambda *a, **k: ([], [])
    )
    monkeypatch.setattr(
        job_daily, "_extract_ai_workflow_for_day", lambda **k: ([], [], [], [], [], [])
    )
    monkeypatch.setattr(job_daily, "compute_ai_impact_metrics_daily", lambda **k: [])
    monkeypatch.setattr(job_daily, "run_benchmarking_for_day", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "_write_compounding_risk_for_day", lambda **k: 0)


_ALL_ZERO_ROW_FAMILIES = {
    "cicd",
    "deploy",
    "incident",
    "testops_risk.release_confidence",
    "testops_risk.quality_drag",
    "testops_risk.pipeline_stability",
}


@pytest.mark.asyncio
async def test_empty_families_are_recorded_and_do_not_raise(
    monkeypatch: Any,
) -> None:
    recorded: list[tuple[str, str]] = []
    monkeypatch.setattr(
        job_daily,
        "record_metrics_family_zero_rows",
        lambda *, family, cause: recorded.append((family, cause)),
    )

    sink = _RecordingSink("clickhouse://test")
    loader = _FakeLoader()
    _neutralize_daily_job(monkeypatch, sink=sink, loader=loader)

    result = await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
    )

    assert set(result[DAY]) == _ALL_ZERO_ROW_FAMILIES
    assert {family for family, _cause in recorded} == _ALL_ZERO_ROW_FAMILIES
    assert all(cause == "no_rows_computed" for _family, cause in recorded)


@pytest.mark.asyncio
async def test_cicd_not_recorded_when_pipeline_data_present(
    monkeypatch: Any,
) -> None:
    repo_id = uuid.uuid4()
    pipeline_row = {
        "repo_id": repo_id,
        "run_id": "1",
        "status": "success",
        "started_at": datetime(2025, 12, 18, 10, 0, tzinfo=timezone.utc),
        "finished_at": datetime(2025, 12, 18, 10, 5, tzinfo=timezone.utc),
        "queued_at": None,
    }
    recorded: list[str] = []
    monkeypatch.setattr(
        job_daily,
        "record_metrics_family_zero_rows",
        lambda *, family, cause: recorded.append(family),
    )

    sink = _RecordingSink("clickhouse://test")
    loader = _FakeLoader(cicd_rows=([pipeline_row], []))
    _neutralize_daily_job(monkeypatch, sink=sink, loader=loader)

    result = await job_daily.run_daily_metrics_job(
        db_url="clickhouse://test",
        day=DAY,
        backfill_days=1,
        provider="auto",
        org_id=ORG_ID,
        skip_finalize=True,
    )

    # cicd produced a row this run -- it must NOT be flagged zero, but
    # deploy/incident/testops_risk (still empty) must still be.
    assert "cicd" not in result[DAY]
    assert "cicd" not in recorded
    assert {"deploy", "incident"} <= set(result[DAY])
