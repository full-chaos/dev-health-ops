"""CHAOS-4246: deploy visibility when a family computes zero rows.

Before this fix, ``run_daily_metrics_job`` wrote whatever
``compute_cicd_metrics_daily``/``compute_deploy_metrics_daily``/
``compute_incident_metrics_daily`` returned with no signal when that was an
empty list -- ``write_*`` methods no-op on an empty sequence, so a day where
these families produced nothing was indistinguishable from a day that
succeeded normally (the real-world incident: 4 of these tables went stale for
16 days while every ``metrics.daily_partition`` reported ``succeeded``).

testops_risk (``compute_release_confidence``/``compute_quality_drag``/
``compute_pipeline_stability``) used to be covered here too, until
CHAOS-5245 deleted its Python compute entirely -- there is no more
degrade-signal path for it to test. cicd and incident used to be covered
here too, until CHAOS-5234/CHAOS-3092 deleted their Python compute (and
their own ``_note_family_zero_rows`` calls) entirely, the same rule
CHAOS-5245 applied to testops_risk -- ``deploy`` is the only family left
with a degrade-signal path to test.

These tests pin: (1) an empty family is recorded in the job's returned
``families_zero_rows`` map and via the
``dev_health_metrics_family_failures_total`` counter, for EVERY day
processed; (2) the job never raises for an empty family -- this is a
degrade, not a failure (deliberate: a day with no deploy activity is
legitimate).
"""

from __future__ import annotations

from datetime import date
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
    """All sources empty.

    load_cicd_data still exists here (job_daily.py still calls it --
    pipeline_rows feeds active_repos even though cicd's own compute+write is
    deleted, CHAOS-5234/CHAOS-3092), but nothing in this file parameterizes
    it any more -- the only test that used to (test_cicd_not_recorded_when_
    pipeline_data_present) tested cicd's zero-rows-note behavior, which no
    longer exists.
    """

    async def load_git_rows(self, *a: Any, **k: Any) -> tuple[list, list, list]:
        return [], [], []

    async def load_cicd_data(self, *a: Any, **k: Any) -> tuple[list, list]:
        return [], []

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
    # CHAOS-5234/CHAOS-3092: no build_governance_rows_for_day to neutralize
    # here anymore -- job_daily.py no longer calls it at all (deleted, not
    # skip-gated; see CHAOS-5233's shape for work_item_attribution).
    monkeypatch.setattr(
        job_daily, "_extract_ai_workflow_for_day", lambda **k: ([], [], [])
    )
    # CHAOS-5234/CHAOS-3092: no compute_ai_impact_metrics_daily to neutralize
    # here anymore -- job_daily.py no longer calls it at all (deleted, not
    # skip-gated; see CHAOS-5233's shape for work_item_attribution).
    monkeypatch.setattr(job_daily, "run_benchmarking_for_day", lambda *a, **k: None)
    monkeypatch.setattr(job_daily, "_write_compounding_risk_for_day", lambda **k: 0)


_ALL_ZERO_ROW_FAMILIES = {
    "deploy",
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
    )

    assert set(result[DAY]) == _ALL_ZERO_ROW_FAMILIES
    assert {family for family, _cause in recorded} == _ALL_ZERO_ROW_FAMILIES
    assert all(cause == "no_rows_computed" for _family, cause in recorded)

    # CHAOS-5234/CHAOS-3092: test_cicd_not_recorded_when_pipeline_data_present
    # used to live here, pinning that a non-empty cicd compute suppressed
    # cicd's own zero-rows note while deploy/incident's (still empty) fired.
    # cicd's compute+write+note call sites are all deleted outright now (see
    # tests/metrics/test_job_daily_skip_families.py's
    # test_cicd_compute_and_write_are_deleted_from_job_daily), and
    # incident's likewise -- there is no more zero-rows-note path for either
    # family to test here at all.
