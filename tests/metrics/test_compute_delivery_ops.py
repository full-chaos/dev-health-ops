"""cicd/incident compute unit tests.

deploy's own test (test_compute_deploy_metrics_daily_handles_fallbacks_and_
negatives) used to live here too, until CHAOS-5234/CHAOS-3092 deleted
compute_deploy_metrics_daily entirely (deploy is NATIVE, CHAOS-4293, and
DEPLOYMENT_FAILURE_STATUSES -- the one symbol from compute_deployments.py
still in production use, by compute_dora.py -- keeps its own coverage in
test_job_dora.py's cross-provider-vocab parametrized tests, unaffected by
this deletion).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from dev_health_ops.metrics.compute_cicd import compute_cicd_metrics_daily
from dev_health_ops.metrics.compute_incidents import compute_incident_metrics_daily
from dev_health_ops.metrics.schemas import IncidentRow, PipelineRunRow


def test_compute_cicd_metrics_daily_groups_by_repo_and_filters_day():
    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_b = uuid4()

    rows: list[PipelineRunRow] = [
        {
            "repo_id": repo_a,
            "run_id": "1",
            "status": "success",
            "queued_at": datetime(2026, 2, 18, 9, 50, tzinfo=timezone.utc),
            "started_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 10, 30, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_a,
            "run_id": "2",
            "status": "failed",
            "queued_at": datetime(2026, 2, 18, 11, 15, tzinfo=timezone.utc),
            "started_at": datetime(2026, 2, 18, 11, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_b,
            "run_id": "3",
            "status": "passed",
            "queued_at": None,
            "started_at": datetime(2026, 2, 18, 8, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 8, 20, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_a,
            "run_id": "old",
            "status": "success",
            "queued_at": None,
            "started_at": datetime(2026, 2, 17, 23, 59, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 0, 5, tzinfo=timezone.utc),
        },
    ]

    records = compute_cicd_metrics_daily(
        day=day,
        pipeline_runs=rows,
        computed_at=datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
    )

    assert [r.repo_id for r in records] == sorted([repo_a, repo_b], key=str)

    rec_a = next(r for r in records if r.repo_id == repo_a)
    assert rec_a.pipelines_count == 2
    assert rec_a.success_rate == pytest.approx(0.5)
    assert rec_a.avg_duration_minutes == pytest.approx(45.0)
    assert rec_a.p90_duration_minutes == pytest.approx(57.0)
    assert rec_a.avg_queue_minutes == pytest.approx(10.0)

    rec_b = next(r for r in records if r.repo_id == repo_b)
    assert rec_b.pipelines_count == 1
    assert rec_b.success_rate == pytest.approx(1.0)
    assert rec_b.avg_duration_minutes == pytest.approx(20.0)
    assert rec_b.p90_duration_minutes == pytest.approx(20.0)
    assert rec_b.avg_queue_minutes is None


def test_compute_incident_metrics_daily_counts_incidents_and_mttr_distribution():
    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_b = uuid4()

    incidents: list[IncidentRow] = [
        {
            "repo_id": repo_a,
            "incident_id": "i1",
            "status": "resolved",
            "started_at": datetime(2026, 2, 18, 8, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_a,
            "incident_id": "i2",
            "status": "resolved",
            "started_at": datetime(2026, 2, 18, 6, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2026, 2, 18, 18, 0, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_b,
            "incident_id": "i3",
            "status": "resolved",
            "started_at": datetime(2026, 2, 18, 11, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
        },
        {
            "repo_id": repo_a,
            "incident_id": "old",
            "status": "resolved",
            "started_at": datetime(2026, 2, 17, 11, 0, tzinfo=timezone.utc),
            "resolved_at": datetime(2026, 2, 17, 12, 0, tzinfo=timezone.utc),
        },
    ]

    records = compute_incident_metrics_daily(
        day=day,
        incidents=incidents,
        computed_at=datetime(2026, 2, 18, 20, 0, tzinfo=timezone.utc),
    )

    rec_a = next(r for r in records if r.repo_id == repo_a)
    assert rec_a.incidents_count == 2
    assert rec_a.mttr_p50_hours == pytest.approx(8.0)
    assert rec_a.mttr_p90_hours == pytest.approx(11.2)

    rec_b = next(r for r in records if r.repo_id == repo_b)
    assert rec_b.incidents_count == 1
    assert rec_b.mttr_p50_hours is None
    assert rec_b.mttr_p90_hours is None
