from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from dev_health_ops.metrics.compute_deployments import compute_deploy_metrics_daily
from dev_health_ops.metrics.schemas import DeploymentRow

# CHAOS-5234/CHAOS-3092: test_compute_cicd_metrics_daily_groups_by_repo_and_
# filters_day and test_compute_incident_metrics_daily_counts_incidents_and_
# mttr_distribution used to live in this file, exercising
# compute_cicd_metrics_daily/compute_incident_metrics_daily directly. Both
# families' Python compute was deleted outright (native Go executors,
# CICDExecutor/CHAOS-4292 and IncidentExecutor/CHAOS-4269/CHAOS-4295, are
# the only writers now) -- their own dedicated Go frozen-golden parity tests
# (compute_test.go in each family's package) are the surviving coverage.
# compute_deploy_metrics_daily is untouched by this deletion and stays here.


def test_compute_deploy_metrics_daily_handles_fallbacks_and_negatives():
    day = date(2026, 2, 18)
    repo_a = uuid4()
    repo_b = uuid4()

    deployments: list[DeploymentRow] = [
        {
            "repo_id": repo_a,
            "deployment_id": "d1",
            "status": "success",
            "environment": "prod",
            "started_at": datetime(2026, 2, 18, 10, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc),
            "deployed_at": datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc),
            "merged_at": datetime(2026, 2, 18, 9, 0, tzinfo=timezone.utc),
            "pull_request_number": 1,
        },
        {
            "repo_id": repo_a,
            "deployment_id": "d2",
            "status": "canceled",
            "environment": "prod",
            "started_at": datetime(2026, 2, 18, 13, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 13, 30, tzinfo=timezone.utc),
            "deployed_at": datetime(2026, 2, 18, 13, 30, tzinfo=timezone.utc),
            "merged_at": datetime(2026, 2, 18, 14, 0, tzinfo=timezone.utc),
            "pull_request_number": 2,
        },
        {
            "repo_id": repo_b,
            "deployment_id": "d3",
            "status": "failed",
            "environment": "staging",
            "started_at": datetime(2026, 2, 18, 8, 0, tzinfo=timezone.utc),
            "finished_at": datetime(2026, 2, 18, 9, 0, tzinfo=timezone.utc),
            "deployed_at": None,
            "merged_at": None,
            "pull_request_number": None,
        },
    ]

    records = compute_deploy_metrics_daily(
        day=day,
        deployments=deployments,
        computed_at=datetime(2026, 2, 18, 15, 0, tzinfo=timezone.utc),
    )

    rec_a = next(r for r in records if r.repo_id == repo_a)
    assert rec_a.deployments_count == 2
    assert rec_a.failed_deployments_count == 1
    assert rec_a.deploy_time_p50_hours == pytest.approx(1.25)
    assert rec_a.lead_time_p50_hours == pytest.approx(3.0)

    rec_b = next(r for r in records if r.repo_id == repo_b)
    assert rec_b.deployments_count == 1
    assert rec_b.failed_deployments_count == 1
    assert rec_b.deploy_time_p50_hours == pytest.approx(1.0)
    assert rec_b.lead_time_p50_hours is None
