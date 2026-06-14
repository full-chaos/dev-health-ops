from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

from dev_health_ops.metrics.compute_deployments import (
    DEPLOYMENT_FAILURE_STATUSES,
    compute_deploy_metrics_daily,
)
from dev_health_ops.metrics.schemas import DeploymentRow


def test_failure_status_normalization_counts_every_provider_vocab():
    """CHAOS-2395: failed_deployments_count must count BOTH GitHub
    ('failure', 'error') and GitLab ('failed', 'canceled') failure vocab.

    Deployment rows are persisted with the raw provider status (no
    normalization), so both vocabularies coexist in the ``deployments`` table.
    Counting only one provider's vocabulary silently drops the other's
    failures and biases DORA change-failure-rate toward 0.
    """
    day = date(2026, 2, 18)
    repo_id = uuid4()
    computed_at = datetime(2026, 2, 18, 15, 0, tzinfo=timezone.utc)
    deployed_at = datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc)

    # Single repo + single day. One 'success' (excluded) and the full
    # cross-provider failure vocabulary (all 4 counted).
    deployments: list[DeploymentRow] = [
        {
            "repo_id": repo_id,
            "deployment_id": f"d-{status}",
            "status": status,
            "environment": "prod",
            "started_at": deployed_at,
            "finished_at": None,
            "deployed_at": deployed_at,
        }
        for status in ["success", "failure", "failed", "error", "canceled"]
    ]

    records = compute_deploy_metrics_daily(
        day=day,
        deployments=deployments,
        computed_at=computed_at,
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.repo_id == repo_id
    assert rec.day == day
    # All 5 deployments occurred on the day -> counted.
    assert rec.deployments_count == 5
    # 'success' excluded; every provider failure vocab counted.
    assert rec.failed_deployments_count == 4


def test_github_failure_status_is_counted():
    """Guards the original omission: 'failure' (GitHub deployment vocab) must
    be classified as a failure. Counting only {failed,error,canceled} would
    silently drop GitHub failures.
    """
    # The shared source-of-truth set must include the GitHub-specific token.
    assert "failure" in DEPLOYMENT_FAILURE_STATUSES

    day = date(2026, 2, 18)
    repo_id = uuid4()
    computed_at = datetime(2026, 2, 18, 15, 0, tzinfo=timezone.utc)
    deployed_at = datetime(2026, 2, 18, 12, 0, tzinfo=timezone.utc)

    deployments: list[DeploymentRow] = [
        {
            "repo_id": repo_id,
            "deployment_id": "gh-1",
            "status": "failure",
            "environment": "prod",
            "started_at": deployed_at,
            "finished_at": None,
            "deployed_at": deployed_at,
        }
    ]

    records = compute_deploy_metrics_daily(
        day=day,
        deployments=deployments,
        computed_at=computed_at,
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.deployments_count == 1
    assert rec.failed_deployments_count == 1
