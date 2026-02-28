import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.connectors.models import DORAMetric, DORAMetrics
from dev_health_ops.metrics import job_dora


def test_run_dora_metrics_job_rejects_sqlite():
    """DORA metrics job should reject non-ClickHouse backends (CHAOS-641)."""
    with pytest.raises(ValueError, match="Only ClickHouse is supported"):
        job_dora.run_dora_metrics_job(
            db_url="sqlite:///test.db",
            day=date(2025, 1, 1),
            backfill_days=1,
            repo_id=uuid.uuid4(),
            repo_name="group/project",
            auth="token",
            org_id="test-org",
        )


def test_run_dora_metrics_job_writes_clickhouse(monkeypatch):
    class FakeGitLabConnector:
        def __init__(self, url: str, private_token: str) -> None:
            self.url = url
            self.private_token = private_token

        def get_dora_metrics(
            self,
            project_name: str,
            metric: str,
            start_date: str | None = None,
            end_date: str | None = None,
            interval: str = "daily",
        ) -> DORAMetrics:
            return DORAMetrics(
                metric_name=metric,
                data_points=[
                    DORAMetric(
                        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
                        value=1.25,
                    )
                ],
            )

        def close(self) -> None:
            return

    monkeypatch.setattr(job_dora, "GitLabConnector", FakeGitLabConnector)

    mock_sink = MagicMock()
    mock_sink.client = MagicMock()

    repo_id = uuid.uuid4()

    with patch(
        "dev_health_ops.metrics.job_dora.ClickHouseMetricsSink",
        return_value=mock_sink,
    ):
        job_dora.run_dora_metrics_job(
            db_url="clickhouse://localhost:8123/default",
            day=date(2025, 1, 1),
            backfill_days=1,
            repo_id=repo_id,
            repo_name="group/project",
            auth="token",
            org_id="test-org",
        )

    # Verify the sink's write method was called
    assert mock_sink.write_dora_metrics.called
