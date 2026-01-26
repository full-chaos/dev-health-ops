import uuid
from datetime import date, datetime, timezone

from sqlalchemy import create_engine, text

from dev_health_ops.connectors.models import DORAMetric, DORAMetrics
from dev_health_ops.metrics import job_dora


def test_run_dora_metrics_job_writes_sqlite(monkeypatch, tmp_path):
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

    repo_id = uuid.uuid4()
    db_path = tmp_path / "dora.db"
    db_url = f"sqlite:///{db_path}"

    job_dora.run_dora_metrics_job(
        db_url=db_url,
        day=date(2025, 1, 1),
        backfill_days=1,
        repo_id=repo_id,
        repo_name="group/project",
        auth="token",
    )

    engine = create_engine(db_url)
    with engine.connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM dora_metrics_daily
                WHERE repo_id = :repo_id
                """
            ),
            {"repo_id": str(repo_id)},
        ).scalar_one()

    assert count == len(job_dora.DEFAULT_DORA_METRICS)
