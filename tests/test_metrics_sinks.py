import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import text

from dev_health_ops.metrics.schemas import (
    DORAMetricsRecord,
    RepoMetricsDailyRecord,
    WorkItemMetricsDailyRecord,
)
from dev_health_ops.metrics.sinks.clickhouse import ClickHouseMetricsSink
from dev_health_ops.metrics.sinks.sqlite import SQLiteMetricsSink


def test_sqlite_sink_writes_repo_knowledge_metrics(tmp_path):
    db_path = tmp_path / "dev_health_ops.metrics.db"
    sink = SQLiteMetricsSink(f"sqlite:///{db_path}")
    try:
        sink.ensure_tables()
        repo_id = uuid.uuid4()
        row = RepoMetricsDailyRecord(
            repo_id=repo_id,
            day=date(2025, 1, 1),
            commits_count=1,
            total_loc_touched=10,
            avg_commit_size_loc=10.0,
            large_commit_ratio=0.0,
            prs_merged=0,
            median_pr_cycle_hours=0.0,
            bus_factor=3,
            code_ownership_gini=0.42,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_repo_metrics([row])

        with sink.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT bus_factor, code_ownership_gini
                    FROM repo_metrics_daily
                    WHERE repo_id = :repo_id
                    """
                ),
                {"repo_id": str(repo_id)},
            ).fetchone()

        assert result is not None
        assert result[0] == 3
        assert result[1] == pytest.approx(0.42)
    finally:
        sink.close()


def test_sqlite_sink_writes_predictability_score(tmp_path):
    db_path = tmp_path / "dev_health_ops.metrics.db"
    sink = SQLiteMetricsSink(f"sqlite:///{db_path}")
    try:
        sink.ensure_tables()
        row = WorkItemMetricsDailyRecord(
            day=date(2025, 1, 1),
            provider="jira",
            work_scope_id="scope-1",
            team_id="team-1",
            team_name="Team 1",
            items_started=2,
            items_completed=1,
            items_started_unassigned=0,
            items_completed_unassigned=0,
            wip_count_end_of_day=1,
            wip_unassigned_end_of_day=0,
            cycle_time_p50_hours=None,
            cycle_time_p90_hours=None,
            lead_time_p50_hours=None,
            lead_time_p90_hours=None,
            wip_age_p50_hours=None,
            wip_age_p90_hours=None,
            bug_completed_ratio=0.0,
            story_points_completed=0.0,
            predictability_score=0.75,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_work_item_metrics([row])

        with sink.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT predictability_score
                    FROM work_item_metrics_daily
                    WHERE provider = :provider
                      AND work_scope_id = :work_scope_id
                      AND team_id = :team_id
                      AND day = :day
                    """
                ),
                {
                    "provider": "jira",
                    "work_scope_id": "scope-1",
                    "team_id": "team-1",
                    "day": "2025-01-01",
                },
            ).fetchone()

        assert result is not None
        assert result[0] == pytest.approx(0.75)
    finally:
        sink.close()


def test_clickhouse_sink_includes_repo_knowledge_columns():
    mock_client = MagicMock()
    mock_client.insert = MagicMock()
    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.clickhouse_connect.get_client",
        return_value=mock_client,
    ):
        sink = ClickHouseMetricsSink("clickhouse://localhost:8123/default")
        row = RepoMetricsDailyRecord(
            repo_id=uuid.uuid4(),
            day=date(2025, 1, 1),
            commits_count=1,
            total_loc_touched=5,
            avg_commit_size_loc=5.0,
            large_commit_ratio=0.0,
            prs_merged=0,
            median_pr_cycle_hours=0.0,
            bus_factor=2,
            code_ownership_gini=0.35,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_repo_metrics([row])

        args, kwargs = mock_client.insert.call_args
        columns = kwargs["column_names"]
        assert args[0] == "repo_metrics_daily"
        assert "bus_factor" in columns
        assert "code_ownership_gini" in columns
        assert args[1][0][columns.index("bus_factor")] == 2
        assert args[1][0][columns.index("code_ownership_gini")] == pytest.approx(0.35)


def test_clickhouse_sink_includes_predictability_score():
    mock_client = MagicMock()
    mock_client.insert = MagicMock()
    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.clickhouse_connect.get_client",
        return_value=mock_client,
    ):
        sink = ClickHouseMetricsSink("clickhouse://localhost:8123/default")
        row = WorkItemMetricsDailyRecord(
            day=date(2025, 1, 1),
            provider="jira",
            work_scope_id="scope-1",
            team_id="team-1",
            team_name="Team 1",
            items_started=2,
            items_completed=1,
            items_started_unassigned=0,
            items_completed_unassigned=0,
            wip_count_end_of_day=1,
            wip_unassigned_end_of_day=0,
            cycle_time_p50_hours=None,
            cycle_time_p90_hours=None,
            lead_time_p50_hours=None,
            lead_time_p90_hours=None,
            wip_age_p50_hours=None,
            wip_age_p90_hours=None,
            bug_completed_ratio=0.0,
            story_points_completed=0.0,
            predictability_score=0.65,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_work_item_metrics([row])

        args, kwargs = mock_client.insert.call_args
        columns = kwargs["column_names"]
        assert args[0] == "work_item_metrics_daily"
        assert "predictability_score" in columns
        assert args[1][0][columns.index("predictability_score")] == pytest.approx(0.65)


def test_sqlite_sink_writes_dora_metrics(tmp_path):
    db_path = tmp_path / "dev_health_ops.metrics.db"
    sink = SQLiteMetricsSink(f"sqlite:///{db_path}")
    try:
        sink.ensure_tables()
        repo_id = uuid.uuid4()
        row = DORAMetricsRecord(
            repo_id=repo_id,
            day=date(2025, 1, 1),
            metric_name="lead_time_for_changes",
            value=12.5,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_dora_metrics([row])

        with sink.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT value
                    FROM dora_metrics_daily
                    WHERE repo_id = :repo_id
                      AND day = :day
                      AND metric_name = :metric_name
                    """
                ),
                {
                    "repo_id": str(repo_id),
                    "day": "2025-01-01",
                    "metric_name": "lead_time_for_changes",
                },
            ).fetchone()

        assert result is not None
        assert result[0] == pytest.approx(12.5)
    finally:
        sink.close()


def test_clickhouse_sink_writes_dora_metrics():
    mock_client = MagicMock()
    mock_client.insert = MagicMock()
    with patch(
        "dev_health_ops.metrics.sinks.clickhouse.clickhouse_connect.get_client",
        return_value=mock_client,
    ):
        sink = ClickHouseMetricsSink("clickhouse://localhost:8123/default")
        row = DORAMetricsRecord(
            repo_id=uuid.uuid4(),
            day=date(2025, 1, 1),
            metric_name="change_failure_rate",
            value=0.02,
            computed_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        )
        sink.write_dora_metrics([row])

        args, kwargs = mock_client.insert.call_args
        columns = kwargs["column_names"]
        assert args[0] == "dora_metrics_daily"
        assert "metric_name" in columns
        assert "value" in columns
        assert args[1][0][columns.index("metric_name")] == "change_failure_rate"
        assert args[1][0][columns.index("value")] == pytest.approx(0.02)
